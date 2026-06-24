#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock
from urllib import parse as urllib_parse
from urllib import request as urllib_request


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"


def _load_module():
    runner_dir = RUNNER_PATH.parent
    sys.path.insert(0, str(runner_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_runner_ui_contract", RUNNER_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(runner_dir):
            sys.path.pop(0)


_RUNNER = _load_module()


class TestTestRunnerUiContract(unittest.TestCase):
    def test_ci_log_prefix_lines_prefixes_each_nonempty_line(self) -> None:
        text = _RUNNER._ci_log_prefix_lines("a\n\nb\n", now=0.0)
        self.assertEqual(
            text,
            "[1970-01-01 00:00:00 UTC] a\n\n[1970-01-01 00:00:00 UTC] b\n",
        )

    def test_ci_wait_progress_tail_reads_incremental_remote_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with mock.patch.object(
                _RUNNER,
                "_instance_read_text_if_present",
                return_value="line1\nline2\nline3\n",
            ):
                offset, chunk = _RUNNER._ci_wait_progress_tail(
                    {},
                    run_dir=run_dir,
                    last_offset=6,
                )
        self.assertEqual(offset, len("line1\nline2\nline3\n"))
        self.assertEqual(chunk, "line2\nline3\n")

    def test_print_ci_wait_progress_emits_heartbeat_when_no_new_tail(self) -> None:
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with mock.patch.object(_RUNNER, "_ci_wait_progress_tail", return_value=(0, "")):
                with mock.patch.object(_RUNNER.time, "time", return_value=100.0):
                    with mock.patch.object(_RUNNER.sys, "stdout", buf):
                        offset, next_heartbeat = _RUNNER._print_ci_wait_progress(
                            {},
                            run_dir=run_dir,
                            last_offset=0,
                            next_heartbeat_at=90.0,
                            deadline=160.0,
                        )
        self.assertEqual(offset, 0)
        self.assertEqual(next_heartbeat, 115.0)
        self.assertIn(
            "[1970-01-01 00:01:40 UTC] [CI wait exit_code] waiting for ci_runner progress...",
            buf.getvalue(),
        )

    def test_print_ci_wait_progress_emits_new_tail(self) -> None:
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            with mock.patch.object(_RUNNER, "_ci_wait_progress_tail", return_value=(12, "a\nb\n")):
                with mock.patch.object(_RUNNER.time, "time", return_value=100.0):
                    with mock.patch.object(_RUNNER.sys, "stdout", buf):
                        offset, next_heartbeat = _RUNNER._print_ci_wait_progress(
                            {},
                            run_dir=run_dir,
                            last_offset=0,
                            next_heartbeat_at=999.0,
                            deadline=160.0,
                        )
        self.assertEqual(offset, 12)
        self.assertEqual(next_heartbeat, 115.0)
        self.assertEqual(
            buf.getvalue(),
            "[1970-01-01 00:01:40 UTC] a\n[1970-01-01 00:01:40 UTC] b\n",
        )

    def test_runner_stdio_mirror_enabled_only_for_github_actions(self) -> None:
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}, clear=True):
            self.assertTrue(_RUNNER._runner_stdio_mirror_enabled())
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "false"}, clear=True):
            self.assertFalse(_RUNNER._runner_stdio_mirror_enabled())
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(_RUNNER._runner_stdio_mirror_enabled())

    def test_redirect_process_stdio_starts_mirror_on_github_actions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            original_log_fp = _RUNNER._RUNNER_STDIO_LOG_FP
            original_keepalive = _RUNNER._RUNNER_STDIO_KEEPALIVE_FDS
            saved_stdout = sys.stdout
            saved_stderr = sys.stderr
            with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}, clear=False):
                _RUNNER._RUNNER_STDIO_LOG_FP = None
                _RUNNER._RUNNER_STDIO_KEEPALIVE_FDS = (11, 12)
                with mock.patch.object(_RUNNER, "_start_runner_stdio_log_mirror") as start_mirror:
                    with mock.patch.object(_RUNNER.os, "dup2") as dup2_mock:
                        with mock.patch.object(_RUNNER.os, "fdopen", side_effect=lambda *args, **kwargs: sys.__stdout__):
                            _RUNNER._redirect_process_stdio_to_log(workdir)
            self.assertEqual(dup2_mock.call_count, 2)
            start_mirror.assert_called_once()
            kwargs = start_mirror.call_args.kwargs
            expected_log_path = _RUNNER._service_log_base_path(
                workdir, filename=_RUNNER.RUNNER_STDIO_LOG_FILENAME
            )
            self.assertEqual(kwargs["log_path"], expected_log_path)
            self.assertEqual(kwargs["stdout_fd"], 11)
            self.assertNotIn("stderr_fd", kwargs)
            sys.stdout = saved_stdout
            sys.stderr = saved_stderr
            if _RUNNER._RUNNER_STDIO_LOG_FP is not None and _RUNNER._RUNNER_STDIO_LOG_FP not in (
                sys.__stdout__,
                sys.__stderr__,
            ):
                _RUNNER._RUNNER_STDIO_LOG_FP.close()
            _RUNNER._RUNNER_STDIO_LOG_FP = original_log_fp
            _RUNNER._RUNNER_STDIO_KEEPALIVE_FDS = original_keepalive

    def test_redirect_process_stdio_skips_mirror_outside_github_actions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            original_log_fp = _RUNNER._RUNNER_STDIO_LOG_FP
            original_keepalive = _RUNNER._RUNNER_STDIO_KEEPALIVE_FDS
            saved_stdout = sys.stdout
            saved_stderr = sys.stderr
            with mock.patch.dict(os.environ, {}, clear=True):
                _RUNNER._RUNNER_STDIO_LOG_FP = None
                _RUNNER._RUNNER_STDIO_KEEPALIVE_FDS = (11, 12)
                with mock.patch.object(_RUNNER, "_start_runner_stdio_log_mirror") as start_mirror:
                    with mock.patch.object(_RUNNER.os, "dup2") as dup2_mock:
                        with mock.patch.object(_RUNNER.os, "fdopen", side_effect=lambda *args, **kwargs: sys.__stdout__):
                            _RUNNER._redirect_process_stdio_to_log(workdir)
            self.assertEqual(dup2_mock.call_count, 2)
            start_mirror.assert_not_called()
            sys.stdout = saved_stdout
            sys.stderr = saved_stderr
            if _RUNNER._RUNNER_STDIO_LOG_FP is not None and _RUNNER._RUNNER_STDIO_LOG_FP not in (
                sys.__stdout__,
                sys.__stderr__,
            ):
                _RUNNER._RUNNER_STDIO_LOG_FP.close()
            _RUNNER._RUNNER_STDIO_LOG_FP = original_log_fp
            _RUNNER._RUNNER_STDIO_KEEPALIVE_FDS = original_keepalive

    def test_collect_suite_overview_reads_case_runs_and_run_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            _RUNNER._write_yaml_file(
                workdir / "case_runs.yaml",
                {
                    "schema_version": 1,
                    "cases": [
                        {
                            "case_id": "case_a",
                            "case_key": "scene_a__scale_a__profile_a",
                            "total_runs": 1,
                            "counted_runs": 1,
                            "success_runs": 1,
                            "failed_runs": 0,
                            "last_run": {
                                "run_index": 1,
                                "outcome": "SUCCESS",
                                "finished_at_unix_s": 20,
                            },
                        }
                    ],
                },
            )
            run_dir = workdir / "results" / "case_a" / "run_1"
            run_dir.mkdir(parents=True)
            _RUNNER._write_yaml_file(
                run_dir / "summary.yaml",
                {
                    "schema_version": 1,
                    "case_id": "case_a",
                    "case_key": "scene_a__scale_a__profile_a",
                    "run_index": 1,
                    "outcome": "SUCCESS",
                    "counted": True,
                    "timing": {"started_at_unix_s": 10, "finished_at_unix_s": 20},
                    "ci": {"rc": 0},
                },
            )
            overview = _RUNNER._ui_collect_suite_overview(workdir)
            self.assertEqual(len(overview["cases"]), 1)
            case = overview["cases"][0]
            self.assertEqual(case["case_id"], "case_a")
            self.assertEqual(case["success_runs"], 1)
            self.assertEqual(len(case["runs"]), 1)
            self.assertEqual(case["runs"][0]["outcome"], "SUCCESS")

    def test_resolve_run_log_path_stays_under_run_dir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "logs").mkdir()
            log_path = run_dir / "logs" / "ci.log"
            log_path.write_text("ok\n", encoding="utf-8")
            resolved = _RUNNER._ui_resolve_run_log_path(run_dir, name="logs/ci.log")
            self.assertEqual(resolved, log_path.resolve())
            with self.assertRaises(ValueError):
                _RUNNER._ui_resolve_run_log_path(run_dir, name="../escape.log")

    def test_log_chunk_tail_and_before_window(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sample.log"
            path.write_text("0123456789", encoding="utf-8")
            tail = _RUNNER._ui_log_chunk(path, from_offset=None, before_offset=None, max_bytes=4)
            self.assertEqual(tail["text"], "6789")
            self.assertEqual(tail["start"], 6)
            older = _RUNNER._ui_log_chunk(path, from_offset=None, before_offset=6, max_bytes=4)
            self.assertEqual(older["text"], "2345")
            self.assertEqual(older["start"], 2)

    def test_service_log_resolve_read_path_prefers_latest_daily_shard(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            (workdir / "test_runner.2026-06-19.log").write_text("old\n", encoding="utf-8")
            (workdir / "test_runner.2026-06-20.log").write_text("new\n", encoding="utf-8")
            resolved = _RUNNER._service_log_resolve_read_path(
                workdir,
                filename=_RUNNER.RUNNER_STDIO_LOG_FILENAME,
            )
            self.assertEqual(
                resolved,
                (workdir / "test_runner.2026-06-20.log").resolve(),
            )

    def test_ops_logs_base_url_derives_from_controller_proxy(self) -> None:
        url = _RUNNER._ui_ops_logs_base_url("http://127.0.0.1:19080/r/ops/fluxon_testbed")
        self.assertEqual(url, "http://127.0.0.1:19080/logs")

    def test_test_stack_ops_logs_query_uses_stack_cluster_namespace(self) -> None:
        resolved_case = {
            "runtime": {"stack_identity": {"cluster_name": "bench_cluster"}},
            "deploy": {"controller_url": "http://127.0.0.1:19080/r/ops/fluxon_testbed"},
        }
        base_url, query = _RUNNER._ui_test_stack_ops_logs_query(
            resolved_case,
            instance_id="master",
            after_ts="100",
            before_ts=None,
            log_table="fluxon_logs",
            level="warn",
            search="panic",
        )
        self.assertEqual(base_url, "http://127.0.0.1:19080/logs")
        self.assertEqual(query["cluster_name"], "bench_cluster")
        self.assertEqual(query["member_kind"], "kv")
        self.assertEqual(query["role"], "master")
        self.assertEqual(query["member_id"], "master")
        self.assertEqual(query["after_ts"], "100")
        self.assertEqual(query["log_table"], "fluxon_logs")
        self.assertEqual(query["level"], "warn")
        self.assertEqual(query["search"], "panic")

    def test_case_overview_marks_stale_reserved_last_run_as_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            _RUNNER._write_yaml_file(
                workdir / "case_runs.yaml",
                {
                    "schema_version": 1,
                    "cases": [
                        {
                            "case_id": "case_running",
                            "case_key": "scene__scale__profile",
                            "total_runs": 0,
                            "counted_runs": 0,
                            "success_runs": 0,
                            "failed_runs": 0,
                            "last_run": {"run_index": 3},
                        }
                    ],
                },
            )
            run_dir = workdir / "results" / "case_running" / "run_3"
            run_dir.mkdir(parents=True)
            _RUNNER._write_yaml_file(
                run_dir / "summary.yaml",
                {
                    "schema_version": 1,
                    "case_id": "case_running",
                    "case_key": "scene__scale__profile",
                    "run_index": 3,
                    "outcome": "FAILED",
                    "counted": False,
                    "timing": {"started_at_unix_s": 10, "finished_at_unix_s": 10},
                    "error": "INCOMPLETE: run started but did not reach finalize; runner likely exited abruptly.",
                },
            )
            os.utime(workdir / "case_runs.yaml", (10, 10))
            os.utime(run_dir, (10, 10))
            os.utime(run_dir / "summary.yaml", (10, 10))
            with mock.patch.object(_RUNNER.time, "time", return_value=10 + 8 * 86400):
                case = _RUNNER._ui_case_overview(workdir, case_id="case_running")
            self.assertEqual(case["status"], "INCOMPLETE")
            self.assertEqual(case["active_run_index"], 3)

    def test_case_overview_marks_recent_reserved_last_run_as_running(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            _RUNNER._write_yaml_file(
                workdir / "case_runs.yaml",
                {
                    "schema_version": 1,
                    "cases": [
                        {
                            "case_id": "case_running",
                            "case_key": "scene__scale__profile",
                            "total_runs": 0,
                            "counted_runs": 0,
                            "success_runs": 0,
                            "failed_runs": 0,
                            "last_run": {"run_index": 3},
                        }
                    ],
                },
            )
            run_dir = workdir / "results" / "case_running" / "run_3"
            run_dir.mkdir(parents=True)
            _RUNNER._write_yaml_file(
                run_dir / "summary.yaml",
                {
                    "schema_version": 1,
                    "case_id": "case_running",
                    "case_key": "scene__scale__profile",
                    "run_index": 3,
                    "outcome": "FAILED",
                    "counted": False,
                    "timing": {"started_at_unix_s": 10, "finished_at_unix_s": 10},
                    "error": "INCOMPLETE: run started but did not reach finalize; runner likely exited abruptly.",
                },
            )
            os.utime(workdir / "case_runs.yaml", (10, 10))
            os.utime(run_dir, (10, 10))
            os.utime(run_dir / "summary.yaml", (10, 10))
            with mock.patch.object(_RUNNER.time, "time", return_value=10 + 3600):
                case = _RUNNER._ui_case_overview(workdir, case_id="case_running")
            self.assertEqual(case["status"], "RUNNING")
            self.assertEqual(case["active_run_index"], 3)

    def test_ui_case_run_summary_maps_placeholder_failed_to_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td) / "run_1"
            run_dir.mkdir(parents=True)
            _RUNNER._write_yaml_file(
                run_dir / "summary.yaml",
                {
                    "schema_version": 1,
                    "case_id": "case_a",
                    "case_key": "scene__scale__profile",
                    "run_index": 1,
                    "outcome": "FAILED",
                    "counted": False,
                    "timing": {"started_at_unix_s": 10, "finished_at_unix_s": 10},
                    "error": "INCOMPLETE: run started but did not reach finalize; runner likely exited abruptly.",
                },
            )
            summary = _RUNNER._ui_case_run_summary(run_dir)
            self.assertEqual(summary["outcome"], "INCOMPLETE")

    def test_ui_format_unix_ts_uses_full_datetime(self) -> None:
        self.assertEqual(_RUNNER._ui_format_unix_ts(1778344794), "2026-05-10 00:39:54")

    def test_history_register_and_recent_filter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            repo_root = Path(td)
            history_root = repo_root / "fluxon_test_stack" / "test_runner"
            history_root.mkdir(parents=True)
            workdir = repo_root / "suite_a"
            workdir.mkdir()
            (workdir / "case_runs.yaml").write_text("schema_version: 1\ncases: []\n", encoding="utf-8")
            with mock.patch.object(_RUNNER, "_runner_repo_root", return_value=repo_root):
                with mock.patch.object(_RUNNER.time, "time", return_value=1_000_000):
                    _RUNNER._ui_history_register_workdir(workdir)
                recent = _RUNNER._ui_history_list_recent_workdirs(
                    now_unix_s=1_000_000,
                    lookback_days=30,
                )
            self.assertEqual(recent, [workdir.resolve()])

    def test_discover_recent_workdirs_scans_service_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            service_root = Path(td)
            suite = service_root / "service_a"
            suite.mkdir()
            (suite / "case_runs.yaml").write_text("schema_version: 1\ncases: []\n", encoding="utf-8")
            runner_log = suite / "test_runner.log"
            runner_log.write_text("hello\n", encoding="utf-8")
            now = 2_000_000
            with mock.patch.object(_RUNNER.time, "time", return_value=now):
                with mock.patch.object(_RUNNER, "_runner_repo_root", return_value=service_root):
                    with mock.patch.object(_RUNNER, "_ui_default_external_history_roots", return_value=[]):
                        paths = _RUNNER._ui_discover_recent_workdirs(service_root, lookback_days=30)
            self.assertEqual(paths, [suite.resolve()])

    def test_suite_overview_marks_incomplete_when_no_live_running_cases(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            _RUNNER._write_yaml_file(
                workdir / "case_runs.yaml",
                {
                    "schema_version": 1,
                    "cases": [
                        {
                            "case_id": "case_a",
                            "case_key": "scene__scale__profile",
                            "total_runs": 0,
                            "counted_runs": 0,
                            "success_runs": 0,
                            "failed_runs": 0,
                            "last_run": {"run_index": 1},
                        }
                    ],
                },
            )
            run_dir = workdir / "results" / "case_a" / "run_1"
            run_dir.mkdir(parents=True)
            _RUNNER._write_yaml_file(
                run_dir / "summary.yaml",
                {
                    "schema_version": 1,
                    "case_id": "case_a",
                    "case_key": "scene__scale__profile",
                    "run_index": 1,
                    "outcome": "FAILED",
                    "counted": False,
                    "timing": {"started_at_unix_s": 10, "finished_at_unix_s": 10},
                    "error": "INCOMPLETE: run started but did not reach finalize; runner likely exited abruptly.",
                },
            )
            os.utime(workdir / "case_runs.yaml", (10, 10))
            os.utime(run_dir, (10, 10))
            os.utime(run_dir / "summary.yaml", (10, 10))
            with mock.patch.object(_RUNNER.time, "time", return_value=10 + 8 * 86400):
                overview = _RUNNER._ui_collect_suite_overview(workdir)
            self.assertEqual(overview["status"], "INCOMPLETE")
            self.assertEqual(overview["running_case_count"], 0)

    def test_gitops_ui_and_api_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            service_root = root / "service"
            service_root.mkdir()
            gitops_cfg = root / "gitops.yaml"
            gitops_cfg.write_text(
                "\n".join(
                    [
                        "interval: 3600",
                        "retention:",
                        "  max_age_days: 7",
                        "repos:",
                        "  - addr: repo",
                        "    follow:",
                        "      - branch: main",
                        "        run:",
                        "          name_prefix: ci",
                        "          commands:",
                        "            - echo one",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            ctx = _RUNNER.gitops_lib.load_context(
                config_path=gitops_cfg,
                workdir=_RUNNER.gitops_lib.default_runtime_root(service_root),
            )
            run_id = "repo__main__20260608_120000__abcdef0"
            meta_dir = ctx.run_meta_dir / run_id
            steps_dir = meta_dir / "steps"
            steps_dir.mkdir(parents=True)
            run_log = meta_dir / "run.log"
            step_log = steps_dir / "step_1.log"
            progress_file = meta_dir / "progress.jsonl"
            result_file = meta_dir / "result.yaml"
            run_log.write_text("abcdef\n", encoding="utf-8")
            step_log.write_text("step one\n", encoding="utf-8")
            progress_file.write_text(
                json.dumps({"event": "step_started", "payload": {"idx": 1}, "ts": 1.0}) + "\n",
                encoding="utf-8",
            )
            result_file.write_text("ok: true\n", encoding="utf-8")
            _RUNNER._write_yaml_file(
                ctx.run_index_file,
                {
                    "runs": {
                        run_id: {
                            "repo": "repo",
                            "branch": "main",
                            "commit": "abcdef0123456789",
                            "name_prefix": "ci",
                            "status": "ok",
                            "started_ts": "2026-06-08T12:00:00",
                            "finished_ts": "2026-06-08T12:01:00",
                            "log_file": str(run_log),
                            "progress_file": str(progress_file),
                            "result_file": str(result_file),
                            "run_dir": str(ctx.runs_dir / "sample_run_dir"),
                            "meta_dir": str(meta_dir),
                        }
                    }
                },
            )
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind(("127.0.0.1", 0))
                port = int(sock.getsockname()[1])
            with mock.patch.object(_RUNNER, "_ui_discovery_roots", return_value=[service_root.resolve()]):
                server_thread = threading.Thread(
                    target=_RUNNER._serve_test_runner_ui,
                    kwargs={
                        "workdir_root": service_root,
                        "host": "127.0.0.1",
                        "port": port,
                        "lookback_days": 30,
                        "extra_history_roots": [],
                        "gitops_ctx": ctx,
                    },
                    daemon=True,
                )
                server_thread.start()
                base_url = f"http://127.0.0.1:{port}"
                deadline = time.time() + 5.0
                while True:
                    try:
                        with urllib_request.urlopen(base_url + "/health", timeout=0.5) as resp:
                            health = json.loads(resp.read().decode("utf-8"))
                        if health.get("ok") is True:
                            break
                    except Exception:
                        if time.time() >= deadline:
                            raise
                        time.sleep(0.1)
                self.assertEqual(health["service"], "test_runner_ui")
                self.assertEqual(health["workdir_root"], str(service_root.resolve()))
                self.assertEqual(health["host"], "127.0.0.1")
                self.assertEqual(health["port"], port)
                self.assertEqual(health["lookback_days"], 30)
                self.assertEqual(health["history_roots"], [])
                self.assertEqual(health["gitops_config_path"], str(gitops_cfg.resolve()))
                with urllib_request.urlopen(base_url + "/", timeout=3.0) as resp:
                    root_html = resp.read().decode("utf-8")
                self.assertIn("href='/gitops'", root_html)
                with urllib_request.urlopen(base_url + "/api/gitops/state", timeout=1.0) as resp:
                    state = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(state["ok"])
                self.assertEqual(state["run_count"], 1)
                self.assertEqual(state["runs"][0]["id"], run_id)
                chunk_url = (
                    base_url
                    + "/api/gitops/log_chunk?"
                    + urllib_parse.urlencode({"id": run_id, "kind": "run", "max_bytes": 4})
                )
                with urllib_request.urlopen(chunk_url, timeout=1.0) as resp:
                    chunk = json.loads(resp.read().decode("utf-8"))
                self.assertTrue(chunk["ok"])
                self.assertEqual(chunk["text"], "def\n")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
