#!/usr/bin/env python3

from __future__ import annotations

import copy
import importlib.util
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"


def _load_module():
    runner_dir = RUNNER_PATH.parent
    sys.path.insert(0, str(runner_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_runner_testbed_contract", RUNNER_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(runner_dir):
            sys.path.pop(0)


_RUNNER = _load_module()
_CI_RUNTIME_MOD = sys.modules["test_runner_ci_runtime"]


def _top_attention_command(
    *,
    command_id: str,
    script_name: str,
    case_config: bool = False,
    timeout_seconds: int = 21600,
) -> dict:
    command = (
        "__RUN_DIR__/venv/bin/python3 -u "
        f"__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/{script_name}"
    )
    if case_config:
        command += " --case-config __RUN_DIR__/configs/ci_scene_config.yaml"
    return {
        "id": command_id,
        "command": command,
        "timeout_seconds": timeout_seconds,
    }


def _suite_cfg_with_declared_ci_commands(command_map: dict[str, list[dict]]) -> dict:
    suite_cfg = yaml.safe_load(
        (_RUNNER.RUNNER_REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").read_text(encoding="utf-8")
    )
    for scene_id, commands in command_map.items():
        suite_cfg["scenes"][scene_id]["ci"]["commands"] = copy.deepcopy(commands)
    return suite_cfg


class TestTestRunnerTestbedContract(unittest.TestCase):
    def test_write_ci_master_owner_configs_emits_owner_large_file_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            resolved_case = {
                "deploy": {
                    "instances": [
                        {"id": "master", "deployer": {"target": "local-node-a"}},
                        {"id": "owner_0", "deployer": {"target": "local-node-a"}},
                    ],
                    "target_ip_map": {"local-node-a": "127.0.0.1"},
                }
            }

            with mock.patch.object(_RUNNER, "_ci_base_runtime_service_target_ip", side_effect=["127.0.0.1", "127.0.0.1"]):
                with mock.patch.object(_RUNNER, "_ci_base_runtime_service_port", side_effect=[19180, 19190]):
                    _, owner_path = _RUNNER._write_ci_master_owner_configs(
                        resolved_case,
                        run_dir=run_dir,
                        cluster_name="ci_cluster",
                        share_mem_path="/tmp/ci_shm",
                        owner_dram_bytes=1073741824,
                    )

            owner_cfg = yaml.safe_load(owner_path.read_text(encoding="utf-8"))
            self.assertEqual(
                owner_cfg["fluxonkv_spec"]["large_file_paths"],
                [str((run_dir / "services" / "owner_0" / "large").resolve())],
            )
            self.assertNotIn("shared_file_path", owner_cfg["fluxonkv_spec"])

    def test_ci_owner_prepare_wait_uses_shared_bundle_timeout_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            owner_cfg_path = run_dir / "configs" / "ci_owner_0.yaml"
            owner_cfg_path.parent.mkdir(parents=True)
            _RUNNER._write_yaml_file(
                owner_cfg_path,
                {
                    "fluxonkv_spec": {
                        "cluster_name": "ci_cluster",
                        "share_mem_path": "/tmp/ci_shm",
                    },
                },
            )
            resolved_case = {"runtime": {"run_dir": str(run_dir)}}

            with mock.patch.object(_RUNNER, "_wait_instance_running") as wait_running:
                with mock.patch.object(
                    _RUNNER,
                    "_wait_ci_owner_shared_bundle_ready_and_stage_shared_json",
                ) as wait_shared_bundle:
                    _RUNNER._wait_ci_instance_ready(resolved_case, instance_id="owner_0")

            wait_running.assert_called_once_with(resolved_case, instance_id="owner_0", timeout_s=60)
            wait_shared_bundle.assert_called_once()
            self.assertEqual(
                wait_shared_bundle.call_args.kwargs["timeout_s"],
                _RUNNER.CI_RUNNER_SHARED_BUNDLE_TIMEOUT_S,
            )

    def test_ci_runtime_python_executable_requires_python310_on_path(self) -> None:
        with mock.patch.object(_RUNNER.shutil, "which", return_value=None):
            with self.assertRaisesRegex(ValueError, "requires a Python 3.10 interpreter on PATH"):
                _RUNNER._ci_runtime_python_executable()

    def test_ci_runtime_python_executable_accepts_python3_alias_when_it_is_python310(self) -> None:
        with mock.patch.object(
            _RUNNER.shutil,
            "which",
            side_effect=lambda name: {
                "python3.10": None,
                "python3": "/usr/bin/python3",
                "python": "/usr/bin/python",
            }.get(name),
        ):
            with mock.patch.object(_CI_RUNTIME_MOD, "_python_executable_abi", return_value="cpython3.10"):
                self.assertEqual(_RUNNER._ci_runtime_python_executable(), "/usr/bin/python3")

    def test_create_ci_runtime_venv_uses_python310_abi_and_seeds_pip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            venv_dir = (run_dir / "venv").resolve()
            expected_venv_python = (venv_dir / "bin" / "python3").resolve()
            observed_calls: list[list[str]] = []

            def _fake_create_venv(argv: list[str], *, cwd: str) -> None:
                observed_calls.append(argv)
                self.assertEqual(cwd, str(run_dir))
                if len(observed_calls) == 1:
                    self.assertEqual(
                        argv,
                        [
                            "/usr/bin/python3.10",
                            "-m",
                            "venv",
                            "--without-pip",
                            str(venv_dir),
                        ],
                    )
                    expected_venv_python.parent.mkdir(parents=True, exist_ok=True)
                    expected_venv_python.write_text("#!/bin/sh\n", encoding="utf-8")
                    return
                if len(observed_calls) == 2:
                    self.assertEqual(
                        argv,
                        [
                            str(expected_venv_python),
                            "-m",
                            "ensurepip",
                            "--upgrade",
                            "--default-pip",
                        ],
                    )
                    return
                if len(observed_calls) == 3:
                    self.assertEqual(
                        argv,
                        [
                            str(expected_venv_python),
                            "-m",
                            "pip",
                            "--version",
                        ],
                    )
                    return
                self.fail(f"unexpected _run_subprocess call: argv={argv!r}")

            with mock.patch.object(_RUNNER.shutil, "which", return_value="/usr/bin/python3.10"):
                with mock.patch.object(_CI_RUNTIME_MOD, "_python_executable_abi", return_value="cpython3.10"):
                    with mock.patch.object(_RUNNER, "_run_subprocess", side_effect=_fake_create_venv) as run_subprocess_mock:
                        with mock.patch.object(_RUNNER, "_assert_ci_runtime_python_abi") as assert_python_abi:
                            venv_python = _RUNNER._create_ci_runtime_venv(run_dir=run_dir)

            self.assertEqual(venv_python, expected_venv_python)
            self.assertEqual(
                observed_calls,
                [
                    ["/usr/bin/python3.10", "-m", "venv", "--without-pip", str(venv_dir)],
                    [str(expected_venv_python), "-m", "ensurepip", "--upgrade", "--default-pip"],
                    [str(expected_venv_python), "-m", "pip", "--version"],
                ],
            )
            self.assertEqual(run_subprocess_mock.call_count, 3)
            assert_python_abi.assert_called_once_with(venv_python=expected_venv_python)

    def test_declared_bin_kvtest_scene_stays_on_direct_wrapper_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_bin_kvtest": [
                    _top_attention_command(
                        command_id="top_attention_bin_kvtest",
                        script_name="_bin_kvtest.py",
                        case_config=True,
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_bin_kvtest" and item.profile_id == "fluxon_tcp")

        planned = _RUNNER._build_ci_execution_plan(case, suite)

        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_bin_kvtest")
        self.assertIn(
            "fluxon_test_stack/top_attention_test_index/_bin_kvtest.py",
            planned[0].ci_commands[0]["command"],
        )

    def test_run_subprocess_reports_cwd_and_argv_on_failure(self) -> None:
        completed = subprocess.CompletedProcess(
            args=["/usr/bin/python3", "-c", "raise SystemExit(2)"],
            returncode=2,
            stdout="",
            stderr="boom\n",
        )
        with mock.patch.object(_RUNNER.subprocess, "run", return_value=completed):
            with self.assertRaisesRegex(
                RuntimeError,
                r"command failed: rc=2 cwd=/tmp argv=/usr/bin/python3 -c 'raise SystemExit\(2\)'",
            ):
                _RUNNER._run_subprocess(
                    ["/usr/bin/python3", "-c", "raise SystemExit(2)"],
                    cwd="/tmp",
                )

    def test_assert_ci_runtime_python_abi_accepts_python310_venv(self) -> None:
        with mock.patch.object(_RUNNER.subprocess, "check_output", return_value="cpython3.10\n") as check_output_mock:
            _RUNNER._assert_ci_runtime_python_abi(venv_python=Path("/tmp/venv/bin/python3"))

        check_output_mock.assert_called_once()

    def test_assert_ci_runtime_python_abi_rejects_non_python310_venv(self) -> None:
        with mock.patch.object(_RUNNER.subprocess, "check_output", return_value="cpython3.11\n"):
            with self.assertRaisesRegex(ValueError, "must match the prepared offline wheelhouse"):
                _RUNNER._assert_ci_runtime_python_abi(venv_python=Path("/tmp/venv/bin/python3"))

    def test_ci_runtime_tracked_apply_entries_groups_shared_apply_id(self) -> None:
        tracking = _RUNNER._CaseRuntimeTracking(
            ci_attempted_instance_ids=["master", "owner_0", "ci_runner"],
            ci_apply_ids={
                "master": "apply-cluster",
                "owner_0": "apply-cluster",
                "ci_runner": "apply-runner",
            },
        )

        entries = _RUNNER._ci_runtime_tracked_apply_entries(tracking)

        self.assertEqual(
            entries,
            [
                {"apply_id": "apply-cluster", "instance_ids": ["master", "owner_0"]},
                {"apply_id": "apply-runner", "instance_ids": ["ci_runner"]},
            ],
        )

    def test_finalize_ci_case_runtime_deletes_each_apply_id_once(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            tracking = _RUNNER._CaseRuntimeTracking(
                ci_attempted_instance_ids=["master", "owner_0", "ci_runner"],
                ci_apply_ids={
                    "master": "apply-cluster",
                    "owner_0": "apply-cluster",
                    "ci_runner": "apply-runner",
                },
            )
            resolved_case = {
                "case": {
                    "run_mode": _RUNNER.RUN_MODE_FULL_ONCE,
                    "case_id": "ci_top_attention_mq_core__n1_kvowner_dram_20gib__fluxon_tcp_thread",
                }
            }

            with mock.patch.object(_RUNNER, "_delete_apply_id") as delete_apply:
                with mock.patch.object(_RUNNER, "_ci_cleanup_runtime") as cleanup_runtime:
                    _RUNNER._finalize_ci_case_runtime(
                        resolved_case,
                        run_dir=run_dir,
                        runtime_tracking=tracking,
                        outcome=_RUNNER.RUN_OUTCOME_SUCCESS,
                    )

            self.assertEqual(
                [call.kwargs["apply_id"] for call in delete_apply.call_args_list],
                ["apply-runner", "apply-cluster"],
            )
            cleanup_runtime.assert_called_once_with(resolved_case, timeout_s=120)

    def test_finalize_ci_case_runtime_preserves_structured_instance_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            tracking = _RUNNER._CaseRuntimeTracking(
                ci_attempted_instance_ids=["master", "owner_0", "ci_runner"],
                ci_apply_ids={
                    "master": "apply-cluster",
                    "owner_0": "apply-cluster",
                    "ci_runner": "apply-runner",
                },
            )
            resolved_case = {
                "case": {
                    "run_mode": _RUNNER.RUN_MODE_DEBUG_ONE_BY_ONE,
                    "case_id": "ci_top_attention_mq_core__n1_kvowner_dram_20gib__fluxon_tcp_thread",
                }
            }

            _RUNNER._finalize_ci_case_runtime(
                resolved_case,
                run_dir=run_dir,
                runtime_tracking=tracking,
                outcome=_RUNNER.RUN_OUTCOME_FAILED,
            )

            payload = yaml.safe_load((run_dir / _RUNNER.CI_PRESERVED_APPLY_IDS_FILENAME).read_text(encoding="utf-8"))
            self.assertEqual(
                payload,
                {
                    "schema_version": _RUNNER.CI_PRESERVED_APPLY_IDS_SCHEMA_VERSION,
                    "apply_ids": [
                        {"instance_ids": ["master", "owner_0"], "apply_id": "apply-cluster"},
                        {"instance_ids": ["ci_runner"], "apply_id": "apply-runner"},
                    ],
                },
            )

    def test_finalize_test_stack_case_runtime_collects_status_and_records_collect_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            summary_path = run_dir / "summary.yaml"
            _RUNNER._write_yaml_file(
                summary_path,
                {
                    "schema_version": _RUNNER.SCHEMA_VERSION,
                    "case_id": "bench_case",
                    "case_key": "bench_case_key",
                    "run_index": 1,
                    "outcome": _RUNNER.RUN_OUTCOME_FAILED,
                    "counted": False,
                    "timing": {
                        "started_at_unix_s": 100,
                        "finished_at_unix_s": 200,
                    },
                    "test_stack": {
                        "coordinator_addr": "127.0.0.1:19999",
                        "completion_signal": "benchmark_result_json",
                        "result_path": str((run_dir / "benchmark_result.json").resolve()),
                        "result": None,
                        "error": "RuntimeError: benchmark failed",
                        "collect_error": None,
                    },
                },
            )
            resolved_case = {
                "case": {
                    "run_mode": _RUNNER.RUN_MODE_DEBUG_ONE_BY_ONE,
                    "case_id": "bench_case",
                    "case_key": "bench_case_key",
                },
                "deploy": {
                    "instances": [
                        {"id": "coordinator", "deployer": {"target": "local-node-a"}},
                        {"id": "node_0", "deployer": {"target": "local-node-b"}},
                    ]
                },
            }
            tracking = _RUNNER._CaseRuntimeTracking(
                ts_coord_deploy_attempted=True,
                ts_coord_apply_id="apply-coord",
                ts_nodes_deploy_attempted=True,
                ts_nodes_apply_id="apply-node",
            )

            def _fake_run_adapter_action(resolved_case, *, run_dir: Path, action: str):
                self.assertEqual(action, "collect")
                instances = _RUNNER._require_list(resolved_case["deploy"]["instances"], "resolved_case.deploy.instances")
                for instance in instances:
                    inst_id = _RUNNER._require_str(instance.get("id"), "deploy.instances[].id")
                    inst_dir = (run_dir / "logs" / inst_id).resolve()
                    inst_dir.mkdir(parents=True, exist_ok=True)
                    _RUNNER._write_yaml_file(
                        inst_dir / "status.yaml",
                        {"status_code": 500, "status": {"ok": False, "instance_id": inst_id}},
                    )
                raise RuntimeError("collect boom")

            with mock.patch.object(_RUNNER, "_run_adapter_action", side_effect=_fake_run_adapter_action):
                with mock.patch.object(_RUNNER, "_delete_apply_id") as delete_apply:
                    _RUNNER._finalize_test_stack_case_runtime(
                        resolved_case,
                        run_dir=run_dir,
                        runtime_tracking=tracking,
                        outcome=_RUNNER.RUN_OUTCOME_FAILED,
                    )

            delete_apply.assert_not_called()
            self.assertTrue((run_dir / "logs" / "coordinator" / "status.yaml").exists())
            self.assertTrue((run_dir / "logs" / "node_0" / "status.yaml").exists())
            updated_summary = yaml.safe_load(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(
                updated_summary["test_stack"]["collect_error"],
                "RuntimeError: collect boom",
            )

    def test_finalize_error_preserves_success_for_ci_and_bench(self) -> None:
        self.assertTrue(
            _RUNNER._preserve_success_after_finalize_error(
                case_family=_RUNNER.CASE_FAMILY_CI,
                outcome=_RUNNER.RUN_OUTCOME_SUCCESS,
            )
        )
        self.assertTrue(
            _RUNNER._preserve_success_after_finalize_error(
                case_family=_RUNNER.CASE_FAMILY_BENCH,
                outcome=_RUNNER.RUN_OUTCOME_SUCCESS,
            )
        )
        self.assertFalse(
            _RUNNER._preserve_success_after_finalize_error(
                case_family=_RUNNER.CASE_FAMILY_CI,
                outcome=_RUNNER.RUN_OUTCOME_FAILED,
            )
        )
        self.assertFalse(
            _RUNNER._preserve_success_after_finalize_error(
                case_family=_RUNNER.CASE_FAMILY_INFER,
                outcome=_RUNNER.RUN_OUTCOME_SUCCESS,
            )
        )

    def test_write_ci_scene_config_yaml_emits_structured_scene_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            resolved_case = {
                "case": {
                    "scene_id": "ci_top_attention_doc_page_build",
                    "scale_id": "n1_kvowner_dram_3gib",
                    "profile_id": "fluxon_tcp_thread",
                    "case_id": "ci_top_attention_doc_page_build__n1_kvowner_dram_3gib__fluxon_tcp_thread",
                },
                "profile": {
                    "ci": {
                        "runtime": {
                            "base_runtime": {
                                "etcd": {
                                    "target": "local-node-a",
                                    "endpoint": {"host_port": 2379, "scheme": "http"},
                                },
                                "greptime": {
                                    "target": "local-node-a",
                                    "endpoint": {"host_port": 4000, "scheme": "http"},
                                },
                            },
                            "deploy": {"target_ip_map": {"local-node-a": "127.0.0.1"}},
                        },
                        "scene_config": {
                            "doc_site_base_url": "tele-ai.github.io/Fluxon",
                        }
                    }
                },
            }
            with mock.patch.object(_RUNNER, "_ci_base_runtime_service_target_ip", side_effect=["127.0.0.1", "127.0.0.1"]):
                with mock.patch.object(_RUNNER, "_ci_base_runtime_service_port", side_effect=[2379, 4000]):
                    path = _RUNNER._write_ci_scene_config_yaml(resolved_case, run_dir=run_dir)

            self.assertEqual(path, (run_dir / "configs" / "ci_scene_config.yaml").resolve())
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["case"]["scene_id"], "ci_top_attention_doc_page_build")
            self.assertEqual(payload["scene_config"]["doc_site_base_url"], "tele-ai.github.io/Fluxon")
            self.assertEqual(payload["scene_runtime"]["etcd"], {"ip": "127.0.0.1", "port": 2379})
            self.assertEqual(payload["scene_runtime"]["greptime"], {"ip": "127.0.0.1", "port": 4000})

    def test_generated_test_stack_owner_config_emits_large_file_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            cfg_dir = run_dir / "configs"
            cfg_dir.mkdir(parents=True)
            owner_target = "local-node-a"
            target_slug = "local-node-a"
            runtime_instance_prefix = "case1"
            coord_tpl = {"deployer": {"target": ""}}
            cluster_nodes = {"local-node-a": {"python_abi": "cpython3.10"}}
            resolved_case = {
                "runtime": {
                    "run_dir": str(run_dir),
                    "stack_identity": {
                        "cluster_name": "bench_cluster",
                        "share_mem_path": "/tmp/bench_shm",
                    },
                }
            }

            with mock.patch.object(_RUNNER, "_test_stack_runtime_required_python_abi", return_value="cpython3.10"):
                with mock.patch.object(_RUNNER, "_test_stack_etcd_addresses", return_value=["127.0.0.1:19180"]):
                    with mock.patch.object(_RUNNER, "_test_stack_target_host_venv_python", return_value="/tmp/venv/bin/python3"):
                        with mock.patch.object(_RUNNER, "_test_stack_runtime_module_command", return_value="owner-cmd"):
                            owner_instances = _RUNNER._build_test_stack_external_kv_owner_instances(
                                scene_mode="bench",
                                resolved_case=resolved_case,
                                scale={"owner": {"owner_count": 1, "owner_dram_bytes": 1073741824}},
                                runtime=resolved_case["runtime"],
                                run_dir=run_dir,
                                cfg_dir=cfg_dir,
                                coord_tpl=coord_tpl,
                                test_stack_runtime={},
                                cluster_nodes=cluster_nodes,
                                owner_targets=[owner_target],
                                needs_kv_master=True,
                                kv_p2p_port_base=31000,
                                kv_p2p_port_stride=100,
                                kv_p2p_slot_offset=0,
                                p2p_ports_per_slot=10,
                                node_total=1,
                                run_index=1,
                                runtime_instance_prefix=runtime_instance_prefix,
                                kv_base={},
                                test_spec_config={},
                                perf_config=None,
                                runtime_env={},
                                owner_group_processes=None,
                                owner_cpu_core_by_target={},
                            )

            self.assertEqual(len(owner_instances), 1)
            owner_cfg_path = cfg_dir / f"test_stack_kv_owner__{target_slug}.yaml"
            owner_cfg = yaml.safe_load(owner_cfg_path.read_text(encoding="utf-8"))
            self.assertEqual(
                owner_cfg["fluxonkv_spec"]["large_file_paths"],
                [str((run_dir / "services" / "kv_owner" / target_slug / "large").resolve())],
            )

    def test_ci_source_overlay_includes_fluxon_test_stack(self) -> None:
        self.assertIn("fluxon_test_stack", _RUNNER._CI_SOURCE_OVERLAY_ROOTS)
        self.assertNotIn("quartz_prewarm", _RUNNER._CI_SOURCE_OVERLAY_ROOTS)

    def test_top_attention_ci_execution_plan_uses_declared_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_bin_kvtest": [
                    _top_attention_command(
                        command_id="top_attention_bin_kvtest",
                        script_name="_bin_kvtest.py",
                        case_config=True,
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_bin_kvtest" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_bin_kvtest")
        self.assertIn("--case-config __RUN_DIR__/configs/ci_scene_config.yaml", planned[0].ci_commands[0]["command"])

    def test_top_attention_cargo_fs_core_ci_execution_plan_uses_declared_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_cargo_fs_core": [
                    _top_attention_command(
                        command_id="top_attention_cargo_fs_core",
                        script_name="_cargo_fs_core.py",
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_cargo_fs_core" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_cargo_fs_core")
        self.assertIn(
            "__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_cargo_fs_core.py",
            planned[0].ci_commands[0]["command"],
        )
        self.assertNotIn("--case-config", planned[0].ci_commands[0]["command"])

    def test_top_attention_cargo_util_ci_execution_plan_uses_declared_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_cargo_util": [
                    _top_attention_command(
                        command_id="top_attention_cargo_util",
                        script_name="_cargo_util.py",
                        case_config=True,
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_cargo_util" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_cargo_util")
        self.assertIn(
            "__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_cargo_util.py",
            planned[0].ci_commands[0]["command"],
        )
        self.assertIn("--case-config __RUN_DIR__/configs/ci_scene_config.yaml", planned[0].ci_commands[0]["command"])

    def test_top_attention_cargo_kv_unit_ci_execution_plan_uses_declared_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_cargo_kv_unit": [
                    _top_attention_command(
                        command_id="top_attention_cargo_kv_unit",
                        script_name="_cargo_kv_unit.py",
                        case_config=True,
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_cargo_kv_unit" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_cargo_kv_unit")
        self.assertIn(
            "__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_cargo_kv_unit.py",
            planned[0].ci_commands[0]["command"],
        )
        self.assertIn("--case-config __RUN_DIR__/configs/ci_scene_config.yaml", planned[0].ci_commands[0]["command"])

    def test_additional_top_attention_cargo_ci_execution_plans_use_declared_commands(self) -> None:
        expected = {
            "ci_top_attention_cargo_cli": ("top_attention_cargo_cli", "_cargo_cli.py"),
            "ci_top_attention_cargo_commu": ("top_attention_cargo_commu", "_cargo_commu.py"),
            "ci_top_attention_cargo_commu_contract": ("top_attention_cargo_commu_contract", "_cargo_commu_contract.py"),
            "ci_top_attention_cargo_framework": ("top_attention_cargo_framework", "_cargo_framework.py"),
            "ci_top_attention_cargo_fs": ("top_attention_cargo_fs", "_cargo_fs.py"),
            "ci_top_attention_cargo_fs_s3_gateway": ("top_attention_cargo_fs_s3_gateway", "_cargo_fs_s3_gateway.py"),
            "ci_top_attention_cargo_limit_thirdparty": ("top_attention_cargo_limit_thirdparty", "_cargo_limit_thirdparty.py"),
            "ci_top_attention_cargo_mq": ("top_attention_cargo_mq", "_cargo_mq.py"),
            "ci_top_attention_cargo_observability": ("top_attention_cargo_observability", "_cargo_observability.py"),
            "ci_top_attention_cargo_ops": ("top_attention_cargo_ops", "_cargo_ops.py"),
            "ci_top_attention_cargo_pyo3": ("top_attention_cargo_pyo3", "_cargo_pyo3.py"),
        }
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                scene_id: [
                    _top_attention_command(
                        command_id=command_id,
                        script_name=script_name,
                    )
                ]
                for scene_id, (command_id, script_name) in expected.items()
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        for scene_id, (command_id, script_name) in expected.items():
            with self.subTest(scene_id=scene_id):
                case = next(item for item in cases if item.scene_id == scene_id and item.profile_id == "fluxon_tcp")
                planned = _RUNNER._build_ci_execution_plan(case, suite)
                self.assertEqual(len(planned), 1)
                self.assertEqual(planned[0].ci_commands[0]["id"], command_id)
                self.assertIn(
                    f"__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/{script_name}",
                    planned[0].ci_commands[0]["command"],
                )
                self.assertNotIn("--case-config", planned[0].ci_commands[0]["command"])

    def test_top_attention_log_mgmt_ci_execution_plan_uses_declared_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_log_mgmt": [
                    _top_attention_command(
                        command_id="top_attention_log_mgmt",
                        script_name="_log_mgmt.py",
                        case_config=True,
                    )
                ]
            }
        )
        artifact_sets = suite_cfg.get("artifact_sets")
        if isinstance(artifact_sets, dict):
            for artifact_set in artifact_sets.values():
                if not isinstance(artifact_set, dict):
                    continue
                release_artifacts = artifact_set.get("release_artifacts")
                if isinstance(release_artifacts, dict):
                    python_wheel = release_artifacts.get("python_wheel")
                    if isinstance(python_wheel, str) and python_wheel.strip():
                        artifact_set["release_artifacts"] = {"wheel": python_wheel}
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_log_mgmt" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_log_mgmt")
        self.assertIn(
            "__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_log_mgmt.py",
            planned[0].ci_commands[0]["command"],
        )
        self.assertIn("--case-config __RUN_DIR__/configs/ci_scene_config.yaml", planned[0].ci_commands[0]["command"])

    def test_top_attention_mq_core_ci_execution_plan_uses_declared_command(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_mq_core": [
                    _top_attention_command(
                        command_id="top_attention_mq_core",
                        script_name="_mq_core.py",
                        case_config=True,
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_mq_core" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_mq_core")
        self.assertIn(
            "__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_mq_core.py",
            planned[0].ci_commands[0]["command"],
        )
        self.assertIn("--case-config __RUN_DIR__/configs/ci_scene_config.yaml", planned[0].ci_commands[0]["command"])

    def test_requested_top_attention_ci_execution_plans_use_declared_commands(self) -> None:
        suite_cfg = copy.deepcopy(
            yaml.safe_load(
                (_RUNNER.RUNNER_REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").read_text(encoding="utf-8")
            )
        )
        requested = {
            "ci_top_attention_ctrl_c_kv": ("rust", "rust_self_managed", "_ctrl_c_kv.py", "top_attention_ctrl_c_kv", False),
            "ci_top_attention_ctrl_c_mq": ("mq", "rust_self_managed", "_ctrl_c_mq.py", "top_attention_ctrl_c_mq", False),
            "ci_top_attention_mq_mpsc": ("mq", "cluster_kv_owner", "_mq_mpsc.py", "top_attention_mq_mpsc", True),
            "ci_top_attention_mq_mpmc": ("mq", "cluster_kv_owner", "_mq_mpmc.py", "top_attention_mq_mpmc", True),
            "ci_top_attention_mq_mpmc_bench": ("mq", "cluster_kv_owner", "_mq_mpmc_bench.py", "top_attention_mq_mpmc_bench", True),
        }
        scene_configs = suite_cfg["profiles"]["fluxon_tcp"]["runtime"]["ci"].setdefault("scene_configs", {})
        for scene_id, (subject, runtime_contract, script_name, command_id, needs_case_config) in requested.items():
            suite_cfg["scenes"][scene_id] = {
                "ci": {
                    "subject": subject,
                    "runtime_contract": runtime_contract,
                    "commands": [
                        _top_attention_command(
                            command_id=command_id,
                            script_name=script_name,
                            case_config=needs_case_config,
                        )
                    ],
                },
                "select": {
                    "scales": ["n1_kvowner_dram_20gib"],
                    "profiles": ["fluxon_tcp"],
                },
            }
            scene_configs[scene_id] = {}

        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        for scene_id, (_subject, runtime_contract, script_name, command_id, needs_case_config) in requested.items():
            with self.subTest(scene_id=scene_id):
                case = next(item for item in cases if item.scene_id == scene_id and item.profile_id == "fluxon_tcp")
                planned = _RUNNER._build_ci_execution_plan(case, suite)
                self.assertEqual(len(planned), 1)
                self.assertEqual(planned[0].ci_commands[0]["id"], command_id)
                command = planned[0].ci_commands[0]["command"]
                self.assertIn(
                    f"__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/{script_name}",
                    command,
                )
                self.assertEqual(
                    "--case-config __RUN_DIR__/configs/ci_scene_config.yaml" in command,
                    needs_case_config,
                )

                resolved_case = {
                    "case": {
                        "family": "ci",
                        "case_id": f"{scene_id}__n1_kvowner_dram_20gib__fluxon_tcp",
                    },
                    "scene": {
                        "ci": {
                            "runtime_contract": runtime_contract,
                            "subject": suite.scenes[scene_id]["ci"]["subject"],
                        },
                    },
                    "deploy": {
                        "instances": (
                            [{"id": "master"}, {"id": "owner_0"}, {"id": "ci_runner"}]
                            if runtime_contract == "cluster_kv_owner"
                            else [{"id": "ci_runner"}]
                        ),
                    },
                    "runtime_model": {
                        "test_bed": {"kind": "ops"},
                        "base_runtime": {},
                        "case_runtime": {
                            "instance_ids": (
                                ["master", "owner_0", "ci_runner"]
                                if runtime_contract == "cluster_kv_owner"
                                else ["ci_runner"]
                            ),
                        },
                    },
                }
                case_plan = _RUNNER._compile_case_plan(resolved_case)
                if runtime_contract == "cluster_kv_owner":
                    self.assertEqual(tuple(phase.phase_id for phase in case_plan.prepare_phases), ("cluster_runtime",))
                else:
                    self.assertEqual(tuple(phase.phase_id for phase in case_plan.prepare_phases), ())
                self.assertEqual(tuple(phase.phase_id for phase in case_plan.execute_phases), ("ci_runner",))

    def test_top_attention_mq_core_ci_plan_has_no_collect_phase(self) -> None:
        resolved_case = {
            "case": {
                "family": "ci",
                "case_id": "ci_top_attention_mq_core__n1_kvowner_dram_20gib__fluxon_tcp_thread",
            },
            "scene": {
                "ci": {
                    "runtime_contract": "cluster_kv_owner",
                    "subject": "mq",
                },
            },
            "deploy": {
                "instances": [
                    {"id": "master"},
                    {"id": "owner_0"},
                    {"id": "ci_runner"},
                ],
            },
            "runtime_model": {
                "test_bed": {"kind": "ops"},
                "base_runtime": {},
                "case_runtime": {"instance_ids": ["master", "owner_0", "ci_runner"]},
            },
        }
        case_plan = _RUNNER._compile_case_plan(resolved_case)
        self.assertEqual(tuple(phase.phase_id for phase in case_plan.prepare_phases), ("cluster_runtime",))
        self.assertEqual(tuple(phase.phase_id for phase in case_plan.execute_phases), ("ci_runner",))
        self.assertEqual(case_plan.execute_phases[0].instance_ids, ("ci_runner",))

    def test_doc_page_ci_execution_plan_uses_online_docker_image(self) -> None:
        suite_cfg = _suite_cfg_with_declared_ci_commands(
            {
                "ci_top_attention_doc_page_build": [
                    _top_attention_command(
                        command_id="top_attention_doc_page_build",
                        script_name="_doc_page_build.py",
                        case_config=True,
                        timeout_seconds=10800,
                    )
                ]
            }
        )
        suite = _RUNNER._parse_suite_config(suite_cfg)
        cases = _RUNNER._expand_cases(suite)
        case = next(item for item in cases if item.scene_id == "ci_top_attention_doc_page_build" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(
            planned[0].ci_prepare_steps,
            [
                {
                    "kind": "online_docker_image",
                    "image_ref": "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
                    "env": "FLUXON_DOC_SITE_DOCKER_IMAGE_REF",
                }
            ],
        )

    def test_ci_prepare_run_inputs_rebuilds_release_view_without_reusing_source_test_rsc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_root = root / "source_root"
            source_root.mkdir()
            (source_root / "README.md").write_text("repo\n", encoding="utf-8")
            source_test_cfg = source_root / "fluxon_py" / "tests" / "test_config.yaml"
            source_test_cfg.parent.mkdir(parents=True, exist_ok=True)
            source_test_cfg.write_text(
                "\n".join(
                    [
                        "kv_svc_type: fluxon",
                        "etcd_address: 127.0.0.1:2379",
                        "cluster_name: fluxon-example-cluster",
                        "share_mem_path: /tmp/fluxon-example-cluster/shm",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            release_root = root / "release_root"
            release_root.mkdir()
            wheel_name = "fluxon-0.2.1-py3-none-any.whl"
            (release_root / wheel_name).write_text("wheel\n", encoding="utf-8")
            (release_root / "install.py").write_text("print('install')\n", encoding="utf-8")
            (release_root / "ext_images").mkdir()
            source_side_test_rsc = release_root / "test_rsc"
            source_side_test_rsc.mkdir()
            (source_side_test_rsc / "from_release.txt").write_text("release\n", encoding="utf-8")

            test_rsc_root = root / "test_rsc_root"
            test_rsc_root.mkdir()
            (test_rsc_root / "from_case.txt").write_text("case\n", encoding="utf-8")
            (test_rsc_root / "prepare.yaml").write_text(
                "\n".join(
                    [
                        "python_runtime:",
                        "  dependency_sets:",
                        "    base:",
                        "      requirements:",
                        "        - pinned: pytest==8.3.5",
                        "          source: wheel",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            wheelhouse_root = test_rsc_root / "python_runtime" / "cpython3.10" / "wheels"
            wheelhouse_root.mkdir(parents=True, exist_ok=True)
            (wheelhouse_root / "pytest-8.3.5-py3-none-any.whl").write_text("wheel\n", encoding="utf-8")

            ci_src_archive_path = test_rsc_root / "src_ci.tar.gz"
            with tarfile.open(ci_src_archive_path, "w:gz") as tf:
                payload = root / "payload.txt"
                payload.write_text("payload\n", encoding="utf-8")
                tf.add(payload, arcname="payload.txt")

            release_manifest = {
                wheel_name: _RUNNER._sha256_file(release_root / wheel_name),
            }
            (release_root / "fluxon_release.sha256").write_text(
                "".join(f"{digest}  {name}\n" for name, digest in release_manifest.items()),
                encoding="utf-8",
            )
            test_rsc_manifest = {
                "src_ci.tar.gz": _RUNNER._sha256_file(ci_src_archive_path),
                "prepare.yaml": _RUNNER._sha256_file(test_rsc_root / "prepare.yaml"),
                "python_runtime/cpython3.10/wheels/pytest-8.3.5-py3-none-any.whl": _RUNNER._sha256_file(
                    wheelhouse_root / "pytest-8.3.5-py3-none-any.whl"
                ),
            }
            (test_rsc_root / "fluxon_test_rsc.sha256").write_text(
                "".join(f"{digest}  {name}\n" for name, digest in test_rsc_manifest.items()),
                encoding="utf-8",
            )

            src_root = root / "src"
            run_dir = root / "run_dir"
            run_dir.mkdir()
            venv_python = run_dir / "venv" / "bin" / "python3"
            venv_python.parent.mkdir(parents=True, exist_ok=True)
            venv_python.write_text("#!/bin/sh\n", encoding="utf-8")
            testbed_bundle_root = root / "testbed_bundle"
            testbed_bundle_root.mkdir()
            start_cfg = testbed_bundle_root / "start_test_bed.runner.yaml"
            deployconf_path = testbed_bundle_root / "deployconf.yaml"
            start_cfg.write_text(
                "\n".join(
                    [
                        "schema_version: 6",
                        "deployconf_path: ./deployconf.yaml",
                        "controller_url: http://127.0.0.1:19080/r/ops/fluxon_testbed",
                        "controller_basic_auth:",
                        "  username: ops_admin",
                        "  password: ops_password",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            deployconf_path.write_text(
                "\n".join(
                    [
                        "global_envs:",
                        "  FLUXON_CLUSTER_NAME: fluxon_testbed",
                        "  FLUXON_SHARED_MEM: ${HOSTWORKDIR}/shm1",
                        "  FLUXON_SHARED_MEM2: ${HOSTWORKDIR}/shm2_files",
                        "cluster_nodes:",
                        "  - hostname: logic-a",
                        "    ip: 127.0.0.1",
                        "    hostworkdir: /tmp/fluxon_testbed/a",
                        "    execution_mode: local",
                        "service:",
                        "  ops_controller:",
                        "    node_bind:",
                        "      node: [logic-a]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            resolved_case = {
                "artifact_set": {
                    "release_artifacts": {"wheel": wheel_name},
                    "test_rsc_artifacts": {
                        "ci_src_archive": "src_ci.tar.gz",
                        "ci_ext_rsc_archive": "fluxon_ci_ext_rsc.tar.gz",
                    },
                }
            }

            env = {**os.environ, _RUNNER.TEST_STACK_START_TEST_BED_CONFIG_ENV: str(start_cfg)}
            with mock.patch.dict(os.environ, env, clear=True):
                with mock.patch.object(_RUNNER, "_assert_ci_runtime_python_abi") as assert_python_abi:
                    with mock.patch.object(_RUNNER, "_run_subprocess") as run_subprocess_mock:
                        _RUNNER._ci_prepare_run_inputs(
                            resolved_case=resolved_case,
                            source_root=source_root,
                            release_root=release_root,
                            test_rsc_root=test_rsc_root,
                            src_root=src_root,
                            venv_python=venv_python,
                            ci_commands=None,
                            overlay_live_checkout=False,
                            etcd_address="127.0.0.1:32579",
                            cluster_name="ci_case_cluster",
                            share_mem_path="/tmp/ci_case_cluster/shm",
                        )

            release_view_root = src_root / "fluxon_release"
            self.assertTrue(release_view_root.is_dir())
            self.assertTrue((release_view_root / "install.py").is_symlink())
            self.assertEqual((release_view_root / "install.py").resolve(), (release_root / "install.py").resolve())
            self.assertTrue((release_view_root / "test_rsc").is_symlink())
            self.assertEqual((release_view_root / "test_rsc").resolve(), test_rsc_root.resolve())
            self.assertFalse((release_view_root / "from_release.txt").exists())
            self.assertTrue((release_view_root / "test_rsc" / "from_case.txt").exists())
            self.assertTrue((src_root / "payload.txt").is_file())
            rendered_test_cfg = yaml.safe_load((src_root / "fluxon_py" / "tests" / "test_config.yaml").read_text(encoding="utf-8"))
            self.assertEqual(
                rendered_test_cfg,
                {
                    "kv_svc_type": "fluxon",
                    "etcd_address": "127.0.0.1:32579",
                    "cluster_name": "ci_case_cluster",
                    "share_mem_path": "/tmp/ci_case_cluster/shm",
                },
            )
            assert_python_abi.assert_called_once_with(venv_python=venv_python)
            self.assertEqual(run_subprocess_mock.call_count, 2)
            first_call = run_subprocess_mock.call_args_list[0]
            second_call = run_subprocess_mock.call_args_list[1]
            self.assertEqual(
                first_call.kwargs["cwd"],
                str(src_root),
            )
            self.assertEqual(
                first_call.args[0],
                [
                    str(venv_python),
                    "-m",
                    "pip",
                    "install",
                    "--no-index",
                    "--find-links",
                    str(wheelhouse_root),
                    "pytest==8.3.5",
                ],
            )
            self.assertEqual(
                second_call.args[0],
                [
                    str(venv_python),
                    "-m",
                    "pip",
                    "install",
                    "--force-reinstall",
                    str(release_root / wheel_name),
                ],
            )

    def test_ci_runner_script_sources_prepare_env_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            src_root = run_dir / "src"
            src_root.mkdir(parents=True)

            resolved_case = {
                "case": {
                    "family": "ci",
                    "case_id": "ci_top_attention_doc_page_build__n1_kvowner_dram_3gib__fluxon_tcp",
                },
                "artifact_set": {
                    "release_artifacts": {"wheel": "fluxon-0.2.1-py3-none-any.whl"},
                    "test_rsc_artifacts": {
                        "ci_src_archive": "src_ci.tar.gz",
                        "ci_ext_rsc_archive": "fluxon_ci_ext_rsc.tar.gz",
                    },
                },
                "scene": {
                    "ci": {
                        "subject": "doc_page",
                        "runtime_contract": "rust_self_managed",
                        "commands": [
                            {
                                "id": "doc_page_build",
                                "command": "__RUN_DIR__/venv/bin/python3 -u __RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_doc_page_build.py --case-config __RUN_DIR__/configs/ci_scene_config.yaml",
                                "timeout_seconds": 10,
                            }
                        ],
                        "prepare": [
                            {
                                "kind": "setup_dev_env",
                                "config": "setup_and_pack/setup_dev_env/ubuntu24.yaml",
                                "cache_relpath": ".cached/fluxon_ci/toolchain",
                            }
                        ],
                    }
                },
                "deploy": {
                    "target_ip_map": {"logic-a": "127.0.0.1"},
                },
                "runtime": {
                    "workdir_root": str(run_dir),
                    "run_dir": str(run_dir),
                    "stack_identity": {
                        "ops_cluster_name": "fluxon_testbed",
                        "cluster_name": "fluxon_testbed",
                        "controller_url": "http://127.0.0.1:19080/r/ops/fluxon_testbed",
                        "share_mem_path": "/tmp/shm",
                    },
                    "deploy_instances": {
                        "case_runtime": [
                            {
                                "id": "ci_runner",
                                "deployer": {"target": "logic-a"},
                            }
                        ]
                    }
                },
                "runtime_model": {
                    "test_bed": {"kind": "ops"},
                    "base_runtime": {},
                    "case_runtime": {"instance_ids": ["ci_runner"]},
                },
            }

            with mock.patch.object(_RUNNER, "_subst_runtime_tokens", side_effect=lambda _case, text: text):
                script_path = _RUNNER._write_ci_runner_script(
                    resolved_case,
                    run_dir=run_dir,
                    src_root=src_root,
                    share_mem_path="/tmp/shm",
                )
            script_text = script_path.read_text(encoding="utf-8")
            self.assertIn('prepare_env_path="', script_text)
            self.assertIn('. "$prepare_env_path"', script_text)

    def test_parse_ci_prepare_steps_accepts_online_docker_image(self) -> None:
        steps = _RUNNER._parse_ci_prepare_steps(
            [
                {
                    "kind": "setup_dev_env",
                    "config": "setup_and_pack/setup_dev_env/ubuntu24.yaml",
                    "cache_relpath": ".cached/fluxon_ci/toolchain",
                },
                {
                    "kind": "online_docker_image",
                    "image_ref": "fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
                    "env": "FLUXON_DOC_SITE_DOCKER_IMAGE_REF",
                },
            ],
            "scene.ci.prepare",
        )
        self.assertEqual(
            steps,
            [
                {
                    "kind": "setup_dev_env",
                    "config": "setup_and_pack/setup_dev_env/ubuntu24.yaml",
                    "cache_relpath": ".cached/fluxon_ci/toolchain",
                },
                {
                    "kind": "online_docker_image",
                    "image_ref": "fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
                    "env": "FLUXON_DOC_SITE_DOCKER_IMAGE_REF",
                },
            ],
        )
        with self.assertRaisesRegex(ValueError, "unknown keys"):
            _RUNNER._parse_ci_prepare_steps(
                [
                    {
                        "kind": "online_docker_image",
                        "image_ref": "example/image:tag",
                        "env": "IMAGE_REF",
                        "config": "x",
                    }
                ],
                "scene.ci.prepare",
            )
        with self.assertRaisesRegex(ValueError, "valid environment variable name"):
            _RUNNER._parse_ci_prepare_steps(
                [
                    {
                        "kind": "online_docker_image",
                        "image_ref": "example/image:tag",
                        "env": "invalid-name",
                    }
                ],
                "scene.ci.prepare",
            )

    def test_run_ci_prepare_online_docker_image_pulls_and_exports_env(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            src_root = root / "src"
            src_root.mkdir()

            with mock.patch.object(_RUNNER, "_run_subprocess") as run_subprocess_mock:
                exports = _RUNNER._run_ci_prepare_online_docker_image_step(
                    step={
                        "kind": "online_docker_image",
                        "image_ref": "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
                        "env": "FLUXON_DOC_SITE_DOCKER_IMAGE_REF",
                    },
                    src_root=src_root,
                    step_index=0,
                )

            self.assertEqual(
                exports,
                {
                    "FLUXON_DOC_SITE_DOCKER_IMAGE_REF": (
                        "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0"
                    )
                },
            )
            run_subprocess_mock.assert_called_once_with(
                [
                    "docker",
                    "pull",
                    "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
                ],
                cwd=str(src_root),
            )

    def test_normalize_test_stack_targets_accepts_hosts_with_consistent_anchors(self) -> None:
        normalized = _RUNNER._normalize_test_stack_target_hosts(
            {
                "hosts": ["logic-a", "logic-b"],
                "primary": "logic-a",
                "secondary": "logic-b",
            },
            machine_count=2,
            ctx="scale.targets",
        )
        self.assertEqual(normalized, ["logic-a", "logic-b"])

    def test_normalize_test_stack_targets_rejects_inconsistent_hosts_and_anchors(self) -> None:
        with self.assertRaisesRegex(ValueError, "must match"):
            _RUNNER._normalize_test_stack_target_hosts(
                {
                    "hosts": ["logic-a", "logic-b"],
                    "primary": "logic-b",
                    "secondary": "logic-a",
                },
                machine_count=2,
                ctx="scale.targets",
            )

    def test_selection_supervisor_authority_comes_from_repo_deployment_codegen(self) -> None:
        _text, script_path = _RUNNER._expected_test_bed_selection_supervisor_text()
        self.assertEqual(script_path, (REPO_ROOT / "deployment" / "gen_bare_deploy_bash.py").resolve())

    def test_bootstrap_runner_uses_repo_start_test_bed_entry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bundle_root = Path(td)
            start_cfg = bundle_root / "start_test_bed.runner.yaml"
            workdir = bundle_root / "bootstrap_workdir"
            start_cfg.write_text("schema_version: 6\n", encoding="utf-8")
            workdir.mkdir()
            manifest_path = bundle_root / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "start_config_path": str(start_cfg),
                        "workdir": str(workdir),
                        "bootstrap_mode": "apply_only",
                    }
                ),
                encoding="utf-8",
            )

            env = {**os.environ, _RUNNER.TEST_STACK_START_TEST_BED_CONFIG_ENV: str(start_cfg)}
            with mock.patch.dict(os.environ, env, clear=True):
                with mock.patch.object(_RUNNER.subprocess, "run") as run_mock:
                    run_mock.return_value = mock.Mock(returncode=0)
                    ok = _RUNNER._bootstrap_test_bed_via_runner()

            self.assertTrue(ok)
            argv = run_mock.call_args.args[0]
            self.assertEqual(argv[0], sys.executable)
            self.assertEqual(argv[1], str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()))
            self.assertEqual(
                argv,
                [
                    sys.executable,
                    str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()),
                    "--config",
                    str(start_cfg),
                    "--workdir",
                    str(workdir),
                    "--bootstrap-mode",
                    "apply_only",
                ],
            )

    def test_load_source_stack_contract_accepts_same_host_dual_local_hostworkdirs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bundle_root = Path(td)
            start_cfg = bundle_root / "start_test_bed.runner.yaml"
            deployconf_path = bundle_root / "deployconf.yaml"
            start_cfg.write_text(
                "\n".join(
                    [
                        "schema_version: 6",
                        "deployconf_path: ./deployconf.yaml",
                        "controller_url: http://127.0.0.1:19080/r/ops/fluxon_testbed",
                        "controller_basic_auth:",
                        "  username: ops_admin",
                        "  password: ops_password",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            deployconf_path.write_text(
                "\n".join(
                    [
                        "global_envs:",
                        "  FLUXON_CLUSTER_NAME: fluxon_testbed",
                        "  FLUXON_SHARED_MEM: ${HOSTWORKDIR}/shm1",
                        "  FLUXON_SHARED_MEM2: ${HOSTWORKDIR}/shm2_files",
                        "cluster_nodes:",
                        "  - hostname: logic-a",
                        "    ip: 127.0.0.1",
                        "    hostworkdir: /tmp/fluxon_testbed/a",
                        "    execution_mode: local",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "  - hostname: logic-b",
                        "    ip: 127.0.0.1",
                        "    hostworkdir: /tmp/fluxon_testbed/b",
                        "    execution_mode: local",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "service:",
                        "  ops_controller:",
                        "    node_bind:",
                        "      node: [logic-a]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            env = {**os.environ, _RUNNER.TEST_STACK_START_TEST_BED_CONFIG_ENV: str(start_cfg)}
            with mock.patch.dict(os.environ, env, clear=True):
                contract = _RUNNER._load_source_stack_contract()

            self.assertEqual(contract["hostworkdir"], "/tmp/fluxon_testbed/a")
            self.assertEqual(contract["ops_cluster_name"], "fluxon_testbed")
            self.assertEqual(
                contract["ops_controller_url"],
                "http://127.0.0.1:19080/r/ops/fluxon_testbed",
            )
            self.assertEqual(contract["share_mem_hostworkdir"], "${HOSTWORKDIR}/shm1")
            self.assertNotIn("shared_memory_hostworkdir", contract)
            self.assertNotIn("shared_file_hostworkdir", contract)

    def test_load_source_stack_contract_rejects_multi_hostworkdir_remote_layout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bundle_root = Path(td)
            start_cfg = bundle_root / "start_test_bed.runner.yaml"
            deployconf_path = bundle_root / "deployconf.yaml"
            start_cfg.write_text(
                "\n".join(
                    [
                        "schema_version: 6",
                        "deployconf_path: ./deployconf.yaml",
                        "controller_url: http://127.0.0.1:19080/r/ops/fluxon_testbed",
                        "controller_basic_auth:",
                        "  username: ops_admin",
                        "  password: ops_password",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            deployconf_path.write_text(
                "\n".join(
                    [
                        "global_envs:",
                        "  FLUXON_CLUSTER_NAME: fluxon_testbed",
                        "  FLUXON_SHARED_MEM: ${HOSTWORKDIR}/shm1",
                        "  FLUXON_SHARED_MEM2: ${HOSTWORKDIR}/shm2_files",
                        "cluster_nodes:",
                        "  - hostname: logic-a",
                        "    ip: 127.0.0.1",
                        "    hostworkdir: /tmp/fluxon_testbed/a",
                        "    execution_mode: local",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "  - hostname: logic-b",
                        "    ip: 127.0.0.2",
                        "    hostworkdir: /tmp/fluxon_testbed/b",
                        "    execution_mode: ssh",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            env = {**os.environ, _RUNNER.TEST_STACK_START_TEST_BED_CONFIG_ENV: str(start_cfg)}
            with mock.patch.dict(os.environ, env, clear=True):
                with self.assertRaisesRegex(ValueError, "one shared hostworkdir"):
                    _RUNNER._load_source_stack_contract()

    def test_ci_base_runtime_service_target_ip_uses_loopback_for_same_host_local_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            bundle_root = Path(td)
            start_cfg = bundle_root / "start_test_bed.runner.yaml"
            deployconf_path = bundle_root / "deployconf.yaml"
            start_cfg.write_text(
                "\n".join(
                    [
                        "schema_version: 6",
                        "deployconf_path: ./deployconf.yaml",
                        "controller_url: http://127.0.0.1:19080/r/ops/fluxon_testbed",
                        "controller_basic_auth:",
                        "  username: ops_admin",
                        "  password: ops_password",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            deployconf_path.write_text(
                "\n".join(
                    [
                        "global_envs:",
                        "  FLUXON_CLUSTER_NAME: fluxon_testbed",
                        "  FLUXON_SHARED_MEM: ${HOSTWORKDIR}/shm1",
                        "  FLUXON_SHARED_MEM2: ${HOSTWORKDIR}/shm2_files",
                        "cluster_nodes:",
                        "  - hostname: logic-a",
                        "    ip: 127.0.0.1",
                        "    hostworkdir: /tmp/fluxon_testbed/a",
                        "    execution_mode: local",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "  - hostname: logic-b",
                        "    ip: 127.0.0.1",
                        "    hostworkdir: /tmp/fluxon_testbed/b",
                        "    execution_mode: local",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "service:",
                        "  ops_controller:",
                        "    node_bind:",
                        "      node: [logic-a]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            resolved_case = {
                "deploy": {
                    "target_ip_map": {"logic-a": "192.168.1.10", "logic-b": "192.168.1.10"},
                },
                "profile": {
                    "ci": {
                        "runtime": {
                            "base_runtime": {
                                "greptime": {
                                    "target": "logic-a",
                                    "endpoint": {"scheme": "http", "host_port": 19295},
                                }
                            }
                        }
                    }
                },
            }

            env = {**os.environ, _RUNNER.TEST_STACK_START_TEST_BED_CONFIG_ENV: str(start_cfg)}
            with mock.patch.dict(os.environ, env, clear=True):
                self.assertEqual(
                    _RUNNER._ci_base_runtime_service_target_ip(resolved_case, service_id="greptime"),
                    "127.0.0.1",
                )

    def test_write_deployer_manifests_renders_payload_wrapper_from_template(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            resolved_case = {
                "case": {
                    "case_id": "bench_case",
                    "profile_id": "bench_profile",
                },
                "scene": {
                    "bench": {
                        "subject": "kv",
                    }
                },
                "deploy": {
                    "instances": [
                        {
                            "id": "worker_0",
                            "k8s_ref": "deployment/test-worker",
                            "lifecycle": "service",
                            "deployer": {
                                "target": "logic-a",
                                "payload_file": "wheelhouse/pkg.whl",
                                "payload_dest_path": "/tmp/run/pkg.whl",
                                "command": ["/bin/sh", "-lc", "python3 /tmp/run/pkg.whl"],
                            },
                        }
                    ],
                    "payload_delivery": {
                        "kind": _RUNNER.PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3,
                        "s3_base_url": "http://127.0.0.1:19080/fs_s3",
                        "bucket": "bench-bucket",
                        "access_key": "bench-ak",
                        "secret_key": "bench-sk",
                        "region": "bench-region",
                        "key_prefix": "case-prefix",
                    },
                },
                "runtime": {
                    "workdir_root": str(run_dir.parent),
                    "run_dir": str(run_dir),
                    "stack_identity": {
                        "cluster_name": "fluxon_testbed",
                        "controller_url": "http://127.0.0.1:19080/r/ops/fluxon_testbed",
                        "share_mem_path": "/tmp/shm",
                    },
                },
                "artifact_set": {
                    "release_root": str(run_dir / "fluxon_release"),
                    "test_rsc_root": str(run_dir / "test_rsc"),
                },
            }

            template_path = (
                _RUNNER.RUNNER_TEMPLATE_DIR / "payload_fluxon_fs_s3_download_and_exec.sh.template"
            ).resolve()
            self.assertTrue(template_path.is_file())

            _RUNNER._write_deployer_manifests(resolved_case, run_dir, allow_overwrite=False)

            manifest_docs = list(
                yaml.safe_load_all((run_dir / "deployer_deploy.yaml").read_text(encoding="utf-8"))
            )
            self.assertEqual(len(manifest_docs), 1)
            container = manifest_docs[0]["spec"]["template"]["spec"]["containers"][0]
            self.assertEqual(container["command"], ["/bin/bash", "-lc"])
            self.assertEqual(len(container["args"]), 1)
            script_text = container["args"][0]
            self.assertIn("python3 - <<'PY'", script_text)
            self.assertIn('BASE_URL = "http://127.0.0.1:19080/fs_s3"', script_text)
            self.assertIn('OBJECT_KEY = "case-prefix/wheelhouse/pkg.whl"', script_text)
            self.assertIn('DEST_PATH = "/tmp/run/pkg.whl"', script_text)
            self.assertIn('exec /bin/sh -lc', script_text)
            self.assertNotIn("__FLUXON_TMPL_", script_text)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
