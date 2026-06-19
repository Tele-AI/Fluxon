#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import os
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


class TestTestRunnerTestbedContract(unittest.TestCase):
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

    def test_ci_source_overlay_includes_fluxon_test_stack(self) -> None:
        self.assertIn("fluxon_test_stack", _RUNNER._CI_SOURCE_OVERLAY_ROOTS)

    def test_top_attention_ci_execution_plan_is_runner_native(self) -> None:
        suite_cfg = yaml.safe_load((_RUNNER.RUNNER_REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").read_text(encoding="utf-8"))
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
        case = next(item for item in cases if item.scene_id == "ci_top_attention_bin_kvtest" and item.profile_id == "fluxon_tcp")
        planned = _RUNNER._build_ci_execution_plan(case, suite)
        self.assertEqual(len(planned), 1)
        self.assertEqual(planned[0].ci_commands[0]["id"], "top_attention_bin_kvtest")
        self.assertIn("--case-config __RUN_DIR__/configs/ci_scene_config.yaml", planned[0].ci_commands[0]["command"])

    def test_ci_prepare_run_inputs_rebuilds_release_view_without_reusing_source_test_rsc(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            source_root = root / "source_root"
            source_root.mkdir()
            (source_root / "README.md").write_text("repo\n", encoding="utf-8")

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

            resolved_case = {
                "artifact_set": {
                    "release_artifacts": {"wheel": wheel_name},
                    "test_rsc_artifacts": {
                        "ci_src_archive": "src_ci.tar.gz",
                        "ci_ext_rsc_archive": "fluxon_ci_ext_rsc.tar.gz",
                    },
                }
            }

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
            run_subprocess_mock.assert_called_once()

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
                                "config": "setup_and_pack/setup_dev_env/doc_page_ci.yaml",
                                "cache_relpath": ".cached/fluxon_doc_site/toolchain",
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
                        "shared_memory_path": "/tmp/shm",
                        "shared_file_path": "/tmp/share",
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
                    share_file_path="/tmp/share",
                )
            script_text = script_path.read_text(encoding="utf-8")
            self.assertIn('prepare_env_path="', script_text)
            self.assertIn('. "$prepare_env_path"', script_text)

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
            self.assertEqual(contract["shared_memory_hostworkdir"], "${HOSTWORKDIR}/shm1")
            self.assertEqual(contract["shared_file_hostworkdir"], "${HOSTWORKDIR}/shm2_files")

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


if __name__ == "__main__":
    raise SystemExit(unittest.main())
