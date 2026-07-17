#!/usr/bin/env python3

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_2_virt_node.py"
WORKFLOW_HELPER_PATH = REPO_ROOT / "scripts" / "ci_2_virt_node_workflow.py"
CODEX_HELPER_PATH = REPO_ROOT / "scripts" / "ci_codex_failure_analysis.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_ci_2_virt_node_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_ENTRY = _load_module()


def _load_workflow_helper():
    spec = importlib.util.spec_from_file_location(
        "fluxon_ci_2_virt_node_workflow_contract",
        WORKFLOW_HELPER_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_WORKFLOW_HELPER = _load_workflow_helper()


def _load_codex_helper():
    spec = importlib.util.spec_from_file_location(
        "fluxon_ci_codex_failure_analysis_contract",
        CODEX_HELPER_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_CODEX_HELPER = _load_codex_helper()


class TestCi2VirtNodeContract(unittest.TestCase):
    _KVTEST_SCENE_ID = "ci_top_attention_bin_kvtest"
    _CARGO_KV_UNIT_SCENE_ID = "ci_top_attention_cargo_kv_unit"
    _CARGO_CLI_SCENE_ID = "ci_top_attention_cargo_cli"
    _CARGO_COMMU_SCENE_ID = "ci_top_attention_cargo_commu"
    _CARGO_COMMU_CONTRACT_SCENE_ID = "ci_top_attention_cargo_commu_contract"
    _CARGO_FRAMEWORK_SCENE_ID = "ci_top_attention_cargo_framework"
    _CARGO_FS_SCENE_ID = "ci_top_attention_cargo_fs"
    _CARGO_FS_S3_GATEWAY_SCENE_ID = "ci_top_attention_cargo_fs_s3_gateway"
    _CARGO_LIMIT_THIRDPARTY_SCENE_ID = "ci_top_attention_cargo_limit_thirdparty"
    _CARGO_MQ_SCENE_ID = "ci_top_attention_cargo_mq"
    _CARGO_OBSERVABILITY_SCENE_ID = "ci_top_attention_cargo_observability"
    _CARGO_OPS_SCENE_ID = "ci_top_attention_cargo_ops"
    _CARGO_PYO3_SCENE_ID = "ci_top_attention_cargo_pyo3"
    _DOC_SCENE_ID = "ci_top_attention_doc_page_build"
    _LOG_MGMT_SCENE_ID = "ci_top_attention_log_mgmt"
    _MQ_SCENE_ID = "ci_top_attention_mq_core"

    def test_all_test_workflow_runs_test_all_and_largescale_after_one_package_job(self) -> None:
        workflow = (REPO_ROOT / ".github" / "workflows" / "all_test.yml").read_text(
            encoding="utf-8"
        )
        helper = (REPO_ROOT / "scripts" / "ci_2_virt_node_workflow.py").read_text(encoding="utf-8")
        codex_helper = (REPO_ROOT / "scripts" / "ci_codex_failure_analysis.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("scripts/ci_2_virt_node_workflow.py write-suite", workflow)
        self.assertIn("package-wheel:", workflow)
        self.assertIn("ci-large-scale-mq:", workflow)
        self.assertEqual(workflow.count("needs: package-wheel"), 2)
        self.assertNotIn("--suite-kind", workflow)
        self.assertIn('--current-job-name "$CURRENT_JOB_NAME"', workflow)
        self.assertIn("job_name=args.current_job_name", codex_helper)
        self.assertNotIn('job_name="ci-2-virt-node"', codex_helper)
        self.assertIn('repo_root / ".dever" / "ci_large_scale_mq"', codex_helper)
        for diagnostic_name in (
            "failure.json",
            "processes.json",
            "resource_samples.jsonl",
            "run_plan.json",
            "summary.json",
        ):
            self.assertIn(f'"{diagnostic_name}"', codex_helper)
        self.assertEqual(workflow.count("--skip-pack"), 1)
        self.assertEqual(workflow.count("actions/download-artifact@37930b1c2abaa49bbe596cd826c3c89aef350131"), 2)
        self.assertIn("fluxon-ci-release-${{ github.sha }}", workflow)
        self.assertEqual(
            workflow.count(
                "test -f fluxon_release/test_rsc/fluxon_tcp_thread/fluxon_test_rsc.sha256"
            ),
            2,
        )
        self.assertEqual(
            workflow.count("test -f fluxon_release/ext_images/ext_images.sha256"),
            2,
        )
        self.assertIn(
            "jlumbroso/free-disk-space@54081f138730dfa15788a46383842cd2f914a1be",
            workflow,
        )
        self.assertIn("scripts/ci_2_virt_node_workflow.py scan-and-clean-temp", workflow)
        self.assertNotIn("/usr/local/lib/android", workflow)
        self.assertNotIn("ci_top_attention_largescale_mq", helper)
        large_job = workflow.split("  ci-large-scale-mq:", 1)[1].split(
            "  codex_failure_analysis:",
            1,
        )[0]
        self.assertIn(
            "fluxon_test_stack/top_attention_test_index/_largescale_mq.py",
            large_job,
        )
        self.assertIn("--owner-count 2", large_job)
        self.assertIn("--producer-count 80", large_job)
        self.assertIn("--consumer-count 8", large_job)
        self.assertIn("--metric-warmup-seconds 60", large_job)
        self.assertIn("Install packaged Fluxon wheel", large_job)
        self.assertNotIn("--no-deps", large_job)
        self.assertNotIn("--no-index", large_job)
        self.assertIn('subprocess.run([sys.executable, "-m", "pip", "check"]', large_job)
        self.assertIn('"-I",', large_job)
        self.assertIn("import etcd3", large_job)
        self.assertIn(
            "from fluxon_py._api_ext_chan.mq_config_check import MIN_TTL",
            large_job,
        )
        self.assertNotIn("ci_2_virt_node.py", large_job)
        self.assertNotIn("start_test_bed", large_job)
        self.assertNotIn("test_runner.py", large_job)
        self.assertNotIn("--testbed-hostworkdir", large_job)
        self.assertNotIn("rather_no_git_submodule.py", large_job)
        self.assertNotIn("path: .dever/ci_large_scale_mq/**", large_job)
        self.assertIn(".dever/ci_large_scale_mq/logs/**/*.log", large_job)
        self.assertIn(".dever/ci_large_scale_mq/services/**/shared.json", large_job)
        self.assertNotIn("command_variants", helper)
        self.assertNotIn("p8_c8", helper)
        self.assertNotIn("p32_c32", helper)

    def test_test_all_scene_set_excludes_bare_local_largescale(self) -> None:
        all_scenes = _WORKFLOW_HELPER._top_attention_ci_scenes(
            "Tele-AI.github.io/Fluxon"
        )

        self.assertNotIn("ci_top_attention_largescale_mq", all_scenes)
        self.assertTrue(all_scenes)

    def test_scan_and_clean_temp_is_scoped_and_preserves_runner_control_paths(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runner_temp = root / "runner_temp"
            system_temp = root / "system_temp"
            runner_temp.mkdir()
            system_temp.mkdir()
            stale_runner = runner_temp / "stale-build"
            protected_runner = runner_temp / "_runner_file_commands"
            recent_runner = runner_temp / "recent-build"
            stale_system = system_temp / "fluxon_stale"
            unrelated_system = system_temp / "systemd-private-service"
            for path in (
                stale_runner,
                protected_runner,
                recent_runner,
                stale_system,
                unrelated_system,
            ):
                path.mkdir()
                (path / "payload").write_text("x", encoding="utf-8")
            now = _WORKFLOW_HELPER.time.time()
            old_timestamp = now - max(
                _WORKFLOW_HELPER.RUNNER_TEMP_MIN_AGE_SECONDS,
                _WORKFLOW_HELPER.SYSTEM_TEMP_MIN_AGE_SECONDS,
            ) - 60
            for path in (stale_runner, protected_runner, stale_system, unrelated_system):
                os.utime(path, (old_timestamp, old_timestamp))

            with mock.patch.dict(
                _WORKFLOW_HELPER.os.environ,
                {"GITHUB_ACTIONS": "true", "RUNNER_TEMP": str(runner_temp)},
            ):
                with mock.patch.object(
                    _WORKFLOW_HELPER,
                    "SYSTEM_TEMP_ROOT",
                    system_temp,
                ):
                    with mock.patch.object(_WORKFLOW_HELPER.subprocess, "run") as run_mock:
                        _WORKFLOW_HELPER._scan_and_clean_temp(mock.Mock())

            self.assertFalse(stale_runner.exists())
            self.assertFalse(stale_system.exists())
            self.assertTrue(protected_runner.exists())
            self.assertTrue(recent_runner.exists())
            self.assertTrue(unrelated_system.exists())
            run_mock.assert_called_once_with(
                ["systemd-tmpfiles", "--clean"],
                check=True,
            )

    def test_codex_report_prints_in_analysis_job_and_redacts_api_secrets(self) -> None:
        workflow = (REPO_ROOT / ".github" / "workflows" / "all_test.yml").read_text(encoding="utf-8")
        self.assertIn("scripts/ci_codex_failure_analysis.py print-report", workflow)
        self.assertNotIn("publish_codex_failure_report", workflow)
        self.assertNotIn("codex-ci-failure-report-", workflow)
        self.assertNotIn("actions/upload-artifact@v4", workflow)
        self.assertGreaterEqual(
            workflow.count("actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"),
            6,
        )

        with tempfile.TemporaryDirectory() as td:
            summary = Path(td) / "summary.md"
            api_key = "secret-api-key"
            base_url = "https://openai.example.invalid/v1"
            report = f"diagnosis key={api_key} base={base_url} endpoint={base_url}/responses"
            output = io.StringIO()
            with mock.patch.dict(
                _CODEX_HELPER.os.environ,
                {
                    "CODEX_REPORT": report,
                    "FAILED_RUN_URL": "https://github.com/Tele-AI/Fluxon/actions/runs/1",
                    "OPENAI_API_KEY": api_key,
                    "OPENAI_BASE_URL": base_url,
                },
            ):
                with contextlib.redirect_stdout(output):
                    _CODEX_HELPER._print_report(mock.Mock(summary=summary))

            rendered = output.getvalue()
            self.assertIn("# Codex CI failure analysis", rendered)
            self.assertIn("diagnosis key=[REDACTED]", rendered)
            self.assertNotIn(api_key, rendered)
            self.assertNotIn(base_url, rendered)
            self.assertEqual(summary.read_text(encoding="utf-8"), rendered)

    def test_codex_context_collects_testbed_service_logs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            repo_root = root / "repository"
            runner_temp = root / "runner-temp"
            repo_root.mkdir()
            runner_temp.mkdir()

            service_log_root = runner_temp / "fluxon_deploy" / "a" / "log"
            service_log_root.mkdir(parents=True)
            etcd_log = service_log_root / "etcd.2026-07-14.log"
            greptime_log = service_log_root / "greptime.log"
            ops_controller_log = service_log_root / "ops_controller.2026-07-14.log"
            etcd_log.write_text("etcd server line 1\netcd server line 2\n", encoding="utf-8")
            greptime_log.write_text("greptime server line\n", encoding="utf-8")
            ops_controller_log.write_text(
                "ops controller server line\n",
                encoding="utf-8",
            )
            (service_log_root / "owner.2026-07-14.log").write_text(
                "owner service line\n",
                encoding="utf-8",
            )
            middleware_data_log = (
                runner_temp / "fluxon_deploy" / "a" / "data" / "etcd.log"
            )
            middleware_data_log.parent.mkdir()
            middleware_data_log.write_text("data directory file\n", encoding="utf-8")

            context_root = runner_temp / "codex-failure-context"
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                _CODEX_HELPER._collect_context(
                    mock.Mock(
                        repo_root=repo_root,
                        runner_temp=runner_temp,
                        context_root=context_root,
                    )
                )

            copied_log_root = (
                context_root
                / "repository"
                / "runner-temp"
                / "fluxon_deploy"
                / "a"
                / "log"
            )
            self.assertEqual(
                (copied_log_root / etcd_log.name).read_text(encoding="utf-8"),
                etcd_log.read_text(encoding="utf-8"),
            )
            self.assertEqual(
                (copied_log_root / greptime_log.name).read_text(encoding="utf-8"),
                greptime_log.read_text(encoding="utf-8"),
            )
            self.assertEqual(
                (copied_log_root / ops_controller_log.name).read_text(encoding="utf-8"),
                ops_controller_log.read_text(encoding="utf-8"),
            )
            self.assertFalse((copied_log_root / "owner.2026-07-14.log").exists())
            self.assertFalse(
                (
                    context_root
                    / "repository"
                    / "runner-temp"
                    / "fluxon_deploy"
                    / "a"
                    / "data"
                    / "etcd.log"
                ).exists()
            )

            manifest = json.loads(
                (context_root / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["copied_file_count"], 3)
            self.assertEqual(
                manifest["testbed_service_logs"]["services"],
                {
                    "etcd": {"copied_file_count": 1},
                    "greptime": {"copied_file_count": 1},
                    "ops_controller": {"copied_file_count": 1},
                },
            )
            self.assertIn(
                "etcd=1 greptime=1 ops_controller=1",
                output.getvalue(),
            )

    def test_codex_context_redacts_yaml_credentials_and_omits_invalid_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            repo_root = root / "repository"
            runner_temp = root / "runner-temp"
            config_root = repo_root / ".dever" / "ci_2_virt_node" / "runner_run" / "configs"
            config_root.mkdir(parents=True)
            runner_temp.mkdir()

            source = runner_temp / "ci_test_list.ci.yaml"
            source.write_text(
                "service_name: benchmark\n"
                "database:\n"
                "  username: operator\n"
                "  password: db-password-value\n"
                "object_store:\n"
                "  access_key: access-key-value\n"
                "  secret_key: secret-key-value\n"
                "callback_url: https://user:url-password@example.invalid/path\n"
                "callback_query_url: https://example.invalid/path?token=query-token-value\n",
                encoding="utf-8",
            )
            invalid_source = config_root / "invalid.yaml"
            invalid_source.write_text(
                "password: malformed-password-value\nbroken: [\n",
                encoding="utf-8",
            )

            context_root = runner_temp / "codex-failure-context"
            _CODEX_HELPER._collect_context(
                mock.Mock(
                    repo_root=repo_root,
                    runner_temp=runner_temp,
                    context_root=context_root,
                )
            )

            copied_path = (
                context_root
                / "repository"
                / "runner-temp"
                / "ci_test_list.ci.yaml"
            )
            copied = yaml.safe_load(copied_path.read_text(encoding="utf-8"))
            self.assertEqual(copied["service_name"], "benchmark")
            self.assertEqual(copied["database"]["username"], "operator")
            self.assertEqual(copied["database"]["password"], "[REDACTED]")
            self.assertEqual(copied["object_store"]["access_key"], "[REDACTED]")
            self.assertEqual(copied["object_store"]["secret_key"], "[REDACTED]")
            self.assertEqual(
                copied["callback_url"],
                "https://[REDACTED]@example.invalid/path",
            )
            self.assertEqual(
                copied["callback_query_url"],
                "https://example.invalid/path?token=[REDACTED]",
            )
            self.assertFalse(
                (
                    context_root
                    / "repository"
                    / ".dever"
                    / "ci_2_virt_node"
                    / "runner_run"
                    / "configs"
                    / "invalid.yaml"
                ).exists()
            )

            manifest = json.loads(
                (context_root / "manifest.json").read_text(encoding="utf-8")
            )
            copied_entry = next(
                item
                for item in manifest["copied"]
                if item["artifact_path"].endswith("/ci_test_list.ci.yaml")
            )
            self.assertEqual(
                copied_entry["sanitization"],
                "credential_fields_redacted",
            )
            self.assertEqual(copied_entry["redacted_value_count"], 5)
            self.assertTrue(
                any(
                    item["source"] == str(invalid_source)
                    and item["reason"].startswith("YAML sanitization failed; file omitted")
                    for item in manifest["skipped"]
                )
            )
            context_text = "\n".join(
                path.read_text(encoding="utf-8")
                for path in context_root.rglob("*")
                if path.is_file()
            )
            for secret in (
                "db-password-value",
                "access-key-value",
                "secret-key-value",
                "url-password",
                "query-token-value",
                "malformed-password-value",
            ):
                self.assertNotIn(secret, context_text)

    def test_generated_suite_is_public_dual_local_nodes_ci_only(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=[self._DOC_SCENE_ID, self._KVTEST_SCENE_ID, self._LOG_MGMT_SCENE_ID, self._MQ_SCENE_ID],
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(generated["run"]["selectors"]["profile_ids"], ["fluxon_tcp_thread"])
        self.assertEqual(
            set(generated["scenes"].keys()),
            {self._DOC_SCENE_ID, self._KVTEST_SCENE_ID, self._LOG_MGMT_SCENE_ID, self._MQ_SCENE_ID},
        )
        self.assertEqual(generated["profiles"]["fluxon_tcp_thread"]["artifact_set"], "fluxon_tcp_thread")
        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._KVTEST_SCENE_ID][
                "kv_transport_feature"
            ],
            "tcp_thread_transport",
        )
        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._LOG_MGMT_SCENE_ID][
                "enabled"
            ],
            True,
        )
        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._MQ_SCENE_ID],
            {},
        )
        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["deploy"]["target_ip_map"],
            {"local-node-a": "192.0.2.119", "local-node-b": "192.0.2.119"},
        )
        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["runtime_contracts"]["cluster_kv_owner"][
                "base_runtime"
            ]["etcd"]["endpoint"]["host_port"],
            19180,
        )
        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["runtime_contracts"]["cluster_kv_owner"][
                "base_runtime"
            ]["greptime"]["endpoint"]["host_port"],
            19190,
        )
        test_stack_ports = generated["profiles"]["fluxon_tcp_thread"]["runtime"]["test_stack"]["port_alloc"][
            "by_topology"
        ]
        self.assertEqual(test_stack_ports[1]["coordinator_port_base"], 20180)
        self.assertEqual(test_stack_ports[2]["coordinator_port_base"], 20280)
        self.assertEqual(
            generated["artifact_sets"]["fluxon_tcp_thread"]["release_source"]["key_prefix"],
            "profiles/fluxon_tcp_thread",
        )
        self.assertEqual(
            generated["artifact_sets"]["fluxon_tcp_thread"]["release_artifacts"],
            {"wheel": "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"},
        )
        self.assertEqual(set(generated["artifact_sets"].keys()), {"fluxon_tcp_thread"})
        self.assertEqual(
            generated["artifact_sets"]["fluxon_tcp_thread"]["test_rsc_source"]["key_prefix"],
            "test_rsc/fluxon_tcp_thread",
        )
        self.assertEqual(
            generated["artifact_sets"]["fluxon_tcp_thread"]["test_rsc_artifacts"],
            {
                "ci_src_archive": "src_ci.tar.gz",
                "ci_ext_rsc_archive": "fluxon_ci_ext_rsc.tar.gz",
            },
        )
        self.assertEqual(
            generated["scales"]["n1_kvowner_dram_3gib"]["targets"]["hosts"],
            ["local-node-a"],
        )
        self.assertEqual(
            generated["scales"]["n1_kvowner_dram_3gib"]["targets"]["primary"],
            "local-node-a",
        )
        self.assertNotIn("secondary", generated["scales"]["n1_kvowner_dram_3gib"]["targets"])
        self.assertEqual(
            generated["scales"]["n1_kvowner_dram_20gib"]["targets"]["hosts"],
            ["local-node-a"],
        )
        self.assertEqual(
            generated["scales"]["n1_kvowner_dram_20gib"]["targets"]["primary"],
            "local-node-a",
        )
        self.assertNotIn("secondary", generated["scales"]["n1_kvowner_dram_20gib"]["targets"])
        self.assertEqual(
            generated["scenes"][self._DOC_SCENE_ID]["select"]["scales"],
            ["n1_kvowner_dram_3gib"],
        )
        self.assertEqual(
            generated["scenes"][self._KVTEST_SCENE_ID]["select"]["scales"],
            ["n1_kvowner_dram_20gib"],
        )
        self.assertEqual(
            generated["scenes"][self._LOG_MGMT_SCENE_ID]["select"]["scales"],
            ["n1_kvowner_dram_20gib"],
        )
        self.assertEqual(
            generated["scenes"][self._MQ_SCENE_ID]["select"]["scales"],
            ["n1_kvowner_dram_20gib"],
        )
        self.assertEqual(
            set(generated["scales"].keys()),
            {"n1_kvowner_dram_3gib", "n1_kvowner_dram_20gib"},
        )
        self.assertNotIn("commands", generated["scenes"][self._KVTEST_SCENE_ID]["ci"])
        self.assertNotIn("commands", generated["scenes"][self._LOG_MGMT_SCENE_ID]["ci"])
        self.assertNotIn("commands", generated["scenes"][self._MQ_SCENE_ID]["ci"])

    def test_generated_suite_supports_mq_core_ci_scene(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=[self._MQ_SCENE_ID],
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(set(generated["scenes"].keys()), {self._MQ_SCENE_ID})
        self.assertEqual(
            generated["scenes"][self._MQ_SCENE_ID]["ci"]["runtime_contract"],
            "cluster_kv_owner",
        )
        self.assertEqual(
            generated["scenes"][self._MQ_SCENE_ID]["ci"]["subject"],
            "mq",
        )
        self.assertNotIn("commands", generated["scenes"][self._MQ_SCENE_ID]["ci"])
        self.assertEqual(
            generated["scenes"][self._MQ_SCENE_ID]["select"]["scales"],
            ["n1_kvowner_dram_20gib"],
        )
        self.assertEqual(set(generated["scales"].keys()), {"n1_kvowner_dram_20gib"})

    def test_generated_suite_preserves_source_scene_configs(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        suite_cfg["profiles"]["fluxon_tcp"]["runtime"]["ci"]["scene_configs"][self._KVTEST_SCENE_ID]["kv_test_rounds"] = "p2p_only"

        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=[self._KVTEST_SCENE_ID],
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._KVTEST_SCENE_ID][
                "kv_test_rounds"
            ],
            "p2p_only",
        )

    def test_generated_suite_injects_public_transport_feature_for_cargo_kv_unit(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=[self._CARGO_KV_UNIT_SCENE_ID],
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._CARGO_KV_UNIT_SCENE_ID][
                "kv_transport_feature"
            ],
            "tcp_thread_transport",
        )

    def test_generated_suite_supports_additional_cargo_ci_scenes(self) -> None:
        scene_ids = [
            self._CARGO_CLI_SCENE_ID,
            self._CARGO_COMMU_SCENE_ID,
            self._CARGO_COMMU_CONTRACT_SCENE_ID,
            self._CARGO_FRAMEWORK_SCENE_ID,
            self._CARGO_FS_SCENE_ID,
            self._CARGO_FS_S3_GATEWAY_SCENE_ID,
            self._CARGO_LIMIT_THIRDPARTY_SCENE_ID,
            self._CARGO_MQ_SCENE_ID,
            self._CARGO_OBSERVABILITY_SCENE_ID,
            self._CARGO_OPS_SCENE_ID,
            self._CARGO_PYO3_SCENE_ID,
        ]
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=scene_ids,
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(set(generated["scenes"].keys()), set(scene_ids))
        for scene_id in scene_ids:
            self.assertEqual(
                generated["scenes"][scene_id]["ci"]["runtime_contract"],
                "rust_self_managed",
            )
            self.assertEqual(
                generated["scenes"][scene_id]["ci"]["subject"],
                "rust",
            )
            self.assertNotIn("commands", generated["scenes"][scene_id]["ci"])

    def test_generated_suite_supports_doc_page_ci_scene(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=[self._DOC_SCENE_ID],
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(set(generated["scenes"].keys()), {self._DOC_SCENE_ID})
        self.assertEqual(
            generated["scenes"][self._DOC_SCENE_ID]["ci"]["runtime_contract"],
            "rust_self_managed",
        )
        prepare = generated["scenes"][self._DOC_SCENE_ID]["ci"]["prepare"]
        self.assertEqual(
            prepare,
            [
                {
                    "kind": "online_docker_image",
                    "image_ref": "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
                    "env": "FLUXON_DOC_SITE_DOCKER_IMAGE_REF",
                }
            ],
        )
        self.assertNotIn("commands", generated["scenes"][self._DOC_SCENE_ID]["ci"])
        self.assertEqual(
            generated["scenes"][self._DOC_SCENE_ID]["select"]["scales"],
            ["n1_kvowner_dram_3gib"],
        )
        self.assertEqual(set(generated["scales"].keys()), {"n1_kvowner_dram_3gib"})

    def test_generated_deployconf_rewrites_to_dual_local_nodes(self) -> None:
        deployconf_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_DEPLOYCONF_TEMPLATE, ctx="deployconf")
        generated = _ENTRY._rewrite_deployconf_for_local_dual_nodes(
            deployconf_cfg=deployconf_cfg,
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="192.0.2.119",
            primary_hostworkdir=Path("/tmp/fluxon_testbed/a"),
            secondary_hostworkdir=Path("/tmp/fluxon_testbed/b"),
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19180,
        )

        self.assertEqual(len(generated["cluster_nodes"]), 2)
        self.assertEqual(
            [node["hostname"] for node in generated["cluster_nodes"]],
            ["local-node-a", "local-node-b"],
        )
        self.assertEqual(
            [node["hostworkdir"] for node in generated["cluster_nodes"]],
            ["/tmp/fluxon_testbed/a", "/tmp/fluxon_testbed/b"],
        )
        self.assertEqual(
            [node["execution_mode"] for node in generated["cluster_nodes"]],
            ["local", "local"],
        )
        self.assertEqual(
            [node["ip"] for node in generated["cluster_nodes"]],
            ["192.0.2.119", "192.0.2.119"],
        )
        self.assertEqual(generated["global_envs"]["FLUXON_CLUSTER_NODE_IDS"], "local-node-a local-node-b")
        self.assertEqual(
            generated["global_envs"]["FLUXON_RELEASE_WHEEL"],
            "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
        )
        self.assertEqual(
            generated["global_envs"]["FLUXON_RELEASE_WHEEL_PY"],
            "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
        )
        self.assertEqual(generated["global_envs"]["MASTER__PORT"], "19180")
        self.assertEqual(
            generated["global_envs"]["FLUXON_OPS_UI_BASE_URL"],
            "http://${OPS_CONTROLLER__NODE_ID__IP}:19180",
        )
        self.assertIn('--wheel "$FLUXON_RELEASE_WHEEL"', generated["global_envs"]["FLUXON_RELEASE_WHEEL_FETCH_CMD"])
        self.assertEqual(generated["atomic_groups"]["fluxon_core_controller"]["nodes"], ["local-node-a", "local-node-b"])
        self.assertEqual(generated["service"]["owner"]["node_bind"]["node"], ["local-node-a", "local-node-b"])
        self.assertIn(
            'large_file_paths:',
            generated["service"]["owner"]["entrypoint"],
        )
        self.assertIn(
            '- "${HOSTWORKDIR}/large/owner_${NODE_ID}"',
            generated["service"]["owner"]["entrypoint"],
        )
        self.assertEqual(generated["service"]["ops_controller"]["port"], 19180)
        self.assertEqual(generated["namespace"], "fluxon_testbed")
        self.assertEqual(generated["global_envs"]["FLUXON_CLUSTER_NAME"], "fluxon_testbed")
        self.assertIn(
            'http_listen_addr: "0.0.0.0:${OPS_CONTROLLER__PORT}"',
            generated["service"]["ops_controller"]["entrypoint"],
        )
        self.assertNotIn(
            'http_listen_addr: "0.0.0.0:${MASTER__PORT}"',
            generated["service"]["ops_controller"]["entrypoint"],
        )
        self.assertIn("local-node-a", generated["service"]["ops_agent"]["entrypoint"])
        self.assertIn("local-node-b", generated["service"]["ops_agent"]["entrypoint"])
        self.assertIn('    - "192.0.2.119/32"', generated["service"]["master"]["entrypoint"])
        tikv_entrypoint = generated["service"]["tikv"]["entrypoint"]
        self.assertIn('reserve-space = "0KiB"', tikv_entrypoint)
        self.assertIn('reserve-raft-space = "0KiB"', tikv_entrypoint)

    def test_generated_start_test_bed_config_points_to_local_authorities(self) -> None:
        start_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed")
        generated = _ENTRY._rewrite_start_test_bed_for_local_dual_nodes(
            start_cfg=start_cfg,
            generated_deployconf_path=Path("/tmp/deployconf.yaml"),
            primary_node_name="local-node-a",
            controller_access_ip="192.0.2.119",
            controller_port=19080,
            ui_port=18080,
            ui_workdir=Path("/tmp/ui"),
        )

        self.assertEqual(generated["deployconf_path"], "/tmp/deployconf.yaml")
        self.assertEqual(generated["controller_url"], "http://192.0.2.119:19080/r/ops/fluxon_testbed")
        self.assertEqual(generated["controller_basic_auth"]["username"], "ops_admin")
        self.assertEqual(generated["controller_basic_auth"]["password"], "ops_password")
        self.assertEqual(generated["test_runner_ui"]["workdir"], "/tmp/ui")
        self.assertIsNone(generated["test_runner_ui"]["gitops_config_path"])
        self.assertEqual(generated["bootstrap_phases"][0]["node"], "local-node-a")

    def test_generated_local_testbed_supports_explicit_ops_cluster_name(self) -> None:
        deployconf_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_DEPLOYCONF_TEMPLATE, ctx="deployconf")
        deployconf = _ENTRY._rewrite_deployconf_for_local_dual_nodes(
            deployconf_cfg=deployconf_cfg,
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="10.1.1.119",
            primary_hostworkdir=Path("/tmp/fluxon_testbed/a"),
            secondary_hostworkdir=Path("/tmp/fluxon_testbed/b"),
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19180,
            testbed_ops_cluster_name="fluxon_testbed_mq_large_local",
        )
        start_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed")
        start = _ENTRY._rewrite_start_test_bed_for_local_dual_nodes(
            start_cfg=start_cfg,
            generated_deployconf_path=Path("/tmp/deployconf.yaml"),
            primary_node_name="local-node-a",
            controller_access_ip="10.1.1.119",
            controller_port=19180,
            ui_port=18080,
            ui_workdir=Path("/tmp/ui"),
            testbed_ops_cluster_name="fluxon_testbed_mq_large_local",
        )

        self.assertEqual(deployconf["namespace"], "fluxon_testbed_mq_large_local")
        self.assertEqual(deployconf["global_envs"]["FLUXON_CLUSTER_NAME"], "fluxon_testbed_mq_large_local")
        self.assertEqual(
            start["controller_url"],
            "http://10.1.1.119:19180/r/ops/fluxon_testbed_mq_large_local",
        )

    def test_generated_apply_check_config_excludes_control_plane_reapply(self) -> None:
        start_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed")
        local_cfg = _ENTRY._rewrite_start_test_bed_for_local_dual_nodes(
            start_cfg=start_cfg,
            generated_deployconf_path=Path("/tmp/deployconf.yaml"),
            primary_node_name="local-node-a",
            controller_access_ip="192.0.2.119",
            controller_port=19080,
            ui_port=18080,
            ui_workdir=Path("/tmp/ui"),
        )

        generated = _ENTRY._rewrite_start_test_bed_for_apply_check(
            start_cfg=local_cfg,
        )

        self.assertEqual(
            generated["deploy_workloads"],
            ["fluxon_fs_master", "fluxon_fs_agent"],
        )

    def test_write_ci_testbed_bundle_is_run_local_and_relocatable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle_root = root / "runner_run" / "testbed_bundle"
            artifacts_source = root / "release" / "test_rsc"
            artifacts_source.mkdir(parents=True)
            (artifacts_source / "prepare.yaml").write_text("schema_version: 1\n", encoding="utf-8")
            deployconf = {
                "gen_k8s_daemonset_mirror_outdir": "/tmp/old-mirror",
                "cluster_nodes": [
                    {
                        "hostname": "runner-a",
                        "ip": "10.1.1.119",
                        "hostworkdir": "/tmp/runner/a",
                        "execution_mode": "local",
                    }
                ],
                "global_envs": {"FLUXON_CLUSTER_NAME": "fluxon_testbed"},
            }
            start_cfg = {"schema_version": 6, "deployconf_path": "/tmp/old-deployconf.yaml"}
            apply_cfg = {"schema_version": 6, "deployconf_path": "/tmp/old-deployconf.yaml"}

            paths = _ENTRY._write_ci_testbed_bundle(
                bundle_root=bundle_root,
                deployconf=deployconf,
                start_cfg=start_cfg,
                apply_check_start_cfg=apply_cfg,
                artifacts_source_root=artifacts_source,
            )

            self.assertEqual(paths["bundle_root"], bundle_root.resolve())
            manifest = json.loads((bundle_root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(
                manifest,
                {
                    "bootstrap_mode": "apply_only",
                    "controller_request_mode": "direct",
                    "deployconf_path": "deployconf_testbed.local.yaml",
                    "ssh_config_path": "ssh_config",
                    "start_config_path": "start_test_bed.runner.yaml",
                    "workdir": "bootstrap_workdir",
                },
            )
            self.assertTrue((bundle_root / "bootstrap_workdir").is_dir())
            self.assertTrue((bundle_root / "gen_k8s_daemonset").is_dir())
            bundled_deployconf = _ENTRY._load_yaml_mapping(
                bundle_root / "deployconf_testbed.local.yaml",
                ctx="bundle deployconf",
            )
            self.assertEqual(
                bundled_deployconf["gen_k8s_daemonset_mirror_outdir"],
                str((bundle_root / "gen_k8s_daemonset").resolve()),
            )
            bundled_start = _ENTRY._load_yaml_mapping(
                bundle_root / "start_test_bed.runner.yaml",
                ctx="bundle start",
            )
            self.assertEqual(bundled_start["deployconf_path"], "./deployconf_testbed.local.yaml")
            self.assertEqual((bundle_root / "artifacts" / "prepare.yaml").resolve(), (artifacts_source / "prepare.yaml").resolve())

    def test_refresh_testbed_bundle_deployconf_uses_normalized_start_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle_root = root / "runner_run" / "testbed_bundle"
            start_workdir = root / "start_test_bed" / "bare"
            deployconf = {
                "gen_k8s_daemonset_mirror_outdir": "/tmp/old-mirror",
                "cluster_nodes": [],
                "service": {
                    "etcd": {
                        "port": 33579,
                        "entrypoint": "etcd --listen-client-urls http://0.0.0.0:33579",
                    },
                    "greptime": {
                        "port": 35030,
                    },
                },
            }
            start_cfg = {"schema_version": 6, "deployconf_path": "/tmp/old-deployconf.yaml"}
            paths = _ENTRY._write_ci_testbed_bundle(
                bundle_root=bundle_root,
                deployconf=deployconf,
                start_cfg=start_cfg,
                apply_check_start_cfg=start_cfg,
                artifacts_source_root=root / "missing_artifacts",
            )

            start_workdir.mkdir(parents=True)
            _ENTRY._write_yaml(
                start_workdir / "deployconf.with_release_manifest_sha256.yaml",
                {
                    "gen_k8s_daemonset_mirror_outdir": "/tmp/start-workdir-mirror",
                    "cluster_nodes": [],
                    "service": {
                        "etcd": {
                            "port": 19180,
                            "entrypoint": "etcd --listen-client-urls http://0.0.0.0:19180",
                        },
                        "greptime": {
                            "port": 19190,
                        },
                    },
                    "global_envs": {
                        "FLUXON_RELEASE_MANIFEST_SHA256": "must-not-leak-into-runner-bundle",
                    },
                },
            )

            _ENTRY._refresh_ci_testbed_bundle_deployconf_from_start_workdir(
                metadata={
                    "testbed_bundle_path": paths["bundle_root"],
                    "testbed_bundle_deployconf_path": paths["deployconf_path"],
                },
                start_workdir=start_workdir,
            )

            refreshed = _ENTRY._load_yaml_mapping(paths["deployconf_path"], ctx="refreshed deployconf")
            self.assertEqual(refreshed["service"]["etcd"]["port"], 19180)
            self.assertIn("19180", refreshed["service"]["etcd"]["entrypoint"])
            self.assertEqual(refreshed["service"]["greptime"]["port"], 19190)
            self.assertNotIn("FLUXON_RELEASE_MANIFEST_SHA256", refreshed["global_envs"])
            self.assertEqual(
                refreshed["gen_k8s_daemonset_mirror_outdir"],
                str((bundle_root / "gen_k8s_daemonset").resolve()),
            )

    def test_write_yaml_emits_ascii_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sample.yaml"
            _ENTRY._write_yaml(path, {"a": 1, "b": "x"})
            self.assertTrue(path.is_file())
            self.assertIn("a: 1", path.read_text(encoding="utf-8"))

    def test_find_single_wheel_prefers_non_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / _ENTRY.PLACEHOLDER_WHEEL_NAME).write_text("", encoding="utf-8")
            (root / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl").write_text("", encoding="utf-8")

            wheel_name = _ENTRY._find_single_wheel(root, pattern="fluxon-*.whl", ctx="wheel")

            self.assertEqual(wheel_name, "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl")

    def test_ensure_ci_pack_release_env_generates_explicit_companion_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            env_path = root / "generated" / "setup_and_pack" / "pack_fluxonkv_pylib_env.yaml"
            env_template_path = root / "setup_and_pack" / "pack_fluxonkv_pylib_env.yaml.template"
            generator_script_path = root / "setup_and_pack" / "ci" / "gen_pack_release_ci_config.py"
            env_template_path.parent.mkdir(parents=True, exist_ok=True)
            generator_script_path.parent.mkdir(parents=True, exist_ok=True)
            env_template_path.write_text("schema_version: 1\n", encoding="utf-8")
            generator_script_path.write_text("# placeholder\n", encoding="utf-8")

            calls: list[list[str]] = []

            def fake_run(argv: list[str], *, env=None) -> None:
                del env
                calls.append(list(argv))
                env_path.parent.mkdir(parents=True, exist_ok=True)
                env_path.write_text("schema_version: 1\n", encoding="utf-8")

            with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                generated = _ENTRY._ensure_ci_pack_release_env(
                    project_data_root=root / "pack_release_runtime",
                    env_out_path=env_path,
                    env_template_path=env_template_path,
                    generator_script_path=generator_script_path,
                )

            self.assertEqual(generated, env_path.resolve())
            self.assertEqual(len(calls), 1)
            self.assertIn(str(generator_script_path.resolve()), calls[0])
            self.assertIn("--out-path", calls[0])
            self.assertIn(str(env_path.resolve()), calls[0])
            self.assertIn(str((root / "pack_release_runtime").resolve()), calls[0])

    def test_render_ci_nix_pack_config_sets_explicit_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            static_config_path = root / "static.yaml"
            env_companion_path = root / "env.yaml"
            out_path = root / "generated" / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_ci.yaml"

            _ENTRY._write_yaml(
                static_config_path,
                {
                    "schema_version": 1,
                    "runtime": {
                        "base_system": "manylinux_2_28",
                        "architectures": ["x86_64"],
                        "python_abi": "cpython3.10",
                    },
                    "profile": {
                        "source_kind": "bridge_prebuilt",
                        "native_runtime_dir_names": ["cxxpacked"],
                        "target_support_dir_names": ["meson-0.64.0"],
                        "ext_bundle_dir_name": "cxxpacked",
                    },
                    "assembly": {
                        "baseline_path": "/tmp/baseline",
                    },
                },
            )
            _ENTRY._write_yaml(
                env_companion_path,
                {
                    "host_paths": {
                        "root_path": "/tmp/project-data",
                    },
                },
            )

            rendered_path = _ENTRY._render_ci_nix_pack_config(
                static_config_path=static_config_path,
                env_companion_path=env_companion_path,
                out_path=out_path,
                repo_root=REPO_ROOT,
            )

            self.assertEqual(rendered_path, out_path.resolve())
            rendered_cfg = _ENTRY._load_yaml_mapping(rendered_path, ctx="rendered nix pack config")
            self.assertEqual(rendered_cfg["project_root"], str(REPO_ROOT.resolve()))
            self.assertEqual(rendered_cfg["profile"]["build_root_path"], str(REPO_ROOT.resolve()))

    def test_prepare_pack_release_runtime_dirs_creates_expected_layout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "pack_release_runtime"

            _ENTRY._prepare_pack_release_runtime_dirs(project_data_root=root)

            self.assertTrue((root / "manylinux-release").is_dir())
            self.assertTrue((root / "manylinux-cache" / "cargo-registry").is_dir())
            self.assertTrue((root / "manylinux-cache" / "cargo-git").is_dir())

    def test_cleanup_pack_release_runtime_removes_only_owned_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td) / "ci_workdir"
            runtime_root = workdir / "pack_release_runtime"
            nested = runtime_root / "project-data" / "cache"
            nested.mkdir(parents=True)
            (nested / "artifact.bin").write_bytes(b"artifact")
            sibling = workdir / "generated" / "suite.yaml"
            sibling.parent.mkdir(parents=True)
            sibling.write_text("schema_version: 1\n", encoding="utf-8")

            _ENTRY._cleanup_pack_release_runtime_after_success(
                runtime_root=runtime_root,
                workdir=workdir,
            )

            self.assertFalse(runtime_root.exists())
            self.assertEqual(sibling.read_text(encoding="utf-8"), "schema_version: 1\n")

            unexpected = workdir / "unexpected"
            unexpected.mkdir()
            with self.assertRaisesRegex(ValueError, "unexpected pack runtime path"):
                _ENTRY._cleanup_pack_release_runtime_after_success(
                    runtime_root=unexpected,
                    workdir=workdir,
                )

    def test_sync_rather_no_git_submodule_uses_canonical_entrypoint(self) -> None:
        calls: list[list[str]] = []

        def fake_run(argv: list[str], *, env=None) -> None:
            del env
            calls.append(list(argv))

        with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
            _ENTRY._sync_rather_no_git_submodule()

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], sys.executable)
        self.assertEqual(calls[0][1], str(_ENTRY.DEFAULT_RATHER_NO_GIT_SUBMODULE_SCRIPT.resolve()))

    def test_same_host_local_testbed_host_ip_requires_non_loopback(self) -> None:
        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
            self.assertEqual(_ENTRY._same_host_local_testbed_host_ip(), "192.0.2.119")
        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="127.0.0.1"):
            with self.assertRaisesRegex(RuntimeError, "requires a non-loopback IPv4 address"):
                _ENTRY._same_host_local_testbed_host_ip()

    def test_main_passes_generated_start_test_bed_config_to_runner_env(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = root / "release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")
            _ENTRY._write_yaml(
                workdir / "start_test_bed" / "apply" / "deployconf.with_release_manifest_sha256.yaml",
                {
                    "gen_k8s_daemonset_mirror_outdir": "/tmp/mock-start-mirror",
                    "cluster_nodes": [],
                    "service": {
                        "etcd": {"port": 19180},
                        "greptime": {"port": 19190},
                    },
                },
            )
            calls: list[tuple[list[str], dict[str, str] | None]] = []

            def fake_run(argv: list[str], *, env=None) -> None:
                calls.append((list(argv), None if env is None else dict(env)))

            argv = [
                "ci_2_virt_node.py",
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
                "--release-dir",
                str(release_dir),
                "--scene-id",
                self._KVTEST_SCENE_ID,
                "--skip-builder-image",
                "--skip-pack",
                "--skip-dispatch",
                "--skip-start-testbed",
                "--cleanup-successful-case-artifacts",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                    with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
                            sys.argv = argv
                            rc = _ENTRY.main()
            finally:
                sys.argv = original_argv

            self.assertEqual(rc, 0)
            self.assertTrue(calls)
            runner_argv, runner_env = calls[-1]
            self.assertIsNotNone(runner_env)
            self.assertEqual(runner_argv[1], str((REPO_ROOT / "fluxon_test_stack" / "test_runner.py").resolve()))
            self.assertIn("--cleanup-successful-case-artifacts", runner_argv)
            self.assertEqual(
                runner_env[_ENTRY.TEST_STACK_START_TEST_BED_CONFIG_ENV],
                str((workdir / "runner_run" / "testbed_bundle" / "start_test_bed.runner.yaml").resolve()),
            )
            self.assertEqual(
                runner_env["FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"],
                str(release_dir.resolve()),
            )
            refreshed = _ENTRY._load_yaml_mapping(
                workdir / "runner_run" / "testbed_bundle" / "deployconf_testbed.local.yaml",
                ctx="refreshed runner bundle deployconf",
            )
            self.assertEqual(refreshed["service"]["etcd"]["port"], 19180)
            self.assertEqual(refreshed["service"]["greptime"]["port"], 19190)

    def test_main_supports_explicit_suite_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            suite_path = root / "ci_test_list.local.yaml"
            suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
            suite_cfg["scenes"] = {
                key: value
                for key, value in suite_cfg["scenes"].items()
                if key in (self._DOC_SCENE_ID, self._KVTEST_SCENE_ID, self._LOG_MGMT_SCENE_ID, self._MQ_SCENE_ID)
            }
            suite_cfg["profiles"] = {"fluxon_tcp": suite_cfg["profiles"]["fluxon_tcp"]}
            suite_cfg["run"]["selectors"]["profile_ids"] = ["fluxon_tcp"]
            suite_cfg["profiles"]["fluxon_tcp"]["runtime"]["ci"]["scene_configs"][self._KVTEST_SCENE_ID]["kv_test_rounds"] = "p2p_only"
            suite_cfg["profiles"]["fluxon_tcp"]["runtime"]["ci"]["scene_configs"][self._DOC_SCENE_ID]["doc_site_base_url"] = (
                "tele-ai.github.io/Fluxon"
            )
            suite_cfg["profiles"]["fluxon_tcp"]["runtime"]["ci"]["scene_configs"][self._LOG_MGMT_SCENE_ID]["enabled"] = True
            suite_cfg["profiles"]["fluxon_tcp"]["runtime"]["ci"]["scene_configs"][self._MQ_SCENE_ID] = {}
            _ENTRY._write_yaml(suite_path, suite_cfg)
            release_dir = root / "release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")

            argv = [
                "ci_2_virt_node.py",
                "--suite-path",
                str(suite_path),
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
                "--release-dir",
                str(release_dir),
                "--skip-builder-image",
                "--skip-pack",
                "--skip-dispatch",
                "--skip-start-testbed",
                "--skip-runner",
                "--print-generated",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                    with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
                        sys.argv = argv
                        rc = _ENTRY.main()
            finally:
                sys.argv = original_argv

            self.assertEqual(rc, 0)
            generated_suite = _ENTRY._load_yaml_mapping(
                workdir / "generated" / "ci_test_list.local.yaml",
                ctx="generated suite",
            )
            self.assertEqual(
                set(generated_suite["scenes"].keys()),
                {self._DOC_SCENE_ID, self._KVTEST_SCENE_ID, self._LOG_MGMT_SCENE_ID, self._MQ_SCENE_ID},
            )
            self.assertEqual(
                generated_suite["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._KVTEST_SCENE_ID][
                    "kv_test_rounds"
                ],
                "p2p_only",
            )
            self.assertEqual(
                generated_suite["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._DOC_SCENE_ID][
                    "doc_site_base_url"
                ],
                "tele-ai.github.io/Fluxon",
            )
            self.assertEqual(
                generated_suite["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._LOG_MGMT_SCENE_ID][
                    "enabled"
                ],
                True,
            )
            self.assertEqual(
                generated_suite["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._MQ_SCENE_ID],
                {},
            )

    def test_main_same_host_generated_configs_use_non_loopback_host_ip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = root / "release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")

            argv = [
                "ci_2_virt_node.py",
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
                "--release-dir",
                str(release_dir),
                "--scene-id",
                self._DOC_SCENE_ID,
                "--skip-builder-image",
                "--skip-pack",
                "--skip-dispatch",
                "--skip-start-testbed",
                "--skip-runner",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                    with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
                        sys.argv = argv
                        rc = _ENTRY.main()
            finally:
                sys.argv = original_argv

            self.assertEqual(rc, 0)
            generated_deployconf = _ENTRY._load_yaml_mapping(
                workdir / "generated" / "deployconf_testbed.local.yaml",
                ctx="generated deployconf",
            )
            generated_start = _ENTRY._load_yaml_mapping(
                workdir / "generated" / "start_test_bed.local.yaml",
                ctx="generated start_test_bed",
            )
            self.assertEqual(
                [node["ip"] for node in generated_deployconf["cluster_nodes"]],
                ["192.0.2.119", "192.0.2.119"],
            )
            self.assertEqual(
                generated_start["controller_url"],
                "http://192.0.2.119:19080/r/ops/fluxon_testbed",
            )
            self.assertIn('    - "192.0.2.119/32"', generated_deployconf["service"]["master"]["entrypoint"])

    def test_main_syncs_rather_no_git_submodule_before_pack(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = root / "release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")
            calls: list[tuple[list[str], dict[str, str] | None]] = []

            def fake_run(argv: list[str], *, env=None) -> None:
                calls.append((list(argv), None if env is None else dict(env)))
                if argv[1] != str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()):
                    return
                start_workdir = Path(argv[argv.index("-w") + 1])
                _ENTRY._write_yaml(
                    start_workdir / "deployconf.with_release_manifest_sha256.yaml",
                    {
                        "gen_k8s_daemonset_mirror_outdir": "/tmp/mock-start-mirror",
                        "cluster_nodes": [],
                        "service": {
                            "etcd": {"port": 19180},
                            "greptime": {"port": 19190},
                        },
                    },
                )

            argv = [
                "ci_2_virt_node.py",
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
                "--release-dir",
                str(release_dir),
                "--scene-id",
                self._KVTEST_SCENE_ID,
                "--skip-builder-image",
                "--skip-dispatch",
                "--skip-start-testbed",
                "--skip-runner",
                "--cleanup-pack-runtime-after-success",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                    with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
                            with mock.patch.object(_ENTRY, "_ensure_ci_pack_release_env", return_value=Path("/tmp/env.yaml")):
                                with mock.patch.object(_ENTRY, "_render_ci_nix_pack_config", return_value=Path("/tmp/cfg.yaml")):
                                    sys.argv = argv
                                    rc = _ENTRY.main()
            finally:
                sys.argv = original_argv

            self.assertEqual(rc, 0)
            self.assertGreaterEqual(len(calls), 1)
            self.assertEqual(
                calls[0][0],
                [sys.executable, str(_ENTRY.DEFAULT_RATHER_NO_GIT_SUBMODULE_SCRIPT.resolve())],
            )
            self.assertEqual(
                calls[1][0][1],
                str((REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py").resolve()),
            )
            self.assertFalse((workdir / "pack_release_runtime").exists())

    def test_main_passes_explicit_release_dir_to_pack_stage(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = root / "custom_release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")
            calls: list[tuple[list[str], dict[str, str] | None]] = []

            def fake_run(argv: list[str], *, env=None) -> None:
                calls.append((list(argv), None if env is None else dict(env)))

            argv = [
                "ci_2_virt_node.py",
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
                "--release-dir",
                str(release_dir),
                "--scene-id",
                self._KVTEST_SCENE_ID,
                "--skip-builder-image",
                "--skip-dispatch",
                "--skip-start-testbed",
                "--skip-runner",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                    with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
                            with mock.patch.object(_ENTRY, "_ensure_ci_pack_release_env", return_value=Path("/tmp/env.yaml")):
                                with mock.patch.object(_ENTRY, "_render_ci_nix_pack_config", return_value=Path("/tmp/cfg.yaml")):
                                    sys.argv = argv
                                    rc = _ENTRY.main()
            finally:
                sys.argv = original_argv

            self.assertEqual(rc, 0)
            self.assertGreaterEqual(len(calls), 2)
            pack_cmd = calls[1][0]
            self.assertEqual(
                pack_cmd[1],
                str((REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py").resolve()),
            )
            self.assertIn("--release-dir", pack_cmd)
            self.assertEqual(
                pack_cmd[pack_cmd.index("--release-dir") + 1],
                str(release_dir.resolve()),
            )

    def test_main_uses_apply_check_config_for_explicit_apply_validation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = root / "release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")
            calls: list[tuple[list[str], dict[str, str] | None]] = []

            def fake_run(argv: list[str], *, env=None) -> None:
                calls.append((list(argv), None if env is None else dict(env)))
                if argv[1] != str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()):
                    return
                start_workdir = Path(argv[argv.index("-w") + 1])
                _ENTRY._write_yaml(
                    start_workdir / "deployconf.with_release_manifest_sha256.yaml",
                    {
                        "gen_k8s_daemonset_mirror_outdir": "/tmp/mock-start-mirror",
                        "cluster_nodes": [],
                        "service": {
                            "etcd": {"port": 19180},
                            "greptime": {"port": 19190},
                        },
                    },
                )

            argv = [
                "ci_2_virt_node.py",
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
                "--release-dir",
                str(release_dir),
                "--scene-id",
                self._KVTEST_SCENE_ID,
                "--skip-builder-image",
                "--skip-pack",
                "--skip-dispatch",
                "--skip-runner",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                    with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="192.0.2.119"):
                            sys.argv = argv
                            rc = _ENTRY.main()
            finally:
                sys.argv = original_argv

            self.assertEqual(rc, 0)
            start_bed_calls = [
                call_argv for (call_argv, _) in calls if call_argv[1] == str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve())
            ]
            self.assertEqual(len(start_bed_calls), 2)
            self.assertEqual(
                start_bed_calls[0][start_bed_calls[0].index("-c") + 1],
                str((workdir / "runner_run" / "testbed_bundle" / "start_test_bed.runner.yaml").resolve()),
            )
            self.assertEqual(
                start_bed_calls[1][start_bed_calls[1].index("-c") + 1],
                str((workdir / "runner_run" / "testbed_bundle" / "start_test_bed.apply_check.runner.yaml").resolve()),
            )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
