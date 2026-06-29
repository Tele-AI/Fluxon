from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_remote_testbed.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_ci_remote_testbed_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_ENTRY = _load_module()
LOCAL_CONFIG_PATH = REPO_ROOT / "ci_remote_testbed.local.yaml"
REMOTE_SPEC = {
    "cluster_nodes": [
        {
            "hostname": "infra44-ThinkStation-PX",
            "ip": "192.168.151.44",
        },
        {
            "hostname": "infra46-ThinkStation-PX",
            "ip": "192.168.151.46",
        },
    ],
    "bootstrap_primary_hostname": "infra44-ThinkStation-PX",
    "supported_topologies": {1, 2},
    "default_profile_ids": [
        "fluxon_fastws",
        "fluxon_tquic",
        "fluxon_sockudo_ws",
        "fluxon_tcp",
    ],
}


class TestCiRemoteTestbedContract(unittest.TestCase):
    def test_selected_ci_scene_ids_reuses_the_shared_catalog(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_CI_SUITE_PATH, ctx="ci suite")
        self.assertEqual(_ENTRY._selected_ci_scene_ids(suite_cfg), list(_ENTRY.canonical_ci_scene_ids()))

    def test_filter_suite_for_remote_cluster_keeps_only_supported_topologies(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_BENCHMARK_SUITE_PATH, ctx="suite")
        remote_spec = dict(REMOTE_SPEC)
        remote_target_ip_map = _ENTRY._remote_cluster_target_ip_map(remote_spec)

        generated = _ENTRY._filter_suite_for_remote_cluster(
            suite_cfg=suite_cfg,
            scene_ids=["bench_mq"],
            profile_ids=["fluxon_tcp"],
            remote_spec=remote_spec,
            remote_target_ip_map=remote_target_ip_map,
            allowed_scale_topologies={2},
        )

        self.assertEqual(set(generated["scenes"].keys()), {"bench_mq"})
        self.assertEqual(generated["scenes"]["bench_mq"]["select"]["scales"], ["n2_kvowner_dram_20gib"])
        self.assertEqual(generated["scenes"]["bench_mq"]["select"]["profiles"], ["fluxon_tcp"])
        self.assertEqual(set(generated["scales"].keys()), {"n2_kvowner_dram_20gib"})
        self.assertEqual(
            generated["scales"]["n2_kvowner_dram_20gib"]["targets"],
            {
                "hosts": ["infra44-ThinkStation-PX", "infra46-ThinkStation-PX"],
                "primary": "infra44-ThinkStation-PX",
                "secondary": "infra46-ThinkStation-PX",
            },
        )
        self.assertEqual(
            generated["profiles"]["fluxon_tcp"]["runtime"]["test_stack"]["deploy"]["target_ip_map"],
            {
                "infra44-ThinkStation-PX": "192.168.151.44",
                "infra46-ThinkStation-PX": "192.168.151.46",
            },
        )

    def test_selected_benchmark_scene_ids_prefers_multi_machine_topologies(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_BENCHMARK_SUITE_PATH, ctx="benchmark suite")
        selected_scene_ids = _ENTRY._selected_benchmark_scene_ids(suite_cfg, remote_spec=dict(REMOTE_SPEC))
        self.assertIn("bench_mq", selected_scene_ids)
        self.assertIn("kv_read_heavy_zipf", selected_scene_ids)
        self.assertIn("fs_open_read_close_smallfiles", selected_scene_ids)
        self.assertNotIn("ci_top_attention_doc_page_build", selected_scene_ids)

    def test_rewrite_remote_deployconf_uses_bounded_cluster_nodes(self) -> None:
        deployconf_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_DEPLOYCONF_TEMPLATE, ctx="deployconf")
        remote_spec = dict(REMOTE_SPEC)
        remote_target_ip_map = _ENTRY._remote_cluster_target_ip_map(remote_spec)

        generated = _ENTRY._rewrite_remote_deployconf(
            deployconf_cfg=deployconf_cfg,
            remote_spec=remote_spec,
            remote_target_ip_map=remote_target_ip_map,
            remote_hostworkdir_root=Path("/mnt/nvme0/store_team_dev/fluxon_deploy"),
            remote_ssh_user="tester",
            remote_ssh_port=22,
            remote_ssh_password="secret",
            wheel_name="fluxon-0.2.1-py3-none-any.whl",
            controller_port=19080,
        )

        self.assertEqual(
            generated["cluster_nodes"],
            [
                {
                    "hostname": "infra44-ThinkStation-PX",
                    "ip": "192.168.151.44",
                    "hostworkdir": "/mnt/nvme0/store_team_dev/fluxon_deploy",
                    "ssh_host": "192.168.151.44",
                    "ssh_user": "tester",
                    "ssh_port": 22,
                    "ssh_password": "secret",
                },
                {
                    "hostname": "infra46-ThinkStation-PX",
                    "ip": "192.168.151.46",
                    "hostworkdir": "/mnt/nvme0/store_team_dev/fluxon_deploy",
                    "ssh_host": "192.168.151.46",
                    "ssh_user": "tester",
                    "ssh_port": 22,
                    "ssh_password": "secret",
                },
            ],
        )
        self.assertEqual(
            generated["global_envs"]["FLUXON_CLUSTER_NODE_IDS"],
            "infra44-ThinkStation-PX infra46-ThinkStation-PX",
        )
        self.assertEqual(generated["global_envs"]["MASTER__PORT"], "19080")
        self.assertEqual(
            generated["global_envs"]["FLUXON_OPS_UI_BASE_URL"],
            "http://${OPS_CONTROLLER__NODE_ID__IP}:19080",
        )
        self.assertIn('--wheel "$FLUXON_RELEASE_WHEEL"', generated["global_envs"]["FLUXON_RELEASE_WHEEL_FETCH_CMD"])
        self.assertEqual(
            generated["atomic_groups"]["fluxon_core_controller"]["nodes"],
            ["infra44-ThinkStation-PX", "infra46-ThinkStation-PX"],
        )
        service_text = json.dumps(generated["service"], ensure_ascii=False, sort_keys=True)
        self.assertNotIn("example-node-a", service_text)
        self.assertNotIn("example-node-b", service_text)
        self.assertIn("infra44-ThinkStation-PX", service_text)
        self.assertIn("infra46-ThinkStation-PX", service_text)
        self.assertNotIn("deployer-runtime-infra44", service_text)
        self.assertNotIn("deployer-runtime-infra46", service_text)
        self.assertIn("deployer-runtime-node-a", service_text)
        self.assertIn("deployer-runtime-node-b", service_text)

    def test_rewrite_remote_start_test_bed_points_to_public_controller(self) -> None:
        start_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed")
        remote_spec = dict(REMOTE_SPEC)

        generated = _ENTRY._rewrite_remote_start_test_bed(
            start_cfg=start_cfg,
            generated_deployconf_path=Path("/tmp/deployconf.yaml"),
            remote_spec=remote_spec,
            controller_public_url="http://192.168.151.44:19080/r/ops/fluxon_testbed",
            ui_port=18080,
            ui_workdir=Path("/tmp/ui"),
        )

        self.assertEqual(generated["deployconf_path"], "/tmp/deployconf.yaml")
        self.assertEqual(
            generated["controller_url"],
            "http://192.168.151.44:19080/r/ops/fluxon_testbed",
        )
        self.assertEqual(generated["controller_basic_auth"]["username"], "ops_admin")
        self.assertEqual(generated["test_runner_ui"]["workdir"], "/tmp/ui")
        self.assertEqual(generated["bootstrap_phases"][0]["node"], "infra44-ThinkStation-PX")

    def test_main_generates_ci_and_benchmark_phases_and_triggers_remote_runner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_remote_testbed_workdir"
            release_dir = root / "release"
            local_config_path = root / "ci_remote_testbed.local.yaml"
            for artifact_set_id in ("fluxon_fastws", "fluxon_tquic", "fluxon_sockudo_ws", "fluxon_tcp"):
                for relpath in (
                    Path(f"profiles/{artifact_set_id}/fluxon_release.sha256"),
                    Path(f"test_rsc/{artifact_set_id}/fluxon_test_rsc.sha256"),
                ):
                    path = release_dir / relpath
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("", encoding="utf-8")
            (release_dir / "install.py").write_text("print('ok')\n", encoding="utf-8")
            (release_dir / "fluxon_release.sha256").write_text("", encoding="utf-8")
            (release_dir / "fluxon-0.2.1-py3-none-any.whl").write_text("", encoding="utf-8")
            local_config_path.write_text(
                yaml.safe_dump(
                    {
                        "testbed_cluster_id": "testbed_44_46",
                        "testbed_cluster": {
                            "bootstrap_primary_hostname": "infra44-ThinkStation-PX",
                            "supported_topologies": [1, 2],
                            "default_profile_ids": [
                                "fluxon_fastws",
                                "fluxon_tquic",
                                "fluxon_sockudo_ws",
                                "fluxon_tcp",
                            ],
                            "cluster_nodes": [
                                {
                                    "hostname": "infra44-ThinkStation-PX",
                                    "ip": "192.168.151.44",
                                },
                                {
                                    "hostname": "infra46-ThinkStation-PX",
                                    "ip": "192.168.151.46",
                                },
                            ],
                        },
                        "controller_public_host": "192.168.151.44",
                        "controller_port": 19080,
                        "ui_port": 18080,
                        "remote_ssh_user": "tester",
                        "remote_ssh_password": "secret",
                        "remote_ssh_port": 2222,
                        "bastion_host": "192.168.151.44",
                        "bastion_user": "tester",
                        "bastion_port": 2233,
                        "bastion_password": "secret",
                        "controller_exec_host": "192.168.151.44",
                        "controller_exec_user": "tester",
                        "controller_exec_port": 2244,
                        "controller_exec_password": "secret",
                        "remote_repo_root": str(REPO_ROOT),
                        "remote_testbed_hostworkdir": "/mnt/nvme0/store_team_dev/fluxon_deploy",
                    },
                    sort_keys=False,
                    allow_unicode=False,
                ),
                encoding="utf-8",
            )

            LOCAL_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            previous_local_config_text = (
                LOCAL_CONFIG_PATH.read_text(encoding="utf-8") if LOCAL_CONFIG_PATH.exists() else None
            )
            LOCAL_CONFIG_PATH.write_text(local_config_path.read_text(encoding="utf-8"), encoding="utf-8")
            old_argv = sys.argv[:]
            old_cwd = Path.cwd()
            old_env = os.environ.copy()
            run_calls: list[tuple[list[str], dict[str, str] | None]] = []
            remote_calls: list[tuple[str, str, int, str | None, str]] = []

            def fake_run(argv: list[str], env: dict[str, str] | None = None) -> None:
                run_calls.append((list(argv), None if env is None else dict(env)))

            def fake_run_remote_bash(*, ssh_user: str, ssh_host: str, ssh_port: int, ssh_password: str | None, remote_cmd: str) -> None:
                remote_calls.append((ssh_user, ssh_host, ssh_port, ssh_password, remote_cmd))

            def fake_run_remote_bash_output(*, ssh_user: str, ssh_host: str, ssh_port: int, ssh_password: str | None, remote_cmd: str) -> str:
                remote_calls.append((ssh_user, ssh_host, ssh_port, ssh_password, remote_cmd))
                if "REMOTE_EXIT_CODE:" in remote_cmd:
                    return "REMOTE_EXIT_CODE:0\nremote runner complete\n"
                return "remote runner started\n"

            try:
                os.chdir(REPO_ROOT)
                sys.argv = [
                    "ci_remote_testbed.py",
                    "--workdir",
                    str(workdir),
                    "--release-dir",
                    str(release_dir),
                    "--print-generated",
                ]
                with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                    with mock.patch.object(_ENTRY, "_run_remote_bash", side_effect=fake_run_remote_bash):
                        with mock.patch.object(_ENTRY, "_run_remote_bash_output", side_effect=fake_run_remote_bash_output):
                            with mock.patch.object(_ENTRY, "_copy_local_dir_to_remote"):
                                with mock.patch.object(_ENTRY, "_bundle_remote_workdir"):
                                    with mock.patch.object(_ENTRY, "_remote_runner_poll_until_complete", return_value=0):
                                        rc = _ENTRY.main()
            finally:
                sys.argv = old_argv
                os.chdir(old_cwd)
                os.environ.clear()
                os.environ.update(old_env)
                if previous_local_config_text is None:
                    LOCAL_CONFIG_PATH.unlink(missing_ok=True)
                else:
                    LOCAL_CONFIG_PATH.write_text(previous_local_config_text, encoding="utf-8")

            self.assertEqual(rc, 0)
            ci_suite = _ENTRY._load_yaml_mapping(workdir / "generated" / "ci.yaml", ctx="generated ci suite")
            benchmark_suite = _ENTRY._load_yaml_mapping(
                workdir / "generated" / "benchmark.yaml",
                ctx="generated benchmark suite",
            )
            self.assertEqual(set(ci_suite["scenes"].keys()), set(_ENTRY.canonical_ci_scene_ids()))
            self.assertEqual(ci_suite["run"]["selectors"]["profile_ids"], ["fluxon_tcp"])
            self.assertEqual(
                benchmark_suite["scenes"]["bench_mq"]["select"]["scales"],
                ["n2_kvowner_dram_20gib"],
            )
            self.assertEqual(
                benchmark_suite["run"]["selectors"]["profile_ids"],
                ["fluxon_fastws", "fluxon_tquic", "fluxon_sockudo_ws", "fluxon_tcp"],
            )
            self.assertTrue((workdir / "generated" / "ci.yaml").is_file())
            self.assertTrue((workdir / "generated" / "benchmark.yaml").is_file())
            deployconf = _ENTRY._load_yaml_mapping(
                workdir / "testbed_bundle" / "deployconf_testbed.remote.yaml",
                ctx="generated deployconf",
            )
            self.assertEqual(
                deployconf["cluster_nodes"],
                [
                    {
                        "hostname": "infra44-ThinkStation-PX",
                        "ip": "192.168.151.44",
                        "hostworkdir": "/mnt/nvme0/store_team_dev/fluxon_deploy",
                        "ssh_host": "192.168.151.44",
                        "ssh_user": "tester",
                        "ssh_port": 2222,
                        "ssh_password": "secret",
                    },
                    {
                        "hostname": "infra46-ThinkStation-PX",
                        "ip": "192.168.151.46",
                        "hostworkdir": "/mnt/nvme0/store_team_dev/fluxon_deploy",
                        "ssh_host": "192.168.151.46",
                        "ssh_user": "tester",
                        "ssh_port": 2222,
                        "ssh_password": "secret",
                    },
                ],
            )
            self.assertTrue((workdir / "testbed_bundle" / "ssh_config").is_file())
            self.assertTrue((workdir / "testbed_bundle" / "generated" / "ci.yaml").is_file())
            self.assertTrue((workdir / "testbed_bundle" / "generated" / "benchmark.yaml").is_file())
            self.assertTrue((workdir / "testbed_bundle" / "artifacts" / "profiles" / "fluxon_fastws").is_dir())
            self.assertTrue((workdir / "testbed_bundle" / "artifacts" / "test_rsc" / "fluxon_fastws").is_dir())
            self.assertTrue((workdir / "testbed_bundle" / "artifacts" / "profiles" / "fluxon_tcp").is_dir())
            self.assertTrue((workdir / "testbed_bundle" / "artifacts" / "test_rsc" / "fluxon_tcp").is_dir())

            manifest = json.loads((workdir / "testbed_bundle" / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["testbed_cluster_id"], "testbed_44_46")
            self.assertEqual(manifest["controller_request_mode"], "ssh_exec_per_request")
            self.assertEqual(manifest["remote_auth_config_path"], "remote_auth.yaml")
            self.assertEqual(
                manifest["controller_public_url"],
                "http://192.168.151.44:19080/r/ops/fluxon_testbed",
            )
            self.assertEqual(
                manifest["controller_bastion_local_url"],
                "http://127.0.0.1:19080/r/ops/fluxon_testbed",
            )
            self.assertEqual([item["phase_name"] for item in manifest["phase_runs"]], ["ci", "benchmark"])
            self.assertEqual(
                [Path(item["suite_path"]).name for item in manifest["phase_runs"]],
                ["ci.yaml", "benchmark.yaml"],
            )
            self.assertEqual(
                [Path(item["runner_workdir"]).name for item in manifest["phase_runs"]],
                ["ci", "benchmark"],
            )
            self.assertEqual(
                manifest["phase_runs"][0]["allowed_scale_topologies"],
                None,
            )
            self.assertEqual(manifest["phase_runs"][1]["allowed_scale_topologies"], [2])
            self.assertNotIn("bastion_user", manifest)
            self.assertNotIn("bastion_password", manifest)
            self.assertNotIn("controller_exec_host", manifest)
            self.assertNotIn("controller_exec_user", manifest)
            self.assertNotIn("controller_exec_port", manifest)
            self.assertNotIn("controller_exec_password", manifest)

            self.assertEqual(len(run_calls), 2)
            self.assertIn("pack_test_stack_rsc.py", run_calls[0][0][1])
            self.assertTrue(any(arg.endswith("benchmark_full_matrix.yaml") for arg in run_calls[0][0]))
            self.assertIn("manual_dispatch_release.py", run_calls[1][0][1])
            self.assertEqual(len(remote_calls), 1)
            self.assertEqual(remote_calls[0][0], "tester")
            self.assertEqual(remote_calls[0][1], "192.168.151.44")
            self.assertEqual(remote_calls[0][2], 2244)
            self.assertEqual(remote_calls[0][3], "secret")
            self.assertIn("remote runner launch requested", remote_calls[0][4])
            self.assertIn("remote_runner.py", remote_calls[0][4])
            self.assertNotIn("test_runner.py", remote_calls[0][4])

    def test_local_config_yaml_is_gitignored(self) -> None:
        gitignore_text = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("/ci_remote_testbed.local.yaml", gitignore_text)
        self.assertIn("/ci_remote_testbed_workdir/", gitignore_text)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
