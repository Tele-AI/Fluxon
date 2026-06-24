#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_2_virt_node.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_ci_2_virt_node_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_ENTRY = _load_module()


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

    def test_generated_suite_is_public_dual_local_nodes_ci_only(self) -> None:
        suite_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_SUITE_PATH, ctx="suite")
        generated = _ENTRY._rewrite_suite_for_local_dual_nodes(
            suite_cfg=suite_cfg,
            scene_ids=[self._DOC_SCENE_ID, self._KVTEST_SCENE_ID, self._LOG_MGMT_SCENE_ID, self._MQ_SCENE_ID],
            primary_node_name="local-node-a",
            secondary_node_name="local-node-b",
            host_ip="10.1.1.119",
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
            {"local-node-a": "10.1.1.119", "local-node-b": "10.1.1.119"},
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
            host_ip="10.1.1.119",
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
            host_ip="10.1.1.119",
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
            host_ip="10.1.1.119",
            wheel_name="fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            controller_port=19080,
        )

        self.assertEqual(
            generated["profiles"]["fluxon_tcp_thread"]["runtime"]["ci"]["scene_configs"][self._CARGO_KV_UNIT_SCENE_ID][
                "kv_transport_feature"
            ],
            "tcp_thread_transport",
        )

    def test_generated_suite_supports_additional_runner_native_cargo_scenes(self) -> None:
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
            host_ip="10.1.1.119",
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
            host_ip="10.1.1.119",
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
            host_ip="10.1.1.119",
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
            ["10.1.1.119", "10.1.1.119"],
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
        self.assertIn('    - "10.1.1.119/32"', generated["service"]["master"]["entrypoint"])

    def test_generated_start_test_bed_config_points_to_local_authorities(self) -> None:
        start_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed")
        generated = _ENTRY._rewrite_start_test_bed_for_local_dual_nodes(
            start_cfg=start_cfg,
            generated_deployconf_path=Path("/tmp/deployconf.yaml"),
            primary_node_name="local-node-a",
            controller_access_ip="10.1.1.119",
            controller_port=19080,
            ui_port=18080,
            ui_workdir=Path("/tmp/ui"),
        )

        self.assertEqual(generated["deployconf_path"], "/tmp/deployconf.yaml")
        self.assertEqual(generated["controller_url"], "http://10.1.1.119:19080/r/ops/fluxon_testbed")
        self.assertEqual(generated["controller_basic_auth"]["username"], "ops_admin")
        self.assertEqual(generated["controller_basic_auth"]["password"], "ops_password")
        self.assertEqual(generated["test_runner_ui"]["workdir"], "/tmp/ui")
        self.assertIsNone(generated["test_runner_ui"]["gitops_config_path"])
        self.assertEqual(generated["bootstrap_phases"][0]["node"], "local-node-a")

    def test_generated_apply_check_config_excludes_control_plane_reapply(self) -> None:
        start_cfg = _ENTRY._load_yaml_mapping(_ENTRY.DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed")
        local_cfg = _ENTRY._rewrite_start_test_bed_for_local_dual_nodes(
            start_cfg=start_cfg,
            generated_deployconf_path=Path("/tmp/deployconf.yaml"),
            primary_node_name="local-node-a",
            controller_access_ip="10.1.1.119",
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
        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
            self.assertEqual(_ENTRY._same_host_local_testbed_host_ip(), "10.1.1.119")
        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="127.0.0.1"):
            with self.assertRaisesRegex(RuntimeError, "requires a non-loopback IPv4 address"):
                _ENTRY._same_host_local_testbed_host_ip()

    def test_main_passes_generated_start_test_bed_config_to_runner_env(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = REPO_ROOT / "fluxon_release"
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
                "--scene-id",
                self._KVTEST_SCENE_ID,
                "--skip-builder-image",
                "--skip-pack",
                "--skip-dispatch",
                "--skip-start-testbed",
            ]
            original_argv = sys.argv[:]
            try:
                with mock.patch.object(_ENTRY, "_run", side_effect=fake_run):
                    with mock.patch.object(_ENTRY, "_detect_local_hostname", return_value="runner-host"):
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
                            sys.argv = argv
                            rc = _ENTRY.main()
            finally:
                sys.argv = original_argv
                wheel_path.unlink(missing_ok=True)

            self.assertEqual(rc, 0)
            self.assertTrue(calls)
            runner_argv, runner_env = calls[-1]
            self.assertIsNotNone(runner_env)
            self.assertEqual(runner_argv[1], str((REPO_ROOT / "fluxon_test_stack" / "test_runner.py").resolve()))
            self.assertEqual(
                runner_env[_ENTRY.TEST_STACK_START_TEST_BED_CONFIG_ENV],
                str((workdir / "generated" / "start_test_bed.local.yaml").resolve()),
            )
            self.assertEqual(
                runner_env["FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"],
                str((REPO_ROOT / "fluxon_release").resolve()),
            )

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
            release_dir = REPO_ROOT / "fluxon_release"
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
                    with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
                        sys.argv = argv
                        rc = _ENTRY.main()
            finally:
                sys.argv = original_argv
                wheel_path.unlink(missing_ok=True)

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
            release_dir = REPO_ROOT / "fluxon_release"
            release_dir.mkdir(parents=True, exist_ok=True)
            wheel_path = release_dir / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            wheel_path.write_text("", encoding="utf-8")

            argv = [
                "ci_2_virt_node.py",
                "--workdir",
                str(workdir),
                "--testbed-hostworkdir",
                str(hostworkdir),
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
                    with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
                        sys.argv = argv
                        rc = _ENTRY.main()
            finally:
                sys.argv = original_argv
                wheel_path.unlink(missing_ok=True)

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
                ["10.1.1.119", "10.1.1.119"],
            )
            self.assertEqual(
                generated_start["controller_url"],
                "http://10.1.1.119:19080/r/ops/fluxon_testbed",
            )
            self.assertIn('    - "10.1.1.119/32"', generated_deployconf["service"]["master"]["entrypoint"])

    def test_main_syncs_rather_no_git_submodule_before_pack(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            workdir = root / "ci_2_virt_node_workdir"
            hostworkdir = root / "hostworkdir"
            release_dir = REPO_ROOT / "fluxon_release"
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
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
                            with mock.patch.object(_ENTRY, "_ensure_ci_pack_release_env", return_value=Path("/tmp/env.yaml")):
                                with mock.patch.object(_ENTRY, "_render_ci_nix_pack_config", return_value=Path("/tmp/cfg.yaml")):
                                    sys.argv = argv
                                    rc = _ENTRY.main()
            finally:
                sys.argv = original_argv
                wheel_path.unlink(missing_ok=True)

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
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
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
            release_dir = REPO_ROOT / "fluxon_release"
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
                        with mock.patch.object(_ENTRY, "_detect_local_ipv4", return_value="10.1.1.119"):
                            sys.argv = argv
                            rc = _ENTRY.main()
            finally:
                sys.argv = original_argv
                wheel_path.unlink(missing_ok=True)

            self.assertEqual(rc, 0)
            start_bed_calls = [
                call_argv for (call_argv, _) in calls if call_argv[1] == str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve())
            ]
            self.assertEqual(len(start_bed_calls), 2)
            self.assertEqual(
                start_bed_calls[0][start_bed_calls[0].index("-c") + 1],
                str((workdir / "generated" / "start_test_bed.local.yaml").resolve()),
            )
            self.assertEqual(
                start_bed_calls[1][start_bed_calls[1].index("-c") + 1],
                str((workdir / "generated" / "start_test_bed.apply_check.local.yaml").resolve()),
            )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
