#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
INDEX_DIR = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"
MODULE_PATH = INDEX_DIR / "_largescale_mq.py"
RUNNER_PATH = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"


def _load_module():
    sys.path.insert(0, str(INDEX_DIR))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_top_attention_largescale_mq", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(INDEX_DIR):
            sys.path.pop(0)


def _load_runner_module():
    runner_dir = RUNNER_PATH.parent
    sys.path.insert(0, str(runner_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_runner_for_largescale_mq", RUNNER_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(runner_dir):
            sys.path.pop(0)


class TestTopAttentionLargescaleMqContract(unittest.TestCase):
    def test_runner_mpmc_uses_process_fanout_for_single_host_logical_targets(self) -> None:
        runner = _load_runner_module()

        self.assertTrue(
            runner._test_stack_scene_uses_per_target_process_fanout(
                scene_mode=runner.TEST_STACK_MODE_MPMC,
            )
        )

    def test_local_coordinator_port_base_stays_in_range_for_high_controller_ports(self) -> None:
        entry = _load_module()

        self.assertEqual(
            entry._local_test_stack_coordinator_port_base(controller_port=19080, topology_key=4),
            20480,
        )
        self.assertEqual(
            entry._local_test_stack_coordinator_port_base(controller_port=63680, topology_key=4),
            65080,
        )
        self.assertEqual(
            entry._local_test_stack_coordinator_port_base(controller_port=63680, topology_key=16),
            25280,
        )
        self.assertEqual(
            entry._local_test_stack_coordinator_port_base(controller_port=63680, topology_key="42"),
            27880,
        )

    def test_local_p2p_port_block_skips_busy_ports(self) -> None:
        entry = _load_module()

        self.assertEqual(
            entry._find_local_tcp_port_block(
                preferred_start=20000,
                required_count=4,
                busy_ports={20000, 20001, 20002, 20003},
            ),
            20004,
        )

    def test_local_p2p_port_base_uses_free_block(self) -> None:
        entry = _load_module()

        base = entry._local_test_stack_p2p_port_base(
            controller_port=63680,
            topology_key=20,
            required_count=8,
            busy_ports=set(),
        )
        shifted = entry._local_test_stack_p2p_port_base(
            controller_port=63680,
            topology_key=20,
            required_count=8,
            busy_ports=set(range(base, base + 8)),
        )

        self.assertEqual(shifted, base + 8)

    def test_local_p2p_port_base_avoids_ephemeral_ports(self) -> None:
        entry = _load_module()

        base = entry._local_test_stack_p2p_port_base(
            controller_port=63680,
            topology_key=4,
            required_count=512,
            busy_ports=set(range(32768, 61000)),
        )

        self.assertLess(base + 512, 32768)

    def test_local_master_port_base_uses_free_non_ephemeral_block(self) -> None:
        entry = _load_module()

        base = entry._local_test_stack_master_port_base(
            controller_port=23080,
            topology_key=4,
            required_count=10,
            busy_ports=set(range(32768, 61000)),
        )
        shifted = entry._local_test_stack_master_port_base(
            controller_port=23080,
            topology_key=4,
            required_count=10,
            busy_ports=set(range(32768, 61000)) | set(range(base, base + 10)),
        )

        self.assertLess(base + 10, 32768)
        self.assertEqual(shifted, base + 10)

    def test_generate_only_writes_minimal_ci_smoke_suite_without_running_runner(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            suite_out = Path(td) / "largescale_mq_suite.yaml"

            with mock.patch.object(entry, "call", side_effect=AssertionError("test_runner should not run")):
                with mock.patch.object(
                    sys,
                    "argv",
                    [
                        str(MODULE_PATH),
                        "--generate-only",
                        "--suite-out",
                        str(suite_out),
                        "--owner-count",
                        "1",
                        "--owner-dram-gib",
                        "1",
                        "--producer-count",
                        "2",
                        "--consumer-count",
                        "2",
                        "--duration-seconds",
                        "1",
                        "--value-size",
                        "256",
                        "--threads-per-process",
                        "1",
                        "--op-timeout-seconds",
                        "5",
                        "--cluster-ready-timeout-seconds",
                        "1800",
                        "--consumer-sim-min-ms",
                        "1",
                        "--consumer-sim-max-ms",
                        "1",
                    ],
                ):
                    rc = entry.main()

            self.assertEqual(rc, 0)
            suite = yaml.safe_load(suite_out.read_text(encoding="utf-8"))
            scale_id = "largescale_mq_n1owner_1gib_p2_c2"
            self.assertEqual(set(suite["scenes"].keys()), {"bench_mq"})
            self.assertEqual(suite["scenes"]["bench_mq"]["test_stack"]["mode"], "MPMC")
            self.assertEqual(
                suite["scenes"]["bench_mq"]["test_stack"]["role_weights"],
                {"producer": 1, "consumer": 1},
            )
            self.assertEqual(suite["scenes"]["bench_mq"]["select"]["scales"], [scale_id])
            self.assertEqual(suite["run"]["selectors"]["case_ids"], [f"bench_mq__{scale_id}__fluxon_tcp_thread"])
            self.assertEqual(suite["scales"][scale_id]["topology"], 4)
            self.assertEqual(suite["scales"][scale_id]["owner"]["owner_count"], 1)
            self.assertEqual(suite["scales"][scale_id]["owner"]["owner_dram_bytes"], 1073741824)
            self.assertEqual(suite["scales"][scale_id]["benchmark"]["threads_per_process"], 1)
            self.assertEqual(
                suite["scales"][scale_id]["targets"]["hosts"],
                ["node-1", "node-2", "node-3", "node-4"],
            )
            self.assertEqual(suite["scales"][scale_id]["owner"]["targets"], ["node-1"])
            port_entry = suite["profiles"]["fluxon_tcp_thread"]["runtime"]["test_stack"]["port_alloc"]["by_topology"][4]
            self.assertGreaterEqual(port_entry["kv_p2p_port_stride"], 512)

            runner = _load_runner_module()
            parsed = runner._parse_suite_config(suite)
            cases = runner._expand_cases(parsed)
            self.assertEqual([case.case_id for case in cases], [f"bench_mq__{scale_id}__fluxon_tcp_thread"])

    def test_generate_only_writes_explicit_active_producer_runtime_limit(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            suite_out = Path(td) / "largescale_mq_p320_limit.yaml"

            with mock.patch.object(entry, "call", side_effect=AssertionError("test_runner should not run")):
                with mock.patch.object(
                    sys,
                    "argv",
                    [
                        str(MODULE_PATH),
                        "--generate-only",
                        "--single-host-logical-targets",
                        "--suite-out",
                        str(suite_out),
                        "--owner-count",
                        "4",
                        "--owner-dram-gib",
                        "1",
                        "--producer-count",
                        "320",
                        "--consumer-count",
                        "8",
                        "--duration-seconds",
                        "60",
                        "--value-size",
                        "256",
                        "--threads-per-process",
                        "1",
                        "--op-timeout-seconds",
                        "30",
                        "--cluster-ready-timeout-seconds",
                        "1800",
                        "--mpmc-active-producer-runtime-limit",
                        "160",
                        "--consumer-sim-min-ms",
                        "700",
                        "--consumer-sim-max-ms",
                        "1500",
                    ],
                ):
                    rc = entry.main()

            self.assertEqual(rc, 0)
            suite = yaml.safe_load(suite_out.read_text(encoding="utf-8"))
            scale_id = "largescale_mq_n4owner_1gib_p320_c8"
            scale = suite["scales"][scale_id]
            self.assertEqual(scale["topology"], 82)
            self.assertEqual(scale["benchmark"]["processes_per_target"], 4)
            self.assertEqual(scale["benchmark"]["threads_per_process"], 1)
            self.assertEqual(scale["benchmark"]["mpmc_active_producer_runtime_limit"], 160)

            runner = _load_runner_module()
            parsed = runner._parse_suite_config(suite)
            cases = runner._expand_cases(parsed)
            self.assertEqual([case.case_id for case in cases], [f"bench_mq__{scale_id}__fluxon_tcp_thread"])

    def test_single_host_logical_targets_support_ci_owner_producer_consumer_matrix(self) -> None:
        entry = _load_module()
        cases = (
            (8, 8, 4, {"producer": 1, "consumer": 1}),
            (32, 32, 16, {"producer": 7, "consumer": 7}),
            (160, 8, 42, {"producer": 39, "consumer": 1}),
        )
        for producer_count, consumer_count, topology, role_weights in cases:
            with self.subTest(producer_count=producer_count, consumer_count=consumer_count):
                with tempfile.TemporaryDirectory() as td:
                    suite_out = Path(td) / f"largescale_mq_p{producer_count}_c{consumer_count}.yaml"

                    with mock.patch.object(entry, "call", side_effect=AssertionError("test_runner should not run")):
                        with mock.patch.object(
                            sys,
                            "argv",
                            [
                                str(MODULE_PATH),
                                "--generate-only",
                                "--single-host-logical-targets",
                                "--suite-out",
                                str(suite_out),
                                "--owner-count",
                                "4",
                                "--owner-dram-gib",
                                "1",
                                "--producer-count",
                                str(producer_count),
                                "--consumer-count",
                                str(consumer_count),
                                "--duration-seconds",
                                "90",
                                "--metric-warmup-seconds",
                                "60",
                                "--value-size",
                                "256",
                                "--op-timeout-seconds",
                                "5",
                                "--cluster-ready-timeout-seconds",
                                "1800",
                                "--consumer-sim-min-ms",
                                "1",
                                "--consumer-sim-max-ms",
                                "1",
                            ],
                        ):
                            rc = entry.main()

                    self.assertEqual(rc, 0)
                    suite = yaml.safe_load(suite_out.read_text(encoding="utf-8"))
                    scale_id = f"largescale_mq_n4owner_1gib_p{producer_count}_c{consumer_count}"
                    scale = suite["scales"][scale_id]
                    self.assertEqual(scale["topology"], topology)
                    self.assertEqual(scale["owner"]["owner_count"], 4)
                    self.assertEqual(scale["owner"]["targets"], ["node-1", "node-2", "node-3", "node-4"])
                    self.assertEqual(len(scale["targets"]["hosts"]), topology)
                    self.assertEqual(scale["benchmark"]["processes_per_target"], 4)
                    self.assertEqual(scale["benchmark"]["threads_per_process"], 1)
                    self.assertEqual(scale["benchmark"]["owner_group_processes"], 1)
                    self.assertEqual(scale["benchmark"]["value_size"], 256)
                    self.assertEqual(scale["duration_seconds"], 90)
                    self.assertEqual(scale["benchmark"]["metric_warmup_seconds"], 60)
                    target_map = suite["profiles"]["fluxon_tcp_thread"]["runtime"]["test_stack"]["deploy"]["target_ip_map"]
                    self.assertIn(f"node-{topology}", target_map)
                    self.assertEqual(target_map["node-1"], target_map[f"node-{topology}"])
                    self.assertEqual(
                        suite["scenes"]["bench_mq"]["test_stack"]["role_weights"],
                        role_weights,
                    )
                    port_entry = suite["profiles"]["fluxon_tcp_thread"]["runtime"]["test_stack"]["port_alloc"]["by_topology"][topology]
                    self.assertGreaterEqual(port_entry["kv_p2p_port_stride"], 512)

                    runner = _load_runner_module()
                    parsed = runner._parse_suite_config(suite)
                    expanded = runner._expand_cases(parsed)
                    self.assertEqual([case.case_id for case in expanded], [f"bench_mq__{scale_id}__fluxon_tcp_thread"])

    def test_script_defaults_keep_owner_and_payload_small(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            config_path = Path(td) / "benchmark_full_matrix_many_targets.yaml"
            suite_out = Path(td) / "largescale_mq_default.yaml"
            cfg = yaml.safe_load(entry.DEFAULT_CONFIG.read_text(encoding="utf-8"))
            target_map = cfg["profiles"]["fluxon_tcp"]["runtime"]["test_stack"]["deploy"]["target_ip_map"]
            for idx in range(1, 329):
                target_map[f"node-{idx}"] = f"10.88.{idx // 250}.{idx % 250 + 1}"
            config_path.write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=False), encoding="utf-8")

            with mock.patch.object(entry, "call", side_effect=AssertionError("test_runner should not run")):
                with mock.patch.object(
                    sys,
                    "argv",
                    [
                        str(MODULE_PATH),
                        "--generate-only",
                        "--config",
                        str(config_path),
                        "--suite-out",
                        str(suite_out),
                    ],
                ):
                    rc = entry.main()

            self.assertEqual(rc, 0)
            suite = yaml.safe_load(suite_out.read_text(encoding="utf-8"))
            scale_id = "largescale_mq_n4owner_1gib_p320_c8"
            scale = suite["scales"][scale_id]
            self.assertEqual(scale["topology"], 328)
            self.assertEqual(scale["owner"]["owner_count"], 4)
            self.assertEqual(scale["owner"]["owner_dram_bytes"], 1073741824)
            self.assertEqual(scale["benchmark"]["processes_per_target"], 1)
            self.assertEqual(scale["benchmark"]["threads_per_process"], 1)
            self.assertNotIn("owner_group_processes", scale["benchmark"])
            self.assertEqual(scale["benchmark"]["value_size"], 256)
            self.assertEqual(scale["owner"]["targets"], ["node-1", "node-2", "node-3", "node-4"])

    def test_real_run_copies_bundle_uses_active_testbed_ip_and_invokes_runner(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = root / "source_bundle"
            bundle.mkdir()
            (bundle / "bootstrap_workdir").mkdir()
            (bundle / "ssh_config").write_text("# local\n", encoding="utf-8")
            (bundle / "gen_k8s_daemonset").mkdir()
            source_deployconf = bundle / "deployconf_testbed.local.yaml"
            source_start = bundle / "start_test_bed.runner.yaml"
            source_ssh_config = bundle / "ssh_config"
            source_bootstrap_workdir = bundle / "bootstrap_workdir"
            (bundle / "deployconf_testbed.local.yaml").write_text(
                "\n".join(
                    [
                        f"gen_k8s_daemonset_mirror_outdir: {bundle / 'gen_k8s_daemonset'}",
                        "global_envs:",
                        "  FLUXON_CLUSTER_NAME: fluxon_testbed",
                        "  FLUXON_SHARED_MEM: ${HOSTWORKDIR}/shm",
                        "cluster_nodes:",
                        "  - hostname: runner-a",
                        "    ip: 10.9.0.7",
                        "    hostworkdir: /tmp/runner/a",
                        "    execution_mode: local",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (bundle / "start_test_bed.runner.yaml").write_text(
                "\n".join(
                    [
                        "schema_version: 6",
                        f"deployconf_path: {source_deployconf}",
                        "controller_url: http://10.9.0.7:19080/r/ops/fluxon_testbed",
                        "controller_basic_auth:",
                        "  username: ops_admin",
                        "  password: ops_password",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (bundle / "manifest.json").write_text(
                json.dumps(
                    {
                        "deployconf_path": str(source_deployconf),
                        "start_config_path": str(source_start),
                        "ssh_config_path": str(source_ssh_config),
                        "workdir": str(source_bootstrap_workdir),
                        "bootstrap_mode": "apply_only",
                        "controller_request_mode": "direct",
                    }
                ),
                encoding="utf-8",
            )
            workdir = root / "run"
            suite_out = root / "suite.yaml"
            calls: list[tuple[list[str], dict[str, str] | None]] = []

            def fake_call(cmd, *, env=None):
                calls.append((list(cmd), None if env is None else dict(env)))
                return 0

            with mock.patch.object(entry, "call", side_effect=fake_call):
                with mock.patch.object(entry, "_local_busy_tcp_ports", return_value=set()):
                    with mock.patch.dict("os.environ", {"FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT": "/tmp/release"}, clear=True):
                        with mock.patch.object(
                            sys,
                            "argv",
                            [
                                str(MODULE_PATH),
                                "--single-host-logical-targets",
                                "--testbed-bundle-source",
                                str(bundle),
                                "--workdir",
                                str(workdir),
                                "--suite-out",
                                str(suite_out),
                                "--owner-count",
                                "4",
                                "--owner-dram-gib",
                                "1",
                                "--producer-count",
                                "8",
                                "--consumer-count",
                                "8",
                                "--duration-seconds",
                                "30",
                                "--value-size",
                                "256",
                                "--op-timeout-seconds",
                                "5",
                                "--cluster-ready-timeout-seconds",
                                "1800",
                                "--consumer-sim-min-ms",
                                "1",
                                "--consumer-sim-max-ms",
                                "1",
                            ],
                        ):
                            rc = entry.main()

            self.assertEqual(rc, 0)
            self.assertEqual(len(calls), 1)
            run_local_start = workdir / "testbed_bundle" / "start_test_bed.runner.yaml"
            self.assertEqual(
                calls[0][1]["FLUXON_TEST_STACK_START_TEST_BED_CONFIG"],
                str(run_local_start.resolve()),
            )
            self.assertEqual(calls[0][1]["FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"], "/tmp/release")
            self.assertEqual(calls[0][0][0:3], [sys.executable, "-u", str(RUNNER_PATH)])
            self.assertIn("--action", calls[0][0])
            self.assertIn("run", calls[0][0])
            run_local_deployconf = workdir / "testbed_bundle" / "deployconf_testbed.local.yaml"
            run_local_mirror = workdir / "testbed_bundle" / "gen_k8s_daemonset"
            run_local_start_payload = yaml.safe_load(run_local_start.read_text(encoding="utf-8"))
            self.assertEqual(run_local_start_payload["deployconf_path"], "./deployconf_testbed.local.yaml")
            run_local_deployconf_payload = yaml.safe_load(run_local_deployconf.read_text(encoding="utf-8"))
            self.assertEqual(
                run_local_deployconf_payload["gen_k8s_daemonset_mirror_outdir"],
                str(run_local_mirror.resolve()),
            )
            run_local_manifest = json.loads((workdir / "testbed_bundle" / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(run_local_manifest["deployconf_path"], "deployconf_testbed.local.yaml")
            self.assertEqual(run_local_manifest["start_config_path"], "start_test_bed.runner.yaml")
            self.assertEqual(run_local_manifest["ssh_config_path"], "ssh_config")
            self.assertEqual(run_local_manifest["workdir"], "bootstrap_workdir")
            suite = yaml.safe_load(suite_out.read_text(encoding="utf-8"))
            scale = suite["scales"]["largescale_mq_n4owner_1gib_p8_c8"]
            self.assertEqual(scale["topology"], 4)
            self.assertEqual(scale["benchmark"]["processes_per_target"], 4)
            self.assertEqual(scale["benchmark"]["owner_group_processes"], 1)
            target_map = suite["profiles"]["fluxon_tcp_thread"]["runtime"]["test_stack"]["deploy"]["target_ip_map"]
            self.assertEqual(target_map["node-1"], "10.9.0.7")
            self.assertEqual(target_map["node-4"], "10.9.0.7")
            port_entry = suite["profiles"]["fluxon_tcp_thread"]["runtime"]["test_stack"]["port_alloc"][
                "by_topology"
            ][4]
            self.assertEqual(port_entry["coordinator_port_base"], 20480)
            self.assertGreaterEqual(port_entry["kv_master_port_base"], entry.LOCAL_TEST_STACK_P2P_PORT_MIN)
            self.assertNotEqual(port_entry["kv_master_port_base"], 50161)
            self.assertGreaterEqual(port_entry["kv_p2p_port_base"], entry.LOCAL_TEST_STACK_P2P_PORT_MIN)
            self.assertNotEqual(port_entry["kv_p2p_port_base"], 11666)

    def test_real_run_relocates_generated_bundle_mirror_after_bundle_move(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            previous_bundle = root / "previous_runner" / "testbed_bundle"
            source_bundle = root / "current_runner" / "testbed_bundle"
            previous_bundle.mkdir(parents=True)
            source_bundle.mkdir(parents=True)
            (source_bundle / "bootstrap_workdir").mkdir()
            (source_bundle / "ssh_config").write_text("# local\n", encoding="utf-8")
            (source_bundle / "gen_k8s_daemonset").mkdir()
            source_deployconf = source_bundle / "deployconf_testbed.local.yaml"
            source_start = source_bundle / "start_test_bed.runner.yaml"
            source_deployconf.write_text(
                "\n".join(
                    [
                        f"gen_k8s_daemonset_mirror_outdir: {previous_bundle / 'gen_k8s_daemonset'}",
                        "global_envs:",
                        "  FLUXON_CLUSTER_NAME: fluxon_testbed",
                        "  FLUXON_SHARED_MEM: ${HOSTWORKDIR}/shm",
                        "cluster_nodes:",
                        "  - hostname: runner-a",
                        "    ip: 10.9.0.8",
                        "    hostworkdir: /tmp/runner/a",
                        "    execution_mode: local",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            source_start.write_text(
                "\n".join(
                    [
                        "schema_version: 6",
                        "deployconf_path: ./deployconf_testbed.local.yaml",
                        "controller_url: http://10.9.0.8:19080/r/ops/fluxon_testbed",
                        "controller_basic_auth:",
                        "  username: ops_admin",
                        "  password: ops_password",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (source_bundle / "manifest.json").write_text(
                json.dumps(
                    {
                        "deployconf_path": "deployconf_testbed.local.yaml",
                        "start_config_path": "start_test_bed.runner.yaml",
                        "ssh_config_path": "ssh_config",
                        "workdir": "bootstrap_workdir",
                    }
                ),
                encoding="utf-8",
            )
            workdir = root / "run"
            suite_out = root / "suite.yaml"

            with mock.patch.object(entry, "call", return_value=0):
                with mock.patch.object(
                    sys,
                    "argv",
                    [
                        str(MODULE_PATH),
                        "--single-host-logical-targets",
                        "--testbed-bundle-source",
                        str(source_bundle),
                        "--workdir",
                        str(workdir),
                        "--suite-out",
                        str(suite_out),
                        "--owner-count",
                        "4",
                        "--owner-dram-gib",
                        "1",
                        "--producer-count",
                        "8",
                        "--consumer-count",
                        "8",
                    ],
                ):
                    rc = entry.main()

            self.assertEqual(rc, 0)
            run_local_mirror = workdir / "testbed_bundle" / "gen_k8s_daemonset"
            run_local_deployconf = workdir / "testbed_bundle" / "deployconf_testbed.local.yaml"
            run_local_deployconf_payload = yaml.safe_load(run_local_deployconf.read_text(encoding="utf-8"))
            self.assertEqual(
                run_local_deployconf_payload["gen_k8s_daemonset_mirror_outdir"],
                str(run_local_mirror.resolve()),
            )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
