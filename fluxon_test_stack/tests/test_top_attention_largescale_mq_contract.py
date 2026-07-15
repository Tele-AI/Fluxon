#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
INDEX_DIR = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"
MODULE_PATH = INDEX_DIR / "_largescale_mq.py"


def _load_module():
    sys.path.insert(0, str(INDEX_DIR))
    try:
        spec = importlib.util.spec_from_file_location(
            "fluxon_test_stack_top_attention_largescale_mq",
            MODULE_PATH,
        )
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(INDEX_DIR):
            sys.path.pop(0)


def _args(**overrides):
    values = {
        "python": sys.executable,
        "release_dir": str(REPO_ROOT / "fluxon_release"),
        "workdir": "",
        "action": "run",
        "plan_only": True,
        "owner_count": 4,
        "owner_dram_gib": 1,
        "producer_count": 160,
        "consumer_count": 8,
        "threads_per_process": 1,
        "duration_seconds": 90,
        "metric_warmup_seconds": 60,
        "value_size": 256,
        "op_timeout_seconds": 5,
        "cluster_ready_timeout_seconds": 1800,
        "consumer_sim_min_ms": 1,
        "consumer_sim_max_ms": 1,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _success_result(expected_nodes: int) -> dict:
    return {
        "runs": [
            {
                "completed": True,
                "total_ops": 100,
                "total_successful_ops": 100,
                "total_failed_ops": 0,
                "completion": {
                    "status": "SUCCESS",
                    "expected_nodes": expected_nodes,
                    "registered_node_count": expected_nodes,
                    "ready_node_count": expected_nodes,
                    "runtime_ready_node_count": expected_nodes,
                    "reported_result_node_count": expected_nodes,
                    "pending_result_node_count": 0,
                    "completion_error": None,
                },
            }
        ]
    }


class TestTopAttentionLargescaleMqContract(unittest.TestCase):
    def test_entrypoint_is_bare_local_and_has_no_testbed_runner_surface(self) -> None:
        entry = _load_module()
        source = MODULE_PATH.read_text(encoding="utf-8")

        self.assertEqual(entry.TEST_REQUIREMENTS, ["fluxon-release"])
        self.assertIn('execution_model": "bare_local_processes"', source)
        self.assertNotIn("--testbed-bundle-source", source)
        self.assertNotIn("start_test_bed.py", source)
        self.assertNotIn("test_runner.py", source)
        self.assertNotIn("ci_2_virt_node.py", source)
        self.assertNotIn("mpmc_active_producer_runtime_limit", source)

    def test_port_plan_reserves_one_port_for_every_real_process_runtime(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            plan = entry._allocate_port_plan(
                workdir=Path(td),
                owner_count=4,
                worker_count=168,
                busy_ports=set(),
            )

        all_ports = [
            plan.etcd_client,
            plan.etcd_peer,
            plan.greptime_http,
            plan.master,
            plan.coordinator,
            *plan.owners,
            *plan.workers,
        ]
        self.assertEqual(len(plan.owners), 4)
        self.assertEqual(len(plan.workers), 168)
        self.assertEqual(len(all_ports), len(set(all_ports)))
        self.assertGreaterEqual(min(all_ports), entry.PORT_MIN)
        self.assertLessEqual(max(all_ports), entry.PORT_MAX)

    def test_port_allocator_skips_a_busy_contiguous_block(self) -> None:
        entry = _load_module()
        self.assertEqual(
            entry._find_tcp_port_block(
                preferred_start=20000,
                required_count=4,
                busy_ports={20000, 20001, 20002, 20003},
            ),
            20004,
        )

    def test_runtime_config_materializes_all_160_producers_and_8_consumers(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td) / "run"
            ports = entry._allocate_port_plan(
                workdir=workdir,
                owner_count=4,
                worker_count=168,
                busy_ports=set(),
            )
            plan, benchmark, master, owners = entry._build_runtime_artifacts(
                args=_args(workdir=str(workdir)),
                workdir=workdir,
                ports=ports,
                host_ips=["10.0.0.10", "127.0.0.1"],
            )

        self.assertEqual(plan["execution_model"], "bare_local_processes")
        self.assertFalse(plan["uses_testbed"])
        self.assertEqual(plan["topology"]["owner_count"], 4)
        self.assertEqual(plan["topology"]["producer_count"], 160)
        self.assertEqual(plan["topology"]["consumer_count"], 8)
        self.assertEqual(plan["topology"]["worker_count"], 168)
        self.assertEqual(len(plan["workers"]), 168)
        self.assertEqual(len(owners), 4)
        self.assertEqual(master["network"]["subnet_whitelist"], ["10.0.0.10/32", "127.0.0.1/32"])
        self.assertIn("monitoring", master)

        roles = benchmark["benchmark"]["node_roles"]
        self.assertEqual(roles.count("producer"), 160)
        self.assertEqual(roles.count("consumer"), 8)
        self.assertEqual(len(benchmark["node_overrides"]), 168)
        self.assertNotIn("mpmc_active_producer_runtime_limit", benchmark["benchmark"])
        self.assertEqual(
            {worker["owner_index"] for worker in plan["workers"]},
            {0, 1, 2, 3},
        )
        self.assertEqual(
            benchmark["kv_base"]["contribute_to_cluster_pool_size"]["dram"],
            0,
        )

    def test_plan_only_writes_direct_runtime_artifacts_without_starting_processes(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td) / "bare-run"
            argv = [
                str(MODULE_PATH),
                "--plan-only",
                "--workdir",
                str(workdir),
                "--owner-count",
                "2",
                "--producer-count",
                "4",
                "--consumer-count",
                "2",
                "--duration-seconds",
                "31",
                "--metric-warmup-seconds",
                "1",
            ]
            with mock.patch.object(sys, "argv", argv):
                with mock.patch.object(
                    entry.subprocess,
                    "Popen",
                    side_effect=AssertionError("plan-only must not start a process"),
                ):
                    self.assertEqual(entry.main(), 0)

            plan = json.loads((workdir / "run_plan.json").read_text(encoding="utf-8"))
            self.assertEqual(plan["execution_model"], "bare_local_processes")
            self.assertEqual(plan["topology"]["worker_count"], 6)
            self.assertTrue((workdir / "benchmark_config.py").is_file())
            self.assertTrue((workdir / "configs" / "master.yaml").is_file())
            self.assertTrue((workdir / "configs" / "owner_0.yaml").is_file())
            self.assertTrue((workdir / "configs" / "owner_1.yaml").is_file())
            self.assertTrue(
                (workdir / "runtime" / "fluxon_test_stack" / "distributed_benchmark_node.py").is_file()
            )

    def test_result_contract_requires_every_worker_at_every_gate(self) -> None:
        entry = _load_module()
        result = _success_result(168)
        entry._validate_benchmark_result(result, expected_nodes=168)

        result["runs"][0]["completion"]["runtime_ready_node_count"] = 167
        with self.assertRaisesRegex(ValueError, "did not complete on every worker"):
            entry._validate_benchmark_result(result, expected_nodes=168)

    def test_large_runtime_cleanup_preserves_text_diagnostics(self) -> None:
        entry = _load_module()
        with tempfile.TemporaryDirectory() as td:
            workdir = Path(td)
            bundle = workdir / "services" / "share_mem" / "owner_0" / "cluster"
            bundle.mkdir(parents=True)
            (bundle / "mmap.file").write_bytes(b"large")
            (bundle / "shared.json").write_text("{}\n", encoding="utf-8")
            service_log = workdir / "services" / "master" / "log" / "master_core.log"
            service_log.parent.mkdir(parents=True)
            service_log.write_text("diagnostic\n", encoding="utf-8")
            for data_root in (
                workdir / "services" / "etcd" / "data",
                workdir / "services" / "greptime" / "data",
                workdir / "services" / "owner_0" / "large",
            ):
                data_root.mkdir(parents=True)
                (data_root / "payload").write_bytes(b"data")

            entry._remove_large_runtime_data(workdir)

            self.assertFalse((bundle / "mmap.file").exists())
            self.assertTrue((bundle / "shared.json").is_file())
            self.assertTrue(service_log.is_file())
            self.assertFalse((workdir / "services" / "etcd" / "data").exists())
            self.assertFalse((workdir / "services" / "greptime" / "data").exists())
            self.assertFalse((workdir / "services" / "owner_0" / "large").exists())

    def test_argument_contract_rejects_less_than_thirty_effective_seconds(self) -> None:
        entry = _load_module()
        with self.assertRaisesRegex(ValueError, "at least 30"):
            entry._validate_args(
                _args(duration_seconds=60, metric_warmup_seconds=31)
            )


if __name__ == "__main__":
    unittest.main()
