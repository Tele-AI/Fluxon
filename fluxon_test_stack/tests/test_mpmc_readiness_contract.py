#!/usr/bin/env python3

from __future__ import annotations

import threading
import time
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fluxon_test_stack.mpmc_readiness import evaluate_mpmc_topology_ready

try:
    import fluxon_test_stack.distributed_benchmark_node as node_mod
    from fluxon_test_stack.distributed_benchmark_node import (
        BenchmarkNode,
        PreparedWorkerRuntime,
        TestMode,
    )
except ImportError as exc:
    node_mod = None
    NODE_RUNTIME_IMPORT_ERROR = exc
    BenchmarkNode = object  # type: ignore[assignment]
    PreparedWorkerRuntime = object  # type: ignore[assignment]
    TestMode = None  # type: ignore[assignment]
else:
    NODE_RUNTIME_IMPORT_ERROR = None


def _new_coordinator_with_temp_config():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "benchmark_config.py").write_text(
            "\n".join(
                [
                    "CONFIG = {",
                    "    'benchmark': {",
                    "        'mode': 'MPMC',",
                    "        'threads_per_process': 1,",
                    "        'max_benchmark_seconds': 30,",
                    "        'cluster_ready_timeout_seconds': 5,",
                    "        'metric_warmup_seconds': 0,",
                    "        'op_timeout_seconds': 5,",
                    "        'value_size': 256,",
                    "        'node_roles': ['producer', 'consumer'],",
                    "    },",
                    "    'kv_base': {},",
                    "    'mq_base': {'capacity': 100, 'ttl_seconds': 60},",
                    "    'mq_new_or_bind_unique_key': 'test-mpmc',",
                    "    'coordinator': {'host': '127.0.0.1', 'port': 7777},",
                    "    'node_overrides': [",
                    "        {'kv': {'instance_key': 'producer_0'}, 'mq_role': 'producer', 'mq': {'weight': 1}},",
                    "        {'kv': {'instance_key': 'consumer_0'}, 'mq_role': 'consumer', 'mq': {'weight': 1}},",
                    "    ],",
                    "    'output': {'result_path': 'benchmark_result.json'},",
                    "}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        old_cwd = Path.cwd()
        old_module = sys.modules.pop("fluxon_test_stack.distributed_benchmark_coordinator", None)
        try:
            os.chdir(root)
            import fluxon_test_stack.distributed_benchmark_coordinator as coordinator_mod
            return coordinator_mod.CoordinatorServer("127.0.0.1", 0)
        finally:
            os.chdir(old_cwd)
            sys.modules.pop("fluxon_test_stack.distributed_benchmark_coordinator", None)
            if old_module is not None:
                sys.modules["fluxon_test_stack.distributed_benchmark_coordinator"] = old_module


class TestMPMCReadinessContract(unittest.TestCase):
    def test_consumer_does_not_wait_for_ready_channels_before_reporting_ready(self) -> None:
        readiness = evaluate_mpmc_topology_ready(
            role="consumer",
            expected_workers=1,
            total_mpsc_channels=1,
            ready_channels=0,
            active_consumers=1,
        )

        self.assertTrue(readiness.ready)

    def test_producer_still_waits_for_ready_channels_and_active_consumers(self) -> None:
        no_ready_channel = evaluate_mpmc_topology_ready(
            role="producer",
            expected_workers=1,
            total_mpsc_channels=1,
            ready_channels=0,
            active_consumers=1,
        )
        no_consumer = evaluate_mpmc_topology_ready(
            role="producer",
            expected_workers=1,
            total_mpsc_channels=1,
            ready_channels=1,
            active_consumers=0,
        )

        self.assertFalse(no_ready_channel.ready)
        self.assertIn("ready_channels", no_ready_channel.reason)
        self.assertFalse(no_consumer.ready)
        self.assertIn("active_consumers", no_consumer.reason)

    def test_coordinator_runtime_ready_gate_waits_for_all_nodes(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        coordinator.expected_nodes = 2
        coordinator.registered_nodes = {
            "node-a": {"node_role": "producer"},
            "node-b": {"node_role": "consumer"},
        }
        sent = []

        def capture_response(_sock, response):
            sent.append(response)
            return True

        with mock.patch.object(coordinator, "_send_tcp_response", side_effect=capture_response):
            self.assertTrue(coordinator.handle_runtime_ready({"node_id": "node-a"}, object()))
            self.assertEqual(sent[-1]["status"], "waiting")
            self.assertEqual(sent[-1]["runtime_ready_count"], 1)
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a"])

            self.assertTrue(coordinator.handle_runtime_start_request({"node_id": "node-a"}, object()))
            self.assertEqual(sent[-1]["status"], "waiting")
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a"])

            self.assertTrue(coordinator.handle_runtime_ready({"node_id": "node-b"}, object()))
            self.assertEqual(sent[-1]["status"], "success")
            self.assertEqual(sent[-1]["runtime_ready_count"], 2)
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a", "node-b"])

            self.assertTrue(coordinator.handle_runtime_start_request({"node_id": "node-a"}, object()))
            self.assertEqual(sent[-1]["status"], "success")
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a", "node-b"])

    def test_coordinator_runtime_start_rejects_unregistered_nodes(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        coordinator.expected_nodes = 1
        coordinator.registered_nodes = {}
        sent = []

        def capture_response(_sock, response):
            sent.append(response)
            return True

        with mock.patch.object(coordinator, "_send_tcp_response", side_effect=capture_response):
            self.assertTrue(coordinator.handle_runtime_start_request({"node_id": "node-x"}, object()))

        self.assertEqual(sent[-1]["status"], "error")
        self.assertIn("unregistered node", sent[-1]["error"])

    def test_distributed_node_waits_for_runtime_start_before_mpmc_timed_window(self) -> None:
        node_source = Path("fluxon_test_stack/distributed_benchmark_node.py").read_text(encoding="utf-8")
        self.assertIn("RUNTIME_READY = \"runtime_ready\"", node_source)
        self.assertIn("RUNTIME_START = \"runtime_start\"", node_source)
        wait_call = node_source.index("if not self._wait_for_mpmc_runtime_start():")
        start_time_set = node_source.index("self.start_time = time.time()", wait_call)
        start_event_set = node_source.index("round_state.start_event.set()", start_time_set)
        self.assertLess(wait_call, start_time_set)
        self.assertLess(start_time_set, start_event_set)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_prewarm_before_ready_is_nonblocking(self) -> None:
        class ProducerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.prepare_started = threading.Event()
                self.allow_prepare = threading.Event()

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                self.prepare_started.set()
                self.allow_prepare.wait(timeout=2.0)
                return PreparedWorkerRuntime(producer=object())

            def _run_worker_thread(self, *args, **kwargs):
                return []

            def _wait_for_mpmc_runtime_start(self) -> bool:
                return True

        node = ProducerNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
        }

        started_at = time.monotonic()
        node._prepare_mpmc_round_before_ready(workers=1)
        elapsed_s = time.monotonic() - started_at

        self.assertLess(elapsed_s, 0.5)
        self.assertIsNotNone(node._prepared_mpmc_round)
        self.assertFalse(node.prepare_started.is_set())
        self.assertEqual(node._prepared_mpmc_round.prepared_runtimes, {})

        node.allow_prepare.set()
        node._run_mpmc_workers(workers=1, deadline_ts=0.0)
        self.assertTrue(node.prepare_started.is_set())

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_consumer_prewarm_before_ready_still_waits_for_endpoint(self) -> None:
        class ConsumerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.waited_cluster_ready = False

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                return PreparedWorkerRuntime(consumer=object())

            def _wait_mpmc_cluster_ready(self, **kwargs) -> None:
                self.waited_cluster_ready = True

            def _run_worker_thread(self, *args, **kwargs):
                return []

            def _wait_for_mpmc_runtime_start(self) -> bool:
                return True

        node = ConsumerNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
        }

        node._prepare_mpmc_round_before_ready(workers=1)

        self.assertIsNotNone(node._prepared_mpmc_round)
        self.assertEqual(len(node._prepared_mpmc_round.prepared_runtimes), 1)
        self.assertTrue(node.waited_cluster_ready)
        node._prepared_mpmc_round.start_event.set()
        for thread in node._prepared_mpmc_round.pending_threads.values():
            thread.join(timeout=2.0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_runtime_start_gate_runs_before_timed_workers(self) -> None:
        class GatedProducerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.runtime_start_checked = False

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                return PreparedWorkerRuntime(producer=object())

            def _wait_for_mpmc_runtime_start(self) -> bool:
                self.runtime_start_checked = True
                return False

            def _run_worker_thread(self, *args, **kwargs):
                return []

        node = GatedProducerNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
            "value_size": 256,
        }

        node._prepare_mpmc_round_before_ready(workers=1)
        node._run_mpmc_workers(workers=1, deadline_ts=0.0)

        self.assertTrue(node.runtime_start_checked)
        self.assertIsNotNone(node._forced_benchmark_result)
        self.assertEqual(node._forced_benchmark_result["reason"], "mpmc_runtime_start_timeout")

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_stagger_only_applies_to_large_mpmc_runs(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "bench_mq__producer_7_proc_3"
        node.test_config = {
            "test_mode": TestMode.MPMC.value,
            "expected_nodes": 16,
        }
        self.assertEqual(node._runtime_init_stagger_seconds(), 0.0)

        node.test_config["expected_nodes"] = 64
        stagger_s = node._runtime_init_stagger_seconds()
        self.assertGreaterEqual(stagger_s, 0.0)
        self.assertLessEqual(stagger_s, 24.0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_kv_store_init_retries_transient_errors(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
        }
        node._runtime_init_retry_sleep_seconds = lambda attempt: 0.0  # type: ignore[method-assign]
        calls = []
        sentinel_store = object()
        original_init_kv_store = node_mod.init_kv_store

        def fake_init_kv_store(config):
            calls.append(config)
            if len(calls) == 1:
                return None, "Failed to connect to etcd: status probe timed out after 10s"
            return sentinel_store, None

        node_mod.init_kv_store = fake_init_kv_store
        try:
            store, err = node._init_kv_store_with_ready_retry({"backend_kind": "FLUXON"})
        finally:
            node_mod.init_kv_store = original_init_kv_store

        self.assertIs(store, sentinel_store)
        self.assertIsNone(err)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
