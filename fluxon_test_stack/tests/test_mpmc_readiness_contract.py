#!/usr/bin/env python3

from __future__ import annotations

import threading
import time
import unittest

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
