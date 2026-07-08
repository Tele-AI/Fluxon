#!/usr/bin/env python3

from __future__ import annotations

import threading
import time
import ast
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
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
    def test_mpsc_producer_serializes_pyo3_handle_access(self) -> None:
        source = Path("fluxon_py/_api_ext_chan/mpsc.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        producer_class = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "MPSCChanProducer"
        )

        def method(name: str) -> ast.FunctionDef:
            return next(
                node
                for node in producer_class.body
                if isinstance(node, ast.FunctionDef) and node.name == name
            )

        def is_self_handle_lock(expr: ast.AST) -> bool:
            return (
                isinstance(expr, ast.Attribute)
                and expr.attr == "_handle_lock"
                and isinstance(expr.value, ast.Name)
                and expr.value.id == "self"
            )

        def is_self_handle_call(call: ast.Call, attr: str) -> bool:
            return (
                isinstance(call.func, ast.Attribute)
                and call.func.attr == attr
                and isinstance(call.func.value, ast.Attribute)
                and call.func.value.attr == "_handle"
                and isinstance(call.func.value.value, ast.Name)
                and call.func.value.value.id == "self"
            )

        init_func = method("__init__")
        lock_assigns = [
            node
            for node in ast.walk(init_func)
            if isinstance(node, ast.Assign)
            and any(is_self_handle_lock(target) for target in node.targets)
        ]
        self.assertEqual(len(lock_assigns), 1)

        locked_calls = {
            "put_data": "put_flat_dict_ptrs",
            "record_nonblocking_put_success": "record_nonblocking_put_success",
            "record_blocking_put_observed": "record_blocking_put_observed",
        }
        for method_name, handle_attr in locked_calls.items():
            fn = method(method_name)
            with_nodes = [
                node
                for node in ast.walk(fn)
                if isinstance(node, ast.With)
                and any(is_self_handle_lock(item.context_expr) for item in node.items)
            ]
            self.assertTrue(with_nodes, f"{method_name} must hold self._handle_lock")
            self.assertTrue(
                any(
                    is_self_handle_call(call, handle_attr)
                    for with_node in with_nodes
                    for call in ast.walk(with_node)
                    if isinstance(call, ast.Call)
                ),
                f"{method_name} must call self._handle.{handle_attr} under self._handle_lock",
            )

    def test_existing_mpmc_attach_does_not_register_shared_keepalives(self) -> None:
        source = Path("fluxon_py/_api_ext_chan/mpmc.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        mpmc_class = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "MPMCChannel"
        )
        init_func = next(
            node
            for node in mpmc_class.body
            if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        )

        shared_guard = next(
            node
            for node in ast.walk(init_func)
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "keep_shared_mpmc_leases"
        )
        guarded_calls = {
            call.func.id
            for call in ast.walk(shared_guard)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
        }

        self.assertIn("_setup_global_lease_keepalive", guarded_calls)
        self.assertIn("_setup_payload_lease_keepalive", guarded_calls)
        self.assertIn("_setup_id_allocator_cluster_keepalive", guarded_calls)

        def factory_keep_shared_arg(factory_name: str) -> bool:
            factory = next(
                node
                for node in mpmc_class.body
                if isinstance(node, ast.FunctionDef) and node.name == factory_name
            )
            channel_call = next(
                call
                for call in ast.walk(factory)
                if isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id == "MPMCChannel"
            )
            keep_shared_arg = channel_call.args[-1]
            self.assertIsInstance(keep_shared_arg, ast.Constant)
            return bool(keep_shared_arg.value)

        self.assertTrue(factory_keep_shared_arg("new_global_mpmc_channel"))
        self.assertFalse(factory_keep_shared_arg("new_existed_global_mpmc_channel"))

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
    def test_worker_owned_kvcache_config_sets_per_worker_identity_and_port(self) -> None:
        base_config = {
            "instance_key": "producer_0",
            "fluxonkv_spec": {
                "cluster_name": "bench",
                "p2p_listen_port": 11826,
            },
        }

        worker_config = BenchmarkNode._worker_owned_kvcache_config(base_config, thread_id=3)

        self.assertEqual(worker_config["instance_key"], "producer_0__worker_3")
        self.assertEqual(base_config["instance_key"], "producer_0")
        self.assertEqual(worker_config["fluxonkv_spec"]["p2p_listen_port"], 11829)
        self.assertEqual(base_config["fluxonkv_spec"]["p2p_listen_port"], 11826)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_worker_owned_kvcache_config_requires_instance_key(self) -> None:
        base_config = {
            "fluxonkv_spec": {
                "cluster_name": "bench",
                "p2p_listen_port": 11826,
            },
        }

        with self.assertRaisesRegex(ValueError, "instance_key"):
            BenchmarkNode._worker_owned_kvcache_config(base_config, thread_id=0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_consumer_get_retries_no_message_without_failed_sample(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
        }
        outcomes = [
            SimpleNamespace(
                status=node_mod.MQGetStatus.NO_MESSAGE,
                ok=False,
                error_msg="no message",
                data_size=0,
            ),
            SimpleNamespace(
                status=node_mod.MQGetStatus.NO_MESSAGE,
                ok=False,
                error_msg="no message",
                data_size=0,
            ),
            SimpleNamespace(
                status=node_mod.MQGetStatus.DATA,
                ok=True,
                error_msg="",
                data_size=256,
            ),
        ]
        sleeps = []

        with mock.patch.object(node_mod, "mq_get_once", side_effect=outcomes):
            with mock.patch.object(node_mod.time, "sleep", side_effect=lambda seconds: sleeps.append(seconds)):
                result = node._execute_chan_get_operation(
                    object(),
                    inflight_at_start=1,
                    deadline_ts=time.time() + 30.0,
                )

        self.assertTrue(result.success)
        self.assertEqual(result.operation_type, "MPMC_GET")
        self.assertEqual(result.data_size, 256)
        self.assertEqual(sleeps, [node_mod.MPMC_NO_MESSAGE_RETRY_SLEEP_SECONDS] * 2)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_consumer_get_stops_on_no_message_after_deadline(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
        }
        no_message = SimpleNamespace(
            status=node_mod.MQGetStatus.NO_MESSAGE,
            ok=False,
            error_msg="no message",
            data_size=0,
        )

        with mock.patch.object(node_mod, "mq_get_once", return_value=no_message):
            with self.assertRaises(node_mod.BenchmarkWorkerStop):
                node._execute_chan_get_operation(
                    object(),
                    inflight_at_start=1,
                    deadline_ts=time.time() - 1.0,
                )

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_stop_intent_requests_shutdown_before_close(self) -> None:
        calls = []

        class FakeCloseResult:
            def is_ok(self) -> bool:
                return True

            def unwrap(self):
                return None

        class FakeEndpoint:
            def request_shutdown(self) -> None:
                calls.append("request_shutdown")

            def close(self) -> FakeCloseResult:
                calls.append("close")
                return FakeCloseResult()

        endpoint = FakeEndpoint()
        round_state = node_mod.PreparedMPMCRound(
            prepared_runtimes={
                0: PreparedWorkerRuntime(producer=endpoint),
                1: PreparedWorkerRuntime(producer=endpoint),
            }
        )
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
        }

        node._request_shutdown_prepared_mpmc_endpoints_for_stop_intent(round_state=round_state)
        node._close_prepared_mpmc_endpoints_for_stop_intent(round_state=round_state)

        self.assertEqual(calls, ["request_shutdown", "close"])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_worker_owned_kvcache_init_is_parallel_per_process(self) -> None:
        class FakeStore:
            pass

        with tempfile.TemporaryDirectory() as td:
            node = BenchmarkNode()
            node.test_config = {
                "node_role": "producer",
                "test_mode": TestMode.MPMC.value,
                "cluster_ready_timeout_seconds": 5,
                "max_benchmark_seconds": 30,
                "kvcache_config": {
                    "instance_key": "producer_0",
                    "fluxonkv_spec": {
                        "cluster_name": "bench",
                        "share_mem_path": str(Path(td) / "shm1" / "node-1"),
                        "p2p_listen_port": 11826,
                    },
                },
            }
            node.mq_unique_id = "mpmc-test"

            entered = threading.Barrier(3)
            active_lock = threading.Lock()
            active_count = 0
            max_active_count = 0

            def fake_init_kv_store(_config):
                nonlocal active_count, max_active_count
                with active_lock:
                    active_count += 1
                    max_active_count = max(max_active_count, active_count)
                time.sleep(0.05)
                with active_lock:
                    active_count -= 1
                return FakeStore(), None

            def fake_init_mq_channel(*, role, kv_store, chan_config, unique_id, weight):
                return object(), None, None

            errors = []

            def worker(thread_id: int) -> None:
                entered.wait(timeout=2.0)
                try:
                    node._prepare_mpmc_worker_runtime(thread_id=thread_id)
                except Exception as exc:
                    errors.append(exc)

            with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fake_init_kv_store):
                with mock.patch.object(node_mod, "init_mq_channel", side_effect=fake_init_mq_channel):
                    threads = [
                        threading.Thread(target=worker, args=(thread_id,))
                        for thread_id in (0, 1)
                    ]
                    for thread in threads:
                        thread.start()
                    entered.wait(timeout=2.0)
                    for thread in threads:
                        thread.join(timeout=2.0)

            self.assertEqual(errors, [])
            self.assertEqual(max_active_count, 2)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_worker_owned_kvcache_stagger_runs_once_per_process(self) -> None:
        class FakeStore:
            pass

        with tempfile.TemporaryDirectory() as td:
            node = BenchmarkNode()
            node.test_config = {
                "node_role": "producer",
                "test_mode": TestMode.MPMC.value,
                "cluster_ready_timeout_seconds": 5,
                "max_benchmark_seconds": 30,
                "kvcache_config": {
                    "instance_key": "producer_0",
                    "fluxonkv_spec": {
                        "cluster_name": "bench",
                        "share_mem_path": str(Path(td) / "shm1" / "node-1"),
                        "p2p_listen_port": 11826,
                    },
                },
            }
            node.mq_unique_id = "mpmc-test"

            stagger_calls = []
            init_kv_configs = []
            init_mq_calls = []

            def fake_stagger(**kwargs):
                stagger_calls.append(kwargs)

            def fake_init_kv_store(config):
                init_kv_configs.append(config)
                return FakeStore(), None

            def fake_init_mq_channel(*, role, kv_store, chan_config, unique_id, weight):
                init_mq_calls.append(
                    {
                        "role": role,
                        "kv_store": kv_store,
                        "unique_id": unique_id,
                    }
                )
                return object(), None, None

            with mock.patch.object(node, "_sleep_for_runtime_init_stagger", side_effect=fake_stagger):
                with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fake_init_kv_store):
                    with mock.patch.object(node_mod, "init_mq_channel", side_effect=fake_init_mq_channel):
                        node._prepare_mpmc_worker_runtime(thread_id=0)
                        node._prepare_mpmc_worker_runtime(thread_id=1)

            self.assertEqual(len(stagger_calls), 1)
            self.assertEqual(len(init_kv_configs), 2)
            self.assertEqual(len(init_mq_calls), 2)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_producer_runtime_is_worker_owned_per_thread(self) -> None:
        class FakeStore:
            pass

        with tempfile.TemporaryDirectory() as td:
            node = BenchmarkNode()
            node.test_config = {
                "node_role": "producer",
                "test_mode": TestMode.MPMC.value,
                "cluster_ready_timeout_seconds": 5,
                "max_benchmark_seconds": 30,
                "kvcache_config": {
                    "instance_key": "producer_0",
                    "fluxonkv_spec": {
                        "cluster_name": "bench",
                        "share_mem_path": str(Path(td) / "shm1" / "node-1"),
                        "p2p_listen_port": 11826,
                    },
                },
            }
            node.mq_unique_id = "mpmc-test"

            init_kv_configs = []
            init_mq_calls = []
            stores = []
            producers = []

            def fake_init_kv_store(config):
                init_kv_configs.append(config)
                store = FakeStore()
                stores.append(store)
                return store, None

            def fake_init_mq_channel(*, role, kv_store, chan_config, unique_id, weight):
                producer = object()
                producers.append(producer)
                init_mq_calls.append(
                    {
                        "role": role,
                        "kv_store": kv_store,
                        "unique_id": unique_id,
                    }
                )
                return producer, None, None

            with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fake_init_kv_store):
                with mock.patch.object(node_mod, "init_mq_channel", side_effect=fake_init_mq_channel):
                    runtime_0 = node._prepare_mpmc_worker_runtime(thread_id=0)
                    runtime_1 = node._prepare_mpmc_worker_runtime(thread_id=1)

            self.assertEqual(len(init_kv_configs), 2)
            self.assertEqual(init_kv_configs[0]["instance_key"], "producer_0__worker_0")
            self.assertEqual(init_kv_configs[1]["instance_key"], "producer_0__worker_1")
            self.assertEqual(init_kv_configs[0]["fluxonkv_spec"]["p2p_listen_port"], 11826)
            self.assertEqual(init_kv_configs[1]["fluxonkv_spec"]["p2p_listen_port"], 11827)
            self.assertEqual(len(init_mq_calls), 2)
            self.assertIs(init_mq_calls[0]["kv_store"], stores[0])
            self.assertIs(init_mq_calls[1]["kv_store"], stores[1])
            self.assertIs(runtime_0.producer, producers[0])
            self.assertIs(runtime_1.producer, producers[1])
            self.assertIs(runtime_0.kv_store, stores[0])
            self.assertIs(runtime_1.kv_store, stores[1])
            self.assertIsNot(runtime_0.local_mq_state, runtime_1.local_mq_state)
            self.assertTrue(runtime_0.close_producer_on_exit)
            self.assertTrue(runtime_1.close_producer_on_exit)
            self.assertTrue(runtime_0.close_kv_store_on_exit)
            self.assertTrue(runtime_1.close_kv_store_on_exit)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_producer_endpoint_attach_is_not_serialized_across_node_instances(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            shm_root = Path(td) / "shm1"
            entered = threading.Barrier(3)
            endpoint_entered = threading.Barrier(2)
            active_lock = threading.Lock()
            active_count = 0
            max_active_count = 0
            runtimes = []
            errors = []

            class ProducerNode(BenchmarkNode):
                def __init__(self, *, instance_key: str, node_name: str) -> None:
                    super().__init__()
                    self.test_config = {
                        "node_role": "producer",
                        "test_mode": TestMode.MPMC.value,
                        "kvcache_config": {
                            "instance_key": instance_key,
                            "fluxonkv_spec": {
                                "cluster_name": "bench",
                                "share_mem_path": str(shm_root / node_name),
                            },
                        },
                    }
                    self.mq_unique_id = "mpmc-test"
                    self.mq_state = node_mod.MQState(role="producer", weight=1.0)
                    self.chan_config = {}
                    self.instance_key = instance_key
                    self.node_id = instance_key

                def _sleep_for_runtime_init_stagger(self, **kwargs) -> None:
                    return None

                def _init_kv_store_with_ready_retry(self, config):
                    return object(), None

                def _prepare_mpmc_endpoint_runtime_from_kv_store(self, **kwargs) -> PreparedWorkerRuntime:
                    nonlocal active_count, max_active_count
                    with active_lock:
                        active_count += 1
                        max_active_count = max(max_active_count, active_count)
                    endpoint_entered.wait(timeout=2.0)
                    time.sleep(0.01)
                    with active_lock:
                        active_count -= 1
                    return PreparedWorkerRuntime(
                        producer=object(),
                        kv_store=kwargs["worker_owned_kv_store"],
                    )

            nodes = [
                ProducerNode(instance_key="producer_0", node_name="node-1"),
                ProducerNode(instance_key="producer_1", node_name="node-2"),
            ]

            def worker(node: ProducerNode) -> None:
                entered.wait(timeout=2.0)
                try:
                    runtimes.append(node._prepare_mpmc_worker_runtime(thread_id=0))
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(node,)) for node in nodes]
            for thread in threads:
                thread.start()
            entered.wait(timeout=2.0)
            for thread in threads:
                thread.join(timeout=2.0)

        self.assertEqual(errors, [])
        self.assertEqual(len(runtimes), 2)
        self.assertEqual(max_active_count, 2)
        for thread in threads:
            self.assertFalse(thread.is_alive())

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_endpoint_prepare_is_serialized_per_process(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "max_benchmark_seconds": 30,
        }
        node.kv_store = object()
        node.mq_unique_id = "mpmc-test"

        entered = threading.Barrier(3)
        active_lock = threading.Lock()
        active_count = 0
        max_active_count = 0

        def fake_init_mq_channel(*, role, kv_store, chan_config, unique_id, weight):
            nonlocal active_count, max_active_count
            with active_lock:
                active_count += 1
                max_active_count = max(max_active_count, active_count)
            time.sleep(0.05)
            with active_lock:
                active_count -= 1
            return None, object(), None

        errors = []

        def worker(thread_id: int) -> None:
            entered.wait(timeout=2.0)
            try:
                node._prepare_mpmc_worker_runtime(thread_id=thread_id)
            except Exception as exc:
                errors.append(exc)

        with mock.patch.object(node_mod, "init_mq_channel", side_effect=fake_init_mq_channel):
            threads = [
                threading.Thread(target=worker, args=(thread_id,))
                for thread_id in (0, 1)
            ]
            for thread in threads:
                thread.start()
            entered.wait(timeout=2.0)
            for thread in threads:
                thread.join(timeout=2.0)

        self.assertEqual(errors, [])
        self.assertEqual(max_active_count, 1)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_channel_lease_race_is_retryable_runtime_init_error(self) -> None:
        messages = [
            "Failed to create MPSC channel: failed to revoke etcd lease for lock",
            "Failed to get next available channel: ChanCreateError",
            'GRpcStatus(Status { code: NotFound, message: "etcdserver: requested lease not found" })',
            "ChanBindError(20004: Bind failed for chan_id=19)",
        ]

        for message in messages:
            with self.subTest(message=message):
                self.assertTrue(BenchmarkNode._is_retryable_runtime_init_error(message))

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_p2p_bind_failure_is_not_retryable_runtime_init_error(self) -> None:
        message = (
            "BackendInitFailedError(2002: Failed to initialize KV client: "
            "Failed to bind to configured p2p_listen_port=11739: "
            "P2p TCP bind failed: port=11739 (both v4 and v6 bind failed))"
        )

        self.assertFalse(BenchmarkNode._is_retryable_runtime_init_error(message))

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_worker_owned_kvcache_config_rejects_port_range_overflow(self) -> None:
        base_config = {
            "instance_key": "producer_0",
            "fluxonkv_spec": {
                "cluster_name": "bench",
                "p2p_listen_port": 65535,
            },
        }

        with self.assertRaisesRegex(ValueError, "worker-owned p2p_listen_port out of range"):
            BenchmarkNode._worker_owned_kvcache_config(base_config, thread_id=1)

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
            self.assertFalse(coordinator.all_runtime_ready.is_set())

            self.assertTrue(coordinator.handle_runtime_ready({"node_id": "node-a"}, object()))
            self.assertEqual(sent[-1]["status"], "waiting")
            self.assertEqual(sent[-1]["runtime_ready_count"], 1)
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a"])
            self.assertFalse(coordinator.all_runtime_ready.is_set())

            self.assertTrue(coordinator.handle_runtime_start_request({"node_id": "node-a"}, object()))
            self.assertEqual(sent[-1]["status"], "waiting")
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a"])

            self.assertTrue(coordinator.handle_runtime_ready({"node_id": "node-b"}, object()))
            self.assertEqual(sent[-1]["status"], "success")
            self.assertEqual(sent[-1]["runtime_ready_count"], 2)
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a", "node-b"])
            self.assertTrue(coordinator.all_runtime_ready.is_set())
            self.assertTrue(coordinator.wait_for_runtime_ready(timeout_s=0.1))

            self.assertTrue(coordinator.handle_runtime_start_request({"node_id": "node-a"}, object()))
            self.assertEqual(sent[-1]["status"], "success")
            self.assertEqual(sent[-1]["runtime_ready_node_ids"], ["node-a", "node-b"])

        metadata = coordinator._build_completion_metadata(
            status="RESULT_TIMEOUT",
            elapsed_seconds=1.0,
            completion_error="test",
        )
        self.assertEqual(metadata["runtime_ready_node_count"], 2)
        self.assertEqual(metadata["runtime_ready_node_ids"], ["node-a", "node-b"])

    def test_coordinator_waits_for_mpmc_runtime_ready_before_result_wait(self) -> None:
        coordinator_source = Path(
            "fluxon_test_stack/distributed_benchmark_coordinator.py"
        ).read_text(encoding="utf-8")
        ready_wait = coordinator_source.index("ready_ok = coordinator.wait_for_nodes_ready")
        runtime_wait = coordinator_source.index(
            "runtime_ready_ok = coordinator.wait_for_runtime_ready",
            ready_wait,
        )
        result_wait = coordinator_source.index(
            "completed = coordinator.wait_for_completion",
            runtime_wait,
        )

        self.assertLess(ready_wait, runtime_wait)
        self.assertLess(runtime_wait, result_wait)

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
    def test_producer_prewarm_before_ready_waits_for_endpoint(self) -> None:
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

            def _wait_mpmc_cluster_ready(self, **kwargs) -> None:
                return None

        node = ProducerNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
        }

        prepare_thread = threading.Thread(
            target=node._prepare_mpmc_round_before_ready,
            kwargs={"workers": 1},
        )
        prepare_thread.start()
        self.assertTrue(node.prepare_started.wait(timeout=1.0))
        self.assertTrue(prepare_thread.is_alive())
        self.assertIsNone(node._prepared_mpmc_round)

        node.allow_prepare.set()
        prepare_thread.join(timeout=2.0)
        self.assertFalse(prepare_thread.is_alive())
        self.assertIsNotNone(node._prepared_mpmc_round)
        self.assertEqual(sorted(node._prepared_mpmc_round.prepared_runtimes), [0])

        node._run_mpmc_workers(workers=1, deadline_ts=0.0)

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
        self.assertEqual(
            node._forced_benchmark_result["forced_failure_reason"],
            "mpmc_runtime_start_timeout",
        )

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

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_worker_owned_runtime_initializes_kv_before_mq_attach(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "producer_0_proc_0"
        node.node_id = "node_producer"
        node.mq_unique_id = "mpmc-test"
        node.mq_state = node_mod.MQState(role="producer", weight=1.0)
        node.chan_config = {}
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "kvcache_config": {
                "instance_key": "producer_0_proc_0",
                "fluxonkv_spec": {
                    "share_mem_path": "/tmp/fluxon-test-shm/node-1",
                    "cluster_name": "fluxon_benchmark",
                    "p2p_listen_port": 12000,
                },
            },
        }

        events = []
        init_kv_configs = []
        sentinel_store = object()
        sentinel_producer = object()
        original_init_mq_channel = node_mod.init_mq_channel

        def fake_init_kv_store(config):
            init_kv_configs.append(config)
            events.append("kv_init")
            return sentinel_store, None

        def fake_init_mq_channel(**kwargs):
            events.append("mq_attach")
            return sentinel_producer, None, None

        node_mod.init_mq_channel = fake_init_mq_channel
        node._sleep_for_runtime_init_stagger = lambda **kwargs: None  # type: ignore[method-assign]
        try:
            with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fake_init_kv_store):
                runtime = node._prepare_mpmc_worker_runtime(thread_id=0)
        finally:
            node_mod.init_mq_channel = original_init_mq_channel

        self.assertIs(runtime.producer, sentinel_producer)
        self.assertIs(runtime.kv_store, sentinel_store)
        self.assertEqual(init_kv_configs[0]["instance_key"], "producer_0_proc_0__worker_0")
        self.assertEqual(events, ["kv_init", "mq_attach"])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
