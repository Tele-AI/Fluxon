#!/usr/bin/env python3

from __future__ import annotations

import threading
import time
import ast
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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


class _FakeCloseResult:
    def __init__(self) -> None:
        self.consumed = False

    def is_ok(self) -> bool:
        return True

    def unwrap(self):
        self.consumed = True
        return None


class _FakeClosableEndpoint:
    def __init__(self, *, close_delay_seconds: float = 0.0) -> None:
        self.close_delay_seconds = close_delay_seconds
        self.close_calls = 0
        self.close_results = []

    def close(self) -> _FakeCloseResult:
        self.close_calls += 1
        if self.close_delay_seconds > 0.0:
            time.sleep(self.close_delay_seconds)
        result = _FakeCloseResult()
        self.close_results.append(result)
        return result


class _FakeTransactionOperand:
    def __eq__(self, _other):
        return self


class _FakeTransactions:
    def create(self, key):
        return _FakeTransactionOperand()

    def value(self, key):
        return _FakeTransactionOperand()

    def put(self, key, value, lease=None):
        return ("put", key, value, lease)

    def delete(self, key):
        return ("delete", key)


def _new_fake_fluxon_benchmark_store():
    if node_mod is None:
        raise RuntimeError("distributed benchmark node is unavailable")
    from fluxon_test_stack.benchmark_node_kv import KVGetOutput

    return node_mod.KVBenchmarkBlockingStore(
        object(),
        backend_kind=node_mod.BACKEND_KIND_FLUXON,
        get_output=KVGetOutput.HOLDER,
    )


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
    def test_new_or_bind_existing_mapping_binds_without_unique_lock(self) -> None:
        try:
            from fluxon_py import api_ext_chan
            from fluxon_py.api_error import Result
            from fluxon_py.api_ext_chan import ChanRole, ChanType
        except ImportError as exc:
            self.skipTest(f"fluxon_py runtime import unavailable: {exc}")

        unique_id = "mq-fast-bind"
        sentinel_channel = object()

        class FakeEtcd:
            def __init__(self) -> None:
                self.get_keys = []
                self.transaction_calls = 0

            def get(self, key):
                self.get_keys.append(key)
                if key == api_ext_chan._new_unique_mapping_key(unique_id):
                    return b"123", None
                return None, None

            def transaction(self, **_kwargs):
                self.transaction_calls += 1
                raise AssertionError("existing mapping fast-bind must not acquire the unique lock")

        fake_etcd = FakeEtcd()
        bind_calls = []

        def fake_new_etcd_client(_api):
            return Result.new_ok(fake_etcd)

        def fake_construct_bound_channel(api, chan_config, chan_id, chan_type, chan_role, etcd_client):
            bind_calls.append((api, chan_config, chan_id, chan_type, chan_role, etcd_client))
            return Result.new_ok(sentinel_channel)

        with mock.patch.object(api_ext_chan, "new_etcd_client", side_effect=fake_new_etcd_client):
            with mock.patch.object(
                api_ext_chan,
                "_construct_bound_channel",
                side_effect=fake_construct_bound_channel,
            ):
                res = api_ext_chan.new_or_bind_with_unique_key(
                    object(),
                    {"capacity": 10, "ttl_seconds": 90, "weight": 1},
                    unique_id,
                    ChanType.MPMC,
                    ChanRole.PRODUCER,
                )

        self.assertTrue(res.is_ok())
        self.assertIs(res.unwrap(), sentinel_channel)
        self.assertEqual(fake_etcd.transaction_calls, 0)
        self.assertNotIn(api_ext_chan._new_unique_lock_key(unique_id), fake_etcd.get_keys)
        self.assertEqual(len(bind_calls), 1)
        self.assertEqual(bind_calls[0][2], "123")

    def test_new_or_bind_fast_bind_retries_transient_unique_key_read(self) -> None:
        try:
            from fluxon_py import api_ext_chan
            from fluxon_py.api_error import Result
            from fluxon_py.api_ext_chan import ChanRole, ChanType
        except ImportError as exc:
            self.skipTest(f"fluxon_py runtime import unavailable: {exc}")

        unique_id = "mq-fast-bind-read-retry"
        sentinel_channel = object()

        class FakeEtcd:
            def __init__(self) -> None:
                self.unique_get_calls = 0
                self.transaction_calls = 0

            def get(self, key):
                if key == api_ext_chan._new_unique_mapping_key(unique_id):
                    self.unique_get_calls += 1
                    if self.unique_get_calls == 1:
                        raise RuntimeError("etcd connection failed")
                    return b"123", None
                return None, None

            def transaction(self, **_kwargs):
                self.transaction_calls += 1
                raise AssertionError("existing mapping retry fast-bind must not acquire the unique lock")

        fake_etcd = FakeEtcd()
        sleeps = []

        def fake_new_etcd_client(_api):
            return Result.new_ok(fake_etcd)

        def fake_construct_bound_channel(api, chan_config, chan_id, chan_type, chan_role, etcd_client):
            return Result.new_ok(sentinel_channel)

        with mock.patch.object(api_ext_chan, "new_etcd_client", side_effect=fake_new_etcd_client):
            with mock.patch.object(
                api_ext_chan,
                "_construct_bound_channel",
                side_effect=fake_construct_bound_channel,
            ):
                with mock.patch.object(api_ext_chan.time, "sleep", side_effect=lambda seconds: sleeps.append(seconds)):
                    res = api_ext_chan.new_or_bind_with_unique_key(
                        object(),
                        {"capacity": 10, "ttl_seconds": 90, "weight": 1},
                        unique_id,
                        ChanType.MPMC,
                        ChanRole.PRODUCER,
                    )

        self.assertTrue(res.is_ok())
        self.assertIs(res.unwrap(), sentinel_channel)
        self.assertEqual(fake_etcd.unique_get_calls, 2)
        self.assertEqual(fake_etcd.transaction_calls, 0)
        self.assertEqual(sleeps, [api_ext_chan.MQ_UNIQUE_FAST_BIND_READ_RETRY_BASE_SECONDS])

    def test_fast_bind_failure_reuses_locked_resolver(self) -> None:
        try:
            from fluxon_py import api_ext_chan
            from fluxon_py.api_error import InvalidConfigurationError, Result
            from fluxon_py.api_ext_chan import ChanRole, ChanType
        except ImportError as exc:
            self.skipTest(f"fluxon_py runtime import unavailable: {exc}")

        unique_id = "mq-fast-bind-fallback"
        unique_key = api_ext_chan._new_unique_mapping_key(unique_id)
        meta_key = api_ext_chan._new_mpmc_meta_key("123")
        sentinel_channel = object()

        class FakeEtcd:
            def __init__(self) -> None:
                self.transactions = _FakeTransactions()
                self.transaction_calls = 0
                self.delete_calls = []

            def get(self, key):
                if key == unique_key:
                    return b"123", None
                if key == meta_key:
                    return b"present", None
                return None, None

            def lease(self, _ttl_seconds):
                return SimpleNamespace(id=1)

            def transaction(self, **_kwargs):
                self.transaction_calls += 1
                return True, []

            def delete(self, key):
                self.delete_calls.append(key)

        fake_etcd = FakeEtcd()
        bind_calls = 0

        def fake_construct_bound_channel(*_args, **_kwargs):
            nonlocal bind_calls
            bind_calls += 1
            if bind_calls == 1:
                return Result.new_error(
                    InvalidConfigurationError(message="synthetic fast-bind failure")
                )
            return Result.new_ok(sentinel_channel)

        with mock.patch.object(
            api_ext_chan,
            "new_etcd_client",
            return_value=Result.new_ok(fake_etcd),
        ):
            with mock.patch.object(
                api_ext_chan,
                "_construct_bound_channel",
                side_effect=fake_construct_bound_channel,
            ):
                res = api_ext_chan.new_or_bind_with_unique_key(
                    object(),
                    {"capacity": 10, "ttl_seconds": 90, "weight": 1},
                    unique_id,
                    ChanType.MPMC,
                    ChanRole.PRODUCER,
                )

        self.assertTrue(res.is_ok())
        self.assertIs(res.unwrap(), sentinel_channel)
        self.assertEqual(bind_calls, 2)
        self.assertEqual(fake_etcd.transaction_calls, 2)
        self.assertEqual(fake_etcd.delete_calls, [])

    def test_fast_bind_missing_mpsc_meta_rebuilds_under_lock(self) -> None:
        try:
            from fluxon_py import api_ext_chan
            from fluxon_py.api_error import InvalidConfigurationError, Result
            from fluxon_py.api_ext_chan import ChanRole, ChanType
        except ImportError as exc:
            self.skipTest(f"fluxon_py runtime import unavailable: {exc}")

        unique_id = "mq-fast-bind-missing-mpsc-meta"
        unique_key = api_ext_chan._new_unique_mapping_key(unique_id)
        stale_meta_key = api_ext_chan._new_etcd_meta_key("3")
        sentinel_channel = object()

        class FakeEtcd:
            def __init__(self) -> None:
                self.transactions = _FakeTransactions()
                self.transaction_calls = 0
                self.delete_calls = []

            def get(self, key):
                if key == unique_key:
                    return b"3", None
                if key == stale_meta_key:
                    return None, None
                return None, None

            def lease(self, _ttl_seconds):
                return SimpleNamespace(id=1)

            def transaction(self, **_kwargs):
                self.transaction_calls += 1
                return True, []

            def delete(self, key):
                self.delete_calls.append(key)

        fake_etcd = FakeEtcd()
        bind_calls = 0

        def fake_construct_bound_channel(*_args, **_kwargs):
            nonlocal bind_calls
            bind_calls += 1
            return Result.new_error(
                InvalidConfigurationError(
                    message=(
                        "MPSC producer initialize error! failed to bind MPSC producer: "
                        "get_chan_meta failed for chan_id=3: channel meta not found: chan_id=3"
                    )
                )
            )

        with mock.patch.object(
            api_ext_chan,
            "new_etcd_client",
            return_value=Result.new_ok(fake_etcd),
        ):
            with mock.patch.object(
                api_ext_chan,
                "_construct_bound_channel",
                side_effect=fake_construct_bound_channel,
            ):
                with mock.patch.object(
                    api_ext_chan,
                    "chan_new",
                    return_value=Result.new_ok("4"),
                ):
                    with mock.patch.object(
                        api_ext_chan,
                        "get_chan_by_id",
                        return_value=Result.new_ok(sentinel_channel),
                    ):
                        with mock.patch.object(api_ext_chan, "del_chan_by_id"):
                            res = api_ext_chan.new_or_bind_with_unique_key(
                                object(),
                                {"capacity": 10, "ttl_seconds": 90, "weight": 1},
                                unique_id,
                                ChanType.MPSC,
                                ChanRole.PRODUCER,
                            )

        self.assertTrue(res.is_ok())
        self.assertIs(res.unwrap(), sentinel_channel)
        self.assertEqual(bind_calls, 1)
        self.assertEqual(fake_etcd.delete_calls, [unique_key])
        self.assertEqual(fake_etcd.transaction_calls, 3)

    def test_concurrent_fast_bind_returns_each_constructed_handle_directly(self) -> None:
        try:
            from fluxon_py import api_ext_chan
            from fluxon_py.api_error import Result
            from fluxon_py.api_ext_chan import ChanRole, ChanType
        except ImportError as exc:
            self.skipTest(f"fluxon_py runtime import unavailable: {exc}")

        unique_id = "mq-fast-bind-concurrent"
        barrier = threading.Barrier(2)
        handles = {"bind-a": object(), "bind-b": object()}
        results = {}

        class FakeEtcd:
            def get(self, key):
                if key == api_ext_chan._new_unique_mapping_key(unique_id):
                    return b"123", None
                return None, None

        def fake_construct(*_args, **_kwargs):
            thread_name = threading.current_thread().name
            barrier.wait(timeout=5.0)
            return Result.new_ok(handles[thread_name])

        def run_bind() -> None:
            results[threading.current_thread().name] = api_ext_chan.new_or_bind_with_unique_key(
                object(),
                {"capacity": 10, "ttl_seconds": 90, "weight": 1},
                unique_id,
                ChanType.MPMC,
                ChanRole.PRODUCER,
            )

        with mock.patch.object(
            api_ext_chan,
            "new_etcd_client",
            return_value=Result.new_ok(FakeEtcd()),
        ):
            with mock.patch.object(
                api_ext_chan,
                "_construct_bound_channel",
                side_effect=fake_construct,
            ):
                with mock.patch.object(api_ext_chan, "CHANID_2_NODES", {}):
                    threads = [
                        threading.Thread(target=run_bind, name="bind-a"),
                        threading.Thread(target=run_bind, name="bind-b"),
                    ]
                    for thread in threads:
                        thread.start()
                    for thread in threads:
                        thread.join(timeout=5.0)
                    self.assertFalse(any(thread.is_alive() for thread in threads))
                    self.assertEqual(api_ext_chan.CHANID_2_NODES, {})

        self.assertIs(results["bind-a"].unwrap(), handles["bind-a"])
        self.assertIs(results["bind-b"].unwrap(), handles["bind-b"])

    def test_coordinator_start_waiting_warning_is_log_throttled(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        logger_obj = coordinator._log_start_waiting_warning.__globals__["logger"]

        with mock.patch.object(logger_obj, "warning") as warning_mock:
            coordinator._log_start_waiting_warning(node_id="node-1")
            coordinator._log_start_waiting_warning(node_id="node-2")
            coordinator._log_start_waiting_warning(node_id="node-3")

            self.assertEqual(warning_mock.call_count, 1)
            self.assertEqual(coordinator._start_waiting_warning_suppressed_count, 2)

            coordinator._start_waiting_warning_next_log_ts = 0.0
            coordinator._log_start_waiting_warning(node_id="node-4")

            self.assertEqual(warning_mock.call_count, 2)
            self.assertIn("suppressed_waiting_warnings", warning_mock.call_args.args[0])

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

        def is_self_data_path_lock(expr: ast.AST) -> bool:
            return (
                isinstance(expr, ast.Attribute)
                and expr.attr == "_data_path_lock"
                and isinstance(expr.value, ast.Name)
                and expr.value.id == "self"
            )

        def is_local_handle_call(call: ast.Call, attr: str) -> bool:
            return (
                isinstance(call.func, ast.Attribute)
                and call.func.attr == attr
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "handle"
            )

        init_func = method("__init__")
        lock_assigns = [
            node
            for node in ast.walk(init_func)
            if isinstance(node, ast.Assign)
            and any(is_self_data_path_lock(target) for target in node.targets)
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
                and any(is_self_data_path_lock(item.context_expr) for item in node.items)
            ]
            self.assertTrue(with_nodes, f"{method_name} must hold self._data_path_lock")
            self.assertTrue(
                any(
                    is_local_handle_call(call, handle_attr)
                    for with_node in with_nodes
                    for call in ast.walk(with_node)
                    if isinstance(call, ast.Call)
                ),
                f"{method_name} must call local handle.{handle_attr} under self._data_path_lock",
            )

        close_func = method("close")
        self.assertFalse(
            any(
                isinstance(node, ast.With)
                and any(is_self_data_path_lock(item.context_expr) for item in node.items)
                for node in ast.walk(close_func)
            ),
            "MPSCChanProducer.close must not wait for the data-path lock",
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

    def test_fluxon_kv_lease_keepalive_stays_inside_rust(self) -> None:
        pyo3_lease = Path("fluxon_rs/fluxon_pyo3/src/lease_manager.rs").read_text(
            encoding="utf-8"
        )
        pyo3_mpsc = Path("fluxon_rs/fluxon_pyo3/src/mpsc.rs").read_text(
            encoding="utf-8"
        )
        backend_handle = Path(
            "fluxon_rs/fluxon_util/src/lease_manager/lease_backend_handle.rs"
        ).read_text(encoding="utf-8")
        python_mpsc = Path("fluxon_py/_api_ext_chan/mpsc.py").read_text(
            encoding="utf-8"
        )
        python_mpmc = Path("fluxon_py/_api_ext_chan/mpmc.py").read_text(
            encoding="utf-8"
        )
        benchmark_node = Path(
            "fluxon_test_stack/distributed_benchmark_node.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("PyLeaseBackendUid", pyo3_lease)
        self.assertNotIn("run_longtime_py_function", pyo3_lease)
        self.assertNotIn("kv_backend_uid: Py<", pyo3_mpsc)
        self.assertIn("(keepalive)(lease_id).await", backend_handle)
        self.assertNotIn("_ensure_kvclient_lease_backend", python_mpsc)
        self.assertNotIn("keepalive_cb", python_mpsc)
        self.assertIn("_RustMpscContext(etcd_endpoints, raw)", python_mpsc)
        self.assertIn("register_kvclient_lease(", python_mpmc)
        self.assertNotIn("_dummy_shutdown_", benchmark_node)
        self.assertNotIn("time.sleep(30)", benchmark_node)
        self.assertIn("_finish_mpmc_round(", benchmark_node)
        self.assertIn(
            '_close_kv_store(reason="node_process_exit")', benchmark_node
        )

    def test_mpmc_member_lease_is_allocated_after_shared_setup(self) -> None:
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

        def is_member_lease_target(target: ast.AST) -> bool:
            return (
                isinstance(target, ast.Attribute)
                and target.attr == "mpmc_member_lease"
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
            )

        def is_member_lease_assign(node: ast.AST) -> bool:
            if isinstance(node, ast.AnnAssign):
                return is_member_lease_target(node.target)
            return isinstance(node, ast.Assign) and any(
                is_member_lease_target(target) for target in node.targets
            )

        def assign_value(node: ast.AST) -> ast.AST:
            if isinstance(node, ast.AnnAssign):
                assert node.value is not None
                return node.value
            assert isinstance(node, ast.Assign)
            return node.value

        init_body_member_lease_assigns = [
            node
            for node in init_func.body
            if is_member_lease_assign(node)
        ]
        self.assertEqual(len(init_body_member_lease_assigns), 1)
        initial_value = assign_value(init_body_member_lease_assigns[0])
        self.assertIsInstance(initial_value, ast.Constant)
        self.assertIsNone(initial_value.value)

        setup_member_fn = next(
            node
            for node in init_func.body
            if isinstance(node, ast.FunctionDef) and node.name == "_setup_member_and_role_key"
        )
        lease_assigns = [
            node
            for node in ast.walk(setup_member_fn)
            if is_member_lease_assign(node)
            and isinstance(assign_value(node), ast.Call)
            and isinstance(assign_value(node).func, ast.Attribute)
            and assign_value(node).func.attr == "lease"
        ]
        self.assertEqual(len(lease_assigns), 1)

        member_lease_register_calls = [
            node
            for node in ast.walk(setup_member_fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr
            in {"register_etcd_lease", "register_newly_granted_etcd_lease"}
        ]
        self.assertEqual(len(member_lease_register_calls), 1)
        self.assertEqual(
            member_lease_register_calls[0].func.attr,
            "register_newly_granted_etcd_lease",
        )

        shared_setup_guard = next(
            node
            for node in init_func.body
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "keep_shared_mpmc_leases"
        )
        member_setup_call = next(
            node
            for node in init_func.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "_setup_member_and_role_key"
        )
        self.assertLess(
            shared_setup_guard.lineno,
            member_setup_call.lineno,
        )

    def test_consumer_does_not_wait_for_ready_channels_before_reporting_ready(self) -> None:
        readiness = evaluate_mpmc_topology_ready(
            role="consumer",
            expected_workers=1,
            ready_channels=0,
            active_consumers=1,
        )

        self.assertTrue(readiness.ready)

    def test_producer_still_waits_for_ready_channels_and_active_consumers(self) -> None:
        no_ready_channel = evaluate_mpmc_topology_ready(
            role="producer",
            expected_workers=1,
            ready_channels=0,
            active_consumers=1,
        )
        no_consumer = evaluate_mpmc_topology_ready(
            role="producer",
            expected_workers=1,
            ready_channels=1,
            active_consumers=0,
        )

        self.assertFalse(no_ready_channel.ready)
        self.assertIn("ready_channels", no_ready_channel.reason)
        self.assertFalse(no_consumer.ready)
        self.assertIn("active_consumers", no_consumer.reason)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_cluster_ready_waits_for_a_complete_authority_observation(self) -> None:
        from fluxon_test_stack.benchmark_node_mq import ClusterInfoSnapshot

        node = BenchmarkNode()
        node.test_config = {"node_role": "producer"}
        runtime = PreparedWorkerRuntime(producer=object())
        complete_snapshot = ClusterInfoSnapshot(
            mpmc_id="7",
            active_consumers=1,
            ready_channel_ids=("11",),
        )

        with mock.patch.object(
            node_mod,
            "get_cluster_info_snapshot",
            side_effect=[RuntimeError("etcd read failed"), complete_snapshot],
        ) as read_snapshot:
            with mock.patch.object(node_mod.time, "sleep", return_value=None):
                result = node._wait_mpmc_cluster_ready(
                    runtime=runtime,
                    expected_workers=1,
                    timeout_s=1.0,
                )

        self.assertIs(result, complete_snapshot)
        self.assertEqual(read_snapshot.call_count, 2)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_cluster_info_snapshot_uses_typed_ready_channel_authority(self) -> None:
        from fluxon_py.api_ext_chan import ChanRole, MPMCChanProducer
        from fluxon_test_stack.benchmark_node_mq import get_cluster_info_snapshot

        class FakeChannel:
            def get_remote_ready_channels(self):
                return ["12", "11"]

            def get_active_member_ids(self, role):
                self_role = role
                self.assert_role = self_role
                return [21, 22]

        channel = FakeChannel()

        class FakeProducer(MPMCChanProducer):
            def __init__(self) -> None:
                self.mpmc_channel = channel

            def get_chan_id(self) -> str:
                return "7"

            def __del__(self) -> None:
                pass

        snapshot = get_cluster_info_snapshot(FakeProducer())

        self.assertEqual(channel.assert_role, ChanRole.CONSUMER)
        self.assertEqual(snapshot.mpmc_id, "7")
        self.assertEqual(snapshot.active_consumers, 2)
        self.assertEqual(snapshot.ready_channel_ids, ("11", "12"))
        self.assertEqual(snapshot.ready_channels, 2)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_cluster_info_snapshot_rejects_duck_typed_endpoint(self) -> None:
        from fluxon_test_stack.benchmark_node_mq import get_cluster_info_snapshot

        endpoint = SimpleNamespace(
            get_chan_id=lambda: "7",
            mpmc_channel=SimpleNamespace(
                get_remote_ready_channels=lambda: ["11"],
            ),
        )

        with self.assertRaisesRegex(TypeError, "MPMCChanProducer or MPMCChanConsumer"):
            get_cluster_info_snapshot(endpoint)

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
    def test_stop_intent_closes_each_endpoint_once(self) -> None:
        endpoint = _FakeClosableEndpoint()
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

        node._close_mpmc_round_endpoints(
            round_state=round_state,
            reason="test_stop_intent",
        )

        self.assertEqual(endpoint.close_calls, 1)
        self.assertTrue(endpoint.close_results[0].consumed)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_kv_close_waits_past_legacy_timeout_and_consumes_result(self) -> None:
        store = _FakeClosableEndpoint(close_delay_seconds=2.05)
        node = BenchmarkNode()
        node.kv_store = store

        started = time.monotonic()
        with mock.patch.object(node_mod, "close_fs_runtime"):
            with mock.patch.object(node_mod, "close_rpc_runtime"):
                node._close_kv_store(reason="slow_close_regression")

        self.assertGreaterEqual(time.monotonic() - started, 2.0)
        self.assertTrue(node._kv_store_closed)
        self.assertEqual(store.close_calls, 1)
        self.assertTrue(store.close_results[0].consumed)
        self.assertFalse(
            any(thread.name.startswith("timeout-call:") for thread in threading.enumerate())
        )

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_concurrent_kv_close_calls_share_one_completed_close(self) -> None:
        store = _FakeClosableEndpoint(close_delay_seconds=0.05)
        node = BenchmarkNode()
        node.kv_store = store
        errors = []

        def close_store() -> None:
            try:
                node._close_kv_store(reason="concurrent_close_regression")
            except BaseException as exc:
                errors.append(exc)

        with mock.patch.object(node_mod, "close_fs_runtime"):
            with mock.patch.object(node_mod, "close_rpc_runtime"):
                threads = [threading.Thread(target=close_store) for _ in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=2.0)

        self.assertEqual(errors, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(store.close_calls, 1)
        self.assertTrue(store.close_results[0].consumed)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_worker_does_not_close_round_owned_endpoint(self) -> None:
        endpoint = _FakeClosableEndpoint()
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "value_size": 256,
        }
        node._benchmark_stop.set()

        with mock.patch.object(node_mod, "run_fs_worker", return_value=None):
            with mock.patch.object(node_mod, "run_rpc_worker", return_value=None):
                with mock.patch.object(node_mod, "run_kv_worker", return_value=None):
                    results = node._run_worker_thread(
                        0,
                        time.time() + 1.0,
                        prepared_runtime=PreparedWorkerRuntime(producer=endpoint),
                    )

        self.assertEqual(results, [])
        self.assertEqual(endpoint.close_calls, 0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_kvcache_init_is_shared_per_process(self) -> None:
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

            def fake_init_kv_store(_config, **_kwargs):
                nonlocal active_count, max_active_count
                with active_lock:
                    active_count += 1
                    max_active_count = max(max_active_count, active_count)
                time.sleep(0.05)
                with active_lock:
                    active_count -= 1
                return _new_fake_fluxon_benchmark_store(), None

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
            self.assertEqual(max_active_count, 1)
            self.assertIsNotNone(node.kv_store)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_process_shared_kvcache_stagger_runs_once_per_process(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            node = BenchmarkNode()
            node.test_config = {
                "node_role": "producer",
                "test_mode": TestMode.MPMC.value,
                "cluster_ready_timeout_seconds": 1800,
                "expected_nodes": 328,
                "max_benchmark_seconds": 60,
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

            def fake_init_kv_store(config, **_kwargs):
                init_kv_configs.append(config)
                return _new_fake_fluxon_benchmark_store(), None

            def fake_init_mq_channel(*, role, kv_store, chan_config, unique_id, weight):
                init_mq_calls.append(
                    {
                        "role": role,
                        "kv_store": kv_store,
                        "unique_id": unique_id,
                    }
                )
                return object(), None, None

            with mock.patch.object(node, "_wait_for_runtime_init_etcd_health", return_value=None):
                with mock.patch.object(node, "_sleep_for_runtime_init_stagger", side_effect=fake_stagger):
                    with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fake_init_kv_store):
                        with mock.patch.object(node_mod, "init_mq_channel", side_effect=fake_init_mq_channel):
                            node._prepare_mpmc_worker_runtime(thread_id=0)
                            node._prepare_mpmc_worker_runtime(thread_id=1)

            self.assertEqual(len(stagger_calls), 1)
            self.assertEqual(stagger_calls[0], {"max_sleep_seconds": None})
            self.assertEqual(len(init_kv_configs), 1)
            self.assertEqual(len(init_mq_calls), 2)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_producer_workers_share_process_kv_runtime(self) -> None:
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

            def fake_init_kv_store(config, **_kwargs):
                init_kv_configs.append(config)
                store = _new_fake_fluxon_benchmark_store()
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

            self.assertEqual(len(init_kv_configs), 1)
            self.assertEqual(init_kv_configs[0]["instance_key"], "producer_0__worker_0")
            self.assertEqual(init_kv_configs[0]["fluxonkv_spec"]["p2p_listen_port"], 11826)
            self.assertEqual(len(init_mq_calls), 2)
            self.assertIs(init_mq_calls[0]["kv_store"], stores[0].kv_client)
            self.assertIs(init_mq_calls[1]["kv_store"], stores[0].kv_client)
            self.assertIs(runtime_0.producer, producers[0])
            self.assertIs(runtime_1.producer, producers[1])
            self.assertIs(node.kv_store, stores[0])
            self.assertIsNot(runtime_0.local_mq_state, runtime_1.local_mq_state)

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

                def _init_kv_store_with_ready_retry(self, config, **_kwargs):
                    return _new_fake_fluxon_benchmark_store(), None

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
                        producer=_FakeClosableEndpoint(),
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
        store = _new_fake_fluxon_benchmark_store()
        node.kv_store = store
        node.fluxon_client = store.kv_client
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
            "EtcdError(13003: Failed to read unique key before lock: '/mq_unique_keys/x', err=etcd connection failed)",
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

    def test_failed_framework_init_shuts_down_partial_runtime(self) -> None:
        source = (REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "src" / "lib.rs").read_text(
            encoding="utf-8"
        )
        helper_start = source.index("async fn finish_framework_init(")
        helper_end = source.index("async fn run_master_impl(", helper_start)
        helper_source = source[helper_start:helper_end]

        self.assertIn("framework.shutdown().await", helper_source)
        self.assertIn("failed to shut down partially initialized framework", helper_source)
        for init_function in (
            "init_framework_master",
            "init_framework_external",
            "init_framework_owner",
        ):
            init_call = f"{init_function}(&framework, init_args).await"
            call_pos = source.index(init_call)
            self.assertIn(
                "finish_framework_init(",
                source[max(0, call_pos - 120) : call_pos],
                f"{init_function} must release partially initialized modules before returning",
            )

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

    def test_coordinator_force_completes_missing_consumer_without_lock_reentry(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        self.assertIsNotNone(coordinator.test_config)
        coordinator.expected_nodes = 2
        coordinator.test_config.test_id = "missing-consumer-regression"
        coordinator.start_new_test(coordinator.test_config)
        coordinator.registered_nodes = {
            "producer-node": {"node_role": "producer"},
            "consumer-node": {"node_role": "consumer"},
        }

        producer_report = {
            "node_id": "producer-node",
            "results": {
                "node_id": "producer-node",
                "node_role": "producer",
                "p50_latency_us": 0.0,
                "inflight_max": 0,
                "inflight_avg": 0.0,
            },
        }
        with mock.patch.object(coordinator, "_send_tcp_response", return_value=True):
            self.assertTrue(coordinator.handle_report_results(producer_report, object()))

        completion_result = []
        completion_errors = []

        def wait_for_completion() -> None:
            try:
                completion_result.append(coordinator.wait_for_completion(timeout_s=0.01))
            except BaseException as exc:  # noqa: BLE001
                completion_errors.append(exc)

        waiter = threading.Thread(target=wait_for_completion, daemon=True)
        waiter.start()
        waiter.join(timeout=1.0)

        self.assertFalse(
            waiter.is_alive(),
            "wait_for_completion deadlocked while setting round terminal",
        )
        self.assertEqual(completion_errors, [])
        self.assertEqual(completion_result, [True])
        self.assertTrue(coordinator.all_results_received.is_set())

        results = coordinator.test_results[coordinator.test_config.test_id]
        placeholders = [result for result in results if result.node_id == "consumer-node"]
        self.assertEqual(len(placeholders), 1)
        self.assertEqual(
            placeholders[0].error_details,
            {"forced_missing_consumer_result_timeout": 1},
        )
        gate = coordinator._round_gate_snapshot(test_id=coordinator.test_config.test_id)
        self.assertEqual(gate["status"], "completed")
        self.assertEqual(gate["reported_result_node_count"], 2)

        late_consumer_report = {
            "node_id": "consumer-node",
            "results": {
                "node_id": "consumer-node",
                "node_role": "consumer",
                "total_operations": 99,
                "successful_operations": 99,
                "p50_latency_us": 1.0,
                "inflight_max": 1,
                "inflight_avg": 1.0,
            },
        }
        responses = []
        with mock.patch.object(
            coordinator,
            "_send_tcp_response",
            side_effect=lambda _sock, response: responses.append(response) or True,
        ):
            self.assertTrue(
                coordinator.handle_report_results(late_consumer_report, object())
            )

        self.assertEqual(responses[-1]["status"], "success")
        results = coordinator.test_results[coordinator.test_config.test_id]
        placeholders = [
            result for result in results if result.node_id == "consumer-node"
        ]
        self.assertEqual(len(placeholders), 1)
        self.assertEqual(placeholders[0].total_operations, 1)
        self.assertEqual(
            placeholders[0].error_details,
            {"forced_missing_consumer_result_timeout": 1},
        )
        gate = coordinator._round_gate_snapshot(test_id=coordinator.test_config.test_id)
        self.assertEqual(gate["status"], "completed")

    def test_coordinator_timeout_boundary_accepts_completed_real_results(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        self.assertIsNotNone(coordinator.test_config)
        coordinator.expected_nodes = 2
        coordinator.test_config.test_id = "timeout-boundary-real-results"
        coordinator.start_new_test(coordinator.test_config)
        coordinator.registered_nodes = {
            "producer-node": {"node_role": "producer"},
            "consumer-node": {"node_role": "consumer"},
        }

        producer_report = {
            "node_id": "producer-node",
            "results": {
                "node_id": "producer-node",
                "node_role": "producer",
                "p50_latency_us": 0.0,
                "inflight_max": 0,
                "inflight_avg": 0.0,
            },
        }
        consumer_report = {
            "node_id": "consumer-node",
            "results": {
                "node_id": "consumer-node",
                "node_role": "consumer",
                "total_operations": 7,
                "successful_operations": 7,
                "p50_latency_us": 2.0,
                "inflight_max": 1,
                "inflight_avg": 1.0,
            },
        }
        with mock.patch.object(coordinator, "_send_tcp_response", return_value=True):
            self.assertTrue(coordinator.handle_report_results(producer_report, object()))

            def report_consumer_then_return_timeout(*, timeout):
                self.assertGreater(timeout, 0)
                self.assertTrue(
                    coordinator.handle_report_results(consumer_report, object())
                )
                return False

            with mock.patch.object(
                coordinator.all_results_received,
                "wait",
                side_effect=report_consumer_then_return_timeout,
            ):
                self.assertTrue(coordinator.wait_for_completion(timeout_s=0.01))

        results = coordinator.test_results[coordinator.test_config.test_id]
        consumer_results = [
            result for result in results if result.node_id == "consumer-node"
        ]
        self.assertEqual(len(consumer_results), 1)
        self.assertEqual(consumer_results[0].total_operations, 7)
        self.assertNotIn(
            "forced_missing_consumer_result_timeout",
            consumer_results[0].error_details,
        )
        gate = coordinator._round_gate_snapshot(test_id=coordinator.test_config.test_id)
        self.assertEqual(gate["status"], "completed")
        self.assertEqual(gate["reported_result_node_count"], 2)

    def test_coordinator_late_report_cannot_reopen_failed_round(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        self.assertIsNotNone(coordinator.test_config)
        coordinator.expected_nodes = 3
        coordinator.test_config.test_id = "failed-round-remains-terminal"
        coordinator.start_new_test(coordinator.test_config)
        coordinator.registered_nodes = {
            "producer-a": {"node_role": "producer"},
            "producer-b": {"node_role": "producer"},
            "consumer-node": {"node_role": "consumer"},
        }

        producer_report = {
            "node_id": "producer-a",
            "results": {
                "node_id": "producer-a",
                "node_role": "producer",
                "p50_latency_us": 0.0,
                "inflight_max": 0,
                "inflight_avg": 0.0,
            },
        }
        late_consumer_report = {
            "node_id": "consumer-node",
            "results": {
                "node_id": "consumer-node",
                "node_role": "consumer",
                "p50_latency_us": 1.0,
                "inflight_max": 1,
                "inflight_avg": 1.0,
            },
        }
        with mock.patch.object(coordinator, "_send_tcp_response", return_value=True):
            self.assertTrue(coordinator.handle_report_results(producer_report, object()))
            self.assertFalse(coordinator.wait_for_completion(timeout_s=0.01))

            gate = coordinator._round_gate_snapshot(
                test_id=coordinator.test_config.test_id
            )
            self.assertEqual(gate["status"], "failed")
            self.assertTrue(
                coordinator.handle_report_results(late_consumer_report, object())
            )

        gate = coordinator._round_gate_snapshot(test_id=coordinator.test_config.test_id)
        self.assertEqual(gate["status"], "failed")
        self.assertEqual(gate["reported_result_node_count"], 1)
        self.assertEqual(
            [
                result.node_id
                for result in coordinator.test_results[
                    coordinator.test_config.test_id
                ]
            ],
            ["producer-a"],
        )

    def test_coordinator_assigns_one_prefeed_leader_and_global_consumer_count(self) -> None:
        coordinator = _new_coordinator_with_temp_config()
        self.assertEqual(coordinator.expected_mpmc_consumer_workers, 1)
        self.assertEqual(
            coordinator.instance_mpmc_producer_prefeed_leader_map,
            {"producer_0": True},
        )

        sent = []
        handle_register_globals = coordinator.handle_register.__func__.__globals__
        with (
            mock.patch.dict(
                handle_register_globals,
                {"_load_benchmark_section": lambda _path: {}},
            ),
            mock.patch.object(
                coordinator,
                "_send_tcp_response",
                side_effect=lambda _sock, response: sent.append(response) or True,
            ),
        ):
            self.assertTrue(
                coordinator.handle_register(
                    {
                        "node_id": "producer-node",
                        "node_type": "worker",
                        "instance_key": "producer_0",
                    },
                    object(),
                )
            )

        config = sent[-1]["config"]
        self.assertEqual(config["expected_mpmc_consumer_workers"], 1)
        self.assertIs(config["mpmc_producer_prefeed_leader"], True)

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
        prefeed_drain_call = node_source.rindex(
            "self._consume_mpmc_prefeed_messages(",
            0,
            wait_call,
        )
        start_time_set = node_source.index("self.start_time = time.time()", wait_call)
        deadline_publish = node_source.index(
            "round_state.benchmark_deadline_ts = deadline_ts",
            start_time_set,
        )
        start_event_set = node_source.index("round_state.start_event.set()", start_time_set)
        self.assertLess(prefeed_drain_call, wait_call)
        self.assertLess(wait_call, start_time_set)
        self.assertLess(start_time_set, deadline_publish)
        self.assertLess(deadline_publish, start_event_set)
        self.assertLess(start_time_set, start_event_set)

    def test_deferred_producer_retry_deadline_is_created_inside_started_worker(self) -> None:
        node_source = Path("fluxon_test_stack/distributed_benchmark_node.py").read_text(
            encoding="utf-8"
        )
        method_start = node_source.index("def _prepare_mpmc_round_before_ready")
        worker_start = node_source.index("def worker_target", method_start)
        deadline_assignment = node_source.index(
            "prepare_retry_deadline_ts = time.monotonic() + cluster_ready_timeout_s",
            worker_start,
        )
        prepare_call = node_source.index(
            "runtime = self._prepare_mpmc_worker_runtime_with_retry(",
            worker_start,
        )

        self.assertNotIn(
            "prepare_retry_deadline_ts =",
            node_source[method_start:worker_start],
        )
        self.assertLess(worker_start, deadline_assignment)
        self.assertLess(deadline_assignment, prepare_call)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_consumer_receives_deadline_created_after_runtime_start(self) -> None:
        class ConsumerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.runtime_start_release_ts = 0.0
                self.worker_deadlines = []

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                return PreparedWorkerRuntime(consumer=_FakeClosableEndpoint())

            def _wait_mpmc_cluster_ready(self, **kwargs) -> None:
                return None

            def _consume_mpmc_prefeed_messages(self, **kwargs) -> None:
                return None

            def _wait_for_mpmc_runtime_start(self) -> bool:
                self.runtime_start_release_ts = time.time()
                return True

            def _start_network_bandwidth_sampler(self) -> None:
                return None

            def _start_heartbeat(self) -> None:
                return None

            def _run_worker_thread(
                self,
                thread_id,
                deadline_ts,
                *,
                prepared_runtime=None,
            ):
                self.worker_deadlines.append(deadline_ts)
                return []

        node = ConsumerNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 90,
        }

        node._prepare_mpmc_round_before_ready(workers=1)
        node._run_mpmc_workers(workers=1)

        self.assertIsNotNone(node.start_time)
        self.assertIsNotNone(node.end_time)
        self.assertGreaterEqual(node.start_time, node.runtime_start_release_ts)
        self.assertEqual(node.worker_deadlines, [node.end_time])
        self.assertEqual(node.end_time - node.start_time, 90)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_prewarm_before_ready_defers_endpoint_until_start(self) -> None:
        class ProducerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.prepare_started = threading.Event()
                self.allow_prepare = threading.Event()

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                self.prepare_started.set()
                self.allow_prepare.wait(timeout=2.0)
                return PreparedWorkerRuntime(producer=_FakeClosableEndpoint())

            def _run_worker_thread(self, *args, **kwargs):
                return []

            def _wait_for_mpmc_runtime_start(self) -> bool:
                return True

            def _wait_mpmc_cluster_ready(self, **kwargs) -> None:
                return None

            def _prefeed_mpmc_producer_channels(self, **kwargs) -> None:
                return None

        node = ProducerNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "mpmc_producer_prefeed_leader": False,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
            "value_size": 256,
        }

        node._prepare_mpmc_round_before_ready(workers=1)

        self.assertIsNotNone(node._prepared_mpmc_round)
        round_state = node._prepared_mpmc_round
        self.assertEqual(sorted(round_state.pending_threads), [0])
        self.assertEqual(round_state.prepared_runtimes, {})
        self.assertIsNone(round_state.pending_threads[0].ident)
        self.assertFalse(node.prepare_started.is_set())

        run_thread = threading.Thread(
            target=node._run_mpmc_workers,
            kwargs={"workers": 1},
        )
        run_thread.start()
        self.assertTrue(node.prepare_started.wait(timeout=1.0))
        self.assertTrue(run_thread.is_alive())

        node.allow_prepare.set()
        run_thread.join(timeout=2.0)

        self.assertFalse(run_thread.is_alive())
        self.assertEqual(sorted(round_state.prepared_runtimes), [0])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_producer_prefeed_leader_is_explicit_coordinator_assignment(self) -> None:
        node = BenchmarkNode()
        node.test_config = {"mpmc_producer_prefeed_leader": True}
        self.assertTrue(node._is_mpmc_producer_prefeed_leader(thread_id=0))
        self.assertFalse(node._is_mpmc_producer_prefeed_leader(thread_id=1))

        node.test_config = {"mpmc_producer_prefeed_leader": False}
        self.assertFalse(node._is_mpmc_producer_prefeed_leader(thread_id=0))

        node.test_config = {}
        with self.assertRaisesRegex(RuntimeError, "explicit bool"):
            node._is_mpmc_producer_prefeed_leader(thread_id=0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_non_leader_mpmc_producer_skips_prefeed_after_start(self) -> None:
        class NonLeaderProducerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.events = []

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                return PreparedWorkerRuntime(producer=_FakeClosableEndpoint())

            def _wait_for_mpmc_runtime_start(self) -> bool:
                self.events.append("runtime_start")
                return False

            def _run_worker_thread(self, *args, **kwargs):
                return []

            def _prefeed_mpmc_producer_channels(self, **kwargs) -> None:
                self.events.append("prefeed")

        node = NonLeaderProducerNode()
        node.instance_key = (
            "bench_mq__largescale_mq__abc123__producer_0_proc_1"
        )
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "mpmc_producer_prefeed_leader": False,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
            "value_size": 256,
        }

        node._prepare_mpmc_round_before_ready(workers=1)
        node._run_mpmc_workers(workers=1)

        self.assertEqual(node.events, ["runtime_start"])
        self.assertIsNotNone(node._forced_benchmark_result)
        self.assertEqual(
            node._forced_benchmark_result["forced_failure_reason"],
            "mpmc_runtime_start_timeout",
        )

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_consumer_prewarm_before_ready_still_waits_for_endpoint(self) -> None:
        class ConsumerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.waited_cluster_ready = False

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                return PreparedWorkerRuntime(consumer=_FakeClosableEndpoint())

            def _wait_mpmc_cluster_ready(self, **kwargs) -> None:
                self.waited_cluster_ready = True

            def _run_worker_thread(self, *args, **kwargs):
                return []

            def _wait_for_mpmc_runtime_start(self) -> bool:
                return True

            def _prefeed_mpmc_producer_channels(self, **kwargs) -> None:
                return None

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
        node._finish_mpmc_round(
            round_state=node._prepared_mpmc_round,
            reason="test_consumer_prewarm_cleanup",
        )

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_producer_prefeeds_all_ready_channels(self) -> None:
        class PrefeedNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.cluster_ready_calls = []

            def _wait_mpmc_prefeed_topology(self, **kwargs):
                self.cluster_ready_calls.append(kwargs)
                return SimpleNamespace(ready_channel_ids=("11", "12", "13"))

        node = PrefeedNode()
        node.instance_key = "producer_0_proc_0"
        node.node_id = "node_producer_0"
        node.mq_state = node_mod.MQState(role="producer", weight=1.0)
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
            "value_size": 256,
        }
        producer = object()
        runtime = PreparedWorkerRuntime(
            producer=producer,
            local_mq_state=node_mod.MQState(
                role="producer",
                weight=1.0,
                producer_id="producer_0_proc_0",
            ),
        )
        put_calls = []

        def fake_mq_put_to_channel_once(actual_producer, channel_id, value):
            self.assertIs(actual_producer, producer)
            put_calls.append((channel_id, value))
            return None

        with mock.patch.object(
            node_mod,
            "mq_put_to_channel_once",
            side_effect=fake_mq_put_to_channel_once,
        ):
            node._prefeed_mpmc_producer_channels(
                runtime=runtime,
                thread_id=0,
                timeout_s=5.0,
            )

        self.assertEqual(
            len(put_calls),
            3 * node_mod.MPMC_PRODUCER_PREFEED_MESSAGES_PER_CHANNEL,
        )
        self.assertEqual(
            [channel_id for channel_id, _value in put_calls],
            ["11", "11", "12", "12", "13", "13"],
        )
        self.assertEqual(len(node.cluster_ready_calls), 2)
        self.assertEqual(node.cluster_ready_calls[0]["runtime"], runtime)
        self.assertGreater(node.cluster_ready_calls[0]["timeout_s"], 0.0)
        self.assertLessEqual(node.cluster_ready_calls[0]["timeout_s"], 5.0)
        for _channel_id, value in put_calls:
            header = json.loads(value["payload"].split(b"\n", 1)[0].decode("utf-8"))
            self.assertEqual(header["message_kind"], "prefeed")

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_producer_prefeed_retries_transient_put_failure(self) -> None:
        class PrefeedNode(BenchmarkNode):
            def _wait_mpmc_prefeed_topology(self, **_kwargs):
                return SimpleNamespace(ready_channel_ids=("11",))

        node = PrefeedNode()
        node.instance_key = "producer_0_proc_0"
        node.node_id = "node_producer_0"
        node.mq_state = node_mod.MQState(role="producer", weight=1.0)
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "value_size": 256,
        }
        node._runtime_init_retry_sleep_seconds = lambda attempt: 0.0  # type: ignore[method-assign]
        runtime = PreparedWorkerRuntime(
            producer=object(),
            local_mq_state=node_mod.MQState(
                role="producer",
                weight=1.0,
                producer_id="producer_0_proc_0",
            ),
        )

        with mock.patch.object(
            node_mod,
            "mq_put_to_channel_once",
            side_effect=[
                "MPSC channel is not ready for prefeed: channel_id=11",
                None,
                None,
            ],
        ) as put_mock, mock.patch.object(node_mod.time, "sleep", return_value=None):
            node._prefeed_mpmc_producer_channels(
                runtime=runtime,
                thread_id=0,
                timeout_s=5.0,
            )

        self.assertEqual(put_mock.call_count, 3)
        first_value = put_mock.call_args_list[0].args[2]
        retried_value = put_mock.call_args_list[1].args[2]
        self.assertEqual(first_value["unique_id"], retried_value["unique_id"])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_prefeed_waits_for_exact_stable_global_consumer_topology(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "expected_mpmc_consumer_workers": 2,
        }
        runtime = PreparedWorkerRuntime(producer=object())
        snapshots = [
            SimpleNamespace(
                mpmc_id="7",
                ready_channel_ids=("11",),
                active_consumers=1,
            ),
            SimpleNamespace(
                mpmc_id="7",
                ready_channel_ids=("11", "12"),
                active_consumers=2,
            ),
            SimpleNamespace(
                mpmc_id="7",
                ready_channel_ids=("11", "12"),
                active_consumers=2,
            ),
        ]

        with mock.patch.object(
            node_mod,
            "get_cluster_info_snapshot",
            side_effect=snapshots,
        ) as snapshot_mock, mock.patch.object(node_mod.time, "sleep", return_value=None):
            snapshot = node._wait_mpmc_prefeed_topology(
                runtime=runtime,
                timeout_s=5.0,
            )

        self.assertEqual(snapshot.ready_channel_ids, ("11", "12"))
        self.assertEqual(snapshot_mock.call_count, 3)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_prefeed_topology_retries_transient_observation_failure(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "expected_mpmc_consumer_workers": 1,
        }
        node._runtime_init_retry_sleep_seconds = lambda attempt: 0.0  # type: ignore[method-assign]
        ready = SimpleNamespace(
            mpmc_id="7",
            ready_channel_ids=("11",),
            active_consumers=1,
        )
        runtime = PreparedWorkerRuntime(producer=object())

        with mock.patch.object(
            node_mod,
            "get_cluster_info_snapshot",
            side_effect=[RuntimeError("etcd status probe timed out"), ready, ready],
        ) as snapshot_mock, mock.patch.object(node_mod.time, "sleep", return_value=None):
            snapshot = node._wait_mpmc_prefeed_topology(
                runtime=runtime,
                timeout_s=5.0,
            )

        self.assertEqual(snapshot.ready_channel_ids, ("11",))
        self.assertEqual(snapshot_mock.call_count, 3)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_prefeed_put_targets_the_declared_channel(self) -> None:
        import fluxon_test_stack.benchmark_node_mq as mq_mod
        from fluxon_py.api_error import Result

        put_values = []

        class FakeChannel:
            def put_data(self, value):
                put_values.append(value)
                return Result.new_ok(True)

        class FakeMPMCChannel:
            def get_remote_ready_channels(self):
                return ["11", "12"]

        class FakeProducer:
            def __init__(self) -> None:
                self.mpmc_channel = FakeMPMCChannel()
                self.bound_channel_ids = []

            def _new_or_get_mpsc_producer(self, channel_id):
                self.bound_channel_ids.append(channel_id)
                return FakeChannel()

        producer = FakeProducer()
        value = {"unique_id": "prefeed-1", "payload": b"value"}
        with mock.patch.object(mq_mod, "MPMCChanProducer", FakeProducer):
            err = mq_mod.mq_put_to_channel_once(producer, "12", value)

        self.assertIsNone(err)
        self.assertEqual(producer.bound_channel_ids, ["12"])
        self.assertEqual(put_values, [value])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_consumer_drains_and_validates_prefeed_before_runtime_ready(self) -> None:
        from fluxon_test_stack.benchmark_node_mq import MQGetOutcome

        node = BenchmarkNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
        }
        round_state = node_mod.PreparedMPMCRound(
            prepared_runtimes={
                0: PreparedWorkerRuntime(consumer=object()),
                1: PreparedWorkerRuntime(consumer=object()),
            }
        )
        outcomes = [
            MQGetOutcome(
                status=node_mod.MQGetStatus.DATA,
                ok=True,
                error_msg="",
                data_size=256,
                message_kind="prefeed",
            )
            for _ in range(4)
        ]
        with mock.patch.object(node_mod, "mq_get_once", side_effect=outcomes) as get_mock:
            node._consume_mpmc_prefeed_messages(round_state=round_state, timeout_s=5.0)

        self.assertEqual(get_mock.call_count, 4)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_consumer_rejects_benchmark_message_during_prefeed_drain(self) -> None:
        from fluxon_test_stack.benchmark_node_mq import MQGetOutcome

        node = BenchmarkNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
        }
        round_state = node_mod.PreparedMPMCRound(
            prepared_runtimes={0: PreparedWorkerRuntime(consumer=object())}
        )
        outcome = MQGetOutcome(
            status=node_mod.MQGetStatus.DATA,
            ok=True,
            error_msg="",
            data_size=256,
            message_kind="benchmark",
        )
        with mock.patch.object(node_mod, "mq_get_once", return_value=outcome):
            with self.assertRaisesRegex(RuntimeError, "non-prefeed message"):
                node._consume_mpmc_prefeed_messages(
                    round_state=round_state,
                    timeout_s=5.0,
                )

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_mpmc_runtime_start_gate_runs_before_timed_workers(self) -> None:
        class GatedProducerNode(BenchmarkNode):
            def __init__(self) -> None:
                super().__init__()
                self.runtime_start_checked = False
                self.events = []

            def _prepare_mpmc_worker_runtime_with_retry(self, **kwargs) -> PreparedWorkerRuntime:
                return PreparedWorkerRuntime(producer=_FakeClosableEndpoint())

            def _wait_for_mpmc_runtime_start(self) -> bool:
                self.events.append("runtime_start")
                self.runtime_start_checked = True
                return False

            def _run_worker_thread(self, *args, **kwargs):
                return []

            def _prefeed_mpmc_producer_channels(self, **kwargs) -> None:
                self.events.append("prefeed")

        node = GatedProducerNode()
        node.instance_key = (
            "bench_mq__largescale_mq__abc123__producer_0_proc_0"
        )
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "mpmc_producer_prefeed_leader": True,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
            "value_size": 256,
        }

        node._prepare_mpmc_round_before_ready(workers=1)
        node._run_mpmc_workers(workers=1)

        self.assertTrue(node.runtime_start_checked)
        self.assertEqual(node.events, ["prefeed", "runtime_start"])
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
        self.assertLessEqual(stagger_s, 72.0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_stagger_prioritizes_prefeed_producer(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "bench_mq__producer_0_proc_0"
        node.test_config = {
            "test_mode": TestMode.MPMC.value,
            "node_role": "producer",
            "expected_nodes": 168,
            "mpmc_producer_prefeed_leader": False,
        }

        with mock.patch.object(node, "_stable_fraction_from_text", return_value=0.5):
            self.assertEqual(node._runtime_init_stagger_seconds(), 114.0)
            node.test_config["mpmc_producer_prefeed_leader"] = True
            self.assertEqual(node._runtime_init_stagger_seconds(), 0.0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_retry_uses_equal_jittered_exponential_backoff(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "producer_7_proc_3"

        with mock.patch.object(node, "_stable_fraction_from_text", return_value=0.0):
            self.assertEqual(node._runtime_init_retry_sleep_seconds(attempt=1), 0.5)
            self.assertEqual(node._runtime_init_retry_sleep_seconds(attempt=2), 1.0)
            self.assertEqual(node._runtime_init_retry_sleep_seconds(attempt=6), 15.0)
        with mock.patch.object(node, "_stable_fraction_from_text", return_value=1.0):
            self.assertEqual(node._runtime_init_retry_sleep_seconds(attempt=1), 1.0)
            self.assertEqual(node._runtime_init_retry_sleep_seconds(attempt=2), 2.0)
            self.assertEqual(node._runtime_init_retry_sleep_seconds(attempt=6), 30.0)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_etcd_health_cache_is_workload_scoped(self) -> None:
        node = BenchmarkNode()
        node.coordinator_host = "127.0.0.1"
        node.coordinator_port = 7777
        node.mq_unique_id = "mq-run-a"
        node.test_config = {
            "test_mode": TestMode.MPMC.value,
            "expected_nodes": 328,
            "test_id": "run-1",
        }
        config = {
            "instance_key": "producer_0",
            "fluxonkv_spec": {
                "cluster_name": "bench-cluster",
                "share_mem_path": "/tmp/fluxon-test-shm/node-1",
            },
        }

        first_dir = node._runtime_init_etcd_health_scope_dir(config)
        self.assertIn("fluxon_mpmc_runtime_health", str(first_dir))

        same = BenchmarkNode()
        same.coordinator_host = node.coordinator_host
        same.coordinator_port = node.coordinator_port
        same.mq_unique_id = node.mq_unique_id
        same.test_config = dict(node.test_config)
        self.assertEqual(same._runtime_init_etcd_health_scope_dir(config), first_dir)

        other = BenchmarkNode()
        other.coordinator_host = node.coordinator_host
        other.coordinator_port = node.coordinator_port
        other.mq_unique_id = "mq-run-b"
        other.test_config = dict(node.test_config)
        self.assertNotEqual(other._runtime_init_etcd_health_scope_dir(config), first_dir)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_start_wait_poll_sleep_uses_bounded_backoff(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "producer_0_proc_0"

        first_sleep = node._start_wait_poll_sleep_seconds(
            attempt=1,
            remaining_seconds=100.0,
        )
        fifth_sleep = node._start_wait_poll_sleep_seconds(
            attempt=5,
            remaining_seconds=100.0,
        )
        capped_sleep = node._start_wait_poll_sleep_seconds(
            attempt=100,
            remaining_seconds=100.0,
        )
        short_remaining_sleep = node._start_wait_poll_sleep_seconds(
            attempt=100,
            remaining_seconds=0.25,
        )

        self.assertGreaterEqual(first_sleep, node_mod.START_WAIT_POLL_INTERVAL_SECONDS)
        self.assertGreater(fifth_sleep, first_sleep)
        self.assertLessEqual(
            capped_sleep,
            node_mod.START_WAIT_POLL_MAX_SECONDS + node_mod.START_WAIT_POLL_JITTER_SECONDS,
        )
        self.assertEqual(short_remaining_sleep, 0.25)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_etcd_health_urls_are_derived_from_kvcache_config(self) -> None:
        urls = BenchmarkNode._runtime_init_etcd_health_urls(
            {
                "fluxonkv_spec": {
                    "etcd_addresses": [
                        "127.0.0.1:2379",
                        "http://10.1.1.2:2380",
                        "http://10.1.1.2:2380/",
                    ],
                },
            }
        )

        self.assertEqual(
            urls,
            [
                "http://127.0.0.1:2379/health",
                "http://10.1.1.2:2380/health",
            ],
        )

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_etcd_health_urls_are_derived_from_owner_shared_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            share_mem_path = Path(td) / "shm1" / "node-1"
            cluster_name = "fluxon_benchmark_test"
            shared_dir = share_mem_path / cluster_name
            shared_dir.mkdir(parents=True)
            (shared_dir / "shared.json").write_text(
                json.dumps({"etcd_addresses": ["127.0.0.1:2379"]}),
                encoding="utf-8",
            )

            urls = BenchmarkNode._runtime_init_etcd_health_urls(
                {
                    "fluxonkv_spec": {
                        "cluster_name": cluster_name,
                        "share_mem_path": str(share_mem_path),
                    },
                }
            )

        self.assertEqual(urls, ["http://127.0.0.1:2379/health"])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_etcd_health_probe_waits_for_missing_owner_shared_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            node = BenchmarkNode()
            healthy, detail = node._probe_runtime_init_etcd_health(
                {
                    "fluxonkv_spec": {
                        "cluster_name": "fluxon_benchmark_test",
                        "share_mem_path": str(Path(td) / "shm1" / "node-1"),
                    },
                }
            )

        self.assertFalse(healthy)
        self.assertIn("owner shared.json not ready", detail)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_waits_for_etcd_health_before_initialization(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "test_mode": TestMode.MPMC.value,
            "expected_nodes": 328,
        }
        node._runtime_init_retry_sleep_seconds = lambda attempt: 0.0  # type: ignore[method-assign]
        config = {
            "fluxonkv_spec": {
                "etcd_addresses": ["127.0.0.1:2379"],
            },
        }
        probes = [(False, "timeout"), (True, "http://127.0.0.1:2379/health")]
        sleeps = []

        with mock.patch.object(node, "_probe_runtime_init_etcd_health_shared", side_effect=probes):
            with mock.patch.object(node_mod.time, "sleep", side_effect=lambda seconds: sleeps.append(seconds)):
                err = node._wait_for_runtime_init_etcd_health(
                    config,
                    deadline_ts=time.monotonic() + 1.0,
                    ctx="test",
                )

        self.assertIsNone(err)
        self.assertEqual(sleeps, [0.0])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_runtime_init_etcd_health_probe_uses_shared_cache(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            node = BenchmarkNode()
            node.coordinator_host = "127.0.0.1"
            node.coordinator_port = 7777
            node.mq_unique_id = "mq-run-a"
            node.test_config = {
                "test_mode": TestMode.MPMC.value,
                "expected_nodes": 328,
                "test_id": "run-1",
            }
            config = {
                "fluxonkv_spec": {
                    "cluster_name": "bench-cluster",
                    "etcd_addresses": ["127.0.0.1:2379"],
                },
            }
            calls = []

            def fake_probe(_config):
                calls.append("probe")
                return True, "http://127.0.0.1:2379/health"

            with mock.patch.object(node_mod, "RUNTIME_INIT_ETCD_HEALTH_ROOT", Path(td) / "health"):
                with mock.patch.object(node, "_probe_runtime_init_etcd_health", side_effect=fake_probe):
                    first = node._probe_runtime_init_etcd_health_shared(config)
                    second = node._probe_runtime_init_etcd_health_shared(config)

        self.assertEqual(first[0], True)
        self.assertEqual(second[0], True)
        self.assertEqual(calls, ["probe"])
        self.assertIn("cache=", second[1])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_kv_store_init_retries_transient_errors(self) -> None:
        node = BenchmarkNode()
        node.test_config = {
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
        }
        node._runtime_init_retry_sleep_seconds = lambda attempt: 0.0  # type: ignore[method-assign]
        calls = []
        sentinel_store = _new_fake_fluxon_benchmark_store()
        original_init_kv_store = node_mod.init_kv_store

        def fake_init_kv_store(config, **_kwargs):
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
    def test_endpoint_prepare_never_retries_pyo3_style_panic(self) -> None:
        node = BenchmarkNode()
        node._runtime_init_retry_sleep_seconds = lambda attempt: 0.0  # type: ignore[method-assign]
        calls = []
        class SyntheticPanic(BaseException):
            pass

        def fake_prepare(**_kwargs):
            calls.append("prepare")
            raise SyntheticPanic(
                "tcp_thread reactor spawn failed: timed out waiting on channel"
            )

        with mock.patch.object(node, "_prepare_mpmc_worker_runtime", side_effect=fake_prepare):
            with self.assertRaises(SyntheticPanic):
                node._prepare_mpmc_worker_runtime_with_retry(
                    thread_id=0,
                    deadline_ts=time.monotonic() + 5.0,
                    ctx="test",
                )

        self.assertEqual(calls, ["prepare"])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_worker_panic_is_forwarded_to_main_thread_without_prepare_timeout(self) -> None:
        class SyntheticPanic(BaseException):
            pass

        class PanicNode(BenchmarkNode):
            def _prepare_mpmc_worker_runtime_with_retry(self, **_kwargs):
                raise SyntheticPanic("fatal runtime panic")

            def _close_kv_store(self, *, reason: str) -> None:
                self.closed_reason = reason

        node = PanicNode()
        node.test_config = {
            "node_role": "consumer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 30,
            "threads_per_process": 1,
        }

        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "runtime is not recoverable") as ctx:
            node._prepare_mpmc_round_before_ready(workers=1)

        self.assertIsInstance(ctx.exception.__cause__, SyntheticPanic)
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertIn("mpmc_fatal_worker_error", node.closed_reason)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_process_shared_runtime_initializes_kv_before_mq_attach(self) -> None:
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
        sentinel_store = _new_fake_fluxon_benchmark_store()
        sentinel_producer = object()
        original_init_mq_channel = node_mod.init_mq_channel

        def fake_init_kv_store(config, **_kwargs):
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
        self.assertIs(node.kv_store, sentinel_store)
        self.assertEqual(init_kv_configs[0]["instance_key"], "producer_0_proc_0__worker_0")
        self.assertEqual(events, ["kv_init", "mq_attach"])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_workers_reuse_one_process_kv_runtime(self) -> None:
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

        init_kv_configs = []
        attach_stores = []
        sentinel_store = _new_fake_fluxon_benchmark_store()

        def fake_init_kv_store(config, **_kwargs):
            init_kv_configs.append(config)
            return sentinel_store, None

        def fake_init_mq_channel(**kwargs):
            attach_stores.append(kwargs["kv_store"])
            return object(), None, None

        node._sleep_for_runtime_init_stagger = lambda **kwargs: None  # type: ignore[method-assign]
        with mock.patch.object(node_mod, "init_mq_channel", side_effect=fake_init_mq_channel):
            with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fake_init_kv_store):
                first = node._prepare_mpmc_worker_runtime(thread_id=0)
                second = node._prepare_mpmc_worker_runtime(thread_id=1)

        self.assertEqual(len(init_kv_configs), 1)
        self.assertEqual(
            attach_stores,
            [sentinel_store.kv_client, sentinel_store.kv_client],
        )
        self.assertIs(node.kv_store, sentinel_store)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_producer_runtime_initializes_kv_then_attaches_mq_without_global_gate(self) -> None:
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
            "expected_nodes": 328,
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
        sentinel_store = _new_fake_fluxon_benchmark_store()
        sentinel_producer = object()
        original_init_mq_channel = node_mod.init_mq_channel

        def fake_init_kv_store(config, **kwargs):
            events.append("kv_init")
            self.assertEqual(config["instance_key"], "producer_0_proc_0__worker_0")
            self.assertGreater(kwargs["deadline_ts"], time.monotonic())
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
        self.assertEqual(events, ["kv_init", "mq_attach"])

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_logical_only_producer_runtime_skips_kv_and_mq_attach(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "producer_200_proc_0"
        node.node_id = "node_producer_logical"
        node.mq_unique_id = "mpmc-test"
        node.mq_state = node_mod.MQState(role="producer", weight=1.0)
        node.chan_config = {}
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "mpmc_producer_runtime_mode": "logical_only",
            "kvcache_config": {
                "instance_key": "producer_200_proc_0",
                "fluxonkv_spec": {
                    "share_mem_path": "/tmp/fluxon-test-shm/node-1",
                    "cluster_name": "fluxon_benchmark",
                    "p2p_listen_port": 12000,
                },
            },
        }

        def fail_init_kv_store(*_args, **_kwargs):
            raise AssertionError("logical-only producer must not initialize KV")

        def fail_init_mq_channel(**_kwargs):
            raise AssertionError("logical-only producer must not attach MQ")

        original_init_mq_channel = node_mod.init_mq_channel
        node_mod.init_mq_channel = fail_init_mq_channel
        try:
            with mock.patch.object(node, "_init_kv_store_with_ready_retry", side_effect=fail_init_kv_store):
                runtime = node._prepare_mpmc_worker_runtime(thread_id=0)
        finally:
            node_mod.init_mq_channel = original_init_mq_channel

        self.assertTrue(runtime.logical_only)
        self.assertIsNone(runtime.producer)
        self.assertIsNone(runtime.consumer)

    @unittest.skipIf(node_mod is None, f"distributed benchmark node import failed: {NODE_RUNTIME_IMPORT_ERROR}")
    def test_logical_only_producer_worker_reports_empty_successful_result_set(self) -> None:
        node = BenchmarkNode()
        node.instance_key = "producer_200_proc_0"
        node.node_id = "node_producer_logical"
        node.test_config = {
            "node_role": "producer",
            "test_mode": TestMode.MPMC.value,
            "cluster_ready_timeout_seconds": 5,
            "threads_per_process": 1,
            "max_benchmark_seconds": 5,
            "value_size": 256,
            "mpmc_producer_runtime_mode": "logical_only",
        }
        runtime = PreparedWorkerRuntime(
            logical_only=True,
        )

        with mock.patch.object(node_mod, "run_fs_worker", return_value=None):
            with mock.patch.object(node_mod, "run_rpc_worker", return_value=None):
                with mock.patch.object(node_mod, "run_kv_worker", return_value=None):
                    results = node._run_worker_thread(
                        0,
                        time.time() + 1.0,
                        prepared_runtime=runtime,
                    )

        self.assertEqual(results, [])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
