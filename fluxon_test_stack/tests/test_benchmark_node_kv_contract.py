from __future__ import annotations

import ctypes
import gc
import logging
import struct
import sys
import threading
import time
import unittest
import weakref
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_STACK_DIR = REPO_ROOT / "fluxon_test_stack"
sys.path[:0] = [str(TEST_STACK_DIR), str(REPO_ROOT)]
try:
    from fluxon_test_stack import benchmark_node_kv as _KV
finally:
    for import_path in (str(TEST_STACK_DIR), str(REPO_ROOT)):
        if import_path in sys.path:
            sys.path.remove(import_path)

from fluxon_py.api_error import OkNone, Result


class _PutResult:
    def __init__(self, *, success: bool, error_msg: str | None = None) -> None:
        self.success = success
        self.error_msg = error_msg


class _FakeHolder:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.access_count = 0

    def access(self):
        self.access_count += 1
        return _KV._SimpleResult.ok({"payload": self.payload})

    def _benchmark_source_kind(self) -> str:
        return "memory"


class _FakeKvStore:
    def __init__(self, holder: _FakeHolder) -> None:
        self.holder = holder
        self.put_value = None
        self.put_keys: list[str] = []
        self.put_opts: list[object] = []

    def get_blocking(self, key: str):
        del key
        return _KV._SimpleResult.ok(self.holder)

    def put_blocking(self, key: str, value, *, opts):
        self.put_keys.append(key)
        self.put_opts.append(opts)
        self.put_value = value
        return _KV._SimpleResult.ok(None)

    def close(self):
        return _KV._SimpleResult.ok(None)


class _FakeCudaRuntime:
    def __init__(self) -> None:
        self._next_handle = 1
        self._host_buffers: dict[int, ctypes.Array] = {}
        self._device_buffers: dict[int, ctypes.Array] = {}
        self._event_ready: dict[int, bool] = {}
        self.fail_next_event_synchronize = False
        self.event_synchronize_started = threading.Event()
        self.event_synchronize_gate: threading.Event | None = None
        self.copied_payloads: list[bytes] = []

    def _handle(self) -> int:
        handle = self._next_handle
        self._next_handle += 1
        return handle

    def set_device(self, device_index: int) -> None:
        if device_index < 0:
            raise ValueError("device index must be non-negative")

    def malloc(self, size: int) -> int:
        buffer = ctypes.create_string_buffer(size)
        ptr = ctypes.addressof(buffer)
        self._device_buffers[ptr] = buffer
        return ptr

    def free(self, ptr: int) -> None:
        self._device_buffers.pop(ptr)

    def host_alloc(self, size: int) -> int:
        buffer = ctypes.create_string_buffer(size)
        ptr = ctypes.addressof(buffer)
        self._host_buffers[ptr] = buffer
        return ptr

    def host_free(self, ptr: int) -> None:
        self._host_buffers.pop(ptr)

    def stream_create(self) -> int:
        return self._handle()

    def stream_synchronize(self, stream: int) -> None:
        del stream

    def stream_destroy(self, stream: int) -> None:
        del stream

    def event_create(self) -> int:
        event = self._handle()
        self._event_ready[event] = False
        return event

    def event_record(self, event: int, stream: int) -> None:
        del stream
        self._event_ready[event] = False

    def event_query(self, event: int) -> bool:
        return self._event_ready[event]

    def event_synchronize(self, event: int) -> None:
        self.event_synchronize_started.set()
        if self.event_synchronize_gate is not None:
            if not self.event_synchronize_gate.wait(timeout=1.0):
                raise RuntimeError("timed out waiting for fake CUDA event gate")
        if self.fail_next_event_synchronize:
            self.fail_next_event_synchronize = False
            raise RuntimeError("injected CUDA event failure")
        self._event_ready[event] = True

    def event_elapsed_us(self, start_event: int, done_event: int) -> float:
        del start_event, done_event
        return 25.0

    def complete_all_events(self) -> None:
        for event in self._event_ready:
            self._event_ready[event] = True

    def event_destroy(self, event: int) -> None:
        self._event_ready.pop(event)

    def memcpy_host_to_device_async(
        self,
        *,
        device_ptr: int,
        host_ptr: int,
        size: int,
        stream: int,
    ) -> None:
        del stream
        ctypes.memmove(device_ptr, host_ptr, size)
        self.copied_payloads.append(ctypes.string_at(device_ptr, size))


class _KeepAlive:
    pass


class _FakeMooncakeBuffer(bytearray):
    def ptr(self) -> int:
        return ctypes.addressof(ctypes.c_ubyte.from_buffer(self))

    def size(self) -> int:
        return len(self)


class _FakeMooncakeStore:
    def __init__(self, encoded_payload: bytes, *, on_get=None) -> None:
        self._encoded_payload = encoded_payload
        self._on_get = on_get
        self.get_count = 0

    def get_buffer_blocking(self, key: str):
        del key
        self.get_count += 1
        if self._on_get is not None:
            self._on_get(self.get_count)
        return _KV._SimpleResult.ok(_FakeMooncakeBuffer(self._encoded_payload))


class _FakeCounterStore:
    def __init__(self, counts: list[int]) -> None:
        self._counts = iter(counts)

    def _benchmark_offload_rpc_read_count(self) -> int:
        return next(self._counts)


class _FakeOperationResult:
    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.node_id = None
        self.worker_id = None
        self.finish_ts = 0.0


class _FakeOperationOutcome:
    SUCCESS = "success"
    ERROR = "error"
    CACHE_HIT = "cache_hit"
    CACHE_MISS = "cache_miss"


class _FakeCudaBenchmarkNode:
    def __init__(self, adapter, *, stop_after_gets: int) -> None:
        self.test_config = {
            "test_mode": _KV.TEST_MODE_KVSTORE,
            "node_role": "worker",
            "key_prefix": "bench",
            "read_ratio": 1.0,
            "write_ratio": 0.0,
            "request_distribution": _KV.REQUEST_DISTRIBUTION_UNIFORM,
            "keyspace_size": 1,
            "op_timeout_seconds": 1.0,
            "value_size": 12,
            _KV.BENCHMARK_KEY_KV_GET_OUTPUT: _KV.KVGetOutput.CUDA.value,
        }
        self.kv_store = adapter
        self.key_prefix = "bench"
        self.value_size_mode = _KV.VALUE_SIZE_MODE_FIXED
        self.node_id = "node0"
        self.instance_key = "instance0"
        self.start_time = None
        self._benchmark_stop = threading.Event()
        self._inflight = 0
        self.progress: list[int] = []
        self.stop_after_gets = int(stop_after_gets)

    def _resolve_kv_value_size(self, thread_id: int, key_idx: int) -> int:
        del thread_id, key_idx
        return 12

    def _generate_test_data(self, value_size: int) -> bytes:
        return b"x" * value_size

    def _inflight_begin(self) -> int:
        self._inflight += 1
        return self._inflight

    def _inflight_end(self) -> int:
        self._inflight -= 1
        if self._inflight < 0:
            raise AssertionError("negative in-flight count")
        return self._inflight

    def _mark_progress(
        self,
        *,
        thread_id: int,
        op_idx: int,
        finish_ts: float,
        latency_us: float,
    ) -> None:
        del thread_id, finish_ts, latency_us
        self.progress.append(op_idx)

    def _get_single_operation(self, *args, **kwargs):
        del args, kwargs
        raise AssertionError("CUDA worker must use the pipelined GET path")


class _FakeBenchmarkNode:
    def __init__(self) -> None:
        self.test_config = {
            "test_mode": _KV.TEST_MODE_KVSTORE,
            "node_role": "worker",
            "key_prefix": "bench",
            "read_ratio": 0.9,
            "write_ratio": 0.1,
            "request_distribution": _KV.REQUEST_DISTRIBUTION_UNIFORM,
            "keyspace_size": 5,
            "kv_bootstrap_before_ready": True,
            "kv_bootstrap_concurrency": 1,
            "kv_bootstrap_storage_full_policy": _KV.KV_BOOTSTRAP_STORAGE_FULL_POLICY_STOP,
            "op_timeout_seconds": 1,
            "value_size_mode": _KV.VALUE_SIZE_MODE_FIXED,
            "value_size": 1,
        }
        self.kv_store = object()
        self.key_prefix = "bench"
        self.value_size_mode = _KV.VALUE_SIZE_MODE_FIXED
        self.node_id = "node0"
        self.put_keys: list[str] = []

    def _resolve_kv_value_size(self, thread_id: int, key_idx: int) -> int:
        del thread_id, key_idx
        return 1

    def _generate_test_data(self, value_size: int) -> bytes:
        return b"x" * int(value_size)

    def _put_single_operation(
        self,
        key: str,
        value: bytes,
        inflight_at_start: int,
        *,
        deadline_ts: float,
        ctx: str,
    ) -> _PutResult:
        del value, inflight_at_start, deadline_ts, ctx
        self.put_keys.append(key)
        if len(self.put_keys) <= 3:
            return _PutResult(success=True)
        return _PutResult(success=False, error_msg="StorageFullError: No space left")


class _FakeFluxonStore:
    def __init__(self) -> None:
        self._client = object()
        self.zero_contribution_checked = False
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def _record(self, name: str, *args: object, **kwargs: object) -> str:
        self.calls.append((name, args, kwargs))
        return f"{name}-result"

    def put(self, *args: object, **kwargs: object) -> str:
        return self._record("put", *args, **kwargs)

    def get(self, *args: object, **kwargs: object) -> str:
        return self._record("get", *args, **kwargs)

    def get_size(self, *args: object, **kwargs: object) -> str:
        return self._record("get_size", *args, **kwargs)

    def is_exist(self, *args: object, **kwargs: object) -> str:
        return self._record("is_exist", *args, **kwargs)

    def remove(self, *args: object, **kwargs: object) -> str:
        return self._record("remove", *args, **kwargs)

    def sync_kv_to_file(self, *args: object, **kwargs: object) -> str:
        return self._record("sync_kv_to_file", *args, **kwargs)

    def instance_key(self) -> Result[str, object]:
        return Result.new_ok("bench-instance")

    def config(self) -> str:
        return "bench-config"

    def get_cluster_name(self) -> str:
        return "fluxon_benchmark"

    def get_etcd_config(self) -> list[str]:
        return ["127.0.0.1:2379"]

    def third_party_logs_dir(self) -> Result[str, object]:
        return Result.new_ok("/tmp/fluxon-logs")

    def ensure_zero_contribution_for_channel(self) -> None:
        self.zero_contribution_checked = True

    def count_prefix(self, prefix: str) -> Result[int, object]:
        self.calls.append(("count_prefix", (prefix,), {}))
        return Result.new_ok(3)

    def allocate_lease(self, ttl_seconds: int) -> Result[int, object]:
        self.calls.append(("allocate_lease", (ttl_seconds,), {}))
        return Result.new_ok(42)

    def keepalive_lease(self, lease_id: int) -> Result[OkNone, object]:
        self.calls.append(("keepalive_lease", (lease_id,), {}))
        return Result.new_ok(OkNone())

    def close(self) -> Result[OkNone, object]:
        return Result.new_ok(OkNone())


class TestBenchmarkNodeKvContract(unittest.TestCase):
    def test_benchmark_client_strips_owner_only_ssd_backend_fields(self) -> None:
        sanitized = _KV._sanitize_benchmark_client_kvcache_config(
            {
                "test_spec_config": {
                    "kv_ssd_storage_backend": "foyer",
                    "kv_ssd_uring_mode": "single_buffer",
                    "disable_observability": True,
                }
            }
        )
        self.assertEqual(
            sanitized["test_spec_config"],
            {"disable_observability": True},
        )

    def test_get_output_is_a_closed_enum(self) -> None:
        self.assertEqual(
            _KV.normalize_kv_get_output(None),
            _KV.KVGetOutput.HOLDER,
        )
        self.assertEqual(
            _KV.normalize_kv_get_output("CUDA"),
            _KV.KVGetOutput.CUDA,
        )
        with self.assertRaisesRegex(ValueError, "kv_get_output must be one of"):
            _KV.normalize_kv_get_output("length_only")

    def test_cuda_device_index_is_a_non_negative_integer(self) -> None:
        self.assertEqual(_KV.normalize_kv_cuda_device_index(None), 0)
        self.assertEqual(_KV.normalize_kv_cuda_device_index(6), 6)
        for invalid in (-1, True, "6"):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "kv_cuda_device_index"):
                    _KV.normalize_kv_cuda_device_index(invalid)

    def test_flat_dict_payload_view_excludes_encoding_metadata(self) -> None:
        payload = b"payload-data"
        key = b"payload"
        encoded = (
            struct.pack("<I", 1)
            + struct.pack("<I", len(key))
            + key
            + struct.pack("<BI", 5, len(payload))
            + payload
        )

        view = _KV._flat_dict_payload_view(memoryview(encoded), len(payload))

        self.assertEqual(bytes(view), payload)
        self.assertLess(len(view), len(encoded))

    def test_bytes_output_materializes_memholder_payload(self) -> None:
        payload = b"x" * 32
        holder = _FakeHolder(payload)
        adapter = _KV.KVBenchmarkBlockingStore(
            _FakeKvStore(holder),
            backend_kind=_KV.BACKEND_KIND_FLUXON,
            get_output=_KV.KVGetOutput.BYTES,
        )

        completion = adapter.get_blocking(
            "key",
            deadline_ts=10**10,
            ctx="test",
            expected_payload_size=len(payload),
        )

        self.assertIsNone(completion.error_msg)
        self.assertEqual(completion.source_kind, _KV.KVGetSourceKind.MEMORY)
        self.assertEqual(holder.access_count, 1)

    def test_mooncake_counter_window_reports_ssd_read_delta(self) -> None:
        boundary_ts = time.time() - 1.0
        sampler = _KV._MooncakeOffloadReadCounterWindow(
            _FakeCounterStore([17, 23]),
            start_ts=boundary_ts,
            end_ts=boundary_ts,
        )

        sampler.start()
        summary = sampler.finish()

        self.assertTrue(summary["complete"])
        self.assertEqual(summary["ssd_read_count"], 6)

    def test_cuda_output_put_uses_cpu_dlpack_payload(self) -> None:
        payload = b"x" * 32
        store = _FakeKvStore(_FakeHolder(payload))
        adapter = object.__new__(_KV.KVBenchmarkBlockingStore)
        adapter.backend_kind = _KV.BACKEND_KIND_FLUXON
        adapter._store = store
        adapter._get_output = _KV.KVGetOutput.CUDA
        adapter._phase_profiler = _KV._FluxonPhaseProfiler()
        adapter._put_dedupe_condition = threading.Condition()
        adapter._put_confirmed_keys = set()
        adapter._put_inflight_keys = set()

        err = adapter.put_blocking(
            "key",
            payload,
            deadline_ts=10**10,
            ctx="test",
        )

        self.assertIsNone(err)
        self.assertIsInstance(store.put_value["payload"], _KV.DLPackBytesView)

    def test_put_rejects_existing_keys_and_deduplicates_locally(self) -> None:
        payload = b"payload-data"
        store = _FakeKvStore(_FakeHolder(payload))
        adapter = _KV.KVBenchmarkBlockingStore(
            store,
            backend_kind=_KV.BACKEND_KIND_FLUXON,
            get_output=_KV.KVGetOutput.HOLDER,
        )

        first_error = adapter.put_blocking(
            "same-key",
            payload,
            deadline_ts=10**10,
            ctx="first",
        )
        duplicate_error = adapter.put_blocking(
            "same-key",
            payload,
            deadline_ts=10**10,
            ctx="duplicate",
        )

        self.assertIsNone(first_error)
        self.assertIsNone(duplicate_error)
        self.assertEqual(store.put_keys, ["same-key"])
        self.assertEqual(len(store.put_opts), 1)
        self.assertTrue(store.put_opts[0].reject_if_exists)

    def test_cuda_pipeline_retains_host_reference_until_event_completion(self) -> None:
        runtime = _FakeCudaRuntime()
        pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=2,
            runtime=runtime,
        )
        source = ctypes.create_string_buffer(b"payload")
        keepalive = _KeepAlive()
        keepalive_ref = weakref.ref(keepalive)

        completions = pipeline.submit_from_host(
            source_ptr=ctypes.addressof(source),
            size=len(source.raw),
            keepalive=keepalive,
            token="copy-0",
        )
        del keepalive
        gc.collect()

        self.assertEqual(completions, [])
        self.assertIsNotNone(keepalive_ref())
        drained = pipeline.drain_current_thread()
        gc.collect()
        self.assertEqual([completion.token for completion in drained], ["copy-0"])
        self.assertIsNone(keepalive_ref())
        pipeline.close()

    def test_cuda_pipeline_submit_does_not_wait_for_the_h2d_event(self) -> None:
        runtime = _FakeCudaRuntime()
        runtime.event_synchronize_gate = threading.Event()
        pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=2,
            runtime=runtime,
        )
        source = ctypes.create_string_buffer(b"payload")

        submitted = pipeline.submit_from_host(
            source_ptr=ctypes.addressof(source),
            size=len(source.raw),
            keepalive=source,
            token="copy-0",
        )

        self.assertEqual(submitted, [])
        self.assertFalse(runtime.event_synchronize_started.is_set())
        self.assertEqual(pipeline.poll_current_thread(), [])
        runtime.event_synchronize_gate.set()
        self.assertEqual(
            [completion.token for completion in pipeline.drain_current_thread()],
            ["copy-0"],
        )
        pipeline.close()

    def test_cuda_pipeline_poll_collects_a_ready_event_without_waiting(self) -> None:
        runtime = _FakeCudaRuntime()
        pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=2,
            runtime=runtime,
        )
        source = ctypes.create_string_buffer(b"payload")

        pipeline.submit_from_host(
            source_ptr=ctypes.addressof(source),
            size=len(source.raw),
            keepalive=source,
            token="copy-0",
        )
        runtime.complete_all_events()

        completions = pipeline.poll_current_thread()

        self.assertEqual([completion.token for completion in completions], ["copy-0"])
        self.assertFalse(runtime.event_synchronize_started.is_set())
        pipeline.close()

    def test_cuda_pipeline_is_bounded_and_drains_in_submission_order(self) -> None:
        runtime = _FakeCudaRuntime()
        pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=2,
            runtime=runtime,
        )
        sources = [ctypes.create_string_buffer(value) for value in (b"a", b"b", b"c")]

        first = pipeline.submit_from_host(
            source_ptr=ctypes.addressof(sources[0]),
            size=1,
            keepalive=sources[0],
            token="copy-0",
        )
        second = pipeline.submit_from_host(
            source_ptr=ctypes.addressof(sources[1]),
            size=1,
            keepalive=sources[1],
            token="copy-1",
        )
        third = pipeline.submit_from_host(
            source_ptr=ctypes.addressof(sources[2]),
            size=1,
            keepalive=sources[2],
            token="copy-2",
        )

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual([completion.token for completion in third], ["copy-0"])
        self.assertEqual(
            [completion.token for completion in pipeline.drain_current_thread()],
            ["copy-1", "copy-2"],
        )
        self.assertEqual(runtime.copied_payloads, [b"a", b"b", b"c"])
        pipeline.close()

    def test_cuda_pipeline_reports_async_completion_error(self) -> None:
        runtime = _FakeCudaRuntime()
        pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=1,
            runtime=runtime,
        )
        source = ctypes.create_string_buffer(b"x")
        runtime.fail_next_event_synchronize = True
        pipeline.submit_from_host(
            source_ptr=ctypes.addressof(source),
            size=1,
            keepalive=source,
            token="copy-0",
        )

        completions = pipeline.drain_current_thread()

        self.assertEqual(len(completions), 1)
        self.assertEqual(completions[0].token, "copy-0")
        self.assertIn("injected CUDA event failure", completions[0].error_msg or "")
        pipeline.close()

    def test_cuda_store_counts_get_only_after_h2d_event_completion(self) -> None:
        payload = b"payload-data"
        key = b"payload"
        encoded = (
            struct.pack("<I", 1)
            + struct.pack("<I", len(key))
            + key
            + struct.pack("<BI", 5, len(payload))
            + payload
        )
        runtime = _FakeCudaRuntime()
        adapter = object.__new__(_KV.KVBenchmarkBlockingStore)
        adapter.backend_kind = _KV.BACKEND_KIND_MOONCAKE
        adapter._store = _FakeMooncakeStore(encoded)
        adapter._mooncake_store = adapter._store
        adapter._get_output = _KV.KVGetOutput.CUDA
        adapter._phase_profiler = _KV._FluxonPhaseProfiler()
        adapter._cuda_pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=2,
            runtime=runtime,
        )

        submitted = adapter.submit_cuda_get(
            "key",
            deadline_ts=10**10,
            ctx="test",
            expected_payload_size=len(payload),
            token="get-0",
        )
        completed = adapter.drain_cuda_gets()

        self.assertEqual(submitted, [])
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].token, "get-0")
        self.assertIsNone(completed[0].error_msg)
        self.assertEqual(runtime.copied_payloads, [payload])
        phase_summary = adapter.phase_summary()["GET"]
        self.assertEqual(phase_summary["count"], 1)
        self.assertIn("cuda_backend_get_us", phase_summary["extra_avg_us"])
        self.assertIn("cuda_host_stage_us", phase_summary["extra_avg_us"])
        self.assertIn("cuda_submit_us", phase_summary["extra_avg_us"])
        self.assertIn("cuda_h2d_event_us", phase_summary["extra_avg_us"])
        adapter._cuda_pipeline.close()

    def test_cuda_worker_drains_pending_gets_without_double_ending_inflight(self) -> None:
        payload = b"payload-data"
        key = b"payload"
        encoded = (
            struct.pack("<I", 1)
            + struct.pack("<I", len(key))
            + key
            + struct.pack("<BI", 5, len(payload))
            + payload
        )
        runtime = _FakeCudaRuntime()
        store = _FakeMooncakeStore(encoded)
        adapter = object.__new__(_KV.KVBenchmarkBlockingStore)
        adapter.backend_kind = _KV.BACKEND_KIND_MOONCAKE
        adapter._store = store
        adapter._mooncake_store = store
        adapter._get_output = _KV.KVGetOutput.CUDA
        adapter._phase_profiler = _KV._FluxonPhaseProfiler()
        adapter._cuda_pipeline = _KV._CudaHostToDevicePipeline(
            device_index=0,
            depth=2,
            runtime=runtime,
        )
        node = _FakeCudaBenchmarkNode(adapter, stop_after_gets=3)
        store._on_get = lambda count: (
            node._benchmark_stop.set() if count >= node.stop_after_gets else None
        )

        results = _KV.run_kv_worker(
            node,
            thread_id=0,
            deadline_ts=time.time() + 1.0,
            operation_result_cls=_FakeOperationResult,
            operation_outcome=_FakeOperationOutcome,
            metric_warmup_seconds=0.0,
            debug_print=lambda message: None,
        )

        self.assertIsNotNone(results)
        assert results is not None
        self.assertEqual(store.get_count, 3)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(result.success for result in results))
        self.assertEqual(node._inflight, 0)
        self.assertEqual(sorted(node.progress), [0, 1, 2])
        adapter._cuda_pipeline.close()

    def test_bootstrap_stop_policy_shrinks_effective_keyspace_on_storage_full(self) -> None:
        node = _FakeBenchmarkNode()

        ok = _KV.prepare_kv_before_ready(node, logger=logging.getLogger(__name__))

        self.assertTrue(ok)
        self.assertEqual(node.put_keys, ["bench_k0", "bench_k1", "bench_k2", "bench_k3"])
        self.assertEqual(node.test_config["keyspace_size"], 3)


class TestFluxonBlockingStoreContract(unittest.TestCase):
    def test_channel_client_remains_the_typed_inner_owner(self) -> None:
        raw_store = _FakeFluxonStore()
        store = _KV.FluxonBlockingStore(
            raw_store,  # type: ignore[arg-type]
            get_output=_KV.KVGetOutput.HOLDER,
        )

        self.assertIsInstance(store, _KV.KVBenchmarkBlockingStore)
        self.assertEqual(store.backend_kind, _KV.BACKEND_KIND_FLUXON)
        self.assertIs(store.kv_client, raw_store)
        self.assertFalse(hasattr(store, "_client"))
        self.assertFalse(hasattr(store, "get_etcd_config"))
        self.assertFalse(hasattr(store, "allocate_lease"))
        self.assertFalse(hasattr(store, "rpc_call"))


if __name__ == "__main__":
    unittest.main()
