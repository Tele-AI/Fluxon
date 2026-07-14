import types
import threading
import time
import unittest
from unittest import mock

from fluxon_py._api_ext_chan import mpmc as mpmc_module
from fluxon_py._api_ext_chan.mpmc import MPMCChanConsumer, MPMCChanProducer, MPMCChannel
from fluxon_py._api_ext_chan import mpsc as mpsc_module
from fluxon_py._api_ext_chan.mpsc import (
    ChanRole,
    MPSCChanConsumer,
    MPSCChanProducer,
)
from fluxon_py._api_ext_chan.mq_lifecycle import MqShutdownCtl
from fluxon_py._api_ext_chan.utils import TimedPriorityQueue
from fluxon_py.api_error import (
    ChannelClosedError,
    NetworkError,
    OkNone,
    ProducerClosedError,
    ResourceCleanupError,
    Result,
)


class _FakeMpmcChannel:
    def __init__(self, ready_channels, *, mpmc_member_id=1, producer_member_ids=None):
        self.ready_channels = list(ready_channels)
        self.refresh_count = 0
        self.etcd_client = object()
        self.mpmc_member_id = mpmc_member_id
        self.producer_member_ids = (
            list(producer_member_ids)
            if producer_member_ids is not None
            else [mpmc_member_id]
        )

    def _refresh_local_ready_state(self, client):
        self.last_refresh_client = client
        self.refresh_count += 1

    def get_ready_channels(self):
        return list(self.ready_channels)

    def get_active_member_ids(self, role):
        self.role = role
        if role is not ChanRole.PRODUCER:
            return []
        return list(self.producer_member_ids)


class _FakeMpscProducer:
    def __init__(self, chan_id):
        self.chan_id = chan_id


class _BlockingBindMpscProducer(MPSCChanProducer):
    def __init__(self):
        self.put_calls = 0

    def get_producer_id(self):
        return "1"

    def get_chan_id(self):
        return "11"

    def put_data(self, value):
        self.put_calls += 1
        return Result.new_ok(True)

    def __del__(self):
        return


class _FakeCloseable:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeShutdownCtl:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def _make_close_test_handle(
    cls, *, chan_id="7", member_id="11", subchannel=False
):
    obj = cls.__new__(cls)
    obj._chan_id = chan_id
    obj.chan_id = chan_id
    obj._closed_local = False
    obj._membership_cleanup_done = False
    obj._unpublished_rollback_done = False
    obj._created_new_channel = False
    obj._handle_shutdown_ctl = _FakeCloseable()
    obj._ctx = _FakeCloseable()
    obj._handle = None
    obj._data_path_lock = threading.Lock()
    obj.shutdown_ctl = _FakeShutdownCtl()
    obj.api = object()
    obj._parent_mpmc_id = "1" if subchannel else None
    if cls is MPSCChanProducer:
        obj._producer_id = member_id
    else:
        obj._consumer_id = member_id
        obj._dbg_tag = "[test consumer]"
    return obj


class MPMCLazyProducerBindTest(unittest.TestCase):
    def test_close_callback_registered_after_close_runs_immediately(self):
        shutdown_ctl = MqShutdownCtl()
        callback_called = threading.Event()

        shutdown_ctl.close()
        unregister = shutdown_ctl.register_construction_shutdown(callback_called.set)
        unregister()

        self.assertTrue(callback_called.is_set())

    def _new_producer(self, ready_channels, *, mpmc_member_id=1, producer_member_ids=None):
        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.mpsc_producers = {}
        producer._close_lock = threading.Lock()
        producer._channel_queue = TimedPriorityQueue(now=lambda: 100.0)
        producer._channel_queue_lock = threading.Lock()
        producer.mpmc_channel = _FakeMpmcChannel(
            ready_channels,
            mpmc_member_id=mpmc_member_id,
            producer_member_ids=producer_member_ids,
        )
        producer.bind_calls = []

        def _bind(self, mpsc_id):
            self.bind_calls.append(mpsc_id)
            bound = _FakeMpscProducer(mpsc_id)
            self.mpsc_producers[mpsc_id] = bound
            return bound

        producer._new_or_get_mpsc_producer = types.MethodType(_bind, producer)
        return producer

    def test_initialize_priority_queue_does_not_bind_mpsc_producers(self):
        producer = self._new_producer(["11", "12"])

        MPMCChanProducer._initialize_priority_queue(producer)

        self.assertEqual(producer.mpmc_channel.refresh_count, 1)
        self.assertEqual(producer.bind_calls, [])
        self.assertEqual(len(producer._channel_queue), 2)

    def test_get_next_channel_binds_ready_channel_lazily(self):
        producer = self._new_producer(["11"])
        MPMCChanProducer._initialize_priority_queue(producer)

        first = MPMCChanProducer._get_next_channel_from_heap(producer, ["11"], [])
        second = MPMCChanProducer._get_next_channel_from_heap(producer, ["11"], [])

        self.assertIs(first, second)
        self.assertEqual(first.chan_id, "11")
        self.assertEqual(producer.bind_calls, ["11"])

    def test_get_next_channel_rotates_ready_channels_before_reusing(self):
        producer = self._new_producer(
            ["11", "12"],
            mpmc_member_id=2,
            producer_member_ids=[1, 2],
        )
        MPMCChanProducer._initialize_priority_queue(producer)

        first = MPMCChanProducer._get_next_channel_from_heap(producer, ["11", "12"], [])
        second = MPMCChanProducer._get_next_channel_from_heap(producer, ["11", "12"], [])

        self.assertEqual(first.chan_id, "11")
        self.assertEqual(second.chan_id, "12")
        self.assertEqual(producer.bind_calls, ["11", "12"])

    def test_get_next_channel_seeds_empty_queue_from_ready_snapshot(self):
        producer = self._new_producer(
            [],
            mpmc_member_id=2,
            producer_member_ids=[1, 2],
        )

        first = MPMCChanProducer._get_next_channel_from_heap(producer, ["21", "22"], [])

        self.assertEqual(first.chan_id, "21")
        self.assertEqual(producer.bind_calls, ["21"])
        self.assertEqual(len(producer._channel_queue), 2)

    def test_channel_selection_rotates_across_ready_snapshot(self):
        ready_channels = [str(i) for i in range(100, 104)]
        member_ids = (
            list(range(129, 131))
            + list(range(161, 163))
        )

        for member_id in member_ids:
            producer = self._new_producer(
                ready_channels,
                mpmc_member_id=member_id,
                producer_member_ids=member_ids,
            )
            selected = [
                MPMCChanProducer._get_next_channel_from_heap(
                    producer,
                    ready_channels,
                    [],
                ).chan_id
                for _ in range(len(ready_channels))
            ]

            self.assertEqual(selected, ready_channels)

    def test_put_preserves_first_channel_closed_error(self):
        class _LeaseLostChannel:
            def __init__(self):
                self.calls = 0

            def get_next_available_channel(self, api, chan_config, producer):
                self.calls += 1
                producer.shutdown_ctl.close()
                return Result.new_error(
                    ChannelClosedError(
                        message="MPMC member lease expired.",
                        channel_id="7",
                    )
                )

        channel = _LeaseLostChannel()
        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.shutdown_ctl = MqShutdownCtl()
        producer.chan_config = {"capacity": 1}
        producer.mpmc_channel = channel
        producer.mpmc_id = "7"
        producer.api = object()

        first = producer.put_data({"value": b"first"})
        self.assertFalse(first.is_ok())
        first_error = first.unwrap_error()
        self.assertIsInstance(first_error, ChannelClosedError)
        self.assertEqual(first_error.channel_id, "7")
        self.assertTrue(producer.shutdown_ctl.closed)

        second = producer.put_data({"value": b"second"})
        self.assertFalse(second.is_ok())
        self.assertIsInstance(second.unwrap_error(), ProducerClosedError)
        self.assertEqual(channel.calls, 1)

    def test_put_stops_after_shutdown_during_lazy_bind(self):
        bind_entered = threading.Event()
        release_bind = threading.Event()
        candidate = _BlockingBindMpscProducer()

        class _BlockingBindChannel:
            mpmc_id = "1"

            def get_next_available_channel(self, api, chan_config, producer):
                bind_entered.set()
                if not release_bind.wait(timeout=1.0):
                    raise AssertionError("test did not release blocked channel bind")
                return Result.new_ok(candidate)

            def close(self):
                return Result.new_ok(mpmc_module.OK_NONE)

        class _UnexpectedCapacityApi:
            def count_prefix(self, prefix):
                raise AssertionError("shutdown must stop before capacity lookup")

        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.shutdown_ctl = MqShutdownCtl()
        producer.chan_config = {"capacity": 1}
        producer.mpmc_channel = _BlockingBindChannel()
        producer.mpmc_id = "1"
        producer.api = _UnexpectedCapacityApi()
        producer.mpsc_producers = {}
        producer._close_done = False
        producer._close_lock = threading.Lock()
        producer._new_or_get_mpsc_producer_lock = threading.Lock()

        result_holder = []
        worker = threading.Thread(
            target=lambda: result_holder.append(producer.put_data({"value": b"x"})),
            daemon=True,
        )
        worker.start()
        self.assertTrue(bind_entered.wait(timeout=1.0))

        close_results = []
        closer = threading.Thread(
            target=lambda: close_results.append(producer.close()),
            daemon=True,
        )
        closer.start()
        deadline = time.monotonic() + 1.0
        while not producer.shutdown_ctl.closed and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(producer.shutdown_ctl.closed)
        release_bind.set()
        worker.join(timeout=1.0)
        closer.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertFalse(closer.is_alive())
        self.assertEqual(len(close_results), 1)
        close_results[0].unwrap()
        self.assertEqual(candidate.put_calls, 0)
        self.assertEqual(len(result_holder), 1)
        result = result_holder[0]
        self.assertFalse(result.is_ok())
        self.assertIsInstance(result.unwrap_error(), ProducerClosedError)

    def test_put_does_not_retry_capacity_rpc_after_close(self):
        count_entered = threading.Event()
        release_count = threading.Event()
        candidate = _BlockingBindMpscProducer()

        class _ReadyChannel:
            mpmc_id = "1"

            def get_next_available_channel(self, api, chan_config, producer):
                return Result.new_ok(candidate)

            def close(self):
                return Result.new_ok(mpmc_module.OK_NONE)

        class _BlockingCapacityApi:
            def __init__(self):
                self.count_calls = 0

            def count_prefix(self, prefix):
                self.count_calls += 1
                count_entered.set()
                if not release_count.wait(timeout=1.0):
                    raise AssertionError("test did not release blocked capacity RPC")
                return Result.new_error(NetworkError(message="capacity RPC timed out"))

        api = _BlockingCapacityApi()
        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.shutdown_ctl = MqShutdownCtl()
        producer.chan_config = {"capacity": 1}
        producer.mpmc_channel = _ReadyChannel()
        producer.mpmc_id = "1"
        producer.api = api
        producer.mpsc_producers = {}
        producer._close_done = False
        producer._close_lock = threading.Lock()
        producer._new_or_get_mpsc_producer_lock = threading.Lock()

        result_holder = []
        worker = threading.Thread(
            target=lambda: result_holder.append(producer.put_data({"value": b"x"})),
            daemon=True,
        )
        worker.start()
        self.assertTrue(count_entered.wait(timeout=1.0))

        producer.close().unwrap()
        release_count.set()
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(api.count_calls, 1)
        self.assertEqual(candidate.put_calls, 0)
        self.assertEqual(len(result_holder), 1)
        result = result_holder[0]
        self.assertFalse(result.is_ok())
        self.assertIsInstance(result.unwrap_error(), ProducerClosedError)

    def test_close_signals_inflight_mpsc_bind(self):
        bind_entered = threading.Event()
        bind_shutdown_requested = threading.Event()

        class _BlockingMpscProducer:
            def __init__(self, *args, _parent_shutdown_ctl=None, **kwargs):
                if _parent_shutdown_ctl is None:
                    raise AssertionError("parent shutdown controller is required")
                unregister = _parent_shutdown_ctl.register_construction_shutdown(
                    bind_shutdown_requested.set
                )
                try:
                    bind_entered.set()
                    if not bind_shutdown_requested.wait(timeout=1.0):
                        raise AssertionError("close did not signal MPSC bind shutdown")
                    raise RuntimeError("bind stopped after shutdown")
                finally:
                    unregister()

        class _MpmcChannel:
            mpmc_member_id = 1
            mpmc_member_lease = object()
            mpmc_global_lease = object()
            payload_lease_id = 7

            def close(self):
                return Result.new_ok(mpmc_module.OK_NONE)

        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.shutdown_ctl = MqShutdownCtl()
        producer._new_or_get_mpsc_producer_lock = threading.Lock()
        producer._close_done = False
        producer._close_lock = threading.Lock()
        producer.mpsc_producers = {}
        producer.mpmc_channel = _MpmcChannel()
        producer.mpmc_id = "1"
        producer.chan_config = {}
        producer.api = object()
        producer.etcd_client = object()

        errors = []
        old_mpsc_producer = mpmc_module.MPSCChanProducer
        mpmc_module.MPSCChanProducer = _BlockingMpscProducer
        try:
            worker = threading.Thread(
                target=lambda: self._capture_bind_error(producer, errors),
                daemon=True,
            )
            worker.start()
            self.assertTrue(bind_entered.wait(timeout=1.0))

            producer.close().unwrap()
            worker.join(timeout=1.0)
        finally:
            mpmc_module.MPSCChanProducer = old_mpsc_producer

        self.assertFalse(worker.is_alive())
        self.assertTrue(bind_shutdown_requested.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(producer.mpsc_producers, {})

    def test_shutdown_bind_cleanup_failure_retains_subproducer_for_close_retry(self):
        cleanup_error = ResourceCleanupError(
            message="membership cleanup failed",
            resource_type="mq_etcd_state",
            resource_id="11",
        )

        class RetainedProducer:
            def close(self):
                return Result.new_error(cleanup_error)

        retained_producer = RetainedProducer()
        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.shutdown_ctl = MqShutdownCtl()
        producer._new_or_get_mpsc_producer_lock = threading.Lock()
        producer._close_lock = threading.Lock()
        producer._close_done = False
        producer.mpsc_producers = {}
        producer.mpmc_channel = types.SimpleNamespace(
            mpmc_member_id=1,
            mpmc_member_lease=object(),
            mpmc_global_lease=object(),
            payload_lease_id=7,
        )
        producer.mpmc_id = "1"
        producer.chan_config = {}
        producer.api = object()
        producer.etcd_client = object()

        def construct_then_shutdown(*_args, **_kwargs):
            producer.shutdown_ctl.close()
            return retained_producer

        with mock.patch.object(
            mpmc_module,
            "MPSCChanProducer",
            side_effect=construct_then_shutdown,
        ):
            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                producer._new_or_get_mpsc_producer("11")

        self.assertIs(producer.mpsc_producers["11"], retained_producer)
        producer._close_done = True

    def test_mpsc_producer_finishes_construction_then_rolls_back_after_parent_shutdown(self):
        bind_entered = threading.Event()
        created_shutdown_ctls = []
        cleanup_calls = []

        class _BindShutdownCtl:
            def __init__(self):
                self.closed = threading.Event()

            def close(self):
                self.closed.set()

        class _BlockingMpscContext:
            def __init__(self, api):
                self.api = api

            def new_producer(self, *args):
                bind_shutdown_ctl = args[-1]
                bind_entered.set()
                if not bind_shutdown_ctl.closed.wait(timeout=1.0):
                    raise AssertionError("parent close did not signal MPSC bind shutdown")
                return _ConstructedHandle(bind_shutdown_ctl)

            def close(self):
                return

        class _ConstructedHandle:
            def __init__(self, shutdown_ctl):
                self.shutdown_ctl = shutdown_ctl

            def shutdown_clone(self):
                return self.shutdown_ctl

            def chan_id(self):
                return 11

            def producer_idx(self):
                return "7"

            def payload_lease_id(self):
                return 9

        class _ShutdownCtlFactory:
            @staticmethod
            def new_shutdown_ctl():
                shutdown_ctl = _BindShutdownCtl()
                created_shutdown_ctls.append(shutdown_ctl)
                return shutdown_ctl

        parent_shutdown_ctl = MqShutdownCtl()
        errors = []
        old_context = mpsc_module.MpscContext
        old_rust_context = mpsc_module._RustMpscContext
        old_validate = mpsc_module.validate_mpsc_config
        mpsc_module.MpscContext = _BlockingMpscContext
        mpsc_module._RustMpscContext = _ShutdownCtlFactory
        mpsc_module.validate_mpsc_config = lambda config, role: {
            "ttl_seconds": 60,
            "capacity": 1,
            "weight": 1,
        }

        def fake_delete(api, *, keys, prefixes, dbg):
            cleanup_calls.append((list(keys), list(prefixes)))
            return Result.new_ok(OkNone())

        try:
            with mock.patch.object(
                mpsc_module,
                "_delete_owned_etcd_state",
                side_effect=fake_delete,
            ):
                worker = threading.Thread(
                    target=lambda: self._capture_mpsc_construction_error(
                        parent_shutdown_ctl, errors
                    ),
                    daemon=True,
                )
                worker.start()
                self.assertTrue(bind_entered.wait(timeout=1.0))

                parent_shutdown_ctl.close()
                worker.join(timeout=1.0)
        finally:
            mpsc_module.MpscContext = old_context
            mpsc_module._RustMpscContext = old_rust_context
            mpsc_module.validate_mpsc_config = old_validate

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(created_shutdown_ctls), 1)
        self.assertTrue(created_shutdown_ctls[0].closed.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertIn("parent closed during MPSC producer construction", str(errors[0]))
        self.assertEqual(
            cleanup_calls,
            [
                (["/channels/11/producer/producer_7", "/channels/11/producer_weight/7"], []),
                (
                    [
                        "/channels/meta/11",
                        "cluster_lease/channels/11",
                        "cluster_lease/id_allocator/channels/11",
                    ],
                    ["/channels/11/", "dist_id_allocator/channels/11/"],
                ),
            ],
        )

    def test_mpsc_consumer_close_does_not_wait_for_blocked_get(self):
        get_entered = threading.Event()
        shutdown_signaled = threading.Event()
        release_get = threading.Event()

        class _BlockingGetHandle:
            def get_one(self, prefetch_target, timeout_ms):
                get_entered.set()
                if not release_get.wait(timeout=2.0):
                    raise AssertionError("test did not release blocked get")
                raise RuntimeError("get cancelled")

        class _GetShutdownCtl:
            def close(self):
                shutdown_signaled.set()

        consumer = _make_close_test_handle(
            MPSCChanConsumer,
            subchannel=True,
        )
        consumer._handle = _BlockingGetHandle()
        consumer._handle_shutdown_ctl = _GetShutdownCtl()
        results = []
        worker = threading.Thread(
            target=lambda: results.append(consumer.get_data()),
            daemon=True,
        )
        worker.start()
        self.assertTrue(get_entered.wait(timeout=1.0))

        close_results = []
        with mock.patch.object(
            mpsc_module,
            "_delete_owned_etcd_state",
            return_value=Result.new_ok(OkNone()),
        ):
            closer = threading.Thread(
                target=lambda: close_results.append(consumer.close()),
                daemon=True,
            )
            closer.start()
            self.assertTrue(shutdown_signaled.wait(timeout=1.0))
            closer.join(timeout=0.5)

        self.assertFalse(closer.is_alive())
        self.assertTrue(worker.is_alive())
        self.assertEqual(len(close_results), 1)
        close_results[0].unwrap()

        release_get.set()
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].is_ok())
        self.assertIsInstance(results[0].unwrap_error(), ChannelClosedError)

    def test_mpsc_producer_close_does_not_wait_for_blocked_put(self):
        put_entered = threading.Event()
        shutdown_signaled = threading.Event()
        release_put = threading.Event()

        class _BlockingPutHandle:
            def put_flat_dict_ptrs(self, ptrs):
                put_entered.set()
                if not release_put.wait(timeout=2.0):
                    raise AssertionError("test did not release blocked put")
                raise RuntimeError("put cancelled")

        class _PutShutdownCtl:
            def close(self):
                shutdown_signaled.set()

        producer = _make_close_test_handle(
            MPSCChanProducer,
            subchannel=True,
        )
        producer._handle = _BlockingPutHandle()
        producer._handle_shutdown_ctl = _PutShutdownCtl()
        results = []
        fake_pyo3 = types.SimpleNamespace(build_flat_dict_ptrs=lambda *_args: [])

        close_results = []
        with (
            mock.patch.object(mpsc_module, "_fluxon_kv", fake_pyo3),
            mock.patch.object(
                mpsc_module,
                "_delete_owned_etcd_state",
                return_value=Result.new_ok(OkNone()),
            ),
        ):
            worker = threading.Thread(
                target=lambda: results.append(producer.put_data({"value": b"x"})),
                daemon=True,
            )
            worker.start()
            self.assertTrue(put_entered.wait(timeout=1.0))

            closer = threading.Thread(
                target=lambda: close_results.append(producer.close()),
                daemon=True,
            )
            closer.start()
            self.assertTrue(shutdown_signaled.wait(timeout=1.0))
            closer.join(timeout=0.5)

        self.assertFalse(closer.is_alive())
        self.assertTrue(worker.is_alive())
        self.assertEqual(len(close_results), 1)
        close_results[0].unwrap()

        release_put.set()
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].is_ok())
        self.assertIsInstance(results[0].unwrap_error(), ProducerClosedError)

    @staticmethod
    def _capture_mpsc_construction_error(parent_shutdown_ctl, errors):
        try:
            MPSCChanProducer(
                object(),
                None,
                {},
                parent_mpmc_id_opt="1",
                parent_mpmc_member_id_opt=1,
                _parent_shutdown_ctl=parent_shutdown_ctl,
            )
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    @staticmethod
    def _capture_bind_error(producer, errors):
        try:
            producer._new_or_get_mpsc_producer("11")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    def test_subchannel_close_deletes_producer_membership(self):
        calls = []
        def fake_delete(api, *, keys, prefixes, dbg):
            calls.append((list(keys), list(prefixes)))
            return Result.new_ok(OkNone())

        with mock.patch.object(
            mpsc_module, "_delete_owned_etcd_state", side_effect=fake_delete
        ):
            producer = _make_close_test_handle(
                MPSCChanProducer,
                subchannel=True,
            )
            context = producer._ctx
            producer.close().unwrap()

        self.assertEqual(
            calls,
            [([
                "/channels/7/producer/producer_11",
                "/channels/7/producer_weight/11",
            ], [])],
        )
        self.assertFalse(context.closed)

    def test_full_close_deletes_producer_membership(self):
        calls = []
        def fake_delete(api, *, keys, prefixes, dbg):
            calls.append((list(keys), list(prefixes)))
            return Result.new_ok(OkNone())

        with mock.patch.object(
            mpsc_module, "_delete_owned_etcd_state", side_effect=fake_delete
        ):
            producer = _make_close_test_handle(MPSCChanProducer)
            context = producer._ctx
            producer.close().unwrap()

        self.assertEqual(
            calls,
            [([
                "/channels/7/producer/producer_11",
                "/channels/7/producer_weight/11",
            ], [])],
        )
        self.assertTrue(context.closed)

    def test_subchannel_close_deletes_consumer_membership(self):
        calls = []
        def fake_delete(api, *, keys, prefixes, dbg):
            calls.append((list(keys), list(prefixes)))
            return Result.new_ok(OkNone())

        with mock.patch.object(
            mpsc_module, "_delete_owned_etcd_state", side_effect=fake_delete
        ):
            consumer = _make_close_test_handle(
                MPSCChanConsumer,
                subchannel=True,
            )
            context = consumer._ctx
            consumer.close().unwrap()

        self.assertEqual(calls, [(["/channels/7/consumer/consumer_11"], [])])
        self.assertFalse(context.closed)

    def test_full_close_deletes_consumer_membership(self):
        calls = []
        def fake_delete(api, *, keys, prefixes, dbg):
            calls.append((list(keys), list(prefixes)))
            return Result.new_ok(OkNone())

        with mock.patch.object(
            mpsc_module, "_delete_owned_etcd_state", side_effect=fake_delete
        ):
            consumer = _make_close_test_handle(MPSCChanConsumer)
            context = consumer._ctx
            consumer.close().unwrap()

        self.assertEqual(calls, [(["/channels/7/consumer/consumer_11"], [])])
        self.assertTrue(context.closed)

    def test_subchannel_close_keeps_parent_context_but_deletes_membership(self):
        cases = [
            (
                MPSCChanProducer,
                [[
                    "/channels/7/producer/producer_11",
                    "/channels/7/producer_weight/11",
                ]],
            ),
            (MPSCChanConsumer, [["/channels/7/consumer/consumer_11"]]),
        ]

        for endpoint_type, expected_deletes in cases:
            with self.subTest(endpoint_type=endpoint_type.__name__):
                calls = []
                def fake_delete(api, *, keys, prefixes, dbg):
                    calls.append(list(keys))
                    return Result.new_ok(OkNone())

                with mock.patch.object(
                    mpsc_module, "_delete_owned_etcd_state", side_effect=fake_delete
                ):
                    endpoint = _make_close_test_handle(
                        endpoint_type,
                        subchannel=True,
                    )
                    context = endpoint._ctx
                    endpoint.close().unwrap()

                self.assertEqual(calls, expected_deletes)
                self.assertFalse(context.closed)

    def test_unpublished_channel_rollback_deletes_all_channel_scoped_state(self):
        endpoint = _make_close_test_handle(
            MPSCChanProducer,
            subchannel=True,
        )
        endpoint._created_new_channel = True
        calls = []

        def fake_delete(api, *, keys, prefixes, dbg):
            calls.append((list(keys), list(prefixes)))
            return Result.new_ok(OkNone())

        with mock.patch.object(
            mpsc_module, "_delete_owned_etcd_state", side_effect=fake_delete
        ):
            endpoint._rollback_unpublished_channel().unwrap()

        self.assertEqual(
            calls,
            [
                ([
                    "/channels/7/producer/producer_11",
                    "/channels/7/producer_weight/11",
                ], []),
                ([
                    "/channels/meta/7",
                    "cluster_lease/channels/7",
                    "cluster_lease/id_allocator/channels/7",
                ], [
                    "/channels/7/",
                    "dist_id_allocator/channels/7/",
                ]),
            ],
        )

    def test_mpmc_consumer_deletes_membership_before_ready_handoff(self):
        events = []

        class InnerConsumer:
            def close(self):
                events.append("membership")
                return Result.new_ok(OkNone())

        class InnerChannel:
            mpmc_member_id = 11

            def close(self):
                events.append("channel")
                return Result.new_ok(OkNone())

        consumer = MPMCChanConsumer.__new__(MPMCChanConsumer)
        consumer.shutdown_ctl = MqShutdownCtl()
        consumer._close_lock = threading.Lock()
        consumer._close_done = False
        consumer.mpsc_consumer = InnerConsumer()
        consumer.mpmc_channel = InnerChannel()
        consumer.mpmc_id = "1"
        consumer.bound_mpsc_id = "7"
        consumer.api = object()

        def delete_ready(*_args, **_kwargs):
            events.append("ready")
            return Result.new_ok(OkNone())

        with mock.patch.object(
            mpmc_module,
            "stable_delete_ready_keys_for_member",
            side_effect=delete_ready,
        ):
            consumer.close().unwrap()

        self.assertEqual(events, ["membership", "ready", "channel"])

    def test_mpmc_consumer_keeps_ready_key_when_membership_cleanup_fails(self):
        cleanup_error = ResourceCleanupError(
            message="membership delete failed",
            resource_type="mq_etcd_state",
            resource_id="consumer-11",
        )

        class InnerConsumer:
            def close(self):
                return Result.new_error(cleanup_error)

        consumer = MPMCChanConsumer.__new__(MPMCChanConsumer)
        consumer.shutdown_ctl = MqShutdownCtl()
        consumer._close_lock = threading.Lock()
        consumer._close_done = False
        consumer.mpsc_consumer = InnerConsumer()
        consumer.mpmc_channel = object()
        consumer.mpmc_id = "1"
        consumer.bound_mpsc_id = "7"
        consumer.api = object()

        with mock.patch.object(
            mpmc_module,
            "stable_delete_ready_keys_for_member",
        ) as delete_ready:
            close_result = consumer.close()

        self.assertFalse(close_result.is_ok())
        returned_error = close_result.unwrap_error()
        self.assertIsInstance(returned_error, ResourceCleanupError)
        self.assertEqual(returned_error.message, cleanup_error.message)
        self.assertEqual(returned_error.resource_id, cleanup_error.resource_id)
        delete_ready.assert_not_called()
        self.assertFalse(consumer._close_done)
        consumer._close_done = True

    def test_watch_stop_is_bounded_when_cancel_contract_breaks(self):
        class StuckThread:
            def join(self, timeout=None):
                self.timeout = timeout

            def is_alive(self):
                return True

        class StuckStream:
            def __init__(self):
                self.cancelled = False

            def cancel(self):
                self.cancelled = True

        class WatchClient:
            def __init__(self):
                self.close_count = 0

            def close(self):
                self.close_count += 1

        channel = MPMCChannel.__new__(MPMCChannel)
        channel.mpmc_id = "1"
        channel._watch_lock = threading.Lock()
        channel.stop_flag = threading.Event()
        channel.watch_thread = StuckThread()
        channel._watch_client = WatchClient()
        channel._watch_stream = StuckStream()
        channel._watch_request_stop = threading.Event()

        with mock.patch.object(mpmc_module, "MPMC_WATCH_STOP_TIMEOUT_SECONDS", 0.01):
            with self.assertRaisesRegex(RuntimeError, "did not stop"):
                channel.stop_watching()

        self.assertEqual(channel.watch_thread.timeout, 0.01)
        self.assertTrue(channel._watch_stream.cancelled)
        self.assertTrue(channel._watch_request_stop.is_set())
        self.assertEqual(channel._watch_client.close_count, 0)

    def test_watch_stop_joins_direct_stream_before_closing_client(self):
        stream_started = threading.Event()
        snapshot_loaded = threading.Event()
        lifecycle_events = []

        class WatchStream:
            def __init__(self):
                self.cancelled = threading.Event()
                self.first_response = True

            def __iter__(self):
                return self

            def __next__(self):
                if self.first_response:
                    self.first_response = False
                    return types.SimpleNamespace(
                        created=True,
                        compact_revision=0,
                        canceled=False,
                        cancel_reason="",
                        events=[],
                    )
                if not self.cancelled.wait(timeout=2):
                    raise RuntimeError("test watch stream was not canceled")
                lifecycle_events.append("stream-exit")
                raise StopIteration

            def cancel(self):
                if not self.cancelled.is_set():
                    lifecycle_events.append("stream-cancel")
                self.cancelled.set()
                return True

        class WatchClient:
            def __init__(self):
                self.channel = object()
                self.call_credentials = None
                self.metadata = None
                self.close_count = 0

            def close(self):
                lifecycle_events.append("client-close")
                self.close_count += 1

            def watch_prefix(self, *_args, **_kwargs):
                raise AssertionError("etcd3.Watcher must not own the MPMC watch")

        watch_stream = WatchStream()
        watch_client = WatchClient()

        def start_stream(requests, *, credentials, metadata):
            request = next(requests)
            self.assertTrue(request.HasField("create_request"))
            stream_started.set()
            return watch_stream

        channel = MPMCChannel.__new__(MPMCChannel)
        channel.mpmc_id = "1"
        channel._etcd_endpoints = ["127.0.0.1:2379"]
        channel._watch_lock = threading.Lock()
        channel.stop_flag = threading.Event()
        channel.watch_thread = None
        channel._watch_client = None
        channel._watch_stream = None
        channel._watch_request_stop = None
        channel._refresh_local_ready_state = lambda _client: snapshot_loaded.set()

        watch_stub = types.SimpleNamespace(Watch=start_stream)
        with (
            mock.patch.object(mpmc_module.etcd3, "client", return_value=watch_client),
            mock.patch.object(mpmc_module.etcdrpc, "WatchStub", return_value=watch_stub),
        ):
            channel.start_watching()
            self.assertTrue(stream_started.wait(timeout=1))
            self.assertTrue(snapshot_loaded.wait(timeout=1))
            channel.stop_watching()

        self.assertTrue(watch_stream.cancelled.is_set())
        self.assertEqual(watch_client.close_count, 1)
        self.assertIsNone(channel.watch_thread)
        self.assertIsNone(channel._watch_client)
        self.assertIsNone(channel._watch_stream)
        self.assertIsNone(channel._watch_request_stop)
        self.assertEqual(
            lifecycle_events,
            ["stream-cancel", "stream-exit", "client-close"],
        )

    def test_watch_stop_timeout_preserves_channel_owned_leases(self):
        class StuckThread:
            def join(self, timeout=None):
                self.timeout = timeout

            def is_alive(self):
                return True

        class WatchClient:
            def __init__(self):
                self.close_count = 0

            def close(self):
                self.close_count += 1

        class StuckStream:
            def cancel(self):
                return None

        leases = [object() for _ in range(4)]
        channel = MPMCChannel.__new__(MPMCChannel)
        channel.mpmc_id = "1"
        channel.shutdown_ctl = MqShutdownCtl()
        channel._close_lock = threading.Lock()
        channel._close_done = False
        channel._watch_lock = threading.Lock()
        channel.stop_flag = threading.Event()
        channel.watch_thread = StuckThread()
        channel._watch_client = WatchClient()
        channel._watch_stream = StuckStream()
        channel._watch_request_stop = threading.Event()
        channel.mpmc_member_id = None
        channel._lm_mpmc_member = leases[0]
        channel._lm_mpmc_global = leases[1]
        channel._lm_cluster_long = leases[2]
        channel._lm_kv_payload = leases[3]

        with mock.patch.object(mpmc_module, "MPMC_WATCH_STOP_TIMEOUT_SECONDS", 0.01):
            close_result = channel.close()

        self.assertFalse(close_result.is_ok())
        self.assertIsInstance(close_result.unwrap_error(), ResourceCleanupError)
        self.assertFalse(channel._close_done)
        self.assertIs(channel._lm_mpmc_member, leases[0])
        self.assertIs(channel._lm_mpmc_global, leases[1])
        self.assertIs(channel._lm_cluster_long, leases[2])
        self.assertIs(channel._lm_kv_payload, leases[3])
        self.assertEqual(channel._watch_client.close_count, 0)


if __name__ == "__main__":
    unittest.main()
