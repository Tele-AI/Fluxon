import types
import threading
import time
import unittest

from fluxon_py._api_ext_chan import mpmc as mpmc_module
from fluxon_py._api_ext_chan.mpmc import MPMCChanProducer
from fluxon_py._api_ext_chan import mpsc as mpsc_module
from fluxon_py._api_ext_chan.mpsc import (
    ChanRole,
    MPSCChanConsumer,
    MPSCChanProducer,
)
from fluxon_py._api_ext_chan.mq_lifecycle import MqShutdownCtl
from fluxon_py._api_ext_chan.utils import TimedPriorityQueue
from fluxon_py.api_error import ChannelClosedError, ProducerClosedError, Result


class _FakeMpmcChannel:
    def __init__(self, ready_channels, *, mpmc_member_id=1, producer_member_ids=None):
        self.ready_channels = list(ready_channels)
        self.refresh_count = 0
        self.mpmc_member_id = mpmc_member_id
        self.producer_member_ids = (
            list(producer_member_ids)
            if producer_member_ids is not None
            else [mpmc_member_id]
        )

    def _refresh_local_ready_state(self):
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
    obj._handle_shutdown_ctl = _FakeCloseable()
    obj._ctx = _FakeCloseable()
    obj._handle = None
    obj._handle_lock = threading.Lock()
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
        unregister = shutdown_ctl.register_construction_cancel(callback_called.set)
        unregister()

        self.assertTrue(callback_called.is_set())

    def _new_producer(self, ready_channels, *, mpmc_member_id=1, producer_member_ids=None):
        producer = MPMCChanProducer.__new__(MPMCChanProducer)
        producer.mpsc_producers = {}
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

    def test_close_cancels_inflight_mpsc_bind(self):
        bind_entered = threading.Event()
        bind_cancelled = threading.Event()

        class _BlockingMpscProducer:
            def __init__(self, *args, _parent_shutdown_ctl=None, **kwargs):
                if _parent_shutdown_ctl is None:
                    raise AssertionError("parent shutdown controller is required")
                unregister = _parent_shutdown_ctl.register_construction_cancel(
                    bind_cancelled.set
                )
                try:
                    bind_entered.set()
                    if not bind_cancelled.wait(timeout=1.0):
                        raise AssertionError("close did not cancel MPSC bind")
                    raise RuntimeError("bind cancelled")
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
        self.assertTrue(bind_cancelled.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)
        self.assertEqual(producer.mpsc_producers, {})

    def test_mpsc_producer_owns_bind_shutdown_controller(self):
        bind_entered = threading.Event()
        created_shutdown_ctls = []

        class _BindShutdownCtl:
            def __init__(self):
                self.closed = threading.Event()

            def close(self):
                self.closed.set()

        class _BlockingMpscContext:
            def __init__(self, api):
                self.api = api

            @staticmethod
            def new_shutdown_ctl():
                shutdown_ctl = _BindShutdownCtl()
                created_shutdown_ctls.append(shutdown_ctl)
                return shutdown_ctl

            def new_producer(self, *args):
                bind_shutdown_ctl = args[-1]
                bind_entered.set()
                if not bind_shutdown_ctl.closed.wait(timeout=1.0):
                    raise AssertionError("parent close did not cancel MPSC bind")
                raise RuntimeError("bind cancelled")

            def close(self):
                return

        parent_shutdown_ctl = MqShutdownCtl()
        errors = []
        old_context = mpsc_module.MpscContext
        old_validate = mpsc_module.validate_mpsc_config
        mpsc_module.MpscContext = _BlockingMpscContext
        mpsc_module.validate_mpsc_config = lambda config, role: {
            "ttl_seconds": 60,
            "capacity": 1,
            "weight": 1,
        }
        try:
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
            mpsc_module.validate_mpsc_config = old_validate

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(created_shutdown_ctls), 1)
        self.assertTrue(created_shutdown_ctls[0].closed.is_set())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], RuntimeError)

    def test_mpsc_consumer_close_wakes_blocked_get(self):
        get_entered = threading.Event()
        get_stopped = threading.Event()

        class _BlockingGetHandle:
            def get_one(self, prefetch_target, timeout_ms):
                get_entered.set()
                if not get_stopped.wait(timeout=1.0):
                    raise AssertionError("close did not wake blocked get")
                raise RuntimeError("get cancelled")

        class _GetShutdownCtl:
            def close(self):
                get_stopped.set()

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

        consumer.close().unwrap()
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].is_ok())
        self.assertIsInstance(results[0].unwrap_error(), ChannelClosedError)

    @staticmethod
    def _capture_mpsc_construction_error(parent_shutdown_ctl, errors):
        try:
            MPSCChanProducer(
                object(),
                "11",
                {},
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

    def test_subchannel_close_does_not_delete_producer_membership(self):
        calls = []
        old_delete = mpsc_module._delete_owned_etcd_keys_best_effort
        mpsc_module._delete_owned_etcd_keys_best_effort = (
            lambda api, keys, dbg: calls.append(list(keys))
        )
        try:
            producer = _make_close_test_handle(
                MPSCChanProducer,
                subchannel=True,
            )
            context = producer._ctx
            producer.close().unwrap()
        finally:
            mpsc_module._delete_owned_etcd_keys_best_effort = old_delete

        self.assertEqual(calls, [])
        self.assertFalse(context.closed)

    def test_full_close_deletes_producer_membership(self):
        calls = []
        old_delete = mpsc_module._delete_owned_etcd_keys_best_effort
        mpsc_module._delete_owned_etcd_keys_best_effort = (
            lambda api, keys, dbg: calls.append(list(keys))
        )
        try:
            producer = _make_close_test_handle(MPSCChanProducer)
            context = producer._ctx
            producer.close().unwrap()
        finally:
            mpsc_module._delete_owned_etcd_keys_best_effort = old_delete

        self.assertEqual(
            calls,
            [[
                "/channels/7/producer/producer_11",
                "/channels/7/producer_weight/11",
            ]],
        )
        self.assertTrue(context.closed)

    def test_subchannel_close_does_not_delete_consumer_membership(self):
        calls = []
        old_delete = mpsc_module._delete_owned_etcd_keys_best_effort
        mpsc_module._delete_owned_etcd_keys_best_effort = (
            lambda api, keys, dbg: calls.append(list(keys))
        )
        try:
            consumer = _make_close_test_handle(
                MPSCChanConsumer,
                subchannel=True,
            )
            context = consumer._ctx
            consumer.close().unwrap()
        finally:
            mpsc_module._delete_owned_etcd_keys_best_effort = old_delete

        self.assertEqual(calls, [])
        self.assertFalse(context.closed)

    def test_full_close_deletes_consumer_membership(self):
        calls = []
        old_delete = mpsc_module._delete_owned_etcd_keys_best_effort
        mpsc_module._delete_owned_etcd_keys_best_effort = (
            lambda api, keys, dbg: calls.append(list(keys))
        )
        try:
            consumer = _make_close_test_handle(MPSCChanConsumer)
            context = consumer._ctx
            consumer.close().unwrap()
        finally:
            mpsc_module._delete_owned_etcd_keys_best_effort = old_delete

        self.assertEqual(calls, [["/channels/7/consumer/consumer_11"]])
        self.assertTrue(context.closed)

    def test_discard_unattached_subchannel_rolls_back_membership_only(self):
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
                old_delete = mpsc_module._delete_owned_etcd_keys_best_effort
                mpsc_module._delete_owned_etcd_keys_best_effort = (
                    lambda api, keys, dbg: calls.append(list(keys))
                )
                try:
                    endpoint = _make_close_test_handle(
                        endpoint_type,
                        subchannel=True,
                    )
                    context = endpoint._ctx
                    endpoint._discard().unwrap()
                finally:
                    mpsc_module._delete_owned_etcd_keys_best_effort = old_delete

                self.assertEqual(calls, expected_deletes)
                self.assertFalse(context.closed)


if __name__ == "__main__":
    unittest.main()
