import json
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from fluxon_py._api_ext_chan import mpmc as mpmc_module
from fluxon_py._api_ext_chan.mpmc import (
    ChanRole,
    MPMCChannel,
    _new_mpmc_mpsc_channels_key,
    _new_mpmc_ready_channels_prefix,
)


class _CompareTarget:
    def __init__(self, kind, key):
        self.kind = kind
        self.key = key

    def __eq__(self, expected):
        return self.kind, self.key, expected


class _Put:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class _Transactions:
    @staticmethod
    def create(key):
        return _CompareTarget("create", key)

    @staticmethod
    def value(key):
        return _CompareTarget("value", key)

    @staticmethod
    def put(key, value, _lease=None):
        return _Put(key, value)


class _FakeEtcd:
    def __init__(self, *, stale_reader_count):
        self.transactions = _Transactions()
        self.values = {}
        self._lock = threading.Lock()
        self._stale_reader_count = stale_reader_count
        self._initial_read_barrier = threading.Barrier(stale_reader_count)
        self._initial_reads = 0
        self.failed_transactions = 0

    def get(self, key):
        channels_key = _new_mpmc_mpsc_channels_key("239")
        wait_for_other_stale_reader = False
        with self._lock:
            value = self.values.get(key)
            if key == channels_key and self._initial_reads < self._stale_reader_count:
                self._initial_reads += 1
                wait_for_other_stale_reader = True
        if wait_for_other_stale_reader:
            self._initial_read_barrier.wait(timeout=2.0)
        return value, None

    def get_prefix(self, prefix):
        with self._lock:
            return [
                (value, SimpleNamespace(key=key.encode()))
                for key, value in self.values.items()
                if key.startswith(prefix)
            ]

    def transaction(self, *, compare, success, failure):
        del failure
        with self._lock:
            matched = True
            for kind, key, expected in compare:
                if kind == "create":
                    matched = matched and (0 if key not in self.values else 1) == expected
                elif kind == "value":
                    matched = matched and self.values.get(key) == expected
                else:
                    raise AssertionError(f"unexpected compare kind: {kind}")
            if not matched:
                self.failed_transactions += 1
                return False, []
            for operation in success:
                self.values[operation.key] = operation.value
            return True, []


class _NoopEtcdLock:
    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False


class _CloseResult:
    def __init__(self):
        self.consumed = False

    def is_ok(self):
        return True

    def unwrap(self):
        self.consumed = True


class _AlwaysConflictEtcd(_FakeEtcd):
    def transaction(self, *, compare, success, failure):
        del compare, success, failure
        self.failed_transactions += 1
        return False, []


class _FakeMPSCProducer:
    last_instance = None

    def __init__(self, *_args, **_kwargs):
        self.chan_id = "1001"
        self.closed = False
        self.discard_result = _CloseResult()
        type(self).last_instance = self

    def _discard(self):
        self.closed = True
        return self.discard_result


class _FakeMPSCConsumer:
    _next_id = 1000
    _id_lock = threading.Lock()

    def __init__(self, *_args, **_kwargs):
        with self._id_lock:
            type(self)._next_id += 1
            self.chan_id = str(type(self)._next_id)
        self.closed = False
        self._mpmc_ready_claimed = False

    def close(self):
        self.closed = True
        return _CloseResult()


def _new_channel(etcd_client, *, member_id, active_consumers):
    channel = MPMCChannel.__new__(MPMCChannel)
    channel.mpmc_id = "239"
    channel.etcd_client = etcd_client
    channel._etcd_endpoints = ["127.0.0.1:2379"]
    channel.mpmc_member_id = member_id
    channel.mpmc_member_lease = SimpleNamespace(id=member_id)
    channel.mpmc_global_lease = SimpleNamespace(id=900)
    channel.payload_lease_id = 901
    channel._get_active_consumer_count = lambda: active_consumers
    return channel


class MPMCChannelPublishCASTest(unittest.TestCase):
    def test_unpublished_producer_discard_result_is_consumed(self):
        etcd_client = _AlwaysConflictEtcd(stale_reader_count=1)
        channel = _new_channel(etcd_client, member_id=11, active_consumers=1)
        channel.shutdown_ctl = object()

        with mock.patch.object(mpmc_module, "EtcdLock", _NoopEtcdLock):
            with mock.patch.object(
                mpmc_module,
                "MPSCChanProducer",
                _FakeMPSCProducer,
            ):
                result = channel.try_create_mpsc_channel(
                    object(),
                    {},
                    ChanRole.PRODUCER,
                )

        self.assertFalse(result.is_ok())
        result.unwrap_error()
        producer = _FakeMPSCProducer.last_instance
        self.assertIsNotNone(producer)
        self.assertTrue(producer.closed)
        self.assertTrue(producer.discard_result.consumed)

    def test_concurrent_stale_consumer_writers_do_not_lose_channel(self):
        etcd_client = _FakeEtcd(stale_reader_count=2)
        channels = [
            _new_channel(etcd_client, member_id=11, active_consumers=2),
            _new_channel(etcd_client, member_id=12, active_consumers=2),
        ]
        results = []
        errors = []

        def create(channel):
            try:
                results.append(
                    channel.try_create_mpsc_channel(
                        object(),
                        {},
                        ChanRole.CONSUMER,
                    )
                )
            except Exception as exc:
                errors.append(exc)

        with mock.patch.object(mpmc_module, "EtcdLock", _NoopEtcdLock):
            with mock.patch.object(
                mpmc_module,
                "MPSCChanConsumer",
                _FakeMPSCConsumer,
            ):
                threads = [threading.Thread(target=create, args=(channel,)) for channel in channels]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=3.0)

        self.assertEqual(errors, [])
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(len(results), 2)
        self.assertTrue(all(result.is_ok() for result in results))
        consumers = [result.unwrap() for result in results]
        self.assertGreaterEqual(etcd_client.failed_transactions, 1)

        channels_raw = etcd_client.values[_new_mpmc_mpsc_channels_key("239")]
        published_ids = json.loads(channels_raw.decode())
        ready_prefix = _new_mpmc_ready_channels_prefix("239")
        ready_ids = {
            key[len(ready_prefix):]
            for key in etcd_client.values
            if key.startswith(ready_prefix)
        }

        self.assertEqual(len(published_ids), 2)
        self.assertEqual(set(published_ids), ready_ids)
        self.assertTrue(all(consumer._mpmc_ready_claimed for consumer in consumers))
        self.assertTrue(all(not consumer.closed for consumer in consumers))


if __name__ == "__main__":
    unittest.main()
