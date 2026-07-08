import types
import threading
import unittest

from fluxon_py._api_ext_chan.mpmc import MPMCChanProducer
from fluxon_py._api_ext_chan.mpsc import ChanRole
from fluxon_py._api_ext_chan.utils import TimedPriorityQueue


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


class MPMCLazyProducerBindTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
