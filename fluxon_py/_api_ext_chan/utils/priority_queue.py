"""Helper priority queue with lazy stale entry eviction for channel scheduling."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass
from typing import Callable, Collection, Dict, List, Optional


@dataclass(order=True)
class _PriorityItem:
    """Internal heap entry keyed by (timestamp, sequence, channel)."""

    timestamp: float
    sequence: int
    chan_id: str


class TimedPriorityQueue:
    """Priority queue that returns the least recently scheduled channel."""

    def __init__(self, now: Optional[Callable[[], float]] = None) -> None:
        self._now = now or time.time
        self._heap: List[_PriorityItem] = []
        self._latest: Dict[str, _PriorityItem] = {}
        self._sequence = 0

    def update(self, chan_id: str, *, timestamp: Optional[float] = None) -> None:
        """Push a fresh usage timestamp for ``chan_id`` onto the queue."""

        self._sequence += 1
        item = _PriorityItem(
            timestamp if timestamp is not None else self._now(),
            self._sequence,
            chan_id,
        )
        self._latest[chan_id] = item
        heapq.heappush(self._heap, item)

    def ensure_tracked(self, chan_ids: Collection[str]) -> None:
        """Start tracking channels that are not already in the queue."""

        for chan_id in chan_ids:
            if chan_id not in self._latest:
                self.update(chan_id)

    def pop_ready(self, ready_channels: Collection[str]) -> Optional[str]:
        """Return the next ready channel or ``None`` if none available."""

        ready_set = set(ready_channels)
        deferred: List[_PriorityItem] = []
        while self._heap:
            item = heapq.heappop(self._heap)
            latest = self._latest.get(item.chan_id)
            if latest is not item:
                continue
            if item.chan_id not in ready_set:
                # Keep the latest entry for channels that are currently not ready,
                # but continue scanning the heap for other ready channels.
                deferred.append(item)
                continue
            # Restore deferred items before returning a ready channel.
            for d in deferred:
                heapq.heappush(self._heap, d)
            return item.chan_id
        # No ready channels found; restore deferred items.
        for d in deferred:
            heapq.heappush(self._heap, d)
        return None

    def remove(self, chan_id: str) -> None:
        """Drop tracking for ``chan_id`` (stale heap entries will be skipped)."""

        self._latest.pop(chan_id, None)

    def __len__(self) -> int:
        return len(self._latest)
