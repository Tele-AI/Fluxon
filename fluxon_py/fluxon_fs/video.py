from __future__ import annotations

from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from threading import RLock
from typing import Any, Optional, Tuple


class FluxonFsVideoReader:
    """Video reader backed by FluxonFS range reads."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._closed = False

    @classmethod
    def _open(
        cls,
        *,
        agent: Any,
        export_name: str,
        relpath: str,
        height: int,
        width: int,
        num_threads: int,
        request_identity: Optional[Tuple[str, str]],
    ) -> "FluxonFsVideoReader":
        export_name = _require_non_empty_str(export_name, "export_name")
        relpath = _require_non_empty_str(relpath, "relpath")
        height = _require_positive_int(height, "height")
        width = _require_positive_int(width, "width")
        num_threads = _require_positive_int(num_threads, "num_threads")
        inner = agent.open_video_reader(
            export_name,
            relpath,
            height,
            width,
            num_threads,
            request_identity,
        )
        return cls(inner)

    def read_frames_numpy(self, indices: Sequence[int]) -> Any:
        if self._closed:
            raise RuntimeError("FluxonFsVideoReader is closed")
        if not isinstance(indices, Sequence):
            raise TypeError(f"indices must be a sequence of int, got {type(indices)}")
        frame_indices = []
        for idx in indices:
            if type(idx) is not int:
                raise TypeError(f"indices must contain int values, got {type(idx)}")
            if idx < 0:
                raise ValueError("indices must be non-negative")
            frame_indices.append(int(idx))
        return self._inner.read_frames_numpy(frame_indices)

    def stats(self) -> dict[str, int]:
        if self._closed:
            raise RuntimeError("FluxonFsVideoReader is closed")
        stats = self._inner.stats()
        if not isinstance(stats, dict):
            raise TypeError(f"native video stats must be dict, got {type(stats)}")
        out: dict[str, int] = {}
        for key, value in stats.items():
            if not isinstance(key, str):
                raise TypeError(f"native video stats keys must be str, got {type(key)}")
            if type(value) is not int:
                raise TypeError(f"native video stats values must be int, got {type(value)}")
            out[key] = int(value)
        return out

    def close(self) -> None:
        if self._closed:
            return
        self._inner.close()
        self._closed = True

    def __enter__(self) -> "FluxonFsVideoReader":
        if self._closed:
            raise RuntimeError("FluxonFsVideoReader is closed")
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


@dataclass(frozen=True)
class FluxonFsVideoReadResult:
    """Result for one pooled FluxonFS video decode."""

    array: Any
    reader_stats: dict[str, int]
    pool_stats: dict[str, int]
    reader_cache_hit: bool
    batch_stats: dict[str, int]


@dataclass(frozen=True)
class FluxonFsVideoReadRequest:
    """Request for one FluxonFS video clip decode."""

    export_name: str
    relpath: str
    height: int
    width: int
    num_threads: int
    indices: Sequence[int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "export_name", _require_non_empty_str(self.export_name, "export_name"))
        object.__setattr__(self, "relpath", _require_non_empty_str(self.relpath, "relpath"))
        object.__setattr__(self, "height", _require_positive_int(self.height, "height"))
        object.__setattr__(self, "width", _require_positive_int(self.width, "width"))
        object.__setattr__(self, "num_threads", _require_positive_int(self.num_threads, "num_threads"))
        object.__setattr__(self, "indices", tuple(_validate_indices(self.indices)))


@dataclass(frozen=True)
class _VideoReaderKey:
    export_name: str
    relpath: str
    height: int
    width: int
    num_threads: int
    request_identity: Optional[Tuple[str, str]]


@dataclass
class _PooledReader:
    key: _VideoReaderKey
    reader: FluxonFsVideoReader
    active: bool = False
    retired: bool = False


class _ReaderLease:
    def __init__(
        self,
        *,
        pool: "FluxonFsVideoReaderPool",
        reader_id: int,
        entry: _PooledReader,
        cache_hit: bool,
    ) -> None:
        self.pool = pool
        self.reader_id = reader_id
        self.entry = entry
        self.cache_hit = cache_hit

    def __enter__(self) -> "_ReaderLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.pool._release(self.reader_id, self.entry)


class FluxonFsVideoReaderPool:
    """Process-local LRU pool for FluxonFS video readers."""

    def __init__(
        self,
        *,
        agent: Any,
        request_identity: Optional[Tuple[str, str]],
        max_readers: int = 32,
    ) -> None:
        self._agent = agent
        self._request_identity = _validate_request_identity(request_identity)
        self._max_readers = _require_positive_int(max_readers, "max_readers")
        self._lock = RLock()
        self._entries: OrderedDict[int, _PooledReader] = OrderedDict()
        self._next_reader_id = 1
        self._closed = False
        self._open_count = 0
        self._close_count = 0
        self._evict_count = 0
        self._cache_hits = 0
        self._cache_misses = 0

    def read_frames_numpy(
        self,
        *,
        export_name: str,
        relpath: str,
        height: int,
        width: int,
        num_threads: int,
        indices: Sequence[int],
    ) -> Any:
        result = self.read_frames_numpy_with_stats(
            export_name=export_name,
            relpath=relpath,
            height=height,
            width=width,
            num_threads=num_threads,
            indices=indices,
        )
        return result.array

    def read_frames_numpy_with_stats(
        self,
        *,
        export_name: str,
        relpath: str,
        height: int,
        width: int,
        num_threads: int,
        indices: Sequence[int],
    ) -> FluxonFsVideoReadResult:
        key = _VideoReaderKey(
            export_name=_require_non_empty_str(export_name, "export_name"),
            relpath=_require_non_empty_str(relpath, "relpath"),
            height=_require_positive_int(height, "height"),
            width=_require_positive_int(width, "width"),
            num_threads=_require_positive_int(num_threads, "num_threads"),
            request_identity=self._request_identity,
        )
        frame_indices = _validate_indices(indices)

        with self._acquire(key) as lease:
            reader = lease.entry.reader
            before = reader.stats()
            array = reader.read_frames_numpy(frame_indices)
            after = reader.stats()

        return FluxonFsVideoReadResult(
            array=array,
            reader_stats=_stats_delta(before, after),
            pool_stats=self.stats(),
            reader_cache_hit=lease.cache_hit,
            batch_stats={
                "read_many_group_size": 1,
                "read_many_group_frames": len(frame_indices),
                "read_many_group_index": 0,
            },
        )

    def read_many_numpy_with_stats(
        self,
        requests: Sequence[FluxonFsVideoReadRequest],
    ) -> list[FluxonFsVideoReadResult]:
        read_requests = _validate_read_requests(requests)
        if not read_requests:
            return []

        grouped: OrderedDict[_VideoReaderKey, list[tuple[int, FluxonFsVideoReadRequest]]] = OrderedDict()
        for pos, request in enumerate(read_requests):
            key = _VideoReaderKey(
                export_name=request.export_name,
                relpath=request.relpath,
                height=request.height,
                width=request.width,
                num_threads=request.num_threads,
                request_identity=self._request_identity,
            )
            grouped.setdefault(key, []).append((pos, request))

        output: list[Optional[FluxonFsVideoReadResult]] = [None] * len(read_requests)
        for key, group in grouped.items():
            with self._acquire(key) as lease:
                reader = lease.entry.reader
                before = reader.stats()
                counts = [len(request.indices) for _, request in group]
                combined_indices: list[int] = []
                for _, request in group:
                    combined_indices.extend(request.indices)
                combined_array = reader.read_frames_numpy(combined_indices)
                after = reader.stats()

            _validate_combined_array(combined_array, len(combined_indices))
            reader_stats_parts = _split_stats_delta(_stats_delta(before, after), counts)
            pool_stats = self.stats()
            cursor = 0
            for group_index, ((pos, _request), reader_stats) in enumerate(zip(group, reader_stats_parts)):
                count = counts[group_index]
                array = combined_array[cursor : cursor + count]
                cursor += count
                output[pos] = FluxonFsVideoReadResult(
                    array=array,
                    reader_stats=reader_stats,
                    pool_stats=pool_stats,
                    reader_cache_hit=lease.cache_hit,
                    batch_stats={
                        "read_many_group_size": len(group),
                        "read_many_group_frames": len(combined_indices),
                        "read_many_group_index": group_index,
                    },
                )

        return _unwrap_results(output)

    def stats(self) -> dict[str, int]:
        with self._lock:
            active = sum(1 for entry in self._entries.values() if entry.active)
            return {
                "max_readers": self._max_readers,
                "current_readers": len(self._entries),
                "active_readers": active,
                "open_count": self._open_count,
                "close_count": self._close_count,
                "evict_count": self._evict_count,
                "reader_cache_hits": self._cache_hits,
                "reader_cache_misses": self._cache_misses,
            }

    def close(self) -> None:
        readers_to_close: list[FluxonFsVideoReader] = []
        with self._lock:
            if self._closed:
                return
            self._closed = True
            for reader_id, entry in list(self._entries.items()):
                entry.retired = True
                if entry.active:
                    continue
                readers_to_close.append(entry.reader)
                self._entries.pop(reader_id, None)
                self._close_count += 1
        for reader in readers_to_close:
            reader.close()

    def __enter__(self) -> "FluxonFsVideoReaderPool":
        with self._lock:
            if self._closed:
                raise RuntimeError("FluxonFsVideoReaderPool is closed")
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    def _acquire(self, key: _VideoReaderKey) -> _ReaderLease:
        with self._lock:
            if self._closed:
                raise RuntimeError("FluxonFsVideoReaderPool is closed")
            for reader_id, entry in self._entries.items():
                if entry.key == key and not entry.active and not entry.retired:
                    entry.active = True
                    self._entries.move_to_end(reader_id)
                    self._cache_hits += 1
                    return _ReaderLease(
                        pool=self,
                        reader_id=reader_id,
                        entry=entry,
                        cache_hit=True,
                    )
            self._cache_misses += 1

        reader = FluxonFsVideoReader._open(
            agent=self._agent,
            export_name=key.export_name,
            relpath=key.relpath,
            height=key.height,
            width=key.width,
            num_threads=key.num_threads,
            request_identity=key.request_identity,
        )

        with self._lock:
            if self._closed:
                reader.close()
                raise RuntimeError("FluxonFsVideoReaderPool is closed")
            reader_id = self._next_reader_id
            self._next_reader_id += 1
            entry = _PooledReader(key=key, reader=reader, active=True)
            self._entries[reader_id] = entry
            self._open_count += 1
            readers_to_close = self._evict_idle_locked()

        for old_reader in readers_to_close:
            old_reader.close()
        return _ReaderLease(pool=self, reader_id=reader_id, entry=entry, cache_hit=False)

    def _release(self, reader_id: int, entry: _PooledReader) -> None:
        readers_to_close: list[FluxonFsVideoReader] = []
        with self._lock:
            entry.active = False
            if entry.retired or self._closed:
                removed = self._entries.pop(reader_id, None)
                if removed is not None:
                    readers_to_close.append(removed.reader)
                    self._close_count += 1
            elif reader_id in self._entries:
                self._entries.move_to_end(reader_id)
                readers_to_close.extend(self._evict_idle_locked())
        for reader in readers_to_close:
            reader.close()

    def _evict_idle_locked(self) -> list[FluxonFsVideoReader]:
        readers_to_close: list[FluxonFsVideoReader] = []
        while len(self._entries) > self._max_readers:
            evicted = False
            for reader_id, entry in list(self._entries.items()):
                if entry.active:
                    continue
                entry.retired = True
                self._entries.pop(reader_id, None)
                readers_to_close.append(entry.reader)
                self._evict_count += 1
                self._close_count += 1
                evicted = True
                break
            if not evicted:
                break
        return readers_to_close


def _validate_indices(indices: Sequence[int]) -> list[int]:
    if not isinstance(indices, Sequence):
        raise TypeError(f"indices must be a sequence of int, got {type(indices)}")
    frame_indices = []
    for idx in indices:
        if type(idx) is not int:
            raise TypeError(f"indices must contain int values, got {type(idx)}")
        if idx < 0:
            raise ValueError("indices must be non-negative")
        frame_indices.append(int(idx))
    return frame_indices


def _validate_read_requests(
    requests: Sequence[FluxonFsVideoReadRequest],
) -> list[FluxonFsVideoReadRequest]:
    if not isinstance(requests, Sequence):
        raise TypeError(f"requests must be a sequence of FluxonFsVideoReadRequest, got {type(requests)}")
    out: list[FluxonFsVideoReadRequest] = []
    for request in requests:
        if not isinstance(request, FluxonFsVideoReadRequest):
            raise TypeError(
                "requests must contain FluxonFsVideoReadRequest values, "
                f"got {type(request)}"
            )
        out.append(request)
    return out


def _validate_request_identity(value: Optional[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    if value is None:
        return None
    if not isinstance(value, tuple) or len(value) != 2:
        raise TypeError("request_identity must be a (username, password) tuple or None")
    username, password = value
    if not isinstance(username, str) or not isinstance(password, str):
        raise TypeError("request_identity values must be str")
    return (username, password)


def _stats_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = {}
    for key, value in after.items():
        out[key] = int(value) - int(before.get(key, 0))
    return out


def _split_stats_delta(delta: dict[str, int], weights: list[int]) -> list[dict[str, int]]:
    parts = [dict[str, int]() for _ in weights]
    if not weights:
        return parts
    for key, value in delta.items():
        for index, part in enumerate(_split_int_by_weights(int(value), weights)):
            parts[index][key] = part
    return parts


def _split_int_by_weights(value: int, weights: list[int]) -> list[int]:
    if not weights:
        return []
    if value == 0:
        return [0 for _ in weights]

    sign = -1 if value < 0 else 1
    abs_value = abs(value)
    positive_weights = [max(0, int(weight)) for weight in weights]
    total_weight = sum(positive_weights)
    if total_weight <= 0:
        positive_weights = [1 for _ in weights]
        total_weight = len(weights)

    shares: list[int] = []
    remainders: list[tuple[int, int]] = []
    assigned = 0
    for index, weight in enumerate(positive_weights):
        raw = abs_value * weight
        share, remainder = divmod(raw, total_weight)
        shares.append(share)
        remainders.append((remainder, index))
        assigned += share

    remaining = abs_value - assigned
    for _, index in sorted(remainders, reverse=True)[:remaining]:
        shares[index] += 1
    return [sign * share for share in shares]


def _validate_combined_array(array: Any, expected_frames: int) -> None:
    shape = getattr(array, "shape", None)
    if not isinstance(shape, tuple) or len(shape) < 1:
        raise TypeError(f"native video batch must expose tuple shape, got {type(shape)}")
    if int(shape[0]) != expected_frames:
        raise RuntimeError(
            "native video batch returned unexpected frame count: "
            f"expected={expected_frames} actual={shape[0]}"
        )


def _unwrap_results(
    results: list[Optional[FluxonFsVideoReadResult]],
) -> list[FluxonFsVideoReadResult]:
    out: list[FluxonFsVideoReadResult] = []
    for result in results:
        if result is None:
            raise RuntimeError("missing FluxonFS video batch result")
        out.append(result)
    return out


def _require_non_empty_str(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be str, got {type(value)}")
    out = value.strip()
    if not out:
        raise ValueError(f"{name} must be non-empty")
    return out


def _require_positive_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be int, got {type(value)}")
    out = int(value)
    if out <= 0:
        raise ValueError(f"{name} must be positive")
    return out
