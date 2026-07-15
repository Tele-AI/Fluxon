import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    unittest.main()


from fluxon_py.fluxon_fs.video import (  # noqa: E402
    FluxonFsVideoReadRequest,
    FluxonFsVideoReader,
    FluxonFsVideoReaderPool,
)


class _FakeNativeVideoReader:
    def __init__(self) -> None:
        self.calls = []
        self.closed = False
        self.read_at_calls = 0
        self.remote_read_bytes = 0

    def read_frames_numpy(self, indices):
        self.calls.append(list(indices))
        self.read_at_calls += 1
        self.remote_read_bytes += 1024 * len(indices)
        return ("frames", tuple(indices))

    def stats(self):
        return {
            "read_at_calls": self.read_at_calls,
            "remote_read_bytes": self.remote_read_bytes,
        }

    def close(self) -> None:
        self.closed = True


class _FakeArray:
    def __init__(self, values) -> None:
        self.values = tuple(values)
        self.shape = (len(self.values),)
        self.nbytes = len(self.values)

    def __getitem__(self, item):
        selected = self.values[item]
        if isinstance(selected, tuple):
            return _FakeArray(selected)
        return _FakeArray((selected,))

    def __eq__(self, other) -> bool:
        if not isinstance(other, _FakeArray):
            return False
        return self.values == other.values


class _FakeArrayNativeVideoReader(_FakeNativeVideoReader):
    def read_frames_numpy(self, indices):
        self.calls.append(list(indices))
        self.read_at_calls += 1
        self.remote_read_bytes += 1024 * len(indices)
        return _FakeArray(indices)


class _FakeAgent:
    def __init__(self) -> None:
        self.calls = []
        self.natives: list[_FakeNativeVideoReader] = []

    def open_video_reader(
        self,
        export_name: str,
        relpath: str,
        height: int,
        width: int,
        num_threads: int,
        request_identity,
    ):
        self.calls.append(
            (
                export_name,
                relpath,
                height,
                width,
                num_threads,
                request_identity,
            )
        )
        native = _FakeNativeVideoReader()
        self.natives.append(native)
        return native


class _FakeArrayAgent(_FakeAgent):
    def open_video_reader(
        self,
        export_name: str,
        relpath: str,
        height: int,
        width: int,
        num_threads: int,
        request_identity,
    ):
        self.calls.append(
            (
                export_name,
                relpath,
                height,
                width,
                num_threads,
                request_identity,
            )
        )
        native = _FakeArrayNativeVideoReader()
        self.natives.append(native)
        return native


class TestFluxonFsVideoReaderFacade(unittest.TestCase):
    def test_open_passes_strong_arguments_to_native_agent(self) -> None:
        agent = _FakeAgent()

        reader = FluxonFsVideoReader._open(
            agent=agent,
            export_name="videos",
            relpath="a/b.mp4",
            height=480,
            width=832,
            num_threads=8,
            request_identity=("user", "pass"),
        )

        self.assertIsInstance(reader, FluxonFsVideoReader)
        self.assertEqual(
            agent.calls,
            [("videos", "a/b.mp4", 480, 832, 8, ("user", "pass"))],
        )

    def test_read_frames_numpy_validates_indices_and_closes_native(self) -> None:
        native = _FakeNativeVideoReader()
        reader = FluxonFsVideoReader(native)

        out = reader.read_frames_numpy([2, 0, 2])
        self.assertEqual(out, ("frames", (2, 0, 2)))
        self.assertEqual(native.calls, [[2, 0, 2]])
        self.assertEqual(
            reader.stats(),
            {
                "read_at_calls": 1,
                "remote_read_bytes": 3072,
            },
        )

        with self.assertRaisesRegex(ValueError, "non-negative"):
            reader.read_frames_numpy([-1])
        with self.assertRaisesRegex(TypeError, "int values"):
            reader.read_frames_numpy([1, "2"])  # type: ignore[list-item]
        with self.assertRaisesRegex(TypeError, "int values"):
            reader.read_frames_numpy([True])  # type: ignore[list-item]

        reader.close()
        self.assertTrue(native.closed)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            reader.read_frames_numpy([0])
        with self.assertRaisesRegex(RuntimeError, "closed"):
            reader.stats()

    def test_open_rejects_invalid_arguments_before_native_call(self) -> None:
        agent = _FakeAgent()

        with self.assertRaisesRegex(ValueError, "export_name must be non-empty"):
            FluxonFsVideoReader._open(
                agent=agent,
                export_name="",
                relpath="a.mp4",
                height=480,
                width=832,
                num_threads=8,
                request_identity=None,
            )
        with self.assertRaisesRegex(ValueError, "height must be positive"):
            FluxonFsVideoReader._open(
                agent=agent,
                export_name="videos",
                relpath="a.mp4",
                height=0,
                width=832,
                num_threads=8,
                request_identity=None,
            )
        with self.assertRaisesRegex(TypeError, "num_threads must be int"):
            FluxonFsVideoReader._open(
                agent=agent,
                export_name="videos",
                relpath="a.mp4",
                height=480,
                width=832,
                num_threads=True,  # type: ignore[arg-type]
                request_identity=None,
            )

        self.assertEqual(agent.calls, [])


class TestFluxonFsVideoReaderPool(unittest.TestCase):
    def test_pool_reuses_idle_reader_and_returns_per_read_stats(self) -> None:
        agent = _FakeAgent()
        pool = FluxonFsVideoReaderPool(
            agent=agent,
            request_identity=("user", "pass"),
            max_readers=2,
        )

        first = pool.read_frames_numpy_with_stats(
            export_name="videos",
            relpath="a.mp4",
            height=480,
            width=832,
            num_threads=2,
            indices=[0, 1],
        )
        second = pool.read_frames_numpy_with_stats(
            export_name="videos",
            relpath="a.mp4",
            height=480,
            width=832,
            num_threads=2,
            indices=[2, 3, 4],
        )

        self.assertFalse(first.reader_cache_hit)
        self.assertTrue(second.reader_cache_hit)
        self.assertEqual(len(agent.natives), 1)
        self.assertEqual(first.reader_stats["read_at_calls"], 1)
        self.assertEqual(first.reader_stats["remote_read_bytes"], 2048)
        self.assertEqual(second.reader_stats["read_at_calls"], 1)
        self.assertEqual(second.reader_stats["remote_read_bytes"], 3072)
        self.assertEqual(pool.stats()["reader_cache_hits"], 1)
        self.assertEqual(pool.stats()["reader_cache_misses"], 1)

        pool.close()
        self.assertTrue(agent.natives[0].closed)

    def test_pool_evicts_lru_idle_reader(self) -> None:
        agent = _FakeAgent()
        pool = FluxonFsVideoReaderPool(agent=agent, request_identity=None, max_readers=1)

        pool.read_frames_numpy(
            export_name="videos",
            relpath="a.mp4",
            height=480,
            width=832,
            num_threads=2,
            indices=[0],
        )
        first_native = agent.natives[0]
        pool.read_frames_numpy(
            export_name="videos",
            relpath="b.mp4",
            height=480,
            width=832,
            num_threads=2,
            indices=[0],
        )

        self.assertTrue(first_native.closed)
        self.assertEqual(pool.stats()["current_readers"], 1)
        self.assertEqual(pool.stats()["evict_count"], 1)
        pool.close()

    def test_pool_read_many_coalesces_same_reader_key(self) -> None:
        agent = _FakeArrayAgent()
        pool = FluxonFsVideoReaderPool(agent=agent, request_identity=None, max_readers=2)

        results = pool.read_many_numpy_with_stats(
            [
                FluxonFsVideoReadRequest(
                    export_name="videos",
                    relpath="a.mp4",
                    height=480,
                    width=832,
                    num_threads=2,
                    indices=(0, 4),
                ),
                FluxonFsVideoReadRequest(
                    export_name="videos",
                    relpath="a.mp4",
                    height=480,
                    width=832,
                    num_threads=2,
                    indices=(2, 6, 8),
                ),
            ]
        )

        self.assertEqual(len(agent.natives), 1)
        self.assertEqual(agent.natives[0].calls, [[0, 4, 2, 6, 8]])
        self.assertEqual(results[0].array, _FakeArray((0, 4)))
        self.assertEqual(results[1].array, _FakeArray((2, 6, 8)))
        self.assertEqual(sum(result.reader_stats["read_at_calls"] for result in results), 1)
        self.assertEqual(sum(result.reader_stats["remote_read_bytes"] for result in results), 5120)
        self.assertEqual(results[0].batch_stats["read_many_group_size"], 2)
        self.assertEqual(results[1].batch_stats["read_many_group_frames"], 5)
        pool.close()


if __name__ == "__main__":
    main()
