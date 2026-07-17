#!/usr/bin/env python3
"""Deterministic contracts for the MPMC rebind process coordinator."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fluxon_py.tests.test_api_chan_mpmc.rebind_coordinator import (  # noqa: E402
    BarrierTarget,
    RebindBarrierTimeout,
    RebindCoordinationError,
    WorkerProcess,
    WorkerRole,
    coordinate_rebind_rounds,
    pause_ack_key,
    ready_key,
    wait_for_barrier,
)


class _FakeProcess:
    def __init__(self, return_code: Optional[int] = None) -> None:
        self.return_code = return_code

    def poll(self) -> Optional[int]:
        return self.return_code

    def wait(self, timeout: Optional[float] = None) -> int:
        del timeout
        if self.return_code is None:
            raise subprocess.TimeoutExpired("fake", 0)
        return self.return_code

    def terminate(self) -> None:
        self.return_code = -15

    def kill(self) -> None:
        self.return_code = -9


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, duration: float) -> None:
        self.now += duration


class _RecordingActions:
    def __init__(self) -> None:
        self.events: list[tuple[object, ...]] = []

    def run_active_window(self, round_index: int, consumer_id: str) -> None:
        self.events.append(("active", round_index, consumer_id))

    def pause_producers(self, generation: int) -> None:
        self.events.append(("pause", generation))

    def stop_consumer(self, consumer_id: str) -> None:
        self.events.append(("stop", consumer_id))

    def wait_consumer_exit(self, consumer_id: str) -> None:
        self.events.append(("wait_exit", consumer_id))

    def set_loop_index(self, loop_index: int) -> None:
        self.events.append(("loop", loop_index))

    def wait_inactive_gap(self) -> None:
        self.events.append(("gap",))

    def start_consumer(self, consumer_id: str) -> None:
        self.events.append(("start", consumer_id))

    def wait_consumer_ready(self, consumer_id: str) -> None:
        self.events.append(("ready", consumer_id))

    def resume_producers(self, round_index: int, consumer_id: str) -> None:
        self.events.append(("resume", round_index, consumer_id))


class TestRebindCoordinatorContract(unittest.TestCase):
    def test_barrier_waits_for_delayed_readiness(self) -> None:
        clock = _FakeClock()
        worker = WorkerProcess(
            WorkerRole.PRODUCER,
            "P0",
            _FakeProcess(),
            "logs/P0.log",
        )
        key = ready_key("session-a", worker.role, worker.identifier)

        wait_for_barrier(
            [BarrierTarget(key, worker)],
            label="delayed ready",
            read_key=lambda _key: b"ready" if clock.now >= 0.3 else None,
            timeout_s=1.0,
            poll_interval_s=0.1,
            clock=clock.monotonic,
            sleep=clock.sleep,
        )

        self.assertAlmostEqual(clock.now, 0.3)

    def test_barrier_reports_worker_exit_before_ready(self) -> None:
        worker = WorkerProcess(
            WorkerRole.CONSUMER,
            "C2",
            _FakeProcess(return_code=17),
            "logs/C2.log",
        )
        key = ready_key("session-a", worker.role, worker.identifier)

        with self.assertRaisesRegex(
            RebindCoordinationError,
            r"consumer C2 exited with return code 17.*logs/C2\.log",
        ):
            wait_for_barrier(
                [BarrierTarget(key, worker)],
                label="consumer ready",
                read_key=lambda _key: None,
                timeout_s=1.0,
            )

    def test_barrier_timeout_lists_missing_worker_and_key(self) -> None:
        clock = _FakeClock()
        worker = WorkerProcess(
            WorkerRole.CONSUMER,
            "C4",
            _FakeProcess(),
            "logs/C4.log",
        )
        key = ready_key("session-a", worker.role, worker.identifier)

        with self.assertRaises(RebindBarrierTimeout) as raised:
            wait_for_barrier(
                [BarrierTarget(key, worker)],
                label="replacement ready",
                read_key=lambda _key: None,
                timeout_s=0.25,
                poll_interval_s=0.1,
                clock=clock.monotonic,
                sleep=clock.sleep,
            )

        message = str(raised.exception)
        self.assertIn("consumer:C4", message)
        self.assertIn(key, message)
        self.assertIn("logs/C4.log", message)

    def test_barrier_tracks_a_real_child_process(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ready_path = Path(temp_dir) / "ready"
            child = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    (
                        "import pathlib, sys, time; "
                        "time.sleep(0.05); "
                        "pathlib.Path(sys.argv[1]).write_text('ready'); "
                        "time.sleep(0.05)"
                    ),
                    str(ready_path),
                ]
            )
            worker = WorkerProcess(
                WorkerRole.PRODUCER,
                "P0",
                child,
                "child.log",
            )
            key = ready_key("session-real", worker.role, worker.identifier)
            try:
                wait_for_barrier(
                    [BarrierTarget(key, worker)],
                    label="real child ready",
                    read_key=lambda _key: (
                        ready_path.read_bytes() if ready_path.exists() else None
                    ),
                    timeout_s=2.0,
                    poll_interval_s=0.01,
                )
                self.assertEqual(child.wait(timeout=2.0), 0)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=2.0)

    def test_schedule_waits_for_each_replacement_before_resume(self) -> None:
        actions = _RecordingActions()
        consumer_ids = tuple(f"C{index}" for index in range(5))

        coordinate_rebind_rounds(consumer_ids, actions)

        active_events = [
            event for event in actions.events if event[0] == "active"
        ]
        self.assertEqual(
            active_events,
            [
                ("active", index, consumer_id)
                for index, consumer_id in enumerate(consumer_ids)
            ],
        )
        for index, consumer_id in enumerate(consumer_ids[1:], start=1):
            start_position = actions.events.index(("start", consumer_id))
            ready_position = actions.events.index(("ready", consumer_id))
            resume_position = actions.events.index(
                ("resume", index, consumer_id)
            )
            self.assertLess(start_position, ready_position)
            self.assertLess(ready_position, resume_position)
        self.assertEqual(
            actions.events[-4:],
            [
                ("active", 4, "C4"),
                ("pause", 5),
                ("stop", "C4"),
                ("wait_exit", "C4"),
            ],
        )

    def test_pause_ack_keys_are_generation_scoped(self) -> None:
        self.assertNotEqual(
            pause_ack_key("session-a", "P0", 1),
            pause_ack_key("session-a", "P0", 2),
        )
        self.assertNotEqual(
            pause_ack_key("session-a", "P0", 1),
            pause_ack_key("session-b", "P0", 1),
        )

    def test_schedule_rejects_duplicate_consumers(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be unique"):
            coordinate_rebind_rounds(("C0", "C0"), _RecordingActions())


if __name__ == "__main__":
    unittest.main()
