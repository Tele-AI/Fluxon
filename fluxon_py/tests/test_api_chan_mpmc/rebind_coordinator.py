"""Deterministic process coordination for the MPMC rebind test."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional, Protocol, Sequence


class WorkerRole(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"


class ProcessHandle(Protocol):
    def poll(self) -> Optional[int]: ...

    def wait(self, timeout: Optional[float] = None) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


@dataclass(frozen=True)
class WorkerProcess:
    role: WorkerRole
    identifier: str
    process: ProcessHandle
    log_file: str

    def __post_init__(self) -> None:
        if not isinstance(self.role, WorkerRole):
            raise TypeError(f"worker role must be WorkerRole: {self.role!r}")
        _validate_key_component("worker identifier", self.identifier)
        if not self.log_file:
            raise ValueError("worker log_file must not be empty")


@dataclass(frozen=True)
class BarrierTarget:
    key: str
    worker: WorkerProcess

    def __post_init__(self) -> None:
        if not self.key.startswith("/"):
            raise ValueError(f"barrier key must be absolute: {self.key!r}")


class RebindCoordinationError(RuntimeError):
    """Raised when a worker cannot satisfy a coordination contract."""


class RebindBarrierTimeout(RebindCoordinationError):
    """Raised when a bounded coordination barrier does not complete."""


def _validate_key_component(label: str, value: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be str: {value!r}")
    if not value or "/" in value:
        raise ValueError(
            f"{label} must be non-empty and must not contain '/': {value!r}"
        )


def ready_key(
    session_id: str,
    role: WorkerRole,
    identifier: str,
) -> str:
    _validate_key_component("session_id", session_id)
    if not isinstance(role, WorkerRole):
        raise TypeError(f"worker role must be WorkerRole: {role!r}")
    _validate_key_component("worker identifier", identifier)
    return (
        f"/test_mpmc_rebind/sessions/{session_id}/ready/"
        f"{role.value}/{identifier}"
    )


def pause_ack_key(session_id: str, producer_id: str, generation: int) -> str:
    _validate_key_component("session_id", session_id)
    _validate_key_component("producer_id", producer_id)
    if generation <= 0:
        raise ValueError(f"pause generation must be positive: {generation}")
    return (
        f"/test_mpmc_rebind/sessions/{session_id}/pause_ack/"
        f"{generation}/{producer_id}"
    )


def wait_for_barrier(
    targets: Sequence[BarrierTarget],
    *,
    label: str,
    read_key: Callable[[str], object],
    timeout_s: float,
    poll_interval_s: float = 0.1,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Wait for all keys while proving that every owning process stays alive."""

    if not targets:
        raise ValueError("barrier targets must not be empty")
    if timeout_s <= 0:
        raise ValueError(f"barrier timeout must be positive: {timeout_s}")
    if poll_interval_s <= 0:
        raise ValueError(
            f"barrier poll interval must be positive: {poll_interval_s}"
        )

    pending: dict[str, BarrierTarget] = {}
    for target in targets:
        if target.key in pending:
            raise ValueError(f"duplicate barrier key: {target.key}")
        pending[target.key] = target

    deadline = clock() + timeout_s
    while pending:
        for key, target in tuple(pending.items()):
            return_code = target.worker.process.poll()
            if return_code is not None:
                worker = target.worker
                raise RebindCoordinationError(
                    f"{label}: {worker.role.value} {worker.identifier} exited "
                    f"with return code {return_code} before barrier key {key}; "
                    f"log={worker.log_file}"
                )
            try:
                value = read_key(key)
            except Exception as exc:
                raise RebindCoordinationError(
                    f"{label}: failed to read barrier key {key}: {exc}"
                ) from exc
            if value is not None:
                del pending[key]

        if not pending:
            return

        remaining = deadline - clock()
        if remaining <= 0:
            missing = ", ".join(
                f"{target.worker.role.value}:{target.worker.identifier} "
                f"key={target.key} log={target.worker.log_file}"
                for target in pending.values()
            )
            raise RebindBarrierTimeout(
                f"{label}: timed out after {timeout_s:.3f}s; missing={missing}"
            )
        sleep(min(poll_interval_s, remaining))


class RebindRoundActions(Protocol):
    def run_active_window(self, round_index: int, consumer_id: str) -> None: ...

    def pause_producers(self, generation: int) -> None: ...

    def stop_consumer(self, consumer_id: str) -> None: ...

    def wait_consumer_exit(self, consumer_id: str) -> None: ...

    def set_loop_index(self, loop_index: int) -> None: ...

    def wait_inactive_gap(self) -> None: ...

    def start_consumer(self, consumer_id: str) -> None: ...

    def wait_consumer_ready(self, consumer_id: str) -> None: ...

    def resume_producers(self, round_index: int, consumer_id: str) -> None: ...


def coordinate_rebind_rounds(
    consumer_ids: Sequence[str],
    actions: RebindRoundActions,
) -> None:
    """Run every consumer through one complete ready-to-drained active round."""

    ordered_ids = tuple(consumer_ids)
    if not ordered_ids:
        raise ValueError("consumer_ids must not be empty")
    if len(set(ordered_ids)) != len(ordered_ids):
        raise ValueError(f"consumer_ids must be unique: {ordered_ids!r}")
    for consumer_id in ordered_ids:
        _validate_key_component("consumer_id", consumer_id)

    for round_index, consumer_id in enumerate(ordered_ids):
        actions.run_active_window(round_index, consumer_id)
        actions.pause_producers(round_index + 1)
        actions.stop_consumer(consumer_id)
        actions.wait_consumer_exit(consumer_id)

        next_round_index = round_index + 1
        if next_round_index == len(ordered_ids):
            continue

        next_consumer_id = ordered_ids[next_round_index]
        actions.set_loop_index(next_round_index)
        actions.wait_inactive_gap()
        actions.start_consumer(next_consumer_id)
        actions.wait_consumer_ready(next_consumer_id)
        actions.resume_producers(next_round_index, next_consumer_id)
