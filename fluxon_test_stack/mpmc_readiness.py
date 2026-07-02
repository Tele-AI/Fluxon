from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MPMCTopologyReadiness:
    """Result of a pre-READY MPMC topology check."""

    ready: bool
    reason: str


def evaluate_mpmc_topology_ready(
    *,
    role: str,
    expected_workers: int,
    total_mpsc_channels: Optional[int],
    ready_channels: Optional[int],
    active_consumers: Optional[int],
) -> MPMCTopologyReadiness:
    """Evaluate whether an MPMC node can report READY to the coordinator."""
    if expected_workers <= 0:
        raise ValueError(f"expected_workers must be > 0, got {expected_workers}")

    if total_mpsc_channels is not None and total_mpsc_channels < expected_workers:
        return MPMCTopologyReadiness(
            ready=False,
            reason=(
                "total_mpsc_channels below expected_workers: "
                f"{total_mpsc_channels} < {expected_workers}"
            ),
        )

    normalized_role = (role or "").strip().lower()
    if normalized_role == "producer":
        if ready_channels is not None and ready_channels < expected_workers:
            return MPMCTopologyReadiness(
                ready=False,
                reason=(
                    "ready_channels below expected_workers: "
                    f"{ready_channels} < {expected_workers}"
                ),
            )
        if active_consumers is not None and active_consumers < 1:
            return MPMCTopologyReadiness(
                ready=False,
                reason=f"active_consumers below 1: {active_consumers}",
            )
    elif normalized_role == "consumer":
        # A consumer may be the channel creator. Requiring ready_channels before that
        # consumer reports READY can deadlock a one-worker process at the coordinator
        # barrier, while producers still wait for consumers before START is released.
        pass
    else:
        return MPMCTopologyReadiness(
            ready=False,
            reason=f"unsupported MPMC role: {role!r}",
        )

    return MPMCTopologyReadiness(ready=True, reason="ready")
