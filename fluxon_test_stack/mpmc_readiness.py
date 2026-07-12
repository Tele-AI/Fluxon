from __future__ import annotations

from dataclasses import dataclass
@dataclass(frozen=True)
class MPMCTopologyReadiness:
    """Result of a pre-READY MPMC topology check."""

    ready: bool
    reason: str


def evaluate_mpmc_topology_ready(
    *,
    role: str,
    expected_workers: int,
    ready_channels: int,
    active_consumers: int,
) -> MPMCTopologyReadiness:
    """Evaluate whether an MPMC node can report READY to the coordinator."""
    if expected_workers <= 0:
        raise ValueError(f"expected_workers must be > 0, got {expected_workers}")

    normalized_role = (role or "").strip().lower()
    if normalized_role == "producer":
        if ready_channels < expected_workers:
            return MPMCTopologyReadiness(
                ready=False,
                reason=(
                    "ready_channels below expected_workers: "
                    f"{ready_channels} < {expected_workers}"
                ),
            )
        if active_consumers < 1:
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
