from __future__ import annotations

"""Shared benchmark role contract."""

from typing import Tuple

KV_NODE_ROLE_SEED = "seed"
KV_NODE_ROLE_WORKER = "worker"

KV_NODE_ROLES_CANONICAL: Tuple[str, str] = (
    KV_NODE_ROLE_SEED,
    KV_NODE_ROLE_WORKER,
)
def canonicalize_kv_node_role(value: object) -> str:
    raw = str(value).strip().lower()
    if raw not in KV_NODE_ROLES_CANONICAL:
        raise ValueError(
            f"unsupported KV node role: {value!r}; expected one of {sorted(KV_NODE_ROLES_CANONICAL)}"
        )
    return raw


def is_kv_seed_role(value: object) -> bool:
    return canonicalize_kv_node_role(value) == KV_NODE_ROLE_SEED


def is_kv_worker_role(value: object) -> bool:
    return canonicalize_kv_node_role(value) == KV_NODE_ROLE_WORKER
