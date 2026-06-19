#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from _common import REPO_ROOT, run_cargo


TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]
KV_TEST_ROUND_NAMES = ("p2p_only", "rdma_transfer_only", "rdma_transfer_with_rpc")


def _parse_rounds_arg(raw: str) -> str:
    text = raw.strip()
    if not text:
        raise argparse.ArgumentTypeError("--rounds must be non-empty")
    if text == "all":
        return text
    rounds = [item.strip() for item in text.split(",") if item.strip()]
    if not rounds:
        raise argparse.ArgumentTypeError("--rounds must contain at least one round name")
    invalid = [item for item in rounds if item not in KV_TEST_ROUND_NAMES]
    if invalid:
        expected = ", ".join(KV_TEST_ROUND_NAMES)
        raise argparse.ArgumentTypeError(
            f"unsupported --rounds entries {invalid!r}; expected one or more of: {expected}, or 'all'"
        )
    return ",".join(rounds)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for the existing Rust kv_test binary."
    )
    parser.add_argument(
        "--feature",
        default=os.environ.get("FLUXON_KV_TEST_TRANSPORT_FEATURE", "tcp_thread_transport"),
        help="Transport feature appended to test_bins,p2p_transfer.",
    )
    parser.add_argument(
        "--rounds",
        type=_parse_rounds_arg,
        default=_parse_rounds_arg(os.environ.get("FLUXON_KV_TEST_ROUNDS", "all")),
        help=(
            "Comma-separated kv_test round profile list passed through to the Rust binary. "
            "Use 'all' to keep the Rust default full round set."
        ),
    )
    args, passthrough = parser.parse_known_args()

    cargo_args = [
        "run",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
        "--bin",
        "kv_test",
        "--no-default-features",
        "--features",
        f"test_bins,p2p_transfer,{args.feature}",
    ]
    if passthrough:
        cargo_args.extend(["--", *passthrough])
    env = None
    if args.rounds != "all":
        env = os.environ.copy()
        env["FLUXON_KV_TEST_ROUNDS"] = args.rounds
    return run_cargo(cargo_args, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
