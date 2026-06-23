#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from _common import REPO_ROOT, run_cargo



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for Rust KV crate unit tests."
    )
    parser.add_argument(
        "--feature",
        default=os.environ.get("FLUXON_KV_TEST_TRANSPORT_FEATURE", "tcp_thread_transport"),
        help="Transport feature appended to p2p_transfer.",
    )
    args, passthrough = parser.parse_known_args()
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
        "--no-default-features",
        "--features",
        f"p2p_transfer,{args.feature}",
        *passthrough,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
