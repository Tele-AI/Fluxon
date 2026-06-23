#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from _common import REPO_ROOT, run_cargo



def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for the existing Rust external_client_test binary."
    )
    parser.add_argument(
        "--feature",
        default=os.environ.get("FLUXON_KV_TEST_TRANSPORT_FEATURE", "tcp_thread_transport"),
        help="Transport feature appended to test_bins,p2p_transfer.",
    )
    args, passthrough = parser.parse_known_args()

    cargo_args = [
        "run",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
        "--bin",
        "external_client_test",
        "--no-default-features",
        "--features",
        f"test_bins,p2p_transfer,{args.feature}",
    ]
    if passthrough:
        cargo_args.extend(["--", *passthrough])
    return run_cargo(cargo_args)


if __name__ == "__main__":
    raise SystemExit(main())
