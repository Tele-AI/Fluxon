#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _common import REPO_ROOT, run_cargo


TEST_REQUIREMENTS = ["cargo", "ops", "submodules"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for Rust CLI crate tests."
    )
    parser.parse_args()
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_cli" / "Cargo.toml"),
    ])


if __name__ == "__main__":
    raise SystemExit(main())
