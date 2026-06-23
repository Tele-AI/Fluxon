#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _common import (
    REPO_ROOT,
    load_case_config,
    run_cargo,
    run_python_file,
)


TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]
SCENE_ID = "ci_top_attention_log_mgmt"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for shared-supervisor ops log rolling and Rust KV log sharding coverage."
    )
    parser.add_argument(
        "--case-config",
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args, passthrough = parser.parse_known_args()
    if args.case_config:
        _ = load_case_config(args.case_config, expected_scene_id=SCENE_ID)
    if passthrough:
        raise ValueError(f"_log_mgmt does not accept passthrough args: {tuple(passthrough)!r}")

    rc = run_python_file(
        "Flat index entry for ops/shared-supervisor log shard helper coverage.",
        "deployment/tests/test_log_shard.py",
    )
    if rc != 0:
        return rc
    for test_id in (
        "runtime_log_path_uses_daily_shard_files",
        "runtime_log_shards_roll_and_preserve_content_boundaries",
    ):
        rc = run_python_file(
            "Flat index entry for ops/shared-supervisor log routing coverage.",
            "deployment/tests/test_selection_supervisor_codegen.py",
            extra_args=("--test-id", test_id),
        )
        if rc != 0:
            return rc
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_util" / "Cargo.toml"),
        "--test",
        "log_mgmt",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
