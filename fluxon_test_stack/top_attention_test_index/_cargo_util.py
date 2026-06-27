#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import (
    REPO_ROOT,
    load_case_config_payload,
    run_cargo,
    write_build_config_ext,
)

TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]
SCENE_ID = "ci_top_attention_cargo_util"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for Rust util crate tests."
    )
    parser.add_argument(
        "--case-config",
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args = parser.parse_args()
    if args.case_config:
        case_cfg_path = Path(args.case_config).resolve()
        case_payload = load_case_config_payload(case_cfg_path, expected_scene_id=SCENE_ID)
        scene_runtime = case_payload.get("scene_runtime")
        if not isinstance(scene_runtime, dict):
            raise ValueError("case config must define scene_runtime mapping")
        write_build_config_ext(case_cfg_path, scene_runtime=scene_runtime)
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_util" / "Cargo.toml"),
    ])


if __name__ == "__main__":
    raise SystemExit(main())
