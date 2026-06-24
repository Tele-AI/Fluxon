#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from _common import REPO_ROOT, load_case_config_payload, run_cargo, write_build_config_ext


TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]
SCENE_ID = "ci_top_attention_cargo_kv_unit"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for Rust KV crate unit tests."
    )
    parser.add_argument(
        "--case-config",
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    parser.add_argument(
        "--feature",
        default=os.environ.get("FLUXON_KV_TEST_TRANSPORT_FEATURE", "tcp_thread_transport"),
        help="Transport feature appended to p2p_transfer.",
    )
    args = parser.parse_args()
    feature = str(args.feature).strip()
    if args.case_config:
        case_cfg_path = Path(args.case_config).resolve()
        case_payload = load_case_config_payload(case_cfg_path, expected_scene_id=SCENE_ID)
        scene_config = case_payload["scene_config"]
        configured_feature = str(scene_config.get("kv_transport_feature") or "").strip()
        if not configured_feature:
            raise ValueError("scene_config.kv_transport_feature must be set")
        if feature != configured_feature:
            raise ValueError(
                f"--feature must match scene_config.kv_transport_feature when --case-config is set: {configured_feature!r}"
            )
        scene_runtime = case_payload.get("scene_runtime")
        if not isinstance(scene_runtime, dict):
            raise ValueError("case config must define scene_runtime mapping")
        write_build_config_ext(case_cfg_path, scene_runtime=scene_runtime)
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
        "--no-default-features",
        "--features",
        f"p2p_transfer,{feature}",
    ])


if __name__ == "__main__":
    raise SystemExit(main())
