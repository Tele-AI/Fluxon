#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import (
    REPO_ROOT,
    inject_build_config_ext_env,
    load_case_config_payload,
    run_cargo,
    write_build_config_ext,
)


TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]
SCENE_ID = "ci_top_attention_cargo_kv_unit"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for Rust KV crate unit tests."
    )
    parser.add_argument(
        "--case-config",
        required=True,
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args = parser.parse_args()
    case_cfg_path = Path(args.case_config).resolve()
    case_payload = load_case_config_payload(case_cfg_path, expected_scene_id=SCENE_ID)
    scene_config = case_payload["scene_config"]
    feature = str(scene_config.get("kv_transport_feature") or "").strip()
    if not feature:
        raise ValueError("scene_config.kv_transport_feature must be set")
    scene_runtime = case_payload.get("scene_runtime")
    if not isinstance(scene_runtime, dict):
        raise ValueError("case config must define scene_runtime mapping")
    build_config_ext_path = write_build_config_ext(case_cfg_path, scene_runtime=scene_runtime)
    env = inject_build_config_ext_env(
        None,
        build_config_ext_path=build_config_ext_path,
    )
    return run_cargo([
        "test",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
        "--no-default-features",
        "--features",
        f"p2p_transfer,{feature}",
    ], env=env)


if __name__ == "__main__":
    raise SystemExit(main())
