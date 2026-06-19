#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from _common import REPO_ROOT, load_case_config, run_cargo


TEST_REQUIREMENTS = ["cargo", "etcd", "ops", "submodules"]
SCENE_ID = "ci_top_attention_bin_kvtest"
KV_TEST_ROUND_NAMES = ("p2p_only", "rdma_transfer_only", "rdma_transfer_with_rpc")


def _parse_kv_test_rounds(raw: object) -> str:
    text = str(raw).strip()
    if not text:
        raise ValueError("scene_config.kv_test_rounds must be non-empty")
    if text == "all":
        return text
    rounds = [item.strip() for item in text.split(",") if item.strip()]
    if not rounds:
        raise ValueError("scene_config.kv_test_rounds must contain at least one round name")
    invalid = [item for item in rounds if item not in KV_TEST_ROUND_NAMES]
    if invalid:
        expected = ", ".join(KV_TEST_ROUND_NAMES)
        raise ValueError(
            f"unsupported scene_config.kv_test_rounds entries {invalid!r}; expected one or more of: {expected}, or 'all'"
        )
    return ",".join(rounds)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for the existing Rust kv_test binary."
    )
    parser.add_argument(
        "--case-config",
        required=True,
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args, passthrough = parser.parse_known_args()
    scene_config = load_case_config(args.case_config, expected_scene_id=SCENE_ID)
    feature = str(scene_config.get("kv_transport_feature") or "").strip()
    if not feature:
        raise ValueError("scene_config.kv_transport_feature must be set")
    rounds = _parse_kv_test_rounds(scene_config.get("kv_test_rounds"))

    cargo_args = [
        "run",
        "--manifest-path",
        str(REPO_ROOT / "fluxon_rs" / "fluxon_kv" / "Cargo.toml"),
        "--bin",
        "kv_test",
        "--no-default-features",
        "--features",
        f"test_bins,p2p_transfer,{feature}",
    ]
    if passthrough:
        cargo_args.extend(["--", *passthrough])
    env = None
    if rounds != "all":
        env = os.environ.copy()
        env["FLUXON_KV_TEST_ROUNDS"] = rounds
    return run_cargo(cargo_args, env=env)


if __name__ == "__main__":
    raise SystemExit(main())
