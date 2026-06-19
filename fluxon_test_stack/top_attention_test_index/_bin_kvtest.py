#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import yaml

from _common import REPO_ROOT, load_case_config_payload, run_cargo


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


def _require_scene_runtime_endpoint(scene_runtime: object, *, service_id: str) -> tuple[str, int]:
    if not isinstance(scene_runtime, dict):
        raise ValueError("case config scene_runtime must be a mapping")
    raw_service = scene_runtime.get(service_id)
    if not isinstance(raw_service, dict):
        raise ValueError(f"case config scene_runtime.{service_id} must be a mapping")
    ip = str(raw_service.get("ip") or "").strip()
    if not ip:
        raise ValueError(f"case config scene_runtime.{service_id}.ip must be set")
    port = raw_service.get("port")
    if not isinstance(port, int):
        raise ValueError(f"case config scene_runtime.{service_id}.port must be an int")
    return ip, port


def _write_build_config_ext(case_cfg_path: Path, scene_runtime: dict) -> None:
    etcd_ip, etcd_port = _require_scene_runtime_endpoint(scene_runtime, service_id="etcd")
    greptime_ip, greptime_port = _require_scene_runtime_endpoint(scene_runtime, service_id="greptime")
    out_path = case_cfg_path.resolve().parents[1] / "src" / "build_config_ext.yml"
    out_path.write_text(
        yaml.safe_dump(
            {
                "etcd": f"{etcd_ip}:{etcd_port}",
                "prom": f"http://{greptime_ip}:{greptime_port}/v1/prometheus",
                "prom_remote_write_url": f"http://{greptime_ip}:{greptime_port}/v1/prometheus/write",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


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
    case_cfg_path = Path(args.case_config).resolve()
    case_payload = load_case_config_payload(case_cfg_path, expected_scene_id=SCENE_ID)
    scene_config = case_payload["scene_config"]
    feature = str(scene_config.get("kv_transport_feature") or "").strip()
    if not feature:
        raise ValueError("scene_config.kv_transport_feature must be set")
    rounds = _parse_kv_test_rounds(scene_config.get("kv_test_rounds"))
    scene_runtime = case_payload.get("scene_runtime")
    if not isinstance(scene_runtime, dict):
        raise ValueError("case config must define scene_runtime mapping")
    _write_build_config_ext(case_cfg_path, scene_runtime)

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
