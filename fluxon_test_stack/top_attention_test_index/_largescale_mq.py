#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml

from _common import REPO_ROOT, call



SCENE_ID = "bench_mq"
DEFAULT_PROFILE_ID = "fluxon_tcp"
DEFAULT_CONFIG = REPO_ROOT / "fluxon_test_stack" / "benchmark_full_matrix.yaml"
DEFAULT_WORKDIR = REPO_ROOT / ".tmp" / "test_largescale_mq_p300_c8"
RUNNER = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"

DEFAULT_BENCHMARK = {
    "processes_per_target": 1,
    "threads_per_process": 4,
    "value_size": 16384,
    "metric_warmup_seconds": 0,
    "op_timeout_seconds": 30,
    "cluster_ready_timeout_seconds": 1800,
    "value_size_list": [],
    "consumer_sim_handle_ms_range": [700, 1500],
}

_NODE_TARGET_RE = re.compile(r"node-([1-9][0-9]*)$")


def _repo_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def _require_dict(raw: Any, ctx: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SystemExit(f"{ctx} must be a mapping")
    return raw


def _split_ids(raw_values: list[str] | None, *, default: str) -> list[str]:
    if not raw_values:
        return [default]
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for part in raw.split(","):
            value = part.strip()
            if not value:
                continue
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
    if not out:
        raise SystemExit("at least one profile id is required")
    return out


def _target_sort_key(target: str) -> tuple[int, int | str]:
    match = _NODE_TARGET_RE.fullmatch(target)
    if match is not None:
        return (0, int(match.group(1)))
    return (1, target)


def _profile_test_stack(cfg: dict[str, Any], profile_id: str) -> dict[str, Any]:
    profiles = _require_dict(cfg.get("profiles"), "config.profiles")
    profile = _require_dict(profiles.get(profile_id), f"config.profiles[{profile_id!r}]")
    runtime = _require_dict(profile.get("runtime"), f"config.profiles[{profile_id!r}].runtime")
    return _require_dict(runtime.get("test_stack"), f"config.profiles[{profile_id!r}].runtime.test_stack")


def _profile_target_map(cfg: dict[str, Any], profile_id: str) -> dict[str, Any]:
    test_stack = _profile_test_stack(cfg, profile_id)
    deploy = _require_dict(
        test_stack.get("deploy"),
        f"config.profiles[{profile_id!r}].runtime.test_stack.deploy",
    )
    return _require_dict(
        deploy.get("target_ip_map"),
        f"config.profiles[{profile_id!r}].runtime.test_stack.deploy.target_ip_map",
    )


def _ordered_usable_targets(target_ip_map: dict[str, Any], *, ctx: str) -> list[str]:
    out: list[str] = []
    for raw_target in target_ip_map:
        if not isinstance(raw_target, str):
            raise SystemExit(f"{ctx} target key must be a string: {raw_target!r}")
        if "bastion" in raw_target.lower():
            continue
        out.append(raw_target)
    return sorted(out, key=_target_sort_key)


def _common_targets(cfg: dict[str, Any], profile_ids: list[str], required_count: int) -> list[str]:
    ordered_by_profile: list[tuple[str, list[str]]] = []
    common: set[str] | None = None
    for profile_id in profile_ids:
        target_map = _profile_target_map(cfg, profile_id)
        ordered = _ordered_usable_targets(
            target_map,
            ctx=f"config.profiles[{profile_id!r}].runtime.test_stack.deploy.target_ip_map",
        )
        ordered_by_profile.append((profile_id, ordered))
        current = set(ordered)
        common = current if common is None else common & current

    assert common is not None
    first_profile, first_ordered = ordered_by_profile[0]
    ordered_common = [target for target in first_ordered if target in common]
    if len(ordered_common) < required_count:
        counts = ", ".join(f"{profile_id}={len(targets)}" for profile_id, targets in ordered_by_profile)
        raise SystemExit(
            "large-scale MQ needs "
            f"{required_count} common non-bastion deploy targets across selected profiles, "
            f"but only found {len(ordered_common)} from {first_profile!r}; profile target counts: {counts}. "
            "Pass --config pointing at a TEST_STACK suite with the large target_ip_map."
        )
    return ordered_common[:required_count]


def _base_benchmark(cfg: dict[str, Any]) -> dict[str, Any]:
    scenes = _require_dict(cfg.get("scenes"), "config.scenes")
    scene = _require_dict(scenes.get(SCENE_ID), f"config.scenes[{SCENE_ID!r}]")
    select = _require_dict(scene.get("select"), f"config.scenes[{SCENE_ID!r}].select")
    scale_ids = select.get("scales")
    scales = _require_dict(cfg.get("scales"), "config.scales")
    if isinstance(scale_ids, list):
        for raw_scale_id in scale_ids:
            if not isinstance(raw_scale_id, str):
                continue
            scale = scales.get(raw_scale_id)
            if isinstance(scale, dict) and isinstance(scale.get("benchmark"), dict):
                return copy.deepcopy(scale["benchmark"])
    return copy.deepcopy(DEFAULT_BENCHMARK)


def _role_weights_for_exact_mpmc_counts(producer_count: int, consumer_count: int) -> dict[str, int]:
    if producer_count < 2 or consumer_count < 2:
        raise SystemExit(
            "exact MPMC count encoding requires producer-count and consumer-count to both be >= 2 "
            "because test_runner assigns one target to each role before applying role_weights"
        )
    return {
        "producer": int(producer_count) - 1,
        "consumer": int(consumer_count) - 1,
    }


def _ensure_largescale_port_alloc(
    cfg: dict[str, Any],
    *,
    profile_ids: list[str],
    topology: int,
    required_p2p_ports_per_slot: int,
) -> None:
    for profile_id in profile_ids:
        test_stack = _profile_test_stack(cfg, profile_id)
        kind = str(test_stack.get("kind", "")).strip().upper()
        if kind != "FLUXON":
            raise SystemExit(
                f"profile {profile_id!r} has test_stack.kind={kind!r}; "
                f"{SCENE_ID} large-scale MQ requires a FLUXON TEST_STACK profile"
            )

        port_alloc = _require_dict(
            test_stack.get("port_alloc"),
            f"config.profiles[{profile_id!r}].runtime.test_stack.port_alloc",
        )
        by_topology = _require_dict(
            port_alloc.get("by_topology"),
            f"config.profiles[{profile_id!r}].runtime.test_stack.port_alloc.by_topology",
        )
        exact = by_topology.get(topology)
        if exact is None:
            exact = by_topology.get(str(topology))
        default = by_topology.get("DEFAULT")
        source = exact or default
        if source is None:
            numeric_entries = [
                (key, value)
                for key, value in by_topology.items()
                if isinstance(key, int) and isinstance(value, dict)
            ]
            if numeric_entries:
                source = sorted(numeric_entries, key=lambda item: item[0])[-1][1]
        if source is None:
            raise SystemExit(
                f"profile {profile_id!r} has no usable port_alloc entry to clone for topology={topology}"
            )

        entry = copy.deepcopy(_require_dict(source, f"profile {profile_id!r} port_alloc source"))
        p2p_stride = int(entry.get("kv_p2p_port_stride", 0))
        entry["kv_p2p_port_stride"] = max(p2p_stride, required_p2p_ports_per_slot, 512)
        by_topology[int(topology)] = entry


def _pruned_artifact_sets(cfg: dict[str, Any], profile_ids: list[str]) -> dict[str, Any]:
    profiles = _require_dict(cfg.get("profiles"), "config.profiles")
    artifact_sets = _require_dict(cfg.get("artifact_sets"), "config.artifact_sets")
    out: dict[str, Any] = {}
    for profile_id in profile_ids:
        profile = _require_dict(profiles.get(profile_id), f"config.profiles[{profile_id!r}]")
        artifact_set_id = profile.get("artifact_set")
        if not isinstance(artifact_set_id, str):
            raise SystemExit(f"config.profiles[{profile_id!r}].artifact_set must be a string")
        artifact_set = artifact_sets.get(artifact_set_id)
        if not isinstance(artifact_set, dict):
            raise SystemExit(f"profile {profile_id!r} references missing artifact_set {artifact_set_id!r}")
        out[artifact_set_id] = copy.deepcopy(artifact_set)
    return out


def _build_suite(cfg: dict[str, Any], args: argparse.Namespace, profile_ids: list[str]) -> dict[str, Any]:
    producer_count = int(args.producer_count)
    consumer_count = int(args.consumer_count)
    owner_count = int(args.owner_count)
    for name, value in (
        ("producer-count", producer_count),
        ("consumer-count", consumer_count),
        ("owner-count", owner_count),
        ("owner-dram-gib", int(args.owner_dram_gib)),
        ("duration-seconds", int(args.duration_seconds)),
        ("op-timeout-seconds", int(args.op_timeout_seconds)),
        ("cluster-ready-timeout-seconds", int(args.cluster_ready_timeout_seconds)),
    ):
        if value <= 0:
            raise SystemExit(f"--{name} must be > 0")
    if int(args.metric_warmup_seconds) < 0:
        raise SystemExit("--metric-warmup-seconds must be >= 0")
    if int(args.value_size) < 0:
        raise SystemExit("--value-size must be >= 0")
    if int(args.consumer_sim_min_ms) < 0 or int(args.consumer_sim_max_ms) < 0:
        raise SystemExit("--consumer-sim-min-ms and --consumer-sim-max-ms must be >= 0")
    if int(args.consumer_sim_min_ms) > int(args.consumer_sim_max_ms):
        raise SystemExit("--consumer-sim-min-ms must be <= --consumer-sim-max-ms")

    topology = producer_count + consumer_count
    if owner_count > topology:
        raise SystemExit(
            f"owner-count={owner_count} cannot exceed benchmark topology={topology} "
            "when owner targets are co-located with benchmark targets"
        )

    target_hosts = _common_targets(cfg, profile_ids, topology)
    owner_targets = target_hosts[:owner_count]
    owner_dram_bytes = int(args.owner_dram_gib) * 1024 * 1024 * 1024
    scale_id = f"largescale_mq_n{owner_count}owner_{args.owner_dram_gib}gib_p{producer_count}_c{consumer_count}"
    if len(scale_id) > 64:
        raise SystemExit(f"generated scale id is too long for test_runner: {scale_id!r}")

    benchmark = _base_benchmark(cfg)
    benchmark.update(
        {
            "processes_per_target": 1,
            "threads_per_process": 4,
            "value_size": int(args.value_size),
            "metric_warmup_seconds": int(args.metric_warmup_seconds),
            "op_timeout_seconds": int(args.op_timeout_seconds),
            "cluster_ready_timeout_seconds": int(args.cluster_ready_timeout_seconds),
            "value_size_list": [],
            "consumer_sim_handle_ms_range": [
                int(args.consumer_sim_min_ms),
                int(args.consumer_sim_max_ms),
            ],
        }
    )

    _ensure_largescale_port_alloc(
        cfg,
        profile_ids=profile_ids,
        topology=topology,
        required_p2p_ports_per_slot=topology + 1 + owner_count,
    )

    scenes = _require_dict(cfg.get("scenes"), "config.scenes")
    scene = copy.deepcopy(_require_dict(scenes.get(SCENE_ID), f"config.scenes[{SCENE_ID!r}]"))
    scene["test_stack"] = copy.deepcopy(_require_dict(scene.get("test_stack"), f"config.scenes[{SCENE_ID!r}].test_stack"))
    scene["test_stack"]["mode"] = "MPMC"
    scene["test_stack"]["role_weights"] = _role_weights_for_exact_mpmc_counts(
        producer_count,
        consumer_count,
    )
    scene["select"] = {"scales": [scale_id], "profiles": list(profile_ids)}

    profiles = _require_dict(cfg.get("profiles"), "config.profiles")
    case_ids = [f"{SCENE_ID}__{scale_id}__{profile_id}" for profile_id in profile_ids]
    return {
        "schema_version": cfg.get("schema_version"),
        "run": {
            "mode": "full_once",
            "selectors": {
                "case_ids": case_ids,
                "profile_ids": list(profile_ids),
                "command_ids": "ALL",
                "test_ids": "ALL",
            },
        },
        "scenes": {SCENE_ID: scene},
        "scales": {
            scale_id: {
                "duration_seconds": int(args.duration_seconds),
                "topology": topology,
                "targets": {"hosts": target_hosts},
                "owner": {
                    "owner_count": owner_count,
                    "owner_dram_bytes": owner_dram_bytes,
                    "targets": owner_targets,
                },
                "benchmark": benchmark,
            }
        },
        "artifact_sets": _pruned_artifact_sets(cfg, profile_ids),
        "profiles": {profile_id: copy.deepcopy(profiles[profile_id]) for profile_id in profile_ids},
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Flat index entry for the TEST_STACK large-scale MQ benchmark "
            "(default: 30 owners at 5GiB, 300 producers, 8 consumers)."
        )
    )
    parser.add_argument("--python", default=os.environ.get("PYTHON", sys.executable))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Base TEST_STACK suite YAML.")
    parser.add_argument("--workdir", default=str(DEFAULT_WORKDIR), help="test_runner workdir.")
    parser.add_argument("--suite-out", help="Generated suite YAML path; default is <workdir>/largescale_mq_suite.yaml.")
    parser.add_argument("--profile", action="append", dest="profiles", help="Profile id to run; repeat or comma-separate.")
    parser.add_argument("--action", choices=["run", "clean"], default="run")
    parser.add_argument("--generate-only", action="store_true", help="Write the generated suite YAML and do not invoke test_runner.")
    parser.add_argument("--owner-count", type=int, default=30)
    parser.add_argument("--owner-dram-gib", type=int, default=5)
    parser.add_argument("--producer-count", type=int, default=300)
    parser.add_argument("--consumer-count", type=int, default=8)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--value-size", type=int, default=16384)
    parser.add_argument("--metric-warmup-seconds", type=int, default=0)
    parser.add_argument("--op-timeout-seconds", type=int, default=30)
    parser.add_argument("--cluster-ready-timeout-seconds", type=int, default=1800)
    parser.add_argument("--consumer-sim-min-ms", type=int, default=700)
    parser.add_argument("--consumer-sim-max-ms", type=int, default=1500)
    args = parser.parse_args()

    workdir = _repo_path(args.workdir)
    if args.action == "clean":
        return call([args.python, "-u", str(RUNNER), "--workdir", str(workdir), "--action", "clean"])

    config_path = _repo_path(args.config)
    if not config_path.exists():
        raise SystemExit(f"--config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as fh:
        cfg = _require_dict(yaml.safe_load(fh), f"config file {config_path}")

    profile_ids = _split_ids(args.profiles, default=DEFAULT_PROFILE_ID)
    suite = _build_suite(cfg, args, profile_ids)

    suite_out = _repo_path(args.suite_out) if args.suite_out else (workdir / "largescale_mq_suite.yaml")
    suite_out.parent.mkdir(parents=True, exist_ok=True)
    with suite_out.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(suite, fh, sort_keys=False, allow_unicode=False)

    print(f"generated suite: {suite_out}", flush=True)
    if args.generate_only:
        return 0
    return call([args.python, "-u", str(RUNNER), "--config", str(suite_out), "--workdir", str(workdir), "--action", "run"])


if __name__ == "__main__":
    raise SystemExit(main())
