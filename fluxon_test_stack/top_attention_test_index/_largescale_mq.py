#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from _common import REPO_ROOT, call


TEST_REQUIREMENTS = ["fluxon-release", "ops", "submodules", "test-stack-targets"]


SCENE_ID = "bench_mq"
BASE_FLUXON_PROFILE_ID = "fluxon_tcp"
CI_PUBLIC_PROFILE_ID = "fluxon_tcp_thread"
DEFAULT_PROFILE_ID = CI_PUBLIC_PROFILE_ID
LOCAL_RELEASE_ROOT_ENV = "FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"
RELEASE_MANIFEST_FILENAME = "fluxon_release.sha256"
RELEASE_MANIFEST_SHA256_ENV_KEY = "FLUXON_RELEASE_MANIFEST_SHA256"
DEFAULT_CONFIG = REPO_ROOT / "fluxon_test_stack" / "benchmark_full_matrix.yaml"
DEFAULT_WORKDIR = REPO_ROOT / ".tmp" / "test_largescale_mq_p160_c8"
RUNNER = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"
LOCAL_TEST_STACK_COORDINATOR_PORT_OFFSET = 1000
LOCAL_TEST_STACK_TOPOLOGY_PORT_SPAN = 100
LOCAL_TEST_STACK_COORDINATOR_FALLBACK_PORT_BASE = 20000
LOCAL_TEST_STACK_COORDINATOR_FALLBACK_PORT_SPAN = 30000
LOCAL_TEST_STACK_P2P_PORT_MIN = 20000
LOCAL_TEST_STACK_P2P_PORT_MAX = 61000

DEFAULT_BENCHMARK = {
    "processes_per_target": 1,
    "threads_per_process": 4,
    "value_size": 256,
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


def _resolve_user_path(raw: str) -> Path:
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (Path.cwd() / path).resolve()


def _require_dict(raw: Any, ctx: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise SystemExit(f"{ctx} must be a mapping")
    return raw


def _is_within_root(path: Path, root: Path) -> bool:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


def _clean_bundle_relpath(raw: str, *, field_name: str) -> Path:
    relpath = Path(raw)
    if relpath.is_absolute() or ".." in relpath.parts:
        raise SystemExit(f"{field_name} must be a clean relative path: {relpath}")
    return relpath


def _load_yaml_mapping(path: Path, *, ctx: str) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _require_dict(payload, ctx)


def _write_yaml_mapping(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding="utf-8")


def _load_start_test_bed_module() -> Any:
    module_path = REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py"
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_start_test_bed_for_largescale_mq", module_path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load start_test_bed module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sync_run_local_deployconf_from_normalized_view(*, deployconf_path: Path) -> None:
    deployconf_payload = _load_yaml_mapping(deployconf_path, ctx=f"deployconf {deployconf_path}")
    start_test_bed_mod = _load_start_test_bed_module()
    normalized, _notes = start_test_bed_mod._normalize_bootstrap_deployconf(
        deployconf=deployconf_payload,
    )
    global_envs = normalized.get("global_envs")
    if isinstance(global_envs, dict):
        global_envs.pop(RELEASE_MANIFEST_SHA256_ENV_KEY, None)
    _write_yaml_mapping(deployconf_path, normalized)


def _parse_sha256_manifest_names(path: Path) -> list[str]:
    out: list[str] = []
    for index, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            raise SystemExit(f"invalid sha256 manifest line {index}: {path}: {raw_line!r}")
        out.append(parts[1].strip())
    return out


def _find_release_wheel_name_from_manifest(root: Path) -> str | None:
    manifest_path = (root / RELEASE_MANIFEST_FILENAME).resolve()
    if not manifest_path.is_file():
        return None
    wheel_names = [
        Path(name).name
        for name in _parse_sha256_manifest_names(manifest_path)
        if Path(name).name.startswith("fluxon-") and Path(name).name.endswith(".whl")
    ]
    if not wheel_names:
        return None
    unique_names = sorted(set(wheel_names))
    if len(unique_names) != 1:
        raise SystemExit(f"release manifest must contain one Fluxon wheel: {manifest_path} wheels={unique_names}")
    return unique_names[0]


def _ci_public_release_wheel_name(fallback: str) -> str:
    roots: list[Path] = []
    env_root = os.environ.get(LOCAL_RELEASE_ROOT_ENV, "").strip()
    if env_root:
        roots.append(Path(env_root).expanduser().resolve())
    roots.append((REPO_ROOT / "fluxon_release").resolve())
    for root in roots:
        wheel_name = _find_release_wheel_name_from_manifest(root)
        if wheel_name is not None:
            return wheel_name
    return fallback


def _ensure_ci_public_profile(cfg: dict[str, Any], profile_ids: list[str]) -> None:
    if CI_PUBLIC_PROFILE_ID not in profile_ids:
        return
    profiles = _require_dict(cfg.get("profiles"), "config.profiles")
    artifact_sets = _require_dict(cfg.get("artifact_sets"), "config.artifact_sets")
    if CI_PUBLIC_PROFILE_ID in profiles and CI_PUBLIC_PROFILE_ID in artifact_sets:
        return

    base_profile = copy.deepcopy(
        _require_dict(profiles.get(BASE_FLUXON_PROFILE_ID), f"config.profiles[{BASE_FLUXON_PROFILE_ID!r}]")
    )
    base_artifact_set = copy.deepcopy(
        _require_dict(
            artifact_sets.get(BASE_FLUXON_PROFILE_ID),
            f"config.artifact_sets[{BASE_FLUXON_PROFILE_ID!r}]",
        )
    )

    release_source = _require_dict(
        base_artifact_set.get("release_source"),
        f"config.artifact_sets[{BASE_FLUXON_PROFILE_ID!r}].release_source",
    )
    test_rsc_source = _require_dict(
        base_artifact_set.get("test_rsc_source"),
        f"config.artifact_sets[{BASE_FLUXON_PROFILE_ID!r}].test_rsc_source",
    )
    release_artifacts = _require_dict(
        base_artifact_set.get("release_artifacts"),
        f"config.artifact_sets[{BASE_FLUXON_PROFILE_ID!r}].release_artifacts",
    )
    release_source["key_prefix"] = f"profiles/{CI_PUBLIC_PROFILE_ID}"
    test_rsc_source["key_prefix"] = f"test_rsc/{CI_PUBLIC_PROFILE_ID}"
    release_artifacts["wheel"] = _ci_public_release_wheel_name(
        str(release_artifacts.get("wheel", "")).strip() or "fluxon-0.2.1-py3-none-any.whl"
    )
    base_profile["artifact_set"] = CI_PUBLIC_PROFILE_ID
    artifact_sets[CI_PUBLIC_PROFILE_ID] = base_artifact_set
    profiles[CI_PUBLIC_PROFILE_ID] = base_profile


def _bundle_relpath(path: Path, *, base: Path, dot_prefix: bool = False) -> str:
    rel = os.path.relpath(str(path.resolve()), str(base.resolve())).replace(os.sep, "/")
    if dot_prefix and rel != "." and not rel.startswith("."):
        return f"./{rel}"
    return rel


def _relocated_bundle_path(
    raw: Any,
    *,
    src_root: Path,
    dst_root: Path,
    base: Path,
    field_name: str,
    generated_bundle_child_name: str | None = None,
) -> Path:
    if not isinstance(raw, str) or not raw.strip():
        raise SystemExit(f"{field_name} must be a non-empty path string")
    raw_path = Path(raw).expanduser()
    if raw_path.is_absolute():
        resolved = raw_path.resolve()
        if _is_within_root(resolved, dst_root):
            return resolved
        if _is_within_root(resolved, src_root):
            return (dst_root / resolved.relative_to(src_root.resolve())).resolve()
        if generated_bundle_child_name is not None and resolved.name == generated_bundle_child_name:
            return (dst_root / generated_bundle_child_name).resolve()
        raise SystemExit(
            f"{field_name} must point inside the testbed bundle: path={resolved} src={src_root} dst={dst_root}"
        )
    return (base / raw_path).resolve()


def _normalize_run_local_testbed_bundle(
    *,
    src_root: Path,
    dst_root: Path,
    start_config_relpath: str,
) -> Path:
    relpath = _clean_bundle_relpath(start_config_relpath, field_name="--start-config-relpath")
    start_cfg = (dst_root / relpath).resolve()
    if not _is_within_root(start_cfg, dst_root):
        raise SystemExit(f"--start-config-relpath escapes testbed bundle: {relpath}")
    if not start_cfg.is_file():
        raise SystemExit(f"start config is missing inside testbed bundle: {start_cfg}")

    start_payload = _load_yaml_mapping(start_cfg, ctx=f"start config {start_cfg}")
    deployconf_path = _relocated_bundle_path(
        start_payload.get("deployconf_path"),
        src_root=src_root,
        dst_root=dst_root,
        base=start_cfg.parent,
        field_name="start_test_bed.deployconf_path",
    )
    if not _is_within_root(deployconf_path, dst_root):
        raise SystemExit(f"start_test_bed.deployconf_path escapes testbed bundle: {deployconf_path}")
    if not deployconf_path.is_file():
        raise SystemExit(f"start config deployconf_path is missing: {deployconf_path}")
    start_payload["deployconf_path"] = _bundle_relpath(deployconf_path, base=start_cfg.parent, dot_prefix=True)
    _write_yaml_mapping(start_cfg, start_payload)

    deployconf_payload = _load_yaml_mapping(deployconf_path, ctx=f"deployconf {deployconf_path}")
    mirror_outdir = _relocated_bundle_path(
        deployconf_payload.get("gen_k8s_daemonset_mirror_outdir"),
        src_root=src_root,
        dst_root=dst_root,
        base=deployconf_path.parent,
        field_name="deployconf.gen_k8s_daemonset_mirror_outdir",
        generated_bundle_child_name="gen_k8s_daemonset",
    )
    if not _is_within_root(mirror_outdir, dst_root):
        raise SystemExit(f"deployconf.gen_k8s_daemonset_mirror_outdir escapes testbed bundle: {mirror_outdir}")
    mirror_outdir.mkdir(parents=True, exist_ok=True)
    deployconf_payload["gen_k8s_daemonset_mirror_outdir"] = str(mirror_outdir.resolve())
    _write_yaml_mapping(deployconf_path, deployconf_payload)
    _sync_run_local_deployconf_from_normalized_view(deployconf_path=deployconf_path)

    manifest_path = start_cfg.with_name("manifest.json")
    if manifest_path.exists():
        try:
            manifest = _require_dict(
                json.loads(manifest_path.read_text(encoding="utf-8")),
                f"testbed bundle manifest {manifest_path}",
            )
        except Exception as exc:
            raise SystemExit(f"failed to load testbed bundle manifest {manifest_path}: {exc}") from exc

        manifest_targets = {
            "deployconf_path": deployconf_path,
            "start_config_path": start_cfg,
        }
        for field_name in ("ssh_config_path", "workdir"):
            if field_name not in manifest:
                continue
            target_path = _relocated_bundle_path(
                manifest.get(field_name),
                src_root=src_root,
                dst_root=dst_root,
                base=manifest_path.parent,
                field_name=f"manifest.{field_name}",
            )
            if not _is_within_root(target_path, dst_root):
                raise SystemExit(f"manifest.{field_name} escapes testbed bundle: {target_path}")
            if field_name == "workdir":
                target_path.mkdir(parents=True, exist_ok=True)
            elif not target_path.exists():
                raise SystemExit(f"manifest.{field_name} is missing inside testbed bundle: {target_path}")
            manifest_targets[field_name] = target_path

        for field_name, target_path in manifest_targets.items():
            manifest[field_name] = _bundle_relpath(target_path, base=manifest_path.parent)
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return start_cfg


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


def _local_test_stack_topology_port_offset(topology_key: Any) -> int:
    if isinstance(topology_key, int):
        return int(topology_key) * LOCAL_TEST_STACK_TOPOLOGY_PORT_SPAN
    elif isinstance(topology_key, str) and topology_key.isdigit():
        return int(topology_key) * LOCAL_TEST_STACK_TOPOLOGY_PORT_SPAN
    elif topology_key != "DEFAULT":
        raise SystemExit(f"unsupported test_stack port_alloc topology key: {topology_key!r}")
    return 0


def _local_test_stack_coordinator_port_base(*, controller_port: int, topology_key: Any) -> int:
    topology_offset = _local_test_stack_topology_port_offset(topology_key)
    port = int(controller_port) + LOCAL_TEST_STACK_COORDINATOR_PORT_OFFSET + topology_offset
    if port > 65535:
        port = LOCAL_TEST_STACK_COORDINATOR_FALLBACK_PORT_BASE + (
            (int(controller_port) + topology_offset) % LOCAL_TEST_STACK_COORDINATOR_FALLBACK_PORT_SPAN
        )
    if port <= 0 or port > 65535:
        raise SystemExit(f"computed local TEST_STACK coordinator_port_base out of range: {port}")
    return port


def _local_ephemeral_tcp_ports() -> set[int]:
    try:
        raw = Path("/proc/sys/net/ipv4/ip_local_port_range").read_text(encoding="utf-8").split()
    except FileNotFoundError:
        return set()
    if len(raw) != 2:
        return set()
    try:
        start, end = int(raw[0]), int(raw[1])
    except ValueError:
        return set()
    if start <= 0 or end < start or end > 65535:
        return set()
    return set(range(start, end + 1))


def _local_busy_tcp_ports() -> set[int]:
    ports: set[int] = set()
    for proc_path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = proc_path.read_text(encoding="utf-8").splitlines()[1:]
        except FileNotFoundError:
            continue
        for line in lines:
            parts = line.split()
            if len(parts) < 4:
                continue
            try:
                ports.add(int(parts[1].rsplit(":", 1)[1], 16))
            except ValueError:
                continue
    ports.update(_local_ephemeral_tcp_ports())
    return ports


def _find_local_tcp_port_block(
    *,
    preferred_start: int,
    required_count: int,
    busy_ports: set[int] | None = None,
) -> int:
    required_count = int(required_count)
    if required_count <= 0:
        raise SystemExit(f"required local TEST_STACK P2P port count must be positive: {required_count}")

    min_port = LOCAL_TEST_STACK_P2P_PORT_MIN
    max_start = LOCAL_TEST_STACK_P2P_PORT_MAX - required_count + 1
    if max_start < min_port:
        raise SystemExit(
            "local TEST_STACK P2P port window is too small: "
            f"required_count={required_count} min={LOCAL_TEST_STACK_P2P_PORT_MIN} max={LOCAL_TEST_STACK_P2P_PORT_MAX}"
        )

    busy = _local_busy_tcp_ports() if busy_ports is None else set(busy_ports)
    preferred = min(max(int(preferred_start), min_port), max_start)
    starts = list(range(preferred, max_start + 1)) + list(range(min_port, preferred))
    for start in starts:
        end = start + required_count - 1
        if all(port not in busy for port in range(start, end + 1)):
            return start

    raise SystemExit(
        "no free local TEST_STACK P2P port block found: "
        f"required_count={required_count} min={LOCAL_TEST_STACK_P2P_PORT_MIN} max={LOCAL_TEST_STACK_P2P_PORT_MAX}"
    )


def _local_test_stack_p2p_port_base(
    *,
    controller_port: int,
    topology_key: Any,
    required_count: int,
    busy_ports: set[int] | None = None,
) -> int:
    topology_offset = _local_test_stack_topology_port_offset(topology_key)
    search_span = LOCAL_TEST_STACK_P2P_PORT_MAX - LOCAL_TEST_STACK_P2P_PORT_MIN - int(required_count) + 1
    if search_span <= 0:
        raise SystemExit(f"required local TEST_STACK P2P port count is too large: {required_count}")
    preferred = LOCAL_TEST_STACK_P2P_PORT_MIN + (
        (int(controller_port) + topology_offset * 17) % search_span
    )
    return _find_local_tcp_port_block(
        preferred_start=preferred,
        required_count=int(required_count),
        busy_ports=busy_ports,
    )


def _local_test_stack_master_port_base(
    *,
    controller_port: int,
    topology_key: Any,
    required_count: int,
    busy_ports: set[int] | None = None,
) -> int:
    topology_offset = _local_test_stack_topology_port_offset(topology_key)
    search_span = LOCAL_TEST_STACK_P2P_PORT_MAX - LOCAL_TEST_STACK_P2P_PORT_MIN - int(required_count) + 1
    if search_span <= 0:
        raise SystemExit(f"required local TEST_STACK master port count is too large: {required_count}")
    preferred = LOCAL_TEST_STACK_P2P_PORT_MIN + (
        (int(controller_port) + topology_offset * 11 + 7000) % search_span
    )
    return _find_local_tcp_port_block(
        preferred_start=preferred,
        required_count=int(required_count),
        busy_ports=busy_ports,
    )


def _rewrite_test_stack_coordinator_ports_for_local_controller(
    suite: dict[str, Any],
    *,
    controller_port: int,
) -> None:
    busy_ports = _local_busy_tcp_ports()
    profiles = _require_dict(suite.get("profiles"), "suite.profiles")
    for profile_id, profile in profiles.items():
        if not isinstance(profile, dict):
            continue
        runtime = _require_dict(profile.get("runtime"), f"suite.profiles[{profile_id!r}].runtime")
        test_stack = _require_dict(
            runtime.get("test_stack"),
            f"suite.profiles[{profile_id!r}].runtime.test_stack",
        )
        port_alloc = _require_dict(
            test_stack.get("port_alloc"),
            f"suite.profiles[{profile_id!r}].runtime.test_stack.port_alloc",
        )
        by_topology = _require_dict(
            port_alloc.get("by_topology"),
            f"suite.profiles[{profile_id!r}].runtime.test_stack.port_alloc.by_topology",
        )
        for topology_key, entry in by_topology.items():
            if not isinstance(entry, dict):
                continue
            if "coordinator_port_base" not in entry:
                continue
            entry["coordinator_port_base"] = _local_test_stack_coordinator_port_base(
                controller_port=int(controller_port),
                topology_key=topology_key,
            )
            if "kv_master_port_base" in entry and "kv_master_port_stride" in entry:
                entry["kv_master_port_base"] = _local_test_stack_master_port_base(
                    controller_port=int(controller_port),
                    topology_key=topology_key,
                    required_count=int(entry["kv_master_port_stride"]),
                    busy_ports=busy_ports,
                )
                busy_ports.update(
                    range(
                        int(entry["kv_master_port_base"]),
                        int(entry["kv_master_port_base"]) + int(entry["kv_master_port_stride"]),
                    )
                )
            if "kv_p2p_port_base" in entry and "kv_p2p_port_stride" in entry:
                entry["kv_p2p_port_base"] = _local_test_stack_p2p_port_base(
                    controller_port=int(controller_port),
                    topology_key=topology_key,
                    required_count=int(entry["kv_p2p_port_stride"]),
                    busy_ports=busy_ports,
                )
                busy_ports.update(
                    range(
                        int(entry["kv_p2p_port_base"]),
                        int(entry["kv_p2p_port_base"]) + int(entry["kv_p2p_port_stride"]),
                    )
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


def _apply_single_host_logical_targets(
    cfg: dict[str, Any],
    *,
    profile_ids: list[str],
    required_count: int,
    anchor_ip_override: str | None,
) -> None:
    if required_count <= 0:
        raise SystemExit("--single-host-logical-targets requires a positive target count")
    override_ip = None
    if anchor_ip_override is not None:
        override_ip = str(anchor_ip_override).strip()
        if not override_ip:
            raise SystemExit("single-host anchor IP override must be non-empty")
    for profile_id in profile_ids:
        target_map = _profile_target_map(cfg, profile_id)
        ordered = _ordered_usable_targets(
            target_map,
            ctx=f"config.profiles[{profile_id!r}].runtime.test_stack.deploy.target_ip_map",
        )
        if not ordered:
            raise SystemExit(
                f"profile {profile_id!r} has no non-bastion target to use as the single-host anchor"
            )
        anchor_target = ordered[0]
        anchor_ip = target_map.get(anchor_target)
        if not isinstance(anchor_ip, str) or not anchor_ip.strip():
            raise SystemExit(
                f"profile {profile_id!r} anchor target {anchor_target!r} has no usable IP"
            )
        resolved_anchor_ip = override_ip or anchor_ip.strip()
        for idx in range(1, int(required_count) + 1):
            target_map[f"node-{idx}"] = resolved_anchor_ip


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


def _build_suite(
    cfg: dict[str, Any],
    args: argparse.Namespace,
    profile_ids: list[str],
    *,
    single_host_anchor_ip: str | None = None,
) -> dict[str, Any]:
    producer_count = int(args.producer_count)
    consumer_count = int(args.consumer_count)
    owner_count = int(args.owner_count)
    for name, value in (
        ("producer-count", producer_count),
        ("consumer-count", consumer_count),
        ("owner-count", owner_count),
        ("owner-dram-gib", int(args.owner_dram_gib)),
        ("duration-seconds", int(args.duration_seconds)),
        ("threads-per-process", int(args.threads_per_process)),
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

    single_host = bool(args.single_host_logical_targets)
    processes_per_target = owner_count if single_host else 1
    if single_host:
        if producer_count % processes_per_target != 0:
            raise SystemExit(
                "--single-host-logical-targets requires producer-count to be divisible by owner-count "
                f"so process fanout can preserve the requested count: producer={producer_count} owner={owner_count}"
            )
        if consumer_count % processes_per_target != 0:
            raise SystemExit(
                "--single-host-logical-targets requires consumer-count to be divisible by owner-count "
                f"so process fanout can preserve the requested count: consumer={consumer_count} owner={owner_count}"
            )
        producer_targets = producer_count // processes_per_target
        consumer_targets = consumer_count // processes_per_target
    else:
        producer_targets = producer_count
        consumer_targets = consumer_count
    topology = producer_targets + consumer_targets
    if owner_count > topology:
        raise SystemExit(
            f"owner-count={owner_count} cannot exceed benchmark topology={topology} "
            "when owner targets are co-located with benchmark targets"
        )
    if single_host:
        _apply_single_host_logical_targets(
            cfg,
            profile_ids=profile_ids,
            required_count=topology,
            anchor_ip_override=single_host_anchor_ip,
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
            "processes_per_target": processes_per_target,
            "threads_per_process": int(args.threads_per_process),
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
    if single_host:
        benchmark["owner_group_processes"] = 1

    _ensure_largescale_port_alloc(
        cfg,
        profile_ids=profile_ids,
        topology=topology,
        required_p2p_ports_per_slot=(
            producer_targets * processes_per_target * int(args.threads_per_process)
            + consumer_targets * processes_per_target
            + owner_count
            + 1
        ),
    )

    scenes = _require_dict(cfg.get("scenes"), "config.scenes")
    scene = copy.deepcopy(_require_dict(scenes.get(SCENE_ID), f"config.scenes[{SCENE_ID!r}]"))
    scene["test_stack"] = copy.deepcopy(_require_dict(scene.get("test_stack"), f"config.scenes[{SCENE_ID!r}].test_stack"))
    scene["test_stack"]["mode"] = "MPMC"
    scene["test_stack"]["role_weights"] = _role_weights_for_exact_mpmc_counts(
        producer_targets,
        consumer_targets,
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


def _prepare_run_local_testbed_bundle(
    *,
    source: str,
    workdir: Path,
    start_config_relpath: str,
) -> Path:
    src = _resolve_user_path(source)
    if not src.is_dir():
        raise SystemExit(f"--testbed-bundle-source must be an existing directory: {src}")
    dst = (workdir / "testbed_bundle").resolve()
    src_root_for_relocation = src.resolve()
    if src == dst:
        pass
    else:
        if src in dst.parents:
            raise SystemExit(f"--testbed-bundle-source cannot contain the run-local destination: src={src} dst={dst}")
        if dst in src.parents:
            raise SystemExit(f"--testbed-bundle-source cannot be inside the run-local destination: src={src} dst={dst}")
        if dst.exists():
            shutil.rmtree(dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst, symlinks=True)

    return _normalize_run_local_testbed_bundle(
        src_root=src_root_for_relocation,
        dst_root=dst,
        start_config_relpath=start_config_relpath,
    )


def _single_host_anchor_ip_from_start_config(start_cfg: Path) -> str:
    start_payload = yaml.safe_load(start_cfg.read_text(encoding="utf-8"))
    start = _require_dict(start_payload, f"start config {start_cfg}")
    raw_deployconf = start.get("deployconf_path")
    if not isinstance(raw_deployconf, str) or not raw_deployconf.strip():
        raise SystemExit(f"start config {start_cfg} must define deployconf_path")
    deployconf_path = Path(raw_deployconf).expanduser()
    if not deployconf_path.is_absolute():
        deployconf_path = (start_cfg.parent / deployconf_path).resolve()
    if not deployconf_path.is_file():
        raise SystemExit(f"start config deployconf_path is missing: {deployconf_path}")
    deployconf_payload = yaml.safe_load(deployconf_path.read_text(encoding="utf-8"))
    deployconf = _require_dict(deployconf_payload, f"deployconf {deployconf_path}")
    cluster_nodes = deployconf.get("cluster_nodes")
    if not isinstance(cluster_nodes, list) or not cluster_nodes:
        raise SystemExit(f"deployconf {deployconf_path} must define non-empty cluster_nodes")
    for index, raw_node in enumerate(cluster_nodes):
        node = _require_dict(raw_node, f"deployconf.cluster_nodes[{index}]")
        hostname = node.get("hostname")
        if isinstance(hostname, str) and "bastion" in hostname.lower():
            continue
        node_ip = node.get("ip")
        if isinstance(node_ip, str) and node_ip.strip():
            return node_ip.strip()
    raise SystemExit(f"deployconf {deployconf_path} has no non-bastion cluster node IP")


def _controller_port_from_start_config(start_cfg: Path) -> int:
    start_payload = yaml.safe_load(start_cfg.read_text(encoding="utf-8"))
    start = _require_dict(start_payload, f"start config {start_cfg}")
    raw_url = start.get("controller_url")
    if not isinstance(raw_url, str) or not raw_url.strip():
        raise SystemExit(f"start config {start_cfg} must define controller_url")
    parsed = urlparse(raw_url.strip())
    if parsed.port is None:
        raise SystemExit(f"start config {start_cfg} controller_url must include an explicit port: {raw_url}")
    return int(parsed.port)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Flat index entry for the TEST_STACK large-scale MQ benchmark "
            "(default: 4 owners at 1GiB, 160 producers, 8 consumers, 256-byte values)."
        )
    )
    parser.add_argument("--python", default=os.environ.get("PYTHON", sys.executable))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Base TEST_STACK suite YAML.")
    parser.add_argument("--workdir", default=str(DEFAULT_WORKDIR), help="test_runner workdir.")
    parser.add_argument("--suite-out", help="Generated suite YAML path; default is <workdir>/largescale_mq_suite.yaml.")
    parser.add_argument("--profile", action="append", dest="profiles", help="Profile id to run; repeat or comma-separate.")
    parser.add_argument("--action", choices=["run", "clean"], default="run")
    parser.add_argument("--generate-only", action="store_true", help="Write the generated suite YAML and do not invoke test_runner.")
    parser.add_argument(
        "--testbed-bundle-source",
        help="Existing TEST_STACK testbed bundle directory copied to <workdir>/testbed_bundle before a real run.",
    )
    parser.add_argument(
        "--start-config-relpath",
        default="start_test_bed.runner.yaml",
        help="Start-testbed config path inside the run-local testbed bundle.",
    )
    parser.add_argument(
        "--single-host-logical-targets",
        action="store_true",
        help="Generate node-1..N logical TEST_STACK targets on the first usable target IP of each selected profile.",
    )
    parser.add_argument("--owner-count", type=int, default=4)
    parser.add_argument("--owner-dram-gib", type=int, default=1)
    parser.add_argument("--producer-count", type=int, default=160)
    parser.add_argument("--consumer-count", type=int, default=8)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--value-size", type=int, default=256)
    parser.add_argument("--metric-warmup-seconds", type=int, default=0)
    parser.add_argument(
        "--threads-per-process",
        type=int,
        default=int(DEFAULT_BENCHMARK["threads_per_process"]),
        help="Worker threads per benchmark process.",
    )
    parser.add_argument("--op-timeout-seconds", type=int, default=30)
    parser.add_argument("--cluster-ready-timeout-seconds", type=int, default=1800)
    parser.add_argument("--consumer-sim-min-ms", type=int, default=700)
    parser.add_argument("--consumer-sim-max-ms", type=int, default=1500)
    args = parser.parse_args()

    workdir = _repo_path(args.workdir)
    if args.action == "clean":
        return call([args.python, "-u", str(RUNNER), "--workdir", str(workdir), "--action", "clean"])

    start_cfg: Path | None = None
    single_host_anchor_ip: str | None = None
    local_controller_port: int | None = None
    if not args.generate_only:
        if not args.testbed_bundle_source:
            raise SystemExit("--testbed-bundle-source is required unless --generate-only is set")
        start_cfg = _prepare_run_local_testbed_bundle(
            source=args.testbed_bundle_source,
            workdir=workdir,
            start_config_relpath=args.start_config_relpath,
        )
        local_controller_port = _controller_port_from_start_config(start_cfg)
        if bool(args.single_host_logical_targets):
            single_host_anchor_ip = _single_host_anchor_ip_from_start_config(start_cfg)

    config_path = _repo_path(args.config)
    if not config_path.exists():
        raise SystemExit(f"--config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as fh:
        cfg = _require_dict(yaml.safe_load(fh), f"config file {config_path}")

    profile_ids = _split_ids(args.profiles, default=DEFAULT_PROFILE_ID)
    _ensure_ci_public_profile(cfg, profile_ids)
    suite = _build_suite(
        cfg,
        args,
        profile_ids,
        single_host_anchor_ip=single_host_anchor_ip,
    )
    if local_controller_port is not None:
        _rewrite_test_stack_coordinator_ports_for_local_controller(
            suite,
            controller_port=local_controller_port,
        )

    suite_out = _repo_path(args.suite_out) if args.suite_out else (workdir / "largescale_mq_suite.yaml")
    suite_out.parent.mkdir(parents=True, exist_ok=True)
    with suite_out.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(suite, fh, sort_keys=False, allow_unicode=False)

    print(f"generated suite: {suite_out}", flush=True)
    if args.generate_only:
        return 0
    assert start_cfg is not None
    env = os.environ.copy()
    env["FLUXON_TEST_STACK_START_TEST_BED_CONFIG"] = str(start_cfg)
    return call(
        [args.python, "-u", str(RUNNER), "--config", str(suite_out), "--workdir", str(workdir), "--action", "run"],
        env=env,
    )


if __name__ == "__main__":
    raise SystemExit(main())
