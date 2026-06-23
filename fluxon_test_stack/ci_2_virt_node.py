#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SUITE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml"
DEFAULT_DEPLOYCONF_TEMPLATE = REPO_ROOT / "fluxon_test_stack" / "deployconf_testbed.yml"
DEFAULT_START_TEST_BED_TEMPLATE = REPO_ROOT / "fluxon_test_stack" / "start_test_bed.yaml"
DEFAULT_PACK_RELEASE_ENV_TEMPLATE = REPO_ROOT / "setup_and_pack" / "pack_fluxonkv_pylib_env.yaml.template"
DEFAULT_PACK_RELEASE_ENV_GEN_SCRIPT = REPO_ROOT / "setup_and_pack" / "ci" / "gen_pack_release_ci_config.py"
DEFAULT_PACK_RELEASE_STATIC_CONFIG = REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_static.yaml"
DEFAULT_RATHER_NO_GIT_SUBMODULE_SCRIPT = (
    REPO_ROOT / "fluxon_rs" / "scripts" / "rather_no_git_submodule.py"
)
DEFAULT_CI_2_VIRT_NODE_WORKDIR = REPO_ROOT / ".dever" / "ci_2_virt_node"
DEFAULT_RELEASE_DIR = REPO_ROOT / "fluxon_release"
PUBLIC_PROFILE_ID = "fluxon_tcp_thread"
PUBLIC_ARTIFACT_SET_ID = "fluxon_tcp_thread"
PUBLIC_TRANSPORT_FEATURE = "tcp_thread_transport"
DEFAULT_TESTBED_BOOTSTRAP_MODE = "bare_then_apply"
DEFAULT_TESTBED_UI_PORT = 18080
DEFAULT_TESTBED_CONTROLLER_PORT = 19080
DEFAULT_TESTBED_HOSTWORKDIR = Path("/mnt/nvme0/store_team_dev/fluxon_deploy")
LOCAL_PRIMARY_NODE_SUFFIX = "a"
LOCAL_SECONDARY_NODE_SUFFIX = "b"
TEST_STACK_START_TEST_BED_CONFIG_ENV = "FLUXON_TEST_STACK_START_TEST_BED_CONFIG"
PLACEHOLDER_WHEEL_NAME = "fluxon-0.0.0-ci-placeholder-cp38-abi3-manylinux_2_28_x86_64.whl"
SAME_HOST_LOCAL_MULTI_NODE_ETCD_CLIENT_PORT_OFFSET = 100
SAME_HOST_LOCAL_MULTI_NODE_GREPTIME_PORT_OFFSET = 110


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Canonical 2-virtual-node CI entrypoint. It packages release/test resources, starts a same-host "
            "dual-logical-node testbed, verifies controller apply on the testbed, runs test_runner, and builds docs."
        )
    )
    parser.add_argument(
        "--suite-path",
        type=Path,
        default=DEFAULT_SUITE_PATH,
        help="Suite YAML consumed by test_runner and rewritten for same-host local dual-node CI.",
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=DEFAULT_CI_2_VIRT_NODE_WORKDIR,
        help="State root for generated configs and local CI runs.",
    )
    parser.add_argument(
        "--testbed-hostworkdir",
        type=Path,
        dest="testbed_hostworkdir",
        default=DEFAULT_TESTBED_HOSTWORKDIR,
        help="Host-side runtime root used by the self-host testbed.",
    )
    parser.add_argument(
        "--release-dir",
        type=Path,
        default=DEFAULT_RELEASE_DIR,
        help="Release artifact root used for dispatch, runner reuse, and wheel discovery.",
    )
    parser.add_argument(
        "--scene-id",
        action="append",
        dest="scene_ids",
        default=[],
        help=(
            "Restrict the generated suite to the listed scene ids. May be passed multiple times. "
            "Defaults to every CI scene."
        ),
    )
    parser.add_argument(
        "--bootstrap-mode",
        choices=("bare_then_apply", "apply_only", "bare_only"),
        default=DEFAULT_TESTBED_BOOTSTRAP_MODE,
        help=(
            "End-state testbed bootstrap mode. The full CI flow still runs a bare bootstrap first, "
            "then an explicit apply validation pass."
        ),
    )
    parser.add_argument(
        "--reuse-existing-release",
        action="store_true",
        help="Reuse the existing top-level release and only prepare missing profile/test_rsc artifacts.",
    )
    parser.add_argument(
        "--skip-builder-image",
        action="store_true",
        help="Skip setup_and_pack/build_pack_fluxonkv_pylib_img.py.",
    )
    parser.add_argument(
        "--skip-pack",
        action="store_true",
        help="Skip release/test_rsc packaging and assume artifacts already exist.",
    )
    parser.add_argument(
        "--skip-dispatch",
        action="store_true",
        help="Skip deployment/manual_dispatch_release.py.",
    )
    parser.add_argument(
        "--skip-start-testbed",
        action="store_true",
        help="Skip fluxon_test_stack/start_test_bed.py for both bare bootstrap and apply validation.",
    )
    parser.add_argument(
        "--skip-apply-check",
        action="store_true",
        help="Skip the explicit apply-only validation pass after bare bootstrap.",
    )
    parser.add_argument(
        "--skip-runner",
        action="store_true",
        help="Skip fluxon_test_stack/test_runner.py.",
    )
    parser.add_argument(
        "--runner-workdir",
        type=Path,
        default=None,
        help="Optional explicit test_runner workdir. Defaults to <workdir>/runner_run.",
    )
    parser.add_argument(
        "--ui-port",
        type=int,
        default=DEFAULT_TESTBED_UI_PORT,
        help="test_runner_ui port for the generated start_test_bed config.",
    )
    parser.add_argument(
        "--controller-port",
        type=int,
        default=DEFAULT_TESTBED_CONTROLLER_PORT,
        help="Fluxon Ops controller HTTP port for the generated testbed configs.",
    )
    parser.add_argument(
        "--print-generated",
        action="store_true",
        help="Print generated config paths before executing commands.",
    )
    return parser.parse_args()


def _resolve_repo_root_cli_path(raw_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    return (REPO_ROOT / raw_path).resolve()


def _load_yaml_mapping(path: Path, *, ctx: str) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{ctx} must be a YAML mapping: {path}")
    return raw


def _detect_local_ipv4() -> str:
    try:
        output = subprocess.check_output(
            ["bash", "-lc", "ip -4 route get 1.1.1.1 | sed -n 's/.* src \\([0-9.]*\\).*/\\1/p' | head -n1"],
            text=True,
        ).strip()
        if output and "." in output and not output.startswith("127."):
            return output
    except Exception:
        pass
    try:
        output = subprocess.check_output(["bash", "-lc", "hostname -I"], text=True).strip()
        for token in output.split():
            if "." in token and not token.startswith("127."):
                return token
    except Exception:
        pass
    hostname = socket.gethostname()
    for _, _, _, _, sockaddr in socket.getaddrinfo(hostname, None, family=socket.AF_INET):
        ip = sockaddr[0]
        if ip and not ip.startswith("127."):
            return ip
    return "127.0.0.1"


def _same_host_local_testbed_host_ip() -> str:
    ip = _detect_local_ipv4()
    if ip.startswith("127."):
        raise RuntimeError(
            "ci_2_virt_node requires a non-loopback IPv4 address for same-host local node identity"
        )
    return ip


def _same_host_local_controller_access_ip(*, node_ip: str) -> str:
    return node_ip


def _cidr32_list_for_ips(*, ips: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for ip in ips:
        text = _require_nonempty_str(ip, "ips[]")
        cidr = f"{text}/32"
        if cidr in seen:
            continue
        seen.add(cidr)
        out.append(cidr)
    if not out:
        raise ValueError("ips must be non-empty")
    return out


def _detect_local_hostname() -> str:
    try:
        return subprocess.check_output(["bash", "-lc", "hostname -s"], text=True).strip()
    except Exception:
        return socket.gethostname().split(".")[0]


def _find_single_wheel(release_dir: Path, *, pattern: str, ctx: str) -> str:
    matches = sorted(path.name for path in release_dir.glob(pattern) if path.is_file())
    if len(matches) == 1:
        return matches[0]
    non_placeholder_matches = [name for name in matches if name != PLACEHOLDER_WHEEL_NAME]
    if len(non_placeholder_matches) == 1:
        return non_placeholder_matches[0]
    if len(matches) != 1:
        raise ValueError(f"{ctx} expected exactly one match for {pattern!r}, got {matches}")
    return matches[0]


def _local_logical_node_names(host_name: str) -> tuple[str, str]:
    return (f"{host_name}-{LOCAL_PRIMARY_NODE_SUFFIX}", f"{host_name}-{LOCAL_SECONDARY_NODE_SUFFIX}")


def _local_logical_hostworkdirs(hostworkdir: Path) -> tuple[Path, Path]:
    root = hostworkdir.resolve()
    return (root / LOCAL_PRIMARY_NODE_SUFFIX, root / LOCAL_SECONDARY_NODE_SUFFIX)


def _replace_template_node_names(obj: Any, *, primary_node_name: str, secondary_node_name: str) -> Any:
    if isinstance(obj, str):
        return (
            obj.replace("example-node-a", primary_node_name)
            .replace("example-node-b", secondary_node_name)
            .replace("deployer-runtime-node-a", f"deployer-runtime-{LOCAL_PRIMARY_NODE_SUFFIX}")
            .replace("deployer-runtime-node-b", f"deployer-runtime-{LOCAL_SECONDARY_NODE_SUFFIX}")
        )
    if isinstance(obj, list):
        return [
            _replace_template_node_names(
                item,
                primary_node_name=primary_node_name,
                secondary_node_name=secondary_node_name,
            )
            for item in obj
        ]
    if isinstance(obj, dict):
        return {
            key: _replace_template_node_names(
                value,
                primary_node_name=primary_node_name,
                secondary_node_name=secondary_node_name,
            )
            for key, value in obj.items()
        }
    return obj


def _default_scene_ids(suite_cfg: dict[str, Any]) -> list[str]:
    scenes = suite_cfg.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError("suite.scenes must be a mapping")
    out: list[str] = []
    for scene_id, scene_obj in scenes.items():
        if not isinstance(scene_obj, dict):
            continue
        if scene_obj.get("ci") is not None:
            out.append(str(scene_id))
    if not out:
        raise ValueError("suite has no CI scenes")
    return out


def _selected_scene_ids(args: argparse.Namespace, suite_cfg: dict[str, Any]) -> list[str]:
    if args.scene_ids:
        return list(dict.fromkeys(args.scene_ids))
    return _default_scene_ids(suite_cfg)


def _rewrite_suite_for_local_dual_nodes(
    *,
    suite_cfg: dict[str, Any],
    scene_ids: list[str],
    primary_node_name: str,
    secondary_node_name: str,
    host_ip: str,
    wheel_name: str,
    controller_port: int,
) -> dict[str, Any]:
    suite = copy.deepcopy(suite_cfg)
    scenes = suite.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError("suite.scenes must be a mapping")
    selected_scenes: dict[str, Any] = {}
    selected_scale_ids: list[str] = []
    for scene_id in scene_ids:
        scene_obj = scenes.get(scene_id)
        if not isinstance(scene_obj, dict):
            raise ValueError(f"unknown scene id for generated suite: {scene_id}")
        if scene_obj.get("ci") is None:
            raise ValueError(f"generated ci_2_virt_node suite currently supports CI scenes only: {scene_id}")
        selected = copy.deepcopy(scene_obj)
        select_cfg = selected.get("select")
        if not isinstance(select_cfg, dict):
            raise ValueError(f"scene[{scene_id}].select must be a mapping")
        select_cfg["profiles"] = [PUBLIC_PROFILE_ID]
        raw_scale_ids = select_cfg.get("scales")
        if not isinstance(raw_scale_ids, list) or not raw_scale_ids:
            raise ValueError(f"scene[{scene_id}].select.scales must be a non-empty list")
        for raw_scale_id in raw_scale_ids:
            scale_id = _require_nonempty_str(str(raw_scale_id), f"scene[{scene_id}].select.scales[]")
            if scale_id not in selected_scale_ids:
                selected_scale_ids.append(scale_id)
        selected_scenes[scene_id] = selected
    suite["scenes"] = selected_scenes

    run_cfg = suite.get("run")
    if not isinstance(run_cfg, dict):
        raise ValueError("suite.run must be a mapping")
    selectors = run_cfg.get("selectors")
    if not isinstance(selectors, dict):
        raise ValueError("suite.run.selectors must be a mapping")
    selectors["profile_ids"] = [PUBLIC_PROFILE_ID]

    scales = suite.get("scales")
    if not isinstance(scales, dict):
        raise ValueError("suite.scales must be a mapping")
    selected_scales: dict[str, Any] = {}
    for scale_id in selected_scale_ids:
        scale_obj = scales.get(scale_id)
        if not isinstance(scale_obj, dict):
            raise ValueError(f"selected scale is missing or not a mapping: {scale_id}")
        selected_scales[scale_id] = scale_obj
    suite["scales"] = selected_scales
    for scale_id, scale_obj in selected_scales.items():
        if not isinstance(scale_obj, dict):
            continue
        topology = scale_obj.get("topology")
        targets = scale_obj.get("targets")
        if not isinstance(targets, dict):
            raise ValueError(f"scale[{scale_id}].targets must be a mapping")
        if topology == 1:
            hosts = targets.get("hosts")
            if not isinstance(hosts, list) or len(hosts) != 1:
                raise ValueError(f"scale[{scale_id}].targets.hosts must be a single-host list for topology=1")
            targets["hosts"] = [primary_node_name]
            targets["primary"] = primary_node_name
            targets.pop("secondary", None)
            continue
        if topology == 2:
            hosts = targets.get("hosts")
            if not isinstance(hosts, list) or len(hosts) != 2:
                raise ValueError(f"scale[{scale_id}].targets.hosts must be a two-host list for topology=2")
            targets["hosts"] = [primary_node_name, secondary_node_name]
            targets["primary"] = primary_node_name
            targets["secondary"] = secondary_node_name

    artifact_sets = suite.get("artifact_sets")
    if not isinstance(artifact_sets, dict):
        raise ValueError("suite.artifact_sets must be a mapping")
    public_artifact_set = artifact_sets.get("fluxon_tcp")
    if not isinstance(public_artifact_set, dict):
        raise ValueError("suite must contain artifact_sets.fluxon_tcp")
    artifact_sets[PUBLIC_ARTIFACT_SET_ID] = copy.deepcopy(public_artifact_set)
    public_release_source = artifact_sets[PUBLIC_ARTIFACT_SET_ID].get("release_source")
    public_test_rsc_source = artifact_sets[PUBLIC_ARTIFACT_SET_ID].get("test_rsc_source")
    if not isinstance(public_release_source, dict) or not isinstance(public_test_rsc_source, dict):
        raise ValueError("public artifact set must define release_source and test_rsc_source")
    public_release_source["key_prefix"] = f"profiles/{PUBLIC_PROFILE_ID}"
    public_test_rsc_source["key_prefix"] = f"test_rsc/{PUBLIC_PROFILE_ID}"
    artifact_sets[PUBLIC_ARTIFACT_SET_ID]["release_artifacts"] = {"wheel": wheel_name}
    suite["artifact_sets"] = {PUBLIC_ARTIFACT_SET_ID: artifact_sets[PUBLIC_ARTIFACT_SET_ID]}

    profiles = suite.get("profiles")
    if not isinstance(profiles, dict):
        raise ValueError("suite.profiles must be a mapping")
    public_profile = profiles.get("fluxon_tcp")
    if not isinstance(public_profile, dict):
        raise ValueError("suite must contain profiles.fluxon_tcp")
    generated_profile = copy.deepcopy(public_profile)
    generated_profile["artifact_set"] = PUBLIC_ARTIFACT_SET_ID
    runtime = generated_profile.get("runtime")
    if not isinstance(runtime, dict):
        raise ValueError("generated public profile runtime must be a mapping")
    ci_base_runtime_host_ports = {
        "etcd": int(controller_port) + SAME_HOST_LOCAL_MULTI_NODE_ETCD_CLIENT_PORT_OFFSET,
        "greptime": int(controller_port) + SAME_HOST_LOCAL_MULTI_NODE_GREPTIME_PORT_OFFSET,
    }
    ci_runtime = runtime.get("ci")
    if not isinstance(ci_runtime, dict):
        raise ValueError("generated public profile must define runtime.ci")
    scene_configs = ci_runtime.get("scene_configs")
    if scene_configs is not None:
        if not isinstance(scene_configs, dict):
            raise ValueError("generated public profile runtime.ci.scene_configs must be a mapping")
        kv_scene_config = scene_configs.get("ci_top_attention_bin_kvtest")
        if kv_scene_config is not None:
            if not isinstance(kv_scene_config, dict):
                raise ValueError(
                    "generated public profile runtime.ci.scene_configs['ci_top_attention_bin_kvtest'] must be a mapping"
                )
            # The generated public profile is fixed to the tcp-thread transport branch.
            kv_scene_config["kv_transport_feature"] = PUBLIC_TRANSPORT_FEATURE
    for runtime_key in ("ci", "test_stack"):
        runtime_block = runtime.get(runtime_key)
        if not isinstance(runtime_block, dict):
            continue
        deploy_cfg = runtime_block.get("deploy")
        if not isinstance(deploy_cfg, dict):
            raise ValueError(f"generated public profile runtime.{runtime_key}.deploy must be a mapping")
        deploy_cfg["target_ip_map"] = {
            primary_node_name: host_ip,
            secondary_node_name: host_ip,
        }
        if runtime_key == "ci":
            runtime_contracts = runtime_block.get("runtime_contracts")
            if not isinstance(runtime_contracts, dict):
                raise ValueError("generated public profile runtime.ci.runtime_contracts must be a mapping")
            for contract in runtime_contracts.values():
                if not isinstance(contract, dict):
                    continue
                base_runtime = contract.get("base_runtime")
                if isinstance(base_runtime, dict):
                    for svc_name in ("etcd", "greptime"):
                        svc_cfg = base_runtime.get(svc_name)
                        if isinstance(svc_cfg, dict):
                            svc_cfg["target"] = primary_node_name
                            endpoint_cfg = svc_cfg.get("endpoint")
                            if isinstance(endpoint_cfg, dict):
                                endpoint_cfg["host_port"] = int(ci_base_runtime_host_ports[svc_name])
                case_runtime = contract.get("case_runtime")
                if isinstance(case_runtime, dict):
                    master_cfg = case_runtime.get("master")
                    if isinstance(master_cfg, dict):
                        deployer_cfg = master_cfg.get("deployer")
                        if isinstance(deployer_cfg, dict):
                            deployer_cfg["target"] = primary_node_name
        if runtime_key == "test_stack":
            deploy_templates = runtime_block.get("deploy_templates")
            if isinstance(deploy_templates, dict):
                coordinator = deploy_templates.get("coordinator")
                if isinstance(coordinator, dict):
                    deployer_cfg = coordinator.get("deployer")
                    if isinstance(deployer_cfg, dict):
                        deployer_cfg["target"] = primary_node_name
            runtime_cfg = runtime_block.get("runtime_config")
            if isinstance(runtime_cfg, dict):
                alluxio_cfg = runtime_cfg.get("alluxio")
                if isinstance(alluxio_cfg, dict):
                    alluxio_cfg["mount_root_by_target"] = {
                        primary_node_name: "/mnt/alluxio",
                        secondary_node_name: "/mnt/alluxio",
                    }

    suite["profiles"] = {PUBLIC_PROFILE_ID: generated_profile}
    return suite


def _require_mapping_rewritten_template(
    payload: dict[str, Any],
    *,
    primary_node_name: str,
    secondary_node_name: str,
) -> dict[str, Any]:
    rewritten = _replace_template_node_names(
        copy.deepcopy(payload),
        primary_node_name=primary_node_name,
        secondary_node_name=secondary_node_name,
    )
    if not isinstance(rewritten, dict):
        raise ValueError("rewritten template payload must stay a mapping")
    return rewritten


def _rewrite_deployconf_for_local_dual_nodes(
    *,
    deployconf_cfg: dict[str, Any],
    primary_node_name: str,
    secondary_node_name: str,
    host_ip: str,
    primary_hostworkdir: Path,
    secondary_hostworkdir: Path,
    wheel_name: str,
    controller_port: int,
) -> dict[str, Any]:
    cfg = _require_mapping_rewritten_template(
        deployconf_cfg,
        primary_node_name=primary_node_name,
        secondary_node_name=secondary_node_name,
    )
    cfg["name_prefix"] = "fluxon-ci-2-virt-node-local2"
    cfg["gen_k8s_daemonset_mirror_outdir"] = str((primary_hostworkdir / "gen_k8s_daemonset").resolve())
    cfg["cluster_nodes"] = [
        {
            "hostname": primary_node_name,
            "ip": host_ip,
            "hostworkdir": str(primary_hostworkdir.resolve()),
            "execution_mode": "local",
            "ssh_host": "127.0.0.1",
            "ssh_user": _require_nonempty_str(os.environ.get("USER", ""), "USER"),
            "ssh_port": 22,
            "ssh_password": None,
        },
        {
            "hostname": secondary_node_name,
            "ip": host_ip,
            "hostworkdir": str(secondary_hostworkdir.resolve()),
            "execution_mode": "local",
            "ssh_host": "127.0.0.1",
            "ssh_user": _require_nonempty_str(os.environ.get("USER", ""), "USER"),
            "ssh_port": 22,
            "ssh_password": None,
        },
    ]
    atomic_groups = cfg.get("atomic_groups")
    if isinstance(atomic_groups, dict):
        controller_group = atomic_groups.get("fluxon_core_controller")
        if isinstance(controller_group, dict):
            controller_group["nodes"] = [primary_node_name, secondary_node_name]
    global_envs = cfg.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")
    global_envs["FLUXON_RELEASE_WHEEL"] = wheel_name
    global_envs["FLUXON_RELEASE_WHEEL_PY"] = wheel_name
    global_envs["FLUXON_CLUSTER_NODE_IDS"] = f"{primary_node_name} {secondary_node_name}"
    global_envs["MASTER__PORT"] = str(int(controller_port))
    global_envs["FLUXON_OPS_UI_BASE_URL"] = f"http://${{OPS_CONTROLLER__NODE_ID__IP}}:{int(controller_port)}"
    fetch_cmd = global_envs.get("FLUXON_RELEASE_WHEEL_FETCH_CMD")
    if not isinstance(fetch_cmd, str):
        raise ValueError("deployconf.global_envs.FLUXON_RELEASE_WHEEL_FETCH_CMD must be a string")
    global_envs["FLUXON_RELEASE_WHEEL_FETCH_CMD"] = fetch_cmd.replace(
        '--wheel-py "$FLUXON_RELEASE_WHEEL_PY" --wheel-pyo3 "$FLUXON_RELEASE_WHEEL_PYO3"',
        '--wheel "$FLUXON_RELEASE_WHEEL"',
    )
    service_cfg = cfg.get("service")
    if not isinstance(service_cfg, dict):
        raise ValueError("deployconf.service must be a mapping")
    ops_controller_cfg = service_cfg.get("ops_controller")
    if not isinstance(ops_controller_cfg, dict):
        raise ValueError("deployconf.service.ops_controller must be a mapping")
    ops_controller_cfg["port"] = int(controller_port)
    master_cfg = service_cfg.get("master")
    if not isinstance(master_cfg, dict):
        raise ValueError("deployconf.service.master must be a mapping")
    entrypoint = master_cfg.get("entrypoint")
    if not isinstance(entrypoint, str):
        raise ValueError("deployconf.service.master.entrypoint must be a string")
    new_cidr_lines = "".join(f'    - "{cidr}"\n' for cidr in _cidr32_list_for_ips(ips=[host_ip]))
    new_block = "network:\n  subnet_whitelist:\n" + new_cidr_lines
    entrypoint_updated, replaced = re.subn(
        r'network:\n  subnet_whitelist:\n(?:    - ".*"\n)+',
        new_block,
        entrypoint,
        count=1,
    )
    if replaced != 1:
        raise ValueError("deployconf.service.master.entrypoint missing expected subnet_whitelist block")
    master_cfg["entrypoint"] = entrypoint_updated
    return cfg


def _rewrite_start_test_bed_for_local_dual_nodes(
    *,
    start_cfg: dict[str, Any],
    generated_deployconf_path: Path,
    primary_node_name: str,
    controller_access_ip: str,
    controller_port: int,
    ui_port: int,
    ui_workdir: Path,
) -> dict[str, Any]:
    cfg = copy.deepcopy(start_cfg)
    cfg["deployconf_path"] = str(generated_deployconf_path)
    cfg["controller_url"] = f"http://{controller_access_ip}:{controller_port}/r/ops/fluxon_testbed"
    cfg["controller_basic_auth"] = {"username": "ops_admin", "password": "ops_password"}
    ui_cfg = cfg.get("test_runner_ui")
    if not isinstance(ui_cfg, dict):
        raise ValueError("start_test_bed.test_runner_ui must be a mapping")
    ui_cfg["enabled"] = True
    ui_cfg["host"] = "0.0.0.0"
    ui_cfg["port"] = int(ui_port)
    ui_cfg["workdir"] = str(ui_workdir)
    ui_cfg["gitops_config_path"] = None
    bootstrap_phases = cfg.get("bootstrap_phases")
    if isinstance(bootstrap_phases, list):
        for phase in bootstrap_phases:
            if isinstance(phase, dict) and "node" in phase:
                phase["node"] = primary_node_name
    return cfg


def _rewrite_start_test_bed_for_apply_check(
    *,
    start_cfg: dict[str, Any],
) -> dict[str, Any]:
    cfg = copy.deepcopy(start_cfg)
    deploy_workloads = cfg.get("deploy_workloads")
    if not isinstance(deploy_workloads, list):
        raise ValueError("start_test_bed.deploy_workloads must be a list")
    cfg["deploy_workloads"] = [
        item
        for item in deploy_workloads
        if str(item) != "fluxon_core_controller"
    ]
    return cfg


def _require_nonempty_str(value: str, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding="utf-8")


def _prepare_pack_release_runtime_dirs(*, project_data_root: Path) -> None:
    root = project_data_root.resolve()
    for relpath in (
        Path("manylinux-release"),
        Path("manylinux-cache/cargo-registry"),
        Path("manylinux-cache/cargo-git"),
    ):
        (root / relpath).mkdir(parents=True, exist_ok=True)


def _run(argv: list[str], *, env: dict[str, str] | None = None) -> None:
    print("RUN: " + " ".join(_shell_quote(part) for part in argv), flush=True)
    subprocess.check_call(argv, cwd=str(REPO_ROOT), env=env)


def _shell_quote(text: str) -> str:
    if not text:
        return "''"
    safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./:=@+-"
    if all(ch in safe for ch in text):
        return text
    return "'" + text.replace("'", "'\\''") + "'"


def _ensure_ci_pack_release_env(
    *,
    project_data_root: Path,
    env_out_path: Path,
    env_template_path: Path = DEFAULT_PACK_RELEASE_ENV_TEMPLATE,
    generator_script_path: Path = DEFAULT_PACK_RELEASE_ENV_GEN_SCRIPT,
) -> Path:
    resolved_env_path = env_out_path.resolve()
    project_data_root = project_data_root.resolve()
    project_data_root.mkdir(parents=True, exist_ok=True)
    _run(
        [
            sys.executable,
            str(generator_script_path.resolve()),
            "--env-template",
            str(env_template_path.resolve()),
            "--out-path",
            str(resolved_env_path),
            "--project-data-root",
            str(project_data_root),
        ]
    )
    if not resolved_env_path.is_file():
        raise RuntimeError(f"failed to generate pack_release env companion: {resolved_env_path}")
    return resolved_env_path


def _sync_rather_no_git_submodule(
    script_path: Path = DEFAULT_RATHER_NO_GIT_SUBMODULE_SCRIPT,
) -> None:
    resolved_script_path = script_path.resolve()
    if not resolved_script_path.is_file():
        raise RuntimeError(f"missing rather_no_git_submodule entrypoint: {resolved_script_path}")
    _run([sys.executable, str(resolved_script_path)])


def _render_ci_nix_pack_config(
    *,
    static_config_path: Path,
    env_companion_path: Path,
    out_path: Path,
    repo_root: Path = REPO_ROOT,
) -> Path:
    static_cfg = _load_yaml_mapping(static_config_path.resolve(), ctx="NIX pack static config")
    env_cfg = _load_yaml_mapping(env_companion_path.resolve(), ctx="CI pack env companion")
    merged_cfg = copy.deepcopy(static_cfg)
    merged_cfg.update(copy.deepcopy(env_cfg))
    merged_cfg["project_root"] = str(repo_root.resolve())

    profile_cfg = merged_cfg.get("profile")
    if not isinstance(profile_cfg, dict):
        raise ValueError("NIX pack config profile must be a mapping")
    profile_cfg["build_root_path"] = str(repo_root.resolve())

    out_path = out_path.resolve()
    _write_yaml(out_path, merged_cfg)
    return out_path


def _build_generated_configs(
    *,
    args: argparse.Namespace,
    workdir: Path,
    generated_dir: Path,
    suite_cfg: dict[str, Any],
    deployconf_template: dict[str, Any],
    start_test_bed_template: dict[str, Any],
    host_name: str,
    host_ip: str,
    controller_access_ip: str,
    primary_node_name: str,
    secondary_node_name: str,
    primary_hostworkdir: Path,
    secondary_hostworkdir: Path,
    wheel_name: str,
) -> dict[str, Any]:
    scene_ids = _selected_scene_ids(args, suite_cfg)
    generated_suite = _rewrite_suite_for_local_dual_nodes(
        suite_cfg=suite_cfg,
        scene_ids=scene_ids,
        primary_node_name=primary_node_name,
        secondary_node_name=secondary_node_name,
        host_ip=host_ip,
        wheel_name=wheel_name,
        controller_port=int(args.controller_port),
    )
    generated_deployconf = _rewrite_deployconf_for_local_dual_nodes(
        deployconf_cfg=deployconf_template,
        primary_node_name=primary_node_name,
        secondary_node_name=secondary_node_name,
        host_ip=host_ip,
        primary_hostworkdir=primary_hostworkdir,
        secondary_hostworkdir=secondary_hostworkdir,
        wheel_name=wheel_name,
        controller_port=int(args.controller_port),
    )
    generated_start_cfg = _rewrite_start_test_bed_for_local_dual_nodes(
        start_cfg=start_test_bed_template,
        generated_deployconf_path=generated_dir / "deployconf_testbed.local.yaml",
        primary_node_name=primary_node_name,
        controller_access_ip=controller_access_ip,
        controller_port=int(args.controller_port),
        ui_port=int(args.ui_port),
        ui_workdir=workdir / "test_runner_ui_runtime",
    )
    generated_apply_check_cfg = _rewrite_start_test_bed_for_apply_check(
        start_cfg=generated_start_cfg,
    )

    suite_path = generated_dir / "ci_test_list.local.yaml"
    deployconf_path = generated_dir / "deployconf_testbed.local.yaml"
    start_cfg_path = generated_dir / "start_test_bed.local.yaml"
    start_apply_check_cfg_path = generated_dir / "start_test_bed.apply_check.local.yaml"
    _write_yaml(suite_path, generated_suite)
    _write_yaml(deployconf_path, generated_deployconf)
    _write_yaml(start_cfg_path, generated_start_cfg)
    _write_yaml(start_apply_check_cfg_path, generated_apply_check_cfg)

    runner_workdir = args.runner_workdir.resolve() if args.runner_workdir else (workdir / "runner_run").resolve()
    bootstrap_root = (workdir / "start_test_bed").resolve()
    return {
        "suite_path": suite_path,
        "deployconf_path": deployconf_path,
        "start_test_bed_path": start_cfg_path,
        "start_test_bed_apply_check_path": start_apply_check_cfg_path,
        "bootstrap_root": bootstrap_root,
        "bootstrap_bare_workdir": bootstrap_root / "bare",
        "bootstrap_apply_workdir": bootstrap_root / "apply",
        "runner_workdir": runner_workdir,
        "host_name": host_name,
        "host_ip": host_ip,
        "controller_access_ip": controller_access_ip,
        "primary_node_name": primary_node_name,
        "secondary_node_name": secondary_node_name,
        "primary_hostworkdir": str(primary_hostworkdir),
        "secondary_hostworkdir": str(secondary_hostworkdir),
        "scene_ids": scene_ids,
    }


def _print_generated(metadata: dict[str, Any]) -> None:
    serializable = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in metadata.items()
    }
    print(json.dumps(serializable, ensure_ascii=False, indent=2, sort_keys=True))


def _runner_env(*, release_dir: Path, start_cfg_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"] = str(release_dir.resolve())
    env[TEST_STACK_START_TEST_BED_CONFIG_ENV] = str(start_cfg_path.resolve())
    return env


def main() -> int:
    args = _parse_args()
    workdir = _resolve_repo_root_cli_path(args.workdir)
    hostworkdir = (
        args.testbed_hostworkdir.resolve()
        if args.testbed_hostworkdir.is_absolute()
        else args.testbed_hostworkdir.resolve()
    )
    generated_dir = (workdir / "generated").resolve()
    generated_dir.mkdir(parents=True, exist_ok=True)

    suite_cfg = _load_yaml_mapping(_resolve_repo_root_cli_path(args.suite_path), ctx="ci suite template")
    deployconf_template = _load_yaml_mapping(DEFAULT_DEPLOYCONF_TEMPLATE, ctx="deployconf template")
    start_test_bed_template = _load_yaml_mapping(DEFAULT_START_TEST_BED_TEMPLATE, ctx="start_test_bed template")

    host_name = _detect_local_hostname()
    host_ip = _same_host_local_testbed_host_ip()
    controller_access_ip = _same_host_local_controller_access_ip(node_ip=host_ip)
    primary_node_name, secondary_node_name = _local_logical_node_names(host_name)
    primary_hostworkdir, secondary_hostworkdir = _local_logical_hostworkdirs(hostworkdir)
    release_dir = _resolve_repo_root_cli_path(args.release_dir)

    # Generate a pack-time suite first so pack_test_stack_rsc can derive the selected public profile.
    pack_metadata = _build_generated_configs(
        args=args,
        workdir=workdir,
        generated_dir=generated_dir,
        suite_cfg=suite_cfg,
        deployconf_template=deployconf_template,
        start_test_bed_template=start_test_bed_template,
        host_name=host_name,
        host_ip=host_ip,
        controller_access_ip=controller_access_ip,
        primary_node_name=primary_node_name,
        secondary_node_name=secondary_node_name,
        primary_hostworkdir=primary_hostworkdir,
        secondary_hostworkdir=secondary_hostworkdir,
        wheel_name=PLACEHOLDER_WHEEL_NAME,
    )

    pack_release_runtime_root = (workdir / "pack_release_runtime").resolve()
    ci_pack_env_path = (generated_dir / "setup_and_pack" / "pack_fluxonkv_pylib_env.yaml").resolve()
    ci_nix_pack_config_path = (generated_dir / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_ci.yaml").resolve()

    if not args.skip_pack:
        _sync_rather_no_git_submodule()
        if not args.skip_builder_image:
            _run([sys.executable, str((REPO_ROOT / "setup_and_pack" / "build_pack_fluxonkv_pylib_img.py").resolve())])
        _prepare_pack_release_runtime_dirs(project_data_root=pack_release_runtime_root)
        _ensure_ci_pack_release_env(
            project_data_root=pack_release_runtime_root,
            env_out_path=ci_pack_env_path,
        )
        _render_ci_nix_pack_config(
            static_config_path=DEFAULT_PACK_RELEASE_STATIC_CONFIG,
            env_companion_path=ci_pack_env_path,
            out_path=ci_nix_pack_config_path,
        )
        pack_cmd = [
            sys.executable,
            str((REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py").resolve()),
            "--all-profiles",
            "--release-dir",
            str(release_dir),
            "-c",
            str(pack_metadata["suite_path"]),
        ]
        if args.reuse_existing_release:
            pack_cmd.append("--reuse-existing-release")
        pack_env = os.environ.copy()
        pack_env["FLUXON_PACK_RELEASE_NIX_CONFIG"] = str(ci_nix_pack_config_path)
        _run(pack_cmd, env=pack_env)

    wheel_name = _find_single_wheel(release_dir, pattern="fluxon-*.whl", ctx="top-level release wheel")

    metadata = _build_generated_configs(
        args=args,
        workdir=workdir,
        generated_dir=generated_dir,
        suite_cfg=suite_cfg,
        deployconf_template=deployconf_template,
        start_test_bed_template=start_test_bed_template,
        host_name=host_name,
        host_ip=host_ip,
        controller_access_ip=controller_access_ip,
        primary_node_name=primary_node_name,
        secondary_node_name=secondary_node_name,
        primary_hostworkdir=primary_hostworkdir,
        secondary_hostworkdir=secondary_hostworkdir,
        wheel_name=wheel_name,
    )
    metadata["pack_release_runtime_root"] = pack_release_runtime_root
    metadata["ci_pack_env_path"] = ci_pack_env_path
    metadata["ci_nix_pack_config_path"] = ci_nix_pack_config_path

    if args.print_generated:
        _print_generated(metadata)

    if not args.skip_dispatch:
        dispatch_cmd = [
            sys.executable,
            str((REPO_ROOT / "deployment" / "manual_dispatch_release.py").resolve()),
            "-c",
            str(metadata["deployconf_path"]),
            "--release-dir",
            str(release_dir),
            "--release-scope",
            "deploy_and_profiles",
        ]
        _run(dispatch_cmd)

    if not args.skip_start_testbed:
        metadata["bootstrap_bare_workdir"].mkdir(parents=True, exist_ok=True)
        start_bare_cmd = [
            sys.executable,
            str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()),
            "-c",
            str(metadata["start_test_bed_path"]),
            "-w",
            str(metadata["bootstrap_bare_workdir"]),
            "--bootstrap-mode",
            "bare_only" if not args.skip_apply_check else args.bootstrap_mode,
        ]
        _run(start_bare_cmd)

        if not args.skip_apply_check:
            metadata["bootstrap_apply_workdir"].mkdir(parents=True, exist_ok=True)
            start_apply_cmd = [
                sys.executable,
                str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()),
                "-c",
                str(metadata["start_test_bed_apply_check_path"]),
                "-w",
                str(metadata["bootstrap_apply_workdir"]),
                "--bootstrap-mode",
                "apply_only",
            ]
            _run(start_apply_cmd)
        elif args.bootstrap_mode in ("apply_only", "bare_then_apply"):
            metadata["bootstrap_apply_workdir"].mkdir(parents=True, exist_ok=True)
            start_apply_cmd = [
                sys.executable,
                str((REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py").resolve()),
                "-c",
                str(metadata["start_test_bed_path"]),
                "-w",
                str(metadata["bootstrap_apply_workdir"]),
                "--bootstrap-mode",
                args.bootstrap_mode,
            ]
            _run(start_apply_cmd)

    if not args.skip_runner:
        runner_workdir = Path(metadata["runner_workdir"])
        runner_workdir.mkdir(parents=True, exist_ok=True)
        runner_cmd = [
            sys.executable,
            str((REPO_ROOT / "fluxon_test_stack" / "test_runner.py").resolve()),
            "-c",
            str(metadata["suite_path"]),
            "-w",
            str(runner_workdir),
        ]
        _run(runner_cmd, env=_runner_env(release_dir=release_dir, start_cfg_path=Path(metadata["start_test_bed_path"])))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
