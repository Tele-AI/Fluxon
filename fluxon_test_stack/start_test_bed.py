#!/usr/bin/env python3

import argparse
import base64
import binascii
import copy
import fcntl
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import hashlib
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DEPLOYMENT_DIR = REPO_ROOT / "deployment"
DEPLOYMENT_UTILS_DIR = DEPLOYMENT_DIR / "utils"
sys.path.insert(0, str(DEPLOYMENT_DIR))
sys.path.insert(0, str(DEPLOYMENT_UTILS_DIR))
import manual_dispatch_release
from utils import log_shard
from selection_runtime import (
    atomic_group_member_authority_name as _selection_atomic_group_member_authority_name,
    atomic_group_member_selection_workload_name as _selection_atomic_group_member_selection_workload_name,
    daemonset_selection_supervisor_label as _selection_daemonset_supervisor_label,
    plain_selection_authority_name as _selection_plain_selection_authority_name,
    plain_selection_workload_name as _selection_plain_workload_name,
    resolve_coverage_selection_name as _selection_resolve_coverage_selection_name,
    resolve_selection_nodes as _selection_resolve_selection_nodes,
    resolve_selection_target_nodes as _selection_resolve_selection_target_nodes,
    resolve_selection_service_name as _selection_resolve_selection_service_name,
)


SCHEMA_VERSION = 6
OPS_AGENT_INSTANCE_KEY_PREFIX = "fluxon_ops_"
OPS_AGENT_DESIRED_SNAPSHOT_FILENAME = "agent_desired_snapshot.json"
OPS_AGENT_DESIRED_SNAPSHOT_MISSING_SENTINEL = "__FLUXON_OPS_AGENT_SNAPSHOT_MISSING__"
HTTP_TIMEOUT_SECONDS = 30
CONTROLLER_TRANSIENT_HTTP_CODES = (502, 503, 504)
INITIAL_CONTROLLER_REACHABILITY_PROBE_TIMEOUT_SECONDS = 5.0
BOOTSTRAP_MODE_BARE_THEN_APPLY = "bare_then_apply"
BOOTSTRAP_MODE_APPLY_ONLY = "apply_only"
BOOTSTRAP_MODE_BARE_ONLY = "bare_only"
PHASE_MODE_FIXED_BARE = "fixed_bare"
PHASE_MODE_COVERAGE_BARE = "coverage_bare"
BOOTSTRAP_RUNTIME_DIR = (REPO_ROOT / "fluxon_test_stack" / "start_test_bed").resolve()
BOOTSTRAP_TARGET_LOCK_DIR = BOOTSTRAP_RUNTIME_DIR / "target_locks"
CONTROL_PLANE_HANDOVER_SERVICE_NAMES = {"master", "owner", "ops_controller", "ops_agent"}
LOCAL_CONTROLLER_BOOTSTRAP_SERVICE_NAMES = {"master", "owner", "ops_controller", "ops_agent"}
DELETE_APPLY_RETRYABLE_ERRS = (
    "one or more workloads may still be stopping",
    "one or more agents failed to stop workload(s)",
)
WAIT_DELETE_APPLY_REQUIRES_DELETE_ERR = "wait_delete_apply requires delete_apply first"
DEPLOY_GUARD_ERR = "another deploy operation is in-flight; try again later"
DEPLOY_GUARD_WAIT_SECONDS = 120.0
DEPLOY_GUARD_POLL_SECONDS = 2.0
INOTIFY_MAX_USER_WATCHES_PROC_PATH = Path("/proc/sys/fs/inotify/max_user_watches")
INOTIFY_MAX_USER_INSTANCES_PROC_PATH = Path("/proc/sys/fs/inotify/max_user_instances")
RELEASE_MANIFEST_SHA256_ENV_KEY = "FLUXON_RELEASE_MANIFEST_SHA256"
_CONTROLLER_BASIC_AUTH_HEADER_NAME = "x-fluxon-ops-authorization"
_CONTROLLER_BASIC_AUTH_HEADER: str | None = None
TEST_RUNNER_UI_DEFAULT_HOST = "0.0.0.0"
TEST_RUNNER_UI_DEFAULT_PORT = 18080
TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS = 30
TEST_RUNNER_UI_DEFAULT_WORKDIR_NAME = "test_runner_ui"
TEST_RUNNER_UI_LOG_FILENAME = "test_runner_ui.log"
TEST_RUNNER_UI_HEALTH_POLL_SECONDS = 0.5
TEST_RUNNER_UI_HEALTH_TIMEOUT_SECONDS = 30.0
SAME_HOST_LOCAL_MULTI_NODE_MASTER_PORT_OFFSET = 10
SAME_HOST_LOCAL_MULTI_NODE_FS_MASTER_PANEL_PORT_OFFSET = 20
SAME_HOST_LOCAL_MULTI_NODE_OPS_CONTROLLER_KV_P2P_PORT_OFFSET = 30
SAME_HOST_LOCAL_MULTI_NODE_OPS_AGENT_P2P_PORT_BASE_OFFSET = 40
SAME_HOST_LOCAL_MULTI_NODE_ETCD_CLIENT_PORT_OFFSET = 100
SAME_HOST_LOCAL_MULTI_NODE_ETCD_PEER_PORT_OFFSET = 101
SAME_HOST_LOCAL_MULTI_NODE_GREPTIME_PORT_OFFSET = 110
SAME_HOST_LOCAL_MULTI_NODE_TIKV_PD_PORT_OFFSET = 120
SAME_HOST_LOCAL_MULTI_NODE_TIKV_PD_PEER_PORT_OFFSET = 121
SAME_HOST_LOCAL_MULTI_NODE_TIKV_PORT_OFFSET = 130
SAME_HOST_LOCAL_MULTI_NODE_TIKV_STATUS_PORT_OFFSET = 131


class HttpJsonResponseError(ValueError):
    pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Start the self-host test bed after release artifacts are already prepared on target hosts: "
            "either run the config-derived bare launch waves and then re-apply ops-managed workloads, "
            "or skip bare bootstrap and do controller-first apply only."
        )
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        required=True,
        help=(
            "Bootstrap config YAML path; if relative, resolve against the repo root inferred "
            "from this script path"
        ),
    )
    parser.add_argument(
        "-w",
        "--workdir",
        type=Path,
        required=True,
        help=(
            "Bootstrap workdir; if relative, resolve against the repo root inferred from this "
            "script path"
        ),
    )
    parser.add_argument(
        "--bootstrap-mode",
        choices=[
            BOOTSTRAP_MODE_BARE_THEN_APPLY,
            BOOTSTRAP_MODE_APPLY_ONLY,
            BOOTSTRAP_MODE_BARE_ONLY,
        ],
        default=BOOTSTRAP_MODE_BARE_THEN_APPLY,
        help=(
            "Bootstrap execution mode. "
            f"Use '{BOOTSTRAP_MODE_BARE_THEN_APPLY}' for the existing bare-then-apply flow, "
            f"'{BOOTSTRAP_MODE_BARE_ONLY}' to stop after config-derived bare launch waves and controller stability, "
            f"or '{BOOTSTRAP_MODE_APPLY_ONLY}' to skip bare bootstrap and only submit ordered apply payloads."
        ),
    )
    args = parser.parse_args()

    config_path = _resolve_repo_root_cli_path(raw_path=args.config, field_name="config")
    if not config_path.exists():
        print(f"Missing config file: {config_path}")
        raise SystemExit(1)

    workdir = _resolve_repo_root_cli_path(raw_path=args.workdir, field_name="workdir")
    workdir.mkdir(parents=True, exist_ok=True)
    workdir_lock = _acquire_workdir_lock(workdir)
    _ = workdir_lock

    config = _load_yaml_mapping(config_path, "bootstrap config")
    _validate_config_header(config, config_path)
    test_runner_ui_cfg = _parse_test_runner_ui_config(
        config,
        config_root=config_path.parent,
    )

    deployconf_path = _resolve_config_path(
        config_path.parent,
        _require_str(config.get("deployconf_path"), "deployconf_path"),
        "deployconf_path",
    )
    deployconf_raw = _load_yaml_mapping(deployconf_path, "deployconf")
    deployconf, deployconf_normalization_notes = _normalize_bootstrap_deployconf(
        deployconf=deployconf_raw,
    )
    if deployconf_normalization_notes:
        print(
            "[startbare.deployconf_normalized] "
            + json.dumps(deployconf_normalization_notes, sort_keys=True)
        )
    cluster_nodes = _parse_cluster_nodes(deployconf)
    cluster_name = _parse_cluster_name(deployconf)
    local_node_cfg = _resolve_local_node_cfg(cluster_nodes)
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    bootstrap_mode = args.bootstrap_mode
    bootstrap_bare_services = _parse_bootstrap_bare_services(deployconf)
    bootstrap_phases: list[dict[str, Any]] = []
    fixed_bootstrap_batches: list[dict[str, Any]] = []
    coverage_bootstrap_services: list[str] = []
    if bootstrap_mode in (BOOTSTRAP_MODE_BARE_THEN_APPLY, BOOTSTRAP_MODE_BARE_ONLY):
        bootstrap_phases = _parse_bootstrap_phases(
            config.get("bootstrap_phases"),
            field_name="bootstrap_phases",
            cluster_nodes=cluster_nodes,
        )
        fixed_bootstrap_batches = _bootstrap_fixed_batches(bootstrap_phases)
        coverage_bootstrap_services = _bootstrap_coverage_services(bootstrap_phases)
    deploy_workloads = _parse_name_list(
        config.get("deploy_workloads"),
        field_name="deploy_workloads",
    )
    controller_url = _require_str(config.get("controller_url"), "controller_url").rstrip("/")
    _install_controller_basic_auth(
        config.get("controller_basic_auth"),
        field_name="controller_basic_auth",
    )
    bootstrap_target_lock = _acquire_bootstrap_target_lock(
        controller_url=controller_url,
        deployconf_path=deployconf_path,
    )
    _ = bootstrap_target_lock
    controller_ready_timeout_seconds = _require_int(
        config.get("controller_ready_timeout_seconds"),
        "controller_ready_timeout_seconds",
        min_value=1,
    )
    bootstrap_stability_window_seconds = _require_int(
        config.get("bootstrap_stability_window_seconds"),
        "bootstrap_stability_window_seconds",
        min_value=1,
    )
    required_inotify_max_user_watches = _require_int(
        config.get("required_inotify_max_user_watches"),
        "required_inotify_max_user_watches",
        min_value=1,
    )
    required_inotify_max_user_instances = _require_int(
        config.get("required_inotify_max_user_instances"),
        "required_inotify_max_user_instances",
        min_value=1,
    )
    if bootstrap_mode in (BOOTSTRAP_MODE_BARE_THEN_APPLY, BOOTSTRAP_MODE_BARE_ONLY):
        _validate_fixed_bootstrap_batches(
            deployconf=deployconf,
            fixed_bootstrap_batches=fixed_bootstrap_batches,
            bootstrap_bare_services=bootstrap_bare_services,
            local_node_name=local_node_name,
        )
        _validate_coverage_bootstrap_services(
            deployconf=deployconf,
            coverage_bootstrap_services=coverage_bootstrap_services,
            bootstrap_bare_services=bootstrap_bare_services,
        )
    _validate_deploy_workloads(
        deployconf=deployconf,
        deploy_workloads=deploy_workloads,
        bootstrap_bare_services=bootstrap_bare_services,
    )
    if bootstrap_mode == BOOTSTRAP_MODE_BARE_THEN_APPLY:
        _validate_coverage_takeover_targets(
            deployconf=deployconf,
            coverage_bootstrap_services=coverage_bootstrap_services,
            deploy_workloads=deploy_workloads,
        )
    _validate_release_generation_prerequisites(
        deployconf=deployconf,
        local_node_cfg=local_node_cfg,
    )

    # English note:
    # - This entry is intentionally post-dispatch only.
    # - `pack_release.py`, `fluxon_test_stack/pack_test_stack_rsc.py`, and `manual_dispatch_release.py`
    #   must already have prepared release artifacts on every target host.
    # - `start_test_bed.py` regenerates bare scripts from the current normalized deployconf and
    #   refreshes `${HOSTWORKDIR}/gen_bare_deploy_bash` before any bare bootstrap wave starts.
    # - Runtime ownership has two explicit modes:
    #   `bare_then_apply`: start config-derived bare launch waves -> apply controller workloads -> wait apply ->
    #   apply remaining workloads -> wait apply.
    #   `apply_only`: require the controller to already be reachable, then submit the ordered
    #   controller-first apply payloads without any bare launch/retire side effects.
    # - Runtime-side effects must stay collected: this entry only calls generated bare start/stop
    #   scripts plus controller apply/apply_wait endpoints.
    # - This entry must not query `selection_supervisor.py status` directly.
    # - The external contract here is generated bare start/stop scripts plus ops/controller APIs.
    # - Supervisor state remains an internal implementation detail behind those interfaces.
    # - It must not prune desired files or inspect cluster membership directly.
    # - Coverage bare is config-derived, not controller-readiness-derived.
    # - This entry intentionally does not run takeover rescue or fallback loops.
    release_manifest_sha256 = _read_local_release_manifest_sha256(
        deployconf=deployconf,
        local_node_cfg=local_node_cfg,
    )
    deployconf_for_generation = _with_release_manifest_sha256_env(
        deployconf=deployconf,
        release_manifest_sha256=release_manifest_sha256,
    )
    deployconf_generation_path = workdir / "deployconf.with_release_manifest_sha256.yaml"
    deployconf_generation_path.write_text(
        yaml.safe_dump(deployconf_for_generation, sort_keys=False),
        encoding="utf-8",
    )

    daemonset_dir = workdir / "gen_k8s_daemonset"
    _generate_daemonset_artifacts(
        deployconf_path=deployconf_generation_path,
        daemonset_dir=daemonset_dir,
    )
    if bootstrap_mode in (BOOTSTRAP_MODE_BARE_THEN_APPLY, BOOTSTRAP_MODE_BARE_ONLY):
        bare_scripts_dir = workdir / "gen_bare_deploy_bash"
        _refresh_cluster_bare_deploy_scripts(
            deployconf_path=deployconf_generation_path,
            cluster_nodes=cluster_nodes,
            bare_scripts_dir=bare_scripts_dir,
        )
        _validate_bare_bootstrap_prerequisites(
            deployconf=deployconf,
            cluster_nodes=cluster_nodes,
            local_node_cfg=local_node_cfg,
            fixed_bootstrap_batches=fixed_bootstrap_batches,
            coverage_bootstrap_services=coverage_bootstrap_services,
        )
        _validate_ops_agent_snapshot_prerequisites(
            deployconf=deployconf,
            cluster_nodes=cluster_nodes,
            local_node_cfg=local_node_cfg,
            fixed_bootstrap_batches=fixed_bootstrap_batches,
            coverage_bootstrap_services=coverage_bootstrap_services,
        )
        _validate_local_inotify_capacity(
            required_inotify_max_user_watches=required_inotify_max_user_watches,
            required_inotify_max_user_instances=required_inotify_max_user_instances,
        )

    coverage_bootstrap_excluded_targets: list[dict[str, Any]] = []
    apply_wait_atomic: dict[str, Any] | None = None
    apply_wait_plain: dict[str, Any] | None = None
    if coverage_bootstrap_services:
        coverage_bootstrap_excluded_targets = _local_control_plane_coverage_excluded_targets(
            deployconf=deployconf,
            fixed_bootstrap_batches=fixed_bootstrap_batches,
            local_node_name=local_node_name,
            coverage_bootstrap_services=coverage_bootstrap_services,
        )
        if coverage_bootstrap_excluded_targets:
            print(
                "[startbare.coverage_bare_excluded_targets] "
                f"targets={json.dumps(coverage_bootstrap_excluded_targets, sort_keys=True)}"
            )
    coverage_bootstrap_batches = _build_coverage_bootstrap_batches(
        deployconf=deployconf,
        coverage_bootstrap_services=coverage_bootstrap_services,
        excluded_targets={
            (
                _require_str(item.get("node"), "coverage_bootstrap_excluded_targets[].node"),
                _require_str(
                    item.get("selection_name"),
                    "coverage_bootstrap_excluded_targets[].selection_name",
                ),
            )
            for item in coverage_bootstrap_excluded_targets
        },
    )
    fixed_bootstrap_waves = _build_bare_launch_waves_from_batches(
        batches=fixed_bootstrap_batches,
    )
    coverage_bootstrap_waves = _build_bare_launch_waves_from_batches(
        batches=coverage_bootstrap_batches,
    )

    initial_controller_reachable = _is_controller_initially_reachable(controller_url=controller_url)
    if bootstrap_mode == BOOTSTRAP_MODE_APPLY_ONLY and not initial_controller_reachable:
        raise RuntimeError(
            "bootstrap_mode=apply_only requires the controller to already be reachable before startup; "
            f"controller_url={controller_url}"
        )
    deleted_target_apply_ids: list[str] = []
    controller_handover_selection_names: list[str] = []
    ordinary_initial_delete_selection_names: list[str] = []
    if bootstrap_mode == BOOTSTRAP_MODE_APPLY_ONLY:
        print("[startbare.mode] bootstrap_mode=apply_only skip_bare_bootstrap=true")
    if bootstrap_mode == BOOTSTRAP_MODE_BARE_ONLY:
        print("[startbare.mode] bootstrap_mode=bare_only skip_apply=true")
    if bootstrap_mode == BOOTSTRAP_MODE_BARE_THEN_APPLY and initial_controller_reachable:
        controller_handover_selection_names, ordinary_initial_delete_selection_names = _split_initial_delete_selection_names(
            deployconf=deployconf,
            selection_names=deploy_workloads,
            local_node_name=local_node_name,
        )
        initial_delete_selection_names = (
            ordinary_initial_delete_selection_names + controller_handover_selection_names
        )
        initial_delete_agent_instance_keys = _selection_agent_instance_keys(
            deployconf=deployconf,
            selection_names=initial_delete_selection_names,
        )
        _wait_controller_agents_ready(
            controller_url=controller_url,
            agent_instance_keys=initial_delete_agent_instance_keys,
            timeout_seconds=controller_ready_timeout_seconds,
        )
        if ordinary_initial_delete_selection_names and _selection_has_attached_current_deployment_groups(
            controller_url=controller_url,
            deployconf=deployconf,
            selection_names=ordinary_initial_delete_selection_names,
            ctx="initial_delete ordinary",
        ):
            deleted_target_apply_ids.extend(
                _delete_selection_current_deployment_groups(
                    controller_url=controller_url,
                    deployconf=deployconf,
                    selection_names=ordinary_initial_delete_selection_names,
                    ctx="initial_delete ordinary",
                )
            )
        if controller_handover_selection_names:
            if _selection_has_attached_current_deployment_groups(
                controller_url=controller_url,
                deployconf=deployconf,
                selection_names=controller_handover_selection_names,
                ctx="initial_delete controller_handover",
            ):
                deleted_target_apply_ids.extend(
                    _delete_selection_current_deployment_groups_no_wait(
                        controller_url=controller_url,
                        deployconf=deployconf,
                        selection_names=controller_handover_selection_names,
                        ctx="initial_delete controller_handover",
                    )
                )
            _stop_local_controller_handover_selections(
                deployconf=deployconf,
                local_node_cfg=local_node_cfg,
                selection_names=controller_handover_selection_names,
            )
            _wait_controller_unreachable(
                controller_url=controller_url,
                timeout_seconds=controller_ready_timeout_seconds,
            )
    if bootstrap_mode in (BOOTSTRAP_MODE_BARE_THEN_APPLY, BOOTSTRAP_MODE_BARE_ONLY) and fixed_bootstrap_waves:
        _run_bare_waves(
            workdir=workdir,
            deployconf=deployconf,
            cluster_nodes=cluster_nodes,
            local_node_cfg=local_node_cfg,
            waves=fixed_bootstrap_waves,
            bootstrap_bare_services=bootstrap_bare_services,
        )
    if bootstrap_mode in (BOOTSTRAP_MODE_BARE_THEN_APPLY, BOOTSTRAP_MODE_BARE_ONLY) and coverage_bootstrap_waves:
        _run_bare_waves(
            workdir=workdir,
            deployconf=deployconf,
            cluster_nodes=cluster_nodes,
            local_node_cfg=local_node_cfg,
            waves=coverage_bootstrap_waves,
            bootstrap_bare_services=bootstrap_bare_services,
        )
    if bootstrap_mode in (BOOTSTRAP_MODE_BARE_THEN_APPLY, BOOTSTRAP_MODE_BARE_ONLY):
        _wait_controller_ready_stable(
            controller_url=controller_url,
            timeout_seconds=controller_ready_timeout_seconds,
            stability_window_seconds=bootstrap_stability_window_seconds,
        )
    test_runner_ui_summary = _ensure_test_runner_ui_started(ui_cfg=test_runner_ui_cfg)
    if bootstrap_mode == BOOTSTRAP_MODE_BARE_THEN_APPLY:
        post_bootstrap_agent_instance_keys = _selection_agent_instance_keys(
            deployconf=deployconf,
            selection_names=deploy_workloads,
        )
        _wait_controller_agents_ready(
            controller_url=controller_url,
            agent_instance_keys=post_bootstrap_agent_instance_keys,
            timeout_seconds=controller_ready_timeout_seconds,
        )
    if bootstrap_mode == BOOTSTRAP_MODE_BARE_THEN_APPLY and controller_handover_selection_names:
        controller_handover_agent_instance_keys = _selection_agent_instance_keys(
            deployconf=deployconf,
            selection_names=controller_handover_selection_names,
        )
        if _selection_has_attached_current_deployment_groups(
            controller_url=controller_url,
            deployconf=deployconf,
            selection_names=controller_handover_selection_names,
            ctx="post_handover_delete_wait",
        ):
            deleted_target_apply_ids.extend(
                _delete_selection_current_deployment_groups(
                    controller_url=controller_url,
                    deployconf=deployconf,
                    selection_names=controller_handover_selection_names,
                    ctx="post_handover_delete_wait",
                )
            )

    if bootstrap_mode == BOOTSTRAP_MODE_BARE_ONLY:
        summary = _build_start_test_bed_summary(
            bootstrap_mode=bootstrap_mode,
            controller_url=controller_url,
            bootstrap_phases=bootstrap_phases,
            initial_controller_reachable=initial_controller_reachable,
            deleted_target_apply_ids=deleted_target_apply_ids,
            coverage_bootstrap_excluded_targets=coverage_bootstrap_excluded_targets,
            fixed_bootstrap_waves=fixed_bootstrap_waves,
            coverage_bootstrap_batches=coverage_bootstrap_batches,
            coverage_bootstrap_waves=coverage_bootstrap_waves,
            deploy_atomic_selection_names=[],
            deploy_plain_selection_names=[],
            deploy_response_atomic=None,
            deploy_response_plain=None,
            apply_wait_atomic=None,
            apply_wait_plain=None,
            test_runner_ui=test_runner_ui_summary,
        )
        (workdir / "start_test_bed_summary.yaml").write_text(
            yaml.safe_dump(summary, sort_keys=False),
            encoding="utf-8",
        )
        print(f"[wait_apply.ready] cluster_name={cluster_name} controller_url={controller_url}")
        return

    atomic_selection_names, plain_selection_names = _split_deploy_workloads_by_atomic_group(
        deployconf=deployconf,
        deploy_workloads=deploy_workloads,
    )
    deploy_response_atomic: dict[str, Any] | None = None
    deploy_response_plain: dict[str, Any] | None = None

    if atomic_selection_names:
        deploy_payload_atomic = _load_deploy_payload(
            deployconf=deployconf,
            daemonset_dir=daemonset_dir,
            deploy_workloads=atomic_selection_names,
        )
        deploy_response_atomic = _deploy_controller_payload(
            controller_url=controller_url,
            yaml_text=deploy_payload_atomic,
        )
        print("[apply.deploy_controller_atomic] desired payload accepted; wait for controller apply")
        apply_wait_atomic = _wait_apply_id(
            controller_url=controller_url,
            apply_id=_require_str(deploy_response_atomic.get("apply_id"), "deploy_response_atomic.apply_id"),
            timeout_seconds=controller_ready_timeout_seconds,
        )

    if plain_selection_names:
        deploy_payload_plain = _load_deploy_payload(
            deployconf=deployconf,
            daemonset_dir=daemonset_dir,
            deploy_workloads=plain_selection_names,
        )
        deploy_response_plain = _deploy_controller_payload(
            controller_url=controller_url,
            yaml_text=deploy_payload_plain,
        )
        print("[apply.deploy_controller_plain] desired payload accepted; wait for remaining workloads")
        apply_wait_plain = _wait_apply_id(
            controller_url=controller_url,
            apply_id=_require_str(deploy_response_plain.get("apply_id"), "deploy_response_plain.apply_id"),
            timeout_seconds=controller_ready_timeout_seconds,
        )

    summary = _build_start_test_bed_summary(
        bootstrap_mode=bootstrap_mode,
        controller_url=controller_url,
        bootstrap_phases=bootstrap_phases,
        initial_controller_reachable=initial_controller_reachable,
        deleted_target_apply_ids=deleted_target_apply_ids,
        coverage_bootstrap_excluded_targets=coverage_bootstrap_excluded_targets,
        fixed_bootstrap_waves=fixed_bootstrap_waves,
        coverage_bootstrap_batches=coverage_bootstrap_batches,
        coverage_bootstrap_waves=coverage_bootstrap_waves,
        deploy_atomic_selection_names=atomic_selection_names,
        deploy_plain_selection_names=plain_selection_names,
        deploy_response_atomic=deploy_response_atomic,
        deploy_response_plain=deploy_response_plain,
        apply_wait_atomic=apply_wait_atomic,
        apply_wait_plain=apply_wait_plain,
        test_runner_ui=test_runner_ui_summary,
    )
    (workdir / "start_test_bed_summary.yaml").write_text(
        yaml.safe_dump(summary, sort_keys=False),
        encoding="utf-8",
    )
    print(f"[wait_apply.ready] cluster_name={cluster_name} controller_url={controller_url}")


def _split_deploy_workloads_by_atomic_group(
    *,
    deployconf: dict[str, Any],
    deploy_workloads: list[str],
) -> tuple[list[str], list[str]]:
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    atomic: list[str] = []
    plain: list[str] = []
    for selection_name in _dedup_str_list(deploy_workloads):
        if selection_name in atomic_groups:
            atomic.append(selection_name)
        else:
            plain.append(selection_name)
    return atomic, plain


def _split_initial_delete_selection_names(
    *,
    deployconf: dict[str, Any],
    selection_names: list[str],
    local_node_name: str,
) -> tuple[list[str], list[str]]:
    controller_handover: list[str] = []
    ordinary: list[str] = []
    for selection_name in _dedup_str_list(selection_names):
        if _selection_owns_local_controller_endpoint(
            deployconf=deployconf,
            selection_name=selection_name,
            local_node_name=local_node_name,
        ):
            controller_handover.append(selection_name)
            continue
        ordinary.append(selection_name)
    return controller_handover, ordinary


def _selection_owns_local_controller_endpoint(
    *,
    deployconf: dict[str, Any],
    selection_name: str,
    local_node_name: str,
) -> bool:
    target_nodes = _resolve_selection_target_nodes(
        deployconf=deployconf,
        selection_name=selection_name,
    )
    if local_node_name not in target_nodes:
        return False
    service_names = _selection_service_names_for_target_node(
        deployconf=deployconf,
        selection_name=selection_name,
        node_name=local_node_name,
    )
    return "ops_controller" in service_names


def _stop_local_controller_handover_selections(
    *,
    deployconf: dict[str, Any],
    local_node_cfg: dict[str, Any],
    selection_names: list[str],
) -> list[str]:
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    stopped_service_names: list[str] = []
    seen_service_names: set[str] = set()
    for selection_name in _dedup_str_list(selection_names):
        if not _selection_owns_local_controller_endpoint(
            deployconf=deployconf,
            selection_name=selection_name,
            local_node_name=local_node_name,
        ):
            continue
        service_names = _selection_service_names_for_target_node(
            deployconf=deployconf,
            selection_name=selection_name,
            node_name=local_node_name,
        )
        for service_name in service_names:
            if service_name not in CONTROL_PLANE_HANDOVER_SERVICE_NAMES:
                continue
            if service_name in seen_service_names:
                continue
            seen_service_names.add(service_name)
            print(
                "[startbare.local_controller_handover_stop] "
                f"node={local_node_name} selection={selection_name} service={service_name}",
                flush=True,
            )
            try:
                _run_local_stop(
                    local_node_cfg=local_node_cfg,
                    service_name=service_name,
                )
            except Exception as exc:
                raise RuntimeError(
                    "local controller handover stop failed via generated bare stop script: "
                    f"node={local_node_name} selection={selection_name} service={service_name}"
                ) from exc
            stopped_service_names.append(service_name)
    return stopped_service_names

def _generate_daemonset_artifacts(
    *,
    deployconf_path: Path,
    daemonset_dir: Path,
) -> None:
    _run_subprocess(
        [
            sys.executable,
            str(REPO_ROOT / "deployment" / "gen_k8s_daemonset.py"),
            "-c",
            str(deployconf_path),
            "-w",
            str(daemonset_dir),
        ],
        cwd=REPO_ROOT,
    )


def _generate_bare_deploy_scripts(
    *,
    deployconf_path: Path,
    bare_scripts_dir: Path,
) -> None:
    _run_subprocess(
        [
            sys.executable,
            str(REPO_ROOT / "deployment" / "gen_bare_deploy_bash.py"),
            "-c",
            str(deployconf_path),
            "-w",
            str(bare_scripts_dir),
        ],
        cwd=REPO_ROOT,
    )


def _refresh_cluster_bare_deploy_scripts(
    *,
    deployconf_path: Path,
    cluster_nodes: dict[str, dict[str, Any]],
    bare_scripts_dir: Path,
) -> None:
    _generate_bare_deploy_scripts(
        deployconf_path=deployconf_path,
        bare_scripts_dir=bare_scripts_dir,
    )
    for node_name, node_cfg in cluster_nodes.items():
        hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
        ssh_user = _require_str(node_cfg.get("ssh_user"), f"cluster_nodes[{node_name}].ssh_user")
        dst_dir_s = str(Path(hostworkdir) / "gen_bare_deploy_bash")
        dst_owner = f"{ssh_user}:{ssh_user}"
        execution_mode = _require_str(
            node_cfg.get("execution_mode", "ssh"),
            f"cluster_nodes[{node_name}].execution_mode",
        )
        print(
            "[startbare.refresh_bare_scripts] "
            f"node={node_name} execution_mode={execution_mode} dst={dst_dir_s}"
        )
        if _cluster_node_is_local(node_cfg):
            manual_dispatch_release._copy_local_artifact(
                src_dir=bare_scripts_dir,
                dst_dir_s=dst_dir_s,
                dst_owner=dst_owner,
            )
            continue
        ssh_port = _require_int(node_cfg.get("ssh_port"), f"cluster_nodes[{node_name}].ssh_port", min_value=1)
        ssh_password_raw = node_cfg.get("ssh_password")
        ssh_password = None if ssh_password_raw is None else _require_str(
            ssh_password_raw,
            f"cluster_nodes[{node_name}].ssh_password",
        )
        manual_dispatch_release._copy_remote_artifact(
            src_dir=bare_scripts_dir,
            dst_dir_s=dst_dir_s,
            ssh_user=ssh_user,
            ip=_cluster_node_ssh_host(node_cfg, node_name=node_name),
            ssh_port=ssh_port,
            ssh_password=ssh_password,
            dst_owner=dst_owner,
        )


def _normalize_bootstrap_deployconf(
    *,
    deployconf: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    normalized = copy.deepcopy(deployconf)
    notes: list[str] = []
    same_host_notes = _rewrite_same_host_local_multi_node_fixed_ports(
        deployconf=normalized,
    )
    notes.extend(same_host_notes)
    services = normalized.get("service")
    if not isinstance(services, dict):
        return normalized, notes

    master_cfg = services.get("master")
    if isinstance(master_cfg, dict):
        entrypoint = master_cfg.get("entrypoint")
        if isinstance(entrypoint, str):
            master_port = _extract_master_listen_port(entrypoint=entrypoint)
            if master_port is not None:
                _set_service_port(master_cfg, port=master_port)
            normalized_entrypoint, removed = _strip_legacy_master_p2p_listen_port(entrypoint=entrypoint)
            if removed:
                master_cfg["entrypoint"] = normalized_entrypoint
                notes.append("service.master.entrypoint: removed legacy master field p2p_listen_port")

    fluxon_fs_master_cfg = services.get("fluxon_fs_master")
    if isinstance(fluxon_fs_master_cfg, dict):
        entrypoint = fluxon_fs_master_cfg.get("entrypoint")
        if isinstance(entrypoint, str):
            _require_fluxon_fs_master_panel_prometheus_base_url(
                entrypoint=entrypoint,
            )

    greptime_cfg = services.get("greptime")
    if isinstance(greptime_cfg, dict):
        entrypoint = greptime_cfg.get("entrypoint")
        if isinstance(entrypoint, str):
            _require_greptime_loopback_bind_addrs(
                entrypoint=entrypoint,
            )
    return normalized, notes


def _rewrite_same_host_local_multi_node_fixed_ports(
    *,
    deployconf: dict[str, Any],
) -> list[str]:
    cluster_nodes = deployconf.get("cluster_nodes")
    if not isinstance(cluster_nodes, list) or len(cluster_nodes) < 2:
        return []
    for idx, raw_node in enumerate(cluster_nodes):
        if not isinstance(raw_node, dict):
            raise ValueError(f"deployconf.cluster_nodes[{idx}] must be a mapping")
        execution_mode = _require_str(
            raw_node.get("execution_mode", "ssh"),
            f"deployconf.cluster_nodes[{idx}].execution_mode",
        )
        if execution_mode != "local":
            return []

    global_envs = deployconf.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")

    controller_port = _require_port_number(
        global_envs.get("MASTER__PORT"),
        "deployconf.global_envs.MASTER__PORT",
    )
    plan = _build_same_host_local_multi_node_port_plan(
        controller_port=controller_port,
        node_count=len(cluster_nodes),
    )

    etcd_cfg = _require_mapping(services.get("etcd"), "deployconf.service.etcd")
    greptime_cfg = _require_mapping(services.get("greptime"), "deployconf.service.greptime")
    tikv_pd_cfg = _require_mapping(services.get("tikv_pd"), "deployconf.service.tikv_pd")
    tikv_cfg = _require_mapping(services.get("tikv"), "deployconf.service.tikv")
    master_cfg = _require_mapping(services.get("master"), "deployconf.service.master")
    ops_agent_cfg = _require_mapping(services.get("ops_agent"), "deployconf.service.ops_agent")
    ops_controller_cfg = _require_mapping(services.get("ops_controller"), "deployconf.service.ops_controller")
    global_envs["TIKV_PD_PEER_PORT"] = str(plan["tikv_pd_peer_port"])
    global_envs["TIKV_STATUS_FULL_ADDRESS"] = (
        "${${TIKV__NODE_ID}__IP}:" + str(plan["tikv_status_port"])
    )
    global_envs["FLUXON_FS_MASTER_PANEL_BASE_URL"] = (
        f"http://${{FLUXON_FS_MASTER__NODE_ID__IP}}:{plan['fs_master_panel_port']}"
    )
    global_envs["FLUXON_FS_MASTER_PANEL_LISTEN_ADDR"] = (
        f"0.0.0.0:{plan['fs_master_panel_port']}"
    )

    _set_service_port(etcd_cfg, port=plan["etcd_client_port"])
    _set_service_port(greptime_cfg, port=plan["greptime_port"])
    _set_service_port(tikv_pd_cfg, port=plan["tikv_pd_port"])
    _set_service_port(tikv_cfg, port=plan["tikv_port"])
    _set_service_port(master_cfg, port=plan["master_port"])

    etcd_entrypoint = _require_str(etcd_cfg.get("entrypoint"), "deployconf.service.etcd.entrypoint")
    etcd_entrypoint = _replace_expected_substring(
        value=etcd_entrypoint,
        old="http://0.0.0.0:33579",
        new=f"http://0.0.0.0:{plan['etcd_client_port']}",
        ctx="deployconf.service.etcd.entrypoint client port",
    )
    etcd_entrypoint = _replace_expected_substring(
        value=etcd_entrypoint,
        old="http://0.0.0.0:2480",
        new=f"http://0.0.0.0:{plan['etcd_peer_port']}",
        ctx="deployconf.service.etcd.entrypoint peer port",
    )
    etcd_cfg["entrypoint"] = etcd_entrypoint

    greptime_entrypoint = _require_str(
        greptime_cfg.get("entrypoint"),
        "deployconf.service.greptime.entrypoint",
    )
    greptime_cfg["entrypoint"] = _replace_expected_substring(
        value=greptime_entrypoint,
        old="--http-addr 0.0.0.0:35030",
        new=f"--http-addr 0.0.0.0:{plan['greptime_port']}",
        ctx="deployconf.service.greptime.entrypoint http addr",
    )

    master_entrypoint = _require_str(master_cfg.get("entrypoint"), "deployconf.service.master.entrypoint")
    master_cfg["entrypoint"] = _replace_expected_substring(
        value=master_entrypoint,
        old="port: 51051",
        new=f"port: {plan['master_port']}",
        ctx="deployconf.service.master.entrypoint port",
    )

    ops_controller_entrypoint = _require_str(
        ops_controller_cfg.get("entrypoint"),
        "deployconf.service.ops_controller.entrypoint",
    )
    ops_controller_cfg["entrypoint"] = _replace_expected_substring(
        value=ops_controller_entrypoint,
        old="p2p_listen_port: 12102",
        new=f"p2p_listen_port: {plan['ops_controller_kv_p2p_port']}",
        ctx="deployconf.service.ops_controller.entrypoint kv p2p_listen_port",
    )

    ops_agent_entrypoint = _require_str(
        ops_agent_cfg.get("entrypoint"),
        "deployconf.service.ops_agent.entrypoint",
    )
    ops_agent_cfg["entrypoint"] = _rewrite_ops_agent_case_ports(
        entrypoint=ops_agent_entrypoint,
        replacement_ports=plan["ops_agent_ports"],
    )

    return [
        "same_host_local_multi_node: rewrote fixed host-listen ports from controller anchor "
        f"{controller_port}"
    ]


def _build_same_host_local_multi_node_port_plan(
    *,
    controller_port: int,
    node_count: int,
) -> dict[str, Any]:
    if node_count < 2:
        raise ValueError("same-host local multi-node port plan requires at least two nodes")

    ops_agent_ports = [
        controller_port + SAME_HOST_LOCAL_MULTI_NODE_OPS_AGENT_P2P_PORT_BASE_OFFSET + idx
        for idx in range(node_count)
    ]
    plan = {
        "master_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_MASTER_PORT_OFFSET,
        "fs_master_panel_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_FS_MASTER_PANEL_PORT_OFFSET,
        "ops_controller_kv_p2p_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_OPS_CONTROLLER_KV_P2P_PORT_OFFSET,
        "ops_agent_ports": ops_agent_ports,
        "etcd_client_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_ETCD_CLIENT_PORT_OFFSET,
        "etcd_peer_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_ETCD_PEER_PORT_OFFSET,
        "greptime_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_GREPTIME_PORT_OFFSET,
        "tikv_pd_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_TIKV_PD_PORT_OFFSET,
        "tikv_pd_peer_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_TIKV_PD_PEER_PORT_OFFSET,
        "tikv_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_TIKV_PORT_OFFSET,
        "tikv_status_port": controller_port + SAME_HOST_LOCAL_MULTI_NODE_TIKV_STATUS_PORT_OFFSET,
    }
    max_port = max(
        int(plan["master_port"]),
        int(plan["fs_master_panel_port"]),
        int(plan["ops_controller_kv_p2p_port"]),
        int(plan["etcd_client_port"]),
        int(plan["etcd_peer_port"]),
        int(plan["greptime_port"]),
        int(plan["tikv_pd_port"]),
        int(plan["tikv_pd_peer_port"]),
        int(plan["tikv_port"]),
        int(plan["tikv_status_port"]),
        *(int(port) for port in ops_agent_ports),
    )
    if max_port > 65535:
        raise ValueError(
            "same-host local multi-node port plan exceeds 65535; "
            f"controller_port={controller_port} max_port={max_port}"
        )
    return plan


def _require_port_number(value: Any, field_name: str) -> int:
    port: int
    if isinstance(value, int):
        port = value
    elif isinstance(value, str) and value.strip():
        try:
            port = int(value.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid integer port") from exc
    else:
        raise ValueError(f"{field_name} must be a valid integer port")
    if not (1 <= port <= 65535):
        raise ValueError(f"{field_name} must be in range 1..65535")
    return port


def _set_service_port(service_cfg: dict[str, Any], *, port: int) -> None:
    service_cfg["port"] = int(port)
    if "in_container_port" in service_cfg:
        service_cfg["in_container_port"] = int(port)


def _extract_master_listen_port(*, entrypoint: str) -> int | None:
    match = re.search(r"(?m)^[ \t]*port:\s*(\d+)\s*$", entrypoint)
    if match is None:
        return None
    return _require_port_number(match.group(1), "deployconf.service.master.entrypoint port")


def _replace_expected_substring(*, value: str, old: str, new: str, ctx: str) -> str:
    if old in value:
        return value.replace(old, new)
    if new in value:
        return value
    raise ValueError(f"{ctx} missing expected fragment: {old!r}")


def _rewrite_ops_agent_case_ports(
    *,
    entrypoint: str,
    replacement_ports: list[int],
) -> str:
    lines = entrypoint.splitlines(keepends=True)
    matched_indexes: list[int] = []
    next_port_idx = 0
    for idx, line in enumerate(lines):
        if "OPS_AGENT_P2P_LISTEN_PORT=" not in line:
            continue
        matched_indexes.append(idx)
        if next_port_idx >= len(replacement_ports):
            raise ValueError(
                "deployconf.service.ops_agent.entrypoint has more fixed p2p port assignments "
                f"than local nodes: line_count={len(matched_indexes)} node_count={len(replacement_ports)}"
            )
        prefix, _, suffix = line.partition("OPS_AGENT_P2P_LISTEN_PORT=")
        if not suffix:
            raise ValueError("deployconf.service.ops_agent.entrypoint contains an empty p2p assignment")
        trailing = ""
        stripped_suffix = suffix.rstrip("\n")
        if suffix.endswith("\n"):
            trailing = "\n"
        lines[idx] = (
            f"{prefix}OPS_AGENT_P2P_LISTEN_PORT={replacement_ports[next_port_idx]}{trailing}"
        )
        next_port_idx += 1
    if not matched_indexes:
        raise ValueError("deployconf.service.ops_agent.entrypoint is missing fixed p2p port assignments")
    if next_port_idx != len(replacement_ports):
        raise ValueError(
            "deployconf.service.ops_agent.entrypoint fixed p2p port assignments do not match local node count: "
            f"line_count={next_port_idx} node_count={len(replacement_ports)}"
        )
    return "".join(lines)


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _strip_legacy_master_p2p_listen_port(*, entrypoint: str) -> tuple[str, bool]:
    lines = entrypoint.splitlines(keepends=True)
    out_lines: list[str] = []
    in_yaml_heredoc = False
    removed = False
    for line in lines:
        stripped = line.strip()
        if in_yaml_heredoc:
            if stripped == "YAML":
                in_yaml_heredoc = False
                out_lines.append(line)
                continue
            if stripped.startswith("p2p_listen_port:"):
                removed = True
                continue
            out_lines.append(line)
            continue
        out_lines.append(line)
        if stripped.startswith("cat >") and "<<YAML" in stripped:
            in_yaml_heredoc = True
    return "".join(out_lines), removed


def _require_fluxon_fs_master_panel_prometheus_base_url(*, entrypoint: str) -> None:
    lines = entrypoint.splitlines(keepends=True)
    in_yaml_heredoc = False
    in_master_panel_block = False
    master_panel_indent = 0
    has_prometheus_base_url = False

    for line in lines:
        stripped = line.strip()
        current_indent = len(line) - len(line.lstrip(" "))

        if in_yaml_heredoc and in_master_panel_block:
            if stripped == "YAML" or (
                stripped
                and not line.lstrip(" ").startswith("#")
                and current_indent <= master_panel_indent
            ):
                if not has_prometheus_base_url:
                    raise ValueError(
                        "deployconf.service.fluxon_fs_master.entrypoint is missing "
                        "fluxon_fs.master_panel.prometheus_base_url"
                    )
                in_master_panel_block = False
                has_prometheus_base_url = False

        if in_yaml_heredoc:
            if stripped == "YAML":
                in_yaml_heredoc = False
                continue
            if stripped == "master_panel:":
                in_master_panel_block = True
                master_panel_indent = current_indent
                has_prometheus_base_url = False
                continue
            if in_master_panel_block:
                if stripped.startswith("prometheus_base_url:"):
                    has_prometheus_base_url = True
            continue

        if stripped.startswith("cat >") and "<<YAML" in stripped:
            in_yaml_heredoc = True

    if in_master_panel_block and not has_prometheus_base_url:
        raise ValueError(
            "deployconf.service.fluxon_fs_master.entrypoint is missing "
            "fluxon_fs.master_panel.prometheus_base_url"
        )


def _require_greptime_loopback_bind_addrs(*, entrypoint: str) -> None:
    if "greptime standalone start" not in entrypoint:
        return

    has_rpc_bind_addr = "--rpc-bind-addr" in entrypoint
    has_mysql_addr = "--mysql-addr" in entrypoint
    has_postgres_addr = "--postgres-addr" in entrypoint
    missing: list[str] = []
    if not has_rpc_bind_addr:
        missing.append("--rpc-bind-addr 127.0.0.1:$((GREPTIME__PORT + 1))")
    if not has_mysql_addr:
        missing.append("--mysql-addr 127.0.0.1:$((GREPTIME__PORT + 2))")
    if not has_postgres_addr:
        missing.append("--postgres-addr 127.0.0.1:$((GREPTIME__PORT + 3))")
    if missing:
        raise ValueError(
            "deployconf.service.greptime.entrypoint is missing required loopback bind flags: "
            + ", ".join(missing)
        )


def _read_local_release_manifest_sha256(
    *,
    deployconf: dict[str, Any],
    local_node_cfg: dict[str, Any],
) -> str:
    global_envs = deployconf.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")
    release_manifest_name = _require_str(
        global_envs.get("FLUXON_RELEASE_SHA256_FILE"),
        "global_envs.FLUXON_RELEASE_SHA256_FILE",
    )
    local_hostworkdir = _require_str(local_node_cfg.get("hostworkdir"), "local_node_cfg.hostworkdir")
    manifest_path = Path(local_hostworkdir) / "fluxon_release" / release_manifest_name
    if not manifest_path.exists():
        raise ValueError(f"Missing local release manifest for payload fingerprint: {manifest_path}")
    return hashlib.sha256(manifest_path.read_bytes()).hexdigest()


def _with_release_manifest_sha256_env(
    *,
    deployconf: dict[str, Any],
    release_manifest_sha256: str,
) -> dict[str, Any]:
    global_envs = deployconf.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")
    if RELEASE_MANIFEST_SHA256_ENV_KEY in global_envs:
        raise ValueError(
            f"deployconf.global_envs must not predefine {RELEASE_MANIFEST_SHA256_ENV_KEY}; "
            "start_test_bed injects the current release fingerprint explicitly"
        )

    # English note:
    # - Desired workload identity must change when `fluxon_release.sha256` changes, even if the
    #   daemonset topology and wheel filenames stay the same.
    # - Otherwise `skip identical desired payload` will preserve stale self-host runtimes that are
    #   still running the old release bits, which breaks fresh-run validation after a rebuild.
    deployconf_with_release_fingerprint = dict(deployconf)
    deployconf_with_release_fingerprint["global_envs"] = dict(global_envs)
    deployconf_with_release_fingerprint["global_envs"][RELEASE_MANIFEST_SHA256_ENV_KEY] = release_manifest_sha256
    return deployconf_with_release_fingerprint


def _acquire_workdir_lock(workdir: Path) -> Any:
    lock_path = workdir / ".start_test_bed.lock"
    lock_file = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.seek(0)
        holder = lock_file.read().strip()
        raise RuntimeError(
            f"start_test_bed workdir is already active: workdir={workdir} holder={holder or 'unknown'}"
        )
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(
        json.dumps(
            {
                "pid": os.getpid(),
                "entry": "start_test_bed.py",
                "workdir": str(workdir),
                "started_at_epoch_s": int(time.time()),
            },
            sort_keys=True,
        )
        + "\n"
    )
    lock_file.flush()
    return lock_file


def _acquire_bootstrap_target_lock(*, controller_url: str, deployconf_path: Path) -> Any:
    BOOTSTRAP_TARGET_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    # Bootstrap exclusivity belongs to the controller target itself.
    # The deployconf path may vary across generated per-case bootstrap repos while still
    # converging the same live controller, so it must not widen the lock scope.
    target_identity = json.dumps({"controller_url": controller_url}, sort_keys=True)
    target_lock_name = hashlib.sha256(target_identity.encode("utf-8")).hexdigest()[:16] + ".lock"
    lock_path = BOOTSTRAP_TARGET_LOCK_DIR / target_lock_name
    lock_file = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.seek(0)
        holder = lock_file.read().strip()
        raise RuntimeError(
            "start_test_bed target is already active: "
            f"controller_url={controller_url} deployconf_path={deployconf_path} holder={holder or 'unknown'}"
        )
    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(
        json.dumps(
            {
                "pid": os.getpid(),
                "entry": "start_test_bed.py",
                "controller_url": controller_url,
                "deployconf_path": str(deployconf_path.resolve()),
                "started_at_epoch_s": int(time.time()),
            },
            sort_keys=True,
        )
        + "\n"
    )
    lock_file.flush()
    return lock_file


def _load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{label} root must be a mapping: {path}")
    return data


def _validate_config_header(config: dict[str, Any], config_path: Path) -> None:
    schema_version = _require_int(config.get("schema_version"), "schema_version", min_value=1)
    if schema_version != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema_version in {config_path}: {schema_version}; expected {SCHEMA_VERSION}"
        )


def _read_proc_sysctl_int(path: Path, field_name: str) -> int:
    if not path.exists():
        raise ValueError(f"{field_name} proc path does not exist: {path}")
    raw_value = path.read_text(encoding="utf-8").strip()
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an integer, got: {raw_value}") from exc


def _count_current_inotify_usage() -> tuple[int, int]:
    total_watches = 0
    total_instances = 0
    for proc_entry in Path("/proc").iterdir():
        if not proc_entry.name.isdigit():
            continue
        fdinfo_dir = proc_entry / "fdinfo"
        if not fdinfo_dir.is_dir():
            continue
        try:
            fdinfo_paths = list(fdinfo_dir.iterdir())
        except OSError:
            continue
        for fdinfo_path in fdinfo_paths:
            try:
                text = fdinfo_path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            watch_count = sum(1 for line in text.splitlines() if line.startswith("inotify"))
            if watch_count == 0:
                continue
            total_instances += 1
            total_watches += watch_count
    return total_watches, total_instances


def _validate_local_inotify_capacity(
    *,
    required_inotify_max_user_watches: int,
    required_inotify_max_user_instances: int,
) -> None:
    current_max_user_watches = _read_proc_sysctl_int(
        INOTIFY_MAX_USER_WATCHES_PROC_PATH,
        "fs.inotify.max_user_watches",
    )
    current_max_user_instances = _read_proc_sysctl_int(
        INOTIFY_MAX_USER_INSTANCES_PROC_PATH,
        "fs.inotify.max_user_instances",
    )
    current_watch_usage, current_instance_usage = _count_current_inotify_usage()
    if current_max_user_watches < required_inotify_max_user_watches:
        raise ValueError(
            "local host fs.inotify.max_user_watches is below startup requirement: "
            f"current={current_max_user_watches} required={required_inotify_max_user_watches} "
            f"current_usage={current_watch_usage}; "
            "raise it before startup, for example: "
            f"sudo sysctl -w fs.inotify.max_user_watches={required_inotify_max_user_watches}"
        )
    if current_max_user_instances < required_inotify_max_user_instances:
        raise ValueError(
            "local host fs.inotify.max_user_instances is below startup requirement: "
            f"current={current_max_user_instances} required={required_inotify_max_user_instances} "
            f"current_usage={current_instance_usage}; "
            "raise it before startup, for example: "
            f"sudo sysctl -w fs.inotify.max_user_instances={required_inotify_max_user_instances}"
        )
        print(
            "[startbare.validate_local_inotify_capacity] ready "
            f"max_user_watches={current_max_user_watches} "
            f"max_user_instances={current_max_user_instances} "
            f"current_watch_usage={current_watch_usage} "
            f"current_instance_usage={current_instance_usage}"
        )


def _resolve_config_path(config_root: Path, raw_path: str, field_name: str) -> Path:
    resolved = Path(raw_path)
    if not resolved.is_absolute():
        resolved = (config_root / resolved).resolve()
    if not resolved.exists():
        raise ValueError(f"{field_name} does not exist: {resolved}")
    return resolved


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise ValueError(f"{field_name} does not exist after repo-root resolution: {raw_path}")
    return resolved


def _resolve_config_relative_path(config_root: Path, raw_path: str, field_name: str) -> Path:
    text = _require_str(raw_path, field_name)
    path = Path(text)
    if not path.is_absolute():
        path = (config_root / path).resolve()
    else:
        path = path.resolve()
    return path


def _test_runner_ui_external_url(*, host: str, port: int) -> str:
    host_text = host.strip()
    if ":" in host_text and not host_text.startswith("["):
        host_text = f"[{host_text}]"
    return f"http://{host_text}:{int(port)}"


def _test_runner_ui_probe_host(host: str) -> str:
    host_text = host.strip()
    if host_text in ("0.0.0.0", ""):
        return "127.0.0.1"
    if host_text == "::":
        return "::1"
    return host_text


def _test_runner_ui_probe_url(*, host: str, port: int) -> str:
    return _test_runner_ui_external_url(host=_test_runner_ui_probe_host(host), port=port)


def _test_runner_ui_disabled_summary() -> dict[str, Any]:
    return {
        "enabled": False,
        "status": "disabled",
        "host": None,
        "port": None,
        "url": None,
        "probe_url": None,
        "workdir": None,
        "log_path": None,
        "history_lookback_days": None,
        "history_roots": [],
        "gitops_config_path": None,
        "reused_existing": False,
        "pid": None,
    }


def _test_runner_ui_summary_from_cfg(
    ui_cfg: dict[str, Any],
    *,
    status: str,
    reused_existing: bool,
    pid: int | None,
) -> dict[str, Any]:
    return {
        "enabled": True,
        "status": status,
        "host": ui_cfg["host"],
        "port": int(ui_cfg["port"]),
        "url": ui_cfg["url"],
        "probe_url": ui_cfg["probe_url"],
        "workdir": str(ui_cfg["workdir"]),
        "log_path": str(ui_cfg["active_log_path"]),
        "history_lookback_days": int(ui_cfg["history_lookback_days"]),
        "history_roots": [str(path) for path in ui_cfg["history_roots"]],
        "gitops_config_path": (
            str(ui_cfg["gitops_config_path"]) if ui_cfg["gitops_config_path"] is not None else None
        ),
        "reused_existing": bool(reused_existing),
        "pid": pid,
    }


def _parse_test_runner_ui_config(
    config: dict[str, Any],
    *,
    config_root: Path,
) -> dict[str, Any]:
    raw = config.get("test_runner_ui")
    if raw is None:
        return _test_runner_ui_disabled_summary()
    ui_cfg = _require_mapping(raw, "test_runner_ui")
    enabled = ui_cfg.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("test_runner_ui.enabled must be a bool")
    if not enabled:
        return _test_runner_ui_disabled_summary()

    host = _require_str(ui_cfg.get("host", TEST_RUNNER_UI_DEFAULT_HOST), "test_runner_ui.host")
    port = _require_int(ui_cfg.get("port", TEST_RUNNER_UI_DEFAULT_PORT), "test_runner_ui.port", min_value=1)
    if port > 65535:
        raise ValueError("test_runner_ui.port must be <= 65535")
    history_lookback_days = _require_int(
        ui_cfg.get("history_lookback_days", TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS),
        "test_runner_ui.history_lookback_days",
        min_value=1,
    )
    workdir = _resolve_config_relative_path(
        config_root,
        str(ui_cfg.get("workdir", f"./{TEST_RUNNER_UI_DEFAULT_WORKDIR_NAME}")),
        "test_runner_ui.workdir",
    )
    history_roots_raw = ui_cfg.get("history_roots", [])
    if history_roots_raw is None:
        history_roots_raw = []
    if not isinstance(history_roots_raw, list):
        raise ValueError("test_runner_ui.history_roots must be a list")
    history_roots: list[Path] = []
    for idx, item in enumerate(history_roots_raw):
        history_roots.append(
            _resolve_config_relative_path(
                config_root,
                _require_str(item, f"test_runner_ui.history_roots[{idx}]"),
                f"test_runner_ui.history_roots[{idx}]",
            )
        )
    gitops_config_path: Path | None = None
    if ui_cfg.get("gitops_config_path") is not None:
        gitops_config_path = _resolve_config_path(
            config_root,
            _require_str(ui_cfg.get("gitops_config_path"), "test_runner_ui.gitops_config_path"),
            "test_runner_ui.gitops_config_path",
        )
    log_path = (workdir.resolve() / TEST_RUNNER_UI_LOG_FILENAME).resolve()
    active_log_path = log_shard.daily_sharded_log_path(log_path)
    return {
        "enabled": True,
        "host": host,
        "port": int(port),
        "url": _test_runner_ui_external_url(host=host, port=port),
        "probe_url": _test_runner_ui_probe_url(host=host, port=port),
        "workdir": workdir.resolve(),
        "log_path": log_path,
        "active_log_path": active_log_path,
        "history_lookback_days": int(history_lookback_days),
        "history_roots": [path.resolve() for path in history_roots],
        "gitops_config_path": gitops_config_path.resolve() if gitops_config_path is not None else None,
        "entrypoint": (REPO_ROOT / "fluxon_test_stack" / "test_runner_ui.py").resolve(),
    }


def _read_text_tail(path: Path, *, max_bytes: int = 8192) -> str:
    if not path.exists():
        return ""
    raw = path.read_bytes()
    if len(raw) > max_bytes:
        raw = raw[-max_bytes:]
    return raw.decode("utf-8", errors="replace")


def _bare_service_runtime_log_path(*, local_node_cfg: dict[str, Any], service_name: str) -> Path | None:
    hostworkdir = local_node_cfg.get("hostworkdir")
    if not isinstance(hostworkdir, str) or not hostworkdir.strip():
        return None
    root = Path(hostworkdir)
    if service_name == "tikv":
        return root / "monitor" / "tikv" / "store" / "tikv.log"
    if service_name == "tikv_pd":
        return root / "monitor" / "tikv" / "pd" / "pd.log"
    return root / "log" / f"{service_name}.log"


def _resolve_bare_service_readable_runtime_log_path(
    *,
    local_node_cfg: dict[str, Any],
    service_name: str,
) -> Path | None:
    runtime_log_path = _bare_service_runtime_log_path(
        local_node_cfg=local_node_cfg,
        service_name=service_name,
    )
    if runtime_log_path is None:
        return None
    resolved_log_path = log_shard.resolve_readable_log_path(runtime_log_path)
    if resolved_log_path is not None:
        return resolved_log_path
    return runtime_log_path


def _test_runner_ui_health_payload(*, probe_url: str, timeout_seconds: float) -> dict[str, Any] | None:
    req = urllib.request.Request(probe_url.rstrip("/") + "/health", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_seconds)) as resp:
            raw = resp.read()
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _test_runner_ui_health_matches(
    payload: dict[str, Any],
    *,
    ui_cfg: dict[str, Any],
) -> bool:
    if payload.get("ok") is not True:
        return False
    if payload.get("service") != "test_runner_ui":
        return False
    if str(payload.get("workdir_root") or "") != str(ui_cfg["workdir"]):
        return False
    payload_port = payload.get("port")
    if not isinstance(payload_port, int) or payload_port < 1 or payload_port != int(ui_cfg["port"]):
        return False
    if str(payload.get("host") or "") != str(ui_cfg["host"]):
        return False
    payload_lookback_days = payload.get("lookback_days")
    if (
        not isinstance(payload_lookback_days, int)
        or payload_lookback_days < 1
        or payload_lookback_days != int(ui_cfg["history_lookback_days"])
    ):
        return False
    payload_history_roots = payload.get("history_roots")
    if not isinstance(payload_history_roots, list):
        return False
    if [str(item) for item in payload_history_roots] != [str(path) for path in ui_cfg["history_roots"]]:
        return False
    payload_gitops = payload.get("gitops_config_path")
    expected_gitops = str(ui_cfg["gitops_config_path"]) if ui_cfg["gitops_config_path"] is not None else None
    if payload_gitops != expected_gitops:
        return False
    return True


def _ensure_test_runner_ui_started(*, ui_cfg: dict[str, Any]) -> dict[str, Any]:
    if not ui_cfg.get("enabled"):
        return _test_runner_ui_disabled_summary()

    workdir = Path(ui_cfg["workdir"]).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    probe_url = _require_str(ui_cfg.get("probe_url"), "test_runner_ui.probe_url")
    health_payload = _test_runner_ui_health_payload(
        probe_url=probe_url,
        timeout_seconds=1.0,
    )
    if isinstance(health_payload, dict):
        if _test_runner_ui_health_matches(health_payload, ui_cfg=ui_cfg):
            return _test_runner_ui_summary_from_cfg(
                ui_cfg,
                status="reused_existing",
                reused_existing=True,
                pid=None,
            )
        raise RuntimeError(
            "test_runner_ui port is already serving a different process: "
            f"probe_url={probe_url} payload={health_payload}"
        )

    argv = [
        sys.executable,
        str(ui_cfg["entrypoint"]),
        "--workdir",
        str(workdir),
        "--host",
        str(ui_cfg["host"]),
        "--port",
        str(ui_cfg["port"]),
        "--history-lookback-days",
        str(ui_cfg["history_lookback_days"]),
    ]
    for history_root in ui_cfg["history_roots"]:
        argv.extend(["--history-root", str(history_root)])
    if ui_cfg["gitops_config_path"] is not None:
        argv.extend(["--gitops-config", str(ui_cfg["gitops_config_path"])])

    log_path = Path(ui_cfg["active_log_path"]).resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_handle = log_path.open("a", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            argv,
            cwd=str(REPO_ROOT),
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log_handle.close()

    deadline = time.time() + TEST_RUNNER_UI_HEALTH_TIMEOUT_SECONDS
    while time.time() < deadline:
        health_payload = _test_runner_ui_health_payload(
            probe_url=probe_url,
            timeout_seconds=1.0,
        )
        if isinstance(health_payload, dict) and _test_runner_ui_health_matches(health_payload, ui_cfg=ui_cfg):
            return _test_runner_ui_summary_from_cfg(
                ui_cfg,
                status="started",
                reused_existing=False,
                pid=int(proc.pid),
            )
        rc = proc.poll()
        if rc is not None:
            raise RuntimeError(
                "test_runner_ui exited before becoming healthy: "
                f"rc={rc} log_path={log_path} tail={_read_text_tail(log_path)!r}"
            )
        time.sleep(TEST_RUNNER_UI_HEALTH_POLL_SECONDS)

    raise RuntimeError(
        "test_runner_ui did not become healthy before timeout: "
        f"probe_url={probe_url} log_path={log_path} tail={_read_text_tail(log_path)!r}"
    )


def _build_start_test_bed_summary(
    *,
    bootstrap_mode: str,
    controller_url: str,
    bootstrap_phases: list[dict[str, Any]],
    initial_controller_reachable: bool,
    deleted_target_apply_ids: list[str],
    coverage_bootstrap_excluded_targets: list[dict[str, Any]],
    fixed_bootstrap_waves: list[dict[str, Any]],
    coverage_bootstrap_batches: list[dict[str, Any]],
    coverage_bootstrap_waves: list[dict[str, Any]],
    deploy_atomic_selection_names: list[str],
    deploy_plain_selection_names: list[str],
    deploy_response_atomic: dict[str, Any] | None,
    deploy_response_plain: dict[str, Any] | None,
    apply_wait_atomic: dict[str, Any] | None,
    apply_wait_plain: dict[str, Any] | None,
    test_runner_ui: dict[str, Any],
) -> dict[str, Any]:
    summary = {
        "bootstrap_mode": bootstrap_mode,
        "controller_url": controller_url,
        "bootstrap_phases": bootstrap_phases,
        "initial_controller_reachable": initial_controller_reachable,
        "deleted_target_apply_ids": _dedup_str_list(deleted_target_apply_ids),
        "coverage_bootstrap_excluded_targets": coverage_bootstrap_excluded_targets,
        "fixed_bootstrap_waves": fixed_bootstrap_waves,
        "coverage_bootstrap_batches": coverage_bootstrap_batches,
        "coverage_bootstrap_waves": coverage_bootstrap_waves,
        "deploy_atomic_selection_names": deploy_atomic_selection_names,
        "deploy_plain_selection_names": deploy_plain_selection_names,
        "deploy_response_atomic": deploy_response_atomic,
        "deploy_response_plain": deploy_response_plain,
        "apply_wait_atomic": apply_wait_atomic,
        "apply_wait_plain": apply_wait_plain,
        "test_runner_ui": test_runner_ui,
    }
    summary["test_runner_ui_enabled"] = test_runner_ui.get("enabled")
    summary["test_runner_ui_status"] = test_runner_ui.get("status")
    summary["test_runner_ui_host"] = test_runner_ui.get("host")
    summary["test_runner_ui_port"] = test_runner_ui.get("port")
    summary["test_runner_ui_url"] = test_runner_ui.get("url")
    summary["test_runner_ui_probe_url"] = test_runner_ui.get("probe_url")
    summary["test_runner_ui_workdir"] = test_runner_ui.get("workdir")
    summary["test_runner_ui_log_path"] = test_runner_ui.get("log_path")
    summary["test_runner_ui_history_lookback_days"] = test_runner_ui.get("history_lookback_days")
    summary["test_runner_ui_history_roots"] = test_runner_ui.get("history_roots")
    summary["test_runner_ui_gitops_config_path"] = test_runner_ui.get("gitops_config_path")
    summary["test_runner_ui_reused_existing"] = test_runner_ui.get("reused_existing")
    summary["test_runner_ui_pid"] = test_runner_ui.get("pid")
    return summary


def _parse_cluster_nodes(deployconf: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_nodes = deployconf.get("cluster_nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise ValueError("deployconf.cluster_nodes must be a non-empty list")

    out: dict[str, dict[str, Any]] = {}
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            raise ValueError("deployconf.cluster_nodes[] must be mappings")
        hostname = _require_str(raw_node.get("hostname"), "cluster_nodes[].hostname")
        _require_str(raw_node.get("ip"), f"cluster_nodes[{hostname}].ip")
        _require_str(raw_node.get("hostworkdir"), f"cluster_nodes[{hostname}].hostworkdir")
        execution_mode = raw_node.get("execution_mode", "ssh")
        if not isinstance(execution_mode, str) or execution_mode.strip() not in {"ssh", "local"}:
            raise ValueError(f"cluster_nodes[{hostname}].execution_mode must be 'ssh' or 'local'")
        _require_str(raw_node.get("ssh_user"), f"cluster_nodes[{hostname}].ssh_user")
        _require_int(raw_node.get("ssh_port"), f"cluster_nodes[{hostname}].ssh_port", min_value=1)
        ssh_password = raw_node.get("ssh_password")
        if ssh_password is not None and (not isinstance(ssh_password, str) or not ssh_password.strip()):
            raise ValueError(f"cluster_nodes[{hostname}].ssh_password must be a non-empty string when present")
        if hostname in out:
            raise ValueError(f"Duplicate cluster_nodes hostname: {hostname}")
        node_cfg = dict(raw_node)
        node_cfg["execution_mode"] = execution_mode.strip()
        out[hostname] = node_cfg
    return out


def _cluster_node_ssh_host(node_cfg: dict[str, Any], *, node_name: str) -> str:
    ssh_host = node_cfg.get("ssh_host")
    if ssh_host is None:
        return _require_str(node_cfg.get("ip"), f"cluster_nodes[{node_name}].ip")
    return _require_str(ssh_host, f"cluster_nodes[{node_name}].ssh_host")


def _parse_cluster_name(deployconf: dict[str, Any]) -> str:
    global_envs = deployconf.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")
    return _require_str(global_envs.get("FLUXON_CLUSTER_NAME"), "global_envs.FLUXON_CLUSTER_NAME")


def _parse_bootstrap_bare_services(deployconf: dict[str, Any]) -> set[str]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")

    raw = deployconf.get("bootstrap_bare_services")
    service_names = _require_list_of_str(raw, "deployconf.bootstrap_bare_services")
    out: set[str] = set()
    for service_name in service_names:
        if service_name not in services:
            raise ValueError(f"deployconf.bootstrap_bare_services references unknown service: {service_name}")
        out.add(service_name)
    return out


def _parse_bootstrap_phases(
    raw: Any,
    *,
    field_name: str,
    cluster_nodes: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{field_name} must be a non-empty list")

    phases: list[dict[str, Any]] = []
    for idx, raw_phase in enumerate(raw):
        if not isinstance(raw_phase, dict):
            raise ValueError(f"{field_name}[{idx}] must be a mapping")
        mode = _require_str(raw_phase.get("mode"), f"{field_name}[{idx}].mode")
        services = _require_list_of_str(raw_phase.get("services"), f"{field_name}[{idx}].services")
        if mode == PHASE_MODE_FIXED_BARE:
            node_name = _require_str(raw_phase.get("node"), f"{field_name}[{idx}].node")
            if node_name not in cluster_nodes:
                raise ValueError(f"{field_name}[{idx}].node references unknown node: {node_name}")
            phases.append({"mode": mode, "node": node_name, "services": services})
            continue
        if mode == PHASE_MODE_COVERAGE_BARE:
            if raw_phase.get("node") is not None:
                raise ValueError(f"{field_name}[{idx}].node is not allowed for mode={PHASE_MODE_COVERAGE_BARE}")
            phases.append({"mode": mode, "services": services})
            continue
        raise ValueError(
            f"{field_name}[{idx}].mode must be '{PHASE_MODE_FIXED_BARE}' or '{PHASE_MODE_COVERAGE_BARE}'"
        )
    return phases


def _bootstrap_fixed_batches(bootstrap_phases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for phase in bootstrap_phases:
        if phase["mode"] != PHASE_MODE_FIXED_BARE:
            continue
        out.append({"node": phase["node"], "services": list(phase["services"])})
    if not out:
        raise ValueError("bootstrap_phases must contain at least one fixed_bare phase")
    return out


def _bootstrap_coverage_services(bootstrap_phases: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for phase in bootstrap_phases:
        if phase["mode"] != PHASE_MODE_COVERAGE_BARE:
            continue
        out.extend(phase["services"])
    return _dedup_str_list(out)


def _parse_name_list(raw: Any, *, field_name: str) -> list[str]:
    return _require_list_of_str(raw, field_name)


def _resolve_local_node_cfg(cluster_nodes: dict[str, dict[str, Any]]) -> dict[str, Any]:
    local_nodes = [node_cfg for node_cfg in cluster_nodes.values() if _cluster_node_is_local(node_cfg)]
    if local_nodes:
        local_hostname = subprocess.check_output(["hostname"], text=True).strip()
        local_controller_nodes = [
            node_cfg
            for node_cfg in local_nodes
            if _require_str(node_cfg.get("hostname"), "cluster_nodes[].hostname") == local_hostname
        ]
        if local_controller_nodes:
            return local_controller_nodes[0]
        return local_nodes[0]
    local_hostname = subprocess.check_output(["hostname"], text=True).strip()
    node_cfg = cluster_nodes.get(local_hostname)
    if node_cfg is None:
        raise ValueError(f"Current hostname is not present in deployconf.cluster_nodes: {local_hostname}")
    return node_cfg


def _cluster_node_is_local(node_cfg: dict[str, Any]) -> bool:
    execution_mode = _require_str(node_cfg.get("execution_mode", "ssh"), "cluster_nodes[].execution_mode")
    return execution_mode == "local"


def _cluster_node_is_local_name(
    cluster_nodes: dict[str, dict[str, Any]],
    *,
    node_name: str,
) -> bool:
    node_cfg = cluster_nodes.get(node_name)
    if node_cfg is None:
        raise ValueError(f"unknown cluster node: {node_name}")
    return _cluster_node_is_local(node_cfg)


def _validate_fixed_bootstrap_batches(
    *,
    deployconf: dict[str, Any],
    fixed_bootstrap_batches: list[dict[str, Any]],
    bootstrap_bare_services: set[str],
    local_node_name: str,
) -> None:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")

    started_bootstrap_bare_services: set[str] = set()
    seen_non_bootstrap_batch = False
    local_fixed_bootstrap_service_names: set[str] = set()
    for batch_idx, batch in enumerate(fixed_bootstrap_batches):
        node_name = batch["node"]
        seen_service_names: set[str] = set()
        bootstrap_services_in_batch: list[str] = []
        non_bootstrap_services_in_batch: list[str] = []
        for service_name in batch["services"]:
            if service_name in seen_service_names:
                raise ValueError(
                    f"bootstrap_phases fixed_bare[{batch_idx}] contains duplicate service: {service_name}"
                )
            seen_service_names.add(service_name)

            bind_nodes = _resolve_selection_nodes(
                deployconf=deployconf,
                selection_name=service_name,
            )
            if node_name not in bind_nodes:
                raise ValueError(
                    f"bootstrap_phases fixed_bare[{batch_idx}] schedules '{service_name}' on '{node_name}', "
                    f"but deployconf binds it to {bind_nodes}"
                )
            if node_name == local_node_name:
                local_fixed_bootstrap_service_names.update(
                    _selection_service_names(
                        deployconf=deployconf,
                        selection_name=service_name,
                    )
                )

            if service_name in bootstrap_bare_services:
                bootstrap_services_in_batch.append(service_name)
                started_bootstrap_bare_services.add(service_name)
            else:
                non_bootstrap_services_in_batch.append(service_name)

        if bootstrap_services_in_batch and non_bootstrap_services_in_batch:
            raise ValueError(
                "A batch that starts deployconf.bootstrap_bare_services must contain only bootstrap bare services; "
                f"mixed batch at bootstrap_phases fixed_bare[{batch_idx}]"
            )
        if bootstrap_services_in_batch:
            if seen_non_bootstrap_batch:
                raise ValueError(
                    "deployconf.bootstrap_bare_services must appear only in the leading bootstrap phase; "
                    f"found bootstrap services again in bootstrap_phases fixed_bare[{batch_idx}]"
                )
            continue
        seen_non_bootstrap_batch = True

    missing_bootstrap_bare_services = sorted(bootstrap_bare_services - started_bootstrap_bare_services)
    if missing_bootstrap_bare_services:
        raise ValueError(
            "bootstrap_phases fixed_bare must start every deployconf.bootstrap_bare_services entry: "
            + ", ".join(missing_bootstrap_bare_services)
        )
    missing_local_controller_bootstrap_services = sorted(
        LOCAL_CONTROLLER_BOOTSTRAP_SERVICE_NAMES - local_fixed_bootstrap_service_names
    )
    if missing_local_controller_bootstrap_services:
        raise ValueError(
            "bootstrap_phases fixed_bare must cover the local controller bootstrap services: "
            + ", ".join(missing_local_controller_bootstrap_services)
        )


def _validate_coverage_bootstrap_services(
    *,
    deployconf: dict[str, Any],
    coverage_bootstrap_services: list[str],
    bootstrap_bare_services: set[str],
) -> None:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")

    seen: set[str] = set()
    for service_name in coverage_bootstrap_services:
        if service_name in seen:
            raise ValueError(f"coverage_bootstrap_services contains duplicate entry: {service_name}")
        seen.add(service_name)
        if service_name in bootstrap_bare_services:
            raise ValueError(
                f"coverage_bootstrap_services cannot contain bare-only bootstrap service: {service_name}"
            )
        if service_name in atomic_groups:
            raise ValueError(
                f"coverage_bootstrap_services must contain plain services, not atomic groups: {service_name}"
            )
        if service_name not in services:
            raise ValueError(f"coverage_bootstrap_services references unknown service: {service_name}")


def _validate_deploy_workloads(
    *,
    deployconf: dict[str, Any],
    deploy_workloads: list[str],
    bootstrap_bare_services: set[str],
) -> None:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")

    seen: set[str] = set()
    for selection_name in deploy_workloads:
        if selection_name in seen:
            raise ValueError(f"deploy_workloads contains duplicate entry: {selection_name}")
        seen.add(selection_name)
        if selection_name in bootstrap_bare_services:
            raise ValueError(f"deploy_workloads cannot contain bare-only bootstrap service: {selection_name}")
        if selection_name in atomic_groups:
            continue
        if selection_name not in services:
            raise ValueError(f"deploy_workloads references unknown service or atomic group: {selection_name}")
        target_nodes = _resolve_selection_target_nodes(
            deployconf=deployconf,
            selection_name=selection_name,
        )
        if not target_nodes:
            raise ValueError(
                "deploy_workloads references a service that is fully covered by atomic_groups: "
                f"{selection_name}"
            )


def _validate_coverage_takeover_targets(
    *,
    deployconf: dict[str, Any],
    coverage_bootstrap_services: list[str],
    deploy_workloads: list[str],
) -> None:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    deploy_workloads_set = set(deploy_workloads)
    for service_name in coverage_bootstrap_services:
        expected_nodes = _resolve_service_nodes(services=services, service_name=service_name)
        required_selection_names = _dedup_str_list(
            [
                _resolve_coverage_selection_name(
                    deployconf=deployconf,
                    service_name=service_name,
                    node_name=node_name,
                )
                for node_name in expected_nodes
            ]
        )
        missing_selection_names = [
            selection_name
            for selection_name in required_selection_names
            if selection_name not in deploy_workloads_set
        ]
        if missing_selection_names:
            raise ValueError(
                "deploy_workloads must include every takeover selection required by coverage service "
                f"{service_name}: missing={missing_selection_names}"
            )


def _resolve_service_nodes(*, services: dict[str, Any], service_name: str) -> list[str]:
    service_cfg = services.get(service_name)
    if not isinstance(service_cfg, dict):
        raise ValueError(f"service.{service_name} must be a mapping")
    node_bind = service_cfg.get("node_bind")
    if not isinstance(node_bind, dict):
        raise ValueError(f"service.{service_name}.node_bind must be a mapping")
    return _require_list_of_str(node_bind.get("node"), f"service.{service_name}.node_bind.node")


def _resolve_selection_service_name(*, deployconf: dict[str, Any], selection_name: str) -> str:
    return _selection_resolve_selection_service_name(
        selection_name=selection_name,
    )


def _resolve_selection_nodes(*, deployconf: dict[str, Any], selection_name: str) -> list[str]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    service_nodes_by_service = {
        service_name: _resolve_service_nodes(services=services, service_name=service_name)
        for service_name in services
    }
    return _selection_resolve_selection_nodes(
        selection_name=selection_name,
        services=services,
        atomic_groups=atomic_groups,
        service_nodes_by_service=service_nodes_by_service,
    )


def _resolve_selection_target_nodes(*, deployconf: dict[str, Any], selection_name: str) -> list[str]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    service_nodes_by_service = {
        service_name: _resolve_service_nodes(services=services, service_name=service_name)
        for service_name in services
    }
    return _selection_resolve_selection_target_nodes(
        selection_name=selection_name,
        services=services,
        atomic_groups=atomic_groups,
        service_nodes_by_service=service_nodes_by_service,
    )


def _resolve_coverage_selection_name(*, deployconf: dict[str, Any], service_name: str, node_name: str) -> str:
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    return _selection_resolve_coverage_selection_name(
        service_name=service_name,
        node_name=node_name,
        atomic_groups=atomic_groups,
    )


def _selection_expected_workloads(
    *,
    deployconf: dict[str, Any],
    selection_name: str,
    name_prefix: str,
) -> list[dict[str, Any]]:
    services = _require_mapping(deployconf.get("service"), "deployconf.service")
    atomic_groups = _require_mapping(deployconf.get("atomic_groups"), "deployconf.atomic_groups")
    if selection_name in atomic_groups:
        group_cfg = _require_mapping(atomic_groups.get(selection_name), f"atomic_groups.{selection_name}")
        group_nodes = _require_list_of_str(group_cfg.get("nodes"), f"atomic_groups.{selection_name}.nodes")
        service_names = _require_list_of_str(group_cfg.get("services"), f"atomic_groups.{selection_name}.services")
        expected: list[dict[str, Any]] = []
        for service_name in service_names:
            if service_name not in services:
                raise ValueError(f"atomic_groups.{selection_name}.services references unknown service: {service_name}")
            bind_nodes = _resolve_service_nodes(services=services, service_name=service_name)
            nodes = [node_name for node_name in group_nodes if node_name in bind_nodes]
            if not nodes:
                continue
            expected.append({
                "selection_name": selection_name,
                "kind": "DaemonSet",
                "name": _selection_atomic_group_member_selection_workload_name(
                    name_prefix=name_prefix,
                    selection_name=selection_name,
                    service_name=service_name,
                ),
                "nodes": nodes,
                "agent_instance_keys": [_ops_agent_instance_key(node_name) for node_name in nodes],
                "member_keys": _service_member_keys(service_name=service_name, nodes=nodes),
            })
        if not expected:
            raise ValueError(
                "expected workload selection resolved to zero member workloads; this indicates an invalid "
                f"deploy_workloads entry: {selection_name}"
            )
        return expected

    nodes = _resolve_selection_target_nodes(deployconf=deployconf, selection_name=selection_name)
    if not nodes:
        raise ValueError(
            "expected workload selection resolved to zero target nodes; this indicates an invalid "
            f"deploy_workloads entry: {selection_name}"
        )
    return [{
        "selection_name": selection_name,
        "kind": "DaemonSet",
        "name": _selection_plain_workload_name(
            name_prefix=name_prefix,
            selection_name=selection_name,
        ),
        "nodes": nodes,
        "agent_instance_keys": [_ops_agent_instance_key(node_name) for node_name in nodes],
        "member_keys": _selection_member_keys(
            deployconf=deployconf,
            selection_name=selection_name,
            nodes=nodes,
        ),
    }]


def _selection_transition_cleanup_workloads(
    *,
    deployconf: dict[str, Any],
    selection_name: str,
    name_prefix: str,
) -> list[dict[str, Any]]:
    expected_workloads = _selection_expected_workloads(
        deployconf=deployconf,
        selection_name=selection_name,
        name_prefix=name_prefix,
    )
    atomic_groups = _require_mapping(deployconf.get("atomic_groups"), "deployconf.atomic_groups")
    if selection_name not in atomic_groups:
        return expected_workloads

    # English note:
    # - `fluxon_core_controller` used to be materialized as one monolithic DaemonSet workload.
    # - Newer deployconf expands that logical selection into explicit atomic-group member workloads.
    # - Bare recovery and stale-desired cleanup must therefore target both identities during the
    #   transition window, otherwise the old monolithic desired owner survives and races the new
    #   member-based runtime back into place.
    return expected_workloads + [{
        "selection_name": selection_name,
        "kind": "DaemonSet",
        "name": _selection_plain_workload_name(
            name_prefix=name_prefix,
            selection_name=selection_name,
        ),
        "nodes": [],
        "agent_instance_keys": [],
        "member_keys": [],
    }]


def _selection_member_keys(
    *,
    deployconf: dict[str, Any],
    selection_name: str,
    nodes: list[str],
) -> list[str]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")

    if selection_name in atomic_groups:
        group_cfg = atomic_groups[selection_name]
        if not isinstance(group_cfg, dict):
            raise ValueError(f"atomic_groups.{selection_name} must be a mapping")
        service_names = _require_list_of_str(group_cfg.get("services"), f"atomic_groups.{selection_name}.services")
        out: list[str] = []
        for service_name in service_names:
            bind_nodes = _resolve_service_nodes(services=services, service_name=service_name)
            eligible_nodes = [
                node_name
                for node_name in nodes
                if node_name in bind_nodes
            ]
            if not eligible_nodes:
                continue
            out.extend(_service_member_keys(service_name=service_name, nodes=eligible_nodes))
        return _dedup_str_list(out)

    service_name = _resolve_selection_service_name(deployconf=deployconf, selection_name=selection_name)
    return _service_member_keys(service_name=service_name, nodes=nodes)


def _service_member_keys(*, service_name: str, nodes: list[str]) -> list[str]:
    if service_name in {"etcd", "greptime"}:
        return []
    if service_name == "master":
        return ["unified_master"]
    if service_name == "owner":
        return [f"owner_{node_name}" for node_name in nodes]
    if service_name == "fluxon_fs_master":
        return ["fluxon_fs_master"]
    if service_name == "fluxon_fs_agent":
        return [f"fluxon_fs_agent_{node_name}" for node_name in nodes]
    if service_name == "ops_controller":
        return [f"ops_controller_{node_name}" for node_name in nodes]
    if service_name == "ops_agent":
        return [_ops_agent_instance_key(node_name) for node_name in nodes]
    return []


def _build_coverage_bootstrap_batches(
    *,
    deployconf: dict[str, Any],
    coverage_bootstrap_services: list[str],
    excluded_targets: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    by_node: dict[str, list[str]] = {}
    for service_name in coverage_bootstrap_services:
        for node_name in _resolve_service_nodes(services=services, service_name=service_name):
            selection_name = _resolve_coverage_selection_name(
                deployconf=deployconf,
                service_name=service_name,
                node_name=node_name,
            )
            if (node_name, selection_name) in excluded_targets:
                continue
            node_selection_names = by_node.setdefault(node_name, [])
            if selection_name not in node_selection_names:
                node_selection_names.append(selection_name)
    return [
        {
            "node": node_name,
            "services": _order_coverage_bootstrap_selections(
                deployconf=deployconf,
                selection_names=selection_names,
            ),
        }
        for node_name, selection_names in by_node.items()
    ]


def _build_bare_launch_waves_from_batches(
    *,
    batches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    node_order: list[str] = []
    pending_selection_names_by_node: dict[str, list[str]] = {}
    for batch_idx, batch in enumerate(batches):
        node_name = _require_str(batch.get("node"), f"batches[{batch_idx}].node")
        selection_names = _require_list_of_str(
            batch.get("services"),
            f"batches[{batch_idx}].services",
        )
        if node_name not in pending_selection_names_by_node:
            node_order.append(node_name)
            pending_selection_names_by_node[node_name] = []
        pending_selection_names_by_node[node_name].extend(selection_names)

    waves: list[dict[str, Any]] = []
    while True:
        wave_launches: list[dict[str, str]] = []
        for node_name in node_order:
            pending_selection_names = pending_selection_names_by_node[node_name]
            if not pending_selection_names:
                continue
            wave_launches.append(
                {
                    "node": node_name,
                    "selection_name": pending_selection_names.pop(0),
                }
            )
        if not wave_launches:
            break
        waves.append({"launches": wave_launches})
    return waves


def _order_coverage_bootstrap_selections(
    *,
    deployconf: dict[str, Any],
    selection_names: list[str],
) -> list[str]:
    # English note:
    # - Coverage bare is launch-only repair, but the repair unit must still match the real
    #   selection identity for that node.
    # - Core protocol/control-plane selections must come first so dependent fs selections never
    #   start against a half-present or version-mismatched cluster path.
    return sorted(
        selection_names,
        key=lambda selection_name: (
            _coverage_bootstrap_selection_priority(
                deployconf=deployconf,
                selection_name=selection_name,
            ),
            selection_name,
        ),
    )


def _coverage_bootstrap_selection_priority(*, deployconf: dict[str, Any], selection_name: str) -> int:
    service_names = _selection_service_names(
        deployconf=deployconf,
        selection_name=selection_name,
    )
    if any(service_name in {"master", "owner", "ops_controller", "ops_agent"} for service_name in service_names):
        return 0
    if "fluxon_fs_master" in service_names:
        return 1
    if "fluxon_fs_agent" in service_names:
        return 2
    return 3


def _local_control_plane_coverage_excluded_targets(
    *,
    deployconf: dict[str, Any],
    fixed_bootstrap_batches: list[dict[str, Any]],
    local_node_name: str,
    coverage_bootstrap_services: list[str],
) -> list[dict[str, Any]]:
    local_fixed_service_names: set[str] = set()
    for batch_idx, batch in enumerate(fixed_bootstrap_batches):
        node_name = _require_str(batch.get("node"), f"fixed_bootstrap_batches[{batch_idx}].node")
        if node_name != local_node_name:
            continue
        batch_selection_names = _require_list_of_str(
            batch.get("services"),
            f"fixed_bootstrap_batches[{batch_idx}].services",
        )
        for selection_name in batch_selection_names:
            for service_name in _selection_service_names_for_target_node(
                deployconf=deployconf,
                selection_name=selection_name,
                node_name=local_node_name,
            ):
                if service_name in CONTROL_PLANE_HANDOVER_SERVICE_NAMES:
                    local_fixed_service_names.add(service_name)
    excluded_targets: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for service_name in coverage_bootstrap_services:
        if service_name not in local_fixed_service_names:
            continue
        selection_name = _resolve_coverage_selection_name(
            deployconf=deployconf,
            service_name=service_name,
            node_name=local_node_name,
        )
        key = (local_node_name, selection_name)
        if key in seen:
            continue
        seen.add(key)
        service_names = _selection_service_names_for_target_node(
            deployconf=deployconf,
            selection_name=selection_name,
            node_name=local_node_name,
        )
        excluded_targets.append(
            {
                "node": local_node_name,
                "selection_name": selection_name,
                "service_names": service_names,
                "reason": "local_fixed_bare_already_started_same_control_plane_service",
            }
        )
    return excluded_targets


def _normalize_deploy_manifest_workload_name(
    *,
    deployconf: dict[str, Any],
    doc: dict[str, Any],
    doc_index: int,
) -> None:
    # English note:
    # - bare bootstrap and desired apply must share one workload identity for the same logical
    #   selection on a node. Only apply-owned fields such as `apply_id` are allowed to differ.
    # - start_test_bed therefore fail-closes if the generated DaemonSet manifest drifts away from
    #   the exact name implied by the current deployconf name_prefix.
    metadata = _require_mapping(doc.get("metadata"), f"deploy_payload[{doc_index}].metadata")
    annotations = _require_mapping(
        metadata.get("annotations"),
        f"deploy_payload[{doc_index}].metadata.annotations",
    )
    logical_selection = _require_str(
        annotations.get("fluxon.io/logical_selection"),
        f"deploy_payload[{doc_index}].metadata.annotations.fluxon.io/logical_selection",
    )
    service_name = _require_str(
        annotations.get("fluxon.io/service_name"),
        f"deploy_payload[{doc_index}].metadata.annotations.fluxon.io/service_name",
    )
    atomic_group_raw = annotations.get("fluxon.io/atomic_group")
    atomic_group = None
    if atomic_group_raw is not None:
        atomic_group = _require_str(
            atomic_group_raw,
            f"deploy_payload[{doc_index}].metadata.annotations.fluxon.io/atomic_group",
        )

    current_name = _require_str(
        metadata.get("name"),
        f"deploy_payload[{doc_index}].metadata.name",
    )
    current_name_prefix = _require_str(deployconf.get("name_prefix"), "deployconf.name_prefix")

    if atomic_group is None:
        expected_current_name = _selection_plain_workload_name(
            name_prefix=current_name_prefix,
            selection_name=logical_selection,
        )
    else:
        expected_current_name = _selection_atomic_group_member_selection_workload_name(
            name_prefix=current_name_prefix,
            selection_name=logical_selection,
            service_name=service_name,
        )
    if current_name != expected_current_name:
        raise ValueError(
            "generated deploy manifest workload identity drifted from the shared naming contract: "
            f"doc_index={doc_index} current_name={current_name!r} expected_name={expected_current_name!r}"
        )


def _load_deploy_payload(
    *,
    deployconf: dict[str, Any],
    daemonset_dir: Path,
    deploy_workloads: list[str],
) -> str:
    manifests: list[dict[str, Any]] = []
    for selection_name in deploy_workloads:
        path = daemonset_dir / f"{selection_name}.daemonset.yaml"
        if not path.exists():
            raise ValueError(f"Missing generated DaemonSet YAML for deploy workload: {path}")
        text = path.read_text(encoding="utf-8")
        for doc_index, raw_doc in enumerate(yaml.safe_load_all(text)):
            if raw_doc is None:
                continue
            doc = _require_mapping(
                raw_doc,
                f"generated deploy manifest {path} document[{doc_index}]",
            )
            _normalize_deploy_manifest_workload_name(
                deployconf=deployconf,
                doc=doc,
                doc_index=doc_index,
            )
            manifests.append(doc)
    if not manifests:
        raise ValueError(f"Generated deploy payload is empty: {daemonset_dir}")
    return yaml.safe_dump_all(manifests, sort_keys=False, explicit_start=len(manifests) > 1)


def _run_subprocess(argv: list[str], *, cwd: Path) -> None:
    print("[startbare.cmd] " + " ".join(manual_dispatch_release.sh_quote(x) for x in argv))
    subprocess.check_call(argv, cwd=str(cwd))


def _ops_agent_instance_key(node_name: str) -> str:
    return OPS_AGENT_INSTANCE_KEY_PREFIX + node_name


def _selection_workload_keys(*, deployconf: dict[str, Any], selection_names: list[str]) -> list[str]:
    name_prefix = _require_str(deployconf.get("name_prefix"), "deployconf.name_prefix")
    keys: list[str] = []
    for selection_name in selection_names:
        for workload in _selection_transition_cleanup_workloads(
            deployconf=deployconf,
            selection_name=selection_name,
            name_prefix=name_prefix,
        ):
            keys.append(
                f"{_require_str(workload.get('kind'), 'expected_workload.kind')}/"
                f"{_require_str(workload.get('name'), 'expected_workload.name')}"
            )
    return _dedup_str_list(keys)


def _selection_agent_instance_keys(
    *,
    deployconf: dict[str, Any],
    selection_names: list[str],
) -> list[str]:
    name_prefix = _require_str(deployconf.get("name_prefix"), "deployconf.name_prefix")
    keys: list[str] = []
    for selection_name in selection_names:
        for workload in _selection_transition_cleanup_workloads(
            deployconf=deployconf,
            selection_name=selection_name,
            name_prefix=name_prefix,
        ):
            raw_agent_instance_keys = workload.get("agent_instance_keys")
            if not isinstance(raw_agent_instance_keys, list):
                raise ValueError("expected_workload.agent_instance_keys must be a list")
            for idx, raw_instance_key in enumerate(raw_agent_instance_keys):
                keys.append(
                    _require_str(
                        raw_instance_key,
                        f"expected_workload.agent_instance_keys[{idx}]",
                    )
                )
    return _dedup_str_list(keys)


def _selection_has_attached_current_deployment_groups(
    *,
    controller_url: str,
    deployconf: dict[str, Any],
    selection_names: list[str],
    ctx: str,
) -> bool:
    matched_groups = _current_deployment_groups_matching_workloads(
        controller_url=controller_url,
        workload_keys=_selection_workload_keys(
            deployconf=deployconf,
            selection_names=selection_names,
        ),
        ctx=ctx,
    )
    attached_apply_ids = [
        _require_str(group.get("apply_id"), "matched_group.apply_id")
        for group in matched_groups
        if _require_str(group.get("runtime_goal"), "matched_group.runtime_goal") == "ATTACHED"
    ]
    if attached_apply_ids:
        print(
            "[startbare.current_deployments] matched attached groups still need delete: "
            f"ctx={ctx} apply_ids={attached_apply_ids}",
            flush=True,
        )
        return True
    if matched_groups:
        print(
            "[startbare.current_deployments] matched groups are already detached; skip delete handover wait: "
            f"ctx={ctx} apply_ids={[group['apply_id'] for group in matched_groups]}",
            flush=True,
        )
    else:
        print(
            f"[startbare.current_deployments] no matched groups; skip delete handover wait: ctx={ctx}",
            flush=True,
        )
    return False


def _current_deployment_groups_matching_workloads(
    *,
    controller_url: str,
    workload_keys: list[str],
    ctx: str,
) -> list[dict[str, Any]]:
    workload_key_set = set(workload_keys)
    if not workload_key_set:
        return []
    current_deployments = _http_json_retry_until_deadline(
        _controller_endpoint(controller_url, "/api/current_deployments"),
        method="GET",
        deadline=time.time() + 30.0,
        ctx=f"{ctx} current_deployments",
    )
    raw_groups = current_deployments.get("groups")
    if not isinstance(raw_groups, list):
        raise ValueError("current_deployments.groups must be a list")
    matched_groups: list[dict[str, Any]] = []
    for idx, raw_group in enumerate(raw_groups):
        if not isinstance(raw_group, dict):
            raise ValueError(f"current_deployments.groups[{idx}] must be a mapping")
        group = raw_group
        observed_workload_keys = _current_deployment_group_workload_keys(group=group, idx=idx)
        matched_workload_keys = sorted(observed_workload_keys & workload_key_set)
        if not matched_workload_keys:
            continue
        matched_groups.append(
            {
                "idx": idx,
                "apply_id": _require_str(group.get("apply_id"), f"current_deployments.groups[{idx}].apply_id"),
                "runtime_goal": _require_str(group.get("runtime_goal"), f"current_deployments.groups[{idx}].runtime_goal"),
                "phase": _require_str(group.get("phase"), f"current_deployments.groups[{idx}].phase"),
                "matched_workload_keys": matched_workload_keys,
                "observed_workload_keys": sorted(observed_workload_keys),
            }
        )
    return matched_groups


def _delete_current_deployment_groups_with_workloads(
    *,
    controller_url: str,
    workload_keys: list[str],
    ctx: str,
    wait_for_stop: bool,
) -> list[str]:
    deleted_apply_ids: list[str] = []
    for group in _current_deployment_groups_matching_workloads(
        controller_url=controller_url,
        workload_keys=workload_keys,
        ctx=ctx,
    ):
        idx = _require_int(group.get("idx"), "matched_group.idx", min_value=0)
        apply_id = _require_str(group.get("apply_id"), "matched_group.apply_id")
        matched_workload_keys = _require_list_of_str(
            group.get("matched_workload_keys"),
            "matched_group.matched_workload_keys",
        )
        observed_workload_keys = _require_list_of_str(
            group.get("observed_workload_keys"),
            "matched_group.observed_workload_keys",
        )
        print(
            f"[startbare.delete_current_deployment_group] ctx={ctx} apply_id={apply_id} matched_workloads={matched_workload_keys} "
            f"group_workloads={observed_workload_keys}",
            flush=True,
        )
        if wait_for_stop:
            _delete_apply_id(
                controller_url=controller_url,
                apply_id=apply_id,
                ctx=f"{ctx} groups[{idx}]",
            )
        else:
            _delete_apply_id_no_wait(
                controller_url=controller_url,
                apply_id=apply_id,
                ctx=f"{ctx} groups[{idx}]",
            )
        deleted_apply_ids.append(apply_id)
    return deleted_apply_ids


def _delete_selection_current_deployment_groups(
    *,
    controller_url: str,
    deployconf: dict[str, Any],
    selection_names: list[str],
    ctx: str,
) -> list[str]:
    return _delete_current_deployment_groups_with_workloads(
        controller_url=controller_url,
        workload_keys=_selection_workload_keys(
            deployconf=deployconf,
            selection_names=selection_names,
        ),
        ctx=ctx,
        wait_for_stop=True,
    )


def _delete_selection_current_deployment_groups_no_wait(
    *,
    controller_url: str,
    deployconf: dict[str, Any],
    selection_names: list[str],
    ctx: str,
) -> list[str]:
    return _delete_current_deployment_groups_with_workloads(
        controller_url=controller_url,
        workload_keys=_selection_workload_keys(
            deployconf=deployconf,
            selection_names=selection_names,
        ),
        ctx=ctx,
        wait_for_stop=False,
    )


def _current_deployment_group_workload_keys(*, group: dict[str, Any], idx: int) -> set[str]:
    workloads = _require_list(group.get("workloads"), f"current_deployments.groups[{idx}].workloads")
    observed_workload_keys: set[str] = set()
    for work_idx, raw_workload in enumerate(workloads):
        if not isinstance(raw_workload, dict):
            raise ValueError(f"current_deployments.groups[{idx}].workloads[{work_idx}] must be a mapping")
        workload = raw_workload
        kind = _require_str(workload.get("kind"), f"current_deployments.groups[{idx}].workloads[{work_idx}].kind")
        name = _require_str(workload.get("name"), f"current_deployments.groups[{idx}].workloads[{work_idx}].name")
        observed_workload_keys.add(f"{kind}/{name}")
    return observed_workload_keys

def _validate_release_generation_prerequisites(
    *,
    deployconf: dict[str, Any],
    local_node_cfg: dict[str, Any],
) -> None:
    global_envs = deployconf.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")

    required_release_files = [
        "install.py",
        _require_str(global_envs.get("FLUXON_RELEASE_SHA256_FILE"), "global_envs.FLUXON_RELEASE_SHA256_FILE"),
        _require_str(global_envs.get("FLUXON_RELEASE_PYLIB_SRC_TAR"), "global_envs.FLUXON_RELEASE_PYLIB_SRC_TAR"),
        _require_str(global_envs.get("FLUXON_RELEASE_WHEEL"), "global_envs.FLUXON_RELEASE_WHEEL"),
    ]
    local_hostworkdir = _require_str(local_node_cfg.get("hostworkdir"), "local_node_cfg.hostworkdir")
    local_release_dir = Path(local_hostworkdir) / "fluxon_release"
    for filename in required_release_files:
        path = local_release_dir / filename
        if not path.exists():
            raise ValueError(f"Missing required local release artifact: {path}")


def _validate_bare_bootstrap_prerequisites(
    *,
    deployconf: dict[str, Any],
    cluster_nodes: dict[str, dict[str, Any]],
    local_node_cfg: dict[str, Any],
    fixed_bootstrap_batches: list[dict[str, Any]],
    coverage_bootstrap_services: list[str],
) -> None:
    local_hostworkdir = _require_str(local_node_cfg.get("hostworkdir"), "local_node_cfg.hostworkdir")
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    local_release_dir = Path(local_hostworkdir) / "fluxon_release"
    local_etcdctl = local_release_dir / "ext_images" / "etcd" / "etcdctl"
    if not local_etcdctl.exists():
        raise ValueError(f"Missing required local etcdctl for test bed checks: {local_etcdctl}")
    bare_target_services = _collect_bare_target_services(
        deployconf=deployconf,
        fixed_bootstrap_batches=fixed_bootstrap_batches,
        coverage_bootstrap_services=coverage_bootstrap_services,
    )
    for node_name, node_cfg in cluster_nodes.items():
        if not _cluster_node_is_local(node_cfg):
            continue
        hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
        services = sorted(bare_target_services.get(node_name, set()))
        for service_name in services:
            for prefix in ("start", "stop"):
                script_path = Path(hostworkdir) / "gen_bare_deploy_bash" / f"{prefix}_{service_name}.sh"
                if not script_path.exists():
                    raise ValueError(f"Missing required local bare script: {script_path}")


def _validate_ops_agent_snapshot_prerequisites(
    *,
    deployconf: dict[str, Any],
    cluster_nodes: dict[str, dict[str, Any]],
    local_node_cfg: dict[str, Any],
    fixed_bootstrap_batches: list[dict[str, Any]],
    coverage_bootstrap_services: list[str],
) -> None:
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    bare_target_services = _collect_bare_target_services(
        deployconf=deployconf,
        fixed_bootstrap_batches=fixed_bootstrap_batches,
        coverage_bootstrap_services=coverage_bootstrap_services,
    )
    validated_targets: set[tuple[str, str]] = set()
    for node_name, selection_names in bare_target_services.items():
        node_cfg = cluster_nodes.get(node_name)
        if node_cfg is None:
            raise ValueError(f"missing cluster node config for ops_agent snapshot validation: {node_name}")
        for selection_name in selection_names:
            for service_name in _selection_service_names_for_target_node(
                deployconf=deployconf,
                selection_name=selection_name,
                node_name=node_name,
            ):
                if service_name != "ops_agent":
                    continue
                target_key = (node_name, service_name)
                if target_key in validated_targets:
                    continue
                validated_targets.add(target_key)
                hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
                snapshot_path = _ops_agent_desired_snapshot_path(hostworkdir=hostworkdir, node_name=node_name)
                if _cluster_node_is_local(node_cfg):
                    raw_bytes = _read_local_file_bytes_if_exists(snapshot_path)
                else:
                    raw_bytes = _read_remote_file_bytes_if_exists(
                        node_name=node_name,
                        node_cfg=node_cfg,
                        path=snapshot_path,
                    )
                if raw_bytes is None:
                    continue
                _validate_ops_agent_snapshot_payload(
                    snapshot_path=snapshot_path,
                    node_name=node_name,
                    raw_bytes=raw_bytes,
                )


def _ops_agent_desired_snapshot_path(*, hostworkdir: str, node_name: str) -> Path:
    return Path(hostworkdir) / "ops_agent" / node_name / OPS_AGENT_DESIRED_SNAPSHOT_FILENAME


def _read_local_file_bytes_if_exists(path: Path) -> bytes | None:
    if not path.exists():
        return None
    return path.read_bytes()


def _read_remote_file_bytes_if_exists(*, node_name: str, node_cfg: dict[str, Any], path: Path) -> bytes | None:
    remote_cmd = "\n".join(
        [
            "python3 - <<'PY'",
            "import base64",
            "from pathlib import Path",
            f"path = Path({json.dumps(str(path))})",
            "if not path.exists():",
            f"    print({json.dumps(OPS_AGENT_DESIRED_SNAPSHOT_MISSING_SENTINEL)})",
            "else:",
            "    print(base64.b64encode(path.read_bytes()).decode('ascii'))",
            "PY",
        ]
    )
    output = _run_remote_bash_output(
        node_name=node_name,
        node_cfg=node_cfg,
        remote_cmd=remote_cmd,
    ).strip()
    if output == OPS_AGENT_DESIRED_SNAPSHOT_MISSING_SENTINEL:
        return None
    try:
        return base64.b64decode(output.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
        raise ValueError(
            "decode remote ops agent desired snapshot bytes failed: "
            f"node={node_name} path={path} output={output!r}"
        ) from exc


def _validate_ops_agent_snapshot_payload(*, snapshot_path: Path, node_name: str, raw_bytes: bytes) -> None:
    if not raw_bytes:
        raise ValueError(
            "ops agent desired snapshot exists but is empty; remove the file instead of truncating it "
            f"to an empty file: node={node_name} path={snapshot_path}"
        )
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "ops agent desired snapshot must be valid UTF-8 JSON; remove the file instead of truncating "
            f"it to an empty file if you want a clean state: node={node_name} path={snapshot_path}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError(
            "ops agent desired snapshot root must be a mapping: "
            f"node={node_name} path={snapshot_path}"
        )
    expected_instance_key = _ops_agent_instance_key(node_name)
    instance_key = payload.get("instance_key")
    if instance_key != expected_instance_key:
        raise ValueError(
            "ops agent desired snapshot instance_key mismatch: "
            f"node={node_name} path={snapshot_path} expected={expected_instance_key!r} actual={instance_key!r}"
        )
    desired_keys = payload.get("desired_keys")
    if not isinstance(desired_keys, list):
        raise ValueError(
            "ops agent desired snapshot desired_keys must be a list: "
            f"node={node_name} path={snapshot_path}"
        )
    workloads = payload.get("workloads")
    if not isinstance(workloads, list):
        raise ValueError(
            "ops agent desired snapshot workloads must be a list: "
            f"node={node_name} path={snapshot_path}"
        )
    delete_workloads = payload.get("delete_workloads")
    if delete_workloads is not None and not isinstance(delete_workloads, list):
        raise ValueError(
            "ops agent desired snapshot delete_workloads must be a list when present: "
            f"node={node_name} path={snapshot_path}"
        )


def _collect_bare_target_services(
    *,
    deployconf: dict[str, Any],
    fixed_bootstrap_batches: list[dict[str, Any]],
    coverage_bootstrap_services: list[str],
) -> dict[str, set[str]]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    out: dict[str, set[str]] = {}
    for batch in fixed_bootstrap_batches:
        out.setdefault(batch["node"], set()).update(batch["services"])
    for service_name in coverage_bootstrap_services:
        for node_name in _resolve_service_nodes(services=services, service_name=service_name):
            out.setdefault(node_name, set()).add(
                _resolve_coverage_selection_name(
                    deployconf=deployconf,
                    service_name=service_name,
                    node_name=node_name,
                )
            )
    return out


def _run_local_stop(*, local_node_cfg: dict[str, Any], service_name: str) -> None:
    hostworkdir = _require_str(local_node_cfg.get("hostworkdir"), "local_node_cfg.hostworkdir")
    script_path = Path(hostworkdir) / "gen_bare_deploy_bash" / f"stop_{service_name}.sh"
    if not script_path.exists():
        raise ValueError(f"Missing local stop script: {script_path}")
    print(f"[startbare.stop_local] service={service_name} script={script_path}")
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    env = os.environ.copy()
    env["NODE_ID"] = local_node_name
    subprocess.check_call([str(script_path)], cwd=str(REPO_ROOT), env=env)


def _run_remote_stop(*, node_name: str, node_cfg: dict[str, Any], service_name: str) -> None:
    hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
    script_path = hostworkdir + f"/gen_bare_deploy_bash/stop_{service_name}.sh"
    remote_cmd = (
        "NODE_ID="
        + manual_dispatch_release.sh_quote(node_name)
        + " bash "
        + manual_dispatch_release.sh_quote(script_path)
    )
    print(f"[startbare.stop_remote] node={node_name} service={service_name} mode=generated_bare_script")
    _run_remote_bash(node_name=node_name, node_cfg=node_cfg, remote_cmd=remote_cmd)


def _selection_service_names(*, deployconf: dict[str, Any], selection_name: str) -> list[str]:
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    group_cfg = atomic_groups.get(selection_name)
    if isinstance(group_cfg, dict):
        return _require_list_of_str(group_cfg.get("services"), f"atomic_groups.{selection_name}.services")
    return [_resolve_selection_service_name(deployconf=deployconf, selection_name=selection_name)]


def _selection_service_names_for_target_node(
    *,
    deployconf: dict[str, Any],
    selection_name: str,
    node_name: str,
) -> list[str]:
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise ValueError("deployconf.service must be a mapping")
    bind_nodes = _resolve_selection_target_nodes(deployconf=deployconf, selection_name=selection_name)
    if node_name not in bind_nodes:
        raise ValueError(
            "selection is not bound to requested target node: "
            f"selection_name={selection_name} node_name={node_name} bind_nodes={bind_nodes}"
        )
    service_names = _selection_service_names(deployconf=deployconf, selection_name=selection_name)
    out: list[str] = []
    for service_name in service_names:
        if node_name in _resolve_service_nodes(services=services, service_name=service_name):
            out.append(service_name)
    out = _dedup_str_list(out)
    if not out:
        raise ValueError(
            "selection resolved to zero node-bound services; check deployconf atomic_groups/services: "
            f"selection_name={selection_name} node_name={node_name} service_names={service_names}"
        )
    return out


def _selection_bare_script_name(*, deployconf: dict[str, Any], selection_name: str) -> str:
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    if selection_name in atomic_groups:
        return selection_name
    return _resolve_selection_service_name(deployconf=deployconf, selection_name=selection_name)


def _bare_plain_selection_supervisor_identity(
    *,
    deployconf: dict[str, Any],
    hostworkdir: str,
    service_name: str,
) -> tuple[str, str]:
    name_prefix = _require_str(deployconf.get("name_prefix"), "deployconf.name_prefix")
    workload_name = _selection_plain_workload_name(
        name_prefix=name_prefix,
        selection_name=service_name,
    )
    pidfile = _selection_daemonset_supervisor_pidfile_path(
        hostworkdir=hostworkdir,
        workload_name=workload_name,
    )
    label = _selection_daemonset_supervisor_label(workload_name=workload_name)
    return pidfile, label


def _bare_selection_supervisor_identity(
    *,
    deployconf: dict[str, Any],
    hostworkdir: str,
    selection_name: str,
    service_name: str,
) -> tuple[str, str]:
    atomic_groups = deployconf.get("atomic_groups")
    if not isinstance(atomic_groups, dict):
        raise ValueError("deployconf.atomic_groups must be a mapping")
    if selection_name in atomic_groups:
        name_prefix = _require_str(deployconf.get("name_prefix"), "deployconf.name_prefix")
        workload_name = _selection_atomic_group_member_selection_workload_name(
            name_prefix=name_prefix,
            selection_name=selection_name,
            service_name=service_name,
        )
        pidfile = _selection_daemonset_supervisor_pidfile_path(
            hostworkdir=hostworkdir,
            workload_name=workload_name,
        )
        label = _selection_daemonset_supervisor_label(workload_name=workload_name)
        return pidfile, label
    return _bare_plain_selection_supervisor_identity(
        deployconf=deployconf,
        hostworkdir=hostworkdir,
        service_name=selection_name,
    )


def _direct_selection_supervisor_status_is_unsupported(*, ctx: str) -> None:
    raise RuntimeError(
        "direct selection_supervisor status inspection is no longer supported: "
        f"ctx={ctx}. Use generated bare start/stop scripts plus ops/controller APIs instead."
    )


def _selection_supervisor_running(*, status: dict[str, Any], ctx: str) -> bool:
    _ = status
    _direct_selection_supervisor_status_is_unsupported(ctx=ctx)


def _sanitize_bare_log_component(value: str) -> str:
    out_chars: list[str] = []
    for ch in value:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out_chars.append(ch)
            continue
        out_chars.append("_")
    return "".join(out_chars)


def _bare_wave_bootstrap_log_path(
    *,
    workdir: Path,
    wave_idx: int,
    launch_idx: int,
    node_name: str,
    selection_name: str,
) -> Path:
    wave_dir = workdir / "bare_start_logs" / f"wave_{wave_idx + 1:02d}"
    wave_dir.mkdir(parents=True, exist_ok=True)
    filename = (
        f"{launch_idx + 1:02d}"
        + "__"
        + _sanitize_bare_log_component(node_name)
        + "__"
        + _sanitize_bare_log_component(selection_name)
        + ".log"
    )
    return wave_dir / filename


def _new_bare_launch_result(
    *,
    mode: str,
    node_name: str,
    selection_name: str,
    bare_script_name: str,
    bootstrap_log_path: Path,
    expected_service_names: list[str],
) -> dict[str, Any]:
    return {
        "mode": mode,
        "node_name": node_name,
        "selection_name": selection_name,
        "bare_script_name": bare_script_name,
        "bootstrap_log_path": bootstrap_log_path,
        "expected_service_names": expected_service_names,
        "process": None,
        "log_handle": None,
        "askpass_tempdir": None,
        "launch_error": None,
        "launcher_rc": None,
        "runtime_statuses": [],
    }


def _spawn_local_start(
    *,
    local_node_cfg: dict[str, Any],
    selection_name: str,
    bare_script_name: str,
    bootstrap_log_path: Path,
    expected_service_names: list[str],
    allow_already_present: bool,
) -> dict[str, Any]:
    result = _new_bare_launch_result(
        mode="local",
        node_name=_require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname"),
        selection_name=selection_name,
        bare_script_name=bare_script_name,
        bootstrap_log_path=bootstrap_log_path,
        expected_service_names=expected_service_names,
    )
    log_handle = bootstrap_log_path.open("w", encoding="utf-8")
    result["log_handle"] = log_handle
    try:
        local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
        hostworkdir = _require_str(local_node_cfg.get("hostworkdir"), "local_node_cfg.hostworkdir")
        script_path = Path(hostworkdir) / "gen_bare_deploy_bash" / f"start_{bare_script_name}.sh"
        if not script_path.exists():
            raise ValueError(f"Missing local start script: {script_path}")
        print(
            "[startbare.start_local] "
            f"selection={selection_name} script={script_path} bootstrap_log={bootstrap_log_path}"
        )
        env = os.environ.copy()
        env["NODE_ID"] = local_node_name
        if allow_already_present:
            env["FLUXON_BARE_ALLOW_ALREADY_PRESENT"] = "true"
        result["process"] = subprocess.Popen(
            [str(script_path)],
            cwd=str(REPO_ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        return result
    except Exception as err:
        result["launch_error"] = str(err)
        log_handle.write(
            "[startbare.start_local] "
            f"selection={selection_name} script={bare_script_name} spawn_failed={err}\n"
        )
        log_handle.flush()
        return result


def _spawn_remote_start(
    *,
    node_name: str,
    node_cfg: dict[str, Any],
    selection_name: str,
    bare_script_name: str,
    bootstrap_log_path: Path,
    expected_service_names: list[str],
    allow_already_present: bool,
) -> dict[str, Any]:
    result = _new_bare_launch_result(
        mode="remote",
        node_name=node_name,
        selection_name=selection_name,
        bare_script_name=bare_script_name,
        bootstrap_log_path=bootstrap_log_path,
        expected_service_names=expected_service_names,
    )
    log_handle = bootstrap_log_path.open("w", encoding="utf-8")
    result["log_handle"] = log_handle
    try:
        hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
        script_path = hostworkdir + f"/gen_bare_deploy_bash/start_{bare_script_name}.sh"
        remote_cmd = (
            "NODE_ID="
            + manual_dispatch_release.sh_quote(node_name)
            + (" FLUXON_BARE_ALLOW_ALREADY_PRESENT=true" if allow_already_present else "")
            + " bash "
            + manual_dispatch_release.sh_quote(script_path)
        )
        ssh_password, ssh_cmd = _build_remote_ssh_cmd(
            node_name=node_name,
            node_cfg=node_cfg,
            remote_cmd=remote_cmd,
        )
        argv = ["bash", "-lc", ssh_cmd]
        env = None
        if ssh_password is not None:
            askpass_tempdir, askpass_path = manual_dispatch_release._write_askpass_script(password=ssh_password)
            env = os.environ.copy()
            env["SSH_ASKPASS"] = str(askpass_path)
            env["SSH_ASKPASS_REQUIRE"] = "force"
            env["DISPLAY"] = "fluxon:0"
            result["askpass_tempdir"] = askpass_tempdir
        print(
            "[startbare.start_remote] "
            f"node={node_name} selection={selection_name} mode=generated_bare_script "
            f"bootstrap_log={bootstrap_log_path}"
        )
        result["process"] = subprocess.Popen(
            argv,
            cwd=str(REPO_ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        return result
    except Exception as err:
        result["launch_error"] = str(err)
        log_handle.write(
            "[startbare.start_remote] "
            f"node={node_name} selection={selection_name} script={bare_script_name} spawn_failed={err}\n"
        )
        log_handle.flush()
        return result


def _join_bare_launch(result: dict[str, Any]) -> None:
    process = result.get("process")
    if process is not None:
        result["launcher_rc"] = process.wait()
    log_handle = result.get("log_handle")
    if log_handle is not None:
        log_handle.flush()
        log_handle.close()
        result["log_handle"] = None
    askpass_tempdir = result.get("askpass_tempdir")
    if askpass_tempdir is not None:
        askpass_tempdir.cleanup()
        result["askpass_tempdir"] = None


def _run_local_start(*, local_node_cfg: dict[str, Any], service_name: str) -> None:
    hostworkdir = _require_str(local_node_cfg.get("hostworkdir"), "local_node_cfg.hostworkdir")
    script_path = Path(hostworkdir) / "gen_bare_deploy_bash" / f"start_{service_name}.sh"
    if not script_path.exists():
        raise ValueError(f"Missing local start script: {script_path}")
    print(f"[startbare.start_local] service={service_name} script={script_path}")
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    env = os.environ.copy()
    env["NODE_ID"] = local_node_name
    subprocess.check_call([str(script_path)], cwd=str(REPO_ROOT), env=env)


def _run_remote_start(
    *,
    node_name: str,
    node_cfg: dict[str, Any],
    service_name: str,
) -> None:
    hostworkdir = _require_str(node_cfg.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
    script_path = hostworkdir + f"/gen_bare_deploy_bash/start_{service_name}.sh"
    remote_cmd = (
        "NODE_ID="
        + manual_dispatch_release.sh_quote(node_name)
        + " bash "
        + manual_dispatch_release.sh_quote(script_path)
    )
    print(f"[startbare.start_remote] node={node_name} service={service_name} mode=generated_bare_script")
    _run_remote_bash(node_name=node_name, node_cfg=node_cfg, remote_cmd=remote_cmd)


def _parse_selection_supervisor_status_output(*, raw_output: str, ctx: str) -> dict[str, Any]:
    _ = raw_output
    _direct_selection_supervisor_status_is_unsupported(ctx=ctx)


def _selection_supervisor_present(*, status: dict[str, Any], ctx: str) -> bool:
    _ = status
    _direct_selection_supervisor_status_is_unsupported(ctx=ctx)


def _build_remote_ssh_cmd(*, node_name: str, node_cfg: dict[str, Any], remote_cmd: str) -> tuple[str | None, str]:
    ssh_user = _require_str(node_cfg.get("ssh_user"), f"cluster_nodes[{node_name}].ssh_user")
    ssh_port = _require_int(node_cfg.get("ssh_port"), f"cluster_nodes[{node_name}].ssh_port", min_value=1)
    ssh_password_raw = node_cfg.get("ssh_password")
    ssh_password = None if ssh_password_raw is None else _require_str(
        ssh_password_raw,
        f"cluster_nodes[{node_name}].ssh_password",
    )
    ssh_host = _cluster_node_ssh_host(node_cfg, node_name=node_name)
    ssh_cmd = (
        "ssh -p "
        + manual_dispatch_release.sh_quote(str(ssh_port))
        + " "
        + manual_dispatch_release.sh_quote(f"{ssh_user}@{ssh_host}")
        + " "
        + manual_dispatch_release.sh_quote(remote_cmd)
    )
    return ssh_password, ssh_cmd


def _run_remote_bash(*, node_name: str, node_cfg: dict[str, Any], remote_cmd: str) -> None:
    ssh_password, ssh_cmd = _build_remote_ssh_cmd(
        node_name=node_name,
        node_cfg=node_cfg,
        remote_cmd=remote_cmd,
    )
    manual_dispatch_release._check_call_bash_with_optional_password(password=ssh_password, cmd=ssh_cmd)


def _run_remote_bash_output(*, node_name: str, node_cfg: dict[str, Any], remote_cmd: str) -> str:
    ssh_password, ssh_cmd = _build_remote_ssh_cmd(
        node_name=node_name,
        node_cfg=node_cfg,
        remote_cmd=remote_cmd,
    )
    return manual_dispatch_release._check_output_bash_with_optional_password(
        password=ssh_password,
        cmd=ssh_cmd,
    )


def _local_bare_service_present(
    *,
    deployconf: dict[str, Any],
    local_node_cfg: dict[str, Any],
    service_name: str,
) -> bool:
    _ = deployconf, local_node_cfg
    _direct_selection_supervisor_status_is_unsupported(ctx=f"local bare status service={service_name}")


def _local_bare_service_status(
    *,
    deployconf: dict[str, Any],
    local_node_cfg: dict[str, Any],
    selection_name: str,
    service_name: str,
) -> dict[str, Any]:
    _ = deployconf, local_node_cfg
    _direct_selection_supervisor_status_is_unsupported(
        ctx=f"local bare status selection={selection_name} service={service_name}"
    )


def _remote_bare_service_present(
    *,
    deployconf: dict[str, Any],
    node_name: str,
    node_cfg: dict[str, Any],
    service_name: str,
) -> bool:
    _ = deployconf, node_cfg
    _direct_selection_supervisor_status_is_unsupported(
        ctx=f"remote bare status node={node_name} service={service_name}"
    )


def _remote_bare_service_status(
    *,
    deployconf: dict[str, Any],
    node_name: str,
    node_cfg: dict[str, Any],
    selection_name: str,
    service_name: str,
) -> dict[str, Any]:
    _ = deployconf, node_cfg
    _direct_selection_supervisor_status_is_unsupported(
        ctx=f"remote bare status node={node_name} selection={selection_name} service={service_name}"
    )


def _collect_bare_runtime_statuses(
    *,
    deployconf: dict[str, Any],
    cluster_nodes: dict[str, dict[str, Any]],
    local_node_cfg: dict[str, Any],
    result: dict[str, Any],
) -> list[dict[str, Any]]:
    expected_service_names = _require_list_of_str(
        result.get("expected_service_names"),
        "bare_launch_result.expected_service_names",
    )
    confirmed_ready, ready_source, ready_error = _bare_launch_ready_summary(result=result)
    bootstrap_log_path = result.get("bootstrap_log_path")
    if not isinstance(bootstrap_log_path, Path):
        raise ValueError("bare_launch_result.bootstrap_log_path must be a Path")
    statuses: list[dict[str, Any]] = []
    for service_name in expected_service_names:
        runtime_log_path = _resolve_bare_service_readable_runtime_log_path(
            local_node_cfg=local_node_cfg,
            service_name=service_name,
        )
        status_error = None if confirmed_ready else ready_error
        if not confirmed_ready:
            detail_parts: list[str] = []
            if isinstance(ready_error, str) and ready_error:
                detail_parts.append(ready_error)
            bootstrap_tail = _read_text_tail(bootstrap_log_path)
            if bootstrap_tail:
                detail_parts.append(f"bootstrap_log_tail={bootstrap_tail!r}")
            if runtime_log_path is not None:
                runtime_tail = _read_text_tail(runtime_log_path)
                if runtime_tail:
                    detail_parts.append(f"service_log_tail={runtime_tail!r}")
            status_error = " | ".join(detail_parts) if detail_parts else ready_error
        statuses.append({
            "service_name": service_name,
            "present": confirmed_ready,
            "running": confirmed_ready,
            "log_path": str(runtime_log_path if runtime_log_path is not None else bootstrap_log_path),
            "status_source": ready_source,
            "status_error": status_error,
        })
    return statuses


def _bare_launch_ready_markers(*, result: dict[str, Any]) -> list[str]:
    selection_name = _require_str(result.get("selection_name"), "bare_launch_result.selection_name")
    bare_script_name = _require_str(result.get("bare_script_name"), "bare_launch_result.bare_script_name")
    node_name = _require_str(result.get("node_name"), "bare_launch_result.node_name")
    expected_service_names = _require_list_of_str(
        result.get("expected_service_names"),
        "bare_launch_result.expected_service_names",
    )
    if len(expected_service_names) > 1:
        return [f"[atomic-group] ready group={selection_name} node={node_name}"]
    service_name = expected_service_names[0] if expected_service_names else bare_script_name
    return [f"Started {service_name} (label:"]


def _read_bootstrap_log_text(*, bootstrap_log_path: Path) -> str:
    if not bootstrap_log_path.exists():
        return ""
    return bootstrap_log_path.read_text(encoding="utf-8", errors="replace")


def _bare_launch_ready_summary(result: dict[str, Any]) -> tuple[bool, str, str | None]:
    launch_error = result.get("launch_error")
    if isinstance(launch_error, str) and launch_error:
        return False, "launch_error", launch_error
    launcher_rc = result.get("launcher_rc")
    if isinstance(launcher_rc, int) and launcher_rc == 0:
        return True, "launcher_rc", None
    bootstrap_log_path = result.get("bootstrap_log_path")
    if not isinstance(bootstrap_log_path, Path):
        raise ValueError("bare_launch_result.bootstrap_log_path must be a Path")
    markers = _bare_launch_ready_markers(result=result)
    missing_markers = markers
    for _ in range(3):
        log_text = _read_bootstrap_log_text(bootstrap_log_path=bootstrap_log_path)
        missing_markers = [marker for marker in markers if marker not in log_text]
        if not missing_markers:
            return True, "bootstrap_log_ready", None
        time.sleep(1.0)
    launcher_rc_text = str(launcher_rc) if launcher_rc is not None else "<not-started>"
    return (
        False,
        "bootstrap_log_ready",
        "launcher did not report success and bootstrap log never reached ready marker(s): "
        f"rc={launcher_rc_text} missing_markers={missing_markers}",
    )


def _bare_launch_failed(result: dict[str, Any]) -> bool:
    confirmed_ready, _, _ = _bare_launch_ready_summary(result=result)
    return not confirmed_ready


def _print_bare_wave_summary(*, wave_idx: int, results: list[dict[str, Any]]) -> None:
    print(
        "[startbare.wave_summary] "
        f"wave={wave_idx + 1} launched={len(results)}"
    )
    for result in results:
        launch_ok = result.get("launch_error") is None and result.get("launcher_rc") == 0
        overall_state = "ok" if not _bare_launch_failed(result) else "failed"
        launcher_rc = result.get("launcher_rc")
        launcher_rc_str = str(launcher_rc) if launcher_rc is not None else "<not-started>"
        print(
            "  - "
            f"result={overall_state} mode={result['mode']} node={result['node_name']} "
            f"selection={result['selection_name']} script={result['bare_script_name']} "
            f"launcher={'ok' if launch_ok else 'failed'} rc={launcher_rc_str} "
            f"bootstrap_log={result['bootstrap_log_path']}"
        )
        launch_error = result.get("launch_error")
        if isinstance(launch_error, str) and launch_error:
            print(f"    launch_error={launch_error}")
        runtime_statuses = _require_list(result.get("runtime_statuses"), "bare_launch_result.runtime_statuses")
        for runtime_status_raw in runtime_statuses:
            runtime_status = _require_mapping(runtime_status_raw, "bare_launch_result.runtime_statuses[]")
            runtime_log = runtime_status.get("log_path")
            runtime_log_text = runtime_log if isinstance(runtime_log, str) and runtime_log else "<unknown>"
            status_source = runtime_status.get("status_source")
            status_source_text = status_source if isinstance(status_source, str) and status_source else "<unknown>"
            print(
                "    * "
                f"service={_require_str(runtime_status.get('service_name'), 'runtime_status.service_name')} "
                f"present={runtime_status.get('present')} "
                f"running={runtime_status.get('running')} "
                f"source={status_source_text} "
                f"runtime_log={runtime_log_text}"
            )
            status_error = runtime_status.get("status_error")
            if isinstance(status_error, str) and status_error:
                print(f"      status_error={status_error}")


def _run_bare_waves(
    *,
    workdir: Path,
    deployconf: dict[str, Any],
    cluster_nodes: dict[str, dict[str, Any]],
    local_node_cfg: dict[str, Any],
    waves: list[dict[str, Any]],
    bootstrap_bare_services: set[str],
) -> None:
    local_node_name = _require_str(local_node_cfg.get("hostname"), "local_node_cfg.hostname")
    for wave_idx, wave in enumerate(waves):
        launch_plans = _require_list(
            wave.get("launches"),
            f"waves[{wave_idx}].launches",
        )
        launched_results: list[dict[str, Any]] = []
        seen_nodes_in_wave: set[str] = set()
        for launch_idx, launch_plan_raw in enumerate(launch_plans):
            launch_plan = _require_mapping(
                launch_plan_raw,
                f"waves[{wave_idx}].launches[{launch_idx}]",
            )
            node_name = _require_str(
                launch_plan.get("node"),
                f"waves[{wave_idx}].launches[{launch_idx}].node",
            )
            selection_name = _require_str(
                launch_plan.get("selection_name"),
                f"waves[{wave_idx}].launches[{launch_idx}].selection_name",
            )
            if node_name in seen_nodes_in_wave:
                raise ValueError(
                    f"bare launch wave {wave_idx} contains duplicate node: {node_name}"
                )
            seen_nodes_in_wave.add(node_name)
            node_cfg = cluster_nodes[node_name]
            bare_script_name = _selection_bare_script_name(
                deployconf=deployconf,
                selection_name=selection_name,
            )
            expected_service_names = _selection_service_names_for_target_node(
                deployconf=deployconf,
                selection_name=selection_name,
                node_name=node_name,
            )
            bootstrap_log_path = _bare_wave_bootstrap_log_path(
                workdir=workdir,
                wave_idx=wave_idx,
                launch_idx=launch_idx,
                node_name=node_name,
                selection_name=selection_name,
            )
            print(
                "[startbare.run_bare_wave] "
                f"mode={'local' if _cluster_node_is_local(node_cfg) else 'remote'} "
                f"node={node_name} selection={selection_name} script={bare_script_name} "
                f"bootstrap_log={bootstrap_log_path}"
            )
            if _cluster_node_is_local(node_cfg):
                allow_already_present = selection_name in bootstrap_bare_services
                launched_results.append(
                    _spawn_local_start(
                        local_node_cfg=node_cfg,
                        selection_name=selection_name,
                        bare_script_name=bare_script_name,
                        bootstrap_log_path=bootstrap_log_path,
                        expected_service_names=expected_service_names,
                        allow_already_present=allow_already_present,
                    )
                )
                continue
            allow_already_present = selection_name in bootstrap_bare_services
            launched_results.append(
                _spawn_remote_start(
                    node_name=node_name,
                    node_cfg=node_cfg,
                    selection_name=selection_name,
                    bare_script_name=bare_script_name,
                    bootstrap_log_path=bootstrap_log_path,
                    expected_service_names=expected_service_names,
                    allow_already_present=allow_already_present,
                )
            )
        if not launched_results:
            print(f"[startbare.wave_summary] wave={wave_idx + 1} launched=0")
            continue
        for result in launched_results:
            _join_bare_launch(result)
        for result in launched_results:
            result["runtime_statuses"] = _collect_bare_runtime_statuses(
                deployconf=deployconf,
                cluster_nodes=cluster_nodes,
                local_node_cfg=local_node_cfg,
                result=result,
            )
        _print_bare_wave_summary(wave_idx=wave_idx, results=launched_results)
        failed_results = [
            result
            for result in launched_results
            if _bare_launch_failed(result)
        ]
        if failed_results:
            failed_selections = ", ".join(
                f"{result['node_name']}:{result['selection_name']}"
                for result in failed_results
            )
            raise RuntimeError(
                f"bare bootstrap wave failed after summary: wave={wave_idx + 1} failed={failed_selections}"
            )


def _controller_endpoint(controller_url: str, suffix: str) -> str:
    return controller_url + suffix


def _new_controller_request(
    url: str,
    *,
    method: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> urllib.request.Request:
    if _CONTROLLER_BASIC_AUTH_HEADER is None:
        raise RuntimeError("controller_basic_auth is not initialized")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header(_CONTROLLER_BASIC_AUTH_HEADER_NAME, _CONTROLLER_BASIC_AUTH_HEADER)
    if data is not None and content_type is not None:
        req.add_header("Content-Type", content_type)
    return req


def _http_json(url: str, *, method: str, data: bytes | None = None) -> dict[str, Any]:
    req = _new_controller_request(
        url,
        method=method,
        data=data,
        content_type="text/yaml; charset=utf-8" if data is not None else None,
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
        return _parse_http_json_response(url=url, response=resp)


def _http_json_with_timeout(
    url: str,
    *,
    method: str,
    timeout_seconds: float,
    data: bytes | None = None,
) -> dict[str, Any]:
    req = _new_controller_request(
        url,
        method=method,
        data=data,
        content_type="text/yaml; charset=utf-8" if data is not None else None,
    )
    with urllib.request.urlopen(req, timeout=float(timeout_seconds)) as resp:
        return _parse_http_json_response(url=url, response=resp)


def _http_json_allow_error_status(
    url: str,
    *,
    method: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> tuple[int, dict[str, Any]]:
    req = _new_controller_request(
        url,
        method=method,
        data=data,
        content_type=content_type,
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as resp:
            return int(resp.status), _parse_http_json_response(url=url, response=resp)
    except urllib.error.HTTPError as err:
        return int(err.code), _parse_http_json_response(url=url, response=err)


def _controller_retry_sleep_or_raise(*, deadline: float, ctx: str, url: str, exc: Exception) -> None:
    if time.time() >= deadline:
        raise ValueError(
            f"{ctx} controller request timed out after retry deadline: "
            f"url={url} err={type(exc).__name__}: {exc}"
        ) from exc
    print(
        "[wait_apply.http_retry] controller request transient error; retrying: "
        f"ctx={ctx} url={url} err={type(exc).__name__}: {exc}",
        flush=True,
    )
    time.sleep(1.0)


def _http_json_retry_until_deadline(
    url: str,
    *,
    method: str,
    deadline: float,
    ctx: str,
    data: bytes | None = None,
) -> dict[str, Any]:
    while True:
        try:
            return _http_json(url, method=method, data=data)
        except urllib.error.HTTPError as exc:
            if int(exc.code) not in CONTROLLER_TRANSIENT_HTTP_CODES:
                raise
            _controller_retry_sleep_or_raise(deadline=deadline, ctx=ctx, url=url, exc=exc)
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, HttpJsonResponseError) as exc:
            _controller_retry_sleep_or_raise(deadline=deadline, ctx=ctx, url=url, exc=exc)


def _http_json_allow_error_status_retry_until_deadline(
    url: str,
    *,
    method: str,
    deadline: float,
    ctx: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> tuple[int, dict[str, Any]]:
    while True:
        try:
            return _http_json_allow_error_status(
                url,
                method=method,
                data=data,
                content_type=content_type,
            )
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, HttpJsonResponseError) as exc:
            _controller_retry_sleep_or_raise(deadline=deadline, ctx=ctx, url=url, exc=exc)


def _parse_http_json_response(*, url: str, response: Any) -> dict[str, Any]:
    payload = response.read()
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HttpJsonResponseError(
            f"http response utf-8 decode failed: url={url} err={exc}"
        ) from exc
    try:
        obj = json.loads(decoded)
    except json.JSONDecodeError as exc:
        snippet = decoded[:200].replace("\n", "\\n")
        raise HttpJsonResponseError(
            f"http response json decode failed: url={url} err={exc} snippet={snippet!r}"
        ) from exc
    if not isinstance(obj, dict):
        raise HttpJsonResponseError(f"http response must be a JSON object: url={url}")
    return obj


def _delete_apply_id(*, controller_url: str, apply_id: str, ctx: str) -> None:
    deadline = time.time() + 120.0
    delete_apply_url = _controller_endpoint(controller_url, "/api/delete_apply")
    while True:
        status_code, resp = _http_json_allow_error_status_retry_until_deadline(
            delete_apply_url,
            method="POST",
            deadline=deadline,
            ctx=f"{ctx} delete_apply",
            data=json.dumps({"apply_id": apply_id}).encode("utf-8"),
            content_type="application/json",
        )
        if status_code == 200:
            break
        if status_code == 409:
            if time.time() >= deadline:
                raise ValueError(f"{ctx} delete_apply timed out waiting for deploy guard: apply_id={apply_id} resp={resp}")
            time.sleep(1.0)
            continue
        raise ValueError(f"{ctx} delete_apply failed: apply_id={apply_id} status={status_code} resp={resp}")

    wait_delete_apply_url = _controller_endpoint(controller_url, "/api/wait_delete_apply")
    while True:
        status_code, resp = _http_json_allow_error_status_retry_until_deadline(
            wait_delete_apply_url,
            method="POST",
            deadline=deadline,
            ctx=f"{ctx} wait_delete_apply",
            data=json.dumps({"apply_id": apply_id}).encode("utf-8"),
            content_type="application/json",
        )
        if status_code == 200:
            return
        if status_code == 409:
            if _resp_contains_any_text(resp, WAIT_DELETE_APPLY_REQUIRES_DELETE_ERR):
                raise ValueError(f"{ctx} wait_delete_apply called before delete_apply converged: apply_id={apply_id} resp={resp}")
            if time.time() >= deadline:
                raise ValueError(f"{ctx} wait_delete_apply timed out waiting for deploy guard: apply_id={apply_id} resp={resp}")
            time.sleep(1.0)
            continue
        if _delete_apply_should_retry(status_code=status_code, resp=resp):
            if time.time() >= deadline:
                raise ValueError(f"{ctx} wait_delete_apply timed out waiting for workload stop: apply_id={apply_id} resp={resp}")
            print(
                "[startbare.delete_apply] wait_delete_apply stop still converging; retrying: "
                f"ctx={ctx} apply_id={apply_id} resp={resp}",
                flush=True,
            )
            time.sleep(1.0)
            continue
        raise ValueError(f"{ctx} wait_delete_apply failed: apply_id={apply_id} status={status_code} resp={resp}")


def _delete_apply_id_no_wait(*, controller_url: str, apply_id: str, ctx: str) -> None:
    deadline = time.time() + 60.0
    delete_apply_url = _controller_endpoint(controller_url, "/api/delete_apply")
    while True:
        status_code, resp = _http_json_allow_error_status_retry_until_deadline(
            delete_apply_url,
            method="POST",
            deadline=deadline,
            ctx=f"{ctx} delete_apply",
            data=json.dumps({"apply_id": apply_id}).encode("utf-8"),
            content_type="application/json",
        )
        if status_code == 200:
            return
        if status_code == 409:
            if time.time() >= deadline:
                raise ValueError(f"{ctx} delete_apply timed out waiting for deploy guard: apply_id={apply_id} resp={resp}")
            time.sleep(1.0)
            continue
        raise ValueError(f"{ctx} delete_apply failed: apply_id={apply_id} status={status_code} resp={resp}")


def _delete_apply_should_retry(*, status_code: int, resp: dict[str, Any]) -> bool:
    if status_code not in (500, 502):
        return False
    return _resp_contains_any_text(resp, DELETE_APPLY_RETRYABLE_ERRS)


def _resp_contains_any_text(obj: Any, needles: tuple[str, ...] | str) -> bool:
    if isinstance(needles, str):
        needle_values = (needles,)
    else:
        needle_values = needles
    if isinstance(obj, str):
        return any(needle in obj for needle in needle_values)
    if isinstance(obj, dict):
        return any(_resp_contains_any_text(value, needle_values) for value in obj.values())
    if isinstance(obj, list):
        return any(_resp_contains_any_text(value, needle_values) for value in obj)
    return False


def _is_controller_initially_reachable(*, controller_url: str) -> bool:
    current_deployments_url = _controller_endpoint(controller_url, "/api/health")
    try:
        _http_json_retry_until_deadline(
            current_deployments_url,
            method="GET",
            deadline=time.time() + INITIAL_CONTROLLER_REACHABILITY_PROBE_TIMEOUT_SECONDS,
            ctx="initial_controller_reachability",
        )
        print(
            "[startbare.initial_controller_probe] controller is reachable at bootstrap start; "
            "run initial delete_current_deployments step",
            flush=True,
        )
        return True
    except (urllib.error.HTTPError, urllib.error.URLError, ConnectionError, TimeoutError, OSError, ValueError) as exc:
        print(
            "[startbare.initial_controller_probe] controller is not reachable at bootstrap start; "
            "skip initial delete_current_deployments and proceed to bare bootstrap: "
            f"url={current_deployments_url} err={type(exc).__name__}: {exc}",
            flush=True,
        )
        return False


def _wait_controller_ready_stable(
    *,
    controller_url: str,
    timeout_seconds: int,
    stability_window_seconds: int,
) -> None:
    deadline = time.time() + float(timeout_seconds)
    stable_since_ts: float | None = None
    last_error = ""
    # Use the lightweight controller liveness endpoint for the bootstrap stability gate.
    # `/api/current_deployments` fan-outs into runtime diagnostics and can exceed the
    # request timeout under large parallel bring-up, even when the controller is already
    # healthy enough to accept deploy/apply requests.
    controller_status_url = _controller_endpoint(controller_url, "/api/health")
    while time.time() < deadline:
        now = time.time()
        try:
            _http_json_retry_until_deadline(
                controller_status_url,
                method="GET",
                deadline=time.time() + 5.0,
                ctx="wait_controller_ready_stable",
            )
            if stable_since_ts is None:
                stable_since_ts = now
                print(
                    "[startbare.wait_controller_stable] controller is reachable; "
                    f"begin stability window seconds={stability_window_seconds}"
                )
            stable_elapsed_seconds = int(now - stable_since_ts)
            if now - stable_since_ts >= float(stability_window_seconds):
                print(
                    "[startbare.wait_controller_stable] ready "
                    f"stable_for_seconds={stable_elapsed_seconds} "
                    f"required_window_seconds={stability_window_seconds}"
                )
                return
            print(
                "[startbare.wait_controller_stable] waiting stability window "
                f"elapsed_seconds={stable_elapsed_seconds} "
                f"required_window_seconds={stability_window_seconds}"
            )
        except Exception as err:
            if stable_since_ts is not None:
                print("[startbare.wait_controller_stable] stability window reset due to controller fetch error")
            stable_since_ts = None
            last_error = f"{type(err).__name__}: {err}"
            print(f"[startbare.wait_controller_stable] pending detail={last_error}")
        time.sleep(1.0)
    raise RuntimeError(
        "controller did not stay reachable for the required stability window: "
        f"timeout_seconds={timeout_seconds} "
        f"stability_window_seconds={stability_window_seconds} "
        f"last_error={last_error}"
    )


def _wait_controller_unreachable(
    *,
    controller_url: str,
    timeout_seconds: int,
) -> None:
    deadline = time.time() + float(timeout_seconds)
    controller_status_url = _controller_endpoint(controller_url, "/api/health")
    while time.time() < deadline:
        try:
            _http_json_with_timeout(
                controller_status_url,
                method="GET",
                timeout_seconds=5.0,
            )
            print("[startbare.wait_controller_unreachable] controller is still reachable")
        except Exception as err:
            print(
                "[startbare.wait_controller_unreachable] controller is unreachable; "
                f"proceed to bare bootstrap err={type(err).__name__}: {err}"
            )
            return
        time.sleep(1.0)
    raise RuntimeError(
        "controller stayed reachable after non-blocking controller apply delete: "
        f"timeout_seconds={timeout_seconds}"
    )


def _wait_controller_reachable(
    *,
    controller_url: str,
    timeout_seconds: int,
) -> None:
    deadline = time.time() + float(timeout_seconds)
    last_error = ""
    controller_status_url = _controller_endpoint(controller_url, "/api/health")
    while time.time() < deadline:
        try:
            _http_json_retry_until_deadline(
                controller_status_url,
                method="GET",
                deadline=time.time() + 5.0,
                ctx="wait_controller_reachable",
            )
            print("[startbare.wait_controller_reachable] controller is reachable")
            return
        except Exception as err:
            last_error = f"{type(err).__name__}: {err}"
            print(f"[startbare.wait_controller_reachable] pending detail={last_error}")
        time.sleep(1.0)
    raise RuntimeError(
        "controller did not become reachable before delete_current_deployments: "
        f"timeout_seconds={timeout_seconds} last_error={last_error}"
    )


def _wait_controller_agents_ready(
    *,
    controller_url: str,
    agent_instance_keys: list[str],
    timeout_seconds: int,
) -> None:
    requested_agent_keys = _dedup_str_list(agent_instance_keys)
    if not requested_agent_keys:
        return

    deadline = time.time() + float(timeout_seconds)
    last_detail = ""
    while time.time() < deadline:
        agents_url = (
            _controller_endpoint(controller_url, "/api/agents")
            + "?"
            + urllib.parse.urlencode(
                [("instance_key", instance_key) for instance_key in requested_agent_keys]
            )
        )
        status_code, resp = _http_json_allow_error_status_retry_until_deadline(
            agents_url,
            method="GET",
            deadline=time.time() + 5.0,
            ctx="wait_controller_agents_ready",
        )
        if status_code not in (200, 502):
            raise RuntimeError(
                "controller agents readiness probe failed: "
                f"status={status_code} resp={resp}"
            )
        raw_agents = resp.get("agents")
        if not isinstance(raw_agents, list):
            raise ValueError("agents response must contain a list field: agents")

        seen: set[str] = set()
        not_ready: list[str] = []
        for idx, raw_agent in enumerate(raw_agents):
            if not isinstance(raw_agent, dict):
                raise ValueError(f"agents[{idx}] must be a mapping")
            instance_key = _require_str(raw_agent.get("instance_key"), f"agents[{idx}].instance_key")
            seen.add(instance_key)
            if raw_agent.get("ok") is True:
                continue
            err = raw_agent.get("err")
            if err is not None and not isinstance(err, str):
                raise ValueError(f"agents[{idx}].err must be a string when present")
            not_ready.append(f"{instance_key}:{err or 'not_ready'}")

        missing = [instance_key for instance_key in requested_agent_keys if instance_key not in seen]
        if not missing and not not_ready:
            print(
                "[startbare.wait_controller_agents_ready] ready "
                f"agent_instance_keys={json.dumps(requested_agent_keys)}"
            )
            return

        detail_parts: list[str] = []
        if missing:
            detail_parts.append(f"missing={json.dumps(missing)}")
        if not_ready:
            detail_parts.append(f"not_ready={json.dumps(not_ready)}")
        last_detail = " ".join(detail_parts) or f"status={status_code} resp={resp}"
        print(f"[startbare.wait_controller_agents_ready] pending detail={last_detail}")
        time.sleep(1.0)

    raise RuntimeError(
        "controller agents did not become ready before the next handover/apply step: "
        f"timeout_seconds={timeout_seconds} last_detail={last_detail}"
    )


def _deploy_controller_payload(*, controller_url: str, yaml_text: str) -> dict[str, Any]:
    deploy_url = _controller_endpoint(controller_url, "/api/deploy")
    deadline = time.time() + DEPLOY_GUARD_WAIT_SECONDS
    payload = yaml_text.encode("utf-8")
    while True:
        status_code, resp = _http_json_allow_error_status_retry_until_deadline(
            deploy_url,
            method="POST",
            deadline=deadline,
            ctx="deploy_controller_payload",
            data=payload,
            content_type="text/yaml; charset=utf-8",
        )
        # `/api/deploy` is asynchronous by contract and may return either immediate
        # success (`200`) or accepted-for-reconcile (`202`) with the apply `history_id`.
        if 200 <= status_code < 300 and resp.get("ok") is True:
            resp["apply_id"] = _require_str(resp.get("history_id"), "deploy_response.history_id")
            return resp
        if (
            (status_code == 409 or status_code == 200)
            and resp.get("err") == DEPLOY_GUARD_ERR
        ):
            if time.time() >= deadline:
                raise ValueError(f"deploy timed out waiting for deploy guard: {resp}")
            print(
                "[startbare.deploy_controller_payload] deploy guard active; retrying: "
                f"resp={resp}",
                flush=True,
            )
            time.sleep(DEPLOY_GUARD_POLL_SECONDS)
            continue
        raise ValueError(f"deploy failed: status={status_code} resp={resp}")


def _wait_apply_id(
    *,
    controller_url: str,
    apply_id: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + float(timeout_seconds)
    apply_wait_url = _controller_endpoint(controller_url, "/api/apply_wait")
    while time.time() < deadline:
        status_code, resp = _http_json_allow_error_status_retry_until_deadline(
            apply_wait_url,
            method="POST",
            deadline=deadline,
            ctx="wait_apply_id",
            data=json.dumps({"apply_id": apply_id}).encode("utf-8"),
            content_type="application/json",
        )
        if status_code == 200:
            print(f"[wait_apply.wait_apply_id] ready apply_id={apply_id}")
            return resp
        if status_code == 502:
            print(f"[wait_apply.wait_apply_id] pending apply_id={apply_id} detail={resp}")
            time.sleep(1.0)
            continue
        raise RuntimeError(f"apply_wait failed: apply_id={apply_id} status={status_code} resp={resp}")
    raise RuntimeError(f"apply_wait timed out after {timeout_seconds}s: apply_id={apply_id}")


def _dedup_str_list(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _require_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_int(value: Any, field_name: str, *, min_value: int) -> int:
    if not isinstance(value, int) or value < min_value:
        raise ValueError(f"{field_name} must be an int >= {min_value}")
    return value


def _require_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _require_basic_auth_username(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if value.strip() != value:
        raise ValueError(f"{field_name} must not have leading/trailing whitespace")
    if ":" in value:
        raise ValueError(f"{field_name} must not contain ':'")
    return value


def _require_basic_auth_password(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if value.strip() != value:
        raise ValueError(f"{field_name} must not have leading/trailing whitespace")
    return value


def _parse_controller_basic_auth(value: Any, *, field_name: str) -> dict[str, str]:
    auth = _require_mapping(value, field_name)
    return {
        "username": _require_basic_auth_username(auth.get("username"), f"{field_name}.username"),
        "password": _require_basic_auth_password(auth.get("password"), f"{field_name}.password"),
    }


def _install_controller_basic_auth(value: Any, *, field_name: str) -> None:
    auth = _parse_controller_basic_auth(value, field_name=field_name)
    raw = f"{auth['username']}:{auth['password']}".encode("utf-8")
    global _CONTROLLER_BASIC_AUTH_HEADER
    _CONTROLLER_BASIC_AUTH_HEADER = "Basic " + base64.b64encode(raw).decode("ascii")


def _require_list_of_str(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list of strings")
    out: list[str] = []
    for idx, raw_item in enumerate(value):
        out.append(_require_str(raw_item, f"{field_name}[{idx}]"))
    return out


if __name__ == "__main__":
    main()
