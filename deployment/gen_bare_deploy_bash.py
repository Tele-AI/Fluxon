#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
UTILS_DIR = SCRIPT_DIR / "utils"
sys.path.insert(0, str(UTILS_DIR))

from placeholder_utils import (  # type: ignore
    build_mapping_for_cfg as _ph_build_mapping,
    resolve_placeholders_nested as _ph_resolve_nested,
    resolve_values_or_raise as _ph_resolve_or_raise,
)
from proc_lifecycle_codegen import (  # type: ignore
    StopTimeouts,
    render_bash_proc_lifecycle_funcs_pid_tree,
)
from log_shard import render_module_source as render_log_shard_module_source  # type: ignore
from selection_supervisor_codegen import (  # type: ignore
    LOG_SHARD_HELPER_FILENAME,
    PYTHON_SELECTION_SUPERVISOR_FILENAME,
    render_python_selection_supervisor_module,
)
from selection_runtime import (  # type: ignore
    atomic_group_member_authority_name as _selection_atomic_group_member_authority_name,
    atomic_group_member_selection_workload_name as _selection_atomic_group_member_selection_workload_name,
    daemonset_selection_supervisor_label as _selection_daemonset_supervisor_label,
    plain_selection_authority_name as _selection_plain_selection_authority_name,
    plain_selection_workload_name as _selection_plain_workload_name,
)


STOP_TIMEOUTS = StopTimeouts(term_seconds=60, kill_seconds=10, supersede_seconds=30)
STANDALONE_BACKOFF_MAX_SECONDS = 30
ATOMIC_GROUP_BACKOFF_MAX_SECONDS = 25
ATOMIC_GROUP_CRASHLOOP_CONSECUTIVE_RESTARTS = 10
ATOMIC_GROUP_CRASHLOOP_INTERVAL_LT_SECONDS = 30
ATOMIC_GROUP_PROBABLE_READY_SECONDS = 10
STANDALONE_PROBABLE_READY_SECONDS = 10
STANDALONE_STARTUP_DEADLINE_SECONDS = 20
ATOMIC_GROUP_STARTUP_DEADLINE_SECONDS = 20
HOSTWORKDIR_RUNTIME_TOKEN = "${HOSTWORKDIR}"
REPO_ROOT = SCRIPT_DIR.parent
BARE_TEMPLATE_DIR = SCRIPT_DIR / "templates" / "gen_bare_deploy_bash"
_TEMPLATE_TOKEN_RE = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


@lru_cache(maxsize=None)
def _load_bare_template(*, template_name: str) -> str:
    template_path = BARE_TEMPLATE_DIR / template_name
    if not template_path.is_file():
        raise RuntimeError(f"missing bare deploy template: {template_path}")
    return template_path.read_text(encoding="utf-8")


def _render_bare_template(*, template_name: str, values: Dict[str, str]) -> str:
    template = _load_bare_template(template_name=template_name)

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise RuntimeError(f"missing bare deploy template value: template={template_name} key={key}")
        value = values[key]
        if not isinstance(value, str):
            raise ValueError(f"bare deploy template value must be a string: template={template_name} key={key}")
        return value

    return _TEMPLATE_TOKEN_RE.sub(_replace, template)


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate bare deploy bash scripts from deployconf")
    parser.add_argument(
        "-c",
        "--config",
        required=True,
        help="Path to deployconf YAML; if relative, resolve against the repo root inferred from this script path",
    )
    parser.add_argument(
        "-w",
        "--workdir",
        required=True,
        help=(
            "Output directory for generated scripts; if relative, resolve against the repo root inferred "
            "from this script path"
        ),
    )
    args = parser.parse_args()

    cfg = _load_yaml(_resolve_repo_root_cli_path(raw_path=Path(args.config), field_name="config"))
    outdir = _resolve_repo_root_cli_path(raw_path=Path(args.workdir), field_name="workdir")
    outdir.mkdir(parents=True, exist_ok=True)
    _clean_outdir(outdir)
    _write_script(
        outdir / PYTHON_SELECTION_SUPERVISOR_FILENAME,
        render_python_selection_supervisor_module(timeouts=STOP_TIMEOUTS),
    )
    (outdir / LOG_SHARD_HELPER_FILENAME).write_text(
        render_log_shard_module_source(),
        encoding="utf-8",
    )

    name_prefix = _require_str(cfg.get("name_prefix"), "name_prefix")
    cluster_nodes_raw = _require_list(cfg.get("cluster_nodes"), "cluster_nodes")
    cluster_nodes = [_require_dict(node, f"cluster_nodes[{idx}]") for idx, node in enumerate(cluster_nodes_raw)]
    node_ids = [_require_str(node.get("hostname"), f"cluster_nodes[{idx}].hostname") for idx, node in enumerate(cluster_nodes)]
    services = _require_dict(cfg.get("service"), "service")
    atomic_groups = _validate_atomic_groups(cfg.get("atomic_groups"), services=services, cluster_nodes=node_ids)
    bootstrap_bare_services = _validate_bootstrap_bare_services(
        cfg.get("bootstrap_bare_services"),
        services=services,
        atomic_groups=atomic_groups,
    )
    mapping = _ph_build_mapping(cluster_nodes=cluster_nodes, services=services)
    global_envs = _ph_resolve_or_raise(cfg.get("global_envs", {}) or {}, mapping, label="global_envs")
    global_envs_runtime = {
        key: _hostworkdir_runtime_value(value)
        for key, value in _require_dict(global_envs, "resolved_global_envs").items()
    }

    for service_name, raw_service_cfg in services.items():
        service_cfg = _require_dict(raw_service_cfg, f"service.{service_name}")
        standalone_entrypoint = _resolve_service_entrypoint(
            service_name=service_name,
            service_cfg=service_cfg,
            placeholder_mapping=mapping,
        )
        standalone_workload_name = _bare_plain_workload_name(name_prefix=name_prefix, service_name=service_name)
        _write_script(
            outdir / _bare_entrypoint_script_name(workload_name=standalone_workload_name),
            _render_bare_entrypoint_script(
                service_name=service_name,
                entrypoint=standalone_entrypoint,
            ),
        )
        _write_script(
            outdir / f"start_{service_name}.sh",
            _render_standalone_start_script(
                name_prefix=name_prefix,
                cluster_nodes=cluster_nodes,
                global_envs=global_envs_runtime,
                service_name=service_name,
                service_cfg=service_cfg,
            ),
        )
        _write_script(
            outdir / f"stop_{service_name}.sh",
            _render_standalone_stop_script(
                name_prefix=name_prefix,
                cluster_nodes=cluster_nodes,
                service_name=service_name,
                service_cfg=service_cfg,
            ),
        )

    for group_name, group_cfg in atomic_groups.items():
        for service_name in group_cfg["services"]:
            service_cfg = _require_dict(services.get(service_name), f"service.{service_name}")
            group_entrypoint = _resolve_service_entrypoint(
                service_name=service_name,
                service_cfg=service_cfg,
                placeholder_mapping=mapping,
            )
            group_workload_name = _bare_atomic_group_member_workload_name(
                name_prefix=name_prefix,
                group_name=group_name,
                service_name=service_name,
            )
            _write_script(
                outdir / _bare_entrypoint_script_name(workload_name=group_workload_name),
                _render_bare_entrypoint_script(
                    service_name=service_name,
                    entrypoint=group_entrypoint,
                ),
            )
        _write_script(
            outdir / f"start_{group_name}.sh",
            _render_atomic_group_start_script(
                name_prefix=name_prefix,
                cluster_nodes=cluster_nodes,
                global_envs=global_envs_runtime,
                group_name=group_name,
                group_cfg=group_cfg,
                services=services,
            ),
        )
        _write_script(
            outdir / f"stop_{group_name}.sh",
            _render_atomic_group_stop_script(
                name_prefix=name_prefix,
                cluster_nodes=cluster_nodes,
                group_name=group_name,
                group_cfg=group_cfg,
            ),
        )


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"config path does not exist: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _require_dict(data, str(path))


def _clean_outdir(outdir: Path) -> None:
    for path in outdir.iterdir():
        if path.is_file() or path.is_symlink():
            path.unlink()


def _write_script(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _validate_bootstrap_bare_services(
    raw: Any,
    *,
    services: Dict[str, Any],
    atomic_groups: Dict[str, Dict[str, Any]],
) -> set[str]:
    if raw is None:
        return set()
    items = _require_list_of_str(raw, "bootstrap_bare_services")
    atomic_group_names = set(atomic_groups.keys())
    out: set[str] = set()
    for service_name in items:
        if service_name not in services:
            raise ValueError(f"bootstrap_bare_services references unknown service: {service_name}")
        if service_name in atomic_group_names:
            raise ValueError(f"bootstrap_bare_services must not reference atomic group name: {service_name}")
        out.add(service_name)
    return out


def _validate_atomic_groups(
    raw: Any,
    *,
    services: Dict[str, Any],
    cluster_nodes: List[str],
) -> Dict[str, Dict[str, Any]]:
    if raw is None:
        return {}
    atomic_groups = _require_dict(raw, "atomic_groups")
    known_nodes = set(cluster_nodes)
    out: Dict[str, Dict[str, Any]] = {}
    seen_phase: set[int] = set()
    for group_name, raw_group_cfg in atomic_groups.items():
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("atomic_groups keys must be non-empty strings")
        group_cfg = _require_dict(raw_group_cfg, f"atomic_groups.{group_name}")
        phase = group_cfg.get("phase")
        if not isinstance(phase, int) or phase <= 0:
            raise ValueError(f"atomic_groups.{group_name}.phase must be a positive int")
        if phase in seen_phase:
            raise ValueError(f"atomic_groups.{group_name}.phase duplicates another atomic group phase={phase}")
        seen_phase.add(phase)
        nodes = _require_list_of_str(group_cfg.get("nodes"), f"atomic_groups.{group_name}.nodes")
        for node_name in nodes:
            if node_name not in known_nodes:
                raise ValueError(f"atomic_groups.{group_name}.nodes references unknown node: {node_name}")
        service_names = _require_list_of_str(group_cfg.get("services"), f"atomic_groups.{group_name}.services")
        for service_name in service_names:
            if service_name not in services:
                raise ValueError(f"atomic_groups.{group_name}.services references unknown service: {service_name}")
        out[group_name] = {"phase": phase, "nodes": nodes, "services": service_names}
    return out


def _bare_plain_workload_name(*, name_prefix: str, service_name: str) -> str:
    return _selection_plain_workload_name(
        name_prefix=name_prefix,
        selection_name=service_name,
    )


def _bare_atomic_group_member_workload_name(
    *,
    name_prefix: str,
    group_name: str,
    service_name: str,
) -> str:
    return _selection_atomic_group_member_selection_workload_name(
        name_prefix=name_prefix,
        selection_name=group_name,
        service_name=service_name,
    )


def _bare_plain_selection_supervisor_label(*, name_prefix: str, service_name: str) -> str:
    return _selection_daemonset_supervisor_label(
        workload_name=_bare_plain_workload_name(
            name_prefix=name_prefix,
            service_name=service_name,
        )
    )


def _bare_atomic_group_member_selection_supervisor_label(
    *,
    name_prefix: str,
    group_name: str,
    service_name: str,
) -> str:
    return _selection_daemonset_supervisor_label(
        workload_name=_bare_atomic_group_member_workload_name(
            name_prefix=name_prefix,
            group_name=group_name,
            service_name=service_name,
        )
    )


def _bare_entrypoint_script_name(*, workload_name: str) -> str:
    return f"entrypoint__{workload_name}.sh"


def _render_bare_entrypoint_script(*, service_name: str, entrypoint: str) -> str:
    return _render_bare_template(
        template_name="bare_entrypoint.sh.tmpl",
        values={
            "SERVICE_EXPORT": _sh_quote(service_name),
            "ENTRYPOINT": entrypoint.strip(),
        },
    )


def _bare_entrypoint_command(*, workload_name: str) -> List[str]:
    return [
        "/usr/bin/env",
        "bash",
        f"${{HOSTWORKDIR}}/gen_bare_deploy_bash/{_bare_entrypoint_script_name(workload_name=workload_name)}",
    ]


def _bare_runtime_state_json(
    *,
    workload_name: str,
    authority_name: str,
    service_name: str,
    log_path: str,
) -> str:
    return json.dumps(
        {
            "kind": "DaemonSet",
            "name": workload_name,
            "authority": authority_name,
            "service_name": service_name,
            "argv": _bare_entrypoint_command(workload_name=workload_name),
            "cwd": "${HOSTWORKDIR}",
            "log_path": log_path,
        },
        sort_keys=True,
    )


def _render_standalone_start_script(
    *,
    name_prefix: str,
    cluster_nodes: List[Dict[str, Any]],
    global_envs: Dict[str, Any],
    service_name: str,
    service_cfg: Dict[str, Any],
) -> str:
    allowed_nodes = _extract_nodes(service_cfg)
    return _render_bare_template(
        template_name="standalone_start.sh.tmpl",
        values={
            "SERVICE_ASSIGN": _sh_quote(service_name),
            "NAME_PREFIX_ASSIGN": _sh_quote(name_prefix),
            "ALLOWED_NODES_BLOCK": _render_nodes_bash(name="ALLOWED_NODES", nodes=allowed_nodes),
            "HOST_PRELUDE": _render_host_prelude(cluster_nodes=cluster_nodes),
            "COMMON_NODE_RESOLUTION_TAIL": _render_common_node_resolution_tail(service_name=service_name),
            "SELECTION_SUPERVISOR_PATH_BLOCK": _render_selection_supervisor_path_from_script_dir(),
            "PROC_LIFECYCLE_HELPERS": _render_proc_lifecycle_pid_tree_helpers(),
            "SELECTION_PRESENT_PROBE_FN": _render_selection_present_probe_fn(),
            "START_LOCK_BLOCK": _render_start_lock_block(),
            "GLOBAL_ENV_EXPORTS": _render_global_env_exports(global_envs),
            "PORT_EXPORT": _render_service_port_export(service_name=service_name, service_cfg=service_cfg),
            "START_BODY": _render_standalone_start_body(
                name_prefix=name_prefix,
                service_name=service_name,
            ),
        },
    )


def _render_standalone_stop_script(
    *,
    name_prefix: str,
    cluster_nodes: List[Dict[str, Any]],
    service_name: str,
    service_cfg: Dict[str, Any],
) -> str:
    allowed_nodes = _extract_nodes(service_cfg)
    return _render_bare_template(
        template_name="standalone_stop.sh.tmpl",
        values={
            "SERVICE_ASSIGN": _sh_quote(service_name),
            "NAME_PREFIX_ASSIGN": _sh_quote(name_prefix),
            "ALLOWED_NODES_BLOCK": _render_nodes_bash(name="ALLOWED_NODES", nodes=allowed_nodes),
            "HOST_PRELUDE": _render_host_prelude(cluster_nodes=cluster_nodes),
            "COMMON_NODE_RESOLUTION_TAIL": _render_common_node_resolution_tail(service_name=service_name),
            "SELECTION_SUPERVISOR_PATH_BLOCK": _render_selection_supervisor_path_from_script_dir(),
            "SUPERVISOR_LABEL_ASSIGN": _sh_quote(
                _bare_plain_selection_supervisor_label(name_prefix=name_prefix, service_name=service_name)
            ),
        },
    )


def _render_atomic_group_start_script(
    *,
    name_prefix: str,
    cluster_nodes: List[Dict[str, Any]],
    global_envs: Dict[str, Any],
    group_name: str,
    group_cfg: Dict[str, Any],
    services: Dict[str, Any],
) -> str:
    service_blocks: List[str] = []
    for service_name in group_cfg["services"]:
        service_cfg = _require_dict(services.get(service_name), f"service.{service_name}")
        service_blocks.append(
            _render_atomic_group_service_block(
                name_prefix=name_prefix,
                group_name=group_name,
                service_name=service_name,
                service_cfg=service_cfg,
            )
        )
    return _render_bare_template(
        template_name="atomic_group_start.sh.tmpl",
        values={
            "GROUP_ASSIGN": _sh_quote(group_name),
            "NAME_PREFIX_ASSIGN": _sh_quote(name_prefix),
            "HOST_PRELUDE": _render_host_prelude(cluster_nodes=cluster_nodes),
            "ATOMIC_GROUP_NODE_RESOLUTION_TAIL": _render_atomic_group_node_resolution_tail(group_cfg["nodes"]),
            "SELECTION_SUPERVISOR_PATH_BLOCK": _render_selection_supervisor_path_from_script_dir(),
            "PROC_LIFECYCLE_HELPERS": _render_proc_lifecycle_pid_tree_helpers(),
            "GLOBAL_ENV_EXPORTS": _render_global_env_exports(global_envs),
            "GROUP_STARTUP_DEADLINE_ASSIGN": str(ATOMIC_GROUP_STARTUP_DEADLINE_SECONDS),
            "SERVICE_BLOCKS": "".join(service_blocks),
        },
    )


def _render_atomic_group_stop_script(
    *,
    name_prefix: str,
    cluster_nodes: List[Dict[str, Any]],
    group_name: str,
    group_cfg: Dict[str, Any],
) -> str:
    stop_services = list(reversed(group_cfg["services"]))
    return _render_bare_template(
        template_name="atomic_group_stop.sh.tmpl",
        values={
            "GROUP_ASSIGN": _sh_quote(group_name),
            "NAME_PREFIX_ASSIGN": _sh_quote(name_prefix),
            "HOST_PRELUDE": _render_host_prelude(cluster_nodes=cluster_nodes),
            "ATOMIC_GROUP_NODE_RESOLUTION_TAIL": _render_atomic_group_node_resolution_tail(group_cfg["nodes"]),
            "SELECTION_SUPERVISOR_PATH_BLOCK": _render_selection_supervisor_path_from_script_dir(),
            "ATOMIC_GROUP_STOP_FN": _render_atomic_group_stop_fn(
                runtime_specs=[
                    {
                        "service_name": service_name,
                        "supervisor_label": _bare_atomic_group_member_selection_supervisor_label(
                            name_prefix=name_prefix,
                            group_name=group_name,
                            service_name=service_name,
                        ),
                    }
                    for service_name in stop_services
                ],
            ),
        },
    )


def _render_host_prelude(*, cluster_nodes: List[Dict[str, Any]]) -> str:
    all_nodes = [_require_str(node.get("hostname"), "cluster_nodes[].hostname") for node in cluster_nodes]
    ip_case_lines: list[str] = []
    host_case_lines: list[str] = []
    for node in cluster_nodes:
        node_name = _require_str(node.get("hostname"), "cluster_nodes[].hostname")
        node_ip = _require_str(node.get("ip"), f"cluster_nodes[{node_name}].ip")
        hostworkdir = _require_str(node.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
        ip_case_lines.append(f"        {_sh_quote(node_name)}) _ip_n={_sh_quote(node_ip)};;")
        host_case_lines.append(
            f"  {_sh_quote(node_name)}) HOST_IP={_sh_quote(node_ip)}; HOSTWORKDIR={_sh_quote(hostworkdir)};;"
        )
    return _render_bare_template(
        template_name="host_prelude.sh.tmpl",
        values={
            "ALL_NODES_BLOCK": _render_nodes_bash(name="ALL_NODES", nodes=all_nodes),
            "KNOWN_NODES": " ".join(all_nodes),
            "IP_CASE_LINES": "\n".join(ip_case_lines),
            "HOST_CASE_LINES": "\n".join(host_case_lines),
        },
    )


def _render_common_node_resolution_tail(*, service_name: str) -> str:
    return _render_bare_template(
        template_name="common_node_resolution_tail.sh.tmpl",
        values={"SERVICE_NAME": service_name},
    )


def _render_atomic_group_node_resolution_tail(allowed_nodes: List[str]) -> str:
    return _render_bare_template(
        template_name="atomic_group_node_resolution_tail.sh.tmpl",
        values={"GROUP_NODES_BLOCK": _render_nodes_bash(name="GROUP_NODES", nodes=allowed_nodes)},
    )


def _render_start_lock_block() -> str:
    return _load_bare_template(template_name="start_lock_block.sh.tmpl")


def _render_proc_lifecycle_pid_tree_helpers() -> str:
    return render_bash_proc_lifecycle_funcs_pid_tree(timeouts=STOP_TIMEOUTS) + "\n\n"


def _render_selection_present_probe_fn() -> str:
    return _load_bare_template(template_name="selection_present_probe_fn.sh.tmpl")


def _render_selection_supervisor_launch_wait_block(
    *,
    run_cmd: str,
    stable_seconds_expr: str,
    deadline_seconds_expr: str,
    context: str,
) -> str:
    return _render_bare_template(
        template_name="selection_supervisor_launch_wait_block.sh.tmpl",
        values={
            "RUN_CMD": run_cmd,
            "STABLE_SECONDS_EXPR": stable_seconds_expr,
            "DEADLINE_SECONDS_EXPR": deadline_seconds_expr,
            "CONTEXT": context,
        },
    )


def _render_service_port_export(*, service_name: str, service_cfg: Dict[str, Any], indent: str = "") -> str:
    service_port = _extract_port(service_cfg)
    if service_port is None:
        return indent + "unset SERVICE_PORT\n"
    return (
        indent + f"export {service_name.upper()}__PORT={_sh_quote(str(service_port))}\n"
        + indent + f"export SERVICE_PORT={_sh_quote(str(service_port))}\n"
    )


def _indent_script_block(*, script: str, prefix: str) -> str:
    out_lines: list[str] = []
    for line in script.splitlines(keepends=True):
        if line.strip():
            out_lines.append(prefix + line)
        else:
            out_lines.append(line)
    return "".join(out_lines)


def _render_standalone_start_body(*, name_prefix: str, service_name: str) -> str:
    workload_name = _bare_plain_workload_name(name_prefix=name_prefix, service_name=service_name)
    child_command = _bare_entrypoint_command(workload_name=workload_name)
    runtime_state_json = _bare_runtime_state_json(
        workload_name=workload_name,
        authority_name=_selection_plain_selection_authority_name(selection_name=service_name),
        service_name=service_name,
        log_path=f"${{HOSTWORKDIR}}/log/{service_name}.log",
    )
    run_cmd = _render_selection_supervisor_run_shell(
        subcommand="run",
        supervisor_expr='"$SELECTION_SUPERVISOR"',
        state_json_expr='"$RUNTIME_STATE_JSON"',
        label_expr='"$SUPERVISOR_LABEL"',
        workdir_expr='"$HOSTWORKDIR"',
        restart_policy="always",
        restart_delay_seconds=5,
        max_backoff_seconds=STANDALONE_BACKOFF_MAX_SECONDS,
        crashloop_consecutive_restarts=0,
        crashloop_interval_lt_seconds=0,
        child_command=child_command,
    )
    return _render_bare_template(
        template_name="standalone_start_body.sh.tmpl",
        values={
            "SUPERVISOR_LABEL_ASSIGN": _sh_quote(
                _bare_plain_selection_supervisor_label(name_prefix=name_prefix, service_name=service_name)
            ),
            "RUNTIME_STATE_JSON_ASSIGN": _sh_quote(runtime_state_json),
            "STARTUP_DEADLINE_SECONDS": str(STANDALONE_STARTUP_DEADLINE_SECONDS),
            "SELECTION_SUPERVISOR_LAUNCH_WAIT_BLOCK": _render_selection_supervisor_launch_wait_block(
                run_cmd=run_cmd,
                stable_seconds_expr=str(STANDALONE_PROBABLE_READY_SECONDS),
                deadline_seconds_expr=str(STANDALONE_STARTUP_DEADLINE_SECONDS),
                context="[bare]",
            ),
        },
    )


def _render_selection_supervisor_path_from_script_dir() -> str:
    return _render_bare_template(
        template_name="selection_supervisor_path_from_script_dir.sh.tmpl",
        values={"SELECTION_SUPERVISOR_FILENAME": PYTHON_SELECTION_SUPERVISOR_FILENAME},
    )


def _render_atomic_group_service_block(
    *,
    name_prefix: str,
    group_name: str,
    service_name: str,
    service_cfg: Dict[str, Any],
) -> str:
    workload_name = _bare_atomic_group_member_workload_name(
        name_prefix=name_prefix,
        group_name=group_name,
        service_name=service_name,
    )
    child_command = _bare_entrypoint_command(workload_name=workload_name)
    runtime_state_json = _bare_runtime_state_json(
        workload_name=workload_name,
        authority_name=_selection_atomic_group_member_authority_name(
            selection_name=group_name,
            service_name=service_name,
        ),
        service_name=service_name,
        log_path=f"${{HOSTWORKDIR}}/log/{service_name}.log",
    )
    allowed_nodes = _extract_nodes(service_cfg)
    run_cmd = _render_selection_supervisor_run_shell(
        subcommand="run",
        supervisor_expr='"$SELECTION_SUPERVISOR"',
        state_json_expr='"$RUNTIME_STATE_JSON"',
        label_expr='"$SUPERVISOR_LABEL"',
        workdir_expr='"$HOSTWORKDIR"',
        restart_policy="always",
        restart_delay_seconds=5,
        max_backoff_seconds=ATOMIC_GROUP_BACKOFF_MAX_SECONDS,
        crashloop_consecutive_restarts=ATOMIC_GROUP_CRASHLOOP_CONSECUTIVE_RESTARTS,
        crashloop_interval_lt_seconds=ATOMIC_GROUP_CRASHLOOP_INTERVAL_LT_SECONDS,
        child_command=child_command,
    )
    return _render_bare_template(
        template_name="atomic_group_service_block.sh.tmpl",
        values={
            "SERVICE_NAME": service_name,
            "ALLOWED_NODES_BLOCK": _render_nodes_bash(name="ALLOWED_NODES", nodes=allowed_nodes),
            "SERVICE_EXPORT": _sh_quote(service_name),
            "PORT_EXPORT": _render_service_port_export(
                service_name=service_name,
                service_cfg=service_cfg,
                indent="  ",
            ),
            "SUPERVISOR_LABEL_ASSIGN": _sh_quote(
                _bare_atomic_group_member_selection_supervisor_label(
                    name_prefix=name_prefix,
                    group_name=group_name,
                    service_name=service_name,
                )
            ),
            "RUNTIME_STATE_JSON_ASSIGN": _sh_quote(runtime_state_json),
            "LOGFILE_PATH": f"$HOSTWORKDIR/log/{service_name}.log",
            "INDENTED_SELECTION_SUPERVISOR_LAUNCH_WAIT_BLOCK": _indent_script_block(
                script=_render_selection_supervisor_launch_wait_block(
                    run_cmd=run_cmd,
                    stable_seconds_expr=str(ATOMIC_GROUP_PROBABLE_READY_SECONDS),
                    deadline_seconds_expr=str(ATOMIC_GROUP_STARTUP_DEADLINE_SECONDS),
                    context="[rollout]",
                ).rstrip()
                + "\n",
                prefix="  ",
            ).rstrip(),
        },
    )


def _render_atomic_group_stop_fn(*, runtime_specs: List[Dict[str, str]]) -> str:
    out = "stop_group() {\n"
    out += "  local STOP_FAILED=0\n"
    for spec in runtime_specs:
        supervisor_label = _require_str(spec.get("supervisor_label"), "runtime_specs[].supervisor_label")
        out += f'  SUPERVISOR_LABEL={_sh_quote(supervisor_label)}\n'
        out += '  if ! python3 "$SELECTION_SUPERVISOR" stop --label "$SUPERVISOR_LABEL" --scope-key "$HOSTWORKDIR" --missing-ok >/dev/null; then\n'
        out += '    echo "[atomic-group] stop failed group=$GROUP node=$NODE_ID label=$SUPERVISOR_LABEL"\n'
        out += "    STOP_FAILED=1\n"
        out += "  fi\n"
    out += '  if [ "$STOP_FAILED" -ne 0 ]; then\n'
    out += "    return 1\n"
    out += "  fi\n"
    out += "  return 0\n}\n\n"
    return out


def _render_selection_supervisor_run_shell(
    *,
    subcommand: str,
    supervisor_expr: str,
    state_json_expr: str,
    label_expr: str,
    workdir_expr: str,
    restart_policy: str,
    restart_delay_seconds: int,
    max_backoff_seconds: int,
    crashloop_consecutive_restarts: int,
    crashloop_interval_lt_seconds: int,
    child_command: List[str],
) -> str:
    # English note:
    # - Bare bootstrap / repair publishes one fixed owner token for one supervisor big loop.
    # - Internal child restarts stay inside that same logical generation.
    # - A later bare/apply handoff must therefore pass a strictly newer owner token instead of
    #   mutating per-apply local files.
    return (
        f'python3 {supervisor_expr} {subcommand}'
        + f' --label {label_expr}'
        + ' --scope-key "$HOSTWORKDIR"'
        + f' --state-json {state_json_expr}'
        + ' --owner-ts-ms "$OWNER_TS_MS"'
        + f" --restart-policy {restart_policy}"
        + f" --restart-delay-seconds {restart_delay_seconds}"
        + f" --max-backoff-seconds {max_backoff_seconds}"
        + f" --crashloop-consecutive-restarts {crashloop_consecutive_restarts}"
        + f" --crashloop-interval-lt-seconds {crashloop_interval_lt_seconds}"
        + f' --workdir {workdir_expr}'
        + " -- "
        + " ".join(_sh_quote_runtime_expand_arg(part) for part in child_command)
    )


def _render_global_env_exports(global_envs: Dict[str, Any]) -> str:
    def _sh_escape_double_quotes(val: str) -> str:
        return val.replace("\\", "\\\\").replace('"', '\\"')

    out = ""
    for key, value in global_envs.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(f"global_envs key must be a non-empty string: {key!r}")
        if not isinstance(value, str):
            raise ValueError(f"global_envs.{key} must resolve to a string")
        if "\n" in value or "\r" in value:
            delim = f"__FLUXON_ENV_{key}__"
            out += f"{key}=$(cat <<'{delim}'\n"
            out += value.rstrip("\n") + "\n"
            out += f"{delim}\n"
            out += ")\n"
            out += f"export {key}\n"
            continue
        if "$" in value:
            rest = value.replace("$HOSTWORKDIR", "").replace("${HOSTWORKDIR}", "")
            if "$" in rest or "`" in value or "$(" in value:
                raise ValueError(
                    f"global_envs.{key} contains unsupported bash expansion in a single-line value: {value!r}. "
                    "Only $HOSTWORKDIR / ${HOSTWORKDIR} runtime expansion is allowed."
                )
            out += f'export {key}="{_sh_escape_double_quotes(value)}"\n'
        else:
            out += f"export {key}={_sh_quote(value)}\n"
    if out:
        out += "\n"
    return out


def _resolve_service_entrypoint(
    *,
    service_name: str,
    service_cfg: Dict[str, Any],
    placeholder_mapping: Dict[str, str],
) -> str:
    raw_entrypoint = _require_str(service_cfg.get("entrypoint"), f"service.{service_name}.entrypoint")
    resolved_entrypoint = _ph_resolve_nested(raw_entrypoint, placeholder_mapping)
    if "${${" in resolved_entrypoint:
        raise ValueError(
            f"service.{service_name}.entrypoint contains an unresolved nested placeholder '${{${{...}}}}'"
        )
    return _hostworkdir_runtime_value(resolved_entrypoint)


def _hostworkdir_runtime_value(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError(f"value must be a string, got: {value!r}")
    marker = "__FLUXON_HOSTWORKDIR_RUNTIME__"
    return (
        value.replace(HOSTWORKDIR_RUNTIME_TOKEN, marker)
        .replace("$HOSTWORKDIR", marker)
        .replace("/hostworkdir", marker)
        .replace(marker, HOSTWORKDIR_RUNTIME_TOKEN)
    )


def _extract_nodes(service_cfg: Dict[str, Any]) -> List[str]:
    node_bind = _require_dict(service_cfg.get("node_bind"), "service.node_bind")
    raw_nodes = node_bind.get("node")
    if isinstance(raw_nodes, list):
        return [_require_str(node_name, "service.node_bind.node[]") for node_name in raw_nodes]
    if isinstance(raw_nodes, str) and raw_nodes.strip():
        return [raw_nodes.strip()]
    raise ValueError("service.node_bind.node must be a non-empty list or string")


def _extract_port(service_cfg: Dict[str, Any]) -> int | None:
    raw = service_cfg.get("port")
    if raw is None:
        return None
    if isinstance(raw, int) and raw > 0:
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        port = int(raw.strip())
        if port > 0:
            return port
    raise ValueError(f"service.port must be a positive integer when present, got: {raw!r}")


def _render_nodes_bash(*, name: str, nodes: List[str]) -> str:
    return f"{name}=(" + " ".join(_sh_quote(node_name) for node_name in nodes) + ")\n"


def _require_dict(raw: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return raw


def _require_list(raw: Any, field_name: str) -> List[Any]:
    if not isinstance(raw, list):
        raise ValueError(f"{field_name} must be a list")
    return raw


def _require_list_of_str(raw: Any, field_name: str) -> List[str]:
    values = _require_list(raw, field_name)
    return [_require_str(value, f"{field_name}[]") for value in values]


def _require_str(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return raw.strip()


def _sh_quote(value: str) -> str:
    return shlex.quote(value)


def _sh_quote_runtime_expand_arg(value: str) -> str:
    # English note:
    # - Bare child argv can legitimately contain `${HOSTWORKDIR}` because the generated
    #   supervisor is launched on a host whose final hostworkdir is only known there.
    # - Wrapping such an argument in single quotes would freeze the placeholder as a
    #   literal string and make `bash ${HOSTWORKDIR}/...` fail with rc=127 on the host.
    # - Keep ordinary shell quoting for stable literals, but switch to double-quote
    #   expansion-preserving quoting for the specific hostworkdir runtime token path.
    if HOSTWORKDIR_RUNTIME_TOKEN in value or "$HOSTWORKDIR" in value:
        return _sh_expand_quote(value)
    return _sh_quote(value)


def _sh_expand_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


if __name__ == "__main__":
    main()
