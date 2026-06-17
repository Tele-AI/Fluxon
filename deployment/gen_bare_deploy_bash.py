#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
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
from selection_supervisor_codegen import (  # type: ignore
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
STANDALONE_PROBABLE_READY_SECONDS = 3
STANDALONE_STARTUP_DEADLINE_SECONDS = 60
ATOMIC_GROUP_STARTUP_DEADLINE_SECONDS = 10 * 60
HOSTWORKDIR_RUNTIME_TOKEN = "${HOSTWORKDIR}"
REPO_ROOT = SCRIPT_DIR.parent


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
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        f"export SERVICE={_sh_quote(service_name)}\n"
        + entrypoint.strip()
        + "\n"
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
    service_port = _extract_port(service_cfg)
    port_export = ""
    if service_port is not None:
        port_export = f"export {service_name.upper()}__PORT={_sh_quote(str(service_port))}\n"
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        f"SERVICE={_sh_quote(service_name)}\n"
        f"NAME_PREFIX={_sh_quote(name_prefix)}\n"
        + _render_nodes_bash(name="ALLOWED_NODES", nodes=allowed_nodes)
        + _render_host_prelude(cluster_nodes=cluster_nodes)
        + _render_common_node_resolution_tail(service_name=service_name)
        + _render_selection_supervisor_path_from_script_dir()
        + _render_proc_lifecycle_pid_tree_helpers()
        + _render_selection_present_probe_fn()
        + _render_start_lock_block()
        + _render_global_env_exports(global_envs)
        + port_export
        + _render_standalone_start_body(
            name_prefix=name_prefix,
            service_name=service_name,
        )
    )


def _render_standalone_stop_script(
    *,
    name_prefix: str,
    cluster_nodes: List[Dict[str, Any]],
    service_name: str,
    service_cfg: Dict[str, Any],
) -> str:
    allowed_nodes = _extract_nodes(service_cfg)
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        f"SERVICE={_sh_quote(service_name)}\n"
        f"NAME_PREFIX={_sh_quote(name_prefix)}\n"
        + _render_nodes_bash(name="ALLOWED_NODES", nodes=allowed_nodes)
        + _render_host_prelude(cluster_nodes=cluster_nodes)
        + _render_common_node_resolution_tail(service_name=service_name)
        + _render_selection_supervisor_path_from_script_dir()
        + f'SUPERVISOR_LABEL={_sh_quote(_bare_plain_selection_supervisor_label(name_prefix=name_prefix, service_name=service_name))}\n'
        + "# English note:\n"
        + "# - Generated bare stop is retained as a manual operator tool.\n"
        + "# - Automation must not depend on this path for handover or rollout convergence.\n"
        + "# - The command only asks the shared selection supervisor to retire the concrete selection\n"
        + "#   identity identified by label on this node.\n"
        + 'if ! python3 "$SELECTION_SUPERVISOR" stop --label "$SUPERVISOR_LABEL" --scope-key "$HOSTWORKDIR" --missing-ok >/dev/null; then\n'
        + '  echo "[bare] stop failed svc=$SERVICE label=$SUPERVISOR_LABEL hostworkdir=$HOSTWORKDIR"\n'
        + "  exit 1\n"
        + "fi\n"
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
    return (
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n\n"
        f"GROUP={_sh_quote(group_name)}\n"
        f"NAME_PREFIX={_sh_quote(name_prefix)}\n"
        + _render_host_prelude(cluster_nodes=cluster_nodes)
        + _render_atomic_group_node_resolution_tail(group_cfg["nodes"])
        + _render_selection_supervisor_path_from_script_dir()
        + _render_proc_lifecycle_pid_tree_helpers()
        + _render_global_env_exports(global_envs)
        + f"GROUP_STARTUP_DEADLINE_TS=$(( $(date +%s) + {ATOMIC_GROUP_STARTUP_DEADLINE_SECONDS} ))\n"
        + "".join(service_blocks)
        + 'echo "[atomic-group] ready group=$GROUP node=$NODE_ID"\n'
    )


def _render_atomic_group_stop_script(
    *,
    name_prefix: str,
    cluster_nodes: List[Dict[str, Any]],
    group_name: str,
    group_cfg: Dict[str, Any],
) -> str:
    stop_services = list(reversed(group_cfg["services"]))
    return (
        "#!/usr/bin/env bash\n"
        "set -u -o pipefail\n\n"
        f"GROUP={_sh_quote(group_name)}\n"
        f"NAME_PREFIX={_sh_quote(name_prefix)}\n"
        + _render_host_prelude(cluster_nodes=cluster_nodes)
        + _render_atomic_group_node_resolution_tail(group_cfg["nodes"])
        + _render_selection_supervisor_path_from_script_dir()
        + _render_atomic_group_stop_fn(
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
        )
        + "stop_group\n"
    )


def _render_host_prelude(*, cluster_nodes: List[Dict[str, Any]]) -> str:
    all_nodes = [_require_str(node.get("hostname"), "cluster_nodes[].hostname") for node in cluster_nodes]
    out = _render_nodes_bash(name="ALL_NODES", nodes=all_nodes)
    out += "\nLOCAL_HOSTNAME=$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)\n"
    out += 'LOCAL_FQDN=$(hostname -f 2>/dev/null || echo "$LOCAL_HOSTNAME")\n'
    out += 'NODE_ID="${NODE_ID:-}"\n'
    out += 'if [ -n "$NODE_ID" ]; then\n'
    out += '  _node_id_known=false\n'
    out += '  for n in "${ALL_NODES[@]}"; do\n'
    out += '    if [ "$n" = "$NODE_ID" ]; then\n'
    out += '      _node_id_known=true\n'
    out += "      break\n"
    out += "    fi\n"
    out += "  done\n"
    out += '  if [ "$_node_id_known" != true ]; then\n'
    out += '    echo "Unknown preset NODE_ID: $NODE_ID"\n'
    out += f'    echo "Known nodes: {" ".join(all_nodes)}"\n'
    out += "    exit 1\n"
    out += "  fi\n"
    out += "fi\n"
    out += 'if [ -z "$NODE_ID" ]; then\n'
    out += 'for n in "${ALL_NODES[@]}"; do\n'
    out += '  if [ "$n" = "$LOCAL_HOSTNAME" ] || [ "$n" = "$LOCAL_FQDN" ]; then\n'
    out += '    NODE_ID="$n"\n'
    out += "    break\n"
    out += "  fi\n"
    out += "done\n"
    out += "fi\n"
    out += 'if [ -z "$NODE_ID" ] && [ ${#ALL_NODES[@]} -eq 1 ]; then\n'
    out += '  NODE_ID="${ALL_NODES[0]}"\n'
    out += "fi\n"
    out += 'if [ -z "$NODE_ID" ]; then\n'
    out += '  for ip in $(hostname -I 2>/dev/null); do\n'
    out += '    for n in "${ALL_NODES[@]}"; do\n'
    out += '      _ip_n=""\n'
    out += '      case "$n" in\n'
    for node in cluster_nodes:
        node_name = _require_str(node.get("hostname"), "cluster_nodes[].hostname")
        node_ip = _require_str(node.get("ip"), f"cluster_nodes[{node_name}].ip")
        out += f"        {_sh_quote(node_name)}) _ip_n={_sh_quote(node_ip)};;\n"
    out += '        *) _ip_n="";;\n'
    out += "      esac\n"
    out += '      if [ "$_ip_n" = "$ip" ]; then\n'
    out += '        NODE_ID="$n"\n'
    out += "        break\n"
    out += "      fi\n"
    out += "    done\n"
    out += '    [ -n "$NODE_ID" ] && break\n'
    out += "  done\n"
    out += "fi\n"
    out += 'if [ -z "$NODE_ID" ]; then\n'
    out += '  echo "Cannot map host to a configured node. Hostname=$LOCAL_HOSTNAME FQDN=$LOCAL_FQDN IPs=$(hostname -I 2>/dev/null)"\n'
    out += f'  echo "Known nodes: {" ".join(all_nodes)}"\n'
    out += "  exit 1\n"
    out += "fi\n\n"
    out += 'HOST_IP=""\nHOSTWORKDIR=""\ncase "$NODE_ID" in\n'
    for node in cluster_nodes:
        node_name = _require_str(node.get("hostname"), "cluster_nodes[].hostname")
        node_ip = _require_str(node.get("ip"), f"cluster_nodes[{node_name}].ip")
        hostworkdir = _require_str(node.get("hostworkdir"), f"cluster_nodes[{node_name}].hostworkdir")
        out += f"  {_sh_quote(node_name)}) HOST_IP={_sh_quote(node_ip)}; HOSTWORKDIR={_sh_quote(hostworkdir)};;\n"
    out += '  *) echo "Unknown NODE_ID: $NODE_ID"; exit 1;;\n'
    out += "esac\n"
    return out


def _render_common_node_resolution_tail(*, service_name: str) -> str:
    return (
        'if [ ${#ALLOWED_NODES[@]} -gt 0 ]; then\n'
        + '  _ok=false\n'
        + '  for n in "${ALLOWED_NODES[@]}"; do\n'
        + '    if [ "$n" = "$NODE_ID" ]; then _ok=true; fi\n'
        + "  done\n"
        + '  if [ "$_ok" != true ]; then\n'
        + f'    echo "Service {service_name} not scheduled on this node ($NODE_ID). Allowed: ${{ALLOWED_NODES[*]}}"\n'
        + "    exit 0\n"
        + "  fi\n"
        + "fi\n\n"
        + 'export NODE_ID="$NODE_ID"\n'
        + 'export HOST_IP="$HOST_IP"\n'
        + 'export HOSTWORKDIR="$HOSTWORKDIR"\n\n'
    )


def _render_atomic_group_node_resolution_tail(allowed_nodes: List[str]) -> str:
    return (
        _render_nodes_bash(name="GROUP_NODES", nodes=allowed_nodes)
        + 'scheduled=false\n'
        + 'for n in "${GROUP_NODES[@]}"; do\n'
        + '  if [ "$n" = "$NODE_ID" ]; then scheduled=true; fi\n'
        + "done\n"
        + 'if [ "$scheduled" != true ]; then\n'
        + '  echo "[atomic-group] skip group=$GROUP node=$NODE_ID allowed=${GROUP_NODES[*]}"\n'
        + "  exit 0\n"
        + "fi\n\n"
        + 'export NODE_ID="$NODE_ID"\n'
        + 'export HOST_IP="$HOST_IP"\n'
        + 'export HOSTWORKDIR="$HOSTWORKDIR"\n'
        + 'echo "[atomic-group] group=$GROUP node=$NODE_ID hostworkdir=$HOSTWORKDIR"\n\n'
    )


def _render_start_lock_block() -> str:
    return (
        'PID_DIR="$HOSTWORKDIR/run"\n'
        + 'mkdir -p "$PID_DIR"\n'
        + 'START_LOCKFILE="$PID_DIR/${SERVICE}.start.lock"\n'
        + 'if ! command -v flock >/dev/null 2>&1; then\n'
        + '  echo "Missing required command: flock"\n'
        + "  exit 1\n"
        + "fi\n"
        + 'exec 9>"$START_LOCKFILE"\n'
        + 'if ! flock -xn 9; then\n'
        + '  echo "[bare] start skipped svc=$SERVICE reason=another start is already running lockfile=$START_LOCKFILE"\n'
        + "  exit 0\n"
        + "fi\n"
        + 'exec 9>&-\n\n'
    )


def _render_proc_lifecycle_pid_tree_helpers() -> str:
    return render_bash_proc_lifecycle_funcs_pid_tree(timeouts=STOP_TIMEOUTS) + "\n\n"


def _render_selection_present_probe_fn() -> str:
    return (
        "selection_present() {\n"
        + "  python3 - \"$SELECTION_SUPERVISOR\" \"$SUPERVISOR_LABEL\" \"$HOSTWORKDIR\" <<'__FLUXON_SELECTION_PRESENT__'\n"
        + "import importlib.util\n"
        + "import sys\n"
        + "from pathlib import Path\n"
        + "\n"
        + "supervisor_path = Path(sys.argv[1])\n"
        + "label = sys.argv[2]\n"
        + "scope_key = sys.argv[3]\n"
        + 'spec = importlib.util.spec_from_file_location("fluxon_selection_supervisor_probe", supervisor_path)\n'
        + "if spec is None or spec.loader is None:\n"
        + '    raise RuntimeError(f"failed to load selection supervisor module: {supervisor_path}")\n'
        + "module = importlib.util.module_from_spec(spec)\n"
        + "sys.modules[spec.name] = module\n"
        + "spec.loader.exec_module(module)\n"
        + "raise SystemExit(0 if module._selection_present(label, scope_key=scope_key) else 1)\n"
        + "__FLUXON_SELECTION_PRESENT__\n"
        + "}\n\n"
    )


def _render_selection_supervisor_launch_wait_block(
    *,
    run_cmd: str,
    logfile_expr: str,
    stable_seconds_expr: str,
    deadline_ts_expr: str,
    context: str,
) -> str:
    return (
        'SUPERVISOR_PID=$( '
        + run_cmd
        + f' >>{logfile_expr} 2>&1 < /dev/null & echo "$!" )\n'
        + 'if [[ ! "$SUPERVISOR_PID" =~ ^[0-9]+$ ]]; then\n'
        + f'  echo "{context} launch failed svc=$SERVICE label=$SUPERVISOR_LABEL supervisor_pid=$SUPERVISOR_PID"\n'
        + "  exit 1\n"
        + "fi\n"
        + 'if ! wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID" '
        + stable_seconds_expr
        + " "
        + deadline_ts_expr
        + f' "{context}"; then\n'
        + f'  echo "{context} probable-ready failed svc=$SERVICE label=$SUPERVISOR_LABEL supervisor_pid=$SUPERVISOR_PID"\n'
        + "  exit 1\n"
        + "fi\n"
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
    return (
        f'SUPERVISOR_LABEL={_sh_quote(_bare_plain_selection_supervisor_label(name_prefix=name_prefix, service_name=service_name))}\n'
        + f'RUNTIME_STATE_JSON={_sh_quote(runtime_state_json)}\n'
        + 'OWNER_TS_MS=$(python3 -c \'import time; print(int(time.time() * 1000))\')\n'
        + f"STARTUP_DEADLINE_TS=$(( $(date +%s) + {STANDALONE_STARTUP_DEADLINE_SECONDS} ))\n"
        + 'LOG_DIR="$HOSTWORKDIR/log"\n'
        + 'LOGFILE="$LOG_DIR/${SERVICE}.log"\n'
        + 'mkdir -p "$LOG_DIR"\n'
        + 'touch "$LOGFILE"\n'
        + 'echo "Starting $SERVICE on $NODE_ID (IP: $HOST_IP, workdir: $HOSTWORKDIR)"\n'
        + "# English note:\n"
        + "# - bootstrap bare start must be idempotent when the shared selection supervisor already owns\n"
        + "#   a live child for the same label.\n"
        + "# - start_test_bed enables this path only for deployconf.bootstrap_bare_services.\n"
        + 'if [ "${FLUXON_BARE_ALLOW_ALREADY_PRESENT:-false}" = "true" ]; then\n'
        + "  if selection_present; then\n"
        + '    echo "[bare] already present svc=$SERVICE label=$SUPERVISOR_LABEL"\n'
        + '    echo "Started $SERVICE (label: $SUPERVISOR_LABEL)"\n'
        + '    echo "Logs: $LOGFILE"\n'
        + "    exit 0\n"
        + "  fi\n"
        + "fi\n"
        + "# English note:\n"
        + "# - Bare start must not depend on extra supervisor observation subcommands because the shared\n"
        + "#   runtime surface is intentionally reduced to run/stop.\n"
        + "# - We therefore launch the detached supervisor and wait until its pid subtree keeps a live child\n"
        + "#   process for a short stable window.\n"
        + _render_selection_supervisor_launch_wait_block(
            run_cmd=run_cmd,
            logfile_expr='"$LOGFILE"',
            stable_seconds_expr=str(STANDALONE_PROBABLE_READY_SECONDS),
            deadline_ts_expr='"$STARTUP_DEADLINE_TS"',
            context="[bare]",
        )
        + 'echo "Started $SERVICE (label: $SUPERVISOR_LABEL)"\n'
        + 'echo "Logs: $LOGFILE"\n'
    )


def _render_selection_supervisor_path_from_script_dir() -> str:
    return (
        'DIR=$(cd "$(dirname "$0")" && pwd)\n'
        + f'SELECTION_SUPERVISOR="$DIR/{PYTHON_SELECTION_SUPERVISOR_FILENAME}"\n'
        + 'if [ ! -f "$SELECTION_SUPERVISOR" ]; then\n'
        + '  echo "Missing selection supervisor: $SELECTION_SUPERVISOR"\n'
        + "  exit 1\n"
        + "fi\n\n"
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
    service_port = _extract_port(service_cfg)
    port_export = ""
    if service_port is not None:
        port_export = f"  export {service_name.upper()}__PORT={_sh_quote(str(service_port))}\n"
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
    return (
        f"\n# rollout: {service_name}\n"
        + _render_nodes_bash(name="ALLOWED_NODES", nodes=allowed_nodes)
        + "scheduled=false\n"
        + 'for n in "${ALLOWED_NODES[@]}"; do\n'
        + '  if [ "$n" = "$NODE_ID" ]; then scheduled=true; fi\n'
        + "done\n"
        + 'if [ "$scheduled" != true ]; then\n'
        + f'  echo "[rollout] skip {service_name}: not scheduled on node $NODE_ID"\n'
        + "else\n"
        + f"  export SERVICE={_sh_quote(service_name)}\n"
        + port_export
        + '  LOG_DIR="$HOSTWORKDIR/log"\n'
        + '  mkdir -p "$LOG_DIR"\n'
        + f'  SUPERVISOR_LABEL={_sh_quote(_bare_atomic_group_member_selection_supervisor_label(name_prefix=name_prefix, group_name=group_name, service_name=service_name))}\n'
        + f'  RUNTIME_STATE_JSON={_sh_quote(runtime_state_json)}\n'
        + '  OWNER_TS_MS=$(python3 -c \'import time; print(int(time.time() * 1000))\')\n'
        + f'  LOGFILE="$HOSTWORKDIR/log/{service_name}.log"\n'
        + '  touch "$LOGFILE"\n'
        + f'  echo "[rollout] start {service_name} node=$NODE_ID hostworkdir=$HOSTWORKDIR"\n'
        + "  # English note:\n"
        + "  # - Atomic-group order still depends on a readiness gate, but that gate now observes only the\n"
        + "  #   detached supervisor process subtree on this host.\n"
        + "  # - Ownership stays inside the shared selection supervisor big loop; the group runner only waits\n"
        + "  #   until that loop has a stable live child before advancing to the next service.\n"
        # English note:
        # - The embedded `run_cmd` contains a nested `bash -lc` payload, and that payload may contain
        #   heredocs used by real service entrypoints.
        # - A blind newline replacement would shift heredoc terminators away from column 0 inside the
        #   child shell and silently turn valid entrypoints into immediate no-op exits.
        # - Indent only the outer block lines while preserving each inner line start exactly.
        + _indent_script_block(
            script=_render_selection_supervisor_launch_wait_block(
                run_cmd=run_cmd,
                logfile_expr='"$LOGFILE"',
                stable_seconds_expr=str(ATOMIC_GROUP_PROBABLE_READY_SECONDS),
                deadline_ts_expr='"$GROUP_STARTUP_DEADLINE_TS"',
                context="[rollout]",
            ).rstrip() + "\n",
            prefix="  ",
        ).rstrip()
        + "\n"
        + "fi\n"
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
