#!/usr/bin/env python3
"""
Generate fluxon-deployer compatible DaemonSet YAMLs from deployconf.

Important:
- fluxon-deployer parses a strict subset of `apps/v1 DaemonSet` / `apps/v1 Deployment`.
  The generated YAML is intentionally *not* valid for a real Kubernetes cluster because
  most K8s fields are rejected by deployer (`serde(deny_unknown_fields)`).
- For self-host bare deployments, `${HOSTWORKDIR}` is the authority runtime token for the
  mapped hostworkdir on the selected node.
- The generator still normalizes legacy `/hostworkdir` / `$HOSTWORKDIR` spellings into
  `${HOSTWORKDIR}` so the emitted scripts stay single-sourced.
- Atomic groups are emitted as one logical-selection YAML file that may contain multiple
  manifest documents. Each document is a plain workload for one member service. Runtime
  authority stays in fluxon_ops; generated YAML must remain declarative and must not embed a
  selection supervisor or runner ownership model.

CLI:
- Only `--config/-c` and `--workdir/-w` (no extra flags).
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Any, Dict, List, Optional

_UTILS_DIR = os.path.join(os.path.dirname(__file__), "utils")
sys.path.insert(0, _UTILS_DIR)
from placeholder_utils import (  # type: ignore
    build_mapping_for_cfg as _ph_build_mapping,
    resolve_values_or_raise as _ph_resolve_or_raise,
    resolve_placeholders_nested as _ph_resolve_nested,
)
from selection_runtime import (  # type: ignore
    atomic_group_member_workload_name as _selection_atomic_group_member_workload_name,
    plain_selection_workload_name as _selection_plain_workload_name,
    resolve_selection_target_nodes as _selection_resolve_selection_target_nodes,
)

import yaml  # type: ignore


class _LiteralStr(str):
    """Marker type for YAML block literal strings in generated manifests."""

    pass


# Annotations are the only "human hint" fields that fluxon-deployer's strict YAML subset accepts.
_MANIFEST_ANNOTATIONS: Dict[str, str] = {
    "fluxon-deployer.manifest_flavor": "fluxon-deployer",
    "fluxon-deployer.note": "Not a kubectl-valid DaemonSet YAML; deploy via fluxon-deployer controller /api/deploy.",
}
_ANNOT_KEY_NAMESPACE = "fluxon.io/namespace"
_ANNOT_KEY_LOGICAL_SELECTION = "fluxon.io/logical_selection"
_ANNOT_KEY_SERVICE_NAME = "fluxon.io/service_name"
_ANNOT_KEY_ATOMIC_GROUP = "fluxon.io/atomic_group"
_ANNOT_KEY_ATOMIC_GROUP_PHASE = "fluxon.io/atomic_group_phase"
_ANNOT_KEY_ATOMIC_GROUP_ORDER = "fluxon.io/atomic_group_order"
HOSTWORKDIR_RUNTIME_TOKEN = "${HOSTWORKDIR}"


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Generate fluxon-deployer compatible DaemonSet YAMLs from deployconf"
    )
    parser.add_argument("-c", "--config", required=True, help="Path to deployconf YAML")
    parser.add_argument(
        "-w",
        "--workdir",
        help="Output directory for generated DaemonSet YAMLs",
    )
    args = parser.parse_args(argv)

    cfg = _load_yaml(args.config)

    name_prefix = str(cfg.get("name_prefix", "")).strip()
    if not name_prefix:
        print("Field 'name_prefix' is required in deployconf.yaml for deployer YAML generation.")
        return 1

    try:
        namespace = _coalesce_namespace_string(cfg.get("namespace"))
    except ValueError as e:
        print(f"Invalid namespace at top-level 'namespace': {e}")
        return 1

    cluster_nodes = _ensure_list(cfg.get("cluster_nodes", []))
    if not cluster_nodes:
        print("cluster_nodes is empty in config. Nothing to generate.")
        return 1

    node_ip: Dict[str, str] = {}
    node_hostworkdir: Dict[str, str] = {}
    for node in cluster_nodes:
        if not isinstance(node, dict):
            print(f"Invalid cluster_nodes entry (expect mapping): {node}")
            return 1
        nid = str(node.get("hostname", "")).strip()
        ip = str(node.get("ip", "")).strip()
        if not nid or not ip:
            print(f"Invalid cluster_nodes entry (require hostname, ip): {node}")
            return 1
        hw = str(node.get("hostworkdir") or "").strip()
        if not hw:
            print(f"cluster_nodes[].hostworkdir is required for deployer YAML generation; missing on: {nid}")
            return 1
        node_ip[nid] = ip
        node_hostworkdir[nid] = hw

    try:
        top_image_default = _coalesce_image_string(cfg.get("image"))
    except ValueError as e:
        print(f"Invalid image at top-level 'image': {e}")
        return 1

    services = cfg.get("service", {}) or {}
    if not isinstance(services, dict) or not services:
        print("No services found under 'service:' in config.")
        return 1

    # Resolve placeholders in global_envs at generation time, then emit explicit exports
    # into the generated workload entrypoint script.
    global_envs: Dict[str, Any] = cfg.get("global_envs", {}) or {}
    try:
        mapping = _ph_build_mapping(cluster_nodes=cluster_nodes, services=services)
        resolved_global_envs = _ph_resolve_or_raise(global_envs, mapping, label="global_envs")
    except Exception as e:
        print(f"Failed to resolve placeholders in global_envs: {e}")
        return 1
    if not _validate_cluster_node_ids_env(
        resolved_global_envs=resolved_global_envs,
        cluster_node_ids=list(node_hostworkdir.keys()),
    ):
        return 1

    atomic_groups = cfg.get("atomic_groups", {}) or {}
    if not isinstance(atomic_groups, dict):
        print("atomic_groups must be a mapping when present.")
        return 1
    atomic_groups = _validate_atomic_groups(
        atomic_groups,
        services,
        cluster_nodes=list(node_hostworkdir.keys()),
    )
    bootstrap_bare_services = _validate_bootstrap_bare_services(
        cfg.get("bootstrap_bare_services"),
        services=services,
        services_in_groups={s for g in atomic_groups.values() for s in g["services"]},
    )
    service_nodes_by_service = {
        service_name: _extract_nodes(service_cfg)
        for service_name, service_cfg in services.items()
        if isinstance(service_cfg, dict)
    }

    outdir = args.workdir
    if not outdir:
        # Reason: keep CLI minimal; default output sits next to the generator script.
        outdir = os.path.join(os.path.dirname(__file__), "gen_k8s_daemonset")
    os.makedirs(outdir, exist_ok=True)
    try:
        _clean_daemonset_yaml_outdir(outdir)
    except Exception as e:
        print(f"Failed to clean output dir '{outdir}': {e}")
        return 1

    mirror_outdir = str(cfg.get("gen_k8s_daemonset_mirror_outdir", "") or "").strip()
    if mirror_outdir:
        if not os.path.isabs(mirror_outdir):
            print("gen_k8s_daemonset_mirror_outdir must be an absolute path when set.")
            return 1
        os.makedirs(mirror_outdir, exist_ok=True)
        try:
            _clean_daemonset_yaml_outdir(mirror_outdir)
        except Exception as e:
            print(f"Failed to clean mirror output dir '{mirror_outdir}': {e}")
            return 1

    # 1) Atomic groups => one YAML file per logical selection.
    #
    # Each atomic-group file contains one manifest document per member service workload.
    # The file-level selection object stays stable (`<group>.daemonset.yaml`), but the runtime
    # workload identities are the explicit member workloads. This keeps YAML declarative and lets
    # fluxon_ops own the only supervisor authority.
    groups_sorted = sorted(
        atomic_groups.items(),
        key=lambda kv: (kv[1]["phase"], kv[0]),
    )
    for group_name, group_cfg in groups_sorted:
        try:
            manifests = _build_atomic_group_daemonsets(
                namespace=namespace,
                name_prefix=name_prefix,
                group_name=group_name,
                group_cfg=group_cfg,
                services=services,
                top_image_default=top_image_default,
                node_ip=node_ip,
                node_hostworkdir=node_hostworkdir,
                global_envs=resolved_global_envs,
                placeholder_mapping=mapping,
            )
        except Exception as e:
            print(f"Failed to build atomic group DaemonSet '{group_name}': {e}")
            return 1

        out_path = os.path.join(outdir, f"{group_name}.daemonset.yaml")
        _write_yaml_docs(out_path, manifests)
        print(f"Generated: {os.path.relpath(out_path)}")
        _print_deploy_hint(out_path=out_path, resolved_global_envs=resolved_global_envs)

    # 2) Plain services => one DaemonSet YAML per service selection.
    #
    # Semantics:
    # - Atomic groups always win per (service,node) ownership.
    # - A plain service selection is still the config-level source of truth, but its emitted
    #   target nodes exclude the nodes already owned by atomic_groups for the same service.
    # - Services listed in bootstrap_bare_services are bare-only bootstrap ownership and are NEVER
    #   emitted as deployer DaemonSets, otherwise deployer would depend on its own bootstrap path.
    for sname, scfg in services.items():
        if sname in bootstrap_bare_services:
            continue
        if not isinstance(scfg, dict):
            print(f"Service '{sname}' config must be a mapping.")
            return 1
        try:
            target_nodes = _selection_resolve_selection_target_nodes(
                selection_name=sname,
                services=services,
                atomic_groups=atomic_groups,
                service_nodes_by_service=service_nodes_by_service,
            )
        except Exception as e:
            print(f"Failed to resolve desired target nodes for service '{sname}': {e}")
            return 1
        if not target_nodes:
            continue
        try:
            manifest = _build_single_service_daemonset(
                namespace=namespace,
                name_prefix=name_prefix,
                service_name=sname,
                service_cfg=scfg,
                top_image_default=top_image_default,
                node_ip=node_ip,
                node_hostworkdir=node_hostworkdir,
                global_envs=resolved_global_envs,
                placeholder_mapping=mapping,
                affinity_nodes=target_nodes,
            )
        except Exception as e:
            print(f"Failed to build DaemonSet for service '{sname}': {e}")
            return 1

        out_path = os.path.join(outdir, f"{sname}.daemonset.yaml")
        _write_yaml_docs(out_path, [manifest])
        print(f"Generated: {os.path.relpath(out_path)}")
        _print_deploy_hint(out_path=out_path, resolved_global_envs=resolved_global_envs)

    if mirror_outdir:
        try:
            _mirror_daemonset_yaml_outdir(src_outdir=outdir, dst_outdir=mirror_outdir)
        except Exception as e:
            print(f"Failed to mirror DaemonSet YAMLs to '{mirror_outdir}': {e}")
            return 1
        print(f"Mirrored DaemonSet YAMLs to: {mirror_outdir}")

    return 0


def _validate_bootstrap_bare_services(
    raw: Any,
    *,
    services: Dict[str, Any],
    services_in_groups: set[str],
) -> set[str]:
    if raw is None:
        return set()
    if not isinstance(raw, list):
        raise ValueError("bootstrap_bare_services must be a list of strings when present")
    out: set[str] = set()
    for idx, item in enumerate(raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"bootstrap_bare_services[{idx}] must be a non-empty string")
        service_name = item.strip()
        if service_name not in services:
            raise ValueError(f"bootstrap_bare_services references unknown service: {service_name}")
        if service_name in services_in_groups:
            raise ValueError(
                f"bootstrap_bare_services contains '{service_name}', but that service is already owned by an atomic group"
            )
        out.add(service_name)
    return out


def _validate_cluster_node_ids_env(*, resolved_global_envs: Dict[str, Any], cluster_node_ids: List[str]) -> bool:
    # English note:
    # - `FLUXON_CLUSTER_NODE_IDS` is a business-level invariant used by multiple services to derive
    #   per-node instance keys and to form the initial node list for P2P.
    # - If it drifts from `cluster_nodes[].hostname`, the system may try to route RPCs to nodes that
    #   are not deployed (e.g. NodeNotFound), which looks like "deployment didn't start".
    # - Therefore we fail-fast at generation time instead of letting runtime behavior silently drift.
    raw = resolved_global_envs.get("FLUXON_CLUSTER_NODE_IDS")
    if raw is None:
        print("global_envs.FLUXON_CLUSTER_NODE_IDS is required (no implicit default).")
        return False
    if not isinstance(raw, str):
        print("global_envs.FLUXON_CLUSTER_NODE_IDS must be a string.")
        return False

    want = [s for s in raw.split(" ") if s.strip()]
    if not want:
        print("global_envs.FLUXON_CLUSTER_NODE_IDS is empty.")
        return False
    if len(set(want)) != len(want):
        print(f"global_envs.FLUXON_CLUSTER_NODE_IDS contains duplicates: {raw!r}")
        return False

    have = cluster_node_ids
    if set(want) != set(have):
        print("global_envs.FLUXON_CLUSTER_NODE_IDS must match cluster_nodes[].hostname exactly.")
        print(f"- cluster_nodes: {have}")
        print(f"- FLUXON_CLUSTER_NODE_IDS: {want}")
        return False
    return True


def _print_deploy_hint(*, out_path: str, resolved_global_envs: Dict[str, Any]) -> None:
    base_url = resolved_global_envs.get("FLUXON_OPS_UI_BASE_URL")
    if not isinstance(base_url, str) or not base_url.strip():
        return
    base_url = base_url.rstrip("/")
    cluster_name = resolved_global_envs.get("FLUXON_CLUSTER_NAME")
    if not isinstance(cluster_name, str) or not cluster_name.strip():
        return
    cluster_name = cluster_name.strip()
    # `--data-binary` keeps newlines as-is (YAML is newline-sensitive).
    print(
        f"Deploy: curl -sS -X POST {base_url}/r/ops/{cluster_name}/api/deploy --data-binary @{out_path}"
    )


def _mirror_daemonset_yaml_outdir(*, src_outdir: str, dst_outdir: str) -> None:
    # English note: keep mirror semantics explicit and minimal.
    #
    # Causal chain:
    # - Operators often run the generator from the repo workdir, while fluxon-deployer reads
    #   manifests from a hostworkdir-mounted directory (e.g. /tmp/fluxon-example/deploy).
    # - If the two directories drift, deployer applies stale manifests and can crash-loop.
    # - Therefore we provide an explicit opt-in "mirror_outdir" that:
    #   - cleans the destination directory (same semantics as the primary outdir)
    #   - copies all freshly generated `*.daemonset.yaml` into it.
    if os.path.abspath(src_outdir) == os.path.abspath(dst_outdir):
        return
    import shutil

    for name in os.listdir(src_outdir):
        if not name.endswith(".daemonset.yaml"):
            continue
        src = os.path.join(src_outdir, name)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(dst_outdir, name)
        shutil.copy2(src, dst)


def _clean_daemonset_yaml_outdir(outdir: str) -> None:
    # English note: generator output is not backwards-compatible by design.
    #
    # Causal chain:
    # - We may change which workloads are generated (e.g. move a service into an atomic group).
    # - If we keep stale `*.daemonset.yaml` around, operators may accidentally apply old manifests
    #   and end up with duplicated workloads and version-mismatch windows.
    # - Therefore we delete all previously generated DaemonSet YAMLs in the output directory.
    for name in os.listdir(outdir):
        if not name.endswith(".daemonset.yaml"):
            continue
        path = os.path.join(outdir, name)
        if not os.path.isfile(path):
            continue
        os.remove(path)


_RE_ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _render_global_env_exports(*, global_envs: Dict[str, Any]) -> str:
    if not global_envs:
        return ""
    if not isinstance(global_envs, dict):
        raise ValueError("global_envs must be a mapping")

    def _normalize_hostworkdir_tokens(val: str) -> str:
        marker = "__FLUXON_HOSTWORKDIR_RUNTIME__"
        return (
            val.replace(HOSTWORKDIR_RUNTIME_TOKEN, marker)
            .replace("$HOSTWORKDIR", marker)
            .replace("/hostworkdir", marker)
            .replace(marker, HOSTWORKDIR_RUNTIME_TOKEN)
        )

    def _sh_escape_double_quotes(val: str) -> str:
        return val.replace("\\", "\\\\").replace('"', '\\"')

    # Render envs as explicit exports in the entrypoint script to avoid reliance on an external
    # deployer-agent environment. Multiline values are represented via a heredoc assignment.
    lines: List[str] = []
    lines.append("# === Global envs (generated) ===\n")
    for k in sorted(global_envs.keys()):
        if not isinstance(k, str) or not k.strip():
            raise ValueError("global_envs keys must be non-empty strings")
        key = k.strip()
        if not _RE_ENV_KEY.match(key):
            raise ValueError(f"global_envs contains an invalid env key: {key!r}")
        v = global_envs[k]
        if v is None:
            raise ValueError(f"global_envs.{key} must not be null")

        if isinstance(v, str):
            val = _normalize_hostworkdir_tokens(v)
        else:
            val = str(v)

        if "\n" in val or "\r" in val:
            delim = f"__FLUXON_ENV_{key}__"
            lines.append(f"{key}=$(cat <<'{delim}'\n")
            lines.append(val.rstrip("\n") + "\n")
            lines.append(f"{delim}\n")
            lines.append(")\n")
            lines.append(f"export {key}\n")
        else:
            if "$" in val:
                # Keep global_envs single-line values as "data" (no bash logic) so the interface stays clean.
                # The only allowed runtime expansion is ${HOSTWORKDIR} (needed for per-node hostworkdir mapping).
                rest = val.replace(HOSTWORKDIR_RUNTIME_TOKEN, "")
                if "$" in rest or "`" in val or "$(" in val:
                    raise ValueError(
                        f"global_envs.{key} contains unsupported bash expansion in a single-line value: {val!r}. "
                        "Move the logic into a multi-line bash block (|-), or into the service entrypoint."
                    )
                lines.append(f'export {key}="{_sh_escape_double_quotes(val)}"\n')
            else:
                lines.append(f"export {key}={_sh_quote(val)}\n")
    lines.append("\n")
    return "".join(lines)


def _validate_runtime_string_template(*, value: str, field_name: str) -> str:
    if "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a single-line string")
    allowed = value.replace("${NODE_ID}", "").replace("$NODE_ID", "")
    if "$(" in value or "`" in value or "$" in allowed:
        raise ValueError(
            f"{field_name} contains unsupported runtime expansion: {value!r}. "
            "Only $NODE_ID / ${NODE_ID} are allowed."
        )
    return value


def _load_yaml(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        data = yaml.safe_load(text) or {}
    except FileNotFoundError:
        print(f"Config not found: {path}")
        raise SystemExit(1)
    except Exception as e:
        print(f"Failed to parse YAML {path}: {e}")
        raise SystemExit(1)
    if not isinstance(data, dict):
        print("Top-level YAML must be a mapping")
        raise SystemExit(1)
    return data


def _ensure_list(x: Any) -> List[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _coalesce_image_string(image_val: Any) -> str:
    if image_val is None:
        return ""
    if isinstance(image_val, str):
        s = image_val.strip()
        if not s:
            return ""
        if ":" not in s:
            raise ValueError(f"image must be 'repository:tag' string; got: {s}")
        return s
    raise ValueError(f"image must be 'repository:tag' string; got type {type(image_val).__name__}")


def _coalesce_namespace_string(namespace_val: Any) -> str:
    if not isinstance(namespace_val, str):
        raise ValueError("namespace must be a non-empty string")
    namespace = namespace_val.strip()
    if not namespace:
        raise ValueError("namespace must be a non-empty string")
    for ch in namespace:
        if ch.isascii() and (ch.isalnum() or ch in "-_."):
            continue
        raise ValueError(f"namespace contains unsupported character: {ch!r}")
    return namespace


def _extract_port(svc_cfg: Dict[str, Any]) -> Optional[int]:
    if not isinstance(svc_cfg, dict):
        return None
    port = svc_cfg.get("port")
    if isinstance(port, int):
        return port
    nb = svc_cfg.get("node_bind") or {}
    nb_port = nb.get("port")
    if isinstance(nb_port, int):
        return nb_port
    return None


def _extract_nodes(svc_cfg: Dict[str, Any]) -> List[str]:
    if not isinstance(svc_cfg, dict):
        return []
    nb = svc_cfg.get("node_bind") or {}
    nodes = nb.get("node") or []
    if isinstance(nodes, list):
        return [str(n).strip() for n in nodes if str(n).strip()]
    if isinstance(nodes, str) and nodes.strip():
        return [nodes.strip()]
    return []


def _yaml_dumper() -> type[yaml.SafeDumper]:
    class _LiteralDumper(yaml.SafeDumper):
        def ignore_aliases(self, data: object) -> bool:  # type: ignore[override]
            return True

        def represent_str(self, data: str) -> yaml.ScalarNode:  # type: ignore[override]
            if isinstance(data, _LiteralStr):  # type: ignore[name-defined]
                return self.represent_scalar("tag:yaml.org,2002:str", str(data), style="|")
            return super().represent_str(data)

    _LiteralDumper.add_representer(_LiteralStr, _LiteralDumper.represent_str)  # type: ignore[name-defined]
    _LiteralDumper.add_representer(str, _LiteralDumper.represent_str)
    return _LiteralDumper


def _write_yaml_docs(path: str, manifests: List[Dict[str, Any]]) -> None:
    if not manifests:
        raise ValueError("manifests must be non-empty")
    dumper = _yaml_dumper()
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        yaml.dump_all(
            manifests,
            f,
            Dumper=dumper,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            explicit_start=len(manifests) > 1,
        )
def _validate_atomic_groups(
    atomic_groups: Dict[str, Any],
    services: Dict[str, Any],
    *,
    cluster_nodes: List[str],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    if not cluster_nodes:
        raise ValueError("cluster_nodes must be non-empty")

    seen_phase: Dict[int, str] = {}
    seen_service_node: Dict[tuple[str, str], str] = {}
    known_nodes = set(cluster_nodes)

    for group_name, raw in atomic_groups.items():
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("atomic_groups keys must be non-empty strings")
        if not isinstance(raw, dict):
            raise ValueError(f"atomic_groups.{group_name} must be a mapping")

        phase = raw.get("phase")
        if not isinstance(phase, int) or phase <= 0:
            raise ValueError(f"atomic_groups.{group_name}.phase must be a positive int")
        if phase in seen_phase:
            raise ValueError(
                f"atomic_groups.{group_name}.phase duplicates atomic_groups.{seen_phase[phase]} (phase={phase})"
            )
        seen_phase[phase] = group_name

        nodes_raw = raw.get("nodes")
        if not isinstance(nodes_raw, list) or not nodes_raw:
            raise ValueError(f"atomic_groups.{group_name}.nodes must be a non-empty list of strings")
        nodes: List[str] = []
        for n in nodes_raw:
            if not isinstance(n, str) or not n.strip():
                raise ValueError(f"atomic_groups.{group_name}.nodes must be a non-empty list of strings")
            nn = n.strip()
            if nn not in known_nodes:
                raise ValueError(f"atomic_groups.{group_name}.nodes contains unknown node: {nn}")
            if nn not in nodes:
                nodes.append(nn)

        svcs_raw = raw.get("services")
        if not isinstance(svcs_raw, list) or not svcs_raw:
            raise ValueError(f"atomic_groups.{group_name}.services must be a non-empty list of strings")
        svcs: List[str] = []
        for s in svcs_raw:
            if not isinstance(s, str) or not s.strip():
                raise ValueError(f"atomic_groups.{group_name}.services must be a non-empty list of strings")
            ss = s.strip()
            if ss not in services:
                raise ValueError(f"atomic_groups.{group_name}.services references unknown service: {ss}")
            if ss not in svcs:
                svcs.append(ss)

        # Reject overlapping scheduling for the same (service,node) across groups to avoid
        # accidental double restarts.
        for s in svcs:
            for n in nodes:
                k = (s, n)
                if k in seen_service_node:
                    raise ValueError(
                        f"service '{s}' is scheduled on node '{n}' in multiple atomic_groups: "
                        f"{seen_service_node[k]} and {group_name}"
                    )
                seen_service_node[k] = group_name

        out[group_name.strip()] = {
            "phase": phase,
            "nodes": nodes,
            "services": svcs,
        }

    return out


def _build_single_service_daemonset(
    *,
    namespace: str,
    name_prefix: str,
    service_name: str,
    service_cfg: Dict[str, Any],
    top_image_default: str,
    node_ip: Dict[str, str],
    node_hostworkdir: Dict[str, str],
    global_envs: Dict[str, Any],
    placeholder_mapping: Dict[str, str],
    affinity_nodes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    service_nodes = _extract_nodes(service_cfg)
    if not service_nodes:
        raise ValueError(f"service '{service_name}': node_bind.node must be non-empty for deployer generation")
    nodes = service_nodes if affinity_nodes is None else affinity_nodes
    for node_name in nodes:
        if node_name not in service_nodes:
            raise ValueError(
                f"service '{service_name}': workload nodes {nodes} must be a subset of service node_bind.node {service_nodes}"
            )

    image = _coalesce_image_string(service_cfg.get("image", None)) or top_image_default
    if not image:
        raise ValueError(f"service '{service_name}': missing image")

    entrypoint = service_cfg.get("entrypoint")
    if not isinstance(entrypoint, str) or not entrypoint.strip():
        raise ValueError(f"service '{service_name}': entrypoint must be a non-empty string")
    # Resolve deployconf placeholders (e.g. ${${SVC__NODE_ID}__IP}) at generation time.
    # This avoids bash `bad substitution` crashes in the generated rollout script.
    entrypoint = _ph_resolve_nested(entrypoint, placeholder_mapping)
    if "${${" in entrypoint:
        raise ValueError(
            f"service '{service_name}': entrypoint contains an unresolved nested placeholder '${{${{...}}}}'. "
            "Move it into global_envs (so the generator resolves it), or bind the referenced service to "
            "exactly one node so <SVC>__NODE_ID can be resolved."
        )

    script = _render_service_entrypoint_script(
        service_name=service_name,
        entrypoint=entrypoint,
        service_port=_extract_port(service_cfg),
        target_nodes=nodes,
        node_ip=node_ip,
        node_hostworkdir=node_hostworkdir,
        global_envs=global_envs,
    )
    return _wrap_script_as_daemonset(
        namespace=namespace,
        ds_name=_selection_plain_workload_name(
            name_prefix=name_prefix,
            selection_name=service_name,
        ),
        affinity_nodes=nodes,
        container_name=service_name,
        image=image,
        script=script,
        annotations_extra={
            _ANNOT_KEY_LOGICAL_SELECTION: service_name,
            _ANNOT_KEY_SERVICE_NAME: service_name,
        },
    )


def _build_atomic_group_daemonsets(
    *,
    namespace: str,
    name_prefix: str,
    group_name: str,
    group_cfg: Dict[str, Any],
    services: Dict[str, Any],
    top_image_default: str,
    node_ip: Dict[str, str],
    node_hostworkdir: Dict[str, str],
    global_envs: Dict[str, Any],
    placeholder_mapping: Dict[str, str],
) -> List[Dict[str, Any]]:
    svc_list = group_cfg["services"]
    group_nodes = group_cfg["nodes"]
    phase = group_cfg["phase"]
    manifests: List[Dict[str, Any]] = []
    for order, svc_name in enumerate(svc_list):
        svc_cfg = services.get(svc_name)
        if not isinstance(svc_cfg, dict):
            raise ValueError(f"atomic group '{group_name}': service config must be a mapping: {svc_name}")
        svc_nodes = _extract_nodes(svc_cfg)
        if not svc_nodes:
            raise ValueError(f"atomic group '{group_name}': service '{svc_name}' missing node_bind.node")
        target_nodes = [node_name for node_name in group_nodes if node_name in svc_nodes]
        if not target_nodes:
            continue
        image = _coalesce_image_string(svc_cfg.get("image", None)) or top_image_default
        if not image:
            raise ValueError(
                f"atomic group '{group_name}': service '{svc_name}' missing image (use service image or top-level image)"
            )
        entrypoint = svc_cfg.get("entrypoint")
        if not isinstance(entrypoint, str) or not entrypoint.strip():
            raise ValueError(f"atomic group '{group_name}': service '{svc_name}' missing entrypoint")
        entrypoint = _ph_resolve_nested(entrypoint, placeholder_mapping)
        if "${${" in entrypoint:
            raise ValueError(
                f"atomic group '{group_name}': service '{svc_name}' entrypoint contains an unresolved nested "
                "placeholder '${${...}}'. Move it into global_envs, or bind the referenced service to exactly "
                "one node so <SVC>__NODE_ID can be resolved."
            )
        workload_name = _selection_atomic_group_member_workload_name(
            selection_name=group_name,
            service_name=svc_name,
        )
        script = _render_service_entrypoint_script(
            service_name=svc_name,
            entrypoint=entrypoint,
            service_port=_extract_port(svc_cfg),
            target_nodes=target_nodes,
            node_ip=node_ip,
            node_hostworkdir=node_hostworkdir,
            global_envs=global_envs,
        )
        manifests.append(
            _wrap_script_as_daemonset(
                namespace=namespace,
                ds_name=_selection_plain_workload_name(
                    name_prefix=name_prefix,
                    selection_name=workload_name,
                ),
                affinity_nodes=target_nodes,
                container_name=svc_name,
                image=image,
                script=script,
                annotations_extra={
                    _ANNOT_KEY_LOGICAL_SELECTION: group_name,
                    _ANNOT_KEY_SERVICE_NAME: svc_name,
                    _ANNOT_KEY_ATOMIC_GROUP: group_name,
                    _ANNOT_KEY_ATOMIC_GROUP_PHASE: str(phase),
                    _ANNOT_KEY_ATOMIC_GROUP_ORDER: str(order),
                },
            )
        )
    if not manifests:
        raise ValueError(f"atomic group '{group_name}' does not own any member workload on its target nodes")
    return manifests


def _render_service_entrypoint_script(
    *,
    service_name: str,
    entrypoint: str,
    service_port: Optional[int],
    target_nodes: List[str],
    node_ip: Dict[str, str],
    node_hostworkdir: Dict[str, str],
    global_envs: Dict[str, Any],
) -> str:
    lines: List[str] = [
        _render_host_prelude(
            node_ip=node_ip,
            node_hostworkdir=node_hostworkdir,
            known_nodes=target_nodes,
        ),
        "\n",
        'echo "[manifest] flavor=fluxon-deployer (not a kubectl-valid DaemonSet YAML)"\n',
        _render_global_env_exports(global_envs=global_envs),
        f"export SERVICE={_sh_quote(service_name)}\n",
    ]
    if service_port is not None:
        lines.append(f"export {service_name.upper()}__PORT={_sh_quote(str(service_port))}\n")
    lines.append(
        entrypoint
        .replace(HOSTWORKDIR_RUNTIME_TOKEN, "__FLUXON_HOSTWORKDIR_RUNTIME__")
        .replace("$HOSTWORKDIR", "__FLUXON_HOSTWORKDIR_RUNTIME__")
        .replace("/hostworkdir", "__FLUXON_HOSTWORKDIR_RUNTIME__")
        .replace("__FLUXON_HOSTWORKDIR_RUNTIME__", HOSTWORKDIR_RUNTIME_TOKEN)
        .strip()
    )
    lines.append("\n")
    return "".join(lines)


def _wrap_script_as_daemonset(
    *,
    namespace: str,
    ds_name: str,
    affinity_nodes: List[str],
    container_name: str,
    image: str,
    script: str,
    annotations_extra: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    annotations = dict(_MANIFEST_ANNOTATIONS)
    annotations[_ANNOT_KEY_NAMESPACE] = namespace
    if annotations_extra:
        annotations.update(annotations_extra)
    return {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": ds_name, "annotations": annotations},
        "spec": {
            "template": {
                "spec": {
                    "affinity": _render_node_affinity(affinity_nodes),
                    "containers": [
                        {
                            "name": container_name,
                            "image": image,
                            "command": ["/bin/bash", "-lc"],
                            "args": [_LiteralStr(script)],
                        }
                    ],
                }
            }
        },
    }


def _render_node_affinity(nodes: List[str]) -> Dict[str, Any]:
    if not nodes:
        raise ValueError("node list must be non-empty for affinity")
    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [
                            {
                                "key": "kubernetes.io/hostname",
                                "operator": "In",
                                "values": nodes,
                            }
                        ]
                    }
                ]
            }
        }
    }


def _render_host_prelude(*, node_ip: Dict[str, str], node_hostworkdir: Dict[str, str], known_nodes: List[str]) -> str:
    if not known_nodes:
        raise ValueError("known_nodes must be non-empty")
    unknown = [n for n in known_nodes if n not in node_ip or n not in node_hostworkdir]
    if unknown:
        raise ValueError(f"known_nodes contains unknown nodes: {', '.join(unknown)}")

    all_nodes = sorted(set(known_nodes))
    all_nodes_block = "ALL_NODES=(" + " ".join(_sh_quote(n) for n in all_nodes) + ")"

    ip_case_lines: List[str] = []
    node_case_lines: List[str] = []
    for n in all_nodes:
        ip = node_ip[n]
        hw = node_hostworkdir[n]
        ip_case_lines.append(f"        {n}) _ip_n={_sh_quote(ip)};;")
        node_case_lines.append(f"  {n}) HOST_IP={_sh_quote(ip)}; HOSTWORKDIR={_sh_quote(hw)};;")

    ip_case = "\n".join(ip_case_lines)
    node_case = "\n".join(node_case_lines)

    # NODE_ID may be preset by a node-local launcher such as ops_agent. This is
    # required for same-host logical nodes that share one IP address.
    return (
        "set -euo pipefail\n"
        f"{all_nodes_block}\n"
        "\n"
        "LOCAL_HOSTNAME=$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)\n"
        "LOCAL_FQDN=$(hostname -f 2>/dev/null || echo \"$LOCAL_HOSTNAME\")\n"
        "NODE_ID=\"${NODE_ID:-}\"\n"
        "if [ -n \"$NODE_ID\" ]; then\n"
        "  _node_id_known=false\n"
        "  for n in \"${ALL_NODES[@]}\"; do\n"
        "    if [ \"$n\" = \"$NODE_ID\" ]; then\n"
        "      _node_id_known=true\n"
        "      break\n"
        "    fi\n"
        "  done\n"
        "  if [ \"$_node_id_known\" != true ]; then\n"
        "    echo \"Unknown preset NODE_ID: $NODE_ID\"\n"
        f"    echo \"Known nodes: {' '.join(all_nodes)}\"\n"
        "    exit 1\n"
        "  fi\n"
        "fi\n"
        "if [ -z \"$NODE_ID\" ]; then\n"
        "for n in \"${ALL_NODES[@]}\"; do\n"
        "  if [ \"$n\" = \"$LOCAL_HOSTNAME\" ] || [ \"$n\" = \"$LOCAL_FQDN\" ]; then\n"
        "    NODE_ID=\"$n\"\n"
        "    break\n"
        "  fi\n"
        "done\n"
        "fi\n"
        "if [ -z \"$NODE_ID\" ] && [ ${#ALL_NODES[@]} -eq 1 ]; then\n"
        "  NODE_ID=\"${ALL_NODES[0]}\"\n"
        "fi\n"
        "if [ -z \"$NODE_ID\" ]; then\n"
        "  for ip in $(hostname -I 2>/dev/null); do\n"
        "    for n in \"${ALL_NODES[@]}\"; do\n"
        "      _ip_n=\"\"\n"
        "      case \"$n\" in\n"
        f"{ip_case}\n"
        "        *) _ip_n=\"\";;\n"
        "      esac\n"
        "      if [ \"$_ip_n\" = \"$ip\" ]; then\n"
        "        NODE_ID=\"$n\"\n"
        "        break\n"
        "      fi\n"
        "    done\n"
        "    [ -n \"$NODE_ID\" ] && break\n"
        "  done\n"
        "fi\n"
        "if [ -z \"$NODE_ID\" ]; then\n"
        "  echo \"Cannot map host to a configured node. Hostname=$LOCAL_HOSTNAME FQDN=$LOCAL_FQDN IPs=$(hostname -I 2>/dev/null)\"\n"
        f"  echo \"Known nodes: {' '.join(all_nodes)}\"\n"
        "  exit 1\n"
        "fi\n"
        "\n"
        "HOST_IP=\"\"\n"
        "HOSTWORKDIR=\"\"\n"
        "case \"$NODE_ID\" in\n"
        f"{node_case}\n"
        "  *) echo \"Unknown NODE_ID: '$NODE_ID'\"; exit 1;;\n"
        "esac\n"
        "\n"
        "export NODE_ID=\"$NODE_ID\"\n"
        "export HOST_IP=\"$HOST_IP\"\n"
        "export HOSTWORKDIR=\"$HOSTWORKDIR\"\n"
    )


def _sh_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
