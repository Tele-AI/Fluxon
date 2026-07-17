from __future__ import annotations

from typing import Any, Dict, List


SELECTION_WORKLOAD_KIND_DAEMONSET = "DaemonSet"
SELECTION_SUPERVISOR_DIR_NAME = "selection_supervisor"


def atomic_group_service_nodes(*, atomic_groups: Dict[str, Any]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for group_name, raw_group_cfg in atomic_groups.items():
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("atomic_groups keys must be non-empty strings")
        if not isinstance(raw_group_cfg, dict):
            raise ValueError(f"atomic_groups.{group_name} must be a mapping")
        group_nodes = _require_list_of_str(raw_group_cfg.get("nodes"), f"atomic_groups.{group_name}.nodes")
        service_names = _require_list_of_str(raw_group_cfg.get("services"), f"atomic_groups.{group_name}.services")
        for service_name in service_names:
            for node_name in group_nodes:
                out.add((service_name, node_name))
    return out


def resolve_selection_service_name(
    *,
    selection_name: str,
) -> str:
    return _require_str(selection_name, "selection_name")


def plain_selection_workload_name(*, name_prefix: str, selection_name: str) -> str:
    return f"{_require_str(name_prefix, 'name_prefix')}-{_require_str(selection_name, 'selection_name')}"


def atomic_group_member_workload_name(*, selection_name: str, service_name: str) -> str:
    return f"{_require_str(selection_name, 'selection_name')}__{_require_str(service_name, 'service_name')}"


def atomic_group_member_selection_workload_name(
    *,
    name_prefix: str,
    selection_name: str,
    service_name: str,
) -> str:
    return plain_selection_workload_name(
        name_prefix=name_prefix,
        selection_name=atomic_group_member_workload_name(
            selection_name=selection_name,
            service_name=service_name,
        ),
    )


def daemonset_selection_supervisor_label(*, workload_name: str) -> str:
    return (
        f"{SELECTION_WORKLOAD_KIND_DAEMONSET}/"
        f"{_require_str(workload_name, 'workload_name')}"
    )

def resolve_selection_nodes(
    *,
    selection_name: str,
    services: Dict[str, Any],
    atomic_groups: Dict[str, Any],
    service_nodes_by_service: Dict[str, List[str]],
) -> List[str]:
    if selection_name in services:
        bind_nodes = service_nodes_by_service.get(selection_name)
        if bind_nodes is None:
            raise ValueError(f"missing service_nodes_by_service entry for selection={selection_name}")
        return bind_nodes
    group_cfg = atomic_groups.get(selection_name)
    if not isinstance(group_cfg, dict):
        raise ValueError(f"Unknown service or atomic group: {selection_name}")
    return _require_list_of_str(group_cfg.get("nodes"), f"atomic_groups.{selection_name}.nodes")


def resolve_selection_target_nodes(
    *,
    selection_name: str,
    services: Dict[str, Any],
    atomic_groups: Dict[str, Any],
    service_nodes_by_service: Dict[str, List[str]],
) -> List[str]:
    if selection_name in services:
        bind_nodes = resolve_selection_nodes(
            selection_name=selection_name,
            services=services,
            atomic_groups=atomic_groups,
            service_nodes_by_service=service_nodes_by_service,
        )
        atomic_service_nodes = atomic_group_service_nodes(atomic_groups=atomic_groups)
        return [
            node_name
            for node_name in bind_nodes
            if (selection_name, node_name) not in atomic_service_nodes
        ]
    return resolve_selection_nodes(
        selection_name=selection_name,
        services=services,
        atomic_groups=atomic_groups,
        service_nodes_by_service=service_nodes_by_service,
    )


def resolve_coverage_selection_name(
    *,
    service_name: str,
    node_name: str,
    atomic_groups: Dict[str, Any],
) -> str:
    for atomic_group_name, raw_group_cfg in atomic_groups.items():
        if not isinstance(raw_group_cfg, dict):
            raise ValueError(f"atomic_groups.{atomic_group_name} must be a mapping")
        service_names = _require_list_of_str(
            raw_group_cfg.get("services"),
            f"atomic_groups.{atomic_group_name}.services",
        )
        if service_name not in service_names:
            continue
        group_nodes = _require_list_of_str(raw_group_cfg.get("nodes"), f"atomic_groups.{atomic_group_name}.nodes")
        if node_name in group_nodes:
            return atomic_group_name
    return service_name


def _require_list_of_str(raw: Any, field_name: str) -> List[str]:
    if not isinstance(raw, list):
        raise ValueError(f"{field_name} must be a list")
    out: List[str] = []
    for index, item in enumerate(raw):
        out.append(_require_str(item, f"{field_name}[{index}]"))
    return out


def _require_str(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return raw.strip()
