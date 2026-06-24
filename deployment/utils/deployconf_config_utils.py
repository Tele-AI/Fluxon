from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple

import yaml

from deployment.utils.placeholder_utils import (
    build_mapping_for_cfg as _ph_build_mapping,
    resolve_values_or_raise as _ph_resolve_or_raise,
    svc_ip_port_from_mapping as _ph_svc_ip_port_from_mapping,
)

__all__ = [
    "load_deployconf_mapping",
    "load_deployconf_resolved_global_envs",
    "load_deployconf_etcd_address",
    "load_deployconf_prometheus_base_url",
    "load_deployconf_prom_remote_write_url",
    "load_deployconf_fluxon_cluster_name",
    "load_deployconf_fluxon_share_mem_path",
    "load_deployconf_service_ip_port",
]


def _load_yaml_mapping(config_path: Path, *, label: str) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing {label}: {config_path}")
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if loaded is None:
        raise ValueError(f"{label} is empty: {config_path}")
    if not isinstance(loaded, dict):
        raise ValueError(f"{label} must be a YAML mapping: {config_path}")
    return loaded


def _verify_host_port(value: Any, *, field: str) -> Tuple[str, int]:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, e.g. '127.0.0.1:9090'")
    raw = value.strip()
    if not raw:
        raise ValueError(f"{field} must not be empty, e.g. '127.0.0.1:9090'")
    if "://" in raw:
        raise ValueError(f"{field} must not include a scheme like 'http://'; expected 'host:port'")

    if raw.startswith("["):
        end = raw.find("]:")
        if end == -1:
            raise ValueError(f"{field} IPv6 format should look like '[::1]:9090'")
        host = raw[1:end]
        port_str = raw[end + 2 :]
    else:
        if raw.count(":") != 1:
            raise ValueError(f"{field} expected 'host:port', e.g. 127.0.0.1:9090")
        host, port_str = raw.split(":", 1)

    if not host:
        raise ValueError(f"{field} hostname must not be empty")
    try:
        port = int(port_str)
    except Exception:
        raise ValueError(f"{field} port must be an integer, e.g. 127.0.0.1:9090")
    if not (1 <= port <= 65535):
        raise ValueError(f"{field} port out of range (1-65535)")
    return host, port


def _verify_url(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, e.g. http://127.0.0.1:9090/api/v1/write")
    raw = value.strip()
    if not raw:
        raise ValueError(f"{field} must not be empty, e.g. http://127.0.0.1:9090/api/v1/write")
    from urllib.parse import urlparse as _urlparse

    parsed = _urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(
            f"{field} must be a full URL starting with http/https, e.g. http://127.0.0.1:9090/api/v1/write"
        )
    if parsed.port is None:
        raise ValueError(f"{field} must explicitly include a port, e.g. http://127.0.0.1:9090/api/v1/write")
    if not parsed.path or parsed.path == "/":
        raise ValueError(f"{field} must include a path, e.g. http://127.0.0.1:9090/api/v1/write")
    return raw


def load_deployconf_mapping(*, config_path: Path) -> Dict[str, Any]:
    return _load_yaml_mapping(config_path, label="deployconf")


def load_deployconf_resolved_global_envs(*, config_path: Path) -> Dict[str, Any]:
    """Resolve deployconf global_envs with deployment placeholder rules."""
    cfg = load_deployconf_mapping(config_path=config_path)
    cluster_nodes = cfg.get("cluster_nodes")
    if not isinstance(cluster_nodes, list) or not cluster_nodes:
        raise ValueError("deployconf.cluster_nodes must be a non-empty list")
    services = cfg.get("service")
    if not isinstance(services, dict) or not services:
        raise ValueError("deployconf.service must be a non-empty mapping")
    global_envs = cfg.get("global_envs")
    if not isinstance(global_envs, dict):
        raise ValueError("deployconf.global_envs must be a mapping")
    mapping = _ph_build_mapping(cluster_nodes=cluster_nodes, services=services)
    return _ph_resolve_or_raise(global_envs, mapping, label="deployconf.global_envs")


def load_deployconf_etcd_address(*, config_path: Path) -> str:
    global_envs = load_deployconf_resolved_global_envs(config_path=config_path)
    raw = global_envs.get("ETCD_FULL_ADDRESS")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("deployconf.global_envs.ETCD_FULL_ADDRESS must resolve to a non-empty string")
    addr = raw.strip()
    _verify_host_port(addr, field="deployconf.global_envs.ETCD_FULL_ADDRESS")
    return addr


def load_deployconf_prometheus_base_url(*, config_path: Path) -> str:
    global_envs = load_deployconf_resolved_global_envs(config_path=config_path)
    raw = global_envs.get("FLUXON_PROMETHEUS_BASE_URL")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("deployconf.global_envs.FLUXON_PROMETHEUS_BASE_URL must resolve to a non-empty string")
    return _verify_url(raw.strip(), field="deployconf.global_envs.FLUXON_PROMETHEUS_BASE_URL")


def load_deployconf_prom_remote_write_url(*, config_path: Path) -> str:
    global_envs = load_deployconf_resolved_global_envs(config_path=config_path)
    raw = global_envs.get("MONITOR_GREPTIMEDB_WRITE_URL")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("deployconf.global_envs.MONITOR_GREPTIMEDB_WRITE_URL must resolve to a non-empty string")
    return _verify_url(raw.strip(), field="deployconf.global_envs.MONITOR_GREPTIMEDB_WRITE_URL")


def load_deployconf_fluxon_cluster_name(*, config_path: Path) -> str:
    global_envs = load_deployconf_resolved_global_envs(config_path=config_path)
    raw = global_envs.get("FLUXON_CLUSTER_NAME")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("deployconf.global_envs.FLUXON_CLUSTER_NAME must resolve to a non-empty string")
    return raw.strip()


def load_deployconf_fluxon_share_mem_path(*, config_path: Path) -> str:
    global_envs = load_deployconf_resolved_global_envs(config_path=config_path)
    raw = global_envs.get("FLUXON_SHARED_MEM")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("deployconf.global_envs.FLUXON_SHARED_MEM must resolve to a non-empty string")
    return raw.strip()


def load_deployconf_service_ip_port(*, config_path: Path, service_name: str) -> Tuple[str, int]:
    cfg = load_deployconf_mapping(config_path=config_path)
    cluster_nodes = cfg.get("cluster_nodes")
    if not isinstance(cluster_nodes, list) or not cluster_nodes:
        raise ValueError("deployconf.cluster_nodes must be a non-empty list")
    services = cfg.get("service")
    if not isinstance(services, dict) or not services:
        raise ValueError("deployconf.service must be a non-empty mapping")
    mapping = _ph_build_mapping(cluster_nodes=cluster_nodes, services=services)
    ip_port = _ph_svc_ip_port_from_mapping(mapping, service_name)
    if ip_port is None:
        raise ValueError(f"failed to resolve ip:port for deployconf service {service_name!r}")
    return ip_port
