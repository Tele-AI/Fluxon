"""Installed Fluxon Quick Start entrypoints."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Optional, Sequence


_DEFAULT_EXPORT_NAME = "quick-start-export"


def _validate_export_name(export_name: str) -> str:
    if not isinstance(export_name, str):
        raise TypeError(
            f"export_name must be a string, got {type(export_name).__name__}"
        )
    if (
        not 3 <= len(export_name) <= 63
        or export_name.startswith("-")
        or export_name.endswith("-")
        or any(
            not (character.isascii() and (character.islower() or character.isdigit()))
            and character != "-"
            for character in export_name
        )
    ):
        raise ValueError(
            "export_name must be a valid S3 bucket name: 3-63 lowercase ASCII "
            "letters, digits, or hyphens, without a leading or trailing hyphen"
        )
    return export_name


def _copy_config(config: dict[str, Any], *, name: str) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise TypeError(f"{name} must be a dict, got {type(config).__name__}")
    return deepcopy(config)


def _required_string(config: dict[str, Any], key: str, *, path: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}.{key} must be a non-empty string")
    return value


def _required_string_list(config: dict[str, Any], key: str, *, path: str) -> tuple[str, ...]:
    value = config.get(key)
    if not isinstance(value, list) or not value:
        raise ValueError(f"{path}.{key} must be a non-empty list")
    if any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{path}.{key} must contain non-empty strings")
    return tuple(value)


def _required_dict(config: dict[str, Any], key: str, *, path: str) -> dict[str, Any]:
    value = config.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{path}.{key} must be a dict")
    return value


def _validate_monitoring_config(master: dict[str, Any]) -> None:
    monitoring = _required_dict(master, "monitoring", path="kv_master_config")
    _required_string(
        monitoring,
        "prometheus_base_url",
        path="kv_master_config.monitoring",
    )
    _required_string_list(
        monitoring,
        "prom_remote_write_url",
        path="kv_master_config.monitoring",
    )
    otlp_log_api = _required_dict(
        monitoring,
        "otlp_log_api",
        path="kv_master_config.monitoring",
    )
    for key in ("otlp_endpoint", "db_name", "table_name"):
        _required_string(
            otlp_log_api,
            key,
            path="kv_master_config.monitoring.otlp_log_api",
        )


def _prepare_kv_configs(
    kv_master_config: dict[str, Any],
    kv_owner_config: dict[str, Any],
    *,
    require_local_etcd: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], int, int]:
    master = _copy_config(kv_master_config, name="kv_master_config")
    owner = _copy_config(kv_owner_config, name="kv_owner_config")
    _validate_monitoring_config(master)
    owner_spec = owner.get("fluxonkv_spec")
    if not isinstance(owner_spec, dict):
        raise ValueError("kv_owner_config.fluxonkv_spec must be a dict")

    master_cluster = _required_string(master, "cluster_name", path="kv_master_config")
    owner_cluster = _required_string(
        owner_spec,
        "cluster_name",
        path="kv_owner_config.fluxonkv_spec",
    )
    if master_cluster != owner_cluster:
        raise ValueError(
            "kv_master_config.cluster_name must match "
            "kv_owner_config.fluxonkv_spec.cluster_name"
        )

    master_endpoints = _required_string_list(
        master,
        "etcd_endpoints",
        path="kv_master_config",
    )
    owner_endpoints = _required_string_list(
        owner_spec,
        "etcd_addresses",
        path="kv_owner_config.fluxonkv_spec",
    )
    if master_endpoints != owner_endpoints:
        raise ValueError(
            "kv_master_config.etcd_endpoints must match "
            "kv_owner_config.fluxonkv_spec.etcd_addresses"
        )
    if len(master_endpoints) != 1:
        raise ValueError("serve_s3_single_node requires exactly one etcd endpoint")

    endpoint = master_endpoints[0]
    host, separator, port_text = endpoint.rpartition(":")
    if separator == "" or not host:
        raise ValueError(
            "serve_s3_single_node requires the etcd endpoint to use host:<port>"
        )
    if require_local_etcd and host not in {"127.0.0.1", "localhost"}:
        raise ValueError(
            "serve_s3_single_node requires the etcd endpoint to use 127.0.0.1:<port> "
            "or localhost:<port>"
        )
    try:
        etcd_port = int(port_text)
    except ValueError as exc:
        raise ValueError("serve_s3_single_node etcd endpoint port must be an integer") from exc
    if not 1 <= etcd_port <= 65535:
        raise ValueError("serve_s3_single_node etcd endpoint port must be in [1, 65535]")

    master_port = master.get("port")
    if (
        isinstance(master_port, bool)
        or not isinstance(master_port, int)
        or not 1 <= master_port <= 65535
    ):
        raise ValueError("kv_master_config.port must be an integer in [1, 65535]")
    return master, owner, etcd_port, master_port


def main(
    argv: Optional[Sequence[str]] = None,
    *,
    fs_kv_master_config: Optional[dict[str, Any]] = None,
    fs_kv_owner_config: Optional[dict[str, Any]] = None,
) -> None:
    from .start import main as _main

    _main(
        argv,
        fs_kv_master_config=fs_kv_master_config,
        fs_kv_owner_config=fs_kv_owner_config,
    )


def serve_s3_single_node(
    fs_root: str | Path,
    state_root: str | Path,
    *,
    kv_master_config: dict[str, Any],
    kv_owner_config: dict[str, Any],
    export_name: str = _DEFAULT_EXPORT_NAME,
    panel_port: int = 26180,
    greptime_http_port: int = 24000,
    start_middleware: bool = True,
    greptime_base_url: str | None = None,
) -> None:
    """Expose ``fs_root`` as the ``export_name`` S3 bucket on one node."""

    export_name = _validate_export_name(export_name)
    master, owner, etcd_client_port, kv_master_port = _prepare_kv_configs(
        kv_master_config,
        kv_owner_config,
        require_local_etcd=start_middleware,
    )
    if start_middleware and greptime_base_url is not None:
        raise ValueError(
            "greptime_base_url is only valid when start_middleware is False"
        )
    if not start_middleware and greptime_base_url is None:
        greptime_base_url = f"http://127.0.0.1:{greptime_http_port}"
    fs_root = Path(fs_root).expanduser().resolve()
    state_root = Path(state_root).expanduser().resolve()
    command = [
        "--mode",
        "fs",
        "--serve",
        "--fs-root",
        str(fs_root),
        "--export-name",
        export_name,
        "--workdir",
        str(state_root),
        "--panel-port",
        str(panel_port),
        "--etcd-client-port",
        str(etcd_client_port),
        "--kv-master-port",
        str(kv_master_port),
        "--greptime-http-port",
        str(greptime_http_port),
    ]
    if not start_middleware:
        command.extend(
            [
                "--external-middleware",
                "--greptime-base-url",
                str(greptime_base_url),
            ]
        )
    main(
        command,
        fs_kv_master_config=master,
        fs_kv_owner_config=owner,
    )


__all__ = ["main", "serve_s3_single_node"]
