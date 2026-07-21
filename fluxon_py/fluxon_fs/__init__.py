from typing import Any

from .config_types import (
    CacheMode,
    FluxonFsExport,
    FluxonFsExportRoutingMode,
    FluxonFsExportRpcPaths,
    FluxonFsGlobalConfig,
    FluxonFsMasterConfig,
    FluxonFsMasterPanelConfig,
    FluxonFsRule,
    OnRefreshError,
    WriteMode,
    extract_global_config_yaml_from_file,
    export_rpc_paths_for_export_name_v1,
    export_to_json_text,
    parse_global_config_from_yaml_text,
    parse_master_config_from_file,
    parse_master_panel_config_from_file,
)
from .patcher import FluxonFsPatcher
from .bootstrap import install_patcher_from_master
from .video import (
    FluxonFsVideoReadRequest,
    FluxonFsVideoReadResult,
    FluxonFsVideoReader,
    FluxonFsVideoReaderPool,
)


def publish_export(
    *,
    kv_store: Any,
    target_instance_key: str,
    schema_version: int,
    export_name: str,
    export: FluxonFsExport,
) -> None:
    inner = getattr(kv_store, "_client", None)
    if inner is None:
        raise RuntimeError(
            "fluxon_fs publish_export requires a fluxon backend store exposing _client (fluxon_pyo3.KvClient)"
        )
    import fluxon_pyo3  # type: ignore

    result = fluxon_pyo3.fluxon_fs_agent_publish_export(
        inner,
        target_instance_key,
        int(schema_version),
        export_name,
        export_to_json_text(export),
    )
    if not result.is_ok():
        raise RuntimeError(f"fluxon_fs publish_export failed: {result.unwrap_error()}")
    _ = result.unwrap()


def unpublish_export(
    *,
    kv_store: Any,
    target_instance_key: str,
    schema_version: int,
    export_name: str,
) -> None:
    inner = getattr(kv_store, "_client", None)
    if inner is None:
        raise RuntimeError(
            "fluxon_fs unpublish_export requires a fluxon backend store exposing _client (fluxon_pyo3.KvClient)"
        )
    import fluxon_pyo3  # type: ignore

    result = fluxon_pyo3.fluxon_fs_agent_unpublish_export(
        inner,
        target_instance_key,
        int(schema_version),
        export_name,
    )
    if not result.is_ok():
        raise RuntimeError(f"fluxon_fs unpublish_export failed: {result.unwrap_error()}")
    _ = result.unwrap()


__all__ = [
    "FluxonFsMasterConfig",
    "FluxonFsMasterPanelConfig",
    "FluxonFsGlobalConfig",
    "FluxonFsRule",
    "FluxonFsExport",
    "FluxonFsExportRoutingMode",
    "FluxonFsExportRpcPaths",
    "CacheMode",
    "WriteMode",
    "OnRefreshError",
    "export_rpc_paths_for_export_name_v1",
    "extract_global_config_yaml_from_file",
    "parse_global_config_from_yaml_text",
    "parse_master_config_from_file",
    "parse_master_panel_config_from_file",
    "FluxonFsPatcher",
    "FluxonFsVideoReadRequest",
    "FluxonFsVideoReadResult",
    "FluxonFsVideoReader",
    "FluxonFsVideoReaderPool",
    "install_patcher_from_master",
    "publish_export",
    "unpublish_export",
]
