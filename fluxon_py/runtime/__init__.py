"""Stable runtime entrypoints for deploy, CI, and installed-wheel bring-up."""

import importlib
from typing import Any

__all__ = [
    "run_kv_master_blocking",
    "run_kv_master_service_blocking",
    "start_kv_master_process",
    "start_kv_master_process_with_config_b64",
    "run_broker_blocking",
    "run_broker_service_blocking",
    "start_broker_process",
    "start_broker_process_with_config_b64",
    "run_owner_kvclient_blocking",
    "run_owner_kvclient_service_blocking",
    "start_owner_kvclient_process",
    "start_owner_kvclient_process_with_config_b64",
    "run_ops_agent_blocking",
    "run_ops_agent_service_blocking",
    "run_ops_controller_blocking",
    "run_ops_controller_service_blocking",
    "run_fs_master_blocking",
    "run_fs_master_service_blocking",
    "run_fs_transfer_check_blocking",
    "run_fs_transfer_check_service_blocking",
    "start_fs_master_process",
    "start_fs_master_process_with_config_b64",
    "start_fs_transfer_check_process",
    "start_fs_transfer_check_process_with_config_b64",
    "run_fs_agent_blocking",
    "run_fs_agent_service_blocking",
    "start_fs_agent_process",
    "start_fs_agent_process_with_config_b64",
    "register_ctrlc_callback",
    "wait_subproc_or_ctrlc",
]

_LAZY_RUNTIME_EXPORTS = {
    "run_kv_master_blocking": ("start_master", "run_kv_master_blocking"),
    "run_kv_master_service_blocking": ("start_master", "run_kv_master_service_blocking"),
    "start_kv_master_process": ("start_master", "start_kv_master_process"),
    "start_kv_master_process_with_config_b64": ("start_master", "start_kv_master_process_with_config_b64"),
    "run_broker_blocking": ("start_broker", "run_kv_broker_blocking"),
    "run_broker_service_blocking": ("start_broker", "run_kv_broker_service_blocking"),
    "start_broker_process": ("start_broker", "start_kv_broker_process"),
    "start_broker_process_with_config_b64": ("start_broker", "start_kv_broker_process_with_config_b64"),
    "run_owner_kvclient_blocking": ("start_owner_kvclient", "run_owner_kvclient_blocking"),
    "run_owner_kvclient_service_blocking": ("start_owner_kvclient", "run_owner_kvclient_service_blocking"),
    "start_owner_kvclient_process": ("start_owner_kvclient", "start_owner_kvclient_process"),
    "start_owner_kvclient_process_with_config_b64": ("start_owner_kvclient", "start_owner_kvclient_process_with_config_b64"),
    "run_ops_agent_blocking": ("start_ops_agent", "run_ops_agent_blocking"),
    "run_ops_agent_service_blocking": ("start_ops_agent", "run_ops_agent_service_blocking"),
    "run_ops_controller_blocking": ("start_ops_controller", "run_ops_controller_blocking"),
    "run_ops_controller_service_blocking": ("start_ops_controller", "run_ops_controller_service_blocking"),
    "run_fs_master_blocking": ("start_fs_master", "run_fs_master_blocking"),
    "run_fs_master_service_blocking": ("start_fs_master", "run_fs_master_service_blocking"),
    "run_fs_transfer_check_blocking": ("start_fs_master", "run_fs_transfer_check_blocking"),
    "run_fs_transfer_check_service_blocking": ("start_fs_master", "run_fs_transfer_check_service_blocking"),
    "start_fs_master_process": ("start_fs_master", "start_fs_master_process"),
    "start_fs_master_process_with_config_b64": ("start_fs_master", "start_fs_master_process_with_config_b64"),
    "start_fs_transfer_check_process": ("start_fs_master", "start_fs_transfer_check_process"),
    "start_fs_transfer_check_process_with_config_b64": ("start_fs_master", "start_fs_transfer_check_process_with_config_b64"),
    "run_fs_agent_blocking": ("start_fs_agent", "run_fs_agent_blocking"),
    "run_fs_agent_service_blocking": ("start_fs_agent", "run_fs_agent_service_blocking"),
    "start_fs_agent_process": ("start_fs_agent", "start_fs_agent_process"),
    "start_fs_agent_process_with_config_b64": ("start_fs_agent", "start_fs_agent_process_with_config_b64"),
    "register_ctrlc_callback": ("process_runner", "register_ctrlc_callback"),
    "wait_subproc_or_ctrlc": ("process_runner", "wait_subproc_or_ctrlc"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_RUNTIME_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _LAZY_RUNTIME_EXPORTS[name]
    module = importlib.import_module(f"{__name__}.{module_name}")
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(list(globals().keys()) + list(__all__)))
