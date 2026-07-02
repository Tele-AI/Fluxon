#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
import yaml

from fluxon_py.tool import import_fluxon_pyo3_local

from .process_runner import (
    bind_current_process_parent_death_sigterm,
    build_runtime_singleton_spec,
    RuntimeConfigInput,
    decode_runtime_config_b64,
    encode_runtime_config_b64,
    resolve_runtime_config_path,
    run_singleton_process,
    start_python_module_process,
    start_python_module_process_with_config_b64,
)


BROKER_MODULE_NAME = "fluxon_py.runtime.start_broker"
STOP_EXISTING_BROKER_TIMEOUT_SECONDS = 30
BROKER_RUNTIME_CONFIG_FILENAME = "kv_broker.runtime.yaml"


def run_kv_broker_blocking(
    *,
    workdir: Path,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
) -> None:
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=BROKER_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    singleton_spec = build_runtime_singleton_spec(
        module_name=BROKER_MODULE_NAME,
        entrypoint_path=Path(__file__),
        workdir=workdir,
    )
    run_singleton_process(
        config_path=resolved_config,
        singleton_spec=singleton_spec,
        stop_timeout_seconds=STOP_EXISTING_BROKER_TIMEOUT_SECONDS,
        start_fn=lambda: run_kv_broker_service_blocking(
            config_path=resolved_config,
            workdir=resolved_workdir,
        ),
    )


def run_kv_broker_service_blocking(*, config_path: Path, workdir: Path) -> None:
    fluxon_pyo3 = import_fluxon_pyo3_local()
    result = fluxon_pyo3.run_broker_blocking(str(config_path))
    if not result.is_ok():
        raise RuntimeError(f"run_broker_blocking failed: {result.unwrap_error()}")

    _ = result.unwrap()


def run_kv_broker_service_blocking_from_yaml_text(*, config_yaml: str) -> None:
    config = yaml.safe_load(config_yaml)
    if not isinstance(config, dict):
        raise TypeError(f"broker config must decode to dict, got {type(config).__name__}")
    fluxon_pyo3 = import_fluxon_pyo3_local()
    result = fluxon_pyo3.run_broker_blocking(config)
    if not result.is_ok():
        raise RuntimeError(f"run_broker_blocking failed: {result.unwrap_error()}")

    _ = result.unwrap()


def start_kv_broker_process(
    *,
    workdir: Path | None = None,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    if config_path is None and isinstance(config, dict) and workdir is None:
        return start_kv_broker_process_with_config_b64(config=config, log_path=log_path)
    if workdir is None:
        raise ValueError("workdir is required when config is not a dict and config_path is not provided")
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=BROKER_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    return start_python_module_process(
        module_name=BROKER_MODULE_NAME,
        config_path=resolved_config,
        workdir=resolved_workdir,
        extra_cli_args=(),
        log_path=log_path,
    )


def start_kv_broker_process_with_config_b64(
    *,
    config: dict,
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    return start_python_module_process_with_config_b64(
        module_name=BROKER_MODULE_NAME,
        config_b64=encode_runtime_config_b64(config),
        extra_cli_args=(),
        log_path=log_path,
    )


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon KV broker (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=False, help="Path to broker YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=False, help="Working directory")
    parser.add_argument("--config-b64", required=False, help="Base64-encoded YAML config")
    args = parser.parse_args()
    if args.config_b64 is not None:
        # Keep the same config transport contract as other runtime entrypoints.
        run_kv_broker_service_blocking_from_yaml_text(
            config_yaml=decode_runtime_config_b64(args.config_b64)
        )
        return
    if args.config is None or args.workdir is None:
        raise ValueError("--config and --workdir are required when --config-b64 is not used")
    run_kv_broker_blocking(config=args.config, workdir=args.workdir)


if __name__ == "__main__":
    main()
