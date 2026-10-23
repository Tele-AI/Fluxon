#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import threading
from pathlib import Path
import subprocess
import sys
import sysconfig
import yaml

from fluxon_py import FluxonKvClientConfig, new_store

from .process_runner import (
    bind_current_process_parent_death_sigterm,
    register_ctrlc_callback,
    RuntimeConfigInput,
    decode_runtime_config_b64,
    encode_runtime_config_b64,
    resolve_runtime_config_path,
    start_python_module_process,
    start_python_module_process_with_config_b64,
)


OWNER_MODULE_NAME = "fluxon_py.runtime.start_owner_kvclient"
SIDE_TRANSFER_WORKER_PYTHON_ENV = "FLUXON_KV_SIDE_WORKER_PYTHON"
OWNER_RUNTIME_CONFIG_FILENAME = "owner_kvclient.runtime.yaml"


def _resolve_side_transfer_worker_python() -> str:
    configured = os.environ.get(SIDE_TRANSFER_WORKER_PYTHON_ENV)
    if configured:
        return configured

    prefix = Path(sys.prefix)
    candidates = []
    scripts_dir = sysconfig.get_path("scripts")
    if scripts_dir:
        candidates.append(Path(scripts_dir))
    if prefix.as_posix() not in {"", "."}:
        candidates.append(prefix / "bin")

    seen: set[str] = set()
    for bin_dir in candidates:
        key = str(bin_dir)
        if key in seen:
            continue
        seen.add(key)
        for name in (
            f"python{sys.version_info.major}.{sys.version_info.minor}",
            f"python{sys.version_info.major}",
            "python3",
            "python",
        ):
            candidate = bin_dir / name
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)

    return sys.executable


def _load_owner_runtime_config(
    config_yaml: str,
) -> FluxonKvClientConfig:
    if not config_yaml.strip():
        raise ValueError("config yaml is empty")
    config = yaml.safe_load(config_yaml)
    if not isinstance(config, dict):
        raise TypeError(f"kvclient config must decode to dict, got {type(config).__name__}")
    return FluxonKvClientConfig(config)


def _new_owner_client(config_yaml: str) -> object:
    config = _load_owner_runtime_config(config_yaml)
    result = new_store(config)
    if not result.is_ok():
        raise RuntimeError(f"new_store failed: {result.unwrap_error()}")
    return result.unwrap()


def main() -> None:
    bind_current_process_parent_death_sigterm()
    parser = argparse.ArgumentParser(description="Start Fluxon owner kvclient (blocking)")
    parser.add_argument("-c", "--config", type=Path, required=False, help="Path to kvclient YAML config")
    parser.add_argument("-w", "--workdir", type=Path, required=False, help="Working directory")
    parser.add_argument("--config-b64", required=False, help="Base64-encoded YAML config")
    args = parser.parse_args()
    if args.config_b64 is not None:
        run_owner_kvclient_service_blocking_from_yaml_text(
            config_yaml=decode_runtime_config_b64(args.config_b64)
        )
        return
    if args.config is None or args.workdir is None:
        raise ValueError("--config and --workdir are required when --config-b64 is not used")
    run_owner_kvclient_blocking(config=args.config, workdir=args.workdir)


def run_owner_kvclient_blocking(
    *,
    workdir: Path,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
) -> None:
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=OWNER_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    resolved_workdir.mkdir(parents=True, exist_ok=True)
    os.chdir(resolved_workdir)
    run_owner_kvclient_service_blocking(config_path=resolved_config)


def run_owner_kvclient_service_blocking(*, config_path: Path) -> None:
    config_yaml = config_path.read_text(encoding="utf-8")

    os.environ.setdefault(
        SIDE_TRANSFER_WORKER_PYTHON_ENV,
        _resolve_side_transfer_worker_python(),
    )

    _client = _new_owner_client(config_yaml)
    _wait_until_stopped(_client)


def run_owner_kvclient_service_blocking_from_yaml_text(*, config_yaml: str) -> None:
    os.environ.setdefault(
        SIDE_TRANSFER_WORKER_PYTHON_ENV,
        _resolve_side_transfer_worker_python(),
    )

    _client = _new_owner_client(config_yaml)
    _wait_until_stopped(_client)


def _wait_until_stopped(client: object) -> None:
    shutdown_requested = threading.Event()

    def _on_ctrlc(reason: str) -> None:
        if shutdown_requested.is_set():
            return
        shutdown_requested.set()
        print(f"[fluxon_owner_kvclient] caught {reason}, closing store")

    restore_ctrlc = register_ctrlc_callback(_on_ctrlc, thread_name="fluxon-owner-kvclient-signal")
    try:
        while not shutdown_requested.wait(0.5):
            pass
    finally:
        restore_ctrlc()
        close_res = client.close()
        if not close_res.is_ok():
            raise RuntimeError(f"store close failed: {close_res.unwrap_error()}")
        close_res.unwrap()


def start_owner_kvclient_process(
    *,
    workdir: Path | None = None,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    if config_path is None and isinstance(config, dict):
        return start_owner_kvclient_process_with_config_b64(config=config, log_path=log_path)
    if workdir is None:
        raise ValueError("workdir is required when config is not a dict and config_path is not provided")
    resolved_workdir = workdir.resolve()
    resolved_config = resolve_runtime_config_path(
        workdir=resolved_workdir,
        runtime_config_filename=OWNER_RUNTIME_CONFIG_FILENAME,
        config=config,
        config_path=config_path,
    )
    return start_python_module_process(
        module_name=OWNER_MODULE_NAME,
        config_path=resolved_config,
        workdir=resolved_workdir,
        extra_cli_args=(),
        log_path=log_path,
    )


def start_owner_kvclient_process_with_config_b64(
    *,
    config: dict,
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    return start_python_module_process_with_config_b64(
        module_name=OWNER_MODULE_NAME,
        config_b64=encode_runtime_config_b64(config),
        extra_cli_args=(),
        log_path=log_path,
    )


if __name__ == "__main__":
    main()
