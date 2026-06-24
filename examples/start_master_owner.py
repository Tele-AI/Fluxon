#!/usr/bin/env python3

import argparse
from pathlib import Path

from fluxon_py.runtime import (
    start_kv_master_process,
    start_owner_kvclient_process,
    wait_subproc_or_ctrlc,
)
from fluxon_py.runtime.process_runner import ManagedSubprocess

ETCD_ENDPOINT = "127.0.0.1:2379"
GREPTIME_HTTP_PORT = 34030
GREPTIME_BASE_URL = f"http://127.0.0.1:{GREPTIME_HTTP_PORT}"
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = Path("/dev/shm/fluxon_kv_demo").resolve()
WORKDIR = Path("/tmp/fluxon_kv_demo/runtime").resolve()
MASTER_PORT = 31000
MASTER_INSTANCE_KEY = "demo_kv_master"
OWNER_INSTANCE_KEY = "demo_kv_owner"
OWNER_DRAM_BYTES = 1073741824


def build_owner_large_file_paths() -> list[str]:
    return [str((WORKDIR / "large" / "owner").resolve())]


def main() -> None:
    args = parse_args()
    log_dir = (WORKDIR / "log").resolve()

    if args.with_master:
        master_log_dir = (WORKDIR / "master_logs").resolve()
        master_log_dir.mkdir(parents=True, exist_ok=True)
        master_stdout_log = log_dir / "master.log"
        master_proc = start_kv_master_process(
            config=build_master_config(log_dir=master_log_dir),
            log_path=master_stdout_log,
        )
    else:
        master_stdout_log = None
        master_proc = None

    owner_stdout_log = log_dir / "owner.log"
    owner_proc = start_owner_kvclient_process(
        config=build_owner_config(),
        log_path=owner_stdout_log,
    )
    children: list[ManagedSubprocess] = []
    if master_proc is not None:
        children.append(
            ManagedSubprocess(
                label="master",
                proc=master_proc,
            )
        )
    children.append(
        ManagedSubprocess(
            label="owner",
            proc=owner_proc,
        )
    )

    print(f"[fluxon_kv] share_mem_path: {SHARE_MEM_PATH}")
    print(f"[fluxon_kv] etcd endpoint: {ETCD_ENDPOINT}")
    print(f"[fluxon_kv] greptime base url: {GREPTIME_BASE_URL}")
    print(f"[fluxon_kv] start master in this script: {args.with_master}")
    if master_stdout_log is not None:
        print(f"[fluxon_kv] master stdout log: {master_stdout_log}")
    else:
        print("[fluxon_kv] master stdout log: disabled by --without-master")
    print(f"[fluxon_kv] owner stdout log: {owner_stdout_log}")
    stack_label = "master and owner" if args.with_master else "owner"
    print(f"[fluxon_kv] waiting for Ctrl-C to stop {stack_label}")
    wait_subproc_or_ctrlc(
        children,
        on_ctrlc=lambda: print(f"[fluxon_kv] caught Ctrl-C, stopping {stack_label}"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start KV demo owner, optionally with a local master")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--with-master",
        dest="with_master",
        action="store_true",
        help="Start a local kv master in this script (default)",
    )
    group.add_argument(
        "--without-master",
        dest="with_master",
        action="store_false",
        help="Do not start a local kv master; only start owner and attach to an existing cluster master",
    )
    parser.set_defaults(with_master=True)
    return parser.parse_args()


def build_master_config(*, log_dir: Path) -> dict:
    return {
        "instance_key": MASTER_INSTANCE_KEY,
        "cluster_name": CLUSTER_NAME,
        "port": MASTER_PORT,
        "etcd_endpoints": [ETCD_ENDPOINT],
        "log_dir": str(log_dir),
        "monitoring": {
            "prometheus_base_url": f"{GREPTIME_BASE_URL}/v1/prometheus",
            "prom_remote_write_url": [f"{GREPTIME_BASE_URL}/v1/prometheus/write"],
            "otlp_log_api": {
                "otlp_endpoint": f"{GREPTIME_BASE_URL}/v1/otlp/v1/logs",
            },
        },
    }


def build_owner_config() -> dict:
    return {
        "instance_key": OWNER_INSTANCE_KEY,
        "contribute_to_cluster_pool_size": {
            "dram": OWNER_DRAM_BYTES,
            "vram": {},
        },
        "fluxonkv_spec": {
            "etcd_addresses": [ETCD_ENDPOINT],
            "cluster_name": CLUSTER_NAME,
            "share_mem_path": str(SHARE_MEM_PATH),
            "sub_cluster": "default",
            "large_file_paths": build_owner_large_file_paths(),
        },
    }


if __name__ == "__main__":
    main()
