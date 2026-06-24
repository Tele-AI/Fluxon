#!/usr/bin/env python3

import argparse
from pathlib import Path

from fluxon_py.runtime import (
    start_fs_agent_process,
    start_fs_master_process,
    start_kv_master_process,
    start_owner_kvclient_process,
    wait_subproc_or_ctrlc,
)
from fluxon_py.runtime.process_runner import ManagedSubprocess

ETCD_ENDPOINT = "127.0.0.1:2379"
GREPTIME_HTTP_PORT = 34030
GREPTIME_BASE_URL = f"http://127.0.0.1:{GREPTIME_HTTP_PORT}"
CLUSTER_NAME = "demo-fs-cluster"
SHARE_MEM_PATH = Path("/dev/shm/fluxon_fs_demo").resolve()
WORKDIR = Path("/tmp/fluxon_fs_demo/runtime").resolve()
REMOTE_ROOT_DIR = Path("/tmp/fluxon_fs_demo/remote_root").resolve()
KV_MASTER_PORT = 34100
FS_PANEL_PORT = 34180
FS_PANEL_LISTEN_ADDR = f"0.0.0.0:{FS_PANEL_PORT}"
FS_PANEL_PUBLIC_BASE_URL = f"http://127.0.0.1:{FS_PANEL_PORT}"
KV_MASTER_INSTANCE_KEY = "demo_fs_kv_master"
OWNER_INSTANCE_KEY = "demo_fs_owner"
FS_MASTER_INSTANCE_KEY = "demo_fs_master"
FS_AGENT_INSTANCE_KEY = "demo_fs_agent"
EXPORT_NAME = "demo-export"
OWNER_DRAM_BYTES = 1073741824
EXPORT_CACHE_MAX_BYTES = 1073741824
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"
TRANSFER_STATE_STORE_PD_ENDPOINTS = ["127.0.0.1:12379"]
TRANSFER_STATE_STORE_KEY_PREFIX = f"/fluxon_fs_transfer/{CLUSTER_NAME}/"
FS_MASTER_ACCESS_DB_PATH = (WORKDIR / "fs_master" / "access.db").resolve()


def build_owner_large_file_paths() -> list[str]:
    return [str((WORKDIR / "large" / "owner").resolve())]


def main() -> None:
    args = parse_args()
    WORKDIR.mkdir(parents=True, exist_ok=True)
    REMOTE_ROOT_DIR.mkdir(parents=True, exist_ok=True)

    log_dir = (WORKDIR / "log").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.with_master:
        kv_master_log_dir = (WORKDIR / "kv_master_logs").resolve()
        kv_master_log_dir.mkdir(parents=True, exist_ok=True)
        kv_master_stdout_log = (log_dir / "kv_master.log").resolve()
        # FS master persists panel auth state in this sqlite file, so the parent
        # directory must exist before Rust opens access_db_path.
        FS_MASTER_ACCESS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        fs_master_stdout_log = (log_dir / "fs_master.log").resolve()
        # FS depends on the KV service plane, so bring up KV roles before FS roles.
        kv_master_proc = start_kv_master_process(
            config=build_kv_master_config(log_dir=kv_master_log_dir),
            log_path=kv_master_stdout_log,
        )
    else:
        kv_master_stdout_log = None
        fs_master_stdout_log = None
        kv_master_proc = None
        fs_master_proc = None

    owner_stdout_log = (log_dir / "owner.log").resolve()
    owner_proc = start_owner_kvclient_process(
        config=build_owner_config(),
        log_path=owner_stdout_log,
    )

    if args.with_master:
        fs_master_proc = start_fs_master_process(
            config=build_fs_master_config(),
            log_path=fs_master_stdout_log,
        )

    fs_agent_stdout_log = (log_dir / "fs_agent.log").resolve()
    fs_agent_proc = start_fs_agent_process(
        config=build_fs_agent_config(),
        log_path=fs_agent_stdout_log,
    )
    children: list[ManagedSubprocess] = []
    if kv_master_proc is not None:
        children.append(
            ManagedSubprocess(
                label="kv_master",
                proc=kv_master_proc,
            )
        )
    children.append(
        ManagedSubprocess(
            label="owner",
            proc=owner_proc,
        )
    )
    if fs_master_proc is not None:
        children.append(
            ManagedSubprocess(
                label="fs_master",
                proc=fs_master_proc,
            )
        )
    # Stop order is the reverse of this list, so append fs_agent last.
    children.append(
        ManagedSubprocess(
            label="fs_agent",
            proc=fs_agent_proc,
        )
    )

    print(f"[fluxon_fs] cluster name: {CLUSTER_NAME}")
    print(f"[fluxon_fs] share_mem_path: {SHARE_MEM_PATH}")
    print(f"[fluxon_fs] remote root dir: {REMOTE_ROOT_DIR}")
    print(f"[fluxon_fs] export name: {EXPORT_NAME}")
    print(f"[fluxon_fs] owner instance key: {OWNER_INSTANCE_KEY}")
    print(f"[fluxon_fs] fs master instance key: {FS_MASTER_INSTANCE_KEY}")
    print(f"[fluxon_fs] fs agent instance key: {FS_AGENT_INSTANCE_KEY}")
    print(f"[fluxon_fs] start masters in this script: {args.with_master}")
    if args.with_master:
        print(f"[fluxon_fs] panel listen addr: {FS_PANEL_LISTEN_ADDR}")
        print(f"[fluxon_fs] panel public base url: {FS_PANEL_PUBLIC_BASE_URL}")
        print(f"[fluxon_fs] transfer state store pd_endpoints: {TRANSFER_STATE_STORE_PD_ENDPOINTS}")
        print(f"[fluxon_fs] transfer state store key_prefix: {TRANSFER_STATE_STORE_KEY_PREFIX}")
        print(f"[fluxon_fs] bootstrap admin username: {ADMIN_USERNAME}")
        print(f"[fluxon_fs] bootstrap admin password: {ADMIN_PASSWORD}")
        print(f"[fluxon_fs] kv master stdout log: {kv_master_stdout_log}")
        print(f"[fluxon_fs] fs master stdout log: {fs_master_stdout_log}")
    else:
        print("[fluxon_fs] panel listen addr: disabled by --without-master")
        print("[fluxon_fs] panel public base url: disabled by --without-master")
        print("[fluxon_fs] transfer state store pd_endpoints: disabled by --without-master")
        print("[fluxon_fs] transfer state store key_prefix: disabled by --without-master")
        print("[fluxon_fs] bootstrap admin username: disabled by --without-master")
        print("[fluxon_fs] bootstrap admin password: disabled by --without-master")
        print("[fluxon_fs] kv master stdout log: disabled by --without-master")
        print("[fluxon_fs] fs master stdout log: disabled by --without-master")
    print(f"[fluxon_fs] owner stdout log: {owner_stdout_log}")
    print(f"[fluxon_fs] fs agent stdout log: {fs_agent_stdout_log}")
    stack_label = "fs demo stack" if args.with_master else "owner and fs agent"
    print(f"[fluxon_fs] waiting for Ctrl-C to stop {stack_label}")
    wait_subproc_or_ctrlc(
        children,
        on_ctrlc=lambda: print(f"[fluxon_fs] caught Ctrl-C, stopping {stack_label}"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start FS demo roles, optionally with local masters")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--with-master",
        dest="with_master",
        action="store_true",
        help="Start local kv master and fs master in this script (default)",
    )
    group.add_argument(
        "--without-master",
        dest="with_master",
        action="store_false",
        help="Do not start any master in this script; only start owner and fs_agent and attach to an existing cluster",
    )
    parser.set_defaults(with_master=True)
    return parser.parse_args()


def build_kv_master_config(*, log_dir: Path) -> dict:
    return {
        "instance_key": KV_MASTER_INSTANCE_KEY,
        "cluster_name": CLUSTER_NAME,
        "port": KV_MASTER_PORT,
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


def build_fs_master_config() -> dict:
    return {
        "kvclient": {
            "instance_key": FS_MASTER_INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": str(SHARE_MEM_PATH),
            },
        },
        "fluxon_fs": {
            "master": {
                "instance_key": FS_MASTER_INSTANCE_KEY,
                "pull_interval_ms": 1000,
            },
            "master_panel": {
                "listen_addr": FS_PANEL_LISTEN_ADDR,
                "public_base_url": FS_PANEL_PUBLIC_BASE_URL,
                "auto_refresh_interval_secs": 2,
                "access_db_path": str(FS_MASTER_ACCESS_DB_PATH),
                # bootstrap_access_model only seeds an empty access_db; once the DB has users,
                # later restarts keep using the DB state instead of overwriting it from config.
                # Manager users keep full export access through runtime auth checks, not by writing
                # synthetic root scopes into the DB.
                "bootstrap_access_model": {
                    "users": [
                        {
                            "username": ADMIN_USERNAME,
                            "password": ADMIN_PASSWORD,
                            "can_manage_users": True,
                        }
                    ],
                    "scope_access": [],
                },
                "transfer_state_store": {
                    "kind": "tikv",
                    "tikv": {
                        "pd_endpoints": TRANSFER_STATE_STORE_PD_ENDPOINTS,
                        "key_prefix": TRANSFER_STATE_STORE_KEY_PREFIX,
                    },
                },
                "s3_gateway": {
                    "get_object_inflight_pieces": 8,
                    "kv_miss_policy": "remote_read",
                },
            },
            "cache": {
                "stale_window_ms": 1000,
                "rules": [],
                "exports": {
                    EXPORT_NAME: {
                        "remote_root_dir_abs": str(REMOTE_ROOT_DIR),
                        "cache_max_bytes": EXPORT_CACHE_MAX_BYTES,
                    },
                },
            },
        },
    }


def build_fs_agent_config() -> dict:
    return {
        "kvclient": {
            "instance_key": FS_AGENT_INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": str(SHARE_MEM_PATH),
            },
        },
        "fluxon_fs": {
            "master": {
                # The agent follows this master instance key to pull the current export snapshot.
                "instance_key": FS_MASTER_INSTANCE_KEY,
            },
            "cache": {
                "stale_window_ms": 1000,
                "rules": [],
                "exports": {
                    EXPORT_NAME: {
                        "remote_root_dir_abs": str(REMOTE_ROOT_DIR),
                        "cache_max_bytes": EXPORT_CACHE_MAX_BYTES,
                    },
                },
            },
        },
    }


if __name__ == "__main__":
    main()
