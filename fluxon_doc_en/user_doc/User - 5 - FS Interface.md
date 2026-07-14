# User - 5 - FS Interface

## FS Interface

Fluxon FS mounts a directory from a remote machine into the current Python process. After the mount is installed, application code continues to use ordinary `open()`, `read()`, and `write()` calls.

### Four Names to Understand First

- `export`: A directory exposed by FS Agent.
- `EXPORT_NAME`: The logical name of that export.
- `REMOTE_ROOT_DIR`: The actual directory on the FS Agent machine.
- `mount_dir_abs`: The local mount directory in the Reader process.

Their relationship is:

```text
FS Agent exposes REMOTE_ROOT_DIR
              ↓
FS Master publishes EXPORT_NAME
              ↓
Reader mounts the export at mount_dir_abs
              ↓
       open() / read() / write()
```

FS Master manages export configuration, access control, and the management UI. FS Agent provides remote directory access. `FluxonFsPatcher` is installed in the Reader process and forwards operations under the mount directory to the corresponding FS Agent.

### Checks Before Starting

Confirm the following before running the examples:

- Greptime and etcd have been started as described in [User - 2 - Service Plane](<./User - 2 - Service Plane.md>).
- The current Python environment has installed `fluxon-*.whl` and `fluxon_pyo3-*.whl`; see [User - 0 - Installation](<./User - 0 - Installation.md>).
- The local example ports are free, and both `WORKDIR` and `REMOTE_ROOT_DIR` are writable.
- The `KvClient` basics are understood; see [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>).

Ordinary mounting and file I/O do not use directory-transfer state. PD and TiKV are needed only for `/ui/transfers/` or pre-scan.

### Choose the Scenario First

- **Complete local example**: Start a local KV Master, Owner Client, FS Master, and FS Agent. This is the recommended first-run path.
- **Attach to an existing cluster**: Start only a new local Owner Client and FS Agent, then attach to the existing KV Master and FS Master.
- **Directory transfer or pre-scan**: Prepare PD and TiKV after the basic FS services are working. This flow appears at the end of the page.

### Start the Local FS Services

By default, `examples/start_kv_and_fs_svc.py` starts:

```text
KV Master → Owner Client → FS Master → FS Agent
```

#### Values to Check on the First Run

- `CLUSTER_NAME`: Cluster name shared by KV and FS.
- `SHARE_MEM_PATH`: Shared-memory directory used by the local Owner Client and every local FS process.
- `FS_MASTER_INSTANCE_KEY`: FS Master identity and the target used by FS Agent and Reader when fetching configuration.
- `EXPORT_NAME`: Export name published by FS Agent and mounted by Reader.
- `REMOTE_ROOT_DIR`: Absolute directory exposed by the local FS Agent.
- `ADMIN_USERNAME` / `ADMIN_PASSWORD`: Administrator credentials used when creating an empty access DB.

Other instance identities, ports, `WORKDIR`, and cache sizes can remain unchanged for a first local run. `admin/admin` is suitable only for a local demo and must be changed in a real environment.

#### Startup Command and Success Conditions

Run in a separate terminal:

```bash
python3 examples/start_kv_and_fs_svc.py
```

After a successful startup:

- The terminal prints `cluster name`, `remote root dir`, `export name`, and log paths for all four roles.
- The final terminal line includes `waiting for Ctrl-C to stop fs demo stack`, and the process remains running.
- `/tmp/fluxon_fs_demo/remote_root` exists.
- FS Panel is available at `http://127.0.0.1:34180`.
- The four logs under `/tmp/fluxon_fs_demo/runtime/log/` contain no startup errors.

The full script is included below:

<details>
<summary><strong>📄 View full script (click to expand)</strong> | <code>examples/start_kv_and_fs_svc.py</code></summary>

```python
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
```

</details>

#### Attach to an Existing Cluster

On a remote Agent machine, run:

```bash
python3 examples/start_kv_and_fs_svc.py --without-master
```

Before starting, adjust:

- `ETCD_ENDPOINT` and `CLUSTER_NAME`: Point to the existing cluster.
- `FS_MASTER_INSTANCE_KEY`: Match the existing FS Master exactly.
- `SHARE_MEM_PATH`: Match the new Owner Client on the current machine.
- `OWNER_INSTANCE_KEY` and `FS_AGENT_INSTANCE_KEY`: Assign new values unique within the cluster.
- `EXPORT_NAME`: Assign a distinct name for this remote directory.
- `REMOTE_ROOT_DIR`: Use an absolute path on the current Agent machine.

This mode manages only the local Owner Client and FS Agent. It does not start KV Master, FS Master, or Panel.

### Remote Mount Read / Write Verification

The verification flow uses three scripts:

- `start_kv_and_fs_svc.py`: Start the service roles.
- `start_fluxon_fs_writer.py`: Register the export and continuously write a remote file and a local comparison file.
- `start_fluxon_fs_reader.py`: Mount the export and continuously read the remote and local comparison files.

#### Configuration Relationships

Writer and Reader both require:

- `-c/--config` pointing to a prepared environment YAML.
- `-w/--workdir` pointing to a writable directory owned by the current process.
- A `kvclient.instance_key` unique within the cluster.
- A `cluster_name` matching the FS services.
- A `share_mem_path` matching the Owner Client on the same machine.

In addition:

- The export name and remote root in Writer config must describe the target FS Agent.
- Reader `fluxon_fs.master.instance_key` must equal the target `FS_MASTER_INSTANCE_KEY`.
- Reader `export_name` must match the export registered by Writer and published by FS Master.
- Writer and Reader must use the same remote relative path.

This page does not embed complete Writer or Reader YAML. `-c` should point to configuration already validated for the current deployment environment.

#### Reader Mount Directory Requirements

`mount_dir_abs` must:

- Be an absolute path.
- Not be `/`.
- Be created by Fluxon if it does not exist.
- Be empty if it already exists.
- Not overlap another mount directory in the same process.

The mount does not need to live below `/fluxon_fs/`; `/tmp/fluxon_fs_demo/mount_demo` is also valid.

#### Start Writer

Keep the FS services running and execute in terminal two:

```bash
python3 examples/start_fluxon_fs_writer.py \
  -c <writer-config.yaml> \
  -w <writer-workdir>
```

After Writer starts successfully, it continually prints:

```text
[writer] op=write_remote ...
[writer] op=write_local ...
```

#### Start Reader

Execute in terminal three:

```bash
python3 examples/start_fluxon_fs_reader.py \
  -c <reader-config.yaml> \
  -w <reader-workdir>
```

After Reader mounts the export and finds both files, it continually prints:

```text
[reader] op=read_remote ...
[reader] op=read_local ...
```

This confirms that the remote export mount and local cache rule are both active.

> **Current example limitation**: `start_fluxon_fs_reader.py` does not yet call `set_request_identity(...)` and does not read a username or password from YAML. With an access-controlled FS Master, application code must set a request identity explicitly or remote file operations fail because the authentication token is missing.

### `FluxonFsPatcher` Call Order

The recommended Reader flow is:

```text
new_store(...)
→ install_patcher_from_master(...)
→ wait_cache_config_loaded()
→ set_request_identity(...)
→ mount_remote_dir(...)
→ open() / read() / write()
→ patcher.uninstall()
→ store.close()
```

Key rules:

- `FluxonFsPatcher` depends on the `KvClient` returned by `new_store(...)`.
- `install_patcher_from_master(...)` fetches export configuration from FS Master and installs the Patcher.
- `set_request_identity(username, password)` sets the identity for later FS requests.
- Call `patcher.uninstall()` before closing `store`.

To control configuration loading manually:

- `load_cache_config_from_master_config_file(config_path)`: Read the FS Master identity from the file and block until configuration is fetched successfully.
- `start_cache_config_fetch_from_master_config_file(config_path)`: Continuously fetch configuration from FS Master in the background. `install_patcher_from_master(...)` already calls this API.
- `set_cache_config_yaml(...)`: Inject export and cache configuration directly when no FS Master fetch is used.

Ordinary Readers should prefer `install_patcher_from_master(...)` instead of composing these APIs themselves.

### Advanced Configuration

#### Panel and Access Control

- `FS_PANEL_LISTEN_ADDR` determines the local interface and port used by FS Master.
- `FS_PANEL_PUBLIC_BASE_URL` is the public address used by browsers and generated links. For remote access, use a reachable hostname or IP address.
- `FS_MASTER_ACCESS_DB_PATH` stores users, passwords, and permissions and should be a stable writable absolute path.
- `bootstrap_access_model` seeds the first accounts only when the access DB has no users.
- After users exist in the database, changing `ADMIN_USERNAME` or `ADMIN_PASSWORD` does not overwrite them.
- An administrator with `can_manage_users: true` can access every current export at runtime.

#### Logging

The service script writes child-process standard output to:

- `WORKDIR/log/kv_master.log`
- `WORKDIR/log/owner.log`
- `WORKDIR/log/fs_master.log`
- `WORKDIR/log/fs_agent.log`

Writer and Reader Python logs go to the current terminal by default. To increase logging:

```bash
FLUXON_LOG=DEBUG python3 examples/start_fluxon_fs_reader.py \
  -c <reader-config.yaml> \
  -w <reader-workdir>
```

#### Common Errors

- **`new_store failed`**: Check that the service script is still running and that `cluster_name` and `share_mem_path` match the local Owner Client.
- **`unknown export_name`**: Check that Reader `export_name` was registered by FS Agent and published by FS Master.
- **Invalid mount directory**: Check that the path is absolute and empty and does not overlap an existing mount.
- **`fluxon_fs cache config is not loaded yet`**: Check the FS Master identity and confirm that configuration fetching has completed.
- **`permission denied` / `PermissionError`**: Check that `set_request_identity(...)` was called and that the credentials and access DB permissions are correct.

### Optional: Directory Transfer and Pre-Scan

Directory transfer is designed for large directory migrations across machines or shared-storage systems. It continuously records scan progress, batch counts, active workers, and live bandwidth.

This feature depends on the TiKV `transfer_state_store`. Start PD and TiKV as described in [User - 2 - Service Plane](<./User - 2 - Service Plane.md>) before continuing.

#### Start a Directory Transfer from the UI

In the dual-pane page:

1. Locate the source folder on the left.
2. Locate the target export and directory on the right.
3. Drag the source folder to the right pane.
4. Set `desired_worker_count` and `batch_ready_bytes`.
5. Submit and inspect the job under `/ui/transfers/`.

`FluxonFS Transfer Jobs` displays scan progress, batch counts, active workers, and live bandwidth.

#### Import a Pre-Scan

The `Pre-Scans` area under `/ui/transfers/` can import an existing scan as a transfer job:

1. Find the pre-scan record and click `Import`.
2. Select the source export and target export.
3. Enter the target prefix and `desired_worker_count`.
4. Submit and inspect it under `FluxonFS Transfer Jobs`.

#### TiKV Namespace

FS Master and the standalone pre-scan process must use the same:

- `pd_endpoints`
- `key_prefix`

The examples on this page consistently use:

```yaml
transfer_state_store:
  kind: tikv
  tikv:
    pd_endpoints:
      - "127.0.0.1:12379"
    key_prefix: "/fluxon_fs_transfer/demo-fs-cluster/"
```

#### Standalone Pre-Scan Example

A standalone pre-scan needs only PD and TiKV. KV Master, Owner Client, FS Master, and FS Agent do not need to be running. When FS Master later starts with the same `pd_endpoints` and `key_prefix`, the UI can display the pre-scan result.

<details>
<summary><strong>📄 View full pre-scan example (click to expand)</strong></summary>

```python
#!/usr/bin/env python3

from fluxon_py.fluxon_fs import (
    FluxonFsTransferSkipEntry,
    FluxonFsTransferSkipEntryKind,
    FluxonFsTransferStateStoreConfig,
    FluxonFsTransferStateStoreKind,
    FluxonFsTransferStateStoreTiKvConfig,
    transfer_check_local_blocking,
)

STORE = FluxonFsTransferStateStoreConfig(
    kind=FluxonFsTransferStateStoreKind.TIKV,
    tikv=FluxonFsTransferStateStoreTiKvConfig(
        pd_endpoints=["127.0.0.1:12379"],
        key_prefix="/fluxon_fs_transfer/demo-fs-cluster/",
    ),
)

summary = transfer_check_local_blocking(
    src_root_dir="/data/demo_src",
    transfer_state_store=STORE,
    batch_ready_bytes=8 * 1024 * 1024 * 1024,
    skip_entries=[
        FluxonFsTransferSkipEntry(
            kind=FluxonFsTransferSkipEntryKind.DIR,
            relpath="tmp",
        ),
        FluxonFsTransferSkipEntry(
            kind=FluxonFsTransferSkipEntryKind.FILE,
            relpath="logs/debug.txt",
        ),
    ],
    checker_concurrency_limit=4,
    enable_cli_progress=True,
)

print(summary)
```

</details>

Set `src_root_dir` to an existing source directory before running. The most useful fields in `summary` are `job_id`, `scan_epoch`, and `batch_count`.
