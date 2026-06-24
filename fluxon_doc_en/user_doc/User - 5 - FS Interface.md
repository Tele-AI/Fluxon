# User - 5 - FS Interface

## Overview

Fluxon FS lets you mount a remote export into the current Python process and keep using `open() / read() / write()` semantics.

The core objects are:

- KV service-plane objects: `etcd`, `greptime`, `master`, `owner`
- FS role objects: `fs_master`, `fs_agent`
- In-process mount objects: `FluxonKvClientConfig`, `new_store(...)`, `FluxonFsPatcher`, `mount_remote_dir(...)`

```text
etcd + greptime + fluxonkv master + owner
                       |
                       v
               fluxon_fs master
                       |
                       v
               fluxon_fs agent
                       |
                       v
FluxonKvClientConfig -> new_store(...) -> KvClient(store)
                       |
                       v
FluxonFsPatcher(store)
                       |
                       +-- set_master_config_yaml(...)
                       +-- set_cache_config_yaml(...)
                       +-- set_request_identity(...)
                       +-- install()
                       +-- mount_remote_dir(...)
                       |
                       v
open() / read() / write() / close()
```

See [Architecture and Concepts](<./User - 1 - Architecture and Concepts.md>) for the role model and [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>) for `FluxonKvClientConfig` and `new_store(...)`.

## Service Plane

FS depends on the KV service plane and then adds two more roles on top:

1. `greptime`
2. `etcd`
3. `fluxonkv master`
4. `owner`
5. `fs master`
6. `fs agent`
7. your mount verification script

`examples/start_kv_and_fs_svc.py` only starts Fluxon-native roles. `etcd` and `greptime` still follow [User - 2 - Service Plane](<./User - 2 - Service Plane.md>). If you need `/ui/transfers/` and pre-scan, start the TiKV PD / TiKV pair for `transfer_state_store` first.

## `fs_master` and `fs_agent`

After the KV service plane is ready, FS adds two roles:

- `fs_master`: attaches to the KV plane as an external client and owns panel / export snapshot distribution
- `fs_agent`: registers exports to `fs_master` and exposes remote directory access

The reference script is `examples/start_kv_and_fs_svc.py`.

Start it with:

```bash
python3 examples/start_kv_and_fs_svc.py
python3 examples/start_kv_and_fs_svc.py --without-master
```

Default mode starts `kv master + owner + fs master + fs agent`. `--without-master` only starts `owner + fs_agent` and expects the cluster's `kv master` and `fs master` to already exist elsewhere.

Most important remote-agent constraints:

- `ETCD_ENDPOINT` must point at the real cluster etcd endpoint
- `FS_MASTER_INSTANCE_KEY` must match the existing `fs master`
- `OWNER_INSTANCE_KEY`, `FS_AGENT_INSTANCE_KEY`, `EXPORT_NAME`, and `REMOTE_ROOT_DIR` must be unique per agent machine
- `FS_PANEL_PUBLIC_BASE_URL` controls external links shown by the UI, while `FS_PANEL_LISTEN_ADDR` only controls the bind address

Default mode collects subprocess `stdout/stderr` into `WORKDIR/log` and keeps only summary output in the terminal.

## Remote Mount Read / Write Verification

The public FS verification flow under `examples/` is:

- `examples/start_kv_and_fs_svc.py`
- `examples/start_fluxon_fs_writer.py`
- `examples/start_fluxon_fs_reader.py`

Minimum success path:

1. run `python3 examples/start_kv_and_fs_svc.py`
2. keep it running
3. prepare writer config and run `python3 examples/start_fluxon_fs_writer.py -c <writer-config.yaml> -w <writer-workdir>`
4. prepare reader config and run `python3 examples/start_fluxon_fs_reader.py -c <reader-config.yaml> -w <reader-workdir>`

The reader side always does three things:

- attach to the local owner through one external client
- install the patcher through `install_patcher_from_master(...)`
- mount the selected export and alternate between remote and local reads

Once the reader keeps printing `op=read_remote` / `op=read_local`, the remote mount chain and local cache rules are both working.

## Directory Transfer and Pre-Scan

Directory transfer and pre-scan are designed for long-running large-folder jobs such as cross-cluster migration or migration across shared-storage domains.

The main user-facing UI is `/ui/transfers/`, which exposes:

- `Pre-Scans`
- `FluxonFS Transfer Jobs`

Typical direct-transfer flow from the UI:

1. open two panes
2. locate the source folder on the left
3. locate the target export and target directory on the right
4. drag the folder across panes
5. fill `desired_worker_count` and `batch_ready_bytes`
6. submit and inspect the job in `/ui/transfers/`

Typical pre-scan import flow:

1. open `/ui/transfers/`
2. find the record in `Pre-Scans`
3. click `Import`
4. choose `source export`
5. choose `target export`
6. fill `target prefix`
7. fill `desired_worker_count`
8. submit

### TiKV Config for Transfer State

Directory transfer and pre-scan both depend on `transfer_state_store`. The `fs master` panel and any standalone pre-scan process must share the same TiKV namespace.

The most important fields are:

- `pd_endpoints`
- `key_prefix`

The `start_kv_and_fs_svc.py` example uses:

- `TRANSFER_STATE_STORE_PD_ENDPOINTS = ["127.0.0.1:12379"]`
- `TRANSFER_STATE_STORE_KEY_PREFIX = "/fluxon_fs_transfer/demo-fs-cluster/"`

`fs master` needs:

```yaml
transfer_state_store:
  kind: tikv
  tikv:
    pd_endpoints:
      - "127.0.0.1:12379"
    key_prefix: "/fluxon_fs_transfer/demo-fs-cluster/"
```

Standalone pre-scan code must use the same values.

### Standalone Pre-Scan Example

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
        key_prefix="/fluxon_fs_transfer/demo_prescan/",
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

`summary` is most useful for `job_id`, `scan_epoch`, and `batch_count`.

## `FluxonFsPatcher`

`FluxonFsPatcher` is not a standalone public entrypoint. It must sit on top of the `store` returned by `new_store(...)`.

Required order:

1. `store = new_store(cfg)...`
2. `patcher = FluxonFsPatcher(store)`
3. `patcher.set_master_config_yaml(...)`
4. `patcher.set_cache_config_yaml(...)`
5. `patcher.set_request_identity(...)`
6. `patcher.install()`
7. `patcher.mount_remote_dir(...)`
8. `open()` / `read()` / `write()`
9. `patcher.uninstall()`
10. `store.close()`

Do not close `store` before `patcher.uninstall()`.

### Config Injection

- `set_master_config_yaml(...)`: injects `fluxon_fs.master.instance_key`
- `set_cache_config_yaml(...)`: injects the current export snapshot
- `set_request_identity(username, password)`: binds later FS requests to one identity

User-facing examples should set identity explicitly instead of depending on an implicit unauthenticated path.

### `bootstrap_access_model`

`bootstrap_access_model` is a required startup-time seed for an empty `access_db`.

```yaml
fluxon_fs:
  master_panel:
    access_db_path: /path/to/access.db
    bootstrap_access_model:
      users:
        - username: admin
          password: admin
          can_manage_users: true
      scope_access: []
```

Rules:

- `access_db_path` is the long-lived authority
- `bootstrap_access_model` must be provided explicitly in startup config
- it only writes when `access_db` has no users yet
- once users already exist, restarts follow the database state
- `can_manage_users: true` grants runtime access to all current exports without writing synthetic root scopes

## Mount Directory Rules

`mount_remote_dir(local_mount_dir_abs=..., export_name=...)` requires:

- an absolute path
- not `/`
- if the directory does not exist, Fluxon creates it
- if the directory already exists, it must be empty
- it must not overlap with another mount path in the same process

## Logging

For more Python-side logs:

```bash
FLUXON_LOG=DEBUG python3 examples/start_fluxon_fs_reader.py -c <reader-config.yaml> -w <reader-workdir>
```

Common levels:

- `DEBUG`
- `INFO`
- `WARNING`
- `ERROR`
- `CRITICAL`

## Common Errors

### `new_store failed`

Usually means the external client did not attach to the local owner. Check:

- whether `start_kv_and_fs_svc.py` is still running
- `CLUSTER_NAME`
- `SHARE_MEM_PATH`

### `fluxon_fs cache config is not loaded yet`

Usually means `set_cache_config_yaml(...)` did not complete successfully, or the client-side cache config does not match the current server export config. Check:

- `FS_MASTER_INSTANCE_KEY`
- `EXPORT_NAME`
- `REMOTE_ROOT_DIR`

### `unknown export_name`

The client is trying to mount an `EXPORT_NAME` that does not exist in the current `fs master` export snapshot. Check:

- whether writer and reader use the same `export_name`
- whether `REMOTE_ROOT_DIR` matches the export definition

### `permission denied` or `PermissionError`

The path exists but the current identity does not have access. Check:

- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- whether the current `access_db` was already overwritten by newer user data

If the admin password was changed through the UI or the database, the old `bootstrap_access_model` password no longer applies.
