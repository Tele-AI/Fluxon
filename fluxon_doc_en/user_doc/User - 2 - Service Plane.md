# User - 2 - Service Plane

<!-- Maintenance note: Keep the minimum KV path centered on Greptime, etcd, Master, and Owner Client. PD and TiKV are optional dependencies for persistent FS transfer state. MQ and FS feature details belong to their own pages and should only be linked from here. -->

## Service Plane

This page explains how to start the Fluxon KV service processes and which settings matter on the first run. The minimum KV path needs only `Greptime`, `etcd`, `Master`, and `Owner Client`. TiKV is needed only by features that persist task state, such as FS directory transfer and pre-scan.

### Choose the Startup Scenario First

- **Minimum local KV**: Start `Greptime`, `etcd`, `Master`, and `Owner Client` on the local machine. This is the recommended path for a first run.
- **Attach to an existing Master**: If `Greptime`, `etcd`, and `Master` are already running in the cluster, start only a new local `Owner Client`.
- **Enable FS directory transfer or pre-scan**: Start PD and TiKV after the KV service plane is running. TiKV can be skipped when using only KV, RPC, or MQ.

### Components and Startup Order

The recommended local startup order is:

```text
Greptime → etcd → Master → Owner Client → application process new_store(...)
```

Each component has one clear responsibility:

- `Greptime` receives monitoring data reported by Master.
- `etcd` stores control-plane metadata such as membership, routes, and leases.
- `Master` manages the KV cluster.
- `Owner Client` provides the shared-memory pool on the current machine and creates the `shared.json` used by application processes.
- PD and TiKV persist task state for features such as FS directory transfer and pre-scan. They are not part of the minimum KV read/write path.

The deployment layout is shown below:

![](../../pics/deploy_arch_1.png)

`Greptime`, `etcd`, PD, and TiKV are external dependencies and must be started separately. `fluxon_py.runtime` starts only Fluxon-native roles such as `Master` and `Owner Client`.

### Shortest Local Startup Path

Prepare the runtime package described in [User - 0 - Installation](<./User - 0 - Installation.md>) and confirm that the files required by the minimum KV path exist:

- `ext_images/greptime/greptime`
- `ext_images/greptime/start.sh`
- `ext_images/etcd/etcd`
- `ext_images/etcd/etcdctl`
- `ext_images/etcd/start.sh`

Run the following commands from the Fluxon repository root. Each startup command is long-running, so use a separate terminal or `tmux` window for each component and confirm that it starts successfully before continuing.

#### Understand `--config` and `--workdir` First

Each external-service startup script accepts two arguments:

- `--config/-c` determines how the component starts. The file must define the component's Bash array, such as `GREPTIME_ARGS` or `ETCD_ARGS`.
- `--workdir/-w` determines where the component stores its local data and logs. Each instance should have its own writable work directory.

Inside the config file, `WORKDIR` is the directory passed through `--workdir`. For example:

```text
--workdir /tmp/fluxon_service_plane_demo/etcd
--data-dir "$WORKDIR/etcd-data"
```

The resulting data directory is `/tmp/fluxon_service_plane_demo/etcd/etcd-data`. For a first local run, confirm only that the work directories are writable and the example ports are free. The single-node membership settings can normally remain unchanged.

#### 1. Start Greptime

**Purpose**: Provide monitoring query and write endpoints for Master.

**First-run checks**:

- Port `34030` in `--http-addr 0.0.0.0:34030` is free.
- The work directory used by `--data-home "$WORKDIR/greptimedb"` is writable.

Configuration and startup command:

```bash
cat > /tmp/greptime.config.sh <<'EOF'
GREPTIME_ARGS=(
  standalone start
  --data-home "$WORKDIR/greptimedb"
  --http-addr 0.0.0.0:34030
)
EOF

bash ./ext_images/greptime/start.sh \
  --config /tmp/greptime.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/greptime
```

**Success condition**: The process remains running, the terminal shows no startup error, and local port `34030` accepts connections. The later Master example uses this port by default.

<details>
<summary><strong>🌐 Multi-machine deployment</strong> | <code>--http-addr</code></summary>

`0.0.0.0` means that Greptime listens on every local network interface. It is not an address that another machine should use as a destination. Set `GREPTIME_BASE_URL` in the Master config to a real hostname or IP address reachable from the Master host.

</details>

#### 2. Start etcd

**Purpose**: Store the control-plane metadata used by Master and Owner Client.

**First-run checks**:

- `--advertise-client-urls "http://127.0.0.1:2379"` corresponds to `ETCD_ENDPOINT = "127.0.0.1:2379"` in the later Python script.
- Ports `2379` and `2380` are free.
- The work directory used by `--data-dir "$WORKDIR/etcd-data"` is writable.

`--name`, the peer addresses, and `--initial-cluster` define etcd membership. The values below can remain unchanged for a local single-node deployment.

Configuration and startup command:

```bash
cat > /tmp/etcd.config.sh <<'EOF'
ETCD_ARGS=(
  --data-dir "$WORKDIR/etcd-data"
  --name etcd0
  --advertise-client-urls "http://127.0.0.1:2379"
  --listen-client-urls "http://0.0.0.0:2379"
  --listen-peer-urls "http://0.0.0.0:2380"
  --initial-advertise-peer-urls "http://127.0.0.1:2380"
  --initial-cluster "etcd0=http://127.0.0.1:2380"
  --initial-cluster-token "etcd-cluster"
  --initial-cluster-state "new"
  --auto-compaction-retention=1
)
EOF

bash ./ext_images/etcd/start.sh \
  --config /tmp/etcd.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/etcd
```

**Success condition**: The health endpoint returns `"health":"true"`.

```bash
curl -sS http://127.0.0.1:2379/health
```

<details>
<summary><strong>🌐 Multi-machine deployment</strong> | etcd listen and member addresses</summary>

- `--listen-client-urls` and `--listen-peer-urls` determine which local interfaces etcd listens on.
- `--advertise-client-urls` is the address used by clients such as Master and Owner Client.
- `--initial-advertise-peer-urls` is the address used by other etcd members.
- `--name` must match its member name in `--initial-cluster`, and every member address must be reachable between hosts.

</details>

#### 3. Start Master and Owner Client

The two common `fluxon_py.runtime` entrypoints are:

- `start_kv_master_process(config=...)`
- `start_owner_kvclient_process(config=...)`

By default, `examples/start_master_owner.py` starts a local Master and Owner Client together. Only the following fields need to be understood before the first run:

- `ETCD_ENDPOINT`: The etcd endpoint used by Master and Owner Client. Use `host:port` without `http://`. Keep `127.0.0.1:2379` when following the local etcd example above.
- `CLUSTER_NAME`: The logical cluster name. Master, Owner Client, and later application processes must use the same value.
- `SHARE_MEM_PATH`: The local shared-memory directory. Owner Client and later application processes on the same machine must use the same path.
- `MASTER_INSTANCE_KEY`: The Master instance identity. It must be unique within the cluster.
- `OWNER_INSTANCE_KEY`: The Owner Client instance identity. It must be unique within the cluster.
- `OWNER_DRAM_BYTES`: The number of DRAM bytes contributed by Owner Client. The default is `1073741824`, or 1 GiB. The value must be greater than zero and satisfy the capacity-alignment requirement.

When Greptime and etcd use the default ports above and local port `31000` is free, `GREPTIME_HTTP_PORT`, `MASTER_PORT`, and the example directories can remain unchanged.

The script supports two startup modes:

- **Start a local Master and Owner Client**:

  ```bash
  python3 examples/start_master_owner.py
  ```

- **Start only Owner Client and attach to an existing Master**:

  ```bash
  python3 examples/start_master_owner.py --without-master
  ```

  In this mode, `ETCD_ENDPOINT` and `CLUSTER_NAME` must match the existing cluster. `OWNER_INSTANCE_KEY` must be a new value, and `SHARE_MEM_PATH` must be a local path on the current machine.

**Success condition**: The script remains running, the terminal prints `waiting for Ctrl-C`, and `shared.json` appears under `SHARE_MEM_PATH`. With the default config, check it with:

```bash
ls -l /dev/shm/fluxon_kv_demo/shared.json
```

Master and Owner Client standard output is written to `/tmp/fluxon_kv_demo/runtime/log/master.log` and `/tmp/fluxon_kv_demo/runtime/log/owner.log`, respectively. Check these files if startup fails.

The full script is included below for running directly from a source checkout:

<details>
<summary><strong>📄 View full script (click to expand)</strong> | <code>examples/start_master_owner.py</code></summary>

```python
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
    children = []
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
            "large_file_paths": [str((WORKDIR / "large" / "owner").resolve())],
        },
    }


if __name__ == "__main__":
    main()
```

</details>

When running from a source checkout, this script can be used directly. With a `wheel` installation, call the two `fluxon_py.runtime` entrypoints from the application startup program and pass Python dicts directly instead of depending on the `examples/` directory.

The example uses `wait_subproc_or_ctrlc(...)` to wait for all child processes. Pressing Ctrl-C stops the Master and Owner Client processes that it started.

### Optional: Start PD and TiKV for FS Directory Transfer

Skip this section when using only KV, RPC, or MQ. When FS directory transfer or pre-scan needs `transfer_state_store`, use this startup order:

```text
PD → TiKV → FS Master
```

Confirm that the runtime package provides these files:

- `ext_images/tikv/pd-server`
- `ext_images/tikv/tikv-server`
- `ext_images/tikv/start_pd.sh`
- `ext_images/tikv/start_tikv.sh`

The PD and TiKV scripts use the same `--config/-c` and `--workdir/-w` arguments.

#### 1. Start PD

**Purpose**: Manage the TiKV cluster and provide the cluster entrypoint used by TiKV and FS `transfer_state_store`.

**First-run checks**:

- `12379` is the client port and `12380` is the PD member port. Both ports must be free.
- The work directory used by `--data-dir` and `--log-file` is writable.
- The config array is named `PD_ARGS`.

The member name, peer addresses, and `--initial-cluster` can remain unchanged for a local single-node deployment.

Configuration and startup command:

```bash
cat > /tmp/pd.config.sh <<'EOF'
PD_ARGS=(
  --name pd0
  --data-dir "$WORKDIR/pd-data"
  --client-urls "http://127.0.0.1:12379"
  --advertise-client-urls "http://127.0.0.1:12379"
  --peer-urls "http://127.0.0.1:12380"
  --advertise-peer-urls "http://127.0.0.1:12380"
  --initial-cluster "pd0=http://127.0.0.1:12380"
  --log-file "$WORKDIR/pd.log"
)
EOF

bash ./ext_images/tikv/start_pd.sh \
  --config /tmp/pd.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/tikv_pd
```

**Success condition**: The PD process remains running and the members endpoint responds successfully.

```bash
curl -sS http://127.0.0.1:12379/pd/api/v1/members
```

#### 2. Start TiKV

**Purpose**: Persist task state for FS directory transfer and pre-scan.

**First-run checks**:

- PD is already running, and `--pd-endpoints "127.0.0.1:12379"` matches the PD client address above.
- `20160` is the TiKV service port and `20180` is the status and metrics port. Both ports must be free.
- The work directory used by `--data-dir` and `--log-file` is writable.
- The config array is named `TIKV_ARGS`.

Configuration and startup command:

```bash
cat > /tmp/tikv.config.sh <<'EOF'
TIKV_ARGS=(
  --pd-endpoints "127.0.0.1:12379"
  --addr "127.0.0.1:20160"
  --advertise-addr "127.0.0.1:20160"
  --status-addr "127.0.0.1:20180"
  --data-dir "$WORKDIR/tikv-data"
  --log-file "$WORKDIR/tikv.log"
)
EOF

bash ./ext_images/tikv/start_tikv.sh \
  --config /tmp/tikv.config.sh \
  --workdir /tmp/fluxon_service_plane_demo/tikv
```

**Success condition**: The TiKV process remains running, its log has no PD connection or storage-initialization errors, and local port `20160` accepts connections.

<details>
<summary><strong>🌐 Multi-machine deployment</strong> | published PD and TiKV addresses</summary>

- PD `--advertise-client-urls` must be reachable from TiKV and FS Master. `transfer_state_store.pd_endpoints` uses this client address.
- PD `--advertise-peer-urls` and `--initial-cluster` use addresses reachable between PD members.
- TiKV `--pd-endpoints` must point to a running PD, and `--advertise-addr` must be reachable from other machines.
- `--client-urls`, `--peer-urls`, `--addr`, and `--status-addr` determine where each process listens. Do not use bind-only `0.0.0.0` as a remote destination.

</details>

### Advanced Configuration

The following settings can remain unchanged for a first local run. Adjust them only when ports, directories, or deployment hosts change.

#### Custom Monitoring Address and Master Port

- `GREPTIME_HTTP_PORT` is the Greptime HTTP port and must match Greptime `--http-addr`.
- `GREPTIME_BASE_URL` is the Greptime address reachable from Master, in `http://host:port` form.
- The example derives three endpoints from `GREPTIME_BASE_URL`: Prometheus queries use `/v1/prometheus`, remote write uses `/v1/prometheus/write`, and OTLP logs use `/v1/otlp/v1/logs`. These paths stay unchanged when the host or port changes.
- `MASTER_PORT` is the local Master listen port. It is unused when the script starts only Owner Client. When a local Master is started, the port must not conflict with another process.

#### Logs and Large-File Directories

> **Note**: `log_path` stores child-process standard output and standard error captured by the Python startup script. `log_dir` is Master's own business-log and profile directory. `large_file_paths` contains the directories where Owner Client stores backend logs, caches, and large-file data. These fields have different purposes.

The example derives these paths:

- `WORKDIR/log/master.log` and `WORKDIR/log/owner.log`: Passed as `log_path` for startup troubleshooting.
- `WORKDIR/master_logs`: Passed as `log_dir` in the Master config.
- `WORKDIR/large/owner`: Passed as Owner Client `fluxonkv_spec.large_file_paths`. Owner Client mode requires at least one writable directory.

Each process group should use its own writable `WORKDIR`. When one startup program manages several roles, assign a different `log_path` to each role.

#### Multi-Machine Address, Path, and Port Planning

For a multi-machine deployment, check the following items in order:

1. Every Master, Owner Client, and application process uses the same `CLUSTER_NAME`.
2. Every Master and Owner Client has a unique `instance_key`.
3. Each machine has its own `WORKDIR`. `SHARE_MEM_PATH` may differ between machines, but Owner Client and application processes on the same machine must use the same value.
4. Ports must not conflict with other processes on the same host.
5. `listen` fields, or fields without `advertise`, determine where a process listens locally. `advertise` fields determine the address used by other machines.
6. Every etcd, Greptime, PD, and TiKV address supplied to another machine must be reachable. Do not use remote-inaccessible `127.0.0.1` or bind-only `0.0.0.0`.

For the full KV configuration objects and field semantics, see [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>).

### Where to Go Next

- When writing Python KV APIs or making node-to-node RPC calls, see [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>).
- When using Producer / Consumer messaging, see [User - 4 - MQ Interface](<./User - 4 - MQ Interface.md>).
- When starting FS Master / Agent, registering exports, or mounting remote directories, see [User - 5 - FS Interface](<./User - 5 - FS Interface.md>).
