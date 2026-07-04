# User - 3 - KV and RPC Interface

## KV and RPC Interface

This page describes Fluxon's Python KV API and node-to-node RPC calls. Both are exposed from the same `KvClient` instance and share one lifecycle.

See [Architecture and Concepts](<./User - 1 - Architecture and Concepts.md>) for `cluster_name`, `instance_key`, `etcd`, and the other base concepts.

For business code, prefer passing a Python dict directly into `FluxonKvClientConfig(...)`. YAML is better suited to standalone processes, supervisors, deployment, and example environments.

### Service Plane

Before writing `put_blocking`, `get_blocking`, or `rpc_call`, start the KV service plane first. The shared role model, startup order, and runtime boundary are described in [User - 2 - Service Plane](<./User - 2 - Service Plane.md>).

![](../../pics/deploy_arch_1.png)

The most common objects are:

- `Greptime`: standard observability path
- `etcd`: KV control-plane metadata
- `start_kv_master_process(...)`: starts `Fluxon KV Master`
- `start_owner_kvclient_process(...)`: starts `Owner Client`

The minimal local startup example is `examples/start_master_owner.py`. It only starts Fluxon-native roles and assumes:

- `etcd` at `127.0.0.1:2379`
- `Greptime` HTTP at `127.0.0.1:34030`
- the current Python environment already installed `fluxon-*.whl` and `fluxon_pyo3-*.whl`

### Minimal Role Startup Example

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
MASTER_UI_PORT = 18080
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
        children.append(ManagedSubprocess(label="master", proc=master_proc))
    children.append(ManagedSubprocess(label="owner", proc=owner_proc))

    print(f"[fluxon_kv] share_mem_path: {SHARE_MEM_PATH}")
    print(f"[fluxon_kv] etcd endpoint: {ETCD_ENDPOINT}")
    print(f"[fluxon_kv] greptime base url: {GREPTIME_BASE_URL}")
    print(f"[fluxon_kv] start master in this script: {args.with_master}")
    if master_stdout_log is not None:
        print(f"[fluxon_kv] master stdout log: {master_stdout_log}")
        print(
            "[fluxon_kv] kv web ui: "
            f"http://<host-ip-or-domain>:{MASTER_UI_PORT}/view?cluster_name={CLUSTER_NAME}&member_kind=kv"
        )
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
    group.add_argument("--with-master", dest="with_master", action="store_true")
    group.add_argument("--without-master", dest="with_master", action="store_false")
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
        "master_ui": {
            "http_listen_addr": f"0.0.0.0:{MASTER_UI_PORT}",
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

Start it with:

```bash
python3 examples/start_master_owner.py
python3 examples/start_master_owner.py --without-master
```

### Lifecycle and Call Flow

```text
FluxonKvClientConfig
            |
            v
new_store(cfg) -> KvClient
     |               |
     |               +-- KV: put_blocking / get_blocking / remove / is_exist / ...
     |               |
     |               +-- RPC server: rpc_register(path, handler)
     |               |
     |               +-- RPC client: rpc_call(node_id, path, payload, timeout_ms)
     |
     v
close()
```

### Core Python Objects

- `FluxonKvClientConfig`: config object, usually built from a Python dict
- `new_store(config: FluxonKvClientConfig) -> Result[KvClient, ApiError]`: create one KV client
- `KvClient`: single entrypoint for both KV and RPC
- `KvClient.third_party_logs_dir() -> Result[str, ApiError]`: return the Fluxon-assigned log root for third-party Python components. Components should derive their own subdirectories under this root, for example `mq/`.
- `MemHolder`: successful result holder from `get_blocking(...)`
- `PutOptionalArgs`: optional write controls, most commonly `lease_id`

Notes:

- `MemHolder` does not expose `bytes()` directly; call `access()` and read the bytes field from the returned flat dict
- `store.close()` waits until all user-visible `MemHolder` references are dropped
- `Result` values must be consumed explicitly with `unwrap()` or `unwrap_error()`

### Data Model

Both KV values and RPC payloads use one flat-dict contract:

- `FlatDict = Dict[str, Union[int, float, bool, str, bytes, dlpack]]`

### Minimal KV Example

`examples/external_put_get_del.py`:

```python
#!/usr/bin/env python3

from fluxon_py import FluxonKvClientConfig, new_store

INSTANCE_KEY = "demo_kv_external"
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"


def main() -> None:
    cfg = FluxonKvClientConfig(
        {
            "instance_key": INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": SHARE_MEM_PATH,
            },
            "test_spec_config": {
                "disable_observability": True,
            },
        }
    )
    store = new_store(cfg).unwrap("new_store failed")

    key = "hello"
    value = b"world"

    try:
        store.put_blocking(key, {"payload": value}).unwrap("put_blocking failed")
        mem = store.get_blocking(key).unwrap("get_blocking failed")
        flat = mem.access().unwrap("mem.access failed")
        payload = flat["payload"]
        print(bytes(payload).decode("utf-8"))
        store.remove(key).unwrap("remove failed")
        exists = store.is_exist(key).unwrap("is_exist failed")
        if exists:
            raise RuntimeError("expected deleted key to be absent")
    finally:
        if "flat" in locals():
            del flat
        if "mem" in locals():
            del mem
        store.close().unwrap("close failed")


if __name__ == "__main__":
    main()
```

Useful calls:

- `put_blocking(key, value, opts=None)`: write or overwrite one KV object
- `get_blocking(key)`: return `MemHolder`
- `MemHolder.access()`: expand to `FlatDict`
- `get_size(key)`: query payload size without reading the whole object
- `is_exist(key)`: existence check
- `remove(key)`: delete a key
- `third_party_logs_dir()`: return `{large_file_paths[0]}/{cluster_name}_cluster_third_party_logs` as a `Result[str, ApiError]`

To increase user-process logs:

```bash
FLUXON_LOG=DEBUG python3 examples/external_put_get_del.py
```

Third-party Python components should place file logs under `store.third_party_logs_dir().unwrap(...)` and then append a component subdirectory such as `mq/`. This keeps log directory usage bounded and lets the Fluxon observability plane discover and collect those file logs through one `Owner Client`-derived root.

### Minimal Node-to-Node RPC Example

`examples/rpc_call.py`:

```python
#!/usr/bin/env python3

import argparse
import signal

from fluxon_py import FluxonKvClientConfig, new_store

RPC_SERVER_INSTANCE_KEY = "demo_rpc_server"
RPC_CLIENT_INSTANCE_KEY = "demo_rpc_client"
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"


def _build_config(*, instance_key: str) -> FluxonKvClientConfig:
    return FluxonKvClientConfig(
        {
            "instance_key": instance_key,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": SHARE_MEM_PATH,
            },
            "test_spec_config": {
                "disable_observability": True,
            },
        }
    )


def run_server(*, instance_key: str) -> None:
    store = new_store(_build_config(instance_key=instance_key)).unwrap("new_store failed")
    count = 0

    def count_handler(from_node_id: str, payload: dict) -> dict:
        nonlocal count
        count += 1
        print(f"rpc from={from_node_id} payload={payload} count={count}")
        return {
            "count": count,
            "payload": payload["payload"],
        }

    try:
        store.rpc_register("/count", count_handler).unwrap("rpc_register failed")
        signal.pause()
    finally:
        store.close().unwrap("close failed")
```

Important constraints:

- `node_id` usually matches the target node's `instance_key`
- `timeout_ms` defaults to `10000`
- Keep one primary public pattern: `rpc_call(...).wait()` on the response handle

### Config Objects

You usually touch two config layers:

- `Master` config: starts the control-plane process
- external-client config: attaches business code to the local `Owner Client` and drives KV / RPC

Minimal master YAML:

```yaml
instance_key: my-master-1
cluster_name: demo-kv-cluster
port: 31000
etcd_endpoints:
  - 127.0.0.1:2379
log_dir: /var/lib/fluxon/master_logs
```

Minimal external-client YAML:

```yaml
instance_key: my-kv-client-1

fluxonkv_spec:
  cluster_name: demo-kv-cluster
  share_mem_path: /dev/shm/fluxon
  p2p_listen_port: 31001
```

`Owner Client` config adds memory contribution and `etcd` addresses:

```yaml
instance_key: my-owner-1

contribute_to_cluster_pool_size:
  dram: 1677721600
  vram: {}

fluxonkv_spec:
  etcd_addresses:
    - 127.0.0.1:2379
  cluster_name: demo-kv-cluster
  share_mem_path: /dev/shm/fluxon
  p2p_listen_port: 31000
  sub_cluster: default
```

Keep these roots separate:

- `share_mem_path`: shared bundle root. Runtime appends `cluster_name`, and that directory holds `mmap.file`, `shared.json`, and peer metadata.
- `large_file_paths`: `Owner Client`-only large-file authority for logs, profiles, caches, and other derived runtime assets
- `FLUXON_LOG`: console log threshold for the user process

In zero-contribution external mode, `Owner Client`-only fields such as `fluxonkv_spec.etcd_addresses`, `fluxonkv_spec.sub_cluster`, `fluxonkv_spec.large_file_paths`, and `fluxonkv_spec.redis_compat` should not appear.
