# User - 3 - KV and RPC Interface

## KV and RPC Interface

This page introduces the Fluxon Python KV API and node-to-node RPC. Both are provided by the same `KvClient` and share one configuration model, data model, and shutdown flow.

Before continuing, follow [User - 2 - Service Plane](<./User - 2 - Service Plane.md>) to start Greptime, etcd, Master, and the local Owner Client. This page focuses only on how an application process connects to an already running KV cluster.

Application code should normally pass a Python dict directly to `FluxonKvClientConfig(...)`. YAML files and role-specific configuration are covered in the advanced section.

### Checks Before Starting

Confirm the following before running the examples:

- Master and the local Owner Client are running, and `shared.json` exists under the local shared-memory directory.
- The current Python environment has installed `fluxon-*.whl` and `fluxon_pyo3-*.whl`; see [User - 0 - Installation](<./User - 0 - Installation.md>).
- The application process uses the same `cluster_name` as the target cluster.
- Its `share_mem_path` matches the Owner Client on the same machine.
- Every application process has a different `instance_key`.

When the default configuration from User 2 is used, the examples on this page can keep:

```python
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"
```

The examples create External Clients that use the local Owner Client's memory pool without contributing capacity. Their configs therefore must not contain Owner-only fields such as `contribute_to_cluster_pool_size`, `etcd_addresses`, `sub_cluster`, or `large_file_paths`.

### Shared Call Flow

Both KV and RPC begin with the same `KvClient`:

```text
FluxonKvClientConfig
        ↓
new_store(config).unwrap(...) → KvClient
        ↓
call KV or RPC APIs
        ↓
store.close().unwrap(...)
```

- `FluxonKvClientConfig` holds the connection settings for the current Python process.
- `new_store(...)` creates a `KvClient`.
- `KvClient` provides both KV and RPC APIs.
- Most public APIs return `Result`. The examples use `unwrap("error context")` to obtain a successful value; failures are raised with that context.
- Call `close()` and consume its `Result` when the client is no longer needed.

### FlatDict Data Model

KV values, RPC requests, and RPC responses all use a single-level Python dict:

```python
value = {
    "payload": b"hello",
    "count": 1,
    "source": "demo",
}
```

Keys must be strings. Values may be `int`, `float`, `bool`, `str`, `bytes`, or DLPack data. Do not nest another dict or list inside a value.

The corresponding type is:

```python
FlatDict = Dict[str, Union[int, float, bool, str, bytes, DLPacked]]
```

### Minimal KV Example

`examples/external_put_get_del.py` performs one write, read, delete, and existence check.

Only three values need to be checked before running it:

- `INSTANCE_KEY`: Unique identity of the current Python process. It must not duplicate another process in the cluster.
- `CLUSTER_NAME`: Must match Master and the local Owner Client.
- `SHARE_MEM_PATH`: Must match the local Owner Client.

The example also sets `test_spec_config.disable_observability = True` to keep observability background tasks out of this minimal process. It is not a core field required to attach to the cluster.

Run:

```bash
python3 examples/external_put_get_del.py
```

A successful run prints:

```text
OK put key=hello
world
OK del key=hello
OK is_exist after remove -> False
```

The script performs these steps:

1. Attach to the local Owner Client with `new_store(...)`.
2. Write `{"payload": b"world"}` with `put_blocking(...)`.
3. Obtain a `MemHolder` with `get_blocking(...)`, then call `access()` to get the `FlatDict`.
4. Delete the key with `remove(...)` and confirm its absence with `is_exist(...)`.
5. Release references associated with the `MemHolder`, then close the `KvClient`.

<details>
<summary><strong>📄 View full script (click to expand)</strong> | <code>examples/external_put_get_del.py</code></summary>

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
        print(f"OK put key={key}")

        mem = store.get_blocking(key).unwrap("get_blocking failed")
        flat = mem.access().unwrap("mem.access failed")
        payload = flat["payload"]
        if not isinstance(payload, (bytes, bytearray)):
            raise RuntimeError(f"payload is not bytes: {type(payload)}")
        print(bytes(payload).decode("utf-8"))

        store.remove(key).unwrap("remove failed")
        print(f"OK del key={key}")

        exists = store.is_exist(key).unwrap("is_exist failed")
        if exists:
            raise RuntimeError(f"expected is_exist({key!r}) to be False after remove")
        print("OK is_exist after remove -> False")
    finally:
        # Release MemHolder-related references before close(); client shutdown waits
        # until all user-visible holders are dropped.
        if "flat" in locals():
            del flat
        if "mem" in locals():
            del mem
        store.close().unwrap("close failed")


if __name__ == "__main__":
    main()
```

</details>

### Common KV APIs

#### Write

```python
put_blocking(
    key: str,
    value: FlatDict,
    opts: Optional[PutOptionalArgs] = None,
) -> Result[OkNone, ApiError]
```

- `key` is the KV key to write.
- `value` is a single-level `FlatDict`.
- Ordinary writes do not need `opts`.
- A successful return means that the write is complete; no additional `wait()` is needed.

#### Read

```python
get_blocking(key: str) -> Result[MemHolder, ApiError]
MemHolder.access() -> Result[FlatDict, ApiError]
```

`get_blocking(...)` returns a `MemHolder`. Call `access()` to obtain the application dict. `MemHolder` does not expose `bytes()` directly; read bytes fields from the `FlatDict` returned by `access()`.

`store.close()` waits for all `MemHolder` objects exposed to application code to be released. Do not retain them longer than needed, and release related references before closing as shown in the example.

#### Delete and Check

```python
remove(key: str) -> Result[OkNone, ApiError]
is_exist(key: str) -> Result[bool, ApiError]
```

Use `remove(...)` to delete a key and `is_exist(...)` to check whether it exists. An immediate `get_blocking(...)` after deletion is not guaranteed to return `KeyNotFoundError` because Owner Client and Master metadata caches may still be converging. Prefer `is_exist(...)` when verifying the delete request.

### Minimal Node-to-Node RPC Example

Node-to-node RPC lets one application process register a handler and another call it through the target `instance_key`. Both processes first create their own `KvClient`.

Before running the example, confirm:

- `RPC_SERVER_INSTANCE_KEY` and `RPC_CLIENT_INSTANCE_KEY` are different and both unique within the cluster.
- Server and Client use the target cluster's `CLUSTER_NAME`.
- Each process uses the `SHARE_MEM_PATH` of the Owner Client on its own machine. The local example uses one shared path.
- `--target-instance-key` matches the `instance_key` actually used by Server.
- The Client call path matches the path registered by Server. This example uses `/count`.

Start Server in terminal one:

```bash
python3 examples/rpc_call.py serve
```

Keep Server running after it prints:

```text
[rpc] handler ready instance_key=demo_rpc_server
[rpc] waiting for Ctrl-C
```

Start Client in terminal two:

```bash
python3 examples/rpc_call.py call --target-instance-key demo_rpc_server
```

On the first successful call, Client prints `1`. Server also prints the caller, payload, and cumulative call count.

The call flow is:

```text
Server: new_store → rpc_register("/count", handler) → keep running
Client: new_store → rpc_call(...) → wait() → FlatDict response → close
```

<details>
<summary><strong>📄 View full script (click to expand)</strong> | <code>examples/rpc_call.py</code></summary>

```python
#!/usr/bin/env python3

import argparse
import signal

from fluxon_py import FluxonKvClientConfig, new_store

RPC_SERVER_INSTANCE_KEY = "demo_rpc_server"
RPC_CLIENT_INSTANCE_KEY = "demo_rpc_client"
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal node-to-node RPC example")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Start one RPC handler process")
    serve_parser.add_argument("--instance-key", default=RPC_SERVER_INSTANCE_KEY, help="RPC handler instance key")

    call_parser = subparsers.add_parser("call", help="Call one RPC handler and print the counter")
    call_parser.add_argument("--instance-key", default=RPC_CLIENT_INSTANCE_KEY, help="RPC caller instance key")
    call_parser.add_argument(
        "--target-instance-key",
        default=RPC_SERVER_INSTANCE_KEY,
        help="Target RPC handler instance key",
    )

    args = parser.parse_args()
    if args.command == "serve":
        run_server(instance_key=args.instance_key)
        return
    if args.command == "call":
        run_client(instance_key=args.instance_key, target_instance_key=args.target_instance_key)
        return
    raise AssertionError("unreachable")


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
        print(f"[rpc] handler ready instance_key={instance_key}")
        print("[rpc] waiting for Ctrl-C")
        signal.pause()
    except KeyboardInterrupt:
        print("[rpc] caught Ctrl-C, stopping handler")
        raise SystemExit(130)
    finally:
        store.close().unwrap("close failed")


def run_client(*, instance_key: str, target_instance_key: str) -> None:
    store = new_store(_build_config(instance_key=instance_key)).unwrap("new_store failed")
    try:
        resp = (
            store.rpc_call(target_instance_key, "/count", {"payload": b"hi"})
            .unwrap("rpc_call failed")
            .wait()
            .unwrap("rpc wait failed")
        )
        print(resp["count"])
    finally:
        store.close().unwrap("close failed")


if __name__ == "__main__":
    main()
```

</details>

### Common RPC APIs

#### Register a Handler

```python
rpc_register(
    path: str,
    handler: Callable[[from_node_id: str, payload: FlatDict], FlatDict],
) -> Result[OkNone, ApiError]
```

- `path` is the RPC path and must match the Client call path.
- `handler` receives the caller's `instance_key` and a `FlatDict` payload, then returns a `FlatDict`.
- Server must remain running after registration succeeds.

#### Make a Call

```python
rpc_call(
    node_id: str,
    path: str,
    payload: FlatDict,
    timeout_ms: int = 10000,
) -> Result[KvFuture, ApiError]
```

- `node_id` normally identifies the target process by its `instance_key`.
- `timeout_ms` defaults to `10000`; an explicit value must not be lower than `10000`.
- `rpc_call(...).unwrap(...)` first obtains the response handle.
- Calling `wait().unwrap(...)` on that handle waits for remote completion and returns the response `FlatDict`.

### Advanced Notes

#### Async APIs and Write Options

`put_blocking(...)` and `get_blocking(...)` return only after the operation completes. To submit an operation first and wait for its result later, use:

```python
put(...) -> Result[KvFuture, ApiError]
get(...) -> Result[KvFuture, ApiError]
```

Both return a `KvFuture` first. Call `wait()` to obtain the final result.

Other APIs:

- `get_size(key) -> Result[int, ApiError]`: Query the value size without reading the full payload.
- `PutOptionalArgs(lease_id=None, reject_if_inflight_same_key=False)`: Bind a write to a lease or reject it immediately when another write for the same key is already in flight.
- `PutOptionalArgs.support_mooncake() -> Tuple[bool, List[str]]`: Check whether the selected write options are compatible with Mooncake and return the incompatible field names.

#### Logging and Observability

- `FLUXON_LOG` controls the console log level of the current Python process. Valid values are `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`; the default is `INFO`.
- `store.third_party_logs_dir().unwrap(...)` returns the file-log root allocated by Fluxon for third-party Python components. Each component should create its own subdirectory below that root, such as `mq/`.
- `test_spec_config.disable_observability` is a testing and minimal-example switch. Ordinary application configs should not depend on it merely to attach to the cluster.
- When Master configures `monitoring.otlp_log_api`, background service logs are also written to the Greptime `fluxon_logs` table.

To enable more detailed logs for the example process:

```bash
FLUXON_LOG=DEBUG python3 examples/external_put_get_del.py
```

#### Python Dicts and YAML

Application code should normally construct `FluxonKvClientConfig` directly:

```python
from fluxon_py import FluxonKvClientConfig

cfg = FluxonKvClientConfig(
    {
        "instance_key": "my-kv-client-1",
        "fluxonkv_spec": {
            "cluster_name": "demo-kv-cluster",
            "share_mem_path": "/dev/shm/fluxon_kv_demo",
        },
    }
)
```

When configuration is already stored in YAML, load it from a file:

```python
cfg = FluxonKvClientConfig.from_file("./kv_external.yaml")
```

The External Client YAML below is equivalent to the Python dict above:

```yaml
instance_key: my-kv-client-1
fluxonkv_spec:
  cluster_name: demo-kv-cluster
  share_mem_path: /dev/shm/fluxon_kv_demo
```

Owner Client contributes memory to the cluster and therefore also needs capacity, etcd, sub-cluster, and large-file settings:

```yaml
instance_key: my-owner-1
contribute_to_cluster_pool_size:
  dram: 1073741824
  vram: {}
fluxonkv_spec:
  etcd_addresses:
    - 127.0.0.1:2379
  cluster_name: demo-kv-cluster
  share_mem_path: /dev/shm/fluxon_kv_demo
  sub_cluster: default
  large_file_paths:
    - /tmp/fluxon_kv_demo/runtime/large/owner
```

Master uses a separate startup config that is not passed to `FluxonKvClientConfig`:

```yaml
instance_key: my-master-1
cluster_name: demo-kv-cluster
port: 31000
etcd_endpoints:
  - 127.0.0.1:2379
log_dir: /tmp/fluxon_kv_demo/runtime/master_logs
```

See [User - 2 - Service Plane](<./User - 2 - Service Plane.md>) for startup commands, log directories, and multi-machine constraints for these roles.

#### Optional: KV Web UI

To inspect KV cluster state, add the following block to the Master config:

```yaml
master_ui:
  http_listen_addr: 0.0.0.0:18080
```

`master_ui` depends on the Master `monitoring` config. After Master starts, open:

```text
http://<master-host>:18080/view?cluster_name=demo-kv-cluster&member_kind=kv
```

`0.0.0.0` only means "listen on all local interfaces." Use a reachable Master hostname or IP address in the browser URL.
