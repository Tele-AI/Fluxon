# 用户 - 3 - KV 和 RPC 接口

## KV 和 RPC 接口

本页描述 Fluxon 的 Python KV API 和节点间 RPC 调用。两者由同一个 `KvClient` 实例提供，共享生命周期。

`cluster_name`、`instance_key`、`etcd` 等前置概念见 [架构和概念](用户%20-%201%20-%20架构和概念.md)。

Python 业务代码优先直接在代码里写 Python dict，并传给 `FluxonKvClientConfig(...)`；YAML 更适合独立进程启动、supervisor、部署和示例环境。

### 服务平面

在写 `put_blocking/get_blocking/rpc_call` 这些 Python 业务代码之前，需要先把 KV 依赖的服务平面拉起来。共性的角色关系、启动顺序和 runtime 边界，统一见 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md)。

![](../../pics/deploy_arch_1.png)

直接接触的服务平面对象主要有：

- `greptime`：用于标准监控链路。安装与启动见 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md)
- `etcd`：KV 控制面元数据存储。安装与启动见 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md)
- `start_kv_master_process(...)`：启动 `fluxonkv master`
- `start_owner_kvclient_process(...)`：启动 `owner`

最小可运行示例脚本如下。这个脚本只启动 Fluxon 自己的角色；`etcd` / `greptime` 仍按服务平面文档单独启动，并且默认假设：

- `etcd` 在 `127.0.0.1:2379`
- `greptime` HTTP 在 `127.0.0.1:34030`
- 当前 `python3` 所在环境已经安装 `fluxon-*.whl` 和 `fluxon_pyo3-*.whl`；安装方式见 [用户 - 0 - 安装](./用户%20-%200%20-%20安装.md)

对应示例脚本：`examples/start_master_owner.py`

这个脚本支持两种启动方式：

- 默认方式：启动 `master + owner`
- `--without-master`：只启动 `owner`，接入已经存在的 KV 集群 `master`

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
SHARED_MEMORY_PATH = Path("/dev/shm/fluxon_kv_demo").resolve()
SHARED_FILE_PATH = Path("/tmp/fluxon_kv_demo/shared").resolve()
WORKDIR = Path("/tmp/fluxon_kv_demo/runtime").resolve()
MASTER_PORT = 31000
MASTER_UI_PORT = 18080
MASTER_INSTANCE_KEY = "demo_kv_master"
OWNER_INSTANCE_KEY = "demo_kv_owner"
OWNER_DRAM_BYTES = 1073741824


def main() -> None:
    args = parse_args()
    SHARED_FILE_PATH.mkdir(parents=True, exist_ok=True)
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

    print(f"[fluxon_kv] shared memory path: {SHARED_MEMORY_PATH}")
    print(f"[fluxon_kv] shared file path: {SHARED_FILE_PATH}")
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
            "shared_memory_path": str(SHARED_MEMORY_PATH),
            "shared_file_path": str(SHARED_FILE_PATH),
            "sub_cluster": "default",
        },
    }


if __name__ == "__main__":
    main()
```

启动命令：

```bash
python3 examples/start_master_owner.py
python3 examples/start_master_owner.py --without-master
```

默认命令会启动本机 `master + owner`。`--without-master` 只启动本机 `owner`，要求同一个 `cluster_name` 对应的 `master` 已经在别处运行。

上面的 `build_master_config(...)` 里，`master_ui` 是可缺省配置块。配置后，`start_kv_master_process(...)` 会让 KV Web UI 直接作为 `master` 内的 HTTP 服务一起启动：

```yaml
master_ui:
  http_listen_addr: 0.0.0.0:18080
```

这个 UI 的实际宿主是 `fluxon_cli` 的 KV monitor web。URL 形状固定为：

```text
http://<host-ip-or-domain>:18080/view?cluster_name=demo-kv-cluster&member_kind=kv
```

`owner` 把共享内存池和 `shared.json` 准备好之后，再运行下面的业务最小示例。

### 生命周期与调用流程（Call Flow）

```text
User-visible processes:
- etcd (control-plane metadata store)
- Fluxon cluster node process(es) (the remote peers you address via instance_key/node_id)
- Your Python process (business code using fluxon_py)

FluxonKvClientConfig (prefer Python dict; YAML also supported)
            |
            v
new_store(cfg) -> KvClient (one instance in your Python process)
     |               |
     |               +-- KV: put_blocking/get_blocking/remove/... -> Result[...]
     |               |                                   get_blocking() -> Result[MemHolder, ApiError]
     |               |                                                  -> access() -> Result[FlatDict, ApiError]
     |               |
     |               +-- Node call(server): rpc_register(path, handler) -> Result[OkNone, ApiError]
     |               |                       (handler lives in your Python process; keep it running)
     |               |
     |               +-- Node call(client): rpc_call(node_id, path, payload, timeout_ms) -> Result[响应句柄, ApiError]
     |                                                                    -> wait() -> Result[FlatDict, ApiError]
     |
     v
api.close() -> Result[OkNone, ApiError]
```

### 核心对象（Python）

- `FluxonKvClientConfig`：配置对象，优先直接从 Python dict 创建，也支持从 YAML 文件加载。
- `new_store(config: FluxonKvClientConfig) -> Result[KvClient, ApiError]`：创建 KV client 实例。
- `KvClient`：统一入口，同时提供 KV 读写与节点间调用。
- `MemHolder`：`get_blocking(...)` 成功后的读取结果持有者，`access()` 取得 `FlatDict`。
- `PutOptionalArgs`：`put_blocking(...)` 的可选参数对象，当前常用字段是 `lease_id`。
- `test_spec_config.disable_observability`：最小 external client 示例里显式设为 `True`，避免把 OTLP / observe 后台任务引入“只验证 KV/RPC 基本链路”的示例生命周期。

注意：

- `MemHolder` 没有 `bytes()`；需要 `access()` 后从 dict 里取 bytes 字段（常用字段名 `payload`）。
- `store.close()` 会等待当前 client 暴露出去的 `MemHolder` 全部释放；示例里在 `close()` 前显式删掉 `mem` / `flat`，就是为了满足这个关闭约束。
- `Result` 必须显式消费：调用 `unwrap()` 或 `unwrap_error()`。

### FlatDict（数据模型）

KV value 和节点间调用的 payload 统一为 flat dict：

- `FlatDict = Dict[str, Union[int, float, bool, str, bytes, dlpack]]`

### KV 接口最小示例

对应示例脚本：`examples/external_put_get_del.py`

```python
#!/usr/bin/env python3

from fluxon_py import FluxonKvClientConfig, new_store

INSTANCE_KEY = "demo_kv_external"
CLUSTER_NAME = "demo-kv-cluster"
SHARED_MEMORY_PATH = "/dev/shm/fluxon_kv_demo"
SHARED_FILE_PATH = "/tmp/fluxon_kv_demo/shared"


def main() -> None:
    cfg = FluxonKvClientConfig(
        {
            "instance_key": INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "shared_memory_path": SHARED_MEMORY_PATH,
                "shared_file_path": SHARED_FILE_PATH,
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

### 常用接口（KV）

上面的 KV 最小示例如果要打开更详细的用户进程日志，直接在启动 Python 进程前设置：

```bash
FLUXON_LOG=DEBUG python3 examples/external_put_get_del.py
```

日志相关对象如下：

- `FLUXON_LOG`：控制当前 Python 业务进程 console logger 的输出门限
- Fluxon Python 侧 logger 会读取 `FLUXON_LOG`；合法值是 `DEBUG`、`INFO`、`WARNING`、`ERROR`、`CRITICAL`，默认 `INFO`
- `log_dir`：`master` 本地日志 authority
- `shared_file_path`：本机共享文件 authority，`shared.json`、日志、profile 等文件位于这里

如果服务平面的 `master.monitoring.otlp_log_api` 已经配置，后台服务日志还会继续采集到 Greptime 的 `fluxon_logs` 表。

`put_blocking(key: str, value: FlatDict, opts: Optional[PutOptionalArgs] = None) -> Result[OkNone, ApiError]`

- 作用：写入或覆盖一个 KV。
- `key`：要写入的 KV key。
- `value`：要写入的 flat dict payload。
- `opts`：可选写参数；普通写入通常传 `None`，需要额外写控制时再传 `PutOptionalArgs(...)`。
- 返回链路：调用返回成功后，这次写入已经完成，不需要再额外 `wait()`。

`PutOptionalArgs(lease_id: Optional[int] = None)`

- 作用：`put_blocking(...)` 的可选参数对象。
- `lease_id`：提交写入时，把这个 key 绑定到指定 lease。
- 常用方式：普通业务写入一般不用传；只有需要 lease 生命周期控制时才显式构造它。

`PutOptionalArgs.support_mooncake() -> Tuple[bool, List[str]]`

- 作用：检查当前这组写参数是否兼容 mooncake 写入路径。
- 返回值：第一个返回值表示是否兼容，第二个返回值列出不兼容字段名。

`get_blocking(key: str) -> Result[MemHolder, ApiError]`

- 作用：读取一个 KV。
- `key`：要读取的 KV key。
- 返回链路：接口成功后直接拿到 `MemHolder`，再调用 `access()` 取得 `FlatDict`。

`MemHolder`

- 作用：`get_blocking(...)` 成功后的读取结果持有者。
- 理解方式：它不是最终的业务 dict，也不是原始 bytes；还要继续 `access()`。

`MemHolder.access() -> Result[FlatDict, ApiError]`

- 作用：把 `MemHolder` 中的数据展开成 `FlatDict`。
- 常用用法：`flat = mem.access().unwrap(...)`，然后再从 `flat["payload"]` 之类的字段里取业务值。
- 注意：`MemHolder` 本身没有 `bytes()`；如果 value 里有 bytes 字段，要先 `access()` 再取。

`get_size(key: str) -> Result[int, ApiError]`

- 作用：只查询 value 大小，不把 payload 整体取回。
- `key`：要查询的 KV key。
- 适合场景：先判断对象大小，再决定是否继续 `get(...)`。

`is_exist(key: str) -> Result[bool, ApiError]`

- 作用：判断某个 KV key 当前是否存在。
- `key`：要检查的 KV key。

`remove(key: str) -> Result[OkNone, ApiError]`

- 作用：删除一个 KV。
- `key`：要删除的 KV key。

`is_exist(key: str) -> Result[bool, ApiError]`

- 作用：查询当前 key 是否还存在。
- 最小示例里，`remove(...)` 之后优先用它验证“删除请求已经生效”。
- 注意：`remove(...)` 之后立刻 `get_blocking(...)` 不保证马上返回 `KeyNotFoundError`；删除后的读路径还会受 owner / master 元数据 cache 清理时序影响。如果你要验证“删除传播后不可读”，需要给删除传播留出观察时间。

### 节点间 RPC 调用最小示例

这里的 RPC 指节点间 RPC 调用，目标节点通常用目标实例的 `instance_key` 来标识。

对应示例脚本：`examples/rpc_call.py`

```python
#!/usr/bin/env python3

import argparse
import signal

from fluxon_py import FluxonKvClientConfig, new_store

RPC_SERVER_INSTANCE_KEY = "demo_rpc_server"
RPC_CLIENT_INSTANCE_KEY = "demo_rpc_client"
CLUSTER_NAME = "demo-kv-cluster"
SHARED_MEMORY_PATH = "/dev/shm/fluxon_kv_demo"
SHARED_FILE_PATH = "/tmp/fluxon_kv_demo/shared"


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
                "shared_memory_path": SHARED_MEMORY_PATH,
                "shared_file_path": SHARED_FILE_PATH,
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

### 常用接口（节点间 RPC）

- `rpc_register(path: str, handler: Callable[[from_node_id: str, payload: FlatDict], FlatDict]) -> Result[OkNone, ApiError]`
- `rpc_call(node_id: str, path: str, payload: FlatDict, timeout_ms: int = 10000) -> Result[响应句柄, ApiError]`

使用约束如下：

- `node_id` 通常对应目标节点的 `instance_key`（见：[架构和概念](用户%20-%201%20-%20架构和概念.md)）。
- `timeout_ms` 默认是 `10000`；如果调用方显式指定，必须满足 `timeout_ms >= 10000`。

### 配置对象与配置文件

KV 环境至少会直接接触两类配置对象：

- master 配置：启动控制面进程，负责 etcd、成员路由、监控和 master 日志目录
- client / external 配置：创建 `FluxonKvClientConfig`，供 `new_store(...)` 附着到同机 owner 并发起 KV / RPC

业务代码直接编辑的通常是第二类配置对象，并且通常直接编辑 Python dict；需要先把 KV 集群拉起来时，两类配置对象都要准备，此时再把配置落成 YAML 给 CLI / runtime 进程使用。

#### 1) master 配置

最小 master 配置示例：

```yaml
instance_key: my-master-1
cluster_name: demo-kv-cluster
port: 31000
etcd_endpoints:
  - 127.0.0.1:2379
log_dir: /var/lib/fluxon/master_logs
```

理解方式：

- `etcd_endpoints`：master 控制面连接的 etcd 地址
- `log_dir`：master 自己的日志 / profile authority；运行时会在这个目录下继续派生 cluster 级日志子目录

#### 2) client / external 配置

业务代码里直接把 Python dict 传给 `FluxonKvClientConfig(...)` 的构造方式如下：

```python
from fluxon_py import FluxonKvClientConfig

cfg = FluxonKvClientConfig(
    {
        "instance_key": "my-kv-client-1",
        "fluxonkv_spec": {
            "cluster_name": "demo-kv-cluster",
            "shared_memory_path": "/dev/shm/fluxon",
            "shared_file_path": "/var/lib/fluxon/shared",
        },
    }
)
```

如果配置已经落成 YAML 文件，也可以直接从文件构造：

```python
from fluxon_py import FluxonKvClientConfig

cfg = FluxonKvClientConfig.from_file("./kv_external.yaml")
```

这两种方式最终得到的是同一个配置对象，后续都传给 `new_store(cfg)`。

最小 external-client 配置示例：

```yaml
# 当前 Python 进程 / external client 的唯一实例标识
instance_key: my-kv-client-1

fluxonkv_spec:
  # 目标集群名；必须和 master / owner 保持一致
  cluster_name: demo-kv-cluster
  # 本机共享内存 authority；external 靠它附着到同机 owner 的内存池
  shared_memory_path: /dev/shm/fluxon
  # 本机共享文件 authority；shared.json、日志、profile 等文件位于这里
  shared_file_path: /var/lib/fluxon/shared
  # 可选：覆盖当前 client 的 P2P 监听端口
  p2p_listen_port: 31001
```

Owner 节点需要额外配置内存贡献和 etcd 地址：

```yaml
# owner 实例标识；同样要求全局唯一
instance_key: my-owner-1

# owner 向集群贡献的内存池大小
contribute_to_cluster_pool_size:
  # DRAM 贡献，单位字节
  dram: 1677721600
  # VRAM 贡献；这里为空表示不贡献显存
  vram: {}

fluxonkv_spec:
  # owner 连接 etcd 的地址列表
  etcd_addresses:
    - 127.0.0.1:2379
  # 目标集群名；必须和 master / external 保持一致
  cluster_name: demo-kv-cluster
  # 本机共享内存 authority；external 进程会附着到这里
  shared_memory_path: /dev/shm/fluxon
  # 本机共享文件 authority；shared.json、日志、profile 等文件位于这里
  shared_file_path: /var/lib/fluxon/shared
  # owner 自己的 P2P 监听端口
  p2p_listen_port: 31000
  # owner 所属子集群标签
  sub_cluster: default
```

这里需要把两个本机 authority 分清楚：

- `shared_memory_path`：共享内存 / mmap authority，同机进程靠它附着到同一块内存池
- `shared_file_path`：共享文件 authority，`shared.json`、日志、profile 等文件位于这里
- `FLUXON_LOG`：用户 Python 进程 console log 的门限，不写时默认 `INFO`

zero-contribution external 模式下有一个硬约束：`fluxonkv_spec.etcd_addresses`、`fluxonkv_spec.sub_cluster`、`fluxonkv_spec.redis_compat` 这类 owner 侧字段不应出现。
