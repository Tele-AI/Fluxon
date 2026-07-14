# 用户 - 3 - KV 和 RPC 接口

## KV 和 RPC 接口

本页介绍 Fluxon 的 Python KV API 和节点间 RPC。两类接口都由同一个 `KvClient` 提供，使用相同的配置、数据模型和关闭流程。

开始前应先按 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md) 启动 Greptime、etcd、Master 和本机 Owner。本页只关注业务进程如何连接已经运行的 KV 集群。

业务代码建议直接把 Python dict 传给 `FluxonKvClientConfig(...)`。YAML 文件和角色配置差异放在本页的进阶说明中。

### 开始前检查

运行本页示例前，确认以下条件：

- Master 和本机 Owner 已经启动，本机共享内存目录下已经生成 `shared.json`。
- 当前 Python 环境已经安装 `fluxon-*.whl` 和 `fluxon_pyo3-*.whl`；安装方式见 [用户 - 0 - 安装](./用户%20-%200%20-%20安装.md)。
- 业务进程的 `cluster_name` 与目标集群一致。
- 业务进程的 `share_mem_path` 与同一台机器上的 Owner 一致。
- 每个业务进程使用不同的 `instance_key`。

按用户 2 的默认配置启动时，本页示例可以直接保留：

```python
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"
```

本页示例创建的是只使用本机 Owner 内存池的 External Client。它不向集群贡献内存，因此配置中不应加入 Owner 专用的 `contribute_to_cluster_pool_size`、`etcd_addresses`、`sub_cluster` 或 `large_file_paths`。

### 共同的调用流程

KV 和 RPC 都从同一个 `KvClient` 开始：

```text
FluxonKvClientConfig
        ↓
new_store(config).unwrap(...) → KvClient
        ↓
调用 KV 或 RPC 接口
        ↓
store.close().unwrap(...)
```

- `FluxonKvClientConfig` 保存当前 Python 进程的连接配置。
- `new_store(...)` 创建 `KvClient`。
- `KvClient` 同时提供 KV 和 RPC 接口。
- 大多数公开接口返回 `Result`。示例使用 `unwrap("错误说明")` 取得成功值；失败时会带着这段说明抛出错误。
- 不再使用客户端时，应调用 `close()` 并消费它返回的 `Result`。

### FlatDict 数据模型

KV value、RPC 请求和 RPC 响应都使用一层 Python dict，例如：

```python
value = {
    "payload": b"hello",
    "count": 1,
    "source": "demo",
}
```

字段名必须是字符串，字段值可以是 `int`、`float`、`bool`、`str`、`bytes` 或 DLPack 数据。不要在 value 中继续嵌套 dict 或 list。

对应类型为：

```python
FlatDict = Dict[str, Union[int, float, bool, str, bytes, DLPacked]]
```

### KV 最小示例

`examples/external_put_get_del.py` 完成一次写入、读取、删除和存在性检查。

运行前只需确认三个值：

- `INSTANCE_KEY`：当前 Python 进程的唯一标识。同一集群内不能与其他进程重复。
- `CLUSTER_NAME`：必须与 Master 和本机 Owner 一致。
- `SHARE_MEM_PATH`：必须与本机 Owner 一致。

示例中的 `test_spec_config.disable_observability = True` 只用于关闭示例进程的观测后台任务，让最小示例专注于 KV 调用；它不是连接集群所需的核心字段。

运行命令：

```bash
python3 examples/external_put_get_del.py
```

成功时会看到：

```text
OK put key=hello
world
OK del key=hello
OK is_exist after remove -> False
```

脚本依次执行：

1. 使用 `new_store(...)` 连接本机 Owner。
2. 使用 `put_blocking(...)` 写入 `{"payload": b"world"}`。
3. 使用 `get_blocking(...)` 取得 `MemHolder`，再调用 `access()` 得到 `FlatDict`。
4. 使用 `remove(...)` 删除 key，并用 `is_exist(...)` 确认 key 已不存在。
5. 释放 `MemHolder` 相关引用，再关闭 `KvClient`。

<details>
<summary><strong>📄 查看完整脚本（点击展开）</strong>｜<code>examples/external_put_get_del.py</code></summary>

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

### 常用 KV 接口

#### 写入

```python
put_blocking(
    key: str,
    value: FlatDict,
    opts: Optional[PutOptionalArgs] = None,
) -> Result[OkNone, ApiError]
```

- `key` 是要写入的 KV key。
- `value` 是一层 `FlatDict`。
- 普通写入不需要传 `opts`。
- 返回成功时写入已经完成，不需要再调用 `wait()`。

#### 读取

```python
get_blocking(key: str) -> Result[MemHolder, ApiError]
MemHolder.access() -> Result[FlatDict, ApiError]
```

`get_blocking(...)` 返回 `MemHolder`，需要再调用 `access()` 才能取得业务 dict。`MemHolder` 本身没有 `bytes()`；bytes 字段应从 `access()` 返回的 `FlatDict` 中读取。

`store.close()` 会等待当前客户端交给业务代码的 `MemHolder` 全部释放。读取结束后不要长期持有 `MemHolder`，关闭前应像示例一样释放相关引用。

#### 删除与检查

```python
remove(key: str) -> Result[OkNone, ApiError]
is_exist(key: str) -> Result[bool, ApiError]
```

`remove(...)` 用于删除 key，`is_exist(...)` 用于检查 key 是否存在。删除后立即调用 `get_blocking(...)` 不保证马上返回 `KeyNotFoundError`，因为读取路径仍可能受到 Owner 和 Master 元数据缓存清理时序的影响；验证删除请求时优先使用 `is_exist(...)`。

### 节点间 RPC 最小示例

节点间 RPC 允许一个业务进程注册处理函数，另一个业务进程通过目标 `instance_key` 调用它。两个进程都需要先创建自己的 `KvClient`。

运行前确认：

- `RPC_SERVER_INSTANCE_KEY` 和 `RPC_CLIENT_INSTANCE_KEY` 不同，并且在集群内都唯一。
- Server 和 Client 的 `CLUSTER_NAME` 与目标集群一致。
- Server 和 Client 的 `SHARE_MEM_PATH` 分别与各自所在机器的 Owner 一致；本机示例使用相同路径。
- `--target-instance-key` 等于 Server 实际使用的 `instance_key`。
- Client 调用的路径与 Server 注册的路径一致；本例都是 `/count`。

先在终端一启动 Server：

```bash
python3 examples/rpc_call.py serve
```

看到下面的输出后保持 Server 运行：

```text
[rpc] handler ready instance_key=demo_rpc_server
[rpc] waiting for Ctrl-C
```

再在终端二启动 Client：

```bash
python3 examples/rpc_call.py call --target-instance-key demo_rpc_server
```

首次调用成功时，Client 输出 `1`；Server 同时打印调用方、payload 和累计调用次数。

调用过程如下：

```text
Server：new_store → rpc_register("/count", handler) → 持续运行
Client：new_store → rpc_call(...) → wait() → FlatDict 响应 → close
```

<details>
<summary><strong>📄 查看完整脚本（点击展开）</strong>｜<code>examples/rpc_call.py</code></summary>

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

### 常用 RPC 接口

#### 注册处理函数

```python
rpc_register(
    path: str,
    handler: Callable[[from_node_id: str, payload: FlatDict], FlatDict],
) -> Result[OkNone, ApiError]
```

- `path` 是 RPC 路径，必须与 Client 调用时使用的路径一致。
- `handler` 接收调用方的 `instance_key` 和 `FlatDict` payload，并返回一个 `FlatDict`。
- 注册成功后，Server 进程必须保持运行。

#### 发起调用

```python
rpc_call(
    node_id: str,
    path: str,
    payload: FlatDict,
    timeout_ms: int = 10000,
) -> Result[KvFuture, ApiError]
```

- `node_id` 通常是目标进程的 `instance_key`。
- `timeout_ms` 默认是 `10000`；显式指定时不能小于 `10000`。
- `rpc_call(...).unwrap(...)` 先取得响应句柄。
- 响应句柄的 `wait().unwrap(...)` 等待远端完成并取得 `FlatDict` 响应。

### 进阶说明

#### 异步接口与写入选项

`put_blocking(...)` 和 `get_blocking(...)` 会等待操作完成后再返回。需要先提交操作、稍后再等待结果时，可以使用：

```python
put(...) -> Result[KvFuture, ApiError]
get(...) -> Result[KvFuture, ApiError]
```

两者都先返回 `KvFuture`，再通过 `wait()` 取得最终结果。

其他接口：

- `get_size(key) -> Result[int, ApiError]`：只查询 value 大小，不读取完整 payload。
- `PutOptionalArgs(lease_id=None, reject_if_inflight_same_key=False)`：控制 lease 绑定，或在同一个 key 已有写入进行时立即拒绝新的写入。
- `PutOptionalArgs.support_mooncake() -> Tuple[bool, List[str]]`：检查当前写入选项是否兼容 Mooncake，并返回不兼容字段名。

#### 日志与观测

- `FLUXON_LOG` 控制当前 Python 进程的控制台日志级别，可选值为 `DEBUG`、`INFO`、`WARNING`、`ERROR`、`CRITICAL`，默认是 `INFO`。
- `store.third_party_logs_dir().unwrap(...)` 返回 Fluxon 为第三方 Python 组件分配的文件日志根目录。组件应继续在该目录下创建自己的子目录，例如 `mq/`。
- `test_spec_config.disable_observability` 是测试和最小示例使用的开关。普通业务配置不应为了连接集群而依赖这个字段。
- Master 配置了 `monitoring.otlp_log_api` 后，后台服务日志会继续写入 Greptime 的 `fluxon_logs` 表。

需要查看更详细的示例进程日志时，可以运行：

```bash
FLUXON_LOG=DEBUG python3 examples/external_put_get_del.py
```

#### Python dict 与 YAML

业务代码优先直接构造 `FluxonKvClientConfig`：

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

配置已经保存为 YAML 时，也可以从文件加载：

```python
cfg = FluxonKvClientConfig.from_file("./kv_external.yaml")
```

External Client 的 YAML 与上面的 Python dict 等价：

```yaml
instance_key: my-kv-client-1
fluxonkv_spec:
  cluster_name: demo-kv-cluster
  share_mem_path: /dev/shm/fluxon_kv_demo
```

Owner 会向集群贡献内存，因此需要额外提供容量、etcd、子集群和大文件目录：

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

Master 使用独立的启动配置，不传给 `FluxonKvClientConfig`：

```yaml
instance_key: my-master-1
cluster_name: demo-kv-cluster
port: 31000
etcd_endpoints:
  - 127.0.0.1:2379
log_dir: /tmp/fluxon_kv_demo/runtime/master_logs
```

这些角色的启动方式、日志目录和多机约束见 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md)。

#### 可选：KV Web UI

需要查看 KV 集群状态时，可以在 Master 配置中加入：

```yaml
master_ui:
  http_listen_addr: 0.0.0.0:18080
```

`master_ui` 依赖 Master 的 `monitoring` 配置。Master 启动后，浏览器访问：

```text
http://<master-host>:18080/view?cluster_name=demo-kv-cluster&member_kind=kv
```

`0.0.0.0` 只表示监听本机所有网卡；浏览器地址应使用实际可访问的 Master 主机名或 IP。
