# 用户 - 4 - MQ 接口

## MQ 接口

Fluxon MQ 复用 KV 服务平面和本机 Owner 的内存池，在 `KvClient` 上提供 Producer 和 Consumer。Producer 使用 `put_data(...)` 发送消息，Consumer 使用 `get_data(...)` 接收消息。

最重要的连接规则是：Producer 和 Consumer 使用相同的 `CHANNEL_KEY`、Channel 类型和 Channel 配置，才能进入同一个 Channel。

### 开始前检查

运行 MQ 示例前，确认以下条件：

- 已按 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md) 启动 Greptime、etcd、Master 和本机 Owner。
- 本机共享内存目录下已经生成 `shared.json`。
- 当前 Python 环境已经安装 `fluxon-*.whl` 和 `fluxon_pyo3-*.whl`；安装方式见 [用户 - 0 - 安装](./用户%20-%200%20-%20安装.md)。
- Producer 和 Consumer 的 `cluster_name` 与目标集群一致。
- 每个进程的 `share_mem_path` 与同一台机器上的 Owner 一致。
- 每个 Producer 和 Consumer 进程使用不同的 `instance_key`。

按 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md) 的默认配置启动时，本页示例可以直接保留：

```python
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"
```

Producer 和 Consumer 都是 External Client，只使用 Owner 已经提供的容量。它们的配置中不应加入 Owner 专用的 `contribute_to_cluster_pool_size`、`etcd_addresses`、`sub_cluster` 或 `large_file_paths`。

`KvClient`、`FlatDict` 和 `Result` 的基础用法见 [用户 - 3 - KV 和 RPC 接口](./用户%20-%203%20-%20KV-RPC接口.md)。

### 对象关系

MQ 的最小调用关系如下：

```text
FluxonKvClientConfig
        ↓
new_store(...) → KvClient
        ↓
使用相同 CHANNEL_KEY 绑定 Channel
        ├─ Producer → put_data(...)
        └─ Consumer → get_data(...)
```

`new_or_bind_with_unique_key(...)` 会根据 `CHANNEL_KEY` 查找 Channel：

- 第一个使用该 key 的进程创建 Channel。
- 后续进程使用相同 key 绑定已有 Channel。
- 不同逻辑 Channel 应使用不同的 key。

每个进程都先创建自己的 `KvClient`，再在其上创建 Producer 或 Consumer handle。退出时顺序固定为：

```text
先关闭 Producer / Consumer handle → 再关闭 KvClient
```

### MQ 最小示例

`examples/start_mpmc_demo.py` 使用 `ChanType.MPMC`，允许多个 Producer 和多个 Consumer。首次运行只启动一个 Consumer 和一个 Producer。

#### 首次只需确认这些值

- `--role`：当前进程运行 Producer 还是 Consumer。
- `CLUSTER_NAME`：必须与 Master 和本机 Owner 一致。
- `SHARE_MEM_PATH`：必须与本机 Owner 一致。
- `CHANNEL_KEY`：两个进程必须完全一致；示例使用 `demo_mq_channel_doc`。
- `instance_key`：脚本根据角色生成 `demo_mq_producer` 或 `demo_mq_consumer`，因此两个进程的标识不同。

示例中的其他值可以先保留：

- `CHANNEL_CAPACITY = 128`
- `CHANNEL_TTL_SECONDS = 300`
- `PRODUCER_INTERVAL_SECONDS = 1.0`
- `CONSUMER_BATCH_SIZE = 1`

这些配置的含义和约束见后面的进阶说明。如果需要同时启动两个相同角色的进程，必须为每个进程分配不同的 `instance_key`，不能继续直接复用示例根据角色生成的固定值。

#### 消息内容

Producer 发送的每条消息都是 `FlatDict`：

```python
{
    "seq": 1,
    "payload": b"hello mq #1",
}
```

- `seq` 是示例自定义的进程内消息序号。
- `payload` 是示例自定义的数据字段。
- `b"..."` 表示 Python bytes。

这些字段名不是 MQ 的固定字段，可以替换为业务需要的其他 `FlatDict` 字段。

#### 启动 Consumer 和 Producer

先在终端一启动 Consumer：

```bash
python3 examples/start_mpmc_demo.py --role consumer
```

Consumer 绑定成功后会打印包含下面内容的日志：

```text
[consumer] ready: channel_key=demo_mq_channel_doc
```

再在终端二启动 Producer：

```bash
python3 examples/start_mpmc_demo.py --role producer
```

成功运行后，两端会持续出现类似日志：

```text
[producer] ready: channel_key=demo_mq_channel_doc
[producer] sent: seq=1 payload=hello mq #1
[consumer] got: seq=1 payload=hello mq #1
```

看到 Producer 持续发送、Consumer 持续收到相同序号和 payload，即表示最小链路已经跑通。分别在两个终端按 Ctrl-C 可以停止进程。

`seq` 只保存在 Producer 当前进程中。重启 Producer 后，它会从 `1` 重新开始，不会跨进程持久化。

<details>
<summary><strong>📄 查看完整脚本（点击展开）</strong>｜<code>examples/start_mpmc_demo.py</code></summary>

```python
#!/usr/bin/env python3

import argparse
import threading
from pathlib import Path

from fluxon_py.api_ext_chan import (  # type: ignore
    ChanRole,
    ChanType,
    new_or_bind_with_unique_key,
)
from fluxon_py.api_error import ChannelClosedError, ProducerClosedError  # type: ignore
from fluxon_py.config import FluxonKvClientConfig  # type: ignore
from fluxon_py.kvclient import new_store  # type: ignore
from fluxon_py.logging import init_logger  # type: ignore
from fluxon_py.runtime import register_ctrlc_callback

# These constants are the only user-facing knobs in the minimal example.
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"
CHANNEL_KEY = "demo_mq_channel_doc"
CHANNEL_CAPACITY = 128
CHANNEL_TTL_SECONDS = 300
PRODUCER_INTERVAL_SECONDS = 1.0
CONSUMER_BATCH_SIZE = 1


def _must_ok(res, msg: str):
    if not res.is_ok():
        raise SystemExit(f"{msg}: {res.unwrap_error()}")
    return res.unwrap()


def _best_effort_close_result(obj, logger, role: str) -> None:
    try:
        close_res = obj.close()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[{role}] close raised (ignored): {e}")
        return

    if close_res.is_ok():
        _ = close_res.unwrap()
    else:
        logger.warning(f"[{role}] close error (ignored): {close_res.unwrap_error()}")


def _build_store_config(*, role: str) -> FluxonKvClientConfig:
    # MQ first attaches to the local owner via one external KvClient,
    # then binds a producer or consumer handle on top of that store.
    return FluxonKvClientConfig(
        {
            "instance_key": f"demo_mq_{role}",
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": SHARE_MEM_PATH,
            },
        }
    )


def _run_producer(store, logger, shutdown_requested: threading.Event) -> None:
    interrupted = False
    closed = False
    producer = None
    restore_signal_listener = lambda: None
    seq = 1
    try:
        # Producer and consumer must bind the same channel key so they land on
        # the same channel id.
        producer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                {"capacity": CHANNEL_CAPACITY, "ttl_seconds": CHANNEL_TTL_SECONDS},
                unique_id=CHANNEL_KEY,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.PRODUCER,
            ),
            "bind producer failed",
        )

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted, closed
            # The signal callback only requests shutdown and closes the handle once.
            # The main loop still exits through its normal close-observation path.
            interrupted = True
            shutdown_requested.set()
            if closed:
                return
            closed = True
            logger.info(f"[producer] caught {reason}, calling close...")
            _best_effort_close_result(producer, logger, "producer")

        restore_signal_listener = register_ctrlc_callback(
            _on_ctrlc,
            thread_name="mpmc-demo-producer-signal",
        )
        logger.info(f"[producer] ready: channel_key={CHANNEL_KEY}")
        while not shutdown_requested.is_set():
            payload_text = f"hello mq #{seq}"
            payload = payload_text.encode("utf-8")
            put_res = producer.put_data(
                {
                    "seq": seq,
                    "payload": payload,
                }
            )
            if put_res.is_ok():
                _ = put_res.unwrap()
                logger.info(f"[producer] sent: seq={seq} payload={payload_text}")
                seq += 1
            else:
                err = put_res.unwrap_error()
                # ProducerClosedError is the expected signal that close() already
                # propagated into the handle, not an unexpected data-path failure.
                if isinstance(err, ProducerClosedError):
                    logger.info("[producer] close observed, exit loop")
                    break
                raise SystemExit(f"put_data failed: {err}")
            if shutdown_requested.wait(PRODUCER_INTERVAL_SECONDS):
                break
    finally:
        restore_signal_listener()
        # Handle lifetime must end before store lifetime.
        if producer is not None and not closed:
            _best_effort_close_result(producer, logger, "producer")
    if interrupted:
        raise SystemExit(130)


def _run_consumer(store, logger, shutdown_requested: threading.Event) -> None:
    interrupted = False
    closed = False
    consumer = None
    restore_signal_listener = lambda: None
    try:
        # Consumer binds the same channel key as producer and only changes role.
        consumer = _must_ok(
            new_or_bind_with_unique_key(
                store,
                {"capacity": CHANNEL_CAPACITY, "ttl_seconds": CHANNEL_TTL_SECONDS},
                unique_id=CHANNEL_KEY,
                chan_type=ChanType.MPMC,
                chan_role=ChanRole.CONSUMER,
            ),
            "bind consumer failed",
        )

        def _on_ctrlc(reason: str) -> None:
            nonlocal interrupted, closed
            # Keep the callback minimal: request shutdown, close the MQ handle once,
            # and let the main loop observe ChannelClosedError.
            interrupted = True
            shutdown_requested.set()
            if closed:
                return
            closed = True
            logger.info(f"[consumer] caught {reason}, calling close...")
            _best_effort_close_result(consumer, logger, "consumer")

        restore_signal_listener = register_ctrlc_callback(
            _on_ctrlc,
            thread_name="mpmc-demo-consumer-signal",
        )
        logger.info(f"[consumer] ready: channel_key={CHANNEL_KEY}")
        while not shutdown_requested.is_set():
            get_res = consumer.get_data(batch_size=CONSUMER_BATCH_SIZE)
            if not get_res.is_ok():
                err = get_res.unwrap_error()
                # ChannelClosedError is the normal close path after Ctrl-C/SIGTERM.
                if isinstance(err, ChannelClosedError):
                    logger.info("[consumer] close observed, exit loop")
                    break
                raise SystemExit(f"get_data failed: {err}")
            for item in get_res.unwrap() or []:
                payload = item.get("payload", b"") if isinstance(item, dict) else item
                seq = item.get("seq") if isinstance(item, dict) else None
                if isinstance(payload, (bytes, bytearray, memoryview)):
                    logger.info(
                        f"[consumer] got: seq={seq} payload={bytes(payload).decode('utf-8', 'ignore')}"
                    )
                else:
                    logger.info(f"[consumer] got: seq={seq} payload={payload}")
            if shutdown_requested.wait(0.2):
                break
    finally:
        restore_signal_listener()
        # Always close the consumer before main() closes the backing store.
        if consumer is not None and not closed:
            _best_effort_close_result(consumer, logger, "consumer")
    if interrupted:
        raise SystemExit(130)


def main() -> None:
    parser = argparse.ArgumentParser(description="Start MQ minimal demo")
    parser.add_argument("--role", choices=["producer", "consumer"], required=True)
    args = parser.parse_args()

    # init_logger() reads FLUXON_LOG and sets the user-process console log level.
    logger = init_logger(f"mpmc_demo_{args.role}")
    shutdown_requested = threading.Event()
    store = None
    try:
        store = _must_ok(new_store(_build_store_config(role=args.role)), "new_store failed")
        if args.role == "producer":
            _run_producer(store, logger, shutdown_requested)
        else:
            _run_consumer(store, logger, shutdown_requested)
    finally:
        store_to_close = store
        store = None
        # Store is closed last because MQ handles are already closed inside _run_*.
        if store_to_close is not None:
            _best_effort_close_result(store_to_close, logger, "store")
        logger.info(f"[{args.role}] exit")


if __name__ == "__main__":
    main()
```

</details>

完整脚本还处理了 Ctrl-C 和关闭期间的并发情况。首次使用只需记住：先关闭 Producer 或 Consumer handle，再关闭底层 `KvClient`。

### 常用接口

#### 创建或绑定 Channel

```python
new_or_bind_with_unique_key(
    api: KvClient,
    chan_config: Dict[str, int],
    unique_id: str,
    chan_type: ChanType,
    chan_role: ChanRole,
) -> Result[
    Union[MPSCChanProducer, MPSCChanConsumer, MPMCChanProducer, MPMCChanConsumer],
    ApiError,
]
```

- `api` 是 `new_store(...)` 创建的 `KvClient`。
- `chan_config` 是 Channel 配置。
- `unique_id` 是稳定的 Channel key。相同 key 指向同一个逻辑 Channel。
- `chan_type` 选择 `ChanType.MPMC` 或 `ChanType.MPSC`。
- `chan_role` 选择 `ChanRole.PRODUCER` 或 `ChanRole.CONSUMER`。

创建或绑定失败时，应先打印 `unwrap_error()`，再检查 `cluster_name`、`share_mem_path`、`unique_id`、Channel 类型和 Channel 配置是否一致。Producer 和 Consumer 的 `chan_role` 本来就不同，不需要设置成相同角色。

#### 发送消息

```python
producer.put_data(value: FlatDict) -> Result[bool, ApiError]
```

`value` 是一层 `FlatDict`。返回成功表示消息已经交给 Channel；返回错误时应消费 `unwrap_error()`。

#### 接收消息

```python
consumer.get_data(
    batch_size: int = 1,
    try_time: Optional[int] = None,
    prefetch_num: int = 0,
) -> Result[List[Any], ApiError]
```

- `batch_size`：本次最多请求的消息数，必须是正整数。
- `try_time`：阻塞路径的最长等待秒数；设为 `0` 时用于非阻塞尝试。
- `prefetch_num`：额外预取窗口大小，入门示例保留 `0`。

成功结果是消息列表；本页 MPMC 示例中的每条消息都是 `FlatDict`。没有消息时可能返回空列表，业务循环应继续等待下一次读取。

#### 查看身份和关闭

- `producer.get_chan_id()` / `consumer.get_chan_id()`：返回当前 handle 绑定的 Channel ID。
- `producer.get_producer_id()`：返回 Producer member ID。
- `consumer.get_consumer_id()`：返回 Consumer member ID。
- `close() -> Result[OkNone, ApiError]`：关闭当前 MQ handle。

### 进阶说明

#### Channel 类型和配置

- `ChanType.MPMC` 支持多个 Producer 和多个 Consumer；`capacity` 与 `ttl_seconds` 都是必填项。
- `ChanType.MPSC` 支持多个 Producer 和一个 Consumer；`ttl_seconds` 必填，`capacity` 可选。
- `capacity` 必须是正整数，用于限制在途消息数量。达到限制后，Producer 会等待 Channel 出现可用空间。
- `ttl_seconds` 必须是整数且不小于 `90`，用于 Channel 和成员租约。
- 绑定同一个 `CHANNEL_KEY` 的进程应使用相同的 Channel 类型、`capacity` 和 `ttl_seconds`。

示例中的两个本地参数不属于 Channel 配置：

- `PRODUCER_INTERVAL_SECONDS` 只控制 Producer 两次发送之间的等待时间，必须为非负值。
- `CONSUMER_BATCH_SIZE` 控制每次 `get_data(...)` 请求的消息数，必须是正整数。

#### 生命周期和错误处理

Ctrl-C 关闭期间，发送或接收循环可能观察到：

- `ProducerClosedError`：Producer handle 已关闭，正常退出发送循环。
- `ChannelClosedError`：Consumer handle 已关闭，正常退出接收循环。

这两种错误在主动关闭路径中不代表新的数据面故障。其他错误应打印 `unwrap_error()` 并停止当前循环。

关闭顺序始终是：

1. 请求业务循环停止。
2. 关闭 Producer 或 Consumer handle。
3. 等待循环退出。
4. 最后关闭 `KvClient`。

#### 日志

- MQ Python 日志由 `init_logger(...)` 初始化，默认输出到当前终端。
- `FLUXON_LOG` 控制日志级别，可选值为 `DEBUG`、`INFO`、`WARNING`、`ERROR`、`CRITICAL`，默认是 `INFO`。
- MQ Rust 和 KV 后台日志使用 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md) 中说明的服务平面日志目录。
- Master 配置了 `monitoring.otlp_log_api` 后，后台日志还会写入 Greptime 的 `fluxon_logs` 表。

需要增加日志时，可以运行：

```bash
FLUXON_LOG=INFO python3 examples/start_mpmc_demo.py --role producer
FLUXON_LOG=DEBUG python3 examples/start_mpmc_demo.py --role consumer
```

### 网页监控

先按 [用户 - 3 - KV 和 RPC 接口](./用户%20-%203%20-%20KV-RPC接口.md) 中的“可选：KV Web UI”启用 Master Web UI。进入对应集群页面后，MQ 主要查看：

- `Channels`：Channel 汇总。
- `Members`：单个 Producer 和 Consumer 明细。

#### 1. 先看是否存在积压

在 `Channels` 表中，先按 `current_inflight` 降序排列：

- 接近 `0`：当前 Channel 基本消费完毕。
- 持续升高：消息产生速度高于消费速度，需要继续查看具体 Producer。

#### 2. 再看具体 Producer

`producer_offsets` 的格式为：

```text
producer_idx: produce_offset/consume_offset
```

例如：

```text
producer_1: 101/88, producer_2: 57/57
```

两个 offset 都表示下一条位置：

- `produce_offset`：Producer 下一条要写入的位置。
- `consume_offset`：Consumer 下一条要提交的位置。

单个 Producer 的未消费量为：

```text
max(produce_offset - consume_offset, 0)
```

`current_inflight` 是同一个 Channel 中所有 Producer 未消费量的总和。某一个 Producer 的差值持续增大时，说明积压主要来自该 Producer；多个 Producer 都有明显差值时，说明整个 Channel 的消费速度不足。

#### 3. 最后定位成员

在 `Members` 表中：

1. 使用 `channel_unique_keys` 搜索 Python 侧传入的 `CHANNEL_KEY`。
2. 查看对应行的 `chan_id`、`owner_id` 和 `external_client_id`。
3. 继续检查 offset 和消费延迟字段。

`channel_unique_keys` 是 Channel 级 key，不是单个 Producer 或 Consumer 的 member ID。需要查看 handle ID 时，使用 Python 接口的 `get_producer_id()` 或 `get_consumer_id()`。

`Channels` 和 `Members` 都支持字段筛选和多级排序。`current_inflight` 适合寻找积压最大的 Channel，`channel_unique_keys` 适合定位业务代码使用的 Channel。

### 延迟排查

MQ 大约每 30 秒打印一次消费延迟统计。基础链路已经运行但消费较慢时，再搜索以下关键词：

| 日志关键词 | 观测位置 | 含义 |
|---|---|---|
| `py-get latency` | Python 调用侧 | `get_data()` 的总耗时 |
| `get_one breakdown` | Python/Rust 边界 | 跨语言等待时间拆分 |
| `MpscConsumer prefetch` | Rust MQ | 预取队列和单条任务耗时 |

快速判断：

- `py-get` 总耗时高时，先看 `avg_wait_rx_ms` 是否较高。
- `avg_get_handle_ms` 高时，预取队列可能为空，常见原因是 Producer 暂时没有数据或预取窗口过小。
- `avg_handle_await_ms` 高时，单条任务本身较慢，例如 KV 读取或 etcd 提交较慢。
