# User - 4 - MQ Interface

## MQ Interface

Fluxon MQ reuses the KV service plane and the local Owner Client's memory pool. It provides Producer and Consumer handles on top of `KvClient`. Producer sends messages with `put_data(...)`, and Consumer receives them with `get_data(...)`.

The most important connection rule is that Producer and Consumer must use the same `CHANNEL_KEY`, channel type, and channel config to join the same channel.

### Checks Before Starting

Confirm the following before running the MQ example:

- Greptime, etcd, Master, and the local Owner Client have been started as described in [User - 2 - Service Plane](<./User - 2 - Service Plane.md>).
- `shared.json` exists under the local shared-memory directory.
- The current Python environment has installed `fluxon-*.whl` and `fluxon_pyo3-*.whl`; see [User - 0 - Installation](<./User - 0 - Installation.md>).
- Producer and Consumer use the same `cluster_name` as the target cluster.
- Each process uses the `share_mem_path` of the Owner Client on the same machine.
- Every Producer and Consumer process has a different `instance_key`.

When the default configuration from User 2 is used, the example can keep:

```python
CLUSTER_NAME = "demo-kv-cluster"
SHARE_MEM_PATH = "/dev/shm/fluxon_kv_demo"
```

Producer and Consumer are External Clients that use capacity already provided by Owner Client. Their configs must not contain Owner-only fields such as `contribute_to_cluster_pool_size`, `etcd_addresses`, `sub_cluster`, or `large_file_paths`.

See [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>) for the basic `KvClient`, `FlatDict`, and `Result` contracts.

### Object Relationship

The minimum MQ call relationship is:

```text
FluxonKvClientConfig
        ↓
new_store(...) → KvClient
        ↓
bind a channel with the same CHANNEL_KEY
        ├─ Producer → put_data(...)
        └─ Consumer → get_data(...)
```

`new_or_bind_with_unique_key(...)` looks up a channel by `CHANNEL_KEY`:

- The first process using that key creates the channel.
- Later processes using the same key bind to the existing channel.
- Different logical channels should use different keys.

Each process creates its own `KvClient` and then creates a Producer or Consumer handle on top of it. The shutdown order is fixed:

```text
close the Producer / Consumer handle first → close KvClient second
```

### Minimal MQ Example

`examples/start_mpmc_demo.py` uses `ChanType.MPMC`, which supports multiple Producers and multiple Consumers. The first run needs only one Consumer and one Producer.

#### Values to Check on the First Run

- `--role`: Run the current process as Producer or Consumer.
- `CLUSTER_NAME`: Must match Master and the local Owner Client.
- `SHARE_MEM_PATH`: Must match the local Owner Client.
- `CHANNEL_KEY`: Must match exactly in both processes. The example uses `demo_mq_channel_doc`.
- `instance_key`: The script derives `demo_mq_producer` or `demo_mq_consumer` from the role, so the two process identities are different.

The other example values can remain unchanged:

- `CHANNEL_CAPACITY = 128`
- `CHANNEL_TTL_SECONDS = 300`
- `PRODUCER_INTERVAL_SECONDS = 1.0`
- `CONSUMER_BATCH_SIZE = 1`

Their meanings and constraints are covered in the advanced section. To start two processes with the same role, assign each process a different `instance_key` instead of reusing the example's fixed role-derived value.

#### Message Content

Each message sent by Producer is a `FlatDict`:

```python
{
    "seq": 1,
    "payload": b"hello mq #1",
}
```

- `seq` is a process-local sequence field defined by the example.
- `payload` is an application-data field defined by the example.
- `b"..."` is a Python bytes literal.

These field names are not fixed by MQ and may be replaced with other application-specific `FlatDict` fields.

#### Start Consumer and Producer

Start Consumer in terminal one:

```bash
python3 examples/start_mpmc_demo.py --role consumer
```

After Consumer binds successfully, its log contains:

```text
[consumer] ready: channel_key=demo_mq_channel_doc
```

Start Producer in terminal two:

```bash
python3 examples/start_mpmc_demo.py --role producer
```

After both processes are running, their logs contain messages like:

```text
[producer] ready: channel_key=demo_mq_channel_doc
[producer] sent: seq=1 payload=hello mq #1
[consumer] got: seq=1 payload=hello mq #1
```

The minimum path is working when Producer continues to send and Consumer receives the matching sequence and payload. Press Ctrl-C in each terminal to stop both processes.

`seq` is stored only in the current Producer process. Restarting Producer resets it to `1`; it is not persisted across processes.

<details>
<summary><strong>📄 View full script (click to expand)</strong> | <code>examples/start_mpmc_demo.py</code></summary>

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

The full script also handles Ctrl-C and concurrency during shutdown. For a first use, remember only that the Producer or Consumer handle closes before the underlying `KvClient`.

### Common APIs

#### Create or Bind a Channel

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

- `api` is the `KvClient` created by `new_store(...)`.
- `chan_config` contains the channel settings.
- `unique_id` is the stable channel key. The same key identifies the same logical channel.
- `chan_type` selects `ChanType.MPMC` or `ChanType.MPSC`.
- `chan_role` selects `ChanRole.PRODUCER` or `ChanRole.CONSUMER`.

If creation or binding fails, print `unwrap_error()` and check `cluster_name`, `share_mem_path`, `unique_id`, channel type, and channel config first. Producer and Consumer naturally use different `chan_role` values; their roles do not need to match.

#### Send a Message

```python
producer.put_data(value: FlatDict) -> Result[bool, ApiError]
```

`value` is a single-level `FlatDict`. A successful result means that the message has been submitted to the channel. Consume `unwrap_error()` when the call fails.

#### Receive Messages

```python
consumer.get_data(
    batch_size: int = 1,
    try_time: Optional[int] = None,
    prefetch_num: int = 0,
) -> Result[List[Any], ApiError]
```

- `batch_size`: Maximum number of messages requested by this call. It must be a positive integer.
- `try_time`: Maximum wait in seconds for blocking paths. Set it to `0` for a non-blocking attempt.
- `prefetch_num`: Additional prefetch-window size. Keep `0` for the introductory example.

A successful result is a message list; each item in this page's MPMC example is a `FlatDict`. The list may be empty when no message is available, so an application loop should continue to the next read.

#### Inspect Identity and Close

- `producer.get_chan_id()` / `consumer.get_chan_id()`: Return the channel ID bound to the current handle.
- `producer.get_producer_id()`: Return the Producer member ID.
- `consumer.get_consumer_id()`: Return the Consumer member ID.
- `close() -> Result[OkNone, ApiError]`: Close the current MQ handle.

### Advanced Notes

#### Channel Types and Configuration

- `ChanType.MPMC` supports multiple Producers and multiple Consumers. Both `capacity` and `ttl_seconds` are required.
- `ChanType.MPSC` supports multiple Producers and one Consumer. `ttl_seconds` is required and `capacity` is optional.
- `capacity` must be a positive integer and limits in-flight messages. Producer waits for available channel space after the limit is reached.
- `ttl_seconds` must be an integer of at least `90` and is used by channel and member leases.
- Processes binding the same `CHANNEL_KEY` should use the same channel type, `capacity`, and `ttl_seconds`.

Two local example settings are not part of the channel config:

- `PRODUCER_INTERVAL_SECONDS` controls only the delay between Producer sends and must be non-negative.
- `CONSUMER_BATCH_SIZE` controls the number of messages requested by each `get_data(...)` call and must be positive.

#### Lifecycle and Error Handling

During Ctrl-C shutdown, the send or receive loop may observe:

- `ProducerClosedError`: The Producer handle is closed; exit the send loop normally.
- `ChannelClosedError`: The Consumer handle is closed; exit the receive loop normally.

These errors are expected during an explicit shutdown and do not indicate a new data-plane failure. For other errors, print `unwrap_error()` and stop the current loop.

Always shut down in this order:

1. Ask the application loop to stop.
2. Close the Producer or Consumer handle.
3. Wait for the loop to exit.
4. Close `KvClient` last.

#### Logging

- Python MQ logs are initialized by `init_logger(...)` and go to the current terminal by default.
- `FLUXON_LOG` controls the log level. Valid values are `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`; the default is `INFO`.
- Rust MQ and KV background logs use the service-plane log directories described in User 2.
- When Master configures `monitoring.otlp_log_api`, background logs are also written to the Greptime `fluxon_logs` table.

To change the example log level:

```bash
FLUXON_LOG=INFO python3 examples/start_mpmc_demo.py --role producer
FLUXON_LOG=DEBUG python3 examples/start_mpmc_demo.py --role consumer
```

### Web Monitoring

First enable the Master Web UI through “Optional: KV Web UI” in [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>). On the target cluster page, the two main MQ tables are:

- `Channels`: Channel summary.
- `Members`: Individual Producer and Consumer details.

#### 1. Check for Backlog

In `Channels`, sort by `current_inflight` in descending order:

- Near `0`: The channel is mostly caught up.
- Continually increasing: Messages are arriving faster than they are consumed; inspect individual Producers next.

#### 2. Inspect Individual Producers

`producer_offsets` has this format:

```text
producer_idx: produce_offset/consume_offset
```

For example:

```text
producer_1: 101/88, producer_2: 57/57
```

Both offsets describe the next position:

- `produce_offset`: The next position Producer will write.
- `consume_offset`: The next position Consumer will commit.

The unconsumed count for one Producer is:

```text
max(produce_offset - consume_offset, 0)
```

`current_inflight` is the sum of unconsumed counts across Producers in one channel. A growing gap for one Producer identifies a localized backlog. Clear gaps across several Producers indicate that the channel as a whole is consuming too slowly.

#### 3. Locate Members

In `Members`:

1. Search `channel_unique_keys` with the `CHANNEL_KEY` passed by Python.
2. Inspect `chan_id`, `owner_id`, and `external_client_id` on the matching row.
3. Continue with the offset and consumer-latency fields.

`channel_unique_keys` is a channel-level key, not the member ID of one Producer or Consumer. To inspect handle IDs in Python, call `get_producer_id()` or `get_consumer_id()`.

Both `Channels` and `Members` support field filters and multi-level sorting. Use `current_inflight` to find the largest backlog and `channel_unique_keys` to locate an application channel.

### Latency Triage

MQ prints consumer-latency statistics roughly every 30 seconds. When the basic path works but consumption is slow, search these keywords:

| Keyword | Observation point | Meaning |
|---|---|---|
| `py-get latency` | Python caller | Total `get_data()` latency |
| `get_one breakdown` | Python/Rust boundary | Breakdown of cross-language wait time |
| `MpscConsumer prefetch` | Rust MQ | Prefetch-queue and per-task latency |

Quick interpretation:

- When total `py-get` latency is high, check whether `avg_wait_rx_ms` is also high.
- High `avg_get_handle_ms` may indicate an empty prefetch queue, often because Producer has no data or the prefetch window is too small.
- High `avg_handle_await_ms` indicates a slow individual task, such as a slow KV read or etcd commit.
