# User - 4 - MQ Interface

## Overview

Fluxon MQ is the queue layer built on top of the KV substrate. It is not a separate service stack. It reuses the same service plane, the same local shared memory pool, and the same Python client attachment path, then adds producer / consumer semantics.

MQ objects can be understood in three layers:

- Service plane: `etcd`, `greptime`, `fluxonkv master`
- Local resident data-plane instance: `owner`
- Business-process attachment layer: `FluxonKvClientConfig`, `new_store(...) -> KvClient`, plus the bound `producer` / `consumer` handles

```text
etcd + greptime + fluxonkv master
                |
                v
         kvclient owner
                |
                v
+--------------------------------------------------------------+
| kvclient external                                            |
| FluxonKvClientConfig -> new_store(...) -> KvClient(store)    |
+--------------------------------------------------------------+
                                |
                                +-> new_or_bind_with_unique_key(...)
                                        |
                                        +-> producer
                                        +-> consumer
```

See [Architecture and Concepts](<./User - 1 - Architecture and Concepts.md>) for `owner`, `external client`, and shared-memory terminology, and [User - 3 - KV and RPC Interface](<./User - 3 - KV and RPC Interface.md>) for `new_store(...) -> KvClient`.

MQ user processes have one fixed role constraint: producer / consumer processes must run as zero-contribution `external_client` attachments. Their lifecycles are expected to be dynamic, so they must not change cluster capacity; the long-lived capacity provider remains the local `owner`.

## Service Plane

MQ reuses the KV service plane directly. Start the shared chain first:

1. `greptime`
2. `etcd`
3. `fluxonkv master`
4. `owner`
5. your producer / consumer process

The common startup pattern is still `examples/start_master_owner.py`.

## Object Relationship

```text
FluxonKvClientConfig
        |
        v
new_store(cfg) -> KvClient (store)
        |
        +-- new_or_bind_with_unique_key(...)
        |
        +-- producer handle
        |       put_data(...)
        |       close()
        |
        +-- consumer handle
        |       get_data(...)
        |       close()
        |
        v
store.close()
```

Key rules:

- `new_store(...)` creates `KvClient`; `store` is just the example variable name
- `new_or_bind_with_unique_key(...)` is not a standalone public entrypoint; it must run on top of a store
- Shutdown order is fixed: close the MQ handle first, then close `store`

## Minimal MQ Example

The public minimal example is `examples/start_mpmc_demo.py`.

Run one producer and one consumer after the service plane is ready:

```bash
python3 examples/start_mpmc_demo.py --role producer
python3 examples/start_mpmc_demo.py --role consumer
```

This example keeps one process-local `seq` counter. Restarting the producer resets that counter; it is not a cross-process persistent sequence.

The most important part of the example is the ownership chain:

- one external `KvClient`
- one bound producer or consumer handle on top of that store
- `Ctrl-C` only requests shutdown and closes the handle once
- `ProducerClosedError` and `ChannelClosedError` are normal close-path signals

Common startup form:

```bash
FLUXON_LOG=INFO python3 examples/start_mpmc_demo.py --role producer
FLUXON_LOG=DEBUG python3 examples/start_mpmc_demo.py --role consumer
```

## Common Interfaces

- `new_or_bind_with_unique_key(api, chan_config, unique_id, chan_type, chan_role)`: bind if present, otherwise create
- `producer.put_data(value: FlatDict) -> Result[bool, ApiError]`: send one message
- `consumer.get_data(batch_size: int = 1, try_time: Optional[int] = None, prefetch_num: int = 0) -> Result[List[Any], ApiError]`: fetch messages in batches
- `producer.get_chan_id()` / `consumer.get_chan_id()`: current channel id
- `producer.get_producer_id()` / `consumer.get_consumer_id()`: current member id
- `close() -> Result[OkNone, ApiError]`: close the current MQ handle

Parameter constraints:

- `chan_type` is commonly `ChanType.MPMC`, and `ChanType.MPSC` is also supported
- `chan_role` must be either `ChanRole.PRODUCER` or `ChanRole.CONSUMER`
- `try_time` is a second-level wait bound

### Common Error Handling

- `new_or_bind_with_unique_key(...)` fails: first check cluster name, shared memory / shared file paths, `unique_id`, and that both ends use matching roles
- `producer.put_data(...)` returns `ProducerClosedError`: treat it as a normal shutdown signal and exit the main loop
- `consumer.get_data(...)` returns `ChannelClosedError`: treat it as a normal shutdown signal and exit the main loop

## Log Paths

- Python-side MQ logs come from `init_logger(...)` and go to the current terminal by default; the threshold is controlled by `FLUXON_LOG`
- Rust / KV background logs follow the shared service-plane pipeline, and the master's local log authority is `master_cfg["log_dir"]`
- `shared_file_path` remains the local shared-file authority for `shared.json` and related files

If `master.monitoring.otlp_log_api` is configured, backend logs continue to flow into the Greptime `fluxon_logs` table.

## Web Monitoring

Two UI tables are especially useful:

- `Channels`: channel-level summary
- `Members`: individual producer / consumer detail

### Channel Summary

`producer_offsets` is rendered as:

```text
producer_idx: produce_offset/consume_offset
```

Example:

```text
producer_1: 101/88, producer_2: 57/57
```

Both offsets mean "the next offset":

- `produce_offset`: next message offset the producer will write
- `consume_offset`: next offset the consumer will commit

Current backlog per producer is:

```text
max(produce_offset - consume_offset, 0)
```

`current_inflight` is the sum across producers in the same channel.

### Member Detail

Useful fields in `Members`:

- `channel_unique_keys`
- `produce_offset` / `consume_offset`
- `chan_id`, `owner_id`, `external_client_id`

When you attach from Python through `new_or_bind_with_unique_key(...)`, a common lookup flow is:

1. search `channel_unique_keys`
2. inspect `chan_id`, `owner_id`, `external_client_id`, offsets, and latency-related fields

## Latency Triage

MQ prints consumer-latency statistics roughly every 30 seconds. Search logs with these keywords:

| Keyword | Layer | Meaning |
|---|---|---|
| `py-get latency` | Python caller | total `get_data()` latency |
| `get_one breakdown` | PyO3 bridge | breakdown of cross-language wait time |
| `MpscConsumer prefetch` | Rust MQ layer | prefetch-queue and single-task cost |

Quick reading:

- high `py-get` total latency -> inspect PyO3-side `avg_wait_rx_ms`
- high Rust `avg_get_handle_ms` -> prefetch queue is empty, or the producer side is idle, or the window is too small
- high Rust `avg_handle_await_ms` -> the single task is slow, for example `kv_get` or etcd commit
