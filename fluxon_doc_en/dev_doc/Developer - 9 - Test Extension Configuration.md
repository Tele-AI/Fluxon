# Developer - 9 - Test Extension Configuration

## Scope

`test_spec_config` is a developer-only surface for tests, benchmarks, fault isolation, and implementation experiments. Normal deployments should omit the entire block. These switches may bypass production paths or change performance behavior, and they are not part of the stable user configuration contract.

The stable user-facing block for network policy and transport tuning is `network`. There is no public `protocol` YAML block. Protocol selection remains test-only under `test_spec_config.protocol_type`.

Unknown fields are rejected. The runtime accepts `test_spec_config` in Fluxon KV master and client configurations; TestStack accepts the same fields under `profiles.<id>.runtime.test_stack.runtime_config.kv_base.test_spec_config` and adds one runner-only field described below.

## Protocol selection

Select TCP for a test with:

```yaml
test_spec_config:
  protocol_type: tcp
```

Select RDMA and pin test devices with:

```yaml
test_spec_config:
  protocol_type: rdma
  rdma_device_names:
    - mlx5_0
```

If `protocol_type` is omitted, the current Fluxon KV default remains RDMA. `rdma_device_names` in this block is a test override and takes precedence over `network.rdma_device_names`. The Mooncake test path remains RDMA and does not consume this Fluxon KV protocol override.

## Runtime fields

### Observability, indexes, and data-path controls

| Field | Type and default | Effect |
| --- | --- | --- |
| `disable_observability` | `bool`, `false` | Disables Fluxon KV observability and OTLP background work. |
| `disable_master_replica_cache` | `bool`, `false` | Disables master replica-cache maintenance. |
| `disable_prefix_index` | `bool`, `false` | Disables master prefix-index maintenance. |
| `prefer_local_placement` | `bool`, `false` | Reserved parsed test field; no current runtime branch consumes it. |
| `short_circuit_put_payload_path` | `bool`, `false` | Keeps put allocation but skips payload copy and transfer for path isolation. |
| `skip_put_end_commit` | `bool`, `false` | Returns after payload transfer without the `put_end` commit; cleanup relies on inflight-put TTL. |

### Protocol, IPC, and thread tuning

| Field | Type and default | Effect |
| --- | --- | --- |
| `protocol_type` | `tcp` / `rdma`; omitted means RDMA except for derived side-transfer workers | Selects the Fluxon KV protocol for a test. |
| `transport_mode` | `transfer_only` / `transfer_with_rpc`; effective default `transfer_with_rpc` except for side-transfer workers | Enables or disables the transfer RPC fast path. |
| `rdma_device_names` | non-empty string list; omitted | Pins and fans out RDMA devices for a test; values are trimmed, deduplicated, and sorted. |
| `disable_local_ipc` | `bool`, `false` | Disables all same-machine local IPC so peers use direct transport. |
| `disable_crossowner_ipc` | `bool`, `false` | Keeps same-owner local IPC but sends same-host cross-owner traffic through direct transport. |
| `enable_iceoryx_logs` | `bool`, `false` | Enables normally suppressed iceoryx2 logs. |
| `iceoryx_external_busy_poll` | `bool`, `false` | Uses busy polling for the external local-IPC receiver instead of its wait set. |
| `iceoryx_owner_client_busy_poll` | `bool`, `true` | Uses busy polling for the owner/client local-IPC receiver. |
| `tcp_thread_reactor_shard_count` | integer `1..16`; omitted | Overrides the TCP-thread reactor shard count. |
| `tcp_thread_bulk_lane_count` | integer `1..8`; omitted | Overrides the TCP-thread bulk-lane count. |
| `tcp_thread_control_lane_count` | integer `1..8`; omitted | Overrides the TCP-thread control-lane count. |
| `user_rpc_sync_handler_thread_count` | positive integer; omitted | Overrides the dedicated synchronous user-RPC worker count. |
| `require_transfer_rpc_fast_path_ready_timeout_seconds` | positive integer; omitted | Makes owner readiness wait for the transfer RPC fast path, with the given timeout. |

### Side transfer and KV SSD experiments

| Field | Type and default | Effect |
| --- | --- | --- |
| `enable_side_transfer` | `bool`, `false` | Enables the TCP side-transfer fast path for the configured client. |
| `side_transfer_worker_count` | non-negative integer, `0` | Makes an owner start the requested number of side-transfer workers. |
| `side_transfer_worker_p2p_port_base` | non-zero `u16`; omitted | Pins worker ports to `base + worker_index`. |
| `side_transfer_role` | `worker`; omitted | Marks a zero-contribution client as an internal side-transfer worker. Its protocol is derived as TCP. |
| `kv_ssd_storage_backend` | `native` / `foyer`, `native` | Selects the KV SSD implementation for a test. |
| `kv_ssd_uring_mode` | `single_buffer` / `iovec`, `single_buffer` | Selects the native KV SSD io_uring buffer mode. |

## Combination constraints

- `rdma_device_names` is valid only with effective RDMA. It is rejected with `protocol_type: tcp`.
- An explicitly configured `transport_mode` on an RDMA test requires explicit `test_spec_config.rdma_device_names`; this prevents implicit device selection in benchmark variants. TCP does not require an RDMA device list.
- `require_transfer_rpc_fast_path_ready_timeout_seconds` requires `transport_mode: transfer_with_rpc`. RDMA also requires an explicit test device list; TCP must be selected explicitly.
- `side_transfer_role: worker` requires zero-contribution client mode. A worker cannot set `protocol_type` or `rdma_device_names`, because its TCP protocol is derived from the role.
- A positive `side_transfer_worker_count` is valid only on an owner. If a port base is set, it must be non-zero and every derived worker port must fit in `u16`.
- `kv_ssd_storage_backend: foyer` supports only `kv_ssd_uring_mode: single_buffer`; `iovec` is native-only.

## TestStack-only field

TestStack additionally accepts:

| Field | Type and default | Effect |
| --- | --- | --- |
| `p2p_transport_impl` | `tcp` / `tcp_thread`; omitted | Selects the matching staged P2P transport artifact set. The runner consumes this field and removes it before generating runtime YAML. |

Example location:

```yaml
profiles:
  example_profile:
    runtime:
      test_stack:
        runtime_config:
          kv_base:
            test_spec_config:
              protocol_type: tcp
              p2p_transport_impl: tcp_thread
```

Do not place `p2p_transport_impl` directly in a Fluxon KV master or client runtime configuration.
