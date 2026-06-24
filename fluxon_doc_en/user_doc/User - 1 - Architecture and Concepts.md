# User - 1 - Architecture and Concepts

## Architecture and Concepts

Project overview and doc navigation start from the [Fluxon documentation home](../../README.md).

This page explains the core concepts and config fields that appear throughout the rest of the docs.

### System Overview

![](../../pics/架构全景图.png)

- Control plane / metadata: `etcd + master` for members, leases, routing, and connection-state metadata
- Data plane: `shared memory + transfer engine` for same-host reuse and cross-node data movement
- KV: base read/write and RPC capability; `owner` contributes the memory pool and `external` attaches in zero-contribution mode
- MQ: queue semantics built on top of KV and reusing the same service plane and shared memory pool
- FluxonFS: remote file access built on top of KV; access control is persisted through `fluxon_fs.master_panel.access_db_path`, and transfer-state persistence uses `fluxon_fs.master_panel.transfer_state_store`
- FluxonOps: deployment and operations control plane built on KV

### Distributed Deployment View

![](../../pics/deploy_arch_1.png)

Control plane:

- **Fluxon KV Master**: cluster management, routing, coordination
- **ETCD**: metadata store for member state, MQ state, offsets, and connection data
- **Prometheus / GreptimeDB**: metrics collection and storage for the monitoring panel

Per machine:

- **Fluxon KV Owner**: contributes local data-plane resources and shared memory
- **Fluxon KV External**: attaches to the owner's shared pool and exposes access to business processes

Cross-machine transport:

- **High Performance P2P**: owners exchange data over RDMA / DPDK / SPDK / WebSocket / TCP / QUIC with busy-polling-first behavior and cross-cluster relay

### Roles

| Role | Responsibility |
|---|---|
| **master** | Control-plane entrypoint: membership, routing, leases, monitoring broadcast |
| **owner_client** | Data-plane resource provider: contributes the shared memory pool |
| **external_client** | User-facing access point: attaches to an owner's pool without contributing memory |

### Core Config Fields

`cluster_name`

- Logical cluster name
- Separates metadata keyspaces across clusters
- Must match on every component in one cluster

`instance_key`

- Unique process identifier
- Used for registration, RPC addressing, logs, and monitoring
- Must be unique inside one `cluster_name`

`node_id` / `member_id`

- Target locator fields in APIs
- Usually equal to `instance_key`

`etcd_endpoints`

- List of etcd addresses
- Some components need `http://...`, others accept `host:port`
- MQ and panel features depend on etcd reachability

`prometheus_base_url`

- Metrics source queried by the panel
- The panel only reads; it does not scrape by itself

`share_mem_path`

- Shared bundle root. Runtime appends `cluster_name`, and that cluster-scoped directory holds `mmap.file`, `shared.json`, and peer metadata.

`log_dir`

- Master-local log authority

`contribute_to_cluster_pool_size`

- Memory contribution config
- Non-zero for owners, omitted or zero for external clients

### MQ Concepts

`unique_key` and `chan_id`

- `unique_key` is the stable logical channel name
- `chan_id` is the bound existing channel instance id

See [User - 4 - MQ Interface](<./User - 4 - MQ Interface.md>) for details.
