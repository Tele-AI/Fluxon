# Roadmap

## Coming soon

- [CI] Cover all tests with GitHub Actions and fix existing bugs
- [KV] Adapt and optimize `sglang` `KV Cache`, and add a `BatchKV` interface plus a local-side elastic preallocated-memory `put` interface with `write-back` mode
- [OPS] Consolidate GitHub Actions integration tests into `FluxonOps` so internal clusters can reuse them directly later

## Release Notes

### 0.2.1

- [PERF] Optimize `RPC`, `KV`, and `FS` performance
- [MQ] Fix MQ control-plane scalability issues
- [etcd] Fix the gRPC size limit issue when listing etcd prefixes
- [OSS] Improve open-source readiness and related workflows

### 0.1.7

- [KV\RPC] Added the asynchronous multi-tier connection manager `tiermanager` and decoupled the main transport path from the transport control plane
- [KV\RPC] Added `tcp_thread` to improve TCP transport throughput
- [KV\RPC] Converged the external cross-owner path to an intra-node ICE plus inter-node TCP/RDMA layered design
- [FS] Added FluxonFS to manage files on top of KV and provide one unified cache for multimodal data payloads
- [FS] Added distributed concurrent scan and transfer for large cross-domain folders
- [OPS] Added FluxonOps for bare-process deployment and hot update across Fluxon clusters

### 0.1.6

- [KV\RPC] Added inter-process communication support
- [LIB] Refactored framework lifecycle management to make invariants easier to maintain
- [KV\RPC] Added multi-hop relay for cross-cluster transport
- [KV\RPC] Added `cp_kv_to_file` as a building block for future intermediate cache layers
- [TOOL] Added MQ coverage in the monitoring panel

### 0.1.5

- [KV\RPC] Tuned `tquic`; overall QUIC performance now exceeds the previous `qp2p` path and meets both low-latency control-plane and high-throughput data-plane needs

### 0.1.4

- [TOOL] Added a simple SSR monitoring panel so a separate Grafana deployment is not required
- [MQ] Rebuilt MQ in Rust for better stability, better performance, and prefetch support

### 0.1.2

- [KV\RPC] Added SHM-based shared-memory architecture with a stronger two-tier scale-out model, better memory efficiency, and better hit rate
