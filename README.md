# Fluxon

![](./pics/fluxon架构图20260423.png)

<div align="center">

[![Linux Only](https://img.shields.io/badge/Linux-Only-2ea44f)](#runtime-requirements)
[![Python](https://img.shields.io/badge/Python-%3E%3D3.10-3776AB)](#runtime-requirements)
[![Rust](https://img.shields.io/badge/Rust-1.93.0-000000)](./fluxon_rs/rust-toolchain.toml)
[![Latest](https://img.shields.io/badge/Latest-v0.2.1-f28500)](./fluxon_release)
[![Interfaces](https://img.shields.io/badge/Interfaces-KV%2FRPC%20%7C%20MQ%20%7C%20FS-1f6feb)](#interface-capabilities)

[English](./README.md) | [中文](./README_CN.md) | [Docs](./README.md) | [中文文档](./README_CN.md) | <a href="https://github.com/Tele-AI/fluxon" title="GitHub Repository"><img src="https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png" width="18" height="18" alt="GitHub repository" /></a>

</div>

Fluxon is a high-performance distributed communication and caching substrate for world models and other AI-native training and inference systems. It uses a single Rust-based integrated storage-and-transport foundation to provide unified `KV/RPC`, `MQ`, and `FS` interfaces, focusing on three classes of problems: cross-process and cross-node reuse of inference-side `KVCache` and `latent cache`, decoupled elastic message transport across heterogeneous resource pools, and remote access, `S3` forwarding, cache acceleration, and large-scale cross-cluster data migration for AI data and model files. As GPU performance keeps increasing, bottlenecks and wasted resources on CPU and IO paths become more visible. This increasingly calls for more efficient infrastructure to handle this high-performance work and reuse it across different business scenarios. Fluxon addresses this by first consolidating the complexity of low-level storage and transport in Rust, then exposing scenario-oriented `KV/RPC`, `MQ`, and `FS` interfaces on top.

## Foundation Capabilities

- End-to-end Rust: moves connection handling, protocol encoding/decoding, state-machine progression, shared-memory management, and observability collection into Rust hot paths
- Integrated storage and transport: prioritizes the cross-process shared-memory fast path and optimizes storage and transport within one unified data plane
- High-performance inter-node transport: inside the cluster, `RDMA` is preferred, with automatic `TCP` fallback, and NICs can be enabled, disabled, and switched dynamically from the GUI
- Automatic inter-node relay: supports automatic relay / forwarding across nodes and sub-clusters, reducing the integration cost of complex network topologies
- Global memory allocation and governance: uniformly manages global memory allocation, object lifecycles, capacity boundaries, and reclamation policies to avoid fragmentation and uncontrolled growth
- Unified role model: `master`, `owner_client`, and `external_client` cooperate in layers, organizing control-plane and data-plane responsibilities into a scalable tree topology while decoupling business service processes from data-plane resource governance and low-level communication paths
- Unified object interface: lets the system organize multi-field objects uniformly, balancing API flexibility, ease of use, and room for low-level optimization
- Tensor-native zero-copy handoff path: better suited for reusing high-frequency tensor objects across caching and transport paths
- Unified observability: uses the `Prometheus` protocol and `Greptime` to consolidate `metric / trace / log`, and includes a built-in GUI for cluster member state, log information, key metrics, and topology
- Shared capabilities across all three interfaces: `KV/RPC`, `MQ`, and `FS` reuse the same caching, transport, lease, capacity-governance, and observability substrate

![](./pics/fluxon_commu.png)

![](./pics/topology_ui.png)

For more usage details, see [User Docs](./fluxon_doc_en/user_doc/).

## Interface Capabilities

### Fluxon KV/RPC

Designed for world-model inference caches, state sharing, service-to-service calls, and tensor object reuse. In scenarios such as multi-view latent-space prediction, state extrapolation, and prefix-cache reuse, Fluxon KV/RPC provides a more general AI data plane rather than a point solution for only a single `KVCache` use case.

- Local cache replicas and eventually consistent read path: prioritizes local fast-path hits while synchronizing metadata asynchronously in the background
- Batched reclamation and hot-object management: advances invalid-object cleanup asynchronously through `batch_delete`, and combines it with `TinyLFU` to reuse hot objects more efficiently
- Simultaneous control over `L2` and `L3` in AI workloads: keeps global data objects indexed, discoverable, and reusable, reducing redundant memory waste from duplicate residency across cache tiers
- KV and RPC synergy: the same parameter organization, caching, and communication foundation serves both state storage and service-to-service calls

![](./pics/fluxon_kv.png)

### Fluxon MQ

Designed for heterogeneous training, data-processing pipelines, and intermediate-state handoff across resource pools. When the `producer` side and `consumer` side are split across different machines, different resource pools, or even different sub-clusters, Fluxon MQ consolidates message retention, capacity governance, and cross-cluster placement into one unified messaging layer.

- `Lease`-based retention semantics: binds message retention to the `channel`, ensuring data has bounded-time reliable retention before actual consumption
- `channel`-level prefix statistics and capacity governance: continuously tracks message counts and capacity usage boundaries for scaling and traffic control
- Cross-cluster load-aware placement: uses consumer-side location to decide payload placement, shortening prefetch paths and stabilizing throughput
- Co-designed with KV: message shells and member metadata stay on the control plane, while large payloads stay on the `FluxonKV` data plane, avoiding a second duplicated large-object transport stack

![](./pics/training_use_mq.png)

![](./pics/fluxon_mq.png)

### Fluxon FS

Designed for file IO acceleration, remote access, `S3` forwarding, cache hits, and large-scale cross-cluster migration for AI data and model files. When dealing with high-resolution video, trajectory samples, checkpoints, and other large file objects, Fluxon FS puts access acceleration, cache reuse, and migration progress into one unified file data plane.

- Unified caching system: directly reuses `FluxonKV/RPC` caching and communication capabilities, splitting files into `KeyValue` shards for sharded caching
- `S3` forwarding access: supports object-storage access and forwarding for AI data and model files
- Transparent Python file semantics: preserves the upper-layer `open() / read() / write()` experience as much as possible while reducing system-call and cross-process overhead
- Specialized optimization for small-file / large-file reads and writes: optimizes concurrency and transport paths by file granularity and read / write path to improve bandwidth utilization and overall throughput
- Large-scale cross-cluster migration: supports `PB`-scale data migration and keeps caching, transport, and failure recovery in one unified path

## Benchmark

The benchmark section mainly covers the `RPC`, `KV`, and `FS` data planes, and the related scripts and configurations are primarily under `fluxon_test_stack/`.

### Fluxon RPC Benchmark

The RPC benchmark mainly shows call latency and throughput across different message sizes and concurrency levels, to observe the stability and tail-latency behavior of the service-to-service call path.

![](./pics/fluxon_rpc_bench.png)

### Fluxon KV Benchmark

The `TCP` benchmark shows that Fluxon is significantly ahead of `MooncakeStore` and `Redis` on the two read-heavy workloads `Read-affinity` and `Read-Zipf`. For `put_only`, the current main constraint remains the inflight metadata deduplication path rather than payload transport.

![](./pics/kv_benchmark_chart.png)

### Fluxon FS Benchmark

The benchmark results show that small-file reads and large-file writes are already significantly ahead of `Alluxio`, large-file reads are roughly comparable, and small-file writes still have room for further optimization.

![](./pics/fs_benchmark_chart.png)

### Fluxon MQ Benchmark

`MQ` currently focuses mainly on scenario problems and data-plane design. The automated runtime entrypoints are `test_runner.py` and `fluxon_test_stack/`.

## Runtime Requirements

- Linux only
- Python `>= 3.10`
- When building from source, the Rust toolchain follows [fluxon_rs/rust-toolchain.toml](./fluxon_rs/rust-toolchain.toml), currently pinned to `1.93.0`
- External middleware dependencies: the minimum service plane requires `etcd` and `greptime`; `FluxonFS` features such as directory transfer and pre-scan that persist task state also require `TiKV PD` and `TiKV`
- Quick Start and runtime packaging workflows depend on Docker

## Quick Start

Quick Start is the shortest path to try Fluxon. For formal installation, deployment, and operations, see [User Docs](./fluxon_doc_en/user_doc/).

### KV Quick Start

```bash
docker run --rm -it --network host \
  hanbaoaaa/fluxon_quick_start:0.2.1 \
  --mode kv \
  --etcd-client-port 12379 \
  --master-p2p-port 31000 \
  --panel-port 18080 \
  --greptime-http-port 14000 \
  --kv-http-port 8083
```

Once inside, you can type:

```text
put demo:hello world
get demo:hello
del demo:hello
```

Runtime view:

![](./pics/quickstart_kv.png)

Open the printed link to view the KV Web UI:

![](./pics/quickstart_kvui.gif)

Related interface docs:

- [KV and RPC Interface](<./fluxon_doc_en/user_doc/User - 3 - KV and RPC Interface.md>)

### MQ Quick Start

```bash
docker run --rm -it --network host \
  hanbaoaaa/fluxon_quick_start:0.2.1 \
  --mode mq \
  --etcd-client-port 37379 \
  --kv-master-port 34200 \
  --greptime-http-port 14000 \
  --panel-port 18080
```

Once inside, you can type:

```text
put hello
put world
exit
```

The background consumer keeps printing received messages.  
Startup also prints the `MQ Web UI` address.

Runtime view:

![](./pics/quickstart_mq.png)

Related interface docs:

- [MQ Interface](<./fluxon_doc_en/user_doc/User - 4 - MQ Interface.md>)

### FS Quick Start

```bash
docker run --rm -it --network host \
  hanbaoaaa/fluxon_quick_start:0.2.1 \
  --mode fs \
  --etcd-client-port 36379 \
  --kv-master-port 34100 \
  --greptime-http-port 14000 \
  --panel-port 34180
```

Once inside, you can type:

```text
ls
echo "hello fs" > notes.txt
cat notes.txt
ui
```

FS Quick Start additionally prints:

- `fs_s3` endpoint
- Basic Auth entry; the default username / password is `admin / admin`

Runtime view:

![](./pics/quickstart_fs.png)

Open the printed link to view the FS Web UI:

![](./pics/quickstart_fsui.gif)

Related interface docs:

- [FS Interface](<./fluxon_doc_en/user_doc/User - 5 - FS Interface.md>)

## Repository Structure

- `fluxon_rs/`: Rust core implementation and low-level capabilities
- `fluxon_py/`: Python interfaces, runtime, and bindings
- `deployment/`: deployment and operations toolchain
- `scripts/`: utility scripts and helper entrypoints
- `setup_and_pack/`: packaging and release resource preparation entrypoints
- `examples/fluxon_quick_start/`: minimal runnable environment entrypoint
- `fluxon_test_stack/`: test stack, benchmarks, and gitops entrypoint

## Contributing

Thank you for your contribution. Start with the local developer docs:

- [Developer Docs](./fluxon_doc_en/dev_doc/)
- [Developer - 1 - Package core install artifacts](<./fluxon_doc_en/dev_doc/Developer - 1 - Package Core Install Artifacts.md>)
- [Developer - 2 - Package middleware and images](<./fluxon_doc_en/dev_doc/Developer - 2 - Package Middleware and Images.md>)

## Contributors

<a href="https://github.com/Tele-AI/fluxon/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Tele-AI/fluxon" />
</a>

Some earlier contribution records are no longer fully reflected in the current commit history. Historical highlights:

<p>
  <a href="https://github.com/yxrxy"><img src="https://wsrv.nl/?url=github.com/yxrxy.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="yxrxy" /></a>
  <a href="https://github.com/zTz01"><img src="https://wsrv.nl/?url=github.com/zTz01.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="zTz01" /></a>
  <a href="https://github.com/pakkah"><img src="https://wsrv.nl/?url=github.com/pakkah.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="pakkah" /></a>
  <a href="https://github.com/unity1263"><img src="https://wsrv.nl/?url=github.com/unity1263.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="unity1263" /></a>
  <a href="https://github.com/mumupika"><img src="https://wsrv.nl/?url=github.com/mumupika.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="mumupika" /></a>
  <a href="https://github.com/maplestarplayl"><img src="https://wsrv.nl/?url=github.com/maplestarplayl.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="maplestarplayl" /></a>
  <a href="https://github.com/RuileLu"><img src="https://wsrv.nl/?url=github.com/RuileLu.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="RuileLu" /></a>
  <a href="https://github.com/Summage"><img src="https://wsrv.nl/?url=github.com/Summage.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="Summage" /></a>
</p>

- `yxrxy`: FluxonFS implementation and optimization
- `zTz01`: KVCache optimization
- `pakkah`: RDMA support, VLM exploration
- `unity1263`: KV shared-memory design integration, benchmark toolchain
- `mumupika`: Initial MQ implementation
- `maplestarplayl`: IPC integration, SPDK integration
- `RuileLu`: KV lease support
- `Summage`: Initial KV architecture optimization

## License

Fluxon is open-sourced under Apache License 2.0, see [LICENSE](./LICENSE).

## Stargazers over time

[![Stargazers over time](https://starchart.cc/Tele-AI/fluxon.svg)](https://starchart.cc/Tele-AI/fluxon)
