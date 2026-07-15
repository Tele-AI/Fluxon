# Fluxon: A Rust Data Plane for KV Cache, RPC, Message Queues, and S3-Compatible Object Caching

![](../../pics/post_en.png)

As GPU compute power continues to scale, the bottleneck in AI systems is expanding from individual operators into the data plane. Inference services need to reuse `KV Cache`, `latent cache`, and prefix cache across requests, processes, and nodes. Training pipelines need to pass intermediate state across heterogeneous resource pools. Model files, sample data, and `Checkpoint` data need to move reliably across remote access, local caching, and cross-cluster migration paths.

Traditional approaches usually introduce separate caching systems, message queues, file systems, object storage gateways, and observability pipelines for these problems. Each system has its own connection, capacity, reclamation, routing, and observability logic. As model scale, request concurrency, training pipelines, and dataset size continue to grow, data-plane overhead keeps expanding and gradually consumes CPU, I/O, memory, and operational effort.

Fluxon is a storage and transport integrated distributed system for the AI training and inference data plane. It brings distributed key-value caching, `RPC`, message queues, and an `S3`-compatible file object cache acceleration layer into one data plane acceleration foundation, so these high-frequency data objects can reuse one set of caching, transport, lease, capacity governance, and observability capabilities.

## Origin: Pain from Elastic VAE Decoupling

One early engineering motivation for Fluxon came from cross-resource-pool data handoff in `VAE` decoupled heterogeneous training. We wanted `Producer` and `Consumer` components to scale independently in different resource pools and hand off intermediate `Payload` objects asynchronously, instead of binding training components again through a fixed communication group.

In this scenario, the boundary of `NCCL` is clear. It is a strong tool for synchronous collective communication inside a fixed member set, and it fits high-performance communication inside a training process group. However, when dynamic membership, asynchronous handoff, backpressure, message retention, and elastic scheduling across resource pools are required, the fixed-member communication model couples together the training components that we wanted to decouple.

We also tried to use high-performance transport and caching systems such as `MooncakeStore`, which target `KV Cache`, for large `Payload` movement in training pipelines. They have clear value in specific cache scenarios. When directly generalized into a common AI training data plane, however, new engineering constraints appear. Under high load and long-running stability tests, if low-level memory lifetime and transport state are not handled completely, the business side may see process crashes or task interruptions. When the `RDMA` network jitters, training pipelines often struggle to recover if there is no automatic fallback to paths such as `TCP`. In addition, if low-level exceptions cannot be captured reliably and propagated upstream, `Producer` / `Consumer` components can enter inconsistent states, block for a long time, or require repeated manual diagnosis.

These experiences made us realize that the AI data plane cannot only pursue peak performance on one segment of the transport path. It also needs disciplined error propagation, highly available network fallback, unified object lifecycle management, and a resource governance model that supports dynamic business membership.

Because existing systems struggled to cover elastic decoupling, high-performance movement, and failure fallback at the same time, Fluxon chose to redesign this path from the data plane acceleration foundation.

## Why a Unified AI Data Plane Is Needed

Data objects in AI workloads are becoming larger, hotter, and more frequently moved across boundaries.

On the inference side, high-frequency caches are no longer temporary objects inside a single process. In multi-replica inference services, world models, multimodal models, and long-context scenarios, these caches often need reuse across requests, processes, and nodes. If each instance independently manages cache residency, reclamation, and transport, GPU memory and host memory can be exhausted quickly by redundant data and duplicate residency.

Training-side data flow is also becoming more complex. Cross-resource-pool `Producer` / `Consumer` handoff scenarios, represented by VAE decoupled heterogeneous training, can place `Producer` and `Consumer` components on different machines, different resource pools, or even different sub-clusters. If intermediate `Payload` objects can only move through traditional MQ, file landing, or object storage across multiple systems, the data path becomes longer and capacity governance becomes harder.

The same applies on the storage side. High-resolution video, trajectory samples, model files, and `Checkpoint` data need to support remote access, local caching, `S3` forwarding, and cross-cluster migration at the same time. If file object caching and `KV` caching are split into two systems, large objects are repeatedly split, copied, landed, and re-indexed across systems.

Fluxon focuses on the full AI data-plane lifecycle: how objects are allocated, where they are placed, how they are transferred across nodes, when they are evicted, how business processes connect to them, and how issues are located when something goes wrong.

## Why Patchwork AI Data Planes Hit a Bottleneck

A common traditional AI data-plane pattern is to use one KV or custom cache for high-frequency caching, send intermediate handoff through MQ or object storage, attach file data through another file system or object cache, and collect observability status from each component separately.

This approach can assemble functionality quickly at an early stage, but several problems appear as the system grows.

**First, local scenario experience is hard to transfer.** `MooncakeStore` provides a specialized design for `KV Cache` scenarios by binding cache semantics and `RDMA` transport to a specific path. In more general data-plane scenarios, low-level transport and capacity governance are hard to move over directly.

**Second, unified resource control and scheduling are incomplete.** Framework-level caches such as `SGLang` `L2` keep in-framework indexes to provide lowest-latency access. External caches such as `MooncakeStore` as `L3` handle reuse across instances. These two layers often live in the same host `CPU` memory, while `L2` memory is hard to include in unified indexing, placement, and eviction governance. This also increases cache-crossing and object-handoff overhead.

**Third, there is no shared-memory fast path for colocated processes.** When `MooncakeStore` is used as `L3`, for example, the data path is usually organized around `RDMA` / `TCP`, which fits cross-node pooling and remote prefetch. If colocated `Worker` or `Producer` / `Consumer` processes cannot directly enter the shared-memory data plane, object handoff still detours through the network transport stack.

**Fourth, AI Infra lacks a dynamically elastic communication plane.** Collective communication libraries such as `NCCL` are strong tools for synchronous communication inside a fixed member set. In VAE decoupled heterogeneous training, cross-resource-pool `Producer` / `Consumer` handoff needs dynamic membership, asynchronous handoff, backpressure, message retention, and large `Payload` placement. A fixed-member communication model couples training components again and amplifies connection and recovery complexity.

**Fifth, business processes are coupled with data-plane resource governance.** When business processes start, stop, scale, or fail dynamically, and also contribute capacity, manage object lifecycles, and maintain cross-node connections, data-plane capacity and connection topology fluctuate with business lifecycles. This affects cache reuse, failure recovery, and operational diagnosis.

**Sixth, object lifecycle management is hard to converge.** When caches, messages, and file object access each maintain references, leases, eviction, routing, and reclamation state, it becomes difficult for a team to determine in one place whether an object is still being consumed, when it can be released, when it needs migration, or when indexes need rebuilding. The more components there are, the more state spreads across the business framework, cache layer, and transport layer.

**Seventh, observability pipelines are fragmented.** If cache hits, `Owner` queues, transport paths, object materialization, and business-process latency are spread across separate systems, it is hard to locate the bottleneck quickly when a performance issue occurs. Teams often have to stitch clues together across multiple metric sets and logs, and diagnosis cost grows with the number of components.

Fluxon is designed around these problems. It separates data-plane resources, object lifecycles, cross-node transport, and business integration into explicit abstractions, then governs them inside one data plane acceleration foundation.

## Fluxon's Approach

Fluxon is built on a storage and transport integrated data plane acceleration foundation and exposes three types of entrypoints upward:

| Entrypoint | Target Scenario | Capability |
| --- | --- | --- |
| Distributed key-value cache / `RPC` | Inference cache, state sharing, service-to-service calls, tensor object reuse | Unified key-value read/write and inter-node `RPC`, serving high-frequency state caches and tensor object reuse |
| Message queue | Cross-resource-pool `Producer` / `Consumer` intermediate-state handoff, such as VAE decoupled heterogeneous training, and data-processing pipelines | Dynamically elastic `AI Infra` communication plane that reuses the key-value cache data plane for large `Payload` objects |
| File object cache acceleration layer | AI data, model files, `Checkpoint` data, remote object access, `S3` forwarding | `S3`-compatible file object cache acceleration layer that reuses the data plane acceleration foundation with `KV/MQ`, so tensors, message `Payload` objects, and file objects enter a unified caching and acceleration path |

All three entrypoints reuse the same caching, transport, lease, capacity governance, object lifecycle, and observability capabilities. This means high-frequency data objects in these scenarios can all receive unified caching and acceleration inside the same data plane acceleration foundation. AI workloads do not need to build multiple separate data-plane paths.

For colocated resource governance, Fluxon prefers to organize cache layers by real physical resources. Host `CPU` memory should first enter shared memory and unified object lifecycle governance, reducing the extra paths introduced by artificial `L2` / `L3` splits inside business frameworks. Cross-machine, cross-cluster, and remote object access should use distributed transport paths.

This is the main difference between Fluxon and a single-point cache, single-point message queue, or single-point file interface. The core of Fluxon is the data plane acceleration foundation, and the APIs are the entrypoints that this foundation exposes for different scenarios.

## Architecture Layers: Clear Role Boundaries

Fluxon abstracts the control plane, data-plane resources, and business integration into three core roles. This layering decouples resource governance from business lifecycles and removes topology and connection bottlenecks for AI data-plane scale-out in large clusters.

Master, the control plane, uniformly manages memory allocation, object placement, eviction, routing, and leases. It converges key decisions into the control plane, keeps object lifecycles consistent across multi-process and multi-node scenarios, and enables precise memory reclamation.

Owner Client, the data-plane resource provider, is a resident node that contributes the shared memory pool and carries inter-Owner communication. Business processes connect to their local Owner, and the Owner performs cross-machine transport. This local proxy plus cross-machine backbone structure avoids connection storms caused by direct interconnection between business processes. Cross-machine connections are strictly converged between Owners, so network topology complexity, routing state, and data movement paths remain easier to control as the cluster grows.

External Client, the business integration layer, carries dynamic access from inference services, MQ `Producer` / `Consumer`, FluxonFS, FluxonOps, and other components, and does not contribute cluster capacity. Because External Client does not provide physical capacity, frequent business-side start/stop, abnormal restart, or elastic scaling does not directly trigger data-plane capacity migration or `Rebalance` churn. Elasticity of compute services is isolated from stability of the data foundation, and the business layer can implement stateless scale-out more easily.

Together, these three roles build the base stability and scalability of the AI data plane. Master, as the decision maker, converges global state and prevents object placement, routing, and reclamation logic from spreading into business processes. Owner Client, as the provider and carrier, anchors physical capacity and the cross-machine backbone, keeping cross-machine connections at the data-plane resource layer. External Client, as the accessor, carries elastic business workloads without disturbing the underlying topology. Once these boundaries are clear, Fluxon can let cache, message, and file object access truly reuse the same scalable data plane acceleration foundation without giving up the single-machine fast path.

## Data Paths: Local Shared Memory, Cross-Node P2P, and Automatic Relay

Fluxon covers local object handoff, cross-node object movement, and relay forwarding under complex network topologies.

On the local path, business processes first connect to the Owner shared memory pool and reduce object-handoff cost through SHM / Busy Polling / Epoll UDS. High-frequency data objects can avoid unnecessary copying and reconstruction.

On the cross-node path, Owners transfer data through P2P transport. The deployment can select `RDMA`, `TCP`, `QUIC`, and other paths, and Fluxon supports automatic relay forwarding across nodes and sub-clusters. When the source Owner and target Owner cannot communicate directly through the preferred link, the data plane can still complete transport through a relay path. Business processes do not need to understand complex network topology. They only connect to the local Owner, and cross-machine data movement converges to Owner-to-Owner paths. This layered architecture reduces cross-machine connection spread between business processes and further improves system scalability.

This design allows Fluxon to serve multi-process reuse inside one machine, cross-node reuse inside a cluster, and cross-cluster data flow at the same time. The former focuses on shared memory and object handoff, while the latter focuses on connection convergence, routing adaptation, and cross-machine transport.

## Core Engine: Rust for a Low-Overhead, Controllable Data Plane Acceleration Foundation

Rising GPU compute and growing cluster scale make I/O and CPU explicit bottlenecks in AI systems. Connection handling, protocol encoding and decoding, high-concurrency transport, shared-memory management, and observability collection all sit on data-plane hot paths. If these hot paths are filled with interpreted execution, runtime scheduling, cross-language-boundary copies, and uncontrolled memory copying, the time saved on the GPU side can be consumed by data-plane overhead.

Fluxon implements these key paths in Rust, with the goal of bringing concurrency safety, memory lifetimes, and system-call boundaries under stronger engineering constraints.

- Concurrency: CPU-intensive paths are not constrained by the GIL, making it easier to use multi-core resources fully.
- Predictable latency: no GC pauses, reducing unpredictable jitter on hot paths.
- Long-running safety: ownership, lifetimes, and the type system constrain shared memory, connection state, and object references.
- Auditable code: strongly typed interfaces and explicit state machines make the low-level data plane easier to review by humans and tools.

This system-level control of the low-level data plane is the foundation for efficient and safe movement of high-frequency data objects on the same data plane acceleration foundation. For the low-level data plane, this matters more than simply pursuing peak performance of one interface.

## Unified Observability

Fluxon's observability foundation uses GreptimeDB, which is also built in Rust. This matches Fluxon's low-level philosophy: GreptimeDB converges the three observability pillars, Metrics, Logs, and Traces, into one engine, just as Fluxon converges cache, message, and file object access into one data plane acceleration foundation.

Fluxon collects metrics through the Prometheus protocol, combines them with tracing and structured logs, and provides a built-in GUI that clearly presents cluster topology, member status, key latencies, and queue depths.

For a data-plane system, observability is part of governance, not a nice-to-have. Only when an issue can be located precisely to Owner, External Client, transport path, queue waiting, or object processing can the data-plane foundation be truly governable. Unified observability is a key part of closing the loop for Fluxon's data plane acceleration foundation.

## FluxonKV: Distributed Key-Value Cache and RPC on a Shared Data Plane Acceleration Foundation

Fluxon `KV/RPC` targets world model inference caches, state sharing, service-to-service calls, and tensor object reuse. In scenarios such as multi-view latent-space prediction, state extrapolation, and prefix-cache reuse, it covers a more general AI data plane than a single `KV Cache` scenario.

KV and RPC share the same parameter organization, caching, and communication path. State storage, object reuse, and service-to-service calls do not need two separate paths. High-frequency calls and large-object reuse can be completed inside the same role model.

![](../../pics/fluxon_kv.png)

On the read path, Fluxon prioritizes local fast-path hits and advances metadata synchronization asynchronously in the background. The system reduces duplicate residency and memory waste across multiple cache tiers through hot-object reuse, and uses batched reclamation to converge fragmented control-plane interactions into batch operations. This reduces control-plane traffic and compute overhead, improving overall throughput and long-running behavior.

For inference systems, this means high-frequency state caches, tensor objects, and service calls can enter the same data plane acceleration foundation without repeated conversion between a cache system and an RPC system.

## FluxonMQ: Dynamically Elastic Communication Plane for AI Data Flow

Fluxon `MQ` targets cross-resource-pool `Producer` / `Consumer` intermediate-state handoff and data-processing pipelines. VAE decoupled heterogeneous training is one representative scenario. When `Producer` and `Consumer` components are distributed across different machines, different resource pools, or even different sub-clusters, `MQ` converges message retention, capacity governance, and cross-cluster placement into a unified messaging layer.

Traditional `MQ` systems are usually built on `TCP` and disk logs. Their original design is not oriented toward large `Tensor Payload` objects, so they lack a native high-speed `RDMA` data path. When intermediate state reaches dozens of `MB` or even `GB`, general-purpose `MQ` systems struggle to satisfy low-latency handoff, capacity governance, and high-bandwidth movement at the same time.

Collective communication such as `NCCL` fits synchronous communication inside a fixed member set. Cross-resource-pool `Producer` / `Consumer` handoff needs dynamic membership, asynchronous handoff, backpressure, message retention, and large `Payload` placement. Specialized cache systems such as `MooncakeStore` perform strongly for `KV Cache` reuse, but their core cache-eviction semantics do not directly replace the `MQ` requirement that a message must remain before it is consumed. Their transport paths also usually depend on deployment-side choices between `RDMA` / `TCP`, making it difficult for them to take on automatic fallback, cross-cluster relay, and elastic handoff inside a data plane acceleration foundation.

The core design philosophy here is: keep the control plane light, retaining only message shells, member topology, and Offset; keep the data plane heavy, moving large Payload objects directly through the KV data plane. This means Fluxon does not need to build a second large-object transport path for the message queue.

`Producer` / `Consumer` components join dynamically as `External Client` instances without changing cluster capacity. They can scale with business load, while `Owner` remains stable as the resident data-plane resource provider.

`Lease` binds message retention to the message channel, giving pre-consumption data retention an explicit time boundary. In cross-resource-pool and cross-sub-cluster scenarios, `Payload` placement can use the consumer-side location to shorten the prefetch path as much as possible.

## FluxonFS: S3-Compatible File Object Cache Acceleration Layer

Fluxon `FS` is positioned as an `S3`-compatible file object cache acceleration layer. It targets AI data, model files, `Checkpoint` data, high-resolution video, and trajectory samples, covering remote access, cache hits, `S3` forwarding, and cross-cluster migration.

The key point of FS is reusing `KV/RPC` caching and communication capabilities. Files are split into `KeyValue` shards and enter Fluxon's caching, transport, and capacity-governance paths. In this way, files, objects, and KV caches converge from three fragmented systems into different entrypoints of the data plane acceleration foundation.

For AI data platforms, this path can reduce the cost of switching file data across remote access, local caching, and cross-cluster migration. The upper layer still uses file object semantics, while the lower layer reuses unified data-plane capabilities.

## Benchmark

The current public benchmark charts cover three paths: RPC, KV, and FS, the file object cache acceleration layer. The following data is generated from specific test scenarios and focuses on architectural benefits and performance boundaries across different data paths.

Note: Benchmark data is generated under specific topology and Payload sizes. Actual business gains depend on network environment, object size, and access pattern.

### RPC Benchmark

The RPC Benchmark compares ZeroRPC, Fluxon TCP Thread, and Fluxon RDMA under a 4 KB echo Payload, covering throughput and end-to-end latency. The charts show that Fluxon's RPC path significantly reduces latency and improves aggregate throughput in both P1 and P8 panels.

![](../../pics/fluxon_rpc_bench.png)

This result corresponds to the service-to-service call path. It shows that the parameter organization, routing, and communication foundation shared by RPC and KV can carry high-frequency small-Payload calls. End-to-end time for actual business handlers will still be affected by application logic.

### KV Benchmark

The KV Benchmark shows three scenarios: READ_AFFINITY, READ_ZIPF, and PUT_ONLY. Read-heavy scenarios are the current public result's strong area, especially for explaining the benefits from locality, hot-object reuse, and cross-node object location.

![](../../pics/kv_benchmark_chart.png)

In the pure-write PUT_ONLY scenario, the current performance constraint is mainly in the inflight metadata deduplication path rather than Payload transport itself. This is also one of the core directions for later optimization.

### FS Benchmark: File Object Cache Acceleration

`FS Benchmark` compares Fluxon `FS` with `Alluxio`, covering small-file read, large-file read, small-file write, and large-file write under cache warmup. The most prominent area in the chart is large-file write. Small-file read already shows an advantage, large-file read performance is roughly on par, and small-file write performance still has room for further optimization.

![](../../pics/fs_benchmark_chart.png)

This result corresponds to the file object cache acceleration layer. Fluxon FS benefits from reusing the KV/RPC data plane, while small-file writes are still affected by upper-layer open, write, close, and commit flows.

## Why Open Source

Fluxon open-sources a complete data plane acceleration foundation for the AI data plane: Rust core implementation, Python interfaces, distributed key-value cache, RPC, message queue, `S3`-compatible file object cache acceleration layer, deployment toolchain, test stack, and Benchmark are all organized in one project.

We want developers to directly see how Fluxon organizes the control plane, data plane, business integration, observability, and tests, and to understand the data plane acceleration foundation behind these interfaces. AI infrastructure is moving from single-model, single-service, and single-cluster forms toward more complex data-flow patterns. Cache, message, and file object access all need to be considered again inside one data-plane path.

Fluxon is open-sourced under Apache License 2.0. We hope to work with the community on AI inference caching, heterogeneous training, file object caching, cross-node transport, shared memory, Rust data planes, and observability systems.

## Next Steps

Fluxon is still evolving quickly. In the short term, we will continue to optimize `KV Cache` integration in `SGLang`. To meet `L2` requirements for extremely low-latency access, the interface layer will also gain new capabilities that make cooperation among in-framework indexing, local shared memory, and the data plane acceleration foundation more direct.

In the longer term, Fluxon aims to become the data plane acceleration foundation inside AI systems: bringing AI workload data objects and cross-node transport into one governable and observable path.

We expect algorithm engineers and model-serving developers to spend more energy on model innovation itself, instead of repeatedly paying for duplicated low-level data movement and reactive patches. Fluxon is designed to serve as this data plane acceleration foundation and support more complex, freer data movement in the AI era.

Fluxon is developed by the AI Infra team at the Artificial Intelligence Research Institute of China Telecom (TeleAI), led by China Telecom Chief Scientist Professor Xuelong Li. Fluxon is open-sourced under Apache License 2.0. GitHub repository: https://github.com/Tele-AI/Fluxon. Developers interested in AI inference caching, heterogeneous training, Rust data planes, and distributed systems are welcome to participate.
