# SGLang Fluxon KV 集成设计

## 设计目标

本文说明开源仓库中 SGLang HiCache 接入 Fluxon KV 的 hostless 实现设计，聚焦接口契约、状态归属和生命周期边界。本文不是通用 `FlatDict` KV API 的完整说明。

当前调用链主要分为三条主线：

- 写入侧：`local_fast_put_start -> SGLang native write -> local_fast_put_commit`。
- 读取侧：`get_start -> get_transfer -> SGLang native restore -> release_views`。
- 放弃读取侧 restore 时：`cancel_get_transfer` 释放 `get_start` 持有的资源。

Fluxon 仍然按 key 保存 opaque value bytes。SGLang 负责 page key、page layout、MHA/MLA/Mamba value 内部格式、GPU KV cache index 和 native kernel 调度；Fluxon 负责 key-version 路由、put 准入与 reservation、owner-local reserve、host value 地址、holder 生命周期和跨 owner 数据面。

这个集成把 SGLang 逻辑上的 L2/L3 缓存落到 Fluxon 统一管理的本机/远端 KV 层中。这样可以用同一套 owner、holder、commit/release 语义管理 KV page，减少传统 L2 host cache 与 L3 backend 分属不同系统时产生的同机重复缓存和生命周期割裂。

常见部署下，一台机器启动一个 Fluxon owner；同机多个 GPU 对应的多个 SGLang worker 进程通过 external/client attach 到同一个 owner shared segment。相比一个 SGLang 进程一个后端 segment、进程间不共享后端 segment 的形态，Fluxon 把同机 host/shared memory、owner local reserve slots 和 holder 生命周期放到同一个 owner 生命周期模型里。这样多个 SGLang worker 可以通过同一个 owner segment 获得本机快速可见性和受控的本地可写内存供给，减少各进程为了各自安全边界重复持有 KV、固定预留 segment 或独立维护 pin/release 状态带来的浪费。

## 范围边界

| 范围 | 当前结论 |
| --- | --- |
| SGLang hostless 写入 | 已接入。SGLang 通过 `local_fast_put_start` 取得一批可写 host value 地址，native kernel 写入后再调用 `local_fast_put_commit` 提交。 |
| SGLang hostless 读取 | 使用 `get_start/get_transfer/release_views`。`get_start` 先计算连续可恢复前缀，`get_transfer` 再把可恢复前缀转换成 readable `plan_ptr`。 |
| Fluxon value layout | Fluxon 不理解 KV page 内部布局；只按 `key + value_len` 管理连续字节。 |
| SGLang node 状态 | SGLang 的 `storage_*` 字段是调度层状态，不等同于 Fluxon master route；跨节点复用以 Fluxon commit future 成功为准。 |

## 总体架构

```mermaid
flowchart LR
    A["SGLang HiCache radix node"] --> B["HiCacheFluxon backend"]
    B --> C["FluxonKVCacheStore"]
    C --> D["fluxon_pyo3::KvClient"]
    D --> E["Fluxon external / owner"]
    E --> F["Fluxon master"]

    B --> G["sgl-kernel kvcacheio"]
    D --> H["plan_ptr blob<br/>magic/count/value_ptrs"]
    H --> G
    G --> I["GPU KV cache"]
    E --> J["owner shared segment<br/>local reserve slots / MemHolder"]
    J --> H
```

这里有两个层次：

- KV 语义层：SGLang 传入 page key，Fluxon 对外保存 `key -> value`。
- hostless 数据层：Fluxon 返回 `plan_ptr`，SGLang native kernel 根据 blob 里的 `value_ptrs[]` 直接执行 GPU/host 数据传输。

从缓存物理层级看，Fluxon 把传统 L2/L3 逻辑抽象落到 local side / remote side：local side 覆盖本机 GPU KV、owner shared segment、owner local reserve slots 和 `MemHolder`；remote side 覆盖跨 owner 或跨机器的数据面。多个 SGLang worker attach 到同一个 owner segment 时，同机 KV bytes 不需要按 worker 进程重复保存在多个后端 segment 中。

`plan_ptr` 只是一轮 backup 或 restore 的短生命周期 carrier。它不能作为 key、缓存地址、跨进程句柄或长期状态保存。

## 公共契约

本节只列 SGLang HiCache hostless 接入 Fluxon KV 时直接依赖的接口。

| 接口 | 层级 | 契约 |
| --- | --- | --- |
| `wait_local_segments_ready()` | Fluxon + SGLang 集成 | 返回当前进程可见的 local segment mapping，供 SGLang 做 CUDA host registration。 |
| `local_fast_put_start(keys, value_len, opts)` | SGLang hostless 写入 | 为一批等长 values 准备可写地址，返回 put `plan_ptr`。 |
| `local_fast_put_commit(plan_ptr)` | SGLang hostless 写入 | 在 SGLang native kernel 写完 `value_ptrs[]` 后消费 put plan，把对应 slots 提交为 Fluxon KV route，返回 `KvFuture`。 |
| `put_abort(plan_ptr)` | SGLang hostless 写入 | 在 commit 前释放 put plan、key reservation 和 local reserve slot lease。 |
| `GetStartResult` | SGLang hostless 读取 | 描述连续命中前缀、可传输长度、atomic group 命中数和第一个 miss 位置。 |
| `GetStartHandle` | SGLang hostless 读取 | 持有一次 get-start 结果和 backend handle；必须被 `get_transfer` 消费或被 `cancel_get_transfer` 取消。 |
| `get_start(keys, prefix_best_effort, atomic_group_lens)` | SGLang hostless 读取 | 按 key 顺序计算连续命中的 prefix，并返回 `GetStartHandle`。 |
| `get_transfer(handle)` | SGLang hostless 读取 | 消费 handle 的可传输前缀，执行必要 transfer，并返回 readable `plan_ptr`。 |
| `cancel_get_transfer(handle)` | SGLang hostless 读取 | 放弃未 transfer 的 `GetStartHandle`，释放 get-start 期间持有的 owner/external 资源。 |
| `release_views(plan_ptr)` | SGLang hostless 读取 | 释放 get-transfer 产生的 readable plan，丢弃其持有的 holder 引用。 |

`PutOptionalArgs` 在 SGLang hostless 写入路径中的语义如下：

| 字段 | SGLang 使用方式 |
| --- | --- |
| `reject_if_inflight_same_key` | 固定开启，避免同一 page key 并发写回造成重复 inflight put。 |
| `reject_if_exist_same_key` | 固定开启，SGLang 把重复 key 当作已写回或冲突重试处理。 |
| `write_through` | 当前配置决定提交策略；调用方显式传入时 Fluxon 必须按字段语义执行。 |
| `lease_id` | 当前 SGLang hostless 主线不依赖 lease。 |

## Key 与组件命名

SGLang 传给 Fluxon 的 key 必须先经过 backend namespace 处理：

```text
storage_key = key_prefix + ":" + logical_key
logical_key = page_hash + optional_component_suffix + config_suffix + optional_extra_backend_tag
```

规则：

- page hash 是 SGLang prefix 复用和 Fluxon KV 存取的共同语义 ID。
- `PoolName.KV` 使用默认 component；Mamba 等额外 component 通过 suffix 区分。
- `config_suffix` 编入模型名、TP/PP 等会影响 page layout 的维度，避免不同运行配置复用同一批 physical values。
- `extra_backend_tag` 用于同一集群内隔离实验或实例，不改变 Fluxon KV 的值格式。

Fluxon 只看最终 `storage_key`。page 内部如何拆成 K/V layer、MLA tensor 或 Mamba state，由 SGLang kernel 参数解释。

## Segment Registration

hostless 读写依赖 SGLang 进程可访问的 Fluxon owner segment 已经完成 CUDA host registration。

常见部署中，同一台机器上的多个 SGLang worker 连接同一个 Fluxon owner，并映射同一个 owner shared segment。每个 SGLang 进程仍需要在自己的 CUDA context 中完成 host registration；底层内存归属、holder 引用和回收由 owner 统一管理。

```mermaid
sequenceDiagram
    participant S as SGLang HiCache
    participant B as HiCacheFluxon
    participant P as Fluxon Python store
    participant R as fluxon_pyo3
    participant O as owner segment mapping
    participant C as CUDA runtime

    S->>B: register_mem_pool_host / register_mem_host_pool_v2
    B->>P: wait_local_segments_ready()
    P->>R: wait_local_segments_ready()
    R->>O: wait mapped range
    O-->>R: segment_label, write_ptr, read_ptr, len, generation
    R-->>P: segment list
    P-->>B: dict list
    B->>C: cudaHostRegister(write_ptr/read_ptr, len)
```

`wait_local_segments_ready()` 返回的 item 至少包含：

| 字段 | 含义 |
| --- | --- |
| `segment_label` | owner 本地一般为 `cpu:0`；external attach owner 时为 `external_owner:0`。 |
| `write_ptr` | 当前进程可写映射地址。 |
| `read_ptr` | 当前进程可读映射地址，存在时也可注册。 |
| `len` | 映射长度。 |
| `generation` | owner 启动代际，用于拒绝过期 holder 或 mapping。 |
| `node_id` | segment 所属 Fluxon node。 |

SGLang external-client 模式要求看到 `external_owner:*` segment。注册失败时必须同步报错，不能退回到未注册 host memory 的 direct H2D path。

## Plan Blob ABI

`plan_ptr` 是 Fluxon 返回给 SGLang 的临时句柄，本质上是一段 plan blob 的首地址。SGLang native kernel 通过 `plan_ptr` 找到 blob，再从 blob 里读取本次 batch 对应的 value 地址表。

plan blob 是 Fluxon 在 `local_fast_put_start(...)` 或 `get_transfer(...)` 时创建的一段连续 host memory，格式固定：

```c
uint64_t magic;             // 固定校验值，确认 plan_ptr 指向 Fluxon plan blob
uint64_t count;             // value_ptrs 的数量，也就是本次 batch 的 page 数
uint64_t value_ptrs[count]; // 每个 page 对应的 Fluxon value 起始地址
```

如果 SGLang 一次写入或恢复 10 个 page，Fluxon 会创建一个 blob，并返回一个 `plan_ptr`：

```text
plan_ptr -> blob 起始地址

blob[0]  = magic
blob[1]  = 10
blob[2]  = value_ptr_0
blob[3]  = value_ptr_1
...
blob[11] = value_ptr_9
```

`magic` 不是 KV value 地址，只是固定校验值；`value_ptr_0 ... value_ptr_9` 才是 Fluxon 为这些 page 准备的 value 起始地址。它们是当前进程可访问的绝对地址，不是偏移量。

`value_len` 不写入 blob。Fluxon 只负责按 `value_len` 分配每个 value 的连续字节区间，并把起始地址放进 `value_ptrs[]`；每个 value 内部如何切成 K/V、layer、MLA 或 Mamba state，由 SGLang 调用 write/restore kernel 时显式传入 layout 参数。

`plan_ptr` 只在当前进程、当前 batch 生命周期内有效。`local_fast_put_commit(plan_ptr)`、`put_abort(plan_ptr)` 或 `release_views(plan_ptr)` 后，Fluxon 会清理对应 plan，SGLang 不能继续使用这个 `plan_ptr`。

## Backup 时序

hostless backup 的核心约束是：Fluxon 负责 GPU KV cache 之下的本机/远端 KV 层的地址分配、route 提交和生命周期管理，但真正的 KV bytes 由 SGLang native kernel 从 GPU KV cache 写入。因此 Fluxon 不能在收到 key 后立刻发布 KV route；它必须先完成 key reservation、put id 分配和 owner-local reserve slot claim，把稳定可写的 `value_ptrs[]` 通过 `plan_ptr` 返回给 SGLang。SGLang native kernel 写完这些地址后，`local_fast_put_commit` 才能把这些 slots 提交为 resident values，并发布 Fluxon KV route。

这条写路径拆成两个 Fluxon API 阶段：

1. `local_fast_put_start(keys, value_len)`：只做写入准入、key reservation、put id 分配和可写 value 地址准备，返回 `plan_ptr(value_ptrs)`；此时 value bytes 还没有写完，不能作为可读 KV route 暴露。
2. `local_fast_put_commit(plan_ptr)`：在 SGLang native kernel 完成写入后消费 put plan，提交 slot / transfer / route，并返回 `KvFuture`；只有 future 成功后，SGLang 才能把 node 标记为 `storage_backed`。

写入阶段的拆分主要是为了同时满足两个约束：

- 数据面效率：SGLang 不需要先把 GPU KV page 包装成通用 KV payload 再交给后端，而是直接用 kernel 写入 Fluxon 返回的 value 地址。
- 可见性安全：`put_start` 阶段只预留地址，不发布 route；避免其它 get 读到尚未写完或尚未 commit 的 value。

```mermaid
sequenceDiagram
    participant U as UnifiedRadixCache
    participant B as HiCacheFluxon
    participant F as Fluxon store
    participant K as sgl-kernel
    participant O as Fluxon owner
    participant M as Fluxon master

    U->>B: local_fast_put_start(missing_keys, value_len)
    B->>F: local_fast_put_start(storage_keys, value_len, opts)
    F->>O: claim local reserve slots / external owner offsets
    O-->>F: plan_ptr(value_ptrs)
    B-->>U: plan_ptr
    U->>K: write_*_to_fluxon_values(plan_ptr, page_indices, layout ptrs)
    K-->>U: writes queued on CUDA stream
    U->>U: record local_ready_event
    U->>B: local_fast_put_commit(plan_ptr) after event ready
    B->>F: local_fast_put_commit(plan_ptr)
    F->>O: record precommit visible / transfer_end or put_done
    O->>M: commit route
    F-->>B: KvFuture
    U->>U: scheduler poll future
```

hostless backup 默认不依赖 put 前 exists 扫描。重复 key 或在途 key 由 `local_fast_put_start` 的 `reject_if_exist_same_key` 和 `reject_if_inflight_same_key` 准入语义处理；SGLang 上层按冲突错误做重试或跳过。

`local_fast_put_start(keys, value_len)` 的要求：

- `keys` 不能为空。
- `value_len` 必须大于 0，且同一批 keys 共享同一个 value size。
- SGLang 必须在 `local_fast_put_commit` 前完成 native write；写入失败时必须调用 `put_abort`。
- `local_fast_put_commit` 只能调用一次；调用后 plan 从 registry 清理，后续只能等待返回的 `KvFuture`。

commit 请求会带上 `len`、`src_offset` 和 target 信息。Fluxon 用这些字段判断本次 value 是否落在当前进程可访问的 owner segment 或 owner-local reserve slot 中；如果需要本地可见索引，`put_done` 会返回 owner 分配的 `local_cache_holder_id`。`MemoryInfo` 和 holder 生命周期在下文说明。

## Restore 时序

hostless restore 的核心约束是：SGLang 只能恢复有序 page keys 的连续前缀，并且不能切开一个 radix node 对应的 atomic group。Fluxon 需要先在本机/远端 KV 层里判断这批 keys 的可恢复边界，再把真正可恢复的部分转换成 SGLang kernel 可读取的 `plan_ptr(value_ptrs)`。因此 restore 被拆成 `get_start` 和 `get_transfer` 两个阶段。

`get_start(keys, prefix_best_effort, atomic_group_lens)` 只做恢复规划：按 key 顺序做 local visible check / owner get start，计算 page 级连续命中前缀 `raw_prefix_hit_len`，再按 `atomic_group_lens` 向下收敛成 `transferable_len`。这个阶段回答“本次最多能恢复多少”，但不要求 SGLang 立刻分配 GPU KV pages，也不暴露 readable plan。

`get_transfer(handle)` 在 SGLang 决定恢复后执行：它消费 `get_start` 返回的 handle，等待或完成必要 transfer，只 materialize `keys[..transferable_len]`，并返回持有 holder 引用的 readable `plan_ptr`。随后 SGLang native kernel 使用 `restore_*_from_fluxon_values(...)`，把 Fluxon value memory 拷回 GPU KV cache。

读取阶段拆成 planning 和 materialization 两步，主要是为了保证：

- prefix 安全：中间 page miss 时，只恢复连续命中的完整前缀，不构造带洞的 GPU KV 状态。
- atomic group 安全：`transferable_len` 不会切开 radix node group，避免恢复半个 node。
- 资源效率：SGLang 在知道可恢复边界后再分配 GPU KV pages，避免先分配再发现 miss。
- 生命周期安全：`get_transfer` 返回的 plan 持有 holder 引用，直到 `release_views(plan_ptr)` 后才释放，保证 kernel restore 期间 value 地址稳定。

```mermaid
sequenceDiagram
    participant U as UnifiedRadixCache
    participant B as HiCacheFluxon
    participant F as Fluxon store
    participant E as external client
    participant O as Fluxon owner / external
    participant K as sgl-kernel

    U->>B: get_start(page_keys, atomic_group_lens)
    B->>F: get_start(storage_keys, prefix_best_effort, atomic_group_lens)
    F->>E: batch_get_start(keys)
    E->>O: ExternalBatchGetStartReq
    O-->>E: handle + raw_prefix_hit_len
    E-->>F: backend handle + raw_prefix_hit_len
    F->>F: build GetStartResult(raw_prefix_hit_len, transferable_len, ...)
    F-->>B: GetStartHandle + GetStartResult
    B-->>U: transferable_len / prefix_hit_groups / first_miss_index
    alt transferable_len > 0 and caller chooses restore
        U->>B: get_transfer(handle)
        B->>F: get_transfer(handle)
        F->>E: batch_get_transfer(handle, transferable keys)
        E->>O: ExternalBatchGetTransferReq
        O-->>E: holders / transfer results
        E-->>F: plan_ptr(value_ptrs, holders kept alive)
        F-->>B: plan_ptr
        B-->>U: plan_ptr
        U->>K: restore_*_from_fluxon_values(plan_ptr, prefix page indices, layout ptrs)
        K-->>U: H2D queued on CUDA stream
        U->>B: release_views(plan_ptr) after restore finalizer
    else caller gives up restore
        U->>B: cancel_get_transfer(handle)
        B->>F: cancel_get_transfer(handle)
        F->>E: cancel_batch_get_start(handle)
    end
```

`GetStartResult` 的关键字段如下：

| 字段 | 含义 |
| --- | --- |
| `raw_prefix_hit_len` | 按 key 顺序连续命中的 page 数，未按 atomic group 收敛。 |
| `transferable_len` | 可以交给 `get_transfer` 的 page 数；它不会切开 atomic group。 |
| `prefix_hit_groups` | 完整命中的 atomic group 数。 |
| `first_miss_index` | 第一个 miss page 的 index；全部命中时为 `None`。 |
| `first_miss_group_index` | 第一个 miss 所在 atomic group；全部命中时为 `None`。 |
| `all_hit` | `transferable_len == len(keys)`。 |

生命周期规则：

- `get_start` 成功后，调用方必须二选一：`get_transfer(handle)` 或 `cancel_get_transfer(handle)`。
- `get_transfer(handle)` 成功后，handle 已被消费；后续由 returned `plan_ptr` 和 `release_views(plan_ptr)` 管理。
- `release_views(plan_ptr)` 必须在 native restore 完成后执行，即使 native restore 失败也要释放。
- `get_start` 只命中部分前缀时，SGLang 只能恢复 `transferable_len` 覆盖的完整 atomic groups，不能构造半个 atomic group 的 GPU KV 状态。
- `get_transfer` 返回 miss / KeyNotFound 时，SGLang 必须放弃本次 restore 并执行 rollback。

## Fluxon 本地可见索引与生命周期

Fluxon client/external 侧会维护当前进程可直接访问的 value 索引，以及 get/put plan 持有的 holder 引用。SGLang hostless 路径主要涉及下面几类状态：

| 状态 | 创建入口 | 生命周期 |
| --- | --- | --- |
| precommit local visible | `local_fast_put_commit` 开始后，由 owner-local reserve slot 对应的 `MemoryInfo` 记录到 `precommit_local_visible_info` | 表示本次 put 的 value 已在当前进程可读，但后台 put commit 尚未最终完成；成功后转为 `get_cached_info` 中的 committed entry，失败或取消时移除。 |
| committed local visible info | put commit 成功后，由 SGLang 进程内的 Fluxon external/client 将 `MemoryInfo` 记录到 `get_cached_info` | 保存 key、put version、`holder_id`、`offset`、`len` 和 owner node；后续 `get_start/get_transfer` 可以复用这份 `MemoryInfo` 构造 readable plan。 |
| get-transfer holder | `get_transfer` 成功后绑定到 readable plan，并由 plan 持有引用 | `release_views(plan_ptr)` 后释放引用；plan 生命周期内 holder 保证对应 value 不被释放。 |

收到 `local_cache_holder_id` 后，SGLang 进程内的 Fluxon external/client 使用 `holder_id`、`offset` 和 `len` 构造 `MemoryInfo`，并记录到自身 `get_cached_info`。`MemoryInfo` 记录当前进程访问该 value 所需的地址信息和释放动作；后续 `get_start` 命中 `get_cached_info` 时，可以直接把这份 `MemoryInfo` 纳入本次 get 结果，`get_transfer` 再把这些 value 地址写入 readable plan。底层内存的回收由 owner route、holder 引用和 owner-local reserve grant 生命周期共同约束。

`precommit_local_visible_info` 只覆盖 commit 进行中的短窗口。put commit 成功并确认 holder 后，会移除 precommit entry 并记录 committed entry；如果 commit 失败，precommit entry 必须被清理，不能作为后续 storage-backed 恢复来源。

## Owner 本地写入预留池

Owner 本地写入预留池是 owner segment 中为 SGLang hostless put 预先划分的 writable slots。`local_fast_put_start` 从这些 slots 中为本次 put 分配地址，并把地址写入 `plan_ptr(value_ptrs)` 返回给 SGLang native kernel。此时 slot 只处于 reserved/prepared 状态，还没有绑定为 Fluxon KV 的正式 `key -> value` route。

这个 pool 是共享 owner segment 上的弹性本地可写内存供给层。它让多个 SGLang worker 都能快速取得受 owner 生命周期管理的 `value_ptrs[]`，同时避免为每个 worker 固定切出长期独占的后端 segment。reserve slot 不足时可以按需求补充 grant，空闲后再按 cooldown 回收。

SGLang native kernel 写完 `value_ptrs[]` 后，`local_fast_put_commit` 才把这些 slots 提交为 resident values，并完成 Fluxon KV route commit。commit 前如果 native write 失败，`put_abort(plan_ptr)` 会释放这些 reserved slots。

这条路径把写入拆成两个阶段：

1. `local_fast_put_start` 完成 key reservation、put id 分配和本地 slot claim，返回 `plan_ptr(value_ptrs)`。
2. SGLang native kernel 写完 `value_ptrs[]` 后，`local_fast_put_commit` 再把这些 slot 转为 resident values，并完成 Fluxon KV 的 put 提交。

这里的 `local_fast_*` 是 Python/PyO3 public hostless plan API。真正的 value bytes 由 SGLang native kernel 写入 `value_ptrs[]` 指向的地址；Fluxon 在 commit 阶段只消费 put plan、处理必要 transfer 或 direct done，并完成 route commit。

对象含义：

| 对象 | 含义 |
| --- | --- |
| grant | owner 侧一次申请的大块本地内存，当前固定为 `512 MiB`。 |
| slot | grant 内按 `slot_size` 切分的小块；一个 slot 承载一个 Fluxon value。 |
| slot lease | `local_fast_put_start` 为本次 batch 临时 claim 到的一组 slots；失败或 abort 时必须释放。 |
| value pointer | slot 的起始地址，会写入 plan blob 的 `value_ptrs[]`，供 SGLang kernel 直接写入。 |
| resident value | `local_fast_put_commit` 后由 slot 构造出的本地可读 value。 |
| route | master/owner 确认后的 key 到 value 位置映射；route 成功后该 value 才是全局可见的 KV replica。 |

slot 生命周期：

```text
Free
  -> Prepared             // local_fast_put_start claim slot
  -> PendingLocalVisible  // local_fast_put_commit 开始，本地 resident value 已记录为 pending visible
  -> Committed            // put_done 成功，route 引用该 slot
  -> Free                 // route 和 holder 引用都释放后回收
```

如果 native write 失败，调用方必须执行 `put_abort(plan_ptr)`，Prepared slots 会回到 Free。`local_fast_put_commit` 成功返回后，slot 是否能释放由 route 引用和 holder 引用共同决定；只要 master/owner route 或 `MemHolder` 仍引用该 slot，底层 grant 就不能释放。

当前容量策略：

| 项 | 当前实现 |
| --- | --- |
| grant 物理粒度 | `OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES = 512 * 1024 * 1024` |
| 最小 slot size | `4 KiB` |
| slot size 计算 | `max(value_len, 4 KiB).next_power_of_two()` |
| slot 上限 | `slot_size <= 512 MiB` |
| refill 触发 | 当前 slot class free slots 不足时登记 pending demand 并唤醒 rebalance actor。 |
| 默认等待 | soft wait `10 ms`，hard timeout `1 s`。 |
| shrink 单位 | 整个 grant；不做 live grant compaction。 |

底层物理释放收束在 grant 级别。单个 committed slot 只是 grant 内逻辑索引，不直接拥有释放整块 mmap/registered memory 的权力。

## SGLang Node Storage 状态

SGLang 侧的 node metadata 不等价于 Fluxon master route 状态。当前四个字段建议按下面语义解释：

| 字段 | true 的含义 | 清理时机 |
| --- | --- | --- |
| `storage_staged` | 该 node 有一批 Fluxon hostless backup 正在 staged 路径中。 | `KvFuture` 完成或失败后清空。 |
| `storage_local_ready` | CUDA write 已完成，SGLang 已调用 `local_fast_put_commit`，但返回的 `KvFuture` 还未 ack；该状态只表示本次 hostless backup 已进入 Fluxon commit 流程，不表示 KV route 已经全局确认。 | async ack 结束后清空。 |
| `storage_pending` | Fluxon `KvFuture` 还没结束。 | future 成功或失败后清空。 |
| `storage_backed` | Fluxon 后台提交成功，KV route 已确认可作为 shared backing。 | 该 node 被删除或失效时清空。 |

因此，SGLang 可以用 `storage_staged/storage_local_ready` 判断本次 hostless backup 已经推进到本地写入或 commit 阶段；但跨节点复用和长期共享必须等 `KvFuture` 成功，并以 `storage_backed` 为准。

TP 场景下，每个 rank 仍有各自的 radix tree 和恢复决策。`get_start` 的结果只描述当前 rank 这批 keys 的可恢复前缀；如果一个 rank miss、另一个 rank hit，上层必须按 SGLang 的 TP restore 约束处理一致性，不能把单 rank 的部分成功当作完整 request 已恢复。

## 失败处理

| 场景 | 必须动作 |
| --- | --- |
| `local_fast_put_start` 后 native write 失败 | 调用 `put_abort(plan_ptr)`，释放 key reservation 和 local reserve slot lease。 |
| `local_fast_put_commit` 返回 future 后后台失败 | SGLang 清理 `storage_staged/storage_pending/storage_local_ready`，必要时删除已 evicted 的 dead leaf。 |
| `get_start` 后放弃 restore | 调用 `cancel_get_transfer(handle)`，释放 get-start 持有的 owner/external 资源。 |
| `get_start` 只命中部分前缀 | 只允许恢复 `transferable_len` 覆盖的完整 atomic groups；后续 page 按 miss 处理。 |
| `get_transfer` 报 key miss | SGLang rollback 当前 restore，不继续构造半个 node 的 GPU 恢复。 |
| `get_transfer` 成功后 native restore 失败 | 先 `release_views(plan_ptr)`，再执行 SGLang rollback；此时 handle 已被消费。 |
| CUDA host registration 失败 | direct path 同步失败，不能降级为未注册 host memory。 |
| `plan_ptr` 类型用错 | `local_fast_put_commit`、`put_abort`、`release_views` 都按 registry entry 类型校验并 fail fast。 |
