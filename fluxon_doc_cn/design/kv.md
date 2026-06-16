# KV 设计

## 目标

本文补充当前 `Fluxon KV` 的内部设计，聚焦以下几个问题：

- `master`、`owner`、`external` 三类角色各自持有什么状态。
- `put / get / delete` 在当前实现里的真实调用时序。
- `PutOptionalArgs` 这类特殊参数在当前版本里的语义边界。
- 热路径如何做并发控制，避免把主状态机长期卡在大锁上。

这里描述的是当前代码实现，不是历史设想，也不是未来规划。

## 角色与状态归属

### master

先看当前核心结构：

```rust
pub struct MasterKvRouterInner {
    pub inflight_puts: moka::future::Cache<(String, u64, u32), InflightPutInfo>,
    // 保存尚未 PutDone 的 put 在途状态。
    pub inflight_put_key_counts: Arc<DashMap<String, u32>>,
    // 按 key 统计在途 put 数，用于 reject_if_inflight_same_key 准入控制。
    pub inflight_gets: moka::future::Cache<u64, InflightGetInfo>,
    // 保存尚未 GetDone 的 get 在途状态。
    pub get_holding: MasterOwnerMemMgr,
    // owner 侧 holder 持有表，键是 (node_id, holder_id)。
    pub kv_routes: DashMap<String, Arc<OneKvNodesRoutes>>,
    // 每个 key 当前最新已提交版本的权威路由表。
    pub prefix_index: ARwLock<PrefixRadixTree>,
    // 从 kv_routes 派生出的前缀索引。
    pub node_kv_cache_controller:
        DashMap<NodeIDString, Arc<moka::sync::SegmentedCache<String, NodeValueReplicaDesc>>>,
    // 每个节点的副本缓存控制器，主要服务非 lease 热 key。
    pub lease_reserved_bytes: DashMap<NodeIDString, Arc<AtomicU64>>,
    // 每个节点为 lease 副本预留并从缓存容量中扣减的字节数。
    pub delete_broadcast: EnsureMemholderMgmtDeleteHandle<DeleteKeyInfo>,
    // delete 广播与缓存清理的异步管线入口。
}

pub struct OneKvNodesRoutes {
    pub put_id: PutIDForAKey,
    // 当前已提交 value 的稳定版本号。
    pub lease_id: Option<u64>,
    // 这个 key-version 绑定的 lease；None 表示非 lease key。
    pub nodes_replicas: RwLock<HashMap<NodeID, KvRouteInfo>>,
    // 这个已提交版本当前所有 live replica。
    pub get_durable_slots_used: AtomicU32,
    // 限制 get 驱动的 durable replica 提升并发数。
}

pub struct InflightPutInfo {
    pub node_id: NodeID,
    // 放置策略最终选中的目标节点。
    pub key: String,
    pub req_node_id: NodeID,
    // 发起这次 put 的原始请求节点。
    pub len: u64,
    pub src_target_allocation: Arc<Mutex<Option<InflightPutAllocation>>>,
    // 从 PutStart 到 PutDone / PutRevoke 期间保留的源/目标 allocation。
}

pub struct InflightGetInfo {
    pub put_id: PutIDForAKey,
    // 本次读取对应的版本号，用于拒绝过期完成。
    pub src_node_id: NodeID,
    // master 为这次 get 选择的源 replica 节点。
    pub key: String,
    pub req_node_id: NodeID,
    // 接收数据或复用本地 replica 的请求节点。
    pub len: u64,
    pub allocation: Arc<Allocation>,
    // 请求方侧的目标 allocation。
    pub route: Arc<OneKvNodesRoutes>,
    pub allocation_mode: GetAllocationMode,
    // 这次 get 的分配模式：ReuseReplica / DurableReplica / Temporary。
}
```

这些结构放在一起看，`master` 上的核心状态可以直接分成两类：

- 稳定状态：`kv_routes[key] = OneKvNodesRoutes`
- 在途状态：`inflight_puts` / `inflight_gets`

其中稳定状态 `OneKvNodesRoutes` 表示“这个 key 当前已提交版本到底是什么”：

- `put_id`：本版本 key 的唯一版本号，形状是 `(put_time_ms, put_version)`。
- `lease_id`：这个版本是否绑定 lease。`None` 表示非 lease key，`Some(id)` 表示受 lease 管理。
- `nodes_replicas`：该版本当前有哪些副本，每个副本对应哪个 node、哪块 allocation、当前 tomb 状态如何。

这意味着：

- 同一个 key 的“当前值”只有一条主版本视图。
- 新的 `put_done` 会整体替换旧版本路由，而不是在原版本上原地修补。
- 旧版本的删除广播与本地缓存失效在替换后异步完成。

在途状态则故意不直接写进稳定路由：

- `put` 走 `put_start -> 传输 -> put_done`
- `get` 走 `get_start -> 传输 -> get_done`
- 对应状态分别放在 `inflight_puts` 和 `inflight_gets`

只有 `put_done` 成功后，key 才进入或替换 `kv_routes`；只有 `get_done` 成功后，调用方才拿到稳定 `holder_id` 并暴露 `MemHolder`。

`master` 不直接持有业务 payload bytes；它持有的是路由、版本、lease、holder、缓存控制这类控制面状态。

### owner

先看 owner 侧读取完成后的持有结构：

```rust
pub struct OwnerHoldingGetInfo {
    pub key: String,
    // GetDone 之后当前持有的逻辑 key。
    pub holding_node_id: NodeID,
    // 当前持有这个 holder 的请求节点。
    pub len: u64,
    pub allocation: Arc<Allocation>,
    // 返回给调用方的 holder 背后真实 owner allocation。
}

pub struct MemoryInfo {
    pub offset: u64,
    // 本地共享内存 segment 内的偏移。
    pub addr: u64,
    // 由 segment base + offset 计算出的绝对地址。
    pub len: u32,
    pub holder_id: u64,
    // master 在 GetDone 返回的稳定 holder 标识。
    pub key: String,
    pub master_node_id: NodeID,
    // 后续生命周期 ack 要回报给哪个 master。
    pub view: ClientKvApiView,
    // holder 生命周期回调所需的本地 client view。
}

pub struct UserMemHolder {
    pub memory_info: Arc<MemoryInfo>,
    // 内存元数据以及数据访问入口。
    pub refcount: Arc<AllMemholderRefCount>,
    expose_kind: UserMemHolderExposeKind,
    // 暴露方式：SegPtr 表示零拷贝，OwnedCopy 表示拷贝后暴露。
}
```

所以 owner 的本质不是“知道 key 路由”，而是“贡献 segment，承接 allocation，持有实际数据和 holder 生命周期”。

### external

先看 external / client 入口保存的状态：

```rust
pub struct ClientKvApiInner {
    pub get_remote_kv_lock: AMapLock<String>,
    // 按 key 的 miss 锁，用来合并并发 cache miss。
    get_cached_info: DashMap<String, GetCachedInfo>,
    // 当前 client 上的本地元数据 / 本地 replica 缓存。
    pub external_invalidate_delete: EnsureMemholderMgmtDeleteHandle<DeleteClientKvMetaCacheItem>,
    // owner 发给 external 弱缓存失效的 delete 流。
    pub delete_ack_batch: EnsureMemholderMgmtDeleteHandle<OwnerDeleteAckItem>,
    // 回传给 master 的 delete ack 批处理入口。
    pub owner_delete_ack_mgr: OwnerDeleteAckMemMgr,
    // owner 侧共享的 delete ack 管理器。
    pub external_get_holding: OwnerExternalMemMgr,
    // 仍暴露给用户代码的 external holder 表。
    pub all_memholder_refcount: OnceLock<Weak<AllMemholderRefCount>>,
    // holder 仍存活时阻止 client 被提前销毁的生命周期保护。
    default_lease_id: parking_lot::RwLock<Option<u64>>,
    // 仅做便利记录，绝不会自动应用到 put。
    external_pending_puts: moka::sync::SegmentedCache<(String, u64, u32), ExternalPendingPutCtx>,
    // 远端 put 在 commit / revoke 完成前保留的上下文。
}

pub struct ExternalHoldingGetInfo {
    pub key: String,
    pub req_node_id: String,
    pub memory_info: Arc<MemoryInfo>,
    // external 侧的持有态，底层仍然指向 owner 内存。
}

pub struct ExternalMemHolder {
    pub offset: u64,
    // 附着到 owner 共享内存后的偏移。
    pub addr: u64,
    // 当前 external 进程可见的映射绝对地址。
    pub len: u32,
    pub holder_id: u64,
    // drop 时发送 release ack 所用的 holder 标识。
    pub key: String,
    pub external_client_id: String,
    pub owner_start_time: i64,
    // owner 代际，用来拒绝过期 holder 的释放请求。
}
```

因此，当前 KV 更准确的分层是：

- `master` 持控制面状态。
- `owner` 持数据面 allocation 和 owner 侧 holder 状态。
- `external` 持业务接入态、本地缓存、远程请求上下文和 external holder 状态。

## 调用时序

### put

`put` 的核心链路是：`PutStart -> 数据写入/传输 -> PutDone`。

```text
请求方(external/owner)      master                  源 owner                 目标 owner
        |                    |                        |                         |
        |--- PutStartReq --->|                        |                         |
        |                    | 选择源/目标 allocation |                         |
        |                    | 记录 inflight_puts     |                         |
        |<-- PutStartResp ---|                        |                         |
        |                    |                        |                         |
        |--- 写入 src allocation ------------------->|                         |
        |--- transfer_data_no_copy ------------------------------------------->|
        |                    |                        |                         |
        |--- PutDoneReq ---->|                        |                         |
        |                    | attach lease(可选)     |                         |
        |                    | 更新 kv_routes         |                         |
        |                    | 异步失效旧版本/旧缓存   |                         |
        |<-- PutDoneResp ----|                        |                         |
```

关键点：

- 当前默认放置策略是 `RandomPlacementPolicy`，不是固定本地优先。
- 如果请求方本身就是目标 owner，本图里的“请求方 / 源 owner / 目标 owner”可能部分重合，此时会退化为本地快路。
- 如果传输失败，请求方会发 `PutRevokeReq`，master 只回收在途状态，不写入稳定路由。

### get

`get` 的核心链路是：`GetStart -> 数据传输/复用 -> GetDone`。

```text
请求方(external/owner)      master                  源 owner              请求方 owner
        |                    |                        |                      |
        | 本地 cache check    |                        |                      |
        | miss 后拿 per-key 锁 |                        |                      |
        |--- GetStartReq --->|                        |                      |
        |                    | 读取 kv_routes         |                      |
        |                    | 选择源 replica         |                      |
        |                    | 为请求方分配 target    |                      |
        |                    | 记录 inflight_gets     |                      |
        |<-- GetStartResp ---|                        |                      |
        |                    |                        |                      |
        |--- transfer_data_no_copy ------------------>|--------------------->|
        |                    |                        |                      |
        |--- GetDoneReq ---->|                        |                      |
        |                    | 创建 holder_id         |                      |
        |                    | 按 allocation_mode     |                      |
        |                    | 决定是否提升为 replica  |                      |
        |<-- GetDoneResp ----|                        |                      |
        | 暴露 MemHolder      |                        |                      |
```

当前 `get` 有三种分配模式：

- `ReuseReplica`：请求节点本来就有该 key 的副本，直接复用本地 allocation，不发生真实传输。
- `DurableReplica`：在请求节点新分配一块目标内存，并在 `get_done` 后把它提升为稳定副本。
- `Temporary`：只为本次读取分配临时目标，完成后作为 holder 使用，但不进入稳定副本集合。

实现里对 `DurableReplica` 做了上限控制：同一 key 最多同时保留 2 个 durable get 槽位，避免一次热点扩散把副本数无限放大。

### delete

`delete` 的权威动作发生在 master，失效传播是异步后续动作。

```text
请求方(external/owner)      master                    其他 client / owner cache
        |                    |                                   |
        |--- DeleteReq ----->|                                   |
        |                    | 删除 kv_routes                    |
        |                    | 删除 prefix_index                 |
        |<-- DeleteResp -----|                                   |
        | 本地缓存按版本清理   |                                   |
        |                    |--- delete_broadcast ------------->|
        |                    |--- remove node cache ----------->|
```

关键点：

- `delete` 的权威动作是先删 `kv_routes`。
- 客户端缓存失效和节点侧副本缓存清理由后台任务继续完成。
- 如果 key 不存在，返回 `KeyNotFound`，不会 silent success。

## 特殊参数功能设计

### 对外公开参数

当前公开到 Python `PutOptionalArgs` 的稳定字段主要有：

- `lease_id`
- `reject_if_inflight_same_key`

Rust 内部还支持：

- `preferred_sub_cluster`

但它还没有完整暴露成 Python 稳定公开契约，应视为实现内已有能力，不应在用户示例里假定它始终可用。

### `lease_id`

语义：

- `put_done` 时显式把当前 key 版本绑定到某个 lease。
- 只有调用方明确传 `lease_id`，该次 put 才是 lease put。
- `lease_id=None` 必须保持为纯非 lease put，当前实现明确禁止默认回退到“最近一次 lease”。

绑定后的设计效果：

- `OneKvNodesRoutes.lease_id` 成为这个 key 版本的稳定属性。
- lease key 不进入普通 moka 副本缓存。
- `get` 热路径只需要读 `route.lease_id`，不需要再向 lease manager 额外探测。
- lease 过期后，由 lease manager 触发清理，而不是交给普通缓存淘汰间接删除。

这是当前实现里“lease 语义收敛到版本路由对象上”的关键设计。

### `reject_if_inflight_same_key`

语义：

- 在 `put_start` 时，如果同一 key 已有在途写入，master 直接返回 `KeyBeingWritten`。
- 不开启时，允许同 key 并发 put，最终以后提交成功的版本替换前一个稳定版本。

当前实现不是给 key 加全局写锁，而是维护 `inflight_put_key_counts` 计数：

- 这是轻量的准入控制。
- 它只限制“是否允许新的同 key put 进入”，不阻塞其他 key，也不让大传输过程占住中心锁。

### `preferred_sub_cluster`

语义：

- 仅影响 `put_start` 的目标放置。
- master 会优先在指定 `sub_cluster` 的 kvclient 里找目标分配。
- 找不到合适节点或 allocator 时，会记录告警，然后退回默认放置搜索。

注意：

- 这是“优先偏好”，不是强约束亲和。
- 当前默认策略仍然是随机放置，只是先筛一轮 preferred 集合。

### `source_node_id`

这是内部参数，不是普通用户接口。

语义：

- 仅供 side-transfer worker 覆盖 put 的源节点。
- 要求 requester 与 source 属于同一 owner 代际、同一 `local_ipc_root`，并且 requester 本身是 side-transfer worker。

它的作用是让共享同一 mmap 的辅助工作线程代表 owner 发起 put，而不破坏 owner/external 的基本角色约束。

## 并发控制与热路径

### 不把主状态机卡在大锁上

当前实现的核心原则是：

- 大对象传输不持有 master 主路由写锁。
- 稳定状态更新尽量缩到 `put_done/get_done/delete` 的短临界区。
- 慢操作放到异步 follow-up task。

例如：

- `put` 的 bytes 填充和跨节点传输都发生在 client/transfer engine，不发生在 master 锁内。
- `delete` 先删路由，再异步广播失效。
- `put_done` 提交后，前缀索引更新和 moka 插入都在后台 task 完成。

这就是文档占位里“hold the main state machine when using”的真实含义：当前实现显式避免在主状态机路径上长时间持锁或等待大传输。

### 读热路径：先无锁缓存命中，再按 key 合并 miss

client 侧 `get` 的热路径是：

1. 先查本地 `get_cached_info`。
2. 命中本地副本则直接返回，不经过异步锁。
3. miss 后再获取按 key 的 `AMapLock`。
4. 拿到 miss lock 后二次检查缓存，避免并发 miss 重复回源。
5. 只有真正需要远程 `get_start` 的那个请求才进入 master。

这意味着：

- cache hit 不会被统一大锁拖慢。
- 同 key 并发 miss 会折叠成一次远程查询。
- 锁粒度是 per-key，不是全局。

### master 路由访问：短读锁 + 复制快照

当前 `kv_routes` 是 `DashMap<String, Arc<OneKvNodesRoutes>>`，而 `nodes_replicas` 是 `RwLock<HashMap<...>>`。

典型做法是：

- 先从 `kv_routes` 取出 `Arc<OneKvNodesRoutes>`。
- 用很短的读锁把 `nodes_replicas` clone 成局部 `HashMap` 快照。
- 后续选源副本、处理 tomb、决定分配模式时都基于快照继续。

这样做的目的不是绝对无锁，而是：

- 把共享读锁持有时间压到很短。
- 避免在副本选择、分配、传输准备过程中一直占着路由锁。
- 允许后续通过 `put_id` 再次校验版本一致性，避免旧快照误提交。

这就是占位里“using rwlock, read lock when hot path holding”的准确落地版本：热路径允许短时读锁，但不会把长流程绑在这个锁上。

### 版本号而不是隐式推断

并发下的正确性主要依赖 `put_id`：

- `put_done` 生成新版本并替换旧版本。
- `get_done` 提升 durable replica 前会核对当前 `kv_routes` 的 `put_id` 是否仍与在途读取一致。
- `delete` 和缓存失效也用 `(key, put_time_ms, put_version)` 控制删的是哪个版本。

因此当前实现更依赖“版本校验 + 快照读取”，而不是依赖模糊的动态探测或鸭子类型回退。

## 生命周期说明

### MemHolder 与 close

`get` 返回的是 `MemHolder`，不是直接 bytes。

当前语义是：

- `MemHolder.access()` 才把 value 展开成 `FlatDict`。
- client 关闭时会等待当前 client 暴露出去的 `MemHolder` 释放完成。
- 所以业务代码在 `close()` 前应释放仍在使用的 holder 引用。

这和前面的两阶段 `get` 设计一致：只有 `get_done` 后 holder 才成为稳定生命周期对象，之后它的释放不再由主路由状态机同步阻塞管理，而由 holder 生命周期管理收尾。

## 设计结论

当前 KV 的实现特点可以概括为：

- 用 `master` 管控制面与版本路由，用 `owner` 持数据面 allocation，用 `external` 提供业务入口。
- 用 `put_start/get_start` 与 `put_done/get_done` 分离慢传输和快提交。
- 用 `put_id` 保证并发下的版本一致性。
- 用 per-key miss lock、短读锁、后台 follow-up task 保护热路径。
- 用 `lease_id` 把租约语义固化到 key-version 路由对象上，而不是在热路径做额外探测。

这套设计的重点不是“所有流程都完全无锁”，而是“把锁和状态机只放在必须做权威决策的位置，把传输、失效、缓存维护从主提交路径拆出去”。

## Segment Lease 与跨库保活

### 问题背景

当前 `owner / external` 的数据传输会经过 `fluxon_kv -> fluxon_commu -> closed_sdk` 这条链路。

`fluxon_kv` 本地原本已经有 segment read guard：

```rust
pub struct ClientCpuMemReadGuard {
    guard: ARwLockReadGuardOwned<Option<ClientMappedMem>>,
}
```

它能保证两件事：

- segment 在 guard 存活期间不会被 `take(None)` 卸载。
- segment 的地址范围、mmap 和注册内存不会在 guard 存活期间提前释放。

但它不能直接跨到 `commu / closed_sdk`：

- 这个 guard 是 `fluxon_kv` 内部 Rust 类型，不是 public contract。
- 它不是 FFI-safe 类型，不能直接跨库传递。
- 如果让 `commu` 直接依赖 `ClientCpuMemReadGuard`，层次会反过来。

所以这里必须落成一套 host-owned 的跨库 lease 协议，而不是把 Rust guard 本体往外传。

### 设计原则

当前实现采用下面这套契约：

- host/open2 持有真实的 segment guard。
- `commu` 和 `closed_sdk` 只持有 `segment_lease_handle: u64`。
- `segment_lease_handle` 是 opaque handle，只能回调 host retain/release。
- 真正的 segment 生命周期仍然完全由 host 控制。

这套设计不是重新发明新的内存管理器，而是把现有 `ClientCpuMemReadGuard` 显式提升成跨库 lease：

- host 侧 registry 保存 `handle -> LocalSegmentGuard`
- closed 侧只回传 handle
- handle 消费完成后由 host 侧删除 registry 项

### 生命周期语义

segment lease 只保证 **segment 生命周期**，不保证 **payload 内容稳定性**。

也就是说，它保证：

- segment 不会在传输尚未完成时被 `unregister / unmap / free`
- `src_addr / target_addr` 仍然落在有效 segment 范围内

但它不保证：

- 同一块共享段内容不会被后续业务写覆盖
- transport 线程异步消费时读到的内容一定还是旧值

所以要把两个问题分清：

- `segment lease` 解决“内存段是否还活着”
- `range / slot pin` 或发送边界 copy 解决“这段内容是否仍然稳定”

当前版本只把第一层 contract 做强。

### 现有实现落点

当前 `commu` 里的 closed transfer engine 维护一个 host-owned 的 lease registry：

- host 生成 `segment_lease_handle`
- handle 对应一个真实的 `LocalSegmentGuard`
- closed runtime 通过 FFI 只传这个 handle

主链路分两类：

1. `EnsureLocalSegmentGuard / P2pReadToLocal / P2pWriteFromLocal`

- 这是原本已有的 P2P transfer fast path
- closed 侧请求一个 guard handle
- host 校验地址并返回 handle
- 后续 read/write 消费这个 handle

2. `TransferDataNoCopy`

- 这是通用 transfer engine 路径
- 现在也允许把已有的 local segment guard 先注册成 handle，再通过 closed runtime 带过去
- 这样 `commu` 在跨库后仍然能托住 segment 生命周期，而不是在发送边界把 guard 丢掉

进一步地，当前主链路已经不再要求上层显式传入 `seg_guard`：

- 如果 `transfer_data_no_copy(...)` 发现调用方没有传 lease
- 且当前 runtime 支持 local segment transfer
- 它会根据传输方向自动对本地地址申请一个 segment lease
  - put 路径 pin `src_addr`
  - get 路径 pin `target_addr`

这样 `put/get` 主路径不会再因为上层漏传 guard，退化成“裸地址跨库提交”。

### 为什么 unregister 会等待 lease 归零

这版没有引入额外的“宿主机全局引用计数器”。

原因是当前 `ClientCpuMemReadGuard` 本身就建立在 `ARwLockReadGuardOwned<Option<ClientMappedMem>>` 之上：

- lease 存活时持有读锁
- `unregister()` 需要拿写锁并 `take()` 掉 `cpu_allocated_mem`

因此只要 lease handle 最终回到 host 并还原成真实 guard：

- 所有 inflight lease 没释放前，写锁拿不到
- `unregister()` 就不会越过这些 inflight 传输提前释放 segment

这实际上已经形成了“跨库 lease + 本地读写互斥”的语义：

- 读侧：inflight transfer 持有 lease
- 写侧：unregister / close 需要独占写锁

### 约束

这套 contract 有几个明确约束：

- `commu` 不感知 `ClientCpuMemReadGuard` 的具体类型。
- `closed_sdk` 不持有 host Rust 对象，只持 opaque handle。
- public API 不暴露“有时传 guard，有时不传”的模糊契约；是否需要 local segment lease 由内部 transfer path 决定。
- 如果后续要做真正的 end-to-end zero-copy，就必须新增 `range / slot lease` 或等价 pin 语义，不能把 `segment lease` 误当作 payload 不变性保证。

### 为什么不是 master allocation pin

这里不能把第二层设计建在 `master` 的 `Allocation` 上。

原因很直接：

- `Allocation` 是 master 控制面的分配对象。
- 本地 owner / external / side-worker 进程并不持有这个对象，也不依赖它做共享段地址复用控制。
- 本地真正会被异步 transport 读取的是当前进程里已经映射好的 segment 地址。

所以第二层修复必须仍然落在 **本地 segment 语义**：

- 第一层：segment lease，保证这段 mmap/segment 不会提前卸载。
- 第二层：如果未来要继续去掉 copy，再设计更细粒度的 range/slot pin，保证这段 payload 内容在 transport 真正消费前不被业务覆盖。
