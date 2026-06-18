# KV 设计 1 - 概览与分层

## 目标

本文补充当前 `Fluxon KV` 的内部设计，聚焦以下几个问题：

- `master`、`owner`、`external` 三类角色各自持有什么状态。
- `put / get / delete` 在当前实现里的真实调用时序。
- `PutOptionalArgs` 这类特殊参数在当前版本里的语义边界。
- 热路径如何做并发控制，避免把主状态机长期卡在大锁上。

这里描述的是当前代码实现，不是历史设想，也不是未来规划。

## 角色与状态归属

这一节先回答一个更基础的问题：为什么当前 KV 要拆成 `master` / `owner` / `external` 三类角色，以及为什么不让每个业务进程都同时持有完整状态。

切分原则很直接：把那些不得不中心化、必须由单一 authority 主管的状态收口到 `master`，把节点级数据面资源和执行面状态收口到 `owner`，把业务进程自身的接入态收口到 `external`。当前实现里，像 KV 全局路由、版本提交、lease 绑定，以及 segment 生命周期归属这类状态，如果没有单一主管，就会出现冲突决策和难以收敛的问题；这些都属于 `master` 必须负责的部分。

与此同时，集群里的扩散连接不应按业务进程数膨胀，尤其是在 AI native 的 Python 程序里，多进程往往就是常态。当前模型把集群互联和数据面连接主要收敛在 `owner` 之间，`external` 不参与 owner-owner 网状互联，只附着到本机 `owner`。这样做有几个直接收益：

- 连接规模主要由 owner 数量决定，业务进程数量不会直接放大 owner-owner 拓扑，可扩展性更好。
- `owner` 和 `external` 进程隔离，业务进程抖动、重启、崩溃不会直接把本机数据面治理和对外连接一起带坏，稳定性更高。
- 共享内存、热点对象、segment 和本地缓存都沉到 `owner` 这一层统一承载；`external` 直接复用本机 `owner` 的这套数据面对象，不需要每个业务进程各自维护一份，缓存利用率也更高。

### 通信模型

当前实现里的通信可以分成三层看：

- 控制面成员发现：`master`、`owner`、`external` 都通过 `ClusterManager` 注册到 `etcd`。成员区分主要靠 `member_id + metadata`，其中会带上角色标记（声明当前成员是 `master`、`owner` 还是 `external`）、`local_ipc_root`（声明本机 IPC 根目录，供本地拓扑规划和 attach 路径对齐）、`shared_storage_node_id`（声明当前 `external` 绑定并复用的是哪个 `owner` 数据面，也说明共享内存路径归属在哪一侧）、`rdma_control`（声明该成员当前的 RDMA 控制面配置与开关状态）等字段；`master` 基于这套成员视图维护路由、lease 和 readiness authority。
- 本机 `external -> owner`：`external` 启动时先等待 `owner` 发布的 `shared.json + mmap.file`，完成共享内存 attach，再等待 owner 成员可观测。后续 `get / put / delete / is_exist / delete_ack` 这些入口，代码里都是先取 `shared_storage_node_id()`，也就是先把请求发给绑定 owner。本地 IPC tier 用的是 `iceoryx2`，`local_ipc_root` 也会发布到成员元数据里供拓扑规划；但 value payload 本体主要还是通过 owner 共享内存 bundle 零拷贝读写，不是只靠 IPC 消息搬运。若启用了 side-transfer，`put_start / put_transfer_end` 也可能先打 owner 拉起的 side-transfer worker，但仍属于 owner 这一侧的本地入口。
- `owner <-> master` 与 `owner <-> owner`：统一走 `fluxon_commu` 的 `P2pModule + transfer_engine`。控制 RPC、owner 间转发、跨机数据搬运都在这层完成；底层传输按配置可走 RDMA 或 TCP。这一层不只覆盖同集群直连，也支持跨子集群、跨集群的 relay / forwarding；当源 owner 和目标 owner 之间不能直接按理想链路互通时，数据面和控制面都可以经由中继路径继续完成。

### master

先看当前核心结构：

```rust
pub struct MasterKvRouterInner {
    // 保存尚未 PutDone 的 put 在途状态。
    pub inflight_puts: moka::future::Cache<(String, u64, u32), InflightPutInfo>,
    // 按 key 统计在途 put 数，用于 reject_if_inflight_same_key 准入控制。
    pub inflight_put_key_counts: Arc<DashMap<String, u32>>,
    // 保存尚未 GetDone 的 get 在途状态。
    pub inflight_gets: moka::future::Cache<u64, InflightGetInfo>,
    // owner 侧 holder 持有表，键是 (node_id, holder_id)。
    pub get_holding: MasterOwnerMemMgr,
    // 每个 key 当前最新已提交版本的权威路由表。
    pub kv_routes: DashMap<String, Arc<OneKvNodesRoutes>>,
    // 从 kv_routes 派生出的前缀索引；不保证 put 时立即可见的强一致性，当前主要用于 MQ 的容量背压限制。
    pub prefix_index: ARwLock<PrefixRadixTree>,
    // 每个节点的副本缓存控制器，主要服务非 lease 热 key。
    pub node_kv_cache_controller:
        DashMap<NodeIDString, Arc<moka::sync::SegmentedCache<String, NodeValueReplicaDesc>>>,
    // 每个节点为 lease 副本预留并从缓存容量中扣减的字节数。
    pub lease_reserved_bytes: DashMap<NodeIDString, Arc<AtomicU64>>,
    // delete 广播与缓存清理的异步管线入口。
    pub delete_broadcast: EnsureMemholderMgmtDeleteHandle<DeleteKeyInfo>,
}

pub struct OneKvNodesRoutes {
    // 当前已提交 value 的稳定版本号。
    pub put_id: PutIDForAKey,
    // 这个 key-version 绑定的 lease；None 表示非 lease key。
    pub lease_id: Option<u64>,
    // 这个已提交版本当前所有 live replica。
    pub nodes_replicas: RwLock<HashMap<NodeID, KvRouteInfo>>,
    // 限制 get 驱动的 durable replica 提升并发数。
    pub get_durable_slots_used: AtomicU32,
}

pub struct InflightPutInfo {
    // 放置策略最终选中的目标节点。
    pub node_id: NodeID,
    pub key: String,
    // 发起这次 put 的原始请求节点。
    pub req_node_id: NodeID,
    pub len: u64,
    // 从 PutStart 到 PutDone / PutRevoke 期间保留的源/目标 allocation。
    pub src_target_allocation: Arc<Mutex<Option<InflightPutAllocation>>>,
}

pub struct InflightGetInfo {
    // 本次读取对应的版本号，用于拒绝过期完成。
    pub put_id: PutIDForAKey,
    // master 为这次 get 选择的源 replica 节点。
    pub src_node_id: NodeID,
    pub key: String,
    // 接收数据或复用本地 replica 的请求节点。
    pub req_node_id: NodeID,
    pub len: u64,
    // 请求方侧的目标 allocation。
    pub allocation: Arc<Allocation>,
    pub route: Arc<OneKvNodesRoutes>,
    // 这次 get 的分配模式：ReuseReplica / DurableReplica / Temporary。
    pub allocation_mode: GetAllocationMode,
}
```

这些结构放在一起看，`master` 上的核心状态可以直接分成两类：

- 稳定状态：`kv_routes[key] = OneKvNodesRoutes`
- 在途状态：`inflight_puts` / `inflight_gets`

其中稳定状态 `OneKvNodesRoutes` 表示“这个 key 当前已提交版本到底是什么”：

- `put_id`：当前实现里用于区分同一 key 不同版本的版本标识，形状是 `(put_time_ms, put_version)`，不是数学意义上的全局唯一 ID。
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
    // GetDone 之后当前持有的逻辑 key。
    pub key: String,
    // 当前持有这个 holder 的请求节点。
    pub holding_node_id: NodeID,
    pub len: u64,
    // 返回给调用方的 holder 背后真实 owner allocation。
    pub allocation: Arc<Allocation>,
}

pub struct MemoryInfo {
    // 本地共享内存 segment 内的偏移。
    pub offset: u64,
    // 由 segment base + offset 计算出的绝对地址。
    pub addr: u64,
    pub len: u32,
    // master 在 GetDone 返回的稳定 holder 标识。
    pub holder_id: u64,
    pub key: String,
    // 后续生命周期 ack 要回报给哪个 master。
    pub master_node_id: NodeID,
    // holder 生命周期回调所需的本地 client view。
    pub view: ClientKvApiView,
}

pub struct UserMemHolder {
    // 内存元数据以及数据访问入口。
    pub memory_info: Arc<MemoryInfo>,
    pub refcount: Arc<AllMemholderRefCount>,
    // 暴露方式：SegPtr 表示零拷贝，OwnedCopy 表示拷贝后暴露。
    expose_kind: UserMemHolderExposeKind,
}
```

从这组状态可以看出，owner 在这一层承担的是本机数据面 authority：它维护本地 key 元数据 / replica 缓存，贡献 segment，承接 allocation，并持有实际数据和 holder 生命周期。

### external

先看 external / client 入口保存的状态：

```rust
pub struct ClientKvApiInner {
    // 按 key 的 miss 锁，用来合并并发 cache miss。
    pub get_remote_kv_lock: AMapLock<String>,
    // 当前 client 上的本地元数据 / 本地 replica 缓存。
    get_cached_info: DashMap<String, GetCachedInfo>,
    // owner 发给 external 弱缓存失效的 delete 流。
    pub external_invalidate_delete: EnsureMemholderMgmtDeleteHandle<DeleteClientKvMetaCacheItem>,
    // 回传给 master 的 delete ack 批处理入口。
    pub delete_ack_batch: EnsureMemholderMgmtDeleteHandle<OwnerDeleteAckItem>,
    // owner 侧共享的 delete ack 管理器。
    pub owner_delete_ack_mgr: OwnerDeleteAckMemMgr,
    // 仍暴露给用户代码的 external holder 表。
    pub external_get_holding: OwnerExternalMemMgr,
    // holder 仍存活时阻止 client 被提前销毁的生命周期保护。
    pub all_memholder_refcount: OnceLock<Weak<AllMemholderRefCount>>,
    // 仅做便利记录，绝不会自动应用到 put。
    default_lease_id: parking_lot::RwLock<Option<u64>>,
    // 远端 put 在 commit / revoke 完成前保留的上下文。
    external_pending_puts: moka::sync::SegmentedCache<(String, u64, u32), ExternalPendingPutCtx>,
}

pub struct ExternalHoldingGetInfo {
    pub key: String,
    pub req_node_id: String,
    // external 侧的持有态，底层仍然指向 owner 内存。
    pub memory_info: Arc<MemoryInfo>,
}

pub struct ExternalMemHolder {
    // 附着到 owner 共享内存后的偏移。
    pub offset: u64,
    // 当前 external 进程可见的映射绝对地址。
    pub addr: u64,
    pub len: u32,
    // drop 时发送 release ack 所用的 holder 标识。
    pub holder_id: u64,
    pub key: String,
    pub external_client_id: String,
    // owner 代际，用来拒绝过期 holder 的释放请求。
    pub owner_start_time: i64,
}
```

因此，当前 KV 更准确的分层是：

- `master` 持全局控制面状态，以及那些不得不中心化的 authority，例如版本路由、lease 绑定和稳定生命周期归属。
- `owner` 持节点级数据面 allocation、segment、本地 key 元数据 / replica 缓存和 owner 侧 holder 状态，并承担 owner 间互联。
- `external` 持业务接入态、本地缓存、远程请求上下文和 external holder 状态；它附着到 owner，但不承担集群容量贡献和 owner-owner 互联。

## 设计结论

当前 KV 的实现特点可以概括为：

- 用 `master` 管控制面与版本路由，用 `owner` 持数据面 allocation，用 `external` 提供业务入口。
- 用 `put_start/get_start` 与 `put_done/get_done` 分离慢传输和快提交。
- 用 `put_id` 保证并发下的版本一致性。
- 用 per-key miss lock、短读锁、后台 follow-up task 保护热路径。
- 用 `lease_id` 把租约语义固化到 key-version 路由对象上，而不是在热路径做额外探测。
- 用 owner 间统一的 P2P + transfer engine 承担同集群直连和跨集群中继通信，避免把复杂网络拓扑暴露到业务接入层。

这套设计的重点不是“所有流程都完全无锁”，而是“把锁和状态机只放在必须做权威决策的位置，把传输、失效、缓存维护从主提交路径拆出去”。
