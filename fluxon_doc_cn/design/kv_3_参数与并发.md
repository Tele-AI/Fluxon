# KV 设计 3 - 参数与并发

## 特殊参数功能设计

### 对外公开参数

当前公开到 Python `PutOptionalArgs` 的稳定字段主要有：

- `lease_id`
- `reject_if_inflight_same_key`

Rust 内部还支持：

- `preferred_sub_cluster`

但它还没有完整暴露成 Python 稳定公开契约，应视为实现内已有能力，不应在用户示例里假定它始终可用。
当前它主要是 Fluxon MQ 通过 Rust 接口直接依赖的能力。

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

Fluxon MQ 也通过 `lease_id` 实现 KV/MQ 的协同能力支持，确保消息需要长期有效时不会被普通缓存驱逐。

这是当前实现里“lease 语义收敛到版本路由对象上”的关键设计。

### `reject_if_inflight_same_key`

语义：

- 在 `put_start` 时，如果同一 key 已有在途写入，master 直接返回 `KeyBeingWritten`。
- 不开启时，允许同 key 并发 put，最终以后提交成功的版本替换前一个稳定版本。

当前实现不是给 key 加全局写锁，而是维护 `inflight_put_key_counts` 计数：

- 这是轻量的准入控制。
- 它只限制“是否允许新的同 key put 进入”，不阻塞其他 key，也不让大传输过程占住中心锁。
- 完整的 put 在途上下文仍放在 `inflight_puts` 这个 `moka` 表里；`inflight_put_key_counts` 只是按 key 聚合的辅助索引，用于快速做同 key 准入判断。

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
- `put_done` 提交后，前缀索引更新和 moka 插入都在后台 task 完成；因此 `prefix_index` 不保证 put 时立即可见的强一致性，当前主要用于 MQ 的容量背压限制。

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

当前 `kv_routes` 是 `DashMap<String, Arc<OneKvNodesRoutes>>`，而 `node_replicas` 是 `RwLock<HashMap<NodeID, KvNodeReplicas>>`。每个 node entry 用独立的 `memory` / `ssd` 选项表示两层副本。

典型做法是：

- 先从 `kv_routes` 取出 `Arc<OneKvNodesRoutes>`。
- 用很短的读锁把 `node_replicas` clone 成局部 `HashMap` 快照。
- 后续先选择 `memory` 副本；没有可用内存副本时，再从同一快照选择 `ssd` 副本。
- tomb 清理删除整个 node entry；内存和 SSD 驱逐只更新各自的 `Option`，两个字段都为空时才删除 entry。

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
