---
name: "prompt-2793a3a4-310f-40c8-ba5d-bc7f5c1cafd7"
description: "状态机正确性"
metadata:
  short-description: "状态机正确性"
---

# 状态机正确性

高度抽象规约（公式级）
任何副作用（发请求/写缓存/写 store/patch UI）都必须是一个纯函数式映射：Effect = f(ActorIdentity, IntentId, TargetIdentity, Payload)
禁止让 Effect 依赖任何隐式上下文：currentXxx/globalXxx/数组下标对齐/默认回退对象/闭包里过期的 selectedXxx；因为这些在“列表重排 + 异步回写 + 流式并发”下会漂移，导致“写对了 payload，但写错了容器”。
落地要点（必须同时满足）
ActorIdentity（我是谁/谁在做）：谁发起这次 effect（组件/Hook/Stream/Workspace），用于界定权限与并发域（如 workspaceId、agentKey/machineKey）。
IntentId（我这次想做什么）：一次用户意图/事务的稳定编号（如 click/open 的 seq），用于丢弃过期回写。
TargetIdentity（我对谁做）：被操作对象的最小不可歧义闭包（按场景至少包含：machineKey/agentId + projectId + bucket/filterKey + sessionId/cursor）。
写入前校验：在真正 setQueryData/setState 前，二次校验 Actor/Intent/Target 仍匹配当前上下文；不匹配就丢弃/重取，不能“凑合写”。
具体例子（对照）
1) TanStack Query 列表/分页
正确：queryKey 与返回数据都绑定 machineKey+projectId+bucket+filterKey(+cursor)；渲染/回写前校验这些字段一致。
错误：useQueries 用 i * buckets + j 取结果，projects 列表一重排就把 A 项目的结果读成 B 项目的。
2) SSE/Stream patch
正确：patch 必须携带 agentId/machineKey + projectId + sessionId，并且只允许更新对应 identity 的 queryKey 容器。
错误：只带 sessionId 就去更新“当前项目的 sessions 列表”（TargetIdentity 缺失）。
3) UI 选择态（跨 await）
正确：点击会话生成 intentSeq，await 返回后如果 intentSeq 已变化则不写入（IntentId 防过期回写）；并且写入的目标必须是同一个 agentId+projectId+sessionId。
错误：await 回来直接 setSelectedSession(x)，同时依赖“当前 selectedProject”作为目标（隐式 current 指针）。
