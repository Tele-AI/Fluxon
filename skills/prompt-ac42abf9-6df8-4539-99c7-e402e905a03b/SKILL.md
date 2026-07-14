---
name: "prompt-ac42abf9-6df8-4539-99c7-e402e905a03b"
description: "逻辑收束到agent"
metadata:
  short-description: "逻辑收束到agent"
---

# 逻辑收束到agent

规约：Manager/Agent 边界（强制收束）

前端只允许访问 manager：所有 API 必须走 /api/router/:agentId/...（local 也一样）。
manager 只负责：请求转发（router）、agent registry、（未来）登录鉴权、（可选）静态资源托管；严禁在 manager 实现任何业务能力与持久化。
大部分“后端能力 + 数据落盘”（projects/sessions/chat/fs/git/terminal/notifications/uiState/uiWorkspaces/uiDock/uiScroll 等）必须在 agent 内实现与持久化；manager 不得读写 .dever/agent_data.json 或任何业务数据文件。
发现 manager 出现新增 /api/* 实现模块或 store/JSON 持久化代码，一律视为架构违规：要么迁到 agent，要么删除并改为转发。
