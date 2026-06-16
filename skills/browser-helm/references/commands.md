# `browser-helm` 命令参考

## 前置（必须）：插件安装与配对

CLI 能否操作浏览器，取决于 **Chrome 插件是否已连接 daemon（WebSocket）**。

最小闭环步骤：

```bash
# 1) 启动/确保 daemon
browser-helm daemon ensure
browser-helm daemon status
browser-helm daemon restart

# 2) 在 Chrome 打开 Web UI（用 Chrome 能访问到的地址打开）
#    http://127.0.0.1:5181
#    从页面复制 Pairing Code（推荐；含多网卡候选地址）或 WS URL + Pairing Token（Advanced）
#
# 3) 安装扩展（Unpacked）
#    - Web UI 下载插件 zip -> 解压
#    - chrome://extensions 开启开发者模式 -> 加载已解压扩展
#
# 4) 插件弹窗填 Pairing Code -> Connect

# 5) 验证浏览器已连接
browser-helm browser list
```

## 基础命令（新主路径）

```bash
browser-helm daemon status
browser-helm daemon ensure
browser-helm daemon stop
browser-helm daemon restart
browser-helm status
browser-helm browser list
browser-helm tab list [browser-id] [--mine]
browser-helm recorder start [browser-id] [managed-tab-id]
browser-helm recorder stop [browser-id] [managed-tab-id]
```

## 受控 tab 生命周期

```bash
browser-helm tab create [browser-id] [url] [--note <text>]
browser-helm tab adopt-active [browser-id] [--note <text>]
browser-helm tab attach [browser-id] [managed-tab-id]
browser-helm page navigate [browser-id] [managed-tab-id] <url>
```

## 交互与分析

```bash
browser-helm page click [browser-id] [managed-tab-id] <selector> [--wait-(selector|text|js) <value>] [--timeout-ms <n>] [--interval-ms <n>]
browser-helm page eval [browser-id] [managed-tab-id] <expression>
browser-helm page wait [browser-id] [managed-tab-id] --until-(selector|text|js) <value> [--timeout-ms <n>] [--interval-ms <n>]
browser-helm page type [browser-id] [managed-tab-id] <selector> <text>
browser-helm page press [browser-id] [managed-tab-id] <key>
browser-helm page summary [browser-id] [managed-tab-id] [output-path]
browser-helm page snapshot [browser-id] [managed-tab-id] [output-path]
browser-helm page screenshot [browser-id] [managed-tab-id] [output-path]
browser-helm events console [browser-id] [managed-tab-id] [--limit <n>] [--since <ms>]
browser-helm events network [browser-id] [managed-tab-id] [--limit <n>] [--since <ms>]
browser-helm events interaction [browser-id] [managed-tab-id] [--limit <n>] [--since <ms>]
browser-helm picker last [browser-id] [managed-tab-id]
browser-helm picker clear [browser-id] [managed-tab-id]
```

说明：

- `page snapshot` 会生成可复用的 interactive refs：`@i1/@i2/...`（按 interactives 列表顺序）。
- `page click/@iN`、`page type/@iN` 会把 ref 解析为 snapshot 中记录的 selector（落盘于 `.tmp/browser-helm/refs/<managed_tab_id>.json`，按 `--session` 隔离）。

## Context（session-like，新主路径）

长对话/长任务里，为了避免反复提供 `browser-id` / `managed-tab-id`，可以把默认对象写入本地 context：

```bash
browser-helm context use-browser <browser-id>
browser-helm context use-tab <managed-tab-id>
browser-helm context show
browser-helm context clear
```

## 多 AI 对话隔离（推荐）

为了避免“同一浏览器 + 多个 AI 对话”串台，建议为每条对话固定一个 `session`：

```bash
browser-helm --session chat-a browser list
browser-helm --session chat-a tab list --mine
browser-helm --session chat-a tab create <browser-id> https://example.com --note "这条对话的用途说明"
```

说明：

- `tab create` 会自动加前缀：`[session:chat-a] ...`
- `tab list --mine` 需要非 default session（否则会报错）

## 输出约定

- `page summary`
  - 默认只打印
  - `--save` 时默认落到 [`.tmp/browser-helm/summaries/`]
- `page snapshot`
  - 默认只打印
  - `--save` 时默认落到 [`.tmp/browser-helm/snapshots/`]
- `page screenshot`
  - 默认落到 [`.tmp/browser-helm/screenshots/`]
- 若使用 `--session <name>` / `BROWSER_HELM_SESSION=<name>`：上述目录会自动切换到 [`.tmp/browser-helm/sessions/<session>/...`]

## 推荐示例

```bash
browser-helm browser list
browser-helm tab create <browser-id> https://example.com --note "说明这个 tab 的用途"
browser-helm tab attach <browser-id> <managed-tab-id>
browser-helm page snapshot <browser-id> <managed-tab-id>
browser-helm --save page summary <browser-id> <managed-tab-id>
browser-helm page screenshot <browser-id> <managed-tab-id>
```

说明：

- `tab create` 若省略 `--note` 且提供 URL，会自动生成：`打开页面：<url>`

## 命令约定

- 仅支持 namespaced 命令面：`browser list`、`tab create`、`tab attach`、`page navigate`、`picker last` 等。
- 文档与 skill 后续默认都以 namespaced 命令作为主路径。
