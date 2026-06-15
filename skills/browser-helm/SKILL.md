---
name: browser-helm
description: Helm-only browser runtime workflow for operating Browser Helm managed tabs via `browser-helm`, with namespaced `browser` / `tab` / `page` / `picker` / `events` commands and namespaced `.tmp/browser-helm/` output conventions.
allowed-tools: Bash(*)
---

# 用 `browser-helm` 操作 Browser Helm 受控标签页

当用户想通过 **Helm-only runtime** 操作浏览器，而不是使用通用 `agent-browser` 时，使用这个 skill。

适用场景：

- 需要列出已连接浏览器 / managed tab
- 需要创建 managed tab 并 attach debugger
- 需要执行 `page navigate` / `page click` / `page eval` / `page wait` / `page type` / `page press` / `page summary` / `page snapshot` / `page screenshot`
- 需要通过 picker 获取/清空最近一次选中元素的 metadata（无需用户粘贴 JSON）
- 需要遵守 `browser-helm` 当前的输出与落盘约定

不适用场景：

- 用户明确要用通用 `agent-browser` / noVNC 工作流
- 用户只是要解释代码，不需要运行 `browser-helm`

## 默认工作流（新主路径）

默认 Base URL：`http://127.0.0.1:5181`（不需要设置环境变量）。

如需覆盖（可选）：在命令前追加 `--base-url http://127.0.0.1:5181`。

如本机未全局安装 `browser-helm`，也可以用 `node browser-helm/dist/cli.js` 替代下方命令。

## 多人/多 AI 会话（互信）约定（重要）

当前产品定位下，daemon / Web UI / WS **默认不做鉴权**，更偏向“同一局域网多人互信”的协作模型。

但为了避免 **同一台浏览器 + 多个 AI 对话** 时出现“串台/误操作”，推荐强制使用 `session` 做操作隔离：

- 每个 AI 对话固定用一个 `--session <name>`（或设置环境变量 `BROWSER_HELM_SESSION=<name>`）
- `session` 会隔离：
  - CLI context 落盘：`.tmp/browser-helm/context.json`（default）或 `.tmp/browser-helm/sessions/<session>/context.json`
  - CLI 输出落盘：`.tmp/browser-helm/<type>/...`（default）或 `.tmp/browser-helm/sessions/<session>/<type>/...`
  - `tab create` 会自动加前缀：`[session:<session>] ...`（用于人类/AI 识别归属）
- `tab list --mine` 只在非 default session 下可用（通过 note 前缀过滤“我这条会话创建的 tab”）

注意：`session` 只是“操作习惯/隔离约定”，**不是安全边界**。知道 `managed-tab-id` 仍然能跨 session 操作；不要把端口暴露到不可信网络。

### 前置（必须）：安装插件并配对

`browser-helm` 的所有浏览器动作都依赖 **Chrome 插件已连接 daemon（WebSocket）**：

- 创建 managed tab 时建议提供 `--note <text>`，用于描述这个 tab 的意图/用途。
  - 若省略 `--note` 且提供 URL，CLI 会自动生成：`打开页面：<url>`

- 若 `browser-helm browser list` 一直为空，优先判断是「插件未安装/未 Connect」而不是 CLI 出错。

一次性配对步骤：

1) 启动 daemon

```bash
browser-helm daemon ensure
```

（可选）如需重启：

```bash
browser-helm daemon restart
```

2) 用 Chrome 打开 Web UI（用“Chrome 能访问到的地址”打开）

- Web UI：`http://127.0.0.1:5181`
- 页面上会显示 `Pairing Code`（推荐）以及 `WS URL`/`Pairing Token`（Advanced）

3) 安装扩展（Unpacked）

- 在 Web UI 点击“下载插件 zip”，解压
- 打开 `chrome://extensions`，开启开发者模式
- 点击“加载已解压的扩展程序”，选择解压后的目录

4) 插件配对（Connect）

- 打开扩展弹窗
- 粘贴 Web UI 中的 `Pairing Code`，点击 `Connect`
- （可选）点一次 `Status` 确认连接 OK
- Advanced：也可手填 `WS URL` + `Pairing Token`

5) CLI 验证插件已连接

```bash
browser-helm browser list
```

### 默认动作流

1. 确保 `Browser Helm daemon` 已启动（AI 可通过 CLI 直接启动/拉起）

```bash
browser-helm daemon ensure
```

注：`daemon ensure` 会启动内置的预编译 daemon（当前提供 `linux-x64`），不要求用户安装 `cargo`。

2. 确认扩展已连接，并列出浏览器

```bash
browser-helm browser list
```

（推荐）3. Pin 默认 browser/tab（减少长对话遗忘成本）

```bash
browser-helm context use-browser <browser-id>
browser-helm context use-tab <managed-tab-id>
browser-helm context show
```

4. 列 tab；如无 tab，则创建新 tab

```bash
browser-helm browser list
browser-helm tab list <browser-id>
browser-helm tab create <browser-id> https://example.com --note "说明这个 tab 的用途"
```

5. （可选）显式 `tab attach` debugger

`tab create` / `page navigate` 已会自动 ensure debugger attach（用于更早捕获 network/console）。如果你准备在浏览器里手动刷新/导航，也建议先 `tab attach`。

```bash
browser-helm tab attach <browser-id> <managed-tab-id>
```

6. 页面分析优先走返回值主路

```bash
browser-helm page summary <browser-id> <managed-tab-id>
browser-helm page snapshot <browser-id> <managed-tab-id>
```

7. 只有在需要留档时才显式保存 `page summary` / `page snapshot`

```bash
browser-helm --save page summary <browser-id> <managed-tab-id>
browser-helm --save page snapshot <browser-id> <managed-tab-id>
```

8. `page screenshot` 默认会落盘；`page click` 会走受控页遮罩下的程序化点击

```bash
browser-helm page click <browser-id> <managed-tab-id> '#selector'
browser-helm page click <browser-id> <managed-tab-id> '#selector' --wait-text 'Finished working' --timeout-ms 15000
browser-helm page eval <browser-id> <managed-tab-id> '1+1'
browser-helm page wait <browser-id> <managed-tab-id> --until-text 'Finished working' --timeout-ms 15000
browser-helm page type <browser-id> <managed-tab-id> 'div[aria-label="Composer"]' 'hello'
browser-helm page press <browser-id> <managed-tab-id> 'Enter'
browser-helm page screenshot <browser-id> <managed-tab-id>
```

9. 推荐先 `page snapshot` 生成 `@iN` refs，再用 ref 操作（类似 agent-browser 的 `@eN`）

```bash
browser-helm page snapshot <browser-id> <managed-tab-id>
browser-helm page click @i1
browser-helm page type @i2 'hello'
```

9. 如用户在 SidePanel 做了元素选择（Start Picking），AI 可直接从 daemon 拉取最近一次选择结果

```bash
browser-helm picker last
browser-helm picker clear
```

### 交互录制（用户手动复现）

当你需要「AI 先打开受控 tab，然后用户自己操作复现问题，再让 AI 回看」时，可以开启交互录制：

```bash
# 记录起始时间（ms）
t0=$(date +%s%3N)

# 开始录制（会注入监听脚本，并临时隐藏遮罩，允许用户点击/输入）
browser-helm recorder start <browser-id> <managed-tab-id>

# ...用户在该 tab 上手动复现...

# 拉取复现阶段的交互/console/network 事件（按 since 过滤）
browser-helm events interaction <browser-id> <managed-tab-id> --since $t0 --limit 2000
browser-helm events console <browser-id> <managed-tab-id> --since $t0 --limit 2000
browser-helm events network <browser-id> <managed-tab-id> --since $t0 --limit 2000

# 停止录制（恢复遮罩）
browser-helm recorder stop <browser-id> <managed-tab-id>
```

注意：交互录制会包含 input 的原始 value（不脱敏）。仅建议在互信/本地环境使用。

## 输出与落盘约定

- `page summary`：默认只打印；传 `output-path` 或 `--save` 时，写入 `.tmp/browser-helm/summaries/`
- `page snapshot`：默认只打印；传 `output-path` 或 `--save` 时，写入 `.tmp/browser-helm/snapshots/`
- `page screenshot`：默认写入 `.tmp/browser-helm/screenshots/`
- 若使用 `--session <name>` / `BROWSER_HELM_SESSION=<name>`：上述目录会自动切换到 `.tmp/browser-helm/sessions/<session>/...`
- 如用户显式提供路径，优先使用用户路径

## 命令参考

详细命令与示例见：[`browser-helm/skills/browser-helm/references/commands.md`]

优先顺序建议：

1. `browser list`
2. `tab list`
3. `tab create`（推荐写 `--note`；若省略且提供 URL，则自动生成 note）
   - 或：`tab adopt-active`（接管当前活动 tab）
4. `tab attach`
5. `page navigate`
6. `page summary` / `page snapshot`
7. `page click` / `page screenshot`


## 目录约定

- 项目内 skill 源目录：[`browser-helm/skills/browser-helm/`]
- 仓库根入口：[`skills/browser-helm/`]


## 命令约定

- 仅支持 namespaced 命令面：`browser list`、`tab create`、`page navigate`、`picker last` 等。
- 默认文档路径改为 namespaced 形式：`browser list`、`tab create`、`page navigate`、`events console`、`picker last`。
