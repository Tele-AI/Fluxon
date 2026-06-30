# Observ 设计 0 - 监控面板 Metric Trends

## 0. 总起

本文只定义 KV 监控面板里的 `Metric Trends` 区域。对应页面模板在 `fluxon_rs/fluxon_cli/templates/monitor_table.html`，页面上显示为：

```html
<summary><b>Metric Trends</b> <span class="muted">(KV aggregate + member drill-down)</span></summary>
```

稳定结论：

- `Metric Trends` 是 KV 面板里的趋势图区域，负责展示聚合指标卡片和 per owner 展开视图。
- 指标卡片支持多线折线图。容量型指标必须把用量和总量放在同一张图里。
- 用户可以多选指标卡片，同时展开多个 owner drilldown block。
- 折线 hover 时必须显示 tooltip、垂直辅助线和折线上的对齐辅助点。
- 周期刷新必须复用已有 DOM，避免页面跳动、展开状态丢失和 hover 中断。

## 1. 区域结构

`Metric Trends` 区域由三层组成：

| 层级 | 页面元素 | 职责 |
| --- | --- | --- |
| 顶部控制 | window selector、role filters | 控制趋势窗口和可见成员角色 |
| 聚合卡片 | `#metric_grid` 下的 `.metric_card` | 展示每个指标的最新值和聚合曲线 |
| 展开视图 | `#member_metric_sections` 下的 owner blocks | 展示选中指标的 per owner 曲线和成员行 |

用户进入 KV 面板时先看到聚合卡片。点击一个指标卡片后，该指标会进入选中集合，并在下方生成一个 owner drilldown block；再次点击同一卡片会关闭该指标的展开视图。

## 2. 指标卡片

每个 `.metric_card` 展示三类信息：

- 指标名，例如 `Node CPU`、`Node Memory`、`GPU Memory`。
- 最新值。多线指标用 `主线 / 对比线 / 附加线` 的顺序展示。
- 一张 sparkline 折线图。

当前卡片按以下语义渲染：

| 指标 | 曲线要求 |
| --- | --- |
| `Node CPU` | `Used`、`Capacity`、`Process CPU` 三条线 |
| `Node Memory` | `Used`、`Total`、`Process RSS` 三条线 |
| `Segment Usage` | `Used`、`Capacity` 两条线 |
| `GPU Memory` | `Used`、`Total` 两条线 |
| `Process Network` | `TX`、`RX` 两条线 |
| `Node Network` | `TX`、`RX` 两条线 |
| `Cache Hit %` | 一条命中率曲线，选择后可看 per owner 命中率 |
| 其他单值指标 | 一条主曲线 |

CPU 指标按核堆叠展示，GPU 百分比指标按设备聚合展示；这类按资源实例求和的百分比聚合值都可以超过 `100%`。折线图的 Y 轴起点固定为 `0`，避免资源曲线因为局部波动被视觉放大。

## 3. 多线折线图

折线图由 `buildSparklineSvg(...)` 生成。输入统一归并为 `data-lines`：

```text
primary series
comparison series
additional series...
```

渲染规则：

- 第一条线是主线。
- comparison line 用于容量、总量或反方向指标。
- additional line 用于同图补充进程资源，例如 `Process CPU`、`Process RSS`。
- 多线图必须显示 legend，legend 文案使用 `series_label`。
- 没有有效 series 时显示 `N/A`，不生成空白 SVG。

这个规则保证 `Node Memory` 这类指标能在一张图里同时看节点用量、节点总量和 Fluxon 进程 RSS。

## 4. Hover 交互

鼠标悬浮在折线图上时，UI 必须显示：

- 垂直 hover 辅助线：`.metric_chart_hover_line`
- 每条曲线的对齐辅助点：`.metric_chart_hover_point_ring` 和 `.metric_chart_hover_point`
- tooltip：时间戳和当前 x 位置上每条曲线的格式化数值

辅助点和 tooltip 都从同一份 `data-lines` 取值。这样点位、颜色、legend 和 tooltip 数值保持一致。

离开图表时，tooltip 和所有辅助点隐藏。

## 5. 多选展开

`Metric Trends` 支持同时展开多个指标。状态保存在：

```text
selectedMetricKeys: string[]
```

交互规则：

- 点击未选中的 metric card：加入 `selectedMetricKeys`，创建对应 owner drilldown block。
- 点击已选中的 metric card：从 `selectedMetricKeys` 删除，同时删除该指标的 owner 展开状态。
- 多个选中指标按 `selectedMetricKeys` 顺序逐个渲染，不互相覆盖。

这意味着用户可以同时查看 `Node CPU`、`Node Memory`、`Cache Hit %` 等多个指标的 per owner 视图。

## 6. Owner Drilldown

每个选中指标对应一个 owner drilldown block。block 里每个 owner 用 `<details class="owner_metric_card">` 渲染。

owner card 的内容：

- owner id 和 node key。
- owner 汇总最新值。
- owner 汇总折线图。
- 展开后的成员行。

owner 展开状态按指标分别保存：

```text
expandedOwnersByMetric[metric_key] = [owner_id, ...]
```

因此，同一个 owner 在 `Node CPU` 里展开，不会强制影响 `Node Memory` 里的展开状态。

## 7. 刷新稳定性

`Metric Trends` 会随页面周期刷新。刷新时必须满足：

- 不清空整个 `#metric_grid` 后重建。
- 不清空整个 owner drilldown section 后重建。
- 已存在卡片按 `data-patch-key` 复用 DOM。
- 已展开的 owner 继续保持展开。
- 初次加载后不反复写回 `Loading metric panel...`，避免高度跳动。

当前实现用 `patchChildrenByKey(...)` 复用 metric card 和 owner card。刷新只更新必要 HTML，保留卡片节点本身。

## 8. 关键结论

- `Metric Trends` 的核心 contract 是“多线趋势 + 多选 owner drilldown + 稳定刷新”。
- `Node CPU`、`Node Memory`、`Segment Usage`、`GPU Memory`、网络指标必须保持多线展示；CPU/GPU 这类资源实例聚合百分比允许超过 `100%`。
- hover 辅助点是趋势图可读性的一部分，不能只显示 tooltip。
- 多选展开状态和 owner 展开状态必须分别持久化，避免用户刷新或轮询后丢失上下文。
