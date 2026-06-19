# Fluxon

![](./pics/fluxon架构图20260423.png)


当 GPU 算力持续提升，CPU 与 I/O 链路正越来越多地成为拖慢 AI 训练与推理效率的隐性瓶颈。Fluxon 的目标就是把底层“存”和“传”的复杂度尽量收束起来，让系统资源更多用于模型本身，而不是耗散在数据面的拼装与搬运上。

基于统一的 Rust 存传一体底座，Fluxon 向上提供三大标准化接口，直接面向 AI 系统里的核心瓶颈：

- **KV/RPC（统一键值与 RPC）**：打破数据孤岛，实现推理侧 `KVCache` 与 `latent cache` 的跨节点、跨进程高效复用
- **MQ（弹性消息队列）**：解耦系统依赖，支撑异构资源池之间的弹性消息传输
- **FS（兼容 `S3` 的文件、对象与缓存加速系统）**：统一键值、文件、对象三类缓存能力，并支持 AI 数据与模型文件的远端访问、`S3` 转发和跨集群大规模迁移


<div align="center">

[![Linux Only](https://img.shields.io/badge/Linux-Only-2ea44f)](#运行要求)
[![Python](https://img.shields.io/badge/Python-%3E%3D3.10-3776AB)](#运行要求)
[![Rust](https://img.shields.io/badge/Rust-1.93.0-000000)](./fluxon_rs/rust-toolchain.toml)
[![Latest](https://img.shields.io/badge/Latest-v0.2.1-f28500)](./fluxon_release)
[![Interfaces](https://img.shields.io/badge/Interfaces-KV%2FRPC%20%7C%20MQ%20%7C%20FS-1f6feb)](#接口能力)

[中文](./README_CN.md) | [English](./README.md) | [用户文档](https://tele-ai.github.io/fluxon/cn/) | [English Docs](https://tele-ai.github.io/fluxon/) | <a href="https://github.com/Tele-AI/fluxon" title="GitHub 仓库"><img src="https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png" width="18" height="18" alt="GitHub repository" /></a>

</div>

<a id="当前目录"></a>

## 🧭 当前目录

- [底座能力](#底座能力)
- [接口能力](#接口能力)
- [基准测试](#基准测试)
- [运行要求](#运行要求)
- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [贡献](#贡献)
- [Contributors](#contributors)
- [许可证](#许可证)
- [Star 增长趋势](#star-增长趋势)

<a id="底座能力"></a>

## 🧱 底座能力

- 全链路 Rust：把连接处理、协议编解码、状态机推进、共享内存管理和观测采集收进 Rust 热路径
- 存传一体：优先走跨进程共享内存快路径，把“存”和“传”放进同一套数据面统一优化
- 跨节点高性能传输：集群内优先使用 `RDMA`，并支持 `TCP` 自动兜底切换，以及通过界面动态启停和切换网卡
- 自动跨节点中继：支持跨节点、跨子集群的自动 relay / 中继转发，收敛复杂网络拓扑带来的接入成本
- 全局内存分配与治理：统一管控全局内存分配、对象生命周期、容量边界和回收策略，避免资源碎片化与失控膨胀
- 统一角色模型：`master`、`owner_client`、`external_client` 分层协作，把控制面和数据面组织成可扩展的树状拓扑，也把业务服务进程从数据面资源治理和底层通信链路里解耦出来
- 统一对象接口：由系统统一组织多字段对象，平衡接口灵活性、使用简单性和底层优化空间
- 张量原生零拷贝交接路径：更适合高频张量对象在缓存与传输路径中的复用
- 统一观测：基于 `Prometheus` 协议和 `Greptime` 收束 `metric / trace / log`，并内置完善的 GUI，用于观测集群成员状态、log 信息、关键指标和拓扑结构
- 三类接口复用：`KV/RPC`、`MQ`、`FS` 共用缓存、传输、租约、容量治理和观测能力

![](./pics/fluxon_commu.png)

![](./pics/topology_ui.png)

<a id="接口能力"></a>

## 🔌 接口能力

### Fluxon KV/RPC 接口

面向世界模型推理缓存、状态共享、服务间调用和张量对象复用。在多视角潜在空间预测、状态外推和前缀缓存复用场景下，Fluxon KV/RPC 提供的是更通用的 AI 数据面，而不只是面向单一 `KVCache` 的专项能力。

- 本地缓存副本与最终一致性读路径：优先命中本地快路径，后台异步同步元数据
- 批量回收与热点治理：通过 `batch_delete` 异步推进失效清理，并结合 `TinyLFU` 更高效地复用热点对象
- 同时管控 AI Workload 中的 `L2`、`L3`：让全局数据对象可索引、可定位、可复用，减少多级缓存重复驻留带来的冗余内存浪费
- KV 与 RPC 协同：同一套参数组织、缓存和通信底座同时服务状态存储与服务间调用

![](./pics/fluxon_kv.png)

### Fluxon MQ 接口

![](./pics/training_use_mq.png)

面向异构训练、数据处理流水线和跨资源池中间态交接。当前端 `producer` 和后端 `consumer` 被拆到不同机器、不同资源池甚至不同子集群时，Fluxon MQ 负责把消息保活、容量治理和跨集群放置收束到统一消息层。

- `Lease` 保活语义：将消息保活绑定到 `channel`，确保数据在真正消费前具备有限时域的可靠保留语义
- `channel` 级前缀统计与容量治理：持续维护消息数量与容量占用边界，方便扩缩和流量治理
- 跨集群负载感知放置：结合消费侧位置做 payload 放置决策，尽量缩短预取链路并稳定吞吐
- 与 KV 协同设计：消息壳和成员元数据留在控制面，大 payload 留在 `FluxonKV` 数据面，避免重复建设第二套大对象传输链路

![](./pics/fluxon_mq.png)

### Fluxon FS 接口

Fluxon FS 是一款面向 AI 数据与模型文件、兼容 `S3` 的高性能文件与对象缓存系统，具备读写加速、远端访问、`S3` 转发、缓存命中及跨集群大规模迁移等功能。面对高分辨率视频、轨迹样本和 Checkpoint 等大文件场景，Fluxon FS 能够将这些复杂的流动与加速需求，完美交付给同一套数据面。

- 统一缓存体系：直接复用 `FluxonKV/RPC` 的缓存与通信能力，把文件拆成 `KeyValue` 片段做分片缓存，实现一套系统同时兼容键值、文件、对象缓存读写加速
- `S3` 转发访问：支持面向 AI 数据与模型文件的对象存储访问入口和转发能力
- Python 文件语义透明接入：尽量保持 `open() / read() / write()` 的上层使用方式，同时减少系统调用与跨进程开销
- 小文件 / 大文件读写特化优化：针对不同文件粒度和读写路径分别做并发与链路优化，提升带宽利用率与整体吞吐
- 跨集群大规模搬迁：支持 `PB` 级数据迁移，并把缓存、传输和失败恢复放进统一链路

<a id="基准测试"></a>

## 📊 基准测试

benchmark 主要覆盖 `RPC`、`KV` 和 `FS` 三类数据面；相关脚本和配置主要位于 `fluxon_test_stack/`。

### Fluxon RPC 基准测试

RPC benchmark 主要展示不同消息规模和并发条件下的调用延迟与吞吐表现，用于观察服务间调用链路的稳定性和尾延迟表现。

![](./pics/fluxon_rpc_bench.png)

### Fluxon KV 基准测试

`TCP` benchmark 显示，Fluxon 在 `Read-affinity` 和 `Read-Zipf` 两类读多负载上明显高于 `MooncakeStore` 和 `Redis`；`put_only` 当前的主要约束仍在 inflight 元数据判重路径，而不在 payload 传输。

![](./pics/kv_benchmark_chart.png)

### Fluxon FS 基准测试

benchmark 显示，小文件读和大文件写已显著领先 `Alluxio`，大文件读基本接近，小文件写仍有继续优化空间。

![](./pics/fs_benchmark_chart.png)

### Fluxon MQ 基准测试

`MQ` 目前主要展示场景问题和数据面设计，自动化运行入口见 `test_runner.py` 与 `fluxon_test_stack/`。

<a id="运行要求"></a>

## 🧰 运行要求

**用于 Quick Start（`Docker`）：**

- 已安装 Docker
- Quick Start 镜像已经内置 demo 流程所需的中间件

**用于生产部署或源码构建：**

- **操作系统**：仅支持 Linux
- **Python**：`>= 3.10`
- **Rust**：工具链固定为 `1.93.0`，见 [fluxon_rs/rust-toolchain.toml](./fluxon_rs/rust-toolchain.toml)
- **外部中间件**：
  - 最小服务平面需要 `etcd` 和 `Greptime`
  - `FluxonFS` 的目录传输、预扫描等持久任务状态能力还需要 `TiKV PD` 和 `TiKV`
- **Docker**：Quick Start 镜像链路和运行时打包链路都需要 Docker

<a id="快速开始"></a>

## 🚀 快速开始

Quick Start 用于最短路径体验；正式安装、部署和运维入口见 [用户文档](https://tele-ai.github.io/fluxon/cn/user_doc/)。

### KV 快速开始

```bash
docker run --rm -it --network host \
  hanbaoaaa/fluxon_quick_start:0.2.1 \
  --mode kv \
  --etcd-client-port 12379 \
  --master-p2p-port 31000 \
  --panel-port 18080 \
  --greptime-http-port 14000 \
  --kv-http-port 8083
```

进入后可直接输入：

```text
put demo:hello world
get demo:hello
del demo:hello
```

运行效果：

![](./pics/quickstart_kv.png)

点开提示链接可以查看 KV Web UI：

![](./pics/quickstart_kvui.gif)

对应接口文档：

- [KV 和 RPC 接口](https://tele-ai.github.io/fluxon/cn/user_doc/%E7%94%A8%E6%88%B7---3---KV-RPC%E6%8E%A5%E5%8F%A3/)

### MQ 快速开始

```bash
docker run --rm -it --network host \
  hanbaoaaa/fluxon_quick_start:0.2.1 \
  --mode mq \
  --etcd-client-port 37379 \
  --kv-master-port 34200 \
  --greptime-http-port 14000 \
  --panel-port 18080
```

进入后可直接输入：

```text
put hello
put world
exit
```

后台 consumer 会持续打印收到的消息。  
启动后会额外打印 `MQ Web UI` 地址。

运行效果：

![](./pics/quickstart_mq.png)

对应接口文档：

- [MQ 接口](https://tele-ai.github.io/fluxon/cn/user_doc/%E7%94%A8%E6%88%B7---4---MQ%E6%8E%A5%E5%8F%A3/)

### FS 快速开始

```bash
docker run --rm -it --network host \
  hanbaoaaa/fluxon_quick_start:0.2.1 \
  --mode fs \
  --etcd-client-port 36379 \
  --kv-master-port 34100 \
  --greptime-http-port 14000 \
  --panel-port 34180
```

进入后可直接输入：

```text
ls
echo "hello fs" > notes.txt
cat notes.txt
ui
```

FS Quick Start 会额外打印：

- `fs_s3` endpoint
- Basic Auth 入口，默认账号密码是 `admin / admin`

运行效果：

![](./pics/quickstart_fs.png)

点开提示链接可以查看 FS Web UI：

![](./pics/quickstart_fsui.gif)

对应接口文档：

- [FS 接口](https://tele-ai.github.io/fluxon/cn/user_doc/%E7%94%A8%E6%88%B7---5---FS%E6%8E%A5%E5%8F%A3/)

<a id="项目结构"></a>

## 🗂️ 项目结构

- `fluxon_rs/`：Rust 核心实现与底层能力
- `fluxon_py/`：Python 接口、运行时与绑定
- `deployment/`：部署与运维工具链
- `scripts/`：脚本工具与辅助入口
- `setup_and_pack/`：打包与发布资源准备入口
- `examples/fluxon_quick_start/`：最小可运行环境入口
- `fluxon_test_stack/`：测试栈、benchmark 与 gitops 入口

<a id="贡献"></a>

## 🤝 贡献

欢迎参与贡献。开始之前，建议先阅读 GitHub Pages 上的开发者文档：

- [开发者文档总入口](https://tele-ai.github.io/fluxon/cn/dev_doc/)
- [开发者 - 1 - 打包核心安装包](https://tele-ai.github.io/fluxon/cn/dev_doc/%E5%BC%80%E5%8F%91%E8%80%85---1---%E6%89%93%E5%8C%85%E6%A0%B8%E5%BF%83%E5%AE%89%E8%A3%85%E5%8C%85/)
- [开发者 - 2 - 打包中间件和镜像](https://tele-ai.github.io/fluxon/cn/dev_doc/%E5%BC%80%E5%8F%91%E8%80%85---2---%E6%89%93%E5%8C%85%E4%B8%AD%E9%97%B4%E4%BB%B6%E5%92%8C%E9%95%9C%E5%83%8F/)
- [开发者 - 4 - 发布 Release](https://tele-ai.github.io/fluxon/cn/dev_doc/%E5%BC%80%E5%8F%91%E8%80%85---4---%E5%8F%91%E5%B8%83-Release/)

<a id="contributors"></a>

## 👥 Contributors

<a href="https://github.com/Tele-AI/fluxon/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=Tele-AI/fluxon" />
</a>

部分更早期的贡献记录已经无法从当前 commit 历史里完整反映，这里补充说明：

<p>
  <a href="https://github.com/yxrxy"><img src="https://wsrv.nl/?url=github.com/yxrxy.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="yxrxy" /></a>
  <a href="https://github.com/zTz01"><img src="https://wsrv.nl/?url=github.com/zTz01.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="zTz01" /></a>
  <a href="https://github.com/pakkah"><img src="https://wsrv.nl/?url=github.com/pakkah.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="pakkah" /></a>
  <a href="https://github.com/unity1263"><img src="https://wsrv.nl/?url=github.com/unity1263.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="unity1263" /></a>
  <a href="https://github.com/mumupika"><img src="https://wsrv.nl/?url=github.com/mumupika.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="mumupika" /></a>
  <a href="https://github.com/maplestarplayl"><img src="https://wsrv.nl/?url=github.com/maplestarplayl.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="maplestarplayl" /></a>
  <a href="https://github.com/RuileLu"><img src="https://wsrv.nl/?url=github.com/RuileLu.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="RuileLu" /></a>
  <a href="https://github.com/Summage"><img src="https://wsrv.nl/?url=github.com/Summage.png%3Fsize%3D64&amp;mask=circle&amp;w=64&amp;h=64&amp;fit=cover&amp;output=png" width="64" height="64" alt="Summage" /></a>
</p>

- `yxrxy`: FluxonFS 实现和优化
- `zTz01`: KVCache 优化
- `pakkah`: RDMA 支持、VLM 探索
- `unity1263`: KV共享内存设计接入、benchmark工具链
- `mumupika`: 初始 MQ 版本实现
- `maplestarplayl`: IPC 接入、SPDK 接入
- `RuileLu`: KV lease 功能支持
- `Summage`: 初始 KV 架构设计优化

<a id="许可证"></a>

## 📄 许可证

Fluxon 基于 Apache License 2.0 开源，见 [LICENSE](./LICENSE)。

<a id="star-增长趋势"></a>

## ⭐ Star 增长趋势

[![Stargazers over time](https://starchart.cc/Tele-AI/fluxon.svg)](https://starchart.cc/Tele-AI/fluxon)
