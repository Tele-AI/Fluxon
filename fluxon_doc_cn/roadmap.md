# Roadmap

## Release Notes

### 0.2.1

- [RELEASE] 统一 Python / Rust 包版本与发布产物命名到 `0.2.1`
- [DOC] 统一 Quick Start、安装部署文档和仓库首页中的版本口径到 `0.2.1`

### 0.1.7

- [KV\RPC] 引入异步多级连接管理模块 `tiermanager`，将通信主链路与通信控制面解耦。
- [KV\RPC] 引入 `tcp_thread`，提升 TCP 链路吞吐能力。
- [KV\RPC] 收束 external 跨 owner 通信路径为机内 ICE 与机间 TCP/RDMA 分层架构，提升扩展性。
- [FS] 引入 FluxonFS，统一 KV 文件纳管，支持多模态数据负载的 All in one 缓存体系。
- [FS] 支持跨域大文件夹的分布式并发扫描与传输。
- [OPS] 引入 FluxonOps，支持 Fluxon 集群分布式裸进程自部署与热更新。

### 0.1.6

- [KV\RPC] 支持进程间通信
- [LIB] framework 重构生命周期更易治理，支持可持续迭代
- [KV\RPC] 支持多跳 relay，跨多集群互联传输
- [KV\RPC] 支持 `cp_kv_to_file` 原语，为后续做中间缓存层功能做铺垫
- [TOOL] 支持监控面板 MQ 部分

### 0.1.5

- [KV\RPC] `tquic` 调优，整体优于 `qp2p` 版本 QUIC 性能，满足低延迟控制面和高吞吐数据面需求

### 0.1.4

- [TOOL] 支持 SSR 渲染简易监控面板，无需部署冗余 Grafana
- [MQ] 重构到 Rust，更加稳定，性能更好，也支持 prefetch

### 0.1.2

- [KV\RPC] 支持 shm 共享内存架构，两级架构 scale 更强，内存利用和命中也更好
