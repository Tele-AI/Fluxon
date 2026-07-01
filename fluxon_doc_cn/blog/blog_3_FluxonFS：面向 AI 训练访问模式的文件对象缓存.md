# FluxonFS S1：面向 AI 训练访问模式的文件对象缓存

FluxonFS 是 Fluxon 面向文件对象访问提供的缓存加速层。它服务的对象包括训练样本、模型文件、checkpoint、高分辨率视频、轨迹数据和远端 export。上层仍然使用文件语义访问数据；下层则复用 Fluxon KV 的共享内存、跨节点传输、容量治理和可观测性能力，让文件对象进入统一的数据面加速底座。

## AI 训练为什么需要文件对象缓存

AI 训练链路中的文件访问已经超出单一数据集读取。训练任务启动前需要发现目录、扫描样本、读取模型文件；训练过程中会持续加载样本、写入日志和阶段性产物；训练恢复和容灾依赖 checkpoint 的稳定保存与读取。随着数据集规模、模型体积和训练节点数量增长，远端访问、本机缓存、跨节点复用和大文件传输会同时出现在一条训练链路里。

常见文件访问可以分成几类：

| 访问类型 | 典型对象 | 对数据面的要求 |
| --- | --- | --- |
| 训练前加载 | 样本文件、索引文件、配置文件 | 小文件和中等对象重复读取稳定，冷读和热读差距可控 |
| 训练中写入 | 日志、中间结果、阶段性产物 | 写入、关闭和提交路径开销可控 |
| checkpoint | 模型权重、优化器状态、训练快照 | 大块连续写入和整文件读取稳定 |
| 随机读取 | 被打散访问的小对象或切片数据 | 热态随机访问延迟和吞吐稳定 |
| 元数据扫描 | 目录遍历、文件发现、状态查询 | 高并发 `list/stat` 能力稳定 |
| 远端 export | 跨节点或远端目录 | 本机缓存和跨节点传输协同工作 |

如果这些路径分别依赖远端对象存储、本机临时缓存、独立文件系统和额外同步脚本，训练系统会在多个组件之间反复搬运、落盘和重新索引同一批数据。FluxonFS 的定位是把这些文件对象接入 Fluxon 已有的数据面，让文件访问、KV 缓存和跨节点传输共享一套底层资源。

## FluxonFS 的架构位置

FluxonFS 建立在 Fluxon KV 服务平面之上。KV 平面提供 `etcd`、`greptime`、`master` 和 `owner`，其中 `owner` 贡献共享内存池并承载跨节点传输。FS 在这条链路上增加 `fs_master` 和 `fs_agent`：`fs_master` 承载 FS 控制面、panel 和 export 快照分发；`fs_agent` 注册 export，并对外提供远端目录访问。用户进程通过 `FluxonFsPatcher` 挂载远端目录后，继续使用 `open()`、`read()`、`write()` 和 `close()` 访问文件。

这条架构有两个关键点。

第一，FS 角色本身不重新建立一套大对象数据面。文件内容会被切成 `KeyValue` 片段，进入 Fluxon KV 的缓存、传输和容量治理路径。这样，文件对象和 KV 对象可以复用同机共享内存、跨节点 P2P 传输以及统一的观测链路。

第二，用户进程仍然以文件语义接入。业务代码面对的是远端 export 和普通文件读写接口，不需要直接感知底层对象切片、owner 放置或跨节点传输路径。这个分层让 FluxonFS 可以同时服务“像文件一样使用”和“像数据面对象一样治理”两个目标。

## 测试覆盖的访问模式

测试选择了训练链路中最常见的六类文件访问行为：

| 场景 | 关注点 | 参数 |
| --- | --- | --- |
| `read_baseline` | 训练前样本读取和整文件拉取 | `4KiB x 2000`、`256KiB x 400`、`4MiB x 40`，`iterations=3`；单大文件为 `1GiB x 1`，`iterations=2`，`worker_threads=1` |
| `write_commit_baseline` | 训练产物写入并提交 | `4KiB x 2000`、`256KiB x 400`、`4MiB x 40`，`chunk_size=256KiB`，`iterations=3` |
| `ml_dataloader` | loader 连续读取同一批样本 | `32KiB x 2000`，`epochs=3` |
| `checkpoint_save` | 模型快照保存 | `128MiB x 8`，`chunk_size=256KiB`，`iterations=2` |
| `random_access` | 小对象随机访问 | `4KiB`，`working_set=1000`，`access=500`，`iterations=3` |
| `metadata_scan` | 目录遍历、文件发现和状态查询 | `4KiB x 10000`，`iterations=3` |

这里的 `cold` 表示这批数据第一次被读取，系统里还没有这批数据的缓存；`warm` 表示同一批数据已经读过，再读一次。`local` 表示访问本机节点上的数据，`remote` 表示跨节点访问数据。`epoch` 只出现在训练加载场景里，表示 loader 把同一批样本完整读过一轮。

吞吐用 `MB/s = total_bytes / elapsed_seconds / 1048576` 计算；元数据扫描用 `ops/s = total_ops / elapsed_seconds` 计算。训练加载按 `file_size x file_count x epochs` 计算有效读取量，checkpoint 保存按 `file_size x file_count x iterations` 计算写入量。

测试运行在双机环境里。压测机硬件为 `AMD Ryzen Threadripper PRO 7995WX`，`96 cores / 192 threads`，`502 GiB` 内存，`Ubuntu 24.04.1`，内核 `6.17.0-35-generic`。本次 FS 测试使用 `16` 个 worker，总 inflight 为 `64`。小文件场景统计 `30s`，checkpoint 和单大文件场景统计 `120s`。

## 读路径

训练开始前，loader 往往会反复读取大量样本文件。样本可能是 `4KiB` 级的小对象，也可能是 `256KiB` 到 `4MiB` 的中大对象。本次顺序读测试把本机数据和远端数据分开看。

| 测试项 | FluxonFS | Alluxio | 差异 |
| --- | ---: | ---: | ---: |
| `4KiB cold local` | 45.5 MB/s | 38.2 MB/s | +19.1% |
| `4KiB warm local` | 61.4 MB/s | 22.0 MB/s | +179.1% |
| `256KiB cold local` | 2148.1 MB/s | 1741.6 MB/s | +23.3% |
| `256KiB warm local` | 3286.3 MB/s | 1366.2 MB/s | +140.5% |
| `4MiB cold local` | 2096.3 MB/s | 6367.1 MB/s | -67.1% |
| `4MiB warm local` | 1676.0 MB/s | 6154.1 MB/s | -72.8% |
| `4KiB cold remote` | 44.0 MB/s | 35.4 MB/s | +24.3% |
| `4KiB warm remote` | 60.1 MB/s | 24.3 MB/s | +147.3% |
| `256KiB cold remote` | 2027.3 MB/s | 1321.3 MB/s | +53.4% |
| `256KiB warm remote` | 3802.1 MB/s | 1328.9 MB/s | +186.1% |
| `4MiB cold remote` | 53.7 MB/s | 14.6 MB/s | +267.8% |
| `4MiB warm remote` | 107.9 MB/s | 6336.0 MB/s | -98.3% |

FluxonFS 在 `4KiB` 和 `256KiB` 上更稳定，热读优势尤其明显。这类对象更接近样本文件、切片数据和中等粒度训练输入。`4MiB` 顺序读则暴露出边界：本机 cold/warm 两个点 Alluxio 更高，远端 warm 点差距也明显高于同组其它结果。

这说明 FluxonFS 适合高频样本和中等对象的重复读取，但 `4MiB` 级连续读还需要继续优化。尤其是 `4MiB warm remote` 这个点，FluxonFS 为 `107.9 MB/s`，Alluxio 为 `6336.0 MB/s`，差距明显大于同组其它结果，后续复测应优先确认。

## 写路径：本机提交写优势明显，远端大块写仍需优化

训练过程中会持续写入中间结果、日志和阶段性产物。`write_commit_baseline` 看的是文件写入并完成提交后的吞吐。

| 测试项 | FluxonFS | Alluxio | 差异 |
| --- | ---: | ---: | ---: |
| `4KiB local` | 21.2 MB/s | 5.7 MB/s | +271.9% |
| `256KiB local` | 1920.7 MB/s | 247.4 MB/s | +676.4% |
| `4MiB local` | 11634.0 MB/s | 4515.2 MB/s | +157.7% |
| `4KiB remote` | 12.9 MB/s | 5.3 MB/s | +143.4% |
| `256KiB remote` | 51.6 MB/s | 142.6 MB/s | -63.8% |
| `4MiB remote` | 78.7 MB/s | 254.6 MB/s | -69.1% |

本机写入是 FluxonFS 的优势场景，`256KiB` 和 `4MiB` 都明显高于 Alluxio。远端写入里，FluxonFS 仍然在 `4KiB` 上占优，但 `256KiB` 和 `4MiB` 落后。这个结果表明，跨节点大块写入需要进一步优化。

## 训练加载：整体接近持平

`ml_dataloader` 场景让 loader 连续读取同一批样本 `3` 轮，更接近训练时持续取样的状态。

| 测试项 | FluxonFS | Alluxio | 差异 |
| --- | ---: | ---: | ---: |
| `local` | 65.7 MB/s | 59.0 MB/s | +11.4% |
| `remote` | 65.7 MB/s | 69.3 MB/s | -5.2% |

FluxonFS 和 Alluxio 在这组 loader 测试里接近持平。本机数据 FluxonFS 略高，远端数据 Alluxio 略高，整体差距不大。

## Checkpoint：本机快照保存优势明显

checkpoint 保存对应模型快照写入，重点看大块连续写能否跑出足够高的吞吐。

| 测试项 | FluxonFS | Alluxio | 差异 |
| --- | ---: | ---: | ---: |
| `local` | 32165.9 MB/s | 3407.0 MB/s | +844.1% |
| `remote` | 95.6 MB/s | 60.5 MB/s | +58.0% |

本机 checkpoint 写入中，FluxonFS 高出 Alluxio 一个数量级。远端写入也保持领先，但优势没有本机明显。这个差异也提醒我们：引用 checkpoint 结果时需要同时写清 `local` 和 `remote`，不能只拿本机峰值代表全部写入路径。

## 随机访问：热态小对象优势更明显

随机访问模拟小对象被打散读取的情况。这个场景重点看数据读过一次之后，再次访问是否还能保持稳定。

| 测试项 | FluxonFS | Alluxio | 差异 |
| --- | ---: | ---: | ---: |
| `cold local` | 37.1 MB/s | 36.6 MB/s | +1.4% |
| `warm local` | 41.0 MB/s | 23.5 MB/s | +74.5% |
| `cold remote` | 37.4 MB/s | 36.3 MB/s | +3.0% |
| `warm remote` | 37.7 MB/s | 27.1 MB/s | +39.1% |

第一次读取时两者接近。再次读取时 FluxonFS 领先更明显，说明小对象重复访问的路径更稳定。这类结果更贴近高频样本、小文件切片和训练过程中的重复读取。

## 元数据扫描：首次目录遍历是短板

元数据扫描看的是目录遍历、文件发现和状态查询能力，单位是 `ops/s`，不代表文件内容吞吐。

| 测试项 | FluxonFS | Alluxio | 差异 |
| --- | ---: | ---: | ---: |
| `cold local` | 21479.3 ops/s | 62437.3 ops/s | -65.6% |
| `warm local` | 25344.3 ops/s | 23572.9 ops/s | +7.5% |
| `cold remote` | 16660.8 ops/s | 60392.6 ops/s | -72.4% |
| `warm remote` | 20570.5 ops/s | 23618.1 ops/s | -12.9% |

第一次扫描时 Alluxio 明显更快；再次扫描后差距缩小，本机 warm 场景里 FluxonFS 略高。

## 结尾

FluxonFS 这轮测试给出的判断较为明确：在小文件重复读取、本机提交写、checkpoint 保存、随机访问热读和单大文件读取上，FluxonFS 已经表现出文件对象缓存加速层的价值。
