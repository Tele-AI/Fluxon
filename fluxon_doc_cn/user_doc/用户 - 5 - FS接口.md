# 用户 - 5 - FS接口

<!-- Maintenance note: Keep this page in three layers: KV service plane dependency, FS master/agent roles, then the remote mount/read/write verification script. Do not merge FS roles into the KV-only service-plane page, and do not skip the KV dependency layer here. -->

## 总体介绍

Fluxon FS 提供的是“把远端 export 挂到当前 Python 进程里，然后继续用 `open()` / `read()` / `write()` 访问”的能力。

用户直接接触的核心对象有三层：

- KV 服务平面对象：`etcd`、`greptime`、`master`、`owner`
- FS 角色对象：`fs_master`、`fs_agent`
- 当前 Python 进程内的 FS 挂载对象：`FluxonKvClientConfig`、`new_store(...)`、`FluxonFsPatcher`、`mount_remote_dir(...)`

它们之间的关系如下：

```text
etcd + greptime + fluxonkv master + owner
                       |
                       v
               fluxon_fs master
                       |
                       v
               fluxon_fs agent
                       |
                       v
FluxonKvClientConfig -> new_store(...) -> KvClient(store)
                       |
                       v
FluxonFsPatcher(store)
                       |
                       +-- set_master_config_yaml(...)
                       +-- set_cache_config_yaml(...)
                       +-- set_request_identity(...)
                       +-- install()
                       +-- mount_remote_dir(...)
                       |
                       v
open() / read() / write() / close()
```

`owner`、`external client`、`shared memory` 这些前置概念见 [架构和概念](./用户%20-%201%20-%20架构和概念.md)。`FluxonKvClientConfig` 和 `new_store(...)` 的基础语义见 [KV 和 RPC 接口](./用户%20-%203%20-%20KV-RPC接口.md)。

## 服务平面

在进入挂载代码之前，需要先把 FS 依赖的 KV 服务平面拉起来。共性的角色关系、启动顺序和 runtime 边界，统一见 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md)。

FS 直接复用 KV / MQ 的这条服务平面链路，直接接触的对象主要有：

- `greptime`：标准监控链路
- `etcd`：KV / FS 控制面元数据存储
- `start_kv_master_process(...)`：启动 `fluxonkv master`
- `start_owner_kvclient_process(...)`：启动 `owner`

FS 用户进程的共享前置链路如下：

1. 先起 `greptime`
2. 再起 `etcd`
3. 再起 `fluxonkv master`
4. 再起 `owner`
5. 再起 `fs master`
6. 再起 `fs agent`
7. 最后再运行挂载验证脚本

这条顺序对应的是本页默认的本地完整示例，也就是由当前脚本同时拉起 `kv master + owner + fs master + fs_agent`。

`examples/start_kv_and_fs_svc.py` 只启动 Fluxon 自己的角色。`etcd` / `greptime` 仍按服务平面文档单独启动；如果需要 `/ui/transfers/` 和预扫描，还要先启动 `transfer_state_store` 对应的 `pd` / `tikv`：

- 当前 `python3` 所在环境已经安装 `fluxon-*.whl` 和 `fluxon_pyo3-*.whl`；安装方式见 [用户 - 0 - 安装](./用户%20-%200%20-%20安装.md)

这个脚本支持两种启动方式：

- 默认方式：启动 `kv master + owner + fs master + fs agent`
- `--without-master`：只启动 `owner + fs_agent`，接入已经存在的 `kv master + fs master`

## FS master 与 fs_agent

KV 服务平面起来以后，FS 在这条链路上再加两个角色：

- `fs_master`：复用 KV external client 接入 KV 平面，并承载 panel / export 快照分发
- `fs_agent`：向 `fs_master` 注册 export，并对外提供远端目录访问

对应示例脚本：`examples/start_kv_and_fs_svc.py`

完整脚本如下：

```python
#!/usr/bin/env python3

import argparse

from pathlib import Path

from fluxon_py.runtime import (
    start_fs_agent_process,
    start_fs_master_process,
    start_kv_master_process,
    start_owner_kvclient_process,
    wait_subproc_or_ctrlc,
)
from fluxon_py.runtime.process_runner import ManagedSubprocess

ETCD_ENDPOINT = "127.0.0.1:2379"
GREPTIME_HTTP_PORT = 34030
GREPTIME_BASE_URL = f"http://127.0.0.1:{GREPTIME_HTTP_PORT}"
CLUSTER_NAME = "demo-fs-cluster"
SHARE_MEM_PATH = Path("/dev/shm/fluxon_fs_demo").resolve()
WORKDIR = Path("/tmp/fluxon_fs_demo/runtime").resolve()
REMOTE_ROOT_DIR = Path("/tmp/fluxon_fs_demo/remote_root").resolve()
KV_MASTER_PORT = 34100
FS_PANEL_PORT = 34180
FS_PANEL_LISTEN_ADDR = f"0.0.0.0:{FS_PANEL_PORT}"
FS_PANEL_PUBLIC_BASE_URL = f"http://127.0.0.1:{FS_PANEL_PORT}"
KV_MASTER_INSTANCE_KEY = "demo_fs_kv_master"
OWNER_INSTANCE_KEY = "demo_fs_owner"
FS_MASTER_INSTANCE_KEY = "demo_fs_master"
FS_AGENT_INSTANCE_KEY = "demo_fs_agent"
EXPORT_NAME = "demo-export"
OWNER_DRAM_BYTES = 1073741824
EXPORT_CACHE_MAX_BYTES = 1073741824
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"
TRANSFER_STATE_STORE_PD_ENDPOINTS = ["127.0.0.1:12379"]
TRANSFER_STATE_STORE_KEY_PREFIX = f"/fluxon_fs_transfer/{CLUSTER_NAME}/"
FS_MASTER_ACCESS_DB_PATH = (WORKDIR / "fs_master" / "access.db").resolve()


def main() -> None:
    args = parse_args()
    WORKDIR.mkdir(parents=True, exist_ok=True)
    REMOTE_ROOT_DIR.mkdir(parents=True, exist_ok=True)
    log_dir = (WORKDIR / "log").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    if args.with_master:
        kv_master_log_dir = (WORKDIR / "kv_master_logs").resolve()
        kv_master_log_dir.mkdir(parents=True, exist_ok=True)
        kv_master_stdout_log = (log_dir / "kv_master.log").resolve()
        # FS master persists panel auth state in this sqlite file, so the parent
        # directory must exist before Rust opens access_db_path.
        FS_MASTER_ACCESS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        fs_master_stdout_log = (log_dir / "fs_master.log").resolve()
        # FS depends on the KV service plane, so bring up KV roles before FS roles.
        kv_master_proc = start_kv_master_process(
            config=build_kv_master_config(log_dir=kv_master_log_dir),
            log_path=kv_master_stdout_log,
        )
    else:
        kv_master_stdout_log = None
        fs_master_stdout_log = None
        kv_master_proc = None
        fs_master_proc = None

    owner_stdout_log = (log_dir / "owner.log").resolve()
    owner_proc = start_owner_kvclient_process(
        config=build_owner_config(),
        log_path=owner_stdout_log,
    )

    if args.with_master:
        fs_master_proc = start_fs_master_process(
            config=build_fs_master_config(),
            log_path=fs_master_stdout_log,
        )

    fs_agent_stdout_log = (log_dir / "fs_agent.log").resolve()
    fs_agent_proc = start_fs_agent_process(
        config=build_fs_agent_config(),
        log_path=fs_agent_stdout_log,
    )
    children = []
    if kv_master_proc is not None:
        children.append(
            ManagedSubprocess(
                label="kv_master",
                proc=kv_master_proc,
            )
        )
    children.append(
        ManagedSubprocess(
            label="owner",
            proc=owner_proc,
        )
    )
    if fs_master_proc is not None:
        children.append(
            ManagedSubprocess(
                label="fs_master",
                proc=fs_master_proc,
            )
        )
    # Stop order is the reverse of this list, so append fs_agent last.
    children.append(
        ManagedSubprocess(
            label="fs_agent",
            proc=fs_agent_proc,
        )
    )

    print(f"[fluxon_fs] cluster name: {CLUSTER_NAME}")
    print(f"[fluxon_fs] share_mem_path: {SHARE_MEM_PATH}")
    print(f"[fluxon_fs] remote root dir: {REMOTE_ROOT_DIR}")
    print(f"[fluxon_fs] export name: {EXPORT_NAME}")
    print(f"[fluxon_fs] owner instance key: {OWNER_INSTANCE_KEY}")
    print(f"[fluxon_fs] fs master instance key: {FS_MASTER_INSTANCE_KEY}")
    print(f"[fluxon_fs] fs agent instance key: {FS_AGENT_INSTANCE_KEY}")
    print(f"[fluxon_fs] start masters in this script: {args.with_master}")
    if args.with_master:
        print(f"[fluxon_fs] panel listen addr: {FS_PANEL_LISTEN_ADDR}")
        print(f"[fluxon_fs] panel public base url: {FS_PANEL_PUBLIC_BASE_URL}")
        print(f"[fluxon_fs] transfer state store pd_endpoints: {TRANSFER_STATE_STORE_PD_ENDPOINTS}")
        print(f"[fluxon_fs] transfer state store key_prefix: {TRANSFER_STATE_STORE_KEY_PREFIX}")
        print(f"[fluxon_fs] bootstrap admin username: {ADMIN_USERNAME}")
        print(f"[fluxon_fs] bootstrap admin password: {ADMIN_PASSWORD}")
        print(f"[fluxon_fs] kv master stdout log: {kv_master_stdout_log}")
        print(f"[fluxon_fs] fs master stdout log: {fs_master_stdout_log}")
    else:
        print("[fluxon_fs] panel listen addr: disabled by --without-master")
        print("[fluxon_fs] panel public base url: disabled by --without-master")
        print("[fluxon_fs] transfer state store pd_endpoints: disabled by --without-master")
        print("[fluxon_fs] transfer state store key_prefix: disabled by --without-master")
        print("[fluxon_fs] bootstrap admin username: disabled by --without-master")
        print("[fluxon_fs] bootstrap admin password: disabled by --without-master")
        print("[fluxon_fs] kv master stdout log: disabled by --without-master")
        print("[fluxon_fs] fs master stdout log: disabled by --without-master")
    print(f"[fluxon_fs] owner stdout log: {owner_stdout_log}")
    print(f"[fluxon_fs] fs agent stdout log: {fs_agent_stdout_log}")
    stack_label = "fs demo stack" if args.with_master else "owner and fs agent"
    print(f"[fluxon_fs] waiting for Ctrl-C to stop {stack_label}")
    wait_subproc_or_ctrlc(
        children,
        on_ctrlc=lambda: print(f"[fluxon_fs] caught Ctrl-C, stopping {stack_label}"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start FS demo roles, optionally with local masters")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--with-master",
        dest="with_master",
        action="store_true",
        help="Start local kv master and fs master in this script (default)",
    )
    group.add_argument(
        "--without-master",
        dest="with_master",
        action="store_false",
        help="Do not start any master in this script; only start owner and fs_agent and attach to an existing cluster",
    )
    parser.set_defaults(with_master=True)
    return parser.parse_args()


def build_kv_master_config(*, log_dir: Path) -> dict:
    return {
        "instance_key": KV_MASTER_INSTANCE_KEY,
        "cluster_name": CLUSTER_NAME,
        "port": KV_MASTER_PORT,
        "etcd_endpoints": [ETCD_ENDPOINT],
        "log_dir": str(log_dir),
        "monitoring": {
            "prometheus_base_url": f"{GREPTIME_BASE_URL}/v1/prometheus",
            "prom_remote_write_url": [f"{GREPTIME_BASE_URL}/v1/prometheus/write"],
            "otlp_log_api": {
                "otlp_endpoint": f"{GREPTIME_BASE_URL}/v1/otlp/v1/logs",
            },
        },
    }


def build_owner_config() -> dict:
    return {
        "instance_key": OWNER_INSTANCE_KEY,
        "contribute_to_cluster_pool_size": {
            "dram": OWNER_DRAM_BYTES,
            "vram": {},
        },
        "fluxonkv_spec": {
            "etcd_addresses": [ETCD_ENDPOINT],
            "cluster_name": CLUSTER_NAME,
            "share_mem_path": str(SHARE_MEM_PATH),
            "sub_cluster": "default",
            "large_file_paths": [str((WORKDIR / "large" / "owner").resolve())],
        },
    }


def build_fs_master_config() -> dict:
    return {
        "kvclient": {
            "instance_key": FS_MASTER_INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": str(SHARE_MEM_PATH),
            },
        },
        "fluxon_fs": {
            "master": {
                "instance_key": FS_MASTER_INSTANCE_KEY,
                "pull_interval_ms": 1000,
            },
            "master_panel": {
                "listen_addr": FS_PANEL_LISTEN_ADDR,
                "public_base_url": FS_PANEL_PUBLIC_BASE_URL,
                "auto_refresh_interval_secs": 2,
                "access_db_path": str(FS_MASTER_ACCESS_DB_PATH),
                # bootstrap_access_model only seeds an empty access_db; once the DB has users,
                # later restarts keep using the DB state instead of overwriting it from config.
                # Manager users keep full export access through runtime auth checks, not by writing
                # synthetic root scopes into the DB.
                "bootstrap_access_model": {
                    "users": [
                        {
                            "username": ADMIN_USERNAME,
                            "password": ADMIN_PASSWORD,
                            "can_manage_users": True,
                        }
                    ],
                    "scope_access": [],
                },
                "transfer_state_store": {
                    "kind": "tikv",
                    "tikv": {
                        "pd_endpoints": TRANSFER_STATE_STORE_PD_ENDPOINTS,
                        "key_prefix": TRANSFER_STATE_STORE_KEY_PREFIX,
                    },
                },
                "s3_gateway": {
                    "get_object_inflight_pieces": 8,
                    "kv_miss_policy": "remote_read",
                },
            },
            "cache": {
                "stale_window_ms": 1000,
                "rules": [],
                "exports": {
                    EXPORT_NAME: {
                        "remote_root_dir_abs": str(REMOTE_ROOT_DIR),
                        "cache_max_bytes": EXPORT_CACHE_MAX_BYTES,
                    },
                },
            },
        },
    }


def build_fs_agent_config() -> dict:
    return {
        "kvclient": {
            "instance_key": FS_AGENT_INSTANCE_KEY,
            "fluxonkv_spec": {
                "cluster_name": CLUSTER_NAME,
                "share_mem_path": str(SHARE_MEM_PATH),
            },
        },
        "fluxon_fs": {
            "master": {
                # The agent follows this master instance key to pull the current export snapshot.
                "instance_key": FS_MASTER_INSTANCE_KEY,
            },
            "cache": {
                "stale_window_ms": 1000,
                "rules": [],
                "exports": {
                    EXPORT_NAME: {
                        "remote_root_dir_abs": str(REMOTE_ROOT_DIR),
                        "cache_max_bytes": EXPORT_CACHE_MAX_BYTES,
                    },
                },
            },
        },
    }


if __name__ == "__main__":
    main()
```

启动命令：

```bash
python3 examples/start_kv_and_fs_svc.py
python3 examples/start_kv_and_fs_svc.py --without-master
```

默认命令会启动本机 `kv master + owner + fs master + fs agent`。`--without-master` 只启动本机 `owner + fs_agent`，要求同一个 `cluster_name` 对应的 `kv master` 和 `fs master` 已经在别处运行。

远端 agent 机器最关键的约束如下：

- `ETCD_ENDPOINT` 必须改成现有集群实际使用的 `etcd` 地址
- `FS_MASTER_INSTANCE_KEY` 必须和现有 `fs master` 的实例 key 一致
- `OWNER_INSTANCE_KEY`、`FS_AGENT_INSTANCE_KEY`、`EXPORT_NAME`、`REMOTE_ROOT_DIR` 必须在每台 agent 机器上都唯一；重复使用这些值会让 UI 里的 agents 或 runtime exports 折叠到同一个成员上
- `FS_PANEL_PUBLIC_BASE_URL` 控制 UI 页面里的对外链接；外部访问 `fs master` 时，要把它改成实际可访问地址，`FS_PANEL_LISTEN_ADDR` 只控制绑定地址

脚本会持续运行，并打印：

- `cluster name`
- `share_mem_path`
- `remote root dir`
- `export name`
- `owner instance key`
- `fs master instance key`
- `fs agent instance key`
- `start masters in this script`
- `WORKDIR/log/owner.log`
- `WORKDIR/log/fs_agent.log`

默认模式下，还会额外打印：

- `panel listen addr`
- `panel public base url`
- `transfer state store pd_endpoints`
- `transfer state store key_prefix`
- 默认管理员用户名密码
- `WORKDIR/log/kv_master.log`
- `WORKDIR/log/fs_master.log`

默认模式下，这个脚本把四个子进程的 `stdout/stderr` 都收进 `WORKDIR/log`，终端只保留摘要信息；按 `Ctrl-C` 时，主进程会统一停止这四个角色。`--without-master` 模式下，脚本只管理本机 `owner + fs_agent` 两个角色。

## 远程挂载读写验证

根目录 `examples/` 里的公开 FS 验证脚本收束成三类对象：

- `examples/start_kv_and_fs_svc.py`
  - 本机直接拉起 `kv master + owner + fs master + fs agent`
- `examples/start_fluxon_fs_writer.py`
  - 注册 export，并持续写远端 export 文件和本地 cache 规则文件
- `examples/start_fluxon_fs_reader.py`
  - 通过 `install_patcher_from_master(...)` 安装 patcher，挂载 export，并持续交替读取远端文件和本地文件

最小成功路径如下：

1. 运行 `python3 examples/start_kv_and_fs_svc.py`
2. 保持它持续运行
3. 准备 writer 配置，运行 `python3 examples/start_fluxon_fs_writer.py -c <writer-config.yaml> -w <writer-workdir>`
4. 准备 reader 配置，运行 `python3 examples/start_fluxon_fs_reader.py -c <reader-config.yaml> -w <reader-workdir>`

这条最小成功路径默认对应本页的本地完整示例，也就是不带 `--without-master` 的启动方式。`--without-master` 用于把当前机器接到已经存在的 KV / FS 集群；如果继续运行 `start_fluxon_fs_writer.py` / `start_fluxon_fs_reader.py`，配置里的这些对象必须和现有集群一致：

- `cluster_name`
- `share_mem_path`
- `fluxon_fs.master.instance_key`
- `export_name`
- `remote_root_dir_abs`

`start_fluxon_fs_writer.py` 固定负责两件事：

- 向当前 `fs master` 注册当前 export
- 持续写入远端 export 文件和本地 cache 规则文件

`start_fluxon_fs_reader.py` 固定负责三件事：

- 用 external client 接到本机 owner
- 通过 `install_patcher_from_master(...)` 安装 patcher，并从 `fs master` 拉取配置
- 把指定 export 挂到本地 mount dir，持续交替读取远端文件和本地文件

当 `reader` 开始稳定打印 `op=read_remote` / `op=read_local` 时，说明远端挂载链路和本地 cache 规则都已经打通。

如果你要直接在自己的 Python 进程里调用 `FluxonFsPatcher` 和 `mount_remote_dir(...)`，继续看下面的 API 规则；根目录 `examples/` 不再保留单独的进程内挂载演示脚本。

## 目录传输与预扫描

有时候需要做一次很大的目录搬迁，例如跨机群搬迁，或者跨共享存储搬迁。目录里可能有多层子目录、很多文件，整个任务会持续很久。这个时候，用户真正关心的通常不是某一个文件，而是这次目录任务有没有开始、当前扫到哪里了、是不是已经开始写入、现在带宽怎么样。

目录传输与预扫描就是为这种场景准备的。它把这种长时间的大目录搬运收成一个持续运行的任务：目录可以直接在网页里发起，也可以先做预扫描，等目录规模、目标位置和并发策略确定后，再导入成正式任务。

### 在网页里直接发起目录传输

最常见的入口是双 pane 浏览页面里的跨 export 文件夹拖拽。这里说的是网页里的文件夹从一个 pane 拖到另一个 pane，不是把本地文件夹拖进浏览器。

操作顺序如下：

1. 打开两个 pane
2. 左侧定位源文件夹
3. 右侧定位目标 export 和目标目录
4. 把左侧文件夹拖到右侧
5. 在弹窗里填写 `desired_worker_count` 和 `batch_ready_bytes`
6. 提交后，到 `/ui/transfers/` 查看任务

提交之后，这个目录任务会出现在 `/ui/transfers/` 的 `FluxonFS Transfer Jobs` 里。

### 页面上能看到什么

`/ui/transfers/` 页面里，和这组功能直接相关的有两个区域：

- `Pre-Scans`
- `FluxonFS Transfer Jobs`

`FluxonFS Transfer Jobs` 用来查看已经进入正式传输链路的目录任务。页面上会持续显示：

- 扫描进度
- 已经拆出的 batch 数量
- 当前 running batches 数量
- 当前 `live bandwidth`
- worker 明细

这些信息通常足够判断：

- 任务现在还在扫描，还是已经开始写入
- 当前带宽是否正常
- 当前并发是否达到预期

### 在页面上导入预扫描

当 `Pre-Scans` 里已经有一条预扫描记录后，可以直接在网页里把它导入成正式目录任务。

操作顺序如下：

1. 打开 `/ui/transfers/`
2. 在 `Pre-Scans` 里找到对应任务
3. 点击 `Import`
4. 在弹窗里选择 `source export`
5. 选择 `target export`
6. 填写 `target prefix`
7. 填写 `desired_worker_count`
8. 提交后，这个任务会进入 `FluxonFS Transfer Jobs`

如果同一个源目录同时匹配多个 `source export`，页面会把这些候选都列出来，由用户自己选择。

### 启动时的 TiKV 配置

目录传输和预扫描都依赖 `transfer_state_store`。`fs master` 页面和独立发起预扫描的进程，必须共用同一份 TiKV 命名空间；否则脚本里发起的预扫描，页面上看不到。

启动时最关键的是下面两项要一致：

- `pd_endpoints`
- `key_prefix`

本页这个 `start_kv_and_fs_svc.py` 示例已经把它们写成：

- `TRANSFER_STATE_STORE_PD_ENDPOINTS = ["127.0.0.1:12379"]`
- `TRANSFER_STATE_STORE_KEY_PREFIX = "/fluxon_fs_transfer/demo-fs-cluster/"`

`fs master` 的 `master_panel` 配置里需要带上这一段：

```yaml
transfer_state_store:
  kind: tikv
  tikv:
    pd_endpoints:
      - "127.0.0.1:12379"
    key_prefix: "/fluxon_fs_transfer/demo-fs-cluster/"
```

独立预扫描脚本里用的 `FluxonFsTransferStateStoreTiKvConfig(...)`，也要使用同样的 `pd_endpoints` 和 `key_prefix`。

### 页面截图（待补）

- 图：`/ui/transfers/` 页面总览
  需要标出 `Pre-Scans` 和 `FluxonFS Transfer Jobs`
- 图：`Pre-Scans` 的 `Import` 弹窗
  需要标出 `source export`、`target export`、`target prefix`、`desired_worker_count`
- 图：双 pane 页面里跨 export 拖拽文件夹后的目录传输表单
  需要标出 `desired_worker_count` 和 `batch_ready_bytes`

### 独立进程发起预扫描示例

独立发起预扫描时，需要先启动的是 TiKV `transfer_state_store` 对应的 PD 和 TiKV。

这一段不依赖下面这些组件先启动：

- `etcd`
- `fluxonkv master`
- `owner`
- `fs master`
- `fs agent`

也就是说，只要 TiKV `transfer_state_store` 可用，预扫描脚本就可以先单独跑起来。等后面 `fs master` 页面使用同一份 `pd_endpoints` 和 `key_prefix` 启动后，网页里就能看到这次预扫描。

下面这个示例会扫描 `/data/demo_src`，并把结果写到和页面共用的 `transfer_state_store` 里。脚本跑完后，打开 `/ui/transfers/`，就可以在 `Pre-Scans` 里看到这次预扫描。

最小示例如下：

```python
#!/usr/bin/env python3

from fluxon_py.fluxon_fs import (
    FluxonFsTransferSkipEntry,
    FluxonFsTransferSkipEntryKind,
    FluxonFsTransferStateStoreConfig,
    FluxonFsTransferStateStoreKind,
    FluxonFsTransferStateStoreTiKvConfig,
    transfer_check_local_blocking,
)

STORE = FluxonFsTransferStateStoreConfig(
    kind=FluxonFsTransferStateStoreKind.TIKV,
    tikv=FluxonFsTransferStateStoreTiKvConfig(
        pd_endpoints=["127.0.0.1:12379"],
        key_prefix="/fluxon_fs_transfer/demo_prescan/",
    ),
)

summary = transfer_check_local_blocking(
    src_root_dir="/data/demo_src",
    transfer_state_store=STORE,
    batch_ready_bytes=8 * 1024 * 1024 * 1024,
    skip_entries=[
        FluxonFsTransferSkipEntry(
            kind=FluxonFsTransferSkipEntryKind.DIR,
            relpath="tmp",
        ),
        FluxonFsTransferSkipEntry(
            kind=FluxonFsTransferSkipEntryKind.FILE,
            relpath="logs/debug.txt",
        ),
    ],
    checker_concurrency_limit=4,
    enable_cli_progress=True,
)

print(summary)
```

`summary` 里最有用的是 `job_id`、`scan_epoch` 和 `batch_count`。网页上的 `Pre-Scans` 会按同一个 `job_id` 展示这次预扫描结果。

## 关键对象与规则

### `FluxonFsPatcher`

`FluxonFsPatcher` 不是独立入口，它必须依附在 `new_store(...)` 返回的 `store` 上。

固定顺序如下：

1. `store = new_store(cfg)...`
2. `patcher = FluxonFsPatcher(store)`
3. `patcher.set_master_config_yaml(...)`
4. `patcher.set_cache_config_yaml(...)`
4. `patcher.set_request_identity(...)`
5. `patcher.install()`
6. `patcher.mount_remote_dir(...)`
7. `open()` / `read()` / `write()`
8. `patcher.uninstall()`
9. `store.close()`

这里不能把 `store.close()` 放到 `patcher.uninstall()` 前面，因为 patcher 还在工作时，后续文件操作仍然可能走到底层 client。

### `set_master_config_yaml(...)` 与 `set_cache_config_yaml(...)`

这两个接口一起构成当前示例里的最小 authority：

- `set_master_config_yaml(...)` 负责注入 `fluxon_fs.master.instance_key`，让挂载后的 mount-registry RPC 能回报给正确的 `fs master`
- `set_cache_config_yaml(...)` 负责注入当前 export 快照，让 patcher 知道有哪些 export 可以挂载

业务代码可以直接在 Python 里构造这两段 YAML 文本；当前公开 runtime 示例 `start_fluxon_fs_reader.py` 则通过 `install_patcher_from_master(...)` 从配置文件和 `fs master` 拉取这两层信息。

### `set_request_identity(...)`

`set_request_identity(username, password)` 负责把当前 Python 进程里的后续 FS 请求绑定到这组身份。

如果不设置身份：

- 当 `access_db` 为空、系统还没有启用真实用户模型时，请求可能处于未鉴权路径
- 一旦 master 已经有用户和 `scope_access`，请求会按这个身份做权限判断

用户示例里应该显式设置身份，不要把鉴权省略掉。

### `bootstrap_access_model`

`bootstrap_access_model` 是 `fs master` 启动配置里的必填项，用来给一个空的 `access_db` 写入第一批账号和目录权限。

配置位置：

```yaml
 fluxon_fs:
  master_panel:
    access_db_path: /path/to/access.db
    bootstrap_access_model:
      users:
        - username: admin
          password: admin
          can_manage_users: true
      scope_access: []
```

规则如下：

- `access_db_path` 是长期 authority
- `bootstrap_access_model` 必须在启动配置里显式提供
- 只有当 `access_db` 还没有用户时，`bootstrap_access_model` 才会写入数据库
- 数据库一旦已有用户，后续重启以数据库为准
- `can_manage_users: true` 的用户在运行时可以访问所有当前 export，不依赖在数据库里预先展开 root `scope_access`
- 页面不再提供首次管理员创建入口；首个管理员只能通过启动配置初始化

## 挂载目录规则

`mount_remote_dir(local_mount_dir_abs=..., export_name=...)` 对本地挂载目录的要求如下：

- 必须是绝对路径
- 不能是 `/`
- 如果目录不存在，Fluxon 会创建它
- 如果目录已存在，它必须是空目录
- 不能和当前进程里已有挂载目录互相重叠

因此，挂载目录并不要求必须放在 `/fluxon_fs/...` 下。`/tmp/fluxon_fs_demo/mount_demo` 这种绝对路径也可以。

## 日志

如果需要更详细的 Python 侧日志，可以在启动用户进程前设置：

```bash
FLUXON_LOG=DEBUG python3 examples/start_fluxon_fs_reader.py -c <reader-config.yaml> -w <reader-workdir>
```

常用值有：

- `DEBUG`
- `INFO`
- `WARNING`
- `ERROR`
- `CRITICAL`

## 常见错误

### `new_store failed`

通常表示当前 external client 没有接上本机 owner。先检查：

- `start_kv_and_fs_svc.py` 是否还在运行
- `CLUSTER_NAME`
- `SHARE_MEM_PATH`

### `fluxon_fs cache config is not loaded yet`

通常表示 `set_cache_config_yaml(...)` 没有成功完成，或者脚本里的 cache 配置和服务端当前 export 配置不一致。先检查：

- `FS_MASTER_INSTANCE_KEY` 是否一致
- `EXPORT_NAME`
- `REMOTE_ROOT_DIR`

### `unknown export_name`

表示客户端要挂载的 `EXPORT_NAME` 没有出现在当前 `fs master` 的 export 快照里。先检查：

- `start_fluxon_fs_writer.py` 和 `start_fluxon_fs_reader.py` 的 `export_name` 是否一致
- `REMOTE_ROOT_DIR` 是否和启动脚本里的 export 配置对应

### `permission denied` 或 `PermissionError`

这表示路径存在，但当前身份没有访问权限。先检查：

- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- 当前 `access_db` 里是否已经被新的用户数据覆盖

如果你已经通过页面或数据库改过管理员密码，那么旧的 `bootstrap_access_model` 密码不会再生效。

管理员对所有 export 的访问权限也是运行时按 `can_manage_users` 判断，不需要额外往 `scope_access` 里补根路径。
