# 用户 - 5 - FS 接口

## FS 接口

Fluxon FS 可以把远端机器上的目录挂载到当前 Python 进程中。挂载完成后，业务代码继续使用普通的 `open()`、`read()` 和 `write()` 访问文件。

### 先理解四个名称

- `export`：FS Agent 对外暴露的一份目录。
- `EXPORT_NAME`：这份 export 的逻辑名称。
- `REMOTE_ROOT_DIR`：该目录在 FS Agent 机器上的实际路径。
- `mount_dir_abs`：Reader 进程中的本地挂载目录。

它们的关系如下：

```text
FS Agent 暴露 REMOTE_ROOT_DIR
              ↓
FS Master 发布 EXPORT_NAME
              ↓
Reader 把 export 挂载到 mount_dir_abs
              ↓
       open() / read() / write()
```

FS Master 负责 export 配置、访问控制和管理页面；FS Agent 负责提供远端目录访问。`FluxonFsPatcher` 安装在 Reader 进程中，把对挂载目录的文件操作转发到对应的 FS Agent。

### 开始前检查

运行本页示例前，确认以下条件：

- 已按 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md) 启动 Greptime 和 etcd。
- 当前 Python 环境已经安装 `fluxon-*.whl` 和 `fluxon_pyo3-*.whl`；安装方式见 [用户 - 0 - 安装](./用户%20-%200%20-%20安装.md)。
- 本机完整示例使用的端口没有被占用，`WORKDIR` 和 `REMOTE_ROOT_DIR` 可写。
- `KvClient` 的基础用法已经明确；相关说明见 [用户 - 3 - KV 和 RPC 接口](./用户%20-%203%20-%20KV-RPC接口.md)。

普通挂载和文件读写不使用目录传输状态。只有启用 `/ui/transfers/` 或预扫描时，才需要额外启动 PD 和 TiKV。

### 先按使用场景选择

- **本机完整示例**：启动本机 KV Master、Owner、FS Master 和 FS Agent。首次运行优先使用这条路径。
- **接入已有集群**：本机只启动新的 Owner 和 FS Agent，连接已有 KV Master 与 FS Master。
- **目录传输或预扫描**：完成 FS 基础服务后，再准备 PD 和 TiKV。该流程放在本页末尾。

### 启动本机 FS 服务

`examples/start_kv_and_fs_svc.py` 默认在本机启动：

```text
KV Master → Owner → FS Master → FS Agent
```

#### 首次只需确认这些值

- `CLUSTER_NAME`：KV 与 FS 共用的集群名。
- `SHARE_MEM_PATH`：本机 Owner 和所有本机 FS 进程共用的共享内存目录。
- `FS_MASTER_INSTANCE_KEY`：FS Master 的实例标识，也是 FS Agent 和 Reader 获取配置时使用的目标。
- `EXPORT_NAME`：FS Agent 发布、Reader 挂载的 export 名。
- `REMOTE_ROOT_DIR`：本机 FS Agent 暴露的绝对目录。
- `ADMIN_USERNAME` / `ADMIN_PASSWORD`：首次创建 access DB 时使用的管理员凭据。

首次本机运行时，其他实例标识、端口、`WORKDIR` 和 cache 大小可以先保留示例值。`admin/admin` 只适合本机演示，实际环境必须修改。

#### 启动命令与成功判据

在独立终端中运行：

```bash
python3 examples/start_kv_and_fs_svc.py
```

启动成功后：

- 终端打印 `cluster name`、`remote root dir`、`export name` 和四个角色的日志路径。
- 终端最后打印 `waiting for Ctrl-C to stop fs demo stack`，进程保持运行。
- `/tmp/fluxon_fs_demo/remote_root` 已经创建。
- FS Panel 可以通过 `http://127.0.0.1:34180` 访问。
- `/tmp/fluxon_fs_demo/runtime/log/` 下的四个日志中没有启动错误。

完整脚本如下：

<details>
<summary><strong>📄 查看完整脚本（点击展开）</strong>｜<code>examples/start_kv_and_fs_svc.py</code></summary>

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


def build_owner_large_file_paths() -> list[str]:
    return [str((WORKDIR / "large" / "owner").resolve())]


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
    children: list[ManagedSubprocess] = []
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
            "large_file_paths": build_owner_large_file_paths(),
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

</details>

#### 接入已有集群

远端 Agent 机器使用：

```bash
python3 examples/start_kv_and_fs_svc.py --without-master
```

运行前必须调整：

- `ETCD_ENDPOINT` 和 `CLUSTER_NAME`：指向已有集群。
- `FS_MASTER_INSTANCE_KEY`：与已有 FS Master 完全一致。
- `SHARE_MEM_PATH`：与当前机器的新 Owner 一致。
- `OWNER_INSTANCE_KEY` 和 `FS_AGENT_INSTANCE_KEY`：在集群内使用新值。
- `EXPORT_NAME`：为当前远端目录分配不重复的名称。
- `REMOTE_ROOT_DIR`：使用当前 Agent 机器上的绝对路径。

该模式只管理本机 Owner 和 FS Agent，不会启动 KV Master、FS Master 或 Panel。

### 远程挂载读写验证

验证流程使用三个脚本：

- `start_kv_and_fs_svc.py`：启动服务角色。
- `start_fluxon_fs_writer.py`：注册 export，并持续写远端文件和本地对照文件。
- `start_fluxon_fs_reader.py`：挂载 export，并持续读取远端文件和本地对照文件。

#### 运行前的配置对应关系

Writer 和 Reader 都要求：

- `-c/--config` 指向已经准备好的环境 YAML。
- `-w/--workdir` 指向当前进程独占的可写目录。
- `kvclient.instance_key` 在集群内唯一。
- `cluster_name` 与 FS 服务一致。
- `share_mem_path` 与各自机器上的 Owner 一致。

此外：

- Writer 配置中的 export 名和远端根目录必须与目标 FS Agent 对应。
- Reader 的 `fluxon_fs.master.instance_key` 必须等于目标 `FS_MASTER_INSTANCE_KEY`。
- Reader 的 `export_name` 必须等于 Writer 注册、FS Master 发布的 export 名。
- Writer 和 Reader 使用相同的远端相对路径。

本页不内嵌完整 Writer/Reader YAML；`-c` 应使用当前部署环境已经确认过的配置文件。

#### Reader 挂载目录要求

`mount_dir_abs` 必须满足：

- 使用绝对路径。
- 不能是 `/`。
- 目录不存在时可以由 Fluxon 创建。
- 目录已经存在时必须为空。
- 不能与当前进程中的其他挂载目录重叠。

挂载目录不要求放在 `/fluxon_fs/` 下，例如 `/tmp/fluxon_fs_demo/mount_demo` 也可以。

#### 启动 Writer

保持 FS 服务运行，在第二个终端执行：

```bash
python3 examples/start_fluxon_fs_writer.py \
  -c <writer-config.yaml> \
  -w <writer-workdir>
```

Writer 成功后会持续打印：

```text
[writer] op=write_remote ...
[writer] op=write_local ...
```

#### 启动 Reader

在第三个终端执行：

```bash
python3 examples/start_fluxon_fs_reader.py \
  -c <reader-config.yaml> \
  -w <reader-workdir>
```

Reader 完成挂载并找到文件后，会持续打印：

```text
[reader] op=read_remote ...
[reader] op=read_local ...
```

这表示远端 export 挂载和本地 cache 规则都已经生效。

> **当前示例限制**：`start_fluxon_fs_reader.py` 尚未调用 `set_request_identity(...)`，也不会从 YAML 读取用户名和密码。在启用了访问控制的 FS Master 上，需要在应用代码中显式设置请求身份，否则远端文件操作会因缺少认证 token 失败。

### `FluxonFsPatcher` 调用顺序

Reader 的推荐流程是：

```text
new_store(...)
→ install_patcher_from_master(...)
→ wait_cache_config_loaded()
→ set_request_identity(...)
→ mount_remote_dir(...)
→ open() / read() / write()
→ patcher.uninstall()
→ store.close()
```

关键规则：

- `FluxonFsPatcher` 依赖 `new_store(...)` 返回的 `KvClient`。
- `install_patcher_from_master(...)` 会从 FS Master 拉取 export 配置并安装 Patcher。
- `set_request_identity(username, password)` 为后续 FS 请求设置身份。
- 必须先调用 `patcher.uninstall()`，再关闭 `store`。

需要手动控制配置加载时，可以使用：

- `load_cache_config_from_master_config_file(config_path)`：读取文件中的 FS Master 实例信息，并阻塞到配置拉取成功。
- `start_cache_config_fetch_from_master_config_file(config_path)`：在后台持续从 FS Master 拉取配置；`install_patcher_from_master(...)` 已经调用了这个接口。
- `set_cache_config_yaml(...)`：不从 FS Master 拉取时，直接注入 export 和 cache 配置。

普通 Reader 优先使用 `install_patcher_from_master(...)`，不需要自行组合这些接口。

### 进阶配置

#### Panel 与访问控制

- `FS_PANEL_LISTEN_ADDR` 决定 FS Master 在本机监听的网卡和端口。
- `FS_PANEL_PUBLIC_BASE_URL` 是浏览器和页面链接使用的公开地址。跨机器访问时，应改成实际可达的主机名或 IP。
- `FS_MASTER_ACCESS_DB_PATH` 保存用户、密码和权限，应使用稳定、可写的绝对路径。
- `bootstrap_access_model` 只在 access DB 没有用户时写入首批账号。
- 数据库已有用户后，修改 `ADMIN_USERNAME` 或 `ADMIN_PASSWORD` 不会覆盖现有账号。
- `can_manage_users: true` 的管理员在运行时可以访问所有当前 export。

#### 日志

服务脚本把子进程标准输出写入：

- `WORKDIR/log/kv_master.log`
- `WORKDIR/log/owner.log`
- `WORKDIR/log/fs_master.log`
- `WORKDIR/log/fs_agent.log`

Writer 和 Reader 的 Python 日志默认输出到当前终端。需要增加日志时，可以设置：

```bash
FLUXON_LOG=DEBUG python3 examples/start_fluxon_fs_reader.py \
  -c <reader-config.yaml> \
  -w <reader-workdir>
```

#### 常见错误

- **`new_store failed`**：先检查服务脚本是否仍在运行，以及 `cluster_name`、`share_mem_path` 是否与本机 Owner 一致。
- **`unknown export_name`**：检查 Reader 的 `export_name` 是否已经由 FS Agent 注册并由 FS Master 发布。
- **挂载目录错误**：检查路径是否为绝对路径、是否为空，以及是否与已有挂载重叠。
- **`fluxon_fs cache config is not loaded yet`**：检查 FS Master 实例标识是否正确，并确认配置拉取已经完成。
- **`permission denied` / `PermissionError`**：检查是否调用了 `set_request_identity(...)`，以及账号密码和 access DB 权限。

### 可选：目录传输与预扫描

目录传输适合跨机器或跨共享存储的大目录搬迁。它会持续记录扫描进度、batch 数量、运行中的 worker 和实时带宽。

该功能依赖 TiKV `transfer_state_store`。开始前先按 [用户 - 2 - 服务平面](./用户%20-%202%20-%20服务平面.md) 启动 PD 和 TiKV。

#### 在页面发起目录传输

在双 pane 页面中：

1. 左侧定位源文件夹。
2. 右侧定位目标 export 和目录。
3. 把左侧文件夹拖到右侧。
4. 设置 `desired_worker_count` 和 `batch_ready_bytes`。
5. 提交后到 `/ui/transfers/` 查看任务。

`FluxonFS Transfer Jobs` 会显示扫描进度、batch 数量、运行中的 worker 和实时带宽。

#### 导入预扫描

`/ui/transfers/` 的 `Pre-Scans` 区域可以把已有预扫描导入为正式任务：

1. 找到预扫描记录并点击 `Import`。
2. 选择 source export 和 target export。
3. 填写 target prefix 和 `desired_worker_count`。
4. 提交后在 `FluxonFS Transfer Jobs` 中查看。

#### TiKV 命名空间

FS Master 与独立预扫描进程必须使用相同的：

- `pd_endpoints`
- `key_prefix`

本页示例统一使用：

```yaml
transfer_state_store:
  kind: tikv
  tikv:
    pd_endpoints:
      - "127.0.0.1:12379"
    key_prefix: "/fluxon_fs_transfer/demo-fs-cluster/"
```

#### 独立预扫描示例

独立预扫描只依赖 PD 和 TiKV，不要求 KV Master、Owner、FS Master 或 FS Agent 已经启动。后续 FS Master 使用相同 `pd_endpoints` 和 `key_prefix` 启动后，页面即可看到预扫描结果。

<details>
<summary><strong>📄 查看完整预扫描示例（点击展开）</strong></summary>

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
        key_prefix="/fluxon_fs_transfer/demo-fs-cluster/",
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

</details>

运行前将 `src_root_dir` 改成实际存在的源目录。`summary` 中最常用的是 `job_id`、`scan_epoch` 和 `batch_count`。
