# Fluxon Log 设计 1 - 统一 log 标准与 Greptime OTLP 导出链路

## 0. 总起
本文定义 Fluxon 服务平面的统一日志标准。主线代码落在 `fluxon_rs/fluxon_kv/src/config.rs`、`fluxon_rs/fluxon_kv/src/lib.rs`、`fluxon_rs/fluxon_util/src/log.rs`、`fluxon_rs/fluxon_observability/src/greptime_otlp_tracing.rs`、`fluxon_rs/fluxon_observability/src/greptime_otlp_log_orchestrator.rs` 和 `fluxon_rs/fluxon_observability/src/greptime_otlp_log.rs`。

稳定结论先说死：

- 本地文件日志始终启用，作为可回放的安全网。
- Greptime OTLP 导出由 `master.monitoring.otlp_log_api` 控制，`master` 负责配置源，`owner` / `external` 只消费广播。
- `testbed` 是独立的 `log_service_kind`，启动器、runner、UI 和 workload 统一按同一套日志语义落盘。
- 当前导出链路采用 best-effort 策略，不阻塞主业务路径。

本文重点回答四个问题：

1. 各条日志链路当前落在哪些目录边界里。
2. 当前 canonical 文件名、按天分片和 31 天清理语义是什么。
3. Rust / Python 之间哪些 contract 已经对齐，哪些还没有。
4. 当前实现里哪些地方已经收口，哪些地方仍是未完全收口点。

KV 里的 `external` 与 side worker 都只消费 owner 感知结果。当前稳定 contract 是：它们显式配置单一 `share_mem_path` 作为 attach owner 的共享 bundle 根目录，`mmap.file`、`shared.json` 和 peer metadata 都在运行时拼接出的 cluster-scoped 目录下；`large_file_paths` 则从 owner 发布的 `shared.json` 继承，日志和 cache 从启动起就直接落到 owner 派生出来的大文件目录。

## 1. 目录边界
目录边界只管物理隔离，不管统一 root。统一的是命名、元数据、归档窗口和清理语义。

### 1.1 KV
- `master` 以 `log_dir` 作为本地主日志根，并在其下派生 cluster-scoped runtime 日志目录。
- `owner`、`external` 和 side worker 共享单一 `share_path` 作为 share 根，用来放 `mmap.file`、`shared.json`、peer metadata 和 side transfer 相关文件。
- `owner` 的 `large_file_paths` 定义 runtime log、cache 等大文件资产的物理根目录。
- `external` 和 side worker 不再单独声明自己的 `large_file_paths`。它们在 zero-contribution bootstrap 阶段从 owner `shared.json` 继承同一组大文件根目录，然后直接复用 owner 派生出来的 runtime log / cache 边界。

### 1.2 ops / bare shared supervisor control plane
这里不要把 `ops` 和 `bare` 理解成两套彼此独立的面。两者确实共用同一个 `selection_supervisor.py + log_shard.py` 实现源，但当前实际落盘边界不是一棵完全统一的目录树。

先区分两个层次：

| 层次 | 稳定根 | 主要内容 |
| --- | --- | --- |
| `deployconf -> gen_bare -> bare bootstrap` | `hostworkdir` | generated control scripts、bare 服务日志 |
| `ops` runtime | `workdir` | runtime config、embedded supervisor runtime、ops-managed workload 日志 |

其中：

- `hostworkdir` 是节点级宿主根，用来承载 deployer 下发产物、bare 控制脚本和其他需要跨进程稳定复用的目录。
- `workdir` 是某个具体进程实例自己的运行子目录，用来承载该实例的 runtime config、embedded supervisor runtime 和它托管出来的 workload 日志。
- 位置关系上，当前 self-host deployconf 里 `workdir` 通常是 `hostworkdir` 的子目录；语义关系上，`workdir` 仍然只是“某个实例的运行子树”，不能反过来代表整个 `hostworkdir`。

bare 稳定根当前可以直观看成：

```text
${HOSTWORKDIR}/
  log/
    ops_controller.<YYYY-MM-DD>.log
    ops_agent.<YYYY-MM-DD>.log
    <bare_service_name>.<YYYY-MM-DD>.log
  gen_bare_deploy_bash/
    start_ops_controller.sh
    start_ops_agent.sh
    start_<service>.sh
    stop_ops_controller.sh
    stop_ops_agent.sh
    stop_<service>.sh
    start_<atomic_group>.sh
    stop_<atomic_group>.sh
    selection_supervisor.py
    log_shard.py
    entrypoint__<workload_name>.sh
```

当前 self-host deployconf 下，`hostworkdir` 与 `ops workdir` 的实际位置关系可以直观看成：

```text
${HOSTWORKDIR}/
  gen_bare_deploy_bash/
    ...
  log/
    ops_controller.<YYYY-MM-DD>.log
    ops_agent.<YYYY-MM-DD>.log
    <bare_service_name>.<YYYY-MM-DD>.log
  ops_controller/
    ops_controller.yaml
    selection_supervisor/
      selection_supervisor.py
      log_shard.py
    log/
      workload__<workload_kind>__<workload_name>.<YYYY-MM-DD>.log
  ops_agent/
    <NODE_ID>/
      ops_agent.yaml
      selection_supervisor/
        selection_supervisor.py
        log_shard.py
      log/
        workload__<workload_kind>__<workload_name>.<YYYY-MM-DD>.log
```

这里再把 contract 说清楚：

- `${HOSTWORKDIR}/gen_bare_deploy_bash/` 里的 `start_*.sh` / `stop_*.sh` 是 generated control scripts，是这套 shared supervisor 控制面的入口脚本，不是另一套独立 authority。
- bare 这一层的稳定逻辑基名仍然是 `${HOSTWORKDIR}/log/<service_name>.log`，shared supervisor runtime 再把它收口为 `${HOSTWORKDIR}/log/<service_name>.<YYYY-MM-DD>.log`。
- ops-managed workload 这一层的稳定逻辑基名则是 `${WORKDIR}/log/workload__<workload_kind>__<workload_name>.log`，shared supervisor runtime 再把它收口为 `${WORKDIR}/log/workload__<workload_kind>__<workload_name>.<YYYY-MM-DD>.log`。
- 两层真正共享的是 `selection_supervisor.py + log_shard.py` 这组控制与滚动实现，不是“所有路径和文件名完全一样”。

在当前 self-host deployconf 示例里：

- `ops_controller` 的 workdir 是 `${HOSTWORKDIR}/ops_controller`
- `ops_agent` 的 workdir 是 `${HOSTWORKDIR}/ops_agent/${NODE_ID}`

### 1.3 testbed
- `workdir`、`run_dir` 分别承担 launcher、runner、UI、workload 的 run-scoped 落盘边界。
- `testbed` 必须显式作为 `log_service_kind` 出现，不再用泛化名称代替。
- launcher 和 workload 的目录语义要和 ops 对齐。
- 当前优先级不是先把 testbed 做到完美支持，而是先把 ops 长时服务日志 contract 讲清楚并收口；testbed 继续按“服务级日志”和“case artifact”分开讨论。

### 1.4 FS
- `share_mem_path` 与 `export.remote_root_dir_abs` 分开使用。
- 前者负责 KV attachment 所需的共享 bundle 边界。
- 后者负责 FS 业务数据边界。

这里的目标很明确：目录可以不同，语义必须一致。`log`、`cache`、`shared attachment`、`workload data` 不能混在同一个边界里。

## 2. 文件命名
当前实现里的文件命名还没有完全统一，但已经可以明确分成下面几类。

| 类别 | 当前逻辑基名 | 当前实际落盘 |
| --- | --- | --- |
| KV runtime | `fluxon-kv-<instance_key>.log` | `fluxon-kv-<instance_key>.<YYYY-MM-DD>.log` |
| bare 服务日志 | `<service_name>.log` | `<service_name>.<YYYY-MM-DD>.log` |
| ops-managed workload | `workload__<workload_kind>__<workload_name>.log` | `workload__<workload_kind>__<workload_name>.<YYYY-MM-DD>.log` |
| testbed 服务日志 | `test_runner.log` / `test_runner_ui.log` | `test_runner.<YYYY-MM-DD>.log` / `test_runner_ui.<YYYY-MM-DD>.log` |
| KV side worker stdio | `side_worker_<worker_idx>.stdout.log` / `side_worker_<worker_idx>.stderr.log` | 当前还没补日期分片 |

补充说明：

- KV runtime 日志当前仍由 `fluxon_util::init_log(...)` 创建，`run_master_impl(...)` 和 `run_client_impl(...)` 都会初始化这套本地文件日志，所以 `master`、`owner`、`external` 这些 KV 运行时进程当前确实都会产生这类文件。
- `ops` 里还保留一些特例命名，例如 `smoke.log`、`smoke_bare.log`、`smoke_workloads_bare.log`。这些都属于当前实现尚未收口的历史命名。
- `testbed` 当前仍然没有单一 canonical log filename。服务级日志已经补上时间分片，但 `ci_runner` 等 case 级日志仍主要落在 `results/<case_id>/run_<N>/logs/**` 与 `summary.yaml`、`exception.txt`、`ci.log` 这类 run artifact 里。

清理只依据文件名里约定好的日期分片字段，不按目录数量、文件大小或历史批次做判断。这样本地清理和 Greptime retention 才能共享同一时间窗口。

## 3. 元数据字段
这一节描述的是当前 KV OTLP 导出链路已经实际写入 Greptime 的元数据字段。

| 字段 | 含义 |
| --- | --- |
| `service.name` | 当前固定为 `fluxon` |
| `fluxon_cluster_name` | 集群名 |
| `fluxon_member_kind` | 当前业务类型标签，例如 `kv` |
| `fluxon_role` | 当前进程角色标签，例如 `master`、`owner_client`、`external_client` |
| `fluxon_member_id` | 当前实例标识 |

当前实现里的日志元数据仍然是围绕 `cluster_name`、`member_kind`、`role`、`member_id` 这组字段组织的；`log_service_kind`、`log_kind`、`process_role`、`instance_key`、`workload_kind`、`workload_name` 这些更细的统一字段，目前还没有完整进入导出链路。

## 4. 归档、超时与清理
本地文件日志按天滚动归档，默认保留 31 天。清理时只扫描 canonical log file name，并按命名约定提取日期分片删除过期文件，不按文件数量或目录总量触发。

流式备份和 OTLP 导出也服从同一套窗口：

| 项目 | 规则 |
| --- | --- |
| 导出策略 | best-effort，不阻塞主业务路径 |
| 队列满 | 允许丢弃，并保留可观测信号 |
| 发送失败 | 允许跳过当前 batch，本地文件仍在 |
| 停机行为 | shutdown 时执行 best-effort flush |
| 超时语义 | 单次导出必须有硬上界，不能无限挂起 |

Greptime 侧的 retention / TTL 也按同一日期窗口收口，保证本地与远端的保留语义一致。这里要把远端清理语义说死：写入 `fluxon_logs` 的日志记录默认只保留 1 个月，超过窗口的数据必须由 Greptime 表级 TTL 或定时清理任务删除，不能只依赖查询层按时间过滤“看不见旧数据”。

如果后续本地窗口仍保持 31 天，那么 Greptime 侧也应保持同一 31 天窗口；如果本地窗口改为新的 canonical 值，远端 TTL 也必须同步调整。`disable_observability=true` 只关闭 OTLP 层，不关闭本地文件日志。

如果某条 stream 只是“备份副本”，它不能绕开本地日志的归档窗口单独永久存活。超时后应停止 tailing、释放资源，并交回本地文件归档策略处理历史文件。

## 5. 当前实现里已经收口的点
这一节只写已经可以当作当前事实使用的内容。

### 5.1 本地文件按天分片与 31 天窗口
- KV runtime 已具备稳定的按天滚动与保留窗口。
- bare 服务日志已经接到 shared supervisor 的按天分片与同口径清理。
- ops-managed workload 日志已经接到 shared supervisor 的按天分片与同口径清理。
- `test_runner` / `test_runner_ui` 这类 testbed 服务级日志已补齐按天分片与本地 31 天保留窗口。

### 5.2 shared supervisor 已经统一到一个实现源
- bare bootstrap 与 ops-managed workload 现在都复用 `selection_supervisor.py + log_shard.py` 这组实现。
- `gen_bare_deploy_bash.py` 会把同一个 `log_shard.py` helper 下发到生成目录。
- bare 启动脚本层保留的是稳定逻辑基名，真正的 stdio 重定向和实际分片写入都在共享 `selection_supervisor.py` 运行时里生效。

### 5.3 Rust / Python 已经有三类明确对齐
- 按天分片与 31 天清理
- 日志目录派生规则
- OTLP 基础字段与 Greptime header

## 6. 当前还没有完全收口的点
这一节只写未完全收口点，避免把“当前事实”和“目标态”混在一起。

### 6.1 KV 共享 bundle 已收口到单一 `share_mem_path`
- 当前 KV public contract 只保留 `share_mem_path`。
- 运行时在 `share_mem_path` 下拼接 `cluster_name`，统一承载 `mmap.file`、`shared.json`、peer metadata 和 side transfer metadata。

### 6.2 side worker stdio 仍未收口到统一按天分片
- zero-contribution bootstrap 已经在启动前继承 owner 的 `large_file_paths`，因此 KV runtime logger 不再依赖 attach 后热切换文件路径。
- 但 side worker stdio 当前仍然直接写 `side_worker_<worker_idx>.stdout.log` / `side_worker_<worker_idx>.stderr.log`，还没有补到统一的按天分片命名。

### 6.3 side worker stdio 与历史 `smoke` 文件还没纳入这轮收口
- side worker stdio 当前仍是 `side_worker_<worker_idx>.stdout.log` / `side_worker_<worker_idx>.stderr.log`。
- `smoke.log`、`smoke_bare.log`、`smoke_workloads_bare.log` 一类历史命名仍然存在。

### 6.4 testbed 只有服务级日志收口到了同类语义
- `test_runner`、`test_runner_ui` 已改为“稳定逻辑基名 + 按天分片落盘”。
- case 级 `run_dir/logs/**`、`summary.yaml`、`resolved_case.yaml`、`benchmark_result.json` 等仍按 run artifact 生命周期消费。
- `history_lookback_days` 仍只是控制 UI 回看哪些 workdir；`gitops retention.max_age_days` 仍然清理 gitops run 目录，不是 testbed 服务日志文件的统一 TTL。

### 6.5 OTLP 统一字段和统一状态机还没有全部收口
- 当前导出链路仍以 `cluster_name`、`member_kind`、`role`、`member_id` 为主。
- `log_service_kind`、`log_kind`、`process_role`、`instance_key`、`workload_kind`、`workload_name` 这组更细的 canonical 字段还没有完整进入导出链路。
- Rust 通用链路已经把 `disabled`、`direct`、`proxy`、失败分支显式枚举出来；Python benchmark exporter 仍是直连特化路径，还没有进入同一套通用发送状态机。

## 7. rs / py 模块对齐与防漂移
稳定结论先说死：

- 共享 log contract 以 Rust canonical 模块为准，Python 优先复用 Rust 已经导出的结果。
- 当前已经能从代码直接看出三类对齐：按天分片与 31 天清理、日志目录派生、OTLP 基础字段与 header。
- 当前还没有完全收口的是通用 OTLP 发送状态机。Rust 已经显式枚举发送分支，Python 侧 benchmark exporter 仍是直连特化路径。

### 7.1 按天分片与本地保留窗口
Rust `fluxon_rs/fluxon_util/src/log.rs`：

```rust
const LOG_RETENTION_DAYS: usize = 31;

pub fn current_daily_sharded_log_path(base_path: &Path) -> anyhow::Result<PathBuf> {
    daily_sharded_log_path(base_path, current_shard_date()?)
}

fn cleanup_old_daily_sharded_logs(base_path: &Path, retention_days: usize) -> anyhow::Result<()> {
    let keep_since = current_shard_date()? - chrono::Days::new(retention_days.saturating_sub(1) as u64);
    ...
    if shard_date < keep_since {
        fs::remove_file(&path)?;
    }
}

impl DailyShardedFileWriter {
    fn rotate_if_needed(&self, state: &mut DailyShardedFileWriterState) -> io::Result<()> {
        let next_path = self.current_path()?;
        cleanup_old_daily_sharded_logs(&self.base_path, self.retention_days)?;
        let file = fs::OpenOptions::new().create(true).append(true).open(&next_path)?;
        state.current_path = Some(next_path);
        state.current_file = Some(file);
        Ok(())
    }
}
```

Python `deployment/utils/log_shard.py`：

```python
DEFAULT_DAILY_LOG_RETENTION_DAYS = 31

def daily_sharded_log_path(base_path: Path, *, now: Optional[datetime.datetime] = None) -> Path:
    shard_date = _resolve_shard_date(ts)
    return (base_path.parent / f"{stem}.{shard_date.isoformat()}.log").resolve()

def cleanup_old_daily_sharded_logs(base_path: Path, *, retention_days: int = DEFAULT_DAILY_LOG_RETENTION_DAYS) -> None:
    current_shard_date = _resolve_shard_date(datetime.datetime.now(datetime.timezone.utc))
    keep_since = current_shard_date - datetime.timedelta(
        days=max(int(retention_days) - 1, 0)
    )
```

这两段现在对齐的是同一个显式 contract：逻辑基名保持不变，日期字段统一落在 `.<YYYY-MM-DD>.log`，默认本地窗口都是 31 天，而且过期删除都显式按日期分片判断。这里不要机械要求两边 helper 名称完全一样；对齐的是“按天分片 + 31 天窗口 + 同口径清理”这条 contract。

### 7.2 KV 主日志是 Rust；Python 侧要分 bare 服务日志和 ops-managed workload 日志两层
先把边界说死：KV runtime 主日志当前基本都是 Rust 在输出。`master`、`owner`、`external` 这些 KV 进程走的是 `fluxon_util::init_log(...)` 这条链。Python 一侧真正需要单独检查的，当前已经分成两层：

- `deployconf -> gen_bare -> bare bootstrap` 这一层，负责 `ops_controller`、`ops_agent` 和其他 bare service 自身的 stdout/stderr。
- `ops_agent` 进入 desired-runtime 管理之后，再去托管 workload；这一层的日志 contract 不再沿用 bare `${service_name}.log`，而是 `workload__<kind>__<name>.log`。

先看 bare 这一层：

Python `deployment/gen_bare_deploy_bash.py`：

```python
from log_shard import render_module_source as render_log_shard_module_source

(outdir / LOG_SHARD_HELPER_FILENAME).write_text(
    render_log_shard_module_source(),
    encoding="utf-8",
)
```

```python
runtime_state_json = _bare_runtime_state_json(
    workload_name=workload_name,
    authority_name=...,
    service_name=service_name,
    log_path=f"${{HOSTWORKDIR}}/log/{service_name}.log",
)

LOG_DIR="$HOSTWORKDIR/log"
LOGFILE="$LOG_DIR/${SERVICE}.log"
...
SUPERVISOR_PID=$( ... < /dev/null & echo "$!" )
```

Python `deployment/utils/selection_supervisor_codegen.py`：

```python
def _redirect_process_stdio_to_runtime_log(runtime_state: Optional[SelectionRuntimeState]) -> None:
    base_log_path = _require_non_empty_str(runtime_state.log_path, "state.log_path")

    def _router_loop() -> None:
        _LOG_SHARD.relay_fd_to_daily_sharded_logs(
            base_log_path=base_log_path,
            read_fd=read_fd,
            retention_days=_LOG_SHARD.DEFAULT_DAILY_LOG_RETENTION_DAYS,
        )

    os.dup2(write_fd, sys.stdout.fileno())
    os.dup2(write_fd, sys.stderr.fileno())

...

_redirect_process_stdio_to_runtime_log(runtime_state)
```

再看 ops-managed workload 这一层：

Rust `fluxon_rs/fluxon_ops/src/lib.rs`：

```rust
fn workload_log_filename(kind: WorkloadKind, name: &str) -> anyhow::Result<String> {
    Ok(format!("workload__{}__{}.log", kind.as_str(), name))
}

let runtime_dir = workdir.join(OPS_SELECTION_SUPERVISOR_DIR_NAME);
let log_dir = workdir.join(OPS_LOG_DIR_NAME);
let log_path = self.log_dir.join(log_filename);
```

这组代码说明当前现状是：

- bare bootstrap 与 ops-managed workload 确实已经复用了同一个 `selection_supervisor.py + log_shard.py` 实现源。
- bare 服务日志与 ops-managed workload 日志也都已经真正接到这套滚动管理 helper 上。
- 但两层当前并不是同一个 path contract：
  - bare 服务日志保留的是 `${HOSTWORKDIR}/log/${service_name}.log`
  - ops-managed workload 保留的是 `${WORKDIR}/log/workload__<workload_kind>__<workload_name>.log`

### 7.3 OTLP 基础字段与 header 已经同名对齐
Rust `fluxon_rs/fluxon_observability/src/greptime_otlp_log.rs`：

```rust
let kvs = vec![
    KeyValue { key: KEY_CLUSTER_NAME.to_string(), value: Some(...) },
    KeyValue { key: KEY_MEMBER_KIND.to_string(), value: Some(...) },
    KeyValue { key: KEY_ROLE.to_string(), value: Some(...) },
    KeyValue { key: KEY_MEMBER_ID.to_string(), value: Some(...) },
];

let mut reqb = self
    .http
    .post(&self.endpoint)
    .header("X-Greptime-DB-Name", &self.db_name)
    .header("X-Greptime-Log-Extract-Keys", GREPTIME_LOG_EXTRACT_KEYS_HEADER_VALUE);
```

Python `fluxon_test_stack/distributed_benchmark_node.py`：

```python
log_attrs: Dict[str, Any] = {
    "fluxon_cluster_name": self._cfg.cluster_name,
    "fluxon_member_kind": self._cfg.member_kind,
    "fluxon_role": self._cfg.role,
    "fluxon_member_id": self._cfg.member_id,
}

headers = {
    "Content-Type": "application/x-protobuf",
    "X-Greptime-DB-Name": self._cfg.db_name,
    "X-Greptime-Log-Extract-Keys": ",".join(extract_keys),
}
```

这两边已经对齐到同一个最小公共集合：`fluxon_cluster_name`、`fluxon_member_kind`、`fluxon_role`、`fluxon_member_id` 这组基础属性同名同义，Greptime header 也保持同一协议面。Python benchmark exporter 可以补 phase summary 字段，但不能改写这组基础字段的含义。

### 7.4 发送状态机还没有完全收口
Rust `fluxon_rs/fluxon_observability/src/greptime_otlp_log_orchestrator.rs`：

```rust
pub enum GreptimeOtlpLogAttemptResult<N> {
    Disabled,
    Sent { path: GreptimeOtlpLogSendPath, proxy_node: Option<N> },
    SkippedNoProxy { detail: String },
    ProxyFailed { proxy_node: N, detail: String },
}
```

Python `fluxon_test_stack/distributed_benchmark_node.py`：

```python
with urllib.request.urlopen(req, timeout=GREPTIME_OTLP_LOG_TIMEOUT_SECONDS) as resp:
    status = getattr(resp, "status", 200)
    if int(status) < 200 or int(status) >= 300:
        body_text = resp.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"greptime otlp http {status}: {body_text}")
```

这组对照反映的是当前边界：Rust 通用链路已经把 `disabled`、`direct`、`proxy`、失败分支显式枚举出来；Python 这里只是 benchmark phase summary 的直连特化路径，还没有进入同一套通用发送状态机。后续如果 Python 需要承担通用 service-plane 导出，应该复用 Rust 这组有限分支，而不是再发明一套平行状态模型。

### 7.5 防止未来漂移
只保留四条工程规则：

1. 共享 contract 只保留一个真相源。目录派生、canonical 字段、发送状态、TTL 这类会跨语言消费的语义，优先由 Rust 定义，Python 复用导出结果或逐项镜像实现。
2. 任何改动如果影响 canonical 文件名、OTLP 字段、Greptime header、发送分支或 retention，必须同一个 PR 同时更新 Rust 代码、Python 代码、设计文档和至少一层 contract test。
3. Python 特化路径必须显式标出作用域。`test_runner` 服务日志和 benchmark phase summary 可以保留自己的实现，但不能反向成为公共 contract 的定义源。
4. 多语言边界坚持一个概念一个名字。不要在 rs / py 两边分别引入近义字段、别名参数或平行配置面，否则文档、查询、清理和告警都会漂移。
