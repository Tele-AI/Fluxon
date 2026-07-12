use super::lease_backend_handle::LeaseBackendHandle;
use super::lease_backend_uid::LeaseBackendUid;
use super::lease_handle::{LeaseEntry, LeaseEntryKind};
use super::lifecycle::OnKeepalive;
use crate::auto_clean_map::{AutoCleanMap, AutoCleanMapEntry};
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::debug;

/// Per-lease keepalive timeout budget for a single task.
///
/// 设计说明（与 review_mq_lease_manager_tuning.md 收敛）：
/// - 这里的 timeout 目标是“防止单个 keepalive 调用拖慢整个 TTL
///   bucket 的 tick”，而不是“保证在 TTL 过期前一定完成 keepalive”。
///   真正的 TTL 守恒由 etcd / kvclient 自身的 lease 语义保障。
/// - etcd / kvclient keepalive 的正常延迟通常在几十到几百毫秒量级；
///   1.5s 远高于这一 SLA 上界，但又远小于常见 TTL（例如 20s）的 1/3，
///   因此既能避免长尾调用阻塞，也不会因为 budget 过小导致频繁误杀。
/// - 早期实现曾尝试按 `ttl_seconds` 动态放大 timeout（约等于 tick
///   周期），在大 TTL 场景下会导致“单个长尾调用阻塞整轮 join”的风险。
///   目前版本选择固定 1.5s 作为全局上限，更符合“keepalive 是后台心跳”
///   的定位；如需按 TTL 微调，可在未来在不改变语义的前提下再演进。
pub(crate) const KEEPALIVE_PER_TASK_BUDGET_MS: u64 = 1500;

/// Per-lease error log rate limit period for keepalive failures.
///
/// 设计说明：
/// - 单个 lease 在底层 etcd/kvclient 出现持续错误时，如果每次 tick
///   都直接按错误路径完整打日志，很容易在高 QPS 或大量 lease 场景下
///   把错误日志刷爆，并放大下游异常信号。
/// - keepalive actor 的职责是“汇聚并驱动后台心跳”，这里的错误日志只
///   需要起到“该 lease 正处于异常态”的告警作用，而不需要在每一次
///   重试上都打印完整堆栈。
/// - 因此这里按 lease 维度做一个简单的限频：同一个 lease 在该时间窗
///   内的 keepalive 错误（包括超时、etcd stream 异常以及回调返回 Err）
///   只允许打一条错误/告警日志，避免重复噪音，占用过多 I/O。
const KEEPALIVE_ERROR_LOG_PERIOD_SECS: u64 = 30;
const KEEPALIVE_ERROR_LOG_SKIP_FIRST: bool = false;
const KEEPALIVE_ERROR_LOG_KEY_PREFIX: &str = "lease_keepalive_error:";

/// Helper: rate-limit a keepalive-related log for a given lease.
///
/// - key 按 lease 维度聚合（`KEEPALIVE_ERROR_LOG_KEY_PREFIX + lease_id`）；
/// - 具体 log 内容与级别由闭包内部的 `tracing::warn!/error!` 决定；
/// - 只封装限频逻辑，不改变调用方的控制流和错误分类。
fn log_keepalive_error_rate_limited<I, F>(lease_id: I, log: F)
where
    I: std::fmt::Display,
    F: FnOnce(),
{
    let key = format!("{}{}", KEEPALIVE_ERROR_LOG_KEY_PREFIX, lease_id);
    if crate::limitrate::allow(
        &key,
        Duration::from_secs(KEEPALIVE_ERROR_LOG_PERIOD_SECS),
        KEEPALIVE_ERROR_LOG_SKIP_FIRST,
    ) {
        log();
    }
}

// OnKeepalive alias moved to get_or_init.rs

// debug helpers moved to get_or_init.rs

// Cleanup is now handled by LeaseEntry::drop (via AutoCleanMapEntry RAII)

// ---------- OneTtlKeepAliveActor & registry ----------

pub(crate) struct EtcdState {
    pub(crate) client: Client,
    pub(crate) lease_id: i64,
    pub(crate) keeper: Option<LeaseKeeper>,
    pub(crate) stream: Option<LeaseKeepAliveStream>,
    pub(crate) last_stage: &'static str,
}

impl EtcdState {
    pub(crate) fn reset_stream(&mut self) {
        self.keeper = None;
        self.stream = None;
    }

    pub(crate) fn last_stage(&self) -> &'static str {
        self.last_stage
    }

    async fn ensure_stream(&mut self) -> anyhow::Result<()> {
        if self.keeper.is_some() && self.stream.is_some() {
            return Ok(());
        }
        let lease_id = self.lease_id;
        let mut last_err: Option<String> = None;
        for attempts in 0..10 {
            self.last_stage = "ensure_stream.open_stream";
            match self.client.lease_keep_alive(lease_id).await {
                Ok((keeper, stream)) => {
                    debug!(
                        "renewed keepalive stream for lease_id={} attempts={}",
                        lease_id, attempts
                    );
                    self.keeper = Some(keeper);
                    self.stream = Some(stream);
                    return Ok(());
                }
                Err(e) => {
                    let e_dbg = format!("{:?}", e);
                    last_err = Some(e_dbg.clone());
                    // 限频打印 stream 打开失败，避免在持续故障时放大日志噪音。
                    log_keepalive_error_rate_limited(lease_id, || {
                        tracing::warn!(
                            "failed to open keepalive stream for lease_id={} (attempt {}): {:?}",
                            lease_id,
                            attempts,
                            e_dbg
                        );
                    });
                }
            }
        }
        self.reset_stream();
        Err(anyhow::anyhow!(
            "failed to open keepalive stream for lease_id={} after 10 attempts, last_err={:?}",
            lease_id,
            last_err
        ))
    }

    pub(crate) async fn keepalive_once(&mut self) -> anyhow::Result<()> {
        let lease_id = self.lease_id;
        self.last_stage = "ensure_stream";
        self.ensure_stream().await?;

        // Hard error: etcd reported the lease is already expired (ttl<=0). Re-opening
        // the keepalive stream cannot recover an expired lease; callers must treat
        // this as a lost lease and rebuild state with a new lease.
        let mut hard_err: Option<anyhow::Error> = None;
        let mut need_reopen = false;
        if let (Some(keeper), Some(stream)) = (self.keeper.as_mut(), self.stream.as_mut()) {
            self.last_stage = "keep_alive.request";
            let ok = match keeper.keep_alive().await {
                Ok(()) => {
                    self.last_stage = "keep_alive.response";
                    match stream.message().await {
                        Ok(Some(resp)) => {
                            if resp.id() == lease_id {
                                let ttl = resp.ttl();
                                debug!(
                                    "lease keepalive response for lease_id={} ttl={}",
                                    lease_id, ttl
                                );
                                if ttl <= 0 {
                                    log_keepalive_error_rate_limited(lease_id, || {
                                        tracing::error!(
                                            lease_id,
                                            ttl,
                                            "etcd keepalive returned non-positive ttl; lease is expired"
                                        );
                                    });
                                    hard_err = Some(anyhow::anyhow!(
                                        "etcd keepalive returned ttl={} (expired) for lease_id={}",
                                        ttl,
                                        lease_id
                                    ));
                                    false
                                } else {
                                    true
                                }
                            } else {
                                log_keepalive_error_rate_limited(lease_id, || {
                                    tracing::error!(
                                        "lease keepalive id mismatch: expected {} got {}",
                                        lease_id,
                                        resp.id()
                                    );
                                });
                                false
                            }
                        }
                        Ok(None) => {
                            log_keepalive_error_rate_limited(lease_id, || {
                                tracing::warn!(
                                    "lease keepalive stream closed for lease_id={}",
                                    lease_id
                                );
                            });
                            false
                        }
                        Err(err) => {
                            log_keepalive_error_rate_limited(lease_id, || {
                                tracing::error!(
                                    "lease keepalive stream error for lease_id={}: {:?}",
                                    lease_id,
                                    err
                                );
                            });
                            false
                        }
                    }
                }
                Err(err) => {
                    log_keepalive_error_rate_limited(lease_id, || {
                        tracing::error!(
                            "lease keepalive error for lease_id={}: {:?}",
                            lease_id,
                            err
                        );
                    });
                    false
                }
            };
            if !ok {
                need_reopen = true;
            }
        } else {
            need_reopen = true;
        }

        if let Some(err) = hard_err {
            // NOTE: Do not clear/destroy the keepalive stream here.
            //
            // This error path can be hit during normal shutdown (e.g. the lease was
            // already expired), and dropping the stream/keeper while the runtime is
            // tearing down has caused SIGABRT in practice. We still surface the
            // error to the caller; the actor loop will rate-limit logs.
            return Err(err);
        }

        if need_reopen {
            // Failure observed; try to reopen the stream. Only return error if restart fails.
            match self.ensure_stream().await {
                Ok(()) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(
                    "etcd keepalive not ok and restart failed for lease_id={}: {}",
                    lease_id,
                    e
                )),
            }
        } else {
            Ok(())
        }
    }
}

// LeaseEntryKind/LeaseEntry moved to lease_handle.rs

// ---------- lease key ----------

/// Composite key for a single lease entry in the TTL actor.
///
/// 使用 `backend_uid + lease_id` 作为组合键，避免不同 backend
/// 之间的 lease id 冲突。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseKey {
    backend_uid: LeaseBackendUid,
    lease_id: u64,
}

impl LeaseKey {
    pub(crate) fn new(backend_uid: LeaseBackendUid, lease_id: u64) -> Self {
        Self {
            backend_uid,
            lease_id,
        }
    }
    pub(crate) fn lease_id(&self) -> u64 {
        self.lease_id
    }
    pub(crate) fn backend_uid(&self) -> &LeaseBackendUid {
        &self.backend_uid
    }
}

// Drop for LeaseEntry is implemented in lifecycle.rs to keep lifecycle-related
// cleanups colocated.

pub(crate) struct OneTtlKeepAliveInner {
    pub(crate) ttl_seconds: i64,
    pub(crate) registry: AutoCleanMap<LeaseKey, LeaseEntry>,
    // Whether a keepalive loop is currently running for this inner.
    pub(crate) running_state: Mutex<bool>,
}

impl OneTtlKeepAliveInner {}

fn new_keepalive_ticker(period: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(Instant::now() + period, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker
}

// Spawn the actor loop for a given inner. The loop exits when:
// - `inner.stop` is true; or
// - the registry is observed empty on a tick.
fn spawn_loop(rt: &tokio::runtime::Handle, inner: Arc<OneTtlKeepAliveInner>) {
    // Clone the runtime handle into the loop so we can spawn per-lease tasks.
    let rth = rt.clone();
    rt.spawn(async move {
        // Mark running at loop start under mutex.
        {
            let mut running = inner.running_state.lock().await;
            *running = true;
        }
        let ttl = inner.ttl_seconds;
        let mut period = {
            let secs = (ttl / 3).max(0) + 1;
            Duration::from_secs(secs as u64)
        };
        if period.as_millis() == 0 {
            period = Duration::from_secs(1);
        }
        // Registration already validates an existing lease or follows a fresh grant.
        // Start at the normal TTL cadence so a large batch does not open every
        // keepalive stream twice immediately after registration.
        let mut ticker = new_keepalive_ticker(period);
        let mut retry_after_failure = false;

        loop {
            if retry_after_failure {
                // A short retry delay keeps failed leases responsive without turning
                // a backend outage into a tight loop.
                tokio::time::sleep(Duration::from_millis(100)).await;
            } else {
                ticker.tick().await;
            }

            type SnapItem = (LeaseKey, LeaseBackendHandle);

            // snapshot leases; if empty, exit
            let snapshot: Vec<SnapItem> = if inner.registry.is_empty() {
                Vec::new()
            } else {
                let mut items = Vec::new();
                // KvClient entries
                items.extend(inner.registry.snapshot_filter_map(
                    |_, v| matches!(v.kind, LeaseEntryKind::KvClient { .. }),
                    |key, v| {
                        if let LeaseEntryKind::KvClient { handle, .. } = &v.kind {
                            (key.clone(), handle.clone())
                        } else {
                            unreachable!("filtered to KvClient")
                        }
                    },
                ));
                // Etcd entries
                items.extend(inner.registry.snapshot_filter_map(
                    |_, v| matches!(v.kind, LeaseEntryKind::Etcd { .. }),
                    |key, v| {
                        if let LeaseEntryKind::Etcd { handle, .. } = &v.kind {
                            (key.clone(), handle.clone())
                        } else {
                            unreachable!("filtered to Etcd")
                        }
                    },
                ));
                items
            };

            if snapshot.is_empty() {
                // Attempt to exit under mutex; if a new registration slipped in,
                // keep running instead of exiting.
                let mut running = inner.running_state.lock().await;
                if inner.registry.is_empty() {
                    *running = false;
                    break;
                }
                // else: a new lease arrived while waiting for the lock; continue.
                drop(running);
            }

            // Drive keepalive concurrently: spawn one task per lease and join them.
            // Any join failure is unexpected and should be treated as Unreachable.
            //
            // IMPORTANT: bound the wait time for each keepalive task to avoid
            // head-of-line blocking in this TTL bucket. If a task exceeds the
            // budget, abort its join handle (the underlying work may still
            // complete) and proceed to the next; the next tick will retry.
            let mut joins: Vec<(u64, tokio::task::JoinHandle<(u64, anyhow::Result<()>)>)> =
                Vec::with_capacity(snapshot.len());
            for (key, handle) in snapshot.into_iter() {
                let lease_id = key.lease_id();
                let h = handle.clone();
                joins.push((
                    lease_id,
                    rth.spawn(async move {
                        let r = h.keepalive(lease_id).await;
                        (lease_id, r)
                    }),
                ));
            }
            // Per-task timeout budget is capped by a fixed SLA-oriented upper
            // bound instead of scaling linearly with `ttl_seconds`. See
            // KEEPALIVE_PER_TASK_BUDGET_MS for rationale.
            let per_task_budget = Duration::from_millis(KEEPALIVE_PER_TASK_BUDGET_MS);
            let mut exist_fail = false;
            for (lease_id, jh) in joins {
                // Pin the join handle and race it with a sleep to allow abort on timeout
                let j = jh;
                tokio::pin!(j);
                let t = tokio::time::sleep(per_task_budget);
                tokio::pin!(t);
                tokio::select! {
                    _ = &mut t => {
                        j.abort();
                        log_keepalive_error_rate_limited(lease_id, || {
                            tracing::error!(
                                lease_id,
                                timeout_secs = ?per_task_budget.as_secs(),
                                "keepalive task timed out; aborted join and will retry next tick",
                            );
                        });
                        exist_fail = true;
                    }
                    res = &mut j => {
                        match res {
                            Ok((_lease_id, Ok(()))) => {}
                            Ok((lid, Err(err))) => {
                                // Task ran to completion but reported error: classify as Unreachable and continue.
                                exist_fail = true;
                                log_keepalive_error_rate_limited(lid, || {
                                    tracing::error!(
                                        lease_id = lid,
                                        error = %format!("{:?}", err),
                                        "Unreachable: keepalive task returned error"
                                    );
                                });
                            }
                            Err(join_err) => {
                                // JoinError indicates the task panicked or was cancelled; this should never happen.
                                exist_fail = true;
                                log_keepalive_error_rate_limited(lease_id, || {
                                    tracing::error!(
                                        error = %format!("{:?}", join_err),
                                        "Unreachable: keepalive task join failed"
                                    );
                                });
                            }
                        }
                    }
                }
            }

            retry_after_failure = exist_fail;
        }
    });
}

/// Ensure a loop is running for the given inner; spawn if not. Uses mutex to serialize start/stop.
pub(crate) async fn ensure_inner_running(
    rt: tokio::runtime::Handle,
    inner: Arc<OneTtlKeepAliveInner>,
) {
    let mut running = inner.running_state.lock().await;
    if !*running {
        *running = true;
        drop(running);
        spawn_loop(&rt, inner);
    }
}

// ---------- actor registry per ttl (Weak map) ----------

// moved to get_or_init.rs

// ---------- backend registry / guards ----------

// unified backend object table now lives in lease_backend_handle.rs

// No global kvclient closure registries here. KvClient callbacks live
// inside `LeaseBackendUid::KvClientWithCallbacks` and are cloned by
// the lease manager or provided during backend acquire.

// ---------- actor register / unregister (KvClient & Etcd) ----------
#[allow(clippy::large_enum_variant)]
pub enum ActorRegisterInvocation {
    KvClient {
        cb: OnKeepalive,
        label: Option<String>,
    },
    Etcd {
        client: Client,
    },
}

// register_entry moved to get_or_init.rs

pub fn actor_register_lease(
    backend_uid: LeaseBackendUid,
    lease_id: u64,
    ttl_seconds: i64,
    inv: ActorRegisterInvocation,
    rt: tokio::runtime::Handle,
) -> AutoCleanMapEntry<LeaseKey, LeaseEntry> {
    let rth = rt.clone();
    super::lifecycle::actor_get_or_spawn_and_register(
        ttl_seconds,
        LeaseKey::new(backend_uid, lease_id),
        &inv,
        move |inner| {
            // spawn the loop for a newly created inner
            spawn_loop(&rth, inner);
        },
        rt,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn periodic_keepalive_does_not_tick_immediately_after_registration() {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        runtime.block_on(async {
            let period = Duration::from_millis(200);
            let mut ticker = new_keepalive_ticker(period);

            assert!(
                tokio::time::timeout(Duration::from_millis(30), ticker.tick())
                    .await
                    .is_err(),
                "the first periodic keepalive must wait for its configured cadence"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(400), ticker.tick())
                    .await
                    .is_ok(),
                "the first periodic keepalive must run after the cadence elapses"
            );
        });
    }
}
