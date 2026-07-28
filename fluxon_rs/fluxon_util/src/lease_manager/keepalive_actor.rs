use super::lease_backend_uid::LeaseBackendUid;
use super::lease_handle::LeaseEntry;
use super::lifecycle::OnKeepalive;
use crate::auto_clean_map::AutoCleanMapEntry;
use etcd_client::{Client, LeaseKeepAliveStream, LeaseKeeper};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::debug;

/// Per-lease keepalive timeout budget for a single task.
///
/// 设计说明（与 review_mq_lease_manager_tuning.md 收敛）：
/// - 这里的 timeout 目标是“防止单个 keepalive 调用长期占用公共 actor
///   的并发槽位”，而不是“保证在 TTL 过期前一定完成 keepalive”。
///   真正的 TTL 守恒由 etcd / kvclient 自身的 lease 语义保障。
/// - etcd / kvclient keepalive 的正常延迟通常在几十到几百毫秒量级；
///   backend 使用 1.5s deadline，actor 使用 2s cancellation backstop。前者
///   必须先到期，以便 Etcd 在 actor 取消 future 前重置失败的 stream。
/// - 早期实现曾尝试按 `ttl_seconds` 动态放大 timeout（约等于 tick
///   周期），在大 TTL 场景下会让长尾调用长期占用调度容量。
///   目前版本选择固定、分层的 deadline，更符合“keepalive 是后台心跳”
///   的定位；如需按 TTL 微调，可在未来在不改变语义的前提下再演进。
pub(crate) const KEEPALIVE_BACKEND_OPERATION_BUDGET_MS: u64 = 1500;
const KEEPALIVE_ACTOR_OPERATION_BUDGET_MS: u64 = 2000;

// The backend deadline must win before the actor's cancellation backstop. In
// particular, Etcd uses its backend timeout to reset a broken keepalive stream.
const _: () = assert!(KEEPALIVE_ACTOR_OPERATION_BUDGET_MS > KEEPALIVE_BACKEND_OPERATION_BUDGET_MS);

/// Per-lease error log rate limit period for keepalive failures.
///
/// Persistent backend failures can otherwise emit one full error per lease on
/// every tick. Rate limiting retains the unhealthy-lease signal without
/// amplifying log I/O at large scale.
const KEEPALIVE_ERROR_LOG_PERIOD_SECS: u64 = 30;
const KEEPALIVE_ERROR_LOG_SKIP_FIRST: bool = false;
const KEEPALIVE_ERROR_LOG_KEY_PREFIX: &str = "lease_keepalive_error:";

/// Helper: rate-limit a keepalive-related log for a given lease.
///
/// - key 按 lease 维度聚合（`KEEPALIVE_ERROR_LOG_KEY_PREFIX + lease_id`）；
/// - 具体 log 内容与级别由闭包内部的 `tracing::warn!/error!` 决定；
/// - 只封装限频逻辑，不改变调用方的控制流和错误分类。
pub(crate) fn log_keepalive_error_rate_limited<I, F>(lease_id: I, log: F)
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
            // Drop the failed stream before reopening. Keeping Some/Some here would
            // make ensure_stream treat the broken pair as healthy.
            self.reset_stream();
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

pub type LeaseKeepaliveFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
pub type LeaseKeepaliveOperation<K> =
    Arc<dyn Fn(K) -> LeaseKeepaliveFuture + Send + Sync + 'static>;
pub type LeaseKeepaliveFailureCallback = Arc<dyn Fn(LeaseKeepaliveFailure) + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseKeepaliveFailure {
    Operation(String),
    Timeout { timeout: Duration },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseKeepaliveFailurePolicy {
    RetryAfter(Duration),
    Unregister,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LeaseKeepaliveDuplicatePolicy {
    Reject,
    Replace,
}

struct LeaseKeepaliveTarget<K> {
    operation: LeaseKeepaliveOperation<K>,
    failure_policy: LeaseKeepaliveFailurePolicy,
    on_failure: LeaseKeepaliveFailureCallback,
}

struct LeaseKeepaliveEntry<K> {
    key: K,
    generation: u64,
    target: Weak<LeaseKeepaliveTarget<K>>,
    next_due: Instant,
    in_flight: bool,
}

struct LeaseKeepaliveState<K> {
    generation_by_key: HashMap<K, u64>,
    entries: HashMap<u64, LeaseKeepaliveEntry<K>>,
    schedule: BinaryHeap<Reverse<(Instant, u64)>>,
}

impl<K> Default for LeaseKeepaliveState<K> {
    fn default() -> Self {
        Self {
            generation_by_key: HashMap::new(),
            entries: HashMap::new(),
            schedule: BinaryHeap::new(),
        }
    }
}

struct LeaseKeepaliveCompletion<K> {
    key: K,
    generation: u64,
    target: Arc<LeaseKeepaliveTarget<K>>,
    result: Result<(), LeaseKeepaliveFailure>,
}

type LeaseKeepaliveTask<K> =
    Pin<Box<dyn Future<Output = LeaseKeepaliveCompletion<K>> + Send + 'static>>;

pub struct LeaseKeepaliveActor<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    cadence: Duration,
    max_jitter: Duration,
    operation_timeout: Duration,
    max_concurrency: usize,
    closed: AtomicBool,
    next_generation: AtomicU64,
    state: Mutex<LeaseKeepaliveState<K>>,
    changed: Notify,
    #[cfg(test)]
    loop_iterations: AtomicU64,
}

impl<K> std::fmt::Debug for LeaseKeepaliveActor<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseKeepaliveActor")
            .field("cadence", &self.cadence)
            .field("max_jitter", &self.max_jitter)
            .field("operation_timeout", &self.operation_timeout)
            .field("max_concurrency", &self.max_concurrency)
            .field("closed", &self.closed.load(Ordering::Acquire))
            .field("registered_leases", &self.state.lock().entries.len())
            .finish_non_exhaustive()
    }
}

pub struct LeaseKeepaliveRegistration<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    actor: Weak<LeaseKeepaliveActor<K>>,
    _target: Arc<LeaseKeepaliveTarget<K>>,
    key: K,
    generation: u64,
}

impl<K> std::fmt::Debug for LeaseKeepaliveRegistration<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseKeepaliveRegistration")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl<K> Drop for LeaseKeepaliveRegistration<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(actor) = self.actor.upgrade() {
            actor.unregister(&self.key, self.generation);
        }
    }
}

impl<K> LeaseKeepaliveActor<K>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
{
    pub fn new(
        cadence: Duration,
        max_jitter: Duration,
        operation_timeout: Duration,
        max_concurrency: usize,
    ) -> Self {
        assert!(
            !cadence.is_zero(),
            "lease keepalive cadence must be positive"
        );
        assert!(
            !operation_timeout.is_zero(),
            "lease keepalive operation timeout must be positive"
        );
        assert!(
            max_concurrency > 0,
            "lease keepalive concurrency must be positive"
        );
        Self {
            cadence,
            max_jitter,
            operation_timeout,
            max_concurrency,
            closed: AtomicBool::new(false),
            next_generation: AtomicU64::new(1),
            state: Mutex::new(LeaseKeepaliveState::default()),
            changed: Notify::new(),
            #[cfg(test)]
            loop_iterations: AtomicU64::new(0),
        }
    }

    pub fn register(
        self: &Arc<Self>,
        key: K,
        operation: LeaseKeepaliveOperation<K>,
        failure_policy: LeaseKeepaliveFailurePolicy,
        on_failure: LeaseKeepaliveFailureCallback,
    ) -> Result<LeaseKeepaliveRegistration<K>, String> {
        self.register_with_duplicate_policy(
            key,
            operation,
            failure_policy,
            on_failure,
            LeaseKeepaliveDuplicatePolicy::Reject,
        )
    }

    /// Replace an existing generation for the same key.
    ///
    /// This is reserved for an authoritative higher-level registry that has
    /// already created a new value generation. Its old value can remain alive
    /// for a few instructions after the registry key is removed; replacing the
    /// actor generation closes that drop/recreate window without weakening the
    /// strict duplicate check used by direct callers such as FluxonFS.
    pub(crate) fn register_replacing(
        self: &Arc<Self>,
        key: K,
        operation: LeaseKeepaliveOperation<K>,
        failure_policy: LeaseKeepaliveFailurePolicy,
        on_failure: LeaseKeepaliveFailureCallback,
    ) -> Result<LeaseKeepaliveRegistration<K>, String> {
        self.register_with_duplicate_policy(
            key,
            operation,
            failure_policy,
            on_failure,
            LeaseKeepaliveDuplicatePolicy::Replace,
        )
    }

    fn register_with_duplicate_policy(
        self: &Arc<Self>,
        key: K,
        operation: LeaseKeepaliveOperation<K>,
        failure_policy: LeaseKeepaliveFailurePolicy,
        on_failure: LeaseKeepaliveFailureCallback,
        duplicate_policy: LeaseKeepaliveDuplicatePolicy,
    ) -> Result<LeaseKeepaliveRegistration<K>, String> {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let target = Arc::new(LeaseKeepaliveTarget {
            operation,
            failure_policy,
            on_failure,
        });
        {
            let mut state = self.state.lock();
            if self.closed.load(Ordering::Acquire) {
                return Err("lease keepalive actor is closed".to_string());
            }
            if let Some(existing_generation) = state.generation_by_key.get(&key).copied() {
                let existing_is_live = state
                    .entries
                    .get(&existing_generation)
                    .and_then(|entry| entry.target.upgrade())
                    .is_some();
                if existing_is_live && duplicate_policy == LeaseKeepaliveDuplicatePolicy::Reject {
                    return Err("lease is already registered with keepalive actor".to_string());
                }
                Self::remove_generation_locked(&mut state, existing_generation);
            }
            let next_due = Instant::now() + self.delay_for_key(&key);
            state.generation_by_key.insert(key.clone(), generation);
            state.entries.insert(
                generation,
                LeaseKeepaliveEntry {
                    key: key.clone(),
                    generation,
                    target: Arc::downgrade(&target),
                    next_due,
                    in_flight: false,
                },
            );
            state.schedule.push(Reverse((next_due, generation)));
        }
        self.changed.notify_one();
        Ok(LeaseKeepaliveRegistration {
            actor: Arc::downgrade(self),
            _target: target,
            key,
            generation,
        })
    }

    pub fn close(&self) {
        {
            let _state = self.state.lock();
            self.closed.store(true, Ordering::Release);
        }
        self.changed.notify_waiters();
    }

    pub async fn run_until_stopped<F>(self: Arc<Self>, stop: F)
    where
        F: Future<Output = ()> + Send,
    {
        self.run(stop, false).await;
    }

    pub(crate) async fn run_until_idle(self: Arc<Self>) {
        self.run(std::future::pending::<()>(), true).await;
    }

    async fn run<F>(self: Arc<Self>, stop: F, stop_when_idle: bool)
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(stop);
        let mut in_flight = FuturesUnordered::<LeaseKeepaliveTask<K>>::new();
        loop {
            #[cfg(test)]
            self.loop_iterations.fetch_add(1, Ordering::Relaxed);
            if self.closed.load(Ordering::Acquire)
                || (stop_when_idle && self.registered_lease_count() == 0)
            {
                return;
            }
            self.fill_available_slots(&mut in_flight);
            let changed = self.changed.notified();
            tokio::pin!(changed);
            let next_due = (in_flight.len() < self.max_concurrency)
                .then(|| self.next_due())
                .flatten();
            let completion = match (next_due, in_flight.is_empty()) {
                (Some(next_due), false) => {
                    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(next_due));
                    tokio::pin!(sleep);
                    tokio::select! {
                        biased;
                        _ = &mut stop => return,
                        completion = in_flight.next() => completion,
                        _ = &mut sleep => None,
                        _ = &mut changed => None,
                    }
                }
                (Some(next_due), true) => {
                    let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(next_due));
                    tokio::pin!(sleep);
                    tokio::select! {
                        biased;
                        _ = &mut stop => return,
                        _ = &mut sleep => None,
                        _ = &mut changed => None,
                    }
                }
                (None, false) => tokio::select! {
                    biased;
                    _ = &mut stop => return,
                    completion = in_flight.next() => completion,
                    _ = &mut changed => None,
                },
                (None, true) => tokio::select! {
                    biased;
                    _ = &mut stop => return,
                    _ = &mut changed => None,
                },
            };
            if let Some(completion) = completion {
                self.handle_completion(completion);
            }
        }
    }

    fn delay_for_key(&self, key: &K) -> Duration {
        let max_jitter_ms = u64::try_from(self.max_jitter.as_millis()).unwrap_or(u64::MAX);
        if max_jitter_ms == 0 {
            return self.cadence;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        let jitter_ms = hasher.finish() % max_jitter_ms.saturating_add(1);
        self.cadence + Duration::from_millis(jitter_ms)
    }

    fn remove_generation_locked(state: &mut LeaseKeepaliveState<K>, generation: u64) -> bool {
        let Some(entry) = state.entries.remove(&generation) else {
            return false;
        };
        if state
            .generation_by_key
            .get(&entry.key)
            .is_some_and(|current| *current == generation)
        {
            state.generation_by_key.remove(&entry.key);
        }
        true
    }

    fn prune_stale_schedule_locked(state: &mut LeaseKeepaliveState<K>) {
        loop {
            let Some(Reverse((next_due, generation))) = state.schedule.peek().copied() else {
                return;
            };
            let (is_current, target_is_gone) = match state.entries.get(&generation) {
                Some(entry)
                    if entry.generation == generation
                        && entry.next_due == next_due
                        && !entry.in_flight =>
                {
                    let target_is_gone = entry.target.upgrade().is_none();
                    (!target_is_gone, target_is_gone)
                }
                _ => (false, false),
            };
            if is_current {
                return;
            }
            state.schedule.pop();
            if target_is_gone {
                Self::remove_generation_locked(state, generation);
            }
        }
    }

    fn compact_schedule_if_needed_locked(state: &mut LeaseKeepaliveState<K>) {
        const STALE_SCHEDULE_ALLOWANCE: usize = 1024;
        let compact_threshold = state
            .entries
            .len()
            .saturating_mul(2)
            .saturating_add(STALE_SCHEDULE_ALLOWANCE);
        if state.schedule.len() <= compact_threshold {
            return;
        }
        let stale_generations = state
            .entries
            .iter()
            .filter_map(|(generation, entry)| {
                (!entry.in_flight && entry.target.upgrade().is_none()).then_some(*generation)
            })
            .collect::<Vec<_>>();
        for generation in stale_generations {
            Self::remove_generation_locked(state, generation);
        }
        state.schedule = state
            .entries
            .iter()
            .filter(|(_, entry)| !entry.in_flight)
            .map(|(generation, entry)| Reverse((entry.next_due, *generation)))
            .collect();
    }

    fn next_due(&self) -> Option<Instant> {
        let mut state = self.state.lock();
        Self::prune_stale_schedule_locked(&mut state);
        state
            .schedule
            .peek()
            .map(|Reverse((next_due, _))| *next_due)
    }

    fn take_due(&self, limit: usize) -> Vec<(K, u64, Arc<LeaseKeepaliveTarget<K>>)> {
        let now = Instant::now();
        let mut state = self.state.lock();
        let mut due = Vec::new();
        while due.len() < limit {
            Self::prune_stale_schedule_locked(&mut state);
            let Some(Reverse((next_due, generation))) = state.schedule.peek().copied() else {
                break;
            };
            if next_due > now {
                break;
            }
            state.schedule.pop();
            let entry = state
                .entries
                .get_mut(&generation)
                .expect("current keepalive schedule must have an entry");
            let target = entry
                .target
                .upgrade()
                .expect("current keepalive schedule must have a live target");
            debug_assert_eq!(entry.generation, generation);
            debug_assert_eq!(entry.next_due, next_due);
            debug_assert!(!entry.in_flight);
            entry.in_flight = true;
            due.push((entry.key.clone(), generation, target));
        }
        due
    }

    fn fill_available_slots(&self, in_flight: &mut FuturesUnordered<LeaseKeepaliveTask<K>>) {
        let available = self.max_concurrency.saturating_sub(in_flight.len());
        if available == 0 {
            return;
        }
        for (key, generation, target) in self.take_due(available) {
            let operation_timeout = self.operation_timeout;
            in_flight.push(Box::pin(async move {
                let result =
                    match tokio::time::timeout(operation_timeout, (target.operation)(key.clone()))
                        .await
                    {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(detail)) => Err(LeaseKeepaliveFailure::Operation(detail)),
                        Err(_) => Err(LeaseKeepaliveFailure::Timeout {
                            timeout: operation_timeout,
                        }),
                    };
                LeaseKeepaliveCompletion {
                    key,
                    generation,
                    target,
                    result,
                }
            }));
        }
    }

    fn handle_completion(&self, completion: LeaseKeepaliveCompletion<K>) {
        match completion.result {
            Ok(()) => {
                self.reschedule(
                    &completion.key,
                    completion.generation,
                    self.delay_for_key(&completion.key),
                );
            }
            Err(failure) => match completion.target.failure_policy {
                LeaseKeepaliveFailurePolicy::RetryAfter(delay) => {
                    if self.reschedule(&completion.key, completion.generation, delay) {
                        (completion.target.on_failure)(failure);
                    }
                }
                LeaseKeepaliveFailurePolicy::Unregister => {
                    if self.unregister(&completion.key, completion.generation) {
                        (completion.target.on_failure)(failure);
                    }
                }
            },
        }
    }

    fn reschedule(&self, key: &K, generation: u64, delay: Duration) -> bool {
        let mut state = self.state.lock();
        let next_due = Instant::now() + delay;
        let rescheduled = if state
            .generation_by_key
            .get(key)
            .is_some_and(|current| *current == generation)
        {
            if let Some(entry) = state.entries.get_mut(&generation)
                && entry.in_flight
            {
                entry.in_flight = false;
                entry.next_due = next_due;
                true
            } else {
                false
            }
        } else {
            false
        };
        if rescheduled {
            state.schedule.push(Reverse((next_due, generation)));
        }
        rescheduled
    }

    fn unregister(&self, key: &K, generation: u64) -> bool {
        let removed = {
            let mut state = self.state.lock();
            let is_current = state
                .generation_by_key
                .get(key)
                .is_some_and(|current| *current == generation);
            if is_current {
                let removed = Self::remove_generation_locked(&mut state, generation);
                Self::compact_schedule_if_needed_locked(&mut state);
                removed
            } else {
                false
            }
        };
        if removed {
            self.changed.notify_one();
        }
        removed
    }

    pub fn registered_lease_count(&self) -> usize {
        self.state.lock().entries.len()
    }

    #[cfg(test)]
    fn loop_iteration_count(&self) -> u64 {
        self.loop_iterations.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn make_all_due(&self) {
        let now = Instant::now();
        let mut state = self.state.lock();
        state.schedule.clear();
        let mut scheduled = Vec::with_capacity(state.entries.len());
        for (generation, entry) in &mut state.entries {
            entry.next_due = now;
            if !entry.in_flight {
                scheduled.push(Reverse((now, *generation)));
            }
        }
        state.schedule.extend(scheduled);
        drop(state);
        self.changed.notify_one();
    }
}

/// Composite key for a single lease entry in an MQ keepalive actor.
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

pub(crate) const MQ_KEEPALIVE_RETRY_DELAY: Duration = Duration::from_millis(100);
const MQ_KEEPALIVE_MAX_CONCURRENCY: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LeaseActorMapKey {
    ttl_seconds: i64,
    runtime_id: tokio::runtime::Id,
}

impl LeaseActorMapKey {
    pub(crate) fn new(ttl_seconds: i64, runtime_id: tokio::runtime::Id) -> Self {
        Self {
            ttl_seconds,
            runtime_id,
        }
    }

    pub(crate) fn ttl_seconds(self) -> i64 {
        self.ttl_seconds
    }
}

#[derive(Debug, Clone, Copy)]
struct KeepaliveRunnerState {
    running: bool,
    generation: u64,
}

pub(crate) struct OneTtlKeepAliveInner {
    pub(crate) ttl_seconds: i64,
    pub(crate) registry: crate::auto_clean_map::AutoCleanMap<LeaseKey, LeaseEntry>,
    pub(crate) actor: Arc<LeaseKeepaliveActor<LeaseKey>>,
    running_state: Mutex<KeepaliveRunnerState>,
}

impl OneTtlKeepAliveInner {
    pub(crate) fn new(ttl_seconds: i64) -> Self {
        let cadence = Duration::from_secs(((ttl_seconds / 3).max(0) + 1) as u64);
        Self {
            ttl_seconds,
            registry: crate::auto_clean_map::AutoCleanMap::new(),
            actor: Arc::new(LeaseKeepaliveActor::new(
                cadence,
                Duration::ZERO,
                Duration::from_millis(KEEPALIVE_ACTOR_OPERATION_BUDGET_MS),
                MQ_KEEPALIVE_MAX_CONCURRENCY,
            )),
            running_state: Mutex::new(KeepaliveRunnerState {
                running: false,
                generation: 0,
            }),
        }
    }
}

struct KeepaliveRunnerGuard {
    inner: Arc<OneTtlKeepAliveInner>,
    generation: u64,
    armed: bool,
}

impl Drop for KeepaliveRunnerGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self.inner.running_state.lock();
        if state.generation == self.generation {
            state.running = false;
        }
    }
}

fn spawn_loop(rt: &tokio::runtime::Handle, inner: Arc<OneTtlKeepAliveInner>, generation: u64) {
    rt.spawn(async move {
        let mut guard = KeepaliveRunnerGuard {
            inner: inner.clone(),
            generation,
            armed: true,
        };
        loop {
            inner.actor.clone().run_until_idle().await;
            let mut state = inner.running_state.lock();
            if inner.actor.registered_lease_count() == 0 {
                if state.generation == generation {
                    state.running = false;
                }
                guard.armed = false;
                return;
            }
            drop(state);
        }
    });
}

pub(crate) fn ensure_inner_running(rt: tokio::runtime::Handle, inner: Arc<OneTtlKeepAliveInner>) {
    let generation = {
        let mut state = inner.running_state.lock();
        if state.running {
            return;
        }
        state.generation = state.generation.wrapping_add(1).max(1);
        state.running = true;
        state.generation
    };
    spawn_loop(&rt, inner, generation);
}

// ---------- actor registry per ttl (Weak map) ----------

// moved to get_or_init.rs

// ---------- backend registry / guards ----------

// unified backend object table now lives in lease_backend_handle.rs

// Native Fluxon KV lease operations live inside `LeaseBackendUid`.

// ---------- actor register / unregister (KvClient & Etcd) ----------
#[allow(clippy::large_enum_variant)]
pub enum ActorRegisterInvocation {
    KvClient {
        keepalive: OnKeepalive,
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
    super::lifecycle::actor_get_or_spawn_and_register(
        ttl_seconds,
        LeaseKey::new(backend_uid, lease_id),
        &inv,
        rt,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore;

    fn actor(
        cadence: Duration,
        timeout: Duration,
        max_concurrency: usize,
    ) -> Arc<LeaseKeepaliveActor<u64>> {
        Arc::new(LeaseKeepaliveActor::new(
            cadence,
            Duration::ZERO,
            timeout,
            max_concurrency,
        ))
    }

    fn operation<F>(f: F) -> LeaseKeepaliveOperation<u64>
    where
        F: Fn(u64) -> LeaseKeepaliveFuture + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn first_keepalive_waits_for_cadence() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_operation = calls.clone();
        let actor = actor(Duration::from_millis(100), Duration::from_secs(1), 1);
        let _registration = actor
            .register(
                1,
                operation(move |_| {
                    let calls = calls_for_operation.clone();
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(|_| {}),
            )
            .expect("register lease");
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        tokio::time::timeout(Duration::from_millis(300), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first keepalive did not run after cadence");
        stop.notify_waiters();
        task.await.expect("join actor");
    }

    #[test]
    fn registration_drop_unregisters_immediately() {
        let actor = actor(Duration::from_secs(60), Duration::from_secs(1), 1);
        let registration = actor
            .register(
                7,
                operation(|_| Box::pin(async { Ok(()) })),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(|_| {}),
            )
            .expect("register lease");
        assert_eq!(actor.registered_lease_count(), 1);
        drop(registration);
        assert_eq!(actor.registered_lease_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacement_registration_is_generation_safe() {
        let old_calls = Arc::new(AtomicUsize::new(0));
        let new_calls = Arc::new(AtomicUsize::new(0));
        let old_calls_for_operation = old_calls.clone();
        let new_calls_for_operation = new_calls.clone();
        let actor = actor(Duration::from_secs(60), Duration::from_secs(1), 1);
        let old_registration = actor
            .register(
                8,
                operation(move |_| {
                    let calls = old_calls_for_operation.clone();
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(|_| {}),
            )
            .expect("register old generation");
        assert!(
            actor
                .register(
                    8,
                    operation(|_| Box::pin(async { Ok(()) })),
                    LeaseKeepaliveFailurePolicy::Unregister,
                    Arc::new(|_| {}),
                )
                .is_err(),
            "direct callers must still reject duplicate registrations"
        );

        let new_registration = actor
            .register_replacing(
                8,
                operation(move |_| {
                    let calls = new_calls_for_operation.clone();
                    Box::pin(async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                }),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(|_| {}),
            )
            .expect("replace old generation");
        assert_eq!(actor.registered_lease_count(), 1);

        // The old AutoCleanMap value can finish dropping after its replacement
        // has registered. Its stale generation must not remove the new slot.
        drop(old_registration);
        assert_eq!(actor.registered_lease_count(), 1);

        actor.make_all_due();
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while new_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement keepalive did not run");
        assert_eq!(old_calls.load(Ordering::SeqCst), 0);
        stop.notify_waiters();
        task.await.expect("join actor");

        drop(new_registration);
        assert_eq!(actor.registered_lease_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminal_failure_unregisters_only_matching_lease() {
        let actor = actor(Duration::from_secs(60), Duration::from_secs(1), 2);
        let failed = Arc::new(AtomicUsize::new(0));
        let failed_for_callback = failed.clone();
        let operation = operation(|lease_id| {
            Box::pin(async move {
                if lease_id == 11 {
                    Err("keepalive failed".to_string())
                } else {
                    Ok(())
                }
            })
        });
        let _failed_registration = actor
            .register(
                11,
                operation.clone(),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(move |failure| {
                    assert_eq!(
                        failure,
                        LeaseKeepaliveFailure::Operation("keepalive failed".to_string())
                    );
                    failed_for_callback.fetch_add(1, Ordering::SeqCst);
                }),
            )
            .expect("register failed lease");
        let _healthy_registration = actor
            .register(
                12,
                operation,
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(|_| panic!("healthy lease must not fail")),
            )
            .expect("register healthy lease");
        actor.make_all_due();
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            while failed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed keepalive did not complete");
        assert_eq!(actor.registered_lease_count(), 1);
        stop.notify_waiters();
        task.await.expect("join actor");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn retry_failure_does_not_reschedule_healthy_lease() {
        let failed_calls = Arc::new(AtomicUsize::new(0));
        let healthy_calls = Arc::new(AtomicUsize::new(0));
        let actor = actor(Duration::from_secs(60), Duration::from_secs(1), 2);
        let failed_calls_for_operation = failed_calls.clone();
        let healthy_calls_for_operation = healthy_calls.clone();
        let operation = operation(move |lease_id| {
            let failed_calls = failed_calls_for_operation.clone();
            let healthy_calls = healthy_calls_for_operation.clone();
            Box::pin(async move {
                if lease_id == 21 {
                    failed_calls.fetch_add(1, Ordering::SeqCst);
                    Err("retry".to_string())
                } else {
                    healthy_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
        });
        let _failed_registration = actor
            .register(
                21,
                operation.clone(),
                LeaseKeepaliveFailurePolicy::RetryAfter(Duration::from_millis(10)),
                Arc::new(|_| {}),
            )
            .expect("register failed lease");
        let _healthy_registration = actor
            .register(
                22,
                operation,
                LeaseKeepaliveFailurePolicy::RetryAfter(Duration::from_millis(10)),
                Arc::new(|_| panic!("healthy lease must not fail")),
            )
            .expect("register healthy lease");
        actor.make_all_due();
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            while failed_calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed lease was not retried");
        assert_eq!(healthy_calls.load(Ordering::SeqCst), 1);
        stop.notify_waiters();
        task.await.expect("join actor");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timeout_notifies_and_unregisters_lease() {
        let actor = actor(Duration::from_secs(60), Duration::from_millis(10), 1);
        let timed_out = Arc::new(AtomicBool::new(false));
        let timed_out_for_callback = timed_out.clone();
        let _registration = actor
            .register(
                31,
                operation(|_| Box::pin(std::future::pending())),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(move |failure| {
                    assert_eq!(
                        failure,
                        LeaseKeepaliveFailure::Timeout {
                            timeout: Duration::from_millis(10)
                        }
                    );
                    timed_out_for_callback.store(true, Ordering::Release);
                }),
            )
            .expect("register lease");
        actor.make_all_due();
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            while !timed_out.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out keepalive did not complete");
        assert_eq!(actor.registered_lease_count(), 0);
        stop.notify_waiters();
        task.await.expect("join actor");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actor_bounds_concurrency_without_child_tasks() {
        struct InFlightGuard(Arc<AtomicUsize>);
        impl Drop for InFlightGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let calls_for_operation = calls.clone();
        let current_for_operation = current.clone();
        let peak_for_operation = peak.clone();
        let release_for_operation = release.clone();
        let operation = operation(move |_| {
            let calls = calls_for_operation.clone();
            let current = current_for_operation.clone();
            let peak = peak_for_operation.clone();
            let release = release_for_operation.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let in_flight = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(in_flight, Ordering::SeqCst);
                let _guard = InFlightGuard(current);
                let permit = release.acquire_owned().await.expect("semaphore open");
                permit.forget();
                Ok(())
            })
        });
        let actor = actor(Duration::from_secs(60), Duration::from_secs(1), 2);
        let registrations = (40..43)
            .map(|lease_id| {
                actor
                    .register(
                        lease_id,
                        operation.clone(),
                        LeaseKeepaliveFailurePolicy::Unregister,
                        Arc::new(|_| {}),
                    )
                    .expect("register lease")
            })
            .collect::<Vec<_>>();
        actor.make_all_due();
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first keepalive wave did not start");
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        let iterations_before_wait = actor.loop_iteration_count();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            actor
                .loop_iteration_count()
                .saturating_sub(iterations_before_wait)
                <= 2
        );
        release.add_permits(3);
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) < 3 || current.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued keepalive did not use released slot");
        stop.notify_waiters();
        task.await.expect("join actor");
        drop(registrations);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopping_actor_cancels_in_flight_operation() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let started = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let started_for_operation = started.clone();
        let dropped_for_operation = dropped.clone();
        let actor = actor(Duration::from_secs(60), Duration::from_secs(30), 1);
        let _registration = actor
            .register(
                51,
                operation(move |_| {
                    let started = started_for_operation.clone();
                    let dropped = dropped_for_operation.clone();
                    Box::pin(async move {
                        let _probe = DropProbe(dropped);
                        started.store(true, Ordering::Release);
                        std::future::pending::<Result<(), String>>().await
                    })
                }),
                LeaseKeepaliveFailurePolicy::Unregister,
                Arc::new(|_| {}),
            )
            .expect("register lease");
        actor.make_all_due();
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(
            actor
                .clone()
                .run_until_stopped(stop.clone().notified_owned()),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while !started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("keepalive did not start");
        stop.notify_waiters();
        task.await.expect("join actor");
        assert!(dropped.load(Ordering::Acquire));
    }
}
