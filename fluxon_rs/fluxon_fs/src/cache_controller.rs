use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;

use fluxon_fs_core::config::{FluxonFsCacheControllerConfig, FluxonFsRequestIdentity};

/// Key of a single cacheable piece of an S3-gateway object. See design §4.1.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PieceKey {
    pub export: String,
    pub relpath: String,
    pub sig: String,
    pub piece_idx: i64,
}

/// Per-piece access bookkeeping used by stats GC.
#[derive(Debug)]
struct Stats {
    last_access_ms: AtomicI64,
}

impl Stats {
    fn new() -> Self {
        Self {
            last_access_ms: AtomicI64::new(now_ms()),
        }
    }
}

/// Task enqueued by the coordinator for a stage worker to consume.
#[derive(Clone, Debug)]
pub struct StageTask {
    pub piece_key: PieceKey,
    pub identity: Option<FluxonFsRequestIdentity>,
}

/// Callback the agent provides to stage a piece into KV. The controller
/// doesn't know the RPC transport — it just invokes this. Signature intentionally
/// synchronous + blocking: worker already runs on a dedicated tokio task and
/// calls spawn_blocking-style code paths internally.
pub type StagePieceFn = Arc<
    dyn Fn(&PieceKey, Option<&FluxonFsRequestIdentity>) -> Result<(), String>
        + Send
        + Sync
        + 'static,
>;

pub type StagePieceRangeFn = Arc<
    dyn Fn(&PieceKey, usize, Option<&FluxonFsRequestIdentity>) -> Result<(), String>
        + Send
        + Sync
        + 'static,
>;

#[derive(Debug, Clone, Copy)]
pub struct CacheControllerStatsSnapshot {
    pub suggest_enqueued_count: u64,
    pub suggest_admission_rejected_count: u64,
    pub suggest_deduped_inflight_count: u64,
    pub suggest_queue_dropped_count: u64,
    pub stage_success_count: u64,
    pub stage_fail_count: u64,
    pub stage_panic_count: u64,
    pub queue_depth: usize,
    pub inflight_count: usize,
    pub stats_count: usize,
}

/// The controller. One instance per coordinator member (at most one per host).
pub struct CacheController {
    inflight: Arc<DashMap<PieceKey, ()>>,
    stats: Arc<DashMap<PieceKey, Arc<Stats>>>,
    stage_queue_tx: mpsc::Sender<StageTask>,
    config: FluxonFsCacheControllerConfig,
    suggest_enqueued_count: AtomicU64,
    suggest_admission_rejected_count: AtomicU64,
    suggest_deduped_inflight_count: AtomicU64,
    suggest_queue_dropped_count: AtomicU64,
    stage_success_count: Arc<AtomicU64>,
    stage_fail_count: Arc<AtomicU64>,
    stage_panic_count: Arc<AtomicU64>,
    queue_depth: Arc<AtomicUsize>,
    max_coalesced_piece_count: usize,
}

impl CacheController {
    /// Start the controller and its background workers. Workers keep running
    /// until the returned handle (and all its clones) are dropped.
    pub fn start(
        config: FluxonFsCacheControllerConfig,
        stage_piece_fn: StagePieceFn,
        stage_piece_range_fn: StagePieceRangeFn,
        rt_handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let (stage_queue_tx, stage_queue_rx) =
            mpsc::channel::<StageTask>(config.stage_queue_capacity);
        let inflight: Arc<DashMap<PieceKey, ()>> = Arc::new(DashMap::new());
        let stats: Arc<DashMap<PieceKey, Arc<Stats>>> = Arc::new(DashMap::new());
        let queue_depth = Arc::new(AtomicUsize::new(0));
        let stage_success_count = Arc::new(AtomicU64::new(0));
        let stage_fail_count = Arc::new(AtomicU64::new(0));
        let stage_panic_count = Arc::new(AtomicU64::new(0));

        let ctrl = Arc::new(Self {
            inflight: inflight.clone(),
            stats: stats.clone(),
            stage_queue_tx,
            config: config.clone(),
            suggest_enqueued_count: AtomicU64::new(0),
            suggest_admission_rejected_count: AtomicU64::new(0),
            suggest_deduped_inflight_count: AtomicU64::new(0),
            suggest_queue_dropped_count: AtomicU64::new(0),
            stage_success_count: stage_success_count.clone(),
            stage_fail_count: stage_fail_count.clone(),
            stage_panic_count: stage_panic_count.clone(),
            queue_depth: queue_depth.clone(),
            max_coalesced_piece_count: config.max_coalesced_piece_count.max(1),
        });

        // Workers share the rx via a single mpsc Receiver wrapped in a Mutex.
        // Alternatives (broadcast / flume) would give each worker its own tap,
        // but then task dispatch becomes all-at-once instead of work-stealing.
        let shared_rx = Arc::new(tokio::sync::Mutex::new(stage_queue_rx));
        for worker_id in 0..config.stage_worker_count {
            let rx = shared_rx.clone();
            let inflight_clone = inflight.clone();
            let fn_clone = stage_piece_fn.clone();
            let rt = rt_handle.clone();
            let queue_depth_clone = queue_depth.clone();
            let stage_success_count_clone = stage_success_count.clone();
            let stage_fail_count_clone = stage_fail_count.clone();
            let stage_panic_count_clone = stage_panic_count.clone();
            let stage_piece_range_fn_clone = stage_piece_range_fn.clone();
            let max_coalesced_piece_count = config.max_coalesced_piece_count.max(1);
            rt_handle.spawn(async move {
                stage_worker_loop(
                    worker_id,
                    rx,
                    inflight_clone,
                    fn_clone,
                    stage_piece_range_fn_clone,
                    rt,
                    queue_depth_clone,
                    stage_success_count_clone,
                    stage_fail_count_clone,
                    stage_panic_count_clone,
                    max_coalesced_piece_count,
                )
                .await;
            });
        }

        let stats_gc = stats.clone();
        let stats_gc_interval = Duration::from_secs(config.stats_gc_scan_interval_secs.max(1));
        let stats_gc_max_age_ms = (config.stats_gc_max_entry_age_secs as i64) * 1000;
        rt_handle.spawn(async move {
            let mut ticker = tokio::time::interval(stats_gc_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let now = now_ms();
                stats_gc.retain(|_, s| {
                    now - s.last_access_ms.load(Ordering::Relaxed) < stats_gc_max_age_ms
                });
            }
        });

        ctrl
    }

    /// Handle a suggest from a member (or self). Wait-free: bumps counters,
    /// checks admission, best-effort enqueues. Never blocks the caller.
    ///
    /// Returns one of:
    ///   - `Enqueued`    → admitted + queued (new stage task)
    ///   - `AdmissionRejected` → admission policy held it back
    ///   - `DedupedInflight`   → already staging
    ///   - `QueueDropped`      → queue full, counter rolled back
    pub fn handle_suggest(
        &self,
        key: PieceKey,
        identity: Option<FluxonFsRequestIdentity>,
    ) -> SuggestOutcome {
        // 1. touch stats entry for GC/accounting.
        let entry = self
            .stats
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Stats::new()));
        entry
            .value()
            .last_access_ms
            .store(now_ms(), Ordering::Relaxed);

        // 2. admission.
        let admitted = match self.config.admission_policy.as_str() {
            // Default to immediate admission so the first miss starts filling cache.
            _ => true,
        };
        if !admitted {
            self.suggest_admission_rejected_count
                .fetch_add(1, Ordering::Relaxed);
            return SuggestOutcome::AdmissionRejected;
        }

        // 3. singleflight.
        if self.inflight.insert(key.clone(), ()).is_some() {
            self.suggest_deduped_inflight_count
                .fetch_add(1, Ordering::Relaxed);
            return SuggestOutcome::DedupedInflight;
        }

        // 4. try_send (non-blocking).
        let task = StageTask {
            piece_key: key.clone(),
            identity,
        };
        match self.stage_queue_tx.try_send(task) {
            Ok(()) => {
                self.queue_depth.fetch_add(1, Ordering::Relaxed);
                self.suggest_enqueued_count.fetch_add(1, Ordering::Relaxed);
                SuggestOutcome::Enqueued
            }
            Err(_) => {
                self.inflight.remove(&key);
                self.suggest_queue_dropped_count
                    .fetch_add(1, Ordering::Relaxed);
                SuggestOutcome::QueueDropped
            }
        }
    }

    /// Debug accessor: current queue depth as visible to the controller.
    /// (Approximate — mpsc doesn't expose len on the sender.)
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    pub fn stats_count(&self) -> usize {
        self.stats.len()
    }

    pub fn config(&self) -> &FluxonFsCacheControllerConfig {
        &self.config
    }

    pub fn stats_snapshot(&self) -> CacheControllerStatsSnapshot {
        CacheControllerStatsSnapshot {
            suggest_enqueued_count: self.suggest_enqueued_count.load(Ordering::Relaxed),
            suggest_admission_rejected_count: self
                .suggest_admission_rejected_count
                .load(Ordering::Relaxed),
            suggest_deduped_inflight_count: self
                .suggest_deduped_inflight_count
                .load(Ordering::Relaxed),
            suggest_queue_dropped_count: self.suggest_queue_dropped_count.load(Ordering::Relaxed),
            stage_success_count: self.stage_success_count.load(Ordering::Relaxed),
            stage_fail_count: self.stage_fail_count.load(Ordering::Relaxed),
            stage_panic_count: self.stage_panic_count.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            inflight_count: self.inflight_count(),
            stats_count: self.stats_count(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuggestOutcome {
    Enqueued,
    AdmissionRejected,
    DedupedInflight,
    QueueDropped,
}

async fn stage_worker_loop(
    _worker_id: usize,
    rx: Arc<tokio::sync::Mutex<mpsc::Receiver<StageTask>>>,
    inflight: Arc<DashMap<PieceKey, ()>>,
    stage_piece_fn: StagePieceFn,
    stage_piece_range_fn: StagePieceRangeFn,
    rt_handle: tokio::runtime::Handle,
    queue_depth: Arc<AtomicUsize>,
    stage_success_count: Arc<AtomicU64>,
    stage_fail_count: Arc<AtomicU64>,
    stage_panic_count: Arc<AtomicU64>,
    max_coalesced_piece_count: usize,
) {
    let mut pending_task: Option<StageTask> = None;
    loop {
        let mut queue_guard = None;
        let (task, task_from_queue) = if let Some(t) = pending_task.take() {
            (t, false)
        } else {
            let mut guard = rx.lock().await;
            let t = match guard.recv().await {
                Some(t) => t,
                None => return, // sender dropped, nothing to do
            };
            queue_guard = Some(guard);
            (t, true)
        };
        if task_from_queue {
            queue_depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
                .ok();
        }

        let mut staged_piece_keys: Vec<PieceKey> = vec![task.piece_key.clone()];
        let mut staged_piece_count = 1usize;

        // Keep the receiver lock from the initial recv while peeking follow-up items.
        // Otherwise another worker can grab the single shared receiver and block on
        // recv(), which stalls this worker before it ever reaches the stage callback.
        if max_coalesced_piece_count > 1 {
            loop {
                if staged_piece_count >= max_coalesced_piece_count {
                    break;
                }
                let maybe_next = if let Some(guard) = queue_guard.as_mut() {
                    guard.try_recv().ok()
                } else if let Ok(mut guard) = rx.try_lock() {
                    guard.try_recv().ok()
                } else {
                    None
                };
                let Some(next_task) = maybe_next else {
                    break;
                };
                queue_depth
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
                    .ok();
                let anchor = &task.piece_key;
                let next = &next_task.piece_key;
                let same_identity = next_task.identity == task.identity;
                let same_object = next.export == anchor.export
                    && next.relpath == anchor.relpath
                    && next.sig == anchor.sig;
                let adjacent = next.piece_idx == anchor.piece_idx + staged_piece_count as i64;
                if same_identity && same_object && adjacent {
                    staged_piece_keys.push(next.clone());
                    staged_piece_count += 1;
                } else {
                    pending_task = Some(next_task);
                    break;
                }
            }
        }

        // Run the stage fn off-thread so we don't block the tokio worker on
        // blocking KV puts. The closure is Send + Sync.
        let key_clone = task.piece_key.clone();
        let staged_piece_count_clone = staged_piece_count;
        let identity_clone = task.identity.clone();
        let fn_clone = stage_piece_fn.clone();
        let fn_range_clone = stage_piece_range_fn.clone();
        let result = rt_handle
            .spawn_blocking(move || {
                if staged_piece_count_clone > 1 {
                    stage_piece_range_fn_invoke(
                        &fn_range_clone,
                        &key_clone,
                        staged_piece_count_clone,
                        identity_clone.as_ref(),
                    )
                } else {
                    stage_piece_fn_invoke(&fn_clone, &key_clone, identity_clone.as_ref())
                }
            })
            .await;

        // Always release inflight.
        for piece_key in &staged_piece_keys {
            inflight.remove(piece_key);
        }

        match result {
            Ok(Ok(())) => {
                stage_success_count.fetch_add(staged_piece_count as u64, Ordering::Relaxed);
            }
            Ok(Err(err)) => {
                stage_fail_count.fetch_add(staged_piece_count as u64, Ordering::Relaxed);
                tracing::debug!(
                    piece = ?task.piece_key,
                    piece_count = staged_piece_count,
                    err = %err,
                    "cache_controller stage failure (design §4.4: no retry)"
                );
            }
            Err(join_err) => {
                stage_panic_count.fetch_add(staged_piece_count as u64, Ordering::Relaxed);
                tracing::warn!(
                    piece = ?task.piece_key,
                    piece_count = staged_piece_count,
                    err = %join_err,
                    "cache_controller stage worker panic (join error)"
                );
            }
        }
    }
}

fn stage_piece_fn_invoke(
    f: &StagePieceFn,
    key: &PieceKey,
    identity: Option<&FluxonFsRequestIdentity>,
) -> Result<(), String> {
    f(key, identity)
}

fn stage_piece_range_fn_invoke(
    f: &StagePieceRangeFn,
    key: &PieceKey,
    piece_count: usize,
    identity: Option<&FluxonFsRequestIdentity>,
) -> Result<(), String> {
    f(key, piece_count, identity)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::{Condvar, Mutex};
    use tokio::time::{Duration, sleep};

    fn sample_key() -> PieceKey {
        PieceKey {
            export: "exp".to_string(),
            relpath: "dir/file.bin".to_string(),
            sig: "sig".to_string(),
            piece_idx: 0,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suggest_enqueues_and_worker_runs() {
        let stage_calls = Arc::new(AtomicUsize::new(0));
        let stage_calls_clone = stage_calls.clone();
        let (stage_started_tx, stage_started_rx) = mpsc::sync_channel(1);
        let stage_piece_fn: StagePieceFn = Arc::new(move |_key, _identity| {
            stage_calls_clone.fetch_add(1, AtomicOrdering::Relaxed);
            let _ = stage_started_tx.send(());
            Ok(())
        });
        let stage_piece_range_fn: StagePieceRangeFn =
            Arc::new(move |_key, _count, _identity| Ok(()));
        let ctrl = CacheController::start(
            FluxonFsCacheControllerConfig::default(),
            stage_piece_fn,
            stage_piece_range_fn,
            tokio::runtime::Handle::current(),
        );

        let outcome = ctrl.handle_suggest(sample_key(), None);
        assert_eq!(outcome, SuggestOutcome::Enqueued);
        stage_started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("stage worker did not run within 5s");

        for _ in 0..50 {
            if stage_calls.load(AtomicOrdering::Relaxed) == 1 && ctrl.inflight_count() == 0 {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(stage_calls.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(ctrl.inflight_count(), 0);
        let snapshot = ctrl.stats_snapshot();
        assert_eq!(snapshot.suggest_enqueued_count, 1);
        assert_eq!(snapshot.stage_success_count, 1);
        assert_eq!(snapshot.queue_depth, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suggest_dedupes_while_inflight() {
        let stage_calls = Arc::new(AtomicUsize::new(0));
        let stage_calls_clone = stage_calls.clone();
        let (stage_started_tx, stage_started_rx) = mpsc::sync_channel(1);
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let gate_clone = gate.clone();
        let stage_piece_fn: StagePieceFn = Arc::new(move |_key, _identity| {
            stage_calls_clone.fetch_add(1, AtomicOrdering::Relaxed);
            let _ = stage_started_tx.send(());
            let (lock, cv) = &*gate_clone;
            let mut state = lock.lock().unwrap();
            state.0 = true;
            cv.notify_all();
            while !state.1 {
                state = cv.wait(state).unwrap();
            }
            Ok(())
        });
        let stage_piece_range_fn: StagePieceRangeFn =
            Arc::new(move |_key, _count, _identity| Ok(()));
        let ctrl = CacheController::start(
            FluxonFsCacheControllerConfig::default(),
            stage_piece_fn,
            stage_piece_range_fn,
            tokio::runtime::Handle::current(),
        );

        let key = sample_key();
        assert_eq!(
            ctrl.handle_suggest(key.clone(), None),
            SuggestOutcome::Enqueued
        );
        stage_started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("stage worker did not start within 5s");
        {
            let (lock, cv) = &*gate;
            let mut state = lock.lock().unwrap();
            while !state.0 {
                state = cv.wait(state).unwrap();
            }
            state.1 = true;
            cv.notify_all();
        }
        assert_eq!(
            ctrl.handle_suggest(key, None),
            SuggestOutcome::DedupedInflight
        );

        for _ in 0..50 {
            if stage_calls.load(AtomicOrdering::Relaxed) == 1 && ctrl.inflight_count() == 0 {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(stage_calls.load(AtomicOrdering::Relaxed), 1);
        let snapshot = ctrl.stats_snapshot();
        assert_eq!(snapshot.suggest_enqueued_count, 1);
        assert_eq!(snapshot.suggest_deduped_inflight_count, 1);
        assert_eq!(snapshot.stage_success_count, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_drop_updates_snapshot() {
        let (stage_started_tx, stage_started_rx) = mpsc::sync_channel(1);
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let gate_clone = gate.clone();
        let stage_piece_fn: StagePieceFn = Arc::new(move |_key, _identity| {
            let _ = stage_started_tx.send(());
            let (lock, cv) = &*gate_clone;
            let mut state = lock.lock().unwrap();
            state.0 = true;
            cv.notify_all();
            while !state.1 {
                state = cv.wait(state).unwrap();
            }
            Ok(())
        });
        let stage_piece_range_fn: StagePieceRangeFn =
            Arc::new(move |_key, _count, _identity| Ok(()));
        let ctrl = CacheController::start(
            FluxonFsCacheControllerConfig {
                stage_queue_capacity: 1,
                stage_worker_count: 1,
                ..FluxonFsCacheControllerConfig::default()
            },
            stage_piece_fn,
            stage_piece_range_fn,
            tokio::runtime::Handle::current(),
        );

        let key0 = sample_key();
        let key1 = PieceKey {
            piece_idx: 1,
            ..sample_key()
        };
        let key2 = PieceKey {
            piece_idx: 2,
            ..sample_key()
        };

        assert_eq!(ctrl.handle_suggest(key0, None), SuggestOutcome::Enqueued);
        stage_started_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("stage worker did not start within 5s");
        {
            let (lock, cv) = &*gate;
            let mut state = lock.lock().unwrap();
            while !state.0 {
                state = cv.wait(state).unwrap();
            }
        }
        assert_eq!(ctrl.handle_suggest(key1, None), SuggestOutcome::Enqueued);
        assert_eq!(
            ctrl.handle_suggest(key2, None),
            SuggestOutcome::QueueDropped
        );
        {
            let (lock, cv) = &*gate;
            let mut state = lock.lock().unwrap();
            state.1 = true;
            cv.notify_all();
        }

        let snapshot = ctrl.stats_snapshot();
        assert_eq!(snapshot.suggest_enqueued_count, 2);
        assert_eq!(snapshot.suggest_queue_dropped_count, 1);
    }
}
