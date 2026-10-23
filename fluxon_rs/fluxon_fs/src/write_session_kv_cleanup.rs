use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot};

use fluxon_util::notify_state;

const KV_CLEANUP_MAX_TRACKED_KEYS: usize = 64;
const KV_PUT_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(30);
const KV_DELETE_OPERATION_TIMEOUT: Duration = Duration::from_secs(1);
const KV_DELETE_RETRY_INITIAL: Duration = Duration::from_millis(100);
const KV_DELETE_RETRY_MAX: Duration = Duration::from_secs(5);
const KV_DELETE_RETRY_MAX_JITTER: Duration = Duration::from_millis(250);
const KV_DELETE_UNCERTAIN_RECHECK: Duration = Duration::from_secs(5);

pub(crate) type RemoteWriteSessionKvDeleteFuture =
    Pin<Box<dyn Future<Output = RemoteWriteSessionKvDeleteResult> + Send + 'static>>;
pub(crate) type RemoteWriteSessionKvDeleteOperation =
    Arc<dyn Fn(String) -> RemoteWriteSessionKvDeleteFuture + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteWriteSessionKvDeleteResult {
    Deleted,
    NotFound,
    Failed(String),
}

#[derive(Debug, Clone, Copy)]
struct RemoteWriteSessionKvCleanupConfig {
    max_tracked_keys: usize,
    put_resolution_timeout: Duration,
    delete_operation_timeout: Duration,
    delete_retry_initial: Duration,
    delete_retry_max: Duration,
    delete_retry_max_jitter: Duration,
    uncertain_recheck: Duration,
}

impl RemoteWriteSessionKvCleanupConfig {
    fn production() -> Self {
        Self {
            max_tracked_keys: KV_CLEANUP_MAX_TRACKED_KEYS,
            put_resolution_timeout: KV_PUT_RESOLUTION_TIMEOUT,
            delete_operation_timeout: KV_DELETE_OPERATION_TIMEOUT,
            delete_retry_initial: KV_DELETE_RETRY_INITIAL,
            delete_retry_max: KV_DELETE_RETRY_MAX,
            delete_retry_max_jitter: KV_DELETE_RETRY_MAX_JITTER,
            uncertain_recheck: KV_DELETE_UNCERTAIN_RECHECK,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteWriteSessionKvCleanupCertainty {
    Committed,
    CommitUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteWriteSessionKvCleanupPhase {
    Putting,
    Borrowed {
        certainty: RemoteWriteSessionKvCleanupCertainty,
    },
    DeletePending {
        certainty: RemoteWriteSessionKvCleanupCertainty,
        attempts: u32,
    },
}

#[derive(Debug)]
struct RemoteWriteSessionKvCleanupEntry {
    key: String,
    lease_id: u64,
    phase: RemoteWriteSessionKvCleanupPhase,
}

struct RemoteWriteSessionKvCleanupState {
    accepting: bool,
    next_generation: u64,
    entries: HashMap<u64, RemoteWriteSessionKvCleanupEntry>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Default for RemoteWriteSessionKvCleanupState {
    fn default() -> Self {
        Self {
            accepting: true,
            next_generation: 1,
            entries: HashMap::new(),
            tasks: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct RemoteWriteSessionKvCleanupStop {
    stopped: AtomicBool,
    changed: Notify,
}

impl RemoteWriteSessionKvCleanupStop {
    fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }

    async fn wait(&self) {
        notify_state::wait_until(&self.changed, || self.is_stopped()).await;
    }
}

#[derive(Debug)]
struct RemoteWriteSessionKvPutControl {
    released: AtomicBool,
    changed: Notify,
}

impl RemoteWriteSessionKvPutControl {
    fn new() -> Self {
        Self {
            released: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.changed.notify_waiters();
        }
    }

    async fn wait_released(&self) {
        notify_state::wait_until(&self.changed, || self.released.load(Ordering::Acquire)).await;
    }
}

pub(crate) struct RemoteWriteSessionKvPutTicket {
    control: Arc<RemoteWriteSessionKvPutControl>,
    completion: oneshot::Receiver<Result<(), String>>,
}

impl RemoteWriteSessionKvPutTicket {
    pub(crate) async fn wait_for_put(&mut self, timeout: Duration) -> Result<(), String> {
        match tokio::time::timeout(timeout, &mut self.completion).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                Err("write-session KV put supervisor stopped before completion".to_string())
            }
            Err(_) => Err(format!("KV put timed out after {}s", timeout.as_secs_f64())),
        }
    }
}

impl Drop for RemoteWriteSessionKvPutTicket {
    fn drop(&mut self) {
        self.control.release();
    }
}

/// Controller-owned final-release authority for temporary write-session KV keys.
///
/// A key is registered before its put starts. The ticket only protects the borrower's use of the
/// key; dropping it releases the key to this actor, which owns deletion and retry until shutdown.
pub(crate) struct RemoteWriteSessionKvCleanupActor {
    runtime: Handle,
    delete: RemoteWriteSessionKvDeleteOperation,
    config: RemoteWriteSessionKvCleanupConfig,
    stop: RemoteWriteSessionKvCleanupStop,
    state: Mutex<RemoteWriteSessionKvCleanupState>,
    entries_changed: Notify,
}

impl RemoteWriteSessionKvCleanupActor {
    pub(crate) fn new(runtime: Handle, delete: RemoteWriteSessionKvDeleteOperation) -> Self {
        Self::new_with_config(
            runtime,
            delete,
            RemoteWriteSessionKvCleanupConfig::production(),
        )
    }

    fn new_with_config(
        runtime: Handle,
        delete: RemoteWriteSessionKvDeleteOperation,
        config: RemoteWriteSessionKvCleanupConfig,
    ) -> Self {
        assert!(config.max_tracked_keys > 0);
        assert!(!config.put_resolution_timeout.is_zero());
        assert!(!config.delete_operation_timeout.is_zero());
        assert!(!config.delete_retry_initial.is_zero());
        assert!(config.delete_retry_max >= config.delete_retry_initial);
        assert!(!config.uncertain_recheck.is_zero());
        Self {
            runtime,
            delete,
            config,
            stop: RemoteWriteSessionKvCleanupStop::new(),
            state: Mutex::new(RemoteWriteSessionKvCleanupState::default()),
            entries_changed: Notify::new(),
        }
    }

    pub(crate) fn begin_put<F>(
        self: &Arc<Self>,
        key: String,
        lease_id: u64,
        put: F,
    ) -> Result<RemoteWriteSessionKvPutTicket, String>
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let control = Arc::new(RemoteWriteSessionKvPutControl::new());
        let (completion_tx, completion_rx) = oneshot::channel();
        let mut state = self.state.lock();
        if !state.accepting || self.stop.is_stopped() {
            return Err("write-session temporary KV cleanup is shutting down".to_string());
        }
        state.tasks.retain(|task| !task.is_finished());
        if state.entries.len() >= self.config.max_tracked_keys {
            // Reject before starting the put. Existing cleanup records are never evicted to make
            // room, so capacity pressure safely downgrades the caller to raw transport.
            return Err(format!(
                "write-session temporary KV cleanup capacity exhausted: pending={} limit={}",
                state.entries.len(),
                self.config.max_tracked_keys
            ));
        }
        let generation = state.next_generation;
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .expect("write-session temporary KV cleanup generation overflow");
        state.entries.insert(
            generation,
            RemoteWriteSessionKvCleanupEntry {
                key: key.clone(),
                lease_id,
                phase: RemoteWriteSessionKvCleanupPhase::Putting,
            },
        );

        let actor = self.clone();
        let control_for_task = control.clone();
        let task = self.runtime.spawn(async move {
            actor
                .run_entry(
                    generation,
                    key,
                    lease_id,
                    control_for_task,
                    completion_tx,
                    put,
                )
                .await;
        });
        state.tasks.push(task);
        drop(state);

        Ok(RemoteWriteSessionKvPutTicket {
            control,
            completion: completion_rx,
        })
    }

    async fn run_entry<F>(
        self: Arc<Self>,
        generation: u64,
        key: String,
        lease_id: u64,
        control: Arc<RemoteWriteSessionKvPutControl>,
        completion: oneshot::Sender<Result<(), String>>,
        put: F,
    ) where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        let _entry_guard = RemoteWriteSessionKvCleanupEntryGuard {
            actor: Arc::downgrade(&self),
            generation,
        };
        let put_result = tokio::select! {
            biased;
            _ = self.stop.wait() => return,
            result = tokio::time::timeout(self.config.put_resolution_timeout, put) => result,
        };
        let certainty = match put_result {
            Ok(Ok(())) => {
                let _ = completion.send(Ok(()));
                RemoteWriteSessionKvCleanupCertainty::Committed
            }
            Ok(Err(detail)) => {
                let _ = completion.send(Err(detail));
                // Transport errors may be returned after a backend commit.
                RemoteWriteSessionKvCleanupCertainty::CommitUnknown
            }
            Err(_) => {
                let detail = format!(
                    "KV put did not resolve within cleanup ownership window of {}s",
                    self.config.put_resolution_timeout.as_secs_f64()
                );
                let _ = completion.send(Err(detail));
                RemoteWriteSessionKvCleanupCertainty::CommitUnknown
            }
        };
        self.set_phase(
            generation,
            RemoteWriteSessionKvCleanupPhase::Borrowed { certainty },
        );

        tokio::select! {
            biased;
            _ = self.stop.wait() => return,
            _ = control.wait_released() => {}
        }
        self.delete_until_reclaimed(generation, &key, lease_id, certainty)
            .await;
    }

    async fn delete_until_reclaimed(
        &self,
        generation: u64,
        key: &str,
        lease_id: u64,
        certainty: RemoteWriteSessionKvCleanupCertainty,
    ) {
        let mut attempt = 0_u32;
        loop {
            self.set_phase(
                generation,
                RemoteWriteSessionKvCleanupPhase::DeletePending {
                    certainty,
                    attempts: attempt,
                },
            );
            let delete = (self.delete)(key.to_string());
            let result = tokio::select! {
                biased;
                _ = self.stop.wait() => return,
                result = tokio::time::timeout(self.config.delete_operation_timeout, delete) => {
                    match result {
                        Ok(result) => result,
                        Err(_) => RemoteWriteSessionKvDeleteResult::Failed(format!(
                            "delete timed out after {}s",
                            self.config.delete_operation_timeout.as_secs_f64()
                        )),
                    }
                }
            };

            match (&certainty, result) {
                (
                    RemoteWriteSessionKvCleanupCertainty::Committed,
                    RemoteWriteSessionKvDeleteResult::Deleted
                    | RemoteWriteSessionKvDeleteResult::NotFound,
                ) => return,
                (
                    RemoteWriteSessionKvCleanupCertainty::CommitUnknown,
                    RemoteWriteSessionKvDeleteResult::Deleted
                    | RemoteWriteSessionKvDeleteResult::NotFound,
                ) => {
                    // A delete can race ahead of a late put commit. Keep probing until the lease
                    // is retired at Controller shutdown.
                    attempt = attempt.saturating_add(1);
                    let delay = self.uncertain_recheck_delay(key, attempt);
                    if self.wait_retry_or_stop(delay).await {
                        return;
                    }
                }
                (_, RemoteWriteSessionKvDeleteResult::Failed(detail)) => {
                    if attempt == 0 || attempt.is_power_of_two() {
                        tracing::warn!(
                            "write-session temporary KV cleanup failed; retrying: key={} lease_id={} attempt={} err={}",
                            key,
                            lease_id,
                            attempt.saturating_add(1),
                            detail
                        );
                    }
                    let delay = self.retry_delay(key, attempt);
                    attempt = attempt.saturating_add(1);
                    if self.wait_retry_or_stop(delay).await {
                        return;
                    }
                }
            }
        }
    }

    async fn wait_retry_or_stop(&self, delay: Duration) -> bool {
        tokio::select! {
            biased;
            _ = self.stop.wait() => true,
            _ = tokio::time::sleep(delay) => false,
        }
    }

    fn retry_delay(&self, key: &str, attempt: u32) -> Duration {
        let shift = attempt.min(20);
        let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
        let base = self
            .config
            .delete_retry_initial
            .saturating_mul(multiplier)
            .min(self.config.delete_retry_max);
        base.saturating_add(self.retry_jitter(key, attempt))
    }

    fn uncertain_recheck_delay(&self, key: &str, attempt: u32) -> Duration {
        self.config
            .uncertain_recheck
            .saturating_add(self.retry_jitter(key, attempt))
    }

    fn retry_jitter(&self, key: &str, attempt: u32) -> Duration {
        let max_millis =
            u64::try_from(self.config.delete_retry_max_jitter.as_millis()).unwrap_or(u64::MAX);
        if max_millis == 0 {
            return Duration::ZERO;
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        attempt.hash(&mut hasher);
        Duration::from_millis(hasher.finish() % max_millis.saturating_add(1))
    }

    fn set_phase(&self, generation: u64, phase: RemoteWriteSessionKvCleanupPhase) {
        if let Some(entry) = self.state.lock().entries.get_mut(&generation) {
            entry.phase = phase;
        }
    }

    fn finish_entry(&self, generation: u64) {
        if self.state.lock().entries.remove(&generation).is_some() {
            self.entries_changed.notify_waiters();
        }
    }

    pub(crate) fn stop_admission(&self) {
        self.state.lock().accepting = false;
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.state.lock().entries.len()
    }

    pub(crate) async fn wait_for_idle(&self, timeout: Duration) -> Result<(), String> {
        tokio::time::timeout(
            timeout,
            notify_state::wait_until(&self.entries_changed, || self.pending_count() == 0),
        )
        .await
        .map_err(|_| {
            let state = self.state.lock();
            let mut putting = 0_usize;
            let mut borrowed = 0_usize;
            let mut delete_pending = 0_usize;
            let mut lease_ids = std::collections::BTreeSet::new();
            for entry in state.entries.values() {
                lease_ids.insert(entry.lease_id);
                match entry.phase {
                    RemoteWriteSessionKvCleanupPhase::Putting => putting += 1,
                    RemoteWriteSessionKvCleanupPhase::Borrowed { .. } => borrowed += 1,
                    RemoteWriteSessionKvCleanupPhase::DeletePending { .. } => delete_pending += 1,
                }
            }
            let sample_key = state
                .entries
                .values()
                .next()
                .map(|entry| entry.key.as_str())
                .unwrap_or("none");
            format!(
                "write-session temporary KV cleanup did not drain within {}s: pending={} putting={} borrowed={} delete_pending={} leases={} sample_key={}",
                timeout.as_secs_f64(),
                state.entries.len(),
                putting,
                borrowed,
                delete_pending,
                lease_ids.len(),
                sample_key
            )
        })
    }

    pub(crate) fn request_stop_and_abort(&self) {
        self.stop_admission();
        self.stop.stop();
        for task in self.state.lock().tasks.iter() {
            task.abort();
        }
    }

    fn restore_tasks(&self, tasks: Vec<tokio::task::JoinHandle<()>>) {
        self.state.lock().tasks.extend(tasks);
    }

    pub(crate) async fn stop_join_and_abort(&self, timeout: Duration) -> Result<(), String> {
        self.stop_admission();
        self.stop.stop();
        let tasks = std::mem::take(&mut self.state.lock().tasks);
        let deadline = Instant::now() + timeout;
        let mut remaining = Vec::new();
        let mut tasks = tasks.into_iter();
        while let Some(mut task) = tasks.next() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                remaining.push(task);
                remaining.extend(tasks);
                break;
            }
            match tokio::time::timeout(left, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) if err.is_cancelled() => {}
                Ok(Err(err)) => tracing::warn!(
                    "write-session temporary KV cleanup task failed during shutdown: {}",
                    err
                ),
                Err(_) => {
                    remaining.push(task);
                    remaining.extend(tasks);
                    break;
                }
            }
        }
        if remaining.is_empty() {
            return self.joined_result();
        }

        for task in &remaining {
            task.abort();
        }
        let abort_deadline = Instant::now() + timeout;
        let mut remaining = remaining.into_iter();
        while let Some(mut task) = remaining.next() {
            let left = abort_deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                let mut unfinished = vec![task];
                unfinished.extend(remaining);
                self.restore_tasks(unfinished);
                return Err(
                    "write-session temporary KV cleanup task abort did not complete".to_string(),
                );
            }
            match tokio::time::timeout(left, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) if err.is_cancelled() => {}
                Ok(Err(err)) => tracing::warn!(
                    "write-session temporary KV cleanup task failed while aborting: {}",
                    err
                ),
                Err(_) => {
                    let mut unfinished = vec![task];
                    unfinished.extend(remaining);
                    self.restore_tasks(unfinished);
                    return Err(
                        "write-session temporary KV cleanup task abort timed out".to_string()
                    );
                }
            }
        }
        self.joined_result()
    }

    fn joined_result(&self) -> Result<(), String> {
        let pending = self.pending_count();
        if pending == 0 {
            Ok(())
        } else {
            Err(format!(
                "write-session temporary KV cleanup stopped with {} tracked entries",
                pending
            ))
        }
    }
}

struct RemoteWriteSessionKvCleanupEntryGuard {
    actor: std::sync::Weak<RemoteWriteSessionKvCleanupActor>,
    generation: u64,
}

impl Drop for RemoteWriteSessionKvCleanupEntryGuard {
    fn drop(&mut self) {
        if let Some(actor) = self.actor.upgrade() {
            actor.finish_entry(self.generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn test_config() -> RemoteWriteSessionKvCleanupConfig {
        RemoteWriteSessionKvCleanupConfig {
            max_tracked_keys: 8,
            put_resolution_timeout: Duration::from_secs(1),
            delete_operation_timeout: Duration::from_millis(100),
            delete_retry_initial: Duration::from_millis(1),
            delete_retry_max: Duration::from_millis(5),
            delete_retry_max_jitter: Duration::ZERO,
            uncertain_recheck: Duration::from_millis(5),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn committed_put_with_timed_out_response_is_reclaimed() {
        let values = Arc::new(Mutex::new(HashSet::<String>::new()));
        let delete_calls = Arc::new(AtomicUsize::new(0));
        let values_for_delete = values.clone();
        let delete_calls_for_op = delete_calls.clone();
        let delete: RemoteWriteSessionKvDeleteOperation = Arc::new(move |key| {
            let values = values_for_delete.clone();
            let delete_calls = delete_calls_for_op.clone();
            Box::pin(async move {
                delete_calls.fetch_add(1, Ordering::AcqRel);
                if values.lock().remove(&key) {
                    RemoteWriteSessionKvDeleteResult::Deleted
                } else {
                    RemoteWriteSessionKvDeleteResult::NotFound
                }
            })
        });
        let actor = Arc::new(RemoteWriteSessionKvCleanupActor::new_with_config(
            Handle::current(),
            delete,
            test_config(),
        ));
        let key = "temporary-key-a".to_string();
        let values_for_put = values.clone();
        let key_for_put = key.clone();
        let put = async move {
            values_for_put.lock().insert(key_for_put);
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(())
        };
        let mut ticket = actor.begin_put(key.clone(), 17, put).expect("begin put");

        let err = ticket
            .wait_for_put(Duration::from_millis(5))
            .await
            .expect_err("caller must time out before the delayed response");
        assert!(err.contains("KV put timed out"));
        assert!(
            values.lock().contains(&key),
            "the put committed before its response"
        );
        drop(ticket);

        actor
            .wait_for_idle(Duration::from_secs(1))
            .await
            .expect("supervisor must observe the late success and delete the key");
        assert!(!values.lock().contains(&key));
        assert_eq!(delete_calls.load(Ordering::Acquire), 1);
        actor
            .stop_join_and_abort(Duration::from_secs(1))
            .await
            .expect("stop cleanup actor");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_unknown_keeps_retrying_after_initial_not_found() {
        let values = Arc::new(Mutex::new(HashSet::<String>::new()));
        let committed = Arc::new(AtomicBool::new(false));
        let delete_calls = Arc::new(AtomicUsize::new(0));
        let first_delete = Arc::new(Notify::new());
        let values_for_delete = values.clone();
        let delete_calls_for_op = delete_calls.clone();
        let first_delete_for_op = first_delete.clone();
        let delete: RemoteWriteSessionKvDeleteOperation = Arc::new(move |key| {
            let values = values_for_delete.clone();
            let delete_calls = delete_calls_for_op.clone();
            let first_delete = first_delete_for_op.clone();
            Box::pin(async move {
                delete_calls.fetch_add(1, Ordering::AcqRel);
                if values.lock().remove(&key) {
                    RemoteWriteSessionKvDeleteResult::Deleted
                } else {
                    first_delete.notify_one();
                    RemoteWriteSessionKvDeleteResult::NotFound
                }
            })
        });
        let mut config = test_config();
        config.put_resolution_timeout = Duration::from_millis(10);
        let actor = Arc::new(RemoteWriteSessionKvCleanupActor::new_with_config(
            Handle::current(),
            delete,
            config,
        ));
        let key = "temporary-key-unknown".to_string();
        let values_for_commit = values.clone();
        let committed_for_task = committed.clone();
        let key_for_commit = key.clone();
        let put = async move {
            tokio::spawn(async move {
                first_delete.notified().await;
                values_for_commit.lock().insert(key_for_commit);
                committed_for_task.store(true, Ordering::Release);
            });
            std::future::pending::<Result<(), String>>().await
        };
        let mut ticket = actor.begin_put(key.clone(), 19, put).expect("begin put");
        ticket
            .wait_for_put(Duration::from_millis(2))
            .await
            .expect_err("caller must time out");
        drop(ticket);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if committed.load(Ordering::Acquire)
                    && !values.lock().contains(&key)
                    && delete_calls.load(Ordering::Acquire) >= 2
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("a post-NotFound commit must be reclaimed by a later retry");
        assert_eq!(
            actor.pending_count(),
            1,
            "commit-unknown records stay tracked until lease retirement"
        );
        actor.request_stop_and_abort();
        actor
            .stop_join_and_abort(Duration::from_secs(1))
            .await
            .expect("stop cleanup actor");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delete_failure_is_retried_until_reclaimed() {
        let values = Arc::new(Mutex::new(HashSet::<String>::new()));
        let delete_calls = Arc::new(AtomicUsize::new(0));
        let values_for_delete = values.clone();
        let delete_calls_for_op = delete_calls.clone();
        let delete: RemoteWriteSessionKvDeleteOperation = Arc::new(move |key| {
            let values = values_for_delete.clone();
            let delete_calls = delete_calls_for_op.clone();
            Box::pin(async move {
                let call = delete_calls.fetch_add(1, Ordering::AcqRel);
                if call == 0 {
                    return RemoteWriteSessionKvDeleteResult::Failed(
                        "injected delete failure".to_string(),
                    );
                }
                if values.lock().remove(&key) {
                    RemoteWriteSessionKvDeleteResult::Deleted
                } else {
                    RemoteWriteSessionKvDeleteResult::NotFound
                }
            })
        });
        let actor = Arc::new(RemoteWriteSessionKvCleanupActor::new_with_config(
            Handle::current(),
            delete,
            test_config(),
        ));
        let key = "temporary-key-b".to_string();
        let values_for_put = values.clone();
        let key_for_put = key.clone();
        let put = async move {
            values_for_put.lock().insert(key_for_put);
            Ok(())
        };
        let mut ticket = actor.begin_put(key.clone(), 18, put).expect("begin put");
        ticket
            .wait_for_put(Duration::from_millis(100))
            .await
            .expect("put response");
        drop(ticket);

        actor
            .wait_for_idle(Duration::from_secs(1))
            .await
            .expect("failed delete must remain tracked and retry");
        assert!(!values.lock().contains(&key));
        assert_eq!(delete_calls.load(Ordering::Acquire), 2);
        actor
            .stop_join_and_abort(Duration::from_secs(1))
            .await
            .expect("stop cleanup actor");
    }
}
