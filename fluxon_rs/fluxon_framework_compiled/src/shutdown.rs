use fluxon_util::notify_state::{self, AsyncStopSignal};
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Notify, watch};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreShutdownRequest {
    Open,
    Requested,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PreShutdownProgress {
    Pending,
    AttemptFailed { attempt: u64, detail: String },
    Finished,
}

struct PreShutdownEntry {
    name: String,
    request_tx: watch::Sender<PreShutdownRequest>,
    progress_rx: watch::Receiver<PreShutdownProgress>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreShutdownBarrierPhase {
    Open,
    Requested,
    Finished,
}

struct PreShutdownBarrierState {
    phase: PreShutdownBarrierPhase,
    next_id: u64,
    entries: BTreeMap<u64, PreShutdownEntry>,
}

/// Coordinates dependent frameworks before this framework starts module shutdown.
///
/// A participant owns its cleanup future independently from the caller of
/// [`PreShutdownBarrier::request_and_wait`]. If one synchronous wait observes a failed cleanup
/// attempt, the participant can keep running and a later wait observes the next attempt or final
/// completion. Registration closes atomically when the first shutdown request starts.
pub struct PreShutdownBarrier {
    state: Mutex<PreShutdownBarrierState>,
}

impl PreShutdownBarrier {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(PreShutdownBarrierState {
                phase: PreShutdownBarrierPhase::Open,
                next_id: 1,
                entries: BTreeMap::new(),
            }),
        }
    }

    pub fn register(
        &self,
        name: impl Into<String>,
    ) -> Result<PreShutdownParticipant, PreShutdownError> {
        let name = name.into();
        let mut state = self.state.lock();
        if state.phase != PreShutdownBarrierPhase::Open {
            return Err(PreShutdownError::new(format!(
                "framework pre-shutdown already started; cannot register dependent {name}"
            )));
        }
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .expect("pre-shutdown participant id overflow");
        let (request_tx, request_rx) = watch::channel(PreShutdownRequest::Open);
        let (progress_tx, progress_rx) = watch::channel(PreShutdownProgress::Pending);
        state.entries.insert(
            id,
            PreShutdownEntry {
                name: name.clone(),
                request_tx,
                progress_rx,
            },
        );
        Ok(PreShutdownParticipant {
            name,
            request_rx,
            progress_tx,
            next_attempt: 1,
            finished: false,
        })
    }

    /// Publish shutdown intent without waiting for dependent completion.
    ///
    /// Returns whether the caller may stop its own framework immediately. A framework with
    /// registered dependents must keep its services alive until `request_and_wait()` observes all
    /// completion acknowledgements.
    pub fn request(&self) -> bool {
        let (request_senders, may_stop_framework) = {
            let mut state = self.state.lock();
            if state.phase == PreShutdownBarrierPhase::Open {
                state.phase = PreShutdownBarrierPhase::Requested;
            }
            (
                state
                    .entries
                    .values()
                    .map(|entry| entry.request_tx.clone())
                    .collect::<Vec<_>>(),
                state.entries.is_empty() || state.phase == PreShutdownBarrierPhase::Finished,
            )
        };
        for request_tx in request_senders {
            request_tx.send_replace(PreShutdownRequest::Requested);
        }
        may_stop_framework
    }

    /// Request every registered dependent to close and wait for one new result from each.
    ///
    /// A failed attempt is returned to the synchronous caller, while the participant retains
    /// authority to retry. Calling this method again waits past the already-observed failure.
    pub async fn request_and_wait(&self) -> Result<(), PreShutdownError> {
        let waits = {
            let mut state = self.state.lock();
            if state.phase == PreShutdownBarrierPhase::Finished {
                return Ok(());
            }
            if state.phase == PreShutdownBarrierPhase::Open {
                state.phase = PreShutdownBarrierPhase::Requested;
            }
            state
                .entries
                .values()
                .map(|entry| {
                    let seen_attempt = match &*entry.progress_rx.borrow() {
                        PreShutdownProgress::AttemptFailed { attempt, .. } => *attempt,
                        PreShutdownProgress::Pending | PreShutdownProgress::Finished => 0,
                    };
                    (
                        entry.name.clone(),
                        entry.request_tx.clone(),
                        entry.progress_rx.clone(),
                        seen_attempt,
                    )
                })
                .collect::<Vec<_>>()
        };

        for (_, request_tx, _, _) in &waits {
            request_tx.send_replace(PreShutdownRequest::Requested);
        }

        let results = futures::future::join_all(waits.into_iter().map(
            |(name, _, progress_rx, seen_attempt)| async move {
                wait_for_pre_shutdown_progress(name, progress_rx, seen_attempt).await
            },
        ))
        .await;
        let errors = results
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        if !errors.is_empty() {
            return Err(PreShutdownError::new(errors.join("; ")));
        }

        self.state.lock().phase = PreShutdownBarrierPhase::Finished;
        Ok(())
    }
}

impl Default for PreShutdownBarrier {
    fn default() -> Self {
        Self::new()
    }
}

async fn wait_for_pre_shutdown_progress(
    name: String,
    mut progress_rx: watch::Receiver<PreShutdownProgress>,
    seen_attempt: u64,
) -> Result<(), String> {
    loop {
        let progress = progress_rx.borrow().clone();
        match progress {
            PreShutdownProgress::Pending => {}
            PreShutdownProgress::AttemptFailed { attempt, detail } if attempt > seen_attempt => {
                return Err(format!(
                    "dependent {name} pre-shutdown attempt {attempt} failed: {detail}"
                ));
            }
            PreShutdownProgress::AttemptFailed { .. } => {}
            PreShutdownProgress::Finished => return Ok(()),
        }
        if progress_rx.changed().await.is_err() {
            return Err(format!(
                "dependent {name} pre-shutdown owner stopped before completion"
            ));
        }
    }
}

/// The sole completion publisher for one dependent framework.
pub struct PreShutdownParticipant {
    name: String,
    request_rx: watch::Receiver<PreShutdownRequest>,
    progress_tx: watch::Sender<PreShutdownProgress>,
    next_attempt: u64,
    finished: bool,
}

impl PreShutdownParticipant {
    pub async fn wait_requested(&mut self) -> Result<(), PreShutdownError> {
        loop {
            if *self.request_rx.borrow_and_update() == PreShutdownRequest::Requested {
                return Ok(());
            }
            if self.request_rx.changed().await.is_err() {
                return Err(PreShutdownError::new(format!(
                    "framework dropped before requesting dependent {} pre-shutdown",
                    self.name
                )));
            }
        }
    }

    /// Publish one recoverable attempt failure while retaining cleanup authority.
    pub fn report_attempt_failure(&mut self, detail: impl Into<String>) {
        assert!(
            !self.finished,
            "finished pre-shutdown participant reported failure"
        );
        let attempt = self.next_attempt;
        self.next_attempt = self
            .next_attempt
            .checked_add(1)
            .expect("pre-shutdown attempt counter overflow");
        self.progress_tx
            .send_replace(PreShutdownProgress::AttemptFailed {
                attempt,
                detail: detail.into(),
            });
    }

    pub fn finish(mut self) {
        assert!(!self.finished, "pre-shutdown participant finished twice");
        self.progress_tx.send_replace(PreShutdownProgress::Finished);
        self.finished = true;
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct PreShutdownError {
    detail: String,
}

impl PreShutdownError {
    fn new(detail: String) -> Self {
        Self { detail }
    }
}

/// Canonical persistent shutdown signal shared by framework pollers and waiters.
#[derive(Debug, Clone)]
pub struct ShutdownPoller {
    inner: Arc<ShutdownSignalInner>,
}

#[derive(Debug)]
struct ShutdownSignalInner {
    running: AtomicBool,
    changed: Notify,
    sync_transition: Mutex<()>,
    sync_changed: Condvar,
}

impl ShutdownPoller {
    pub fn new() -> Self {
        let res = Self {
            inner: Arc::new(ShutdownSignalInner {
                running: AtomicBool::new(true),
                changed: Notify::new(),
                sync_transition: Mutex::new(()),
                sync_changed: Condvar::new(),
            }),
        };
        tracing::debug!(
            "ShutdownPoller created with running=true, shutdown_ptr={:x}",
            res.ptr_addr()
        );
        res
    }

    pub fn is_running(&self) -> bool {
        let res = self.inner.running.load(Ordering::Acquire);
        if !res {
            tracing::info!(
                "ShutdownPoller: detected running=false, system shutting down, shutdown_ptr={:x}",
                self.ptr_addr()
            );
        }
        res
    }

    pub fn shutdown(&self) {
        let _transition = self.inner.sync_transition.lock();
        if self.inner.running.swap(false, Ordering::AcqRel) {
            tracing::info!(
                "ShutdownPoller: setting running to false, system shutting down, shutdown_ptr={:x}",
                self.ptr_addr()
            );
            self.inner.changed.notify_waiters();
            self.inner.sync_changed.notify_all();
        }
    }

    pub async fn wait_stopped(&self) {
        notify_state::wait_until(&self.inner.changed, || {
            !self.inner.running.load(Ordering::Acquire)
        })
        .await;
    }

    pub fn wait_stopped_sync(&self) {
        let mut transition = self.inner.sync_transition.lock();
        while self.inner.running.load(Ordering::Acquire) {
            self.inner.sync_changed.wait(&mut transition);
        }
    }

    pub fn waiter(&self) -> ShutdownWaiter {
        ShutdownWaiter {
            signal: self.clone(),
        }
    }

    pub fn ptr_addr(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }
}

impl Default for ShutdownPoller {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncStopSignal for ShutdownPoller {
    fn is_stopped(&self) -> bool {
        !self.inner.running.load(Ordering::Acquire)
    }

    fn wait_stopped(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(ShutdownPoller::wait_stopped(self))
    }
}

/// A module-scoped admission and quiescence barrier for graceful shutdown.
///
/// Unlike [`ShutdownPoller`], a gate can stop one module while framework
/// dependencies remain available for that module's cleanup.
#[derive(Clone, Debug)]
pub struct ShutdownGate {
    inner: Arc<ShutdownGateInner>,
}

#[derive(Debug)]
struct ShutdownGateInner {
    accepting: AtomicBool,
    active: AtomicUsize,
    stopped: Notify,
    quiesced: Notify,
}

impl ShutdownGate {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownGateInner {
                accepting: AtomicBool::new(true),
                active: AtomicUsize::new(0),
                stopped: Notify::new(),
                quiesced: Notify::new(),
            }),
        }
    }

    /// Enter an operation if this module still accepts work.
    pub fn try_guard(&self) -> Option<ShutdownGuard> {
        if !self.is_accepting() {
            return None;
        }

        self.inner.active.fetch_add(1, Ordering::AcqRel);
        if self.is_accepting() {
            Some(ShutdownGuard { gate: self.clone() })
        } else {
            self.leave();
            None
        }
    }

    /// Enter only while both the framework and this module accept work.
    pub fn try_guard_while_running(&self, poller: &ShutdownPoller) -> Option<ShutdownGuard> {
        if !poller.is_running() {
            return None;
        }
        let guard = self.try_guard()?;
        if poller.is_running() {
            Some(guard)
        } else {
            drop(guard);
            None
        }
    }

    pub fn is_accepting(&self) -> bool {
        self.inner.accepting.load(Ordering::Acquire)
    }

    /// Reject future guards without waiting for active guards to leave.
    pub fn stop_admission(&self) {
        if self.inner.accepting.swap(false, Ordering::AcqRel) {
            self.inner.stopped.notify_waiters();
        }
    }

    pub async fn wait_stopped(&self) {
        notify_state::wait_until(&self.inner.stopped, || !self.is_accepting()).await;
    }

    /// Wait until every guard admitted before shutdown has been dropped.
    pub async fn wait_for_quiescence(&self) {
        notify_state::wait_until(&self.inner.quiesced, || {
            self.inner.active.load(Ordering::Acquire) == 0
        })
        .await;
    }

    pub async fn stop_admission_and_wait(&self) {
        self.stop_admission();
        self.wait_for_quiescence().await;
    }

    fn leave(&self) {
        let previous = self.inner.active.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "ShutdownGate active guard count underflow");
        if previous == 1 {
            self.inner.quiesced.notify_waiters();
        }
    }
}

impl Default for ShutdownGate {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncStopSignal for ShutdownGate {
    fn is_stopped(&self) -> bool {
        !self.is_accepting()
    }

    fn wait_stopped(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(ShutdownGate::wait_stopped(self))
    }
}

/// Keeps one admitted operation inside a [`ShutdownGate`].
#[derive(Debug)]
#[must_use = "dropping ShutdownGuard marks the admitted operation complete"]
pub struct ShutdownGuard {
    gate: ShutdownGate,
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.gate.leave();
    }
}

#[derive(Clone, Debug)]
pub struct ShutdownWaiter {
    signal: ShutdownPoller,
}

impl ShutdownWaiter {
    pub async fn wait(&mut self) {
        self.signal.wait_stopped().await;
    }

    pub fn wait_sync(&mut self) {
        self.signal.wait_stopped_sync();
    }
}

pub trait ViewShutdownExt {
    fn register_shutdown_waiter(&self) -> ShutdownWaiter;
    fn register_shutdown_poller(&self) -> ShutdownPoller;
}

#[cfg(test)]
mod tests {
    use super::{PreShutdownBarrier, ShutdownGate, ShutdownPoller};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn gate_waits_for_active_guards_and_is_repeatable() {
        let gate = ShutdownGate::new();
        let first = gate.try_guard().expect("gate must initially accept work");
        let second = gate
            .try_guard()
            .expect("gate does not serialize operations");

        gate.stop_admission();
        assert!(gate.try_guard().is_none());
        tokio::time::timeout(Duration::from_secs(1), gate.wait_stopped())
            .await
            .expect("gate stop wait must observe prior admission stop");
        let wait = gate.wait_for_quiescence();
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), wait.as_mut())
                .await
                .is_err()
        );

        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), wait.as_mut())
                .await
                .is_err()
        );
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("last guard drop must release shutdown wait");
        gate.stop_admission_and_wait().await;
    }

    #[test]
    fn gate_can_include_framework_poller_state() {
        let gate = ShutdownGate::new();
        let poller = ShutdownPoller::new();
        drop(gate.try_guard_while_running(&poller).unwrap());

        poller.shutdown();
        assert!(gate.try_guard_while_running(&poller).is_none());
    }

    #[tokio::test]
    async fn waiter_registered_after_shutdown_returns() {
        let signal = ShutdownPoller::new();
        signal.shutdown();
        let mut waiter = signal.waiter();

        tokio::time::timeout(Duration::from_secs(1), waiter.wait())
            .await
            .expect("late shutdown waiter blocked");
    }

    #[tokio::test]
    async fn waiter_wakes_when_shutdown_is_published() {
        let signal = ShutdownPoller::new();
        let mut waiter = signal.waiter();
        let waiting = tokio::spawn(async move { waiter.wait().await });
        tokio::task::yield_now().await;

        signal.shutdown();
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("shutdown did not wake waiter")
            .expect("join waiter");
    }

    #[test]
    fn synchronous_waiter_observes_prior_shutdown() {
        let signal = ShutdownPoller::new();
        signal.shutdown();
        signal.wait_stopped_sync();
    }

    #[tokio::test]
    async fn pre_shutdown_retains_owner_across_failed_attempt() {
        let barrier = Arc::new(PreShutdownBarrier::new());
        let mut participant = barrier.register("fs").expect("register FS dependent");
        let (continue_tx, continue_rx) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            participant
                .wait_requested()
                .await
                .expect("receive pre-shutdown request");
            participant.report_attempt_failure("holder barrier timed out");
            let _ = continue_rx.await;
            participant.finish();
        });

        let first = barrier
            .request_and_wait()
            .await
            .expect_err("first attempt must surface its error");
        assert!(first.to_string().contains("holder barrier timed out"));
        assert!(barrier.register("late").is_err());

        let _ = continue_tx.send(());
        barrier
            .request_and_wait()
            .await
            .expect("same owner must publish eventual completion");
        owner.await.expect("pre-shutdown owner task");
    }
}
