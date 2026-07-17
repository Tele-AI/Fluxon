use std::future::Future;
use std::pin::Pin;
use tokio::sync::Notify;

/// A persistent asynchronous stop signal.
///
/// `is_stopped` is authoritative. `wait_stopped` must be cancellation-safe and
/// race-safe when stop occurs before or during waiter registration.
pub trait AsyncStopSignal: Clone + Send + Sync + 'static {
    fn is_stopped(&self) -> bool;

    fn wait_stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[must_use]
pub enum NotifyStateWaitOutcome {
    Ready,
    Stopped,
}

/// Wait until a synchronous persistent-state predicate becomes true.
///
/// The predicate must be side-effect free and safe to call repeatedly.
pub async fn wait_until<P>(notify: &Notify, mut predicate: P)
where
    P: FnMut() -> bool,
{
    loop {
        if predicate() {
            return;
        }

        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if predicate() {
            return;
        }

        notified.await;
    }
}

/// Wait until a synchronous persistent-state predicate becomes true or stop wins.
///
/// Stop has priority when both outcomes become observable together. The
/// predicate must be side-effect free and safe to call repeatedly.
pub async fn wait_until_or_stopped<S, P>(
    notify: &Notify,
    stop: &S,
    mut predicate: P,
) -> NotifyStateWaitOutcome
where
    S: AsyncStopSignal,
    P: FnMut() -> bool,
{
    loop {
        if stop.is_stopped() {
            return NotifyStateWaitOutcome::Stopped;
        }
        if predicate() {
            return NotifyStateWaitOutcome::Ready;
        }

        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        if stop.is_stopped() {
            return NotifyStateWaitOutcome::Stopped;
        }
        if predicate() {
            return NotifyStateWaitOutcome::Ready;
        }

        tokio::select! {
            biased;
            _ = stop.wait_stopped() => return NotifyStateWaitOutcome::Stopped,
            _ = &mut notified => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncStopSignal, NotifyStateWaitOutcome, wait_until, wait_until_or_stopped};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Notify, oneshot};
    use tokio::time::timeout;

    #[derive(Clone)]
    struct TestStopSignal {
        stopped: Arc<AtomicBool>,
        notify: Arc<Notify>,
    }

    impl TestStopSignal {
        fn new() -> Self {
            Self {
                stopped: Arc::new(AtomicBool::new(false)),
                notify: Arc::new(Notify::new()),
            }
        }

        fn stop(&self) {
            self.stopped.store(true, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    impl AsyncStopSignal for TestStopSignal {
        fn is_stopped(&self) -> bool {
            self.stopped.load(Ordering::SeqCst)
        }

        fn wait_stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            let stopped = self.stopped.clone();
            let notify = self.notify.clone();
            Box::pin(async move {
                wait_until(&notify, || stopped.load(Ordering::SeqCst)).await;
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_returns_when_already_ready() {
        let notify = Notify::new();

        timeout(Duration::from_secs(1), wait_until(&notify, || true))
            .await
            .expect("already-ready state wait blocked");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_closes_transition_before_waiter_creation() {
        let notify = Notify::new();
        let ready = AtomicBool::new(false);
        let checks = AtomicUsize::new(0);

        timeout(
            Duration::from_secs(1),
            wait_until(&notify, || {
                let observed = ready.load(Ordering::SeqCst);
                if checks.fetch_add(1, Ordering::SeqCst) == 0 {
                    ready.store(true, Ordering::SeqCst);
                    notify.notify_waiters();
                }
                observed
            }),
        )
        .await
        .expect("state transition before waiter creation was lost");

        assert!(checks.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_wakes_after_waiter_is_armed() {
        let notify = Arc::new(Notify::new());
        let ready = Arc::new(AtomicBool::new(false));
        let waiter_notify = notify.clone();
        let waiter_ready = ready.clone();
        let (armed_tx, armed_rx) = oneshot::channel();
        let waiter = tokio::spawn(async move {
            let mut checks = 0usize;
            let mut armed_tx = Some(armed_tx);
            wait_until(&waiter_notify, || {
                checks += 1;
                if checks == 2 {
                    armed_tx.take().unwrap().send(()).unwrap();
                }
                waiter_ready.load(Ordering::SeqCst)
            })
            .await;
        });

        armed_rx.await.unwrap();
        ready.store(true, Ordering::SeqCst);
        notify.notify_waiters();

        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("armed state waiter ignored notification")
            .expect("armed state waiter task panicked");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_rechecks_after_spurious_notification() {
        let notify = Arc::new(Notify::new());
        let ready = Arc::new(AtomicBool::new(false));
        let waiter_notify = notify.clone();
        let waiter_ready = ready.clone();
        let waiter = tokio::spawn(async move {
            wait_until(&waiter_notify, || waiter_ready.load(Ordering::SeqCst)).await;
        });

        tokio::task::yield_now().await;
        notify.notify_waiters();
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        ready.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("state waiter did not finish after the real transition")
            .expect("state waiter task panicked");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_wakes_all_broadcast_waiters() {
        let notify = Arc::new(Notify::new());
        let ready = Arc::new(AtomicBool::new(false));
        let first_notify = notify.clone();
        let first_ready = ready.clone();
        let first = tokio::spawn(async move {
            wait_until(&first_notify, || first_ready.load(Ordering::SeqCst)).await;
        });
        let second_notify = notify.clone();
        let second_ready = ready.clone();
        let second = tokio::spawn(async move {
            wait_until(&second_notify, || second_ready.load(Ordering::SeqCst)).await;
        });

        tokio::task::yield_now().await;
        ready.store(true, Ordering::SeqCst);
        notify.notify_waiters();

        timeout(Duration::from_secs(1), first)
            .await
            .expect("first broadcast waiter ignored notification")
            .expect("first broadcast waiter task panicked");
        timeout(Duration::from_secs(1), second)
            .await
            .expect("second broadcast waiter ignored notification")
            .expect("second broadcast waiter task panicked");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_or_stopped_returns_ready() {
        let notify = Notify::new();
        let stop = TestStopSignal::new();

        let outcome = timeout(
            Duration::from_secs(1),
            wait_until_or_stopped(&notify, &stop, || true),
        )
        .await
        .expect("already-ready state wait blocked");

        assert_eq!(outcome, NotifyStateWaitOutcome::Ready);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_or_stopped_returns_when_already_stopped() {
        let notify = Notify::new();
        let stop = TestStopSignal::new();
        stop.stop();

        let outcome = timeout(
            Duration::from_secs(1),
            wait_until_or_stopped(&notify, &stop, || false),
        )
        .await
        .expect("already-stopped state wait blocked");

        assert_eq!(outcome, NotifyStateWaitOutcome::Stopped);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_or_stopped_observes_repeated_stop() {
        let notify = Notify::new();
        let stop = TestStopSignal::new();
        stop.stop();
        stop.stop();

        let outcome = timeout(
            Duration::from_secs(1),
            wait_until_or_stopped(&notify, &stop, || false),
        )
        .await
        .expect("repeated stop was not observable");

        assert_eq!(outcome, NotifyStateWaitOutcome::Stopped);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_or_stopped_is_interrupted_by_stop() {
        let notify = Arc::new(Notify::new());
        let stop = TestStopSignal::new();
        let waiter_notify = notify.clone();
        let waiter_stop = stop.clone();
        let waiter = tokio::spawn(async move {
            wait_until_or_stopped(&waiter_notify, &waiter_stop, || false).await
        });

        tokio::task::yield_now().await;
        stop.stop();

        let outcome = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("state wait ignored stop")
            .expect("state wait task panicked");
        assert_eq!(outcome, NotifyStateWaitOutcome::Stopped);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_until_or_stopped_prefers_stop_over_ready() {
        let notify = Arc::new(Notify::new());
        let ready = Arc::new(AtomicBool::new(false));
        let stop = TestStopSignal::new();
        let waiter_notify = notify.clone();
        let waiter_ready = ready.clone();
        let waiter_stop = stop.clone();
        let waiter = tokio::spawn(async move {
            wait_until_or_stopped(&waiter_notify, &waiter_stop, || {
                waiter_ready.load(Ordering::SeqCst)
            })
            .await
        });

        tokio::task::yield_now().await;
        ready.store(true, Ordering::SeqCst);
        stop.stop();
        notify.notify_waiters();

        let outcome = timeout(Duration::from_secs(1), waiter)
            .await
            .expect("state wait did not resolve after stop and ready")
            .expect("state wait task panicked");
        assert_eq!(outcome, NotifyStateWaitOutcome::Stopped);
    }
}
