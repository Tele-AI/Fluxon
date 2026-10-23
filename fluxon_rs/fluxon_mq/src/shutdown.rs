use fluxon_util::notify_state::{self, AsyncStopSignal};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Shared shutdown controller used by MPSC components.
///
/// This mirrors the Python-side `MqShutdownCtl` in spirit: a single
/// close flag that can be shared across producer/consumer handles,
/// retry loops and background actors. Owners call `close()` to signal
/// shutdown; long-running operations periodically call
/// `is_closed()` to decide whether to exit early.
#[derive(Clone)]
pub struct ShutdownCtl {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl ShutdownCtl {
    /// Create a new shutdown controller in the open state.
    pub fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Mark this controller as closed.
    pub fn close(&self) {
        self.flag.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Check whether shutdown has been requested.
    pub fn is_closed(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub async fn wait_closed(&self) {
        notify_state::wait_until(&self.notify, || self.is_closed()).await;
    }

    /// Expose underlying flag for integration with external code that
    /// already uses `Arc<AtomicBool>`.
    pub fn flag(&self) -> Arc<AtomicBool> {
        self.flag.clone()
    }
}

impl AsyncStopSignal for ShutdownCtl {
    fn is_stopped(&self) -> bool {
        self.is_closed()
    }

    fn wait_stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.wait_closed())
    }
}

#[cfg(test)]
mod tests {
    use super::ShutdownCtl;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    #[tokio::test(flavor = "current_thread")]
    async fn wait_closed_returns_when_already_closed() {
        let shutdown = ShutdownCtl::new();
        shutdown.close();

        timeout(Duration::from_secs(1), shutdown.wait_closed())
            .await
            .expect("wait_closed blocked after close");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn wait_closed_wakes_after_close() {
        let shutdown = ShutdownCtl::new();
        let waiter_shutdown = shutdown.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let waiter = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            waiter_shutdown.wait_closed().await;
        });

        started_rx.await.unwrap();
        tokio::task::yield_now().await;
        shutdown.close();

        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("wait_closed ignored close")
            .expect("wait_closed task panicked");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_wakes_all_waiters() {
        let shutdown = ShutdownCtl::new();
        let first = shutdown.clone();
        let second = shutdown.clone();
        let first_waiter = tokio::spawn(async move { first.wait_closed().await });
        let second_waiter = tokio::spawn(async move { second.wait_closed().await });

        tokio::task::yield_now().await;
        shutdown.close();

        timeout(Duration::from_secs(1), first_waiter)
            .await
            .expect("first waiter ignored close")
            .expect("first waiter task panicked");
        timeout(Duration::from_secs(1), second_waiter)
            .await
            .expect("second waiter ignored close")
            .expect("second waiter task panicked");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn repeated_close_remains_observable() {
        let shutdown = ShutdownCtl::new();
        shutdown.close();
        shutdown.close();

        timeout(Duration::from_secs(1), shutdown.wait_closed())
            .await
            .expect("repeated close was not observable");
    }
}
