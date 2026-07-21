use fluxon_util::notify_state;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct ShutdownPoller {
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ShutdownPoller {
    pub fn new() -> Self {
        let res = Self {
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        tracing::debug!(
            "ShutdownPoller created with running=true, shutdown_ptr={:x}",
            res.ptr_addr()
        );
        res
    }

    pub fn is_running(&self) -> bool {
        let res = self.running.load(std::sync::atomic::Ordering::Acquire);
        if !res {
            tracing::info!(
                "ShutdownPoller: detected running=false, system shutting down, shutdown_ptr={:x}",
                self.ptr_addr()
            );
        }
        res
    }

    pub fn shutdown(&self) {
        tracing::info!(
            "ShutdownPoller: setting running to false, system shutting down, shutdown_ptr={:x}",
            self.ptr_addr()
        );
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
    }

    pub fn ptr_addr(&self) -> usize {
        Arc::as_ptr(&self.running) as usize
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
    quiesced: Notify,
}

impl ShutdownGate {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownGateInner {
                accepting: AtomicBool::new(true),
                active: AtomicUsize::new(0),
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
        self.inner.accepting.store(false, Ordering::Release);
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

pub struct ShutdownNotifier {
    sender: limit_thirdparty::tokio::sync::abroadcast::Sender<()>,
}

impl ShutdownNotifier {
    pub fn new() -> Self {
        Self {
            sender: limit_thirdparty::tokio::sync::abroadcast::channel(1).0,
        }
    }

    pub fn listen(&self) -> ShutdownWaiter {
        let rx = self.sender.subscribe();
        ShutdownWaiter { receiver: rx }
    }

    pub fn shutdown(&self) {
        if let Err(e) = self.sender.send(()) {
            tracing::warn!(
                err = ?e,
                "ShutdownNotifier::shutdown: failed to broadcast shutdown signal"
            );
            return;
        }
    }
}

pub struct ShutdownWaiter {
    receiver: limit_thirdparty::tokio::sync::abroadcast::Receiver<()>,
}

impl ShutdownWaiter {
    pub async fn wait(&mut self) {
        if let Err(e) = self.receiver.recv().await {
            tracing::warn!(
                err = ?e,
                "ShutdownWaiter::wait: failed to receive shutdown signal"
            );
            return;
        }
    }
    pub fn wait_sync(&mut self) {
        if let Err(e) = self.receiver.blocking_recv() {
            tracing::warn!(
                err = ?e,
                "ShutdownWaiter::wait_sync: failed to receive shutdown signal"
            );
            return;
        }
    }
}

pub trait ViewShutdownExt {
    fn register_shutdown_waiter(&self) -> ShutdownWaiter;
    fn register_shutdown_poller(&self) -> ShutdownPoller;
}

#[cfg(test)]
mod tests {
    use super::{ShutdownGate, ShutdownPoller};
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
}
