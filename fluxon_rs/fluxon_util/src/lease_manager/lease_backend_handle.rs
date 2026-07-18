use etcd_client::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use super::keepalive_actor::EtcdState;
use super::lease_backend_uid::{KvKeepaliveLease, LeaseBackendUid};
use super::lifecycle::debug_keepalive_log;
use crate::auto_clean_map::AutoCleanMap;
use crate::auto_clean_map::AutoCleanMapEntry;
use crate::etcd::PooledEtcdClient;

/// Backend resources actually used by keepalive actors.
///
/// Keep this separate from `LeaseBackendHandle` to avoid self-referential
/// types when the handle also carries the map guard (AutoCleanMapEntry).
pub enum LeaseBackendInner {
    Etcd {
        _endpoints: Vec<String>,
        /// Final-release owner that keeps this backend's pool entry alive.
        _pool_entry: PooledEtcdClient,
        client: Client,
        /// Per-lease keepalive state keyed by lease_id. Auto-evicts when the last
        /// guard (AutoCleanMapEntry) for that lease is dropped.
        states: AutoCleanMap<u64, Arc<Mutex<EtcdState>>>,
        /// Runtime handle to schedule background keepalive tasks.
        rt: tokio::runtime::Handle,
    },
    KvClient {
        _cluster: String,
        _instance_key: String,
        keepalive: KvKeepaliveLease,
        /// Runtime handle to schedule background tasks.
        rt: tokio::runtime::Handle,
    },
}

/// RAII handle that also holds the `AutoCleanMapEntry` guard of the backend map.
///
/// Dropping the last clone of this handle will drop the guard, which in turn
/// evicts the backend entry from the map (see `AutoCleanMapEntry::drop`).
pub struct LeaseBackendHandle {
    pub(crate) entry: AutoCleanMapEntry<LeaseBackendUid, LeaseBackendInner>,
}

impl Clone for LeaseBackendHandle {
    fn clone(&self) -> Self {
        Self {
            entry: self.entry.clone(),
        }
    }
}

impl LeaseBackendHandle {
    #[inline]
    pub(crate) fn from_entry(entry: AutoCleanMapEntry<LeaseBackendUid, LeaseBackendInner>) -> Self {
        Self { entry }
    }

    #[inline]
    pub fn etcd_client(&self) -> Option<Client> {
        match &*self.entry {
            LeaseBackendInner::Etcd { client, .. } => Some(client.clone()),
            _ => None,
        }
    }

    #[inline]
    pub fn kv_keepalive(&self) -> Option<KvKeepaliveLease> {
        match &*self.entry {
            LeaseBackendInner::KvClient { keepalive, .. } => Some(keepalive.clone()),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn ensure_etcd_state(
        &self,
        lease_id: u64,
        init: impl FnOnce() -> Arc<Mutex<EtcdState>>,
    ) -> crate::auto_clean_map::AutoCleanMapEntry<u64, Arc<Mutex<EtcdState>>> {
        match &*self.entry {
            LeaseBackendInner::Etcd { states, .. } => states.get_or_init(lease_id, init),
            _ => unreachable!("ensure_etcd_state called on non-etcd backend"),
        }
    }

    #[inline]
    pub(crate) fn get_etcd_state(&self, lease_id: u64) -> Option<Arc<Mutex<EtcdState>>> {
        if let LeaseBackendInner::Etcd { states, .. } = &*self.entry {
            states.with_existing(&lease_id, |arc| arc.clone())
        } else {
            None
        }
    }

    #[inline]
    pub fn runtime_handle(&self) -> tokio::runtime::Handle {
        match &*self.entry {
            LeaseBackendInner::Etcd { rt, .. } => rt.clone(),
            LeaseBackendInner::KvClient { rt, .. } => rt.clone(),
        }
    }

    /// Drive one keepalive tick according to backend kind.
    /// - KvClient: await the native Fluxon KV keepalive operation.
    /// - Etcd: lock the per-lease state and run `keepalive_once()`.
    pub(crate) async fn keepalive(&self, lease_id: u64) -> anyhow::Result<()> {
        match &*self.entry {
            LeaseBackendInner::KvClient { keepalive, .. } => {
                (keepalive)(lease_id).await?;
                super::lifecycle::debug_keepalive_log(lease_id, "kvclient lease keepalive tick");
                Ok(())
            }
            LeaseBackendInner::Etcd { .. } => {
                if let Some(state) = self.get_etcd_state(lease_id) {
                    let mut st = state.lock().await;
                    match tokio::time::timeout(
                        Duration::from_millis(super::keepalive_actor::KEEPALIVE_PER_TASK_BUDGET_MS),
                        st.keepalive_once(),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            drop(st);
                            debug_keepalive_log(lease_id as u64, "etcd lease keepalive tick");
                            Ok(())
                        }
                        Ok(Err(e)) => {
                            drop(st);
                            Err(e)
                        }
                        Err(_) => {
                            st.reset_stream();
                            drop(st);
                            Err(anyhow::anyhow!(
                                "etcd keepalive timed out for lease_id={}; reset keepalive stream",
                                lease_id
                            ))
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("etcd handle missing per-lease state"))
                }
            }
        }
    }
}

// Backend map and acquisition live in lifecycle.rs;
// this module only defines the handle/inner types and accessors.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborting_keepalive_drops_native_operation() {
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let started_in_operation = started.clone();
        let dropped_in_operation = dropped.clone();
        let keepalive: KvKeepaliveLease = Arc::new(move |_| {
            let started = started_in_operation.clone();
            let dropped = dropped_in_operation.clone();
            Box::pin(async move {
                let _probe = DropProbe(dropped);
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            })
        });
        let backend = LeaseBackendUid::kv_client(
            "native_keepalive_cancellation_test",
            "native_keepalive_cancellation_client",
            Arc::new(|_| Box::pin(async { Ok(1) })),
            keepalive.clone(),
        );
        let handle = super::super::lifecycle::acquire_backend_handle(
            backend,
            Some(keepalive),
            None,
            None,
            tokio::runtime::Handle::current(),
        );

        let task = tokio::spawn(async move { handle.keepalive(1).await });
        started.notified().await;
        task.abort();
        let join_result = task.await;

        assert!(
            join_result
                .expect_err("keepalive task must be cancelled")
                .is_cancelled()
        );
        assert!(
            dropped.load(Ordering::SeqCst),
            "aborting the join must drop the native keepalive future"
        );
    }
}
