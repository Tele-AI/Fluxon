use anyhow::Result as AnyResult;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use super::keepalive_actor::EtcdState;
use super::lease_backend_uid::LeaseBackendUid;
use super::lifecycle::debug_keepalive_log;
use crate::auto_clean_map::AutoCleanMap;
use crate::auto_clean_map::AutoCleanMapEntry;
use crate::etcd::ManagedEtcdClient;

/// Backend resources actually used by keepalive actors.
///
/// Keep this separate from `LeaseBackendHandle` to avoid self-referential
/// types when the handle also carries the map guard (AutoCleanMapEntry).
pub enum LeaseBackendInner {
    Etcd {
        client: ManagedEtcdClient,
        /// Per-lease keepalive state keyed by lease_id. Auto-evicts when the last
        /// guard (AutoCleanMapEntry) for that lease is dropped.
        states: AutoCleanMap<u64, Arc<Mutex<EtcdState>>>,
        /// Runtime handle to schedule background tasks (keepalive/revoke).
        rt: tokio::runtime::Handle,
    },
    KvClient {
        _cluster: String,
        /// Keepalive closure: input lease_id; must not alter TTL.
        keepalive_cb: Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>,
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
    pub fn managed_etcd_client(&self) -> Option<ManagedEtcdClient> {
        match &*self.entry {
            LeaseBackendInner::Etcd { client, .. } => Some(client.clone()),
            _ => None,
        }
    }

    #[inline]
    pub fn kv_keepalive_cb(
        &self,
    ) -> Option<Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>> {
        match &*self.entry {
            LeaseBackendInner::KvClient { keepalive_cb, .. } => Some(keepalive_cb.clone()),
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
    /// - KvClient: invoke the keepalive callback with `lease_id`.
    /// - Etcd: lock the per-lease state and run `keepalive_once()`.
    pub(crate) async fn keepalive(&self, lease_id: u64) -> AnyResult<()> {
        match &*self.entry {
            LeaseBackendInner::KvClient { keepalive_cb, .. } => {
                // Execute the Python-bridged callback on a blocking thread.
                // Rationale: the callback typically enters the PyO3 layer
                // (fluxon_pyo3), which then bridges back into Rust and performs
                // a blocking wait on the underlying runtime. Running such code
                // directly on a Tokio worker thread would cause a panic, so we
                // consistently use `spawn_blocking` here to isolate this call
                // in the dedicated blocking thread pool.
                let cb = keepalive_cb.clone();
                match limit_thirdparty::tokio::task::spawn_blocking(move || (cb)(lease_id)).await {
                    Ok(Ok(())) => {
                        super::lifecycle::debug_keepalive_log(
                            lease_id as u64,
                            "kvclient lease keepalive tick",
                        );
                        Ok(())
                    }
                    Ok(Err(err)) => {
                        // Return as error so caller can classify (e.g., Unreachable) and log.
                        Err(err)
                    }
                    Err(join_err) => {
                        // Propagate join error as failure.
                        Err(anyhow::anyhow!(
                            "spawn_blocking join failed: {:?}",
                            join_err
                        ))
                    }
                }
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
