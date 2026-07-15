use super::keepalive_actor::{EtcdState, LeaseKey, OneTtlKeepAliveInner};
use super::lease_backend_handle::LeaseBackendHandle;
use super::lease_backend_uid::{LeaseBackendUid, LeaseRegisterKind, LeaseType};
use crate::auto_clean_map::AutoCleanMapEntry;
use anyhow::Result;
use std::sync::Arc;

/// Keepalive entry kinds stored in the per-ttl registry.
pub enum LeaseEntryKind {
    // KvClient keepalive is driven by a backend handle carrying the closure.
    // Keepalive must only accept the lease id and must not mutate TTL.
    KvClient {
        handle: LeaseBackendHandle,
    },
    // Etcd keepalive uses per-lease EtcdState stored inside the backend handle;
    // `revoke_on_drop` only influences drop behavior.
    Etcd {
        handle: LeaseBackendHandle,
        revoke_on_drop: bool,
    },
}

pub(crate) struct LeaseEntry {
    // No ref_count: user-side LeaseHandle/GeneralLease Drop must drive
    // unregister, so a single logical registration corresponds to a single
    // table entry. Duplicate registrations for the same key are treated as a
    // logic error and ignored (we only keep the first one).
    pub(crate) kind: LeaseEntryKind,
    // Guard of `actor_map(): AutoCleanMap<i64, Arc<OneTtlKeepAliveInner>>`, keyed by `ttl_seconds`.
    // Holding this keeps the per-ttl actor (`OneTtlKeepAliveInner`) alive while entries exist.
    pub(crate) _actor_guard: AutoCleanMapEntry<i64, Arc<OneTtlKeepAliveInner>>,
    pub(crate) key: LeaseKey,
    // Present only for Etcd entries. This is the guard of
    // `LeaseBackendInner::Etcd::states: AutoCleanMap<u64, Arc<tokio::sync::Mutex<EtcdState>>>`,
    // keyed by this lease's id. Dropping this guard removes the corresponding
    // entry from that backend `states` map.
    pub(crate) _etcd_state_guard:
        Option<AutoCleanMapEntry<u64, Arc<tokio::sync::Mutex<EtcdState>>>>,
}

/// RAII lease handle used by Python bindings.
pub enum GeneralLease {
    // Etcd leases store backend uid and registry entry
    Etcd {
        id: u64,
        backend_uid: LeaseBackendUid,
        entry: AutoCleanMapEntry<LeaseKey, LeaseEntry>,
    },
    // KvClient leases share the TTL actor table; store only the registry entry
    KvClient {
        id: u64,
        backend_uid: LeaseBackendUid,
        entry: AutoCleanMapEntry<LeaseKey, LeaseEntry>,
    },
}

impl GeneralLease {
    pub fn id(&self) -> u64 {
        match self {
            GeneralLease::Etcd { id, .. } | GeneralLease::KvClient { id, .. } => *id,
        }
    }
    pub fn kind(&self) -> LeaseType {
        match self {
            GeneralLease::Etcd { .. } => LeaseType::Etcd,
            GeneralLease::KvClient { .. } => LeaseType::KvClient,
        }
    }
}

impl Drop for GeneralLease {
    fn drop(&mut self) {
        // Instrument drop of the high-level lease handle so we can correlate
        // who released the last user-visible handle.
        let lease_id = self.id();
        let kind_str = match self.kind() {
            LeaseType::Etcd => "Etcd",
            LeaseType::KvClient => "KvClient",
        };
        let label = super::lifecycle::get_register_by(lease_id);
        let bt = std::backtrace::Backtrace::force_capture();
        tracing::info!(
            lease_id,
            kind = kind_str,
            label = %label.clone().unwrap_or_else(|| "".to_string()),
            backtrace = %format!("{:?}", bt),
            "GeneralLease drop: releasing user-visible lease handle",
        );
        // AutoCleanMapEntry drop happens after this method returns; the map
        // entry removal and LeaseEntry Drop will log its own unregistration.
    }
}

/// Stateless facade over the process-wide lease registries.
///
/// Etcd backend identity is carried explicitly by `LeaseBackendUid`; client
/// lifetime and reuse are owned by the managed etcd client registry.
#[derive(Clone, Default)]
pub struct LeaseManager;

// Expose a global zero-sized lease manager for convenience.
pub static GLOBAL_LM: LeaseManager = LeaseManager;

impl LeaseManager {
    pub fn new() -> Self {
        Self
    }

    /// Unified keepalive entrypoint: etcd leases go through the async keepalive
    /// pipeline; kvclient leases are registered into the same TTL actor with a
    /// Rust callback carried by the backend uid.
    pub async fn register_lease_for_keepalive(
        &self,
        backend_uid: LeaseBackendUid,
        ttl_seconds: i64,
        lease_id: u64,
        kind: LeaseRegisterKind,
        rt: tokio::runtime::Handle,
    ) -> Result<GeneralLease> {
        super::lifecycle::register_lease_for_keepalive(backend_uid, ttl_seconds, lease_id, kind, rt)
            .await
    }

    /// Allocate a kvclient lease id via the per-backend allocator closure
    /// stored inside `LeaseBackendUid::KvClientWithCallbacks`.
    ///
    /// The callback may bridge into Python and block waiting for a result, so
    /// we always isolate it in Tokio's blocking pool.
    pub async fn allocate_kvclient_lease(
        &self,
        backend_uid: LeaseBackendUid,
        ttl_seconds: i64,
    ) -> Result<u64> {
        match backend_uid.kind() {
            super::lease_backend_uid::LeaseType::KvClient => {
                let cluster = backend_uid
                    .cluster()
                    .expect("kvclient backend missing cluster");
                let cb = backend_uid.kv_allocate_cb().ok_or_else(|| {
                    anyhow::anyhow!(
                        "kvclient allocate callback missing in LeaseBackendUid for cluster={}; construct kv backend via kv_client_with_callbacks()",
                        cluster
                    )
                })?;
                match limit_thirdparty::tokio::task::spawn_blocking(move || cb(ttl_seconds)).await {
                    Ok(Ok(id)) => Ok(id),
                    Ok(Err(err)) => Err(err),
                    Err(join_err) => Err(anyhow::anyhow!(
                        "spawn_blocking join failed while allocating kvclient lease for cluster={}: {:?}",
                        cluster,
                        join_err
                    )),
                }
            }
            super::lease_backend_uid::LeaseType::Etcd => {
                anyhow::bail!("allocate_kvclient_lease requires KvClient backend uid")
            }
        }
    }
}
