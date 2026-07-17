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
    KvClient { handle: LeaseBackendHandle },
    // Etcd keepalive uses per-lease EtcdState stored inside the backend handle.
    // Dropping the entry only unregisters local keepalive; it never revokes.
    Etcd { handle: LeaseBackendHandle },
}

pub(crate) struct LeaseEntry {
    // No separate counter: user-side LeaseHandle/GeneralLease Drop releases its
    // AutoCleanMapEntry guard. Registrations for the same key reuse the same
    // table entry; the entry is removed after the last guard is dropped.
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
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }

        // Backtraces are useful for lifecycle diagnostics but are expensive in
        // a large MPMC teardown, so capture them only when debug logging is on.
        let lease_id = self.id();
        let kind_str = match self.kind() {
            LeaseType::Etcd => "Etcd",
            LeaseType::KvClient => "KvClient",
        };
        let label = super::lifecycle::get_register_by(lease_id);
        let bt = std::backtrace::Backtrace::capture();
        tracing::debug!(
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
    /// native async operation carried by the backend uid.
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

    /// Allocate a Fluxon KV lease through the native async backend operation.
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
                let allocate = backend_uid.kv_allocate().ok_or_else(|| {
                    anyhow::anyhow!(
                        "Fluxon KV allocate operation missing from lease backend for cluster={}",
                        cluster
                    )
                })?;
                allocate(ttl_seconds).await
            }
            super::lease_backend_uid::LeaseType::Etcd => {
                anyhow::bail!("allocate_kvclient_lease requires KvClient backend uid")
            }
        }
    }
}
