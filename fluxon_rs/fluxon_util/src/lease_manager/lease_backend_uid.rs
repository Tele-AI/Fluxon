use anyhow::Result as AnyResult;
use std::fmt;
use std::sync::Arc;

use crate::etcd::EtcdEndpointSet;

/// Backend kind for leases supported by the unified lease manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseType {
    Etcd,
    KvClient,
}

/// Unique identifier for a lease backend.
///
/// - Etcd: canonical endpoint set.
/// - KvClient: cluster name; carries allocate/keepalive Rust closures.
pub enum LeaseBackendUid {
    Etcd(EtcdEndpointSet),
    KvClientWithCallbacks {
        cluster: String,
        /// Allocate closure: input ttl_seconds -> lease_id
        allocate_cb: Arc<dyn Fn(i64) -> AnyResult<u64> + Send + Sync + 'static>,
        /// Keepalive closure: input lease_id; must not alter TTL.
        /// Must return `AnyResult<()>` to surface errors to the caller.
        keepalive_cb: Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>,
    },
}

impl LeaseBackendUid {
    pub fn etcd(endpoints: EtcdEndpointSet) -> Self {
        LeaseBackendUid::Etcd(endpoints)
    }

    /// Construct a kvclient backend uid that carries allocate/keepalive callbacks.
    pub fn kv_client_with_callbacks(
        cluster: impl Into<String>,
        allocate_cb: Arc<dyn Fn(i64) -> AnyResult<u64> + Send + Sync + 'static>,
        keepalive_cb: Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>,
    ) -> Self {
        LeaseBackendUid::KvClientWithCallbacks {
            cluster: cluster.into(),
            allocate_cb,
            keepalive_cb,
        }
    }

    pub fn kind(&self) -> LeaseType {
        match self {
            LeaseBackendUid::Etcd(_) => LeaseType::Etcd,
            LeaseBackendUid::KvClientWithCallbacks { .. } => LeaseType::KvClient,
        }
    }

    pub fn etcd_endpoint_set(&self) -> Option<&EtcdEndpointSet> {
        match self {
            LeaseBackendUid::Etcd(v) => Some(v),
            _ => None,
        }
    }

    pub fn cluster(&self) -> Option<&str> {
        match self {
            LeaseBackendUid::KvClientWithCallbacks { cluster, .. } => Some(cluster.as_str()),
            _ => None,
        }
    }

    /// Clone the kvclient allocate callback if present.
    pub fn kv_allocate_cb(
        &self,
    ) -> Option<Arc<dyn Fn(i64) -> AnyResult<u64> + Send + Sync + 'static>> {
        match self {
            LeaseBackendUid::KvClientWithCallbacks { allocate_cb, .. } => Some(allocate_cb.clone()),
            _ => None,
        }
    }

    /// Clone the kvclient keepalive callback if present.
    pub fn kv_keepalive_cb(
        &self,
    ) -> Option<Arc<dyn Fn(u64) -> AnyResult<()> + Send + Sync + 'static>> {
        match self {
            LeaseBackendUid::KvClientWithCallbacks { keepalive_cb, .. } => {
                Some(keepalive_cb.clone())
            }
            _ => None,
        }
    }
}

/// Keepalive registration payload for the unified lease manager.
///
/// Etcd only needs a `revoke_on_drop` flag; KvClient path uses the
/// keepalive closure carried by `LeaseBackendUid::KvClientWithCallbacks`.
pub enum LeaseRegisterKind {
    Etcd { revoke_on_drop: bool },
    KvClient { register_by: String },
}

// Manual trait impls so that hashing/equality only consider the backend identity
// (endpoints for etcd; cluster name for kvclient). Callbacks do not participate
// in identity and are cloned via dedicated helpers when needed.
impl Clone for LeaseBackendUid {
    fn clone(&self) -> Self {
        match self {
            LeaseBackendUid::Etcd(v) => LeaseBackendUid::Etcd(v.clone()),
            LeaseBackendUid::KvClientWithCallbacks {
                cluster,
                allocate_cb,
                keepalive_cb,
            } => LeaseBackendUid::KvClientWithCallbacks {
                cluster: cluster.clone(),
                allocate_cb: allocate_cb.clone(),
                keepalive_cb: keepalive_cb.clone(),
            },
        }
    }
}

impl PartialEq for LeaseBackendUid {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LeaseBackendUid::Etcd(a), LeaseBackendUid::Etcd(b)) => a == b,
            (
                LeaseBackendUid::KvClientWithCallbacks { cluster: a, .. },
                LeaseBackendUid::KvClientWithCallbacks { cluster: b, .. },
            ) => a == b,
            _ => false,
        }
    }
}

impl Eq for LeaseBackendUid {}

impl std::hash::Hash for LeaseBackendUid {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            LeaseBackendUid::Etcd(endpoints) => {
                0u8.hash(state);
                endpoints.hash(state);
            }
            LeaseBackendUid::KvClientWithCallbacks { cluster, .. } => {
                1u8.hash(state);
                cluster.hash(state);
            }
        }
    }
}

impl fmt::Debug for LeaseBackendUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseBackendUid::Etcd(v) => write!(f, "Etcd({:?})", v.as_slice()),
            LeaseBackendUid::KvClientWithCallbacks { cluster, .. } => {
                write!(f, "KvClientWithCallbacks(cluster={})", cluster)
            }
        }
    }
}
