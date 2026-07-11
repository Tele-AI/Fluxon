use anyhow::Result as AnyResult;
use std::fmt;
use std::sync::Arc;

/// Backend kind for leases supported by the unified lease manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseType {
    Etcd,
    KvClient,
}

/// Unique identifier for a lease backend.
///
/// - Etcd: endpoints list (Vec<String>) sorted lexicographically to make
///   identity stable regardless of input order.
/// - KvClient: cluster name; carries allocate/keepalive Rust closures.
pub enum LeaseBackendUid {
    Etcd(Vec<String>),
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
    /// Construct an etcd backend uid from endpoint list; endpoints are sorted
    /// to ensure identical identity regardless of input order.
    pub fn etcd_from(mut endpoints: Vec<String>) -> Self {
        // Sort in-place; caller must pass explicit endpoints, we don't add defaults.
        endpoints.sort();
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

    pub fn endpoints(&self) -> Option<&[String]> {
        match self {
            LeaseBackendUid::Etcd(v) => Some(v.as_slice()),
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
/// Etcd registration only contributes keepalive. Cleanup of owned keys must be
/// performed by the semantic owner explicitly; lease drop never revokes.
pub enum LeaseRegisterKind {
    /// Register an etcd lease id that may already have existed before this call.
    /// Registration validates the lease with an initial keepalive probe.
    Etcd,
    /// Register an etcd lease id that was just granted by the caller on the same
    /// backend. The grant itself proves lease existence, so registration skips
    /// the initial keepalive probe and only installs the periodic keepalive actor.
    EtcdNewlyGranted,
    KvClient {
        register_by: String,
    },
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
                // tag + endpoints (construction sorted; order is stable)
                0u8.hash(state);
                for e in endpoints {
                    e.hash(state);
                }
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
            LeaseBackendUid::Etcd(v) => write!(f, "Etcd({:?})", v),
            LeaseBackendUid::KvClientWithCallbacks { cluster, .. } => {
                write!(f, "KvClientWithCallbacks(cluster={})", cluster)
            }
        }
    }
}
