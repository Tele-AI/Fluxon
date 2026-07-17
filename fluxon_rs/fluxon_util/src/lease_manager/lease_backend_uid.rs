use anyhow::Result as AnyResult;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::etcd::EtcdEndpointSet;

/// A cancellable native Rust operation used by the Fluxon KV lease backend.
pub type KvLeaseFuture<T> = Pin<Box<dyn Future<Output = AnyResult<T>> + Send + 'static>>;

pub type KvAllocateLease = Arc<dyn Fn(i64) -> KvLeaseFuture<u64> + Send + Sync + 'static>;

pub type KvKeepaliveLease = Arc<dyn Fn(u64) -> KvLeaseFuture<()> + Send + Sync + 'static>;

/// Backend kind for leases supported by the unified lease manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseType {
    Etcd,
    KvClient,
}

/// Unique identifier for a lease backend.
///
/// - Etcd: canonical endpoint set with stable identity regardless of input
///   order or equivalent endpoint spelling.
/// - KvClient: cluster and client instance identity; carries native async
///   Fluxon KV lease operations.
pub enum LeaseBackendUid {
    Etcd(EtcdEndpointSet),
    KvClient {
        cluster: String,
        instance_key: String,
        allocate: KvAllocateLease,
        keepalive: KvKeepaliveLease,
    },
}

impl LeaseBackendUid {
    /// Construct an etcd backend identity from raw endpoints or a canonical set.
    pub fn etcd_from<E>(endpoints: E) -> Self
    where
        E: TryInto<EtcdEndpointSet>,
        E::Error: fmt::Display,
    {
        let endpoints = endpoints
            .try_into()
            .unwrap_or_else(|err| panic!("invalid etcd backend endpoints: {err}"));
        LeaseBackendUid::Etcd(endpoints)
    }

    /// Construct a Fluxon KV backend with native async lease operations.
    pub fn kv_client(
        cluster: impl Into<String>,
        instance_key: impl Into<String>,
        allocate: KvAllocateLease,
        keepalive: KvKeepaliveLease,
    ) -> Self {
        LeaseBackendUid::KvClient {
            cluster: cluster.into(),
            instance_key: instance_key.into(),
            allocate,
            keepalive,
        }
    }

    pub fn kind(&self) -> LeaseType {
        match self {
            LeaseBackendUid::Etcd(_) => LeaseType::Etcd,
            LeaseBackendUid::KvClient { .. } => LeaseType::KvClient,
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
            LeaseBackendUid::KvClient { cluster, .. } => Some(cluster.as_str()),
            _ => None,
        }
    }

    pub fn instance_key(&self) -> Option<&str> {
        match self {
            LeaseBackendUid::KvClient { instance_key, .. } => Some(instance_key.as_str()),
            _ => None,
        }
    }

    pub fn kv_allocate(&self) -> Option<KvAllocateLease> {
        match self {
            LeaseBackendUid::KvClient { allocate, .. } => Some(allocate.clone()),
            _ => None,
        }
    }

    pub fn kv_keepalive(&self) -> Option<KvKeepaliveLease> {
        match self {
            LeaseBackendUid::KvClient { keepalive, .. } => Some(keepalive.clone()),
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
    /// Register an etcd lease id whose existence the caller has already validated
    /// on the same backend. A fresh grant and a parent-owned MPMC lease both satisfy
    /// this contract, so registration only installs the periodic keepalive actor.
    EtcdValidated,
    /// Register a kvclient lease whose existence is guaranteed by the caller's
    /// owning control plane. Registration installs periodic keepalive without a
    /// duplicate synchronous probe.
    KvClientValidated {
        register_by: String,
    },
    KvClient {
        register_by: String,
    },
}

// Manual trait impls so that hashing/equality only consider the backend identity
// (endpoints for etcd; cluster and client instance for kvclient). Operations do
// not participate in identity and are cloned via dedicated helpers when needed.
impl Clone for LeaseBackendUid {
    fn clone(&self) -> Self {
        match self {
            LeaseBackendUid::Etcd(v) => LeaseBackendUid::Etcd(v.clone()),
            LeaseBackendUid::KvClient {
                cluster,
                instance_key,
                allocate,
                keepalive,
            } => LeaseBackendUid::KvClient {
                cluster: cluster.clone(),
                instance_key: instance_key.clone(),
                allocate: allocate.clone(),
                keepalive: keepalive.clone(),
            },
        }
    }
}

impl PartialEq for LeaseBackendUid {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LeaseBackendUid::Etcd(a), LeaseBackendUid::Etcd(b)) => a == b,
            (
                LeaseBackendUid::KvClient {
                    cluster: cluster_a,
                    instance_key: instance_a,
                    ..
                },
                LeaseBackendUid::KvClient {
                    cluster: cluster_b,
                    instance_key: instance_b,
                    ..
                },
            ) => cluster_a == cluster_b && instance_a == instance_b,
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
            LeaseBackendUid::KvClient {
                cluster,
                instance_key,
                ..
            } => {
                1u8.hash(state);
                cluster.hash(state);
                instance_key.hash(state);
            }
        }
    }
}

impl fmt::Debug for LeaseBackendUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseBackendUid::Etcd(v) => write!(f, "Etcd({:?})", v),
            LeaseBackendUid::KvClient {
                cluster,
                instance_key,
                ..
            } => {
                write!(
                    f,
                    "KvClient(cluster={}, instance_key={})",
                    cluster, instance_key
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv_backend(cluster: &str, instance_key: &str) -> LeaseBackendUid {
        LeaseBackendUid::kv_client(
            cluster,
            instance_key,
            Arc::new(|_| Box::pin(async { Ok(1) })),
            Arc::new(|_| Box::pin(async { Ok(()) })),
        )
    }

    #[test]
    fn kv_backend_identity_includes_client_instance() {
        let first = kv_backend("cluster", "client-a");
        let same = kv_backend("cluster", "client-a");
        let other_client = kv_backend("cluster", "client-b");

        assert_eq!(first, same);
        assert_ne!(first, other_client);
    }

    #[test]
    fn etcd_backend_identity_accepts_raw_and_canonical_endpoints() {
        let raw = vec!["127.0.0.1:2379".to_string()];
        let canonical = EtcdEndpointSet::from_raw(raw.clone()).unwrap();

        assert_eq!(
            LeaseBackendUid::etcd_from(raw),
            LeaseBackendUid::etcd_from(canonical)
        );
    }
}
