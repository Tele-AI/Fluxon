use std::sync::OnceLock;

use anyhow::{bail, Context, Result};
use etcd_client::Client;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::RwLock;

use crate::auto_clean_map::{AutoCleanMap, AutoCleanMapEntry};

/// Canonical identity of one etcd cluster connection target.
///
/// Endpoints are internal URL strings with an explicit `http://` or `https://`
/// scheme. Construction sorts and deduplicates them because etcd balances over
/// the set and input order does not change cluster identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EtcdEndpointSet(Vec<String>);

impl EtcdEndpointSet {
    pub fn from_raw(endpoints: Vec<String>) -> Result<Self> {
        if endpoints.is_empty() {
            bail!("etcd endpoint set must not be empty");
        }

        let mut normalized = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let endpoint = endpoint.trim();
            if endpoint.is_empty() {
                bail!("etcd endpoint must not be empty");
            }
            if endpoint.contains("://") {
                bail!(
                    "raw etcd endpoint must not include a URL scheme, got: {}",
                    endpoint
                );
            }
            normalized.push(format!("http://{endpoint}"));
        }
        Self::new(normalized)
    }

    pub fn new(endpoints: Vec<String>) -> Result<Self> {
        if endpoints.is_empty() {
            bail!("etcd endpoint set must not be empty");
        }

        let mut canonical = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let endpoint = endpoint.trim();
            if endpoint.is_empty() {
                bail!("etcd endpoint must not be empty");
            }
            if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
                bail!(
                    "etcd endpoint must be an internal URL with http:// or https:// scheme, got: {}",
                    endpoint
                );
            }
            canonical.push(endpoint.to_string());
        }
        canonical.sort();
        canonical.dedup();
        Ok(Self(canonical))
    }

    #[inline]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }
}

impl TryFrom<Vec<String>> for EtcdEndpointSet {
    type Error = anyhow::Error;

    fn try_from(endpoints: Vec<String>) -> Result<Self> {
        Self::from_raw(endpoints)
    }
}

struct EtcdClientSlot {
    generation: u64,
    client: Option<Client>,
}

struct EtcdClientBackend {
    endpoints: EtcdEndpointSet,
    slot: RwLock<EtcdClientSlot>,
}

impl EtcdClientBackend {
    fn new(endpoints: EtcdEndpointSet) -> Self {
        Self {
            endpoints,
            slot: RwLock::new(EtcdClientSlot {
                generation: 0,
                client: None,
            }),
        }
    }

    async fn snapshot(&self, owner: ManagedEtcdClient) -> Result<ManagedEtcdClientSnapshot> {
        {
            let slot = self.slot.read().await;
            if let Some(client) = slot.client.as_ref() {
                return Ok(ManagedEtcdClientSnapshot {
                    owner,
                    generation: slot.generation,
                    client: client.clone(),
                });
            }
        }

        // Hold the write lock while connecting so concurrent cold-start callers
        // share one connection attempt and one resulting client generation.
        let mut slot = self.slot.write().await;
        if let Some(client) = slot.client.as_ref() {
            return Ok(ManagedEtcdClientSnapshot {
                owner,
                generation: slot.generation,
                client: client.clone(),
            });
        }

        let endpoints = self.endpoints.as_slice().to_vec();
        let error_endpoints = endpoints.clone();
        let client = managed_etcd_runtime()
            .spawn(async move { Client::connect(endpoints, None).await })
            .await
            .context("managed etcd client runtime task stopped")?
            .with_context(|| {
                format!("failed to connect etcd for endpoints {:?}", error_endpoints)
            })?;
        slot.generation = slot
            .generation
            .checked_add(1)
            .expect("etcd client generation overflow");
        slot.client = Some(client.clone());
        Ok(ManagedEtcdClientSnapshot {
            owner,
            generation: slot.generation,
            client,
        })
    }

    async fn invalidate_generation(&self, generation: u64) -> bool {
        let mut slot = self.slot.write().await;
        if slot.generation != generation || slot.client.is_none() {
            return false;
        }
        slot.client = None;
        true
    }
}

/// Owns tonic channel workers independently of whichever runtime first asks
/// for an endpoint set. The runtime is intentionally bounded and process-wide.
fn managed_etcd_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("fluxon-etcd-client")
            .enable_all()
            .build()
            .expect("failed to build managed etcd client runtime")
    })
}

fn managed_etcd_client_map() -> &'static AutoCleanMap<EtcdEndpointSet, EtcdClientBackend> {
    static MAP: OnceLock<AutoCleanMap<EtcdEndpointSet, EtcdClientBackend>> = OnceLock::new();
    MAP.get_or_init(AutoCleanMap::new)
}

/// Auto-clean handle for the process-wide etcd client registry.
///
/// Every live owner for the same canonical endpoint set shares one lazily
/// connected `etcd_client::Client` generation. Dropping the last handle removes
/// the backend entry and releases its cached client.
#[derive(Clone)]
pub struct ManagedEtcdClient {
    entry: AutoCleanMapEntry<EtcdEndpointSet, EtcdClientBackend>,
}

impl ManagedEtcdClient {
    pub fn acquire(endpoints: EtcdEndpointSet) -> Self {
        let entry = managed_etcd_client_map()
            .get_or_init(endpoints.clone(), || EtcdClientBackend::new(endpoints));
        Self { entry }
    }

    #[inline]
    pub fn endpoints(&self) -> &EtcdEndpointSet {
        &self.entry.endpoints
    }

    pub async fn snapshot(&self) -> Result<ManagedEtcdClientSnapshot> {
        self.entry.snapshot(self.clone()).await
    }

    pub async fn client(&self) -> Result<Client> {
        Ok(self.snapshot().await?.client)
    }

    pub(crate) fn cached_client(&self) -> Result<Client> {
        let slot = self
            .entry
            .slot
            .try_read()
            .map_err(|_| anyhow::anyhow!("managed etcd client cache is being updated"))?;
        slot.client
            .clone()
            .context("managed etcd client has no connected generation")
    }
}

/// One connected generation borrowed from a `ManagedEtcdClient`.
///
/// The snapshot keeps the registry guard alive. Invalidating it only clears the
/// same generation, so a delayed failure cannot discard a newer replacement.
#[derive(Clone)]
pub struct ManagedEtcdClientSnapshot {
    owner: ManagedEtcdClient,
    generation: u64,
    client: Client,
}

impl ManagedEtcdClientSnapshot {
    #[inline]
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub async fn invalidate(&self) -> bool {
        self.owner
            .entry
            .invalidate_generation(self.generation)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    fn unique_endpoint_set() -> EtcdEndpointSet {
        EtcdEndpointSet::new(vec![format!(
            "http://managed-etcd-client-test-{}:2379",
            uuid::Uuid::new_v4()
        )])
        .unwrap()
    }

    #[test]
    fn endpoint_set_sorts_and_deduplicates_urls() {
        let endpoints = EtcdEndpointSet::new(vec![
            "http://b.example:2379".to_string(),
            " http://a.example:2379 ".to_string(),
            "http://b.example:2379".to_string(),
        ])
        .unwrap();
        assert_eq!(
            endpoints.as_slice(),
            [
                "http://a.example:2379".to_string(),
                "http://b.example:2379".to_string()
            ]
        );
    }

    #[test]
    fn endpoint_set_normalizes_raw_endpoints() {
        let endpoints = EtcdEndpointSet::from_raw(vec![
            " b.example:2379 ".to_string(),
            "a.example:2379".to_string(),
            "b.example:2379".to_string(),
        ])
        .unwrap();
        assert_eq!(
            endpoints.as_slice(),
            [
                "http://a.example:2379".to_string(),
                "http://b.example:2379".to_string()
            ]
        );
    }

    #[test]
    fn endpoint_set_rejects_raw_or_empty_endpoints() {
        assert!(EtcdEndpointSet::new(Vec::new()).is_err());
        assert!(EtcdEndpointSet::new(vec!["".to_string()]).is_err());
        assert!(EtcdEndpointSet::new(vec!["127.0.0.1:2379".to_string()]).is_err());
        assert!(EtcdEndpointSet::from_raw(Vec::new()).is_err());
        assert!(EtcdEndpointSet::from_raw(vec!["http://127.0.0.1:2379".to_string()]).is_err());
    }

    #[test]
    fn registry_reuses_and_auto_cleans_live_entries() {
        let endpoints = unique_endpoint_set();
        let map = managed_etcd_client_map();
        assert!(map.with_existing(&endpoints, |_| ()).is_none());

        {
            let handle_a = ManagedEtcdClient::acquire(endpoints.clone());
            assert!(map.with_existing(&endpoints, |_| ()).is_some());

            {
                let handle_b = ManagedEtcdClient::acquire(endpoints.clone());
                assert!(std::ptr::eq(&*handle_a.entry, &*handle_b.entry));
            }

            assert!(map.with_existing(&endpoints, |_| ()).is_some());
        }

        assert!(map.with_existing(&endpoints, |_| ()).is_none());
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_invalidate_new_generation() {
        let endpoints = EtcdEndpointSet::new(vec!["http://127.0.0.1:1".to_string()]).unwrap();
        let handle = ManagedEtcdClient::acquire(endpoints);

        let first = handle.snapshot().await.unwrap();
        assert!(first.invalidate().await);

        let second = handle.snapshot().await.unwrap();
        assert_ne!(first.generation, second.generation);
        assert!(!first.invalidate().await);

        let current = handle.snapshot().await.unwrap();
        assert_eq!(second.generation, current.generation);
    }

    #[test]
    fn client_worker_outlives_the_runtime_that_requested_the_snapshot() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let accept_thread = thread::spawn(move || {
            let _connection = listener.accept().unwrap();
            accepted_tx.send(()).unwrap();
        });

        let handle = ManagedEtcdClient::acquire(
            EtcdEndpointSet::new(vec![format!("http://{address}")]).unwrap(),
        );
        assert!(handle.cached_client().is_err());
        let caller_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let snapshot = caller_runtime.block_on(handle.snapshot()).unwrap();
        assert!(handle.cached_client().is_ok());
        drop(caller_runtime);

        let request_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        request_runtime.block_on(async move {
            let mut client = snapshot.client();
            let _ = tokio::time::timeout(Duration::from_secs(1), client.status()).await;
        });

        accepted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("managed etcd worker stopped with the caller runtime");
        accept_thread.join().unwrap();
    }
}
