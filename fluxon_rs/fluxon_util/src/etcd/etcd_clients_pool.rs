use std::sync::OnceLock;

use anyhow::{Context, Result};
use etcd_client::Client;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::RwLock;

use crate::auto_clean_map::{AutoCleanMap, AutoCleanMapEntry};

struct EtcdClientSlot {
    generation: u64,
    client: Option<Client>,
}

struct EtcdClientPoolEntry {
    endpoints: Vec<String>,
    slot: RwLock<EtcdClientSlot>,
}

impl EtcdClientPoolEntry {
    fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            slot: RwLock::new(EtcdClientSlot {
                generation: 0,
                client: None,
            }),
        }
    }

    async fn snapshot(&self, owner: PooledEtcdClient) -> Result<PooledEtcdClientSnapshot> {
        {
            let slot = self.slot.read().await;
            if let Some(client) = slot.client.as_ref() {
                return Ok(PooledEtcdClientSnapshot {
                    owner,
                    generation: slot.generation,
                    client: client.clone(),
                });
            }
        }

        // Serialize cold starts so one endpoint list creates one client generation.
        let mut slot = self.slot.write().await;
        if let Some(client) = slot.client.as_ref() {
            return Ok(PooledEtcdClientSnapshot {
                owner,
                generation: slot.generation,
                client: client.clone(),
            });
        }

        let endpoints = self.endpoints.clone();
        let client = etcd_clients_pool_runtime()
            .spawn(async move { Client::connect(endpoints, None).await })
            .await
            .context("etcd clients pool runtime task stopped")??;
        slot.generation = slot
            .generation
            .checked_add(1)
            .expect("etcd client pool generation overflow");
        slot.client = Some(client.clone());
        Ok(PooledEtcdClientSnapshot {
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

/// Process-wide pool for etcd clients keyed by the caller's endpoint list.
///
/// The pool keeps weak registry entries. A client remains cached while at
/// least one `PooledEtcdClient` owner is alive.
pub struct EtcdClientsPool {
    entries: AutoCleanMap<Vec<String>, EtcdClientPoolEntry>,
}

impl EtcdClientsPool {
    fn new() -> Self {
        Self {
            entries: AutoCleanMap::new(),
        }
    }

    pub fn acquire(&self, endpoints: Vec<String>) -> PooledEtcdClient {
        let entry = self
            .entries
            .get_or_init(endpoints.clone(), || EtcdClientPoolEntry::new(endpoints));
        PooledEtcdClient { entry }
    }
}

/// Return the process-wide etcd clients pool.
pub fn etcd_clients_pool() -> &'static EtcdClientsPool {
    static POOL: OnceLock<EtcdClientsPool> = OnceLock::new();
    POOL.get_or_init(EtcdClientsPool::new)
}

/// Keep tonic workers independent of the runtime that acquires a client.
fn etcd_clients_pool_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("fluxon-etcd-client-pool")
            .enable_all()
            .build()
            .expect("failed to build etcd clients pool runtime")
    })
}

/// One live ownership entry in the process-wide etcd clients pool.
#[derive(Clone)]
pub struct PooledEtcdClient {
    entry: AutoCleanMapEntry<Vec<String>, EtcdClientPoolEntry>,
}

impl PooledEtcdClient {
    #[inline]
    pub fn endpoints(&self) -> &[String] {
        &self.entry.endpoints
    }

    pub async fn snapshot(&self) -> Result<PooledEtcdClientSnapshot> {
        self.entry.snapshot(self.clone()).await
    }

    pub async fn client(&self) -> Result<Client> {
        Ok(self.snapshot().await?.client)
    }

    #[cfg(test)]
    pub(crate) fn shares_entry_with(&self, other: &Self) -> bool {
        std::ptr::eq(&*self.entry, &*other.entry)
    }
}

/// A connected client generation borrowed from an `EtcdClientsPool` entry.
#[derive(Clone)]
pub struct PooledEtcdClientSnapshot {
    owner: PooledEtcdClient,
    generation: u64,
    client: Client,
}

impl PooledEtcdClientSnapshot {
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

    fn unique_endpoints() -> Vec<String> {
        vec![format!(
            "http://etcd-client-pool-test-{}:2379",
            uuid::Uuid::new_v4()
        )]
    }

    #[test]
    fn pool_reuses_and_auto_cleans_live_entries() {
        let endpoints = unique_endpoints();
        let pool = etcd_clients_pool();
        assert!(pool.entries.with_existing(&endpoints, |_| ()).is_none());

        {
            let first = pool.acquire(endpoints.clone());
            assert!(pool.entries.with_existing(&endpoints, |_| ()).is_some());
            {
                let second = pool.acquire(endpoints.clone());
                assert!(std::ptr::eq(&*first.entry, &*second.entry));
            }
            assert!(pool.entries.with_existing(&endpoints, |_| ()).is_some());
        }

        assert!(pool.entries.with_existing(&endpoints, |_| ()).is_none());
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_invalidate_new_generation() {
        let pooled = etcd_clients_pool().acquire(vec!["http://127.0.0.1:1".to_string()]);
        let first = pooled.snapshot().await.unwrap();
        assert!(first.invalidate().await);
        let second = pooled.snapshot().await.unwrap();
        assert_ne!(first.generation, second.generation);
        assert!(!first.invalidate().await);
    }

    #[test]
    fn client_worker_outlives_the_requesting_runtime() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let accept_thread = thread::spawn(move || {
            let _connection = listener.accept().unwrap();
            accepted_tx.send(()).unwrap();
        });

        let pooled = etcd_clients_pool().acquire(vec![format!("http://{address}")]);
        let caller_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let snapshot = caller_runtime.block_on(pooled.snapshot()).unwrap();
        drop(caller_runtime);

        let request_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        request_runtime.block_on(async move {
            let mut client = snapshot.client();
            let _ = tokio::time::timeout(Duration::from_secs(1), client.status()).await;
        });

        accepted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("pooled etcd client worker stopped with the requesting runtime");
        accept_thread.join().unwrap();
    }
}
