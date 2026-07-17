use crate::master_kv_router::put::PutIDForAKey;
use crate::rpcresp_kvresult_convert::msg_and_error::LeaseMgrError;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

/// Lease ID type
pub type LeaseID = u64;

/// Lease TTL in seconds
pub type LeaseSecTTl = u64;

/// Key associated with a lease
pub type LeaseKey = String;

// Removed KeyVersionInfo struct - put_id is now stored directly as a value

/// Lease state constants
const LEASE_STATE_ACTIVE: u32 = 0;
const LEASE_STATE_EXPIRED: u32 = 1;

/// Lease state
#[derive(Debug, Clone, PartialEq)]
pub enum LeaseState {
    /// Lease is active and valid
    Active,
    /// Lease is expired
    Expired,
}

/// Lease attributes for validation
#[derive(Debug, Clone)]
pub struct LeaseAttributes {
    pub id: LeaseID,
    pub ttl: LeaseSecTTl,
    pub keys_count: usize, // Changed from keys: Vec<String> to just count
    pub state: LeaseState,
}

/// Individual lease instance
pub struct Lease {
    /// Unique lease identifier
    pub id: LeaseID,
    /// Time-to-live in seconds
    pub ttl: LeaseSecTTl,
    /// Current state of the lease
    pub state: AtomicU32, // LEASE_STATE_ACTIVE, LEASE_STATE_EXPIRED
    /// Keys associated with this lease with put_id information
    pub keys: DashMap<LeaseKey, PutIDForAKey>,
    /// Expiration time
    pub expiration_time: AtomicU64, // timestamp in milliseconds
}

// Removed LeaseEvent enum - not needed for direct API calls

impl Lease {
    /// Create a new lease with optional client ID
    pub fn new(id: LeaseID, ttl: LeaseSecTTl) -> Result<Self, LeaseMgrError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let expiration_time = now.saturating_add(ttl.saturating_mul(1000));

        let lease = Self {
            id,
            ttl,
            state: AtomicU32::new(LEASE_STATE_ACTIVE),
            keys: DashMap::new(),
            expiration_time: AtomicU64::new(expiration_time),
        };

        debug!("Lease {} created with TTL {}s", id, ttl);
        Ok(lease)
    }

    /// Refresh the lease (extend expiration time)
    /// Refresh the lease with optional custom TTL
    ///
    /// # Arguments
    /// * `custom_ttl` - Optional custom TTL in seconds. If None, uses the lease's default TTL
    pub fn refresh(&self, custom_ttl: Option<u64>) -> Result<(), LeaseMgrError> {
        let current_state = self.state.load(Ordering::Acquire);
        if current_state != LEASE_STATE_ACTIVE {
            return Err(LeaseMgrError::LeaseExpired {
                lease_id: self.id,
                message: "Lease is not in Active state".to_string(),
            });
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Use custom TTL if provided, otherwise use default
        let ttl_to_use = custom_ttl.unwrap_or(self.ttl);

        self.expiration_time
            .store(now + (ttl_to_use * 1000), Ordering::Release);

        debug!(
            "Lease {} refreshed with TTL {}s, new expiration: {}",
            self.id,
            ttl_to_use,
            now + (ttl_to_use * 1000)
        );
        Ok(())
    }

    /// Expire the lease
    pub fn expire(&self) -> Result<(), LeaseMgrError> {
        let current_state = self.state.load(Ordering::Acquire);
        if current_state != LEASE_STATE_ACTIVE {
            return Err(LeaseMgrError::LeaseExpired {
                lease_id: self.id,
                message: "Lease is already expired or revoked".to_string(),
            });
        }

        // Set state to expired
        self.state.store(LEASE_STATE_EXPIRED, Ordering::Release);

        info!("Lease {} expired", self.id);
        Ok(())
    }

    /// Attach a key to this lease with put_id information
    pub fn attach_key(&self, key: LeaseKey, put_id: PutIDForAKey) -> Result<(), LeaseMgrError> {
        let current_state = self.state.load(Ordering::Acquire);
        if current_state != LEASE_STATE_ACTIVE {
            return Err(LeaseMgrError::LeaseExpired {
                lease_id: self.id,
                message: "Lease is not active".to_string(),
            });
        }

        self.keys.insert(key.clone(), put_id);

        debug!(
            "Key {} attached to lease {} with put_id {:?}",
            key, self.id, put_id
        );
        Ok(())
    }

    /// Remove only the exact version installed by a failed route publication.
    /// A newer Put may already have rebound the same key, so key-only removal
    /// would be an ABA bug.
    pub fn detach_key_if_version(&self, key: &str, put_id: PutIDForAKey) -> bool {
        self.keys
            .remove_if(key, |_, current_put_id| *current_put_id == put_id)
            .is_some()
    }

    // Note: no detach_all_keys semantics; deletion paths iterate keys directly.

    /// Get all keys with their put_id information
    /// Note: Only used by tests
    #[cfg(test)]
    pub fn get_keys_with_put_ids(&self) -> Vec<(LeaseKey, PutIDForAKey)> {
        self.keys
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Get lease state
    pub fn get_state(&self) -> LeaseState {
        match self.state.load(Ordering::Acquire) {
            LEASE_STATE_ACTIVE => LeaseState::Active,
            LEASE_STATE_EXPIRED => LeaseState::Expired,
            other => panic!(
                "Lease {} encountered invalid state value: {} (expected ACTIVE or EXPIRED)",
                self.id, other
            ),
        }
    }

    /// Get lease attributes (id, ttl, keys_count)
    /// Note: Only used by tests
    #[cfg(test)]
    pub fn get_attributes(&self) -> LeaseAttributes {
        LeaseAttributes {
            id: self.id,
            ttl: self.ttl,
            keys_count: self.keys.len(),
            state: self.get_state(),
        }
    }

    /// Debug string representation (only serializes small fields, not the DashMap)
    pub fn dbg_str(&self) -> String {
        let state_str = match self.state.load(Ordering::Relaxed) {
            LEASE_STATE_ACTIVE => "Active",
            LEASE_STATE_EXPIRED => "Expired",
            _ => "Unknown",
        };

        format!(
            "Lease {{ id: {}, ttl: {}s, state: {}, keys_count: {}, expiration: {} }}",
            self.id,
            self.ttl,
            state_str,
            self.keys.len(),
            self.expiration_time.load(Ordering::Relaxed)
        )
    }

    /// Get remaining TTL in milliseconds
    #[cfg(test)]
    pub fn get_remaining_ttl_ms(&self) -> u64 {
        let current_state = self.state.load(Ordering::Acquire);
        if current_state != LEASE_STATE_ACTIVE {
            return 0;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let expiration = self.expiration_time.load(Ordering::Acquire);
        if now >= expiration {
            0
        } else {
            expiration - now
        }
    }

    /// Get expiration time in milliseconds since UNIX_EPOCH
    pub fn get_expiration_time(&self) -> u64 {
        self.expiration_time.load(Ordering::Acquire)
    }

    // Removed client_id-related helpers; lease no longer tracks client ownership
}

impl Drop for Lease {
    fn drop(&mut self) {
        // 自定义 Drop 实现，避免 DashMap 清理时的潜在死锁
        // 在 shutdown 时，我们不需要完全清理 keys，直接丢弃即可
        debug!(
            "Dropping lease {} (keys will be auto-cleaned by DashMap)",
            self.dbg_str()
        );

        // 不调用 keys.len() 或 keys.clear()，直接丢弃 DashMap
        // 这样可以避免在 shutdown 时的锁竞争问题
        // DashMap 有自己的 Drop 实现，会自动清理所有内部数据
        // Rust 的所有权系统保证所有资源都会被正确释放
    }
}
