use crate::config::NetworkConfig;
use bitcode::{Decode, Encode};
use etcd_client::{Client, GetOptions};
use fluxon_util::etcd::retry_etcd_rpc;
use fluxon_util::prefix_scan::{
    PrefixScanAction, prefix_scan_key_after, prefix_scan_range_end_exclusive,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use thiserror::Error;

pub type NodeID = Cow<'static, str>;
pub type NodeIDString = String;
pub type NodeIDStr = str;
pub const ETCD_PREFIX_SCAN_PAGE_LIMIT: i64 = 1024;

pub type EtcdPrefixScanAction = PrefixScanAction;

#[derive(Error, Debug)]
pub enum EtcdPrefixScanError<E>
where
    E: std::fmt::Display + std::fmt::Debug,
{
    #[error("Get etcd prefix {prefix} failed at start key {start_key:?}: {source}")]
    Get {
        prefix: String,
        start_key: Vec<u8>,
        #[source]
        source: etcd_client::Error,
    },

    #[error("etcd prefix scan callback failed: {0}")]
    Callback(E),
}

pub async fn scan_etcd_prefix_paginated<E, F>(
    client: &mut Client,
    prefix: &str,
    on_kv: F,
) -> Result<(), EtcdPrefixScanError<E>>
where
    E: std::fmt::Display + std::fmt::Debug,
    F: FnMut(&[u8], &[u8]) -> Result<EtcdPrefixScanAction, E>,
{
    scan_etcd_prefix_paginated_with_retry(client, prefix, 0, on_kv).await
}

/// Scans an etcd prefix page by page and retries transient page-range failures.
///
/// A page is delivered to `on_kv` only after its Range RPC succeeds, so retrying
/// the RPC never invokes the callback twice for the same failed attempt.
pub async fn scan_etcd_prefix_paginated_with_retry<E, F>(
    client: &mut Client,
    prefix: &str,
    max_retries: u32,
    mut on_kv: F,
) -> Result<(), EtcdPrefixScanError<E>>
where
    E: std::fmt::Display + std::fmt::Debug,
    F: FnMut(&[u8], &[u8]) -> Result<EtcdPrefixScanAction, E>,
{
    let range_end = prefix_scan_range_end_exclusive(prefix.as_bytes()).unwrap_or_else(|| vec![0]);
    let mut start_key = prefix.as_bytes().to_vec();

    loop {
        let resp = retry_etcd_rpc(max_retries, "prefix_range", || {
            let mut attempt_client = client.clone();
            let attempt_start_key = start_key.clone();
            let attempt_range_end = range_end.clone();
            async move {
                attempt_client
                    .get(
                        attempt_start_key,
                        Some(
                            GetOptions::new()
                                .with_range(attempt_range_end)
                                .with_limit(ETCD_PREFIX_SCAN_PAGE_LIMIT),
                        ),
                    )
                    .await
            }
        })
        .await
        .map_err(|source| EtcdPrefixScanError::Get {
            prefix: prefix.to_string(),
            start_key: start_key.clone(),
            source,
        })?;

        if resp.kvs().is_empty() {
            break;
        }

        for kv in resp.kvs() {
            match on_kv(kv.key(), kv.value()).map_err(EtcdPrefixScanError::Callback)? {
                EtcdPrefixScanAction::Continue => {}
                EtcdPrefixScanAction::Break => return Ok(()),
            }
        }

        if !resp.more() {
            break;
        }

        let last_key = resp
            .kvs()
            .last()
            .expect("non-empty page must have a last key")
            .key();
        start_key = prefix_scan_key_after(last_key);
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("Failed to connect to etcd: {error}, endpoints: {endpoints:?}")]
    EtcdConnection {
        endpoints: Vec<String>,
        error: String,
    },

    #[error("Failed to create lease: {error}, endpoints: {endpoints:?}")]
    LeaseCreation {
        endpoints: Vec<String>,
        error: String,
    },

    #[error("Failed to register member: {0}")]
    MemberRegistration(String),

    #[error("Failed to delete member: {0}")]
    MemberDeletion(String),

    #[error("Failed to sync cluster members: {0}")]
    MemberSync(String),

    #[error("Failed to serialize member info: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Failed to revoke lease: {0}")]
    LeaseRevocation(String),

    #[error("Event callback not set. Call set_event_callback() first")]
    EventCallbackNotSet,

    #[error("Invalid cluster configuration: {0}")]
    InvalidConfiguration(String),

    #[error("Cluster operation failed: {0}")]
    OperationFailed(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Unreachable: {0}")]
    Unreachable(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<anyhow::Error> for ClusterError {
    fn from(error: anyhow::Error) -> Self {
        ClusterError::Unknown(error.to_string())
    }
}

pub type ClusterResult<T> = Result<T, ClusterError>;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum NodeRole {
    Master,
    Client,
    External,
    Unknown,
}

impl NodeRole {
    pub fn as_str(&self) -> &str {
        match self {
            NodeRole::Master => "master",
            NodeRole::Client => "client",
            NodeRole::External => "external_client",
            NodeRole::Unknown => "unknown",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "master" => NodeRole::Master,
            "client" => NodeRole::Client,
            "external_client" => NodeRole::External,
            other => {
                tracing::warn!("Unknown node role string: {}", other);
                NodeRole::Unknown
            }
        }
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct ClusterMember {
    pub id: NodeIDString,
    pub addresses: Vec<String>,
    pub port: Option<u16>,
    pub node_start_time: i64,
    pub metadata: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_cluster: Option<String>,
    #[serde(rename = "network", skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkConfig>,
}

impl ClusterMember {
    pub fn node_role(&self) -> NodeRole {
        if let Some(role) = self.metadata.get("role") {
            return NodeRole::from_str(role);
        }
        if self
            .metadata
            .get("master")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return NodeRole::Master;
        }
        if self
            .metadata
            .get("client")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return NodeRole::Client;
        }
        if self
            .metadata
            .get("external_client")
            .map(|v| v == "true")
            .unwrap_or(false)
        {
            return NodeRole::External;
        }
        if let Some(k_true) = self
            .metadata
            .iter()
            .find_map(|(k, v)| (v == "true").then(|| k.clone()))
        {
            return NodeRole::from_str(&k_true);
        }
        NodeRole::Unknown
    }

    pub fn contact_changed_vs(&self, new_addresses: &[String], new_port: Option<u16>) -> bool {
        let port_changed = self.port != new_port;
        let addr_changed = self.addresses != new_addresses;
        port_changed || addr_changed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClusterEvent {
    MemberJoined(ClusterMember),
    MemberLeft(String),
    MemberUpdated(ClusterMember),
}

impl ClusterEvent {
    pub fn node_id(&self) -> NodeIDString {
        match self {
            ClusterEvent::MemberJoined(member) => member.id.clone(),
            ClusterEvent::MemberLeft(member_id) => member_id.clone(),
            ClusterEvent::MemberUpdated(member) => member.id.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ETCD_PREFIX_SCAN_PAGE_LIMIT;
    use fluxon_util::prefix_scan::{prefix_scan_key_after, prefix_scan_range_end_exclusive};

    #[test]
    fn etcd_prefix_range_end_matches_prefix_scan_ordering() {
        assert_eq!(
            prefix_scan_range_end_exclusive(b"/cluster/transfer_link/p2p"),
            Some(b"/cluster/transfer_link/p2q".to_vec())
        );
        assert_eq!(
            prefix_scan_range_end_exclusive(b"/cluster/transfer_link/p2p/"),
            Some(b"/cluster/transfer_link/p2p0".to_vec())
        );
    }

    #[test]
    fn etcd_key_after_resumes_after_last_seen_key() {
        assert_eq!(prefix_scan_key_after(b"/prefix/a"), b"/prefix/a\0");
        assert!(ETCD_PREFIX_SCAN_PAGE_LIMIT > 0);
    }
}
