pub use crate::cluster::{
    ClusterError, ClusterEvent, ClusterMember, ClusterResult, EtcdPrefixScanAction, NodeID,
    NodeIDStr, NodeIDString, NodeRole, scan_etcd_prefix_paginated,
};
pub use crate::config::NetworkConfig;
pub use crate::member_metadata::{
    META_KEY_HOSTNAME, META_KEY_PID, META_KEY_PRODUCT_UUID, META_KEY_RDMA_CONTROL,
    META_KEY_RDMA_RUNTIME, MemberRdmaControl, MemberRdmaResolvedConfig, MemberRdmaRuntime,
    MemberRdmaTransferEngineRuntime, MemberRdmaTransferEngineState, ShareGroupOwnerRef,
};
pub use crate::transfer::{
    META_KEY_TRANSFER_BACKEND_EPOCH, META_KEY_TRANSFER_READY, P2pTransportKind,
    TransferLinkEtcdWrite, TransferLinkEtcdWriterHandle, TransferLinkP2pState, TransferLinkRecord,
    TransferLinkTeState, TransferReadyInfo,
};

use bitcode::{Decode, Encode};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum ClusterManagerRdmaControlInit {
    Disabled,
    ExplicitDevices(Vec<String>),
    LockedExplicitDevices(Vec<String>),
    DetectAllDevices,
}

#[derive(Clone)]
pub struct IpcBandwidthAttributorHandle {
    // P2P hot paths stay allocation-free; a background actor periodically swaps atomics and emits
    // labeled Prom counters.
    tx_bytes: Arc<AtomicU64>,
    rx_bytes: Arc<AtomicU64>,
}

impl IpcBandwidthAttributorHandle {
    pub fn new() -> Self {
        Self {
            tx_bytes: Arc::new(AtomicU64::new(0)),
            rx_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_tx_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.tx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_rx_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.rx_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn take_tx_bytes(&self) -> u64 {
        self.tx_bytes.swap(0, Ordering::AcqRel)
    }

    pub fn take_rx_bytes(&self) -> u64 {
        self.rx_bytes.swap(0, Ordering::AcqRel)
    }

    #[doc(hidden)]
    pub fn from_raw_parts(tx_bytes: Arc<AtomicU64>, rx_bytes: Arc<AtomicU64>) -> Self {
        Self { tx_bytes, rx_bytes }
    }

    #[doc(hidden)]
    pub fn raw_parts(&self) -> (Arc<AtomicU64>, Arc<AtomicU64>) {
        (self.tx_bytes.clone(), self.rx_bytes.clone())
    }
}

/// Stable open constructor args for ClusterManager.
#[derive(Clone, Debug, Encode, Decode)]
pub struct ClusterManagerNewArg {
    pub etcd_endpoints: Vec<String>,
    pub cluster_name: String,
    pub instance_name: Option<String>,
    pub port: Option<u16>,
    pub metadata: HashMap<String, String>,
    pub local_ipc_root: Option<String>,
    pub rdma_control_init: ClusterManagerRdmaControlInit,
    pub sub_cluster: Option<String>,
    pub network: Option<NetworkConfig>,
}
