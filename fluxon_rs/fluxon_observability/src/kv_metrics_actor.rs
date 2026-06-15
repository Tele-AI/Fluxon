use prometheus::{CounterVec, GaugeVec, Opts, Registry, core::Collector};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use fluxon_util::prom_remote_write::{
    LABEL_NAME as RW_LABEL_NAME, Label as RwLabel, Sample as RwSample, TimeSeries as RwTimeSeries,
};

use crate::keys::{
    PROM_LABEL_COMPONENT, PROM_LABEL_FS_IO_OP, PROM_LABEL_FS_MOUNT_KIND,
    PROM_LABEL_FS_MOUNTPOINT_DIR_ABS, PROM_LABEL_FS_TARGET_DIR_ABS, PROM_LABEL_METRIC,
    PROM_LABEL_NODE, PROM_LABEL_PEER, PROM_LABEL_RDMA_DEVICE, PROM_LABEL_RDMA_NETDEV,
    PROM_LABEL_RDMA_PCI_BDF, PROM_LABEL_RDMA_PORT, PROM_LABEL_RDMA_TRANSFER_STATE, PROM_LABEL_ROLE,
    PROM_LABEL_STAT, PROM_LABEL_TCP_THREAD_LANE, PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES,
    PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES, PROM_METRIC_FS_IO_OPS_TOTAL,
    PROM_METRIC_FS_MOUNT_FS_TOTAL_BYTES, PROM_METRIC_FS_MOUNT_FS_USED_BYTES,
    PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL, PROM_METRIC_P2P_RECV_TRANSPORT_BYTES_TOTAL,
    PROM_METRIC_P2P_RECV_TRANSPORT_MESSAGES_TOTAL, PROM_METRIC_P2P_RPC_COMPLETION_BYTES_TOTAL,
    PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_SAMPLE_COUNT,
    PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_STAT_US, PROM_METRIC_P2P_RPC_COMPLETION_MESSAGES_TOTAL,
    PROM_METRIC_PROCESS_CPU_USAGE_PERCENT, PROM_METRIC_RDMA_PORT_ACTIVE_MTU_BYTES,
    PROM_METRIC_RDMA_PORT_GID_COUNT, PROM_METRIC_RDMA_PORT_NUMA_NODE,
    PROM_METRIC_RDMA_PORT_SPEED_GBPS, PROM_METRIC_RDMA_PORT_USABLE, PROM_METRIC_RDMA_PROBE_ERROR,
    PROM_METRIC_RDMA_PROBE_PORT_COUNT, PROM_METRIC_RDMA_PROBE_USABLE_PORT_COUNT,
    PROM_METRIC_RDMA_TRANSFER_ENGINE_START_FAILURES, PROM_METRIC_RDMA_TRANSFER_ENGINE_STATE,
    PROM_METRIC_SHM_FILE_ALLOCATED_BYTES, PROM_METRIC_SHM_FILE_SIZE_BYTES,
    PROM_METRIC_TCP_THREAD_LATENCY_SAMPLE_COUNT, PROM_METRIC_TCP_THREAD_LATENCY_STAT_US,
    PROM_METRIC_TCP_THREAD_TRANSPORT_BYTES_TOTAL, PROM_METRIC_TCP_THREAD_TRANSPORT_MESSAGES_TOTAL,
    PROM_METRIC_TOKIO_ALIVE_TASKS, PROM_METRIC_TOKIO_BUSY_PERCENT,
    PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH, PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT,
    PROM_METRIC_TOKIO_NUM_WORKERS, PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ,
    PROM_VALUE_KV_COMPONENT_LOCAL_IPC, PROM_VALUE_KV_COMPONENT_RPC_TRANSPORT,
    PROM_VALUE_KV_COMPONENT_TRANSFER_ENGINE,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMIT_FAILED,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMITTED,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_LANE_NOT_DIRECT,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_PEER_NOT_READY,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_REMAINING_HOPS,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_TRANSPORT_POLICY,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_ERROR,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_NOT_READY,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_USED,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_LANE_NOT_DIRECT,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_PEER_NOT_READY,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_REMAINING_HOPS,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_TRANSPORT_POLICY,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_ERROR,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_NOT_READY,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED,
    PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED,
};

use crate::prom_remote_write_actor::PromRemoteWriteHandle;
use crate::types::FsMountKind;

// The actor owns all "gather + build extra series + submit to remote-write actor" logic.
// KV/business code should only do leaf-ish `try_send` into this actor.

// Bounds memory when producers outpace the metrics tick (or when remote-write is slow/disabled).
const MAX_PENDING_EVENTS: usize = 8192;

#[derive(Clone, Debug)]
pub struct KvOpMetricPut {
    pub whole_put_us: i64,
    pub start_us: i64,
    pub transfer_us: i64,
    pub end_us: i64,
    pub rpc_of_put_start_us: i64,
    pub start_handle_us: i64,
    pub end_handle_us: i64,
    pub key: String,
    pub put_id: String,
    pub t1_us: i64,
    pub t2_us: i64,
    pub t3_us: i64,
    pub t4_us: i64,
    pub transfer_submit_blocking_us: i64,
    pub transfer_create_xfer_req_us: i64,
    pub transfer_post_xfer_req_us: i64,
    pub transfer_poll_wait_us: i64,
    pub transfer_poll_iters: i64,
    pub transfer_used_fast_path: bool,
    pub transfer_local_noop: bool,
    pub transfer_remote_transfer: bool,
}

#[derive(Clone, Debug)]
pub struct KvOpMetricGet {
    pub whole_get_us: i64,
    pub start_us: i64,
    pub transfer_us: i64,
    pub end_us: i64,
    pub start_handle_us: i64,
    pub end_handle_us: i64,
    pub key: String,
    pub get_id: String,
    pub t1_us: i64,
    pub t2_us: i64,
    pub t3_us: i64,
    pub t4_us: i64,
}

#[derive(Clone, Debug)]
pub enum KvOpMetric {
    Put(KvOpMetricPut),
    Get(KvOpMetricGet),
}

#[derive(Clone, Debug)]
pub struct KvOpEndBytesPulse {
    pub timestamp_ms: i64,
    pub op: &'static str,
    pub status: &'static str,
    pub key: String,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserveDirection {
    Tx,
    Rx,
}

impl ObserveDirection {
    pub const fn as_label(self) -> &'static str {
        match self {
            ObserveDirection::Tx => "tx",
            ObserveDirection::Rx => "rx",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserveComponent {
    RpcTransport,
    TransferEngine,
    LocalIpc,
}

impl ObserveComponent {
    pub const fn as_label(self) -> &'static str {
        match self {
            ObserveComponent::RpcTransport => PROM_VALUE_KV_COMPONENT_RPC_TRANSPORT,
            ObserveComponent::TransferEngine => PROM_VALUE_KV_COMPONENT_TRANSFER_ENGINE,
            ObserveComponent::LocalIpc => PROM_VALUE_KV_COMPONENT_LOCAL_IPC,
        }
    }

    const ALL: [Self; 3] = [
        ObserveComponent::RpcTransport,
        ObserveComponent::TransferEngine,
        ObserveComponent::LocalIpc,
    ];

    const fn index(self) -> usize {
        match self {
            ObserveComponent::RpcTransport => 0,
            ObserveComponent::TransferEngine => 1,
            ObserveComponent::LocalIpc => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserveFsIoOp {
    Read,
    Write,
}

impl ObserveFsIoOp {
    pub const fn as_label(self) -> &'static str {
        match self {
            ObserveFsIoOp::Read => "read",
            ObserveFsIoOp::Write => "write",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObserveNodeOverride {
    pub node: String,
    pub role: String,
}

#[derive(Clone, Debug)]
pub struct ObservePeerNetworkBytes {
    pub component: ObserveComponent,
    pub node_override: Option<ObserveNodeOverride>,
    pub peer: String,
    pub direction: ObserveDirection,
    pub bytes: u64,
}

#[derive(Clone, Debug)]
pub struct ObserveRdmaPortSnapshot {
    pub device: String,
    pub port: u8,
    pub netdev: Option<String>,
    pub pci_bdf: Option<String>,
    pub usable: bool,
    pub speed_gbps: Option<u32>,
    pub active_mtu_bytes: u32,
    pub gid_count: u32,
    pub numa_node: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct ObserveRdmaSnapshot {
    pub ports: Vec<ObserveRdmaPortSnapshot>,
    pub probe_error: bool,
    pub transfer_engine_state: String,
    pub transfer_engine_consecutive_start_failures: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ObserveTcpThreadLatencySample {
    pub metric: &'static str,
    pub lane: &'static str,
    pub duration_us: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct ObserveTcpThreadTransportSample {
    pub metric: &'static str,
    pub lane: &'static str,
    pub bytes: u64,
    pub messages: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ObserveP2pReceiveTransportSample {
    pub component: ObserveComponent,
    pub metric: ObserveP2pReceiveTransportMetric,
    pub bytes: u64,
    pub messages: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ObserveP2pRpcCompletionLatencySample {
    pub metric: &'static str,
    pub duration_us: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObserveTcpThreadTransportMetricKind {
    SendEnqueued,
    SocketSubmitted,
}

impl ObserveTcpThreadTransportMetricKind {
    const ALL: [Self; 2] = [Self::SendEnqueued, Self::SocketSubmitted];

    const fn as_label(self) -> &'static str {
        match self {
            Self::SendEnqueued => "send_enqueued",
            Self::SocketSubmitted => "socket_submitted",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::SendEnqueued => 0,
            Self::SocketSubmitted => 1,
        }
    }

    fn from_label(label: &'static str) -> Option<Self> {
        match label {
            "send_enqueued" => Some(Self::SendEnqueued),
            "socket_submitted" => Some(Self::SocketSubmitted),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObserveP2pReceiveTransportMetric {
    RecvCompleted,
    DispatchEnqueued,
    DispatchDequeued,
    DispatchStarted,
}

impl ObserveP2pReceiveTransportMetric {
    const ALL: [Self; 4] = [
        Self::RecvCompleted,
        Self::DispatchEnqueued,
        Self::DispatchDequeued,
        Self::DispatchStarted,
    ];

    const fn as_label(self) -> &'static str {
        match self {
            Self::RecvCompleted => "recv_completed",
            Self::DispatchEnqueued => "dispatch_enqueued",
            Self::DispatchDequeued => "dispatch_dequeued",
            Self::DispatchStarted => "dispatch_started",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::RecvCompleted => 0,
            Self::DispatchEnqueued => 1,
            Self::DispatchDequeued => 2,
            Self::DispatchStarted => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObserveP2pRpcCompletionMetricKind {
    ResponseSubmitted,
    ResponseSubmitFailed,
    UserRpcRequestFastPathUsed,
    UserRpcRequestSlowPathUsed,
    UserRpcRequestFastPathBypassTransportPolicy,
    UserRpcRequestFastPathBypassLaneNotDirect,
    UserRpcRequestFastPathBypassRemainingHops,
    UserRpcRequestFastPathBypassPeerNotReady,
    UserRpcRequestFastPathBypassBackendEpochMissing,
    UserRpcRequestFastPathFallbackSendNotReady,
    UserRpcRequestFastPathFallbackSendError,
    UserRpcResponseFastPathUsed,
    UserRpcResponseSlowPathUsed,
    UserRpcResponseFastPathBypassTransportPolicy,
    UserRpcResponseFastPathBypassLaneNotDirect,
    UserRpcResponseFastPathBypassRemainingHops,
    UserRpcResponseFastPathBypassPeerNotReady,
    UserRpcResponseFastPathBypassBackendEpochMissing,
    UserRpcResponseFastPathFallbackSendNotReady,
    UserRpcResponseFastPathFallbackSendError,
}

impl ObserveP2pRpcCompletionMetricKind {
    const ALL: [Self; 20] = [
        Self::ResponseSubmitted,
        Self::ResponseSubmitFailed,
        Self::UserRpcRequestFastPathUsed,
        Self::UserRpcRequestSlowPathUsed,
        Self::UserRpcRequestFastPathBypassTransportPolicy,
        Self::UserRpcRequestFastPathBypassLaneNotDirect,
        Self::UserRpcRequestFastPathBypassRemainingHops,
        Self::UserRpcRequestFastPathBypassPeerNotReady,
        Self::UserRpcRequestFastPathBypassBackendEpochMissing,
        Self::UserRpcRequestFastPathFallbackSendNotReady,
        Self::UserRpcRequestFastPathFallbackSendError,
        Self::UserRpcResponseFastPathUsed,
        Self::UserRpcResponseSlowPathUsed,
        Self::UserRpcResponseFastPathBypassTransportPolicy,
        Self::UserRpcResponseFastPathBypassLaneNotDirect,
        Self::UserRpcResponseFastPathBypassRemainingHops,
        Self::UserRpcResponseFastPathBypassPeerNotReady,
        Self::UserRpcResponseFastPathBypassBackendEpochMissing,
        Self::UserRpcResponseFastPathFallbackSendNotReady,
        Self::UserRpcResponseFastPathFallbackSendError,
    ];

    const fn as_label(self) -> &'static str {
        match self {
            Self::ResponseSubmitted => PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMITTED,
            Self::ResponseSubmitFailed => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMIT_FAILED
            }
            Self::UserRpcRequestFastPathUsed => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_USED
            }
            Self::UserRpcRequestSlowPathUsed => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED
            }
            Self::UserRpcRequestFastPathBypassTransportPolicy => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_TRANSPORT_POLICY
            }
            Self::UserRpcRequestFastPathBypassLaneNotDirect => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_LANE_NOT_DIRECT
            }
            Self::UserRpcRequestFastPathBypassRemainingHops => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_REMAINING_HOPS
            }
            Self::UserRpcRequestFastPathBypassPeerNotReady => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_PEER_NOT_READY
            }
            Self::UserRpcRequestFastPathBypassBackendEpochMissing => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING
            }
            Self::UserRpcRequestFastPathFallbackSendNotReady => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_NOT_READY
            }
            Self::UserRpcRequestFastPathFallbackSendError => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_ERROR
            }
            Self::UserRpcResponseFastPathUsed => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED
            }
            Self::UserRpcResponseSlowPathUsed => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED
            }
            Self::UserRpcResponseFastPathBypassTransportPolicy => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_TRANSPORT_POLICY
            }
            Self::UserRpcResponseFastPathBypassLaneNotDirect => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_LANE_NOT_DIRECT
            }
            Self::UserRpcResponseFastPathBypassRemainingHops => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_REMAINING_HOPS
            }
            Self::UserRpcResponseFastPathBypassPeerNotReady => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_PEER_NOT_READY
            }
            Self::UserRpcResponseFastPathBypassBackendEpochMissing => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING
            }
            Self::UserRpcResponseFastPathFallbackSendNotReady => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_NOT_READY
            }
            Self::UserRpcResponseFastPathFallbackSendError => {
                PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_ERROR
            }
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::ResponseSubmitted => 0,
            Self::ResponseSubmitFailed => 1,
            Self::UserRpcRequestFastPathUsed => 2,
            Self::UserRpcRequestSlowPathUsed => 3,
            Self::UserRpcRequestFastPathBypassTransportPolicy => 4,
            Self::UserRpcRequestFastPathBypassLaneNotDirect => 5,
            Self::UserRpcRequestFastPathBypassRemainingHops => 6,
            Self::UserRpcRequestFastPathBypassPeerNotReady => 7,
            Self::UserRpcRequestFastPathBypassBackendEpochMissing => 8,
            Self::UserRpcRequestFastPathFallbackSendNotReady => 9,
            Self::UserRpcRequestFastPathFallbackSendError => 10,
            Self::UserRpcResponseFastPathUsed => 11,
            Self::UserRpcResponseSlowPathUsed => 12,
            Self::UserRpcResponseFastPathBypassTransportPolicy => 13,
            Self::UserRpcResponseFastPathBypassLaneNotDirect => 14,
            Self::UserRpcResponseFastPathBypassRemainingHops => 15,
            Self::UserRpcResponseFastPathBypassPeerNotReady => 16,
            Self::UserRpcResponseFastPathBypassBackendEpochMissing => 17,
            Self::UserRpcResponseFastPathFallbackSendNotReady => 18,
            Self::UserRpcResponseFastPathFallbackSendError => 19,
        }
    }

    fn from_label(label: &'static str) -> Option<Self> {
        match label {
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMITTED => {
                Some(Self::ResponseSubmitted)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_RESPONSE_SUBMIT_FAILED => {
                Some(Self::ResponseSubmitFailed)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_USED => {
                Some(Self::UserRpcRequestFastPathUsed)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_SLOW_PATH_USED => {
                Some(Self::UserRpcRequestSlowPathUsed)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_TRANSPORT_POLICY => {
                Some(Self::UserRpcRequestFastPathBypassTransportPolicy)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_LANE_NOT_DIRECT => {
                Some(Self::UserRpcRequestFastPathBypassLaneNotDirect)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_REMAINING_HOPS => {
                Some(Self::UserRpcRequestFastPathBypassRemainingHops)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_PEER_NOT_READY => {
                Some(Self::UserRpcRequestFastPathBypassPeerNotReady)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING => {
                Some(Self::UserRpcRequestFastPathBypassBackendEpochMissing)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_NOT_READY => {
                Some(Self::UserRpcRequestFastPathFallbackSendNotReady)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_REQUEST_FAST_PATH_FALLBACK_SEND_ERROR => {
                Some(Self::UserRpcRequestFastPathFallbackSendError)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_USED => {
                Some(Self::UserRpcResponseFastPathUsed)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_SLOW_PATH_USED => {
                Some(Self::UserRpcResponseSlowPathUsed)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_TRANSPORT_POLICY => {
                Some(Self::UserRpcResponseFastPathBypassTransportPolicy)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_LANE_NOT_DIRECT => {
                Some(Self::UserRpcResponseFastPathBypassLaneNotDirect)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_REMAINING_HOPS => {
                Some(Self::UserRpcResponseFastPathBypassRemainingHops)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_PEER_NOT_READY => {
                Some(Self::UserRpcResponseFastPathBypassPeerNotReady)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_BYPASS_BACKEND_EPOCH_MISSING => {
                Some(Self::UserRpcResponseFastPathBypassBackendEpochMissing)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_NOT_READY => {
                Some(Self::UserRpcResponseFastPathFallbackSendNotReady)
            }
            PROM_VALUE_P2P_RPC_COMPLETION_METRIC_USER_RPC_RESPONSE_FAST_PATH_FALLBACK_SEND_ERROR => {
                Some(Self::UserRpcResponseFastPathFallbackSendError)
            }
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObserveTcpThreadLaneKind {
    Control,
    Bulk0,
    Bulk1,
    Bulk2,
    Bulk3,
    Bulk4,
    Bulk5,
    Bulk6,
    Bulk7,
}

impl ObserveTcpThreadLaneKind {
    const ALL: [Self; 9] = [
        Self::Control,
        Self::Bulk0,
        Self::Bulk1,
        Self::Bulk2,
        Self::Bulk3,
        Self::Bulk4,
        Self::Bulk5,
        Self::Bulk6,
        Self::Bulk7,
    ];

    const fn as_label(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Bulk0 => "bulk0",
            Self::Bulk1 => "bulk1",
            Self::Bulk2 => "bulk2",
            Self::Bulk3 => "bulk3",
            Self::Bulk4 => "bulk4",
            Self::Bulk5 => "bulk5",
            Self::Bulk6 => "bulk6",
            Self::Bulk7 => "bulk7",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Bulk0 => 1,
            Self::Bulk1 => 2,
            Self::Bulk2 => 3,
            Self::Bulk3 => 4,
            Self::Bulk4 => 5,
            Self::Bulk5 => 6,
            Self::Bulk6 => 7,
            Self::Bulk7 => 8,
        }
    }

    fn from_label(label: &'static str) -> Option<Self> {
        match label {
            "control" => Some(Self::Control),
            "bulk0" => Some(Self::Bulk0),
            "bulk1" => Some(Self::Bulk1),
            "bulk2" => Some(Self::Bulk2),
            "bulk3" => Some(Self::Bulk3),
            "bulk4" => Some(Self::Bulk4),
            "bulk5" => Some(Self::Bulk5),
            "bulk6" => Some(Self::Bulk6),
            "bulk7" => Some(Self::Bulk7),
            _ => None,
        }
    }
}

const TCP_THREAD_TRANSPORT_LANE_COUNT: usize = 9;
const TCP_THREAD_TRANSPORT_BUCKET_COUNT: usize = 18;

const P2P_RECV_TRANSPORT_COMPONENT_COUNT: usize = 3;
const P2P_RECV_TRANSPORT_BUCKET_COUNT: usize = 12;
const P2P_RPC_COMPLETION_BUCKET_COUNT: usize = ObserveP2pRpcCompletionMetricKind::ALL.len();

struct ObserveTcpThreadTransportBucket {
    bytes: AtomicU64,
    messages: AtomicU64,
}

impl ObserveTcpThreadTransportBucket {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            messages: AtomicU64::new(0),
        }
    }
}

struct ObserveTcpThreadTransportAccumulator {
    buckets: [ObserveTcpThreadTransportBucket; TCP_THREAD_TRANSPORT_BUCKET_COUNT],
    dirty: AtomicBool,
}

impl ObserveTcpThreadTransportAccumulator {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| ObserveTcpThreadTransportBucket::new()),
            dirty: AtomicBool::new(false),
        }
    }

    fn record(
        &self,
        metric: ObserveTcpThreadTransportMetricKind,
        lane: ObserveTcpThreadLaneKind,
        bytes: u64,
        messages: u64,
    ) {
        let idx = tcp_thread_transport_bucket_index(metric, lane);
        let bucket = &self.buckets[idx];
        if bytes > 0 {
            bucket.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        if messages > 0 {
            bucket.messages.fetch_add(messages, Ordering::Relaxed);
        }
    }

    fn drain_once(&self) -> Vec<ObserveTcpThreadTransportSample> {
        let mut out = Vec::with_capacity(TCP_THREAD_TRANSPORT_BUCKET_COUNT);
        for metric in ObserveTcpThreadTransportMetricKind::ALL {
            for lane in ObserveTcpThreadLaneKind::ALL {
                let idx = tcp_thread_transport_bucket_index(metric, lane);
                let bucket = &self.buckets[idx];
                let bytes = bucket.bytes.swap(0, Ordering::AcqRel);
                let messages = bucket.messages.swap(0, Ordering::AcqRel);
                if bytes == 0 && messages == 0 {
                    continue;
                }
                out.push(ObserveTcpThreadTransportSample {
                    metric: metric.as_label(),
                    lane: lane.as_label(),
                    bytes,
                    messages,
                });
            }
        }
        out
    }
}

struct ObserveP2pReceiveTransportBucket {
    bytes: AtomicU64,
    messages: AtomicU64,
}

impl ObserveP2pReceiveTransportBucket {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            messages: AtomicU64::new(0),
        }
    }
}

struct ObserveP2pReceiveTransportAccumulator {
    buckets: [ObserveP2pReceiveTransportBucket; P2P_RECV_TRANSPORT_BUCKET_COUNT],
    dirty: AtomicBool,
}

impl ObserveP2pReceiveTransportAccumulator {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| ObserveP2pReceiveTransportBucket::new()),
            dirty: AtomicBool::new(false),
        }
    }

    fn record(
        &self,
        metric: ObserveP2pReceiveTransportMetric,
        component: ObserveComponent,
        bytes: u64,
        messages: u64,
    ) {
        let idx = p2p_recv_transport_bucket_index(metric, component);
        let bucket = &self.buckets[idx];
        if bytes > 0 {
            bucket.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        if messages > 0 {
            bucket.messages.fetch_add(messages, Ordering::Relaxed);
        }
    }

    fn drain_once(&self) -> Vec<ObserveP2pReceiveTransportSample> {
        let mut out = Vec::with_capacity(P2P_RECV_TRANSPORT_BUCKET_COUNT);
        for metric in ObserveP2pReceiveTransportMetric::ALL {
            for component in ObserveComponent::ALL {
                let idx = p2p_recv_transport_bucket_index(metric, component);
                let bucket = &self.buckets[idx];
                let bytes = bucket.bytes.swap(0, Ordering::AcqRel);
                let messages = bucket.messages.swap(0, Ordering::AcqRel);
                if bytes == 0 && messages == 0 {
                    continue;
                }
                out.push(ObserveP2pReceiveTransportSample {
                    component,
                    metric,
                    bytes,
                    messages,
                });
            }
        }
        out
    }
}

struct ObserveP2pRpcCompletionBucket {
    bytes: AtomicU64,
    messages: AtomicU64,
}

impl ObserveP2pRpcCompletionBucket {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            messages: AtomicU64::new(0),
        }
    }
}

struct ObserveP2pRpcCompletionAccumulator {
    buckets: [ObserveP2pRpcCompletionBucket; P2P_RPC_COMPLETION_BUCKET_COUNT],
    dirty: AtomicBool,
}

impl ObserveP2pRpcCompletionAccumulator {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| ObserveP2pRpcCompletionBucket::new()),
            dirty: AtomicBool::new(false),
        }
    }

    fn record(&self, metric: ObserveP2pRpcCompletionMetricKind, bytes: u64, messages: u64) {
        let bucket = &self.buckets[metric.index()];
        if bytes > 0 {
            bucket.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        if messages > 0 {
            bucket.messages.fetch_add(messages, Ordering::Relaxed);
        }
    }

    fn drain_once(&self) -> Vec<(ObserveP2pRpcCompletionMetricKind, u64, u64)> {
        let mut out = Vec::with_capacity(P2P_RPC_COMPLETION_BUCKET_COUNT);
        for metric in ObserveP2pRpcCompletionMetricKind::ALL {
            let bucket = &self.buckets[metric.index()];
            let bytes = bucket.bytes.swap(0, Ordering::AcqRel);
            let messages = bucket.messages.swap(0, Ordering::AcqRel);
            if bytes == 0 && messages == 0 {
                continue;
            }
            out.push((metric, bytes, messages));
        }
        out
    }
}

const fn tcp_thread_transport_bucket_index(
    metric: ObserveTcpThreadTransportMetricKind,
    lane: ObserveTcpThreadLaneKind,
) -> usize {
    metric.index() * TCP_THREAD_TRANSPORT_LANE_COUNT + lane.index()
}

const fn p2p_recv_transport_bucket_index(
    metric: ObserveP2pReceiveTransportMetric,
    component: ObserveComponent,
) -> usize {
    metric.index() * P2P_RECV_TRANSPORT_COMPONENT_COUNT + component.index()
}

#[derive(Clone, Debug)]
struct TcpThreadLatencyWindowSummary {
    metric: &'static str,
    lane: &'static str,
    sample_count: usize,
    mean_us: f64,
    p50_us: i64,
    p95_us: i64,
    p99_us: i64,
    min_us: i64,
    max_us: i64,
}

#[derive(Clone, Debug)]
struct P2pRpcCompletionCounterWindowSummary {
    metric: &'static str,
    bytes: u64,
    messages: u64,
}

#[derive(Clone, Debug)]
struct OperationWindowSummary {
    metric: &'static str,
    sample_count: usize,
    mean: f64,
    p50: i64,
    p95: i64,
    p99: i64,
    min: i64,
    max: i64,
}

#[derive(Clone, Debug)]
pub enum ObserveOp {
    SubmitKvOpMetric {
        metric: KvOpMetric,
    },
    EmitOpEndBytesPulse {
        pulse: KvOpEndBytesPulse,
    },
    RecordClientNetworkBytes {
        direction: ObserveDirection,
        bytes: u64,
    },
    RecordFsIoOps {
        op: ObserveFsIoOp,
        ops: u64,
    },
    RecordTcpThreadLatencySample {
        sample: ObserveTcpThreadLatencySample,
    },
    RecordP2pRpcCompletionLatencySample {
        sample: ObserveP2pRpcCompletionLatencySample,
    },
    FlushTcpThreadTransportAccumulator,
    FlushP2pReceiveTransportAccumulator,
    FlushP2pRpcCompletionAccumulator,
    RecordPeerNetworkBytes(ObservePeerNetworkBytes),
    SetSegmentCapacityBytes {
        node: String,
        device: String,
        bytes: u64,
    },
    SetSegmentUsedBytes {
        node: String,
        device: String,
        bytes: u64,
    },
    SetFsMountFsBytes {
        mount_kind: FsMountKind,
        target_dir_abs: String,
        mountpoint_dir_abs: String,
        used_bytes: u64,
        total_bytes: u64,
    },
    SetShmFileBytes {
        shm_dir_abs: String,
        file_path_abs: String,
        logical_size_bytes: u64,
        allocated_bytes: u64,
    },
    ReplaceSelfRdmaSnapshot {
        snapshot: ObserveRdmaSnapshot,
    },
}

#[derive(Clone)]
pub struct ObserveHandle {
    tx: mpsc::Sender<ObserveOp>,
    tcp_thread_transport_accumulator: Arc<ObserveTcpThreadTransportAccumulator>,
    p2p_receive_transport_accumulator: Arc<ObserveP2pReceiveTransportAccumulator>,
    p2p_rpc_completion_accumulator: Arc<ObserveP2pRpcCompletionAccumulator>,
}

impl ObserveHandle {
    pub fn try_submit(&self, op: ObserveOp) {
        if let Err(e) = self.tx.try_send(op) {
            warn!("observe actor dropped ObserveOp: {}", e);
        }
    }

    pub fn try_record_peer_network_bytes(
        &self,
        component: ObserveComponent,
        peer: &str,
        direction: ObserveDirection,
        bytes: u64,
    ) {
        if bytes == 0 {
            return;
        }
        // English note:
        // - Avoid allocating `peer: String` unless the channel has capacity.
        // - This keeps the hot path strictly best-effort (no await, no blocking).
        match self.tx.try_reserve() {
            Ok(permit) => {
                permit.send(ObserveOp::RecordPeerNetworkBytes(ObservePeerNetworkBytes {
                    component,
                    node_override: None,
                    peer: peer.to_string(),
                    direction,
                    bytes,
                }));
            }
            Err(e) => {
                warn!("observe actor dropped RecordPeerNetworkBytes: {}", e);
            }
        }
    }

    pub fn try_record_peer_network_bytes_override(
        &self,
        component: ObserveComponent,
        node: &str,
        role: &str,
        peer: &str,
        direction: ObserveDirection,
        bytes: u64,
    ) {
        if bytes == 0 {
            return;
        }
        // English note:
        // - Some fast-paths need to attribute bytes to a different logical node/role than "self"
        //   (e.g. local IPC between externals should be charged to the owner daemon).
        // - Keep this best-effort and non-blocking; drop on backpressure.
        match self.tx.try_reserve() {
            Ok(permit) => {
                permit.send(ObserveOp::RecordPeerNetworkBytes(ObservePeerNetworkBytes {
                    component,
                    node_override: Some(ObserveNodeOverride {
                        node: node.to_string(),
                        role: role.to_string(),
                    }),
                    peer: peer.to_string(),
                    direction,
                    bytes,
                }));
            }
            Err(e) => {
                warn!(
                    "observe actor dropped RecordPeerNetworkBytesOverride: {}",
                    e
                );
            }
        }
    }

    pub fn try_record_tcp_thread_latency_sample(
        &self,
        metric: &'static str,
        lane: &'static str,
        duration_us: i64,
    ) {
        if duration_us <= 0 {
            return;
        }
        self.try_submit(ObserveOp::RecordTcpThreadLatencySample {
            sample: ObserveTcpThreadLatencySample {
                metric,
                lane,
                duration_us,
            },
        });
    }

    pub fn try_record_tcp_thread_transport_sample(
        &self,
        metric: &'static str,
        lane: &'static str,
        bytes: u64,
        messages: u64,
    ) {
        if bytes == 0 && messages == 0 {
            return;
        }
        let Some(metric_kind) = ObserveTcpThreadTransportMetricKind::from_label(metric) else {
            warn!("unsupported tcp_thread transport metric label: {}", metric);
            return;
        };
        let Some(lane_kind) = ObserveTcpThreadLaneKind::from_label(lane) else {
            warn!("unsupported tcp_thread transport lane label: {}", lane);
            return;
        };
        self.tcp_thread_transport_accumulator
            .record(metric_kind, lane_kind, bytes, messages);
        self.tcp_thread_transport_accumulator
            .dirty
            .store(true, Ordering::Release);
        if let Err(e) = self
            .tx
            .try_send(ObserveOp::FlushTcpThreadTransportAccumulator)
        {
            debug!(
                "observe actor dropped FlushTcpThreadTransportAccumulator hint: {}",
                e
            );
        }
    }

    pub fn try_record_p2p_receive_transport_sample(
        &self,
        component: ObserveComponent,
        metric: ObserveP2pReceiveTransportMetric,
        bytes: u64,
        messages: u64,
    ) {
        if bytes == 0 && messages == 0 {
            return;
        }
        self.p2p_receive_transport_accumulator
            .record(metric, component, bytes, messages);
        self.p2p_receive_transport_accumulator
            .dirty
            .store(true, Ordering::Release);
        if let Err(e) = self
            .tx
            .try_send(ObserveOp::FlushP2pReceiveTransportAccumulator)
        {
            debug!(
                "observe actor dropped FlushP2pReceiveTransportAccumulator hint: {}",
                e
            );
        }
    }

    pub fn try_record_p2p_rpc_completion_latency_sample(
        &self,
        metric: &'static str,
        duration_us: i64,
    ) {
        if duration_us <= 0 {
            return;
        }
        self.try_submit(ObserveOp::RecordP2pRpcCompletionLatencySample {
            sample: ObserveP2pRpcCompletionLatencySample {
                metric,
                duration_us,
            },
        });
    }

    pub fn try_record_p2p_rpc_completion_sample(
        &self,
        metric: &'static str,
        bytes: u64,
        messages: u64,
    ) {
        if bytes == 0 && messages == 0 {
            return;
        }
        let Some(metric_kind) = ObserveP2pRpcCompletionMetricKind::from_label(metric) else {
            warn!("unsupported p2p rpc completion metric label: {}", metric);
            return;
        };
        self.p2p_rpc_completion_accumulator
            .record(metric_kind, bytes, messages);
        self.p2p_rpc_completion_accumulator
            .dirty
            .store(true, Ordering::Release);
        if let Err(e) = self
            .tx
            .try_send(ObserveOp::FlushP2pRpcCompletionAccumulator)
        {
            debug!(
                "observe actor dropped FlushP2pRpcCompletionAccumulator hint: {}",
                e
            );
        }
    }

    pub fn set_shm_file_bytes(
        &self,
        shm_dir_abs: &str,
        file_path_abs: &str,
        logical_size_bytes: u64,
        allocated_bytes: u64,
    ) {
        self.try_submit(ObserveOp::SetShmFileBytes {
            shm_dir_abs: shm_dir_abs.to_string(),
            file_path_abs: file_path_abs.to_string(),
            logical_size_bytes,
            allocated_bytes,
        });
    }
}

#[derive(Clone, Debug)]
struct SystemSample {
    total: u64,
    idle: u64,
}

#[derive(Clone, Debug)]
struct ProcessCpuSample {
    total_ticks: u64,
    at: std::time::Instant,
}

#[derive(Clone, Debug)]
struct TokioRuntimeSample {
    at: std::time::Instant,
    worker_busy_nanos: Vec<u128>,
    worker_park_unpark_counts: Vec<u64>,
}

pub struct KvMetricsActorOwned {
    rx: mpsc::Receiver<ObserveOp>,
    prom: PromRemoteWriteHandle,
    tcp_thread_transport_accumulator: Arc<ObserveTcpThreadTransportAccumulator>,
    p2p_receive_transport_accumulator: Arc<ObserveP2pReceiveTransportAccumulator>,
    p2p_rpc_completion_accumulator: Arc<ObserveP2pRpcCompletionAccumulator>,

    registry: Registry,

    // Metrics (Prom collectors)
    operation_stat_gauge: GaugeVec,
    client_network_bytes_counter: CounterVec,
    kv_peer_network_bytes_counter: CounterVec,
    tcp_thread_latency_stat_gauge: GaugeVec,
    tcp_thread_latency_sample_count_gauge: GaugeVec,
    tcp_thread_transport_bytes_counter: CounterVec,
    tcp_thread_transport_messages_counter: CounterVec,
    p2p_receive_transport_bytes_counter: CounterVec,
    p2p_receive_transport_messages_counter: CounterVec,
    p2p_rpc_completion_latency_stat_gauge: GaugeVec,
    p2p_rpc_completion_latency_sample_count_gauge: GaugeVec,
    p2p_rpc_completion_bytes_counter: CounterVec,
    p2p_rpc_completion_messages_counter: CounterVec,
    node_cpu_usage_gauge: GaugeVec,
    node_cpu_logical_cores_gauge: GaugeVec,
    node_memory_usage_gauge: GaugeVec,
    node_memory_total_gauge: GaugeVec,
    container_memory_usage_gauge: GaugeVec,
    container_memory_limit_gauge: GaugeVec,
    process_resident_memory_gauge: GaugeVec,
    process_cpu_usage_gauge: GaugeVec,
    tokio_num_workers_gauge: GaugeVec,
    tokio_alive_tasks_gauge: GaugeVec,
    tokio_global_queue_depth_gauge: GaugeVec,
    tokio_busy_percent_gauge: GaugeVec,
    tokio_max_worker_busy_percent_gauge: GaugeVec,
    tokio_park_unpark_rate_gauge: GaugeVec,
    fs_mount_fs_used_bytes_gauge: GaugeVec,
    fs_mount_fs_total_bytes_gauge: GaugeVec,
    shm_file_size_bytes_gauge: GaugeVec,
    shm_file_allocated_bytes_gauge: GaugeVec,
    fs_io_ops_counter: CounterVec,
    exporter_heartbeat_gauge: GaugeVec,
    node_uptime_counter: CounterVec,
    segment_capacity_gauge: GaugeVec,
    segment_used_gauge: GaugeVec,
    node_network_transmit_bytes_counter: CounterVec,
    node_network_receive_bytes_counter: CounterVec,
    rdma_probe_port_count_gauge: GaugeVec,
    rdma_probe_usable_port_count_gauge: GaugeVec,
    rdma_probe_error_gauge: GaugeVec,
    rdma_port_usable_gauge: GaugeVec,
    rdma_port_speed_gbps_gauge: GaugeVec,
    rdma_port_active_mtu_bytes_gauge: GaugeVec,
    rdma_port_gid_count_gauge: GaugeVec,
    rdma_port_numa_node_gauge: GaugeVec,
    rdma_transfer_engine_state_gauge: GaugeVec,
    rdma_transfer_engine_start_failures_gauge: GaugeVec,
    last_logged_rdma_snapshot_summary: Mutex<Option<String>>,

    // Internal-only sampling state
    cpu_sample: Mutex<Option<SystemSample>>,
    process_cpu_sample: Mutex<Option<ProcessCpuSample>>,
    tokio_runtime_sample: Mutex<Option<TokioRuntimeSample>>,
    net_bytes_last_sample: Mutex<HashMap<String, (u64, u64)>>,
    last_uptime_observed: AtomicU64,

    // Stable labels for node-scoped metrics
    node_id: String,
    node_role: String,

    // Per-tick pending leaf events
    pending_kv_op_metrics: Vec<KvOpMetric>,
    pending_op_end_pulses: Vec<KvOpEndBytesPulse>,
    pending_tcp_thread_latency_samples: Vec<ObserveTcpThreadLatencySample>,
    pending_p2p_rpc_completion_latency_samples: Vec<ObserveP2pRpcCompletionLatencySample>,
    received_kv_op_metric_count: u64,
    received_op_end_pulse_count: u64,
    received_tcp_thread_latency_sample_count: u64,
    received_p2p_rpc_completion_latency_sample_count: u64,
    flush_count: u64,

    enable_system_metrics: bool,
}

fn register_collector(registry: &Registry, collector: Box<dyn Collector>) {
    if let Err(err) = registry.register(collector) {
        if !matches!(err, prometheus::Error::AlreadyReg) {
            warn!("failed to register collector: {err}");
        }
    }
}

impl KvMetricsActorOwned {
    pub fn new(
        node_id: String,
        node_role: String,
        prom: PromRemoteWriteHandle,
        enable_system_metrics: bool,
    ) -> (ObserveHandle, Self) {
        let (tx, rx) = mpsc::channel(MAX_PENDING_EVENTS);
        let tcp_thread_transport_accumulator =
            Arc::new(ObserveTcpThreadTransportAccumulator::new());
        let p2p_receive_transport_accumulator =
            Arc::new(ObserveP2pReceiveTransportAccumulator::new());
        let p2p_rpc_completion_accumulator = Arc::new(ObserveP2pRpcCompletionAccumulator::new());
        let handle = ObserveHandle {
            tx,
            tcp_thread_transport_accumulator: tcp_thread_transport_accumulator.clone(),
            p2p_receive_transport_accumulator: p2p_receive_transport_accumulator.clone(),
            p2p_rpc_completion_accumulator: p2p_rpc_completion_accumulator.clone(),
        };

        let registry = Registry::new();

        let operation_stat_gauge = GaugeVec::new(
            Opts::new(
                "kv_operation_latency_stat_microseconds",
                "Aggregated latency statistics per client (mean/p95/p99/min/max) for KV operations (in microseconds)",
            ),
            &["client", "metric", "stat"],
        )
        .expect("operation stat gauge");

        let client_network_bytes_counter = CounterVec::new(
            Opts::new(
                "client_network_bytes_total",
                "Total bytes exchanged between clients and KVCache node (per node/role/direction)",
            ),
            &["node", "role", "direction"],
        )
        .expect("client network bytes counter");

        let kv_peer_network_bytes_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_KV_PEER_NETWORK_BYTES_TOTAL,
                "Total bytes exchanged between KV members, attributed by (node, peer, component, direction)",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_COMPONENT,
                PROM_LABEL_PEER,
                "direction",
            ],
        )
        .expect("kv peer network bytes counter");

        let tcp_thread_latency_stat_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TCP_THREAD_LATENCY_STAT_US,
                "Windowed tcp_thread latency statistics by node, lane, and metric (microseconds)",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_METRIC,
                PROM_LABEL_TCP_THREAD_LANE,
                PROM_LABEL_STAT,
            ],
        )
        .expect("tcp_thread latency stat gauge");

        let tcp_thread_latency_sample_count_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TCP_THREAD_LATENCY_SAMPLE_COUNT,
                "Windowed tcp_thread latency sample count by node, lane, and metric",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_METRIC,
                PROM_LABEL_TCP_THREAD_LANE,
            ],
        )
        .expect("tcp_thread latency sample count gauge");

        let tcp_thread_transport_bytes_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_TCP_THREAD_TRANSPORT_BYTES_TOTAL,
                "Total tcp_thread transport bytes by node, role, metric, and lane",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_METRIC,
                PROM_LABEL_TCP_THREAD_LANE,
            ],
        )
        .expect("tcp_thread transport bytes counter");

        let tcp_thread_transport_messages_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_TCP_THREAD_TRANSPORT_MESSAGES_TOTAL,
                "Total tcp_thread transport messages by node, role, metric, and lane",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_METRIC,
                PROM_LABEL_TCP_THREAD_LANE,
            ],
        )
        .expect("tcp_thread transport messages counter");

        let p2p_receive_transport_bytes_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_P2P_RECV_TRANSPORT_BYTES_TOTAL,
                "Total p2p receive transport bytes by node, role, component, and metric",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_COMPONENT,
                PROM_LABEL_METRIC,
            ],
        )
        .expect("p2p receive transport bytes counter");

        let p2p_receive_transport_messages_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_P2P_RECV_TRANSPORT_MESSAGES_TOTAL,
                "Total p2p receive transport messages by node, role, component, and metric",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_COMPONENT,
                PROM_LABEL_METRIC,
            ],
        )
        .expect("p2p receive transport messages counter");

        let p2p_rpc_completion_latency_stat_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_STAT_US,
                "Windowed p2p rpc completion latency stats in microseconds",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_METRIC,
                PROM_LABEL_STAT,
            ],
        )
        .expect("p2p rpc completion latency stat gauge");

        let p2p_rpc_completion_latency_sample_count_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_P2P_RPC_COMPLETION_LATENCY_SAMPLE_COUNT,
                "Windowed p2p rpc completion latency sample counts",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE, PROM_LABEL_METRIC],
        )
        .expect("p2p rpc completion latency sample count gauge");

        let p2p_rpc_completion_bytes_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_P2P_RPC_COMPLETION_BYTES_TOTAL,
                "Total p2p rpc completion bytes by node, role, and metric",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE, PROM_LABEL_METRIC],
        )
        .expect("p2p rpc completion bytes counter");

        let p2p_rpc_completion_messages_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_P2P_RPC_COMPLETION_MESSAGES_TOTAL,
                "Total p2p rpc completion messages by node, role, and metric",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE, PROM_LABEL_METRIC],
        )
        .expect("p2p rpc completion messages counter");

        let node_cpu_usage_gauge = GaugeVec::new(
            Opts::new(
                "node_cpu_usage_percent",
                "Instant CPU usage percentage for this node",
            ),
            &["node", "role"],
        )
        .expect("node cpu usage gauge");

        let node_cpu_logical_cores_gauge = GaugeVec::new(
            Opts::new(
                "node_cpu_logical_cores",
                "Logical CPU core count for this node (from /proc/stat cpuN lines)",
            ),
            &["node", "role"],
        )
        .expect("node cpu logical cores gauge");

        let node_memory_usage_gauge = GaugeVec::new(
            Opts::new(
                "node_memory_usage_bytes",
                "Resident memory usage in bytes for this node",
            ),
            &["node", "role"],
        )
        .expect("node memory usage gauge");

        let node_memory_total_gauge = GaugeVec::new(
            Opts::new(
                "node_memory_total_bytes",
                "Total system memory in bytes for this node",
            ),
            &["node", "role"],
        )
        .expect("node memory total gauge");

        let container_memory_usage_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_CONTAINER_MEMORY_USAGE_BYTES,
                "Container/cgroup memory usage in bytes for this process",
            ),
            &["node", "role"],
        )
        .expect("container memory usage gauge");

        let container_memory_limit_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_CONTAINER_MEMORY_LIMIT_BYTES,
                "Container/cgroup memory limit in bytes for this process",
            ),
            &["node", "role"],
        )
        .expect("container memory limit gauge");

        let process_resident_memory_gauge = GaugeVec::new(
            Opts::new(
                "process_resident_memory_bytes",
                "Resident memory (RSS) used by the process in bytes",
            ),
            &["node", "role"],
        )
        .expect("process rss gauge");

        let process_cpu_usage_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_PROCESS_CPU_USAGE_PERCENT,
                "Process CPU usage percentage (top-style: 0..100*cores)",
            ),
            &["node", "role"],
        )
        .expect("process cpu usage gauge");

        let tokio_num_workers_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TOKIO_NUM_WORKERS,
                "Current Tokio runtime worker thread count",
            ),
            &["node", "role"],
        )
        .expect("tokio num workers gauge");

        let tokio_alive_tasks_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TOKIO_ALIVE_TASKS,
                "Current Tokio runtime alive task count",
            ),
            &["node", "role"],
        )
        .expect("tokio alive tasks gauge");

        let tokio_global_queue_depth_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TOKIO_GLOBAL_QUEUE_DEPTH,
                "Current Tokio runtime global queue depth",
            ),
            &["node", "role"],
        )
        .expect("tokio global queue depth gauge");

        let tokio_busy_percent_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TOKIO_BUSY_PERCENT,
                "Windowed Tokio runtime busy percentage across all workers",
            ),
            &["node", "role"],
        )
        .expect("tokio busy percent gauge");

        let tokio_max_worker_busy_percent_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TOKIO_MAX_WORKER_BUSY_PERCENT,
                "Windowed max busy percentage of a single Tokio worker thread",
            ),
            &["node", "role"],
        )
        .expect("tokio max worker busy percent gauge");

        let tokio_park_unpark_rate_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_TOKIO_PARK_UNPARK_RATE_HZ,
                "Windowed Tokio worker park/unpark transition rate per second",
            ),
            &["node", "role"],
        )
        .expect("tokio park/unpark rate gauge");

        let fs_mount_fs_used_bytes_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_FS_MOUNT_FS_USED_BYTES,
                "Used bytes for the filesystem containing a user-facing FS mount dir (statvfs)",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_FS_MOUNT_KIND,
                PROM_LABEL_FS_TARGET_DIR_ABS,
                PROM_LABEL_FS_MOUNTPOINT_DIR_ABS,
            ],
        )
        .expect("fs mount fs used bytes gauge");

        let fs_mount_fs_total_bytes_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_FS_MOUNT_FS_TOTAL_BYTES,
                "Total bytes for the filesystem containing a user-facing FS mount dir (statvfs)",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_FS_MOUNT_KIND,
                PROM_LABEL_FS_TARGET_DIR_ABS,
                PROM_LABEL_FS_MOUNTPOINT_DIR_ABS,
            ],
        )
        .expect("fs mount fs total bytes gauge");

        let shm_file_size_bytes_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_SHM_FILE_SIZE_BYTES,
                "Logical file size in bytes for files under the shared memory root",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                "shm_dir_abs",
                "file_path_abs",
            ],
        )
        .expect("shm file size bytes gauge");

        let shm_file_allocated_bytes_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_SHM_FILE_ALLOCATED_BYTES,
                "Allocated bytes (st_blocks * 512) for files under the shared memory root",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                "shm_dir_abs",
                "file_path_abs",
            ],
        )
        .expect("shm file allocated bytes gauge");

        let fs_io_ops_counter = CounterVec::new(
            Opts::new(
                PROM_METRIC_FS_IO_OPS_TOTAL,
                "Total FluxonFS I/O operations (path-agnostic), attributed by (node, role, fs_io_op)",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE, PROM_LABEL_FS_IO_OP],
        )
        .expect("fs io ops counter");

        let exporter_heartbeat_gauge = GaugeVec::new(
            Opts::new(
                "exporter_heartbeat",
                "Exporter heartbeat timestamp (seconds)",
            ),
            &["node", "role"],
        )
        .expect("exporter heartbeat gauge");

        let node_uptime_counter = CounterVec::new(
            Opts::new("node_uptime_seconds", "Monotonic uptime counter in seconds"),
            &["node", "role"],
        )
        .expect("node uptime counter");

        let segment_capacity_gauge = GaugeVec::new(
            Opts::new(
                "kvcache_segment_capacity_bytes",
                "Total capacity in bytes for each registered segment (per node/device)",
            ),
            &["node", "device"],
        )
        .expect("segment capacity gauge");

        let segment_used_gauge = GaugeVec::new(
            Opts::new(
                "kvcache_segment_used_bytes",
                "Used bytes for each registered segment (per node/device)",
            ),
            &["node", "device"],
        )
        .expect("segment used gauge");

        let node_network_transmit_bytes_counter = CounterVec::new(
            Opts::new(
                "node_network_transmit_bytes_total",
                "System network bytes per interface (TX) counter",
            ),
            &["node", "device"],
        )
        .expect("network tx counter");

        let node_network_receive_bytes_counter = CounterVec::new(
            Opts::new(
                "node_network_receive_bytes_total",
                "System network bytes per interface (RX) counter",
            ),
            &["node", "device"],
        )
        .expect("network rx counter");

        let rdma_probe_port_count_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PROBE_PORT_COUNT,
                "Detected RDMA port count from the latest self probe",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE],
        )
        .expect("rdma probe port count gauge");

        let rdma_probe_usable_port_count_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PROBE_USABLE_PORT_COUNT,
                "Usable RDMA port count from the latest self probe",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE],
        )
        .expect("rdma probe usable port count gauge");

        let rdma_probe_error_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PROBE_ERROR,
                "1 when the latest RDMA self probe reported an error",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE],
        )
        .expect("rdma probe error gauge");

        let rdma_port_usable_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PORT_USABLE,
                "RDMA port usability from the latest self probe",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_RDMA_DEVICE,
                PROM_LABEL_RDMA_PORT,
                PROM_LABEL_RDMA_NETDEV,
                PROM_LABEL_RDMA_PCI_BDF,
            ],
        )
        .expect("rdma port usable gauge");

        let rdma_port_speed_gbps_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PORT_SPEED_GBPS,
                "RDMA port speed in Gbps from the latest self probe",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_RDMA_DEVICE,
                PROM_LABEL_RDMA_PORT,
                PROM_LABEL_RDMA_NETDEV,
                PROM_LABEL_RDMA_PCI_BDF,
            ],
        )
        .expect("rdma port speed gauge");

        let rdma_port_active_mtu_bytes_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PORT_ACTIVE_MTU_BYTES,
                "RDMA port active MTU in bytes from the latest self probe",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_RDMA_DEVICE,
                PROM_LABEL_RDMA_PORT,
                PROM_LABEL_RDMA_NETDEV,
                PROM_LABEL_RDMA_PCI_BDF,
            ],
        )
        .expect("rdma port active mtu gauge");

        let rdma_port_gid_count_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PORT_GID_COUNT,
                "RDMA port gid table size from the latest self probe",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_RDMA_DEVICE,
                PROM_LABEL_RDMA_PORT,
                PROM_LABEL_RDMA_NETDEV,
                PROM_LABEL_RDMA_PCI_BDF,
            ],
        )
        .expect("rdma port gid count gauge");

        let rdma_port_numa_node_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_PORT_NUMA_NODE,
                "NUMA node for the RDMA port device from the latest self probe",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_RDMA_DEVICE,
                PROM_LABEL_RDMA_PORT,
                PROM_LABEL_RDMA_NETDEV,
                PROM_LABEL_RDMA_PCI_BDF,
            ],
        )
        .expect("rdma port numa node gauge");

        let rdma_transfer_engine_state_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_TRANSFER_ENGINE_STATE,
                "One-hot RDMA transfer engine state from the latest self probe cycle",
            ),
            &[
                PROM_LABEL_NODE,
                PROM_LABEL_ROLE,
                PROM_LABEL_RDMA_TRANSFER_STATE,
            ],
        )
        .expect("rdma transfer engine state gauge");

        let rdma_transfer_engine_start_failures_gauge = GaugeVec::new(
            Opts::new(
                PROM_METRIC_RDMA_TRANSFER_ENGINE_START_FAILURES,
                "Consecutive RDMA transfer engine start failures",
            ),
            &[PROM_LABEL_NODE, PROM_LABEL_ROLE],
        )
        .expect("rdma transfer engine start failures gauge");

        register_collector(&registry, Box::new(operation_stat_gauge.clone()));
        register_collector(&registry, Box::new(client_network_bytes_counter.clone()));
        register_collector(&registry, Box::new(kv_peer_network_bytes_counter.clone()));
        register_collector(&registry, Box::new(tcp_thread_latency_stat_gauge.clone()));
        register_collector(
            &registry,
            Box::new(tcp_thread_latency_sample_count_gauge.clone()),
        );
        register_collector(
            &registry,
            Box::new(tcp_thread_transport_bytes_counter.clone()),
        );
        register_collector(
            &registry,
            Box::new(tcp_thread_transport_messages_counter.clone()),
        );
        register_collector(
            &registry,
            Box::new(p2p_receive_transport_bytes_counter.clone()),
        );
        register_collector(
            &registry,
            Box::new(p2p_receive_transport_messages_counter.clone()),
        );
        register_collector(
            &registry,
            Box::new(p2p_rpc_completion_latency_stat_gauge.clone()),
        );
        register_collector(
            &registry,
            Box::new(p2p_rpc_completion_latency_sample_count_gauge.clone()),
        );
        register_collector(
            &registry,
            Box::new(p2p_rpc_completion_bytes_counter.clone()),
        );
        register_collector(
            &registry,
            Box::new(p2p_rpc_completion_messages_counter.clone()),
        );
        register_collector(&registry, Box::new(node_cpu_usage_gauge.clone()));
        register_collector(&registry, Box::new(node_cpu_logical_cores_gauge.clone()));
        register_collector(&registry, Box::new(node_memory_usage_gauge.clone()));
        register_collector(&registry, Box::new(node_memory_total_gauge.clone()));
        register_collector(&registry, Box::new(container_memory_usage_gauge.clone()));
        register_collector(&registry, Box::new(container_memory_limit_gauge.clone()));
        register_collector(&registry, Box::new(process_resident_memory_gauge.clone()));
        register_collector(&registry, Box::new(process_cpu_usage_gauge.clone()));
        register_collector(&registry, Box::new(tokio_num_workers_gauge.clone()));
        register_collector(&registry, Box::new(tokio_alive_tasks_gauge.clone()));
        register_collector(&registry, Box::new(tokio_global_queue_depth_gauge.clone()));
        register_collector(&registry, Box::new(tokio_busy_percent_gauge.clone()));
        register_collector(
            &registry,
            Box::new(tokio_max_worker_busy_percent_gauge.clone()),
        );
        register_collector(&registry, Box::new(tokio_park_unpark_rate_gauge.clone()));
        register_collector(&registry, Box::new(fs_mount_fs_used_bytes_gauge.clone()));
        register_collector(&registry, Box::new(fs_mount_fs_total_bytes_gauge.clone()));
        register_collector(&registry, Box::new(shm_file_size_bytes_gauge.clone()));
        register_collector(&registry, Box::new(shm_file_allocated_bytes_gauge.clone()));
        register_collector(&registry, Box::new(fs_io_ops_counter.clone()));
        register_collector(&registry, Box::new(exporter_heartbeat_gauge.clone()));
        register_collector(&registry, Box::new(node_uptime_counter.clone()));
        register_collector(&registry, Box::new(segment_capacity_gauge.clone()));
        register_collector(&registry, Box::new(segment_used_gauge.clone()));
        register_collector(
            &registry,
            Box::new(node_network_transmit_bytes_counter.clone()),
        );
        register_collector(
            &registry,
            Box::new(node_network_receive_bytes_counter.clone()),
        );
        register_collector(&registry, Box::new(rdma_probe_port_count_gauge.clone()));
        register_collector(
            &registry,
            Box::new(rdma_probe_usable_port_count_gauge.clone()),
        );
        register_collector(&registry, Box::new(rdma_probe_error_gauge.clone()));
        register_collector(&registry, Box::new(rdma_port_usable_gauge.clone()));
        register_collector(&registry, Box::new(rdma_port_speed_gbps_gauge.clone()));
        register_collector(
            &registry,
            Box::new(rdma_port_active_mtu_bytes_gauge.clone()),
        );
        register_collector(&registry, Box::new(rdma_port_gid_count_gauge.clone()));
        register_collector(&registry, Box::new(rdma_port_numa_node_gauge.clone()));
        register_collector(
            &registry,
            Box::new(rdma_transfer_engine_state_gauge.clone()),
        );
        register_collector(
            &registry,
            Box::new(rdma_transfer_engine_start_failures_gauge.clone()),
        );

        let owned = Self {
            rx,
            prom,
            tcp_thread_transport_accumulator,
            p2p_receive_transport_accumulator,
            p2p_rpc_completion_accumulator,
            registry,
            operation_stat_gauge,
            client_network_bytes_counter,
            kv_peer_network_bytes_counter,
            tcp_thread_latency_stat_gauge,
            tcp_thread_latency_sample_count_gauge,
            tcp_thread_transport_bytes_counter,
            tcp_thread_transport_messages_counter,
            p2p_receive_transport_bytes_counter,
            p2p_receive_transport_messages_counter,
            p2p_rpc_completion_latency_stat_gauge,
            p2p_rpc_completion_latency_sample_count_gauge,
            p2p_rpc_completion_bytes_counter,
            p2p_rpc_completion_messages_counter,
            node_cpu_usage_gauge,
            node_cpu_logical_cores_gauge,
            node_memory_usage_gauge,
            node_memory_total_gauge,
            container_memory_usage_gauge,
            container_memory_limit_gauge,
            process_resident_memory_gauge,
            process_cpu_usage_gauge,
            tokio_num_workers_gauge,
            tokio_alive_tasks_gauge,
            tokio_global_queue_depth_gauge,
            tokio_busy_percent_gauge,
            tokio_max_worker_busy_percent_gauge,
            tokio_park_unpark_rate_gauge,
            fs_mount_fs_used_bytes_gauge,
            fs_mount_fs_total_bytes_gauge,
            shm_file_size_bytes_gauge,
            shm_file_allocated_bytes_gauge,
            fs_io_ops_counter,
            exporter_heartbeat_gauge,
            node_uptime_counter,
            segment_capacity_gauge,
            segment_used_gauge,
            node_network_transmit_bytes_counter,
            node_network_receive_bytes_counter,
            rdma_probe_port_count_gauge,
            rdma_probe_usable_port_count_gauge,
            rdma_probe_error_gauge,
            rdma_port_usable_gauge,
            rdma_port_speed_gbps_gauge,
            rdma_port_active_mtu_bytes_gauge,
            rdma_port_gid_count_gauge,
            rdma_port_numa_node_gauge,
            rdma_transfer_engine_state_gauge,
            rdma_transfer_engine_start_failures_gauge,
            last_logged_rdma_snapshot_summary: Mutex::new(None),
            cpu_sample: Mutex::new(None),
            process_cpu_sample: Mutex::new(None),
            tokio_runtime_sample: Mutex::new(None),
            net_bytes_last_sample: Mutex::new(HashMap::new()),
            last_uptime_observed: AtomicU64::new(current_timestamp_seconds() as u64),
            node_id,
            node_role,
            pending_kv_op_metrics: Vec::new(),
            pending_op_end_pulses: Vec::new(),
            pending_tcp_thread_latency_samples: Vec::new(),
            pending_p2p_rpc_completion_latency_samples: Vec::new(),
            received_kv_op_metric_count: 0,
            received_op_end_pulse_count: 0,
            received_tcp_thread_latency_sample_count: 0,
            received_p2p_rpc_completion_latency_sample_count: 0,
            flush_count: 0,
            enable_system_metrics,
        };

        (handle, owned)
    }

    fn replace_self_rdma_snapshot(&self, snapshot: ObserveRdmaSnapshot) {
        let node = self.node_id.as_str();
        let role = self.node_role.as_str();
        let snapshot_summary = render_rdma_snapshot_summary(&snapshot);

        let usable_port_count = snapshot.ports.iter().filter(|port| port.usable).count();
        self.rdma_probe_port_count_gauge
            .with_label_values(&[node, role])
            .set(snapshot.ports.len() as f64);
        self.rdma_probe_usable_port_count_gauge
            .with_label_values(&[node, role])
            .set(usable_port_count as f64);
        self.rdma_probe_error_gauge
            .with_label_values(&[node, role])
            .set(if snapshot.probe_error { 1.0 } else { 0.0 });
        self.rdma_transfer_engine_start_failures_gauge
            .with_label_values(&[node, role])
            .set(snapshot.transfer_engine_consecutive_start_failures as f64);

        self.rdma_port_usable_gauge.reset();
        self.rdma_port_speed_gbps_gauge.reset();
        self.rdma_port_active_mtu_bytes_gauge.reset();
        self.rdma_port_gid_count_gauge.reset();
        self.rdma_port_numa_node_gauge.reset();
        self.rdma_transfer_engine_state_gauge.reset();

        self.rdma_transfer_engine_state_gauge
            .with_label_values(&[
                node,
                role,
                if snapshot.transfer_engine_state.trim().is_empty() {
                    "unknown"
                } else {
                    snapshot.transfer_engine_state.as_str()
                },
            ])
            .set(1.0);

        for port in snapshot.ports {
            let port_label = port.port.to_string();
            let netdev_label = port.netdev.as_deref().unwrap_or("");
            let pci_bdf_label = port.pci_bdf.as_deref().unwrap_or("");
            let labels = [
                node,
                role,
                port.device.as_str(),
                port_label.as_str(),
                netdev_label,
                pci_bdf_label,
            ];

            self.rdma_port_usable_gauge
                .with_label_values(&labels)
                .set(if port.usable { 1.0 } else { 0.0 });
            self.rdma_port_active_mtu_bytes_gauge
                .with_label_values(&labels)
                .set(port.active_mtu_bytes as f64);
            self.rdma_port_gid_count_gauge
                .with_label_values(&labels)
                .set(port.gid_count as f64);
            if let Some(speed_gbps) = port.speed_gbps {
                self.rdma_port_speed_gbps_gauge
                    .with_label_values(&labels)
                    .set(speed_gbps as f64);
            }
            if let Some(numa_node) = port.numa_node {
                self.rdma_port_numa_node_gauge
                    .with_label_values(&labels)
                    .set(numa_node as f64);
            }
        }

        let mut guard = self
            .last_logged_rdma_snapshot_summary
            .lock()
            .expect("rdma snapshot summary mutex poisoned");
        let should_log = guard.as_ref() != Some(&snapshot_summary);
        if should_log {
            info!(
                "rdma observe snapshot node={} role={} {}",
                self.node_id, self.node_role, snapshot_summary
            );
            *guard = Some(snapshot_summary);
        }
    }

    fn apply_msg(&mut self, msg: ObserveOp) {
        match msg {
            ObserveOp::SubmitKvOpMetric { metric } => {
                self.received_kv_op_metric_count =
                    self.received_kv_op_metric_count.saturating_add(1);
                self.pending_kv_op_metrics.push(metric);
                let seq = self.received_kv_op_metric_count;
                if should_log_debug_seq(seq) {
                    match self.pending_kv_op_metrics.last() {
                        Some(KvOpMetric::Put(p)) => {
                            debug!(
                                "kv metrics actor queued kv op metric seq={} pending_kv_op_metrics={} kind=put put_id={} key={} whole_us={} transfer_us={} t1_us={} t4_us={}",
                                seq,
                                self.pending_kv_op_metrics.len(),
                                p.put_id,
                                p.key,
                                p.whole_put_us,
                                p.transfer_us,
                                p.t1_us,
                                p.t4_us
                            );
                        }
                        Some(KvOpMetric::Get(g)) => {
                            debug!(
                                "kv metrics actor queued kv op metric seq={} pending_kv_op_metrics={} kind=get get_id={} key={} whole_us={} transfer_us={} t1_us={} t4_us={}",
                                seq,
                                self.pending_kv_op_metrics.len(),
                                g.get_id,
                                g.key,
                                g.whole_get_us,
                                g.transfer_us,
                                g.t1_us,
                                g.t4_us
                            );
                        }
                        None => {}
                    }
                }
            }
            ObserveOp::EmitOpEndBytesPulse { pulse } => {
                self.received_op_end_pulse_count =
                    self.received_op_end_pulse_count.saturating_add(1);
                self.pending_op_end_pulses.push(pulse);
                let seq = self.received_op_end_pulse_count;
                if should_log_debug_seq(seq) {
                    if let Some(p) = self.pending_op_end_pulses.last() {
                        debug!(
                            "kv metrics actor queued op end pulse seq={} pending_op_end_pulses={} op={} status={} key={} bytes={} timestamp_ms={}",
                            seq,
                            self.pending_op_end_pulses.len(),
                            p.op,
                            p.status,
                            p.key,
                            p.bytes,
                            p.timestamp_ms
                        );
                    }
                }
            }
            ObserveOp::RecordClientNetworkBytes { direction, bytes } => {
                if bytes == 0 {
                    return;
                }
                self.client_network_bytes_counter
                    .with_label_values(&[
                        self.node_id.as_str(),
                        self.node_role.as_str(),
                        direction.as_label(),
                    ])
                    .inc_by(bytes as f64);
            }
            ObserveOp::RecordFsIoOps { op, ops } => {
                if ops == 0 {
                    return;
                }
                self.fs_io_ops_counter
                    .with_label_values(&[
                        self.node_id.as_str(),
                        self.node_role.as_str(),
                        op.as_label(),
                    ])
                    .inc_by(ops as f64);
            }
            ObserveOp::RecordTcpThreadLatencySample { sample } => {
                self.received_tcp_thread_latency_sample_count = self
                    .received_tcp_thread_latency_sample_count
                    .saturating_add(1);
                self.pending_tcp_thread_latency_samples.push(sample);
            }
            ObserveOp::RecordP2pRpcCompletionLatencySample { sample } => {
                self.received_p2p_rpc_completion_latency_sample_count = self
                    .received_p2p_rpc_completion_latency_sample_count
                    .saturating_add(1);
                self.pending_p2p_rpc_completion_latency_samples.push(sample);
            }
            ObserveOp::FlushTcpThreadTransportAccumulator => {
                self.flush_tcp_thread_transport_accumulator();
            }
            ObserveOp::FlushP2pReceiveTransportAccumulator => {
                self.flush_p2p_receive_transport_accumulator();
            }
            ObserveOp::FlushP2pRpcCompletionAccumulator => {
                self.flush_p2p_rpc_completion_accumulator();
            }
            ObserveOp::RecordPeerNetworkBytes(event) => {
                if event.bytes == 0 {
                    return;
                }
                let (node_label, role_label) = match event.node_override.as_ref() {
                    Some(override_labels) => {
                        (override_labels.node.as_str(), override_labels.role.as_str())
                    }
                    None => (self.node_id.as_str(), self.node_role.as_str()),
                };
                self.kv_peer_network_bytes_counter
                    .with_label_values(&[
                        node_label,
                        role_label,
                        event.component.as_label(),
                        event.peer.as_str(),
                        event.direction.as_label(),
                    ])
                    .inc_by(event.bytes as f64);
            }
            ObserveOp::SetSegmentCapacityBytes {
                node,
                device,
                bytes,
            } => {
                self.segment_capacity_gauge
                    .with_label_values(&[&node, &device])
                    .set(bytes as f64);
            }
            ObserveOp::SetSegmentUsedBytes {
                node,
                device,
                bytes,
            } => {
                self.segment_used_gauge
                    .with_label_values(&[&node, &device])
                    .set(bytes as f64);
            }
            ObserveOp::SetFsMountFsBytes {
                mount_kind,
                target_dir_abs,
                mountpoint_dir_abs,
                used_bytes,
                total_bytes,
            } => {
                self.fs_mount_fs_used_bytes_gauge
                    .with_label_values(&[
                        self.node_id.as_str(),
                        self.node_role.as_str(),
                        mount_kind.as_str(),
                        target_dir_abs.as_str(),
                        mountpoint_dir_abs.as_str(),
                    ])
                    .set(used_bytes as f64);
                self.fs_mount_fs_total_bytes_gauge
                    .with_label_values(&[
                        self.node_id.as_str(),
                        self.node_role.as_str(),
                        mount_kind.as_str(),
                        target_dir_abs.as_str(),
                        mountpoint_dir_abs.as_str(),
                    ])
                    .set(total_bytes as f64);
            }
            ObserveOp::SetShmFileBytes {
                shm_dir_abs,
                file_path_abs,
                logical_size_bytes,
                allocated_bytes,
            } => {
                self.shm_file_size_bytes_gauge
                    .with_label_values(&[
                        self.node_id.as_str(),
                        self.node_role.as_str(),
                        shm_dir_abs.as_str(),
                        file_path_abs.as_str(),
                    ])
                    .set(logical_size_bytes as f64);
                self.shm_file_allocated_bytes_gauge
                    .with_label_values(&[
                        self.node_id.as_str(),
                        self.node_role.as_str(),
                        shm_dir_abs.as_str(),
                        file_path_abs.as_str(),
                    ])
                    .set(allocated_bytes as f64);
            }
            ObserveOp::ReplaceSelfRdmaSnapshot { snapshot } => {
                self.replace_self_rdma_snapshot(snapshot);
            }
        }
    }

    fn tick_sample_system_metrics(&self) {
        if !self.enable_system_metrics {
            return;
        }

        let node = &self.node_id;
        let role = &self.node_role;

        // Heartbeat
        self.exporter_heartbeat_gauge
            .with_label_values(&[node, role])
            .set(current_timestamp_seconds());

        // Uptime counter (incremental)
        let now = current_timestamp_seconds();
        let last = self.last_uptime_observed.load(Ordering::SeqCst);
        if now as u64 >= last {
            let delta = now as u64 - last;
            if delta > 0 {
                self.node_uptime_counter
                    .with_label_values(&[node, role])
                    .inc_by(delta as f64);
            }
            self.last_uptime_observed
                .store(now as u64, Ordering::SeqCst);
        }

        if let Err(err) = sample_cpu_usage_percent(self, node, role) {
            warn!("failed to sample cpu usage: {err}");
        }
        if let Err(err) = sample_cpu_logical_cores(self, node, role) {
            warn!("failed to sample cpu logical cores: {err}");
        }
        if let Err(err) = sample_host_memory_bytes(self, node, role) {
            warn!("failed to sample host memory: {err}");
        }
        if let Err(err) = sample_container_memory_bytes(self, node, role) {
            warn!("failed to sample container memory: {err}");
        }
        if let Err(err) = sample_process_cpu_usage_percent(self, node, role) {
            warn!("failed to sample process cpu usage: {err}");
        }
        if let Err(err) = sample_process_rss_bytes(self, node, role) {
            warn!("failed to sample process rss: {err}");
        }
        if let Err(err) = sample_tokio_runtime_metrics(self, node, role) {
            warn!("failed to sample tokio runtime metrics: {err}");
        }
        if let Err(err) = sample_network_bytes_by_interface(self, node) {
            debug!("/proc/net/dev not available: {err}");
        }
    }

    fn tick_compute_and_set_operation_stats(
        &self,
        metrics: &[KvOpMetric],
    ) -> Vec<OperationWindowSummary> {
        // Compute p95/p99/mean/min/max per tick.
        let mut buckets: HashMap<&'static str, Vec<i64>> = HashMap::new();
        let mut summaries = Vec::new();

        for m in metrics {
            match m {
                KvOpMetric::Put(p) => {
                    buckets.entry("put_whole").or_default().push(p.whole_put_us);
                    buckets.entry("put_start").or_default().push(p.start_us);
                    buckets
                        .entry("put_transfer")
                        .or_default()
                        .push(p.transfer_us);
                    buckets.entry("put_end").or_default().push(p.end_us);
                    if p.rpc_of_put_start_us > 0 {
                        buckets
                            .entry("put_rpc")
                            .or_default()
                            .push(p.rpc_of_put_start_us);
                    }
                    if p.start_handle_us > 0 {
                        buckets
                            .entry("put_start_handle")
                            .or_default()
                            .push(p.start_handle_us);
                    }
                    if p.end_handle_us > 0 {
                        buckets
                            .entry("put_end_handle")
                            .or_default()
                            .push(p.end_handle_us);
                    }
                    if p.transfer_submit_blocking_us > 0 {
                        buckets
                            .entry("put_transfer_submit_blocking")
                            .or_default()
                            .push(p.transfer_submit_blocking_us);
                    }
                    if p.transfer_create_xfer_req_us > 0 {
                        buckets
                            .entry("put_transfer_create_xfer_req")
                            .or_default()
                            .push(p.transfer_create_xfer_req_us);
                    }
                    if p.transfer_post_xfer_req_us > 0 {
                        buckets
                            .entry("put_transfer_post_xfer_req")
                            .or_default()
                            .push(p.transfer_post_xfer_req_us);
                    }
                    if p.transfer_poll_wait_us > 0 {
                        buckets
                            .entry("put_transfer_poll_wait")
                            .or_default()
                            .push(p.transfer_poll_wait_us);
                    }
                    if p.transfer_poll_iters > 0 {
                        buckets
                            .entry("put_transfer_poll_iters")
                            .or_default()
                            .push(p.transfer_poll_iters);
                    }
                }
                KvOpMetric::Get(g) => {
                    buckets.entry("get_whole").or_default().push(g.whole_get_us);
                    buckets.entry("get_start").or_default().push(g.start_us);
                    buckets
                        .entry("get_transfer")
                        .or_default()
                        .push(g.transfer_us);
                    buckets.entry("get_end").or_default().push(g.end_us);
                    if g.start_handle_us > 0 {
                        buckets
                            .entry("get_start_handle")
                            .or_default()
                            .push(g.start_handle_us);
                    }
                    if g.end_handle_us > 0 {
                        buckets
                            .entry("get_end_handle")
                            .or_default()
                            .push(g.end_handle_us);
                    }
                }
            }
        }

        for (metric, mut data) in buckets {
            if data.is_empty() {
                continue;
            }
            data.sort_unstable();
            let len = data.len();
            let sum: i64 = data.iter().sum();
            let mean = sum as f64 / len as f64;
            let idx50 = ((len * 50 + 99) / 100).saturating_sub(1).min(len - 1);
            let idx99 = ((len * 99 + 99) / 100).saturating_sub(1).min(len - 1);
            let idx95 = ((len * 95 + 99) / 100).saturating_sub(1).min(len - 1);
            let p99 = data[idx99] as f64;
            let p95 = data[idx95] as f64;
            let min = data[0] as f64;
            let max = data[len - 1] as f64;

            let client = &self.node_id;
            self.operation_stat_gauge
                .with_label_values(&[client.as_str(), metric, "mean"])
                .set(mean);
            self.operation_stat_gauge
                .with_label_values(&[client.as_str(), metric, "p99"])
                .set(p99);
            self.operation_stat_gauge
                .with_label_values(&[client.as_str(), metric, "p95"])
                .set(p95);
            self.operation_stat_gauge
                .with_label_values(&[client.as_str(), metric, "min"])
                .set(min);
            self.operation_stat_gauge
                .with_label_values(&[client.as_str(), metric, "max"])
                .set(max);

            summaries.push(OperationWindowSummary {
                metric,
                sample_count: len,
                mean,
                p50: data[idx50],
                p95: data[idx95],
                p99: data[idx99],
                min: data[0],
                max: data[len - 1],
            });
        }

        summaries.sort_unstable_by(|lhs, rhs| lhs.metric.cmp(rhs.metric));
        summaries
    }

    fn tick_compute_and_set_tcp_thread_latency_stats(
        &self,
        samples: &[ObserveTcpThreadLatencySample],
    ) -> Vec<TcpThreadLatencyWindowSummary> {
        let mut buckets: HashMap<(&'static str, &'static str), Vec<i64>> = HashMap::new();
        let mut summaries = Vec::new();

        for sample in samples {
            if sample.duration_us <= 0 {
                continue;
            }
            buckets
                .entry((sample.metric, sample.lane))
                .or_default()
                .push(sample.duration_us);
        }

        let node = self.node_id.as_str();
        let role = self.node_role.as_str();
        for ((metric, lane), mut data) in buckets {
            if data.is_empty() {
                continue;
            }
            data.sort_unstable();
            let len = data.len();
            let sum: i64 = data.iter().sum();
            let mean = sum as f64 / len as f64;
            let idx50 = ((len * 50 + 99) / 100).saturating_sub(1).min(len - 1);
            let idx95 = ((len * 95 + 99) / 100).saturating_sub(1).min(len - 1);
            let idx99 = ((len * 99 + 99) / 100).saturating_sub(1).min(len - 1);
            let labels = [node, role, metric, lane];

            self.tcp_thread_latency_sample_count_gauge
                .with_label_values(&labels)
                .set(len as f64);
            self.tcp_thread_latency_stat_gauge
                .with_label_values(&[node, role, metric, lane, "mean"])
                .set(mean);
            self.tcp_thread_latency_stat_gauge
                .with_label_values(&[node, role, metric, lane, "p50"])
                .set(data[idx50] as f64);
            self.tcp_thread_latency_stat_gauge
                .with_label_values(&[node, role, metric, lane, "p95"])
                .set(data[idx95] as f64);
            self.tcp_thread_latency_stat_gauge
                .with_label_values(&[node, role, metric, lane, "p99"])
                .set(data[idx99] as f64);
            self.tcp_thread_latency_stat_gauge
                .with_label_values(&[node, role, metric, lane, "min"])
                .set(data[0] as f64);
            self.tcp_thread_latency_stat_gauge
                .with_label_values(&[node, role, metric, lane, "max"])
                .set(data[len - 1] as f64);

            summaries.push(TcpThreadLatencyWindowSummary {
                metric,
                lane,
                sample_count: len,
                mean_us: mean,
                p50_us: data[idx50],
                p95_us: data[idx95],
                p99_us: data[idx99],
                min_us: data[0],
                max_us: data[len - 1],
            });
        }

        summaries.sort_unstable_by(|lhs, rhs| {
            lhs.metric
                .cmp(rhs.metric)
                .then_with(|| lhs.lane.cmp(rhs.lane))
        });
        summaries
    }

    fn tick_compute_and_set_p2p_rpc_completion_latency_stats(
        &self,
        samples: &[ObserveP2pRpcCompletionLatencySample],
    ) -> Vec<OperationWindowSummary> {
        let mut buckets: HashMap<&'static str, Vec<i64>> = HashMap::new();
        let mut summaries = Vec::new();

        for sample in samples {
            if sample.duration_us <= 0 {
                continue;
            }
            buckets
                .entry(sample.metric)
                .or_default()
                .push(sample.duration_us);
        }

        let node = self.node_id.as_str();
        let role = self.node_role.as_str();
        for (metric, mut data) in buckets {
            if data.is_empty() {
                continue;
            }
            data.sort_unstable();
            let len = data.len();
            let sum: i64 = data.iter().sum();
            let mean = sum as f64 / len as f64;
            let idx50 = ((len * 50 + 99) / 100).saturating_sub(1).min(len - 1);
            let idx95 = ((len * 95 + 99) / 100).saturating_sub(1).min(len - 1);
            let idx99 = ((len * 99 + 99) / 100).saturating_sub(1).min(len - 1);

            self.p2p_rpc_completion_latency_sample_count_gauge
                .with_label_values(&[node, role, metric])
                .set(len as f64);
            self.p2p_rpc_completion_latency_stat_gauge
                .with_label_values(&[node, role, metric, "mean"])
                .set(mean);
            self.p2p_rpc_completion_latency_stat_gauge
                .with_label_values(&[node, role, metric, "p50"])
                .set(data[idx50] as f64);
            self.p2p_rpc_completion_latency_stat_gauge
                .with_label_values(&[node, role, metric, "p95"])
                .set(data[idx95] as f64);
            self.p2p_rpc_completion_latency_stat_gauge
                .with_label_values(&[node, role, metric, "p99"])
                .set(data[idx99] as f64);
            self.p2p_rpc_completion_latency_stat_gauge
                .with_label_values(&[node, role, metric, "min"])
                .set(data[0] as f64);
            self.p2p_rpc_completion_latency_stat_gauge
                .with_label_values(&[node, role, metric, "max"])
                .set(data[len - 1] as f64);

            summaries.push(OperationWindowSummary {
                metric,
                sample_count: len,
                mean,
                p50: data[idx50],
                p95: data[idx95],
                p99: data[idx99],
                min: data[0],
                max: data[len - 1],
            });
        }

        summaries.sort_unstable_by(|lhs, rhs| lhs.metric.cmp(rhs.metric));
        summaries
    }

    fn tick_build_extra_timeseries(&mut self) -> Vec<RwTimeSeries> {
        let mut extra = Vec::new();
        let node_id = self.node_id.clone();
        let node_role = self.node_role.clone();

        for m in self.pending_kv_op_metrics.drain(..) {
            match m {
                KvOpMetric::Put(p) => {
                    extend_timeline_from_t1_t4(
                        &mut extra, &node_id, &node_role, "put", &p.key, &p.put_id, p.t1_us,
                        p.t2_us, p.t3_us, p.t4_us,
                    );
                    extend_put_transfer_breakdown_timeline(&mut extra, &node_id, &node_role, &p);

                    // Best-effort: approximate put_rpc as [t1, t1 + rpc_latency].
                    if p.rpc_of_put_start_us > 0 {
                        let t1_ms = p.t1_us / 1000;
                        let t2_ms = (p.t1_us + p.rpc_of_put_start_us) / 1000;
                        extend_timeline_event(
                            &mut extra, &node_id, &node_role, "put_rpc", "put", "begin", &p.key,
                            &p.put_id, t1_ms,
                        );
                        extend_timeline_event(
                            &mut extra, &node_id, &node_role, "put_rpc", "put", "end", &p.key,
                            &p.put_id, t2_ms,
                        );
                    }
                }
                KvOpMetric::Get(g) => {
                    extend_timeline_from_t1_t4(
                        &mut extra, &node_id, &node_role, "get", &g.key, &g.get_id, g.t1_us,
                        g.t2_us, g.t3_us, g.t4_us,
                    );
                }
            }
        }

        for p in self.pending_op_end_pulses.drain(..) {
            extend_op_end_pulses(&mut extra, &node_id, &node_role, &p);
        }

        extra
    }

    fn tick_submit_to_remote_write(&self, extra_timeseries: Vec<RwTimeSeries>) {
        let metric_families = self.registry.gather();
        self.prom
            .try_submit_collected(metric_families, extra_timeseries);
    }

    fn flush_tcp_thread_transport_accumulator(&self) -> usize {
        let mut drained_bucket_count = 0usize;
        loop {
            self.tcp_thread_transport_accumulator
                .dirty
                .store(false, Ordering::Release);
            let drained = self.tcp_thread_transport_accumulator.drain_once();
            if drained.is_empty() {
                if !self
                    .tcp_thread_transport_accumulator
                    .dirty
                    .swap(false, Ordering::AcqRel)
                {
                    break;
                }
                continue;
            }
            drained_bucket_count += drained.len();
            for sample in drained {
                if sample.bytes > 0 {
                    self.tcp_thread_transport_bytes_counter
                        .with_label_values(&[
                            self.node_id.as_str(),
                            self.node_role.as_str(),
                            sample.metric,
                            sample.lane,
                        ])
                        .inc_by(sample.bytes as f64);
                }
                if sample.messages > 0 {
                    self.tcp_thread_transport_messages_counter
                        .with_label_values(&[
                            self.node_id.as_str(),
                            self.node_role.as_str(),
                            sample.metric,
                            sample.lane,
                        ])
                        .inc_by(sample.messages as f64);
                }
            }
            if !self
                .tcp_thread_transport_accumulator
                .dirty
                .swap(false, Ordering::AcqRel)
            {
                break;
            }
        }
        drained_bucket_count
    }

    fn flush_p2p_receive_transport_accumulator(&self) -> usize {
        let mut drained_bucket_count = 0usize;
        loop {
            self.p2p_receive_transport_accumulator
                .dirty
                .store(false, Ordering::Release);
            let drained = self.p2p_receive_transport_accumulator.drain_once();
            if drained.is_empty() {
                if !self
                    .p2p_receive_transport_accumulator
                    .dirty
                    .swap(false, Ordering::AcqRel)
                {
                    break;
                }
                continue;
            }
            drained_bucket_count += drained.len();
            for sample in drained {
                if sample.bytes > 0 {
                    self.p2p_receive_transport_bytes_counter
                        .with_label_values(&[
                            self.node_id.as_str(),
                            self.node_role.as_str(),
                            sample.component.as_label(),
                            sample.metric.as_label(),
                        ])
                        .inc_by(sample.bytes as f64);
                }
                if sample.messages > 0 {
                    self.p2p_receive_transport_messages_counter
                        .with_label_values(&[
                            self.node_id.as_str(),
                            self.node_role.as_str(),
                            sample.component.as_label(),
                            sample.metric.as_label(),
                        ])
                        .inc_by(sample.messages as f64);
                }
            }
            if !self
                .p2p_receive_transport_accumulator
                .dirty
                .swap(false, Ordering::AcqRel)
            {
                break;
            }
        }
        drained_bucket_count
    }

    fn flush_p2p_rpc_completion_accumulator(&self) -> Vec<P2pRpcCompletionCounterWindowSummary> {
        let mut summaries: HashMap<&'static str, (u64, u64)> = HashMap::new();
        loop {
            self.p2p_rpc_completion_accumulator
                .dirty
                .store(false, Ordering::Release);
            let drained = self.p2p_rpc_completion_accumulator.drain_once();
            if drained.is_empty() {
                if !self
                    .p2p_rpc_completion_accumulator
                    .dirty
                    .swap(false, Ordering::AcqRel)
                {
                    break;
                }
                continue;
            }
            for (metric, bytes, messages) in drained {
                let entry = summaries.entry(metric.as_label()).or_insert((0, 0));
                entry.0 = entry.0.saturating_add(bytes);
                entry.1 = entry.1.saturating_add(messages);
                if bytes > 0 {
                    self.p2p_rpc_completion_bytes_counter
                        .with_label_values(&[
                            self.node_id.as_str(),
                            self.node_role.as_str(),
                            metric.as_label(),
                        ])
                        .inc_by(bytes as f64);
                }
                if messages > 0 {
                    self.p2p_rpc_completion_messages_counter
                        .with_label_values(&[
                            self.node_id.as_str(),
                            self.node_role.as_str(),
                            metric.as_label(),
                        ])
                        .inc_by(messages as f64);
                }
            }
            if !self
                .p2p_rpc_completion_accumulator
                .dirty
                .swap(false, Ordering::AcqRel)
            {
                break;
            }
        }
        let mut out = summaries
            .into_iter()
            .map(
                |(metric, (bytes, messages))| P2pRpcCompletionCounterWindowSummary {
                    metric,
                    bytes,
                    messages,
                },
            )
            .collect::<Vec<_>>();
        out.sort_unstable_by(|lhs, rhs| lhs.metric.cmp(rhs.metric));
        out
    }

    pub async fn run<F>(mut self, flush_interval: Duration, shutdown: F)
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let mut tick = tokio::time::interval(flush_interval);
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let pending_kv_op_metrics = self.pending_kv_op_metrics.len();
                    let pending_op_end_pulses = self.pending_op_end_pulses.len();
                    let pending_tcp_thread_latency_samples =
                        self.pending_tcp_thread_latency_samples.len();
                    let pending_p2p_rpc_completion_latency_samples =
                        self.pending_p2p_rpc_completion_latency_samples.len();
                    let pending_tcp_thread_transport_buckets =
                        self.flush_tcp_thread_transport_accumulator();
                    let pending_p2p_receive_transport_buckets =
                        self.flush_p2p_receive_transport_accumulator();
                    let p2p_rpc_completion_counter_summaries =
                        self.flush_p2p_rpc_completion_accumulator();
                    let pending_p2p_rpc_completion_buckets =
                        p2p_rpc_completion_counter_summaries.len();
                    self.shm_file_size_bytes_gauge.reset();
                    self.shm_file_allocated_bytes_gauge.reset();
                    self.tick_sample_system_metrics();
                    let operation_summaries =
                        self.tick_compute_and_set_operation_stats(&self.pending_kv_op_metrics);
                    let tcp_thread_latency_summaries = self.tick_compute_and_set_tcp_thread_latency_stats(
                        &self.pending_tcp_thread_latency_samples,
                    );
                    let p2p_rpc_completion_latency_summaries = self
                        .tick_compute_and_set_p2p_rpc_completion_latency_stats(
                            &self.pending_p2p_rpc_completion_latency_samples,
                        );
                    self.pending_tcp_thread_latency_samples.clear();
                    self.pending_p2p_rpc_completion_latency_samples.clear();
                    let extra = self.tick_build_extra_timeseries();
                    let extra_timeseries = extra.len();
                    self.flush_count = self.flush_count.saturating_add(1);
                    let flush_seq = self.flush_count;
                    if pending_kv_op_metrics > 0 && extra_timeseries == 0 {
                        warn!(
                            "kv metrics actor flush produced no extra timeseries from pending kv metrics flush_seq={} pending_kv_op_metrics={} pending_op_end_pulses={} received_kv_op_metric_count={} received_op_end_pulse_count={} node={} role={}",
                            flush_seq,
                            pending_kv_op_metrics,
                            pending_op_end_pulses,
                            self.received_kv_op_metric_count,
                            self.received_op_end_pulse_count,
                            self.node_id,
                            self.node_role
                        );
                    } else if pending_kv_op_metrics > 0
                        || pending_op_end_pulses > 0
                        || pending_tcp_thread_latency_samples > 0
                        || pending_tcp_thread_transport_buckets > 0
                        || pending_p2p_receive_transport_buckets > 0
                        || pending_p2p_rpc_completion_latency_samples > 0
                        || pending_p2p_rpc_completion_buckets > 0
                        || should_log_debug_seq(flush_seq)
                    {
                        debug!(
                            "kv metrics actor flush flush_seq={} pending_kv_op_metrics={} pending_op_end_pulses={} pending_tcp_thread_latency_samples={} pending_tcp_thread_transport_buckets={} pending_p2p_receive_transport_buckets={} pending_p2p_rpc_completion_latency_samples={} pending_p2p_rpc_completion_buckets={} extra_timeseries={} received_kv_op_metric_count={} received_op_end_pulse_count={} received_tcp_thread_latency_sample_count={} received_p2p_rpc_completion_latency_sample_count={} node={} role={}",
                            flush_seq,
                            pending_kv_op_metrics,
                            pending_op_end_pulses,
                            pending_tcp_thread_latency_samples,
                            pending_tcp_thread_transport_buckets,
                            pending_p2p_receive_transport_buckets,
                            pending_p2p_rpc_completion_latency_samples,
                            pending_p2p_rpc_completion_buckets,
                            extra_timeseries,
                            self.received_kv_op_metric_count,
                            self.received_op_end_pulse_count,
                            self.received_tcp_thread_latency_sample_count,
                            self.received_p2p_rpc_completion_latency_sample_count,
                            self.node_id,
                            self.node_role
                        );
                    }
                    if !tcp_thread_latency_summaries.is_empty() {
                        let summary_text = tcp_thread_latency_summaries
                            .iter()
                            .map(|summary| {
                                format!(
                                    "{}:{} samples={} mean_us={:.1} p50_us={} p95_us={} p99_us={} min_us={} max_us={}",
                                    summary.metric,
                                    summary.lane,
                                    summary.sample_count,
                                    summary.mean_us,
                                    summary.p50_us,
                                    summary.p95_us,
                                    summary.p99_us,
                                    summary.min_us,
                                    summary.max_us,
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("; ");
                        info!(
                            "tcp_thread observe window flush_seq={} node={} role={} {}",
                            flush_seq,
                            self.node_id,
                            self.node_role,
                            summary_text
                        );
                    }
                    if !p2p_rpc_completion_counter_summaries.is_empty()
                        || !p2p_rpc_completion_latency_summaries.is_empty()
                    {
                        let mut summary_sections = Vec::new();
                        if !p2p_rpc_completion_counter_summaries.is_empty() {
                            let counter_text = p2p_rpc_completion_counter_summaries
                                .iter()
                                .map(|summary| {
                                    format!(
                                        "{} messages={} bytes={}",
                                        summary.metric,
                                        summary.messages,
                                        summary.bytes,
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("; ");
                            summary_sections.push(format!("counters {}", counter_text));
                        }
                        if !p2p_rpc_completion_latency_summaries.is_empty() {
                            let latency_text = p2p_rpc_completion_latency_summaries
                                .iter()
                                .map(|summary| {
                                    format!(
                                        "{} samples={} mean_us={:.1} p50_us={} p95_us={} p99_us={} min_us={} max_us={}",
                                        summary.metric,
                                        summary.sample_count,
                                        summary.mean,
                                        summary.p50,
                                        summary.p95,
                                        summary.p99,
                                        summary.min,
                                        summary.max,
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("; ");
                            summary_sections.push(format!("latency {}", latency_text));
                        }
                        info!(
                            "p2p rpc completion observe window flush_seq={} node={} role={} {}",
                            flush_seq,
                            self.node_id,
                            self.node_role,
                            summary_sections.join(" | ")
                        );
                    }
                    let rdma_transfer_summaries = operation_summaries
                        .iter()
                        .filter(|summary| summary.metric.starts_with("put_transfer"))
                        .map(|summary| {
                            format!(
                                "{} samples={} mean={:.1} p50={} p95={} p99={} min={} max={}",
                                summary.metric,
                                summary.sample_count,
                                summary.mean,
                                summary.p50,
                                summary.p95,
                                summary.p99,
                                summary.min,
                                summary.max,
                            )
                        })
                        .collect::<Vec<_>>();
                    if !rdma_transfer_summaries.is_empty() {
                        info!(
                            "rdma transfer observe window flush_seq={} node={} role={} {}",
                            flush_seq,
                            self.node_id,
                            self.node_role,
                            rdma_transfer_summaries.join("; ")
                        );
                    }
                    self.tick_submit_to_remote_write(extra);
                }
                maybe = self.rx.recv() => {
                    let Some(msg) = maybe else {
                        break;
                    };
                    self.apply_msg(msg);
                }
                _ = &mut shutdown => {
                    break;
                }
            }
        }
    }
}

fn current_timestamp_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn should_log_debug_seq(seq: u64) -> bool {
    seq <= 8 || seq.is_power_of_two()
}

fn render_rdma_snapshot_summary(snapshot: &ObserveRdmaSnapshot) -> String {
    let mut ports = snapshot
        .ports
        .iter()
        .map(|port| {
            format!(
                "{}:{} usable={} speed_gbps={} mtu={} gid_count={} numa={} netdev={} pci={}",
                port.device,
                port.port,
                port.usable,
                port.speed_gbps
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                port.active_mtu_bytes,
                port.gid_count,
                port.numa_node
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                port.netdev.as_deref().unwrap_or(""),
                port.pci_bdf.as_deref().unwrap_or(""),
            )
        })
        .collect::<Vec<_>>();
    ports.sort_unstable();
    format!(
        "engine_state={} start_failures={} probe_error={} ports={}",
        snapshot.transfer_engine_state,
        snapshot.transfer_engine_consecutive_start_failures,
        snapshot.probe_error,
        if ports.is_empty() {
            "none".to_string()
        } else {
            ports.join(", ")
        }
    )
}

fn read_cpu_sample() -> anyhow::Result<SystemSample> {
    use anyhow::Context;
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open("/proc/stat").context("open /proc/stat")?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line).context("read /proc/stat")?;
    let mut parts = line.split_whitespace();
    let _ = parts.next();
    let mut values = Vec::new();
    for part in parts {
        values.push(part.parse::<u64>().context("parse /proc/stat cpu fields")?);
    }
    if values.len() < 5 {
        anyhow::bail!("unexpected /proc/stat format");
    }
    let idle = values.get(3).copied().unwrap_or(0);
    let iowait = values.get(4).copied().unwrap_or(0);
    let total: u64 = values.iter().sum();
    Ok(SystemSample {
        total,
        idle: idle.saturating_add(iowait),
    })
}

fn read_cpu_logical_cores() -> anyhow::Result<u64> {
    use anyhow::Context;
    let raw = std::fs::read_to_string("/proc/stat").context("read /proc/stat")?;
    let mut n: u64 = 0;
    for line in raw.lines() {
        let Some(first) = line.split_whitespace().next() else {
            continue;
        };
        let Some(rest) = first.strip_prefix("cpu") else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        if rest.as_bytes().iter().all(|b| b.is_ascii_digit()) {
            n += 1;
        }
    }
    if n == 0 {
        anyhow::bail!("no cpuN lines found in /proc/stat");
    }
    Ok(n)
}

fn sample_cpu_usage_percent(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let sample = read_cpu_sample()?;
    let mut guard = actor.cpu_sample.lock().expect("cpu sample lock poisoned");
    let cpu_usage = guard
        .as_ref()
        .and_then(|prev| {
            let total_delta = sample.total.saturating_sub(prev.total);
            let idle_delta = sample.idle.saturating_sub(prev.idle);
            if total_delta == 0 {
                None
            } else {
                Some(100.0 * (total_delta.saturating_sub(idle_delta)) as f64 / total_delta as f64)
            }
        })
        .unwrap_or(0.0)
        .clamp(0.0, 100.0);

    actor
        .node_cpu_usage_gauge
        .with_label_values(&[node, role])
        .set(cpu_usage);
    *guard = Some(sample);
    Ok(())
}

fn sample_cpu_logical_cores(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let cores = read_cpu_logical_cores()?;
    actor
        .node_cpu_logical_cores_gauge
        .with_label_values(&[node, role])
        .set(cores as f64);
    Ok(())
}

fn sample_host_memory_bytes(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let total = sys.total_memory() as f64;
    let used = sys.used_memory() as f64;
    actor
        .node_memory_usage_gauge
        .with_label_values(&[node, role])
        .set(used);
    actor
        .node_memory_total_gauge
        .with_label_values(&[node, role])
        .set(total);
    Ok(())
}

fn sample_process_rss_bytes(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let mut sys = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().map_err(|e| anyhow::anyhow!("get current pid: {e}"))?;
    sys.refresh_process(pid);
    if let Some(proc_) = sys.process(pid) {
        let rss_bytes = proc_.memory() as f64;
        actor
            .process_resident_memory_gauge
            .with_label_values(&[node, role])
            .set(rss_bytes);
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct CgroupMemorySample {
    usage_bytes: Option<u64>,
    limit_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
struct ProcCgroupRec {
    controllers: Vec<String>,
    path: String,
}

#[derive(Debug, Clone)]
struct MountInfoRec {
    mount_point: PathBuf,
    fs_type: String,
    super_options: Vec<String>,
}

fn sample_container_memory_bytes(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let sample = read_self_container_memory_sample()?;
    if let Some(usage) = sample.usage_bytes {
        actor
            .container_memory_usage_gauge
            .with_label_values(&[node, role])
            .set(usage as f64);
    }
    if let Some(limit) = sample.limit_bytes {
        actor
            .container_memory_limit_gauge
            .with_label_values(&[node, role])
            .set(limit as f64);
    }
    Ok(())
}

fn read_self_container_memory_sample() -> anyhow::Result<CgroupMemorySample> {
    let proc_cgroups = read_proc_self_cgroup()?;
    let mountinfo = read_proc_self_mountinfo()?;

    if let Some(sample) = read_cgroup_v2_memory_sample(&proc_cgroups, &mountinfo)? {
        return Ok(sample);
    }
    if let Some(sample) = read_cgroup_v1_memory_sample(&proc_cgroups, &mountinfo)? {
        return Ok(sample);
    }

    Ok(CgroupMemorySample {
        usage_bytes: None,
        limit_bytes: None,
    })
}

fn read_cgroup_v2_memory_sample(
    proc_cgroups: &[ProcCgroupRec],
    mountinfo: &[MountInfoRec],
) -> anyhow::Result<Option<CgroupMemorySample>> {
    let cgroup = match proc_cgroups.iter().find(|rec| rec.controllers.is_empty()) {
        Some(v) => v,
        None => return Ok(None),
    };
    let mount = match mountinfo.iter().find(|m| m.fs_type == "cgroup2") {
        Some(v) => v,
        None => return Ok(None),
    };
    let cgroup_dir = join_cgroup_mount_path(mount.mount_point.as_path(), &cgroup.path);
    let usage_bytes = read_u64_from_file(cgroup_dir.join("memory.current"))?;
    let limit_bytes = read_cgroup_limit_file(cgroup_dir.join("memory.max"))?;
    Ok(Some(CgroupMemorySample {
        usage_bytes: Some(usage_bytes),
        limit_bytes,
    }))
}

fn read_cgroup_v1_memory_sample(
    proc_cgroups: &[ProcCgroupRec],
    mountinfo: &[MountInfoRec],
) -> anyhow::Result<Option<CgroupMemorySample>> {
    let cgroup = match proc_cgroups.iter().find(|rec| {
        rec.controllers
            .iter()
            .any(|controller| controller == "memory")
    }) {
        Some(v) => v,
        None => return Ok(None),
    };
    let mount = match mountinfo
        .iter()
        .find(|m| m.fs_type == "cgroup" && m.super_options.iter().any(|opt| opt == "memory"))
    {
        Some(v) => v,
        None => return Ok(None),
    };
    let cgroup_dir = join_cgroup_mount_path(mount.mount_point.as_path(), &cgroup.path);
    let usage_bytes = read_u64_from_file(cgroup_dir.join("memory.usage_in_bytes"))?;
    let limit_bytes = read_cgroup_limit_file(cgroup_dir.join("memory.limit_in_bytes"))?;
    Ok(Some(CgroupMemorySample {
        usage_bytes: Some(usage_bytes),
        limit_bytes,
    }))
}

fn read_proc_self_cgroup() -> anyhow::Result<Vec<ProcCgroupRec>> {
    use anyhow::Context;
    let raw = std::fs::read_to_string("/proc/self/cgroup").context("read /proc/self/cgroup")?;
    let mut out: Vec<ProcCgroupRec> = Vec::new();
    for line in raw.lines() {
        let mut parts = line.splitn(3, ':');
        let Some(_hierarchy_id) = parts.next() else {
            continue;
        };
        let Some(controllers_s) = parts.next() else {
            continue;
        };
        let Some(path) = parts.next() else {
            continue;
        };
        let controllers = if controllers_s.is_empty() {
            Vec::new()
        } else {
            controllers_s
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect()
        };
        out.push(ProcCgroupRec {
            controllers,
            path: path.trim().to_string(),
        });
    }
    Ok(out)
}

fn read_proc_self_mountinfo() -> anyhow::Result<Vec<MountInfoRec>> {
    use anyhow::Context;
    let raw =
        std::fs::read_to_string("/proc/self/mountinfo").context("read /proc/self/mountinfo")?;
    let mut out: Vec<MountInfoRec> = Vec::new();
    for line in raw.lines() {
        let Some((pre, post)) = line.split_once(" - ") else {
            continue;
        };
        let mut pre_it = pre.split_whitespace();
        let _mount_id = pre_it.next();
        let _parent_id = pre_it.next();
        let _major_minor = pre_it.next();
        let _root = pre_it.next();
        let Some(mount_point_esc) = pre_it.next() else {
            continue;
        };
        let mount_point = unescape_mountinfo_path(mount_point_esc)
            .with_context(|| format!("decode mountinfo mount point: {mount_point_esc}"))?;
        let mut post_it = post.split_whitespace();
        let Some(fs_type) = post_it.next() else {
            continue;
        };
        let _mount_source = post_it.next();
        let super_options = post_it
            .next()
            .map(|s| s.split(',').map(|x| x.to_string()).collect::<Vec<_>>())
            .unwrap_or_default();
        out.push(MountInfoRec {
            mount_point: PathBuf::from(mount_point),
            fs_type: fs_type.to_string(),
            super_options,
        });
    }
    Ok(out)
}

fn join_cgroup_mount_path(mount_point: &Path, cgroup_path: &str) -> PathBuf {
    let relative = cgroup_path.trim_start_matches('/');
    if relative.is_empty() {
        mount_point.to_path_buf()
    } else {
        mount_point.join(relative)
    }
}

fn read_u64_from_file(path: PathBuf) -> anyhow::Result<u64> {
    use anyhow::Context;
    let raw = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let s = raw.trim();
    let value = s
        .parse::<u64>()
        .with_context(|| format!("parse {} as u64 from {}", s, path.display()))?;
    Ok(value)
}

fn read_cgroup_limit_file(path: PathBuf) -> anyhow::Result<Option<u64>> {
    use anyhow::Context;
    let raw = std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let s = raw.trim();
    if s.eq_ignore_ascii_case("max") {
        return Ok(None);
    }
    let value = s
        .parse::<u64>()
        .with_context(|| format!("parse {} as u64 from {}", s, path.display()))?;
    if value >= (1u64 << 60) {
        return Ok(None);
    }
    Ok(Some(value))
}

fn unescape_mountinfo_path(s: &str) -> io::Result<String> {
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i: usize = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            if i + 3 < b.len()
                && b[i + 1].is_ascii_digit()
                && b[i + 2].is_ascii_digit()
                && b[i + 3].is_ascii_digit()
            {
                let d1 = (b[i + 1] - b'0') as u16;
                let d2 = (b[i + 2] - b'0') as u16;
                let d3 = (b[i + 3] - b'0') as u16;
                let v = d1 * 64 + d2 * 8 + d3;
                out.push((v & 0xff) as u8);
                i += 4;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8(out)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "mountinfo path is not utf-8"))
}

fn read_process_cpu_total_ticks() -> anyhow::Result<u64> {
    use anyhow::Context;
    let s = std::fs::read_to_string("/proc/self/stat").context("read /proc/self/stat")?;
    let end = s
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("unexpected /proc/self/stat format: missing ')'"))?;
    let rest = s
        .get((end + 2)..)
        .ok_or_else(|| anyhow::anyhow!("unexpected /proc/self/stat format: truncated"))?;
    let parts: Vec<&str> = rest.split_whitespace().collect();
    if parts.len() < 13 {
        anyhow::bail!("unexpected /proc/self/stat format: too few fields");
    }
    // After stripping "{pid} ({comm})", the rest begins at field3 (state).
    // utime/stime are field14/15 => indices 11/12 here.
    let utime = parts[11]
        .parse::<u64>()
        .context("parse /proc/self/stat utime")?;
    let stime = parts[12]
        .parse::<u64>()
        .context("parse /proc/self/stat stime")?;
    Ok(utime.saturating_add(stime))
}

fn sample_process_cpu_usage_percent(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if clk_tck <= 0 {
        anyhow::bail!("sysconf(_SC_CLK_TCK) returned {}", clk_tck);
    }
    let clk_tck = clk_tck as f64;

    let now = std::time::Instant::now();
    let total_ticks = read_process_cpu_total_ticks()?;
    let mut guard = actor
        .process_cpu_sample
        .lock()
        .expect("process cpu sample lock poisoned");

    let cpu_percent = guard
        .as_ref()
        .and_then(|prev| {
            let wall_s = now.duration_since(prev.at).as_secs_f64();
            if wall_s <= 0.0 {
                return None;
            }
            let dticks = total_ticks.saturating_sub(prev.total_ticks);
            let proc_s = dticks as f64 / clk_tck;
            Some(100.0 * proc_s / wall_s)
        })
        .unwrap_or(0.0);

    actor
        .process_cpu_usage_gauge
        .with_label_values(&[node, role])
        .set(cpu_percent);

    *guard = Some(ProcessCpuSample {
        total_ticks,
        at: now,
    });
    Ok(())
}

fn sample_tokio_runtime_metrics(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
) -> anyhow::Result<()> {
    let metrics = tokio::runtime::Handle::try_current()
        .map_err(|e| anyhow::anyhow!("tokio handle not available: {e}"))?
        .metrics();

    let workers = metrics.num_workers();
    actor
        .tokio_num_workers_gauge
        .with_label_values(&[node, role])
        .set(workers as f64);
    actor
        .tokio_alive_tasks_gauge
        .with_label_values(&[node, role])
        .set(metrics.num_alive_tasks() as f64);
    actor
        .tokio_global_queue_depth_gauge
        .with_label_values(&[node, role])
        .set(metrics.global_queue_depth() as f64);

    sample_tokio_runtime_windowed_metrics(actor, node, role, &metrics, workers);
    Ok(())
}

#[cfg(target_has_atomic = "64")]
fn sample_tokio_runtime_windowed_metrics(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
    metrics: &tokio::runtime::RuntimeMetrics,
    workers: usize,
) {
    let now = std::time::Instant::now();
    let mut worker_busy_nanos: Vec<u128> = Vec::with_capacity(workers);
    let mut worker_park_unpark_counts: Vec<u64> = Vec::with_capacity(workers);
    for worker in 0..workers {
        worker_busy_nanos.push(metrics.worker_total_busy_duration(worker).as_nanos());
        worker_park_unpark_counts.push(metrics.worker_park_unpark_count(worker));
    }

    let mut busy_percent = 0.0;
    let mut max_worker_busy_percent = 0.0;
    let mut park_unpark_rate_hz = 0.0;
    let mut guard = actor
        .tokio_runtime_sample
        .lock()
        .expect("tokio runtime sample lock poisoned");
    if let Some(prev) = guard.as_ref() {
        let wall_s = now.duration_since(prev.at).as_secs_f64();
        if wall_s > 0.0
            && prev.worker_busy_nanos.len() == worker_busy_nanos.len()
            && prev.worker_park_unpark_counts.len() == worker_park_unpark_counts.len()
            && workers > 0
        {
            let mut total_busy_delta_s = 0.0;
            let mut max_worker_busy_delta_s = 0.0;
            let mut total_park_unpark_delta: u64 = 0;
            for worker in 0..workers {
                let busy_delta_s =
                    worker_busy_nanos[worker].saturating_sub(prev.worker_busy_nanos[worker]) as f64
                        / 1_000_000_000.0;
                total_busy_delta_s += busy_delta_s;
                if busy_delta_s > max_worker_busy_delta_s {
                    max_worker_busy_delta_s = busy_delta_s;
                }
                total_park_unpark_delta = total_park_unpark_delta.saturating_add(
                    worker_park_unpark_counts[worker]
                        .saturating_sub(prev.worker_park_unpark_counts[worker]),
                );
            }
            busy_percent =
                (total_busy_delta_s / (wall_s * workers as f64) * 100.0).clamp(0.0, 100.0);
            max_worker_busy_percent = (max_worker_busy_delta_s / wall_s * 100.0).clamp(0.0, 100.0);
            park_unpark_rate_hz = total_park_unpark_delta as f64 / wall_s;
        }
    }
    *guard = Some(TokioRuntimeSample {
        at: now,
        worker_busy_nanos,
        worker_park_unpark_counts,
    });

    actor
        .tokio_busy_percent_gauge
        .with_label_values(&[node, role])
        .set(busy_percent);
    actor
        .tokio_max_worker_busy_percent_gauge
        .with_label_values(&[node, role])
        .set(max_worker_busy_percent);
    actor
        .tokio_park_unpark_rate_gauge
        .with_label_values(&[node, role])
        .set(park_unpark_rate_hz);
}

#[cfg(not(target_has_atomic = "64"))]
fn sample_tokio_runtime_windowed_metrics(
    actor: &KvMetricsActorOwned,
    node: &str,
    role: &str,
    _metrics: &tokio::runtime::RuntimeMetrics,
    _workers: usize,
) {
    actor
        .tokio_busy_percent_gauge
        .with_label_values(&[node, role])
        .set(0.0);
    actor
        .tokio_max_worker_busy_percent_gauge
        .with_label_values(&[node, role])
        .set(0.0);
    actor
        .tokio_park_unpark_rate_gauge
        .with_label_values(&[node, role])
        .set(0.0);
}

fn read_network_bytes_by_interface() -> anyhow::Result<Vec<(String, u64, u64)>> {
    use anyhow::Context;
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open("/proc/net/dev").context("open /proc/net/dev")?;
    let reader = BufReader::new(file);
    let mut result = Vec::new();
    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res.context("read /proc/net/dev line")?;
        if idx < 2 {
            continue;
        }
        if let Some((iface, rest)) = line.split_once(':') {
            let iface = iface.trim().to_string();
            let cols: Vec<&str> = rest.split_whitespace().collect();
            if cols.len() >= 16 {
                let rx_bytes = cols[0].parse::<u64>().unwrap_or(0);
                let tx_bytes = cols[8].parse::<u64>().unwrap_or(0);
                result.push((iface, rx_bytes, tx_bytes));
            }
        }
    }
    Ok(result)
}

fn sample_network_bytes_by_interface(
    actor: &KvMetricsActorOwned,
    node: &str,
) -> anyhow::Result<()> {
    let samples = read_network_bytes_by_interface()?;
    let mut last = actor
        .net_bytes_last_sample
        .lock()
        .expect("net bytes last sample lock");
    for (dev, rx, tx) in samples {
        let (delta_rx, delta_tx) = match last.get(&dev).copied() {
            Some((prev_rx, prev_tx)) => (rx.saturating_sub(prev_rx), tx.saturating_sub(prev_tx)),
            None => (0, 0),
        };
        if delta_tx > 0 {
            actor
                .node_network_transmit_bytes_counter
                .with_label_values(&[node, &dev])
                .inc_by(delta_tx as f64);
        }
        if delta_rx > 0 {
            actor
                .node_network_receive_bytes_counter
                .with_label_values(&[node, &dev])
                .inc_by(delta_rx as f64);
        }
        last.insert(dev, (rx, tx));
    }
    Ok(())
}

fn sanitize_key_for_label(key: &str) -> String {
    const MAX_KEY_LEN: usize = 64;
    if key.len() > MAX_KEY_LEN {
        let mut truncated = key[..MAX_KEY_LEN].to_string();
        truncated.push_str("...");
        truncated
    } else {
        key.to_string()
    }
}

fn size_range_label(bytes: u64) -> &'static str {
    match bytes {
        0..=64 => "0-64B",
        65..=128 => "64-128B",
        129..=256 => "128-256B",
        257..=512 => "256-512B",
        513..=1024 => "512B-1KiB",
        1025..=2048 => "1-2KiB",
        2049..=4096 => "2-4KiB",
        4097..=8192 => "4-8KiB",
        8193..=16384 => "8-16KiB",
        16385..=32768 => "16-32KiB",
        32769..=65536 => "32-64KiB",
        65537..=131072 => "64-128KiB",
        131073..=262144 => "128-256KiB",
        262145..=524288 => "256-512KiB",
        524289..=1_048_576 => "512KiB-1MiB",
        1_048_577..=2_097_152 => "1-2MiB",
        2_097_153..=4_194_304 => "2-4MiB",
        4_194_305..=8_388_608 => "4-8MiB",
        8_388_609..=16_777_216 => "8-16MiB",
        16_777_217..=33_554_432 => "16-32MiB",
        33_554_433..=67_108_864 => "32-64MiB",
        67_108_865..=134_217_728 => "64-128MiB",
        134_217_729..=268_435_456 => "128-256MiB",
        268_435_457..=536_870_912 => "256-512MiB",
        536_870_913..=1_073_741_824 => "512MiB-1GiB",
        _ => ">=1GiB",
    }
}

fn extend_timeline_event(
    out: &mut Vec<RwTimeSeries>,
    node_id: &str,
    node_role: &str,
    phase: &'static str,
    op: &'static str,
    event: &'static str,
    key: &str,
    op_id: &str,
    timestamp_ms: i64,
) {
    let mut labels = vec![
        (
            RW_LABEL_NAME.to_string(),
            "kvcache_operation_timeline".to_string(),
        ),
        ("client".to_string(), node_id.to_string()),
        ("role".to_string(), node_role.to_string()),
        ("op".to_string(), op.to_string()),
        ("phase".to_string(), phase.to_string()),
        ("event".to_string(), event.to_string()),
    ];
    if !op_id.is_empty() {
        labels.push(("op_id".to_string(), op_id.to_string()));
    }
    if !key.is_empty() {
        labels.push(("key".to_string(), sanitize_key_for_label(key)));
    }

    let value = if event == "begin" { 1.0 } else { 0.0 };
    out.push(RwTimeSeries {
        labels: labels
            .into_iter()
            .map(|(k, v)| RwLabel { name: k, value: v })
            .collect(),
        samples: vec![RwSample {
            value,
            timestamp: timestamp_ms,
        }],
    });
}

fn extend_timeline_span(
    out: &mut Vec<RwTimeSeries>,
    node_id: &str,
    node_role: &str,
    phase: &'static str,
    op: &'static str,
    key: &str,
    op_id: &str,
    begin_us: i64,
    end_us: i64,
) {
    if end_us <= begin_us {
        return;
    }

    extend_timeline_event(
        out,
        node_id,
        node_role,
        phase,
        op,
        "begin",
        key,
        op_id,
        begin_us / 1000,
    );
    extend_timeline_event(
        out,
        node_id,
        node_role,
        phase,
        op,
        "end",
        key,
        op_id,
        end_us / 1000,
    );
}

fn extend_timeline_from_t1_t4(
    out: &mut Vec<RwTimeSeries>,
    node_id: &str,
    node_role: &str,
    op: &'static str,
    key: &str,
    op_id: &str,
    t1_us: i64,
    t2_us: i64,
    t3_us: i64,
    t4_us: i64,
) {
    extend_timeline_span(
        out, node_id, node_role, "whole", op, key, op_id, t1_us, t4_us,
    );
    extend_timeline_span(
        out, node_id, node_role, "start", op, key, op_id, t1_us, t2_us,
    );
    extend_timeline_span(
        out, node_id, node_role, "transfer", op, key, op_id, t2_us, t3_us,
    );
    extend_timeline_span(out, node_id, node_role, "end", op, key, op_id, t3_us, t4_us);
}

fn extend_put_transfer_breakdown_timeline(
    out: &mut Vec<RwTimeSeries>,
    node_id: &str,
    node_role: &str,
    put: &KvOpMetricPut,
) {
    let submit_begin_us = put.t2_us;
    let submit_blocking_us = put.transfer_submit_blocking_us.max(0);
    let create_xfer_req_us = put.transfer_create_xfer_req_us.max(0);
    let post_xfer_req_us = put.transfer_post_xfer_req_us.max(0);
    let poll_wait_us = put.transfer_poll_wait_us.max(0);

    let submit_end_us = submit_begin_us.saturating_add(submit_blocking_us);
    extend_timeline_span(
        out,
        node_id,
        node_role,
        "transfer_submit_blocking",
        "put",
        &put.key,
        &put.put_id,
        submit_begin_us,
        submit_end_us,
    );

    let submit_tail_start_us = if submit_blocking_us > 0 {
        submit_end_us.saturating_sub(create_xfer_req_us.saturating_add(post_xfer_req_us))
    } else {
        submit_begin_us
    };
    let create_begin_us = submit_tail_start_us;
    let create_end_us = create_begin_us.saturating_add(create_xfer_req_us);
    extend_timeline_span(
        out,
        node_id,
        node_role,
        "transfer_create_xfer_req",
        "put",
        &put.key,
        &put.put_id,
        create_begin_us,
        create_end_us,
    );

    let post_begin_us = create_end_us;
    let post_end_us = post_begin_us.saturating_add(post_xfer_req_us);
    extend_timeline_span(
        out,
        node_id,
        node_role,
        "transfer_post_xfer_req",
        "put",
        &put.key,
        &put.put_id,
        post_begin_us,
        post_end_us,
    );

    let poll_begin_us = submit_end_us.max(post_end_us).max(submit_begin_us);
    let poll_end_us = poll_begin_us.saturating_add(poll_wait_us);
    extend_timeline_span(
        out,
        node_id,
        node_role,
        "transfer_poll_wait",
        "put",
        &put.key,
        &put.put_id,
        poll_begin_us,
        poll_end_us,
    );
}

fn extend_op_end_pulses(
    out: &mut Vec<RwTimeSeries>,
    node_id: &str,
    node_role: &str,
    p: &KvOpEndBytesPulse,
) {
    let labels = vec![
        (RW_LABEL_NAME.to_string(), "kv_op_end_bytes".to_string()),
        ("node".to_string(), node_id.to_string()),
        ("role".to_string(), node_role.to_string()),
        ("op".to_string(), p.op.to_string()),
        ("status".to_string(), p.status.to_string()),
        ("key".to_string(), sanitize_key_for_label(&p.key)),
    ];
    let t = p.timestamp_ms;
    let v = p.bytes as f64;
    let samples = vec![
        RwSample {
            value: v,
            timestamp: t,
        },
        RwSample {
            value: 0.0,
            timestamp: t + 1,
        },
    ];
    out.push(RwTimeSeries {
        labels: labels
            .into_iter()
            .map(|(k, v)| RwLabel { name: k, value: v })
            .collect(),
        samples,
    });

    let labels_event = vec![
        (RW_LABEL_NAME.to_string(), "kv_op_end_event".to_string()),
        ("node".to_string(), node_id.to_string()),
        ("role".to_string(), node_role.to_string()),
        ("op".to_string(), p.op.to_string()),
        ("status".to_string(), p.status.to_string()),
        ("key".to_string(), sanitize_key_for_label(&p.key)),
    ];
    let samples_event = vec![
        RwSample {
            value: 1.0,
            timestamp: t,
        },
        RwSample {
            value: 0.0,
            timestamp: t + 1,
        },
    ];
    out.push(RwTimeSeries {
        labels: labels_event
            .into_iter()
            .map(|(k, v)| RwLabel { name: k, value: v })
            .collect(),
        samples: samples_event,
    });

    let range = size_range_label(p.bytes);
    let labels_bytes_range = vec![
        (
            RW_LABEL_NAME.to_string(),
            "kv_op_end_bytes_range".to_string(),
        ),
        ("node".to_string(), node_id.to_string()),
        ("role".to_string(), node_role.to_string()),
        ("op".to_string(), p.op.to_string()),
        ("status".to_string(), p.status.to_string()),
        ("key".to_string(), sanitize_key_for_label(&p.key)),
        ("range".to_string(), range.to_string()),
    ];
    out.push(RwTimeSeries {
        labels: labels_bytes_range
            .into_iter()
            .map(|(k, v)| RwLabel { name: k, value: v })
            .collect(),
        samples: vec![
            RwSample {
                value: v,
                timestamp: t,
            },
            RwSample {
                value: 0.0,
                timestamp: t + 1,
            },
        ],
    });

    let labels_event_range = vec![
        (
            RW_LABEL_NAME.to_string(),
            "kv_op_end_event_range".to_string(),
        ),
        ("node".to_string(), node_id.to_string()),
        ("role".to_string(), node_role.to_string()),
        ("op".to_string(), p.op.to_string()),
        ("status".to_string(), p.status.to_string()),
        ("key".to_string(), sanitize_key_for_label(&p.key)),
        ("range".to_string(), range.to_string()),
    ];
    out.push(RwTimeSeries {
        labels: labels_event_range
            .into_iter()
            .map(|(k, v)| RwLabel { name: k, value: v })
            .collect(),
        samples: vec![
            RwSample {
                value: 1.0,
                timestamp: t,
            },
            RwSample {
                value: 0.0,
                timestamp: t + 1,
            },
        ],
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeline_timestamp_ms(timeseries: &[RwTimeSeries], phase: &str, event: &str) -> Option<i64> {
        timeseries
            .iter()
            .find(|series| {
                let phase_ok = series
                    .labels
                    .iter()
                    .any(|label| label.name == "phase" && label.value == phase);
                let event_ok = series
                    .labels
                    .iter()
                    .any(|label| label.name == "event" && label.value == event);
                phase_ok && event_ok
            })
            .and_then(|series| series.samples.first().map(|sample| sample.timestamp))
    }

    #[test]
    fn transfer_breakdown_timeline_uses_submit_tail_for_create_and_post() {
        let put = KvOpMetricPut {
            whole_put_us: 0,
            start_us: 0,
            transfer_us: 0,
            end_us: 0,
            rpc_of_put_start_us: 0,
            start_handle_us: 0,
            end_handle_us: 0,
            key: "bench-key".to_string(),
            put_id: "1.2".to_string(),
            t1_us: 1_000_000,
            t2_us: 2_000_000,
            t3_us: 3_000_000,
            t4_us: 4_000_000,
            transfer_submit_blocking_us: 120_000,
            transfer_create_xfer_req_us: 40_000,
            transfer_post_xfer_req_us: 30_000,
            transfer_poll_wait_us: 500_000,
            transfer_poll_iters: 7,
            transfer_used_fast_path: true,
            transfer_local_noop: false,
            transfer_remote_transfer: true,
        };

        let mut out = Vec::new();
        extend_put_transfer_breakdown_timeline(&mut out, "node-a", "client", &put);

        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_submit_blocking", "begin"),
            Some(2_000)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_submit_blocking", "end"),
            Some(2_120)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_create_xfer_req", "begin"),
            Some(2_050)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_create_xfer_req", "end"),
            Some(2_090)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_post_xfer_req", "begin"),
            Some(2_090)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_post_xfer_req", "end"),
            Some(2_120)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_poll_wait", "begin"),
            Some(2_120)
        );
        assert_eq!(
            timeline_timestamp_ms(&out, "transfer_poll_wait", "end"),
            Some(2_620)
        );
    }

    #[test]
    fn transfer_breakdown_timeline_skips_zero_length_subphases() {
        let put = KvOpMetricPut {
            whole_put_us: 0,
            start_us: 0,
            transfer_us: 0,
            end_us: 0,
            rpc_of_put_start_us: 0,
            start_handle_us: 0,
            end_handle_us: 0,
            key: "bench-key".to_string(),
            put_id: "1.2".to_string(),
            t1_us: 1_000_000,
            t2_us: 2_000_000,
            t3_us: 3_000_000,
            t4_us: 4_000_000,
            transfer_submit_blocking_us: 0,
            transfer_create_xfer_req_us: 0,
            transfer_post_xfer_req_us: 0,
            transfer_poll_wait_us: 0,
            transfer_poll_iters: 0,
            transfer_used_fast_path: false,
            transfer_local_noop: false,
            transfer_remote_transfer: false,
        };

        let mut out = Vec::new();
        extend_put_transfer_breakdown_timeline(&mut out, "node-a", "client", &put);

        assert!(out.is_empty());
    }
}
