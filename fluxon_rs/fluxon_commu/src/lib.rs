extern crate self as fluxon_commu;

mod provider;

#[path = "facade/closed_sdk.rs"]
pub mod closed_sdk;
#[path = "facade/cluster.rs"]
pub mod cluster;
#[path = "facade/cluster_manager.rs"]
pub mod cluster_manager;
#[path = "facade/config.rs"]
pub mod config;
#[path = "facade/member_metadata.rs"]
pub mod member_metadata;
#[path = "facade/p2p.rs"]
pub mod p2p;
#[path = "facade/transfer.rs"]
pub mod transfer;
#[path = "facade/transfer_engine.rs"]
pub mod transfer_engine;

pub use closed_sdk::ClosedRuntimeHandle;
pub use closed_sdk::{
    RdmaProbeSnapshot, RdmaRuntimeSnapshot, capture_rdma_runtime_snapshot, probe_rdma_snapshot,
};
pub use cluster::{
    ClusterError, ClusterEvent, ClusterMember, ClusterResult, ETCD_PREFIX_SCAN_PAGE_LIMIT,
    EtcdPrefixScanAction, EtcdPrefixScanError, NodeID, NodeIDStr, NodeIDString, NodeRole,
    scan_etcd_prefix_paginated,
};
pub use cluster_manager::{
    ClusterManager, ClusterManagerNewArg, ClusterManagerRdmaControlInit,
    IpcBandwidthAttributorHandle,
};
pub use config::{NetworkConfig, validate_ip_cidr};
pub use member_metadata::{
    AccessibleIpInfo, ETCD_PREFIX_CLUSTER_MEMBER_BASE, ETCD_PREFIX_CLUSTER_MEMBER_EXT,
    ETCD_PREFIX_CLUSTER_RDMA_CONTROL, META_KEY_ACCESSIBLE_IP, META_KEY_CMD, META_KEY_HOSTNAME,
    META_KEY_LOCAL_IPC_ROOT, META_KEY_PID, META_KEY_PRODUCT_UUID, META_KEY_RDMA_CONTROL,
    META_KEY_RDMA_RUNTIME, META_KEY_SHARED_STORAGE_NODE_ID,
    META_KEY_SHARED_STORAGE_NODE_START_TIME, MemberRdmaControl, MemberRdmaResolvedConfig,
    MemberRdmaRuntime, MemberRdmaTransferEngineRuntime, MemberRdmaTransferEngineState,
    RdmaLinkLayer, RdmaPhysState, RdmaPortSnapshot, RdmaPortState, SHARE_GROUP_MEMBER_VALUE,
    ShareGroupOwnerRef, cluster_member_base_key, cluster_member_base_prefix,
    cluster_member_ext_key, cluster_member_ext_prefix, cluster_owner_rdma_control_key,
    cluster_owner_rdma_control_prefix, share_group_member_key, share_group_owner_ref_from_metadata,
    validate_rdma_control_enabled_devices,
};
pub use p2p::{
    MsgId, MsgPackHeadMeta, MsgPackRelay, P2PResult, P2pError, P2pModule, P2pModuleNewArg,
    RpcTransportPolicy, TaskId, UserRpcReq, UserRpcResp, WireMessageBody, decode_head,
    encode_wire_message, network_transport_kind,
};
pub use transfer::{
    META_KEY_TRANSFER_BACKEND_EPOCH, META_KEY_TRANSFER_READY, P2pTransportKind,
    TransferLinkEtcdWrite, TransferLinkEtcdWriterHandle, TransferLinkKeyKind,
    TransferLinkP2pSnapshotSource, TransferLinkP2pState, TransferLinkRecord, TransferLinkTeState,
    TransferReadyInfo, transfer_backend_epoch_from_metadata,
};
pub use transfer_engine::{
    ClientTransferEngineClusterRuntime, ClientTransferEngineComposedRuntime,
    ClientTransferEngineCore, ClientTransferEngineNewArg, ClientTransferEngineOpenRuntime,
    ClientTransferEngineRuntime, ClientTransferEngineRuntimeConfig,
    ClientTransferEngineSystemRuntime, CpuAllocatedMem, P2pRawMemReadFuture, P2pRawMemReadHandler,
    P2pRawMemWriteFuture, P2pRawMemWriteHandler, ProtocolType, RAW_MEM_READ_REQ_MSG_ID,
    RAW_MEM_READ_RESP_MSG_ID, RAW_MEM_WRITE_REQ_MSG_ID, RAW_MEM_WRITE_RESP_MSG_ID, RawMemReadReq,
    RawMemReadRespWire, RawMemWriteReq, RawMemWriteRespWire, TransferBackendActivationMode,
    TransferBreakdown, TransferEngineControl, TransferEngineError, TransferEngineResult,
    TransferEngineType, TransferRpcFastPath,
};
