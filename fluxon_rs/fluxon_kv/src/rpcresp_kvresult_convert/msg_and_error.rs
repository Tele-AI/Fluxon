#![allow(non_camel_case_types)]

// Moved under rpcresp_kvresult_convert to avoid direct public exposure

use crate::cluster_manager::NodeIDString;
use std::fmt;
use thiserror::Error;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgId {
    RequestSegmentRegistrationReq = 2001,
    RequestSegmentRegistrationResp = 2002,
    ResolveSideTransferLaneReq = 2003,
    ResolveSideTransferLaneResp = 2004,
    GetStartReq = 3001,
    GetStartResp = 3002,
    GetRevokeReq = 3003,
    GetRevokeResp = 3004,
    GetDoneReq = 3005,
    PutStartReq = 3007,
    GetDoneResp = 3006,
    PutStartResp = 3008,
    PutRevokeReq = 3009,
    PutRevokeResp = 3010,
    PutDoneReq = 3011,
    PutDoneResp = 3012,
    MemHolderKeepAliveReq = 3013,
    MemHolderKeepAliveResp = 3014,
    MemHolderReleaseReq = 3015,
    MemHolderReleaseResp = 3016,
    DeleteReq = 3017,
    DeleteResp = 3018,
    DeleteAckReq = 3023,
    DeleteAckResp = 3024,
    BatchDeleteAckReq = 3029,
    BatchDeleteAckResp = 3030,
    GetMetaReq = 3019,
    GetMetaResp = 3020,
    BatchDeleteClientKvMetaCacheReq = 3021,
    BatchDeleteClientKvMetaCacheResp = 3022,
    CountPrefixReq = 3025,
    CountPrefixResp = 3026,
    GetMasterOnlyMetricPartReq = 3027,
    GetMasterOnlyMetricPartResp = 3028,
    BatchIsExistReq = 3031,
    BatchIsExistResp = 3032,
    PutAppendStartReq = 3033,
    PutAppendStartResp = 3034,
    PutAppendRevokeReq = 3035,
    PutAppendRevokeResp = 3036,
    PutAppendDoneReq = 3037,
    PutAppendDoneResp = 3038,
    BatchPutStartReq = 3039,
    BatchPutStartResp = 3040,
    BatchPutRevokeReq = 3041,
    BatchPutRevokeResp = 3042,
    BatchPutDoneReq = 3043,
    BatchPutDoneResp = 3044,
    BatchGetStartReq = 3045,
    BatchGetStartResp = 3046,
    BatchGetRevokeReq = 3047,
    BatchGetRevokeResp = 3048,
    BatchGetDoneReq = 3049,
    BatchGetDoneResp = 3050,
    ReserveLocalGrantReq = 3051,
    ReserveLocalGrantResp = 3052,
    ReleaseLocalGrantReq = 3053,
    ReleaseLocalGrantResp = 3054,
    BatchPreparePutKeysReq = 3055,
    BatchPreparePutKeysResp = 3056,
    BatchReleasePutKeyReservationsReq = 3057,
    BatchReleasePutKeyReservationsResp = 3058,
    BatchOwnerReclaimReq = 3059,
    BatchOwnerReclaimResp = 3060,
    BatchEnqueueReplicaTaskReq = 3061,
    BatchEnqueueReplicaTaskResp = 3062,
    GroupedBatchPutDoneReq = 3063,
    GroupedBatchPutDoneResp = 3064,
    ExternalGetReq = 4001,
    ExternalGetResp = 4002,
    ExternalPutStartReq = 4003,
    ExternalPutResp = 4004,
    ExternalPutTransferEndReq = 4005,
    ExternalPutTransferEndResp = 4006,
    ExternalDeleteReq = 4009,
    ExternalDeleteResp = 4010,
    ExternalBatchIsExistReq = 4015,
    ExternalBatchIsExistResp = 4016,
    AllocateClientLeaseReq = 5001,
    AllocateClientLeaseResp = 5002,
    ClientLeaseKeepaliveReq = 5003,
    ClientLeaseKeepaliveResp = 5004,
    HttpPanelProxyReq = 6001,
    HttpPanelProxyResp = 6002,
    UserRpcReq = 7001,
    UserRpcResp = 7002,
    RelayCapsQueryReq = 7003,
    RelayCapsQueryResp = 7004,
}

pub type MsgIdU32 = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    Grpc,
}

impl TransportKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportKind::Grpc => "grpc",
        }
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportUser {
    Etcd,
}

impl TransportUser {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransportUser::Etcd => "etcd",
        }
    }
}

impl fmt::Display for TransportUser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Error, Debug)]
pub enum KvError {
    #[error(
        "CONFIG(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    Config(ConfigError),
    #[error(
        "API(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    Api(ApiError),
    #[error(
        "P2P(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    P2p(P2pError),
    #[error(
        "SHARED_MEM(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    SharedMem(SharedMemError),
    #[error(
        "UNREACHABLE(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    Unreachable(UnreachableError),
    #[error(
        "CLUSTER_MANAGER_EXT(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    ClusterManagerExt(ClusterManagerExtError),
    #[error(
        "METRIC(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    Metric(MetricError),
    #[error(
        "LEASE_MGR(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    LeaseMgr(LeaseMgrError),
    #[error(
        "TRANSFER_ENGINE(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    TransferEngine(TransferEngineError),
    #[error(
        "P2P_TRANSFER(code={code}): {json}",
        code = .0.code(),
        json = serde_json::to_string(.0).unwrap_or_default()
    )]
    P2pTransfer(P2pTransferError),
}

pub type KvResult<T> = Result<T, KvError>;

impl From<anyhow::Error> for KvError {
    fn from(error: anyhow::Error) -> Self {
        KvError::Api(ApiError::Unknown {
            detail: error.to_string(),
        })
    }
}
impl From<fluxon_util::vallocator::AllocError> for KvError {
    fn from(error: fluxon_util::vallocator::AllocError) -> Self {
        KvError::Api(ApiError::Allocator {
            detail: error.to_string(),
        })
    }
}
impl From<P2pError> for KvError {
    fn from(error: P2pError) -> Self {
        KvError::P2p(error)
    }
}

impl From<ApiError> for KvError {
    fn from(error: ApiError) -> Self {
        KvError::Api(error)
    }
}

impl From<ConfigError> for KvError {
    fn from(error: ConfigError) -> Self {
        KvError::Config(error)
    }
}

impl From<SharedMemError> for KvError {
    fn from(error: SharedMemError) -> Self {
        KvError::SharedMem(error)
    }
}

impl From<crate::cluster_manager::ClusterError> for KvError {
    fn from(error: crate::cluster_manager::ClusterError) -> Self {
        use crate::cluster_manager::ClusterError;

        match error {
            ClusterError::EtcdConnection { endpoints, error } => {
                KvError::Api(ApiError::TransportError {
                    transport: TransportKind::Grpc,
                    transport_user: TransportUser::Etcd,
                    detail: format!(
                        "cluster_manager etcd connect failed: endpoints={endpoints:?}, error={error}"
                    ),
                })
            }
            ClusterError::LeaseCreation { endpoints, error } => {
                KvError::Api(ApiError::TransportError {
                    transport: TransportKind::Grpc,
                    transport_user: TransportUser::Etcd,
                    detail: format!(
                        "cluster_manager etcd lease create failed: endpoints={endpoints:?}, error={error}"
                    ),
                })
            }
            ClusterError::MemberRegistration(detail) => KvError::Api(ApiError::TransportError {
                transport: TransportKind::Grpc,
                transport_user: TransportUser::Etcd,
                detail: format!("cluster_manager etcd member registration failed: {detail}"),
            }),
            ClusterError::MemberDeletion(detail) => KvError::Api(ApiError::TransportError {
                transport: TransportKind::Grpc,
                transport_user: TransportUser::Etcd,
                detail: format!("cluster_manager etcd member deletion failed: {detail}"),
            }),
            ClusterError::MemberSync(detail) => KvError::Api(ApiError::TransportError {
                transport: TransportKind::Grpc,
                transport_user: TransportUser::Etcd,
                detail: format!("cluster_manager etcd member sync failed: {detail}"),
            }),
            ClusterError::LeaseRevocation(detail) => KvError::Api(ApiError::TransportError {
                transport: TransportKind::Grpc,
                transport_user: TransportUser::Etcd,
                detail: format!("cluster_manager etcd lease revoke failed: {detail}"),
            }),
            other => KvError::Api(ApiError::Unknown {
                detail: format!("cluster_manager error: {}", other),
            }),
        }
    }
}
impl From<ClusterManagerExtError> for KvError {
    fn from(error: ClusterManagerExtError) -> Self {
        KvError::ClusterManagerExt(error)
    }
}
impl From<LeaseMgrError> for KvError {
    fn from(error: LeaseMgrError) -> Self {
        KvError::LeaseMgr(error)
    }
}
impl From<TransferEngineError> for KvError {
    fn from(error: TransferEngineError) -> Self {
        KvError::TransferEngine(error)
    }
}
impl From<fluxon_commu::TransferEngineError> for TransferEngineError {
    fn from(error: fluxon_commu::TransferEngineError) -> Self {
        match error {
            fluxon_commu::TransferEngineError::OpenPeerSegmentFailed { peer_node, detail } => {
                TransferEngineError::OpenPeerSegmentFailed { peer_node, detail }
            }
            fluxon_commu::TransferEngineError::AllocateBatchIdFailed { peer_node, detail } => {
                TransferEngineError::AllocateBatchIdFailed { peer_node, detail }
            }
            fluxon_commu::TransferEngineError::SubmitTransferFailed { peer_node, detail } => {
                TransferEngineError::SubmitTransferFailed { peer_node, detail }
            }
            fluxon_commu::TransferEngineError::GetTransferStatusFailed {
                peer_node,
                task_id,
                detail,
            } => TransferEngineError::GetTransferStatusFailed {
                peer_node,
                task_id,
                detail,
            },
            fluxon_commu::TransferEngineError::FreeBatchIdFailed { peer_node, detail } => {
                TransferEngineError::FreeBatchIdFailed { peer_node, detail }
            }
            fluxon_commu::TransferEngineError::TransferFailedForBlock { peer_node, task_id } => {
                TransferEngineError::TransferFailedForBlock { peer_node, task_id }
            }
            fluxon_commu::TransferEngineError::RegisterLocalSegmentFailed { detail } => {
                TransferEngineError::RegisterLocalSegmentFailed { detail }
            }
            fluxon_commu::TransferEngineError::UnregisterLocalSegmentFailed { detail } => {
                TransferEngineError::UnregisterLocalSegmentFailed { detail }
            }
            fluxon_commu::TransferEngineError::CreateEngineFailed { detail } => {
                TransferEngineError::CreateEngineFailed { detail }
            }
            fluxon_commu::TransferEngineError::BackendRestarting { detail } => {
                TransferEngineError::BackendRestarting { detail }
            }
            fluxon_commu::TransferEngineError::BackendStopped { detail } => {
                TransferEngineError::BackendStopped { detail }
            }
            fluxon_commu::TransferEngineError::BackendFatal { detail } => {
                TransferEngineError::BackendFatal { detail }
            }
        }
    }
}
impl From<fluxon_commu::TransferEngineError> for KvError {
    fn from(error: fluxon_commu::TransferEngineError) -> Self {
        KvError::TransferEngine(error.into())
    }
}
impl From<P2pTransferError> for KvError {
    fn from(error: P2pTransferError) -> Self {
        KvError::P2pTransfer(error)
    }
}

impl KvError {
    /// Serialize underlying group error as JSON string
    pub fn to_json(&self) -> String {
        match self {
            KvError::Config(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::Config to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::Api(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::Api to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::SharedMem(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::SharedMem to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::P2p(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::P2p to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::Unreachable(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::Unreachable to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::ClusterManagerExt(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!(
                    "KvError::ClusterManagerExt to_json error: {:?}, {:?}",
                    e, e2
                );
            }),
            KvError::Metric(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::Metric to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::LeaseMgr(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::LeaseMgr to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::TransferEngine(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::TransferEngine to_json error: {:?}, {:?}", e, e2);
            }),
            KvError::P2pTransfer(e) => serde_json::to_string(e).unwrap_or_else(|e2| {
                panic!("KvError::P2pTransfer to_json error: {:?}, {:?}", e, e2);
            }),
        }
    }

    /// Deserialize error using numeric `code` and JSON `json` with robust fallbacks.
    /// Heuristic by code range to select error group; for critical API codes, synthesize
    /// minimal payloads when JSON is missing or mismatched. Final fallback preserves input.
    pub fn from_json(code: ErrorCode, json: &str) -> Self {
        // Try decode within the hinted group by code range
        if (100..200).contains(&code) {
            if let Some(v) = ApiError::from_code_and_json(code, json) {
                return KvError::Api(v);
            }
            // Critical API code: ensure callers can detect and recover even if payload is minimal
            if code == codes_api::API_OWNER_START_TIME_MISMATCH {
                return KvError::Api(ApiError::OwnerStartTimeMismatch {
                    expected: 0,
                    got: 0,
                });
            }
        } else if (200..300).contains(&code) {
            if let Some(v) = SharedMemError::from_code_and_json(code, json) {
                return KvError::SharedMem(v);
            }
        } else if (300..400).contains(&code) {
            if let Some(v) = ConfigError::from_code_and_json(code, json) {
                return KvError::Config(v);
            }
        } else if (400..500).contains(&code) {
            if let Some(v) = UnreachableError::from_code_and_json(code, json) {
                return KvError::Unreachable(v);
            }
        } else if (500..600).contains(&code) {
            if let Some(v) = ClusterManagerExtError::from_code_and_json(code, json) {
                return KvError::ClusterManagerExt(v);
            }
        } else if (600..700).contains(&code) {
            if let Some(v) = P2pError::from_code_and_json(code, json) {
                return KvError::P2p(v);
            }
        } else if (700..800).contains(&code) {
            if let Some(v) = MetricError::from_code_and_json(code, json) {
                return KvError::Metric(v);
            }
        } else if (800..900).contains(&code) {
            // 800-899 reserved for transfer_engine
            if let Some(v) = TransferEngineError::from_code_and_json(code, json) {
                return KvError::TransferEngine(v);
            }
        } else if (900..1000).contains(&code) {
            // 900-999 reserved for lease_mgr
            if let Some(v) = LeaseMgrError::from_code_and_json(code, json) {
                return KvError::LeaseMgr(v);
            }
        }

        // Final fallback: carry code + raw json for diagnostics
        let payload = format!("code={}, json={}", code, json);
        KvError::Unreachable(UnreachableError::RpcDecodeError {
            rpc_input_json: payload,
        })
    }

    /// Back-compat convenience: map to unified numeric error code
    pub fn code(&self) -> ErrorCode {
        match self {
            KvError::Api(v) => v.code(),
            KvError::Config(v) => v.code(),
            KvError::SharedMem(v) => v.code(),
            KvError::P2p(v) => v.code(),
            KvError::Unreachable(v) => v.code(),
            KvError::ClusterManagerExt(v) => v.code(),
            KvError::Metric(v) => v.code(),
            KvError::LeaseMgr(v) => v.code(),
            KvError::TransferEngine(v) => v.code(),
            KvError::P2pTransfer(v) => v.code(),
        }
    }

    // from_code_and_json removed; use from_json(code, json) instead for a single entry point
}

// Macro-defined, grouped, templated error helpers
pub type P2pError = fluxon_commu::p2p::P2pError;

crate::define_err_group! {
    metric {
        (700, ReportFailedErr { remote_write_url: String, timeout_ms: u64 },
            msg: "First metrics report timeout: url={remote_write_url}, timeout_ms={timeout_ms}")
        ,
        (701, WaitMasterMetricConfig { timeout_ms: u64 },
            msg: "Wait master monitoring config timeout: timeout_ms={timeout_ms}")
        ,
        (702, WaitMasterReady { timeout_ms: u64 },
            msg: "Wait master ready timeout: timeout_ms={timeout_ms}")
    }
}

// Dedicated errors for client transfer_data path
crate::define_err_group! {
    transfer_engine {
        (800, OpenPeerSegmentFailed { peer_node: Option<NodeIDString>, detail: String },
            msg: "Open peer segment failed: peer_node={peer_node:?}, detail={detail}")
        ,
        (801, AllocateBatchIdFailed { peer_node: Option<NodeIDString>, detail: String },
            msg: "Allocate batch id failed: peer_node={peer_node:?}, detail={detail}")
        ,
        (802, SubmitTransferFailed { peer_node: Option<NodeIDString>, detail: String },
            msg: "Submit transfer failed: peer_node={peer_node:?}, detail={detail}")
        ,
        (803, GetTransferStatusFailed { peer_node: Option<NodeIDString>, task_id: u64, detail: String },
            msg: "Get transfer status failed: peer_node={peer_node:?}, task_id={task_id}, detail={detail}")
        ,
        (804, FreeBatchIdFailed { peer_node: Option<NodeIDString>, detail: String },
            msg: "Free batch id failed: peer_node={peer_node:?}, detail={detail}")
        ,
        (805, TransferFailedForBlock { peer_node: Option<NodeIDString>, task_id: u64 },
            msg: "Transfer failed for block: peer_node={peer_node:?}, task_id={task_id}")
        ,
        (806, CreateEngineFailed { detail: String },
            msg: "Create transfer engine failed: detail={detail}")
        ,
        (807, RegisterLocalSegmentFailed { detail: String },
            msg: "Register local segment failed: detail={detail}")
        ,
        (808, UnregisterLocalSegmentFailed { detail: String },
            msg: "Unregister local segment failed: detail={detail}")
        ,
        (809, BackendRestarting { detail: String },
            msg: "Transfer backend restarting: detail={detail}")
        ,
        (810, BackendStopped { detail: String },
            msg: "Transfer backend stopped: detail={detail}")
        ,
        (811, BackendFatal { detail: String },
            msg: "Transfer backend fatal: detail={detail}")
    }
}

// Dedicated errors for P2P-driven raw memory transfer (feature `p2p_transfer`).
// Numeric range: 1000-1099 reserved for p2p_transfer to avoid overlap with other groups.
crate::define_err_group! {
    p2p_transfer {
        (1000, InvalidArg { detail: String },
            msg: "P2P transfer invalid argument: {detail}")
        ,
        (1001, MissingPayload { detail: String },
            msg: "P2P transfer missing payload: {detail}")
        ,
        (1002, PayloadLenMismatch { expected: u64, actual: u64 },
            msg: "P2P transfer payload length mismatch: expected={expected}, actual={actual}")
        ,
        (1003, RemoteReadFailed { peer: NodeIDString, detail: String },
            msg: "P2P transfer remote read failed: peer={peer}, detail={detail}")
        ,
        (1004, RemoteWriteFailed { peer: NodeIDString, detail: String },
            msg: "P2P transfer remote write failed: peer={peer}, detail={detail}")
    }
}

crate::define_err_group! {
    cluster_manager_ext {
        (500, MasterNotFound { },
            msg: "Master node not found")
        ,
        (501, MultipleMasters { nodes: Vec<NodeIDString> },
            msg: "Multiple master nodes found: {nodes:?}, but not supported")
    }
}

crate::define_err_group! {
    lease_mgr {
        // Move lease_mgr to its own numeric range to avoid overlap with transfer_engine (800-899)
        (900, InvalidTTL { ttl: u64, message: String },
            msg: "Invalid TTL: {message}, ttl={ttl}")
        ,
        (901, LeaseNotFound { lease_id: u64, message: String },
            msg: "Lease not found: {message}, lease_id={lease_id}")
        ,
        (906, LeaseExpired { lease_id: u64, message: String },
            msg: "Lease expired: {message}, lease_id={lease_id}")
    }
}

crate::define_err_group! {
    config {
        (300, InvalidLogLevel { level: String },
            msg: "Invalid log level: {level}"),
        (301, InvalidProtocolType { typ: String },
            msg: "Invalid protocol type: {typ}"),
        (303, InvalidInstanceKey { key: String },
            msg: "Invalid instance key: {key}"),
        (304, InvalidDramSize { size: u64 },
            msg: "DRAM size must be >=0, got: {size}"),
        (305, InvalidVramSize { gpu: String, size: u64 },
            msg: "VRAM size must be positive for GPU {gpu}, got: {size}"),
        (306, InvalidEtcdAddress { addr: String },
            msg: "Invalid etcd address: {addr}"),
        (307, InvalidClusterName { name: String },
            msg: "Invalid cluster name: {name}"),
        (308, InvalidPort { port: u16 },
            msg: "Invalid port: {port}"),
        (309, EmptyEtcdEndpoints { },
            msg: "etcd endpoints cannot be empty"),
        (310, EmptyEtcdAddresses { },
            msg: "etcd addresses cannot be empty"),
        (311, InvalidClusterPoolSize { size: u32 },
            msg: "Contribute to cluster pool size must be positive, got: {size}"),
        (312, InvalidLocalBufferSize { size: u64 },
            msg: "Local buffer size must be positive, got: {size}"),
        (313, MooncakeSpecRequired { },
            msg: "Mooncake spec is required"),
        (314, FileReadError { detail: String },
            msg: "Failed to read config file: {detail}"),
        (315, YamlParseError { detail: String },
            msg: "Failed to parse YAML config: {detail}")
        ,
        (316, InvalidPromRemoteWriteUrl { detail: String },
            msg: "Invalid Prometheus remote write url: {detail}"),
        (317, InvalidLogDir { dir: String },
            msg: "Invalid log directory: {dir}"),
        (318, InvalidTransferEngineType { engine: String },
            msg: "Invalid transfer engine type: {engine}"),
        (319, InvalidPrometheusBaseUrl { detail: String },
            msg: "Invalid Prometheus base url: {detail}"),
        (321, MissingMonitoringConfig { },
            msg: "Missing monitoring config"),
        (325, InvalidSubnetWhitelistCidr { cidr: String, detail: String },
            msg: "Invalid master.network.subnet_whitelist CIDR: cidr={cidr}, detail={detail}"),
        (326, InvalidPprofDurationSeconds { seconds: u64 },
            msg: "Invalid pprof_duration_seconds: must be > 0, got: {seconds}"),
        (327, InvalidP2pRelayConfig { detail: String },
            msg: "Invalid p2p relay config: {detail}"),
        (328, InvalidRedisCompatListenAddr { addr: String },
            msg: "Invalid redis_compat.listen_addr: {addr}"),
        (329, InvalidPrimaryIpToExtendedIpsPrimaryIp { ip: String, detail: String },
            msg: "Invalid master.network.primary_ip_to_extended_ips primary ip: ip={ip}, detail={detail}"),
        (330, InvalidPrimaryIpToExtendedIpsExtendedIp { primary_ip: String, ip: String, detail: String },
            msg: "Invalid master.network.primary_ip_to_extended_ips extended ip: primary_ip={primary_ip}, ip={ip}, detail={detail}"),
        (331, InvalidClientConfig { detail: String },
            msg: "Invalid client config: {detail}"),
        (332, InvalidGreptimeOtlpLogConfig { detail: String },
            msg: "Invalid otlp_log_api config: {detail}"),
        (333, InvalidTestConfig { detail: String },
            msg: "Invalid test config: {detail}"),
    }
}

crate::define_err_group! {
    shared_mem {
        (200, InstanceNotFound { instance: String },
            msg: "Shared memory instance not found: {instance}"),
        (201, InvalidMemHolder { key: Option<String>, holder_id: Option<u64>, external_client_id: Option<String>, detail: Option<String> },
            msg: "Invalid memory holder: key={key:?}, holder_id={holder_id:?}, external_client_id={external_client_id:?}"),
        (202, MemoryAccessViolation { offset: u64, len: u64, total_len: u64, detail: Option<String> },
            msg: "Memory access violation: offset={offset}, len={len}, total_len={total_len}"),
        (203, ReferenceCountError { current: i64, expected: Option<i64>, detail: Option<String> },
            msg: "Reference count error: current={current}, expected={expected:?}"),
        (204, RegistrationFailed { location: String, addr: u64, length: usize, detail: Option<String> },
            msg: "Shared memory registration failed: location={location}, addr={addr:#x}, length={length}"),
        (205, ExternalClientError { operation: String, key: Option<String>, detail: Option<String> },
            msg: "External client operation failed: op={operation}, key={key:?}"),
        (206, NotConfigured { node_id: Option<String>, detail: Option<String> },
            msg: "Shared memory not configured for this node: {node_id:?}"),
        (207, InvalidAddress { address: u64, detail: Option<String> },
            msg: "Invalid shared memory address: {address:#x}"),
        (208, SizeMismatch { expected: u64, actual: u64 },
            msg: "Shared memory segment size mismatch: expected {expected}, got {actual}"),
        (209, MappingFailed { path: String, len: u64, detail: String },
            msg: "Failed to map shared memory: path={path}, len={len}, detail={detail}"),
        (210, StorageInstanceUnavailable { node_id: String, detail: Option<String> },
            msg: "Storage instance unavailable: {node_id}"),
        (211, MemHolderAlreadyReleased { key: Option<String>, holder_id: Option<u64>, detail: Option<String> },
            msg: "Memory holder already released: key={key:?}, holder_id={holder_id:?}")
        ,
        (212, MetaDataLoadError { path: String, detail: String },
            msg: "Failed to load shared memory metadata: path={path}, detail={detail}")
    }
}

crate::define_err_group! {
    api {
        (100, NotImplemented { },
            msg: "Not implemented yet"),
        (101, NoSpace { node: NodeIDString, segment: String, total_capacity: u64, free_capacity: u64 },
            msg: "No space left: node={node}, segment={segment}, total={total_capacity}, free={free_capacity}"),
        (102, Allocator { detail: String },
            msg: "Allocator error: {detail}"),
        (103, Unknown { detail: String },
            msg: "Unknown error: {detail}"),
        (105, KeyNotFound { key: String },
            msg: "Key not found ({key})"),
        (106, RegisterSegmentFailed { detail: String },
            msg: "Register segment failed: {detail}"),
        (107, SegmentNotFound { desc: String },
            msg: "Segment not found ({desc})"),
        (108, NodeNotFound { desc: String },
            msg: "Node not found: {desc}"),
        (109, InvalidPutMasterState { detail: String },
            msg: "Invalid put master state: {detail}"),
        (110, Transfer { from_addr: u64, to_addr: u64, len: u64, error: String },
            msg: "Transfer error: from_addr: {from_addr}, to_addr: {to_addr}, len: {len}, error: {error}"),
        (111, SharedMemory { detail: String },
            msg: "Shared memory error: {detail}"),
        (112, SystemShutdown { detail: String },
            msg: "System shutdown: {detail}")
        ,
        (113, SegmentBaseaddr { detail: String },
            msg: "Segment base address not available: {detail}")
        ,
        (114, MountClientSegmentFailed { detail: String },
            msg: "Mount client segment failed: {detail}")
        ,
        (115, SegmentNotMounted { detail: String },
            msg: "Segment not mounted: {detail}")
        ,
        (116, GetTimeout { timeout_ms: u64, detail: String },
            msg: "Get operation timeout: timeout_ms={timeout_ms}, detail={detail}")
        ,
        (117, OwnerStartTimeMismatch { expected: i64, got: i64 },
            msg: "Owner start_time mismatch: expected={expected}, got={got}")
        ,
        (118, InvalidArgument { detail: String },
            msg: "Invalid argument: {detail}")
        ,
        (119, FileWriteError { path: String, offset: u64, detail: String },
            msg: "File write error: path={path}, offset={offset}, detail={detail}")
        ,
        (120, UserRpcMissingPayload { path: String },
            msg: "UserRpcResp missing payload raw_bytes[0] (path={path})")
        ,
        (121, TransportError { transport: TransportKind, transport_user: TransportUser, detail: String },
            msg: "Transport error: transport={transport}, transport_user={transport_user}, detail={detail}")
        ,
        (122, KeyBeingWritten { key: String },
            msg: "Key is currently being written: key={key}")
        ,
        (123, KeyAlreadyExists { key: String },
            msg: "Key already exists: key={key}")
    }
}

crate::define_err_group! {
    unreachable {
        (400, OwnerNoSeg { detail: String },
            msg: "Unreachable(owner-no-seg): config=0 initializes as external; non-zero initializes as owner; the owner must have memory space (segment). detail={detail}")
        ,
        (401, RpcDecodeError { rpc_input_json: String },
            msg: "Unreachable(rpc-decode-error): RPC payloads are uniformly auto-serialized to JSON; deserialization should never fail unless the transport is corrupted or the system is compromised. detail={rpc_input_json}")
        ,
        (402, DuplicateSegId { device_id: String, node_id: String },
            msg: "Unreachable(duplicate-seg-id): Config must ensure per-node unique segment ids (dictionary semantics); this should never be reached. device_id={device_id}, node_id={node_id}")
    }
}

// Success code (not an error): leave as a simple constant
pub const OK: u32 = 0;

/// Type alias for error codes used across RPC responses and helpers
pub type ErrorCode = u32;

// Back-compat alias removed: use codes_api::* for codes

// from_code_and_desc_private removed; use KvErrorToCodeAndDesc::from_code_and_desc via
// rpcresp_kvresult_convert::error_from_code_and_desc.

// ---- Legacy constants module expected by older code paths ----
pub mod kv {
    pub struct KeyNotFound;
    impl KeyNotFound {
        pub const CODE: super::ErrorCode = super::codes_api::API_KEY_NOT_FOUND;
    }
}

// ---- Legacy conversion helpers expected by config.rs ----
impl ConfigError {
    pub fn into_kverror(self) -> super::msg_and_error::KvError {
        super::msg_and_error::KvError::Config(self)
    }
    pub fn into_box(self) -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(super::msg_and_error::KvError::Config(self))
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiError, KvError, TransportKind, TransportUser};
    use crate::cluster_manager::ClusterError;

    #[test]
    fn transport_error_json_roundtrip_keeps_identity() {
        let err = ApiError::TransportError {
            transport: TransportKind::Grpc,
            transport_user: TransportUser::Etcd,
            detail: "dial failed".to_string(),
        };
        let (code, json) = err.to_code_and_json();
        let decoded = ApiError::from_code_and_json(code, &json).unwrap();
        match decoded {
            ApiError::TransportError {
                transport,
                transport_user,
                detail,
            } => {
                assert_eq!(transport, TransportKind::Grpc);
                assert_eq!(transport_user, TransportUser::Etcd);
                assert_eq!(detail, "dial failed");
            }
            other => panic!("unexpected decoded error: {:?}", other),
        }
    }

    #[test]
    fn cluster_error_etcd_connection_maps_to_transport_error() {
        let err = ClusterError::EtcdConnection {
            endpoints: vec!["127.0.0.1:2379".to_string()],
            error: "connection refused".to_string(),
        };
        let mapped = KvError::from(err);
        match mapped {
            KvError::Api(ApiError::TransportError {
                transport,
                transport_user,
                detail,
            }) => {
                assert_eq!(transport, TransportKind::Grpc);
                assert_eq!(transport_user, TransportUser::Etcd);
                assert!(detail.contains("127.0.0.1:2379"));
                assert!(detail.contains("connection refused"));
            }
            other => panic!("unexpected mapped error: {:?}", other),
        }
    }
}
