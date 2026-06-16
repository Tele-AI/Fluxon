use crate::client_kv_api::delete::handle_batch_delete_client_kv_meta_cache;
use crate::client_kv_api::msg_pack::{
    ExternalDeleteAckReq, ExternalDeleteAckResp, ExternalDeleteReq, ExternalDeleteResp,
    ExternalGetReq, ExternalGetResp, ExternalIsExistReq, ExternalIsExistResp, ExternalPutCommitReq,
    ExternalPutCommitResp, ExternalPutRevokeReq, ExternalPutRevokeResp, ExternalPutStartReq,
    ExternalPutStartResp, ExternalPutTransferEndReq, ExternalPutTransferEndResp, SyncKvToFileReq,
    SyncKvToFileResp, TestPutPhaseTrace,
};
use crate::cluster_manager::NodeIDString;
use crate::config::TestSpecConfig;
use crate::master_kv_router::msg_pack::{
    BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, DeleteClientKvMetaCacheItem,
};
use crate::master_lease_manager::msg_pack::{AllocateClientLeaseReq, ClientLeaseKeepaliveReq};
use crate::memholder::{AllMemholderRefCount, MemoryInfo, UserMemHolder};
use crate::memholder::{
    EnsureMemholderMgmtDeleteHandle, MemholderManagerTrait, NodeHolderKey, OwnerDeleteAckItem,
    OwnerDeleteAckMemMgr, OwnerExternalMemMgr,
};
use crate::{
    client_seg_pool::{ClientSegPool, ClientSegPoolAccessTrait, ResolveSideTransferLaneReq},
    client_transfer_engine::{ClientTransferEngine, ClientTransferEngineAccessTrait},
    cluster_manager::{ClusterEvent, ClusterManager, ClusterManagerAccessTrait},
    master_kv_router::msg_pack::{
        DeleteReq, GetDoneReq, GetMetaReq, GetRevokeReq, GetStartReq, PutDoneReq, PutRevokeReq,
        PutStartReq,
    },
    metric_reporter::{MetricReporter, MetricReporterAccessTrait},
    metrics::{MetricsHandle, OperationKind, RequestStage},
    p2p::{
        msg_pack::{RPCCaller, RPCHandler},
        p2p_module::{P2pModule, P2pModuleAccessTrait},
    },
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use async_trait::async_trait;
use dashmap::DashMap;
use fluxon_framework::{LogicalModule, define_module};
use fluxon_util::map_lock::AMapLock;
use limit_thirdparty::tokio;
use parking_lot::Mutex;
use std::sync::Weak;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tracing::warn;

/// Information about a memholder held by external client
#[derive(Clone)]
pub struct ExternalHoldingGetInfo {
    pub key: String,
    pub req_node_id: String,
    pub memory_info: Arc<MemoryInfo>, // The actual memholder being held
}

pub use get::RemoteGetInfo;
pub mod external_api;
pub use external_api::HandlerForExternalClient;
pub type TestObservePutPhaseSink = Arc<Mutex<Option<TestPutPhaseTrace>>>;

#[derive(Debug, Clone)]
enum CachedValue {
    MetadataOnly,
    LocalReplica(Arc<MemoryInfo>),
}

impl CachedValue {
    fn rank(&self) -> u8 {
        match self {
            Self::MetadataOnly => 0,
            Self::LocalReplica(_) => 1,
        }
    }
}

/// Optional arguments for put operations
#[derive(Clone, Debug)]
pub enum PutOptionalArg {
    /// Attach the written key to the specified lease on commit
    LeaseId(u64),
    /// Ask the master to fail-fast when the same key already has an inflight put.
    RejectIfInflightSameKey,
    /// Prefer placing the target allocation on a kvclient within this sub_cluster.
    PreferredSubCluster(String),
    /// Hidden test-only side-channel for collecting per-put phase timings.
    TestObservePutPhases(TestObservePutPhaseSink),
}

/// Container for optional put arguments
#[derive(Clone, Debug, Default)]
pub struct PutOptionalArgs(pub Vec<PutOptionalArg>);

impl PutOptionalArgs {
    pub fn new() -> Self {
        Self(Vec::new())
    }
    /// Get the last provided lease_id if any
    pub fn lease_id(&self) -> Option<u64> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::LeaseId(id) => Some(*id),
            PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::PreferredSubCluster(_)
            | PutOptionalArg::TestObservePutPhases(_) => None,
        })
    }

    pub fn reject_if_inflight_same_key(&self) -> bool {
        self.0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::RejectIfInflightSameKey))
    }

    /// Get the last provided preferred_sub_cluster if any.
    pub fn preferred_sub_cluster(&self) -> Option<&str> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::PreferredSubCluster(sc) => Some(sc.as_str()),
            PutOptionalArg::LeaseId(_)
            | PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::TestObservePutPhases(_) => None,
        })
    }

    pub fn test_observe_put_phases(&self) -> Option<TestObservePutPhaseSink> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::TestObservePutPhases(sink) => Some(sink.clone()),
            PutOptionalArg::LeaseId(_)
            | PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::PreferredSubCluster(_) => None,
        })
    }
}

/// KV operation timestamp kind with Begin/End events for Grafana state visualization
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MetricTimestampKind {
    // Put operation phases
    PutWholeBegin,
    PutWholeEnd,
    PutStartBegin,
    PutStartEnd,
    PutTransferBegin,
    PutTransferEnd,
    PutEndBegin,
    PutEndEnd,
    PutRpcBegin,
    PutRpcEnd,

    // Get operation phases
    GetWholeBegin,
    GetWholeEnd,
    GetStartBegin,
    GetStartEnd,
    GetTransferBegin,
    GetTransferEnd,
    GetEndBegin,
    GetEndEnd,
}

/// Timestamp for KV operation metrics with enhanced tracking
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricTimestamp {
    pub time: i64,
    pub kind: MetricTimestampKind,
    pub key_opt: Option<String>,
    pub ope_id_opt: Option<String>,
}

impl MetricTimestampKind {
    /// Get the corresponding value for Prometheus (1 for Begin, 0 for End)
    pub fn to_prometheus_value(&self) -> i32 {
        match self {
            Self::PutWholeBegin
            | Self::PutStartBegin
            | Self::PutTransferBegin
            | Self::PutEndBegin
            | Self::PutRpcBegin
            | Self::GetWholeBegin
            | Self::GetStartBegin
            | Self::GetTransferBegin
            | Self::GetEndBegin => 1,

            Self::PutWholeEnd
            | Self::PutStartEnd
            | Self::PutTransferEnd
            | Self::PutEndEnd
            | Self::PutRpcEnd
            | Self::GetWholeEnd
            | Self::GetStartEnd
            | Self::GetTransferEnd
            | Self::GetEndEnd => 0,
        }
    }

    /// Get the operation phase name (without Begin/End suffix)
    pub fn get_phase_name(&self) -> &'static str {
        match self {
            Self::PutWholeBegin | Self::PutWholeEnd => "put_whole",
            Self::PutStartBegin | Self::PutStartEnd => "put_start",
            Self::PutTransferBegin | Self::PutTransferEnd => "put_transfer",
            Self::PutEndBegin | Self::PutEndEnd => "put_end",
            Self::PutRpcBegin | Self::PutRpcEnd => "put_rpc",
            Self::GetWholeBegin | Self::GetWholeEnd => "get_whole",
            Self::GetStartBegin | Self::GetStartEnd => "get_start",
            Self::GetTransferBegin | Self::GetTransferEnd => "get_transfer",
            Self::GetEndBegin | Self::GetEndEnd => "get_end",
        }
    }

    /// Get the base operation name (put/get)
    pub fn get_operation_name(&self) -> &'static str {
        match self {
            Self::PutWholeBegin
            | Self::PutWholeEnd
            | Self::PutStartBegin
            | Self::PutStartEnd
            | Self::PutTransferBegin
            | Self::PutTransferEnd
            | Self::PutEndBegin
            | Self::PutEndEnd
            | Self::PutRpcBegin
            | Self::PutRpcEnd => "put",

            Self::GetWholeBegin
            | Self::GetWholeEnd
            | Self::GetStartBegin
            | Self::GetStartEnd
            | Self::GetTransferBegin
            | Self::GetTransferEnd
            | Self::GetEndBegin
            | Self::GetEndEnd => "get",
        }
    }

    /// Check if this is a begin event
    pub fn is_begin(&self) -> bool {
        self.to_prometheus_value() == 1
    }

    /// Check if this is an end event
    pub fn is_end(&self) -> bool {
        self.to_prometheus_value() == 0
    }
}

/// KV operation metrics type enum
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum KvMetrics {
    /// Various phases of Put operation
    Put {
        whole_put: i64,
        start: i64,

        transfer: i64,
        end: i64,
        rpc_of_put_start: i64,
        /// Server handling time for PutStart RPC (microseconds)
        start_handle: i64,
        /// Server handling time for PutDone RPC (microseconds)
        end_handle: i64,
        /// Key associated with the put operation
        key: String,
        /// Put operation ID formatted as "{}.{}"
        put_id: String,
        /// ✅ 源头时间戳：操作真正开始的时间 (微秒) - t1
        start_timestamp_us: i64,
        /// ✅ 源头时间戳：start阶段结束/transfer阶段开始的时间 (微秒) - t2
        transfer_start_timestamp_us: i64,
        /// ✅ 源头时间戳：transfer阶段结束/end阶段开始的时间 (微秒) - t3
        end_start_timestamp_us: i64,
        /// ✅ 源头时间戳：操作真正结束的时间 (微秒) - t4
        end_timestamp_us: i64,
        transfer_submit_blocking_us: i64,
        transfer_create_xfer_req_us: i64,
        transfer_post_xfer_req_us: i64,
        transfer_poll_wait_us: i64,
        transfer_poll_iters: i64,
        transfer_used_fast_path: bool,
        transfer_local_noop: bool,
        transfer_remote_transfer: bool,
    },
    /// Various phases of Get operation
    Get {
        whole_get: i64,
        start: i64,
        transfer: i64,
        end: i64,
        /// Server handling time for GetStart RPC (microseconds)
        start_handle: i64,
        /// Server handling time for GetDone RPC (microseconds)
        end_handle: i64,
        /// Key associated with the get operation
        key: String,
        /// Get operation ID formatted as "{}.{}"
        get_id: String,
        /// ✅ 源头时间戳：操作真正开始的时间 (微秒) - t1
        start_timestamp_us: i64,
        /// ✅ 源头时间戳：start阶段结束/transfer阶段开始的时间 (微秒) - t2
        transfer_start_timestamp_us: i64,
        /// ✅ 源头时间戳：transfer阶段结束/end阶段开始的时间 (微秒) - t3
        end_start_timestamp_us: i64,
        /// ✅ 源头时间戳：操作真正结束的时间 (微秒) - t4
        end_timestamp_us: i64,
    },
}

#[cfg(test)]
pub mod client_test_record;
mod delete;
mod get;
pub mod msg_pack;
mod put;

// --- External RPC Handlers ---
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::FromError;

// External handlers that use the ExternalApi trait on ClientKvApi
async fn handle_external_get(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalGetReq>,
) -> MsgPack<ExternalGetResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let dbg_req_node_id = req.req_node_id.clone();
    let resp = view
        .client_kv_api()
        .external_get(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_get error: {e}; key={key}, req_node_id={req_node_id}",
                key = dbg_key,
                req_node_id = dbg_req_node_id
            );
            ExternalGetResp {
                external_memholder_info: None,
                ..crate::rpcresp_kvresult_convert::FromError::from_error(&e)
            }
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_start(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutStartReq>,
) -> MsgPack<ExternalPutStartResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let dbg_len = req.len;
    let resp = view
        .client_kv_api()
        .external_put_start(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_start error: {e}; key={key}, len={len}",
                key = dbg_key,
                len = dbg_len
            );
            let mut r: ExternalPutStartResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.src_offset = 0;
            r.target_offset = 0;
            r.transfer_target_offset = None;
            r.peer_id = None;
            r.put_id = None;
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}
async fn handle_external_put_transfer_end(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutTransferEndReq>,
) -> MsgPack<ExternalPutTransferEndResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let dbg_put_id = req.put_id.clone();
    let resp = view
        .client_kv_api()
        .external_put_transfer_end(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_transfer_end error: {e}; key={key}, put_id={put_id:?}",
                key = dbg_key,
                put_id = dbg_put_id
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_commit(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutCommitReq>,
) -> MsgPack<ExternalPutCommitResp> {
    let req = msg.serialize_part.clone();
    let dbg_key = req.key.clone();
    let dbg_put_id = req.put_id.clone();
    let resp = view
        .client_kv_api()
        .external_put_commit(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_commit error: {e}; key={key}, put_id={put_id:?}",
                key = dbg_key,
                put_id = dbg_put_id
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_revoke(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutRevokeReq>,
) -> MsgPack<ExternalPutRevokeResp> {
    let req = msg.serialize_part.clone();
    let dbg_key = req.key.clone();
    let dbg_put_id = req.put_id.clone();
    let resp = view
        .client_kv_api()
        .external_put_revoke(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_revoke error: {e}; key={key}, put_id={put_id:?}",
                key = dbg_key,
                put_id = dbg_put_id
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_delete_ack(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalDeleteAckReq>,
) -> MsgPack<ExternalDeleteAckResp> {
    let req = msg.serialize_part.clone();
    // Validate owner's start_time (allow 0 for legacy callers)
    let expected = view.cluster_manager().get_self_info().node_start_time;
    if req.started_time != 0 && req.started_time != expected {
        let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                expected,
                got: req.started_time,
            },
        );
        return MsgPack {
            serialize_part: ExternalDeleteAckResp::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }
    let inner = view.client_kv_api().inner();
    // Try to remove the holding record for this external client and holder_id
    let mut success = false;
    let mut error_msg = String::new();

    match inner.external_get_holding.remove(&NodeHolderKey::new(
        req.external_client_id.clone(),
        req.holder_id,
    )) {
        Some(_) => success = true,
        None => {
            error_msg = format!(
                "holding id {} not found for client {}",
                req.holder_id, req.external_client_id
            );
        }
    }

    MsgPack {
        serialize_part: ExternalDeleteAckResp {
            error_code: if success {
                crate::rpcresp_kvresult_convert::msg_and_error::OK
            } else {
                crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND
            },
            error_json: error_msg,
        },
        raw_bytes: Vec::new(),
    }
}
async fn handle_external_delete(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalDeleteReq>,
) -> MsgPack<ExternalDeleteResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let resp = view
        .client_kv_api()
        .external_delete(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_delete error: {e}; key={key}",
                key = dbg_key
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_is_exist(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalIsExistReq>,
) -> MsgPack<ExternalIsExistResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let resp = view
        .client_kv_api()
        .external_is_exist(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_is_exist error: {e}; key={key}",
                key = dbg_key
            );
            let mut r: ExternalIsExistResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.exists = false;
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(Error::new(ErrorKind::WriteZero, "write_at returned 0"));
        }
        offset = offset
            .checked_add(n as u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "offset overflow"))?;
        buf = &buf[n..];
    }
    Ok(())
}

fn sync_kv_bytes_field_to_file(
    encoded_flat_dict: &[u8],
    bytes_field_key: &str,
    filepath: &str,
    file_offset: u64,
) -> KvResult<()> {
    use crate::memholder::kvclient_encode::FlatKvValueRange;

    if bytes_field_key.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "bytes_field_key must be non-empty".to_string(),
        }));
    }
    if filepath.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "filepath must be non-empty".to_string(),
        }));
    }

    let entries = crate::memholder::kvclient_encode::flat_kv_decode_ranges(encoded_flat_dict)
        .map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("flat dict decode failed: {}", e),
            })
        })?;

    let mut found: Option<(usize, usize)> = None;
    for (k, v) in entries {
        if k != bytes_field_key {
            continue;
        }
        match v {
            FlatKvValueRange::BytesRange { start, len } => {
                found = Some((start, len));
            }
            _ => {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("field is not bytes: {}", bytes_field_key),
                }));
            }
        }
        break;
    }

    let Some((start, len)) = found else {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("missing bytes field: {}", bytes_field_key),
        }));
    };

    let end = start.checked_add(len).ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: "bytes range overflow".to_string(),
        })
    })?;
    if end > encoded_flat_dict.len() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "bytes range out of bounds".to_string(),
        }));
    }

    let data = &encoded_flat_dict[start..end];

    let path = std::path::Path::new(filepath);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                KvError::Api(ApiError::FileWriteError {
                    path: filepath.to_string(),
                    offset: file_offset,
                    detail: format!("create parent dir failed: {}", e),
                })
            })?;
        }
    }

    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .map_err(|e| {
            KvError::Api(ApiError::FileWriteError {
                path: filepath.to_string(),
                offset: file_offset,
                detail: e.to_string(),
            })
        })?;

    write_all_at(&f, data, file_offset).map_err(|e| {
        KvError::Api(ApiError::FileWriteError {
            path: filepath.to_string(),
            offset: file_offset,
            detail: e.to_string(),
        })
    })?;

    Ok(())
}

async fn handle_sync_kv_to_file_client(
    view: &ClientKvApiView,
    msg: &MsgPack<SyncKvToFileReq>,
) -> MsgPack<SyncKvToFileResp> {
    let req = msg.serialize_part.clone();
    let key = req.key.clone();

    let result: KvResult<()> = async {
        if req.key.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "key must be non-empty".to_string(),
            }));
        }

        let got = view.client_kv_api().get(&req.key).await?;
        let Some((holder, _remote)) = got else {
            return Err(KvError::Api(ApiError::KeyNotFound { key }));
        };

        sync_kv_bytes_field_to_file(
            holder.bytes(),
            req.bytes_field_key.as_str(),
            req.filepath.as_str(),
            req.file_offset,
        )?;
        Ok(())
    }
    .await;

    let (error_code, error_json) = match result {
        Ok(()) => (
            crate::rpcresp_kvresult_convert::msg_and_error::OK,
            String::new(),
        ),
        Err(e) => (e.code(), e.to_json()),
    };

    MsgPack {
        serialize_part: SyncKvToFileResp {
            error_code,
            error_json,
        },
        raw_bytes: Vec::new(),
    }
}

define_module!(
    ClientKvApi,
    (cluster_manager, ClusterManager),
    (p2p, P2pModule),
    (client_kv_api, ClientKvApi),
    (client_transfer_engine, ClientTransferEngine),
    (client_seg_pool, ClientSegPool),
    (metric_reporter, MetricReporter)
);

// Use unified conversion in msg_and_error.rs: ClusterManagerExtError -> KvError::ClusterManagerExt

/// ClientKvApi module creation parameters
#[derive(Clone, Debug)]
pub struct ClientKvApiNewArg {
    pub test_spec_config: TestSpecConfig,
}

pub struct ClientKvApi(ClientKvApiInner);

#[derive(Debug)]
pub struct GetCachedInfo {
    put_time_ms: u64,
    put_version: u32,
    value: CachedValue,
}

struct ClientKvApiViewHolder {
    view: OnceLock<ClientKvApiView>,
}

impl ClientKvApiViewHolder {
    fn new() -> Self {
        Self {
            view: OnceLock::new(),
        }
    }

    fn attach(&self, view: ClientKvApiView) {
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("ClientKvApi view attached twice"));
    }

    fn clone_view(&self) -> ClientKvApiView {
        self.view.get().unwrap().clone()
    }
}

impl std::ops::Deref for ClientKvApiViewHolder {
    type Target = ClientKvApiView;

    fn deref(&self) -> &Self::Target {
        self.view.get().unwrap()
    }
}

pub struct ClientKvApiInner {
    view: ClientKvApiViewHolder,
    test_spec_config: TestSpecConfig,
    metrics: OnceLock<Arc<MetricsHandle>>,

    /// make sure each remote kv get run in order
    pub get_remote_kv_lock: AMapLock<String>,
    /// key -> value info on this node
    /// we can only remove value if it's put_time_ms and put_version match remote eviction command
    get_cached_info: DashMap<String, GetCachedInfo>,

    /// Shared delete actor input for owner -> external weak-index invalidation.
    pub external_invalidate_delete: EnsureMemholderMgmtDeleteHandle<DeleteClientKvMetaCacheItem>,
    /// Shared delete actor input for owner -> master delete-ack batching.
    pub delete_ack_batch: EnsureMemholderMgmtDeleteHandle<OwnerDeleteAckItem>,
    /// Shared manager for owner -> master delete-ack batching.
    pub owner_delete_ack_mgr: OwnerDeleteAckMemMgr,

    // record external_client get_holding info (owned, flattened manager)
    pub external_get_holding: OwnerExternalMemMgr,
    /// Weak handle to a shared refcount tracker for all UserMemHolder of this client.
    ///
    /// - A strong `Arc<AllMemholderRefCount>` is given to every `UserMemHolder` created by this client.
    /// - When the last `UserMemHolder` is dropped, the strong `Arc<AllMemholderRefCount>` is dropped too,
    ///   and this weak handle will no longer upgrade, meaning the client can be safely dropped.
    /// - Stored as `Weak` in `OnceLock` to avoid cycles and allow lazy initialization.
    pub all_memholder_refcount: OnceLock<Weak<AllMemholderRefCount>>,
    /// External API is implemented directly on ClientKvApi; no handler stored here

    #[cfg(test)]
    test_record: crate::client_kv_api::client_test_record::ClientTestRecord,

    rpc_caller_get_start: RPCCaller<GetStartReq>,
    rpc_caller_get_revoke: RPCCaller<GetRevokeReq>,
    rpc_caller_get_done: RPCCaller<GetDoneReq>,
    rpc_caller_put_start: RPCCaller<PutStartReq>,
    rpc_caller_put_revoke: RPCCaller<PutRevokeReq>,
    rpc_caller_put_done: RPCCaller<PutDoneReq>,
    rpc_caller_delete: RPCCaller<DeleteReq>,
    rpc_caller_batch_delete_ack: RPCCaller<BatchDeleteAckReq>,
    rpc_caller_get_meta: RPCCaller<GetMetaReq>,
    _rpc_caller_allocate_client_lease: RPCCaller<AllocateClientLeaseReq>,
    _rpc_caller_client_lease_keepalive: RPCCaller<ClientLeaseKeepaliveReq>,
    rpc_caller_external_put_commit: RPCCaller<ExternalPutCommitReq>,
    rpc_caller_external_put_revoke: RPCCaller<ExternalPutRevokeReq>,
    rpc_caller_resolve_side_transfer_lane: RPCCaller<ResolveSideTransferLaneReq>,

    /// Default lease id recorded for inspection/convenience, but NOT auto-applied.
    /// Callers must explicitly pass `Some(lease_id)` to attach a put to a lease.
    default_lease_id: parking_lot::RwLock<Option<u64>>,
    /// External put (remote target) pending context keyed by (key, put_time_ms, put_version).
    /// 注意：put_id (time_ms,version) 在不同 key 上并不全局唯一，因此必须携带 key 作为索引的一部分，避免碰撞。
    /// 使用 moka::sync::SegmentedCache 并设置 30 分钟 TTL，避免异常路径未清理导致的泄漏；不设置容量上限，纯 TTL 控制。
    external_pending_puts: moka::sync::SegmentedCache<(String, u64, u32), ExternalPendingPutCtx>,
}

impl ClientKvApiInner {
    fn should_replace_cached_info(
        current: &GetCachedInfo,
        put_time_ms: u64,
        put_version: u32,
        new_value_rank: u8,
    ) -> bool {
        if current.put_time_ms != put_time_ms {
            return current.put_time_ms <= put_time_ms;
        }
        if current.put_version != put_version {
            return current.put_version <= put_version;
        }
        current.value.rank() < new_value_rank
    }

    fn upsert_cached_info(&self, key: &str, candidate: GetCachedInfo) {
        match self.get_cached_info.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if Self::should_replace_cached_info(
                    entry.get(),
                    candidate.put_time_ms,
                    candidate.put_version,
                    candidate.value.rank(),
                ) {
                    entry.insert(candidate);
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
        }
    }

    pub(crate) fn cache_metadata_only_after_put(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) {
        self.upsert_cached_info(
            key,
            GetCachedInfo {
                put_time_ms: put_id.0,
                put_version: put_id.1,
                value: CachedValue::MetadataOnly,
            },
        );
    }

    pub(crate) fn cache_local_replica_after_get(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: Arc<MemoryInfo>,
    ) {
        self.upsert_cached_info(
            key,
            GetCachedInfo {
                put_time_ms: put_id.0,
                put_version: put_id.1,
                value: CachedValue::LocalReplica(memory_info),
            },
        );
    }

    pub(crate) fn short_circuit_put_payload_path_enabled(&self) -> bool {
        self.test_spec_config.short_circuit_put_payload_path
    }

    pub(crate) fn skip_put_end_commit_enabled(&self) -> bool {
        self.test_spec_config.skip_put_end_commit
    }
}

#[derive(Debug, Clone)]
pub struct ExternalPendingPutCtx {
    pub peer_id: NodeIDString,
    pub target_base_addr: u64,
    pub target_offset: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSet {
    pub mean: f64,
    pub p99: i64,
    pub p95: i64,
    pub min: i64,
    pub max: i64,
    pub timestamps: Vec<MetricTimestamp>,
}

// Removed StageScope: no longer using stage-scoped gauges; we record
// timestamps (t1..t4) and emit stage success/error directly.

impl MetricsSet {
    /// Convert to Prometheus format string
    pub fn to_prometheus_format(&self, metric_name: &str, client_id: &str) -> String {
        let mut result = String::new();

        // Traditional aggregated metrics (mean, p99, p95, min, max)
        result.push_str(&format!(
            "kvcache_{}_mean{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.mean
        ));

        result.push_str(&format!(
            "kvcache_{}_p99{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.p99
        ));

        result.push_str(&format!(
            "kvcache_{}_p95{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.p95
        ));

        result.push_str(&format!(
            "kvcache_{}_min{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.min
        ));

        result.push_str(&format!(
            "kvcache_{}_max{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.max
        ));

        result.push_str(&format!(
            "kvcache_{}_sample_count{{client=\"{}\"}} {}\n",
            metric_name,
            client_id,
            self.timestamps.len()
        ));

        // Add metrics for unique keys and operations
        let unique_keys: std::collections::HashSet<_> = self
            .timestamps
            .iter()
            .filter_map(|ts| ts.key_opt.as_ref())
            .collect();
        result.push_str(&format!(
            "kvcache_{}_unique_keys_count{{client=\"{}\"}} {}\n",
            metric_name,
            client_id,
            unique_keys.len()
        ));

        let unique_ops: std::collections::HashSet<_> = self
            .timestamps
            .iter()
            .filter_map(|ts| ts.ope_id_opt.as_ref())
            .collect();
        result.push_str(&format!(
            "kvcache_{}_unique_operations_count{{client=\"{}\"}} {}\n",
            metric_name,
            client_id,
            unique_ops.len()
        ));

        // Generate individual timestamp events for Grafana state visualization
        for timestamp in &self.timestamps {
            let phase_name = timestamp.kind.get_phase_name();
            let state_value = timestamp.kind.to_prometheus_value();
            let event_type = if timestamp.kind.is_begin() {
                "begin"
            } else {
                "end"
            };

            // Create a metric for each timestamp event
            result.push_str(&format!(
                "kvcache_operation_event{{client=\"{}\",phase=\"{}\",event=\"{}\",key=\"{}\",op_id=\"{}\"}} {} {}\n",
                client_id,
                phase_name,
                event_type,
                timestamp.key_opt.as_deref().unwrap_or("unknown"),
                timestamp.ope_id_opt.as_deref().unwrap_or("unknown"),
                state_value,
                timestamp.time
            ));
        }

        result
    }

    /// Get the most recent timestamp for this metric type
    pub fn get_latest_timestamp(&self) -> Option<&MetricTimestamp> {
        self.timestamps.iter().max_by_key(|ts| ts.time)
    }

    /// Get operation timeline grouped by operation ID
    pub fn get_operation_timeline(
        &self,
    ) -> std::collections::HashMap<String, Vec<&MetricTimestamp>> {
        let mut timeline = std::collections::HashMap::new();

        for ts in &self.timestamps {
            if let Some(op_id) = &ts.ope_id_opt {
                timeline
                    .entry(op_id.clone())
                    .or_insert_with(Vec::new)
                    .push(ts);
            }
        }

        // Sort each operation's timeline by timestamp
        for events in timeline.values_mut() {
            events.sort_by_key(|ts| ts.time);
        }

        timeline
    }

    /// Generate timeline events for Grafana visualization
    pub fn to_prometheus_timeline_format(&self, client_id: &str) -> String {
        let mut result = String::new();
        let timeline = self.get_operation_timeline();

        for (op_id, events) in timeline {
            for event in events {
                let phase_name = event.kind.get_phase_name();
                let state_value = event.kind.to_prometheus_value();
                let event_type = if event.kind.is_begin() {
                    "begin"
                } else {
                    "end"
                };

                result.push_str(&format!(
                    "kvcache_operation_timeline{{client=\"{}\",op_id=\"{}\",phase=\"{}\",event=\"{}\",key=\"{}\"}} {} {}\n",
                    client_id,
                    op_id,
                    phase_name,
                    event_type,
                    event.key_opt.as_deref().unwrap_or("unknown"),
                    state_value,
                    event.time
                ));
            }
        }

        result
    }
}

impl ClientKvApiInner {
    pub fn get_holding_len(&self) -> usize {
        self.external_get_holding.total()
    }
    pub fn get_cache_len(&self) -> usize {
        self.get_cached_info.len()
    }
    fn metrics_handle(&self) -> Arc<MetricsHandle> {
        self.metrics
            .get()
            .cloned()
            .expect("metrics handle not initialized")
    }

    fn client_id_str(&self) -> String {
        self.view.cluster_manager().get_self_info().id.to_string()
    }

    fn node_role(&self) -> crate::cluster_manager::NodeRole {
        let member = self.view.cluster_manager().get_self_info();
        member.node_role()
    }

    /// Drain pending metric events, compute aggregates and update snapshot.
    pub fn drain_and_compute_metrics(&self) -> std::collections::HashMap<String, MetricsSet> {
        let mut results = std::collections::HashMap::new();

        // Helper to compute avg, p99, p95, min, max and collect timestamps
        let compute = |data: &mut Vec<i64>, timestamps: Vec<MetricTimestamp>| -> MetricsSet {
            if data.is_empty() {
                return MetricsSet {
                    mean: 0.0,
                    p99: 0,
                    p95: 0,
                    min: 0,
                    max: 0,
                    timestamps, // ✅ 保留timestamps，即使没有延迟数据也要上报时间节点
                };
            }
            data.sort_unstable();
            let len = data.len();
            let sum: i64 = data.iter().sum();
            let avg = sum as f64 / len as f64;
            let idx99 = ((len * 99 + 99) / 100).saturating_sub(1);
            let idx95 = ((len * 95 + 99) / 100).saturating_sub(1);
            let p99 = data[idx99.min(len - 1)];
            let p95 = data[idx95.min(len - 1)];
            let min = data[0];
            let max = data[len - 1];
            MetricsSet {
                mean: avg,
                p99,
                p95,
                min,
                max,
                timestamps,
            }
        };

        let metrics_handle = self.metrics_handle();

        // Drain put metrics
        let mut put_whole = Vec::new();
        let mut put_start = Vec::new();
        let mut put_transfer = Vec::new();
        let mut put_end = Vec::new();
        let mut put_rpc = Vec::new();
        let mut put_start_handle = Vec::new();
        let mut put_end_handle = Vec::new();
        let mut put_whole_timestamps = Vec::new();
        let mut put_start_timestamps = Vec::new();
        let mut put_transfer_timestamps = Vec::new();
        let mut put_end_timestamps = Vec::new();
        let mut put_rpc_timestamps = Vec::new();

        for m in metrics_handle.drain_put_metrics() {
            if let KvMetrics::Put {
                whole_put,
                start,
                transfer,
                end,
                rpc_of_put_start,
                start_handle,
                end_handle,
                key,
                put_id,
                start_timestamp_us,
                transfer_start_timestamp_us,
                end_start_timestamp_us,
                end_timestamp_us,
                ..
            } = m
            {
                if whole_put > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Total,
                        whole_put as f64 / 1_000_000.0,
                    );
                }
                if start > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Start,
                        start as f64 / 1_000_000.0,
                    );
                }
                if transfer > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Transfer,
                        transfer as f64 / 1_000_000.0,
                    );
                }
                if end > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::End,
                        end as f64 / 1_000_000.0,
                    );
                }
                if rpc_of_put_start > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Rpc,
                        rpc_of_put_start as f64 / 1_000_000.0,
                    );
                }
                // ✅ 使用源头时间戳，转换为毫秒
                let t1_ms = start_timestamp_us / 1000; // 操作开始
                let t2_ms = transfer_start_timestamp_us / 1000; // start结束/transfer开始
                let t3_ms = end_start_timestamp_us / 1000; // transfer结束/end开始
                let t4_ms = end_timestamp_us / 1000; // 操作结束

                put_whole.push(whole_put);
                put_start.push(start);
                put_transfer.push(transfer);
                put_end.push(end);
                put_rpc.push(rpc_of_put_start);
                if start_handle > 0 {
                    put_start_handle.push(start_handle);
                }
                if end_handle > 0 {
                    put_end_handle.push(end_handle);
                }

                // 使用真实的源头时间戳生成各阶段的Begin/End事件
                // Put Whole phase: t1 -> t4
                put_whole_timestamps.push(MetricTimestamp {
                    time: t1_ms, // Begin time - 真实源头时间戳
                    kind: MetricTimestampKind::PutWholeBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_whole_timestamps.push(MetricTimestamp {
                    time: t4_ms, // End time - 真实源头时间戳
                    kind: MetricTimestampKind::PutWholeEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put Start phase: t1 -> t2
                put_start_timestamps.push(MetricTimestamp {
                    time: t1_ms, // 真实的start开始时间
                    kind: MetricTimestampKind::PutStartBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_start_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的start结束时间
                    kind: MetricTimestampKind::PutStartEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put Transfer phase: t2 -> t3
                put_transfer_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的transfer开始时间
                    kind: MetricTimestampKind::PutTransferBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_transfer_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的transfer结束时间
                    kind: MetricTimestampKind::PutTransferEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put End phase: t3 -> t4
                put_end_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的end开始时间
                    kind: MetricTimestampKind::PutEndBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_end_timestamps.push(MetricTimestamp {
                    time: t4_ms, // 真实的end结束时间
                    kind: MetricTimestampKind::PutEndEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put RPC phase: 通常与start阶段重合 t1 -> t2
                put_rpc_timestamps.push(MetricTimestamp {
                    time: t1_ms, // RPC开始时间
                    kind: MetricTimestampKind::PutRpcBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_rpc_timestamps.push(MetricTimestamp {
                    time: t2_ms, // RPC结束时间 (大概在start阶段结束)
                    kind: MetricTimestampKind::PutRpcEnd,
                    key_opt: Some(key),
                    ope_id_opt: Some(put_id),
                });
            }
        }
        results.insert(
            "put_whole".to_string(),
            compute(&mut put_whole, put_whole_timestamps),
        );
        results.insert(
            "put_start".to_string(),
            compute(&mut put_start, put_start_timestamps),
        );
        results.insert(
            "put_transfer".to_string(),
            compute(&mut put_transfer, put_transfer_timestamps),
        );
        results.insert(
            "put_end".to_string(),
            compute(&mut put_end, put_end_timestamps),
        );
        results.insert(
            "put_rpc".to_string(),
            compute(&mut put_rpc, put_rpc_timestamps),
        );
        results.insert(
            "put_start_handle".to_string(),
            compute(&mut put_start_handle, vec![]),
        );
        results.insert(
            "put_end_handle".to_string(),
            compute(&mut put_end_handle, vec![]),
        );

        // Drain get metrics
        let mut get_whole = Vec::new();
        let mut get_start = Vec::new();
        let mut get_transfer = Vec::new();
        let mut get_end = Vec::new();
        let mut get_start_handle = Vec::new();
        let mut get_end_handle = Vec::new();
        let mut get_whole_timestamps = Vec::new();
        let mut get_start_timestamps = Vec::new();
        let mut get_transfer_timestamps = Vec::new();
        let mut get_end_timestamps = Vec::new();

        for m in metrics_handle.drain_get_metrics() {
            if let KvMetrics::Get {
                whole_get,
                start,
                transfer,
                end,
                start_handle,
                end_handle,
                key,
                get_id,
                start_timestamp_us,
                transfer_start_timestamp_us,
                end_start_timestamp_us,
                end_timestamp_us,
            } = m
            {
                if whole_get > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::Total,
                        whole_get as f64 / 1_000_000.0,
                    );
                }
                if start > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::Start,
                        start as f64 / 1_000_000.0,
                    );
                }
                if transfer > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::Transfer,
                        transfer as f64 / 1_000_000.0,
                    );
                }
                if end > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::End,
                        end as f64 / 1_000_000.0,
                    );
                }
                // ✅ 使用源头时间戳，转换为毫秒
                let t1_ms = start_timestamp_us / 1000; // 操作开始
                let t2_ms = transfer_start_timestamp_us / 1000; // start结束/transfer开始
                let t3_ms = end_start_timestamp_us / 1000; // transfer结束/end开始
                let t4_ms = end_timestamp_us / 1000; // 操作结束

                get_whole.push(whole_get);
                get_start.push(start);
                get_transfer.push(transfer);
                get_end.push(end);
                if start_handle > 0 {
                    get_start_handle.push(start_handle);
                }
                if end_handle > 0 {
                    get_end_handle.push(end_handle);
                }

                // 使用真实的源头时间戳生成各阶段的Begin/End事件
                // Get Whole phase: t1 -> t4
                get_whole_timestamps.push(MetricTimestamp {
                    time: t1_ms, // Begin time - 真实源头时间戳
                    kind: MetricTimestampKind::GetWholeBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_whole_timestamps.push(MetricTimestamp {
                    time: t4_ms, // End time - 真实源头时间戳
                    kind: MetricTimestampKind::GetWholeEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });

                // Get Start phase: t1 -> t2
                get_start_timestamps.push(MetricTimestamp {
                    time: t1_ms, // 真实的start开始时间
                    kind: MetricTimestampKind::GetStartBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_start_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的start结束时间
                    kind: MetricTimestampKind::GetStartEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });

                // Get Transfer phase: t2 -> t3
                get_transfer_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的transfer开始时间
                    kind: MetricTimestampKind::GetTransferBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_transfer_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的transfer结束时间
                    kind: MetricTimestampKind::GetTransferEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });

                // Get End phase: t3 -> t4
                get_end_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的end开始时间
                    kind: MetricTimestampKind::GetEndBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_end_timestamps.push(MetricTimestamp {
                    time: t4_ms, // 真实的end结束时间
                    kind: MetricTimestampKind::GetEndEnd,
                    key_opt: Some(key),
                    ope_id_opt: Some(get_id),
                });
            }
        }
        results.insert(
            "get_whole".to_string(),
            compute(&mut get_whole, get_whole_timestamps),
        );
        results.insert(
            "get_start".to_string(),
            compute(&mut get_start, get_start_timestamps),
        );
        results.insert(
            "get_transfer".to_string(),
            compute(&mut get_transfer, get_transfer_timestamps),
        );
        results.insert(
            "get_end".to_string(),
            compute(&mut get_end, get_end_timestamps),
        );
        results.insert(
            "get_start_handle".to_string(),
            compute(&mut get_start_handle, vec![]),
        );
        results.insert(
            "get_end_handle".to_string(),
            compute(&mut get_end_handle, vec![]),
        );

        // Update in MetricsHandle for non-draining readers
        let metrics_handle = self.metrics_handle();
        metrics_handle.set_latest_metrics_snapshot(results.clone());

        results
    }

    /// Returns a shared `Arc<AllMemholderRefCount>`, creating and storing its `Weak` in
    /// `all_memholder_refcount` if absent. All created `UserMemHolder`s share the same
    /// refcount tracker to coordinate drop lifecycle.
    pub fn get_or_init_all_memholder_refcount(&self) -> Arc<AllMemholderRefCount> {
        // Check if the OnceLock already contains a value
        if let Some(existing) = self.all_memholder_refcount.get() {
            if let Some(upgraded) = existing.upgrade() {
                return upgraded;
            }
        }

        // Create a new Arc<AllMemholderRefCount> and store its Weak reference in the OnceLock
        let new_ref = Arc::new(AllMemholderRefCount::new(self.view.clone_view()));
        let weak_ref = Arc::downgrade(&new_ref);
        if self.all_memholder_refcount.set(weak_ref).is_err() {
            // If setting the OnceLock fails, retrieve the existing value
            if let Some(existing) = self.all_memholder_refcount.get() {
                if let Some(upgraded) = existing.upgrade() {
                    return upgraded;
                }
            }
        }

        new_ref
    }
}
impl ClientKvApi {
    pub fn inner(&self) -> &ClientKvApiInner {
        &self.0
    }

    pub fn attach_view(&self, view: ClientKvApiView) {
        self.0.view.attach(view);
    }

    pub async fn construct(arg: ClientKvApiNewArg) -> Result<Self, KvError> {
        tracing::info!("Constructing ClientKvApi in Client mode (PreView)");

        let inner = ClientKvApiInner {
            view: ClientKvApiViewHolder::new(),
            test_spec_config: arg.test_spec_config,
            metrics: OnceLock::new(),
            all_memholder_refcount: OnceLock::new(),
            get_remote_kv_lock: AMapLock::new(Duration::from_secs(60)),
            get_cached_info: DashMap::new(),
            external_invalidate_delete: EnsureMemholderMgmtDeleteHandle::new(
                OwnerExternalMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            delete_ack_batch: EnsureMemholderMgmtDeleteHandle::new(
                OwnerDeleteAckMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            owner_delete_ack_mgr: OwnerDeleteAckMemMgr::default(),
            external_get_holding: OwnerExternalMemMgr::default(),
            external_pending_puts: moka::sync::Cache::builder()
                .time_to_live(Duration::from_secs(30 * 60))
                .segments(16)
                .build(),
            #[cfg(test)]
            test_record: crate::client_kv_api::client_test_record::ClientTestRecord::new(),
            rpc_caller_get_start: RPCCaller::new(),
            rpc_caller_get_revoke: RPCCaller::new(),
            rpc_caller_get_done: RPCCaller::new(),
            rpc_caller_put_start: RPCCaller::new(),
            rpc_caller_put_revoke: RPCCaller::new(),
            rpc_caller_put_done: RPCCaller::new(),
            rpc_caller_delete: RPCCaller::new(),
            rpc_caller_batch_delete_ack: RPCCaller::new(),
            rpc_caller_get_meta: RPCCaller::new(),
            _rpc_caller_allocate_client_lease: RPCCaller::new(),
            _rpc_caller_client_lease_keepalive: RPCCaller::new(),
            rpc_caller_external_put_commit: RPCCaller::new(),
            rpc_caller_external_put_revoke: RPCCaller::new(),
            rpc_caller_resolve_side_transfer_lane: RPCCaller::new(),
            default_lease_id: parking_lot::RwLock::new(None),
        };
        Ok(Self(inner))
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), KvError> {
        let inner = &self.0;

        let metrics_arc = inner.view.metric_reporter().metrics();
        if inner.metrics.set(metrics_arc.clone()).is_err() {
            tracing::warn!("metrics handle already initialized for ClientKvApi");
        }

        inner.rpc_caller_get_start.regist(inner.view.p2p_module());
        inner.rpc_caller_get_revoke.regist(inner.view.p2p_module());
        inner.rpc_caller_get_done.regist(inner.view.p2p_module());
        inner.rpc_caller_put_start.regist(inner.view.p2p_module());
        inner.rpc_caller_put_revoke.regist(inner.view.p2p_module());
        inner.rpc_caller_put_done.regist(inner.view.p2p_module());
        inner.rpc_caller_delete.regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_delete_ack
            .regist(inner.view.p2p_module());
        inner.rpc_caller_get_meta.regist(inner.view.p2p_module());
        inner
            .rpc_caller_external_put_commit
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_external_put_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_resolve_side_transfer_lane
            .regist(inner.view.p2p_module());
        crate::key_prefix::init_for_p2p_owner(inner.view.p2p_module());
        crate::kvlease::init_for_p2p_owner(inner.view.p2p_module());
        // Register master-only metric RPC callers
        crate::metrics::client::init_for_p2p_owner(inner.view.p2p_module());
        RPCCaller::<BatchDeleteAckReq>::new().regist(inner.view.p2p_module());
        RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new().regist(inner.view.p2p_module());

        // External RPC handlers
        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalGetReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_external_get", async move {
                let result = handle_external_get(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutStartReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_external_put_start", async move {
                let req = msg.serialize_part.clone();
                tracing::info!(
                    "rpc_external_put_start received: self={} peer={} task_id={} key={} len={} started_time={}",
                    view_task.cluster_manager().get_self_info().id,
                    resp.node_id(),
                    resp.task_id(),
                    req.key,
                    req.len,
                    req.started_time
                );
                let result = handle_external_put_start(&view_task, &msg).await;
                if let Err(err) = resp.send_resp(result).await {
                    tracing::warn!(
                        "rpc_external_put_start send_resp failed: self={} peer={} task_id={} key={} err={:?}",
                        view_task.cluster_manager().get_self_info().id,
                        resp.node_id(),
                        resp.task_id(),
                        req.key,
                        err
                    );
                } else {
                    tracing::info!(
                        "rpc_external_put_start response sent: self={} peer={} task_id={} key={}",
                        view_task.cluster_manager().get_self_info().id,
                        resp.node_id(),
                        resp.task_id(),
                        req.key
                    );
                }
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutTransferEndReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_external_put_transfer_end", async move {
                    let result = handle_external_put_transfer_end(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutCommitReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_external_put_commit", async move {
                    let result = handle_external_put_commit(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutRevokeReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_external_put_revoke", async move {
                    let result = handle_external_put_revoke(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalDeleteReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_external_delete", async move {
                let result = handle_external_delete(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalIsExistReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_external_is_exist", async move {
                    let result = handle_external_is_exist(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalDeleteAckReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_external_delete_ack", async move {
                    let result = handle_external_delete_ack(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        // KV->file sync RPC (bytes field -> file@offset)
        RPCCaller::<SyncKvToFileReq>::new().regist(inner.view.p2p_module());
        let view_ext = inner.view.clone_view();
        RPCHandler::<SyncKvToFileReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_sync_kv_to_file", async move {
                let result = handle_sync_kv_to_file_client(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        // client rpc handler register
        let view = inner.view.clone_view();
        RPCHandler::<BatchDeleteClientKvMetaCacheReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let req_node_id = resp.node_id().clone();
                let view = view.clone();
                let view_task = view.clone();
                let _ = view.spawn("rpc_batch_delete_client_kv_meta_cache", async move {
                    let ack =
                        handle_batch_delete_client_kv_meta_cache(&view_task, msg, req_node_id)
                            .await;
                    if let Err(e) = resp.send_resp(ack).await {
                        warn!("Failed to send BatchDeleteClientKvMetaCacheResp: {:?}", e);
                    }
                });
                Ok(())
            },
        );

        let external_invalidate_delete_rx = inner
            .external_invalidate_delete
            .take_rx()
            .expect("external_invalidate_delete rx already taken, that's impossible");
        delete::spawn_external_invalidate_delete(
            inner.view.clone_view(),
            external_invalidate_delete_rx,
        );

        let delete_ack_batch_rx = inner
            .delete_ack_batch
            .take_rx()
            .expect("delete_ack_batch rx already taken, that's impossible");
        delete::spawn_owner_delete_ack_batch(inner.view.clone_view(), delete_ack_batch_rx);

        // Spawn cluster listener to clean up get_holding when external_client leaves
        let view = inner.view.clone_view();
        let view2 = view.clone();
        let view_task = view2.clone();
        let _ = view.spawn("client_cluster_listener", async move {
            let mut listen_cluster_event = view_task.cluster_manager().listen();
            let mut shutdown_waiter = view_task.register_shutdown_waiter();

            loop {
                tokio::select! {
                    event = listen_cluster_event.recv() => {
                        match event {
                            Ok(event) => {
                                match event {
                                    ClusterEvent::MemberLeft(node_id) => {
                                        let removed = view_task
                                            .client_kv_api()
                                            .inner()
                                            .external_get_holding
                                            .cleanup_node(&node_id);
                                        if removed > 0 {
                                            tracing::info!(
                                                "Cleaned up get_holding for external_client: {} (removed {} holdings)",
                                                node_id, removed
                                            );
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to receive cluster event: {}", e);
                                break;
                            }
                        }
                    }
                    _ = shutdown_waiter.wait() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    pub fn can_be_dropped(&self) -> bool {
        // 如果没有初始化 refcount，返回 true
        if self.inner().all_memholder_refcount.get().is_none() {
            return true;
        }
        // 判断 AllMemholderRefCount 能否 upgrade
        if let Some(ref_weak) = self.inner().all_memholder_refcount.get() {
            if ref_weak.upgrade().is_none() {
                return true;
            }
        }
        false
    }

    /// Drain pending metric events and compute a fresh snapshot.
    pub fn drain_and_compute_metrics(&self) -> std::collections::HashMap<String, MetricsSet> {
        self.inner().drain_and_compute_metrics()
    }

    pub fn client_id(&self) -> NodeIDString {
        self.inner().view.cluster_manager().get_self_info().id
    }

    // Removed thin wrappers: get/put/delete/is_exist/send_delete_ack; call via inner()

    /// Convenience wrapper: get KV
    pub async fn get(
        &self,
        key: &str,
    ) -> KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>> {
        self.inner().get(key).await
    }

    /// Convenience wrapper: put KV with optional lease_id
    /// NOTE: If `lease_id` is None, it MUST remain a pure non-lease put.
    ///       We do NOT fallback to any default lease here to avoid surprising behavior.
    pub async fn put(&self, key: &str, value: &[u8], lease_id: Option<u64>) -> KvResult<()> {
        let mut opts = PutOptionalArgs::new();
        // Only attach lease when caller explicitly provides it.
        if let Some(id) = lease_id {
            opts.0.push(PutOptionalArg::LeaseId(id));
        }
        self.inner().put(key, value, opts).await
    }

    /// Allocate a client lease with the given TTL seconds.
    ///
    /// Semantics:
    /// - `ttl_seconds` must be >= the master-side minimum client lease TTL
    ///   (see MasterLeaseManager::MIN_CLIENT_TTL_SECONDS).
    /// - Values smaller than this minimum (including 0) are invalid and will
    ///   cause `LeaseMgrError::InvalidTTL` to be returned from the master.
    pub async fn allocate_lease(&self, ttl_seconds: u64) -> KvResult<u64> {
        let inner = self.inner();
        let lease_id = crate::kvlease::allocate_lease(
            inner.view.p2p_module(),
            inner.view.cluster_manager(),
            ttl_seconds,
        )
        .await?;
        // store as default
        {
            let mut g = inner.default_lease_id.write();
            *g = Some(lease_id);
        }
        Ok(lease_id)
    }

    /// Keepalive a client lease using its existing TTL on the master.
    pub async fn keepalive_lease(&self, lease_id: u64) -> KvResult<()> {
        let inner = self.inner();
        crate::kvlease::keepalive_lease(
            inner.view.p2p_module(),
            inner.view.cluster_manager(),
            lease_id,
        )
        .await
    }

    /// Get current default lease id (set by allocate_lease)
    pub fn get_lease_id(&self) -> Option<u64> {
        self.inner().default_lease_id.read().clone()
    }

    #[cfg(test)]
    pub fn test_record(&self) -> &crate::client_kv_api::client_test_record::ClientTestRecord {
        &self.inner().test_record
    }

    #[cfg(test)]
    pub fn debug_cached_meta(&self) {
        tracing::info!("--- debug cached meta --------------------------------------");
        for entry in self.inner().get_cached_info.iter() {
            tracing::info!("- cached meta: {:?}", entry.value());
        }
        tracing::info!("------------------------------------------------------------");
    }

    #[cfg(any(test, feature = "test_bins"))]
    pub fn has_cached_key(&self, key: &str) -> bool {
        self.inner().get_cached_info.contains_key(key)
    }

    // Removed is_client_mode(): ClientKvApi is owner-only and always constructed.
}

#[async_trait]
impl LogicalModule for ClientKvApi {
    type View = ClientKvApiView;
    type NewArg = ClientKvApiNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "ClientKvApi"
    }

    fn attach_view(&self, view: Self::View) {
        ClientKvApi::attach_view(self, view);
    }

    async fn before_shutdown(&self) -> Result<(), Self::Error> {
        // High cohesion: handle KV client drop readiness here
        tracing::info!("ClientKvApi before_shutdown: waiting until safe to drop");
        loop {
            if self.can_be_dropped() {
                tracing::info!("ClientKvApi can be dropped");
                break;
            }
            tracing::info!(
                "ClientKvApi not ready to drop; retry in 3s (some user memholder may still be in use)"
            );
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), Self::Error> {
        tracing::info!("ClientKvApi shutting down...");
        tracing::info!(
            "ClientKvApi final: holding_len={} , cache_len={}",
            self.0.get_holding_len(),
            self.0.get_cache_len()
        );
        Ok(())
    }
}

impl ClientKvApiInner {
    #[cfg(any(test, feature = "test_bins"))]
    pub fn get_view(&self) -> &ClientKvApiView {
        &self.view
    }
}
