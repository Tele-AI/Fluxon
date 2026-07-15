use crate::client_kv_api::delete::handle_batch_delete_client_kv_meta_cache;
use crate::client_kv_api::local_reserve_rebalance::spawn_owner_local_reserve_rebalance_actor;
use crate::client_kv_api::msg_pack::{
    ExternalBatchGetCancelReq, ExternalBatchGetCancelResp, ExternalBatchGetReq,
    ExternalBatchGetResp, ExternalBatchGetStartReq, ExternalBatchGetStartResp,
    ExternalBatchGetTransferReq, ExternalBatchGetTransferResp, ExternalBatchIsExistReq,
    ExternalBatchIsExistResp, ExternalBatchPutCommitReq, ExternalBatchPutCommitResp,
    ExternalBatchPutStartReq, ExternalBatchPutStartResp, ExternalBatchPutTransferEndReq,
    ExternalBatchPutTransferEndResp, ExternalDeleteAckReq, ExternalDeleteAckResp,
    ExternalDeleteReq, ExternalDeleteResp, ExternalGetReq, ExternalGetResp, ExternalIsExistReq,
    ExternalIsExistResp, ExternalObservabilitySnapshotReq, ExternalObservabilitySnapshotResp,
    ExternalPutCommitReq, ExternalPutCommitResp, ExternalPutRevokeReq, ExternalPutRevokeResp,
    ExternalPutStartReq, ExternalPutStartResp, ExternalPutTransferEndReq,
    ExternalPutTransferEndResp, SyncKvToFileReq, SyncKvToFileResp, TestPutPhaseTrace,
};
use crate::client_kv_api::reclaim::handle_batch_owner_reclaim;
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::config::TestSpecConfig;
use crate::master_kv_router::msg_pack::{
    BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, BatchEnqueueReplicaTaskReq,
    BatchGetDoneReq, BatchGetRevokeReq, BatchGetStartItemResp, BatchGetStartReq, BatchIsExistReq,
    BatchOwnerReclaimReq, BatchPreparePutKeysReq, BatchPutDoneReq, BatchPutRevokeReq,
    BatchPutStartReq, BatchReleasePutKeyReservationsReq, CountPrefixReq,
    DeleteClientKvMetaCacheItem, GroupedBatchPutDoneReq,
};
use crate::master_lease_manager::msg_pack::{AllocateClientLeaseReq, ClientLeaseKeepaliveReq};
use crate::memholder::{AllMemholderRefCount, ExternalMemHolderInfo, MemoryInfo, UserMemHolder};
use crate::memholder::{
    EnsureMemholderMgmtDeleteHandle, MemholderManagerTrait, NodeHolderKey, OwnerDeleteAckItem,
    OwnerDeleteAckMemMgr, OwnerExternalMemMgr,
};
use crate::{
    client_seg_pool::{ClientSegPool, ClientSegPoolAccessTrait, ResolveSideTransferLaneReq},
    client_transfer_engine::{ClientTransferEngine, ClientTransferEngineAccessTrait},
    cluster_manager::{
        ClusterEvent, ClusterManager, ClusterManagerAccessTrait,
        app_logic_ext::{ClusterManagerAppLogicExt, ClusterManagerExtError},
    },
    master_kv_router::msg_pack::{
        DeleteAckReq, DeleteReq, GetDoneReq, GetMetaReq, GetRevokeReq, GetStartReq,
        MemHolderKeepAliveReq, PutAppendDoneReq, PutAppendRevokeReq, PutAppendStartReq, PutDoneReq,
        PutRevokeReq, PutStartReq, ReleaseLocalGrantReq, ReserveLocalGrantReq,
    },
    metric_reporter::{MetricReporter, MetricReporterAccessTrait},
    metrics::{KvLocalitySnapshot, MetricsHandle, OperationKind, RequestStage},
    p2p::{
        msg_pack::{RPCCaller, RPCHandler},
        p2p_module::{P2pModule, P2pModuleAccessTrait},
    },
    rpcresp_kvresult_convert::msg_and_error::{ApiError, ErrorCode, KvError, KvResult},
};
use async_trait::async_trait;
use crossbeam::queue::SegQueue;
use dashmap::{DashMap, DashSet};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard;
use fluxon_util::map_lock::AMapLock;
use limit_thirdparty::tokio;
use moka::notification::RemovalCause;
use moka::ops::compute::Op as MokaComputeOp;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Weak;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

const REPLICA_TASK_QUEUE_CAPACITY: usize = 128;
// Queue only weak source references. This preserves a deep policy backlog without turning queued
// write-back work into non-reclaimable owner memory; the dispatcher pins a still-current source
// immediately before handing it to the bounded replica-task actor.
const OWNER_HOT_EVICTION_QUEUE_CAPACITY: usize = 4096;
const OWNER_LOCAL_PUBLISH_QUEUE_CAPACITY: usize = 4096;
const OWNER_LOCAL_PUBLISH_MAX_INFLIGHT: usize = 64;

/// Information about a memholder held by external client
#[derive(Clone)]
pub struct ExternalHoldingGetInfo {
    pub key: String,
    pub req_node_id: String,
    pub memory_info: Arc<MemoryInfo>, // The actual memholder being held
}

#[derive(Clone, Debug, Default)]
pub struct OwnerRuntimeObserveSnapshot {
    pub external_get_holding_entries: u64,
    pub external_get_holding_bytes: u64,
    pub external_pending_put_entries: u64,
    pub hot_cache_capacity_bytes: u64,
    pub hot_cache_entries: u64,
    pub hot_cache_weighted_bytes: u64,
    pub hot_size_evictions: u64,
    pub hot_replica_enqueued: u64,
    pub hot_replica_completed: u64,
    pub hot_replica_already_satisfied: u64,
    pub hot_replica_failed: u64,
    pub hot_replica_obsolete: u64,
    pub hot_replica_dispatch_failed: u64,
    pub hot_replica_inflight: u64,
    pub hot_eviction_skipped_stale: u64,
    pub hot_eviction_skipped_reclaim: u64,
    pub hot_eviction_skipped_duplicate: u64,
    pub hot_group_registry_entries: u64,
    pub hot_group_replica_triggered: u64,
    pub hot_group_replica_triggers: u64,
    pub hot_group_members_enqueued: u64,
    pub hot_group_trigger_duplicates: u64,
    pub hot_group_trigger_incomplete: u64,
    pub grouped_put_done_batches: u64,
    pub grouped_put_done_items: u64,
    pub legacy_put_done_batches: u64,
    pub legacy_put_done_items: u64,
}

pub use get::RemoteGetInfo;
pub use put::{OwnerLocalPublishItem, OwnerLocalPublishJob, OwnerReservedPutItem};
pub mod external_api;
mod local_reserve_rebalance;
mod reclaim;
pub use external_api::HandlerForExternalClient;
pub type TestObservePutPhaseSink = Arc<Mutex<Option<TestPutPhaseTrace>>>;
pub type ExternalGetStartTransferOutput =
    Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>;

pub enum ExternalGetStartOwnerItem {
    Local {
        memory_info: Arc<MemoryInfo>,
    },
    Started {
        key: String,
        item: BatchGetStartItemResp,
    },
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ExternalGetStartDedupKey {
    pub keys: Vec<String>,
    pub atomic_group_lens: Vec<usize>,
    pub prefix_best_effort: bool,
}

#[derive(Clone, Debug)]
pub struct ExternalGetStartPrefixResult {
    pub raw_prefix_hit_len: usize,
    pub transferable_len: usize,
    pub first_miss_index: Option<usize>,
    pub first_error_kind: Option<String>,
}

#[derive(Clone)]
pub enum ExternalGetStartSharedItemResult {
    Hit {
        memholder: Arc<UserMemHolder>,
    },
    Miss,
    Error {
        error_code: ErrorCode,
        error_json: String,
    },
}

pub enum ExternalGetStartSharedPhase {
    Starting,
    Running {
        prefix: ExternalGetStartPrefixResult,
        keys: Vec<String>,
    },
    Ready {
        prefix: ExternalGetStartPrefixResult,
        keys: Vec<String>,
        items: Vec<ExternalGetStartSharedItemResult>,
    },
    Failed {
        error_code: ErrorCode,
        error_json: String,
    },
}

pub struct ExternalGetStartSharedState {
    pub waiter_count: usize,
    pub phase: ExternalGetStartSharedPhase,
}

pub struct ExternalGetStartSharedOp {
    pub dedup_key: ExternalGetStartDedupKey,
    pub transfer_concurrency: usize,
    /// Set only when every requested page is already owner-local.
    pub inline_local_memory_infos: OnceLock<Vec<Arc<MemoryInfo>>>,
    pub state: Mutex<ExternalGetStartSharedState>,
    pub notify: Arc<limit_thirdparty::tokio::sync::Notify>,
}

impl ExternalGetStartSharedOp {
    pub fn new(dedup_key: ExternalGetStartDedupKey, transfer_concurrency: usize) -> Self {
        Self {
            dedup_key,
            transfer_concurrency,
            inline_local_memory_infos: OnceLock::new(),
            state: Mutex::new(ExternalGetStartSharedState {
                waiter_count: 1,
                phase: ExternalGetStartSharedPhase::Starting,
            }),
            notify: Arc::new(limit_thirdparty::tokio::sync::Notify::new()),
        }
    }
}

pub struct ExternalGetStartEntry {
    pub req_node_id: String,
    pub shared_op: Arc<ExternalGetStartSharedOp>,
}

/// Optional arguments for put operations
#[derive(Clone, Debug)]
pub enum PutOptionalArg {
    /// Attach the written key to the specified lease on commit
    LeaseId(u64),
    /// Ask the master to fail-fast when the same key already has an inflight put.
    RejectIfInflightSameKey,
    /// Ask the master to fail-fast when the key already has a committed live replica.
    RejectIfExistSameKey,
    /// Disable asynchronous remote replica task after local commit in write-back mode.
    SkipMakeReplicaTask,
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
            | PutOptionalArg::RejectIfExistSameKey
            | PutOptionalArg::SkipMakeReplicaTask
            | PutOptionalArg::PreferredSubCluster(_)
            | PutOptionalArg::TestObservePutPhases(_) => None,
        })
    }

    pub fn reject_if_inflight_same_key(&self) -> bool {
        self.0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::RejectIfInflightSameKey))
    }

    pub fn reject_if_exist_same_key(&self) -> bool {
        self.0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::RejectIfExistSameKey))
    }

    pub fn make_replica_task(&self) -> bool {
        !self
            .0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::SkipMakeReplicaTask))
    }

    /// Get the last provided preferred_sub_cluster if any.
    pub fn preferred_sub_cluster(&self) -> Option<&str> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::PreferredSubCluster(sc) => Some(sc.as_str()),
            PutOptionalArg::LeaseId(_)
            | PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::RejectIfExistSameKey
            | PutOptionalArg::SkipMakeReplicaTask
            | PutOptionalArg::TestObservePutPhases(_) => None,
        })
    }

    pub fn test_observe_put_phases(&self) -> Option<TestObservePutPhaseSink> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::TestObservePutPhases(sink) => Some(sink.clone()),
            PutOptionalArg::LeaseId(_)
            | PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::RejectIfExistSameKey
            | PutOptionalArg::SkipMakeReplicaTask
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
        transfer_used_nixl: bool,
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

async fn handle_external_batch_get(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetReq>,
) -> MsgPack<ExternalBatchGetResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_get(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchGetResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_start(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetStartReq>,
) -> MsgPack<ExternalBatchGetStartResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_get_start(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_start error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_transfer(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetTransferReq>,
) -> MsgPack<ExternalBatchGetTransferResp> {
    let req = msg.serialize_part.clone();
    let dbg_handle = req.handle;
    let resp = view
        .client_kv_api()
        .external_batch_get_transfer(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_transfer error: {e}; handle={handle}",
                handle = dbg_handle
            );
            let mut r: ExternalBatchGetTransferResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_cancel(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetCancelReq>,
) -> MsgPack<ExternalBatchGetCancelResp> {
    let req = msg.serialize_part.clone();
    let dbg_handle = req.handle;
    let resp = view
        .client_kv_api()
        .external_batch_get_cancel(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_cancel error: {e}; handle={handle}",
                handle = dbg_handle
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
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

async fn handle_external_batch_put_start(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchPutStartReq>,
) -> MsgPack<ExternalBatchPutStartResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.items.len();
    let resp = view
        .client_kv_api()
        .external_batch_put_start(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_put_start error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchPutStartResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
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

async fn handle_external_batch_put_transfer_end(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchPutTransferEndReq>,
) -> MsgPack<ExternalBatchPutTransferEndResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.items.len();
    let resp = view
        .client_kv_api()
        .external_batch_put_transfer_end(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_put_transfer_end error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchPutTransferEndResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
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

async fn handle_external_batch_put_commit(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchPutCommitReq>,
) -> MsgPack<ExternalBatchPutCommitResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.items.len();
    let resp = view
        .client_kv_api()
        .external_batch_put_commit(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_put_commit error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchPutCommitResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
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

async fn handle_external_batch_is_exist(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchIsExistReq>,
) -> MsgPack<ExternalBatchIsExistResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_is_exist(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_is_exist error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchIsExistResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.exists_list = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_observability_snapshot(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalObservabilitySnapshotReq>,
) -> MsgPack<ExternalObservabilitySnapshotResp> {
    let req = msg.serialize_part.clone();
    let resp = view
        .client_kv_api()
        .external_observability_snapshot(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("handle_external_observability_snapshot error: {e}");
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
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
    /// Logical hot-tier capacity only. This does not resize the owner segment.
    pub owner_hot_cache_capacity_bytes: Option<u64>,
}

pub struct ClientKvApi(ClientKvApiInner);

#[derive(Debug)]
pub struct GetCachedInfo {
    put_time_ms: u64,
    put_version: u32,
    mem_holder: Arc<MemoryInfo>,
}

#[derive(Debug)]
pub struct PrecommitLocalVisibleInfo {
    mem_holder: Arc<MemoryInfo>,
}

#[derive(Debug)]
pub(crate) struct LocalSnapshotInfo {
    put_time_ms: u64,
    put_version: u32,
}

#[derive(Clone)]
struct OwnerHotCacheEntry {
    put_id: crate::master_kv_router::put::PutIDForAKey,
    memory_info: Weak<MemoryInfo>,
    weight_bytes: u32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct OwnerHotReplicaIdentity {
    key: String,
    put_time_ms: u64,
    put_version: u32,
}

impl OwnerHotReplicaIdentity {
    fn from_group_member(member: &crate::master_kv_router::msg_pack::PutAtomicGroupMember) -> Self {
        Self {
            key: member.key.clone(),
            put_time_ms: member.put_id.0,
            put_version: member.put_id.1,
        }
    }
}

#[derive(Default)]
struct OwnerHotCacheCounters {
    size_evictions: AtomicU64,
    replica_enqueued: AtomicU64,
    replica_completed: AtomicU64,
    replica_already_satisfied: AtomicU64,
    replica_failed: AtomicU64,
    replica_obsolete: AtomicU64,
    replica_dispatch_failed: AtomicU64,
    skipped_stale: AtomicU64,
    skipped_reclaim: AtomicU64,
    skipped_duplicate: AtomicU64,
    group_replica_triggers: AtomicU64,
    group_members_enqueued: AtomicU64,
    group_trigger_duplicates: AtomicU64,
    group_trigger_incomplete: AtomicU64,
    grouped_put_done_batches: AtomicU64,
    grouped_put_done_items: AtomicU64,
    legacy_put_done_batches: AtomicU64,
    legacy_put_done_items: AtomicU64,
}

const OWNER_HOT_REPLICA_OUTCOME_PENDING: u32 = 0;
const OWNER_HOT_REPLICA_OUTCOME_RECORDED: u32 = 1;

pub(crate) struct OwnerHotReplicaGuard {
    identity: OwnerHotReplicaIdentity,
    inflight: Arc<DashSet<OwnerHotReplicaIdentity>>,
    counters: Arc<OwnerHotCacheCounters>,
    group_trigger: Option<OwnerHotReplicaIdentity>,
    group_replica_triggered: Arc<DashSet<OwnerHotReplicaIdentity>>,
    outcome: AtomicU32,
}

impl OwnerHotReplicaGuard {
    fn new(
        identity: OwnerHotReplicaIdentity,
        inflight: Arc<DashSet<OwnerHotReplicaIdentity>>,
        counters: Arc<OwnerHotCacheCounters>,
        group_trigger: Option<OwnerHotReplicaIdentity>,
        group_replica_triggered: Arc<DashSet<OwnerHotReplicaIdentity>>,
    ) -> Self {
        Self {
            identity,
            inflight,
            counters,
            group_trigger,
            group_replica_triggered,
            outcome: AtomicU32::new(OWNER_HOT_REPLICA_OUTCOME_PENDING),
        }
    }

    fn release_group_trigger(&self) {
        if let Some(group_trigger) = self.group_trigger.as_ref() {
            self.group_replica_triggered.remove(group_trigger);
        }
    }

    pub(crate) fn mark_completed(&self) {
        if self
            .outcome
            .compare_exchange(
                OWNER_HOT_REPLICA_OUTCOME_PENDING,
                OWNER_HOT_REPLICA_OUTCOME_RECORDED,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.counters
                .replica_completed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn mark_already_satisfied(&self) {
        if self
            .outcome
            .compare_exchange(
                OWNER_HOT_REPLICA_OUTCOME_PENDING,
                OWNER_HOT_REPLICA_OUTCOME_RECORDED,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.counters
                .replica_already_satisfied
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn mark_obsolete(&self) {
        if self
            .outcome
            .compare_exchange(
                OWNER_HOT_REPLICA_OUTCOME_PENDING,
                OWNER_HOT_REPLICA_OUTCOME_RECORDED,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.counters
                .replica_obsolete
                .fetch_add(1, Ordering::Relaxed);
            self.release_group_trigger();
        }
    }

    fn mark_dispatch_failed(&self) {
        if self
            .outcome
            .compare_exchange(
                OWNER_HOT_REPLICA_OUTCOME_PENDING,
                OWNER_HOT_REPLICA_OUTCOME_RECORDED,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.counters
                .replica_dispatch_failed
                .fetch_add(1, Ordering::Relaxed);
            self.release_group_trigger();
        }
    }
}

impl Drop for OwnerHotReplicaGuard {
    fn drop(&mut self) {
        if self.outcome.load(Ordering::Relaxed) == OWNER_HOT_REPLICA_OUTCOME_PENDING {
            self.counters.replica_failed.fetch_add(1, Ordering::Relaxed);
            self.release_group_trigger();
        }
        self.inflight.remove(&self.identity);
    }
}

pub(crate) struct OwnerHotEvictionEvent {
    key: String,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    memory_info: Weak<MemoryInfo>,
    guard: OwnerHotReplicaGuard,
}

pub(crate) struct OwnerPreparedReclaim {
    item: crate::master_kv_router::msg_pack::OwnerReclaimItem,
    cached_info: GetCachedInfo,
    local_snapshot: Option<LocalSnapshotInfo>,
}

pub(crate) enum OwnerReclaimRecord {
    Prepared(OwnerPreparedReclaim),
    Committed(crate::master_kv_router::msg_pack::OwnerReclaimItem),
}

#[derive(Default)]
pub(crate) struct OwnerKeyControlState {
    local_puts: u32,
    reclaim: Option<OwnerReclaimRecord>,
}

fn resolve_local_visible_batch<T>(
    keys: &[String],
    controls: &HashMap<String, OwnerKeyControlState>,
    mut resolve_unfenced: impl FnMut(&str) -> Option<T>,
) -> Vec<Option<T>> {
    keys.iter()
        .map(|key| {
            if controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some())
            {
                None
            } else {
                resolve_unfenced(key)
            }
        })
        .collect()
}

fn allocate_external_holding_id(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .expect("external holding id space exhausted")
}

fn owner_hot_weight_bytes(memory_info: &MemoryInfo) -> u32 {
    let bytes = memory_info
        .local_reserve_resident_slot_ref()
        .map(|(slot_size, _, _)| slot_size)
        .unwrap_or(memory_info.len as u64);
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

fn clone_if_owner_hot_entry_matches<T>(
    current_put_id: crate::master_kv_router::put::PutIDForAKey,
    current: &Arc<T>,
    entry_put_id: crate::master_kv_router::put::PutIDForAKey,
    entry: &Weak<T>,
) -> Option<Arc<T>> {
    (current_put_id == entry_put_id && Weak::ptr_eq(entry, &Arc::downgrade(current)))
        .then(|| current.clone())
}

fn pin_current_owner_hot_source(
    key: &str,
    entry: &OwnerHotCacheEntry,
    owner_key_control: &Mutex<HashMap<String, OwnerKeyControlState>>,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    counters: &OwnerHotCacheCounters,
) -> Option<Arc<MemoryInfo>> {
    let controls = owner_key_control.lock();
    if controls
        .get(key)
        .is_some_and(|state| state.reclaim.is_some())
    {
        counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let Some(cached) = get_cached_info.get(key) else {
        counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
        return None;
    };
    let Some(pinned) = clone_if_owner_hot_entry_matches(
        (cached.put_time_ms, cached.put_version),
        &cached.mem_holder,
        entry.put_id,
        &entry.memory_info,
    ) else {
        counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
        return None;
    };

    // This clone is made under the same key fence used by reclaim. If reclaim
    // wins first the source is absent/fenced; if this clone wins, reclaim sees
    // the additional strong reference and returns Busy.
    Some(pinned)
}

fn owner_hot_group_anchor_identity(
    group: &crate::master_kv_router::msg_pack::PutAtomicGroup,
) -> Option<OwnerHotReplicaIdentity> {
    group
        .members
        .first()
        .map(OwnerHotReplicaIdentity::from_group_member)
}

fn register_owner_hot_atomic_group(
    atomic_groups: &DashMap<
        OwnerHotReplicaIdentity,
        Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    >,
    group: &crate::master_kv_router::msg_pack::PutAtomicGroup,
) {
    let Some(anchor) = owner_hot_group_anchor_identity(group) else {
        return;
    };
    let existing = atomic_groups
        .get(&anchor)
        .map(|entry| entry.value().clone());
    let group = match existing {
        Some(existing) if existing.as_ref() == group => existing,
        Some(_) => {
            tracing::warn!(
                "owner hot atomic-group anchor reused with different membership: key={} put_id=({},{})",
                anchor.key,
                anchor.put_time_ms,
                anchor.put_version
            );
            Arc::new(group.clone())
        }
        None => Arc::new(group.clone()),
    };
    for member in &group.members {
        atomic_groups.insert(
            OwnerHotReplicaIdentity::from_group_member(member),
            group.clone(),
        );
    }
}

fn forget_owner_hot_atomic_group(
    atomic_groups: &DashMap<
        OwnerHotReplicaIdentity,
        Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    >,
    group_replica_triggered: &DashSet<OwnerHotReplicaIdentity>,
    identity: &OwnerHotReplicaIdentity,
) {
    let group = atomic_groups
        .get(identity)
        .map(|entry| entry.value().clone());
    let Some(group) = group else {
        return;
    };
    if let Some(anchor) = owner_hot_group_anchor_identity(group.as_ref()) {
        group_replica_triggered.remove(&anchor);
    }
    for member in &group.members {
        atomic_groups.remove(&OwnerHotReplicaIdentity::from_group_member(member));
    }
}

fn pin_current_owner_hot_group_sources(
    group: &crate::master_kv_router::msg_pack::PutAtomicGroup,
    owner_key_control: &Mutex<HashMap<String, OwnerKeyControlState>>,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    counters: &OwnerHotCacheCounters,
) -> Option<Vec<(OwnerHotReplicaIdentity, Arc<MemoryInfo>)>> {
    let controls = owner_key_control.lock();
    let mut sources = Vec::with_capacity(group.members.len());
    for member in &group.members {
        if controls
            .get(&member.key)
            .is_some_and(|state| state.reclaim.is_some())
        {
            counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let Some(cached) = get_cached_info.get(&member.key) else {
            counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if (cached.put_time_ms, cached.put_version) != member.put_id {
            counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        sources.push((
            OwnerHotReplicaIdentity::from_group_member(member),
            cached.mem_holder.clone(),
        ));
    }
    Some(sources)
}

fn owner_hot_tp_cohort_key_rows(
    identity: &OwnerHotReplicaIdentity,
    group: Option<&crate::master_kv_router::msg_pack::PutAtomicGroup>,
) -> Result<Option<Vec<Vec<String>>>, ()> {
    let member_keys = group
        .map(|group| {
            group
                .members
                .iter()
                .map(|member| member.key.as_str())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![identity.key.as_str()]);
    let Some(first_key) = member_keys.first().copied() else {
        return Err(());
    };
    let first_tp = crate::master_kv_router::infer_sglang_tp_key(first_key);
    let Some(first_tp) = first_tp else {
        return member_keys
            .iter()
            .all(|key| crate::master_kv_router::infer_sglang_tp_key(key).is_none())
            .then_some(None)
            .ok_or(());
    };
    let mut logical_members = Vec::with_capacity(member_keys.len());
    for key in member_keys {
        let Some(member_tp) = crate::master_kv_router::infer_sglang_tp_key(key) else {
            return Err(());
        };
        if member_tp.rank != first_tp.rank || member_tp.size != first_tp.size {
            return Err(());
        }
        logical_members.push(member_tp.logical_prefix.to_string());
    }
    if first_tp.size == 1 {
        return Ok(None);
    }
    Ok(Some(
        (0..first_tp.size)
            .map(|rank| {
                logical_members
                    .iter()
                    .map(|logical_prefix| format!("{logical_prefix}_{rank}_{}", first_tp.size))
                    .collect::<Vec<_>>()
            })
            .collect(),
    ))
}

fn pin_current_owner_hot_tp_cohort_sources(
    cohort_key_rows: &[Vec<String>],
    expect_atomic_group: bool,
    owner_key_control: &Mutex<HashMap<String, OwnerKeyControlState>>,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    atomic_groups: &DashMap<
        OwnerHotReplicaIdentity,
        Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    >,
    counters: &OwnerHotCacheCounters,
) -> Option<Vec<(OwnerHotReplicaIdentity, Arc<MemoryInfo>)>> {
    let controls = owner_key_control.lock();
    let mut cohort_sources = Vec::new();
    for rank_keys in cohort_key_rows {
        if rank_keys.is_empty() {
            return None;
        }
        let mut rank_sources = Vec::with_capacity(rank_keys.len());
        for key in rank_keys {
            if controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some())
            {
                counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            let Some(cached) = get_cached_info.get(key) else {
                counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            rank_sources.push((
                OwnerHotReplicaIdentity {
                    key: key.clone(),
                    put_time_ms: cached.put_time_ms,
                    put_version: cached.put_version,
                },
                cached.mem_holder.clone(),
            ));
        }

        if expect_atomic_group {
            let Some(rank_group) = atomic_groups
                .get(&rank_sources[0].0)
                .map(|entry| entry.value().clone())
            else {
                return None;
            };
            if rank_group.members.len() != rank_sources.len()
                || rank_group.members.iter().zip(rank_sources.iter()).any(
                    |(member, (identity, _))| {
                        member.key != identity.key
                            || member.put_id != (identity.put_time_ms, identity.put_version)
                    },
                )
            {
                return None;
            }
            for (identity, _) in &rank_sources {
                if !atomic_groups
                    .get(identity)
                    .is_some_and(|entry| Arc::ptr_eq(entry.value(), &rank_group))
                {
                    return None;
                }
            }
        } else if rank_sources.len() != 1 || atomic_groups.contains_key(&rank_sources[0].0) {
            return None;
        }
        cohort_sources.extend(rank_sources);
    }
    Some(cohort_sources)
}

fn try_enqueue_owner_hot_replica_event(
    identity: OwnerHotReplicaIdentity,
    memory_info: Arc<MemoryInfo>,
    group_trigger: Option<OwnerHotReplicaIdentity>,
    inflight: &Arc<DashSet<OwnerHotReplicaIdentity>>,
    group_replica_triggered: &Arc<DashSet<OwnerHotReplicaIdentity>>,
    counters: &Arc<OwnerHotCacheCounters>,
    eviction_tx: &tokio::sync::ampsc::Sender<OwnerHotEvictionEvent>,
) -> bool {
    if !inflight.insert(identity.clone()) {
        counters.skipped_duplicate.fetch_add(1, Ordering::Relaxed);
        return false;
    }
    let event = OwnerHotEvictionEvent {
        key: identity.key.clone(),
        put_id: (identity.put_time_ms, identity.put_version),
        memory_info: Arc::downgrade(&memory_info),
        guard: OwnerHotReplicaGuard::new(
            identity,
            inflight.clone(),
            counters.clone(),
            group_trigger,
            group_replica_triggered.clone(),
        ),
    };
    match eviction_tx.try_send(event) {
        Ok(()) => {
            counters.replica_enqueued.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(err) => {
            let event = err.into_inner();
            tracing::warn!(
                "owner hot-cache eviction actor is closed; replica event dropped: key={} put_id=({},{})",
                event.key,
                event.put_id.0,
                event.put_id.1
            );
            event.guard.mark_dispatch_failed();
            false
        }
    }
}

fn build_owner_hot_cache(
    capacity_bytes: u64,
    owner_key_control: Arc<Mutex<HashMap<String, OwnerKeyControlState>>>,
    get_cached_info: Arc<DashMap<String, GetCachedInfo>>,
    inflight: Arc<DashSet<OwnerHotReplicaIdentity>>,
    atomic_groups: Arc<
        DashMap<OwnerHotReplicaIdentity, Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>>,
    >,
    group_replica_triggered: Arc<DashSet<OwnerHotReplicaIdentity>>,
    counters: Arc<OwnerHotCacheCounters>,
    eviction_tx: tokio::sync::ampsc::Sender<OwnerHotEvictionEvent>,
) -> moka::sync::Cache<String, OwnerHotCacheEntry> {
    assert!(
        capacity_bytes > 0,
        "owner hot-cache capacity must be positive"
    );
    moka::sync::Cache::builder()
        .max_capacity(capacity_bytes)
        .weigher(|_key: &String, entry: &OwnerHotCacheEntry| entry.weight_bytes)
        .eviction_listener(move |key, entry, cause| {
            if cause != RemovalCause::Size {
                return;
            }
            counters.size_evictions.fetch_add(1, Ordering::Relaxed);
            let Some(memory_info) = pin_current_owner_hot_source(
                key.as_str(),
                &entry,
                owner_key_control.as_ref(),
                get_cached_info.as_ref(),
                counters.as_ref(),
            ) else {
                return;
            };
            let identity = OwnerHotReplicaIdentity {
                key: (*key).clone(),
                put_time_ms: entry.put_id.0,
                put_version: entry.put_id.1,
            };
            let group = atomic_groups
                .get(&identity)
                .map(|entry| entry.value().clone());
            let cohort_sources = match owner_hot_tp_cohort_key_rows(&identity, group.as_deref()) {
                Ok(Some(cohort_key_rows)) => pin_current_owner_hot_tp_cohort_sources(
                    &cohort_key_rows,
                    group.is_some(),
                    owner_key_control.as_ref(),
                    get_cached_info.as_ref(),
                    atomic_groups.as_ref(),
                    counters.as_ref(),
                ),
                Ok(None) => match group.as_deref() {
                    Some(group) => pin_current_owner_hot_group_sources(
                        group,
                        owner_key_control.as_ref(),
                        get_cached_info.as_ref(),
                        counters.as_ref(),
                    ),
                    None => Some(vec![(identity.clone(), memory_info)]),
                },
                Err(()) => None,
            };
            let Some(cohort_sources) = cohort_sources else {
                counters
                    .group_trigger_incomplete
                    .fetch_add(1, Ordering::Relaxed);
                return;
            };
            if cohort_sources.len() > 1 {
                counters
                    .group_replica_triggers
                    .fetch_add(1, Ordering::Relaxed);
                let mut enqueued = 0u64;
                for (member_identity, member_memory_info) in cohort_sources {
                    if try_enqueue_owner_hot_replica_event(
                        member_identity,
                        member_memory_info,
                        None,
                        &inflight,
                        &group_replica_triggered,
                        &counters,
                        &eviction_tx,
                    ) {
                        enqueued = enqueued.saturating_add(1);
                    }
                }
                counters
                    .group_members_enqueued
                    .fetch_add(enqueued, Ordering::Relaxed);
                if enqueued == 0 {
                    counters
                        .group_trigger_duplicates
                        .fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
            let (identity, memory_info) = cohort_sources
                .into_iter()
                .next()
                .expect("owner hot cohort must contain its trigger source");
            let _ = try_enqueue_owner_hot_replica_event(
                identity,
                memory_info,
                None,
                &inflight,
                &group_replica_triggered,
                &counters,
                &eviction_tx,
            );
        })
        .build()
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

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
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
    get_cached_info: Arc<DashMap<String, GetCachedInfo>>,
    /// key -> locally readable resident slot before backend put_start/put_done finishes.
    precommit_local_visible_info: DashMap<String, PrecommitLocalVisibleInfo>,
    /// key -> local replica version remembered from local put/get durable-replica success.
    /// This authority is positive-only: hit means "can answer exists=true immediately when
    /// allow_local_snapshot is enabled"; miss does not imply non-existence.
    local_snapshot_info: DashMap<String, LocalSnapshotInfo>,
    /// KvOwner-managed resident staging grants for hostless put_start.
    owner_local_reserve_pool: Mutex<OwnerLocalReservePoolState>,
    /// Serialize reserve claims so a later waiter cannot steal slots reclaimed for the
    /// waiter currently assembling a lease. Tokio's mutex provides FIFO acquisition order.
    owner_local_reserve_claim_lock: limit_thirdparty::tokio::sync::AMutex<()>,
    /// Wake the background reserve actor after demand/free-slot changes.
    owner_local_reserve_rebalance_notify: Arc<limit_thirdparty::tokio::sync::Notify>,
    /// Owner-local write-back put ids for external local-first path.
    external_local_first_put_id_counter: AtomicU32,
    /// Atomic owner-side gate for local-first puts, local index access, and reclaim fencing.
    owner_key_control: Arc<Mutex<HashMap<String, OwnerKeyControlState>>>,
    /// A weak-value admission/recency tier. It never owns resident memory and
    /// therefore cannot become a second physical-reclaim authority.
    owner_hot_cache: Option<moka::sync::Cache<String, OwnerHotCacheEntry>>,
    owner_hot_replica_inflight: Arc<DashSet<OwnerHotReplicaIdentity>>,
    owner_hot_atomic_groups: Arc<
        DashMap<OwnerHotReplicaIdentity, Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>>,
    >,
    owner_hot_group_replica_triggered: Arc<DashSet<OwnerHotReplicaIdentity>>,
    owner_hot_counters: Arc<OwnerHotCacheCounters>,
    owner_hot_eviction_rx: Mutex<Option<tokio::sync::ampsc::Receiver<OwnerHotEvictionEvent>>>,

    /// Shared delete actor input for owner -> external weak-index invalidation.
    pub external_invalidate_delete: EnsureMemholderMgmtDeleteHandle<DeleteClientKvMetaCacheItem>,
    /// Shared delete actor input for owner -> master delete-ack batching.
    pub delete_ack_batch: EnsureMemholderMgmtDeleteHandle<OwnerDeleteAckItem>,
    /// Shared manager for owner -> master delete-ack batching.
    pub owner_delete_ack_mgr: OwnerDeleteAckMemMgr,

    // record external_client get_holding info (owned, flattened manager)
    pub external_get_holding: OwnerExternalMemMgr,
    pub external_get_start_registry: DashMap<u64, Arc<ExternalGetStartEntry>>,
    pub external_get_start_by_key: DashMap<ExternalGetStartDedupKey, Arc<ExternalGetStartSharedOp>>,
    next_external_get_start_handle: AtomicU64,
    /// External holding identities are independent from upstream and resident holder ids.
    next_external_holding_id: AtomicU64,
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
    rpc_caller_batch_get_start: RPCCaller<BatchGetStartReq>,
    rpc_caller_batch_get_revoke: RPCCaller<BatchGetRevokeReq>,
    rpc_caller_batch_get_done: RPCCaller<BatchGetDoneReq>,
    rpc_caller_put_start: RPCCaller<PutStartReq>,
    rpc_caller_put_revoke: RPCCaller<PutRevokeReq>,
    rpc_caller_put_done: RPCCaller<PutDoneReq>,
    rpc_caller_batch_put_start: RPCCaller<BatchPutStartReq>,
    rpc_caller_batch_put_revoke: RPCCaller<BatchPutRevokeReq>,
    rpc_caller_batch_put_done: RPCCaller<BatchPutDoneReq>,
    rpc_caller_grouped_batch_put_done: RPCCaller<GroupedBatchPutDoneReq>,
    rpc_caller_batch_prepare_put_keys: RPCCaller<BatchPreparePutKeysReq>,
    rpc_caller_batch_release_put_key_reservations: RPCCaller<BatchReleasePutKeyReservationsReq>,
    rpc_caller_put_append_start: RPCCaller<PutAppendStartReq>,
    rpc_caller_put_append_revoke: RPCCaller<PutAppendRevokeReq>,
    rpc_caller_put_append_done: RPCCaller<PutAppendDoneReq>,
    rpc_caller_reserve_local_grant: RPCCaller<ReserveLocalGrantReq>,
    rpc_caller_release_local_grant: RPCCaller<ReleaseLocalGrantReq>,
    rpc_caller_delete: RPCCaller<DeleteReq>,
    rpc_caller_batch_delete_ack: RPCCaller<BatchDeleteAckReq>,
    rpc_caller_batch_is_exist: RPCCaller<BatchIsExistReq>,
    rpc_caller_get_meta: RPCCaller<GetMetaReq>,
    rpc_caller_allocate_client_lease: RPCCaller<AllocateClientLeaseReq>,
    rpc_caller_client_lease_keepalive: RPCCaller<ClientLeaseKeepaliveReq>,
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
    owner_local_publish_tx: tokio::sync::ampsc::Sender<OwnerLocalPublishJob>,
    owner_local_publish_rx: Mutex<Option<tokio::sync::ampsc::Receiver<OwnerLocalPublishJob>>>,
    replica_task_tx: tokio::sync::ampsc::Sender<ReplicaTaskJob>,
    replica_task_rx: Mutex<Option<tokio::sync::ampsc::Receiver<ReplicaTaskJob>>>,
}

impl ClientKvApiInner {
    fn view(&self) -> &ClientKvApiView {
        &self.view
    }

    fn owner_hot_register_atomic_group(
        &self,
        group: &crate::master_kv_router::msg_pack::PutAtomicGroup,
    ) {
        register_owner_hot_atomic_group(self.owner_hot_atomic_groups.as_ref(), group);
    }

    fn owner_hot_forget_atomic_group_for_member(&self, identity: &OwnerHotReplicaIdentity) {
        forget_owner_hot_atomic_group(
            self.owner_hot_atomic_groups.as_ref(),
            self.owner_hot_group_replica_triggered.as_ref(),
            identity,
        );
    }

    fn owner_hot_track_committed(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: &Arc<MemoryInfo>,
        atomic_group: Option<&crate::master_kv_router::msg_pack::PutAtomicGroup>,
    ) {
        let Some(cache) = self.owner_hot_cache.as_ref() else {
            return;
        };
        if !self.owner_hot_source_is_current(key, put_id, memory_info) {
            return;
        }
        if let Some(group) = atomic_group {
            self.owner_hot_register_atomic_group(group);
        }
        cache.insert(
            key.to_string(),
            OwnerHotCacheEntry {
                put_id,
                memory_info: Arc::downgrade(memory_info),
                weight_bytes: owner_hot_weight_bytes(memory_info.as_ref()),
            },
        );
        if !self.owner_hot_source_is_current(key, put_id, memory_info) {
            self.owner_hot_invalidate_version(key, put_id);
        }
    }

    fn owner_hot_touch_or_promote(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: &Arc<MemoryInfo>,
    ) {
        let Some(cache) = self.owner_hot_cache.as_ref() else {
            return;
        };
        if cache.get(key).is_some_and(|entry| {
            entry.put_id == put_id && Weak::ptr_eq(&entry.memory_info, &Arc::downgrade(memory_info))
        }) {
            return;
        }
        self.owner_hot_track_committed(key, put_id, memory_info, None);
    }

    pub(crate) fn owner_hot_source_is_current(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: &Arc<MemoryInfo>,
    ) -> bool {
        let controls = self.owner_key_control.lock();
        if controls
            .get(key)
            .is_some_and(|state| state.reclaim.is_some())
        {
            return false;
        }
        self.get_cached_info.get(key).is_some_and(|cached| {
            cached.put_time_ms == put_id.0
                && cached.put_version == put_id.1
                && Arc::ptr_eq(&cached.mem_holder, memory_info)
        })
    }

    pub(crate) fn owner_hot_invalidate_version(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) {
        let Some(cache) = self.owner_hot_cache.as_ref() else {
            return;
        };
        let _ = cache.entry(key.to_string()).and_compute_with(|entry| {
            if entry
                .as_ref()
                .is_some_and(|entry| entry.value().put_id == put_id)
            {
                MokaComputeOp::Remove
            } else {
                MokaComputeOp::Nop
            }
        });
        self.owner_hot_forget_atomic_group_for_member(&OwnerHotReplicaIdentity {
            key: key.to_string(),
            put_time_ms: put_id.0,
            put_version: put_id.1,
        });
    }

    pub(crate) fn release_local_reserve_route_for_memory_info(&self, memory_info: &MemoryInfo) {
        let Some((slot_size, grant_id, slot_index)) = memory_info.local_reserve_resident_slot_ref()
        else {
            return;
        };
        if let Err(err) =
            self.owner_release_local_reserve_committed_slot_route(slot_size, grant_id, slot_index)
        {
            tracing::warn!(
                "failed to release local reserve committed slot route: key={} slot_size={} grant_id={} slot_index={} err={}",
                memory_info.key,
                slot_size,
                grant_id,
                slot_index,
                err
            );
        }
    }

    pub(crate) fn short_circuit_put_payload_path_enabled(&self) -> bool {
        self.test_spec_config.short_circuit_put_payload_path
    }

    pub(crate) fn skip_put_end_commit_enabled(&self) -> bool {
        self.test_spec_config.skip_put_end_commit
    }

    pub(crate) fn next_external_local_first_put_id(
        &self,
    ) -> crate::master_kv_router::put::PutIDForAKey {
        (
            now_unix_ms(),
            self.external_local_first_put_id_counter
                .fetch_add(1, Ordering::Relaxed),
        )
    }

    pub fn next_owner_local_first_put_id(&self) -> crate::master_kv_router::put::PutIDForAKey {
        self.next_external_local_first_put_id()
    }

    pub async fn enqueue_owner_local_publish(&self, job: OwnerLocalPublishJob) -> KvResult<()> {
        let key_count = job.items.len();
        let first_key = job
            .items
            .first()
            .map(|item| item.key.as_str())
            .unwrap_or("<empty>")
            .to_string();
        self.owner_local_publish_tx.try_send(job).map_err(|err| {
            KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner local publish queue is full or closed: first_key={} key_count={} err={}",
                    first_key, key_count, err
                ),
            })
        })
    }

    pub(crate) fn reserve_external_local_first_put_key(
        &self,
        key: &str,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
    ) -> KvResult<()> {
        let mut controls = self.owner_key_control.lock();
        if controls
            .get(key)
            .is_some_and(|state| state.reclaim.is_some())
        {
            return Err(KvError::Api(ApiError::KeyBeingWritten {
                key: key.to_string(),
            }));
        }
        if reject_if_exist_same_key
            && (self.precommit_local_visible_info.contains_key(key)
                || self.get_cached_info.contains_key(key)
                || self.local_snapshot_info.contains_key(key))
        {
            return Err(KvError::Api(ApiError::KeyAlreadyExists {
                key: key.to_string(),
            }));
        }
        let state = controls.entry(key.to_string()).or_default();
        if reject_if_inflight_same_key && state.local_puts > 0 {
            return Err(KvError::Api(ApiError::KeyBeingWritten {
                key: key.to_string(),
            }));
        }
        state.local_puts = state
            .local_puts
            .checked_add(1)
            .expect("owner local-first put counter overflow");
        Ok(())
    }

    pub(crate) fn release_external_local_first_put_key(&self, key: &str) {
        let mut controls = self.owner_key_control.lock();
        let remove = {
            let state = controls
                .get_mut(key)
                .expect("owner local-first put state missing on release");
            state.local_puts = state
                .local_puts
                .checked_sub(1)
                .expect("owner local-first put counter underflow");
            state.local_puts == 0 && state.reclaim.is_none()
        };
        if remove {
            controls.remove(key);
        }
    }

    pub(crate) fn remember_local_snapshot(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) {
        let controls = self.owner_key_control.lock();
        if controls
            .get(key)
            .is_some_and(|state| state.reclaim.is_some())
        {
            tracing::debug!(
                "skip local snapshot publication behind owner reclaim fence: key={} put_id=({},{})",
                key,
                put_id.0,
                put_id.1
            );
            return;
        }
        self.local_snapshot_info.insert(
            key.to_string(),
            LocalSnapshotInfo {
                put_time_ms: put_id.0,
                put_version: put_id.1,
            },
        );
    }

    pub(crate) fn has_local_snapshot(&self, key: &str) -> bool {
        let controls = self.owner_key_control.lock();
        if controls
            .get(key)
            .is_some_and(|state| state.reclaim.is_some())
        {
            return false;
        }
        self.precommit_local_visible_info.contains_key(key)
            || self.get_cached_info.contains_key(key)
            || self.local_snapshot_info.contains_key(key)
    }

    pub(crate) fn local_visible_mem_holder(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        let (memory_info, hot_put_id) = {
            let controls = self.owner_key_control.lock();
            if controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some())
            {
                return None;
            }
            let memory_info = self.local_visible_mem_holder_unfenced(key);
            let hot_put_id = memory_info.as_ref().and_then(|memory_info| {
                self.get_cached_info
                    .get(key)
                    .filter(|cached| Arc::ptr_eq(&cached.mem_holder, memory_info))
                    .map(|cached| (cached.put_time_ms, cached.put_version))
            });
            (memory_info, hot_put_id)
        };
        if let Some(put_id) = hot_put_id {
            self.owner_hot_touch_or_promote(
                key,
                put_id,
                memory_info
                    .as_ref()
                    .expect("hot touch requires a local memory holder"),
            );
        }
        memory_info
    }

    pub(crate) fn local_committed_mem_holder_for_put_id(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> Option<Arc<MemoryInfo>> {
        let controls = self.owner_key_control.lock();
        if controls
            .get(key)
            .is_some_and(|state| state.reclaim.is_some())
        {
            return None;
        }
        self.get_cached_info.get(key).and_then(|info| {
            (info.put_time_ms == put_id.0 && info.put_version == put_id.1)
                .then(|| info.mem_holder.clone())
        })
    }

    fn local_visible_mem_holder_unfenced(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        if let Some(info) = self.precommit_local_visible_info.get(key) {
            return Some(info.mem_holder.clone());
        }
        self.get_cached_info
            .get(key)
            .map(|info| info.mem_holder.clone())
    }

    pub(crate) fn local_visible_mem_holders(
        &self,
        keys: &[String],
    ) -> Vec<Option<Arc<MemoryInfo>>> {
        // A hostless get_start commonly checks hundreds of pages from one
        // local prefix. Take one coherent reclaim-fence snapshot instead of
        // locking owner_key_control once per page. The MemoryInfo Arcs keep
        // any selected backing alive if reclaim begins after this snapshot.
        let resolved = {
            let controls = self.owner_key_control.lock();
            resolve_local_visible_batch(keys, &controls, |key| {
                let memory_info = self.local_visible_mem_holder_unfenced(key)?;
                let hot_put_id = self
                    .get_cached_info
                    .get(key)
                    .filter(|cached| Arc::ptr_eq(&cached.mem_holder, &memory_info))
                    .map(|cached| (cached.put_time_ms, cached.put_version));
                Some((memory_info, hot_put_id))
            })
        };
        resolved
            .into_iter()
            .zip(keys)
            .map(|(resolved, key)| {
                let (memory_info, hot_put_id) = resolved?;
                if let Some(put_id) = hot_put_id {
                    self.owner_hot_touch_or_promote(key, put_id, &memory_info);
                }
                Some(memory_info)
            })
            .collect()
    }

    pub(crate) fn install_external_get_holding(
        &self,
        req_node_id: &str,
        memory_info: Arc<MemoryInfo>,
    ) -> ExternalMemHolderInfo {
        let external_holder_id = allocate_external_holding_id(&self.next_external_holding_id);
        let key = NodeHolderKey::new(req_node_id.to_string(), external_holder_id);
        let external_memholder_info = ExternalMemHolderInfo {
            offset: memory_info.offset,
            len: memory_info.len,
            holder_id: external_holder_id,
        };
        let previous = self.external_get_holding.inner().insert(
            key,
            ExternalHoldingGetInfo {
                key: memory_info.key.clone(),
                req_node_id: req_node_id.to_string(),
                memory_info,
            },
        );
        assert!(
            previous.is_none(),
            "fresh external holding id unexpectedly replaced a live holding"
        );
        external_memholder_info
    }

    pub async fn build_local_reserve_resident_memory_info(
        &self,
        key: &str,
        addr: u64,
        len: u32,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> Arc<MemoryInfo> {
        let resident_owner_node_id: NodeID = self.view.cluster_manager().get_self_info().id.into();
        Arc::new(
            MemoryInfo::new_local_reserve_resident(
                addr,
                len,
                key.to_string(),
                resident_owner_node_id,
                self.view.clone(),
                slot_size,
                grant_id,
                slot_index,
            )
            .await,
        )
    }

    pub async fn install_local_committed_memory_info(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        offset: u64,
        len: u32,
        holder_id: u64,
    ) -> KvResult<()> {
        let master_node_id: NodeID = self.view.cluster_manager().get_self_info().id.into();
        let memory_info = Arc::new(
            MemoryInfo::new(
                offset,
                len,
                holder_id,
                key.to_string(),
                master_node_id,
                self.view.clone(),
            )
            .await,
        );
        {
            let controls = self.owner_key_control.lock();
            if controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some())
            {
                return Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }));
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
        }
        self.owner_hot_track_committed(key, put_id, &memory_info, None);
        Ok(())
    }

    pub(crate) fn install_get_cached_info_if_unfenced(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: Arc<MemoryInfo>,
    ) -> bool {
        {
            let controls = self.owner_key_control.lock();
            if controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some())
            {
                tracing::debug!(
                    "skip get cache publication behind owner reclaim fence: key={} put_id=({},{})",
                    key,
                    put_id.0,
                    put_id.1
                );
                return false;
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
        }
        self.owner_hot_track_committed(key, put_id, &memory_info, None);
        true
    }

    pub fn install_precommit_local_visible_memory_info(
        &self,
        key: &str,
        memory_info: Arc<MemoryInfo>,
    ) {
        let controls = self.owner_key_control.lock();
        assert!(
            !controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some()),
            "precommit local index publication must not cross an owner reclaim fence"
        );
        let (slot_size, grant_id, slot_index) = memory_info
            .local_reserve_resident_slot_ref()
            .expect("resident memory_info must carry local reserve slot ref");
        self.owner_mark_local_reserve_slot_pending_visible(slot_size, grant_id, slot_index)
            .expect("marking local reserve slot pending visible must succeed");
        self.owner_retain_local_reserve_resident_slot_holder(slot_size, grant_id, slot_index)
            .expect("retaining local reserve resident holder must succeed");
        let replaced = self.precommit_local_visible_info.insert(
            key.to_string(),
            PrecommitLocalVisibleInfo {
                mem_holder: memory_info.clone(),
            },
        );
        assert!(
            replaced.is_none(),
            "precommit local visible cache must not be replaced for the same key"
        );
    }

    pub fn remove_precommit_local_reserve_resident_slot_if_same(
        &self,
        key: &str,
        expected_mem_holder: &Arc<MemoryInfo>,
    ) -> bool {
        let _controls = self.owner_key_control.lock();
        self.precommit_local_visible_info
            .remove_if(key, |_, info| {
                Arc::ptr_eq(&info.mem_holder, expected_mem_holder)
            })
            .is_some()
    }

    pub fn promote_precommit_local_reserve_resident_slot_if_same(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: Arc<MemoryInfo>,
        atomic_group: Option<&crate::master_kv_router::msg_pack::PutAtomicGroup>,
    ) -> KvResult<()> {
        {
            let controls = self.owner_key_control.lock();
            if controls
                .get(key)
                .is_some_and(|state| state.reclaim.is_some())
            {
                return Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }));
            }
            let is_same_pending = self
                .precommit_local_visible_info
                .get(key)
                .map(|info| Arc::ptr_eq(&info.mem_holder, &memory_info))
                .unwrap_or(false);
            if !is_same_pending {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "precommit local visible cache missing while promoting key={}",
                        key
                    ),
                }));
            }
            let (slot_size, grant_id, slot_index) = memory_info
                .local_reserve_resident_slot_ref()
                .expect("resident memory_info must carry local reserve slot ref");
            self.owner_promote_local_reserve_pending_slot_to_committed(
                slot_size, grant_id, slot_index,
            )?;
            let removed = self
                .precommit_local_visible_info
                .remove_if(key, |_, info| Arc::ptr_eq(&info.mem_holder, &memory_info))
                .is_some();
            if !removed {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "precommit local visible cache disappeared while promoting key={}",
                        key
                    ),
                }));
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
        }
        self.owner_hot_track_committed(key, put_id, &memory_info, atomic_group);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExternalPendingPutCtx {
    pub peer_id: Option<NodeIDString>,
    pub src_offset: u64,
    pub target_base_addr: u64,
    pub target_offset: u64,
    pub len: u64,
    pub make_replica_task: bool,
    pub preferred_sub_cluster: Option<String>,
    pub replica_target: Option<ReplicaTaskTarget>,
    pub local_reserve_slot: Option<OwnerLocalReserveSlotRef>,
    pub local_reserve_slot_size: Option<u64>,
    pub atomic_group: Option<crate::master_kv_router::msg_pack::PutAtomicGroup>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicaTaskTarget {
    pub node_id: String,
    pub target_offset: u64,
    pub target_base_addr: u64,
    pub len: u64,
}

pub(crate) struct ReplicaTaskJob {
    pub key: String,
    pub put_id: crate::master_kv_router::put::PutIDForAKey,
    pub holder: Arc<UserMemHolder>,
    pub target: Option<ReplicaTaskTarget>,
    pub preferred_sub_cluster: Option<String>,
    pub hot_replica_guard: Option<OwnerHotReplicaGuard>,
    pub protect_source_on_remote_complete: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum OwnerLocalReserveSlotState {
    Free,
    Prepared,
    PendingLocalVisible {
        holder_ref_count: u32,
    },
    Committed {
        route_live: bool,
        holder_ref_count: u32,
    },
}

#[derive(Debug, Clone)]
pub struct OwnerLocalReserveSlotRef {
    pub grant_id: u64,
    pub slot_index: u32,
    pub ptr: u64,
    pub base_addr: u64,
}

#[derive(Debug, Clone)]
pub struct OwnerLocalReserveSlotLease {
    pub value_len: u64,
    pub slot_size: u64,
    pub slots: Vec<OwnerLocalReserveSlotRef>,
}

impl OwnerLocalReserveSlotLease {
    pub fn value_ptrs(&self) -> Vec<u64> {
        self.slots.iter().map(|slot| slot.ptr).collect()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnerLocalReserveGrantState {
    pub grant_id: u64,
    pub base_addr: u64,
    pub addr: u64,
    pub len: u64,
    pub slot_size: u64,
    pub slot_count: u32,
    pub slot_states: Vec<OwnerLocalReserveSlotState>,
    pub free_slots: Vec<u32>,
    pub fully_free_since: Option<Instant>,
}

impl OwnerLocalReserveGrantState {
    pub fn new(
        grant_id: u64,
        base_addr: u64,
        addr: u64,
        len: u64,
        slot_size: u64,
        slot_count: u32,
    ) -> Self {
        let mut free_slots = Vec::with_capacity(slot_count as usize);
        for slot_index in (0..slot_count).rev() {
            free_slots.push(slot_index);
        }
        Self {
            grant_id,
            base_addr,
            addr,
            len,
            slot_size,
            slot_count,
            slot_states: vec![OwnerLocalReserveSlotState::Free; slot_count as usize],
            free_slots,
            fully_free_since: Some(Instant::now()),
        }
    }

    pub fn claim_prepared_slot(&mut self) -> Option<OwnerLocalReserveSlotRef> {
        let slot_index = self.free_slots.pop()?;
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        assert!(
            matches!(*state, OwnerLocalReserveSlotState::Free),
            "claim_prepared_slot expects a free slot"
        );
        *state = OwnerLocalReserveSlotState::Prepared;
        self.fully_free_since = None;
        Some(OwnerLocalReserveSlotRef {
            grant_id: self.grant_id,
            slot_index,
            ptr: self.addr + self.slot_size * slot_index as u64,
            base_addr: self.base_addr,
        })
    }

    pub fn mark_prepared_slot_pending_visible(&mut self, slot_index: u32) {
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        assert!(
            matches!(*state, OwnerLocalReserveSlotState::Prepared),
            "mark_prepared_slot_pending_visible expects a prepared slot"
        );
        *state = OwnerLocalReserveSlotState::PendingLocalVisible {
            holder_ref_count: 0,
        };
        self.fully_free_since = None;
    }

    pub fn promote_pending_visible_slot_to_committed(&mut self, slot_index: u32) {
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        let holder_ref_count = match *state {
            OwnerLocalReserveSlotState::PendingLocalVisible { holder_ref_count } => {
                holder_ref_count
            }
            _ => {
                unreachable!("promote_pending_visible_slot_to_committed expects a pending slot");
            }
        };
        *state = OwnerLocalReserveSlotState::Committed {
            route_live: true,
            holder_ref_count,
        };
        self.fully_free_since = None;
    }

    pub fn release_prepared_slot(&mut self, slot_index: u32) {
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        assert!(
            matches!(*state, OwnerLocalReserveSlotState::Prepared),
            "release_prepared_slot expects a prepared slot"
        );
        *state = OwnerLocalReserveSlotState::Free;
        self.free_slots.push(slot_index);
        if self.is_fully_free() {
            self.fully_free_since = Some(Instant::now());
        }
    }

    pub fn retain_resident_slot_holder(&mut self, slot_index: u32) {
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        match state {
            OwnerLocalReserveSlotState::PendingLocalVisible { holder_ref_count }
            | OwnerLocalReserveSlotState::Committed {
                holder_ref_count, ..
            } => {
                *holder_ref_count = holder_ref_count
                    .checked_add(1)
                    .expect("retain_resident_slot_holder overflow");
            }
            _ => {
                unreachable!("retain_resident_slot_holder expects a resident slot");
            }
        }
    }

    pub fn release_resident_slot_holder(&mut self, slot_index: u32) {
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        match state {
            OwnerLocalReserveSlotState::PendingLocalVisible { holder_ref_count } => {
                assert!(
                    *holder_ref_count > 0,
                    "release_resident_slot_holder expects holder_ref_count > 0"
                );
                *holder_ref_count -= 1;
                if *holder_ref_count == 0 {
                    *state = OwnerLocalReserveSlotState::Free;
                    self.free_slots.push(slot_index);
                    if self.is_fully_free() {
                        self.fully_free_since = Some(Instant::now());
                    }
                }
            }
            OwnerLocalReserveSlotState::Committed {
                route_live,
                holder_ref_count,
            } => {
                *holder_ref_count = holder_ref_count
                    .checked_sub(1)
                    .expect("release_resident_slot_holder expects holder_ref_count > 0");
                if !*route_live && *holder_ref_count == 0 {
                    *state = OwnerLocalReserveSlotState::Free;
                    self.free_slots.push(slot_index);
                    if self.is_fully_free() {
                        self.fully_free_since = Some(Instant::now());
                    }
                }
            }
            _ => {
                unreachable!("release_resident_slot_holder expects a resident slot");
            }
        }
    }

    pub fn release_committed_slot_route(&mut self, slot_index: u32) {
        let state = self
            .slot_states
            .get_mut(slot_index as usize)
            .expect("slot state index out of range");
        match state {
            OwnerLocalReserveSlotState::Committed {
                route_live,
                holder_ref_count,
            } => {
                assert!(
                    *route_live,
                    "release_committed_slot_route expects a live route"
                );
                *route_live = false;
                if *holder_ref_count == 0 {
                    *state = OwnerLocalReserveSlotState::Free;
                    self.free_slots.push(slot_index);
                    if self.is_fully_free() {
                        self.fully_free_since = Some(Instant::now());
                    }
                }
            }
            _ => {
                unreachable!("release_committed_slot_route expects a committed slot");
            }
        }
    }

    pub fn is_fully_free(&self) -> bool {
        self.free_slots.len() == self.slot_count as usize
    }

    pub fn used_slot_count(&self) -> usize {
        self.slot_count as usize - self.free_slots.len()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnerLocalReserveClassState {
    pub slot_size: u64,
    pub slots_per_grant: u32,
    pub grants: Vec<OwnerLocalReserveGrantState>,
    pub last_grow_at: Option<Instant>,
    pub pending_slot_demand: usize,
    pub expected_grant_count: usize,
}

impl OwnerLocalReserveClassState {
    pub fn new(slot_size: u64, slots_per_grant: u32) -> Self {
        Self {
            slot_size,
            slots_per_grant,
            grants: Vec::new(),
            last_grow_at: None,
            pending_slot_demand: 0,
            expected_grant_count: 0,
        }
    }

    pub fn free_slot_count(&self) -> usize {
        self.grants.iter().map(|grant| grant.free_slots.len()).sum()
    }

    pub fn used_slot_count(&self) -> usize {
        self.grants
            .iter()
            .map(|grant| grant.used_slot_count())
            .sum()
    }

    pub fn grant_count(&self) -> usize {
        self.grants.len()
    }
}

#[cfg(test)]
mod owner_reclaim_slot_tests {
    use super::{
        GetCachedInfo, OwnerHotCacheCounters, OwnerHotCacheEntry, OwnerHotReplicaGuard,
        OwnerHotReplicaIdentity, OwnerKeyControlState, OwnerLocalReserveGrantState,
        OwnerReclaimRecord, allocate_external_holding_id, build_owner_hot_cache,
        clone_if_owner_hot_entry_matches, forget_owner_hot_atomic_group,
        owner_hot_tp_cohort_key_rows, register_owner_hot_atomic_group, resolve_local_visible_batch,
    };
    use crate::master_kv_router::msg_pack::{
        OwnerReclaimItem, PutAtomicGroup, PutAtomicGroupMember,
    };
    use dashmap::{DashMap, DashSet};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Weak};

    #[test]
    fn external_holding_ids_are_nonzero_and_unique_for_resident_pages() {
        let counter = AtomicU64::new(1);
        let first = allocate_external_holding_id(&counter);
        let second = allocate_external_holding_id(&counter);
        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_ne!(first, second);
    }

    #[test]
    fn hot_source_pin_requires_the_same_version_and_allocation() {
        let current = Arc::new(7u64);
        let current_weak = Arc::downgrade(&current);
        let other = Arc::new(7u64);
        let other_weak = Arc::downgrade(&other);

        let pinned = clone_if_owner_hot_entry_matches((10, 2), &current, (10, 2), &current_weak)
            .expect("matching source should be pinned");
        assert!(Arc::ptr_eq(&pinned, &current));
        assert_eq!(Arc::strong_count(&current), 2);
        drop(pinned);

        assert!(
            clone_if_owner_hot_entry_matches((10, 2), &current, (10, 3), &current_weak).is_none()
        );
        assert!(
            clone_if_owner_hot_entry_matches((10, 2), &current, (10, 2), &other_weak).is_none()
        );
    }

    #[test]
    fn hot_replica_guard_keeps_dedup_until_terminal_outcome() {
        let identity = OwnerHotReplicaIdentity {
            key: "hot-key".to_string(),
            put_time_ms: 10,
            put_version: 2,
        };
        let inflight = Arc::new(DashSet::new());
        assert!(inflight.insert(identity.clone()));
        let counters = Arc::new(OwnerHotCacheCounters::default());
        let group_replica_triggered = Arc::new(DashSet::new());
        let guard = OwnerHotReplicaGuard::new(
            identity.clone(),
            inflight.clone(),
            counters.clone(),
            None,
            group_replica_triggered,
        );
        assert!(inflight.contains(&identity));

        guard.mark_completed();
        drop(guard);
        assert!(!inflight.contains(&identity));
        assert_eq!(counters.replica_completed.load(Ordering::Relaxed), 1);
        assert_eq!(counters.replica_failed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn hot_atomic_group_registry_interns_members_and_forgets_as_one_unit() {
        let group = PutAtomicGroup {
            members: vec![
                PutAtomicGroupMember {
                    key: "group-a".to_string(),
                    put_id: (10, 1),
                },
                PutAtomicGroupMember {
                    key: "group-b".to_string(),
                    put_id: (10, 2),
                },
            ],
        };
        let atomic_groups = DashMap::new();
        register_owner_hot_atomic_group(&atomic_groups, &group);
        register_owner_hot_atomic_group(&atomic_groups, &group);
        assert_eq!(atomic_groups.len(), 2);

        let first = OwnerHotReplicaIdentity::from_group_member(&group.members[0]);
        let second = OwnerHotReplicaIdentity::from_group_member(&group.members[1]);
        let first_group = atomic_groups.get(&first).unwrap().value().clone();
        let second_group = atomic_groups.get(&second).unwrap().value().clone();
        assert!(Arc::ptr_eq(&first_group, &second_group));

        let triggered = DashSet::new();
        assert!(triggered.insert(first.clone()));
        forget_owner_hot_atomic_group(&atomic_groups, &triggered, &second);
        assert!(atomic_groups.is_empty());
        assert!(!triggered.contains(&first));
    }

    #[test]
    fn owner_hot_tp_cohort_expands_the_same_group_boundary_across_ranks() {
        let identity = OwnerHotReplicaIdentity {
            key: "page0_model_0_2".to_string(),
            put_time_ms: 10,
            put_version: 1,
        };
        let rank0_group = PutAtomicGroup {
            members: vec![
                PutAtomicGroupMember {
                    key: "page0_model_0_2".to_string(),
                    put_id: (10, 1),
                },
                PutAtomicGroupMember {
                    key: "page1_model_0_2".to_string(),
                    put_id: (10, 2),
                },
            ],
        };
        assert_eq!(
            owner_hot_tp_cohort_key_rows(&identity, Some(&rank0_group)),
            Ok(Some(vec![
                vec!["page0_model_0_2".to_string(), "page1_model_0_2".to_string(),],
                vec!["page0_model_1_2".to_string(), "page1_model_1_2".to_string(),],
            ]))
        );

        let mixed_rank_group = PutAtomicGroup {
            members: vec![
                rank0_group.members[0].clone(),
                PutAtomicGroupMember {
                    key: "page1_model_1_2".to_string(),
                    put_id: (11, 2),
                },
            ],
        };
        assert_eq!(
            owner_hot_tp_cohort_key_rows(&identity, Some(&mixed_rank_group)),
            Err(())
        );
    }

    #[test]
    fn hot_group_trigger_is_persistent_on_success_and_retryable_on_failure() {
        let member = OwnerHotReplicaIdentity {
            key: "member".to_string(),
            put_time_ms: 10,
            put_version: 2,
        };
        let group_trigger = OwnerHotReplicaIdentity {
            key: "anchor".to_string(),
            put_time_ms: 10,
            put_version: 1,
        };
        let inflight = Arc::new(DashSet::new());
        let triggered = Arc::new(DashSet::new());
        let counters = Arc::new(OwnerHotCacheCounters::default());

        assert!(inflight.insert(member.clone()));
        assert!(triggered.insert(group_trigger.clone()));
        let failed_guard = OwnerHotReplicaGuard::new(
            member.clone(),
            inflight.clone(),
            counters.clone(),
            Some(group_trigger.clone()),
            triggered.clone(),
        );
        drop(failed_guard);
        assert!(!inflight.contains(&member));
        assert!(!triggered.contains(&group_trigger));
        assert_eq!(counters.replica_failed.load(Ordering::Relaxed), 1);

        assert!(inflight.insert(member.clone()));
        assert!(triggered.insert(group_trigger.clone()));
        let completed_guard = OwnerHotReplicaGuard::new(
            member.clone(),
            inflight.clone(),
            counters.clone(),
            Some(group_trigger.clone()),
            triggered.clone(),
        );
        completed_guard.mark_completed();
        drop(completed_guard);
        assert!(!inflight.contains(&member));
        assert!(triggered.contains(&group_trigger));
        assert_eq!(counters.replica_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn hot_cache_only_dispatches_size_removals_and_keeps_capacity() {
        let controls = Arc::new(Mutex::new(HashMap::new()));
        let cached = Arc::new(DashMap::<String, GetCachedInfo>::new());
        let inflight = Arc::new(DashSet::new());
        let atomic_groups = Arc::new(DashMap::new());
        let group_replica_triggered = Arc::new(DashSet::new());
        let counters = Arc::new(OwnerHotCacheCounters::default());
        let (tx, mut rx) = limit_thirdparty::tokio::sync::ampsc::channel(8);
        let cache = build_owner_hot_cache(
            10,
            controls,
            cached,
            inflight,
            atomic_groups,
            group_replica_triggered,
            counters.clone(),
            tx,
        );
        let entry = |put_version| OwnerHotCacheEntry {
            put_id: (10, put_version),
            memory_info: Weak::new(),
            weight_bytes: 6,
        };

        cache.insert("explicit".to_string(), entry(0));
        cache.run_pending_tasks();
        cache.invalidate("explicit");
        cache.run_pending_tasks();
        assert_eq!(counters.size_evictions.load(Ordering::Relaxed), 0);

        cache.insert("size-a".to_string(), entry(1));
        cache.insert("size-b".to_string(), entry(2));
        cache.run_pending_tasks();
        assert!(counters.size_evictions.load(Ordering::Relaxed) >= 1);
        assert_eq!(cache.policy().max_capacity(), Some(10));
        assert!(
            rx.try_recv().is_err(),
            "stale weak entries must not dispatch"
        );
    }

    #[test]
    fn batch_local_visibility_skips_reclaim_fenced_keys() {
        let keys = vec![
            "local-a".to_string(),
            "fenced".to_string(),
            "local-b".to_string(),
        ];
        let mut controls = HashMap::new();
        controls.insert(
            "fenced".to_string(),
            OwnerKeyControlState {
                local_puts: 0,
                reclaim: Some(OwnerReclaimRecord::Committed(OwnerReclaimItem {
                    key: "fenced".to_string(),
                    ..OwnerReclaimItem::default()
                })),
            },
        );
        let mut resolved_keys = Vec::new();

        let visible = resolve_local_visible_batch(&keys, &controls, |key| {
            resolved_keys.push(key.to_string());
            Some(key.to_string())
        });

        assert_eq!(
            visible,
            vec![
                Some("local-a".to_string()),
                None,
                Some("local-b".to_string())
            ]
        );
        assert_eq!(resolved_keys, vec!["local-a", "local-b"]);
    }

    #[test]
    fn committed_slot_becomes_free_only_after_route_and_holder_are_released() {
        let mut grant = OwnerLocalReserveGrantState::new(7, 0, 0, 16, 8, 2);
        let slot = grant.claim_prepared_slot().expect("slot should be free");
        grant.mark_prepared_slot_pending_visible(slot.slot_index);
        grant.retain_resident_slot_holder(slot.slot_index);
        grant.promote_pending_visible_slot_to_committed(slot.slot_index);

        grant.release_committed_slot_route(slot.slot_index);
        assert_eq!(grant.free_slots.len(), 1);

        grant.release_resident_slot_holder(slot.slot_index);
        assert_eq!(grant.free_slots.len(), 2);
        assert!(grant.is_fully_free());
    }
}

#[derive(Debug, Default)]
pub(crate) struct OwnerLocalReservePoolState {
    pub classes: HashMap<u64, OwnerLocalReserveClassState>,
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

fn format_metrics_snapshot_prometheus(
    client_id: &str,
    timestamp_ms: i64,
    metrics: &std::collections::HashMap<String, MetricsSet>,
) -> String {
    let mut result = String::new();

    for (metric_name, metric_set) in metrics {
        result.push_str(&metric_set.to_prometheus_format(metric_name, client_id));
        result.push_str(&metric_set.to_prometheus_timeline_format(client_id));
    }

    result.push_str(&format!(
        "kvcache_metrics_report_timestamp{{client=\"{}\"}} {}\n",
        client_id, timestamp_ms
    ));

    result
}

impl ClientKvApiInner {
    pub fn get_holding_len(&self) -> usize {
        self.external_get_holding.total()
    }

    pub fn runtime_observe_snapshot(&self) -> OwnerRuntimeObserveSnapshot {
        let mut external_get_holding_bytes = 0u64;
        for entry in self.external_get_holding.inner().iter() {
            external_get_holding_bytes =
                external_get_holding_bytes.saturating_add(entry.value().memory_info.len as u64);
        }
        let (hot_cache_capacity_bytes, hot_cache_entries, hot_cache_weighted_bytes) = self
            .owner_hot_cache
            .as_ref()
            .map(|cache| {
                (
                    cache.policy().max_capacity().unwrap_or(0),
                    cache.entry_count(),
                    cache.weighted_size(),
                )
            })
            .unwrap_or_default();
        OwnerRuntimeObserveSnapshot {
            external_get_holding_entries: self.external_get_holding.total() as u64,
            external_get_holding_bytes,
            external_pending_put_entries: self.external_pending_puts.entry_count(),
            hot_cache_capacity_bytes,
            hot_cache_entries,
            hot_cache_weighted_bytes,
            hot_size_evictions: self
                .owner_hot_counters
                .size_evictions
                .load(Ordering::Relaxed),
            hot_replica_enqueued: self
                .owner_hot_counters
                .replica_enqueued
                .load(Ordering::Relaxed),
            hot_replica_completed: self
                .owner_hot_counters
                .replica_completed
                .load(Ordering::Relaxed),
            hot_replica_already_satisfied: self
                .owner_hot_counters
                .replica_already_satisfied
                .load(Ordering::Relaxed),
            hot_replica_failed: self
                .owner_hot_counters
                .replica_failed
                .load(Ordering::Relaxed),
            hot_replica_obsolete: self
                .owner_hot_counters
                .replica_obsolete
                .load(Ordering::Relaxed),
            hot_replica_dispatch_failed: self
                .owner_hot_counters
                .replica_dispatch_failed
                .load(Ordering::Relaxed),
            hot_replica_inflight: self.owner_hot_replica_inflight.len() as u64,
            hot_eviction_skipped_stale: self
                .owner_hot_counters
                .skipped_stale
                .load(Ordering::Relaxed),
            hot_eviction_skipped_reclaim: self
                .owner_hot_counters
                .skipped_reclaim
                .load(Ordering::Relaxed),
            hot_eviction_skipped_duplicate: self
                .owner_hot_counters
                .skipped_duplicate
                .load(Ordering::Relaxed),
            hot_group_registry_entries: self.owner_hot_atomic_groups.len() as u64,
            hot_group_replica_triggered: self.owner_hot_group_replica_triggered.len() as u64,
            hot_group_replica_triggers: self
                .owner_hot_counters
                .group_replica_triggers
                .load(Ordering::Relaxed),
            hot_group_members_enqueued: self
                .owner_hot_counters
                .group_members_enqueued
                .load(Ordering::Relaxed),
            hot_group_trigger_duplicates: self
                .owner_hot_counters
                .group_trigger_duplicates
                .load(Ordering::Relaxed),
            hot_group_trigger_incomplete: self
                .owner_hot_counters
                .group_trigger_incomplete
                .load(Ordering::Relaxed),
            grouped_put_done_batches: self
                .owner_hot_counters
                .grouped_put_done_batches
                .load(Ordering::Relaxed),
            grouped_put_done_items: self
                .owner_hot_counters
                .grouped_put_done_items
                .load(Ordering::Relaxed),
            legacy_put_done_batches: self
                .owner_hot_counters
                .legacy_put_done_batches
                .load(Ordering::Relaxed),
            legacy_put_done_items: self
                .owner_hot_counters
                .legacy_put_done_items
                .load(Ordering::Relaxed),
        }
    }

    pub fn get_cache_len(&self) -> usize {
        self.precommit_local_visible_info.len() + self.get_cached_info.len()
    }
    fn metrics_handle(&self) -> Arc<MetricsHandle> {
        self.metrics
            .get()
            .cloned()
            .expect("metrics handle not initialized")
    }

    pub fn locality_snapshot(&self) -> KvLocalitySnapshot {
        self.metrics_handle().get_locality_snapshot()
    }

    pub fn record_put_locality(&self, remote: bool, bytes: u64, transfer_us: i64) {
        self.metrics_handle()
            .record_put_io_locality(remote, bytes, transfer_us);
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

    pub(crate) fn owner_local_reserve_rebalance_notify(
        &self,
    ) -> Arc<limit_thirdparty::tokio::sync::Notify> {
        self.owner_local_reserve_rebalance_notify.clone()
    }

    pub(crate) fn owner_local_reserve_register_pending_demand(
        &self,
        slot_size: u64,
        slots_per_grant: u32,
        demand_slots: usize,
    ) {
        let mut pool = self.owner_local_reserve_pool.lock();
        let class_state = pool
            .classes
            .entry(slot_size)
            .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
        class_state.pending_slot_demand =
            class_state.pending_slot_demand.saturating_add(demand_slots);
    }

    pub(crate) fn owner_local_reserve_consume_pending_demand(
        &self,
        slot_size: u64,
        slots_per_grant: u32,
        demand_slots: usize,
    ) {
        let mut pool = self.owner_local_reserve_pool.lock();
        let class_state = pool
            .classes
            .entry(slot_size)
            .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
        class_state.pending_slot_demand =
            class_state.pending_slot_demand.saturating_sub(demand_slots);
    }
}
impl ClientKvApi {
    pub fn inner(&self) -> &ClientKvApiInner {
        &self.0
    }

    fn spawn_runtime_observe_reporter(&self) {
        let view = self.0.view.clone_view();
        let view_task = view.clone();
        view.spawn("client_runtime_observe_reporter", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            loop {
                tokio::select! {
                    _ = shutdown_waiter.wait() => break,
                    _ = interval.tick() => {
                        let snapshot = view_task.client_kv_api().inner().runtime_observe_snapshot();
                        let metrics = view_task.metric_reporter().metrics();
                        metrics.set_kv_holding_entries(
                            "owner_external_get_holding",
                            snapshot.external_get_holding_entries,
                        );
                        metrics.set_kv_holding_bytes(
                            "owner_external_get_holding",
                            snapshot.external_get_holding_bytes,
                        );
                        metrics.set_kv_external_pending_put_entries(
                            snapshot.external_pending_put_entries,
                        );
                        if snapshot.hot_cache_capacity_bytes > 0 {
                            tracing::info!(
                                capacity_bytes = snapshot.hot_cache_capacity_bytes,
                                entries = snapshot.hot_cache_entries,
                                weighted_bytes = snapshot.hot_cache_weighted_bytes,
                                size_evictions = snapshot.hot_size_evictions,
                                replica_enqueued = snapshot.hot_replica_enqueued,
                                replica_completed = snapshot.hot_replica_completed,
                                replica_already_satisfied = snapshot.hot_replica_already_satisfied,
                                replica_failed = snapshot.hot_replica_failed,
                                replica_obsolete = snapshot.hot_replica_obsolete,
                                replica_dispatch_failed = snapshot.hot_replica_dispatch_failed,
                                replica_inflight = snapshot.hot_replica_inflight,
                                skipped_stale = snapshot.hot_eviction_skipped_stale,
                                skipped_reclaim = snapshot.hot_eviction_skipped_reclaim,
                                skipped_duplicate = snapshot.hot_eviction_skipped_duplicate,
                                group_registry_entries = snapshot.hot_group_registry_entries,
                                group_replica_triggered = snapshot.hot_group_replica_triggered,
                                group_replica_triggers = snapshot.hot_group_replica_triggers,
                                group_members_enqueued = snapshot.hot_group_members_enqueued,
                                group_trigger_duplicates = snapshot.hot_group_trigger_duplicates,
                                group_trigger_incomplete = snapshot.hot_group_trigger_incomplete,
                                grouped_put_done_batches = snapshot.grouped_put_done_batches,
                                grouped_put_done_items = snapshot.grouped_put_done_items,
                                legacy_put_done_batches = snapshot.legacy_put_done_batches,
                                legacy_put_done_items = snapshot.legacy_put_done_items,
                                "owner hot replica policy snapshot"
                            );
                        }
                    }
                }
            }
        });
    }

    pub fn attach_view(&self, view: ClientKvApiView) {
        self.0.view.attach(view);
    }

    pub async fn construct(arg: ClientKvApiNewArg) -> Result<Self, KvError> {
        tracing::info!("Constructing ClientKvApi in Client mode (PreView)");
        let ClientKvApiNewArg {
            test_spec_config,
            owner_hot_cache_capacity_bytes,
        } = arg;
        let (replica_task_tx, replica_task_rx) =
            tokio::sync::ampsc::channel(REPLICA_TASK_QUEUE_CAPACITY);
        let (owner_local_publish_tx, owner_local_publish_rx) =
            tokio::sync::ampsc::channel(OWNER_LOCAL_PUBLISH_QUEUE_CAPACITY);
        let (owner_hot_eviction_tx, owner_hot_eviction_rx) =
            tokio::sync::ampsc::channel(OWNER_HOT_EVICTION_QUEUE_CAPACITY);
        let get_cached_info = Arc::new(DashMap::new());
        let owner_key_control = Arc::new(Mutex::new(HashMap::new()));
        let owner_hot_replica_inflight = Arc::new(DashSet::new());
        let owner_hot_atomic_groups = Arc::new(DashMap::new());
        let owner_hot_group_replica_triggered = Arc::new(DashSet::new());
        let owner_hot_counters = Arc::new(OwnerHotCacheCounters::default());
        let owner_hot_cache = owner_hot_cache_capacity_bytes.map(|capacity_bytes| {
            build_owner_hot_cache(
                capacity_bytes,
                owner_key_control.clone(),
                get_cached_info.clone(),
                owner_hot_replica_inflight.clone(),
                owner_hot_atomic_groups.clone(),
                owner_hot_group_replica_triggered.clone(),
                owner_hot_counters.clone(),
                owner_hot_eviction_tx,
            )
        });

        let inner = ClientKvApiInner {
            view: ClientKvApiViewHolder::new(),
            test_spec_config,
            metrics: OnceLock::new(),
            all_memholder_refcount: OnceLock::new(),
            get_remote_kv_lock: AMapLock::new(Duration::from_secs(60)),
            get_cached_info,
            precommit_local_visible_info: DashMap::new(),
            local_snapshot_info: DashMap::new(),
            owner_local_reserve_pool: Mutex::new(OwnerLocalReservePoolState::default()),
            owner_local_reserve_claim_lock: limit_thirdparty::tokio::sync::AMutex::new(()),
            owner_local_reserve_rebalance_notify: Arc::new(
                limit_thirdparty::tokio::sync::Notify::new(),
            ),
            external_local_first_put_id_counter: AtomicU32::new(0),
            owner_key_control,
            owner_hot_cache,
            owner_hot_replica_inflight,
            owner_hot_atomic_groups,
            owner_hot_group_replica_triggered,
            owner_hot_counters,
            owner_hot_eviction_rx: Mutex::new(Some(owner_hot_eviction_rx)),
            external_invalidate_delete: EnsureMemholderMgmtDeleteHandle::new(
                OwnerExternalMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            delete_ack_batch: EnsureMemholderMgmtDeleteHandle::new(
                OwnerDeleteAckMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            owner_delete_ack_mgr: OwnerDeleteAckMemMgr::default(),
            external_get_holding: OwnerExternalMemMgr::default(),
            external_get_start_registry: DashMap::new(),
            external_get_start_by_key: DashMap::new(),
            next_external_get_start_handle: AtomicU64::new(1),
            next_external_holding_id: AtomicU64::new(1),
            external_pending_puts: moka::sync::Cache::builder()
                .time_to_live(Duration::from_secs(30 * 60))
                .segments(16)
                .build(),
            #[cfg(test)]
            test_record: crate::client_kv_api::client_test_record::ClientTestRecord::new(),
            rpc_caller_get_start: RPCCaller::new(),
            rpc_caller_get_revoke: RPCCaller::new(),
            rpc_caller_get_done: RPCCaller::new(),
            rpc_caller_batch_get_start: RPCCaller::new(),
            rpc_caller_batch_get_revoke: RPCCaller::new(),
            rpc_caller_batch_get_done: RPCCaller::new(),
            rpc_caller_put_start: RPCCaller::new(),
            rpc_caller_put_revoke: RPCCaller::new(),
            rpc_caller_put_done: RPCCaller::new(),
            rpc_caller_batch_put_start: RPCCaller::new(),
            rpc_caller_batch_put_revoke: RPCCaller::new(),
            rpc_caller_batch_put_done: RPCCaller::new(),
            rpc_caller_grouped_batch_put_done: RPCCaller::new(),
            rpc_caller_batch_prepare_put_keys: RPCCaller::new(),
            rpc_caller_batch_release_put_key_reservations: RPCCaller::new(),
            rpc_caller_put_append_start: RPCCaller::new(),
            rpc_caller_put_append_revoke: RPCCaller::new(),
            rpc_caller_put_append_done: RPCCaller::new(),
            rpc_caller_reserve_local_grant: RPCCaller::new(),
            rpc_caller_release_local_grant: RPCCaller::new(),
            rpc_caller_delete: RPCCaller::new(),
            rpc_caller_batch_delete_ack: RPCCaller::new(),
            rpc_caller_batch_is_exist: RPCCaller::new(),
            rpc_caller_get_meta: RPCCaller::new(),
            rpc_caller_allocate_client_lease: RPCCaller::new(),
            rpc_caller_client_lease_keepalive: RPCCaller::new(),
            rpc_caller_external_put_commit: RPCCaller::new(),
            rpc_caller_external_put_revoke: RPCCaller::new(),
            rpc_caller_resolve_side_transfer_lane: RPCCaller::new(),
            default_lease_id: parking_lot::RwLock::new(None),
            owner_local_publish_tx,
            owner_local_publish_rx: Mutex::new(Some(owner_local_publish_rx)),
            replica_task_tx,
            replica_task_rx: Mutex::new(Some(replica_task_rx)),
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
        inner
            .rpc_caller_batch_get_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_done
            .regist(inner.view.p2p_module());
        inner.rpc_caller_put_start.regist(inner.view.p2p_module());
        inner.rpc_caller_put_revoke.regist(inner.view.p2p_module());
        inner.rpc_caller_put_done.regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_grouped_batch_put_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_prepare_put_keys
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_release_put_key_reservations
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_reserve_local_grant
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_release_local_grant
            .regist(inner.view.p2p_module());
        inner.rpc_caller_delete.regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_delete_ack
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_is_exist
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
        RPCCaller::<BatchIsExistReq>::new().regist(inner.view.p2p_module());
        RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new().regist(inner.view.p2p_module());
        spawn_owner_local_reserve_rebalance_actor(inner.view.clone_view());
        self.spawn_runtime_observe_reporter();

        // External RPC handlers
        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalGetReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            view.spawn("rpc_external_get", async move {
                let result = handle_external_get(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get", async move {
                    let result = handle_external_batch_get(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetStartReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_start", async move {
                    let result = handle_external_batch_get_start(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetTransferReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_transfer", async move {
                    let result = handle_external_batch_get_transfer(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetCancelReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_cancel", async move {
                    let result = handle_external_batch_get_cancel(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutStartReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            view.spawn("rpc_external_put_start", async move {
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
        RPCHandler::<ExternalBatchPutStartReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_put_start", async move {
                    let result = handle_external_batch_put_start(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutTransferEndReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_put_transfer_end", async move {
                    let result = handle_external_put_transfer_end(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchPutTransferEndReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_put_transfer_end", async move {
                    let result = handle_external_batch_put_transfer_end(&view_task, &msg).await;
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
                view.spawn("rpc_external_put_commit", async move {
                    let result = handle_external_put_commit(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchPutCommitReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_put_commit", async move {
                    let result = handle_external_batch_put_commit(&view_task, &msg).await;
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
                view.spawn("rpc_external_put_revoke", async move {
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
            view.spawn("rpc_external_delete", async move {
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
                view.spawn("rpc_external_is_exist", async move {
                    let result = handle_external_is_exist(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchIsExistReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_is_exist", async move {
                    let result = handle_external_batch_is_exist(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalObservabilitySnapshotReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_observability_snapshot", async move {
                    let result = handle_external_observability_snapshot(&view_task, &msg).await;
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
                view.spawn("rpc_external_delete_ack", async move {
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
            view.spawn("rpc_sync_kv_to_file", async move {
                let result = handle_sync_kv_to_file_client(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        // client rpc handler register
        let view = inner.view.clone_view();
        RPCHandler::<BatchEnqueueReplicaTaskReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_batch_enqueue_replica_tasks", async move {
                    let ack = put::handle_batch_enqueue_replica_tasks(&view_task, msg).await;
                    if let Err(e) = resp.send_resp(ack).await {
                        warn!("Failed to send BatchEnqueueReplicaTaskResp: {:?}", e);
                    }
                });
                Ok(())
            },
        );

        let view = inner.view.clone_view();
        RPCHandler::<BatchOwnerReclaimReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let req_node_id = resp.node_id().clone();
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_batch_owner_reclaim", async move {
                    let ack = handle_batch_owner_reclaim(&view_task, msg, req_node_id).await;
                    if let Err(e) = resp.send_resp(ack).await {
                        warn!("Failed to send BatchOwnerReclaimResp: {:?}", e);
                    }
                });
                Ok(())
            },
        );

        let view = inner.view.clone_view();
        RPCHandler::<BatchDeleteClientKvMetaCacheReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let req_node_id = resp.node_id().clone();
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_batch_delete_client_kv_meta_cache", async move {
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

        if let Some(replica_task_rx) = inner.replica_task_rx.lock().take() {
            put::spawn_replica_task_actor(
                inner.view.clone_view(),
                replica_task_rx,
                inner
                    .test_spec_config
                    .replica_task_max_inflight
                    .unwrap_or(1) as usize,
            );
        } else {
            tracing::warn!("replica_task_rx already taken for ClientKvApi");
        }
        if inner.owner_hot_cache.is_some() {
            if let Some(owner_hot_eviction_rx) = inner.owner_hot_eviction_rx.lock().take() {
                put::spawn_owner_hot_replica_dispatcher(
                    inner.view.clone_view(),
                    owner_hot_eviction_rx,
                );
            } else {
                tracing::warn!("owner_hot_eviction_rx already taken for ClientKvApi");
            }
        }
        if let Some(owner_local_publish_rx) = inner.owner_local_publish_rx.lock().take() {
            put::spawn_owner_local_publish_dispatcher(
                inner.view.clone_view(),
                owner_local_publish_rx,
                OWNER_LOCAL_PUBLISH_MAX_INFLIGHT,
            );
        } else {
            tracing::warn!("owner_local_publish_rx already taken for ClientKvApi");
        }

        // Spawn cluster listener to clean up get_holding when external_client leaves
        let view = inner.view.clone_view();
        let view2 = view.clone();
        let view_task = view2.clone();
        view.spawn("client_cluster_listener", async move {
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

    pub fn has_cached_key(&self, key: &str) -> bool {
        self.inner().has_local_snapshot(key)
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
