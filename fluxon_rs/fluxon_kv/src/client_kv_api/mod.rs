use crate::client_kv_api::delete::handle_batch_delete_client_kv_meta_cache;
use crate::client_kv_api::local_reserve_rebalance::{
    spawn_owner_local_reserve_rebalance_actor, spawn_owner_slot_pressure_actor,
};
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
    BatchEvictOwnerSourceReq, BatchGetDoneReq, BatchGetRevokeReq, BatchGetStartItemResp,
    BatchGetStartReq, BatchIsExistReq, BatchOwnerReclaimReq, BatchPreparePutKeysReq,
    BatchPutAppendDoneReq, BatchPutAppendStartReq, BatchPutDoneReq, BatchPutRevokeReq,
    BatchPutStartReq, BatchReleasePutKeyReservationsReq, DeleteClientKvMetaCacheItem,
    GroupedBatchPutDoneReq,
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
    cluster_manager::{ClusterEvent, ClusterManager, ClusterManagerAccessTrait},
    master_kv_router::msg_pack::{
        DeleteReq, GetDoneReq, GetMetaReq, GetRevokeReq, GetStartReq, PutAppendDoneReq,
        PutAppendRevokeReq, PutAppendStartReq, PutDoneReq, PutRevokeReq, PutStartReq,
        ReleaseLocalGrantReq, ReserveLocalGrantReq,
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
use dashmap::{DashMap, mapref::entry::Entry as DashMapEntry};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_util::map_lock::AMapLock;
use fluxon_util::pin_aware_moka::{PinAwareMoka, PinGuard};
use limit_thirdparty::tokio;
use moka::notification::RemovalCause;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Weak;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

const REPLICA_TASK_QUEUE_CAPACITY: usize = 128;
// Queue only weak source references. This preserves a deep policy backlog without turning queued
// write-back work into non-reclaimable owner memory; the dispatcher pins a still-current source
// immediately before handing it to the bounded replica-task actor.
const OWNER_LOCAL_PUBLISH_QUEUE_CAPACITY: usize = 4096;
const OWNER_LOCAL_PUBLISH_MAX_INFLIGHT: usize = 64;

/// Information about a memholder held by external client
#[derive(Clone)]
pub struct ExternalHoldingGetInfo {
    pub key: String,
    pub req_node_id: String,
    /// Requester membership generation observed when this holding was
    /// installed. Unknown generations are never removed by generation-scoped
    /// MemberLeft cleanup.
    pub requester_node_start_time: Option<i64>,
    pub memory_info: Arc<MemoryInfo>, // The actual memholder being held
    _owner_hot_pin: Option<PinGuard>,
}

#[derive(Clone, Debug, Default)]
pub struct OwnerRuntimeObserveSnapshot {
    pub external_get_holding_entries: u64,
    pub external_get_holding_bytes: u64,
    pub external_get_start_handles: u64,
    pub external_get_flights: u64,
    pub external_get_flights_starting: u64,
    pub external_get_flights_finishing: u64,
    pub external_get_flights_revoking: u64,
    pub external_get_undecided_interests: u64,
    pub external_get_retained_interests: u64,
    pub external_pending_put_entries: u64,
    pub local_reserve_slots_free: u64,
    pub local_reserve_slots_prepared: u64,
    pub local_reserve_slots_pending_visible: u64,
    pub local_reserve_slots_committed: u64,
    pub hot_cache_capacity_bytes: u64,
    pub hot_cache_entries: u64,
    pub hot_cache_weighted_bytes: u64,
    pub hot_size_evictions: u64,
    pub hot_source_evict_handoff_members: u64,
    pub hot_source_evict_committed_members: u64,
    pub hot_source_evict_restored_members: u64,
    pub hot_source_evict_obsolete: u64,
    pub hot_source_evict_dispatch_failed: u64,
    pub hot_source_eviction_selected: u64,
    pub hot_source_evict_retry_entries: u64,
    pub hot_source_evict_retry_scheduled: u64,
    pub hot_source_evict_retry_emitted: u64,
    pub hot_selection_debt_bytes: u64,
    pub hot_source_eviction_selected_bytes: u64,
    pub hot_eviction_skipped_stale: u64,
    pub hot_eviction_skipped_reclaim: u64,
    pub hot_eviction_skipped_active_holders: u64,
    pub hot_group_registry_entries: u64,
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
    Local { memory_info: Arc<MemoryInfo> },
    Shared { interest: ExternalGetKeyInterest },
}

/// One request's interest in a per-key Get flight.
///
/// A pending prefix decision is owned by this guard, not by control flow.  If
/// the request future is cancelled at any await point, Drop retires the
/// undecided count so the atomic_batch task can still choose Finish or Revoke.
pub struct ExternalGetKeyInterest {
    op: Arc<ExternalGetKeySharedOp>,
    decision_pending: bool,
}

impl ExternalGetKeyInterest {
    pub fn new(op: Arc<ExternalGetKeySharedOp>, decision_pending: bool) -> Self {
        Self {
            op,
            decision_pending,
        }
    }

    pub fn op(&self) -> &Arc<ExternalGetKeySharedOp> {
        &self.op
    }

    pub fn decide(&mut self, retain: bool) {
        if !self.decision_pending {
            return;
        }
        self.decision_pending = false;

        let wake = {
            let mut state = self.op.state.lock();
            if state.undecided == 0 {
                tracing::error!(
                    "external Get interest observed undecided underflow: key={}",
                    self.op.key
                );
                return;
            }
            state.undecided -= 1;
            if retain
                && matches!(
                    state.phase,
                    ExternalGetKeySharedPhase::Starting | ExternalGetKeySharedPhase::Started { .. }
                )
            {
                state.retained = state
                    .retained
                    .checked_add(1)
                    .expect("external Get singleflight retained overflow");
            }
            state.undecided == 0
        };
        if wake {
            self.op.notify.notify_waiters();
        }
    }
}

impl Drop for ExternalGetKeyInterest {
    fn drop(&mut self) {
        self.decide(false);
    }
}

#[derive(Clone)]
pub enum ExternalGetKeySharedPhase {
    Starting,
    Started {
        item: BatchGetStartItemResp,
    },
    /// At least one request plan's prefix retained this key.  The leader atomic_batch is
    /// transferring/completing it and later publishes one canonical result.
    Finishing {
        item: BatchGetStartItemResp,
    },
    /// No request plan's prefix retained this prepared Get.  Keep the marker in
    /// the owner key fence until BatchGetRevoke and local-slot release finish,
    /// so a new overlapping batch cannot race the revoke.
    Revoking {
        /// Keep the exact master operation and prepared target reachable until
        /// Revoke reaches a definite terminal response.
        item: BatchGetStartItemResp,
    },
    Ready {
        result: ExternalGetStartSharedItemResult,
    },
    Failed {
        error_code: ErrorCode,
        error_json: String,
    },
}

pub struct ExternalGetKeySharedState {
    /// Exact-batch operations which joined while the key was Starting/Started
    /// and have not yet applied their own atomic-prefix decision.
    pub undecided: usize,
    /// Number of those operations whose transferable prefix retained the key.
    pub retained: usize,
    pub phase: ExternalGetKeySharedPhase,
}

pub struct ExternalGetKeySharedOp {
    pub key: String,
    pub state: Mutex<ExternalGetKeySharedState>,
    pub notify: Arc<limit_thirdparty::tokio::sync::Notify>,
}

impl ExternalGetKeySharedOp {
    pub fn new(key: String) -> Self {
        Self {
            key,
            state: Mutex::new(ExternalGetKeySharedState {
                undecided: 1,
                retained: 0,
                phase: ExternalGetKeySharedPhase::Starting,
            }),
            notify: Arc::new(limit_thirdparty::tokio::sync::Notify::new()),
        }
    }
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

pub struct ExternalGetStartEntry {
    pub req_node_id: String,
    /// Requester membership generation observed when this handle was created.
    /// `None` is retained only for compatibility with a temporarily incomplete
    /// cluster cache; generation cleanup must never guess in that case.
    pub requester_node_start_time: Option<i64>,
    pub keys: Vec<String>,
    pub items: Vec<ExternalGetStartOwnerItem>,
    pub created_at: Instant,
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
pub(crate) struct PendingLocalGetInfo {
    get_id: u64,
    put_id: crate::master_kv_router::put::PutIDForAKey,
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
pub(crate) struct OwnerHotPinAlias {
    key: String,
    memory_info_ptr: usize,
}

impl OwnerHotPinAlias {
    fn new(memory_info: &Arc<MemoryInfo>) -> Self {
        Self {
            key: memory_info.key.clone(),
            memory_info_ptr: Arc::as_ptr(memory_info) as usize,
        }
    }
}

type OwnerHotCache = PinAwareMoka<String, OwnerHotPinAlias, OwnerHotCacheEntry>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OwnerHotReplicaIdentity {
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
    source_evict_handoff_members: AtomicU64,
    source_evict_committed_members: AtomicU64,
    source_evict_restored_members: AtomicU64,
    source_evict_obsolete: AtomicU64,
    source_evict_dispatch_failed: AtomicU64,
    source_evict_retry_scheduled: AtomicU64,
    source_evict_retry_emitted: AtomicU64,
    selection_debt_bytes: Arc<AtomicU64>,
    /// Bytes behind an installed source-selection fence. Unlike candidate
    /// debt, every byte here can become a physical Free slot after reclaim.
    source_eviction_selected_bytes: AtomicU64,
    skipped_stale: AtomicU64,
    skipped_reclaim: AtomicU64,
    skipped_active_holders: AtomicU64,
    group_trigger_duplicates: AtomicU64,
    group_trigger_incomplete: AtomicU64,
    grouped_put_done_batches: AtomicU64,
    grouped_put_done_items: AtomicU64,
    legacy_put_done_batches: AtomicU64,
    legacy_put_done_items: AtomicU64,
}

struct OwnerHotSelectionDebt {
    weight_bytes: u64,
    outstanding_bytes: Arc<AtomicU64>,
    released: AtomicU32,
}

impl OwnerHotSelectionDebt {
    fn new(weight_bytes: u64, outstanding_bytes: Arc<AtomicU64>) -> Arc<Self> {
        outstanding_bytes.fetch_add(weight_bytes, Ordering::AcqRel);
        Arc::new(Self {
            weight_bytes,
            outstanding_bytes,
            released: AtomicU32::new(0),
        })
    }

    fn release(&self) {
        if self
            .released
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.outstanding_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(self.weight_bytes))
                })
                .expect("owner hot selection debt update cannot fail");
        }
    }
}

impl OwnerHotCacheCounters {
    fn add_source_eviction_selected_bytes(&self, bytes: u64) {
        self.source_eviction_selected_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes)
            })
            .expect("owner source-selection byte credit overflowed");
    }

    fn remove_source_eviction_selected_bytes(&self, bytes: u64) {
        self.source_eviction_selected_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(bytes)
            })
            .expect("owner source-selection byte credit underflowed");
    }
}

#[derive(Clone)]
pub(crate) struct OwnerHotEvictionEvent {
    key: String,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    memory_info: Weak<MemoryInfo>,
    selection_debt: Arc<OwnerHotSelectionDebt>,
    /// The event was dispatched from the bounded retry queue. It may be a
    /// pre-selection retry (no atomic_batch yet) or an exact installed-atomic_batch retry.
    retry: bool,
    /// Exact owner source-delete atomic_batch retained across RPC retries. Once set,
    /// retries never re-expand or re-pin the local atomic_batch.
    source_eviction_atomic_batch:
        Option<Arc<crate::master_kv_router::msg_pack::OwnerSourceEvictionAtomicBatch>>,
    /// Failure count follows the event while it is dispatched, preserving
    /// exponential backoff across queue take/reinsert cycles.
    retry_failures: u32,
}

pub(crate) enum OwnerHotEvictionPreparation {
    Ready {
        trigger: OwnerHotReplicaIdentity,
        sources: Vec<(OwnerHotReplicaIdentity, Arc<MemoryInfo>)>,
    },
    RetryableReclaimFence,
    RetryableIncompleteAtomicBatch,
    TemporarilyPinned {
        active_members: usize,
    },
    Obsolete,
}

pub(crate) enum OwnerHotSelectionFenceOutcome {
    Fenced,
    Retryable,
    TemporarilyPinned { active_members: usize },
    Obsolete,
}

struct OwnerHotRetryEntry {
    event: OwnerHotEvictionEvent,
    failures: u32,
    next_attempt_at: Instant,
    dispatched: bool,
}

#[derive(Default)]
struct OwnerHotRetryState {
    entries: HashMap<OwnerHotReplicaIdentity, OwnerHotRetryEntry>,
    /// Exactly one deadline for each non-dispatched entry.  Unlike a lazy
    /// generation heap, rescheduling replaces the old tuple, so both memory
    /// and lock-held work are bounded by the live retry set, not its history.
    deadlines: BTreeSet<(Instant, OwnerHotReplicaIdentity)>,
}

/// Exactly-once owner-local retry state.  It is physically bounded by the
/// owner committed-slot pool: one identity can occupy at most one entry, and
/// obsolete identities are removed when their local version is invalidated.
/// The actor emits only a small due batch and applies exponential backoff.
struct OwnerHotRetryQueue {
    state: Mutex<OwnerHotRetryState>,
    notify: Arc<limit_thirdparty::tokio::sync::Notify>,
    counters: Arc<OwnerHotCacheCounters>,
}

impl OwnerHotRetryQueue {
    fn new(counters: Arc<OwnerHotCacheCounters>) -> Self {
        Self {
            state: Mutex::new(OwnerHotRetryState::default()),
            notify: Arc::new(limit_thirdparty::tokio::sync::Notify::new()),
            counters,
        }
    }

    fn retry_delay(failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(8);
        Duration::from_millis(25u64.saturating_mul(1u64 << shift)).min(Duration::from_secs(5))
    }

    fn schedule(&self, mut event: OwnerHotEvictionEvent, reason: &'static str) {
        let identity = OwnerHotReplicaIdentity {
            key: event.key.clone(),
            put_time_ms: event.put_id.0,
            put_version: event.put_id.1,
        };
        let now = Instant::now();
        let mut state = self.state.lock();
        let previous_deadline = state
            .entries
            .get(&identity)
            .and_then(|entry| (!entry.dispatched).then_some(entry.next_attempt_at));
        if let Some(previous_deadline) = previous_deadline {
            state
                .deadlines
                .remove(&(previous_deadline, identity.clone()));
        }
        let entry = state
            .entries
            .entry(identity.clone())
            .or_insert_with(|| OwnerHotRetryEntry {
                event: event.clone(),
                failures: event.retry_failures,
                next_attempt_at: now,
                dispatched: false,
            });
        if !Arc::ptr_eq(&entry.event.selection_debt, &event.selection_debt) {
            entry.event.selection_debt.release();
        }
        if event.source_eviction_atomic_batch.is_none() {
            event.source_eviction_atomic_batch = entry.event.source_eviction_atomic_batch.clone();
        }
        entry.failures = entry.failures.max(event.retry_failures).saturating_add(1);
        event.retry = true;
        event.retry_failures = entry.failures;
        entry.event = event;
        entry.next_attempt_at = now + Self::retry_delay(entry.failures);
        entry.dispatched = false;
        let next_attempt_at = entry.next_attempt_at;
        let inserted = state.deadlines.insert((next_attempt_at, identity.clone()));
        debug_assert!(inserted, "owner retry deadline must be unique per identity");
        drop(state);
        self.counters
            .source_evict_retry_scheduled
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            key = identity.key,
            put_time_ms = identity.put_time_ms,
            put_version = identity.put_version,
            reason,
            "owner writeback entered retryable local state"
        );
        self.notify.notify_waiters();
    }

    fn take_due_batch(&self, now: Instant, limit: usize) -> Vec<OwnerHotEvictionEvent> {
        let mut state = self.state.lock();
        let mut due = Vec::with_capacity(limit);
        while due.len() < limit {
            let Some((deadline, identity)) = state.deadlines.iter().next().cloned() else {
                break;
            };
            if deadline > now {
                break;
            }
            state.deadlines.remove(&(deadline, identity.clone()));
            let Some(entry) = state.entries.get_mut(&identity) else {
                debug_assert!(false, "owner retry deadline must reference a live entry");
                continue;
            };
            if entry.dispatched || entry.next_attempt_at != deadline {
                debug_assert!(false, "owner retry deadline and entry must agree");
                continue;
            }
            due.push(entry.event.clone());
            // Keep the authoritative retry record until the dispatcher has
            // atomically pinned the source and installed an inflight guard,
            // but emit it only once. A failed dispatcher attempt explicitly
            // reschedules it with the next backoff.
            entry.dispatched = true;
        }
        due
    }

    fn remove(&self, identity: &OwnerHotReplicaIdentity) {
        let entry = {
            let mut state = self.state.lock();
            let entry = state.entries.remove(identity);
            if let Some(entry) = entry.as_ref()
                && !entry.dispatched
            {
                state
                    .deadlines
                    .remove(&(entry.next_attempt_at, identity.clone()));
            }
            entry
        };
        if let Some(entry) = entry {
            entry.event.selection_debt.release();
        }
    }

    fn take_for_inflight(
        &self,
        identity: &OwnerHotReplicaIdentity,
    ) -> Option<OwnerHotEvictionEvent> {
        // The inflight guard takes over the same debt token; do not release it.
        let mut state = self.state.lock();
        let entry = state.entries.remove(identity)?;
        if !entry.dispatched {
            state
                .deadlines
                .remove(&(entry.next_attempt_at, identity.clone()));
        }
        Some(entry.event)
    }

    fn len(&self) -> usize {
        self.state.lock().entries.len()
    }
}

pub(crate) struct OwnerPreparedReclaim {
    item: crate::master_kv_router::msg_pack::OwnerReclaimItem,
    cached_info: GetCachedInfo,
    local_snapshot: Option<LocalSnapshotInfo>,
}

pub(crate) struct OwnerSourceEvictionSelection {
    put_id: crate::master_kv_router::put::PutIDForAKey,
    cached_info: GetCachedInfo,
}

pub(crate) enum OwnerReclaimRecord {
    Prepared(OwnerPreparedReclaim),
    /// The local index is fenced and the Commit handler owns the detached
    /// backing while it updates the slot pool. Keeping this marker in the
    /// per-key table lets that O(1) pool update happen without nesting the
    /// pool mutex under the key-shard mutex.
    Releasing(crate::master_kv_router::msg_pack::OwnerReclaimItem),
    Committed(crate::master_kv_router::msg_pack::OwnerReclaimItem),
}

#[derive(Default)]
pub(crate) struct OwnerKeyControlState {
    local_puts: u32,
    /// External Put contexts that may still expose or commit owner-local
    /// backing for this key.  This counter is maintained by an Arc-backed
    /// guard stored in every context, so cache invalidation cannot clear the
    /// reclaim fence while a cloned context is still in use.
    external_pending_puts: u32,
    /// Owner-local pre-Prepare fence installed by the Moka source-eviction
    /// dispatcher.  The matching committed index is moved into this record,
    /// so a new local Get cannot acquire the source between victim selection
    /// and the master's reclaim Prepare RPC.
    source_eviction_selection: Option<OwnerSourceEvictionSelection>,
    reclaim: Option<OwnerReclaimRecord>,
    /// Per-key owner-side Get singleflight marker.  It deliberately lives in
    /// the same fence as local visibility and reclaim so `R ∩ local`,
    /// `R ∩ inflight`, and new leaders are classified atomically.
    external_get: Option<Arc<ExternalGetKeySharedOp>>,
}

impl OwnerKeyControlState {
    fn local_access_fenced(&self) -> bool {
        self.source_eviction_selection.is_some() || self.reclaim.is_some()
    }

    fn is_idle(&self) -> bool {
        self.local_puts == 0
            && self.external_pending_puts == 0
            && self.source_eviction_selection.is_none()
            && self.reclaim.is_none()
            && self.external_get.is_none()
    }
}

const OWNER_KEY_CONTROL_SHARDS: usize = 256;

/// Per-key owner fencing without a process-wide mutex.
///
/// Every operation for one key hashes to the same shard, so local index
/// publication, Get singleflight registration, and reclaim remain linearized.
/// Callers must hold a shard only for one key and must not await, perform RPC,
/// or walk a request batch while holding it.  Unrelated keys normally proceed
/// on independent shards; a hash collision only adds a short O(1) critical
/// section and does not change correctness.
pub(crate) struct OwnerKeyControlTable {
    shards: Box<[Mutex<HashMap<String, OwnerKeyControlState>>]>,
}

impl Default for OwnerKeyControlTable {
    fn default() -> Self {
        Self {
            shards: (0..OWNER_KEY_CONTROL_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }
}

impl OwnerKeyControlTable {
    fn shard_index(key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % OWNER_KEY_CONTROL_SHARDS
    }

    pub(crate) fn lock_key(
        &self,
        key: &str,
    ) -> parking_lot::MutexGuard<'_, HashMap<String, OwnerKeyControlState>> {
        self.shards[Self::shard_index(key)].lock()
    }
}

pub(crate) struct ExternalPendingPutFenceGuard {
    key: String,
    owner_key_control: Arc<OwnerKeyControlTable>,
    owns_local_put: bool,
    local_slot_cleanup_view: Option<ClientKvApiView>,
    local_slot_lease: Mutex<Option<OwnerLocalReserveSlotLease>>,
    local_slot_release_failed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for ExternalPendingPutFenceGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalPendingPutFenceGuard")
            .field("key", &self.key)
            .field("owns_local_put", &self.owns_local_put)
            .finish_non_exhaustive()
    }
}

impl ExternalPendingPutFenceGuard {
    pub(crate) fn attach_local_slot_lease(&self, lease: OwnerLocalReserveSlotLease) {
        assert!(
            self.owns_local_put,
            "only local-first Put owns a slot lease"
        );
        assert_eq!(
            lease.slots.len(),
            1,
            "a pending local-first Put fence must own exactly one slot"
        );
        let mut current = self.local_slot_lease.lock();
        assert!(
            current.is_none(),
            "a pending local-first Put fence cannot replace its slot lease"
        );
        *current = Some(lease);
    }

    /// Transfer the prepared slot to the precommit/committed MemoryInfo.  Once
    /// disarmed, dropping the pending context must not return that resident slot
    /// to the free list.
    pub(crate) fn disarm_local_slot_lease(&self) {
        assert!(
            self.local_slot_lease.lock().take().is_some(),
            "pending local-first Put slot lease is absent while committing"
        );
    }

    pub(crate) async fn release_local_slot_lease_now(
        &self,
        inner: &ClientKvApiInner,
    ) -> KvResult<()> {
        let lease = self.local_slot_lease.lock().take();
        if let Some(lease) = lease {
            if let Err(err) = inner.owner_release_local_reserve_slot_lease(lease).await {
                // The lease object has been consumed and the physical state is
                // now uncertain.  Keep the per-key fence permanently rather
                // than allow reclaim to cross a possibly-live prepared slot.
                self.local_slot_release_failed
                    .store(true, Ordering::Release);
                return Err(err);
            }
        }
        Ok(())
    }
}

fn release_external_pending_put_counts(
    owner_key_control: &Arc<OwnerKeyControlTable>,
    key: &str,
    owns_local_put: bool,
) {
    let mut controls = owner_key_control.lock_key(key);
    let remove = {
        let state = controls
            .get_mut(key)
            .expect("external pending Put fence state missing on release");
        state.external_pending_puts = state
            .external_pending_puts
            .checked_sub(1)
            .expect("external pending Put fence counter underflow");
        if owns_local_put {
            state.local_puts = state
                .local_puts
                .checked_sub(1)
                .expect("owner local-first Put fence counter underflow");
        }
        state.is_idle()
    };
    if remove {
        controls.remove(key);
    }
}

fn acquire_external_pending_put_fence_for_key(
    owner_key_control: &Arc<OwnerKeyControlTable>,
    key: &str,
) -> KvResult<Arc<ExternalPendingPutFenceGuard>> {
    let mut controls = owner_key_control.lock_key(key);
    if controls
        .get(key)
        .is_some_and(|state| state.local_access_fenced())
    {
        return Err(KvError::Api(ApiError::KeyBeingWritten {
            key: key.to_string(),
        }));
    }
    let state = controls.entry(key.to_string()).or_default();
    state.external_pending_puts = state
        .external_pending_puts
        .checked_add(1)
        .expect("external pending Put fence counter overflow");
    Ok(Arc::new(ExternalPendingPutFenceGuard {
        key: key.to_string(),
        owner_key_control: owner_key_control.clone(),
        owns_local_put: false,
        local_slot_cleanup_view: None,
        local_slot_lease: Mutex::new(None),
        local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
    }))
}

impl Drop for ExternalPendingPutFenceGuard {
    fn drop(&mut self) {
        let abandoned_slot_lease = self.local_slot_lease.get_mut().take();
        if let Some(lease) = abandoned_slot_lease {
            let view = self
                .local_slot_cleanup_view
                .as_ref()
                .expect("local-first Put slot cleanup requires an attached owner view")
                .clone();
            let worker_view = view.clone();
            let key = self.key.clone();
            let owner_key_control = self.owner_key_control.clone();
            let owns_local_put = self.owns_local_put;
            view.spawn("external_pending_put_slot_drop_cleanup", async move {
                if let Err(err) = worker_view
                    .client_kv_api()
                    .inner()
                    .owner_release_local_reserve_slot_lease(lease)
                    .await
                {
                    tracing::error!("pending local-first Put slot Drop cleanup failed: {}", err);
                    return;
                }
                release_external_pending_put_counts(&owner_key_control, &key, owns_local_put);
            });
        } else if !self.local_slot_release_failed.load(Ordering::Acquire) {
            release_external_pending_put_counts(
                &self.owner_key_control,
                &self.key,
                self.owns_local_put,
            );
        } else {
            tracing::error!(
                "retaining pending Put fence after local slot release failure: key={}",
                self.key
            );
        }
    }
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

fn owner_hot_source_has_active_holders<T>(selected_source: &Arc<T>) -> bool {
    // A reclaimable committed source has exactly two strong references here:
    // one in get_cached_info and one temporary selection pin. Any additional
    // reference belongs to an active local reader/transfer. Sending such a
    // source to the master would only make Prepare return Busy while its
    // selection debt suppresses choosing a different, reclaimable victim.
    Arc::strong_count(selected_source) > 2
}

enum OwnerHotPinResult<T> {
    Pinned(Arc<T>),
    ReclaimBusy,
    Stale,
}

fn pin_current_owner_hot_source_from_index<T>(
    entry_put_id: crate::master_kv_router::put::PutIDForAKey,
    entry: &Weak<T>,
    resolve_current: impl FnOnce() -> Option<(crate::master_kv_router::put::PutIDForAKey, Arc<T>)>,
) -> OwnerHotPinResult<T> {
    let Some((current_put_id, current)) = resolve_current() else {
        // Do not upgrade the listener's Weak after the local index has
        // disappeared.  A reclaim Prepare may already own the sole Arc and
        // Commit relies on that ownership.  A still-live Weak therefore means
        // "retry after the transient owner transition"; a dead Weak is
        // definitively obsolete.
        return if entry.strong_count() == 0 {
            OwnerHotPinResult::Stale
        } else {
            OwnerHotPinResult::ReclaimBusy
        };
    };
    let Some(pinned) =
        clone_if_owner_hot_entry_matches(current_put_id, &current, entry_put_id, entry)
    else {
        return OwnerHotPinResult::Stale;
    };

    // `resolve_current` clones from the DashMap entry while its shard read
    // guard is held. Reclaim Prepare cannot remove that entry until the clone
    // exists, and its strong-count check will consequently return Busy. This
    // gives us the required per-key pin without the global owner-control lock.
    Some(pinned).map_or(OwnerHotPinResult::Stale, OwnerHotPinResult::Pinned)
}

fn pin_current_owner_hot_source(
    key: &str,
    entry: &OwnerHotCacheEntry,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    counters: &OwnerHotCacheCounters,
) -> OwnerHotPinResult<MemoryInfo> {
    let result = pin_current_owner_hot_source_from_index(entry.put_id, &entry.memory_info, || {
        get_cached_info.get(key).map(|cached| {
            (
                (cached.put_time_ms, cached.put_version),
                cached.mem_holder.clone(),
            )
        })
    });
    match result {
        OwnerHotPinResult::Pinned(pinned) => OwnerHotPinResult::Pinned(pinned),
        OwnerHotPinResult::ReclaimBusy => {
            counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
            OwnerHotPinResult::ReclaimBusy
        }
        OwnerHotPinResult::Stale => {
            counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
            OwnerHotPinResult::Stale
        }
    }
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
    identity: &OwnerHotReplicaIdentity,
) {
    let group = atomic_groups
        .get(identity)
        .map(|entry| entry.value().clone());
    let Some(group) = group else {
        return;
    };
    for member in &group.members {
        atomic_groups.remove(&OwnerHotReplicaIdentity::from_group_member(member));
    }
}

fn pin_current_owner_hot_group_sources(
    group: &crate::master_kv_router::msg_pack::PutAtomicGroup,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    counters: &OwnerHotCacheCounters,
) -> Option<Vec<(OwnerHotReplicaIdentity, Arc<MemoryInfo>)>> {
    let mut sources = Vec::with_capacity(group.members.len());
    for member in &group.members {
        let Some(cached) = get_cached_info.get(&member.key) else {
            counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
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

fn owner_hot_tp_atomic_batch_key_rows(
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

fn pin_current_owner_hot_tp_atomic_batch_sources(
    atomic_batch_key_rows: &[Vec<String>],
    expect_atomic_group: bool,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    atomic_groups: &DashMap<
        OwnerHotReplicaIdentity,
        Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    >,
    counters: &OwnerHotCacheCounters,
) -> Option<Vec<(OwnerHotReplicaIdentity, Arc<MemoryInfo>)>> {
    let mut atomic_batch_sources = Vec::new();
    for rank_keys in atomic_batch_key_rows {
        if rank_keys.is_empty() {
            return None;
        }
        let mut rank_sources = Vec::with_capacity(rank_keys.len());
        for key in rank_keys {
            let Some(cached) = get_cached_info.get(key) else {
                counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
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
        atomic_batch_sources.extend(rank_sources);
    }
    Some(atomic_batch_sources)
}

fn build_owner_hot_cache(
    capacity_bytes: u64,
    counters: Arc<OwnerHotCacheCounters>,
    retry_queue: Arc<OwnerHotRetryQueue>,
    eviction_tx: tokio::sync::ampsc::UnboundedSender<OwnerHotEvictionEvent>,
) -> OwnerHotCache {
    assert!(
        capacity_bytes > 0,
        "owner hot-cache capacity must be positive"
    );
    OwnerHotCache::builder(capacity_bytes)
        .weigher(|_key: &String, entry: &OwnerHotCacheEntry| entry.weight_bytes)
        .eviction_listener(move |key, entry, cause| {
            if cause != RemovalCause::Size {
                return;
            }
            counters.size_evictions.fetch_add(1, Ordering::Relaxed);
            let identity = OwnerHotReplicaIdentity {
                key: (*key).clone(),
                put_time_ms: entry.put_id.0,
                put_version: entry.put_id.1,
            };
            let selection_debt = OwnerHotSelectionDebt::new(
                u64::from(entry.weight_bytes),
                counters.selection_debt_bytes.clone(),
            );
            let event = OwnerHotEvictionEvent {
                key: identity.key.clone(),
                put_id: entry.put_id,
                memory_info: entry.memory_info.clone(),
                selection_debt,
                retry: false,
                source_eviction_atomic_batch: None,
                retry_failures: 0,
            };
            if let Err(err) = eviction_tx.send(event) {
                let event = err.0;
                counters
                    .source_evict_dispatch_failed
                    .fetch_add(1, Ordering::Relaxed);
                retry_queue.schedule(event, "eviction dispatcher closed");
            }
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
    /// Transferred Get targets awaiting an idempotent master GetDone result.
    /// These entries fence reclaim but are never visible to readers.
    pending_local_get_info: DashMap<String, PendingLocalGetInfo>,
    /// key -> local replica version remembered from local put/get durable-replica success.
    /// This authority is positive-only: hit means "can answer exists=true immediately when
    /// allow_local_snapshot is enabled"; miss does not imply non-existence.
    local_snapshot_info: DashMap<String, LocalSnapshotInfo>,
    /// KvOwner-managed resident staging grants for hostless put_start.
    owner_local_reserve_pool: Mutex<OwnerLocalReservePoolState>,
    /// Serialize claims only within one slot-size class. Tokio's mutex provides FIFO
    /// acquisition order for equal-size waiters without making an unrelated class wait
    /// behind a pressured class.
    owner_local_reserve_claim_locks: DashMap<u64, Arc<limit_thirdparty::tokio::sync::AMutex<()>>>,
    /// Wake the background reserve actor after demand/free-slot changes.
    owner_local_reserve_rebalance_notify: Arc<limit_thirdparty::tokio::sync::Notify>,
    /// Owner-local write-back put ids for external local-first path.
    external_local_first_put_id_counter: AtomicU32,
    /// Correlates idempotent owner source-eviction batches in logs and RPC responses.
    next_owner_source_eviction_operation_id: AtomicU64,
    /// Sharded per-key gate for local-first puts, local index access, and reclaim fencing.
    owner_key_control: Arc<OwnerKeyControlTable>,
    /// A weak-value admission/recency tier. It never owns resident memory and
    /// therefore cannot become a second physical-reclaim authority.
    owner_hot_cache: Option<OwnerHotCache>,
    /// Exact selected identities remain here until their backing is physically
    /// Free or the source is restored to owner-hot.
    owner_source_eviction_selected:
        Arc<DashMap<OwnerHotReplicaIdentity, Arc<OwnerHotSelectionDebt>>>,
    owner_hot_atomic_groups: Arc<
        DashMap<OwnerHotReplicaIdentity, Arc<crate::master_kv_router::msg_pack::PutAtomicGroup>>,
    >,
    owner_hot_counters: Arc<OwnerHotCacheCounters>,
    owner_hot_retry_queue: Arc<OwnerHotRetryQueue>,
    owner_hot_eviction_tx: tokio::sync::ampsc::UnboundedSender<OwnerHotEvictionEvent>,
    owner_hot_eviction_rx:
        Mutex<Option<tokio::sync::ampsc::UnboundedReceiver<OwnerHotEvictionEvent>>>,

    /// Shared delete actor input for owner -> external weak-index invalidation.
    pub external_invalidate_delete: EnsureMemholderMgmtDeleteHandle<DeleteClientKvMetaCacheItem>,
    /// Shared delete actor input for owner -> master delete-ack batching.
    pub delete_ack_batch: EnsureMemholderMgmtDeleteHandle<OwnerDeleteAckItem>,
    /// Shared manager for owner -> master delete-ack batching.
    pub owner_delete_ack_mgr: OwnerDeleteAckMemMgr,

    // record external_client get_holding info (owned, flattened manager)
    pub external_get_holding: OwnerExternalMemMgr,
    pub external_get_start_registry: DashMap<u64, ExternalGetStartEntry>,
    /// Metrics-only weak index of active per-key Get flights. Correctness
    /// remains in the sharded key-control table; observing metrics must never
    /// scan or hold those fences.
    external_get_flight_registry: DashMap<String, Weak<ExternalGetKeySharedOp>>,
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
    rpc_caller_batch_put_append_start: RPCCaller<BatchPutAppendStartReq>,
    rpc_caller_put_append_revoke: RPCCaller<PutAppendRevokeReq>,
    rpc_caller_put_append_done: RPCCaller<PutAppendDoneReq>,
    rpc_caller_batch_put_append_done: RPCCaller<BatchPutAppendDoneReq>,
    rpc_caller_batch_evict_owner_source: RPCCaller<BatchEvictOwnerSourceReq>,
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

    pub(crate) fn track_external_get_flight(&self, op: &Arc<ExternalGetKeySharedOp>) {
        self.external_get_flight_registry
            .insert(op.key.clone(), Arc::downgrade(op));
    }

    pub(crate) fn untrack_external_get_flight(&self, op: &Arc<ExternalGetKeySharedOp>) {
        let weak = Arc::downgrade(op);
        self.external_get_flight_registry
            .remove_if(&op.key, |_, current| Weak::ptr_eq(current, &weak));
    }

    fn external_get_flight_snapshot(&self) -> Vec<Arc<ExternalGetKeySharedOp>> {
        let mut ops = Vec::new();
        let mut stale = Vec::new();
        for entry in &self.external_get_flight_registry {
            if let Some(op) = entry.value().upgrade() {
                ops.push(op);
            } else {
                stale.push(entry.key().clone());
            }
        }
        for key in stale {
            self.external_get_flight_registry
                .remove_if(&key, |_, weak| weak.strong_count() == 0);
        }
        ops
    }

    fn owner_hot_register_atomic_group(
        &self,
        group: &crate::master_kv_router::msg_pack::PutAtomicGroup,
    ) {
        register_owner_hot_atomic_group(self.owner_hot_atomic_groups.as_ref(), group);
    }

    fn owner_hot_forget_atomic_group_for_member(&self, identity: &OwnerHotReplicaIdentity) {
        forget_owner_hot_atomic_group(self.owner_hot_atomic_groups.as_ref(), identity);
    }

    pub(crate) fn owner_hot_prepare_eviction(
        &self,
        event: &OwnerHotEvictionEvent,
    ) -> OwnerHotEvictionPreparation {
        let trigger = OwnerHotReplicaIdentity {
            key: event.key.clone(),
            put_time_ms: event.put_id.0,
            put_version: event.put_id.1,
        };
        let cache_entry = OwnerHotCacheEntry {
            put_id: event.put_id,
            memory_info: event.memory_info.clone(),
            weight_bytes: 0,
        };
        let memory_info = match pin_current_owner_hot_source(
            event.key.as_str(),
            &cache_entry,
            self.get_cached_info.as_ref(),
            self.owner_hot_counters.as_ref(),
        ) {
            OwnerHotPinResult::Pinned(memory_info) => memory_info,
            OwnerHotPinResult::ReclaimBusy => {
                return OwnerHotEvictionPreparation::RetryableReclaimFence;
            }
            OwnerHotPinResult::Stale => return OwnerHotEvictionPreparation::Obsolete,
        };

        // A retry reaches this function only when no source-selection atomic_batch
        // has been installed yet. Installed retries are handled from the
        // event's exact `source_eviction_atomic_batch` before this method is called.
        // Therefore a pre-selection retry must resolve the complete current
        // TP/atomic atomic_batch again; degrading it to the trigger key would make
        // the master reject a valid victim as an incomplete atomic_batch forever.
        // AtomicBatch lookup and pinning deliberately happen after the synchronous
        // Moka listener has returned. Each source Arc is cloned directly from
        // its current DashMap entry. A concurrent reclaim must wait for that
        // shard read guard and then observes the extra strong reference, so no
        // global owner-control lock is needed.
        let group = self
            .owner_hot_atomic_groups
            .get(&trigger)
            .map(|entry| entry.value().clone());
        let atomic_batch_sources =
            match owner_hot_tp_atomic_batch_key_rows(&trigger, group.as_deref()) {
                Ok(Some(atomic_batch_key_rows)) => {
                    // pin_current_owner_hot_tp_atomic_batch_sources clones the trigger
                    // again. Drop the probe pin first so the active-holder test
                    // below has the exact index + selection baseline of two Arcs.
                    drop(memory_info);
                    pin_current_owner_hot_tp_atomic_batch_sources(
                        &atomic_batch_key_rows,
                        group.is_some(),
                        self.get_cached_info.as_ref(),
                        self.owner_hot_atomic_groups.as_ref(),
                        self.owner_hot_counters.as_ref(),
                    )
                }
                Ok(None) => match group.as_deref() {
                    Some(group) => {
                        drop(memory_info);
                        pin_current_owner_hot_group_sources(
                            group,
                            self.get_cached_info.as_ref(),
                            self.owner_hot_counters.as_ref(),
                        )
                    }
                    None => Some(vec![(trigger.clone(), memory_info)]),
                },
                Err(()) => None,
            };
        match atomic_batch_sources {
            Some(sources) if !sources.is_empty() => {
                let active_members = sources
                    .iter()
                    .filter(|(_, source)| owner_hot_source_has_active_holders(source))
                    .count();
                if active_members == 0 {
                    OwnerHotEvictionPreparation::Ready { trigger, sources }
                } else {
                    OwnerHotEvictionPreparation::TemporarilyPinned { active_members }
                }
            }
            _ => {
                self.owner_hot_counters
                    .group_trigger_incomplete
                    .fetch_add(1, Ordering::Relaxed);
                OwnerHotEvictionPreparation::RetryableIncompleteAtomicBatch
            }
        }
    }

    pub(crate) fn owner_hot_restore_source_selection(
        &self,
        identity: &OwnerHotReplicaIdentity,
    ) -> bool {
        let mut controls = self.owner_key_control.lock_key(&identity.key);
        let Some(state) = controls.get_mut(&identity.key) else {
            return false;
        };
        let matches = state
            .source_eviction_selection
            .as_ref()
            .is_some_and(|selection| {
                selection.put_id == (identity.put_time_ms, identity.put_version)
            });
        if !matches {
            return false;
        }
        let selection = state
            .source_eviction_selection
            .take()
            .expect("matching owner source selection must exist");
        let replaced = self
            .get_cached_info
            .insert(identity.key.clone(), selection.cached_info);
        assert!(
            replaced.is_none(),
            "rolling back an owner source selection must restore an empty local index"
        );
        if state.is_idle() {
            controls.remove(&identity.key);
        }
        true
    }

    fn owner_hot_install_source_selection_debt(
        &self,
        identity: OwnerHotReplicaIdentity,
        debt: Arc<OwnerHotSelectionDebt>,
    ) -> bool {
        match self.owner_source_eviction_selected.entry(identity) {
            DashMapEntry::Vacant(entry) => {
                entry.insert(debt.clone());
                self.owner_hot_counters
                    .add_source_eviction_selected_bytes(debt.weight_bytes);
                true
            }
            DashMapEntry::Occupied(_) => false,
        }
    }

    fn owner_hot_remove_source_selection_debt(
        &self,
        identity: &OwnerHotReplicaIdentity,
    ) -> Option<Arc<OwnerHotSelectionDebt>> {
        let debt = self
            .owner_source_eviction_selected
            .remove(identity)
            .map(|(_, debt)| debt)?;
        self.owner_hot_counters
            .remove_source_eviction_selected_bytes(debt.weight_bytes);
        Some(debt)
    }

    pub(crate) fn owner_hot_install_source_selection_fences(
        &self,
        sources: &[(OwnerHotReplicaIdentity, Arc<MemoryInfo>)],
    ) -> OwnerHotSelectionFenceOutcome {
        let mut installed = Vec::with_capacity(sources.len());
        let mut outcome = OwnerHotSelectionFenceOutcome::Fenced;

        for (identity, source) in sources {
            let mut controls = self.owner_key_control.lock_key(&identity.key);
            let control_busy = controls.get(&identity.key).is_some_and(|state| {
                state.local_puts != 0
                    || state.external_pending_puts != 0
                    || state.external_get.is_some()
                    || state.local_access_fenced()
            });
            if control_busy
                || self
                    .precommit_local_visible_info
                    .contains_key(&identity.key)
                || self.pending_local_get_info.contains_key(&identity.key)
            {
                outcome = OwnerHotSelectionFenceOutcome::Retryable;
                break;
            }

            let cached_info = self
                .get_cached_info
                .remove_if(&identity.key, |_, cached| {
                    (cached.put_time_ms, cached.put_version)
                        == (identity.put_time_ms, identity.put_version)
                        && Arc::ptr_eq(&cached.mem_holder, source)
                })
                .map(|(_, cached)| cached);
            let Some(cached_info) = cached_info else {
                outcome = OwnerHotSelectionFenceOutcome::Obsolete;
                break;
            };

            let state = controls.entry(identity.key.clone()).or_default();
            assert!(state.source_eviction_selection.is_none());
            state.source_eviction_selection = Some(OwnerSourceEvictionSelection {
                put_id: (identity.put_time_ms, identity.put_version),
                cached_info,
            });
            installed.push(identity.clone());
        }

        if matches!(outcome, OwnerHotSelectionFenceOutcome::Fenced) {
            // Every matching local index is now hidden under its per-key fence.
            // Each source has exactly two expected Arcs: the selection record
            // and this temporary atomic_batch pin.  No new local reader can enter,
            // so an extra reference is an already-active holder and the whole
            // atomic_batch must be rolled back before choosing another victim.
            let active_members = sources
                .iter()
                .filter(|(_, source)| owner_hot_source_has_active_holders(source))
                .count();
            if active_members != 0 {
                outcome = OwnerHotSelectionFenceOutcome::TemporarilyPinned { active_members };
            }
        }

        if !matches!(outcome, OwnerHotSelectionFenceOutcome::Fenced) {
            for identity in installed.iter().rev() {
                assert!(
                    self.owner_hot_restore_source_selection(identity),
                    "partial owner source selection rollback must find every installed fence"
                );
            }
        }
        outcome
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
            [OwnerHotPinAlias::new(memory_info)],
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

    pub(crate) fn owner_hot_admit_published_committed(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        atomic_group: Option<&crate::master_kv_router::msg_pack::PutAtomicGroup>,
    ) -> bool {
        let memory_info = self.get_cached_info.get(key).and_then(|cached| {
            ((cached.put_time_ms, cached.put_version) == put_id).then(|| cached.mem_holder.clone())
        });
        let Some(memory_info) = memory_info else {
            return false;
        };
        self.owner_hot_track_committed(key, put_id, &memory_info, atomic_group);
        true
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
        if cache.get(&key.to_string()).is_some_and(|entry| {
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
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
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
        let identity = OwnerHotReplicaIdentity {
            key: key.to_string(),
            put_time_ms: put_id.0,
            put_version: put_id.1,
        };
        if let Some(cache) = self.owner_hot_cache.as_ref() {
            cache.invalidate_if(&key.to_string(), |entry| entry.put_id == put_id);
        }
        if let Some(debt) = self.owner_hot_remove_source_selection_debt(&identity) {
            debt.release();
            self.owner_hot_counters
                .source_evict_committed_members
                .fetch_add(1, Ordering::Relaxed);
        }
        self.owner_hot_retry_queue.remove(&identity);
        self.owner_hot_forget_atomic_group_for_member(&identity);
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

    pub(crate) fn owner_hot_pin_memory_info(
        &self,
        memory_info: &Arc<MemoryInfo>,
    ) -> Option<PinGuard> {
        self.owner_hot_cache
            .as_ref()?
            .try_pin_alias(OwnerHotPinAlias::new(memory_info))
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
    ) -> KvResult<Arc<ExternalPendingPutFenceGuard>> {
        let mut controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            return Err(KvError::Api(ApiError::KeyBeingWritten {
                key: key.to_string(),
            }));
        }
        if reject_if_exist_same_key
            && (self.precommit_local_visible_info.contains_key(key)
                || self.pending_local_get_info.contains_key(key)
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
        state.external_pending_puts = state
            .external_pending_puts
            .checked_add(1)
            .expect("external pending Put fence counter overflow");
        Ok(Arc::new(ExternalPendingPutFenceGuard {
            key: key.to_string(),
            owner_key_control: self.owner_key_control.clone(),
            owns_local_put: true,
            local_slot_cleanup_view: Some(self.view.clone_view()),
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        }))
    }

    pub(crate) fn acquire_external_pending_put_fence(
        &self,
        key: &str,
    ) -> KvResult<Arc<ExternalPendingPutFenceGuard>> {
        acquire_external_pending_put_fence_for_key(&self.owner_key_control, key)
    }

    pub(crate) fn remember_local_snapshot(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
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
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            return false;
        }
        self.precommit_local_visible_info.contains_key(key)
            || self.get_cached_info.contains_key(key)
            || self.local_snapshot_info.contains_key(key)
    }

    pub(crate) fn local_visible_mem_holder(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        let (memory_info, hot_put_id) = {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
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
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
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
        // Resolve each page under only its own short sharded fence.  The batch
        // itself never owns a synchronous lock.  A cloned MemoryInfo pins the
        // selected backing if reclaim starts after this point.
        let resolved = keys
            .iter()
            .map(|key| {
                let controls = self.owner_key_control.lock_key(key);
                if controls
                    .get(key)
                    .is_some_and(|state| state.local_access_fenced())
                {
                    return None;
                }
                let memory_info = self.local_visible_mem_holder_unfenced(key)?;
                let hot_put_id = self
                    .get_cached_info
                    .get(key)
                    .filter(|cached| Arc::ptr_eq(&cached.mem_holder, &memory_info))
                    .map(|cached| (cached.put_time_ms, cached.put_version));
                Some((memory_info, hot_put_id))
            })
            .collect::<Vec<_>>();
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
        let owner_hot_pin = self.owner_hot_pin_memory_info(&memory_info);
        let previous = self.external_get_holding.inner().insert(
            key,
            ExternalHoldingGetInfo {
                key: memory_info.key.clone(),
                req_node_id: req_node_id.to_string(),
                requester_node_start_time: self
                    .view
                    .cluster_manager()
                    .get_member_info_cached(req_node_id)
                    .map(|member| member.node_start_time),
                memory_info,
                _owner_hot_pin: owner_hot_pin,
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

    pub(crate) fn install_hidden_pending_local_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        addr: u64,
        base_addr: u64,
        len: u32,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<Arc<MemoryInfo>> {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
            || self.pending_local_get_info.contains_key(key)
        {
            return Err(KvError::Api(ApiError::KeyBeingWritten {
                key: key.to_string(),
            }));
        }
        self.owner_mark_local_reserve_slot_pending_visible(slot_size, grant_id, slot_index)?;
        self.owner_retain_local_reserve_resident_slot_holder(slot_size, grant_id, slot_index)?;
        let resident_owner_node_id: NodeID = self.view.cluster_manager().get_self_info().id.into();
        let memory_info = Arc::new(MemoryInfo::new_local_reserve_resident_with_base(
            addr,
            base_addr,
            len,
            key.to_string(),
            resident_owner_node_id,
            self.view.clone(),
            slot_size,
            grant_id,
            slot_index,
        ));
        let previous = self.pending_local_get_info.insert(
            key.to_string(),
            PendingLocalGetInfo {
                get_id,
                put_id,
                mem_holder: memory_info.clone(),
            },
        );
        assert!(
            previous.is_none(),
            "pending local Get must be unique per key"
        );
        drop(controls);
        Ok(memory_info)
    }

    pub(crate) fn abort_hidden_pending_local_get(&self, key: &str, get_id: u64) -> bool {
        let _controls = self.owner_key_control.lock_key(key);
        self.pending_local_get_info
            .remove_if(key, |_, pending| pending.get_id == get_id)
            .is_some()
    }

    pub(crate) fn promote_hidden_pending_local_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> KvResult<Arc<MemoryInfo>> {
        let memory_info = {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                return Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }));
            }
            let Some(pending_memory_info) =
                self.pending_local_get_info.get(key).and_then(|pending| {
                    (pending.get_id == get_id && pending.put_id == put_id)
                        .then(|| pending.mem_holder.clone())
                })
            else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "hidden pending local Get is absent: key={} get_id={}",
                        key, get_id
                    ),
                }));
            };
            let (slot_size, grant_id, slot_index) = pending_memory_info
                .local_reserve_resident_slot_ref()
                .expect("pending local Get must carry a local-reserve slot");
            self.owner_promote_local_reserve_pending_slot_to_committed(
                slot_size, grant_id, slot_index,
            )?;
            let removed = self
                .pending_local_get_info
                .remove_if(key, |_, pending| {
                    pending.get_id == get_id
                        && pending.put_id == put_id
                        && Arc::ptr_eq(&pending.mem_holder, &pending_memory_info)
                })
                .is_some();
            if !removed {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "hidden pending local Get changed while promoting: key={} get_id={}",
                        key, get_id
                    ),
                }));
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: pending_memory_info.clone(),
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
            drop(controls);
            pending_memory_info
        };
        Ok(memory_info)
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
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
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
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
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
        let controls = self.owner_key_control.lock_key(key);
        assert!(
            !controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced()),
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
        let _controls = self.owner_key_control.lock_key(key);
        self.precommit_local_visible_info
            .remove_if(key, |_, info| {
                Arc::ptr_eq(&info.mem_holder, expected_mem_holder)
            })
            .is_some()
    }

    pub fn precommit_local_visible_memory_info(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        self.precommit_local_visible_info
            .get(key)
            .map(|info| info.mem_holder.clone())
    }

    pub(crate) fn committed_local_reserve_slot_is_current(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        expected: &crate::master_kv_router::msg_pack::PutDoneCommittedSlot,
    ) -> bool {
        self.get_cached_info.get(key).is_some_and(|cached| {
            (cached.put_time_ms, cached.put_version) == put_id
                && cached.mem_holder.local_reserve_resident_slot_ref()
                    == Some((expected.slot_size, expected.grant_id, expected.slot_index))
        })
    }

    pub fn promote_precommit_local_reserve_resident_slot_if_same(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: Arc<MemoryInfo>,
        _atomic_group: Option<&crate::master_kv_router::msg_pack::PutAtomicGroup>,
    ) -> KvResult<()> {
        {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
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
    /// Keep the per-key reclaim fence alive for every cache/user clone of this
    /// pending context.  The counter is released only by the final Arc drop.
    pub(crate) _pending_fence: Arc<ExternalPendingPutFenceGuard>,
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
    /// Replica tasks are independent hit-rate work and keep their transfer
    /// source alive until the transfer finishes. Capacity eviction never enters
    /// this queue.
    pub holder: Option<Arc<UserMemHolder>>,
    pub target: Option<ReplicaTaskTarget>,
    pub preferred_sub_cluster: Option<String>,
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
    /// Stable grant identity -> current index in `grants`. Every vector removal uses
    /// swap-remove and repairs the one moved entry, so per-slot state transitions never
    /// scan all grants.
    grant_indices: HashMap<u64, usize>,
    /// Dense set of grants which currently contain at least one free slot. This avoids
    /// scanning full grants while assembling a lease.
    claimable_grant_ids: Vec<u64>,
    claimable_grant_indices: HashMap<u64, usize>,
    free_slots: usize,
    prepared_slots: usize,
    pending_visible_slots: usize,
    committed_slots: usize,
    pub last_grow_at: Option<Instant>,
    pub pending_slot_demand: usize,
    pub max_observed_claim_slots: usize,
    pub expected_grant_count: usize,
}

impl OwnerLocalReserveClassState {
    pub fn new(slot_size: u64, slots_per_grant: u32) -> Self {
        Self {
            slot_size,
            slots_per_grant,
            grants: Vec::new(),
            grant_indices: HashMap::new(),
            claimable_grant_ids: Vec::new(),
            claimable_grant_indices: HashMap::new(),
            free_slots: 0,
            prepared_slots: 0,
            pending_visible_slots: 0,
            committed_slots: 0,
            last_grow_at: None,
            pending_slot_demand: 0,
            max_observed_claim_slots: 0,
            expected_grant_count: 0,
        }
    }

    pub fn free_slot_count(&self) -> usize {
        self.free_slots
    }

    pub fn used_slot_count(&self) -> usize {
        self.prepared_slots
            .saturating_add(self.pending_visible_slots)
            .saturating_add(self.committed_slots)
    }

    pub fn grant_count(&self) -> usize {
        self.grants.len()
    }

    pub fn prepared_slot_count(&self) -> usize {
        self.prepared_slots
    }

    pub fn pending_visible_slot_count(&self) -> usize {
        self.pending_visible_slots
    }

    pub fn committed_slot_count(&self) -> usize {
        self.committed_slots
    }

    fn add_claimable_grant(&mut self, grant_id: u64) {
        if self.claimable_grant_indices.contains_key(&grant_id) {
            return;
        }
        let index = self.claimable_grant_ids.len();
        self.claimable_grant_ids.push(grant_id);
        let previous = self.claimable_grant_indices.insert(grant_id, index);
        assert!(
            previous.is_none(),
            "duplicate claimable local-reserve grant"
        );
    }

    fn remove_claimable_grant(&mut self, grant_id: u64) {
        let Some(index) = self.claimable_grant_indices.remove(&grant_id) else {
            return;
        };
        let removed = self.claimable_grant_ids.swap_remove(index);
        assert_eq!(removed, grant_id, "claimable grant index drift");
        if let Some(moved_grant_id) = self.claimable_grant_ids.get(index).copied() {
            let moved_index = self
                .claimable_grant_indices
                .get_mut(&moved_grant_id)
                .expect("moved claimable grant must remain indexed");
            *moved_index = index;
        }
    }

    pub fn install_grant(&mut self, grant: OwnerLocalReserveGrantState) {
        assert_eq!(grant.slot_size, self.slot_size, "grant slot-size drift");
        assert_eq!(
            grant.slot_count, self.slots_per_grant,
            "grant slot-count drift"
        );
        assert!(
            !self.grant_indices.contains_key(&grant.grant_id),
            "duplicate local-reserve grant id"
        );

        let mut free = 0usize;
        let mut prepared = 0usize;
        let mut pending_visible = 0usize;
        let mut committed = 0usize;
        for state in &grant.slot_states {
            match state {
                OwnerLocalReserveSlotState::Free => free += 1,
                OwnerLocalReserveSlotState::Prepared => prepared += 1,
                OwnerLocalReserveSlotState::PendingLocalVisible { .. } => pending_visible += 1,
                OwnerLocalReserveSlotState::Committed { .. } => committed += 1,
            }
        }
        assert_eq!(free, grant.free_slots.len(), "grant free-slot index drift");

        let grant_id = grant.grant_id;
        let index = self.grants.len();
        self.grants.push(grant);
        let previous = self.grant_indices.insert(grant_id, index);
        assert!(previous.is_none(), "duplicate local-reserve grant id");
        if free != 0 {
            self.add_claimable_grant(grant_id);
        }
        self.free_slots = self
            .free_slots
            .checked_add(free)
            .expect("free slot overflow");
        self.prepared_slots = self
            .prepared_slots
            .checked_add(prepared)
            .expect("prepared slot overflow");
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_add(pending_visible)
            .expect("pending-visible slot overflow");
        self.committed_slots = self
            .committed_slots
            .checked_add(committed)
            .expect("committed slot overflow");
    }

    pub fn claim_available(&mut self, max_slots: usize) -> Vec<OwnerLocalReserveSlotRef> {
        let claim_count = self.free_slots.min(max_slots);
        let mut slots = Vec::with_capacity(claim_count);
        while slots.len() < claim_count {
            let grant_id = *self
                .claimable_grant_ids
                .last()
                .expect("free-slot counter requires a claimable grant");
            let grant_index = *self
                .grant_indices
                .get(&grant_id)
                .expect("claimable grant must be installed");
            let (slot, exhausted) = {
                let grant = &mut self.grants[grant_index];
                let slot = grant
                    .claim_prepared_slot()
                    .expect("claimable grant must contain a free slot");
                (slot, grant.free_slots.is_empty())
            };
            self.free_slots = self
                .free_slots
                .checked_sub(1)
                .expect("free slot counter underflow");
            self.prepared_slots = self
                .prepared_slots
                .checked_add(1)
                .expect("prepared slot overflow");
            if exhausted {
                self.remove_claimable_grant(grant_id);
            }
            slots.push(slot);
        }
        slots
    }

    fn grant_index(&self, grant_id: u64) -> Option<usize> {
        self.grant_indices.get(&grant_id).copied()
    }

    pub fn grant(&self, grant_id: u64) -> Option<&OwnerLocalReserveGrantState> {
        self.grant_index(grant_id)
            .and_then(|index| self.grants.get(index))
    }

    pub fn release_prepared_slot(&mut self, grant_id: u64, slot_index: u32) -> bool {
        let Some(grant_index) = self.grant_index(grant_id) else {
            return false;
        };
        let was_exhausted = self.grants[grant_index].free_slots.is_empty();
        self.grants[grant_index].release_prepared_slot(slot_index);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        self.free_slots = self.free_slots.checked_add(1).expect("free slot overflow");
        if was_exhausted {
            self.add_claimable_grant(grant_id);
        }
        true
    }

    pub fn mark_prepared_slot_pending_visible(&mut self, grant_id: u64, slot_index: u32) -> bool {
        let Some(grant_index) = self.grant_index(grant_id) else {
            return false;
        };
        self.grants[grant_index].mark_prepared_slot_pending_visible(slot_index);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_add(1)
            .expect("pending-visible slot overflow");
        true
    }

    pub fn promote_pending_visible_slot_to_committed(
        &mut self,
        grant_id: u64,
        slot_index: u32,
    ) -> bool {
        let Some(grant_index) = self.grant_index(grant_id) else {
            return false;
        };
        self.grants[grant_index].promote_pending_visible_slot_to_committed(slot_index);
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_sub(1)
            .expect("pending-visible slot counter underflow");
        self.committed_slots = self
            .committed_slots
            .checked_add(1)
            .expect("committed slot overflow");
        true
    }

    pub fn retain_resident_slot_holder(&mut self, grant_id: u64, slot_index: u32) -> bool {
        let Some(grant_index) = self.grant_index(grant_id) else {
            return false;
        };
        self.grants[grant_index].retain_resident_slot_holder(slot_index);
        true
    }

    pub fn release_resident_slot_holder(&mut self, grant_id: u64, slot_index: u32) -> bool {
        let Some(grant_index) = self.grant_index(grant_id) else {
            return false;
        };
        let (was_exhausted, prior_state, became_free) = {
            let grant = &mut self.grants[grant_index];
            let was_exhausted = grant.free_slots.is_empty();
            let prior_state = grant
                .slot_states
                .get(slot_index as usize)
                .expect("slot state index out of range")
                .clone();
            let free_before = grant.free_slots.len();
            grant.release_resident_slot_holder(slot_index);
            (
                was_exhausted,
                prior_state,
                grant.free_slots.len() != free_before,
            )
        };
        if became_free {
            match prior_state {
                OwnerLocalReserveSlotState::PendingLocalVisible { .. } => {
                    self.pending_visible_slots = self
                        .pending_visible_slots
                        .checked_sub(1)
                        .expect("pending-visible slot counter underflow");
                }
                OwnerLocalReserveSlotState::Committed { .. } => {
                    self.committed_slots = self
                        .committed_slots
                        .checked_sub(1)
                        .expect("committed slot counter underflow");
                }
                _ => unreachable!("resident holder must belong to a resident slot"),
            }
            self.free_slots = self.free_slots.checked_add(1).expect("free slot overflow");
            if was_exhausted {
                self.add_claimable_grant(grant_id);
            }
        }
        true
    }

    pub fn release_committed_slot_route(&mut self, grant_id: u64, slot_index: u32) -> bool {
        let Some(grant_index) = self.grant_index(grant_id) else {
            return false;
        };
        let (was_exhausted, became_free) = {
            let grant = &mut self.grants[grant_index];
            let was_exhausted = grant.free_slots.is_empty();
            let free_before = grant.free_slots.len();
            grant.release_committed_slot_route(slot_index);
            (was_exhausted, grant.free_slots.len() != free_before)
        };
        if became_free {
            self.committed_slots = self
                .committed_slots
                .checked_sub(1)
                .expect("committed slot counter underflow");
            self.free_slots = self.free_slots.checked_add(1).expect("free slot overflow");
            if was_exhausted {
                self.add_claimable_grant(grant_id);
            }
        }
        true
    }

    /// Drop the route reference and the resident `MemoryInfo` reference as one
    /// slot-pool transaction. This is the normal owner-reclaim transition and
    /// avoids exposing an intermediate state across two pool lock acquisitions.
    pub fn release_committed_resident_slot(&mut self, grant_id: u64, slot_index: u32) -> bool {
        if self.grant_index(grant_id).is_none() {
            return false;
        }
        assert!(self.release_committed_slot_route(grant_id, slot_index));
        assert!(self.release_resident_slot_holder(grant_id, slot_index));
        true
    }

    pub fn detach_fully_free_grant(
        &mut self,
        grant_id: u64,
    ) -> Option<OwnerLocalReserveGrantState> {
        let grant_index = self.grant_indices.remove(&grant_id)?;
        assert!(
            self.grants[grant_index].is_fully_free(),
            "only a fully-free grant may be detached"
        );
        self.remove_claimable_grant(grant_id);
        let grant = self.grants.swap_remove(grant_index);
        assert_eq!(grant.grant_id, grant_id, "grant index drift");
        if let Some(moved_grant) = self.grants.get(grant_index) {
            let moved_index = self
                .grant_indices
                .get_mut(&moved_grant.grant_id)
                .expect("moved grant must remain indexed");
            *moved_index = grant_index;
        }
        self.free_slots = self
            .free_slots
            .checked_sub(grant.slot_count as usize)
            .expect("free slot counter underflow");
        Some(grant)
    }

    pub fn take_all_grants(&mut self) -> Vec<OwnerLocalReserveGrantState> {
        self.grant_indices.clear();
        self.claimable_grant_ids.clear();
        self.claimable_grant_indices.clear();
        self.free_slots = 0;
        self.prepared_slots = 0;
        self.pending_visible_slots = 0;
        self.committed_slots = 0;
        std::mem::take(&mut self.grants)
    }
}

#[cfg(test)]
mod owner_reclaim_slot_tests {
    use super::{
        ExternalPendingPutCtx, ExternalPendingPutFenceGuard, OwnerHotCacheCounters,
        OwnerHotCacheEntry, OwnerHotEvictionEvent, OwnerHotPinAlias, OwnerHotReplicaIdentity,
        OwnerHotRetryQueue, OwnerHotSelectionDebt, OwnerHotSelectionFenceOutcome,
        OwnerKeyControlState, OwnerKeyControlTable, OwnerLocalReserveGrantState,
        OwnerLocalReserveSlotLease, OwnerLocalReserveSlotRef, OwnerLocalReserveSlotState,
        OwnerReclaimRecord, acquire_external_pending_put_fence_for_key,
        allocate_external_holding_id, build_owner_hot_cache, clone_if_owner_hot_entry_matches,
        forget_owner_hot_atomic_group, owner_hot_source_has_active_holders,
        owner_hot_tp_atomic_batch_key_rows, pin_current_owner_hot_source_from_index,
        register_owner_hot_atomic_group,
    };
    use crate::master_kv_router::msg_pack::{
        BatchOwnerReclaimReq, OwnerReclaimBacking, OwnerReclaimItem, OwnerReclaimItemState,
        OwnerReclaimPhase, OwnerReclaimReason, PutAtomicGroup, PutAtomicGroupMember,
    };
    use crate::p2p::msg_pack::MsgPack;
    use dashmap::DashMap;
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Weak};
    use std::time::{Duration, Instant};

    fn pending_put_count(controls: &Arc<OwnerKeyControlTable>, key: &str) -> Option<u32> {
        controls
            .lock_key(key)
            .get(key)
            .map(|state| state.external_pending_puts)
    }

    fn controls_with_state(key: &str, state: OwnerKeyControlState) -> Arc<OwnerKeyControlTable> {
        let controls = Arc::new(OwnerKeyControlTable::default());
        controls.lock_key(key).insert(key.to_string(), state);
        controls
    }

    #[test]
    fn external_pending_put_guard_clone_releases_only_after_last_drop() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let guard = acquire_external_pending_put_fence_for_key(&controls, "clone-key")
            .expect("pending fence acquisition must succeed");
        let clone = guard.clone();
        assert_eq!(pending_put_count(&controls, "clone-key"), Some(1));

        drop(guard);
        assert_eq!(pending_put_count(&controls, "clone-key"), Some(1));
        drop(clone);
        assert_eq!(pending_put_count(&controls, "clone-key"), None);
    }

    #[test]
    fn stale_pending_put_guard_drop_cannot_erase_new_generation() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let old = acquire_external_pending_put_fence_for_key(&controls, "aba-key")
            .expect("old pending fence acquisition must succeed");
        let new = acquire_external_pending_put_fence_for_key(&controls, "aba-key")
            .expect("new pending fence acquisition must succeed");
        assert_eq!(pending_put_count(&controls, "aba-key"), Some(2));

        drop(old);
        assert_eq!(pending_put_count(&controls, "aba-key"), Some(1));
        drop(new);
        assert_eq!(pending_put_count(&controls, "aba-key"), None);
    }

    #[test]
    fn local_first_pending_guard_releases_both_key_counters() {
        let controls = controls_with_state(
            "local-key",
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
            },
        );
        let guard = Arc::new(ExternalPendingPutFenceGuard {
            key: "local-key".to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        });

        drop(guard);
        assert!(controls.lock_key("local-key").get("local-key").is_none());
    }

    #[test]
    fn failed_local_slot_release_keeps_key_fence_closed() {
        let controls = controls_with_state(
            "failed-slot",
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
            },
        );
        let guard = Arc::new(ExternalPendingPutFenceGuard {
            key: "failed-slot".to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(true),
        });

        drop(guard);
        let controls = controls.lock_key("failed-slot");
        assert_eq!(controls["failed-slot"].local_puts, 1);
        assert_eq!(controls["failed-slot"].external_pending_puts, 1);
    }

    #[test]
    fn committed_local_first_slot_disarms_drop_cleanup_before_fence_release() {
        let controls = controls_with_state(
            "committed-slot",
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
            },
        );
        let guard = Arc::new(ExternalPendingPutFenceGuard {
            key: "committed-slot".to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        });
        guard.attach_local_slot_lease(OwnerLocalReserveSlotLease {
            value_len: 8,
            slot_size: 8,
            slots: vec![OwnerLocalReserveSlotRef {
                grant_id: 7,
                slot_index: 2,
                ptr: 0x1008,
                base_addr: 0x1000,
            }],
        });

        guard.disarm_local_slot_lease();
        drop(guard);
        assert!(
            controls
                .lock_key("committed-slot")
                .get("committed-slot")
                .is_none()
        );
    }

    #[test]
    fn pending_ctx_clone_keeps_fence_after_explicit_cache_invalidation() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let fence = acquire_external_pending_put_fence_for_key(&controls, "cached-key")
            .expect("pending fence acquisition must succeed");
        let cache = moka::sync::Cache::new(1);
        let identity = ("cached-key".to_string(), 10, 2);
        cache.insert(
            identity.clone(),
            ExternalPendingPutCtx {
                peer_id: None,
                src_offset: 0,
                target_base_addr: 0,
                target_offset: 0,
                len: 1,
                make_replica_task: false,
                preferred_sub_cluster: None,
                replica_target: None,
                local_reserve_slot: None,
                local_reserve_slot_size: None,
                atomic_group: None,
                _pending_fence: fence,
            },
        );
        let clone = cache.get(&identity).expect("pending ctx must exist");
        cache.invalidate(&identity);
        cache.run_pending_tasks();
        assert_eq!(pending_put_count(&controls, "cached-key"), Some(1));

        drop(clone);
        assert_eq!(pending_put_count(&controls, "cached-key"), None);
    }

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
    fn pressure_victim_rejects_an_extra_active_holder() {
        let indexed = Arc::new(7u64);
        let selected = indexed.clone();
        assert_eq!(Arc::strong_count(&selected), 2);
        assert!(
            !owner_hot_source_has_active_holders(&selected),
            "index plus the temporary selection pin is reclaimable"
        );

        let active_reader = indexed.clone();
        assert_eq!(Arc::strong_count(&selected), 3);
        assert!(owner_hot_source_has_active_holders(&selected));

        drop(active_reader);
        assert!(!owner_hot_source_has_active_holders(&selected));
    }

    #[limit_thirdparty::tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn source_selection_fence_closes_late_get_and_rolls_back_whole_atomic_batch() {
        use crate::client_kv_api::PutOptionalArgs;
        use crate::kvcore_test_lib::{
            integration_test_lock, start_master_and_client, stop_master_and_client,
        };

        let _test_guard = integration_test_lock().await;
        let (master, client) = start_master_and_client(
            "source_selection_fence_master",
            "source_selection_fence_owner",
        )
        .await;
        let owner_view = client.client_kv_api_view();
        let inner = owner_view.client_kv_api().inner();
        let keys = ["selection-rank0", "selection-rank1"];
        for key in keys {
            inner
                .put(key, &[7u8; 4096], PutOptionalArgs::default())
                .await
                .expect("owner put must publish a committed route");
            let (holder, _remote) = inner
                .get(key)
                .await
                .expect("owner get must succeed")
                .expect("committed route must be readable");
            drop(holder);
        }

        let sources = keys
            .iter()
            .map(|key| {
                let cached = inner
                    .get_cached_info
                    .get(*key)
                    .expect("committed owner source must be indexed");
                (
                    OwnerHotReplicaIdentity {
                        key: (*key).to_string(),
                        put_time_ms: cached.put_time_ms,
                        put_version: cached.put_version,
                    },
                    cached.mem_holder.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            sources
                .iter()
                .all(|(_, source)| !owner_hot_source_has_active_holders(source)),
            "the initial victim check must see only index plus selection pins"
        );

        // Reproduce the r11 TOCTOU exactly: a local Get acquires one atomic_batch
        // member after the dispatcher's first holder check but before the
        // source-selection fence is installed.
        let late_reader = inner
            .local_visible_mem_holder(keys[1])
            .expect("late local Get must acquire the source before fencing");
        assert!(matches!(
            inner.owner_hot_install_source_selection_fences(&sources),
            OwnerHotSelectionFenceOutcome::TemporarilyPinned { active_members: 1 }
        ));
        for key in keys {
            assert!(inner.get_cached_info.contains_key(key));
            assert!(
                inner
                    .owner_key_control
                    .lock_key(key)
                    .get(key)
                    .is_none_or(|state| state.source_eviction_selection.is_none()),
                "a pinned member must roll back every partial atomic_batch fence"
            );
        }

        drop(late_reader);
        assert!(matches!(
            inner.owner_hot_install_source_selection_fences(&sources),
            OwnerHotSelectionFenceOutcome::Fenced
        ));
        for key in keys {
            assert!(!inner.get_cached_info.contains_key(key));
            assert!(inner.local_visible_mem_holder(key).is_none());
            assert!(
                acquire_external_pending_put_fence_for_key(&inner.owner_key_control, key).is_err(),
                "a new local Put must not cross a source-selection fence"
            );
        }

        let identities = sources
            .iter()
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        drop(sources);
        let master_id = master
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;

        let mismatched = OwnerReclaimItem {
            key: identities[0].key.clone(),
            put_id: (
                identities[0].put_time_ms,
                identities[0].put_version.wrapping_add(1),
            ),
            epoch: 90,
            backing: OwnerReclaimBacking::Allocation,
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        };
        let mismatch_resp = super::reclaim::handle_batch_owner_reclaim(
            &owner_view,
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase: OwnerReclaimPhase::Prepare,
                    items: vec![mismatched],
                },
                raw_bytes: Vec::new(),
            },
            master_id.clone().into(),
        )
        .await;
        assert_eq!(
            mismatch_resp.serialize_part.items[0].state,
            OwnerReclaimItemState::Busy
        );
        assert!(
            inner
                .owner_key_control
                .lock_key(&identities[0].key)
                .get(&identities[0].key)
                .is_some_and(|state| state.source_eviction_selection.is_some()),
            "a mismatched Prepare must not consume the owner selection"
        );

        let matching = identities
            .iter()
            .enumerate()
            .map(|(index, identity)| OwnerReclaimItem {
                key: identity.key.clone(),
                put_id: (identity.put_time_ms, identity.put_version),
                epoch: 100 + index as u64,
                backing: OwnerReclaimBacking::Allocation,
                reason: OwnerReclaimReason::OwnerCapacityEviction,
            })
            .collect::<Vec<_>>();
        let prepare_resp = super::reclaim::handle_batch_owner_reclaim(
            &owner_view,
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase: OwnerReclaimPhase::Prepare,
                    items: matching.clone(),
                },
                raw_bytes: Vec::new(),
            },
            master_id.clone().into(),
        )
        .await;
        assert!(
            prepare_resp
                .serialize_part
                .items
                .iter()
                .all(|item| item.state == OwnerReclaimItemState::Prepared),
            "matching Prepare must atomically promote each selection into reclaim"
        );
        for key in keys {
            assert!(inner.local_visible_mem_holder(key).is_none());
        }

        let abort_resp = super::reclaim::handle_batch_owner_reclaim(
            &owner_view,
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase: OwnerReclaimPhase::Abort,
                    items: matching,
                },
                raw_bytes: Vec::new(),
            },
            master_id.into(),
        )
        .await;
        assert!(
            abort_resp
                .serialize_part
                .items
                .iter()
                .all(|item| item.state == OwnerReclaimItemState::Aborted)
        );
        for key in keys {
            assert!(
                inner.local_visible_mem_holder(key).is_some(),
                "Abort must restore the exact committed local index"
            );
        }

        let group = PutAtomicGroup {
            members: identities
                .iter()
                .map(|identity| PutAtomicGroupMember {
                    key: identity.key.clone(),
                    put_id: (identity.put_time_ms, identity.put_version),
                })
                .collect(),
        };
        inner.owner_hot_register_atomic_group(&group);
        let trigger_weak = {
            let cached = inner
                .get_cached_info
                .get(&identities[0].key)
                .expect("Abort-restored trigger must be indexed");
            Arc::downgrade(&cached.mem_holder)
        };
        let retry_event = OwnerHotEvictionEvent {
            key: identities[0].key.clone(),
            put_id: (identities[0].put_time_ms, identities[0].put_version),
            memory_info: trigger_weak,
            selection_debt: OwnerHotSelectionDebt::new(
                4096,
                inner.owner_hot_counters.selection_debt_bytes.clone(),
            ),
            retry: true,
            source_eviction_atomic_batch: None,
            retry_failures: 1,
        };
        match inner.owner_hot_prepare_eviction(&retry_event) {
            super::OwnerHotEvictionPreparation::Ready { sources, .. } => {
                assert_eq!(
                    sources.len(),
                    identities.len(),
                    "a pre-selection retry must resolve the complete atomic atomic_batch"
                );
            }
            _ => panic!("a pre-selection retry with a complete atomic_batch must be ready"),
        }
        retry_event.selection_debt.release();

        stop_master_and_client(master, client).await;
    }

    #[test]
    fn dispatcher_pin_and_reclaim_prepare_are_serialized_by_the_current_index() {
        let current = Mutex::new(Some(((10, 2), Arc::new(7u64))));
        let weak = Arc::downgrade(&current.lock().as_ref().unwrap().1);

        let pinned = match pin_current_owner_hot_source_from_index((10, 2), &weak, || {
            current
                .lock()
                .as_ref()
                .map(|(put_id, value)| (*put_id, value.clone()))
        }) {
            super::OwnerHotPinResult::Pinned(pinned) => pinned,
            _ => panic!("current source must pin under the owner fence"),
        };
        let prepared_after_pin = current.lock().take().unwrap().1;
        assert_eq!(Arc::strong_count(&prepared_after_pin), 2);
        drop(pinned);
        assert_eq!(Arc::strong_count(&prepared_after_pin), 1);

        // If Prepare wins and moves the sole Arc out of the local index, the
        // dispatcher observes an absent current entry. It must not upgrade the
        // Weak that now points into Prepared, or Commit's try_unwrap could race
        // an unexpected second holder.
        let current = Mutex::new(Some(((11, 3), Arc::new(9u64))));
        let weak = Arc::downgrade(&current.lock().as_ref().unwrap().1);
        let prepared_before_pin = current.lock().take().unwrap().1;
        assert!(matches!(
            pin_current_owner_hot_source_from_index((11, 3), &weak, || {
                current
                    .lock()
                    .as_ref()
                    .map(|(put_id, value)| (*put_id, value.clone()))
            }),
            super::OwnerHotPinResult::ReclaimBusy
        ));
        assert_eq!(Arc::strong_count(&prepared_before_pin), 1);

        drop(prepared_before_pin);
        assert!(matches!(
            pin_current_owner_hot_source_from_index((11, 3), &weak, || None),
            super::OwnerHotPinResult::Stale
        ));
    }

    #[test]
    fn owner_hot_retry_queue_is_exactly_once_and_keeps_selection_debt() {
        let counters = Arc::new(OwnerHotCacheCounters::default());
        let retry_queue = OwnerHotRetryQueue::new(counters.clone());
        let identity = OwnerHotReplicaIdentity {
            key: "retry-key".to_string(),
            put_time_ms: 12,
            put_version: 3,
        };
        let debt = OwnerHotSelectionDebt::new(64, counters.selection_debt_bytes.clone());
        let event = OwnerHotEvictionEvent {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version),
            memory_info: Weak::new(),
            selection_debt: debt,
            retry: true,
            source_eviction_atomic_batch: None,
            retry_failures: 0,
        };
        retry_queue.schedule(event.clone(), "first failure");
        retry_queue.schedule(event, "duplicate failure");
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 64);

        let due = retry_queue.take_due_batch(Instant::now() + Duration::from_secs(10), 128);
        assert_eq!(due.len(), 1);
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 64);
        assert!(
            retry_queue
                .take_due_batch(Instant::now() + Duration::from_secs(30), 128)
                .is_empty(),
            "a dispatched retry stays exactly once until dispatcher acknowledgement"
        );
        let accepted = retry_queue
            .take_for_inflight(&identity)
            .expect("dispatcher acknowledgement must take the authoritative event");
        accepted.selection_debt.release();
        assert_eq!(retry_queue.len(), 0);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn owner_hot_retry_queue_deadlines_stay_bounded_under_high_churn() {
        const CHURN: usize = 20_000;

        let counters = Arc::new(OwnerHotCacheCounters::default());
        let retry_queue = OwnerHotRetryQueue::new(counters.clone());
        let identity = OwnerHotReplicaIdentity {
            key: "retry-churn".to_string(),
            put_time_ms: 21,
            put_version: 5,
        };
        let debt = OwnerHotSelectionDebt::new(64, counters.selection_debt_bytes.clone());
        let event = OwnerHotEvictionEvent {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version),
            memory_info: Weak::new(),
            selection_debt: debt,
            retry: true,
            source_eviction_atomic_batch: None,
            retry_failures: 0,
        };

        for _ in 0..CHURN {
            retry_queue.schedule(event.clone(), "repeated failure");
        }
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 1);

        let due = retry_queue.take_due_batch(Instant::now() + Duration::from_secs(10), 1);
        assert_eq!(due.len(), 1);
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 0);

        for _ in 0..CHURN {
            retry_queue.schedule(due[0].clone(), "repeated dispatcher failure");
        }
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 1);

        let event = retry_queue
            .take_for_inflight(&identity)
            .expect("live retry must remain available to the dispatcher");
        assert_eq!(retry_queue.len(), 0);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 0);
        event.selection_debt.release();
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 0);

        let remove_identity = OwnerHotReplicaIdentity {
            key: "retry-remove-churn".to_string(),
            put_time_ms: 22,
            put_version: 6,
        };
        let remove_debt = OwnerHotSelectionDebt::new(32, counters.selection_debt_bytes.clone());
        let remove_event = OwnerHotEvictionEvent {
            key: remove_identity.key.clone(),
            put_id: (remove_identity.put_time_ms, remove_identity.put_version),
            memory_info: Weak::new(),
            selection_debt: remove_debt,
            retry: true,
            source_eviction_atomic_batch: None,
            retry_failures: 0,
        };
        for _ in 0..CHURN {
            retry_queue.schedule(remove_event.clone(), "remove churn");
        }
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 1);
        retry_queue.remove(&remove_identity);
        assert_eq!(retry_queue.len(), 0);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 0);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 0);
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

        forget_owner_hot_atomic_group(&atomic_groups, &second);
        assert!(atomic_groups.is_empty());
    }

    #[test]
    fn owner_hot_tp_atomic_batch_expands_the_same_group_boundary_across_ranks() {
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
            owner_hot_tp_atomic_batch_key_rows(&identity, Some(&rank0_group)),
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
            owner_hot_tp_atomic_batch_key_rows(&identity, Some(&mixed_rank_group)),
            Err(())
        );
    }

    #[test]
    fn hot_cache_only_dispatches_size_removals_and_keeps_capacity() {
        let counters = Arc::new(OwnerHotCacheCounters::default());
        let retry_queue = Arc::new(OwnerHotRetryQueue::new(counters.clone()));
        let (tx, mut rx) = limit_thirdparty::tokio::sync::ampsc::unbounded_channel();
        let cache = build_owner_hot_cache(10, counters.clone(), retry_queue, tx);
        let entry = |put_version| OwnerHotCacheEntry {
            put_id: (10, put_version),
            memory_info: Weak::new(),
            weight_bytes: 6,
        };
        let alias = |key: &str, put_version: usize| OwnerHotPinAlias {
            key: key.to_string(),
            memory_info_ptr: put_version,
        };

        cache.insert("explicit".to_string(), [alias("explicit", 0)], entry(0));
        cache.run_pending_tasks();
        cache.invalidate(&"explicit".to_string());
        cache.run_pending_tasks();
        assert_eq!(counters.size_evictions.load(Ordering::Relaxed), 0);

        cache.insert("size-a".to_string(), [alias("size-a", 1)], entry(1));
        cache.insert("size-b".to_string(), [alias("size-b", 2)], entry(2));
        cache.run_pending_tasks();
        assert!(counters.size_evictions.load(Ordering::Relaxed) >= 1);
        assert_eq!(cache.max_capacity(), Some(10));
        let event = rx
            .try_recv()
            .expect("the Moka listener must emit lightweight metadata without pinning");
        assert!(event.memory_info.upgrade().is_none());
    }

    #[test]
    fn pointwise_batch_visibility_skips_reclaim_fenced_keys() {
        let keys = vec![
            "local-a".to_string(),
            "fenced".to_string(),
            "local-b".to_string(),
        ];
        let controls = OwnerKeyControlTable::default();
        controls.lock_key("fenced").insert(
            "fenced".to_string(),
            OwnerKeyControlState {
                local_puts: 0,
                external_pending_puts: 0,
                source_eviction_selection: None,
                reclaim: Some(OwnerReclaimRecord::Committed(OwnerReclaimItem {
                    key: "fenced".to_string(),
                    ..OwnerReclaimItem::default()
                })),
                external_get: None,
            },
        );
        let mut resolved_keys = Vec::new();
        let visible = keys
            .iter()
            .map(|key| {
                let shard = controls.lock_key(key);
                if shard
                    .get(key)
                    .is_some_and(|state| state.local_access_fenced())
                {
                    None
                } else {
                    resolved_keys.push(key.to_string());
                    Some(key.to_string())
                }
            })
            .collect::<Vec<_>>();

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
    fn sharded_owner_control_does_not_globally_block_unrelated_keys() {
        let controls = OwnerKeyControlTable::default();
        let first = "shard-key-a";
        let first_shard = OwnerKeyControlTable::shard_index(first);
        let second = (0..10_000)
            .map(|idx| format!("shard-key-b-{idx}"))
            .find(|key| OwnerKeyControlTable::shard_index(key) != first_shard)
            .expect("a key on another owner-control shard must exist");

        let _first_guard = controls.lock_key(first);
        assert!(
            controls.shards[OwnerKeyControlTable::shard_index(&second)]
                .try_lock()
                .is_some(),
            "one key fence must not block an unrelated shard"
        );
        assert!(
            controls.shards[first_shard].try_lock().is_none(),
            "the same shard must remain serialized while its guard is held"
        );
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

    #[test]
    fn owner_reclaim_releases_committed_route_and_resident_holder_as_one_pool_update() {
        let mut class = super::OwnerLocalReserveClassState::new(8, 2);
        class.install_grant(OwnerLocalReserveGrantState::new(7, 0, 0, 16, 8, 2));
        let slot = class.claim_available(1).pop().expect("slot should be free");
        assert!(class.mark_prepared_slot_pending_visible(slot.grant_id, slot.slot_index));
        assert!(class.retain_resident_slot_holder(slot.grant_id, slot.slot_index));
        assert!(class.promote_pending_visible_slot_to_committed(slot.grant_id, slot.slot_index));
        assert_eq!(class.committed_slot_count(), 1);
        assert_eq!(class.free_slot_count(), 1);

        assert!(class.release_committed_resident_slot(slot.grant_id, slot.slot_index));
        assert_eq!(class.committed_slot_count(), 0);
        assert_eq!(class.free_slot_count(), 2);
    }

    #[test]
    fn failed_pending_get_releases_only_its_slot() {
        let mut grant = OwnerLocalReserveGrantState::new(7, 0, 0, 24, 8, 3);
        let failed = grant.claim_prepared_slot().expect("first slot");
        let unrelated = grant.claim_prepared_slot().expect("second slot");
        grant.mark_prepared_slot_pending_visible(failed.slot_index);
        grant.retain_resident_slot_holder(failed.slot_index);

        grant.release_resident_slot_holder(failed.slot_index);

        assert!(matches!(
            grant.slot_states[failed.slot_index as usize],
            OwnerLocalReserveSlotState::Free
        ));
        assert!(matches!(
            grant.slot_states[unrelated.slot_index as usize],
            OwnerLocalReserveSlotState::Prepared
        ));
        assert_eq!(grant.free_slots.len(), 2);
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
    pub(crate) fn owner_local_reserve_claim_lock(
        &self,
        slot_size: u64,
    ) -> Arc<limit_thirdparty::tokio::sync::AMutex<()>> {
        self.owner_local_reserve_claim_locks
            .entry(slot_size)
            .or_insert_with(|| Arc::new(limit_thirdparty::tokio::sync::AMutex::new(())))
            .clone()
    }

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
                    cache.max_capacity().unwrap_or(0),
                    cache.entry_count(),
                    cache.weighted_size(),
                )
            })
            .unwrap_or_default();
        let (
            external_get_flights,
            external_get_flights_starting,
            external_get_flights_finishing,
            external_get_flights_revoking,
            external_get_undecided_interests,
            external_get_retained_interests,
        ) = {
            let mut flights = 0u64;
            let mut starting = 0u64;
            let mut finishing = 0u64;
            let mut revoking = 0u64;
            let mut undecided = 0u64;
            let mut retained = 0u64;
            // Metrics use a weak side index and never scan correctness fences.
            for op in self.external_get_flight_snapshot() {
                flights = flights.saturating_add(1);
                let state = op.state.lock();
                undecided = undecided.saturating_add(state.undecided as u64);
                retained = retained.saturating_add(state.retained as u64);
                match &state.phase {
                    ExternalGetKeySharedPhase::Starting
                    | ExternalGetKeySharedPhase::Started { .. } => {
                        starting = starting.saturating_add(1)
                    }
                    ExternalGetKeySharedPhase::Finishing { .. } => {
                        finishing = finishing.saturating_add(1)
                    }
                    ExternalGetKeySharedPhase::Revoking { .. } => {
                        revoking = revoking.saturating_add(1)
                    }
                    ExternalGetKeySharedPhase::Ready { .. }
                    | ExternalGetKeySharedPhase::Failed { .. } => {}
                }
            }
            (flights, starting, finishing, revoking, undecided, retained)
        };
        let (
            local_reserve_slots_free,
            local_reserve_slots_prepared,
            local_reserve_slots_pending_visible,
            local_reserve_slots_committed,
        ) = {
            let pool = self.owner_local_reserve_pool.lock();
            let mut free = 0u64;
            let mut prepared = 0u64;
            let mut pending_visible = 0u64;
            let mut committed = 0u64;
            for class in pool.classes.values() {
                free =
                    free.saturating_add(u64::try_from(class.free_slot_count()).unwrap_or(u64::MAX));
                prepared = prepared
                    .saturating_add(u64::try_from(class.prepared_slot_count()).unwrap_or(u64::MAX));
                pending_visible = pending_visible.saturating_add(
                    u64::try_from(class.pending_visible_slot_count()).unwrap_or(u64::MAX),
                );
                committed = committed.saturating_add(
                    u64::try_from(class.committed_slot_count()).unwrap_or(u64::MAX),
                );
            }
            (free, prepared, pending_visible, committed)
        };
        OwnerRuntimeObserveSnapshot {
            external_get_holding_entries: self.external_get_holding.total() as u64,
            external_get_holding_bytes,
            external_get_start_handles: self.external_get_start_registry.len() as u64,
            external_get_flights,
            external_get_flights_starting,
            external_get_flights_finishing,
            external_get_flights_revoking,
            external_get_undecided_interests,
            external_get_retained_interests,
            external_pending_put_entries: self.external_pending_puts.entry_count(),
            local_reserve_slots_free,
            local_reserve_slots_prepared,
            local_reserve_slots_pending_visible,
            local_reserve_slots_committed,
            hot_cache_capacity_bytes,
            hot_cache_entries,
            hot_cache_weighted_bytes,
            hot_size_evictions: self
                .owner_hot_counters
                .size_evictions
                .load(Ordering::Relaxed),
            hot_source_evict_handoff_members: self
                .owner_hot_counters
                .source_evict_handoff_members
                .load(Ordering::Relaxed),
            hot_source_evict_committed_members: self
                .owner_hot_counters
                .source_evict_committed_members
                .load(Ordering::Relaxed),
            hot_source_evict_restored_members: self
                .owner_hot_counters
                .source_evict_restored_members
                .load(Ordering::Relaxed),
            hot_source_evict_obsolete: self
                .owner_hot_counters
                .source_evict_obsolete
                .load(Ordering::Relaxed),
            hot_source_evict_dispatch_failed: self
                .owner_hot_counters
                .source_evict_dispatch_failed
                .load(Ordering::Relaxed),
            hot_source_eviction_selected: self.owner_source_eviction_selected.len() as u64,
            hot_source_evict_retry_entries: self.owner_hot_retry_queue.len() as u64,
            hot_source_evict_retry_scheduled: self
                .owner_hot_counters
                .source_evict_retry_scheduled
                .load(Ordering::Relaxed),
            hot_source_evict_retry_emitted: self
                .owner_hot_counters
                .source_evict_retry_emitted
                .load(Ordering::Relaxed),
            hot_selection_debt_bytes: self
                .owner_hot_counters
                .selection_debt_bytes
                .load(Ordering::Relaxed),
            hot_source_eviction_selected_bytes: self
                .owner_hot_counters
                .source_eviction_selected_bytes
                .load(Ordering::Relaxed),
            hot_eviction_skipped_stale: self
                .owner_hot_counters
                .skipped_stale
                .load(Ordering::Relaxed),
            hot_eviction_skipped_reclaim: self
                .owner_hot_counters
                .skipped_reclaim
                .load(Ordering::Relaxed),
            hot_eviction_skipped_active_holders: self
                .owner_hot_counters
                .skipped_active_holders
                .load(Ordering::Relaxed),
            hot_group_registry_entries: self.owner_hot_atomic_groups.len() as u64,
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
        class_state.max_observed_claim_slots =
            class_state.max_observed_claim_slots.max(demand_slots);
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
                        tracing::info!(
                            active_handles = snapshot.external_get_start_handles,
                            active_flights = snapshot.external_get_flights,
                            starting_flights = snapshot.external_get_flights_starting,
                            finishing_flights = snapshot.external_get_flights_finishing,
                            revoking_flights = snapshot.external_get_flights_revoking,
                            undecided_interests = snapshot.external_get_undecided_interests,
                            retained_interests = snapshot.external_get_retained_interests,
                            reserve_free = snapshot.local_reserve_slots_free,
                            reserve_prepared = snapshot.local_reserve_slots_prepared,
                            reserve_pending_visible = snapshot.local_reserve_slots_pending_visible,
                            reserve_committed = snapshot.local_reserve_slots_committed,
                            "owner Get lifecycle snapshot"
                        );
                        if snapshot.hot_cache_capacity_bytes > 0 {
                            tracing::info!(
                                capacity_bytes = snapshot.hot_cache_capacity_bytes,
                                entries = snapshot.hot_cache_entries,
                                weighted_bytes = snapshot.hot_cache_weighted_bytes,
                                size_evictions = snapshot.hot_size_evictions,
                                source_evict_handoff_members = snapshot.hot_source_evict_handoff_members,
                                source_evict_committed_members = snapshot.hot_source_evict_committed_members,
                                source_evict_restored_members = snapshot.hot_source_evict_restored_members,
                                source_evict_obsolete = snapshot.hot_source_evict_obsolete,
                                source_evict_dispatch_failed = snapshot.hot_source_evict_dispatch_failed,
                                source_eviction_selected = snapshot.hot_source_eviction_selected,
                                source_evict_retry_entries = snapshot.hot_source_evict_retry_entries,
                                source_evict_retry_scheduled = snapshot.hot_source_evict_retry_scheduled,
                                source_evict_retry_emitted = snapshot.hot_source_evict_retry_emitted,
                                selection_debt_bytes = snapshot.hot_selection_debt_bytes,
                                source_eviction_selected_bytes = snapshot.hot_source_eviction_selected_bytes,
                                skipped_stale = snapshot.hot_eviction_skipped_stale,
                                skipped_reclaim = snapshot.hot_eviction_skipped_reclaim,
                                skipped_active_holders = snapshot.hot_eviction_skipped_active_holders,
                                group_registry_entries = snapshot.hot_group_registry_entries,
                                group_trigger_duplicates = snapshot.hot_group_trigger_duplicates,
                                group_trigger_incomplete = snapshot.hot_group_trigger_incomplete,
                                grouped_put_done_batches = snapshot.grouped_put_done_batches,
                                grouped_put_done_items = snapshot.grouped_put_done_items,
                                legacy_put_done_batches = snapshot.legacy_put_done_batches,
                                legacy_put_done_items = snapshot.legacy_put_done_items,
                                "owner hot source-eviction policy snapshot"
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
        // The Moka eviction listener is synchronous and must never block while
        // holding Moka's housekeeper lock. Events contain only weak payload
        // references and are deduplicated by exact selected identities, so use
        // a lossless metadata channel instead of dropping atomic_batches when the old
        // bounded queue briefly filled under cache pressure.
        let (owner_hot_eviction_tx, owner_hot_eviction_rx) =
            tokio::sync::ampsc::unbounded_channel();
        let get_cached_info = Arc::new(DashMap::new());
        let owner_key_control = Arc::new(OwnerKeyControlTable::default());
        let owner_source_eviction_selected = Arc::new(DashMap::new());
        let owner_hot_atomic_groups = Arc::new(DashMap::new());
        let owner_hot_counters = Arc::new(OwnerHotCacheCounters::default());
        let owner_hot_retry_queue = Arc::new(OwnerHotRetryQueue::new(owner_hot_counters.clone()));
        let owner_hot_cache = owner_hot_cache_capacity_bytes.map(|capacity_bytes| {
            build_owner_hot_cache(
                capacity_bytes,
                owner_hot_counters.clone(),
                owner_hot_retry_queue.clone(),
                owner_hot_eviction_tx.clone(),
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
            pending_local_get_info: DashMap::new(),
            local_snapshot_info: DashMap::new(),
            owner_local_reserve_pool: Mutex::new(OwnerLocalReservePoolState::default()),
            owner_local_reserve_claim_locks: DashMap::new(),
            owner_local_reserve_rebalance_notify: Arc::new(
                limit_thirdparty::tokio::sync::Notify::new(),
            ),
            external_local_first_put_id_counter: AtomicU32::new(0),
            next_owner_source_eviction_operation_id: AtomicU64::new(1),
            owner_key_control,
            owner_hot_cache,
            owner_source_eviction_selected,
            owner_hot_atomic_groups,
            owner_hot_counters,
            owner_hot_retry_queue,
            owner_hot_eviction_tx,
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
            external_get_flight_registry: DashMap::new(),
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
            rpc_caller_batch_put_append_start: RPCCaller::new(),
            rpc_caller_put_append_revoke: RPCCaller::new(),
            rpc_caller_put_append_done: RPCCaller::new(),
            rpc_caller_batch_put_append_done: RPCCaller::new(),
            rpc_caller_batch_evict_owner_source: RPCCaller::new(),
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
            .rpc_caller_batch_put_append_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_append_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_evict_owner_source
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
        spawn_owner_slot_pressure_actor(inner.view.clone_view());
        external_api::spawn_external_get_start_handle_sweeper(inner.view.clone_view());
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
                put::spawn_owner_source_eviction_dispatcher(
                    inner.view.clone_view(),
                    owner_hot_eviction_rx,
                );
                put::spawn_owner_hot_retry_actor(inner.view.clone_view());
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

        // Spawn cluster listener to retire generation-scoped external requester state.
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
                                        let departed_epoch = view_task
                                            .cluster_manager()
                                            .get_prev_member_info(&node_id)
                                            .map(|member| member.node_start_time);
                                        let current_epoch = view_task
                                            .cluster_manager()
                                            .get_member_info_cached(&node_id)
                                            .map(|member| member.node_start_time);
                                        let Some(departed_epoch) = external_api::external_member_left_departed_epoch(
                                            departed_epoch,
                                            current_epoch,
                                        ) else {
                                            tracing::debug!(
                                                "Ignoring ambiguous/delayed external MemberLeft: node={} departed_epoch={:?} current_epoch={:?}",
                                                node_id,
                                                departed_epoch,
                                                current_epoch,
                                            );
                                            continue;
                                        };

                                        let inner = view_task.client_kv_api().inner();
                                        let removed_handles = external_api::cleanup_external_get_start_handles_for_generation(
                                            &inner.external_get_start_registry,
                                            &node_id,
                                            departed_epoch,
                                        );
                                        let removed_holdings = inner
                                            .external_get_holding
                                            .cleanup_node_generation(&node_id, departed_epoch);
                                        if removed_handles > 0 || removed_holdings > 0 {
                                            tracing::info!(
                                                "Cleaned up departed external requester state: node={} epoch={} handles={} holdings={}",
                                                node_id,
                                                departed_epoch,
                                                removed_handles,
                                                removed_holdings,
                                            );
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Client cluster event receiver error (will resubscribe): {}",
                                    e
                                );
                                listen_cluster_event = view_task.cluster_manager().listen();
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
