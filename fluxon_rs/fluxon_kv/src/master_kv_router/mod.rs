pub mod delete;
mod get;
pub mod msg_pack;
pub mod placement;
pub mod put;
mod reclaim;

mod count_prefix_index;
mod route_maintenance;
mod tiered_writeback;

use self::{
    count_prefix_index::PrefixRadixTree,
    delete::handle_batch_delete_ack,
    delete::handle_delete,
    delete::handle_delete_ack,
    get::{
        handle_batch_get_done, handle_batch_get_revoke, handle_batch_get_start,
        handle_batch_is_exist, handle_get_done, handle_get_meta, handle_get_revoke,
        handle_get_start,
    },
    msg_pack::{
        BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, BatchEnqueueReplicaTaskReq,
        BatchGetDoneReq, BatchGetRevokeReq, BatchGetStartReq, BatchIsExistReq,
        BatchOwnerReclaimReq, BatchPreparePutKeysReq, BatchPutDoneReq, BatchPutRevokeReq,
        BatchPutStartReq, BatchReleasePutKeyReservationsReq, CountPrefixReq, CountPrefixResp,
        DeleteAckReq, DeleteReq, GetAllocationMode, GetDoneReq, GetMasterOnlyMetricPartReq,
        GetMasterOnlyMetricPartResp, GetMetaReq, GetRevokeReq, GetStartReq, GroupedBatchPutDoneReq,
        MemHolderKeepAliveReq, MemHolderReleaseReq, OwnerReclaimBacking, OwnerReclaimItem,
        OwnerReclaimReason, PutAppendDoneReq, PutAppendRevokeReq, PutAppendStartReq,
        PutAtomicGroup, PutDoneReq, PutRevokeReq, PutStartReq, ReleaseLocalGrantReq,
        ReserveLocalGrantReq,
    },
    placement::{PlacementPolicy, build_placement_policy},
    put::{
        handle_batch_prepare_put_keys, handle_batch_put_done, handle_batch_put_revoke,
        handle_batch_put_start, handle_batch_release_put_key_reservations,
        handle_grouped_batch_put_done, handle_put_append_done, handle_put_append_revoke,
        handle_put_append_start, handle_put_done, handle_put_revoke, handle_put_start,
        handle_release_local_grant, handle_reserve_local_grant,
    },
};
use crate::ClientKvApiAccessTrait;
use crate::client_kv_api::ClientKvApi;
use crate::cluster_manager::{
    ClusterEvent, ClusterManager, ClusterManagerAccessTrait, NodeID, NodeIDString,
};
use crate::config::{ReplicaTaskPlacementConfig, TestSpecConfig};
use crate::master_kv_router::delete::DeleteKeyInfo;
use crate::master_kv_router::put::PutIDForAKey;
use crate::master_lease_manager::{MasterLeaseManager, MasterLeaseManagerAccessTrait};
use crate::master_seg_manager::MasterSegManager;
use crate::master_seg_manager::MasterSegManagerAccessTrait;
use crate::master_seg_manager::NodeTombTag;
use crate::master_seg_manager::one_seg_allocator::Allocation;
use crate::memholder::{
    EnsureMemholderMgmtDeleteHandle, MasterOwnerMemMgr, MemholderManagerTrait, NodeHolderKey,
};
use crate::metric_reporter::{MetricReporter, MetricReporterAccessTrait};
use crate::p2p::msg_pack::{MsgPack, RPCCaller, RPCHandler};
use crate::p2p::p2p_module::{P2pModule, P2pModuleAccessTrait};
use crate::rpcresp_kvresult_convert;
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, OK};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_framework_compiled::upgrade_view_guard::UpgradeViewGuard;
use fluxon_util::map_lock::AMapLock;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use limit_thirdparty::tokio::sync::{ARwLock, abroadcast};
use limit_thirdparty::tokio::{self, sync::ampsc};
use moka::notification::RemovalCause;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::borrow::Cow;
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

const MAX_GET_DURABLE_REPLICA_SLOTS: u32 = 2;
const PLACEMENT_REPORT_INTERVAL_SECS: u64 = 10;
const INFLIGHT_PUT_TTL_SECONDS: u64 = 60;
const INFLIGHT_PUT_TTL_SECONDS_SKIP_PUT_END_COMMIT: u64 = 5;
const EVICTION_RECLAIM_QUEUE_CAPACITY: usize = 4096;
const POST_ROUTE_MAINTENANCE_QUEUE_CAPACITY: usize = 512;
const TIER1_WRITEBACK_QUEUE_CAPACITY: usize = 4096;

fn subtract_pending_eviction_weight(
    pending_weight: &AtomicU64,
    owner_node_id: &str,
    completed_weight: u64,
) {
    if completed_weight == 0 {
        return;
    }
    pending_weight
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_sub(completed_weight)
        })
        .unwrap_or_else(|pending| {
            panic!(
                "eviction reclaim pending weight underflow for owner {}: pending={} completed={}",
                owner_node_id, pending, completed_weight
            )
        });
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnrecoverableCacheEvictionContext {
    NormalCapacity,
    OwnerLocalReserveNoSpace,
}

fn allow_unrecoverable_cache_eviction(
    owner_is_remote_only: bool,
    has_ready_remote_only_owner: bool,
    context: UnrecoverableCacheEvictionContext,
) -> bool {
    // A normal active-tier eviction must preserve the remote copy whenever a remote-only tier is
    // available. Physical local-reserve exhaustion is the liveness boundary: if that remote tier
    // has filled and no recoverable active-tier entry remains, refusing to evict the last cached
    // copy prevents the owner from accepting any subsequent write-back. The existing three-phase
    // owner reclaim transaction removes the route and slot atomically, so dropping that cold cache
    // entry is safe and a future lookup simply recomputes it.
    context == UnrecoverableCacheEvictionContext::OwnerLocalReserveNoSpace
        || owner_is_remote_only
        || !has_ready_remote_only_owner
}

#[derive(Clone, Copy, Debug)]
pub enum PutPlacementMode {
    Local,
    Remote,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReservedCapacityReason {
    LeaseBoundKv,
}

#[derive(Debug, Default)]
pub struct NodeCacheReservedCapacity {
    pub total_bytes: AtomicU64,
    pub lease_bound_kv_bytes: AtomicU64,
}

impl NodeCacheReservedCapacity {
    fn apply_delta(&self, reason: ReservedCapacityReason, delta_bytes: i64) {
        if delta_bytes >= 0 {
            let delta = delta_bytes as u64;
            self.total_bytes.fetch_add(delta, Ordering::Relaxed);
            match reason {
                ReservedCapacityReason::LeaseBoundKv => {
                    self.lease_bound_kv_bytes
                        .fetch_add(delta, Ordering::Relaxed);
                }
            }
        } else {
            let delta = (-delta_bytes) as u64;
            self.total_bytes.fetch_sub(delta, Ordering::Relaxed);
            match reason {
                ReservedCapacityReason::LeaseBoundKv => {
                    self.lease_bound_kv_bytes
                        .fetch_sub(delta, Ordering::Relaxed);
                }
            }
        }
    }

    fn total_reserved_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RequesterTargetPair {
    requester_node_id: NodeIDString,
    target_node_id: NodeIDString,
}

#[derive(Clone, Debug, Default)]
pub struct ReplicaCacheNodeObserveSnapshot {
    pub owner_node: String,
    pub entries: u64,
    pub weighted_bytes: u64,
    pub effective_capacity_bytes: u64,
    pub reserved_capacity_bytes: u64,
    pub base_capacity_bytes: u64,
    pub pending_eviction_reclaim_bytes: u64,
    pub writeback_tier1_entries: u64,
    pub writeback_tier1_weighted_bytes: u64,
    pub writeback_tier1_capacity_bytes: u64,
    pub writeback_tier1_triggered: u64,
    pub writeback_tier1_owner_accepted: u64,
    pub writeback_tier1_failed: u64,
    pub reclaim_master_activity_deferred: u64,
    pub reclaim_master_get_holder_observed: u64,
    pub reclaim_owner_holder_deferred: u64,
    pub reclaim_owner_other_deferred: u64,
    pub reclaim_route_changed: u64,
    pub reclaim_retry_queued: u64,
    pub reclaim_retry_completed: u64,
    pub reclaim_retry_restored: u64,
    pub reclaim_completed: u64,
    pub owner_hot_demotion_attempts: u64,
    pub owner_hot_demotion_cohorts: u64,
    pub owner_hot_demotion_selected_bytes: u64,
    pub owner_hot_demotion_precheck_rejected: u64,
    pub owner_hot_demotion_partial_mismatch: u64,
    pub recoverable_first_requested_bytes: u64,
    pub recoverable_first_selected_bytes: u64,
    pub recoverable_first_shortfall_bytes: u64,
    pub recoverable_first_eligible_checks: u64,
    pub recoverable_first_route_absent_checks: u64,
    pub recoverable_first_version_changed_checks: u64,
    pub recoverable_first_route_ineligible_checks: u64,
    pub recoverable_first_cpu_route_absent_checks: u64,
    pub recoverable_first_atomic_group_incomplete_checks: u64,
    pub recoverable_first_tp_cohort_incomplete_checks: u64,
}

#[derive(Clone, Debug, Default)]
pub struct MasterRuntimeObserveSnapshot {
    pub get_holding_entries: u64,
    pub get_holding_bytes: u64,
    pub replica_cache_nodes: Vec<ReplicaCacheNodeObserveSnapshot>,
}

impl RequesterTargetPair {
    fn new(requester_node_id: &str, target_node_id: &str) -> Self {
        Self {
            requester_node_id: requester_node_id.to_string(),
            target_node_id: target_node_id.to_string(),
        }
    }

    fn as_log_key(&self) -> String {
        format!("{}->{}", self.requester_node_id, self.target_node_id)
    }
}

#[derive(Clone, Debug)]
pub struct CommittedSlotReplica {
    pub owner_node_id: NodeID,
    pub grant_id: u64,
    pub slot_index: u32,
    pub slot_size: u64,
    pub addr: u64,
    pub len: u64,
    pub base_addr: u64,
}

/// Information about a `put` operation that is currently in progress.
pub enum InflightPutAllocation {
    /// Local fast path: the same allocation is used as both src (staging) and target (final).
    Local(Allocation),
    /// Remote path: separate allocations for src (on requester) and target (on selected node).
    Remote { src: Allocation, target: Allocation },
    /// Local-first fast path: data is already committed in owner-local reserve slot.
    LocalCommittedSlot(CommittedSlotReplica),
}

#[derive(Clone)]
pub struct InflightPutCommitInfo {
    pub node_id: NodeID,
    pub src_target_allocation: Arc<Mutex<Option<InflightPutAllocation>>>,
    pub replica_target: Option<InflightReplicaTaskInfo>,
}

/// Information about a `put` operation that is currently in progress.
#[derive(Clone)]
pub struct InflightPutInfo {
    pub key: String,
    pub req_node_id: NodeID,
    pub len: u64,
    pub commit_info: InflightPutCommitInfo,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
}

#[derive(Clone)]
pub struct InflightReplicaTaskInfo {
    pub node_id: NodeID,
    pub source_node_id: NodeID,
    pub key: String,
    pub put_id: PutIDForAKey,
    pub target_allocation: Arc<Mutex<Option<Allocation>>>,
    pub demote_source_on_remote_complete: bool,
    /// Protect the source-owner copy only after this inclusive replica has been
    /// published successfully.  Backend-admitted replicas set this bit; tiered
    /// write-back and owner-hot exclusive demotion deliberately do not.
    pub protect_source_on_remote_complete: bool,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
}

/// Information about a `get` operation that is currently in progress.
#[derive(Clone)]
pub struct InflightGetInfo {
    pub put_id: PutIDForAKey,
    pub src_node_id: NodeID,
    pub key: String,
    pub req_node_id: NodeID,
    pub len: u64,
    pub allocation: Arc<Allocation>,
    pub route: Arc<OneKvNodesRoutes>,
    pub allocation_mode: GetAllocationMode,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
}

impl InflightGetInfo {
    pub fn release_durable_slot_if_needed(&self) {
        if self.allocation_mode == GetAllocationMode::DurableReplica {
            self.route.release_get_durable_slot();
        }
    }
}

/// Information about a `get` operation that has completed transfer and is being held.
#[derive(Clone)]
pub struct OwnerHoldingGetInfo {
    pub key: String,
    pub holding_node_id: NodeID, // The node that requested the get (holder of the memory)
    pub len: u64,
    pub allocation: Arc<Allocation>, // The target allocation where data was transferred
}

pub struct LocalReserveGrantInfo {
    pub owner_node_id: NodeID,
    pub allocation: Allocation,
}

pub struct PreparedPutKeyReservationInfo {
    pub owner_node_id: NodeID,
    pub key: String,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
}

#[derive(Default)]
pub(crate) struct EvictionReclaimCounters {
    pub master_activity_deferred: AtomicU64,
    pub master_get_holder_observed: AtomicU64,
    pub owner_holder_deferred: AtomicU64,
    pub owner_other_deferred: AtomicU64,
    pub route_changed: AtomicU64,
    pub retry_queued: AtomicU64,
    pub retry_completed: AtomicU64,
    pub retry_restored: AtomicU64,
    pub completed: AtomicU64,
    pub owner_hot_demotion_attempts: AtomicU64,
    pub owner_hot_demotion_cohorts: AtomicU64,
    pub owner_hot_demotion_selected_bytes: AtomicU64,
    pub owner_hot_demotion_precheck_rejected: AtomicU64,
    pub owner_hot_demotion_partial_mismatch: AtomicU64,
    pub recoverable_first_requested_bytes: AtomicU64,
    pub recoverable_first_selected_bytes: AtomicU64,
    pub recoverable_first_shortfall_bytes: AtomicU64,
    pub recoverable_first_eligible_checks: AtomicU64,
    pub recoverable_first_route_absent_checks: AtomicU64,
    pub recoverable_first_version_changed_checks: AtomicU64,
    pub recoverable_first_route_ineligible_checks: AtomicU64,
    pub recoverable_first_cpu_route_absent_checks: AtomicU64,
    pub recoverable_first_atomic_group_incomplete_checks: AtomicU64,
    pub recoverable_first_tp_cohort_incomplete_checks: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MasterKeyActivitySnapshot {
    pub puts: u32,
    pub gets: u32,
    pub replicas: u32,
    pub reclaim_installed: bool,
}

#[derive(Clone, Copy, Debug)]
enum MasterKeyActivityKind {
    Put,
    Get,
    Replica,
}

#[derive(Default)]
struct MasterKeyActivityState {
    puts: u32,
    gets: u32,
    replicas: u32,
    reclaim: Option<OwnerReclaimItem>,
}

#[derive(Default)]
pub(crate) struct MasterKeyActivityTable {
    states: Mutex<HashMap<String, MasterKeyActivityState>>,
}

pub(crate) struct MasterKeyActivityLease {
    table: Arc<MasterKeyActivityTable>,
    key: String,
    kind: MasterKeyActivityKind,
    released: AtomicBool,
}

impl MasterKeyActivityLease {
    pub(crate) fn release_now(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.table.release(&self.key, self.kind);
        }
    }
}

impl Drop for MasterKeyActivityLease {
    fn drop(&mut self) {
        self.release_now();
    }
}

pub(crate) struct MasterKeyActivityCompletionGuard {
    lease: Option<Arc<MasterKeyActivityLease>>,
}

impl MasterKeyActivityCompletionGuard {
    pub(crate) fn new(lease: Arc<MasterKeyActivityLease>) -> Self {
        Self { lease: Some(lease) }
    }

    pub(crate) fn disarm(&mut self) {
        self.lease = None;
    }
}

impl Drop for MasterKeyActivityCompletionGuard {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.as_ref() {
            lease.release_now();
        }
    }
}

impl MasterKeyActivityTable {
    fn reserve(
        self: &Arc<Self>,
        key: &str,
        kind: MasterKeyActivityKind,
        reject_same_kind_if_active: bool,
    ) -> Option<Arc<MasterKeyActivityLease>> {
        let mut states = self.states.lock();
        let state = states.entry(key.to_string()).or_default();
        if state.reclaim.is_some() {
            return None;
        }
        let counter = match kind {
            MasterKeyActivityKind::Put => &mut state.puts,
            MasterKeyActivityKind::Get => &mut state.gets,
            MasterKeyActivityKind::Replica => &mut state.replicas,
        };
        if reject_same_kind_if_active && *counter > 0 {
            return None;
        }
        *counter = counter
            .checked_add(1)
            .expect("master per-key activity counter overflow");
        Some(Arc::new(MasterKeyActivityLease {
            table: self.clone(),
            key: key.to_string(),
            kind,
            released: AtomicBool::new(false),
        }))
    }

    fn release(&self, key: &str, kind: MasterKeyActivityKind) {
        let mut states = self.states.lock();
        let remove = {
            let state = states
                .get_mut(key)
                .expect("master per-key activity state missing on release");
            let counter = match kind {
                MasterKeyActivityKind::Put => &mut state.puts,
                MasterKeyActivityKind::Get => &mut state.gets,
                MasterKeyActivityKind::Replica => &mut state.replicas,
            };
            *counter = counter
                .checked_sub(1)
                .expect("master per-key activity counter underflow");
            state.puts == 0 && state.gets == 0 && state.replicas == 0 && state.reclaim.is_none()
        };
        if remove {
            states.remove(key);
        }
    }

    pub(crate) fn try_install_reclaim(
        &self,
        item: &OwnerReclaimItem,
    ) -> Result<(), MasterKeyActivitySnapshot> {
        let mut states = self.states.lock();
        let state = states.entry(item.key.clone()).or_default();
        if state.puts != 0 || state.gets != 0 || state.replicas != 0 || state.reclaim.is_some() {
            return Err(MasterKeyActivitySnapshot {
                puts: state.puts,
                gets: state.gets,
                replicas: state.replicas,
                reclaim_installed: state.reclaim.is_some(),
            });
        }
        state.reclaim = Some(item.clone());
        Ok(())
    }

    pub(crate) fn is_quiescent(&self, key: &str) -> bool {
        let states = self.states.lock();
        match states.get(key) {
            None => true,
            Some(state) => {
                state.puts == 0 && state.gets == 0 && state.replicas == 0 && state.reclaim.is_none()
            }
        }
    }

    pub(crate) fn reclaim_matches(&self, item: &OwnerReclaimItem) -> bool {
        self.states
            .lock()
            .get(&item.key)
            .and_then(|state| state.reclaim.as_ref())
            == Some(item)
    }

    pub(crate) fn clear_reclaim(&self, item: &OwnerReclaimItem) -> bool {
        let mut states = self.states.lock();
        let Some(state) = states.get_mut(&item.key) else {
            return false;
        };
        if state.reclaim.as_ref() != Some(item) {
            return false;
        }
        state.reclaim = None;
        if state.puts == 0 && state.gets == 0 && state.replicas == 0 {
            states.remove(&item.key);
        }
        true
    }

    #[cfg(test)]
    fn has_reclaim(&self, key: &str) -> bool {
        self.states
            .lock()
            .get(key)
            .is_some_and(|state| state.reclaim.is_some())
    }
}

async fn handle_count_prefix(
    view: &MasterKvRouterView,
    msg: MsgPack<CountPrefixReq>,
) -> MsgPack<CountPrefixResp> {
    let prefix = msg.serialize_part.prefix.clone();
    let inner = view.master_kv_router().inner();

    let count = {
        if view.master_kv_router().prefix_index_enabled() {
            let tree = inner.prefix_index.read().await;
            tree.count_prefix(&prefix)
        } else {
            inner
                .kv_routes
                .iter()
                .filter(|entry| entry.key().starts_with(&prefix))
                .count() as u64
        }
    };

    MsgPack {
        serialize_part: CountPrefixResp {
            count,
            error_code: OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

// --- MasterKvRouter Module ---

define_module!(
    MasterKvRouter,
    (master_kv_router, MasterKvRouter),
    (p2p, P2pModule),
    (master_seg_manager, MasterSegManager),
    (cluster_manager, ClusterManager),
    (metric_reporter, MetricReporter),
    (client_kv_api, ClientKvApi),
    (master_lease_manager, MasterLeaseManager)
);

/// MasterKvRouter module creation parameters
#[derive(Clone, Debug, Default)]
pub struct MasterKvRouterNewArg {
    pub test_spec_config: TestSpecConfig,
    pub replica_task_placement: ReplicaTaskPlacementConfig,
    pub replica_cache_capacity_ratio: f64,
    pub replica_writeback_tier1_capacity_ratio: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct NodeValueReplicaDesc {
    pub weight_bytes: u32,
    pub put_id: PutIDForAKey,
}

/// Information about a completed `put` operation that can be retrieved via `get`.
/// Now supports multiple replicas per key.
#[derive(Clone, Debug)]
pub enum KvReplicaBacking {
    Allocation(Arc<Allocation>),
    CommittedSlot(CommittedSlotReplica),
}

impl KvReplicaBacking {
    pub fn abs_addr(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.base_addr() + allocation.addr(),
            Self::CommittedSlot(slot) => slot.addr,
        }
    }

    pub fn base_addr(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.base_addr(),
            Self::CommittedSlot(slot) => slot.base_addr,
        }
    }

    pub fn len(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.size(),
            Self::CommittedSlot(slot) => slot.len,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KvRouteInfo {
    pub node_id: NodeID,
    pub backing: KvReplicaBacking,
    /// Whether this owner also published the route backing into its local key index.
    /// Replica-task and remote-put targets are raw master-owned allocations and have no
    /// owner-side key entry to fence during capacity eviction.
    pub owner_local_indexed: bool,
    pub tomb_tag: NodeTombTag,
}

#[derive(Debug)]
pub struct OneKvNodesRoutes {
    /// the version id for a kv put operation
    pub put_id: PutIDForAKey,

    /// Lease binding of this key-version on the master. This is an explicit
    /// contract set at PutDone time only when the caller provides a lease_id.
    ///
    /// Semantics and rationale (read before modifying):
    /// - This field records whether the current key route (identified by
    ///   `put_id`) is associated with a lease and which lease it is.
    /// - The association is written once during `put_done` together with the
    ///   route update. Subsequent `get_done` replica additions read this field
    ///   to decide cache behavior deterministically without consulting the
    ///   lease manager again on the hot path.
    /// - We do not use any fallback or implicit default. Absence (`None`)
    ///   means "not leased" for this exact `put_id`. Presence (`Some(lease)`)
    ///   means "leased" and must be respected by cache-controller to prevent
    ///   eviction-driven global deletes for leased keys.
    /// - When a newer `put` arrives, we rebuild a fresh `OneKvNodesRoutes` with
    ///   the new `put_id` and its (possibly different) lease binding. This keeps
    ///   the binding strictly version-scoped and avoids state leakage.
    /// - Lease expiry/cleanup still owns deletion of leased keys. If a lease
    ///   expires, the cleanup task deletes keys via master delete. Until then,
    ///   nodes must not insert leased keys into moka caches.
    pub lease_id: Option<u64>,

    /// Version-scoped multi-key group supplied by the put caller.
    pub atomic_group: Option<Arc<PutAtomicGroup>>,

    /// node_id -> KvRouteInfo
    pub nodes_replicas: RwLock<HashMap<NodeID, KvRouteInfo>>,
    pub get_durable_slots_used: AtomicU32,
}

impl OneKvNodesRoutes {
    fn clean_up_tomb_nodes_replicas(
        &self,
        verify_put_id: PutIDForAKey,
        tombs: HashSet<NodeID>,
        view: &MasterKvRouterView,
    ) -> bool {
        if self.put_id != verify_put_id {
            return false;
        }

        let mut nodes_replicas = self.nodes_replicas.write();
        nodes_replicas.retain(|_, kv_info| !tombs.contains(&kv_info.node_id));

        return true;
    }

    fn try_reserve_get_durable_slot(&self) -> bool {
        self.get_durable_slots_used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current < MAX_GET_DURABLE_REPLICA_SLOTS {
                    Some(current + 1)
                } else {
                    None
                }
            })
            .is_ok()
    }

    fn release_get_durable_slot(&self) {
        self.get_durable_slots_used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            })
            .unwrap_or_else(|_| panic!("get durable slot underflow indicates a logic bug"));
    }
}

fn route_has_live_replica_on_node(route: &OneKvNodesRoutes, node_id: &str) -> bool {
    route
        .nodes_replicas
        .read()
        .iter()
        .any(|(candidate, replica)| candidate.as_ref() == node_id && !replica.tomb_tag.is_tomb())
}

fn route_live_remote_cache_nodes(
    view: &MasterKvRouterView,
    route: &OneKvNodesRoutes,
) -> HashSet<String> {
    route
        .nodes_replicas
        .read()
        .iter()
        .filter_map(|(node_id, replica)| {
            if replica.tomb_tag.is_tomb() {
                return None;
            }
            let member = view
                .cluster_manager()
                .get_member_info_cached(node_id.as_ref());
            placement::member_matches_roles(
                member.as_ref(),
                &view
                    .master_kv_router()
                    .inner()
                    .replica_task_placement
                    .remote_only_node_roles,
            )
            .then(|| node_id.as_ref().to_string())
        })
        .collect()
}

const MAX_INFERRED_TP_COHORT_SIZE: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InferredTpKey<'a> {
    pub(crate) logical_prefix: &'a str,
    pub(crate) rank: usize,
    pub(crate) size: usize,
}

/// Decode the canonical SGLang HiCache suffix `_<tp_rank>_<tp_size>`.
///
/// This is deliberately strict and is only used to make owner reclaim more
/// conservative. A key that cannot be decoded keeps the existing non-TP
/// behavior; a decoded TP key whose peer routes are absent or inconsistent is
/// never considered recoverable.
pub(crate) fn infer_sglang_tp_key(key: &str) -> Option<InferredTpKey<'_>> {
    let (before_size, raw_size) = key.rsplit_once('_')?;
    let (logical_prefix, raw_rank) = before_size.rsplit_once('_')?;
    if logical_prefix.is_empty()
        || raw_rank.is_empty()
        || raw_size.is_empty()
        || !raw_rank.bytes().all(|byte| byte.is_ascii_digit())
        || !raw_size.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let rank = raw_rank.parse::<usize>().ok()?;
    let size = raw_size.parse::<usize>().ok()?;
    if size == 0
        || size > MAX_INFERRED_TP_COHORT_SIZE
        || rank >= size
        || raw_rank != rank.to_string()
        || raw_size != size.to_string()
    {
        return None;
    }
    Some(InferredTpKey {
        logical_prefix,
        rank,
        size,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoverableReplicaStatus {
    Recoverable,
    RouteAbsent,
    VersionChanged,
    RouteIneligible,
    CpuRouteAbsent,
    AtomicGroupIncomplete,
    TpCohortIncomplete,
}

fn recoverable_cohort_weight<F>(
    expected_entries: &HashMap<String, NodeValueReplicaDesc>,
    mut recoverable_status: F,
) -> Option<u64>
where
    F: FnMut(&str, &NodeValueReplicaDesc) -> RecoverableReplicaStatus,
{
    let mut expected_weight = 0u64;
    for (key, desc) in expected_entries {
        if recoverable_status(key, desc) != RecoverableReplicaStatus::Recoverable {
            return None;
        }
        expected_weight = expected_weight.checked_add(u64::from(desc.weight_bytes))?;
    }
    (expected_weight != 0).then_some(expected_weight)
}

impl EvictionReclaimCounters {
    fn record_recoverable_first_status(&self, status: RecoverableReplicaStatus) {
        let counter = match status {
            RecoverableReplicaStatus::Recoverable => &self.recoverable_first_eligible_checks,
            RecoverableReplicaStatus::RouteAbsent => &self.recoverable_first_route_absent_checks,
            RecoverableReplicaStatus::VersionChanged => {
                &self.recoverable_first_version_changed_checks
            }
            RecoverableReplicaStatus::RouteIneligible => {
                &self.recoverable_first_route_ineligible_checks
            }
            RecoverableReplicaStatus::CpuRouteAbsent => {
                &self.recoverable_first_cpu_route_absent_checks
            }
            RecoverableReplicaStatus::AtomicGroupIncomplete => {
                &self.recoverable_first_atomic_group_incomplete_checks
            }
            RecoverableReplicaStatus::TpCohortIncomplete => {
                &self.recoverable_first_tp_cohort_incomplete_checks
            }
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn current_atomic_group_members(
    key: &str,
    route: &OneKvNodesRoutes,
    desc: &NodeValueReplicaDesc,
) -> Result<Vec<msg_pack::PutAtomicGroupMember>, RecoverableReplicaStatus> {
    let Some(group) = route.atomic_group.as_ref() else {
        return Ok(vec![msg_pack::PutAtomicGroupMember {
            key: key.to_string(),
            put_id: desc.put_id,
        }]);
    };
    if !group
        .members
        .iter()
        .any(|member| member.key == key && member.put_id == desc.put_id)
    {
        return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
    }
    Ok(group.members.clone())
}

fn current_tp_cohort_keys(
    key: &str,
    route: &OneKvNodesRoutes,
    desc: &NodeValueReplicaDesc,
) -> Result<Vec<String>, RecoverableReplicaStatus> {
    let current_members = current_atomic_group_members(key, route, desc)?;
    let Some(key_tp) = infer_sglang_tp_key(key) else {
        return Ok(current_members
            .into_iter()
            .map(|member| member.key)
            .collect());
    };
    if key_tp.size == 1 {
        return Ok(current_members
            .into_iter()
            .map(|member| member.key)
            .collect());
    }

    let mut logical_members = Vec::with_capacity(current_members.len());
    for member in current_members {
        let Some(member_tp) = infer_sglang_tp_key(&member.key) else {
            return Err(RecoverableReplicaStatus::TpCohortIncomplete);
        };
        if member_tp.rank != key_tp.rank || member_tp.size != key_tp.size {
            return Err(RecoverableReplicaStatus::TpCohortIncomplete);
        }
        logical_members.push(member_tp.logical_prefix.to_string());
    }

    let mut cohort_keys = Vec::with_capacity(logical_members.len() * key_tp.size);
    for rank in 0..key_tp.size {
        cohort_keys.extend(
            logical_members
                .iter()
                .map(|logical_prefix| format!("{logical_prefix}_{rank}_{}", key_tp.size)),
        );
    }
    Ok(cohort_keys)
}

fn intersect_remote_nodes(
    common_remote_nodes: &mut Option<HashSet<String>>,
    mut member_remote_nodes: HashSet<String>,
    owner_node_id: &str,
) -> Result<(), RecoverableReplicaStatus> {
    member_remote_nodes.remove(owner_node_id);
    if member_remote_nodes.is_empty() {
        return Err(RecoverableReplicaStatus::CpuRouteAbsent);
    }
    match common_remote_nodes.as_mut() {
        Some(common) => common.retain(|node_id| member_remote_nodes.contains(node_id)),
        None => *common_remote_nodes = Some(member_remote_nodes),
    }
    if common_remote_nodes.as_ref().is_some_and(HashSet::is_empty) {
        return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
    }
    Ok(())
}

fn validate_recoverable_group(
    owner_node_id: &str,
    expected_keys: &[String],
    expected_put_ids: Option<&[PutIDForAKey]>,
    route_lookup: &dyn Fn(&str) -> Option<Arc<OneKvNodesRoutes>>,
    live_remote_nodes: &dyn Fn(&OneKvNodesRoutes) -> HashSet<String>,
) -> Result<HashSet<String>, RecoverableReplicaStatus> {
    if expected_put_ids.is_some_and(|put_ids| put_ids.len() != expected_keys.len()) {
        return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
    }
    let Some(first_key) = expected_keys.first() else {
        return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
    };
    let Some(first_route) = route_lookup(first_key) else {
        return Err(RecoverableReplicaStatus::RouteAbsent);
    };

    if expected_keys.len() == 1 {
        if expected_put_ids.is_some_and(|put_ids| first_route.put_id != put_ids[0]) {
            return Err(RecoverableReplicaStatus::VersionChanged);
        }
        if first_route.lease_id.is_some() || first_route.atomic_group.is_some() {
            return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
        }
        let mut common_remote_nodes = None;
        intersect_remote_nodes(
            &mut common_remote_nodes,
            live_remote_nodes(&first_route),
            owner_node_id,
        )?;
        return common_remote_nodes.ok_or(RecoverableReplicaStatus::CpuRouteAbsent);
    }

    let Some(group) = first_route.atomic_group.as_ref() else {
        return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
    };
    if group.members.len() != expected_keys.len()
        || group
            .members
            .iter()
            .zip(expected_keys)
            .any(|(member, expected_key)| member.key != *expected_key)
        || expected_put_ids.is_some_and(|put_ids| {
            group
                .members
                .iter()
                .zip(put_ids)
                .any(|(member, expected_put_id)| member.put_id != *expected_put_id)
        })
    {
        return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
    }

    let mut common_remote_nodes = None;
    for member in &group.members {
        let Some(member_route) = route_lookup(&member.key) else {
            return Err(RecoverableReplicaStatus::RouteAbsent);
        };
        if member_route.put_id != member.put_id
            || member_route.lease_id.is_some()
            || member_route.atomic_group.as_deref() != Some(group.as_ref())
        {
            return Err(RecoverableReplicaStatus::AtomicGroupIncomplete);
        }
        intersect_remote_nodes(
            &mut common_remote_nodes,
            live_remote_nodes(&member_route),
            owner_node_id,
        )?;
    }
    common_remote_nodes.ok_or(RecoverableReplicaStatus::CpuRouteAbsent)
}

fn validate_current_recoverable_group(
    owner_node_id: &str,
    route: &OneKvNodesRoutes,
    current_members: &[msg_pack::PutAtomicGroupMember],
    route_lookup: &dyn Fn(&str) -> Option<Arc<OneKvNodesRoutes>>,
    live_remote_nodes: &dyn Fn(&OneKvNodesRoutes) -> HashSet<String>,
) -> Result<HashSet<String>, RecoverableReplicaStatus> {
    if current_members.len() == 1 {
        let mut common_remote_nodes = None;
        intersect_remote_nodes(
            &mut common_remote_nodes,
            live_remote_nodes(route),
            owner_node_id,
        )?;
        return common_remote_nodes.ok_or(RecoverableReplicaStatus::CpuRouteAbsent);
    }
    let current_keys = current_members
        .iter()
        .map(|member| member.key.clone())
        .collect::<Vec<_>>();
    let current_put_ids = current_members
        .iter()
        .map(|member| member.put_id)
        .collect::<Vec<_>>();
    validate_recoverable_group(
        owner_node_id,
        &current_keys,
        Some(&current_put_ids),
        route_lookup,
        live_remote_nodes,
    )
}

fn route_recoverable_replica_status_with(
    owner_node_id: &str,
    key: &str,
    route: &OneKvNodesRoutes,
    desc: &NodeValueReplicaDesc,
    route_lookup: &dyn Fn(&str) -> Option<Arc<OneKvNodesRoutes>>,
    live_remote_nodes: &dyn Fn(&OneKvNodesRoutes) -> HashSet<String>,
) -> RecoverableReplicaStatus {
    if route.put_id != desc.put_id {
        return RecoverableReplicaStatus::VersionChanged;
    }
    if route.lease_id.is_some() || !route_has_live_replica_on_node(route, owner_node_id) {
        return RecoverableReplicaStatus::RouteIneligible;
    }

    let current_members = match current_atomic_group_members(key, route, desc) {
        Ok(members) => members,
        Err(status) => return status,
    };
    let Some(key_tp) = infer_sglang_tp_key(key) else {
        return match validate_current_recoverable_group(
            owner_node_id,
            route,
            &current_members,
            route_lookup,
            live_remote_nodes,
        ) {
            Ok(_) => RecoverableReplicaStatus::Recoverable,
            Err(status) => status,
        };
    };
    if key_tp.size == 1 {
        return match validate_current_recoverable_group(
            owner_node_id,
            route,
            &current_members,
            route_lookup,
            live_remote_nodes,
        ) {
            Ok(_) => RecoverableReplicaStatus::Recoverable,
            Err(status) => status,
        };
    }

    let mut logical_members = Vec::with_capacity(current_members.len());
    for member in &current_members {
        let Some(member_tp) = infer_sglang_tp_key(&member.key) else {
            return RecoverableReplicaStatus::TpCohortIncomplete;
        };
        if member_tp.rank != key_tp.rank || member_tp.size != key_tp.size {
            return RecoverableReplicaStatus::TpCohortIncomplete;
        }
        logical_members.push(member_tp.logical_prefix.to_string());
    }

    let mut cohort_remote_nodes: Option<HashSet<String>> = None;
    for rank in 0..key_tp.size {
        let peer_keys = logical_members
            .iter()
            .map(|logical_prefix| format!("{logical_prefix}_{rank}_{}", key_tp.size))
            .collect::<Vec<_>>();
        let rank_result = if rank == key_tp.rank {
            validate_current_recoverable_group(
                owner_node_id,
                route,
                &current_members,
                route_lookup,
                live_remote_nodes,
            )
        } else {
            validate_recoverable_group(
                owner_node_id,
                &peer_keys,
                None,
                route_lookup,
                live_remote_nodes,
            )
        };
        let rank_remote_nodes = match rank_result {
            Ok(nodes) => nodes,
            Err(_) => return RecoverableReplicaStatus::TpCohortIncomplete,
        };
        match cohort_remote_nodes.as_mut() {
            Some(common) => common.retain(|node_id| rank_remote_nodes.contains(node_id)),
            None => cohort_remote_nodes = Some(rank_remote_nodes),
        }
        if cohort_remote_nodes.as_ref().is_some_and(HashSet::is_empty) {
            return RecoverableReplicaStatus::TpCohortIncomplete;
        }
    }
    if cohort_remote_nodes.is_some_and(|nodes| !nodes.is_empty()) {
        RecoverableReplicaStatus::Recoverable
    } else {
        RecoverableReplicaStatus::TpCohortIncomplete
    }
}

#[cfg(test)]
fn route_has_recoverable_replica_with(
    owner_node_id: &str,
    key: &str,
    route: &OneKvNodesRoutes,
    desc: &NodeValueReplicaDesc,
    route_lookup: &dyn Fn(&str) -> Option<Arc<OneKvNodesRoutes>>,
    live_remote_nodes: &dyn Fn(&OneKvNodesRoutes) -> HashSet<String>,
) -> bool {
    route_recoverable_replica_status_with(
        owner_node_id,
        key,
        route,
        desc,
        route_lookup,
        live_remote_nodes,
    ) == RecoverableReplicaStatus::Recoverable
}

fn route_recoverable_replica_status(
    view: &MasterKvRouterView,
    owner_node_id: &str,
    key: &str,
    route: &OneKvNodesRoutes,
    desc: &NodeValueReplicaDesc,
) -> RecoverableReplicaStatus {
    route_recoverable_replica_status_with(
        owner_node_id,
        key,
        route,
        desc,
        &|member_key| {
            view.master_kv_router()
                .inner()
                .kv_routes
                .get(member_key)
                .map(|entry| entry.clone())
        },
        &|member_route| route_live_remote_cache_nodes(view, member_route),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_manager::ClusterMember;
    use std::collections::HashMap;

    #[test]
    fn one_kv_nodes_routes_only_reserves_two_get_durable_slots() {
        let routes = OneKvNodesRoutes {
            put_id: (1, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::new()),
            get_durable_slots_used: AtomicU32::new(0),
        };

        assert!(routes.try_reserve_get_durable_slot());
        assert!(routes.try_reserve_get_durable_slot());
        assert!(!routes.try_reserve_get_durable_slot());

        routes.release_get_durable_slot();
        assert!(routes.try_reserve_get_durable_slot());
    }

    #[test]
    fn reclaim_fence_is_atomic_with_all_master_key_activity() {
        let table = Arc::new(MasterKeyActivityTable::default());
        let item = OwnerReclaimItem {
            key: "k".to_string(),
            put_id: (7, 3),
            epoch: 11,
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id: 13,
                slot_index: 17,
                slot_size: 8 * 1024 * 1024,
            },
            reason: OwnerReclaimReason::Reserve,
        };

        let get_lease = table
            .reserve("k", MasterKeyActivityKind::Get, false)
            .expect("get activity should be admitted before reclaim");
        assert!(!table.is_quiescent("k"));
        assert_eq!(
            table.try_install_reclaim(&item),
            Err(MasterKeyActivitySnapshot {
                puts: 0,
                gets: 1,
                replicas: 0,
                reclaim_installed: false,
            })
        );
        drop(get_lease);

        assert!(table.is_quiescent("k"));
        assert!(table.try_install_reclaim(&item).is_ok());
        assert!(!table.is_quiescent("k"));
        assert!(table.has_reclaim("k"));
        assert!(
            table
                .reserve("k", MasterKeyActivityKind::Put, false)
                .is_none()
        );
        assert!(
            table
                .reserve("k", MasterKeyActivityKind::Get, false)
                .is_none()
        );
        assert!(
            table
                .reserve("k", MasterKeyActivityKind::Replica, false)
                .is_none()
        );

        assert!(table.clear_reclaim(&item));
        assert!(!table.has_reclaim("k"));
        assert!(
            table
                .reserve("k", MasterKeyActivityKind::Put, true)
                .is_some()
        );
    }

    #[test]
    fn explicit_activity_completion_is_idempotent_with_retired_cache_clones() {
        let table = Arc::new(MasterKeyActivityTable::default());
        let lease = table
            .reserve("retired-clone", MasterKeyActivityKind::Get, false)
            .expect("get activity should be admitted");
        let retired_cache_clone = lease.clone();

        lease.release_now();
        lease.release_now();
        assert!(table.is_quiescent("retired-clone"));

        drop(lease);
        drop(retired_cache_clone);
        assert!(table.is_quiescent("retired-clone"));
    }

    fn test_route_info(node_id: &str, tomb: bool) -> KvRouteInfo {
        let tomb_tag = NodeTombTag::new();
        if tomb {
            tomb_tag.set_tomb();
        }
        KvRouteInfo {
            node_id: node_id.to_string().into(),
            backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                owner_node_id: node_id.to_string().into(),
                grant_id: 1,
                slot_index: 0,
                slot_size: 1024,
                addr: 0,
                len: 1024,
                base_addr: 0,
            }),
            owner_local_indexed: true,
            tomb_tag,
        }
    }

    fn test_route(
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
        replicas: Vec<(&str, bool)>,
    ) -> OneKvNodesRoutes {
        OneKvNodesRoutes {
            put_id,
            lease_id,
            atomic_group: None,
            nodes_replicas: RwLock::new(
                replicas
                    .into_iter()
                    .map(|(node_id, tomb)| {
                        (node_id.to_string().into(), test_route_info(node_id, tomb))
                    })
                    .collect(),
            ),
            get_durable_slots_used: AtomicU32::new(0),
        }
    }

    fn test_remote_nodes(route: &OneKvNodesRoutes) -> HashSet<String> {
        route
            .nodes_replicas
            .read()
            .iter()
            .filter_map(|(node_id, replica)| {
                (!replica.tomb_tag.is_tomb() && node_id.as_ref().starts_with("cpu"))
                    .then(|| node_id.as_ref().to_string())
            })
            .collect()
    }

    #[test]
    fn recoverable_route_requires_live_local_and_live_remote_cache_same_version() {
        let desc = NodeValueReplicaDesc {
            weight_bytes: 1024,
            put_id: (7, 3),
        };
        let no_lookup = |_key: &str| None;
        let recoverable = |route: &OneKvNodesRoutes| {
            route_has_recoverable_replica_with(
                "gpu0",
                "k",
                route,
                &desc,
                &no_lookup,
                &test_remote_nodes,
            )
        };

        let cpu_copy = test_route((7, 3), None, vec![("gpu0", false), ("cpu0", false)]);
        assert!(recoverable(&cpu_copy));

        let local_only = test_route((7, 3), None, vec![("gpu0", false)]);
        assert!(!recoverable(&local_only));

        let gpu_copy = test_route((7, 3), None, vec![("gpu0", false), ("gpu1", false)]);
        assert!(!recoverable(&gpu_copy));

        let remote_tomb = test_route((7, 3), None, vec![("gpu0", false), ("cpu0", true)]);
        assert!(!recoverable(&remote_tomb));

        let local_tomb = test_route((7, 3), None, vec![("gpu0", true), ("cpu0", false)]);
        assert!(!recoverable(&local_tomb));

        let stale = test_route((8, 0), None, vec![("gpu0", false), ("cpu0", false)]);
        assert!(!recoverable(&stale));

        let leased = test_route((7, 3), Some(11), vec![("gpu0", false), ("cpu0", false)]);
        assert!(!recoverable(&leased));
    }

    #[test]
    fn recoverable_atomic_group_requires_one_common_remote_cache_owner() {
        let group = Arc::new(PutAtomicGroup {
            members: vec![
                msg_pack::PutAtomicGroupMember {
                    key: "k0".to_string(),
                    put_id: (7, 0),
                },
                msg_pack::PutAtomicGroupMember {
                    key: "k1".to_string(),
                    put_id: (7, 1),
                },
            ],
        });
        let make_route = |put_id, remote_node: Option<&str>| {
            let mut replicas = vec![("gpu0", false)];
            if let Some(remote_node) = remote_node {
                replicas.push((remote_node, false));
            }
            let mut route = test_route(put_id, None, replicas);
            route.atomic_group = Some(group.clone());
            Arc::new(route)
        };
        let desc = NodeValueReplicaDesc {
            weight_bytes: 1024,
            put_id: (7, 0),
        };

        let k0 = make_route((7, 0), Some("cpu0"));
        let k1 = make_route((7, 1), Some("cpu0"));
        let routes = HashMap::from([("k0".to_string(), k0.clone()), ("k1".to_string(), k1)]);
        assert!(route_has_recoverable_replica_with(
            "gpu0",
            "k0",
            &k0,
            &desc,
            &|key| routes.get(key).cloned(),
            &test_remote_nodes,
        ));

        let split_k1 = make_route((7, 1), Some("cpu1"));
        let split_routes =
            HashMap::from([("k0".to_string(), k0.clone()), ("k1".to_string(), split_k1)]);
        assert!(!route_has_recoverable_replica_with(
            "gpu0",
            "k0",
            &k0,
            &desc,
            &|key| split_routes.get(key).cloned(),
            &test_remote_nodes,
        ));

        let partial_k1 = make_route((7, 1), None);
        let partial_routes = HashMap::from([
            ("k0".to_string(), k0.clone()),
            ("k1".to_string(), partial_k1),
        ]);
        assert!(!route_has_recoverable_replica_with(
            "gpu0",
            "k0",
            &k0,
            &desc,
            &|key| partial_routes.get(key).cloned(),
            &test_remote_nodes,
        ));
    }

    #[test]
    fn infer_sglang_tp_key_accepts_only_canonical_suffix() {
        assert_eq!(
            infer_sglang_tp_key("prefix:page_model_1_2"),
            Some(InferredTpKey {
                logical_prefix: "prefix:page_model",
                rank: 1,
                size: 2,
            })
        );
        assert!(infer_sglang_tp_key("prefix:page_model_2_2").is_none());
        assert!(infer_sglang_tp_key("prefix:page_model_01_2").is_none());
        assert!(infer_sglang_tp_key("prefix:page_model_x_2").is_none());
        assert!(infer_sglang_tp_key("prefix:page_model_0_0").is_none());
    }

    #[test]
    fn recoverable_atomic_group_requires_complete_tp_cohort_on_one_cpu_owner() {
        let rank0_group = Arc::new(PutAtomicGroup {
            members: vec![
                msg_pack::PutAtomicGroupMember {
                    key: "page0_model_0_2".to_string(),
                    put_id: (10, 0),
                },
                msg_pack::PutAtomicGroupMember {
                    key: "page1_model_0_2".to_string(),
                    put_id: (10, 1),
                },
            ],
        });
        let rank1_group = Arc::new(PutAtomicGroup {
            members: vec![
                msg_pack::PutAtomicGroupMember {
                    key: "page0_model_1_2".to_string(),
                    put_id: (11, 0),
                },
                msg_pack::PutAtomicGroupMember {
                    key: "page1_model_1_2".to_string(),
                    put_id: (11, 1),
                },
            ],
        });
        let make_route = |put_id, group: Arc<PutAtomicGroup>, remote_node: Option<&str>| {
            let mut replicas = vec![("gpu0", false)];
            if let Some(remote_node) = remote_node {
                replicas.push((remote_node, false));
            }
            let mut route = test_route(put_id, None, replicas);
            route.atomic_group = Some(group);
            Arc::new(route)
        };
        let rank0_page0 = make_route((10, 0), rank0_group.clone(), Some("cpu0"));
        let rank0_page1 = make_route((10, 1), rank0_group.clone(), Some("cpu0"));
        let rank1_page0 = make_route((11, 0), rank1_group.clone(), Some("cpu0"));
        let rank1_page1 = make_route((11, 1), rank1_group.clone(), Some("cpu0"));
        let routes = HashMap::from([
            ("page0_model_0_2".to_string(), rank0_page0.clone()),
            ("page1_model_0_2".to_string(), rank0_page1),
            ("page0_model_1_2".to_string(), rank1_page0),
            ("page1_model_1_2".to_string(), rank1_page1),
        ]);
        let desc = NodeValueReplicaDesc {
            weight_bytes: 1024,
            put_id: (10, 0),
        };
        assert_eq!(
            current_tp_cohort_keys("page0_model_0_2", &rank0_page0, &desc).unwrap(),
            vec![
                "page0_model_0_2",
                "page1_model_0_2",
                "page0_model_1_2",
                "page1_model_1_2",
            ]
        );
        assert!(route_has_recoverable_replica_with(
            "gpu0",
            "page0_model_0_2",
            &rank0_page0,
            &desc,
            &|key| routes.get(key).cloned(),
            &test_remote_nodes,
        ));

        let missing_peer_routes = routes
            .iter()
            .filter(|(key, _)| key.as_str() != "page1_model_1_2")
            .map(|(key, route)| (key.clone(), route.clone()))
            .collect::<HashMap<_, _>>();
        assert_eq!(
            route_recoverable_replica_status_with(
                "gpu0",
                "page0_model_0_2",
                &rank0_page0,
                &desc,
                &|key| missing_peer_routes.get(key).cloned(),
                &test_remote_nodes,
            ),
            RecoverableReplicaStatus::TpCohortIncomplete
        );

        let rank1_page0_cpu1 = make_route((11, 0), rank1_group.clone(), Some("cpu1"));
        let rank1_page1_cpu1 = make_route((11, 1), rank1_group, Some("cpu1"));
        let split_cpu_routes = routes
            .iter()
            .map(|(key, route)| (key.clone(), route.clone()))
            .chain([
                ("page0_model_1_2".to_string(), rank1_page0_cpu1),
                ("page1_model_1_2".to_string(), rank1_page1_cpu1),
            ])
            .collect::<HashMap<_, _>>();
        assert!(!route_has_recoverable_replica_with(
            "gpu0",
            "page0_model_0_2",
            &rank0_page0,
            &desc,
            &|key| split_cpu_routes.get(key).cloned(),
            &test_remote_nodes,
        ));
    }

    #[test]
    fn recoverable_single_page_requires_all_tp_ranks() {
        let rank0 = Arc::new(test_route(
            (20, 0),
            None,
            vec![("gpu0", false), ("cpu0", false)],
        ));
        let rank1 = Arc::new(test_route(
            (21, 0),
            None,
            vec![("gpu0", false), ("cpu0", false)],
        ));
        let desc = NodeValueReplicaDesc {
            weight_bytes: 1024,
            put_id: (20, 0),
        };
        let complete = HashMap::from([
            ("page_model_0_2".to_string(), rank0.clone()),
            ("page_model_1_2".to_string(), rank1),
        ]);
        assert!(route_has_recoverable_replica_with(
            "gpu0",
            "page_model_0_2",
            &rank0,
            &desc,
            &|key| complete.get(key).cloned(),
            &test_remote_nodes,
        ));

        let rank0_only = HashMap::from([("page_model_0_2".to_string(), rank0.clone())]);
        assert!(!route_has_recoverable_replica_with(
            "gpu0",
            "page_model_0_2",
            &rank0,
            &desc,
            &|key| rank0_only.get(key).cloned(),
            &test_remote_nodes,
        ));
    }

    #[test]
    fn owner_hot_demotion_rejects_the_whole_cohort_before_moka_selection() {
        let expected_entries = HashMap::from([
            (
                "rank0".to_string(),
                NodeValueReplicaDesc {
                    weight_bytes: 1024,
                    put_id: (30, 0),
                },
            ),
            (
                "rank1".to_string(),
                NodeValueReplicaDesc {
                    weight_bytes: 1024,
                    put_id: (31, 0),
                },
            ),
        ]);
        let cache = moka::sync::SegmentedCache::builder(8)
            .weigher(Box::new(|_key: &String, desc: &NodeValueReplicaDesc| {
                desc.weight_bytes
            }))
            .build();
        for (key, desc) in &expected_entries {
            cache.insert(key.clone(), desc.clone());
        }
        cache.run_pending_tasks();

        let selected_weight = recoverable_cohort_weight(&expected_entries, |key, _desc| {
            if key == "rank0" {
                RecoverableReplicaStatus::Recoverable
            } else {
                RecoverableReplicaStatus::TpCohortIncomplete
            }
        })
        .map_or(0, |expected_weight| {
            cache.evict_some_if(expected_weight, |candidate_key, candidate_desc| {
                expected_entries.get(candidate_key).is_some_and(|expected| {
                    expected.put_id == candidate_desc.put_id
                        && expected.weight_bytes == candidate_desc.weight_bytes
                })
            })
        });

        assert_eq!(selected_weight, 0);
        assert!(cache.get("rank0").is_some());
        assert!(cache.get("rank1").is_some());
    }

    #[test]
    fn unrecoverable_fallback_is_only_for_last_level_or_no_remote_tier() {
        assert!(allow_unrecoverable_cache_eviction(
            false,
            false,
            UnrecoverableCacheEvictionContext::NormalCapacity,
        ));
        assert!(!allow_unrecoverable_cache_eviction(
            false,
            true,
            UnrecoverableCacheEvictionContext::NormalCapacity,
        ));
        assert!(allow_unrecoverable_cache_eviction(
            true,
            true,
            UnrecoverableCacheEvictionContext::NormalCapacity,
        ));
        assert!(allow_unrecoverable_cache_eviction(
            false,
            true,
            UnrecoverableCacheEvictionContext::OwnerLocalReserveNoSpace,
        ));
    }

    #[test]
    fn unbounded_segmented_metadata_cache_only_evicts_when_requested() {
        let cache = moka::sync::SegmentedCache::builder(8)
            .weigher(Box::new(|_key: &u64, weight: &u32| *weight))
            .build();
        assert_eq!(cache.policy().max_capacity(), None);

        for key in 0..128 {
            cache.insert(key, 8);
        }
        cache.run_pending_tasks();
        assert_eq!(cache.weighted_size(), 1024);

        let evicted = cache.evict_some_if(64, |_key, _weight| true);
        assert!(evicted >= 64);
        assert_eq!(cache.weighted_size(), 1024 - evicted);
    }

    #[test]
    fn synchronous_restore_re_eviction_keeps_new_pending_weight() {
        let weight = 8 * 1024 * 1024;
        let pending = AtomicU64::new(weight);

        // A synchronous Moka Size listener enqueues the restored entry again before the old
        // request completes. Completing the old request must leave exactly the new request.
        pending.fetch_add(weight, Ordering::AcqRel);
        subtract_pending_eviction_weight(&pending, "owner", weight);
        assert_eq!(pending.load(Ordering::Acquire), weight);

        subtract_pending_eviction_weight(&pending, "owner", weight);
        assert_eq!(pending.load(Ordering::Acquire), 0);
    }

    fn new_test_member(metadata: HashMap<String, String>) -> ClusterMember {
        ClusterMember {
            id: "node-a".to_string(),
            addresses: Vec::new(),
            port: None,
            node_start_time: 1,
            metadata,
            sub_cluster: Some("owner".to_string()),
            network: None,
        }
    }

    #[test]
    fn segment_registration_readiness_accepts_owner_without_local_ipc_root() {
        let member = new_test_member(HashMap::from([
            ("client".to_string(), "true".to_string()),
            ("p2p_relay".to_string(), "true".to_string()),
        ]));

        assert!(MasterKvRouter::member_ready_for_segment_registration(
            &member
        ));
    }

    #[test]
    fn segment_registration_readiness_rejects_external_and_side_worker() {
        let external = new_test_member(HashMap::from([(
            "external_client".to_string(),
            "true".to_string(),
        )]));
        assert!(!MasterKvRouter::member_ready_for_segment_registration(
            &external
        ));

        let side_worker = new_test_member(HashMap::from([
            ("client".to_string(), "true".to_string()),
            ("side_transfer_worker".to_string(), "true".to_string()),
            ("p2p_relay".to_string(), "true".to_string()),
        ]));
        assert!(!MasterKvRouter::member_ready_for_segment_registration(
            &side_worker
        ));
    }

    #[test]
    fn tier1_source_accepts_gpu_owner_role_and_rejects_remote_cache() {
        let placement_config = ReplicaTaskPlacementConfig::default();

        // This is the production GPU-owner shape: it deliberately does not match the
        // prefill/decode roles carried by the associated zero-contribution client.
        let gpu_owner = new_test_member(HashMap::from([
            ("client".to_string(), "true".to_string()),
            ("p2p_relay".to_string(), "true".to_string()),
        ]));
        assert!(MasterKvRouter::tier1_source_member_eligible(
            Some(&gpu_owner),
            &placement_config,
        ));

        let mut remote_cache = gpu_owner.clone();
        remote_cache.sub_cluster = Some("remote_cache".to_string());
        assert!(!MasterKvRouter::tier1_source_member_eligible(
            Some(&remote_cache),
            &placement_config,
        ));
        assert!(!MasterKvRouter::tier1_source_member_eligible(
            None,
            &placement_config,
        ));
    }
}

pub struct MasterKvRouterInner {
    view: std::sync::OnceLock<MasterKvRouterView>,
    pub policy: Box<dyn PlacementPolicy>,
    test_spec_config: TestSpecConfig,
    pub replica_task_placement: ReplicaTaskPlacementConfig,
    replica_cache_capacity_ratio: f64,
    replica_writeback_tier1_capacity_ratio: Option<f64>,

    /// (key, put_time_ms, put_version) -> inflight_put_info
    pub inflight_puts: moka::future::Cache<(String, u64, u32), InflightPutInfo>,
    pub inflight_replica_tasks: moka::future::Cache<(String, u64, u32), InflightReplicaTaskInfo>,
    pub inflight_gets: moka::future::Cache<u64, InflightGetInfo>,
    pub(crate) key_activity: Arc<MasterKeyActivityTable>,

    /// Cache for holding get operations (owned, flattened by (node_id, holder_id))
    pub get_holding: MasterOwnerMemMgr,

    /// Counter for get_id
    pub next_get_id: AtomicU64,

    /// Counter for holder_id
    pub next_holder_id: AtomicU64,

    /// Counter for local reserve grant identifiers.
    pub next_local_reserve_grant_id: AtomicU64,

    /// Counter for prepared put-key reservation identifiers.
    pub next_prepared_put_key_reservation_id: AtomicU64,

    /// Counter for two-sided owner reclaim epochs.
    pub next_owner_reclaim_epoch: AtomicU64,

    /// Latest version of key-value replicas
    pub kv_routes: DashMap<String, Arc<OneKvNodesRoutes>>,

    /// Interns recent multi-key put groups so all member routes share one descriptor.
    put_atomic_groups: moka::sync::SegmentedCache<(String, u64, u32), Arc<PutAtomicGroup>>,

    /// Serializes route-level existence checks with cache-eviction route cleanup.
    /// This protects route metadata consistency only; it does not create MemHolders
    /// or move payload data.
    pub route_lifetime_lock: Mutex<()>,

    /// Grants reserved for owner-local hot-path staging.
    pub local_reserve_grants: DashMap<u64, LocalReserveGrantInfo>,

    /// Prepared key reservations for owner-local hot-path staging.
    pub prepared_put_key_reservations: DashMap<u64, PreparedPutKeyReservationInfo>,

    /// Prefix-counting index for keys, used by CountPrefix RPC.
    pub prefix_index: ARwLock<PrefixRadixTree>,

    /// Support replicas: node_id -> key -> route_info
    pub node_kv_cache_controller:
        DashMap<NodeIDString, Arc<moka::sync::SegmentedCache<String, NodeValueReplicaDesc>>>,

    /// Inclusive hot tier. L1 eviction starts a remote-only replica task while the owner copy
    /// remains governed by `node_kv_cache_controller` at its unchanged resident capacity.
    pub node_writeback_tier1_controller:
        DashMap<NodeIDString, Arc<moka::sync::SegmentedCache<String, NodeValueReplicaDesc>>>,

    /// Per-node bytes reserved out of moka usable capacity.
    /// The reservation is reason-grouped, but `total_bytes` is the authority
    /// used to derive the effective moka max_capacity for the node.
    pub node_cache_reserved_capacity: DashMap<NodeIDString, Arc<NodeCacheReservedCapacity>>,

    /// Moka weight already removed and queued for owner-side safe reclaim.
    pub eviction_reclaim_pending_weight: DashMap<NodeIDString, Arc<AtomicU64>>,

    /// Per-owner reclaim lifecycle counters. These distinguish transient holder/activity
    /// deferrals from terminal route changes and bounded retry restoration.
    pub(crate) eviction_reclaim_counters: DashMap<NodeIDString, Arc<EvictionReclaimCounters>>,

    /// Serializes all append completions for one logical TP/atomic cohort. Without this lock,
    /// several member completions can pass the recoverability precheck together and concurrently
    /// select overlapping subsets from Moka.
    owner_hot_demotion_locks: AMapLock<String>,

    /// Historical final put placement decisions by target node.
    pub put_target_decision_counts: DashMap<NodeIDString, Arc<AtomicU64>>,

    /// Historical final put placement decisions by requester->target pair.
    pub put_requester_target_decision_counts: DashMap<RequesterTargetPair, Arc<AtomicU64>>,

    /// Historical final put placement decisions grouped by placement mode.
    pub put_placement_mode_counts: DashMap<&'static str, Arc<AtomicU64>>,

    /// Historical accepted replica task reservations by target node.
    pub replica_task_target_counts: DashMap<NodeIDString, Arc<AtomicU64>>,

    /// Historical get source choices by requester->source pair.
    pub get_requester_source_counts: DashMap<RequesterTargetPair, Arc<AtomicU64>>,
    pub get_requester_source_bytes: DashMap<RequesterTargetPair, Arc<AtomicU64>>,
    pub get_allocation_mode_counts: DashMap<&'static str, Arc<AtomicU64>>,

    /// Support replicas: key -> version_id
    recent_key_versionid_allocator: moka::sync::SegmentedCache<String, Arc<AtomicU32>>,

    pub delete_broadcast: EnsureMemholderMgmtDeleteHandle<DeleteKeyInfo>,
    post_route_maintenance_tx: ampsc::Sender<route_maintenance::RoutePublishEvent>,
    post_route_maintenance_rx: Mutex<Option<ampsc::Receiver<route_maintenance::RoutePublishEvent>>>,
    eviction_reclaim_tx: ampsc::Sender<reclaim::EvictionReclaimRequest>,
    eviction_reclaim_rx: Mutex<Option<ampsc::Receiver<reclaim::EvictionReclaimRequest>>>,
    tier1_writeback_tx: ampsc::Sender<tiered_writeback::Tier1WritebackRequest>,
    tier1_writeback_rx: Mutex<Option<ampsc::Receiver<tiered_writeback::Tier1WritebackRequest>>>,
    tier1_writeback_dedupe: moka::sync::SegmentedCache<(String, u64, u32), ()>,
    tier1_writeback_trigger_counts: DashMap<NodeIDString, Arc<AtomicU64>>,
    tier1_writeback_owner_accepted_counts: DashMap<NodeIDString, Arc<AtomicU64>>,
    tier1_writeback_failed_counts: DashMap<NodeIDString, Arc<AtomicU64>>,
}

impl MasterKvRouterInner {
    fn view(&self) -> &MasterKvRouterView {
        self.view.get().unwrap()
    }
}

pub struct MasterKvRouter(MasterKvRouterInner);

#[async_trait]
impl LogicalModule for MasterKvRouter {
    type View = MasterKvRouterView;
    type NewArg = MasterKvRouterNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "MasterKvRouter"
    }

    fn attach_view(&self, view: Self::View) {
        MasterKvRouter::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        info!("Shutting down MasterKvRouter");
        // Send shutdown signal to delete broadcast task to flush and exit.
        if let Err(e) = self
            .0
            .delete_broadcast
            .sender()
            .send(crate::master_kv_router::delete::DeleteKeyInfo::Shutdown)
            .await
        {
            warn!("Failed to send delete broadcast shutdown signal: {}", e);
        }
        Ok(())
    }
}

impl MasterKvRouter {
    fn member_ready_for_segment_registration(
        member: &crate::cluster_manager::ClusterMember,
    ) -> bool {
        // Segment registration must follow owner role semantics, not local IPC capability.
        //
        // Causal chain:
        // - owner/external topology is already encoded in cluster member metadata
        //   (`client`, `external_client`, `side_transfer_worker`, `p2p_relay`);
        // - `disable_local_ipc=true` intentionally suppresses `local_ipc_root` so the planner
        //   does not create same-machine IPC lanes;
        // - owner shared bundle publication still depends on segment registration;
        // - therefore the registration gate must stay tied to "real owner client" identity and
        //   must not reuse `local_ipc_root` as a readiness proxy.
        member.metadata.get("client").is_some_and(|v| v == "true")
            && member
                .metadata
                .get("p2p_relay")
                .is_some_and(|v| v == "true")
            && !member
                .metadata
                .get("external_client")
                .is_some_and(|v| v == "true")
            && !member
                .metadata
                .get("side_transfer_worker")
                .is_some_and(|v| v == "true")
    }

    pub fn attach_view(&self, view: MasterKvRouterView) {
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        self.0
            .view
            .set(view)
            .unwrap_or_else(|_| panic!("MasterKvRouter view attached twice"));
    }

    pub async fn construct(arg: MasterKvRouterNewArg) -> Result<Self, KvError> {
        let policy_impl = build_placement_policy(arg.replica_task_placement.clone());
        let inflight_put_ttl_seconds = if arg.test_spec_config.skip_put_end_commit {
            INFLIGHT_PUT_TTL_SECONDS_SKIP_PUT_END_COMMIT
        } else {
            INFLIGHT_PUT_TTL_SECONDS
        };
        let key_activity = Arc::new(MasterKeyActivityTable::default());
        let inflight_puts = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(inflight_put_ttl_seconds))
            .eviction_listener(|_put_id, inflight_info: InflightPutInfo, cause| {
                if cause == RemovalCause::Expired {
                    inflight_info._activity_lease.release_now();
                    if let Some(replica_target) = inflight_info.commit_info.replica_target.as_ref()
                    {
                        replica_target._activity_lease.release_now();
                    }
                }
            })
            .build();
        let inflight_gets = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .eviction_listener(|_get_id, inflight_info: InflightGetInfo, cause| {
                if cause == RemovalCause::Expired {
                    inflight_info.release_durable_slot_if_needed();
                    inflight_info._activity_lease.release_now();
                }
            })
            .build();
        let inflight_replica_tasks = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .eviction_listener(|_put_id, inflight_info: InflightReplicaTaskInfo, cause| {
                if cause == RemovalCause::Expired {
                    inflight_info._activity_lease.release_now();
                }
            })
            .build();
        let (eviction_reclaim_tx, eviction_reclaim_rx) =
            ampsc::channel(EVICTION_RECLAIM_QUEUE_CAPACITY);
        let (post_route_maintenance_tx, post_route_maintenance_rx) =
            ampsc::channel(POST_ROUTE_MAINTENANCE_QUEUE_CAPACITY);
        let (tier1_writeback_tx, tier1_writeback_rx) =
            ampsc::channel(TIER1_WRITEBACK_QUEUE_CAPACITY);
        let inner = MasterKvRouterInner {
            view: std::sync::OnceLock::new(),
            policy: policy_impl,
            test_spec_config: arg.test_spec_config,
            replica_task_placement: arg.replica_task_placement,
            replica_cache_capacity_ratio: arg.replica_cache_capacity_ratio,
            replica_writeback_tier1_capacity_ratio: arg.replica_writeback_tier1_capacity_ratio,
            inflight_puts,
            inflight_replica_tasks,
            inflight_gets,
            key_activity,
            get_holding: MasterOwnerMemMgr::default(),
            next_get_id: AtomicU64::new(0),
            next_holder_id: AtomicU64::new(0),
            next_local_reserve_grant_id: AtomicU64::new(1),
            next_prepared_put_key_reservation_id: AtomicU64::new(1),
            next_owner_reclaim_epoch: AtomicU64::new(1),
            kv_routes: DashMap::new(),
            put_atomic_groups: moka::sync::SegmentedCache::builder(8)
                .max_capacity(262_144)
                .time_to_idle(Duration::from_secs(30 * 60))
                .build(),
            route_lifetime_lock: Mutex::new(()),
            local_reserve_grants: DashMap::new(),
            prepared_put_key_reservations: DashMap::new(),
            prefix_index: ARwLock::new(PrefixRadixTree::new()),
            node_kv_cache_controller: DashMap::new(),
            node_writeback_tier1_controller: DashMap::new(),
            node_cache_reserved_capacity: DashMap::new(),
            eviction_reclaim_pending_weight: DashMap::new(),
            eviction_reclaim_counters: DashMap::new(),
            owner_hot_demotion_locks: AMapLock::new(Duration::from_secs(10 * 60)),
            put_target_decision_counts: DashMap::new(),
            put_requester_target_decision_counts: DashMap::new(),
            put_placement_mode_counts: DashMap::new(),
            replica_task_target_counts: DashMap::new(),
            get_requester_source_counts: DashMap::new(),
            get_requester_source_bytes: DashMap::new(),
            get_allocation_mode_counts: DashMap::new(),
            recent_key_versionid_allocator: moka::sync::SegmentedCache::builder(8)
                .time_to_idle(Duration::from_secs(5))
                .build(),
            delete_broadcast: EnsureMemholderMgmtDeleteHandle::new(
                MasterOwnerMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            post_route_maintenance_tx,
            post_route_maintenance_rx: Mutex::new(Some(post_route_maintenance_rx)),
            eviction_reclaim_tx,
            eviction_reclaim_rx: Mutex::new(Some(eviction_reclaim_rx)),
            tier1_writeback_tx,
            tier1_writeback_rx: Mutex::new(Some(tier1_writeback_rx)),
            tier1_writeback_dedupe: moka::sync::SegmentedCache::builder(8)
                .time_to_live(Duration::from_secs(60))
                .build(),
            tier1_writeback_trigger_counts: DashMap::new(),
            tier1_writeback_owner_accepted_counts: DashMap::new(),
            tier1_writeback_failed_counts: DashMap::new(),
        };
        Ok(Self(inner))
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), KvError> {
        info!("MasterKvRouter init2_for_init_dag");
        self.register_rpc_handlers();
        self.register_rpc_callers();
        let view = self.0.view().clone();

        self.spawn_cluster_listener();
        self.spawn_put_placement_reporter();
        self.spawn_runtime_observe_reporter();

        let delete_broadcast_rx = self
            .0
            .delete_broadcast
            .take_rx()
            .expect("delete_broadcast rx already taken, that's impossible");
        delete::spawn_delete_broadcast(view, delete_broadcast_rx);
        if let Some(post_route_maintenance_rx) = self.0.post_route_maintenance_rx.lock().take() {
            route_maintenance::spawn_post_route_maintenance_actor(
                self.0.view().clone(),
                post_route_maintenance_rx,
            );
        } else {
            warn!("post_route_maintenance_rx already taken for MasterKvRouter");
        }
        if let Some(eviction_reclaim_rx) = self.0.eviction_reclaim_rx.lock().take() {
            reclaim::spawn_eviction_reclaim_actor(self.0.view().clone(), eviction_reclaim_rx);
        } else {
            warn!("eviction_reclaim_rx already taken for MasterKvRouter");
        }
        if let Some(tier1_writeback_rx) = self.0.tier1_writeback_rx.lock().take() {
            tiered_writeback::spawn_tier1_writeback_actor(
                self.0.view().clone(),
                tier1_writeback_rx,
            );
        } else {
            warn!("tier1_writeback_rx already taken for MasterKvRouter");
        }
        Ok(())
    }

    pub(crate) fn view(&self) -> &MasterKvRouterView {
        self.inner().view()
    }

    pub fn inner(&self) -> &MasterKvRouterInner {
        &self.0
    }

    fn replica_cache_base_capacity(&self, node_space_size: u64) -> u64 {
        (node_space_size as f64 * self.inner().replica_cache_capacity_ratio).floor() as u64
    }

    fn replica_cache_effective_capacity(&self, node_id: &str, node_space_size: u64) -> u64 {
        let reserved_capacity = self
            .inner()
            .node_cache_reserved_capacity
            .get(node_id)
            .map(|reserved| reserved.total_reserved_bytes())
            .unwrap_or(0);
        self.replica_cache_base_capacity(node_space_size)
            .saturating_sub(reserved_capacity)
    }

    fn owner_cache_allows_unrecoverable_eviction_in_context(
        &self,
        owner_node_id: &str,
        context: UnrecoverableCacheEvictionContext,
    ) -> bool {
        let remote_only_roles = &self.inner().replica_task_placement.remote_only_node_roles;
        let owner = self
            .inner()
            .view()
            .cluster_manager()
            .get_member_info_cached(owner_node_id);
        let owner_is_remote_only =
            placement::member_matches_roles(owner.as_ref(), remote_only_roles);
        let has_ready_remote_only_owner = self
            .inner()
            .view()
            .cluster_manager()
            .get_members()
            .into_iter()
            .any(|member| {
                placement::member_matches_roles(Some(&member), remote_only_roles)
                    && self
                        .inner()
                        .view()
                        .master_seg_manager()
                        .get_node_space_size(member.id.as_str())
                        != 0
            });
        allow_unrecoverable_cache_eviction(
            owner_is_remote_only,
            has_ready_remote_only_owner,
            context,
        )
    }

    pub(crate) fn owner_cache_allows_unrecoverable_eviction(&self, owner_node_id: &str) -> bool {
        self.owner_cache_allows_unrecoverable_eviction_in_context(
            owner_node_id,
            UnrecoverableCacheEvictionContext::NormalCapacity,
        )
    }

    pub(crate) fn owner_cache_allows_unrecoverable_reserve_pressure_eviction(
        &self,
        owner_node_id: &str,
    ) -> bool {
        self.owner_cache_allows_unrecoverable_eviction_in_context(
            owner_node_id,
            UnrecoverableCacheEvictionContext::OwnerLocalReserveNoSpace,
        )
    }

    fn writeback_tier1_base_capacity(&self, node_space_size: u64) -> Option<u64> {
        self.inner()
            .replica_writeback_tier1_capacity_ratio
            .map(|ratio| (node_space_size as f64 * ratio).floor() as u64)
    }

    pub fn tiered_writeback_enabled(&self) -> bool {
        self.inner()
            .replica_writeback_tier1_capacity_ratio
            .is_some()
            && self.replica_cache_enabled()
    }

    pub fn replica_cache_enabled(&self) -> bool {
        !self.0.test_spec_config.disable_master_replica_cache
    }

    pub fn prefix_index_enabled(&self) -> bool {
        !self.0.test_spec_config.disable_prefix_index
    }

    /// return (put_time_ms, put_version)
    pub fn get_recent_key_versionid(&self, key: String) -> (u64, u32) {
        let put_time_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let put_version = self
            .inner()
            .recent_key_versionid_allocator
            .get_with(key, || Arc::new(AtomicU32::new(0)))
            .fetch_add(1, Ordering::Relaxed);
        (put_time_ms, put_version)
    }

    pub(crate) fn reserve_inflight_put_key(
        &self,
        key: &str,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
    ) -> Result<Arc<MasterKeyActivityLease>, KvError> {
        if reject_if_exist_same_key && self.key_has_live_replica(key) {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyAlreadyExists {
                    key: key.to_string(),
                },
            ));
        }
        self.inner()
            .key_activity
            .reserve(key, MasterKeyActivityKind::Put, reject_if_inflight_same_key)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyBeingWritten {
                        key: key.to_string(),
                    },
                )
            })
    }

    pub(crate) fn reserve_inflight_get_key(
        &self,
        key: &str,
    ) -> Result<Arc<MasterKeyActivityLease>, KvError> {
        self.inner()
            .key_activity
            .reserve(key, MasterKeyActivityKind::Get, false)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyNotFound {
                        key: key.to_string(),
                    },
                )
            })
    }

    pub(crate) fn reserve_inflight_replica_key(
        &self,
        key: &str,
    ) -> Result<Arc<MasterKeyActivityLease>, KvError> {
        self.inner()
            .key_activity
            .reserve(key, MasterKeyActivityKind::Replica, false)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyBeingWritten {
                        key: key.to_string(),
                    },
                )
            })
    }

    pub fn key_has_live_replica(&self, key: &str) -> bool {
        self.inner()
            .kv_routes
            .get(key)
            .map(|one_kv_nodes_routes| {
                one_kv_nodes_routes
                    .nodes_replicas
                    .read()
                    .values()
                    .any(|kv_info| !kv_info.tomb_tag.is_tomb())
            })
            .unwrap_or(false)
    }

    pub(crate) fn resolve_put_atomic_group(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        group: Option<PutAtomicGroup>,
    ) -> Result<Option<Arc<PutAtomicGroup>>, KvError> {
        let Some(group) = group else {
            return Ok(None);
        };
        if !(2..=4096).contains(&group.members.len()) {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "put atomic group must contain 2..=4096 members; got={}",
                        group.members.len()
                    ),
                },
            ));
        }
        let mut keys = HashSet::with_capacity(group.members.len());
        let mut current_member_count = 0usize;
        for member in &group.members {
            if member.key.is_empty() || !keys.insert(member.key.as_str()) {
                return Err(KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                        detail: "put atomic group member keys must be non-empty and unique"
                            .to_string(),
                    },
                ));
            }
            if member.key == key && member.put_id == put_id {
                current_member_count += 1;
            }
        }
        if current_member_count != 1 {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "put atomic group must contain current member exactly once: key={} put_id=({},{}) count={}",
                        key, put_id.0, put_id.1, current_member_count
                    ),
                },
            ));
        }

        let anchor = group
            .members
            .first()
            .expect("validated non-empty put atomic group");
        let cache_key = (anchor.key.clone(), anchor.put_id.0, anchor.put_id.1);
        if let Some(existing) = self.inner().put_atomic_groups.get(&cache_key) {
            if existing.as_ref() != &group {
                return Err(KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                        detail: format!(
                            "put atomic group anchor was reused with different membership: anchor_key={} anchor_put_id=({},{})",
                            anchor.key, anchor.put_id.0, anchor.put_id.1
                        ),
                    },
                ));
            }
            return Ok(Some(existing));
        }
        let group = Arc::new(group);
        self.inner()
            .put_atomic_groups
            .insert(cache_key, group.clone());
        Ok(Some(group))
    }

    pub(crate) fn next_owner_reclaim_epoch(&self) -> u64 {
        self.inner()
            .next_owner_reclaim_epoch
            .fetch_add(1, Ordering::Relaxed)
    }

    fn enqueue_eviction_reclaim(
        &self,
        owner_node_id: NodeIDString,
        key: String,
        desc: NodeValueReplicaDesc,
    ) {
        let weight_bytes = desc.weight_bytes;
        let pending_weight = self
            .inner()
            .eviction_reclaim_pending_weight
            .entry(owner_node_id.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        pending_weight.fetch_add(u64::from(weight_bytes), Ordering::AcqRel);
        let request = reclaim::EvictionReclaimRequest {
            owner_node_id,
            key,
            desc,
            retry_count: 0,
        };
        if let Err(err) = self.inner().eviction_reclaim_tx.try_send(request) {
            let queue_error = err.to_string();
            let request = err.into_inner();
            subtract_pending_eviction_weight(
                pending_weight.as_ref(),
                request.owner_node_id.as_str(),
                u64::from(weight_bytes),
            );
            let restored = self.restore_eviction_cache_entry_if_current(
                request.owner_node_id.as_str(),
                request.key,
                request.desc,
            );
            warn!(
                "safe eviction reclaim queue is full or closed: {}; restored={}",
                queue_error, restored
            );
        }
    }

    pub(crate) fn eviction_reclaim_pending_weight(&self, owner_node_id: &str) -> u64 {
        self.inner()
            .eviction_reclaim_pending_weight
            .get(owner_node_id)
            .map(|weight| weight.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    pub(crate) fn eviction_reclaim_counters(
        &self,
        owner_node_id: &str,
    ) -> Arc<EvictionReclaimCounters> {
        self.inner()
            .eviction_reclaim_counters
            .entry(owner_node_id.to_string())
            .or_insert_with(|| Arc::new(EvictionReclaimCounters::default()))
            .value()
            .clone()
    }

    pub(crate) fn complete_eviction_reclaim_weight(
        &self,
        owner_node_id: &str,
        completed_weight: u64,
    ) {
        if completed_weight == 0 {
            return;
        }
        let pending_weight = self
            .inner()
            .eviction_reclaim_pending_weight
            .get(owner_node_id)
            .unwrap_or_else(|| {
                panic!(
                    "eviction reclaim pending weight missing for owner {}",
                    owner_node_id
                )
            });
        subtract_pending_eviction_weight(
            pending_weight.value().as_ref(),
            owner_node_id,
            completed_weight,
        );
    }

    pub(crate) fn restore_eviction_cache_entry_if_current(
        &self,
        owner_node_id: &str,
        key: String,
        desc: NodeValueReplicaDesc,
    ) -> bool {
        if !self.eviction_cache_entry_is_current(owner_node_id, &key, &desc) {
            return false;
        }
        self.insert_node_cache_entry(owner_node_id, key, desc)
    }

    pub(crate) fn eviction_cache_entry_is_current(
        &self,
        owner_node_id: &str,
        key: &str,
        desc: &NodeValueReplicaDesc,
    ) -> bool {
        self.inner().kv_routes.get(key).is_some_and(|route| {
            route.put_id == desc.put_id
                && route.lease_id.is_none()
                && route
                    .nodes_replicas
                    .read()
                    .iter()
                    .any(|(node_id, replica)| {
                        node_id.as_ref() == owner_node_id && !replica.tomb_tag.is_tomb()
                    })
        })
    }

    fn eviction_cache_entry_recoverable_status(
        &self,
        owner_node_id: &str,
        key: &str,
        desc: &NodeValueReplicaDesc,
    ) -> RecoverableReplicaStatus {
        let Some(route) = self.inner().kv_routes.get(key).map(|route| route.clone()) else {
            return RecoverableReplicaStatus::RouteAbsent;
        };
        route_recoverable_replica_status(self.inner().view(), owner_node_id, key, &route, desc)
    }

    pub(crate) fn evict_recoverable_cache_weight(
        &self,
        owner_node_id: &str,
        requested_weight: u64,
    ) -> u64 {
        self.evict_recoverable_cache_weight_excluding(
            owner_node_id,
            requested_weight,
            &HashSet::new(),
        )
    }

    pub(crate) fn evict_recoverable_cache_weight_excluding(
        &self,
        owner_node_id: &str,
        requested_weight: u64,
        excluded_keys: &HashSet<String>,
    ) -> u64 {
        if requested_weight == 0 {
            return 0;
        }
        let Some(cache) = self.get_node_cache_controller(owner_node_id) else {
            return 0;
        };
        let counters = self.eviction_reclaim_counters(owner_node_id);
        counters
            .recoverable_first_requested_bytes
            .fetch_add(requested_weight, Ordering::Relaxed);
        let selected_weight = cache.evict_some_if(requested_weight, |key, desc| {
            if excluded_keys.contains(key) {
                return false;
            }
            let status = self.eviction_cache_entry_recoverable_status(owner_node_id, key, desc);
            counters.record_recoverable_first_status(status);
            status == RecoverableReplicaStatus::Recoverable
        });
        counters
            .recoverable_first_selected_bytes
            .fetch_add(selected_weight, Ordering::Relaxed);
        counters.recoverable_first_shortfall_bytes.fetch_add(
            requested_weight.saturating_sub(selected_weight),
            Ordering::Relaxed,
        );
        selected_weight
    }

    pub(crate) async fn demote_owner_hot_cohort_if_recoverable(
        &self,
        owner_node_id: &str,
        key: &str,
        put_id: PutIDForAKey,
    ) -> u64 {
        let counters = self.eviction_reclaim_counters(owner_node_id);
        counters
            .owner_hot_demotion_attempts
            .fetch_add(1, Ordering::Relaxed);

        let Some(cache) = self.get_node_cache_controller(owner_node_id) else {
            return 0;
        };
        cache.run_pending_tasks();
        let Some(anchor_desc) = cache.get(key) else {
            return 0;
        };
        if anchor_desc.put_id != put_id {
            return 0;
        }
        let Some(anchor_route) = self.inner().kv_routes.get(key).map(|route| route.clone()) else {
            return 0;
        };
        let Ok(cohort_keys) = current_tp_cohort_keys(key, &anchor_route, &anchor_desc) else {
            return 0;
        };
        let Some(cohort_anchor) = cohort_keys.iter().min() else {
            return 0;
        };
        let cohort_lock = self
            .inner()
            .owner_hot_demotion_locks
            .get_lock(format!("{owner_node_id}\0{cohort_anchor}"));
        let _cohort_guard = cohort_lock.lock().await;

        // The route and Moka state may have changed while another member completion held the
        // cohort lock. Recompute the complete cohort under the lock before selecting anything.
        cache.run_pending_tasks();
        let Some(anchor_desc) = cache.get(key) else {
            return 0;
        };
        if anchor_desc.put_id != put_id {
            return 0;
        }
        let Some(anchor_route) = self.inner().kv_routes.get(key).map(|route| route.clone()) else {
            return 0;
        };
        let Ok(locked_cohort_keys) = current_tp_cohort_keys(key, &anchor_route, &anchor_desc)
        else {
            return 0;
        };
        if locked_cohort_keys != cohort_keys {
            counters
                .owner_hot_demotion_precheck_rejected
                .fetch_add(1, Ordering::Relaxed);
            return 0;
        }

        // Demotion is an all-or-nothing TP/atomic-cohort transition.  Do not
        // silently shrink the cohort when one of its source entries has
        // already disappeared from this owner's cache: doing so can recreate
        // the mixed local/remote layout that made the prefix unrecoverable in
        // E23.
        let expected_cohort_len = locked_cohort_keys.len();
        let expected_entries = locked_cohort_keys
            .into_iter()
            .map(|cohort_key| cache.get(&cohort_key).map(|desc| (cohort_key, desc)))
            .collect::<Option<HashMap<_, _>>>();
        let Some(expected_entries) = expected_entries else {
            counters
                .owner_hot_demotion_precheck_rejected
                .fetch_add(1, Ordering::Relaxed);
            return 0;
        };
        if expected_entries.len() != expected_cohort_len || expected_entries.is_empty() {
            counters
                .owner_hot_demotion_precheck_rejected
                .fetch_add(1, Ordering::Relaxed);
            return 0;
        }

        // Validate the whole TP/atomic cohort before asking Moka to select anything.  Performing
        // this check in the eviction predicate allowed one recoverable member to be selected even
        // when a later member was not recoverable, which turned an intended exclusive transition
        // into a partial source demotion.
        let Some(expected_weight) =
            recoverable_cohort_weight(&expected_entries, |candidate_key, candidate_desc| {
                let status = self.eviction_cache_entry_recoverable_status(
                    owner_node_id,
                    candidate_key,
                    candidate_desc,
                );
                counters.record_recoverable_first_status(status);
                status
            })
        else {
            counters
                .owner_hot_demotion_precheck_rejected
                .fetch_add(1, Ordering::Relaxed);
            return 0;
        };

        let selected_weight =
            cache.evict_some_if(expected_weight, |candidate_key, candidate_desc| {
                expected_entries
                    .get(candidate_key)
                    .is_some_and(|expected_desc| {
                        expected_desc.put_id == candidate_desc.put_id
                            && expected_desc.weight_bytes == candidate_desc.weight_bytes
                    })
            });
        if selected_weight != expected_weight {
            counters
                .owner_hot_demotion_partial_mismatch
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "owner hot TP-cohort demotion selected a partial cohort: owner={} key={} put_id=({},{}) cohort_entries={} expected_weight={} selected_weight={}",
                owner_node_id,
                key,
                put_id.0,
                put_id.1,
                expected_entries.len(),
                expected_weight,
                selected_weight
            );
        }
        if selected_weight == expected_weight {
            counters
                .owner_hot_demotion_cohorts
                .fetch_add(1, Ordering::Relaxed);
            counters
                .owner_hot_demotion_selected_bytes
                .fetch_add(selected_weight, Ordering::Relaxed);
            tracing::debug!(
                "owner hot complete TP-cohort demotion selected: owner={} key={} put_id=({},{}) cohort_entries={} selected_weight={}",
                owner_node_id,
                key,
                put_id.0,
                put_id.1,
                expected_entries.len(),
                selected_weight
            );
        }
        selected_weight
    }

    pub(crate) fn insert_node_cache_entry(
        &self,
        owner_node_id: &str,
        key: String,
        desc: NodeValueReplicaDesc,
    ) -> bool {
        let Some(cache) = self.get_node_cache_controller(owner_node_id) else {
            return false;
        };
        cache.run_pending_tasks();
        let existing_weight = cache
            .get(&key)
            .map(|existing| u64::from(existing.weight_bytes))
            .unwrap_or(0);
        let projected_weight = cache
            .weighted_size()
            .saturating_sub(existing_weight)
            .saturating_add(u64::from(desc.weight_bytes));
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(owner_node_id);
        let capacity = self.replica_cache_effective_capacity(owner_node_id, node_space_size);
        let requested_weight = projected_weight.saturating_sub(capacity);
        if requested_weight != 0 {
            let recoverable_selected_weight =
                self.evict_recoverable_cache_weight(owner_node_id, requested_weight);
            let fallback_requested_weight =
                requested_weight.saturating_sub(recoverable_selected_weight);
            let fallback_selected_weight =
                if self.owner_cache_allows_unrecoverable_eviction(owner_node_id) {
                    cache.evict_some(fallback_requested_weight)
                } else {
                    0
                };
            tracing::debug!(
                "recoverable-first cache admission: owner={} key={} requested_weight={} recoverable_selected_weight={} fallback_requested_weight={} fallback_selected_weight={} projected_weight={} capacity={}",
                owner_node_id,
                key,
                requested_weight,
                recoverable_selected_weight,
                fallback_requested_weight,
                fallback_selected_weight,
                projected_weight,
                capacity
            );
        }
        cache.insert(key, desc);
        true
    }

    fn register_rpc_callers(&self) {
        RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new().regist(self.0.view().p2p_module());
        RPCCaller::<BatchOwnerReclaimReq>::new().regist(self.0.view().p2p_module());
        RPCCaller::<BatchEnqueueReplicaTaskReq>::new().regist(self.0.view().p2p_module());
    }

    fn register_rpc_handlers(&self) {
        let p2p = self.0.view().p2p_module();

        // --- Get Handlers ---
        let view = self.0.view().clone();
        RPCHandler::<GetStartReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let cleanup_view = view.clone();
            let _ = view.spawn("rpc_get_start", async move {
                let t0 = Utc::now().timestamp_micros();
                let (get_id, mut ack) =
                    handle_get_start(view_task, msg, resp.node_id().clone()).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                let get_started = ack.serialize_part.error_code
                    == crate::rpcresp_kvresult_convert::msg_and_error::OK;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetStartResp: {:?}", e);
                    if get_started {
                        if let Some(inflight_info) = cleanup_view
                            .master_kv_router()
                            .inner()
                            .inflight_gets
                            .remove(&get_id)
                            .await
                        {
                            inflight_info.release_durable_slot_if_needed();
                            inflight_info._activity_lease.release_now();
                        }
                    }
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<GetRevokeReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_get_revoke", async move {
                let ack = handle_get_revoke(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetRevokeResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<GetDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_get_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_get_done(view_task, msg).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        // --- CountPrefix Handler ---
        let view = self.0.view().clone();
        RPCHandler::<CountPrefixReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_count_prefix", async move {
                let ack = handle_count_prefix(&view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send CountPrefixResp: {:?}", e);
                }
            });
            Ok(())
        });

        // --- GetMasterOnlyMetricPart Handler (metrics module registers) ---
        crate::metrics::datasource::register_master_only_metric_handler(self.0.view());

        // --- Put Handlers ---
        let view = self.0.view().clone();
        RPCHandler::<ReserveLocalGrantReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_reserve_local_grant", async move {
                let t0 = Utc::now().timestamp_micros();
                let (grant_id, mut ack) =
                    handle_reserve_local_grant(view_task.clone(), msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send ReserveLocalGrantResp: {:?}", e);
                    if grant_id != 0 {
                        let _ = view_task
                            .master_kv_router()
                            .take_local_reserve_grant(grant_id);
                    }
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<ReleaseLocalGrantReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_release_local_grant", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_release_local_grant(view_task, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send ReleaseLocalGrantResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchPreparePutKeysReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_batch_prepare_put_keys", async move {
                let t0 = Utc::now().timestamp_micros();
                let (reservation_ids, mut ack) =
                    handle_batch_prepare_put_keys(view_task.clone(), msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchPreparePutKeysResp: {:?}", e);
                    for reservation_id in reservation_ids {
                        let _ = view_task
                            .master_kv_router()
                            .take_prepared_put_key_reservation(reservation_id);
                    }
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchReleasePutKeyReservationsReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_batch_release_put_key_reservations", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack =
                    handle_batch_release_put_key_reservations(view_task, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchReleasePutKeyReservationsResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<PutStartReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_put_start", async move {
                let key = msg.serialize_part.key.clone();
                #[cfg(feature = "test_bins")]
                tracing::info!(
                    "rpc_put_start handler begin: self={} peer={} task_id={} key={} len={}",
                    view_task.cluster_manager().get_self_info().id,
                    req_node_id,
                    resp.task_id(),
                    key,
                    msg.serialize_part.len
                );
                let t0 = Utc::now().timestamp_micros();
                let (put_id, mut ack) = handle_put_start(view_task.clone(), msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutStartResp: {:?}", e);
                    if put_id != (0, 0) {
                        if let Some(inflight_info) = view_task
                            .master_kv_router()
                            .inner()
                            .inflight_puts
                            .remove(&(key, put_id.0, put_id.1))
                            .await
                        {
                            inflight_info._activity_lease.release_now();
                            if let Some(replica_target) =
                                inflight_info.commit_info.replica_target.as_ref()
                            {
                                replica_target._activity_lease.release_now();
                            }
                        }
                    }
                } else {
                    #[cfg(feature = "test_bins")]
                    tracing::info!(
                        "rpc_put_start response sent: self={} peer={} task_id={} key={} put_id=({},{})",
                        view_task.cluster_manager().get_self_info().id,
                        resp.node_id(),
                        resp.task_id(),
                        key,
                        put_id.0,
                        put_id.1
                    );
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<PutRevokeReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_put_revoke", async move {
                let ack = handle_put_revoke(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutRevokeResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<PutDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let req_node_id = resp.node_id().clone();
            let _ = view.spawn("rpc_put_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_put_done(view_task, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        // --- MemHolder Handlers ---
        // let view = inner.view.clone();
        // RPCHandler::<MemHolderKeepAliveReq>::new().regist(p2p, move |resp, msg| {
        //     let view = view.clone();
        //     tokio::spawn(async move {
        //         let ack = handle_mem_holder_keep_alive(view, msg).await;
        //         if let Err(e) = resp.send_resp(ack).await {
        //             error!("Failed to send MemHolderKeepAliveResp: {}", e);
        //         }
        //     });
        //     Ok(())
        // });

        // let view = inner.view.clone();
        // RPCHandler::<MemHolderReleaseReq>::new().regist(p2p, move |resp, msg| {
        //     let view = view.clone();
        //     tokio::spawn(async move {
        //         let ack = handle_mem_holder_release(view, msg).await;
        //         if let Err(e) = resp.send_resp(ack).await {
        //             error!("Failed to send MemHolderReleaseResp: {}", e);
        //         }
        //     });
        //     Ok(())
        // });

        // --- Delete Handler ---
        let view = self.0.view().clone();
        RPCHandler::<DeleteReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_delete", async move {
                let ack = handle_delete(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send DeleteResp: {:?}", e);
                }
            });
            Ok(())
        });

        // --- DeleteAck Handler ---
        let view = self.0.view().clone();
        RPCHandler::<DeleteAckReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            view.spawn("rpc_delete_ack", async move {
                let ack = handle_delete_ack(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send DeleteAckResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchDeleteAckReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            view.spawn("rpc_batch_delete_ack", async move {
                let ack = handle_batch_delete_ack(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchDeleteAckResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<PutAppendStartReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_put_append_start", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_put_append_start(view_task.clone(), msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutAppendStartResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<PutAppendRevokeReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_put_append_revoke", async move {
                let ack = handle_put_append_revoke(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutAppendRevokeResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<PutAppendDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_put_append_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_put_append_done(view_task, msg).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutAppendDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        // --- GetMeta Handler ---
        let view = self.0.view().clone();
        RPCHandler::<GetMetaReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            view.spawn("rpc_get_meta", async move {
                let ack = handle_get_meta(view2, msg, resp.node_id().clone()).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetMetaResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchIsExistReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            view.spawn("rpc_batch_is_exist", async move {
                let ack = handle_batch_is_exist(view2, msg, resp.node_id().clone()).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchIsExistResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchPutStartReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            view.spawn("rpc_batch_put_start", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_put_start(view2, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchPutStartResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchPutRevokeReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            view.spawn("rpc_batch_put_revoke", async move {
                let ack = handle_batch_put_revoke(view2, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchPutRevokeResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchPutDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            view.spawn("rpc_batch_put_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_put_done(view2, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchPutDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<GroupedBatchPutDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            view.spawn("rpc_grouped_batch_put_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_grouped_batch_put_done(view2, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GroupedBatchPutDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchGetStartReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let cleanup_view = view.clone();
            let req_node_id = resp.node_id().clone();
            view.spawn("rpc_batch_get_start", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_get_start(view2, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                let started_get_ids = ack
                    .serialize_part
                    .items
                    .iter()
                    .filter(|item| {
                        item.error_code == crate::rpcresp_kvresult_convert::msg_and_error::OK
                    })
                    .map(|item| item.get_id)
                    .collect::<Vec<_>>();
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchGetStartResp: {:?}", e);
                    for get_id in started_get_ids {
                        if let Some(inflight_info) = cleanup_view
                            .master_kv_router()
                            .inner()
                            .inflight_gets
                            .remove(&get_id)
                            .await
                        {
                            inflight_info.release_durable_slot_if_needed();
                            inflight_info._activity_lease.release_now();
                        }
                    }
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchGetRevokeReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            view.spawn("rpc_batch_get_revoke", async move {
                let ack = handle_batch_get_revoke(view2, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchGetRevokeResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchGetDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            view.spawn("rpc_batch_get_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_get_done(view2, msg).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchGetDoneResp: {:?}", e);
                }
            });
            Ok(())
        });
    }

    fn spawn_node_segement_registration_caller(&self) -> ampsc::Sender<ClusterEvent> {
        const KEEP_ALIVE_TIME: Duration = Duration::from_secs(30);
        const NODE_EVENT_QUEUE_CAPACITY: usize = 64;
        let (tx, mut rx) = ampsc::channel::<ClusterEvent>(NODE_EVENT_QUEUE_CAPACITY);
        let view = self.inner().view().clone();
        let view_task = view.clone();
        view.spawn("node_segment_registration_caller", async move {
            use std::future::Future;
            use std::pin::Pin;

            const INITIAL_BACKOFF: Duration = Duration::from_secs(3);
            const MAX_BACKOFF: Duration = Duration::from_secs(60);

            let mut shutdown_waiter = view_task.register_shutdown_waiter();

            // The cluster_listener keeps a per-node sender. This task only receives events for a
            // single node_id (enforced by cluster_listener routing).
            let mut actor_node_id: Option<NodeIDString> = None;

            // Desired registration epoch (ClusterMember.node_start_time) for this node.
            let mut desired_epoch: Option<i64> = None;
            let mut registered_epoch: Option<i64> = None;
            let mut last_seen_epoch: Option<i64> = None;

            let mut backoff: Duration = INITIAL_BACKOFF;
            type SleepFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
            let mut retry_sleep: Option<SleepFuture> = None;
            let mut inflight: Option<(i64, Pin<Box<dyn Future<Output = Result<(), KvError>> + Send>>)> =
                None;

            fn bump_backoff(cur: Duration) -> Duration {
                let next = cur.checked_mul(2).unwrap();
                if next > MAX_BACKOFF {
                    MAX_BACKOFF
                } else {
                    next
                }
            }

	            fn ensure_actor_node_id(view: &MasterKvRouterView, actor: &mut Option<NodeIDString>, node_id: &str) {
	                if let Some(existing) = actor.as_ref() {
	                    if existing != node_id {
	                        view.async_panic("node_segment_registration_caller received mismatched node_id".to_owned());
	                    }
	                } else {
	                    *actor = Some(node_id.to_string());
	                }
	            }

	            fn observe_member_epoch(
	                view: &MasterKvRouterView,
	                member: &crate::cluster_manager::ClusterMember,
	                actor_node_id: &mut Option<NodeIDString>,
	                registered_epoch: Option<i64>,
	                last_seen_epoch: &mut Option<i64>,
	            ) -> Option<i64> {
	                let node_id = member.id.clone();
	                ensure_actor_node_id(view, actor_node_id, &node_id);

	                if !matches!(member.node_role(), crate::cluster_manager::NodeRole::Client) {
	                    return None;
	                }

	                let epoch = member.node_start_time;
	                if let Some(prev) = *last_seen_epoch {
	                    if prev != epoch {
	                        view.master_seg_manager()
	                            .mark_node_tomb(&node_id.clone().into());
	                    }
	                }
	                *last_seen_epoch = Some(epoch);

	                if registered_epoch == Some(epoch) {
	                    return None;
	                }
	                if !MasterKvRouter::member_ready_for_segment_registration(member) {
	                    return None;
	                }
	                Some(epoch)
	            }

	            loop {
	                // Keep the actor alive while there is pending/active work.
	                if desired_epoch.is_some() && inflight.is_none() && retry_sleep.is_none() {
	                    retry_sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(0))));
	                }

                // Idle fast-path: allow actor cleanup when no work remains.
                if desired_epoch.is_none() && inflight.is_none() && retry_sleep.is_none() {
                    tokio::select! {
                        _ = tokio::time::sleep(KEEP_ALIVE_TIME) => {
                            break;
                        }
                        _ = shutdown_waiter.wait() => {
                            break;
                        }
                        msg = rx.recv() => {
                            let Some(event) = msg else {
                                break;
                            };

	                            match event {
	                                ClusterEvent::MemberJoined(member) | ClusterEvent::MemberUpdated(member) => {
	                                    if let Some(epoch) = observe_member_epoch(
	                                        &view_task,
	                                        &member,
	                                        &mut actor_node_id,
	                                        registered_epoch,
	                                        &mut last_seen_epoch,
	                                    ) {
	                                        desired_epoch = Some(epoch);
	                                        backoff = INITIAL_BACKOFF;
	                                        retry_sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(0))));
	                                    }
	                                }
	                                ClusterEvent::MemberLeft(node_id) => {
	                                    ensure_actor_node_id(&view_task, &mut actor_node_id, &node_id);

	                                    desired_epoch = None;
                                    registered_epoch = None;
                                    last_seen_epoch = None;
                                    retry_sleep = None;
                                    inflight = None;

                                    debug!(
                                        "MasterKvRouter received node leave event: {:?}, mark it as tomb",
                                        node_id
                                    );
                                    view_task.master_seg_manager().mark_node_tomb(&node_id.into());
                                }
                            }
                        }
                    }
                    continue;
                }

                // Retry timer branch.
                if let Some(sleep) = retry_sleep.as_mut() {
                    tokio::select! {
                        _ = sleep.as_mut() => {
                            retry_sleep = None;

                            let node_id = actor_node_id
                                .clone()
                                .expect("node_segment_registration_caller missing actor_node_id");
                            let Some(epoch) = desired_epoch else {
                                continue;
                            };

                            // Validate membership and epoch before each attempt.
                            let Some(member) = view_task.cluster_manager().get_member_info_cached(&node_id) else {
                                desired_epoch = None;
                                continue;
                            };
	                            if !matches!(member.node_role(), crate::cluster_manager::NodeRole::Client) {
	                                desired_epoch = None;
	                                continue;
	                            }
	                            if member.node_start_time != epoch {
	                                desired_epoch = None;
	                                continue;
	                            }
	                            if !MasterKvRouter::member_ready_for_segment_registration(&member) {
	                                desired_epoch = None;
	                                continue;
	                            }

                            let fut = view_task
                                .master_seg_manager()
                                .request_segment_registration(node_id.clone().into(), epoch);
                            inflight = Some((epoch, Box::pin(fut)));
                        }
                        _ = shutdown_waiter.wait() => {
                            break;
                        }
                        msg = rx.recv() => {
                            let Some(event) = msg else {
                                break;
                            };

	                            match event {
	                                ClusterEvent::MemberJoined(member) | ClusterEvent::MemberUpdated(member) => {
	                                    if let Some(epoch) = observe_member_epoch(
	                                        &view_task,
	                                        &member,
	                                        &mut actor_node_id,
	                                        registered_epoch,
	                                        &mut last_seen_epoch,
	                                    ) {
	                                        desired_epoch = Some(epoch);
	                                        backoff = INITIAL_BACKOFF;
	                                        retry_sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(0))));
	                                    }
	                                }
	                                ClusterEvent::MemberLeft(node_id) => {
	                                    ensure_actor_node_id(&view_task, &mut actor_node_id, &node_id);

	                                    desired_epoch = None;
                                    registered_epoch = None;
                                    last_seen_epoch = None;
                                    retry_sleep = None;
                                    inflight = None;

                                    debug!(
                                        "MasterKvRouter received node leave event: {:?}, mark it as tomb",
                                        node_id
                                    );
                                    view_task.master_seg_manager().mark_node_tomb(&node_id.into());
                                }
                            }
                        }
                    }
                    continue;
                }

                // In-flight RPC branch.
                if let Some((inflight_epoch, fut)) = inflight.as_mut() {
                    tokio::select! {
                        res = fut => {
                            let epoch = *inflight_epoch;
                            inflight = None;

                            // If a newer epoch was requested while this RPC was in-flight, ignore the result.
                            if desired_epoch != Some(epoch) {
                                continue;
                            }

                            match res {
                                Ok(()) => {
                                    registered_epoch = Some(epoch);
                                    desired_epoch = None;
                                    backoff = INITIAL_BACKOFF;
                                    info!(
                                        "Successfully requested segment registration from client {}",
                                        actor_node_id.clone().unwrap_or_default()
                                    );
                                }
                                Err(e) => {
                                    // Epoch mismatch means the peer has restarted; stop and wait for a new epoch event.
                                    if matches!(
                                        e,
                                        KvError::Api(crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch { .. })
                                    ) {
                                        desired_epoch = None;
                                        continue;
                                    }

                                    error!(
                                        "Failed to request segment registration from client {}: {}",
                                        actor_node_id.clone().unwrap_or_default(),
                                        e
                                    );
                                    retry_sleep = Some(Box::pin(tokio::time::sleep(backoff)));
                                    backoff = bump_backoff(backoff);
                                }
                            }
                        }
                        _ = shutdown_waiter.wait() => {
                            break;
                        }
                        msg = rx.recv() => {
                            let Some(event) = msg else {
                                break;
                            };

	                            match event {
	                                ClusterEvent::MemberJoined(member) | ClusterEvent::MemberUpdated(member) => {
	                                    if let Some(epoch) = observe_member_epoch(
	                                        &view_task,
	                                        &member,
	                                        &mut actor_node_id,
	                                        registered_epoch,
	                                        &mut last_seen_epoch,
	                                    ) {
	                                        desired_epoch = Some(epoch);
	                                        backoff = INITIAL_BACKOFF;
	                                        retry_sleep = Some(Box::pin(tokio::time::sleep(Duration::from_secs(0))));
	                                    }
	                                }
	                                ClusterEvent::MemberLeft(node_id) => {
	                                    ensure_actor_node_id(&view_task, &mut actor_node_id, &node_id);

	                                    desired_epoch = None;
                                    registered_epoch = None;
                                    last_seen_epoch = None;
                                    retry_sleep = None;
                                    inflight = None;

                                    debug!(
                                        "MasterKvRouter received node leave event: {:?}, mark it as tomb",
                                        node_id
                                    );
                                    view_task.master_seg_manager().mark_node_tomb(&node_id.into());
                                }
                            }
                        }
                    }
                    continue;
                }
            }
        });
        tx
    }

    fn spawn_cluster_listener(&self) {
        let view = self.inner().view().clone();
        let view_task = view.clone();
        let _ = view.spawn("cluster_listener", async move {
            // Drive per-node segment registration from a member snapshot periodically.
            // This avoids permanently relying on best-effort event delivery.
            const RECONCILE_INTERVAL_SECS: u64 = 2;
            const SEND_BACKPRESSURE_WARN_SECS: u64 = 5;

            let mut listen_cluster_event = view_task.cluster_manager().listen();
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            let mut each_node_segement_registration_caller: HashMap<
                NodeIDString,
                ampsc::Sender<ClusterEvent>,
            > = HashMap::new();

            async fn send_event_with_warn(
                view: &MasterKvRouterView,
                node_id: &str,
                tx: ampsc::Sender<ClusterEvent>,
                event: ClusterEvent,
            ) -> Result<(), ClusterEvent> {
                let mut send_fut = Box::pin(tx.send(event.clone()));
                let mut warn_sleep = Box::pin(tokio::time::sleep(Duration::from_secs(SEND_BACKPRESSURE_WARN_SECS)));

                loop {
                    tokio::select! {
                        res = &mut send_fut => {
                            return match res {
                                Ok(()) => Ok(()),
                                Err(_e) => Err(event),
                            };
                        }
                        _ = &mut warn_sleep => {
                            warn!(
                                "Backpressure: waiting to deliver cluster event to node registration actor (queue full?): node_id={}, event={:?}",
                                node_id,
                                event
                            );
                            warn_sleep = Box::pin(tokio::time::sleep(Duration::from_secs(SEND_BACKPRESSURE_WARN_SECS)));
                        }
                    }
                }
            }

            async fn deliver_event(
                view: &MasterKvRouterView,
                each_node_segement_registration_caller: &mut HashMap<
                    NodeIDString,
                    ampsc::Sender<ClusterEvent>,
                >,
                event: ClusterEvent,
            ) {
                match &event {
                    ClusterEvent::MemberLeft(node_id) => {
                        let removed = view
                            .master_kv_router()
                            .inner()
                            .get_holding
                            .cleanup_node(&node_id);
                        if removed > 0 {
                            info!("Cleaned up {} holdings for left member {}", removed, node_id);
                        }
                    }
                    _ => {}
                }

                let node_id = event.node_id();
                loop {
                    let tx = each_node_segement_registration_caller
                        .entry(node_id.clone())
                        .or_insert_with(|| {
                            view.master_kv_router()
                                .spawn_node_segement_registration_caller()
                        })
                        .clone();

                    match send_event_with_warn(view, &node_id, tx, event.clone()).await {
                        Ok(()) => return,
                        Err(_ev) => {
                            // Receiver dropped: recreate and retry.
                            each_node_segement_registration_caller.insert(
                                node_id.clone(),
                                view.master_kv_router()
                                    .spawn_node_segement_registration_caller(),
                            );
                        }
                    }
                }
            }

            let mut reconcile_sleep = Box::pin(tokio::time::sleep(Duration::from_secs(RECONCILE_INTERVAL_SECS)));

            loop {
                tokio::select! {
                    _ = &mut reconcile_sleep => {
                        reconcile_sleep = Box::pin(tokio::time::sleep(Duration::from_secs(RECONCILE_INTERVAL_SECS)));
                        let members = view_task.cluster_manager().get_client_members();
                        for member in members {
                            deliver_event(
                                &view_task,
                                &mut each_node_segement_registration_caller,
                                ClusterEvent::MemberUpdated(member),
                            ).await;
                        }
                    }
                    event = listen_cluster_event.recv() => {
                        match event {
                            Ok(ev) => {
                                deliver_event(
                                    &view_task,
                                    &mut each_node_segement_registration_caller,
                                    ev,
                                ).await;
                            }
                            Err(e) => {
                                warn!("Cluster event receiver error (will resubscribe): {}", e);
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
    }

    fn record_put_placement_decision(
        &self,
        requester_node_id: &str,
        target_node_id: &str,
        placement_mode: PutPlacementMode,
    ) {
        // Record only the final accepted placement decision that is returned from put_start.
        // This avoids counting allocator probes or failed attempts as real placement outcomes.
        let target_counter = self
            .inner()
            .put_target_decision_counts
            .entry(target_node_id.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        target_counter.fetch_add(1, Ordering::Relaxed);

        let requester_target_key = RequesterTargetPair::new(requester_node_id, target_node_id);
        let requester_target_counter = self
            .inner()
            .put_requester_target_decision_counts
            .entry(requester_target_key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        requester_target_counter.fetch_add(1, Ordering::Relaxed);

        let mode_key = match placement_mode {
            PutPlacementMode::Local => "local",
            PutPlacementMode::Remote => "remote",
        };
        let mode_counter = self
            .inner()
            .put_placement_mode_counts
            .entry(mode_key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        mode_counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_replica_task_target(&self, target_node_id: &str) {
        let target_counter = self
            .inner()
            .replica_task_target_counts
            .entry(target_node_id.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        target_counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_get_source_selection(
        &self,
        requester_node_id: &str,
        source_node_id: &str,
        bytes: u64,
        allocation_mode: GetAllocationMode,
    ) {
        let requester_source_key = RequesterTargetPair::new(requester_node_id, source_node_id);
        let source_count = self
            .inner()
            .get_requester_source_counts
            .entry(requester_source_key.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        source_count.fetch_add(1, Ordering::Relaxed);

        let source_bytes = self
            .inner()
            .get_requester_source_bytes
            .entry(requester_source_key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        source_bytes.fetch_add(bytes, Ordering::Relaxed);

        let mode_key = match allocation_mode {
            GetAllocationMode::Temporary => "temporary",
            GetAllocationMode::ReuseReplica => "reuse_replica",
            GetAllocationMode::DurableReplica => "durable_replica",
        };
        let mode_count = self
            .inner()
            .get_allocation_mode_counts
            .entry(mode_key)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        mode_count.fetch_add(1, Ordering::Relaxed);
    }

    fn spawn_put_placement_reporter(&self) {
        let view = self.inner().view().clone();
        let view_task = view.clone();
        let _ = view.spawn("put_placement_reporter", async move {
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            let mut interval =
                tokio::time::interval(Duration::from_secs(PLACEMENT_REPORT_INTERVAL_SECS));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let router = view_task.master_kv_router();

                        let mut target_counts: Vec<(String, u64)> = router
                            .inner()
                            .put_target_decision_counts
                            .iter()
                            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        target_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        let mut requester_target_counts: Vec<(String, u64)> = router
                            .inner()
                            .put_requester_target_decision_counts
                            .iter()
                            .map(|entry| (entry.key().as_log_key(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        requester_target_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        let mut mode_counts: Vec<(String, u64)> = router
                            .inner()
                            .put_placement_mode_counts
                            .iter()
                            .map(|entry| (entry.key().to_string(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        mode_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        let mut replica_task_target_counts: Vec<(String, u64)> = router
                            .inner()
                            .replica_task_target_counts
                            .iter()
                            .map(|entry| (entry.key().clone(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        replica_task_target_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        let mut get_source_counts: Vec<(String, u64)> = router
                            .inner()
                            .get_requester_source_counts
                            .iter()
                            .map(|entry| (entry.key().as_log_key(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        get_source_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        let mut get_source_bytes: Vec<(String, u64)> = router
                            .inner()
                            .get_requester_source_bytes
                            .iter()
                            .map(|entry| (entry.key().as_log_key(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        get_source_bytes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        let mut get_allocation_mode_counts: Vec<(String, u64)> = router
                            .inner()
                            .get_allocation_mode_counts
                            .iter()
                            .map(|entry| (entry.key().to_string(), entry.value().load(Ordering::Relaxed)))
                            .collect();
                        get_allocation_mode_counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

                        info!(
                            "placement historical distribution | put_target_counts={:?} | put_mode_counts={:?} | put_requester_target_counts={:?} | replica_task_target_counts={:?} | get_requester_source_counts={:?} | get_requester_source_bytes={:?} | get_allocation_mode_counts={:?}",
                            target_counts,
                            mode_counts,
                            requester_target_counts,
                            replica_task_target_counts,
                            get_source_counts,
                            get_source_bytes,
                            get_allocation_mode_counts,
                        );
                    }
                    _ = shutdown_waiter.wait() => {
                        break;
                    }
                }
            }
        });
    }

    pub fn get_node_cache_controller(
        &self,
        node_id: &str,
    ) -> Option<Arc<moka::sync::SegmentedCache<String, NodeValueReplicaDesc>>> {
        if !self.replica_cache_enabled() {
            return None;
        }
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(node_id);
        if node_space_size == 0 {
            return None;
        }
        let view = self.inner().view().clone();
        let node_id_owned = node_id.to_string();
        Some(
            self.inner()
                .node_kv_cache_controller
                .entry(node_id_owned.clone())
                .or_insert_with(move || {
                    let view = view.clone();
                    let cache_node_id = node_id_owned.clone();
                    Arc::new(
                        moka::sync::SegmentedCache::builder(8)
                            // The resident cache is a metadata/LRU controller. Its aggregate
                            // effective capacity is enforced explicitly with `evict_some_if`.
                            // Leaving the Moka segments unbounded prevents per-segment hash skew
                            // from performing an independent Size eviction outside that policy.
                            // Use the actual allocated/rounded size as weight to
                            // make eviction reflect real memory usage.
                            .weigher(Box::new(|_key: &String, value: &NodeValueReplicaDesc| {
                                value.weight_bytes
                            }))
                            .eviction_listener(Box::new(
                                move |key: Arc<String>,
                                      _value: NodeValueReplicaDesc,
                                      cause: RemovalCause| {
                                    debug!("Evicted key: {:?}, caused by: {:?}", key, cause);
                                    match cause {
                                        // timeout or size exceed
                                        RemovalCause::Size | RemovalCause::Expired => {
                                            let k = (*key).clone();
                                            let evicted_put_id = _value.put_id;
                                            tracing::debug!(
                                                "Eviction-triggered local replica cleanup for key {} on node {} put_id=({},{})",
                                                k,
                                                cache_node_id,
                                                evicted_put_id.0,
                                                evicted_put_id.1
                                            );
                                            view.master_kv_router().enqueue_eviction_reclaim(
                                                cache_node_id.clone(),
                                                k,
                                                _value,
                                            );
                                        }
                                        _ => {}
                                    }
                                },
                            ))
                            .build(),
                    )
                })
                .value()
                .clone(),
        )
    }

    pub fn tier1_source_node_eligible(&self, node_id: &str) -> bool {
        if !self.tiered_writeback_enabled() {
            return false;
        }
        let member = self
            .inner()
            .view()
            .cluster_manager()
            .get_member_info_cached(node_id);
        Self::tier1_source_member_eligible(member.as_ref(), &self.inner().replica_task_placement)
    }

    fn tier1_source_member_eligible(
        member: Option<&crate::cluster_manager::ClusterMember>,
        placement_config: &ReplicaTaskPlacementConfig,
    ) -> bool {
        // Route entries are keyed by the storage owner that holds the payload. In the
        // external-SGLang topology that owner is tagged `sglang_owner`; the separate
        // zero-contribution SGLang client carries the `prefill`/`decode` role. Requiring
        // the owner itself to match active_node_roles therefore disables T1 entirely.
        //
        // A T1 source only needs to be a known, non-remote-only owner. The caller also
        // requires a non-zero registered segment before constructing the tier cache.
        member.is_some()
            && !placement::member_matches_roles(member, &placement_config.remote_only_node_roles)
    }

    pub fn get_node_writeback_tier1_controller(
        &self,
        node_id: &str,
    ) -> Option<Arc<moka::sync::SegmentedCache<String, NodeValueReplicaDesc>>> {
        if !self.tier1_source_node_eligible(node_id) {
            return None;
        }
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(node_id);
        let base_capacity = self.writeback_tier1_base_capacity(node_space_size)?;
        if base_capacity == 0 {
            return None;
        }
        let view = self.inner().view().clone();
        let node_id_owned = node_id.to_string();
        Some(
            self.inner()
                .node_writeback_tier1_controller
                .entry(node_id_owned.clone())
                .or_insert_with(move || {
                    let cache_node_id = node_id_owned.clone();
                    Arc::new(
                        moka::sync::SegmentedCache::builder(8)
                            .max_capacity(base_capacity)
                            .weigher(Box::new(|_key: &String, value: &NodeValueReplicaDesc| {
                                value.weight_bytes
                            }))
                            .eviction_listener(Box::new(
                                move |key: Arc<String>,
                                      value: NodeValueReplicaDesc,
                                      cause: RemovalCause| {
                                    if cause == RemovalCause::Size {
                                        view.master_kv_router().enqueue_tier1_writeback(
                                            cache_node_id.clone(),
                                            (*key).clone(),
                                            value,
                                        );
                                    }
                                },
                            ))
                            .build(),
                    )
                })
                .value()
                .clone(),
        )
    }

    pub fn remove_node_writeback_tier1_entry(&self, node_id: &str, key: &str) {
        if let Some(cache) = self
            .inner()
            .node_writeback_tier1_controller
            .get(node_id)
            .map(|entry| entry.value().clone())
        {
            let _ = cache.remove(key);
        }
    }

    pub(crate) fn tier1_writeback_entry_is_current(
        &self,
        source_node_id: &str,
        key: &str,
        desc: &NodeValueReplicaDesc,
    ) -> bool {
        if self.inner().inflight_replica_tasks.contains_key(&(
            key.to_string(),
            desc.put_id.0,
            desc.put_id.1,
        )) {
            return false;
        }
        self.inner().kv_routes.get(key).is_some_and(|route| {
            if route.put_id != desc.put_id || route.lease_id.is_some() {
                return false;
            }
            let replicas = route.nodes_replicas.read();
            replicas.values().any(|replica| {
                replica.node_id.as_ref() == source_node_id && !replica.tomb_tag.is_tomb()
            }) && replicas.values().all(|replica| {
                replica.tomb_tag.is_tomb() || replica.node_id.as_ref() == source_node_id
            })
        })
    }

    fn enqueue_tier1_writeback(
        &self,
        source_node_id: NodeIDString,
        key: String,
        desc: NodeValueReplicaDesc,
    ) {
        if !self.tier1_writeback_entry_is_current(&source_node_id, &key, &desc) {
            return;
        }
        let dedupe_key = (key.clone(), desc.put_id.0, desc.put_id.1);
        if self
            .inner()
            .tier1_writeback_dedupe
            .get(&dedupe_key)
            .is_some()
        {
            return;
        }
        self.inner()
            .tier1_writeback_dedupe
            .insert(dedupe_key.clone(), ());
        Self::increment_tier1_writeback_counter(
            &self.inner().tier1_writeback_trigger_counts,
            &source_node_id,
            1,
        );
        let request = tiered_writeback::Tier1WritebackRequest {
            source_node_id: source_node_id.clone(),
            key,
            desc,
        };
        if let Err(err) = self.inner().tier1_writeback_tx.try_send(request) {
            self.inner().tier1_writeback_dedupe.remove(&dedupe_key);
            self.record_tier1_writeback_failed(&source_node_id, 1);
            tracing::warn!("tier1 write-back queue is full or closed: {}", err);
        }
    }

    fn increment_tier1_writeback_counter(
        counters: &DashMap<NodeIDString, Arc<AtomicU64>>,
        source_node_id: &str,
        count: u64,
    ) {
        if count == 0 {
            return;
        }
        counters
            .entry(source_node_id.to_string())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_tier1_writeback_owner_accepted(&self, source_node_id: &str, count: u64) {
        Self::increment_tier1_writeback_counter(
            &self.inner().tier1_writeback_owner_accepted_counts,
            source_node_id,
            count,
        );
    }

    pub(crate) fn record_tier1_writeback_failed(&self, source_node_id: &str, count: u64) {
        Self::increment_tier1_writeback_counter(
            &self.inner().tier1_writeback_failed_counts,
            source_node_id,
            count,
        );
    }

    fn tier1_writeback_counter(
        counters: &DashMap<NodeIDString, Arc<AtomicU64>>,
        source_node_id: &str,
    ) -> u64 {
        counters
            .get(source_node_id)
            .map(|counter| counter.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub(crate) fn finish_tier1_writeback_request(
        &self,
        request: tiered_writeback::Tier1WritebackRequest,
    ) {
        self.inner().tier1_writeback_dedupe.remove(&(
            request.key,
            request.desc.put_id.0,
            request.desc.put_id.1,
        ));
    }

    /// Atomically adjust a node's moka usable-capacity reservation by `delta_bytes`.
    /// Positive delta reserves capacity (fetch_sub from usable capacity),
    /// negative delta releases reservation (fetch_add back to usable capacity).
    pub fn adjust_node_cache_reserved_capacity(
        &self,
        node_id: &str,
        reason: ReservedCapacityReason,
        delta_bytes: i64,
    ) -> crate::rpcresp_kvresult_convert::msg_and_error::KvResult<()> {
        if !self.replica_cache_enabled() {
            return Ok(());
        }
        let reserved_capacity = self
            .inner()
            .node_cache_reserved_capacity
            .entry(node_id.to_string())
            .or_insert_with(|| Arc::new(NodeCacheReservedCapacity::default()))
            .value()
            .clone();

        reserved_capacity.apply_delta(reason, delta_bytes);

        // Recompute target capacity from the configured base ratio minus live reservations.
        let reserved_total = reserved_capacity.total_reserved_bytes();
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(node_id);
        if node_space_size == 0 {
            // Node not ready: this should not happen in a successful put_done path.
            // Revert the counter delta before returning error.
            reserved_capacity.apply_delta(reason, -delta_bytes);
            return Err(
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::OwnerNoSeg {
                        detail: format!(
                            "node_id={} has no segment (node_space_size=0) while adjusting cache capacity",
                            node_id
                        ),
                    },
                ),
            );
        }
        let base_capacity = self.replica_cache_base_capacity(node_space_size);
        let new_capacity = base_capacity.saturating_sub(reserved_total);

        if let Some(cache) = self.get_node_cache_controller(node_id) {
            if let Some(tier1_cache) = self
                .inner()
                .node_writeback_tier1_controller
                .get(node_id)
                .map(|entry| entry.value().clone())
            {
                let tier1_capacity = self
                    .writeback_tier1_base_capacity(node_space_size)
                    .unwrap_or(0)
                    .min(new_capacity);
                if let Err(e) = tier1_cache.set_max_capacity(tier1_capacity) {
                    reserved_capacity.apply_delta(reason, -delta_bytes);
                    return Err(
                        crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                            crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                                rpc_input_json: format!(
                                    "tier1 moka.set_max_capacity failed: node_id={}, new_capacity={}, err={}",
                                    node_id, tier1_capacity, e
                                ),
                            },
                        ),
                    );
                }
            }
            cache.run_pending_tasks();
            let requested_weight = cache.weighted_size().saturating_sub(new_capacity);
            let recoverable_selected_weight =
                self.evict_recoverable_cache_weight(node_id, requested_weight);
            let fallback_requested_weight =
                requested_weight.saturating_sub(recoverable_selected_weight);
            let fallback_selected_weight =
                if self.owner_cache_allows_unrecoverable_eviction(node_id) {
                    cache.evict_some(fallback_requested_weight)
                } else {
                    0
                };
            if requested_weight != 0 {
                tracing::debug!(
                    "resident cache effective-capacity adjustment: owner={} effective_capacity={} requested_weight={} recoverable_selected_weight={} fallback_requested_weight={} fallback_selected_weight={}",
                    node_id,
                    new_capacity,
                    requested_weight,
                    recoverable_selected_weight,
                    fallback_requested_weight,
                    fallback_selected_weight
                );
            }
            Ok(())
        } else {
            // Revert counter and return error.
            reserved_capacity.apply_delta(reason, -delta_bytes);
            Err(
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::OwnerNoSeg {
                        detail: format!("node_id={} cache_controller not found", node_id),
                    },
                ),
            )
        }
    }

    pub fn next_local_reserve_grant_id(&self) -> u64 {
        self.inner()
            .next_local_reserve_grant_id
            .fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_prepared_put_key_reservation_id(&self) -> u64 {
        self.inner()
            .next_prepared_put_key_reservation_id
            .fetch_add(1, Ordering::Relaxed)
    }

    pub fn install_local_reserve_grant(&self, grant_id: u64, grant: LocalReserveGrantInfo) {
        if self
            .inner()
            .local_reserve_grants
            .insert(grant_id, grant)
            .is_some()
        {
            panic!(
                "duplicate local reserve grant id indicates a logic bug: grant_id={}",
                grant_id
            );
        }
    }

    pub fn take_local_reserve_grant(&self, grant_id: u64) -> Option<LocalReserveGrantInfo> {
        self.inner()
            .local_reserve_grants
            .remove(&grant_id)
            .map(|(_, grant)| grant)
    }

    pub fn install_prepared_put_key_reservation(
        &self,
        reservation_id: u64,
        info: PreparedPutKeyReservationInfo,
    ) {
        if self
            .inner()
            .prepared_put_key_reservations
            .insert(reservation_id, info)
            .is_some()
        {
            panic!(
                "duplicate prepared put key reservation id indicates a logic bug: reservation_id={}",
                reservation_id
            );
        }
    }

    pub fn take_prepared_put_key_reservation(
        &self,
        reservation_id: u64,
    ) -> Option<PreparedPutKeyReservationInfo> {
        self.inner()
            .prepared_put_key_reservations
            .remove(&reservation_id)
            .map(|(_, info)| info)
    }

    pub fn runtime_observe_snapshot(&self) -> MasterRuntimeObserveSnapshot {
        let mut get_holding_bytes = 0u64;
        for entry in self.inner().get_holding.inner().iter() {
            get_holding_bytes = get_holding_bytes.saturating_add(entry.value().len);
        }

        let mut replica_cache_nodes = Vec::new();
        for entry in self.inner().node_kv_cache_controller.iter() {
            let owner_node = entry.key().clone();
            let cache = entry.value().clone();
            let node_space_size = self
                .inner()
                .view()
                .master_seg_manager()
                .get_node_space_size(owner_node.as_str());
            let base_capacity_bytes = self.replica_cache_base_capacity(node_space_size);
            let reserved_capacity_bytes = self
                .inner()
                .node_cache_reserved_capacity
                .get(owner_node.as_str())
                .map(|reserved| reserved.total_reserved_bytes())
                .unwrap_or(0);
            let effective_capacity_bytes =
                self.replica_cache_effective_capacity(owner_node.as_str(), node_space_size);
            let pending_eviction_reclaim_bytes =
                self.eviction_reclaim_pending_weight(owner_node.as_str());
            let reclaim_counters = self.eviction_reclaim_counters(owner_node.as_str());
            let tier1_cache = self
                .inner()
                .node_writeback_tier1_controller
                .get(owner_node.as_str())
                .map(|entry| entry.value().clone());
            replica_cache_nodes.push(ReplicaCacheNodeObserveSnapshot {
                owner_node: owner_node.clone(),
                entries: cache.entry_count(),
                weighted_bytes: cache.weighted_size(),
                effective_capacity_bytes,
                reserved_capacity_bytes,
                base_capacity_bytes,
                pending_eviction_reclaim_bytes,
                writeback_tier1_entries: tier1_cache
                    .as_ref()
                    .map(|cache| cache.entry_count())
                    .unwrap_or(0),
                writeback_tier1_weighted_bytes: tier1_cache
                    .as_ref()
                    .map(|cache| cache.weighted_size())
                    .unwrap_or(0),
                writeback_tier1_capacity_bytes: tier1_cache
                    .as_ref()
                    .and_then(|cache| cache.policy().max_capacity())
                    .unwrap_or(0),
                writeback_tier1_triggered: Self::tier1_writeback_counter(
                    &self.inner().tier1_writeback_trigger_counts,
                    owner_node.as_str(),
                ),
                writeback_tier1_owner_accepted: Self::tier1_writeback_counter(
                    &self.inner().tier1_writeback_owner_accepted_counts,
                    owner_node.as_str(),
                ),
                writeback_tier1_failed: Self::tier1_writeback_counter(
                    &self.inner().tier1_writeback_failed_counts,
                    owner_node.as_str(),
                ),
                reclaim_master_activity_deferred: reclaim_counters
                    .master_activity_deferred
                    .load(Ordering::Relaxed),
                reclaim_master_get_holder_observed: reclaim_counters
                    .master_get_holder_observed
                    .load(Ordering::Relaxed),
                reclaim_owner_holder_deferred: reclaim_counters
                    .owner_holder_deferred
                    .load(Ordering::Relaxed),
                reclaim_owner_other_deferred: reclaim_counters
                    .owner_other_deferred
                    .load(Ordering::Relaxed),
                reclaim_route_changed: reclaim_counters.route_changed.load(Ordering::Relaxed),
                reclaim_retry_queued: reclaim_counters.retry_queued.load(Ordering::Relaxed),
                reclaim_retry_completed: reclaim_counters.retry_completed.load(Ordering::Relaxed),
                reclaim_retry_restored: reclaim_counters.retry_restored.load(Ordering::Relaxed),
                reclaim_completed: reclaim_counters.completed.load(Ordering::Relaxed),
                owner_hot_demotion_attempts: reclaim_counters
                    .owner_hot_demotion_attempts
                    .load(Ordering::Relaxed),
                owner_hot_demotion_cohorts: reclaim_counters
                    .owner_hot_demotion_cohorts
                    .load(Ordering::Relaxed),
                owner_hot_demotion_selected_bytes: reclaim_counters
                    .owner_hot_demotion_selected_bytes
                    .load(Ordering::Relaxed),
                owner_hot_demotion_precheck_rejected: reclaim_counters
                    .owner_hot_demotion_precheck_rejected
                    .load(Ordering::Relaxed),
                owner_hot_demotion_partial_mismatch: reclaim_counters
                    .owner_hot_demotion_partial_mismatch
                    .load(Ordering::Relaxed),
                recoverable_first_requested_bytes: reclaim_counters
                    .recoverable_first_requested_bytes
                    .load(Ordering::Relaxed),
                recoverable_first_selected_bytes: reclaim_counters
                    .recoverable_first_selected_bytes
                    .load(Ordering::Relaxed),
                recoverable_first_shortfall_bytes: reclaim_counters
                    .recoverable_first_shortfall_bytes
                    .load(Ordering::Relaxed),
                recoverable_first_eligible_checks: reclaim_counters
                    .recoverable_first_eligible_checks
                    .load(Ordering::Relaxed),
                recoverable_first_route_absent_checks: reclaim_counters
                    .recoverable_first_route_absent_checks
                    .load(Ordering::Relaxed),
                recoverable_first_version_changed_checks: reclaim_counters
                    .recoverable_first_version_changed_checks
                    .load(Ordering::Relaxed),
                recoverable_first_route_ineligible_checks: reclaim_counters
                    .recoverable_first_route_ineligible_checks
                    .load(Ordering::Relaxed),
                recoverable_first_cpu_route_absent_checks: reclaim_counters
                    .recoverable_first_cpu_route_absent_checks
                    .load(Ordering::Relaxed),
                recoverable_first_atomic_group_incomplete_checks: reclaim_counters
                    .recoverable_first_atomic_group_incomplete_checks
                    .load(Ordering::Relaxed),
                recoverable_first_tp_cohort_incomplete_checks: reclaim_counters
                    .recoverable_first_tp_cohort_incomplete_checks
                    .load(Ordering::Relaxed),
            });
        }

        MasterRuntimeObserveSnapshot {
            get_holding_entries: self.inner().get_holding.total() as u64,
            get_holding_bytes,
            replica_cache_nodes,
        }
    }

    fn spawn_runtime_observe_reporter(&self) {
        let view = self.0.view().clone();
        let view_task = view.clone();
        view.spawn("master_runtime_observe_reporter", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            loop {
                tokio::select! {
                    _ = shutdown_waiter.wait() => break,
                    _ = interval.tick() => {
                        let snapshot = view_task.master_kv_router().runtime_observe_snapshot();
                        let metrics = view_task.metric_reporter().metrics();
                        metrics.set_kv_holding_entries(
                            "master_get_holding",
                            snapshot.get_holding_entries,
                        );
                        metrics.set_kv_holding_bytes(
                            "master_get_holding",
                            snapshot.get_holding_bytes,
                        );
                        tracing::info!(
                            "master get holding runtime: entries={} bytes={}",
                            snapshot.get_holding_entries,
                            snapshot.get_holding_bytes
                        );
                        for node in snapshot.replica_cache_nodes {
                            tracing::info!(
                                "replica cache runtime: owner={} entries={} weighted_bytes={} effective_capacity_bytes={} base_capacity_bytes={} reserved_capacity_bytes={} pending_eviction_reclaim_bytes={} writeback_tier1_entries={} writeback_tier1_weighted_bytes={} writeback_tier1_capacity_bytes={} writeback_tier1_triggered={} writeback_tier1_owner_accepted={} writeback_tier1_failed={} reclaim_master_activity_deferred={} reclaim_master_get_holder_observed={} reclaim_owner_holder_deferred={} reclaim_owner_other_deferred={} reclaim_route_changed={} reclaim_retry_queued={} reclaim_retry_completed={} reclaim_retry_restored={} reclaim_completed={} owner_hot_demotion_attempts={} owner_hot_demotion_cohorts={} owner_hot_demotion_selected_bytes={} owner_hot_demotion_precheck_rejected={} owner_hot_demotion_partial_mismatch={} recoverable_first_requested_bytes={} recoverable_first_selected_bytes={} recoverable_first_shortfall_bytes={} recoverable_first_eligible_checks={} recoverable_first_route_absent_checks={} recoverable_first_version_changed_checks={} recoverable_first_route_ineligible_checks={} recoverable_first_cpu_route_absent_checks={} recoverable_first_atomic_group_incomplete_checks={} recoverable_first_tp_cohort_incomplete_checks={}",
                                node.owner_node,
                                node.entries,
                                node.weighted_bytes,
                                node.effective_capacity_bytes,
                                node.base_capacity_bytes,
                                node.reserved_capacity_bytes,
                                node.pending_eviction_reclaim_bytes,
                                node.writeback_tier1_entries,
                                node.writeback_tier1_weighted_bytes,
                                node.writeback_tier1_capacity_bytes,
                                node.writeback_tier1_triggered,
                                node.writeback_tier1_owner_accepted,
                                node.writeback_tier1_failed,
                                node.reclaim_master_activity_deferred,
                                node.reclaim_master_get_holder_observed,
                                node.reclaim_owner_holder_deferred,
                                node.reclaim_owner_other_deferred,
                                node.reclaim_route_changed,
                                node.reclaim_retry_queued,
                                node.reclaim_retry_completed,
                                node.reclaim_retry_restored,
                                node.reclaim_completed,
                                node.owner_hot_demotion_attempts,
                                node.owner_hot_demotion_cohorts,
                                node.owner_hot_demotion_selected_bytes,
                                node.owner_hot_demotion_precheck_rejected,
                                node.owner_hot_demotion_partial_mismatch,
                                node.recoverable_first_requested_bytes,
                                node.recoverable_first_selected_bytes,
                                node.recoverable_first_shortfall_bytes,
                                node.recoverable_first_eligible_checks,
                                node.recoverable_first_route_absent_checks,
                                node.recoverable_first_version_changed_checks,
                                node.recoverable_first_route_ineligible_checks,
                                node.recoverable_first_cpu_route_absent_checks,
                                node.recoverable_first_atomic_group_incomplete_checks,
                                node.recoverable_first_tp_cohort_incomplete_checks
                            );
                            metrics.set_kv_replica_cache_entries(
                                node.owner_node.as_str(),
                                node.entries,
                            );
                            metrics.set_kv_replica_cache_weighted_bytes(
                                node.owner_node.as_str(),
                                node.weighted_bytes,
                            );
                            metrics.set_kv_replica_cache_capacity_bytes(
                                node.owner_node.as_str(),
                                "effective",
                                node.effective_capacity_bytes,
                            );
                            metrics.set_kv_replica_cache_capacity_bytes(
                                node.owner_node.as_str(),
                                "reserved",
                                node.reserved_capacity_bytes,
                            );
                            metrics.set_kv_replica_cache_capacity_bytes(
                                node.owner_node.as_str(),
                                "base",
                                node.base_capacity_bytes,
                            );
                            metrics.set_kv_replica_cache_capacity_bytes(
                                node.owner_node.as_str(),
                                "pending_eviction_reclaim",
                                node.pending_eviction_reclaim_bytes,
                            );
                            metrics.set_kv_replica_cache_capacity_bytes(
                                node.owner_node.as_str(),
                                "writeback_tier1",
                                node.writeback_tier1_capacity_bytes,
                            );
                        }
                    }
                }
            }
        });
    }

    // Note: no additional getters for reserved bytes; policy currently relies only on adjust calls.
}
// moved to crate::metrics::client

#[cfg(test)]
mod placement_metrics_tests {
    use super::*;

    #[test]
    fn requester_target_pair_formats_stably() {
        let pair = RequesterTargetPair::new("node-2a", "node-3b");
        assert_eq!(pair.as_log_key(), "node-2a->node-3b");
    }

    #[test]
    fn placement_mode_label_is_stable() {
        let local = match PutPlacementMode::Local {
            PutPlacementMode::Local => "local",
            PutPlacementMode::Remote => "remote",
        };
        let remote = match PutPlacementMode::Remote {
            PutPlacementMode::Local => "local",
            PutPlacementMode::Remote => "remote",
        };
        assert_eq!(local, "local");
        assert_eq!(remote, "remote");
    }

    #[test]
    fn placement_counters_accumulate_by_target_mode_and_requester_target() {
        let target_counts: DashMap<NodeIDString, Arc<AtomicU64>> = DashMap::new();
        let requester_target_counts: DashMap<RequesterTargetPair, Arc<AtomicU64>> = DashMap::new();
        let mode_counts: DashMap<&'static str, Arc<AtomicU64>> = DashMap::new();

        let bump =
            |requester_node_id: &str, target_node_id: &str, placement_mode: PutPlacementMode| {
                let target_counter = target_counts
                    .entry(target_node_id.to_string())
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                    .value()
                    .clone();
                target_counter.fetch_add(1, Ordering::Relaxed);

                let requester_target_counter = requester_target_counts
                    .entry(RequesterTargetPair::new(requester_node_id, target_node_id))
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                    .value()
                    .clone();
                requester_target_counter.fetch_add(1, Ordering::Relaxed);

                let mode_key = match placement_mode {
                    PutPlacementMode::Local => "local",
                    PutPlacementMode::Remote => "remote",
                };
                let mode_counter = mode_counts
                    .entry(mode_key)
                    .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                    .value()
                    .clone();
                mode_counter.fetch_add(1, Ordering::Relaxed);
            };

        bump("node-2a", "node-2a", PutPlacementMode::Local);
        bump("node-3a", "node-2a", PutPlacementMode::Remote);
        bump("node-3a", "node-4a", PutPlacementMode::Remote);

        assert_eq!(
            target_counts
                .get("node-2a")
                .expect("node-2a target counter must exist")
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            target_counts
                .get("node-4a")
                .expect("node-4a target counter must exist")
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            mode_counts
                .get("local")
                .expect("local mode counter must exist")
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            mode_counts
                .get("remote")
                .expect("remote mode counter must exist")
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            requester_target_counts
                .get(&RequesterTargetPair::new("node-3a", "node-2a"))
                .expect("requester-target counter must exist")
                .load(Ordering::Relaxed),
            1
        );
    }
}
