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
        BatchEvictOwnerSourceReq, BatchGetDoneReq, BatchGetRevokeReq, BatchGetStartReq,
        BatchIsExistReq, BatchOwnerReclaimReq, BatchPreparePutKeysReq, BatchPutAppendDoneReq,
        BatchPutAppendStartReq, BatchPutDoneReq, BatchPutRevokeReq, BatchPutStartReq,
        BatchReleasePutKeyReservationsReq, CountPrefixReq, CountPrefixResp, DeleteAckReq,
        DeleteReq, GetAllocationMode, GetDoneReq, GetDoneResp, GetMetaReq, GetRevokeReq,
        GetStartReq, GroupedBatchPutDoneReq, OwnerReclaimItem, PutAppendDoneReq,
        PutAppendRevokeReq, PutAppendStartReq, PutAtomicGroup, PutDoneReq, PutRevokeReq,
        PutStartReq, ReleaseLocalGrantReq, ReserveLocalGrantReq,
    },
    placement::{PlacementPolicy, build_placement_policy},
    put::{
        handle_batch_prepare_put_keys, handle_batch_put_append_done, handle_batch_put_append_start,
        handle_batch_put_done, handle_batch_put_revoke, handle_batch_put_start,
        handle_batch_release_put_key_reservations, handle_grouped_batch_put_done,
        handle_put_append_done, handle_put_append_revoke, handle_put_append_start, handle_put_done,
        handle_put_revoke, handle_put_start, handle_release_local_grant,
        handle_reserve_local_grant,
    },
    reclaim::handle_batch_evict_owner_source,
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
use crate::memholder::{EnsureMemholderMgmtDeleteHandle, MasterOwnerMemMgr, MemholderManagerTrait};
use crate::metric_reporter::{MetricReporter, MetricReporterAccessTrait};
use crate::p2p::msg_pack::{MsgPack, RPCCaller, RPCHandler};
use crate::p2p::p2p_module::{P2pModule, P2pModuleAccessTrait};
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, OK};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_util::map_lock::AMapLock;
use fluxon_util::pin_aware_moka::{PinAwareMoka, PinGuard};

use async_trait::async_trait;
use chrono::Utc;
use dashmap::{DashMap, DashSet};
use limit_thirdparty::tokio::sync::ARwLock;
use limit_thirdparty::tokio::{self, sync::ampsc};
use moka::notification::RemovalCause;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

const MAX_GET_DURABLE_REPLICA_SLOTS: u32 = 2;
const PLACEMENT_REPORT_INTERVAL_SECS: u64 = 10;
const INFLIGHT_PUT_TTL_SECONDS: u64 = 60;
const INFLIGHT_PUT_TTL_SECONDS_SKIP_PUT_END_COMMIT: u64 = 5;
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

fn try_install_eviction_reclaim_identities(
    inflight: &DashSet<reclaim::EvictionReclaimIdentity>,
    identities: Vec<reclaim::EvictionReclaimIdentity>,
) -> bool {
    let mut installed = Vec::with_capacity(identities.len());
    for identity in identities {
        if inflight.insert(identity.clone()) {
            installed.push(identity);
        } else {
            for identity in installed {
                assert!(inflight.remove(&identity).is_some());
            }
            return false;
        }
    }
    true
}

fn classify_existing_eviction_reclaim(
    inflight: &DashSet<reclaim::EvictionReclaimIdentity>,
    identities: &[reclaim::EvictionReclaimIdentity],
) -> reclaim::EnqueueEvictionReclaimResult {
    let inflight_count = identities
        .iter()
        .filter(|identity| inflight.contains(identity))
        .count();
    if inflight_count == identities.len() {
        reclaim::EnqueueEvictionReclaimResult::AlreadyInProgress
    } else if inflight_count == 0 {
        reclaim::EnqueueEvictionReclaimResult::NotInProgress
    } else {
        reclaim::EnqueueEvictionReclaimResult::PartialOverlap
    }
}

#[derive(Clone, Copy, Debug)]
pub enum PutPlacementMode {
    Local,
    Remote,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReservedCapacityReason {
    LocalReserveGrant,
    OwnerIndexedAllocation,
    LeaseBoundKv,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NodeCacheCapacityBoundaries {
    ring_b_bytes: u64,
    tier1_bytes: Option<u64>,
}

fn node_cache_capacity_boundaries(
    node_space_size: u64,
    replica_cache_capacity_ratio: f64,
    replica_writeback_tier1_capacity_ratio: Option<f64>,
    reserved_capacity_bytes: u64,
) -> NodeCacheCapacityBoundaries {
    let ring_b_base = (node_space_size as f64 * replica_cache_capacity_ratio).floor() as u64;
    NodeCacheCapacityBoundaries {
        ring_b_bytes: ring_b_base.saturating_sub(reserved_capacity_bytes),
        // Tier1 contains only metadata and defines when pre-writeback starts.
        // It is inclusive of owner residency, so physical ring-B reservations
        // never reduce this logical policy window.
        tier1_bytes: replica_writeback_tier1_capacity_ratio
            .map(|ratio| (node_space_size as f64 * ratio).floor() as u64),
    }
}

#[derive(Debug)]
pub struct NodeCacheReservedCapacity {
    /// Exact node registration generation owning these counters.
    generation: NodeTombTag,
    pub total_bytes: AtomicU64,
    pub local_reserve_grant_bytes: AtomicU64,
    pub owner_indexed_allocation_bytes: AtomicU64,
    pub lease_bound_kv_bytes: AtomicU64,
}

impl NodeCacheReservedCapacity {
    fn new(generation: NodeTombTag) -> Self {
        Self {
            generation,
            total_bytes: AtomicU64::new(0),
            local_reserve_grant_bytes: AtomicU64::new(0),
            owner_indexed_allocation_bytes: AtomicU64::new(0),
            lease_bound_kv_bytes: AtomicU64::new(0),
        }
    }

    fn apply_delta(&self, reason: ReservedCapacityReason, delta_bytes: i64) {
        if delta_bytes >= 0 {
            let delta = delta_bytes as u64;
            self.total_bytes.fetch_add(delta, Ordering::Relaxed);
            match reason {
                ReservedCapacityReason::LocalReserveGrant => {
                    self.local_reserve_grant_bytes
                        .fetch_add(delta, Ordering::Relaxed);
                }
                ReservedCapacityReason::OwnerIndexedAllocation => {
                    self.owner_indexed_allocation_bytes
                        .fetch_add(delta, Ordering::Relaxed);
                }
                ReservedCapacityReason::LeaseBoundKv => {
                    self.lease_bound_kv_bytes
                        .fetch_add(delta, Ordering::Relaxed);
                }
            }
        } else {
            let delta = (-delta_bytes) as u64;
            self.total_bytes.fetch_sub(delta, Ordering::Relaxed);
            match reason {
                ReservedCapacityReason::LocalReserveGrant => {
                    self.local_reserve_grant_bytes
                        .fetch_sub(delta, Ordering::Relaxed);
                }
                ReservedCapacityReason::OwnerIndexedAllocation => {
                    self.owner_indexed_allocation_bytes
                        .fetch_sub(delta, Ordering::Relaxed);
                }
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

/// Version-scoped reservation that reduces the usable resident-cache capacity
/// while a lease-bound route is alive.  The token owns the exact counter Arc;
/// dropping an old Allocation/route can therefore never decrement a newly
/// reconnected node's counters merely because it reused the same node id.
pub struct NodeCacheCapacityReservation {
    view: MasterKvRouterView,
    node_id: NodeIDString,
    generation: NodeTombTag,
    reserved_capacity: Arc<NodeCacheReservedCapacity>,
    reason: ReservedCapacityReason,
    bytes: u64,
    released: AtomicBool,
}

impl std::fmt::Debug for NodeCacheCapacityReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeCacheCapacityReservation")
            .field("node_id", &self.node_id)
            .field("bytes", &self.bytes)
            .field("released", &self.released.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for NodeCacheCapacityReservation {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(_view_guard) = self.view.try_upgrade() else {
            return;
        };
        if let Err(err) = self
            .view
            .master_kv_router()
            .adjust_node_cache_reserved_capacity_identity(
                &self.node_id,
                &self.generation,
                &self.reserved_capacity,
                self.reason,
                -(self.bytes as i64),
            )
        {
            warn!(
                "failed to release generation-scoped cache reservation: node={} bytes={} err={}",
                self.node_id, self.bytes, err
            );
        }
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
    pub reclaim_owner_holder_deferred: u64,
    pub reclaim_owner_other_deferred: u64,
    pub reclaim_route_changed: u64,
    pub reclaim_retry_queued: u64,
    pub reclaim_retry_completed: u64,
    pub reclaim_retry_restored: u64,
    pub reclaim_completed: u64,
    pub source_evict_rpc_requests: u64,
    pub source_evict_victims: u64,
    pub source_evict_requested_bytes: u64,
    pub source_evict_accepted: u64,
    pub source_evict_in_progress: u64,
    pub source_evict_completed: u64,
    pub source_evict_retryable_busy: u64,
    pub source_evict_stale: u64,
    pub source_evict_rejected: u64,
    pub last_route_removed_members: u64,
    pub last_route_removed_bytes: u64,
    pub capacity_eviction_non_ring_b_entry_total: u64,
    pub capacity_eviction_hit_committed_slot: u64,
    pub eviction_reclaim_deduplicated: u64,
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
    /// Exact target registration generation captured while the allocation was current.
    pub target_tomb_tag: NodeTombTag,
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
    /// Distinguishes repeated remote-copy attempts for the same `(key,
    /// put_id)` generation.  A generation may lose its remote route and need
    /// another append while the previous attempt's replayable terminal result
    /// is still cached.
    pub operation_id: u64,
    pub node_id: NodeID,
    /// Exact target registration generation that owns `target_allocation`.
    pub target_tomb_tag: NodeTombTag,
    pub source_node_id: NodeID,
    pub key: String,
    pub put_id: PutIDForAKey,
    pub target_allocation: Arc<Mutex<Option<Allocation>>>,
    /// Protect the source-owner copy only after this inclusive replica has been
    /// published successfully.  Backend-admitted replicas set this bit; tiered
    /// write-back and owner-hot exclusive demotion deliberately do not.
    pub protect_source_on_remote_complete: bool,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
}

#[derive(Clone, Debug)]
pub struct CompletedReplicaTaskInfo {
    pub appended: bool,
}

/// Information about a `get` operation that is currently in progress.
#[derive(Clone)]
pub struct InflightGetInfo {
    pub put_id: PutIDForAKey,
    pub src_node_id: NodeID,
    pub key: String,
    pub req_node_id: NodeID,
    pub len: u64,
    pub target: InflightGetTarget,
    /// Exact requester registration generation that owns `target`.
    pub target_tomb_tag: NodeTombTag,
    pub route: Arc<OneKvNodesRoutes>,
    pub allocation_mode: GetAllocationMode,
    pub durable_reservation: Option<Arc<GetDurableSlotReservation>>,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
    /// Requester-scoped guard for prepared local-reserve Gets.  Different GPU
    /// owners may fetch the same key concurrently, but one owner must never
    /// materialize two candidate committed slots for the same key.
    pub(crate) _prepared_requester_lease: Option<Arc<PreparedGetRequesterLease>>,
}

#[derive(Clone)]
pub struct CompletedGetInfo {
    pub req_node_id: NodeID,
    pub response: GetDoneResp,
}

#[derive(Clone, Debug)]
pub enum InflightGetTarget {
    Allocation(Arc<Allocation>),
    PreparedLocalReserveSlot(CommittedSlotReplica),
}

impl InflightGetTarget {
    pub fn abs_addr(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.base_addr() + allocation.addr(),
            Self::PreparedLocalReserveSlot(slot) => slot.addr,
        }
    }

    pub fn base_addr(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.base_addr(),
            Self::PreparedLocalReserveSlot(slot) => slot.base_addr,
        }
    }

    pub fn capacity(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.capcity(),
            Self::PreparedLocalReserveSlot(slot) => slot.slot_size,
        }
    }
}

impl InflightGetInfo {
    pub fn release_durable_slot_if_needed(&self) {
        // Durable-slot capacity is returned by the reservation token's Drop.
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
    /// Exact owner registration generation that owns the grant allocation.
    pub tomb_tag: NodeTombTag,
    pub allocation: Allocation,
    /// Excludes this whole grant from the master unindexed-Allocation domain.
    pub capacity_reservation: Option<Arc<NodeCacheCapacityReservation>>,
}

pub struct PreparedPutKeyReservationInfo {
    pub owner_node_id: NodeID,
    pub key: String,
    pub(crate) _activity_lease: Arc<MasterKeyActivityLease>,
}

#[derive(Default)]
pub(crate) struct EvictionReclaimCounters {
    pub master_activity_deferred: AtomicU64,
    pub owner_holder_deferred: AtomicU64,
    pub owner_other_deferred: AtomicU64,
    pub route_changed: AtomicU64,
    pub retry_queued: AtomicU64,
    pub retry_completed: AtomicU64,
    pub retry_restored: AtomicU64,
    pub completed: AtomicU64,
    pub source_evict_rpc_requests: AtomicU64,
    pub source_evict_victims: AtomicU64,
    pub source_evict_requested_bytes: AtomicU64,
    pub source_evict_accepted: AtomicU64,
    pub source_evict_in_progress: AtomicU64,
    pub source_evict_completed: AtomicU64,
    pub source_evict_retryable_busy: AtomicU64,
    pub source_evict_stale: AtomicU64,
    pub source_evict_rejected: AtomicU64,
    /// Exact reclaim commits that removed the final readable route for a key.
    pub last_route_removed_members: AtomicU64,
    /// Physical backing bytes represented by `last_route_removed_members`.
    pub last_route_removed_bytes: AtomicU64,
    /// A Size event from the ring-B controller resolved to any current route
    /// outside `Allocation && !owner_local_indexed`.
    pub capacity_eviction_non_ring_b_entry_total: AtomicU64,
    /// More specific subset retained for diagnostics/backward-compatible
    /// metrics: the non-ring-B route used a CommittedSlot backing.
    pub capacity_eviction_hit_committed_slot: AtomicU64,
    /// Duplicate listener/victim events suppressed while the same physical
    /// cache version already has an outstanding reclaim lifecycle.
    pub eviction_reclaim_deduplicated: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MasterKeyActivitySnapshot {
    pub puts: u32,
    pub gets: u32,
    pub replicas: u32,
    pub reclaim_installed: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MasterKeyActivityObserveSnapshot {
    active_keys: u64,
    put_keys: u64,
    get_keys: u64,
    replica_keys: u64,
    reclaim_keys: u64,
    inflight_puts: u64,
    inflight_gets: u64,
    inflight_replicas: u64,
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
    cache_pins: Mutex<Vec<PinGuard>>,
    released: AtomicBool,
}

impl MasterKeyActivityLease {
    pub(crate) fn attach_cache_pin(&self, pin: PinGuard) {
        let mut pins = self.cache_pins.lock();
        if !self.released.load(Ordering::Acquire) {
            pins.push(pin);
        }
    }

    pub(crate) fn release_now(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.cache_pins.lock().clear();
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PreparedGetRequesterKey {
    key: String,
    requester: NodeID,
}

#[derive(Default)]
pub(crate) struct PreparedGetRequesterTable {
    active: Mutex<HashMap<PreparedGetRequesterKey, u64>>,
}

pub(crate) struct PreparedGetRequesterLease {
    table: Arc<PreparedGetRequesterTable>,
    identity: PreparedGetRequesterKey,
    get_id: u64,
    released: AtomicBool,
}

impl PreparedGetRequesterTable {
    fn reserve(
        self: &Arc<Self>,
        key: &str,
        requester: &NodeID,
        get_id: u64,
    ) -> Option<Arc<PreparedGetRequesterLease>> {
        let identity = PreparedGetRequesterKey {
            key: key.to_string(),
            requester: requester.clone(),
        };
        let mut active = self.active.lock();
        if active.contains_key(&identity) {
            return None;
        }
        active.insert(identity.clone(), get_id);
        Some(Arc::new(PreparedGetRequesterLease {
            table: self.clone(),
            identity,
            get_id,
            released: AtomicBool::new(false),
        }))
    }

    #[cfg(test)]
    fn active_get_id(&self, key: &str, requester: &NodeID) -> Option<u64> {
        self.active
            .lock()
            .get(&PreparedGetRequesterKey {
                key: key.to_string(),
                requester: requester.clone(),
            })
            .copied()
    }
}

impl PreparedGetRequesterLease {
    pub(crate) fn release_now(&self) {
        if self
            .released
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let mut active = self.table.active.lock();
        if active.get(&self.identity) == Some(&self.get_id) {
            active.remove(&self.identity);
        }
    }
}

impl Drop for PreparedGetRequesterLease {
    fn drop(&mut self) {
        self.release_now();
    }
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
            cache_pins: Mutex::new(Vec::new()),
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

    fn observe_snapshot(&self) -> MasterKeyActivityObserveSnapshot {
        let states = self.states.lock();
        let mut snapshot = MasterKeyActivityObserveSnapshot {
            active_keys: u64::try_from(states.len()).unwrap_or(u64::MAX),
            ..Default::default()
        };
        for state in states.values() {
            snapshot.put_keys += u64::from(state.puts != 0);
            snapshot.get_keys += u64::from(state.gets != 0);
            snapshot.replica_keys += u64::from(state.replicas != 0);
            snapshot.reclaim_keys += u64::from(state.reclaim.is_some());
            snapshot.inflight_puts = snapshot.inflight_puts.saturating_add(u64::from(state.puts));
            snapshot.inflight_gets = snapshot.inflight_gets.saturating_add(u64::from(state.gets));
            snapshot.inflight_replicas = snapshot
                .inflight_replicas
                .saturating_add(u64::from(state.replicas));
        }
        snapshot
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

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct MasterPinAlias {
    key: String,
    put_time_ms: u64,
    put_version: u32,
}

impl MasterPinAlias {
    fn new(key: &str, put_id: PutIDForAKey) -> Self {
        Self {
            key: key.to_string(),
            put_time_ms: put_id.0,
            put_version: put_id.1,
        }
    }
}

pub type MasterNodeCache = PinAwareMoka<String, MasterPinAlias, NodeValueReplicaDesc>;

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

    /// Bytes owned by the physical backing.  Capacity accounting must use
    /// this value rather than the logical payload length: an Allocation owns
    /// its allocator capacity and a committed local-reserve slot owns the
    /// whole slot.
    pub fn capacity_bytes(&self) -> u64 {
        match self {
            Self::Allocation(allocation) => allocation.capcity(),
            Self::CommittedSlot(slot) => slot.slot_size,
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
    /// Present only for an allocation replica created by GetDone. The shared
    /// token returns the per-key durable-replica budget when this route entry's
    /// final clone is dropped.
    pub get_durable_reservation: Option<Arc<GetDurableSlotReservation>>,
    /// Excludes a non-ring-B backing from the unindexed-Allocation budget and
    /// is released with the exact route lifetime.
    pub capacity_reservation: Option<Arc<NodeCacheCapacityReservation>>,
    pub tomb_tag: NodeTombTag,
}

pub struct GetDurableSlotReservation {
    route: Weak<OneKvNodesRoutes>,
    released: AtomicBool,
}

impl std::fmt::Debug for GetDurableSlotReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetDurableSlotReservation")
            .field("released", &self.released.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for GetDurableSlotReservation {
    fn drop(&mut self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(route) = self.route.upgrade() {
            route.release_get_durable_slot();
        }
    }
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

    fn try_reserve_get_durable_slot(self: &Arc<Self>) -> Option<Arc<GetDurableSlotReservation>> {
        self.get_durable_slots_used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current < MAX_GET_DURABLE_REPLICA_SLOTS {
                    Some(current + 1)
                } else {
                    None
                }
            })
            .ok()
            .map(|_| {
                Arc::new(GetDurableSlotReservation {
                    route: Arc::downgrade(self),
                    released: AtomicBool::new(false),
                })
            })
    }

    fn release_get_durable_slot(&self) {
        self.get_durable_slots_used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(1)
            })
            .unwrap_or_else(|_| panic!("get durable slot underflow indicates a logic bug"));
    }
}

/// Ring B contains exactly master-only, unindexed Allocation routes.  Node
/// role is deliberately absent from this predicate: placement and capacity
/// ownership are orthogonal.
fn ring_b_route_replica_desc(
    route: &OneKvNodesRoutes,
    node_id: &str,
) -> Option<NodeValueReplicaDesc> {
    if route.lease_id.is_some() {
        return None;
    }
    let replicas = route.nodes_replicas.read();
    let replica = replicas.get(node_id)?;
    if replica.tomb_tag.is_tomb()
        || replica.owner_local_indexed
        || !matches!(&replica.backing, KvReplicaBacking::Allocation(_))
    {
        return None;
    }
    Some(NodeValueReplicaDesc {
        weight_bytes: u32::try_from(replica.backing.capacity_bytes()).unwrap_or(u32::MAX),
        put_id: route.put_id,
    })
}

/// True only while `tag` is the live registration generation currently bound to `node_id`.
/// Completion paths must validate their captured tag instead of borrowing the tag of a later
/// registration that happens to reuse the same node id.
pub(crate) fn node_generation_is_current_live(
    view: &MasterKvRouterView,
    node_id: &NodeID,
    tag: &NodeTombTag,
) -> bool {
    !tag.is_tomb()
        && view
            .master_seg_manager()
            .get_node_tomb_tag(node_id)
            .is_some_and(|current| !current.is_tomb() && current.same_generation(tag))
}

/// Publish one replica under the route-local lock and close the MemberLeft snapshot race.
///
/// There are only two possible linearizations:
/// - publication wins the final tag check, so a later MemberLeft snapshot observes the route;
/// - MemberLeft marks the shared tag first, so this function rolls back the exact generation
///   before releasing the route lock.
///
/// A previous live generation is never overwritten on rollback.  In normal operation such a
/// generation cannot coexist with `replica` (registration replacement tombs the old tag first),
/// but preserving it makes the identity rule explicit and keeps tests resistant to ABA setup.
pub(crate) fn publish_route_replica_tomb_fenced(
    route: &OneKvNodesRoutes,
    node_id: NodeID,
    replica: KvRouteInfo,
) -> bool {
    let publish_tag = replica.tomb_tag.clone();
    if publish_tag.is_tomb() {
        return false;
    }

    let mut replicas = route.nodes_replicas.write();
    if publish_tag.is_tomb() {
        return false;
    }
    // A completion pre-check is necessarily racy with another completion for
    // the same requester.  Never replace a live replica here: only a tombed
    // generation may be superseded.
    if replicas
        .get(&node_id)
        .is_some_and(|current| !current.tomb_tag.is_tomb())
    {
        return false;
    }
    let previous = replicas.insert(node_id.clone(), replica);

    if publish_tag.is_tomb() {
        let published_is_current = replicas.get(&node_id).is_some_and(|current| {
            current.node_id == node_id && current.tomb_tag.same_generation(&publish_tag)
        });
        if published_is_current {
            replicas.remove(&node_id);
            if let Some(previous) = previous.filter(|old| !old.tomb_tag.is_tomb()) {
                replicas.insert(node_id, previous);
            }
        }
        return false;
    }
    true
}

/// Replace one key's primary route while closing the MemberLeft snapshot race.
///
/// The DashMap entry guard is held through the final tomb check.  Therefore a
/// MemberLeft cleanup that starts after a successful check must observe the new
/// route, while a MemberLeft that marks the generation first causes the exact
/// replacement to be rolled back.  Restoring `previous` also protects a newer
/// key version from being lost when a stale completion races node departure.
pub(crate) fn publish_primary_route_tomb_fenced(
    routes: &DashMap<String, Arc<OneKvNodesRoutes>>,
    key: &str,
    new_route: Arc<OneKvNodesRoutes>,
    publish_tag: &NodeTombTag,
) -> Result<Option<Arc<OneKvNodesRoutes>>, ()> {
    if publish_tag.is_tomb() {
        return Err(());
    }

    let mut inserted = false;
    let mut current = routes.entry(key.to_string()).or_insert_with(|| {
        inserted = true;
        new_route.clone()
    });
    let previous = if inserted {
        None
    } else {
        Some(std::mem::replace(&mut *current, new_route.clone()))
    };

    if publish_tag.is_tomb() {
        if let Some(previous) = previous {
            *current = previous;
        } else {
            // DashMap's vacant-entry guard cannot remove itself.  Drop the
            // shard guard first, then remove only our exact Arc.  The route is
            // tombed throughout this tiny window, so readers cannot use it.
            drop(current);
            routes.remove_if(key, |_, route| Arc::ptr_eq(route, &new_route));
        }
        return Err(());
    }

    Ok(previous)
}

fn member_left_can_forward_to_registration_actor(current_node_start_time: Option<i64>) -> bool {
    // MemberLeft carries only a node id. If membership already exposes a live generation,
    // forwarding the ambiguous old leave would cancel that generation's registration actor.
    current_node_start_time.is_none()
}

/// Remove one departed node generation from a single route.
///
/// The route map and replica map deliberately use their normal fine-grained locking only:
/// MemberLeft is a cold path and must not introduce a process-wide lock around a full-table scan.
/// The tomb-tag identity fences a delayed cleanup from deleting a live replica published by a
/// reconnected generation that reused the same node id.
fn remove_departed_generation_from_route(
    routes: &DashMap<String, Arc<OneKvNodesRoutes>>,
    key: &str,
    route: &Arc<OneKvNodesRoutes>,
    node_id: &str,
    departed_tag: &NodeTombTag,
) -> Option<PutIDForAKey> {
    let became_empty = {
        let mut replicas = route.nodes_replicas.write();
        let remove = replicas.get(node_id).is_some_and(|replica| {
            replica.node_id.as_ref() == node_id
                && replica.tomb_tag.is_tomb()
                && replica.tomb_tag.same_generation(departed_tag)
        });
        if !remove {
            return None;
        }
        replicas.remove(node_id);
        replicas.is_empty()
    };

    if !became_empty {
        return None;
    }

    routes
        .remove_if(key, |_, current| {
            // `Arc::ptr_eq` prevents an old cleanup from removing a replacement route (ABA).
            // Recheck emptiness under the per-route read lock because another replica may have
            // joined after the write lock above was released.
            Arc::ptr_eq(current, route) && current.nodes_replicas.read().is_empty()
        })
        .map(|(_, removed)| removed.put_id)
}

const MEMBER_LEFT_ROUTE_CLEANUP_BATCH: usize = 512;

async fn cleanup_departed_generation_routes(
    view: MasterKvRouterView,
    node_id: NodeIDString,
    departed_tag: NodeTombTag,
) {
    // Grants own master-side Allocation guards and therefore must be released
    // for the exact departed generation as well.  Collect ids without keeping
    // DashMap guards across a removal or yield.
    let departed_grant_ids: Vec<u64> = view
        .master_kv_router()
        .inner()
        .local_reserve_grants
        .iter()
        .filter_map(|entry| {
            (entry.value().owner_node_id.as_ref() == node_id.as_str()
                && entry.value().tomb_tag.is_tomb()
                && entry.value().tomb_tag.same_generation(&departed_tag))
            .then_some(*entry.key())
        })
        .collect();
    let mut removed_grants = 0usize;
    for grant_id in departed_grant_ids {
        if view
            .master_kv_router()
            .inner()
            .local_reserve_grants
            .remove_if(&grant_id, |_, grant| {
                grant.owner_node_id.as_ref() == node_id.as_str()
                    && grant.tomb_tag.is_tomb()
                    && grant.tomb_tag.same_generation(&departed_tag)
            })
            .is_some()
        {
            removed_grants = removed_grants.saturating_add(1);
        }
    }

    // Weak snapshots avoid pinning every route Allocation for the duration of a large scan.
    // No DashMap guard or replica lock is held across an await.
    let route_snapshot: Vec<(String, Weak<OneKvNodesRoutes>)> = view
        .master_kv_router()
        .inner()
        .kv_routes
        .iter()
        .map(|entry| (entry.key().clone(), Arc::downgrade(entry.value())))
        .collect();
    let mut removed_empty_routes = Vec::new();
    let mut removed_replicas = 0usize;

    for batch in route_snapshot.chunks(MEMBER_LEFT_ROUTE_CLEANUP_BATCH) {
        for (key, weak_route) in batch {
            let Some(route) = weak_route.upgrade() else {
                continue;
            };
            let had_departed_replica = route
                .nodes_replicas
                .read()
                .get(node_id.as_str())
                .is_some_and(|replica| {
                    replica.node_id.as_ref() == node_id.as_str()
                        && replica.tomb_tag.is_tomb()
                        && replica.tomb_tag.same_generation(&departed_tag)
                });
            if !had_departed_replica {
                continue;
            }
            if let Some(put_id) = remove_departed_generation_from_route(
                &view.master_kv_router().inner().kv_routes,
                key,
                &route,
                node_id.as_str(),
                &departed_tag,
            ) {
                removed_empty_routes.push((key.clone(), put_id));
            }
            removed_replicas = removed_replicas.saturating_add(1);
        }
        tokio::task::yield_now().await;
    }

    if view.master_kv_router().prefix_index_enabled() {
        // Prefix cleanup is batched so one MemberLeft does not spawn one task or acquire one
        // async write lock per key.
        for batch in removed_empty_routes.chunks(MEMBER_LEFT_ROUTE_CLEANUP_BATCH) {
            let mut tree = view.master_kv_router().inner().prefix_index.write().await;
            for (key, put_id) in batch {
                tree.remove(key, *put_id);
            }
            drop(tree);
            tokio::task::yield_now().await;
        }
    }

    info!(
        "MemberLeft route cleanup completed: node={} removed_grants={} removed_replicas={} removed_empty_routes={}",
        node_id,
        removed_grants,
        removed_replicas,
        removed_empty_routes.len()
    );
}

fn remove_exact_cache_entry(
    cache: &MasterNodeCache,
    key: &str,
    expected_desc: &NodeValueReplicaDesc,
) -> bool {
    cache
        .take_if(&key.to_string(), |entry| {
            entry.put_id == expected_desc.put_id && entry.weight_bytes == expected_desc.weight_bytes
        })
        .is_some()
}

fn insert_master_cache_entry(cache: &MasterNodeCache, key: String, desc: NodeValueReplicaDesc) {
    let alias = MasterPinAlias::new(&key, desc.put_id);
    cache.insert(key, [alias], desc);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_manager::ClusterMember;
    use crate::master_kv_router::msg_pack::{OwnerReclaimBacking, OwnerReclaimReason};
    use std::collections::HashMap;

    #[test]
    fn one_kv_nodes_routes_only_reserves_two_get_durable_slots() {
        let routes = Arc::new(OneKvNodesRoutes {
            put_id: (1, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::new()),
            get_durable_slots_used: AtomicU32::new(0),
        });

        let first = routes.try_reserve_get_durable_slot().unwrap();
        let second = routes.try_reserve_get_durable_slot().unwrap();
        assert!(routes.try_reserve_get_durable_slot().is_none());

        drop(first);
        assert!(routes.try_reserve_get_durable_slot().is_some());
        drop(second);
    }

    #[test]
    fn durable_slot_token_returns_capacity_across_ten_fill_demote_cycles() {
        let routes = Arc::new(OneKvNodesRoutes {
            put_id: (1, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::new()),
            get_durable_slots_used: AtomicU32::new(0),
        });

        for cycle in 0..10 {
            let reservation = routes
                .try_reserve_get_durable_slot()
                .expect("each fill must reacquire durable capacity after demotion");
            assert_eq!(routes.get_durable_slots_used.load(Ordering::Acquire), 1);
            let route_clone = reservation.clone();
            drop(reservation);
            assert_eq!(
                routes.get_durable_slots_used.load(Ordering::Acquire),
                1,
                "route clone must retain the token during cycle {cycle}"
            );
            drop(route_clone);
            assert_eq!(
                routes.get_durable_slots_used.load(Ordering::Acquire),
                0,
                "demotion must return durable capacity during cycle {cycle}"
            );
        }
    }

    #[test]
    fn prepared_get_singleflight_is_same_owner_only_and_aba_safe() {
        let table = Arc::new(PreparedGetRequesterTable::default());
        let gpu0: NodeID = "gpu-owner-0".to_string().into();
        let gpu1: NodeID = "gpu-owner-1".to_string().into();

        let gpu0_first = table
            .reserve("shared-key", &gpu0, 11)
            .expect("first prepared Get on gpu0 must lead");
        assert!(
            table.reserve("shared-key", &gpu0, 12).is_none(),
            "same owner cannot materialize a second candidate slot"
        );
        let gpu1_first = table
            .reserve("shared-key", &gpu1, 13)
            .expect("different requester owners remain parallel");
        assert_eq!(table.active_get_id("shared-key", &gpu0), Some(11));
        assert_eq!(table.active_get_id("shared-key", &gpu1), Some(13));

        gpu0_first.release_now();
        let gpu0_second = table
            .reserve("shared-key", &gpu0, 14)
            .expect("released owner identity can be reused");
        // A delayed Drop/release of the old generation must not delete get 14.
        gpu0_first.release_now();
        assert_eq!(table.active_get_id("shared-key", &gpu0), Some(14));

        drop(gpu0_second);
        drop(gpu1_first);
        assert_eq!(table.active_get_id("shared-key", &gpu0), None);
        assert_eq!(table.active_get_id("shared-key", &gpu1), None);
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
            reason: OwnerReclaimReason::OwnerCapacityEviction,
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
    fn master_key_activity_observe_snapshot_counts_keys_and_leases() {
        let table = Arc::new(MasterKeyActivityTable::default());
        let put = table
            .reserve("shared", MasterKeyActivityKind::Put, false)
            .expect("put activity must be admitted");
        let get_a = table
            .reserve("shared", MasterKeyActivityKind::Get, false)
            .expect("first get activity must be admitted");
        let get_b = table
            .reserve("shared", MasterKeyActivityKind::Get, false)
            .expect("second get activity must be admitted");
        let replica = table
            .reserve("replica", MasterKeyActivityKind::Replica, false)
            .expect("replica activity must be admitted");
        let reclaim = OwnerReclaimItem {
            key: "reclaim".to_string(),
            put_id: (9, 1),
            epoch: 17,
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id: 19,
                slot_index: 23,
                slot_size: 8 * 1024 * 1024,
            },
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        };
        table
            .try_install_reclaim(&reclaim)
            .expect("idle key must accept reclaim fence");

        assert_eq!(
            table.observe_snapshot(),
            MasterKeyActivityObserveSnapshot {
                active_keys: 3,
                put_keys: 1,
                get_keys: 1,
                replica_keys: 1,
                reclaim_keys: 1,
                inflight_puts: 1,
                inflight_gets: 2,
                inflight_replicas: 1,
            }
        );

        drop(put);
        drop(get_a);
        drop(get_b);
        drop(replica);
        assert!(table.clear_reclaim(&reclaim));
        assert_eq!(
            table.observe_snapshot(),
            MasterKeyActivityObserveSnapshot::default()
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

    #[test]
    fn replica_terminal_result_is_scoped_to_one_append_attempt() {
        let terminals = moka::sync::Cache::builder()
            .time_to_live(Duration::from_secs(120))
            .build();
        let key = "same-kv-generation".to_string();
        let put_id = (17, 3);
        let first_operation_id = 41;
        let second_operation_id = 42;

        terminals.insert(
            (key.clone(), put_id.0, put_id.1, first_operation_id),
            CompletedReplicaTaskInfo { appended: true },
        );

        assert!(
            terminals
                .get(&(key.clone(), put_id.0, put_id.1, first_operation_id))
                .is_some(),
            "a retry of the same append attempt must replay its terminal result"
        );
        assert!(
            terminals
                .get(&(key, put_id.0, put_id.1, second_operation_id))
                .is_none(),
            "an old terminal result must not complete a later remote-copy attempt"
        );
    }

    fn test_route_info_with_tag(node_id: &str, tomb_tag: NodeTombTag) -> KvRouteInfo {
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
            get_durable_reservation: None,
            capacity_reservation: None,
            tomb_tag,
        }
    }

    fn test_route_info(node_id: &str, tomb: bool) -> KvRouteInfo {
        let tomb_tag = NodeTombTag::new();
        if tomb {
            tomb_tag.set_tomb();
        }
        test_route_info_with_tag(node_id, tomb_tag)
    }

    fn test_allocation_route_info(node_id: &str, owner_local_indexed: bool) -> KvRouteInfo {
        let allocator = Arc::new(
            crate::master_seg_manager::one_seg_allocator::OneSegAllocator::new(
                format!("{node_id}-segment"),
                crate::master_seg_manager::msg_pack::SegmentDeviceDescription::Cpu,
                0,
                4096,
            )
            .unwrap(),
        );
        let allocation = allocator.allocate(1024).unwrap();
        KvRouteInfo {
            node_id: node_id.to_string().into(),
            backing: KvReplicaBacking::Allocation(Arc::new(allocation)),
            owner_local_indexed,
            get_durable_reservation: None,
            capacity_reservation: None,
            tomb_tag: NodeTombTag::new(),
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

    #[test]
    fn ring_b_admission_depends_on_backing_and_local_index_not_node_role() {
        let node: NodeID = "same-node".to_string().into();
        let make_route = |lease_id, replica| OneKvNodesRoutes {
            put_id: (91, 3),
            lease_id,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(node.clone(), replica)])),
            get_durable_slots_used: AtomicU32::new(0),
        };

        let unindexed = make_route(None, test_allocation_route_info("same-node", false));
        let desc = ring_b_route_replica_desc(&unindexed, "same-node")
            .expect("unindexed Allocation must enter ring B on any node role");
        assert_eq!(desc.put_id, (91, 3));
        assert_eq!(desc.weight_bytes, 4096);

        let indexed = make_route(None, test_allocation_route_info("same-node", true));
        assert!(ring_b_route_replica_desc(&indexed, "same-node").is_none());

        let leased = make_route(Some(7), test_allocation_route_info("same-node", false));
        assert!(ring_b_route_replica_desc(&leased, "same-node").is_none());

        let mut committed = test_route_info("same-node", false);
        committed.owner_local_indexed = false;
        let committed = make_route(None, committed);
        assert!(ring_b_route_replica_desc(&committed, "same-node").is_none());
    }

    #[test]
    fn member_left_cleanup_removes_only_the_exact_tomb_generation() {
        let routes = DashMap::new();
        let departed_tag = NodeTombTag::new();
        departed_tag.set_tomb();
        let live_reconnect_tag = NodeTombTag::new();
        let newer_tomb_tag = NodeTombTag::new();
        newer_tomb_tag.set_tomb();

        let departed_route = Arc::new(OneKvNodesRoutes {
            put_id: (10, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                "owner".to_string().into(),
                test_route_info_with_tag("owner", departed_tag.clone()),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let live_route = Arc::new(OneKvNodesRoutes {
            put_id: (11, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                "owner".to_string().into(),
                test_route_info_with_tag("owner", live_reconnect_tag),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let newer_tomb_route = Arc::new(OneKvNodesRoutes {
            put_id: (12, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                "owner".to_string().into(),
                test_route_info_with_tag("owner", newer_tomb_tag),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        routes.insert("departed".to_string(), departed_route.clone());
        routes.insert("live".to_string(), live_route.clone());
        routes.insert("newer-tomb".to_string(), newer_tomb_route.clone());

        assert_eq!(
            remove_departed_generation_from_route(
                &routes,
                "departed",
                &departed_route,
                "owner",
                &departed_tag,
            ),
            Some((10, 0))
        );
        assert!(routes.get("departed").is_none());
        assert_eq!(
            remove_departed_generation_from_route(
                &routes,
                "live",
                &live_route,
                "owner",
                &departed_tag,
            ),
            None
        );
        assert!(
            live_route
                .nodes_replicas
                .read()
                .get("owner")
                .is_some_and(|replica| !replica.tomb_tag.is_tomb())
        );
        assert_eq!(
            remove_departed_generation_from_route(
                &routes,
                "newer-tomb",
                &newer_tomb_route,
                "owner",
                &departed_tag,
            ),
            None
        );
        assert!(newer_tomb_route.nodes_replicas.read().contains_key("owner"));
    }

    #[test]
    fn member_left_empty_route_removal_is_arc_identity_aba_safe() {
        let routes = DashMap::new();
        let departed_tag = NodeTombTag::new();
        departed_tag.set_tomb();
        let old_route = Arc::new(OneKvNodesRoutes {
            put_id: (20, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                "owner".to_string().into(),
                test_route_info_with_tag("owner", departed_tag.clone()),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let replacement = Arc::new(test_route((21, 0), None, vec![("peer", false)]));
        routes.insert("aba".to_string(), old_route.clone());
        routes.insert("aba".to_string(), replacement.clone());

        assert_eq!(
            remove_departed_generation_from_route(
                &routes,
                "aba",
                &old_route,
                "owner",
                &departed_tag,
            ),
            None
        );
        let current = routes.get("aba").expect("replacement route must survive");
        assert!(Arc::ptr_eq(current.value(), &replacement));
        assert!(
            current
                .nodes_replicas
                .read()
                .get("peer")
                .is_some_and(|replica| !replica.tomb_tag.is_tomb())
        );
    }

    #[test]
    fn replica_publish_fence_never_overwrites_live_or_publishes_tomb_generation() {
        let old_tag = NodeTombTag::new();
        let route = test_route((30, 0), None, vec![]);
        route.nodes_replicas.write().insert(
            "owner".to_string().into(),
            test_route_info_with_tag("owner", old_tag.clone()),
        );

        let contender_tag = NodeTombTag::new();
        assert!(!publish_route_replica_tomb_fenced(
            &route,
            "owner".to_string().into(),
            test_route_info_with_tag("owner", contender_tag),
        ));
        assert!(
            route
                .nodes_replicas
                .read()
                .get("owner")
                .is_some_and(|replica| replica.tomb_tag.same_generation(&old_tag))
        );

        old_tag.set_tomb();
        let replacement_tag = NodeTombTag::new();
        assert!(publish_route_replica_tomb_fenced(
            &route,
            "owner".to_string().into(),
            test_route_info_with_tag("owner", replacement_tag.clone()),
        ));
        assert!(
            route
                .nodes_replicas
                .read()
                .get("owner")
                .is_some_and(|replica| replica.tomb_tag.same_generation(&replacement_tag))
        );

        let departed_tag = NodeTombTag::new();
        departed_tag.set_tomb();
        assert!(!publish_route_replica_tomb_fenced(
            &route,
            "departed".to_string().into(),
            test_route_info_with_tag("departed", departed_tag),
        ));
        assert!(!route.nodes_replicas.read().contains_key("departed"));
    }

    #[test]
    fn primary_publish_fence_restores_previous_route_on_tomb_generation() {
        let routes = DashMap::new();
        let previous = Arc::new(test_route((40, 0), None, vec![("peer", false)]));
        routes.insert("key".to_string(), previous.clone());

        let departed_tag = NodeTombTag::new();
        departed_tag.set_tomb();
        let rejected = Arc::new(OneKvNodesRoutes {
            put_id: (41, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                "owner".to_string().into(),
                test_route_info_with_tag("owner", departed_tag.clone()),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        assert!(
            publish_primary_route_tomb_fenced(&routes, "key", rejected, &departed_tag).is_err()
        );
        assert!(
            routes
                .get("key")
                .is_some_and(|route| Arc::ptr_eq(route.value(), &previous))
        );

        let live_tag = NodeTombTag::new();
        let accepted = Arc::new(OneKvNodesRoutes {
            put_id: (42, 0),
            lease_id: None,
            atomic_group: None,
            nodes_replicas: RwLock::new(HashMap::from([(
                "owner".to_string().into(),
                test_route_info_with_tag("owner", live_tag.clone()),
            )])),
            get_durable_slots_used: AtomicU32::new(0),
        });
        let replaced =
            publish_primary_route_tomb_fenced(&routes, "key", accepted.clone(), &live_tag)
                .expect("live generation must publish")
                .expect("previous route must be returned");
        assert!(Arc::ptr_eq(&replaced, &previous));
        assert!(
            routes
                .get("key")
                .is_some_and(|route| Arc::ptr_eq(route.value(), &accepted))
        );
    }

    #[test]
    fn reserved_capacity_counters_are_generation_and_arc_scoped() {
        let old_tag = NodeTombTag::new();
        let new_tag = NodeTombTag::new();
        let old = Arc::new(NodeCacheReservedCapacity::new(old_tag.clone()));
        let new = Arc::new(NodeCacheReservedCapacity::new(new_tag.clone()));

        old.apply_delta(ReservedCapacityReason::LeaseBoundKv, 4096);
        new.apply_delta(ReservedCapacityReason::LeaseBoundKv, 8192);
        old_tag.set_tomb();
        old.apply_delta(ReservedCapacityReason::LeaseBoundKv, -4096);

        assert_eq!(old.total_reserved_bytes(), 0);
        assert_eq!(new.total_reserved_bytes(), 8192);
        assert!(!old.generation.same_generation(&new.generation));
        assert!(!new_tag.is_tomb());
    }

    #[test]
    fn tier1_capacity_is_independent_of_ring_b_reservations() {
        let node_space_size = 128 * 1024 * 1024 * 1024;
        let boundaries =
            node_cache_capacity_boundaries(node_space_size, 0.95, Some(0.75), 124_554_051_584);

        assert_eq!(boundaries.ring_b_bytes, 6_012_954_214);
        assert_eq!(boundaries.tier1_bytes, Some(96 * 1024 * 1024 * 1024));
        assert!(boundaries.tier1_bytes.unwrap() > boundaries.ring_b_bytes);
    }

    #[test]
    fn delayed_member_left_is_not_forwarded_to_a_live_generation_actor() {
        assert!(member_left_can_forward_to_registration_actor(None));
        assert!(!member_left_can_forward_to_registration_actor(Some(17)));
    }

    #[test]
    fn ring_b_cache_is_bounded_for_every_node_role() {
        let cache = MasterNodeCache::builder(64)
            .weigher(|_key: &String, desc: &NodeValueReplicaDesc| desc.weight_bytes)
            .build();
        assert_eq!(cache.max_capacity(), Some(64));

        for key in 0..128 {
            insert_master_cache_entry(
                &cache,
                key.to_string(),
                NodeValueReplicaDesc {
                    weight_bytes: 8,
                    put_id: (10, key),
                },
            );
        }
        cache.run_pending_tasks();
        assert!(cache.weighted_size() <= 64);
    }

    #[test]
    fn ring_b_live_capacity_update_never_clears_boundary() {
        let cache = MasterNodeCache::builder(1024)
            .weigher(|_key: &String, desc: &NodeValueReplicaDesc| desc.weight_bytes)
            .build();
        assert_eq!(cache.max_capacity(), Some(1024));
        cache.set_max_capacity(512).unwrap();
        assert_eq!(cache.max_capacity(), Some(512));
        cache.set_max_capacity(2048).unwrap();
        assert_eq!(cache.max_capacity(), Some(2048));
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

    #[test]
    fn multi_identity_registration_rolls_back_partial_insert() {
        let request = |keys: &[&str]| reclaim::EvictionReclaimRequest {
            owner_node_id: "cpu0".to_string(),
            owner_node_start_time: None,
            members: keys
                .iter()
                .enumerate()
                .map(|(index, key)| reclaim::EvictionReclaimMember {
                    key: (*key).to_string(),
                    desc: NodeValueReplicaDesc {
                        weight_bytes: 1024,
                        put_id: (7, index as u32),
                    },
                    expected_backing: None,
                })
                .collect(),
            origin: reclaim::EvictionReclaimOrigin::MasterAllocationCapacity,
            retry_count: 0,
        };
        let inflight = DashSet::new();
        let first = request(&["a", "b"]);
        assert!(try_install_eviction_reclaim_identities(
            &inflight,
            first.identities(),
        ));
        assert_eq!(inflight.len(), 2);
        assert_eq!(
            classify_existing_eviction_reclaim(&inflight, &first.identities()),
            reclaim::EnqueueEvictionReclaimResult::AlreadyInProgress
        );

        // `c` is tentatively installed before the collision on `a`; failure
        // must roll it back and preserve only the original request.
        let overlapping = reclaim::EvictionReclaimRequest {
            members: vec![
                reclaim::EvictionReclaimMember {
                    key: "c".to_string(),
                    desc: NodeValueReplicaDesc {
                        weight_bytes: 1024,
                        put_id: (7, 2),
                    },
                    expected_backing: None,
                },
                first.members[0].clone(),
            ],
            ..request(&[])
        };
        let tentative_identity = overlapping.identities()[0].clone();
        assert!(!try_install_eviction_reclaim_identities(
            &inflight,
            overlapping.identities(),
        ));
        assert_eq!(inflight.len(), 2);
        assert!(!inflight.contains(&tentative_identity));
        assert_eq!(
            classify_existing_eviction_reclaim(&inflight, &overlapping.identities()),
            reclaim::EnqueueEvictionReclaimResult::PartialOverlap
        );

        for identity in first.identities() {
            assert!(inflight.remove(&identity).is_some());
        }
        assert_eq!(
            classify_existing_eviction_reclaim(&inflight, &first.identities()),
            reclaim::EnqueueEvictionReclaimResult::NotInProgress
        );
    }

    #[test]
    fn eviction_reclaim_metadata_channel_does_not_drop_at_old_queue_limit() {
        let (tx, mut rx) = ampsc::unbounded_channel();
        let count = 4096 * 2 + 1;
        for index in 0..count {
            tx.send(reclaim::EvictionReclaimRequest {
                owner_node_id: "cpu0".to_string(),
                owner_node_start_time: None,
                members: vec![reclaim::EvictionReclaimMember {
                    key: format!("key-{index}"),
                    desc: NodeValueReplicaDesc {
                        weight_bytes: 4096,
                        put_id: (7, index as u32),
                    },
                    expected_backing: None,
                }],
                origin: reclaim::EvictionReclaimOrigin::MasterAllocationCapacity,
                retry_count: 0,
            })
            .unwrap();
        }
        assert_eq!((0..count).filter(|_| rx.try_recv().is_ok()).count(), count);
    }

    #[test]
    fn closed_lossless_channel_rolls_back_identity_accounting_and_metadata() {
        let cache = MasterNodeCache::builder(1024 * 1024).build();
        let request = reclaim::EvictionReclaimRequest {
            owner_node_id: "cpu0".to_string(),
            owner_node_start_time: None,
            members: vec![reclaim::EvictionReclaimMember {
                key: "closed-channel".to_string(),
                desc: NodeValueReplicaDesc {
                    weight_bytes: 4096,
                    put_id: (7, 1),
                },
                expected_backing: None,
            }],
            origin: reclaim::EvictionReclaimOrigin::MasterAllocationCapacity,
            retry_count: 0,
        };
        let inflight = DashSet::new();
        assert!(try_install_eviction_reclaim_identities(
            &inflight,
            request.identities(),
        ));
        let pending = AtomicU64::new(request.weight_bytes());
        let (tx, rx) = ampsc::unbounded_channel();
        drop(rx);

        let returned = tx.send(request).unwrap_err().0;
        for identity in returned.identities() {
            assert!(inflight.remove(&identity).is_some());
        }
        subtract_pending_eviction_weight(&pending, "cpu0", returned.weight_bytes());
        for member in returned.members {
            insert_master_cache_entry(&cache, member.key, member.desc);
        }

        assert!(inflight.is_empty());
        assert_eq!(pending.load(Ordering::Acquire), 0);
        assert!(cache.get(&"closed-channel".to_string()).is_some());
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
    /// Idempotent terminal PutAppendDone results retained across response
    /// loss and batch-to-individual fallback.
    pub completed_replica_tasks:
        moka::future::Cache<(String, u64, u32, u64), CompletedReplicaTaskInfo>,
    /// Serializes Start/Done/Revoke for one replica operation identity. The
    /// lock is per `(key, put_id)`, so unrelated remote writes never share a
    /// queue or actor.
    pub replica_operation_locks: AMapLock<(String, u64, u32)>,
    pub inflight_gets: moka::future::Cache<u64, InflightGetInfo>,
    /// Idempotent terminal GetDone results retained across response loss/retry.
    pub completed_gets: moka::future::Cache<u64, CompletedGetInfo>,
    pub get_done_locks: AMapLock<u64>,
    pub(crate) key_activity: Arc<MasterKeyActivityTable>,
    pub(crate) prepared_get_requesters: Arc<PreparedGetRequesterTable>,

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

    /// Counter for concrete replica append attempts.  This is separate from
    /// `put_id`: one KV generation may legitimately be copied remotely again
    /// after its previous remote route is reclaimed.
    pub next_replica_operation_id: AtomicU64,

    /// Counter for two-sided owner reclaim epochs.
    pub next_owner_reclaim_epoch: AtomicU64,

    /// Latest version of key-value replicas
    pub kv_routes: DashMap<String, Arc<OneKvNodesRoutes>>,

    /// Interns recent multi-key put groups so all member routes share one descriptor.
    put_atomic_groups: moka::sync::SegmentedCache<(String, u64, u32), Arc<PutAtomicGroup>>,

    /// Grants reserved for owner-local hot-path staging.
    pub local_reserve_grants: DashMap<u64, LocalReserveGrantInfo>,

    /// Prepared key reservations for owner-local hot-path staging.
    pub prepared_put_key_reservations: DashMap<u64, PreparedPutKeyReservationInfo>,

    /// Prefix-counting index for keys, used by CountPrefix RPC.
    pub prefix_index: ARwLock<PrefixRadixTree>,

    /// Support replicas: node_id -> key -> route_info
    pub node_kv_cache_controller: DashMap<NodeIDString, Arc<MasterNodeCache>>,

    /// Independent pre-writeback tier. Its Size eviction starts a replica task
    /// while owner residency remains governed by the owner's local hot cache;
    /// `node_kv_cache_controller` tracks ring B only.
    pub node_writeback_tier1_controller: DashMap<NodeIDString, Arc<MasterNodeCache>>,

    /// Per-node bytes reserved out of moka usable capacity.
    /// The reservation is reason-grouped, but `total_bytes` is the authority
    /// used to derive the effective moka max_capacity for the node.
    pub node_cache_reserved_capacity: DashMap<NodeIDString, Arc<NodeCacheReservedCapacity>>,

    /// Moka weight already removed and queued for owner-side safe reclaim.
    pub eviction_reclaim_pending_weight: DashMap<NodeIDString, Arc<AtomicU64>>,

    /// Exact, versioned metadata identities currently owned by the lossless
    /// eviction-reclaim pipeline. This bounds duplicate Size/victim events;
    /// payload memory is not retained here.
    pub(crate) eviction_reclaim_inflight: DashSet<reclaim::EvictionReclaimIdentity>,

    /// Per-owner reclaim lifecycle counters. These distinguish transient holder/activity
    /// deferrals from terminal route changes and bounded retry restoration.
    pub(crate) eviction_reclaim_counters: DashMap<NodeIDString, Arc<EvictionReclaimCounters>>,

    /// Async admission gate in front of Moka's synchronous housekeeper lock.
    /// Bounded maintenance can still process a batch of Size evictions; without
    /// this gate, concurrent async tasks can park multiple Tokio workers on the
    /// same blocking Moka lock. It is a scheduling gate, not a correctness or
    /// request lock, and no cache/global scan occurs under it.
    pub(crate) owner_cache_operation_locks: AMapLock<String>,

    /// Historical final put placement decisions by target node.
    pub put_target_decision_counts: DashMap<NodeIDString, Arc<AtomicU64>>,

    /// Historical final put placement decisions by requester->target pair.
    pub put_requester_target_decision_counts: DashMap<RequesterTargetPair, Arc<AtomicU64>>,

    /// Historical final put placement decisions grouped by placement mode.
    pub put_placement_mode_counts: DashMap<&'static str, Arc<AtomicU64>>,

    /// Historical accepted replica task reservations by target node.
    pub replica_task_target_counts: DashMap<NodeIDString, Arc<AtomicU64>>,
    /// PutAppendDone responses served from the terminal cache after an RPC
    /// replay or batch fallback.
    pub replica_done_terminal_replay_count: AtomicU64,

    /// Historical get source choices by requester->source pair.
    pub get_requester_source_counts: DashMap<RequesterTargetPair, Arc<AtomicU64>>,
    pub get_requester_source_bytes: DashMap<RequesterTargetPair, Arc<AtomicU64>>,
    pub get_allocation_mode_counts: DashMap<&'static str, Arc<AtomicU64>>,

    /// Support replicas: key -> version_id
    recent_key_versionid_allocator: moka::sync::SegmentedCache<String, Arc<AtomicU32>>,

    pub delete_broadcast: EnsureMemholderMgmtDeleteHandle<DeleteKeyInfo>,
    post_route_maintenance_tx: ampsc::Sender<route_maintenance::RoutePublishEvent>,
    post_route_maintenance_rx: Mutex<Option<ampsc::Receiver<route_maintenance::RoutePublishEvent>>>,
    eviction_reclaim_tx: ampsc::UnboundedSender<reclaim::EvictionReclaimRequest>,
    eviction_reclaim_rx: Mutex<Option<ampsc::UnboundedReceiver<reclaim::EvictionReclaimRequest>>>,
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
        let prepared_get_requesters = Arc::new(PreparedGetRequesterTable::default());
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
        let completed_gets = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(120))
            .build();
        let inflight_replica_tasks = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .eviction_listener(|_put_id, inflight_info: InflightReplicaTaskInfo, cause| {
                if cause == RemovalCause::Expired {
                    inflight_info._activity_lease.release_now();
                }
            })
            .build();
        let completed_replica_tasks = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(120))
            .build();
        // A synchronous Moka listener must perform only constant-size metadata
        // work and must never block or drop an Allocation reclaim event.  The
        // versioned inflight set deduplicates this unbounded channel.
        let (eviction_reclaim_tx, eviction_reclaim_rx) = ampsc::unbounded_channel();
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
            completed_replica_tasks,
            replica_operation_locks: AMapLock::new(Duration::from_secs(10 * 60)),
            inflight_gets,
            completed_gets,
            get_done_locks: AMapLock::new(Duration::from_secs(10 * 60)),
            key_activity,
            prepared_get_requesters,
            get_holding: MasterOwnerMemMgr::default(),
            next_get_id: AtomicU64::new(0),
            next_holder_id: AtomicU64::new(0),
            next_local_reserve_grant_id: AtomicU64::new(1),
            next_prepared_put_key_reservation_id: AtomicU64::new(1),
            next_replica_operation_id: AtomicU64::new(1),
            next_owner_reclaim_epoch: AtomicU64::new(1),
            kv_routes: DashMap::new(),
            put_atomic_groups: moka::sync::SegmentedCache::builder(8)
                .max_capacity(262_144)
                .time_to_idle(Duration::from_secs(30 * 60))
                .build(),
            local_reserve_grants: DashMap::new(),
            prepared_put_key_reservations: DashMap::new(),
            prefix_index: ARwLock::new(PrefixRadixTree::new()),
            node_kv_cache_controller: DashMap::new(),
            node_writeback_tier1_controller: DashMap::new(),
            node_cache_reserved_capacity: DashMap::new(),
            eviction_reclaim_pending_weight: DashMap::new(),
            eviction_reclaim_inflight: DashSet::new(),
            eviction_reclaim_counters: DashMap::new(),
            owner_cache_operation_locks: AMapLock::new(Duration::from_secs(10 * 60)),
            put_target_decision_counts: DashMap::new(),
            put_requester_target_decision_counts: DashMap::new(),
            put_placement_mode_counts: DashMap::new(),
            replica_task_target_counts: DashMap::new(),
            replica_done_terminal_replay_count: AtomicU64::new(0),
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
        node_cache_capacity_boundaries(
            node_space_size,
            self.inner().replica_cache_capacity_ratio,
            self.inner().replica_writeback_tier1_capacity_ratio,
            0,
        )
        .ring_b_bytes
    }

    fn replica_cache_effective_capacity(&self, node_id: &str, node_space_size: u64) -> u64 {
        let reserved_capacity = self
            .inner()
            .node_cache_reserved_capacity
            .get(node_id)
            .filter(|reserved| !reserved.generation.is_tomb())
            .map(|reserved| reserved.total_reserved_bytes())
            .unwrap_or(0);
        node_cache_capacity_boundaries(
            node_space_size,
            self.inner().replica_cache_capacity_ratio,
            self.inner().replica_writeback_tier1_capacity_ratio,
            reserved_capacity,
        )
        .ring_b_bytes
    }

    /// Refresh already-created node controllers after segment metadata or
    /// reservation inputs change. Generation-scoped reservations reduce only
    /// the physical ring-B allocation boundary. Tier1 is an inclusive metadata
    /// policy window and always remains ratio-derived from the owner segment.
    fn reconcile_node_cache_capacity(&self, node_id: &str) {
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(node_id);
        if node_space_size == 0 {
            return;
        }
        let capacity = self.replica_cache_effective_capacity(node_id, node_space_size);
        if let Some(cache) = self
            .inner()
            .node_kv_cache_controller
            .get(node_id)
            .map(|entry| entry.value().clone())
        {
            if let Err(err) = cache.set_max_capacity(capacity) {
                error!(
                    "failed to refresh ring-B cache capacity: node={} capacity={} err={}",
                    node_id, capacity, err,
                );
            }
        }
        if let Some(cache) = self
            .inner()
            .node_writeback_tier1_controller
            .get(node_id)
            .map(|entry| entry.value().clone())
        {
            let tier1_capacity = self
                .writeback_tier1_base_capacity(node_space_size)
                .unwrap_or(0);
            if let Err(err) = cache.set_max_capacity(tier1_capacity) {
                error!(
                    "failed to refresh tier1 cache capacity: node={} capacity={} err={}",
                    node_id, tier1_capacity, err,
                );
            }
        }
    }

    fn writeback_tier1_base_capacity(&self, node_space_size: u64) -> Option<u64> {
        node_cache_capacity_boundaries(
            node_space_size,
            self.inner().replica_cache_capacity_ratio,
            self.inner().replica_writeback_tier1_capacity_ratio,
            0,
        )
        .tier1_bytes
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
        let lease = self
            .inner()
            .key_activity
            .reserve(key, MasterKeyActivityKind::Put, reject_if_inflight_same_key)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyBeingWritten {
                        key: key.to_string(),
                    },
                )
            })?;
        self.pin_current_master_cache_entries_for_activity(&lease, key);
        Ok(lease)
    }

    pub(crate) fn reserve_inflight_get_key(
        &self,
        key: &str,
    ) -> Result<Arc<MasterKeyActivityLease>, KvError> {
        let lease = self
            .inner()
            .key_activity
            .reserve(key, MasterKeyActivityKind::Get, false)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyNotFound {
                        key: key.to_string(),
                    },
                )
            })?;
        self.pin_current_master_cache_entries_for_activity(&lease, key);
        Ok(lease)
    }

    pub(crate) fn reserve_prepared_get_requester(
        &self,
        key: &str,
        requester: &NodeID,
        get_id: u64,
    ) -> Result<Arc<PreparedGetRequesterLease>, KvError> {
        self.inner()
            .prepared_get_requesters
            .reserve(key, requester, get_id)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyBeingWritten {
                        key: key.to_string(),
                    },
                )
            })
    }

    pub(crate) fn reserve_inflight_replica_key(
        &self,
        key: &str,
    ) -> Result<Arc<MasterKeyActivityLease>, KvError> {
        let lease = self
            .inner()
            .key_activity
            .reserve(key, MasterKeyActivityKind::Replica, false)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyBeingWritten {
                        key: key.to_string(),
                    },
                )
            })?;
        self.pin_current_master_cache_entries_for_activity(&lease, key);
        Ok(lease)
    }

    fn attach_cache_pin_if_present(
        &self,
        lease: &Arc<MasterKeyActivityLease>,
        cache: &MasterNodeCache,
        key: &str,
        desc: &NodeValueReplicaDesc,
    ) {
        let alias = MasterPinAlias::new(key, desc.put_id);
        if let Some(pin) = cache.try_pin_alias_if(alias, |entry| {
            entry.put_id == desc.put_id && entry.weight_bytes == desc.weight_bytes
        }) {
            lease.attach_cache_pin(pin);
        }
    }

    pub(crate) fn pin_master_cache_identity_for_activity(
        &self,
        lease: &Arc<MasterKeyActivityLease>,
        owner_node_id: &str,
        key: &str,
        desc: &NodeValueReplicaDesc,
    ) {
        if let Some(cache) = self
            .inner()
            .node_kv_cache_controller
            .get(owner_node_id)
            .map(|entry| entry.value().clone())
        {
            self.attach_cache_pin_if_present(lease, cache.as_ref(), key, desc);
        }
        if let Some(cache) = self
            .inner()
            .node_writeback_tier1_controller
            .get(owner_node_id)
            .map(|entry| entry.value().clone())
        {
            self.attach_cache_pin_if_present(lease, cache.as_ref(), key, desc);
        }
    }

    pub(crate) fn pin_current_master_cache_identity_for_activity(
        &self,
        lease: &Arc<MasterKeyActivityLease>,
        owner_node_id: &str,
        key: &str,
        put_id: PutIDForAKey,
    ) {
        let desc = self.inner().kv_routes.get(key).and_then(|route| {
            (route.put_id == put_id).then(|| ring_b_route_replica_desc(&route, owner_node_id))?
        });
        if let Some(desc) = desc {
            self.pin_master_cache_identity_for_activity(lease, owner_node_id, key, &desc);
        }
    }

    fn pin_current_master_cache_entries_for_activity(
        &self,
        lease: &Arc<MasterKeyActivityLease>,
        key: &str,
    ) {
        let entries = self
            .inner()
            .kv_routes
            .get(key)
            .map(|route| {
                let put_id = route.put_id;
                route
                    .nodes_replicas
                    .read()
                    .iter()
                    .filter_map(|(node_id, replica)| {
                        (!replica.tomb_tag.is_tomb()).then(|| {
                            (
                                node_id.as_ref().to_string(),
                                NodeValueReplicaDesc {
                                    weight_bytes: u32::try_from(replica.backing.capacity_bytes())
                                        .unwrap_or(u32::MAX),
                                    put_id,
                                },
                            )
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for (node_id, desc) in entries {
            self.pin_master_cache_identity_for_activity(lease, node_id.as_str(), key, &desc);
        }
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
        origin: reclaim::EvictionReclaimOrigin,
    ) -> bool {
        self.enqueue_eviction_reclaim_request(
            owner_node_id,
            vec![reclaim::EvictionReclaimMember {
                key,
                desc,
                expected_backing: None,
            }],
            origin,
        )
    }

    fn enqueue_eviction_reclaim_request(
        &self,
        owner_node_id: NodeIDString,
        members: Vec<reclaim::EvictionReclaimMember>,
        origin: reclaim::EvictionReclaimOrigin,
    ) -> bool {
        matches!(
            self.enqueue_eviction_reclaim_request_exact(
                owner_node_id,
                None,
                members,
                origin,
                true,
            ),
            reclaim::EnqueueEvictionReclaimResult::Accepted
                | reclaim::EnqueueEvictionReclaimResult::AlreadyInProgress
        )
    }

    pub(crate) fn enqueue_owner_capacity_eviction_victim(
        &self,
        owner_node_id: NodeIDString,
        owner_node_start_time: i64,
        victim: reclaim::EvictionReclaimMember,
    ) -> reclaim::EnqueueEvictionReclaimResult {
        self.enqueue_eviction_reclaim_request_exact(
            owner_node_id,
            Some(owner_node_start_time),
            vec![victim],
            reclaim::EvictionReclaimOrigin::OwnerCapacityEviction,
            true,
        )
    }

    fn enqueue_eviction_reclaim_request_exact(
        &self,
        owner_node_id: NodeIDString,
        owner_node_start_time: Option<i64>,
        members: Vec<reclaim::EvictionReclaimMember>,
        origin: reclaim::EvictionReclaimOrigin,
        allow_new_request: bool,
    ) -> reclaim::EnqueueEvictionReclaimResult {
        if members.len() != 1 {
            return reclaim::EnqueueEvictionReclaimResult::PartialOverlap;
        }
        let request = reclaim::EvictionReclaimRequest {
            owner_node_id,
            owner_node_start_time,
            members,
            origin,
            retry_count: 0,
        };
        if !allow_new_request {
            let identities = request.identities();
            return classify_existing_eviction_reclaim(
                &self.inner().eviction_reclaim_inflight,
                &identities,
            );
        }
        if !self.register_eviction_reclaim(&request) {
            self.eviction_reclaim_counters(&request.owner_node_id)
                .eviction_reclaim_deduplicated
                .fetch_add(1, Ordering::Relaxed);
            // An idempotent retry is accepted only when the exact victim is
            // already owned by the pipeline.
            return if request
                .identities()
                .iter()
                .all(|identity| self.inner().eviction_reclaim_inflight.contains(identity))
            {
                reclaim::EnqueueEvictionReclaimResult::AlreadyInProgress
            } else {
                reclaim::EnqueueEvictionReclaimResult::PartialOverlap
            };
        }
        if let Err(err) = self.inner().eviction_reclaim_tx.send(request) {
            let request = err.0;
            self.complete_eviction_reclaim(&request);
            if request.origin == reclaim::EvictionReclaimOrigin::MasterAllocationCapacity {
                // This branch can run inside Moka's synchronous Size listener.
                // Never re-enter that cache while its housekeeper lock is
                // held; restore by key after yielding out of the callback.
                let restore_view = self.inner().view().clone();
                let restore_request = request.clone();
                let spawn_view = restore_view.clone();
                let _ = spawn_view.spawn("closed_eviction_reclaim_restore", async move {
                    tokio::task::yield_now().await;
                    let mut restored = 0usize;
                    for member in &restore_request.members {
                        if restore_view.master_kv_router().restore_eviction_cache_entry_if_current(
                            &restore_request.owner_node_id,
                            member.key.clone(),
                            member.desc.clone(),
                        ) {
                            restored += 1;
                        }
                    }
                    warn!(
                        "restored master Allocation metadata after reclaim actor closed: owner={} members={} restored={}",
                        restore_request.owner_node_id,
                        restore_request.members.len(),
                        restored,
                    );
                });
            }
            warn!(
                "lossless eviction reclaim actor is closed: owner={} members={} origin={:?} restore_deferred={}",
                request.owner_node_id,
                request.members.len(),
                request.origin,
                request.origin == reclaim::EvictionReclaimOrigin::MasterAllocationCapacity,
            );
            return reclaim::EnqueueEvictionReclaimResult::Closed;
        }
        reclaim::EnqueueEvictionReclaimResult::Accepted
    }

    pub(crate) fn register_eviction_reclaim(
        &self,
        request: &reclaim::EvictionReclaimRequest,
    ) -> bool {
        if !try_install_eviction_reclaim_identities(
            &self.inner().eviction_reclaim_inflight,
            request.identities(),
        ) {
            return false;
        }
        let pending_weight = self
            .inner()
            .eviction_reclaim_pending_weight
            .entry(request.owner_node_id.clone())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .value()
            .clone();
        pending_weight.fetch_add(request.weight_bytes(), Ordering::AcqRel);
        true
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

    pub(crate) fn complete_eviction_reclaim(&self, request: &reclaim::EvictionReclaimRequest) {
        for identity in request.identities() {
            assert!(
                self.inner()
                    .eviction_reclaim_inflight
                    .remove(&identity)
                    .is_some(),
                "eviction reclaim identity completed without registration: {:?}",
                identity,
            );
        }
        let completed_weight = request.weight_bytes();
        let pending_weight = self
            .inner()
            .eviction_reclaim_pending_weight
            .get(&request.owner_node_id)
            .unwrap_or_else(|| {
                panic!(
                    "eviction reclaim pending weight missing for owner {}",
                    request.owner_node_id
                )
            });
        subtract_pending_eviction_weight(
            pending_weight.value().as_ref(),
            &request.owner_node_id,
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
            ring_b_route_replica_desc(&route, owner_node_id).is_some_and(|current| {
                current.put_id == desc.put_id && current.weight_bytes == desc.weight_bytes
            })
        })
    }

    /// Point-remove one exact ring-B metadata identity.  Callers use the
    /// async per-node gate before entering Moka's synchronous housekeeper, so
    /// route conversion never falls back to a cache scan.
    pub(crate) async fn remove_node_cache_entry_exact(
        &self,
        owner_node_id: &str,
        key: &str,
        desc: &NodeValueReplicaDesc,
    ) -> bool {
        let owner_cache_lock = self
            .inner()
            .owner_cache_operation_locks
            .get_lock(owner_node_id.to_string());
        let _owner_cache_guard = owner_cache_lock.lock().await;
        self.inner()
            .node_kv_cache_controller
            .get(owner_node_id)
            .is_some_and(|cache| remove_exact_cache_entry(cache.value(), key, desc))
    }

    /// Remove one superseded route version from both metadata policies using
    /// only the route's own replica list.  This is O(replica count), never a
    /// Moka or global-route scan, and cannot delete a newer same-key version.
    pub(crate) async fn remove_route_cache_entries_exact(
        &self,
        key: &str,
        route: &OneKvNodesRoutes,
    ) {
        let replicas = route
            .nodes_replicas
            .read()
            .iter()
            .filter_map(|(node_id, replica)| {
                (!replica.tomb_tag.is_tomb()).then(|| {
                    (
                        node_id.as_ref().to_string(),
                        NodeValueReplicaDesc {
                            weight_bytes: u32::try_from(replica.backing.capacity_bytes())
                                .unwrap_or(u32::MAX),
                            put_id: route.put_id,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        for (node_id, desc) in replicas {
            let owner_cache_lock = self
                .inner()
                .owner_cache_operation_locks
                .get_lock(node_id.clone());
            let _owner_cache_guard = owner_cache_lock.lock().await;
            if let Some(cache) = self.inner().node_kv_cache_controller.get(&node_id) {
                let _ = remove_exact_cache_entry(cache.value(), key, &desc);
            }
            if let Some(cache) = self.inner().node_writeback_tier1_controller.get(&node_id) {
                let _ = remove_exact_cache_entry(cache.value(), key, &desc);
            }
        }
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
        // Restoration is an exact metadata repair after a failed reclaim
        // dispatch. It must never search for or evict an unrelated victim.
        insert_master_cache_entry(cache.as_ref(), key, desc);
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
            let req_node_id = resp.node_id().clone();
            let _ = view.spawn("rpc_get_revoke", async move {
                let ack = handle_get_revoke(view_task, msg, req_node_id).await;
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
            let req_node_id = resp.node_id().clone();
            let _ = view.spawn("rpc_get_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_get_done(view_task, msg, req_node_id).await;
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
        RPCHandler::<BatchEvictOwnerSourceReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let owner = resp.node_id().clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_batch_evict_owner_source", async move {
                let ack = handle_batch_evict_owner_source(&view_task, msg, owner).await;
                if let Err(err) = resp.send_resp(ack).await {
                    error!("Failed to send BatchEvictOwnerSourceResp: {:?}", err);
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
        RPCHandler::<BatchPutAppendStartReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_batch_put_append_start", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_put_append_start(view_task, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchPutAppendStartResp: {:?}", e);
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

        let view = self.0.view().clone();
        RPCHandler::<BatchPutAppendDoneReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let view_task = view2.clone();
            let _ = view.spawn("rpc_batch_put_append_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_put_append_done(view_task, msg).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchPutAppendDoneResp: {:?}", e);
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
            let req_node_id = resp.node_id().clone();
            view.spawn("rpc_batch_get_revoke", async move {
                let ack = handle_batch_get_revoke(view2, msg, req_node_id).await;
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
            let req_node_id = resp.node_id().clone();
            view.spawn("rpc_batch_get_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_batch_get_done(view2, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchGetDoneResp: {:?}", e);
                }
            });
            Ok(())
        });
    }

    /// Start cleanup for one exact departed membership generation.
    ///
    /// Tomb publication and O(1) controller detachment happen synchronously in the cluster
    /// listener. The potentially large route scan is then performed by a yielding async task.
    fn begin_departed_generation_cleanup(
        &self,
        node_id: &str,
        expected_node_start_time: Option<i64>,
    ) -> bool {
        let node: NodeID = node_id.to_string().into();
        let Some(departed_tag) = self
            .inner()
            .view()
            .master_seg_manager()
            .mark_node_tomb_generation(&node, expected_node_start_time)
        else {
            debug!(
                "MemberLeft generation cleanup skipped because the registered generation changed or no segment was registered: node={} expected_node_start_time={:?}",
                node_id, expected_node_start_time
            );
            return false;
        };

        // Detach generation-scoped controllers before starting the full route scan. A tombed
        // segment reports zero usable space, so old-generation work cannot recreate the caches.
        let resident_cache_detached = self
            .inner()
            .node_kv_cache_controller
            .remove(node_id)
            .is_some();
        let tier1_cache_detached = self
            .inner()
            .node_writeback_tier1_controller
            .remove(node_id)
            .is_some();
        self.inner()
            .node_cache_reserved_capacity
            .remove_if(node_id, |_, reserved| {
                reserved.generation.same_generation(&departed_tag)
            });

        let removed_holdings = self.inner().get_holding.cleanup_node(node_id);
        let view = self.inner().view().clone();
        let node_id_owned = node_id.to_string();
        let _ = view.clone().spawn("member_left_route_cleanup", async move {
            cleanup_departed_generation_routes(view, node_id_owned, departed_tag).await;
        });

        info!(
            "MemberLeft generation marked and controllers detached: node={} expected_node_start_time={:?} resident_cache_detached={} tier1_cache_detached={} removed_holdings={}",
            node_id,
            expected_node_start_time,
            resident_cache_detached,
            tier1_cache_detached,
            removed_holdings
        );
        true
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
	                        view.master_kv_router()
	                            .begin_departed_generation_cleanup(&node_id, Some(prev));
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
                                        "MasterKvRouter registration actor canceled departed node: {:?}",
                                        node_id
                                    );
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
                                        "MasterKvRouter registration actor canceled departed node: {:?}",
                                        node_id
                                    );
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
                                    if let Some(node_id) = actor_node_id.as_deref() {
                                        view_task
                                            .master_kv_router()
                                            .reconcile_node_cache_capacity(node_id);
                                    }
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
                                        "MasterKvRouter registration actor canceled departed node: {:?}",
                                        node_id
                                    );
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
                    ClusterEvent::MemberJoined(member) | ClusterEvent::MemberUpdated(member) => {
                        view.master_kv_router()
                            .reconcile_node_cache_capacity(&member.id);
                    }
                    ClusterEvent::MemberLeft(node_id) => {
                        let departed_epoch = view
                            .cluster_manager()
                            .get_prev_member_info(node_id)
                            .map(|member| member.node_start_time);
                        let current_member = view
                            .cluster_manager()
                            .get_member_info_cached(node_id);
                        let current_epoch = current_member
                            .as_ref()
                            .map(|member| member.node_start_time);

                        // MemberLeft has no epoch. Once a live generation is visible, this leave
                        // is ambiguous and must neither clean state nor reach the per-node actor:
                        // forwarding it would clear desired/registered_epoch for the reconnect.
                        if !member_left_can_forward_to_registration_actor(current_epoch) {
                            debug!(
                                "ignoring delayed MemberLeft after reconnect: node={} current_node_start_time={}",
                                node_id,
                                current_epoch.unwrap_or_default()
                            );
                            return;
                        }

                        let registered_tag_before = view
                            .master_seg_manager()
                            .get_node_tomb_tag(&node_id.clone().into());
                        if !view
                            .master_kv_router()
                            .begin_departed_generation_cleanup(node_id, departed_epoch)
                        {
                            // A registered segment exists but did not match `departed_epoch`:
                            // preserve it and do not forward the old leave to the actor.
                            if registered_tag_before.is_some() {
                                debug!(
                                    "ignoring generation-mismatched MemberLeft: node={} departed_epoch={:?}",
                                    node_id, departed_epoch
                                );
                                return;
                            }

                            // External/zero-contribution members do not register a segment or
                            // own route backing, but they can still own get holdings.
                            let removed = view
                                .master_kv_router()
                                .inner()
                                .get_holding
                                .cleanup_node(node_id);
                            if removed > 0 {
                                info!(
                                    "Cleaned up {} holdings for segmentless left member {}",
                                    removed, node_id
                                );
                            }
                        }
                    }
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
            GetAllocationMode::LocalCommittedSlot => "local_committed_slot",
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
                        let replica_done_terminal_replays = router
                            .inner()
                            .replica_done_terminal_replay_count
                            .load(Ordering::Relaxed);

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
                            "placement historical distribution | put_target_counts={:?} | put_mode_counts={:?} | put_requester_target_counts={:?} | replica_task_target_counts={:?} | replica_done_terminal_replays={} | get_requester_source_counts={:?} | get_requester_source_bytes={:?} | get_allocation_mode_counts={:?}",
                            target_counts,
                            mode_counts,
                            requester_target_counts,
                            replica_task_target_counts,
                            replica_done_terminal_replays,
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

    pub fn get_node_cache_controller(&self, node_id: &str) -> Option<Arc<MasterNodeCache>> {
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
        // Ring B is a backing/index domain, not a placement-role domain.  A
        // GPU owner can also hold master-only Allocation replicas, so every
        // live segment gets the same bounded controller.  Non-ring-B bytes
        // are subtracted through generation-scoped reservation tokens.
        let allocation_capacity = self.replica_cache_effective_capacity(node_id, node_space_size);
        let view = self.inner().view().clone();
        let node_id_owned = node_id.to_string();
        Some(
            self.inner()
                .node_kv_cache_controller
                .entry(node_id_owned.clone())
                .or_insert_with(move || {
                    let view = view.clone();
                    let cache_node_id = node_id_owned.clone();
                    let builder = MasterNodeCache::builder(allocation_capacity)
                        // Admission is restricted to unindexed Allocation
                        // routes. CommittedSlot and owner-indexed Allocation
                        // entries belong to ring A and never enter this cache.
                        .weigher(|_key: &String, value: &NodeValueReplicaDesc| value.weight_bytes)
                        .eviction_listener(
                            move |key: Arc<String>,
                                  value: NodeValueReplicaDesc,
                                  cause: RemovalCause| {
                                if cause == RemovalCause::Size {
                                    // Listener work is deliberately O(1):
                                    // clone fixed metadata, dedupe, and send
                                    // to a lossless channel. Route/victim
                                    // lookup belongs to the async actor.
                                    let _ = view.master_kv_router().enqueue_eviction_reclaim(
                                        cache_node_id.clone(),
                                        (*key).clone(),
                                        value,
                                        reclaim::EvictionReclaimOrigin::MasterAllocationCapacity,
                                    );
                                }
                            },
                        );
                    Arc::new(builder.build())
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
    ) -> Option<Arc<MasterNodeCache>> {
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
                        MasterNodeCache::builder(base_capacity)
                            .weigher(|_key: &String, value: &NodeValueReplicaDesc| {
                                value.weight_bytes
                            })
                            .eviction_listener(
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
                            )
                            .build(),
                    )
                })
                .value()
                .clone(),
        )
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

    /// Adjust one exact generation/counter identity and refresh its live cache
    /// boundary.  Negative releases always stay applied to the captured Arc,
    /// even when the node has already left and its controller was detached.
    fn adjust_node_cache_reserved_capacity_identity(
        &self,
        node_id: &str,
        generation: &NodeTombTag,
        reserved_capacity: &Arc<NodeCacheReservedCapacity>,
        reason: ReservedCapacityReason,
        delta_bytes: i64,
    ) -> crate::rpcresp_kvresult_convert::msg_and_error::KvResult<()> {
        if !self.replica_cache_enabled() {
            return Ok(());
        }
        if !reserved_capacity.generation.same_generation(generation) {
            return Err(
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "cache reservation generation/counter identity mismatch: node_id={}",
                            node_id
                        ),
                    },
                ),
            );
        }

        reserved_capacity.apply_delta(reason, delta_bytes);

        let current_identity = self
            .inner()
            .node_cache_reserved_capacity
            .get(node_id)
            .is_some_and(|current| {
                Arc::ptr_eq(current.value(), reserved_capacity)
                    && current.generation.same_generation(generation)
            })
            && node_generation_is_current_live(
                self.inner().view(),
                &node_id.to_string().into(),
                generation,
            );
        if !current_identity {
            if delta_bytes >= 0 {
                reserved_capacity.apply_delta(reason, -delta_bytes);
                return Err(
                    crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                        crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidPutMasterState {
                            detail: format!(
                                "cache reservation target generation changed: node_id={}",
                                node_id
                            ),
                        },
                    ),
                );
            }
            // The old generation has been detached.  Its exact counter Arc is
            // now balanced, and no live controller must be modified.
            return Ok(());
        }

        // Recompute target capacity from the configured base ratio minus live reservations.
        let reserved_total = reserved_capacity.total_reserved_bytes();
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(node_id);
        if node_space_size == 0 {
            if delta_bytes >= 0 {
                reserved_capacity.apply_delta(reason, -delta_bytes);
            }
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
        let boundaries = node_cache_capacity_boundaries(
            node_space_size,
            self.inner().replica_cache_capacity_ratio,
            self.inner().replica_writeback_tier1_capacity_ratio,
            reserved_total,
        );
        let new_capacity = boundaries.ring_b_bytes;

        if let Some(cache) = self.get_node_cache_controller(node_id) {
            if let Some(tier1_cache) = self
                .inner()
                .node_writeback_tier1_controller
                .get(node_id)
                .map(|entry| entry.value().clone())
            {
                // Local-reserve and owner-indexed reservations consume the
                // physical ring-B allocation domain. They must not collapse
                // the independent inclusive tier1 metadata window.
                let tier1_capacity = boundaries.tier1_bytes.unwrap_or(0);
                if let Err(e) = tier1_cache.set_max_capacity(tier1_capacity) {
                    if delta_bytes >= 0 {
                        reserved_capacity.apply_delta(reason, -delta_bytes);
                    }
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
            if let Err(e) = cache.set_max_capacity(new_capacity) {
                if delta_bytes >= 0 {
                    reserved_capacity.apply_delta(reason, -delta_bytes);
                }
                return Err(
                    crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                        crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                            rpc_input_json: format!(
                                "ring-B allocation moka.set_max_capacity failed: node_id={}, new_capacity={}, err={}",
                                node_id, new_capacity, e
                            ),
                        },
                    ),
                );
            }
            Ok(())
        } else {
            if delta_bytes >= 0 {
                reserved_capacity.apply_delta(reason, -delta_bytes);
            }
            Err(
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::OwnerNoSeg {
                        detail: format!("node_id={} cache_controller not found", node_id),
                    },
                ),
            )
        }
    }

    /// Reserve cache capacity for one exact live node generation.  The returned
    /// route-lifetime token releases the same counter identity on Drop.
    pub fn reserve_node_cache_capacity(
        &self,
        node_id: &NodeID,
        generation: &NodeTombTag,
        reason: ReservedCapacityReason,
        bytes: u64,
    ) -> crate::rpcresp_kvresult_convert::msg_and_error::KvResult<
        Option<Arc<NodeCacheCapacityReservation>>,
    > {
        if !self.replica_cache_enabled() {
            return Ok(None);
        }
        if !node_generation_is_current_live(self.inner().view(), node_id, generation) {
            return Err(
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidPutMasterState {
                        detail: format!(
                            "cannot reserve cache capacity for departed generation: node_id={}",
                            node_id
                        ),
                    },
                ),
            );
        }

        let reserved_capacity = match self
            .inner()
            .node_cache_reserved_capacity
            .entry(node_id.to_string())
        {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if entry.get().generation.same_generation(generation) {
                    entry.get().clone()
                } else if entry.get().generation.is_tomb() {
                    let replacement = Arc::new(NodeCacheReservedCapacity::new(generation.clone()));
                    entry.insert(replacement.clone());
                    replacement
                } else {
                    return Err(
                        crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidPutMasterState {
                                detail: format!(
                                    "live cache reservation counter belongs to another generation: node_id={}",
                                    node_id
                                ),
                            },
                        ),
                    );
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let counter = Arc::new(NodeCacheReservedCapacity::new(generation.clone()));
                entry.insert(counter.clone());
                counter
            }
        };

        self.adjust_node_cache_reserved_capacity_identity(
            node_id.as_ref(),
            generation,
            &reserved_capacity,
            reason,
            bytes as i64,
        )?;
        Ok(Some(Arc::new(NodeCacheCapacityReservation {
            view: self.inner().view().clone(),
            node_id: node_id.to_string(),
            generation: generation.clone(),
            reserved_capacity,
            reason,
            bytes,
            released: AtomicBool::new(false),
        })))
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
                .filter(|reserved| !reserved.generation.is_tomb())
                .map(|reserved| reserved.total_reserved_bytes())
                .unwrap_or(0);
            // Every live node's ring-B controller is bounded, independent of
            // its placement role.
            let effective_capacity_bytes = cache
                .max_capacity()
                .expect("ring-B controller must always be bounded");
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
                    .and_then(|cache| cache.max_capacity())
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
                source_evict_rpc_requests: reclaim_counters
                    .source_evict_rpc_requests
                    .load(Ordering::Relaxed),
                source_evict_victims: reclaim_counters
                    .source_evict_victims
                    .load(Ordering::Relaxed),
                source_evict_requested_bytes: reclaim_counters
                    .source_evict_requested_bytes
                    .load(Ordering::Relaxed),
                source_evict_accepted: reclaim_counters
                    .source_evict_accepted
                    .load(Ordering::Relaxed),
                source_evict_in_progress: reclaim_counters
                    .source_evict_in_progress
                    .load(Ordering::Relaxed),
                source_evict_completed: reclaim_counters
                    .source_evict_completed
                    .load(Ordering::Relaxed),
                source_evict_retryable_busy: reclaim_counters
                    .source_evict_retryable_busy
                    .load(Ordering::Relaxed),
                source_evict_stale: reclaim_counters.source_evict_stale.load(Ordering::Relaxed),
                source_evict_rejected: reclaim_counters
                    .source_evict_rejected
                    .load(Ordering::Relaxed),
                last_route_removed_members: reclaim_counters
                    .last_route_removed_members
                    .load(Ordering::Relaxed),
                last_route_removed_bytes: reclaim_counters
                    .last_route_removed_bytes
                    .load(Ordering::Relaxed),
                capacity_eviction_non_ring_b_entry_total: reclaim_counters
                    .capacity_eviction_non_ring_b_entry_total
                    .load(Ordering::Relaxed),
                capacity_eviction_hit_committed_slot: reclaim_counters
                    .capacity_eviction_hit_committed_slot
                    .load(Ordering::Relaxed),
                eviction_reclaim_deduplicated: reclaim_counters
                    .eviction_reclaim_deduplicated
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
                        let activity = view_task
                            .master_kv_router()
                            .inner()
                            .key_activity
                            .observe_snapshot();
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
                        tracing::info!(
                            active_keys = activity.active_keys,
                            put_keys = activity.put_keys,
                            get_keys = activity.get_keys,
                            replica_keys = activity.replica_keys,
                            reclaim_keys = activity.reclaim_keys,
                            inflight_puts = activity.inflight_puts,
                            inflight_gets = activity.inflight_gets,
                            inflight_replicas = activity.inflight_replicas,
                            "master key activity runtime"
                        );
                        for node in snapshot.replica_cache_nodes {
                            tracing::info!(
                                "replica cache runtime: owner={} entries={} weighted_bytes={} effective_capacity_bytes={} base_capacity_bytes={} reserved_capacity_bytes={} pending_eviction_reclaim_bytes={} writeback_tier1_entries={} writeback_tier1_weighted_bytes={} writeback_tier1_capacity_bytes={} writeback_tier1_triggered={} writeback_tier1_owner_accepted={} writeback_tier1_failed={} reclaim_master_activity_deferred={} reclaim_owner_holder_deferred={} reclaim_owner_other_deferred={} reclaim_route_changed={} reclaim_retry_queued={} reclaim_retry_completed={} reclaim_retry_restored={} reclaim_completed={} source_evict_rpc_requests={} source_evict_victims={} source_evict_requested_bytes={} source_evict_accepted={} source_evict_in_progress={} source_evict_completed={} source_evict_retryable_busy={} source_evict_stale={} source_evict_rejected={} last_route_removed_members={} last_route_removed_bytes={} capacity_eviction_non_ring_b_entry_total={} capacity_eviction_hit_committed_slot={} eviction_reclaim_deduplicated={}",
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
                                node.reclaim_owner_holder_deferred,
                                node.reclaim_owner_other_deferred,
                                node.reclaim_route_changed,
                                node.reclaim_retry_queued,
                                node.reclaim_retry_completed,
                                node.reclaim_retry_restored,
                                node.reclaim_completed,
                                node.source_evict_rpc_requests,
                                node.source_evict_victims,
                                node.source_evict_requested_bytes,
                                node.source_evict_accepted,
                                node.source_evict_in_progress,
                                node.source_evict_completed,
                                node.source_evict_retryable_busy,
                                node.source_evict_stale,
                                node.source_evict_rejected,
                                node.last_route_removed_members,
                                node.last_route_removed_bytes,
                                node.capacity_eviction_non_ring_b_entry_total,
                                node.capacity_eviction_hit_committed_slot,
                                node.eviction_reclaim_deduplicated,
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
