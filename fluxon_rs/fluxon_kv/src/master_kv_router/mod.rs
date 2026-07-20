pub mod delete;
mod get;
pub mod msg_pack;
pub mod placement;
pub mod put;

mod count_prefix_index;

use self::{
    count_prefix_index::PrefixRadixTree,
    delete::handle_delete,
    delete::handle_delete_ack,
    delete::{handle_batch_delete_ack, handle_batch_ssd_replica_evict},
    get::{
        handle_get_done, handle_get_meta, handle_get_revoke, handle_get_start,
        handle_ssd_stage_begin,
    },
    msg_pack::{
        BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, BatchSsdReplicaEvictReq,
        CountPrefixReq, CountPrefixResp, DeleteAckReq, DeleteReq, GetAllocationMode, GetDoneReq,
        GetMetaReq, GetRevokeReq, GetSourceKind, GetStartReq, PutDoneReq, PutRevokeReq,
        PutStartReq, SsdReplicaCommitReq, SsdStageBeginReq,
    },
    placement::{PlacementDefault, PlacementPolicy},
    put::{handle_put_done, handle_put_revoke, handle_put_start, handle_ssd_replica_commit},
};
use crate::ClientKvApiAccessTrait;
use crate::client_kv_api::ClientKvApi;
use crate::client_kv_api::msg_pack::SsdReplicaPersistReq;
use crate::cluster_manager::{
    ClusterEvent, ClusterManager, ClusterManagerAccessTrait, NodeID, NodeIDString,
};
use crate::config::TestSpecConfig;
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
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, OK};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_util::map_lock::AMapLock;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::{DashMap, mapref::entry::Entry};
use limit_thirdparty::tokio::sync::ARwLock;
use limit_thirdparty::tokio::{self, sync::ampsc};
use moka::notification::RemovalCause;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

// Cache capacity policy: fraction of a node's space reserved for the KV cache.
// Keep as a single source of truth to avoid magic numbers scattered across methods.
const MOKA_CACHE_CAPACITY_RATIO: f32 = 0.8;
const MAX_GET_DURABLE_REPLICA_SLOTS: u32 = 2;
const PLACEMENT_REPORT_INTERVAL_SECS: u64 = 10;
const INFLIGHT_PUT_TTL_SECONDS: u64 = 60;
const INFLIGHT_PUT_TTL_SECONDS_SKIP_PUT_END_COMMIT: u64 = 5;
const GET_COMPLETION_REPLAY_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Copy, Debug)]
pub enum PutPlacementMode {
    Local,
    Remote,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RequesterTargetPair {
    requester_node_id: NodeIDString,
    target_node_id: NodeIDString,
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

/// Information about a `put` operation that is currently in progress.
pub enum InflightPutAllocation {
    /// Local fast path: the same allocation is used as both src (staging) and target (final).
    Local(Allocation),
    /// Remote path: separate allocations for src (on requester) and target (on selected node).
    Remote { src: Allocation, target: Allocation },
}

/// Information about a `put` operation that is currently in progress.
#[derive(Clone)]
pub struct InflightPutInfo {
    pub node_id: NodeID,
    // seg_name: String,
    pub key: String,
    pub req_node_id: NodeID,
    pub len: u64,
    pub persist_to_ssd: bool,
    pub src_target_allocation: Arc<Mutex<Option<InflightPutAllocation>>>,
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
    pub source_allocation: Option<Arc<Allocation>>,
    pub route: Arc<OneKvNodesRoutes>,
    pub allocation_mode: GetAllocationMode,
    pub source_kind: GetSourceKind,
    pub(crate) ssd_stage_lifecycle: Option<Arc<Mutex<SsdStageLifecycle>>>,
    pub(crate) cache_capacity_reservation: Option<Arc<NodeCacheCapacityReservation>>,
}

#[derive(Default)]
struct InflightGetMemberIndex {
    by_member: DashMap<NodeIDString, HashSet<u64>>,
}

impl InflightGetMemberIndex {
    fn insert(&self, member_id: &str, get_id: u64) {
        self.by_member
            .entry(member_id.to_string())
            .or_default()
            .insert(get_id);
    }

    fn remove(&self, member_id: &str, get_id: u64) {
        let Entry::Occupied(mut entry) = self.by_member.entry(member_id.to_string()) else {
            return;
        };
        entry.get_mut().remove(&get_id);
        if entry.get().is_empty() {
            entry.remove();
        }
    }

    fn take(&self, member_id: &str) -> HashSet<u64> {
        self.by_member
            .remove(member_id)
            .map(|(_, get_ids)| get_ids)
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SsdStagePhase {
    NotStarted,
    Active,
    Quiescent,
}

#[derive(Debug)]
pub(crate) struct SsdStageLifecycle {
    pub(crate) phase: SsdStagePhase,
    pub(crate) revoke_requested: bool,
    pub(crate) drop_ssd_source: bool,
}

impl SsdStageLifecycle {
    pub(crate) fn new() -> Self {
        Self {
            phase: SsdStagePhase::NotStarted,
            revoke_requested: false,
            drop_ssd_source: false,
        }
    }

    pub(crate) fn begin(&mut self) -> bool {
        match self.phase {
            SsdStagePhase::NotStarted => {
                self.phase = SsdStagePhase::Active;
                true
            }
            SsdStagePhase::Active => true,
            SsdStagePhase::Quiescent => false,
        }
    }

    pub(crate) fn request_revoke(&mut self, drop_ssd_source: bool) -> bool {
        if self.phase != SsdStagePhase::Active {
            return false;
        }
        self.revoke_requested = true;
        self.drop_ssd_source |= drop_ssd_source;
        true
    }

    pub(crate) fn finish_done_from_source(&mut self) -> Option<(bool, bool)> {
        if self.phase != SsdStagePhase::Active {
            return None;
        }
        self.phase = SsdStagePhase::Quiescent;
        Some((self.revoke_requested, self.drop_ssd_source))
    }

    pub(crate) fn finish_revoke_from_source(&mut self, drop_ssd_source: bool) -> Option<bool> {
        if self.phase == SsdStagePhase::Quiescent {
            return None;
        }
        self.phase = SsdStagePhase::Quiescent;
        self.drop_ssd_source |= drop_ssd_source;
        Some(self.drop_ssd_source)
    }
}

#[derive(Clone)]
pub(crate) struct CompletedGetInfo {
    pub(crate) requester_node_id: NodeID,
    pub(crate) committer_node_id: NodeID,
    pub(crate) holder_key: NodeHolderKey,
    pub(crate) response: msg_pack::GetDoneResp,
}

impl CompletedGetInfo {
    pub(crate) fn replay_for(&self, caller_node_id: &NodeID) -> Option<msg_pack::GetDoneResp> {
        (&self.requester_node_id == caller_node_id || &self.committer_node_id == caller_node_id)
            .then(|| self.response.clone())
    }
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
}

#[derive(Clone)]
pub struct NodeValueReplicaDesc {
    pub weight_bytes: u32,
    pub put_id: PutIDForAKey,
}

type NodeReplicaCache = moka::sync::SegmentedCache<String, NodeValueReplicaDesc>;

pub(crate) struct NodeCacheCapacityReservation {
    node_id: NodeIDString,
    bytes: u64,
    base_capacity: u64,
    reserved_bytes: Arc<Mutex<u64>>,
    cache: Arc<NodeReplicaCache>,
}

impl NodeCacheCapacityReservation {
    fn reserve(
        node_id: NodeIDString,
        bytes: u64,
        base_capacity: u64,
        reserved_bytes: Arc<Mutex<u64>>,
        cache: Arc<NodeReplicaCache>,
    ) -> Result<Self, String> {
        let mut reserved = reserved_bytes.lock();
        let new_reserved = reserved.checked_add(bytes).ok_or_else(|| {
            format!(
                "cache capacity reservation overflow: node_id={} reserved={} bytes={}",
                node_id, *reserved, bytes
            )
        })?;
        let new_capacity = base_capacity.saturating_sub(new_reserved);
        cache.set_max_capacity(new_capacity).map_err(|err| {
            format!(
                "moka.set_max_capacity failed while reserving: node_id={} new_capacity={} err={}",
                node_id, new_capacity, err
            )
        })?;
        *reserved = new_reserved;
        drop(reserved);

        Ok(Self {
            node_id,
            bytes,
            base_capacity,
            reserved_bytes,
            cache,
        })
    }
}

impl Drop for NodeCacheCapacityReservation {
    fn drop(&mut self) {
        let mut reserved = self.reserved_bytes.lock();
        let new_reserved = reserved.checked_sub(self.bytes).unwrap_or_else(|| {
            panic!(
                "cache capacity reservation underflow: node_id={} reserved={} bytes={}",
                self.node_id, *reserved, self.bytes
            )
        });
        let new_capacity = self.base_capacity.saturating_sub(new_reserved);
        self.cache
            .set_max_capacity(new_capacity)
            .unwrap_or_else(|err| {
                panic!(
                    "moka.set_max_capacity failed while releasing reservation: node_id={} new_capacity={} err={}",
                    self.node_id, new_capacity, err
                )
            });
        *reserved = new_reserved;
    }
}

#[derive(Clone, Debug)]
pub struct KvSsdReplicaInfo {
    pub len: u64,
}

#[derive(Clone, Debug)]
pub struct KvNodeReplicas {
    pub tomb_tag: NodeTombTag,
    pub memory: Option<Arc<Allocation>>,
    pub ssd: Option<KvSsdReplicaInfo>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SsdReplicaCommitStatus {
    Committed,
    MissingMemory,
    TombedNode,
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

    /// Per-node memory and SSD replicas for this key-version.
    pub node_replicas: RwLock<HashMap<NodeID, KvNodeReplicas>>,
    pub get_durable_slots_used: AtomicU32,
}

impl OneKvNodesRoutes {
    fn new(put_id: PutIDForAKey, lease_id: Option<u64>) -> Self {
        Self {
            put_id,
            lease_id,
            node_replicas: RwLock::new(HashMap::new()),
            get_durable_slots_used: AtomicU32::new(0),
        }
    }

    fn remove_tombed_node_replicas(
        &self,
        verify_put_id: PutIDForAKey,
        tombs: HashSet<NodeID>,
    ) -> bool {
        if self.put_id != verify_put_id {
            return false;
        }

        self.node_replicas
            .write()
            .retain(|node_id, replicas| !tombs.contains(node_id) || !replicas.tomb_tag.is_tomb());

        true
    }

    fn has_live_replica(&self) -> bool {
        self.node_replicas.read().values().any(|replicas| {
            !replicas.tomb_tag.is_tomb() && (replicas.memory.is_some() || replicas.ssd.is_some())
        })
    }

    fn has_memory_replica(&self, node_id: &NodeID) -> bool {
        self.node_replicas
            .read()
            .get(node_id)
            .is_some_and(|replicas| !replicas.tomb_tag.is_tomb() && replicas.memory.is_some())
    }

    fn insert_memory_replica(
        &self,
        node_id: NodeID,
        allocation: Arc<Allocation>,
        tomb_tag: NodeTombTag,
    ) {
        let mut node_replicas = self.node_replicas.write();
        if let Some(replicas) = node_replicas.get_mut(&node_id) {
            if !replicas.tomb_tag.is_tomb() {
                replicas.memory = Some(allocation);
                return;
            }
        }

        node_replicas.insert(
            node_id,
            KvNodeReplicas {
                tomb_tag,
                memory: Some(allocation),
                ssd: None,
            },
        );
    }

    fn commit_ssd_replica(&self, node_id: &NodeID, len: u64) -> SsdReplicaCommitStatus {
        let mut node_replicas = self.node_replicas.write();
        let Some(replicas) = node_replicas.get_mut(node_id) else {
            return SsdReplicaCommitStatus::MissingMemory;
        };
        if replicas.memory.is_none() {
            return SsdReplicaCommitStatus::MissingMemory;
        }
        if replicas.tomb_tag.is_tomb() {
            return SsdReplicaCommitStatus::TombedNode;
        }

        replicas.ssd = Some(KvSsdReplicaInfo { len });
        SsdReplicaCommitStatus::Committed
    }

    fn remove_memory_replica(&self, node_id: &NodeID) -> bool {
        let mut node_replicas = self.node_replicas.write();
        let Some(replicas) = node_replicas.get_mut(node_id) else {
            return false;
        };
        let removed = replicas.memory.take().is_some();
        let remove_node = replicas.ssd.is_none();
        if remove_node {
            node_replicas.remove(node_id);
        }
        removed
    }

    fn remove_ssd_replica(&self, node_id: &NodeID) -> bool {
        let mut node_replicas = self.node_replicas.write();
        let Some(replicas) = node_replicas.get_mut(node_id) else {
            return false;
        };
        let removed = replicas.ssd.take().is_some();
        let remove_node = replicas.memory.is_none();
        if remove_node {
            node_replicas.remove(node_id);
        }
        removed
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_manager::ClusterMember;
    use crate::master_seg_manager::msg_pack::SegmentDeviceDescription;
    use crate::master_seg_manager::one_seg_allocator::OneSegAllocator;
    use std::collections::HashMap;

    #[test]
    fn one_kv_nodes_routes_only_reserves_two_get_durable_slots() {
        let routes = OneKvNodesRoutes::new((1, 0), None);

        assert!(routes.try_reserve_get_durable_slot());
        assert!(routes.try_reserve_get_durable_slot());
        assert!(!routes.try_reserve_get_durable_slot());

        routes.release_get_durable_slot();
        assert!(routes.try_reserve_get_durable_slot());
    }

    #[test]
    fn remote_ssd_stage_defers_revoke_until_quiescent() {
        let mut lifecycle = SsdStageLifecycle::new();
        assert!(!lifecycle.request_revoke(true));
        assert!(lifecycle.begin());
        assert!(lifecycle.begin());
        assert!(lifecycle.request_revoke(false));
        assert!(lifecycle.request_revoke(true));
        assert_eq!(lifecycle.finish_done_from_source(), Some((true, true)));
        assert_eq!(lifecycle.finish_done_from_source(), None);
        assert!(!lifecycle.begin());
    }

    #[test]
    fn remote_ssd_source_revoke_can_finish_before_or_after_begin() {
        let mut before_begin = SsdStageLifecycle::new();
        assert_eq!(before_begin.finish_revoke_from_source(true), Some(true));
        assert_eq!(before_begin.finish_revoke_from_source(false), None);

        let mut after_begin = SsdStageLifecycle::new();
        assert!(after_begin.begin());
        assert!(after_begin.request_revoke(true));
        assert_eq!(after_begin.finish_revoke_from_source(false), Some(true));
    }

    #[test]
    fn completed_get_replays_the_same_holder_for_duplicate_done() {
        let requester: NodeID = "requester-a".to_string().into();
        let committer: NodeID = "source-a".to_string().into();
        let holder_key = NodeHolderKey::new(requester.to_string(), 73);
        let completed = CompletedGetInfo {
            requester_node_id: requester.clone(),
            committer_node_id: committer.clone(),
            holder_key,
            response: msg_pack::GetDoneResp {
                holder_id: 73,
                allocation_mode: GetAllocationMode::Temporary,
                error_code: OK,
                error_json: String::new(),
                server_process_us: 0,
            },
        };
        let cache = moka::sync::Cache::builder()
            .time_to_live(GET_COMPLETION_REPLAY_TTL)
            .build();
        cache.insert(9, completed);

        let first = cache.get(&9).unwrap().replay_for(&requester).unwrap();
        let second = cache.get(&9).unwrap().replay_for(&requester).unwrap();
        assert_eq!(first.holder_id, 73);
        assert_eq!(second.holder_id, first.holder_id);
        assert_eq!(
            cache
                .get(&9)
                .unwrap()
                .replay_for(&committer)
                .unwrap()
                .holder_id,
            73
        );
        let other: NodeID = "requester-b".to_string().into();
        assert!(cache.get(&9).unwrap().replay_for(&other).is_none());
    }

    #[test]
    fn inflight_get_member_index_takes_only_the_left_member() {
        let index = InflightGetMemberIndex::default();
        index.insert("requester-a", 7);
        index.insert("source-b", 7);
        index.insert("requester-a", 9);
        index.insert("source-c", 11);
        index.remove("source-c", 11);

        assert_eq!(index.take("requester-a"), HashSet::from([7_u64, 9_u64]));
        assert!(index.take("requester-a").is_empty());
        assert_eq!(index.take("source-b"), HashSet::from([7_u64]));
        assert!(index.take("source-c").is_empty());
    }

    #[test]
    fn owner_holding_member_index_rejects_departed_members_without_scanning() {
        let allocator = Arc::new(
            OneSegAllocator::new(
                "holding-index-test".to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                4096,
            )
            .expect("test allocator must be created"),
        );
        let allocation = Arc::new(
            allocator
                .allocate(512)
                .expect("test allocation must be created"),
        );
        let holding = |key: &str, node_id: &str| OwnerHoldingGetInfo {
            key: key.to_string(),
            holding_node_id: NodeID::from(node_id.to_string()),
            len: 512,
            allocation: Arc::clone(&allocation),
        };
        let manager = MasterOwnerMemMgr::default();
        let a1 = NodeHolderKey::new("node-a".to_string(), 1);
        let a2 = NodeHolderKey::new("node-a".to_string(), 2);
        let b1 = NodeHolderKey::new("node-b".to_string(), 3);

        assert!(manager.insert_if_member_active(a1.clone(), holding("a1", "node-a")));
        assert!(manager.insert_if_member_active(a2.clone(), holding("a2", "node-a")));
        assert!(manager.insert_if_member_active(b1.clone(), holding("b1", "node-b")));
        assert_eq!(manager.total(), 3);

        assert_eq!(manager.mark_member_left_and_cleanup("node-a"), 2);
        assert_eq!(manager.total(), 1);
        assert!(!manager.insert_if_member_active(a1.clone(), holding("late", "node-a")));
        assert!(manager.inner().contains_key(&b1));

        manager.mark_member_active("node-a");
        assert!(manager.insert_if_member_active(a1.clone(), holding("joined", "node-a")));
        assert_eq!(manager.mark_member_left_and_cleanup("node-a"), 1);
        assert!(manager.remove(&b1).is_some());
        assert_eq!(manager.total(), 0);
    }

    #[test]
    fn one_kv_nodes_routes_updates_memory_and_ssd_independently() {
        let routes = OneKvNodesRoutes::new((1, 0), None);
        let node_id: NodeID = "node-a".to_string().into();
        let allocator = Arc::new(
            OneSegAllocator::new(
                "route-test".to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                4096,
            )
            .expect("test allocator must be created"),
        );
        let allocation = Arc::new(
            allocator
                .allocate(512)
                .expect("test allocation must be created"),
        );

        assert_eq!(
            routes.commit_ssd_replica(&node_id, 512),
            SsdReplicaCommitStatus::MissingMemory
        );
        routes.insert_memory_replica(node_id.clone(), allocation, NodeTombTag::new());
        assert_eq!(
            routes.commit_ssd_replica(&node_id, 512),
            SsdReplicaCommitStatus::Committed
        );

        assert!(routes.remove_memory_replica(&node_id));
        {
            let node_replicas = routes.node_replicas.read();
            let replicas = node_replicas
                .get(&node_id)
                .expect("SSD replica must keep the node entry alive");
            assert!(replicas.memory.is_none());
            assert_eq!(replicas.ssd.as_ref().map(|ssd| ssd.len), Some(512));
        }

        assert!(routes.remove_ssd_replica(&node_id));
        assert!(routes.node_replicas.read().is_empty());

        let old_tomb_tag = NodeTombTag::new();
        let old_allocation = Arc::new(
            allocator
                .allocate(512)
                .expect("old-incarnation test allocation must be created"),
        );
        routes.insert_memory_replica(node_id.clone(), old_allocation, old_tomb_tag.clone());
        assert_eq!(
            routes.commit_ssd_replica(&node_id, 512),
            SsdReplicaCommitStatus::Committed
        );
        old_tomb_tag.set_tomb();

        let new_allocation = Arc::new(
            allocator
                .allocate(512)
                .expect("new-incarnation test allocation must be created"),
        );
        routes.insert_memory_replica(node_id.clone(), new_allocation, NodeTombTag::new());
        assert!(
            routes.remove_tombed_node_replicas(routes.put_id, HashSet::from([node_id.clone()]),)
        );
        assert!(routes.has_memory_replica(&node_id));
        assert!(
            routes
                .node_replicas
                .read()
                .get(&node_id)
                .expect("new node incarnation must remain indexed")
                .ssd
                .is_none()
        );
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
}

pub struct MasterKvRouterInner {
    view: std::sync::OnceLock<MasterKvRouterView>,
    pub policy: Box<dyn PlacementPolicy>,
    test_spec_config: TestSpecConfig,

    /// (key, put_time_ms, put_version) -> inflight_put_info
    pub inflight_puts: moka::future::Cache<(String, u64, u32), InflightPutInfo>,
    /// key -> inflight put admission state
    pub(crate) inflight_put_key_counts: Arc<DashMap<String, InflightPutKeyAdmission>>,
    inflight_gets: DashMap<u64, InflightGetInfo>,
    /// Requester and remote SSD source member -> their active GET ids.
    inflight_gets_by_member: InflightGetMemberIndex,
    pub(crate) get_transition_locks: AMapLock<u64>,
    pub(crate) completed_gets: moka::sync::Cache<u64, CompletedGetInfo>,

    /// Cache for holding get operations (owned, flattened by (node_id, holder_id))
    pub get_holding: MasterOwnerMemMgr,

    /// Counter for get_id
    pub next_get_id: AtomicU64,

    /// Counter for holder_id
    pub next_holder_id: AtomicU64,

    /// Latest version of key-value replicas
    pub kv_routes: DashMap<String, Arc<OneKvNodesRoutes>>,

    /// Prefix-counting index derived from `kv_routes`.
    ///
    /// It is updated through async follow-up maintenance, so it does not guarantee
    /// immediate strong-consistency visibility for a freshly committed put.
    /// The generic surface is `CountPrefix` RPC; the current primary use case is
    /// MQ capacity backpressure / prefix counting.
    pub prefix_index: ARwLock<PrefixRadixTree>,

    /// Support replicas: node_id -> key -> route_info
    pub node_kv_cache_controller: DashMap<NodeIDString, Arc<NodeReplicaCache>>,

    /// Per-node bytes held outside Moka while still consuming allocator space.
    pub cache_reserved_bytes: DashMap<NodeIDString, Arc<Mutex<u64>>>,

    /// Historical final put placement decisions by target node.
    pub put_target_decision_counts: DashMap<NodeIDString, Arc<AtomicU64>>,

    /// Historical final put placement decisions by requester->target pair.
    pub put_requester_target_decision_counts: DashMap<RequesterTargetPair, Arc<AtomicU64>>,

    /// Historical final put placement decisions grouped by placement mode.
    pub put_placement_mode_counts: DashMap<&'static str, Arc<AtomicU64>>,

    /// Support replicas: key -> version_id
    recent_key_versionid_allocator: moka::sync::SegmentedCache<String, Arc<AtomicU32>>,

    pub delete_broadcast: EnsureMemholderMgmtDeleteHandle<DeleteKeyInfo>,
}

#[derive(Debug)]
pub(crate) struct InflightPutKeyAdmission {
    inflight_count: u32,
    create_only: bool,
}

impl MasterKvRouterInner {
    fn view(&self) -> &MasterKvRouterView {
        self.view.get().unwrap()
    }

    pub(crate) fn insert_inflight_get(&self, get_id: u64, info: InflightGetInfo) {
        let Entry::Vacant(entry) = self.inflight_gets.entry(get_id) else {
            panic!("duplicate inflight get_id={get_id}");
        };
        self.inflight_gets_by_member
            .insert(info.req_node_id.as_ref(), get_id);
        if info.ssd_stage_lifecycle.is_some() {
            self.inflight_gets_by_member
                .insert(info.src_node_id.as_ref(), get_id);
        }
        entry.insert(info);
    }

    pub(crate) fn get_inflight_get(&self, get_id: u64) -> Option<InflightGetInfo> {
        self.inflight_gets
            .get(&get_id)
            .map(|inflight| inflight.value().clone())
    }

    pub(crate) fn remove_inflight_get(&self, get_id: u64) -> Option<InflightGetInfo> {
        let (_, info) = self.inflight_gets.remove(&get_id)?;
        self.inflight_gets_by_member
            .remove(info.req_node_id.as_ref(), get_id);
        if info.ssd_stage_lifecycle.is_some() {
            self.inflight_gets_by_member
                .remove(info.src_node_id.as_ref(), get_id);
        }
        Some(info)
    }

    fn take_member_inflight_get_ids(&self, member_id: &str) -> HashSet<u64> {
        self.inflight_gets_by_member.take(member_id)
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
        let policy_impl: Box<dyn PlacementPolicy> = Box::new(PlacementDefault::new());
        let inflight_put_ttl_seconds = if arg.test_spec_config.skip_put_end_commit {
            INFLIGHT_PUT_TTL_SECONDS_SKIP_PUT_END_COMMIT
        } else {
            INFLIGHT_PUT_TTL_SECONDS
        };
        let inflight_put_key_counts: Arc<DashMap<String, InflightPutKeyAdmission>> =
            Arc::new(DashMap::new());
        let inflight_put_key_counts_for_listener = inflight_put_key_counts.clone();
        let inflight_puts = moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(inflight_put_ttl_seconds))
            .eviction_listener(move |_put_id, inflight_info: InflightPutInfo, cause| {
                if cause == RemovalCause::Expired {
                    MasterKvRouter::release_inflight_put_key_count_map(
                        &inflight_put_key_counts_for_listener,
                        &inflight_info.key,
                    );
                }
            })
            .build();
        // In-flight GET allocations contain raw transfer addresses. They must only be
        // released by GetDone/GetRevoke or member-left cleanup after all users are quiescent;
        // a wall-clock TTL cannot prove that a remote DMA or SSD stage has stopped.
        let inflight_gets = DashMap::new();
        let inner = MasterKvRouterInner {
            view: std::sync::OnceLock::new(),
            policy: policy_impl,
            test_spec_config: arg.test_spec_config,
            inflight_puts,
            inflight_put_key_counts,
            inflight_gets,
            inflight_gets_by_member: InflightGetMemberIndex::default(),
            get_transition_locks: AMapLock::new(GET_COMPLETION_REPLAY_TTL),
            completed_gets: moka::sync::Cache::builder()
                .time_to_live(GET_COMPLETION_REPLAY_TTL)
                .build(),
            get_holding: MasterOwnerMemMgr::default(),
            // Zero is reserved by GetStart failure paths and response-send cleanup.
            next_get_id: AtomicU64::new(1),
            next_holder_id: AtomicU64::new(0),
            kv_routes: DashMap::new(),
            prefix_index: ARwLock::new(PrefixRadixTree::new()),
            node_kv_cache_controller: DashMap::new(),
            cache_reserved_bytes: DashMap::new(),
            put_target_decision_counts: DashMap::new(),
            put_requester_target_decision_counts: DashMap::new(),
            put_placement_mode_counts: DashMap::new(),
            recent_key_versionid_allocator: moka::sync::SegmentedCache::builder(8)
                .time_to_idle(Duration::from_secs(5))
                .build(),
            delete_broadcast: EnsureMemholderMgmtDeleteHandle::new(
                MasterOwnerMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
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

        let delete_broadcast_rx = self
            .0
            .delete_broadcast
            .take_rx()
            .expect("delete_broadcast rx already taken, that's impossible");
        delete::spawn_delete_broadcast(view, delete_broadcast_rx);
        Ok(())
    }

    pub(crate) fn view(&self) -> &MasterKvRouterView {
        self.inner().view()
    }

    pub fn inner(&self) -> &MasterKvRouterInner {
        &self.0
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

    fn release_inflight_put_key_count_map(
        counts: &DashMap<String, InflightPutKeyAdmission>,
        key: &str,
    ) {
        if let Entry::Occupied(mut entry) = counts.entry(key.to_string()) {
            let admission = entry.get_mut();
            assert!(
                admission.inflight_count > 0,
                "inflight put admission count must remain positive"
            );
            if admission.inflight_count == 1 {
                entry.remove();
            } else {
                assert!(
                    !admission.create_only,
                    "create-only put admission must remain exclusive"
                );
                admission.inflight_count -= 1;
            }
        }
    }

    pub fn reserve_inflight_put_key(
        &self,
        key: &str,
        reject_if_inflight_same_key: bool,
        reject_if_exists: bool,
    ) -> Result<(), KvError> {
        let counts = &self.inner().inflight_put_key_counts;
        match counts.entry(key.to_string()) {
            Entry::Occupied(mut entry) => {
                if reject_if_exists {
                    return Err(KvError::Api(
                        crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyAlreadyExists {
                            key: key.to_string(),
                        },
                    ));
                }
                if reject_if_inflight_same_key || entry.get().create_only {
                    return Err(KvError::Api(
                        crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyBeingWritten {
                            key: key.to_string(),
                        },
                    ));
                }
                let admission = entry.get_mut();
                admission.inflight_count = admission
                    .inflight_count
                    .checked_add(1)
                    .expect("inflight put admission count overflow");
            }
            Entry::Vacant(entry) => {
                if reject_if_exists && self.inner().kv_routes.contains_key(key) {
                    return Err(KvError::Api(
                        crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyAlreadyExists {
                            key: key.to_string(),
                        },
                    ));
                }
                entry.insert(InflightPutKeyAdmission {
                    inflight_count: 1,
                    create_only: reject_if_exists,
                });
            }
        }
        Ok(())
    }

    pub fn release_inflight_put_key(&self, key: &str) {
        Self::release_inflight_put_key_count_map(&self.inner().inflight_put_key_counts, key);
    }

    fn register_rpc_callers(&self) {
        RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new().regist(self.0.view().p2p_module());
        RPCCaller::<SsdReplicaPersistReq>::new().regist(self.0.view().p2p_module());
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
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetStartResp: {:?}", e);
                    if get_id != 0 {
                        let transition_lock = cleanup_view
                            .master_kv_router()
                            .inner()
                            .get_transition_locks
                            .get_lock(get_id);
                        let _transition = transition_lock.lock().await;
                        let defer_release = cleanup_view
                            .master_kv_router()
                            .inner()
                            .get_inflight_get(get_id)
                            .and_then(|inflight| inflight.ssd_stage_lifecycle.clone())
                            .is_some_and(|lifecycle| lifecycle.lock().request_revoke(false));
                        if !defer_release {
                            if let Some(inflight_info) = cleanup_view
                                .master_kv_router()
                                .inner()
                                .remove_inflight_get(get_id)
                            {
                                inflight_info.release_durable_slot_if_needed();
                            }
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
            let requester_node_id = resp.node_id();
            let _ = view.spawn("rpc_get_revoke", async move {
                let ack = handle_get_revoke(view_task, msg, requester_node_id).await;
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
            let requester_node_id = resp.node_id();
            let _ = view.spawn("rpc_get_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_get_done(view_task, msg, requester_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<SsdStageBeginReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view_task = view.clone();
            let source_node_id = resp.node_id();
            let _ = view.spawn("rpc_ssd_stage_begin", async move {
                let ack = handle_ssd_stage_begin(view_task, msg, source_node_id).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send SsdStageBeginResp: {:?}", e);
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
                        view_task
                            .master_kv_router()
                            .inner()
                            .inflight_puts
                            .remove(&(key, put_id.0, put_id.1))
                            .await;
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
            let _ = view.spawn("rpc_put_done", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_put_done(view_task, msg).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send PutDoneResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<SsdReplicaCommitReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_ssd_replica_commit", async move {
                let t0 = Utc::now().timestamp_micros();
                let mut ack = handle_ssd_replica_commit(view_task, msg, req_node_id).await;
                ack.serialize_part.server_process_us = Utc::now().timestamp_micros() - t0;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send SsdReplicaCommitResp: {:?}", e);
                }
            });
            Ok(())
        });

        let view = self.0.view().clone();
        RPCHandler::<BatchSsdReplicaEvictReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let req_node_id = resp.node_id().clone();
            let view_task = view.clone();
            let _ = view.spawn("rpc_batch_ssd_replica_evict", async move {
                let ack = handle_batch_ssd_replica_evict(view_task, msg, req_node_id).await;
                if let Err(err) = resp.send_resp(ack).await {
                    error!("Failed to send BatchSsdReplicaEvictResp: {:?}", err);
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
            let _ = view.spawn("rpc_delete_ack", async move {
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
            let _ = view.spawn("rpc_batch_delete_ack", async move {
                let ack = handle_batch_delete_ack(view_task, msg).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send BatchDeleteAckResp: {:?}", e);
                }
            });
            Ok(())
        });

        // --- GetMeta Handler ---
        let view = self.0.view().clone();
        RPCHandler::<GetMetaReq>::new().regist(p2p, move |resp, msg| {
            let view = view.clone();
            let view2 = view.clone();
            let _ = view.spawn("rpc_get_meta", async move {
                let ack = handle_get_meta(view2, msg, resp.node_id().clone()).await;
                if let Err(e) = resp.send_resp(ack).await {
                    error!("Failed to send GetMetaResp: {:?}", e);
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
        let _ = view.spawn("node_segment_registration_caller", async move {
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
                _view: &MasterKvRouterView,
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
                            .inner()
                            .get_holding
                            .mark_member_active(&member.id);
                    }
                    ClusterEvent::MemberLeft(node_id) => {
                        let left_node: NodeID = node_id.clone().into();
                        let inflight_ids = view
                            .master_kv_router()
                            .inner()
                            .take_member_inflight_get_ids(node_id);
                        for get_id in inflight_ids {
                            let transition_lock = view
                                .master_kv_router()
                                .inner()
                                .get_transition_locks
                                .get_lock(get_id);
                            let _transition = transition_lock.lock().await;
                            let Some(inflight) = view
                                .master_kv_router()
                                .inner()
                                .get_inflight_get(get_id)
                            else {
                                continue;
                            };
                            let source_left = inflight.src_node_id == left_node;
                            let defer_remote_stage = !source_left
                                && inflight.ssd_stage_lifecycle.as_ref().is_some_and(|lifecycle| {
                                    lifecycle.lock().request_revoke(false)
                                });
                            if defer_remote_stage {
                                info!(
                                    get_id,
                                    member = %node_id,
                                    "Deferred member-left GET cleanup until SSD source quiesces"
                                );
                                continue;
                            }
                            if let Some(inflight) = view
                                .master_kv_router()
                                .inner()
                                .remove_inflight_get(get_id)
                            {
                                inflight.release_durable_slot_if_needed();
                            }
                        }

                        let removed = view
                            .master_kv_router()
                            .inner()
                            .get_holding
                            .mark_member_left_and_cleanup(node_id);
                        if removed > 0 {
                            info!("Cleaned up {} holdings for left member {}", removed, node_id);
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

                        info!(
                            "put placement historical distribution | target_counts={:?} | mode_counts={:?} | requester_target_counts={:?}",
                            target_counts,
                            mode_counts,
                            requester_target_counts,
                        );
                    }
                    _ = shutdown_waiter.wait() => {
                        break;
                    }
                }
            }
        });
    }

    pub fn get_node_cache_controller(&self, node_id: &str) -> Option<Arc<NodeReplicaCache>> {
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
                            .max_capacity((node_space_size as f32 * MOKA_CACHE_CAPACITY_RATIO) as u64)
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
                                            if let Err(code) = crate::master_kv_router::delete::evict_one_kv_replica_for_node(
                                                &view,
                                                k.clone(),
                                                cache_node_id.clone().into(),
                                                evicted_put_id,
                                            ) {
                                                warn!(
                                                    "Eviction-triggered local replica cleanup failed for key {} on node {}: {:?}",
                                                    k,
                                                    cache_node_id,
                                                    code
                                                );
                                            }
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

    pub(crate) fn reserve_node_cache_capacity(
        &self,
        node_id: &str,
        bytes: u64,
    ) -> crate::rpcresp_kvresult_convert::msg_and_error::KvResult<
        Option<NodeCacheCapacityReservation>,
    > {
        if !self.replica_cache_enabled() {
            return Ok(None);
        }
        let node_space_size = self
            .inner()
            .view()
            .master_seg_manager()
            .get_node_space_size(node_id);
        if node_space_size == 0 {
            return Err(
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::OwnerNoSeg {
                        detail: format!(
                            "node_id={} has no segment while reserving cache capacity",
                            node_id
                        ),
                    },
                ),
            );
        }
        let base_capacity = (node_space_size as f32 * MOKA_CACHE_CAPACITY_RATIO) as u64;
        let cache = self.get_node_cache_controller(node_id).ok_or_else(|| {
            crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::OwnerNoSeg {
                    detail: format!("node_id={} cache_controller not found", node_id),
                },
            )
        })?;
        let reserved_bytes = self
            .inner()
            .cache_reserved_bytes
            .entry(node_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(0)))
            .value()
            .clone();

        NodeCacheCapacityReservation::reserve(
            node_id.to_string(),
            bytes,
            base_capacity,
            reserved_bytes,
            cache,
        )
        .map(Some)
        .map_err(|detail| {
            crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::OwnerNoSeg {
                    detail,
                },
            )
        })
    }
}
// moved to crate::metrics::client

#[cfg(test)]
mod cache_capacity_reservation_tests {
    use super::*;
    use crate::master_seg_manager::msg_pack::SegmentDeviceDescription;
    use crate::master_seg_manager::one_seg_allocator::OneSegAllocator;

    fn new_cache(capacity: u64) -> Arc<NodeReplicaCache> {
        Arc::new(
            moka::sync::SegmentedCache::builder(1)
                .max_capacity(capacity)
                .weigher(Box::new(|_key: &String, value: &NodeValueReplicaDesc| {
                    value.weight_bytes
                }))
                .build(),
        )
    }

    #[test]
    fn pending_persist_reservation_reduces_capacity_until_guard_drops() {
        let cache = new_cache(800);
        let reserved_bytes = Arc::new(Mutex::new(0));
        let first = NodeCacheCapacityReservation::reserve(
            "node-a".to_string(),
            120,
            800,
            reserved_bytes.clone(),
            cache.clone(),
        )
        .unwrap();
        let second = NodeCacheCapacityReservation::reserve(
            "node-a".to_string(),
            80,
            800,
            reserved_bytes.clone(),
            cache.clone(),
        )
        .unwrap();

        assert_eq!(*reserved_bytes.lock(), 200);
        assert_eq!(cache.policy().max_capacity(), Some(600));

        drop(first);
        assert_eq!(*reserved_bytes.lock(), 80);
        assert_eq!(cache.policy().max_capacity(), Some(720));

        drop(second);
        assert_eq!(*reserved_bytes.lock(), 0);
        assert_eq!(cache.policy().max_capacity(), Some(800));
    }

    #[test]
    fn inflight_get_keeps_capacity_reserved_until_handoff_drops() {
        let cache = new_cache(800);
        let reserved_bytes = Arc::new(Mutex::new(0));
        let reservation = Arc::new(
            NodeCacheCapacityReservation::reserve(
                "node-a".to_string(),
                120,
                800,
                reserved_bytes.clone(),
                cache.clone(),
            )
            .unwrap(),
        );
        let allocator = Arc::new(
            OneSegAllocator::new(
                "inflight-get-reservation-test".to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                4096,
            )
            .expect("test allocator must be created"),
        );
        let allocation = Arc::new(
            allocator
                .allocate(512)
                .expect("test allocation must be created"),
        );
        let route = Arc::new(OneKvNodesRoutes::new((1, 0), None));
        let mut inflight = InflightGetInfo {
            put_id: (1, 0),
            src_node_id: "node-b".to_string().into(),
            key: "key-a".to_string(),
            req_node_id: "node-a".to_string().into(),
            len: 512,
            allocation,
            source_allocation: None,
            route,
            allocation_mode: GetAllocationMode::DurableReplica,
            source_kind: GetSourceKind::Memory,
            ssd_stage_lifecycle: None,
            cache_capacity_reservation: Some(reservation),
        };

        assert_eq!(*reserved_bytes.lock(), 120);
        assert_eq!(cache.policy().max_capacity(), Some(680));

        let handoff = inflight.cache_capacity_reservation.take();
        drop(inflight);
        assert_eq!(*reserved_bytes.lock(), 120);

        drop(handoff);
        assert_eq!(*reserved_bytes.lock(), 0);
        assert_eq!(cache.policy().max_capacity(), Some(800));
    }

    #[test]
    #[should_panic(expected = "cache capacity reservation underflow")]
    fn reservation_release_fails_fast_on_counter_underflow() {
        let reservation = NodeCacheCapacityReservation {
            node_id: "node-a".to_string(),
            bytes: 1,
            base_capacity: 800,
            reserved_bytes: Arc::new(Mutex::new(0)),
            cache: new_cache(800),
        };
        drop(reservation);
    }
}

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

#[cfg(test)]
mod put_admission_tests {
    use super::{MasterKvRouter, MasterKvRouterNewArg, OneKvNodesRoutes};
    use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError};
    use std::sync::Arc;

    #[::tokio::test]
    async fn reject_if_exists_covers_inflight_and_committed_keys() {
        let router = MasterKvRouter::construct(MasterKvRouterNewArg::default())
            .await
            .expect("router construction must succeed");
        let key = "create-only-key";

        router
            .reserve_inflight_put_key(key, false, true)
            .expect("first create-only reservation must succeed");
        let inflight_err = router
            .reserve_inflight_put_key(key, false, true)
            .expect_err("concurrent create-only reservation must be rejected");
        assert!(matches!(
            inflight_err,
            KvError::Api(ApiError::KeyAlreadyExists { key: ref rejected_key })
                if rejected_key == key
        ));
        let overwrite_err = router
            .reserve_inflight_put_key(key, false, false)
            .expect_err("create-only reservation must exclude ordinary overwrites");
        assert!(matches!(
            overwrite_err,
            KvError::Api(ApiError::KeyBeingWritten { key: ref rejected_key })
                if rejected_key == key
        ));

        // Commit visibility must precede reservation release. A following create-only put
        // then observes either state and cannot enter through a transition gap.
        router.inner().kv_routes.insert(
            key.to_string(),
            Arc::new(OneKvNodesRoutes::new((1, 0), None)),
        );
        router.release_inflight_put_key(key);

        let committed_err = router
            .reserve_inflight_put_key(key, false, true)
            .expect_err("committed key must be rejected");
        assert!(matches!(
            committed_err,
            KvError::Api(ApiError::KeyAlreadyExists { key: ref rejected_key })
                if rejected_key == key
        ));
        assert!(
            !router.inner().inflight_put_key_counts.contains_key(key),
            "a committed-key rejection must not leave a zero-count admission entry"
        );
    }

    #[::tokio::test]
    async fn existing_overwrite_and_inflight_only_contracts_remain_distinct() {
        let router = MasterKvRouter::construct(MasterKvRouterNewArg::default())
            .await
            .expect("router construction must succeed");
        let key = "overwrite-key";
        router.inner().kv_routes.insert(
            key.to_string(),
            Arc::new(OneKvNodesRoutes::new((1, 0), None)),
        );

        router
            .reserve_inflight_put_key(key, false, false)
            .expect("ordinary overwrite must remain allowed");
        let inflight_err = router
            .reserve_inflight_put_key(key, true, false)
            .expect_err("inflight-only admission must retain its typed error");
        assert!(matches!(
            inflight_err,
            KvError::Api(ApiError::KeyBeingWritten { key: ref rejected_key })
                if rejected_key == key
        ));
        router.release_inflight_put_key(key);
    }
}
