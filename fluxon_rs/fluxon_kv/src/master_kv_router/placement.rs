use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Duration;

use crate::{
    cluster_manager::{ClusterMember, NodeID},
    config::{ReplicaTaskPlacementConfig, ReplicaTaskPlacementPolicyKind},
    master_seg_manager::msg_pack::OwnerCapacityReport,
    master_seg_manager::one_seg_allocator::{Allocation, OneSegAllocator},
    rpcresp_kvresult_convert::msg_and_error::KvError,
};
use async_trait::async_trait;
use rand::Rng;
use rand::seq::SliceRandom;
use sha2::{Digest, Sha256};

use super::{MasterKvRouterView, OwnerPlacementCandidate, OwnerPlacementPlan};

const OWNER_CAPACITY_REPORT_MAX_AGE: Duration = Duration::from_secs(5);
const OWNER_CAPACITY_POLICY_EPOCH: u64 = 1;

pub enum PutPlacementTarget {
    /// Place locally by reusing the requester's src allocation as the target.
    Local { node_id: NodeID },
    /// Place remotely with a pre-allocated target allocation.
    Remote {
        node_id: NodeID,
        allocation: Allocation,
    },
}

/// A trait for defining placement policies.
#[async_trait]
pub trait PlacementPolicy: Send + Sync {
    /// Selects a target for a put operation, including allocation retries.
    async fn select_put_target(
        &self,
        view: &MasterKvRouterView,
        req_node_id: &NodeID,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<PutPlacementTarget, KvError>;

    /// Selects a remote-only target for replica task placement.
    fn select_remote_target(
        &self,
        view: &MasterKvRouterView,
        source_node_id: &NodeID,
        excluded_nodes: &HashSet<NodeID>,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<(NodeID, Allocation), KvError> {
        choose_random_remote_target(
            view,
            source_node_id,
            excluded_nodes,
            preferred_sub_cluster,
            len,
        )
    }
}

pub fn build_placement_policy(config: ReplicaTaskPlacementConfig) -> Box<dyn PlacementPolicy> {
    Box::new(ReplicaTaskPlacementPolicy::new(config))
}

/// A policy that prefers placing on the requesting node when possible.
pub struct LocalFirstPlacementPolicy;

impl LocalFirstPlacementPolicy {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PlacementPolicy for LocalFirstPlacementPolicy {
    async fn select_put_target(
        &self,
        view: &MasterKvRouterView,
        req_node_id: &NodeID,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<PutPlacementTarget, KvError> {
        let seg_manager = view.master_seg_manager();

        let mut last_no_space_ctx: Option<(String, String, u64, u64)> = None; // (node, segment, total, free)

        if let Some(sc) = preferred_sub_cluster {
            let mut preferred_nodes: Vec<NodeID> = view
                .cluster_manager()
                .get_client_members()
                .into_iter()
                .filter_map(|m| (m.sub_cluster.as_deref() == Some(sc)).then_some(m.id.into()))
                .collect();

            if preferred_nodes.is_empty() {
                tracing::warn!(
                    "preferred_sub_cluster has no eligible kvclients: sub_cluster={:?}",
                    sc
                );
            } else {
                if preferred_nodes
                    .iter()
                    .any(|n| n.as_ref() == req_node_id.as_ref())
                {
                    return Ok(PutPlacementTarget::Local {
                        node_id: req_node_id.clone(),
                    });
                }

                let mut rng = rand::thread_rng();
                let start_idx = rng.gen_range(0..preferred_nodes.len());
                preferred_nodes.rotate_left(start_idx);

                for node_id in preferred_nodes {
                    let node_allocators = seg_manager.get_node_allocators(&node_id);
                    let Some(allocator) = node_allocators.choose(&mut rng).cloned() else {
                        tracing::warn!(
                            "preferred_sub_cluster kvclient has no registered allocators; node_id={} sub_cluster={:?}",
                            node_id,
                            sc
                        );
                        continue;
                    };

                    let capacity = allocator.node_pool_capacity_snapshot();
                    let total = capacity.active_capacity_bytes;
                    let free = capacity.available_capacity_bytes;
                    last_no_space_ctx = Some((
                        node_id.as_ref().to_string(),
                        allocator.seg_device_id.clone(),
                        total,
                        free,
                    ));

                    if let Ok(allocation) = allocator.allocate(len) {
                        return Ok(PutPlacementTarget::Remote {
                            node_id,
                            allocation,
                        });
                    }
                }
            }
        }

        // Local-first: prefer placing on the requesting node when possible.
        // This reduces cross-node transfers and enables src==target optimization.
        let local_allocators = seg_manager.get_node_allocators(req_node_id);
        if !local_allocators.is_empty() {
            return Ok(PutPlacementTarget::Local {
                node_id: req_node_id.clone(),
            });
        }

        for _attempt in 1..=3 {
            let all_segs = seg_manager.get_all_segments_allocator();
            if let Some((nodeid, allocator)) = all_segs.choose(&mut rand::thread_rng()).cloned() {
                let node_id: NodeID = nodeid.into();
                let capacity = allocator.node_pool_capacity_snapshot();
                let total = capacity.active_capacity_bytes;
                let free = capacity.available_capacity_bytes;
                last_no_space_ctx = Some((
                    node_id.as_ref().to_string(),
                    allocator.seg_device_id.clone(),
                    total,
                    free,
                ));
                if let Ok(allocation) = allocator.allocate(len) {
                    return Ok(PutPlacementTarget::Remote {
                        node_id,
                        allocation,
                    });
                }
            }
        }

        let err = if let Some((node, segment, total_capacity, free_capacity)) = last_no_space_ctx {
            KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace {
                    node,
                    segment,
                    total_capacity,
                    free_capacity,
                },
            )
        } else {
            KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace {
                    node: "unknown".to_string(),
                    segment: "unknown".to_string(),
                    total_capacity: 0,
                    free_capacity: 0,
                },
            )
        };
        Err(err)
    }
}

/// A policy that selects a target randomly across eligible nodes/segments.
pub struct RandomPlacementPolicy;

impl RandomPlacementPolicy {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PlacementPolicy for RandomPlacementPolicy {
    async fn select_put_target(
        &self,
        view: &MasterKvRouterView,
        req_node_id: &NodeID,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<PutPlacementTarget, KvError> {
        let seg_manager = view.master_seg_manager();

        let mut last_no_space_ctx: Option<(String, String, u64, u64)> = None; // (node, segment, total, free)

        if let Some(sc) = preferred_sub_cluster {
            let mut preferred_nodes: Vec<NodeID> = view
                .cluster_manager()
                .get_client_members()
                .into_iter()
                .filter_map(|m| (m.sub_cluster.as_deref() == Some(sc)).then_some(m.id.into()))
                .collect();

            if preferred_nodes.is_empty() {
                tracing::warn!(
                    "preferred_sub_cluster has no eligible kvclients: sub_cluster={:?}",
                    sc
                );
            } else {
                if preferred_nodes
                    .iter()
                    .any(|n| n.as_ref() == req_node_id.as_ref())
                {
                    let local_allocators = seg_manager.get_node_allocators(req_node_id);
                    if !local_allocators.is_empty() {
                        return Ok(PutPlacementTarget::Local {
                            node_id: req_node_id.clone(),
                        });
                    }
                }

                let mut rng = rand::thread_rng();
                let start_idx = rng.gen_range(0..preferred_nodes.len());
                preferred_nodes.rotate_left(start_idx);

                for node_id in preferred_nodes {
                    if node_id.as_ref() == req_node_id.as_ref() {
                        continue;
                    }

                    let node_allocators = seg_manager.get_node_allocators(&node_id);
                    let Some(allocator) = node_allocators.choose(&mut rng).cloned() else {
                        tracing::warn!(
                            "preferred_sub_cluster kvclient has no registered allocators; node_id={} sub_cluster={:?}",
                            node_id,
                            sc
                        );
                        continue;
                    };

                    let capacity = allocator.node_pool_capacity_snapshot();
                    let total = capacity.active_capacity_bytes;
                    let free = capacity.available_capacity_bytes;
                    last_no_space_ctx = Some((
                        node_id.as_ref().to_string(),
                        allocator.seg_device_id.clone(),
                        total,
                        free,
                    ));

                    if let Ok(allocation) = allocator.allocate(len) {
                        return Ok(PutPlacementTarget::Remote {
                            node_id,
                            allocation,
                        });
                    }
                }
            }
        }

        for _attempt in 1..=3 {
            let all_segs = seg_manager.get_all_segments_allocator();
            if let Some((nodeid, allocator)) = all_segs.choose(&mut rand::thread_rng()).cloned() {
                let node_id: NodeID = nodeid.into();
                if node_id.as_ref() == req_node_id.as_ref() {
                    let local_allocators = seg_manager.get_node_allocators(req_node_id);
                    if !local_allocators.is_empty() {
                        return Ok(PutPlacementTarget::Local {
                            node_id: req_node_id.clone(),
                        });
                    }
                    continue;
                }

                let capacity = allocator.node_pool_capacity_snapshot();
                let total = capacity.active_capacity_bytes;
                let free = capacity.available_capacity_bytes;
                last_no_space_ctx = Some((
                    node_id.as_ref().to_string(),
                    allocator.seg_device_id.clone(),
                    total,
                    free,
                ));
                if let Ok(allocation) = allocator.allocate(len) {
                    return Ok(PutPlacementTarget::Remote {
                        node_id,
                        allocation,
                    });
                }
            }
        }

        let err = if let Some((node, segment, total_capacity, free_capacity)) = last_no_space_ctx {
            KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace {
                    node,
                    segment,
                    total_capacity,
                    free_capacity,
                },
            )
        } else {
            KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace {
                    node: "unknown".to_string(),
                    segment: "unknown".to_string(),
                    total_capacity: 0,
                    free_capacity: 0,
                },
            )
        };
        Err(err)
    }
}

#[derive(Clone)]
struct PlacementCandidate {
    node_id: NodeID,
    /// Present only for the legacy master-authoritative allocator path.
    /// Owner-authoritative placement reuses the same scoring record but lets
    /// the target owner perform the only real claim.
    allocator: Option<Arc<OneSegAllocator>>,
    total_bytes: u64,
    free_bytes: u64,
    used_bytes: u64,
    node_write_count: u64,
    requester_target_count: u64,
    is_remote_only_role: bool,
    is_active_role: bool,
    preferred_sub_cluster_match: bool,
}

impl PlacementCandidate {
    fn segment_tiebreaker(&self) -> &str {
        self.allocator
            .as_ref()
            .map(|allocator| allocator.seg_device_id.as_str())
            .unwrap_or("")
    }

    fn queue_wait_ms(&self) -> f64 {
        self.node_write_count as f64 + (self.requester_target_count as f64 * 0.01)
    }

    fn mem_pressure(&self) -> f64 {
        if self.total_bytes == 0 {
            1.0
        } else {
            1.0 - (self.free_bytes as f64 / self.total_bytes as f64)
        }
    }

    fn queue_score(&self) -> f64 {
        self.queue_wait_ms() + self.mem_pressure() * 0.001 + self.used_bytes as f64 * 1e-9
    }
}

type NoSpaceCtx = (String, String, u64, u64);

fn no_space_error(last_no_space_ctx: Option<NoSpaceCtx>) -> KvError {
    if let Some((node, segment, total_capacity, free_capacity)) = last_no_space_ctx {
        KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace {
                node,
                segment,
                total_capacity,
                free_capacity,
            },
        )
    } else {
        KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NoSpace {
                node: "unknown".to_string(),
                segment: "unknown".to_string(),
                total_capacity: 0,
                free_capacity: 0,
            },
        )
    }
}

pub(super) fn member_matches_roles(member: Option<&ClusterMember>, roles: &[String]) -> bool {
    let Some(member) = member else {
        return false;
    };
    let metadata_role = member.metadata.get("role").map(|v| v.as_str());
    let metadata_node_role = member.metadata.get("node_role").map(|v| v.as_str());
    let sub_cluster = member.sub_cluster.as_deref();
    roles.iter().any(|role| {
        let role = role.as_str();
        metadata_role == Some(role) || metadata_node_role == Some(role) || sub_cluster == Some(role)
    })
}

fn is_role_aware_policy(policy: ReplicaTaskPlacementPolicyKind) -> bool {
    matches!(
        policy,
        ReplicaTaskPlacementPolicyKind::WeightedRoleAware
            | ReplicaTaskPlacementPolicyKind::BoundedRoleQueueAware
            | ReplicaTaskPlacementPolicyKind::PressureRoleQueueAware
    )
}

fn filter_remote_only_candidates(candidates: &[PlacementCandidate]) -> Vec<PlacementCandidate> {
    candidates
        .iter()
        .filter(|candidate| candidate.is_remote_only_role)
        .cloned()
        .collect()
}

fn collect_remote_candidates(
    view: &MasterKvRouterView,
    source_node_id: &NodeID,
    excluded_nodes: &HashSet<NodeID>,
    preferred_sub_cluster: Option<&str>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let members_by_id: HashMap<String, ClusterMember> = view
        .cluster_manager()
        .get_client_members()
        .into_iter()
        .map(|member| (member.id.clone(), member))
        .collect();

    let mut candidates = Vec::new();
    for (node_id, allocator) in view.master_seg_manager().get_all_segments_allocator() {
        if node_id.as_ref() == source_node_id.as_ref() || excluded_nodes.contains(&node_id) {
            continue;
        }

        let member = members_by_id.get(node_id.as_ref());
        let preferred_sub_cluster_match = preferred_sub_cluster
            .map(|sc| member.and_then(|m| m.sub_cluster.as_deref()) == Some(sc))
            .unwrap_or(false);
        if preferred_sub_cluster.is_some() && !preferred_sub_cluster_match {
            continue;
        }

        let capacity = allocator.node_pool_capacity_snapshot();
        let total = capacity.active_capacity_bytes;
        let used = capacity.used_capacity_bytes;
        let free = capacity.available_capacity_bytes;
        let node_key = node_id.as_ref().to_string();
        let node_write_count = view
            .master_kv_router()
            .inner()
            .put_target_decision_counts
            .get(&node_key)
            .map(|entry| entry.value().load(AtomicOrdering::Relaxed))
            .unwrap_or(0);
        let requester_target_count = view
            .master_kv_router()
            .inner()
            .put_requester_target_decision_counts
            .get(&super::RequesterTargetPair::new(
                source_node_id.as_ref(),
                node_id.as_ref(),
            ))
            .map(|entry| entry.value().load(AtomicOrdering::Relaxed))
            .unwrap_or(0);

        candidates.push(PlacementCandidate {
            node_id,
            allocator: Some(allocator),
            total_bytes: total,
            free_bytes: free,
            used_bytes: used,
            node_write_count,
            requester_target_count,
            is_remote_only_role: member_matches_roles(member, &config.remote_only_node_roles),
            is_active_role: member_matches_roles(member, &config.active_node_roles),
            preferred_sub_cluster_match,
        });
    }
    candidates
}

fn choose_candidate_pool_from_sets(
    source_node_id: &NodeID,
    preferred_sub_cluster: Option<&str>,
    config: &ReplicaTaskPlacementConfig,
    global: Vec<PlacementCandidate>,
    preferred: Vec<PlacementCandidate>,
) -> Vec<PlacementCandidate> {
    let global_remote_only = filter_remote_only_candidates(&global);
    let Some(sc) = preferred_sub_cluster else {
        if config.restrict_to_remote_only_node_roles {
            return global_remote_only;
        }
        return global;
    };

    let preferred_remote_only = filter_remote_only_candidates(&preferred);
    if config.restrict_to_remote_only_node_roles {
        if !preferred_remote_only.is_empty() {
            return preferred_remote_only;
        }
        if !global_remote_only.is_empty() {
            tracing::warn!(
                "preferred_sub_cluster has no eligible remote-only kvclients; using global remote-only candidates: source_node_id={} sub_cluster={:?} remote_only_node_roles={:?}",
                source_node_id,
                sc,
                config.remote_only_node_roles
            );
            return global_remote_only;
        }
        tracing::warn!(
            "strict remote-only placement has no eligible candidates: source_node_id={} sub_cluster={:?} remote_only_node_roles={:?}",
            source_node_id,
            sc,
            config.remote_only_node_roles
        );
        return Vec::new();
    }

    if preferred.is_empty() {
        tracing::warn!(
            "preferred_sub_cluster has no eligible remote kvclients: source_node_id={} sub_cluster={:?}",
            source_node_id,
            sc
        );
        return global;
    }

    if is_role_aware_policy(config.policy)
        && !preferred
            .iter()
            .any(|candidate| candidate.is_remote_only_role)
        && global.iter().any(|candidate| candidate.is_remote_only_role)
    {
        return global;
    }

    preferred
}

fn choose_candidate_pool(
    view: &MasterKvRouterView,
    source_node_id: &NodeID,
    excluded_nodes: &HashSet<NodeID>,
    preferred_sub_cluster: Option<&str>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let global = collect_remote_candidates(view, source_node_id, excluded_nodes, None, config);
    let preferred = preferred_sub_cluster
        .map(|sc| collect_remote_candidates(view, source_node_id, excluded_nodes, Some(sc), config))
        .unwrap_or_default();
    choose_candidate_pool_from_sets(
        source_node_id,
        preferred_sub_cluster,
        config,
        global,
        preferred,
    )
}

fn owner_capacity_weight(report: &OwnerCapacityReport, allocation_size: u64) -> u64 {
    if !report.settled || allocation_size == 0 {
        return 0;
    }
    let Some(size_class) = report
        .size_classes
        .iter()
        .find(|size_class| size_class.allocation_size_bytes == allocation_size)
    else {
        return 0;
    };
    let global_headroom = report
        .global_target_bytes
        .saturating_sub(report.global_accounted_bytes);
    global_headroom
        .min(size_class.allocatable_bytes)
        .checked_div(allocation_size)
        .unwrap_or(0)
        .saturating_mul(allocation_size)
}

fn owner_global_replacement_needed(
    report: &OwnerCapacityReport,
    allocation_size: u64,
    pending_reclaim_bytes: u64,
) -> bool {
    report.settled
        && allocation_size != 0
        && pending_reclaim_bytes == 0
        && report.global_target_bytes >= allocation_size
        && report.global_accounted_bytes >= allocation_size
        && report
            .global_target_bytes
            .saturating_sub(report.global_accounted_bytes)
            < allocation_size
        && report
            .size_classes
            .iter()
            .any(|size_class| size_class.allocation_size_bytes == allocation_size)
}

fn try_start_owner_global_replacement(
    view: &MasterKvRouterView,
    owner: &crate::owner_segment::OwnerGeneration,
    allocation_size: u64,
) -> u64 {
    let router = view.master_kv_router();
    let owner_node_id = owner.node_id.as_str();
    let capacity_lock = router.node_capacity_boundary_lock(owner_node_id);
    let _capacity_guard = capacity_lock.lock();
    let pending_reclaim_bytes = router.eviction_reclaim_pending_weight(owner_node_id);
    let Some((report, age)) = view
        .master_seg_manager()
        .get_owner_capacity_report(owner_node_id)
    else {
        return 0;
    };
    if age > OWNER_CAPACITY_REPORT_MAX_AGE
        || report.owner_node_start_time != owner.node_start_time
        || !owner_global_replacement_needed(&report, allocation_size, pending_reclaim_bytes)
    {
        return 0;
    }
    let Some(cache) = router.get_node_cache_controller(owner_node_id) else {
        return 0;
    };
    cache.evict_some(allocation_size)
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn hash_i64(hasher: &mut Sha256, value: i64) {
    hasher.update(value.to_be_bytes());
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hash_u64(hasher, u64::try_from(value.len()).unwrap_or(u64::MAX));
    hasher.update(value.as_bytes());
}

fn finish_hash_u64(hasher: Sha256) -> u64 {
    let digest = hasher.finalize();
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("SHA-256 digest always contains eight bytes"),
    )
}

fn capacity_snapshot_id(allocation_size: u64, candidates: &[OwnerPlacementCandidate]) -> u64 {
    let mut hasher = Sha256::new();
    hash_u64(&mut hasher, OWNER_CAPACITY_POLICY_EPOCH);
    hash_u64(&mut hasher, allocation_size);
    for candidate in candidates {
        hash_str(&mut hasher, &candidate.owner.node_id);
        hash_i64(&mut hasher, candidate.owner.node_start_time);
        hash_u64(&mut hasher, candidate.capacity_report_epoch);
        hash_u64(&mut hasher, candidate.weight_bytes);
        hash_str(&mut hasher, candidate.placement_class.as_str());
    }
    finish_hash_u64(hasher)
}

fn capacity_rank(
    source_node_id: &NodeID,
    operation_id: u64,
    snapshot_id: u64,
    candidate: &OwnerPlacementCandidate,
) -> f64 {
    let mut hasher = Sha256::new();
    hash_str(&mut hasher, source_node_id.as_ref());
    hash_u64(&mut hasher, operation_id);
    hash_u64(&mut hasher, OWNER_CAPACITY_POLICY_EPOCH);
    hash_u64(&mut hasher, snapshot_id);
    hash_u64(&mut hasher, candidate.capacity_report_epoch);
    hash_str(&mut hasher, &candidate.owner.node_id);
    hash_i64(&mut hasher, candidate.owner.node_start_time);
    let random = finish_hash_u64(hasher);
    // The midpoint mapping avoids both zero and an implementation-dependent
    // endpoint special case while preserving a uniform deterministic draw.
    let uniform = (random as f64 + 0.5) / (u64::MAX as f64 + 1.0);
    -uniform.ln() / candidate.weight_bytes as f64
}

fn order_owner_capacity_candidates(
    source_node_id: &NodeID,
    operation_id: u64,
    allocation_size: u64,
    mut candidates: Vec<OwnerPlacementCandidate>,
) -> OwnerPlacementPlan {
    candidates.sort_by(|a, b| {
        a.owner
            .node_id
            .cmp(&b.owner.node_id)
            .then_with(|| a.owner.node_start_time.cmp(&b.owner.node_start_time))
    });
    let snapshot_id = capacity_snapshot_id(allocation_size, &candidates);
    for candidate in &mut candidates {
        candidate.rank = capacity_rank(source_node_id, operation_id, snapshot_id, candidate);
    }
    candidates.sort_by(|a, b| {
        a.rank.total_cmp(&b.rank).then_with(|| {
            a.owner
                .node_id
                .cmp(&b.owner.node_id)
                .then_with(|| a.owner.node_start_time.cmp(&b.owner.node_start_time))
        })
    });
    OwnerPlacementPlan {
        policy_epoch: OWNER_CAPACITY_POLICY_EPOCH,
        capacity_snapshot_id: snapshot_id,
        allocation_size_bytes: allocation_size,
        candidates,
    }
}

pub(super) fn select_remote_owner_candidates(
    view: &MasterKvRouterView,
    source_node_id: &NodeID,
    excluded_nodes: &HashSet<NodeID>,
    _preferred_sub_cluster: Option<&str>,
    len: u64,
    operation_id: u64,
) -> Result<OwnerPlacementPlan, KvError> {
    let Some(allocation_size) = crate::owner_segment_allocation_capacity_bytes(len) else {
        return Err(no_space_error(None));
    };
    // Register demand before inspecting reports. Empty RemoteCpu owners learn
    // the class through their existing periodic report response and remain
    // fail-closed until they publish exact allocator-derived capacity.
    view.master_seg_manager()
        .register_owner_capacity_size_class(allocation_size);
    let mut candidates = Vec::new();
    let mut replacement_candidates = Vec::new();
    for member in view.cluster_manager().get_client_members() {
        let node_id: NodeID = member.id.clone().into();
        if node_id.as_ref() == source_node_id.as_ref() || excluded_nodes.contains(&node_id) {
            continue;
        }
        let Some((report, age)) = view
            .master_seg_manager()
            .get_owner_capacity_report(node_id.as_ref())
        else {
            continue;
        };
        if age > OWNER_CAPACITY_REPORT_MAX_AGE
            || report.owner_node_start_time != member.node_start_time
        {
            continue;
        }
        let weight_bytes = owner_capacity_weight(&report, allocation_size);
        if weight_bytes == 0 {
            let pending_reclaim_bytes = view
                .master_kv_router()
                .eviction_reclaim_pending_weight(node_id.as_ref());
            if owner_global_replacement_needed(&report, allocation_size, pending_reclaim_bytes) {
                replacement_candidates.push(OwnerPlacementCandidate {
                    owner: crate::owner_segment::OwnerGeneration::new(
                        member.id,
                        member.node_start_time,
                    ),
                    placement_class: report.placement_class,
                    capacity_report_epoch: report.report_epoch,
                    weight_bytes: allocation_size,
                    rank: 0.0,
                });
            }
            continue;
        }
        candidates.push(OwnerPlacementCandidate {
            owner: crate::owner_segment::OwnerGeneration::new(member.id, member.node_start_time),
            placement_class: report.placement_class,
            capacity_report_epoch: report.report_epoch,
            weight_bytes,
            rank: 0.0,
        });
    }
    if candidates.is_empty() {
        if !replacement_candidates.is_empty() {
            let replacement_plan = order_owner_capacity_candidates(
                source_node_id,
                operation_id,
                allocation_size,
                replacement_candidates,
            );
            for candidate in replacement_plan.candidates {
                let selected_bytes =
                    try_start_owner_global_replacement(view, &candidate.owner, allocation_size);
                if selected_bytes != 0 {
                    tracing::debug!(
                        source_node_id = %source_node_id,
                        target_node_id = candidate.owner.node_id,
                        operation_id,
                        allocation_size,
                        selected_bytes,
                        "full GlobalShared owner started one bounded replacement pop"
                    );
                    break;
                }
            }
        }
        return Err(no_space_error(None));
    }
    Ok(order_owner_capacity_candidates(
        source_node_id,
        operation_id,
        allocation_size,
        candidates,
    ))
}

fn sort_by_queue_score(candidates: &mut [PlacementCandidate]) {
    candidates.sort_by(|a, b| {
        a.queue_score()
            .total_cmp(&b.queue_score())
            .then_with(|| {
                b.preferred_sub_cluster_match
                    .cmp(&a.preferred_sub_cluster_match)
            })
            .then_with(|| a.node_id.as_ref().cmp(b.node_id.as_ref()))
            .then_with(|| a.segment_tiebreaker().cmp(b.segment_tiebreaker()))
    });
}

fn remote_only_first(candidates: Vec<PlacementCandidate>) -> Vec<PlacementCandidate> {
    let has_remote_only = candidates
        .iter()
        .any(|candidate| candidate.is_remote_only_role);
    if !has_remote_only {
        return candidates;
    }
    candidates
        .into_iter()
        .filter(|candidate| candidate.is_remote_only_role)
        .collect()
}

fn order_queue_aware(mut candidates: Vec<PlacementCandidate>) -> Vec<PlacementCandidate> {
    sort_by_queue_score(&mut candidates);
    candidates
}

fn order_weighted_role_aware(
    candidates: Vec<PlacementCandidate>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let mut candidates = remote_only_first(candidates);
    candidates.sort_by(|a, b| {
        let a_role_weight = if a.is_remote_only_role {
            config.remote_only_shard_weight
        } else {
            1.0
        };
        let b_role_weight = if b.is_remote_only_role {
            config.remote_only_shard_weight
        } else {
            1.0
        };
        (a.queue_score() / a_role_weight)
            .total_cmp(&(b.queue_score() / b_role_weight))
            .then_with(|| b.is_remote_only_role.cmp(&a.is_remote_only_role))
            .then_with(|| {
                b.preferred_sub_cluster_match
                    .cmp(&a.preferred_sub_cluster_match)
            })
            .then_with(|| a.node_id.as_ref().cmp(b.node_id.as_ref()))
            .then_with(|| a.segment_tiebreaker().cmp(b.segment_tiebreaker()))
    });
    candidates
}

fn order_bounded_role_queue_aware(
    candidates: Vec<PlacementCandidate>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let Some(best_wait) = candidates
        .iter()
        .map(|candidate| candidate.queue_wait_ms())
        .min_by(|a, b| a.total_cmp(b))
    else {
        return candidates;
    };
    let max_wait = best_wait + config.role_queue_window_ms;
    let mut eligible: Vec<PlacementCandidate> = candidates
        .into_iter()
        .filter(|candidate| candidate.queue_wait_ms() <= max_wait)
        .collect();
    eligible = remote_only_first(eligible);
    eligible.sort_by(|a, b| {
        let a_weight = if a.is_remote_only_role {
            config.remote_only_shard_weight
        } else {
            1.0
        } - ((a.used_bytes as f64 / 4096.0) * 1e-6);
        let b_weight = if b.is_remote_only_role {
            config.remote_only_shard_weight
        } else {
            1.0
        } - ((b.used_bytes as f64 / 4096.0) * 1e-6);
        b_weight
            .total_cmp(&a_weight)
            .then_with(|| a.queue_score().total_cmp(&b.queue_score()))
            .then_with(|| {
                b.preferred_sub_cluster_match
                    .cmp(&a.preferred_sub_cluster_match)
            })
            .then_with(|| a.node_id.as_ref().cmp(b.node_id.as_ref()))
            .then_with(|| a.segment_tiebreaker().cmp(b.segment_tiebreaker()))
    });
    eligible
}

fn average_queue_wait<'a>(candidates: impl Iterator<Item = &'a PlacementCandidate>) -> Option<f64> {
    let mut sum = 0.0;
    let mut count = 0usize;
    for candidate in candidates {
        sum += candidate.queue_wait_ms();
        count += 1;
    }
    (count > 0).then_some(sum / count as f64)
}

fn filter_remote_imbalance(
    candidates: Vec<PlacementCandidate>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let Some(min_count) = candidates
        .iter()
        .map(|candidate| candidate.node_write_count)
        .min()
    else {
        return candidates;
    };
    let max_next = (min_count as f64 + 1.0) * config.role_max_shard_imbalance;
    let filtered: Vec<PlacementCandidate> = candidates
        .iter()
        .filter(|candidate| candidate.node_write_count as f64 + 1.0 <= max_next)
        .cloned()
        .collect();
    if filtered.is_empty() {
        candidates
    } else {
        filtered
    }
}

fn order_pressure_role_queue_aware(
    candidates: Vec<PlacementCandidate>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let remote_candidates: Vec<PlacementCandidate> = candidates
        .iter()
        .filter(|candidate| candidate.is_remote_only_role)
        .cloned()
        .collect();
    if remote_candidates.is_empty() {
        return order_queue_aware(candidates);
    }

    let active_wait = average_queue_wait(candidates.iter().filter(|candidate| {
        candidate.is_active_role || (!candidate.is_remote_only_role && !candidate.is_active_role)
    }));
    let remote_wait = average_queue_wait(remote_candidates.iter());
    if let (Some(active_wait), Some(remote_wait)) = (active_wait, remote_wait) {
        let gap = active_wait - remote_wait;
        let fabric_guard_ok =
            config.role_fabric_guard_ms == 0.0 || gap <= config.role_fabric_guard_ms;
        if gap >= config.role_pressure_gap_ms && fabric_guard_ok {
            let mut remote_candidates = filter_remote_imbalance(remote_candidates, config);
            sort_by_queue_score(&mut remote_candidates);
            return remote_candidates;
        }
    }

    order_queue_aware(candidates)
}

fn order_remote_candidates(
    candidates: Vec<PlacementCandidate>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    match config.policy {
        ReplicaTaskPlacementPolicyKind::LocalFirst | ReplicaTaskPlacementPolicyKind::Random => {
            let mut candidates = candidates;
            candidates.shuffle(&mut rand::thread_rng());
            candidates
        }
        ReplicaTaskPlacementPolicyKind::QueueAware => order_queue_aware(candidates),
        ReplicaTaskPlacementPolicyKind::WeightedRoleAware => {
            order_weighted_role_aware(candidates, config)
        }
        ReplicaTaskPlacementPolicyKind::BoundedRoleQueueAware => {
            order_bounded_role_queue_aware(candidates, config)
        }
        ReplicaTaskPlacementPolicyKind::PressureRoleQueueAware => {
            order_pressure_role_queue_aware(candidates, config)
        }
    }
}

fn try_allocate_candidates(
    candidates: Vec<PlacementCandidate>,
    len: u64,
    last_no_space_ctx: &mut Option<NoSpaceCtx>,
) -> Option<(NodeID, Arc<OneSegAllocator>, Allocation)> {
    for candidate in candidates {
        let Some(allocator) = candidate.allocator else {
            continue;
        };
        *last_no_space_ctx = Some((
            candidate.node_id.as_ref().to_string(),
            allocator.seg_device_id.clone(),
            candidate.total_bytes,
            candidate.free_bytes,
        ));
        if let Ok(allocation) = allocator.allocate(len) {
            return Some((candidate.node_id, allocator, allocation));
        }
    }
    None
}

fn choose_random_remote_target_with_allocator(
    view: &MasterKvRouterView,
    source_node_id: &NodeID,
    excluded_nodes: &HashSet<NodeID>,
    preferred_sub_cluster: Option<&str>,
    len: u64,
) -> Result<(NodeID, Arc<OneSegAllocator>, Allocation), KvError> {
    let seg_manager = view.master_seg_manager();
    let mut last_no_space_ctx: Option<NoSpaceCtx> = None;

    if let Some(sc) = preferred_sub_cluster {
        let mut preferred_nodes: Vec<NodeID> = view
            .cluster_manager()
            .get_client_members()
            .into_iter()
            .filter_map(|m| (m.sub_cluster.as_deref() == Some(sc)).then_some(m.id.into()))
            .collect();
        preferred_nodes.retain(|node_id| {
            node_id.as_ref() != source_node_id.as_ref() && !excluded_nodes.contains(node_id)
        });
        preferred_nodes.shuffle(&mut rand::thread_rng());
        for node_id in preferred_nodes {
            let node_allocators = seg_manager.get_node_allocators(&node_id);
            let Some(allocator) = node_allocators.choose(&mut rand::thread_rng()).cloned() else {
                continue;
            };
            let capacity = allocator.node_pool_capacity_snapshot();
            let total = capacity.active_capacity_bytes;
            let free = capacity.available_capacity_bytes;
            last_no_space_ctx = Some((
                node_id.as_ref().to_string(),
                allocator.seg_device_id.clone(),
                total,
                free,
            ));
            if let Ok(allocation) = allocator.allocate(len) {
                return Ok((node_id, allocator, allocation));
            }
        }
    }

    let all_segs = seg_manager.get_all_segments_allocator();
    let mut candidates: Vec<(NodeID, Arc<OneSegAllocator>)> = all_segs
        .into_iter()
        .filter_map(|(node_id, allocator)| {
            if node_id.as_ref() == source_node_id.as_ref() || excluded_nodes.contains(&node_id) {
                return None;
            }
            Some((node_id, allocator))
        })
        .collect();
    candidates.shuffle(&mut rand::thread_rng());
    for (node_id, allocator) in candidates {
        let capacity = allocator.node_pool_capacity_snapshot();
        let total = capacity.active_capacity_bytes;
        let free = capacity.available_capacity_bytes;
        last_no_space_ctx = Some((
            node_id.as_ref().to_string(),
            allocator.seg_device_id.clone(),
            total,
            free,
        ));
        if let Ok(allocation) = allocator.allocate(len) {
            return Ok((node_id, allocator, allocation));
        }
    }

    Err(no_space_error(last_no_space_ctx))
}

fn choose_random_remote_target(
    view: &MasterKvRouterView,
    source_node_id: &NodeID,
    excluded_nodes: &HashSet<NodeID>,
    preferred_sub_cluster: Option<&str>,
    len: u64,
) -> Result<(NodeID, Allocation), KvError> {
    choose_random_remote_target_with_allocator(
        view,
        source_node_id,
        excluded_nodes,
        preferred_sub_cluster,
        len,
    )
    .map(|(node_id, _allocator, allocation)| (node_id, allocation))
}

pub struct ReplicaTaskPlacementPolicy {
    config: ReplicaTaskPlacementConfig,
}

impl ReplicaTaskPlacementPolicy {
    pub fn new(config: ReplicaTaskPlacementConfig) -> Self {
        Self { config }
    }

    fn select_remote_target_with_allocator(
        &self,
        view: &MasterKvRouterView,
        source_node_id: &NodeID,
        excluded_nodes: &HashSet<NodeID>,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<(NodeID, Arc<OneSegAllocator>, Allocation), KvError> {
        let mut last_no_space_ctx: Option<NoSpaceCtx> = None;
        let candidates = choose_candidate_pool(
            view,
            source_node_id,
            excluded_nodes,
            preferred_sub_cluster,
            &self.config,
        );
        let ordered = order_remote_candidates(candidates, &self.config);
        if let Some(selected) = try_allocate_candidates(ordered, len, &mut last_no_space_ctx) {
            return Ok(selected);
        }

        if preferred_sub_cluster.is_some() {
            let global_candidates =
                choose_candidate_pool(view, source_node_id, excluded_nodes, None, &self.config);
            let ordered = order_remote_candidates(global_candidates, &self.config);
            if let Some(selected) = try_allocate_candidates(ordered, len, &mut last_no_space_ctx) {
                return Ok(selected);
            }
        }

        Err(no_space_error(last_no_space_ctx))
    }
}

#[async_trait]
impl PlacementPolicy for ReplicaTaskPlacementPolicy {
    async fn select_put_target(
        &self,
        view: &MasterKvRouterView,
        req_node_id: &NodeID,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<PutPlacementTarget, KvError> {
        self.select_remote_target_with_allocator(
            view,
            req_node_id,
            &HashSet::new(),
            preferred_sub_cluster,
            len,
        )
        .map(
            |(node_id, _allocator, allocation)| PutPlacementTarget::Remote {
                node_id,
                allocation,
            },
        )
    }

    fn select_remote_target(
        &self,
        view: &MasterKvRouterView,
        source_node_id: &NodeID,
        excluded_nodes: &HashSet<NodeID>,
        preferred_sub_cluster: Option<&str>,
        len: u64,
    ) -> Result<(NodeID, Allocation), KvError> {
        self.select_remote_target_with_allocator(
            view,
            source_node_id,
            excluded_nodes,
            preferred_sub_cluster,
            len,
        )
        .map(|(node_id, _allocator, allocation)| (node_id, allocation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ReplicaTaskPlacementConfig, ReplicaTaskPlacementPolicyKind};
    use crate::master_seg_manager::msg_pack::{
        OwnerPlacementClass, OwnerSizeClassCapacity, SegmentDeviceDescription,
    };

    fn test_allocator(id: &str) -> Arc<OneSegAllocator> {
        Arc::new(
            OneSegAllocator::new(
                id.to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                1024 * 1024,
            )
            .unwrap(),
        )
    }

    fn candidate(
        node_id: &str,
        is_remote_only_role: bool,
        node_write_count: u64,
    ) -> PlacementCandidate {
        PlacementCandidate {
            node_id: node_id.to_string().into(),
            allocator: Some(test_allocator(node_id)),
            total_bytes: 1024 * 1024,
            free_bytes: 1024 * 1024,
            used_bytes: 0,
            node_write_count,
            requester_target_count: 0,
            is_remote_only_role,
            is_active_role: !is_remote_only_role,
            preferred_sub_cluster_match: false,
        }
    }

    fn owner_candidate(node_id: &str, weight_bytes: u64) -> OwnerPlacementCandidate {
        OwnerPlacementCandidate {
            owner: crate::owner_segment::OwnerGeneration::new(node_id, 17),
            placement_class: OwnerPlacementClass::RemoteCpu,
            capacity_report_epoch: 9,
            weight_bytes,
            rank: 0.0,
        }
    }

    fn capacity_report(
        placement_class: OwnerPlacementClass,
        physical: u64,
        local_target: u64,
        global_accounted: u64,
        allocation_size: u64,
        allocatable: u64,
    ) -> OwnerCapacityReport {
        OwnerCapacityReport {
            placement_class,
            physical_capacity_bytes: physical,
            local_target_bytes: local_target,
            global_target_bytes: physical - local_target,
            global_accounted_bytes: global_accounted,
            settled: true,
            size_classes: vec![OwnerSizeClassCapacity {
                allocation_size_bytes: allocation_size,
                allocatable_bytes: allocatable,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn bounded_role_queue_aware_prefers_remote_only_within_window() {
        let mut config = ReplicaTaskPlacementConfig::default();
        config.policy = ReplicaTaskPlacementPolicyKind::BoundedRoleQueueAware;
        config.role_queue_window_ms = 2.0;
        config.remote_only_shard_weight = 1.02;

        let ordered = order_bounded_role_queue_aware(
            vec![
                candidate("active-a", false, 0),
                candidate("remote-a", true, 1),
            ],
            &config,
        );
        assert_eq!(ordered[0].node_id.as_ref(), "remote-a");
    }

    #[test]
    fn strict_remote_only_candidates_filter_out_active_nodes() {
        let filtered = filter_remote_only_candidates(&[
            candidate("prefill-a", false, 0),
            candidate("decode-a", false, 0),
            candidate("remote-cache-a", true, 10),
        ]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].node_id.as_ref(), "remote-cache-a");
    }

    #[test]
    fn owner_weight_uses_global_headroom_and_exact_size_class() {
        let inference = capacity_report(OwnerPlacementClass::Inference, 400, 100, 50, 50, 400);
        assert_eq!(owner_capacity_weight(&inference, 50), 250);

        let remote_cpu = capacity_report(OwnerPlacementClass::RemoteCpu, 400, 0, 100, 50, 250);
        assert_eq!(owner_capacity_weight(&remote_cpu, 50), 250);
        assert_eq!(owner_capacity_weight(&remote_cpu, 64), 0);

        let mut unsettled = remote_cpu;
        unsettled.settled = false;
        assert_eq!(owner_capacity_weight(&unsettled, 50), 0);
    }

    #[test]
    fn full_global_owner_starts_only_one_settled_replacement_window() {
        let full = capacity_report(OwnerPlacementClass::Inference, 400, 100, 300, 50, 200);
        assert!(owner_global_replacement_needed(&full, 50, 0));
        assert!(!owner_global_replacement_needed(&full, 50, 50));

        let with_headroom = capacity_report(OwnerPlacementClass::Inference, 400, 100, 250, 50, 200);
        assert!(!owner_global_replacement_needed(&with_headroom, 50, 0));

        let mut unsettled = full.clone();
        unsettled.settled = false;
        assert!(!owner_global_replacement_needed(&unsettled, 50, 0));

        let no_global_slot = capacity_report(OwnerPlacementClass::Inference, 400, 380, 20, 50, 200);
        assert!(!owner_global_replacement_needed(&no_global_slot, 50, 0));
    }

    #[test]
    fn weighted_order_is_stable_across_input_order() {
        let source: NodeID = "source".to_string().into();
        let forward = vec![
            owner_candidate("a", 100),
            owner_candidate("b", 300),
            owner_candidate("c", 200),
        ];
        let mut reverse = forward.clone();
        reverse.reverse();
        let first = order_owner_capacity_candidates(&source, 41, 50, forward);
        let second = order_owner_capacity_candidates(&source, 41, 50, reverse);
        assert_eq!(first.policy_epoch, second.policy_epoch);
        assert_eq!(first.capacity_snapshot_id, second.capacity_snapshot_id);
        assert_eq!(
            first
                .candidates
                .iter()
                .map(|candidate| (&candidate.owner, candidate.weight_bytes, candidate.rank))
                .collect::<Vec<_>>(),
            second
                .candidates
                .iter()
                .map(|candidate| (&candidate.owner, candidate.weight_bytes, candidate.rank))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn capacity_snapshot_identity_includes_request_size_class() {
        let source: NodeID = "source".to_string().into();
        let candidates = vec![owner_candidate("a", 100), owner_candidate("b", 300)];
        let small = order_owner_capacity_candidates(&source, 41, 50, candidates.clone());
        let large = order_owner_capacity_candidates(&source, 41, 100, candidates);
        assert_ne!(small.capacity_snapshot_id, large.capacity_snapshot_id);
        assert_eq!(small.allocation_size_bytes, 50);
        assert_eq!(large.allocation_size_bytes, 100);
    }

    #[test]
    fn weighted_first_choice_converges_to_capacity_ratio() {
        let source: NodeID = "source".to_string().into();
        let mut small_first = 0u64;
        const SAMPLES: u64 = 20_000;
        for operation_id in 1..=SAMPLES {
            let plan = order_owner_capacity_candidates(
                &source,
                operation_id,
                50,
                vec![owner_candidate("small", 100), owner_candidate("large", 300)],
            );
            if plan.candidates[0].owner.node_id == "small" {
                small_first += 1;
            }
            assert_ne!(
                plan.candidates[0].owner.node_id,
                plan.candidates[1].owner.node_id
            );
        }
        let share = small_first as f64 / SAMPLES as f64;
        assert!(
            (0.23..=0.27).contains(&share),
            "100:300 weighted first-choice share diverged: {share}"
        );
    }
}
