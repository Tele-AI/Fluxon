use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;

use crate::{
    cluster_manager::{ClusterMember, NodeID},
    config::{ReplicaTaskPlacementConfig, ReplicaTaskPlacementPolicyKind},
    master_seg_manager::one_seg_allocator::{Allocation, OneSegAllocator},
    rpcresp_kvresult_convert::msg_and_error::KvError,
};
use async_trait::async_trait;
use rand::Rng;
use rand::seq::SliceRandom;

use super::MasterKvRouterView;

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

                    let total = allocator.total_size_bytes();
                    let used = allocator.used_size_bytes();
                    let free = total.saturating_sub(used);
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
                let total = allocator.total_size_bytes();
                let used = allocator.used_size_bytes();
                let free = total.saturating_sub(used);
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

                    let total = allocator.total_size_bytes();
                    let used = allocator.used_size_bytes();
                    let free = total.saturating_sub(used);
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

                let total = allocator.total_size_bytes();
                let used = allocator.used_size_bytes();
                let free = total.saturating_sub(used);
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
    allocator: Arc<OneSegAllocator>,
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

fn member_matches_roles(member: Option<&ClusterMember>, roles: &[String]) -> bool {
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

        let total = allocator.total_size_bytes();
        let used = allocator.used_size_bytes();
        let free = total.saturating_sub(used);
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
            allocator,
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

fn choose_candidate_pool(
    view: &MasterKvRouterView,
    source_node_id: &NodeID,
    excluded_nodes: &HashSet<NodeID>,
    preferred_sub_cluster: Option<&str>,
    config: &ReplicaTaskPlacementConfig,
) -> Vec<PlacementCandidate> {
    let global = collect_remote_candidates(view, source_node_id, excluded_nodes, None, config);
    let global_remote_only = filter_remote_only_candidates(&global);
    let Some(sc) = preferred_sub_cluster else {
        if config.restrict_to_remote_only_node_roles {
            return global_remote_only;
        }
        return global;
    };

    let preferred =
        collect_remote_candidates(view, source_node_id, excluded_nodes, Some(sc), config);
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

fn sort_by_queue_score(candidates: &mut [PlacementCandidate]) {
    candidates.sort_by(|a, b| {
        a.queue_score()
            .total_cmp(&b.queue_score())
            .then_with(|| {
                b.preferred_sub_cluster_match
                    .cmp(&a.preferred_sub_cluster_match)
            })
            .then_with(|| a.node_id.as_ref().cmp(b.node_id.as_ref()))
            .then_with(|| {
                a.allocator
                    .seg_device_id
                    .as_str()
                    .cmp(b.allocator.seg_device_id.as_str())
            })
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
            .then_with(|| {
                a.allocator
                    .seg_device_id
                    .as_str()
                    .cmp(b.allocator.seg_device_id.as_str())
            })
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
            .then_with(|| {
                a.allocator
                    .seg_device_id
                    .as_str()
                    .cmp(b.allocator.seg_device_id.as_str())
            })
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
        *last_no_space_ctx = Some((
            candidate.node_id.as_ref().to_string(),
            candidate.allocator.seg_device_id.clone(),
            candidate.total_bytes,
            candidate.free_bytes,
        ));
        if let Ok(allocation) = candidate.allocator.allocate(len) {
            return Some((candidate.node_id, candidate.allocator, allocation));
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
            let total = allocator.total_size_bytes();
            let used = allocator.used_size_bytes();
            let free = total.saturating_sub(used);
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
        let total = allocator.total_size_bytes();
        let used = allocator.used_size_bytes();
        let free = total.saturating_sub(used);
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
    use crate::master_seg_manager::msg_pack::SegmentDeviceDescription;

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
            allocator: test_allocator(node_id),
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
}
