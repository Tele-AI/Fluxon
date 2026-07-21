use std::{collections::HashSet, sync::Arc};

use crate::{
    cluster_manager::{ClusterMember, NodeID, member_has_kv_ssd_storage},
    master_seg_manager::one_seg_allocator::{Allocation, OneSegAllocator},
    rpcresp_kvresult_convert::msg_and_error::KvError,
};
use async_trait::async_trait;
use rand::Rng;
use rand::seq::SliceRandom;

use super::MasterKvRouterView;

pub enum PutPlacementTarget {
    /// Place locally by reusing the requester's src allocation as the target.
    Local {
        node_id: NodeID,
        persist_to_ssd: bool,
    },
    /// Place remotely with a pre-allocated target allocation.
    Remote {
        node_id: NodeID,
        allocator: Arc<OneSegAllocator>,
        allocation: Allocation,
        persist_to_ssd: bool,
    },
}

#[derive(Debug)]
struct PutSsdPlacementScope {
    capable_nodes: HashSet<NodeID>,
}

impl PutSsdPlacementScope {
    fn from_client_members(members: &[ClusterMember]) -> Self {
        Self {
            capable_nodes: members
                .iter()
                .filter(|member| member_has_kv_ssd_storage(member))
                .map(|member| member.id.clone().into())
                .collect(),
        }
    }

    fn requires_ssd(&self) -> bool {
        !self.capable_nodes.is_empty()
    }

    fn allows_node(&self, node_id: &str) -> bool {
        !self.requires_ssd() || self.capable_nodes.contains(node_id)
    }

    fn persists_to_ssd(&self, node_id: &str) -> bool {
        self.capable_nodes.contains(node_id)
    }
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
}

/// Compile-time switch for the master placement default.
///
/// Change the type alias to switch behavior (and rebuild):
/// - `LocalFirstPlacementPolicy` prefers local placement when possible.
/// - `RandomPlacementPolicy` selects a random eligible target.
// pub type PlacementDefault = LocalFirstPlacementPolicy;
pub type PlacementDefault = RandomPlacementPolicy;

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
        let client_members = view.cluster_manager().get_client_members();
        let ssd_scope = PutSsdPlacementScope::from_client_members(&client_members);

        let mut last_no_space_ctx: Option<(String, String, u64, u64)> = None; // (node, segment, total, free)

        if let Some(sc) = preferred_sub_cluster {
            let mut preferred_nodes: Vec<NodeID> = client_members
                .iter()
                .filter(|member| {
                    member.sub_cluster.as_deref() == Some(sc)
                        && ssd_scope.allows_node(member.id.as_str())
                })
                .map(|member| member.id.clone().into())
                .collect();

            if preferred_nodes.is_empty() {
                tracing::warn!(
                    "preferred_sub_cluster has no eligible kvclients: sub_cluster={:?} require_ssd={}",
                    sc,
                    ssd_scope.requires_ssd()
                );
            } else {
                if preferred_nodes
                    .iter()
                    .any(|n| n.as_ref() == req_node_id.as_ref())
                {
                    return Ok(PutPlacementTarget::Local {
                        node_id: req_node_id.clone(),
                        persist_to_ssd: ssd_scope.persists_to_ssd(req_node_id.as_ref()),
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
                            persist_to_ssd: ssd_scope.persists_to_ssd(node_id.as_ref()),
                            node_id,
                            allocator,
                            allocation,
                        });
                    }
                }
            }
        }

        // Local-first: prefer placing on the requesting node when possible.
        // This reduces cross-node transfers and enables src==target optimization.
        let local_allocators = seg_manager.get_node_allocators(req_node_id);
        if !local_allocators.is_empty() && ssd_scope.allows_node(req_node_id.as_ref()) {
            return Ok(PutPlacementTarget::Local {
                node_id: req_node_id.clone(),
                persist_to_ssd: ssd_scope.persists_to_ssd(req_node_id.as_ref()),
            });
        }

        for _attempt in 1..=3 {
            let all_segs = seg_manager
                .get_all_segments_allocator()
                .into_iter()
                .filter(|(node_id, _)| ssd_scope.allows_node(node_id.as_ref()))
                .collect::<Vec<_>>();
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
                        persist_to_ssd: ssd_scope.persists_to_ssd(node_id.as_ref()),
                        node_id,
                        allocator,
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
        let client_members = view.cluster_manager().get_client_members();
        let ssd_scope = PutSsdPlacementScope::from_client_members(&client_members);

        let mut last_no_space_ctx: Option<(String, String, u64, u64)> = None; // (node, segment, total, free)

        if let Some(sc) = preferred_sub_cluster {
            let mut preferred_nodes: Vec<NodeID> = client_members
                .iter()
                .filter(|member| {
                    member.sub_cluster.as_deref() == Some(sc)
                        && ssd_scope.allows_node(member.id.as_str())
                })
                .map(|member| member.id.clone().into())
                .collect();

            if preferred_nodes.is_empty() {
                tracing::warn!(
                    "preferred_sub_cluster has no eligible kvclients: sub_cluster={:?} require_ssd={}",
                    sc,
                    ssd_scope.requires_ssd()
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
                            persist_to_ssd: ssd_scope.persists_to_ssd(req_node_id.as_ref()),
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
                            persist_to_ssd: ssd_scope.persists_to_ssd(node_id.as_ref()),
                            node_id,
                            allocator,
                            allocation,
                        });
                    }
                }
            }
        }

        for _attempt in 1..=3 {
            let all_segs = seg_manager
                .get_all_segments_allocator()
                .into_iter()
                .filter(|(node_id, _)| ssd_scope.allows_node(node_id.as_ref()))
                .collect::<Vec<_>>();
            if let Some((nodeid, allocator)) = all_segs.choose(&mut rand::thread_rng()).cloned() {
                let node_id: NodeID = nodeid.into();
                if node_id.as_ref() == req_node_id.as_ref() {
                    let local_allocators = seg_manager.get_node_allocators(req_node_id);
                    if !local_allocators.is_empty() {
                        return Ok(PutPlacementTarget::Local {
                            node_id: req_node_id.clone(),
                            persist_to_ssd: ssd_scope.persists_to_ssd(req_node_id.as_ref()),
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
                        persist_to_ssd: ssd_scope.persists_to_ssd(node_id.as_ref()),
                        node_id,
                        allocator,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_manager::META_KEY_KV_SSD_STORAGE;
    use std::collections::HashMap;

    fn client_member(id: &str, has_ssd: bool) -> ClusterMember {
        let mut metadata = HashMap::from([("client".to_string(), "true".to_string())]);
        if has_ssd {
            metadata.insert(META_KEY_KV_SSD_STORAGE.to_string(), "true".to_string());
        }
        ClusterMember {
            id: id.to_string(),
            addresses: Vec::new(),
            port: None,
            node_start_time: 1,
            metadata,
            sub_cluster: None,
            network: None,
        }
    }

    #[test]
    fn no_ssd_members_keep_memory_only_placement_enabled() {
        let members = vec![
            client_member("owner-a", false),
            client_member("owner-b", false),
        ];
        let scope = PutSsdPlacementScope::from_client_members(&members);

        assert!(!scope.requires_ssd());
        assert!(scope.allows_node("owner-a"));
        assert!(scope.allows_node("owner-b"));
        assert!(!scope.persists_to_ssd("owner-a"));
    }

    #[test]
    fn ssd_members_restrict_targets_to_ssd_capable_owners() {
        let members = vec![
            client_member("owner-memory", false),
            client_member("owner-ssd", true),
        ];
        let scope = PutSsdPlacementScope::from_client_members(&members);

        assert!(scope.requires_ssd());
        assert!(!scope.allows_node("owner-memory"));
        assert!(scope.allows_node("owner-ssd"));
        assert!(scope.persists_to_ssd("owner-ssd"));
    }
}
