use std::sync::Arc;

use crate::{
    cluster_manager::NodeID,
    master_seg_manager::one_seg_allocator::{Allocation, OneSegAllocator},
    rpcresp_kvresult_convert::msg_and_error::KvError,
};
use async_trait::async_trait;
use rand::seq::SliceRandom;
use rand::Rng;

use super::MasterKvRouterView;

pub enum PutPlacementTarget {
    /// Place locally by reusing the requester's src allocation as the target.
    Local { node_id: NodeID },
    /// Place remotely with a pre-allocated target allocation.
    Remote {
        node_id: NodeID,
        allocator: Arc<OneSegAllocator>,
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
}

/// Compile-time switch for the master placement default.
///
/// Change the type alias to switch behavior (and rebuild):
/// - `LocalFirstPlacementPolicy` prefers local placement when possible.
/// - `RandomPlacementPolicy` selects a random eligible target.
pub type PlacementDefault = LocalFirstPlacementPolicy;
// pub type PlacementDefault = RandomPlacementPolicy;

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
                            allocator,
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
