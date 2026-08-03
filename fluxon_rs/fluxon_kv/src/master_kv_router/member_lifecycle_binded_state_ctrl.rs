use super::member_lifecycle::{
    IndexedInflightPut, IndexedReplica, MemberInflightPutIndex, MemberReplicaIndex,
};
use super::{InflightPutInfo, InflightPutKey, OneKvNodesRoutes};
use crate::cluster_manager::NodeID;
use crate::master_seg_manager::NodeTombTag;
use crate::master_seg_manager::one_seg_allocator::Allocation;
use std::sync::Arc;

pub(super) struct MemberBindedStateTransition {
    pub(super) inflight_puts: Vec<IndexedInflightPut>,
    pub(super) replicas: Vec<IndexedReplica>,
}

/// Admission authority for route and PUT state bound to a member generation.
#[derive(Default)]
pub(super) struct MemberLifecycleBindedStateCtrl {
    inflight_puts: MemberInflightPutIndex,
    replicas: MemberReplicaIndex,
}

impl MemberLifecycleBindedStateCtrl {
    pub(super) fn observe_present(
        &self,
        member_id: &str,
        generation: i64,
    ) -> MemberBindedStateTransition {
        MemberBindedStateTransition {
            inflight_puts: self.inflight_puts.observe_present(member_id, generation),
            replicas: self.replicas.observe_present(member_id, generation),
        }
    }

    pub(super) fn mark_absent(
        &self,
        member_id: &str,
        generation: i64,
    ) -> MemberBindedStateTransition {
        // Stop both indexed admission paths before cleanup starts.
        MemberBindedStateTransition {
            inflight_puts: self.inflight_puts.mark_absent(member_id, generation),
            replicas: self.replicas.mark_absent(member_id, generation),
        }
    }

    pub(super) fn register_inflight_put_if_active(
        &self,
        key: &InflightPutKey,
        info: &Arc<InflightPutInfo>,
    ) -> bool {
        self.inflight_puts.register_if_active(key, info)
    }

    pub(super) fn unregister_inflight_put(
        &self,
        key: &InflightPutKey,
        info: &Arc<InflightPutInfo>,
    ) {
        self.inflight_puts.unregister(key, info);
    }

    pub(super) fn contains_current_inflight_put(
        &self,
        key: &InflightPutKey,
        info: &Arc<InflightPutInfo>,
    ) -> bool {
        self.inflight_puts.contains_current(key, info)
    }

    pub(super) fn register_replica_if_active(
        &self,
        node_id: &NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
    ) -> bool {
        self.replicas
            .register_if_active(node_id, generation, key, route)
    }

    pub(super) fn contains_current_replica(
        &self,
        node_id: &NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
    ) -> bool {
        self.replicas
            .contains_current(node_id, generation, key, route)
    }

    pub(super) fn remove_current_replica(
        &self,
        node_id: &NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
    ) {
        self.replicas
            .remove_current(node_id, generation, key, route);
    }

    pub(super) fn publish_memory_replica(
        &self,
        node_id: NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
        allocation: Arc<Allocation>,
        tomb_tag: NodeTombTag,
    ) -> bool {
        if tomb_tag.is_tomb() || !self.register_replica_if_active(&node_id, generation, key, route)
        {
            return false;
        }

        route.insert_memory_replica(
            node_id.clone(),
            generation,
            Arc::clone(&allocation),
            tomb_tag,
        );
        if self.contains_current_replica(&node_id, generation, key, route) {
            return true;
        }

        route.remove_memory_replica_if_allocation(&node_id, generation, &allocation);
        false
    }
}
