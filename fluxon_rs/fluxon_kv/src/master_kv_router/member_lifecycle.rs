use super::{
    InflightPutInfo, InflightPutKey, MasterKvRouterView, OneKvNodesRoutes, PutIDForAKey,
    cleanup_inflight_gets_for_member,
};
use crate::cluster_manager::{ClusterEvent, ClusterMember, NodeID, NodeIDString, NodeRole};
use dashmap::{DashMap, mapref::entry::Entry};
use limit_thirdparty::tokio::{self, sync::ampsc};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tracing::warn;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(2);
const SEND_BACKPRESSURE_WARN_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub(super) enum MemberTransition {
    Present(ClusterMember),
    Absent {
        node_id: NodeIDString,
        generation: i64,
    },
}

pub(super) struct MemberCleanupStats {
    pub(super) inflight_puts_removed: usize,
    pub(super) inflight_gets_released: usize,
    pub(super) inflight_gets_deferred: usize,
    pub(super) holdings_removed: usize,
    pub(super) replicas_removed: usize,
    pub(super) routes_removed: usize,
    pub(super) cache_removed: bool,
    pub(super) reservation_state_removed: bool,
}

impl MemberTransition {
    pub(super) fn node_id(&self) -> NodeIDString {
        match self {
            Self::Present(member) => member.id.clone(),
            Self::Absent { node_id, .. } => node_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReplicaIndexKey {
    key: String,
    put_id: PutIDForAKey,
}

#[derive(Clone)]
pub(super) struct IndexedReplica {
    key: String,
    put_id: PutIDForAKey,
    generation: i64,
    route: Weak<OneKvNodesRoutes>,
}

struct MemberReplicaState {
    generation: Option<i64>,
    departed: bool,
    replicas: HashMap<ReplicaIndexKey, Weak<OneKvNodesRoutes>>,
}

struct MemberReplicaBucket {
    state: Mutex<MemberReplicaState>,
}

#[derive(Default)]
pub(super) struct MemberReplicaIndex {
    by_member: DashMap<NodeIDString, Arc<MemberReplicaBucket>>,
}

impl MemberReplicaBucket {
    fn active(generation: i64) -> Self {
        Self {
            state: Mutex::new(MemberReplicaState {
                generation: Some(generation),
                departed: false,
                replicas: HashMap::new(),
            }),
        }
    }

    fn departed(generation: i64) -> Self {
        Self {
            state: Mutex::new(MemberReplicaState {
                generation: Some(generation),
                departed: true,
                replicas: HashMap::new(),
            }),
        }
    }
}

impl MemberReplicaIndex {
    pub(super) fn observe_present(&self, node_id: &str, generation: i64) -> Vec<IndexedReplica> {
        let bucket = match self.by_member.entry(node_id.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let bucket = Arc::new(MemberReplicaBucket::active(generation));
                entry.insert(Arc::clone(&bucket));
                return Vec::new();
            }
        };
        let mut state = bucket.state.lock();
        if state.generation == Some(generation) {
            state.departed = false;
            return Vec::new();
        }

        let old_generation = state.generation;
        let displaced = state
            .replicas
            .drain()
            .filter_map(|(key, route)| {
                old_generation.map(|generation| IndexedReplica {
                    key: key.key,
                    put_id: key.put_id,
                    generation,
                    route,
                })
            })
            .collect();
        state.generation = Some(generation);
        state.departed = false;
        displaced
    }

    pub(super) fn register_if_active(
        &self,
        node_id: &NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
    ) -> bool {
        let bucket = match self.by_member.entry(node_id.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let bucket = Arc::new(MemberReplicaBucket::active(generation));
                entry.insert(Arc::clone(&bucket));
                bucket
            }
        };
        let mut state = bucket.state.lock();
        if state.departed || state.generation != Some(generation) {
            return false;
        }
        state.replicas.insert(
            ReplicaIndexKey {
                key: key.to_string(),
                put_id: route.put_id,
            },
            Arc::downgrade(route),
        );
        true
    }

    pub(super) fn contains_current(
        &self,
        node_id: &NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
    ) -> bool {
        let Some(bucket) = self
            .by_member
            .get(node_id.as_ref())
            .map(|entry| Arc::clone(entry.value()))
        else {
            return false;
        };
        let state = bucket.state.lock();
        !state.departed
            && state.generation == Some(generation)
            && state
                .replicas
                .get(&ReplicaIndexKey {
                    key: key.to_string(),
                    put_id: route.put_id,
                })
                .is_some_and(|indexed| indexed.ptr_eq(&Arc::downgrade(route)))
    }

    pub(super) fn remove_current(
        &self,
        node_id: &NodeID,
        generation: i64,
        key: &str,
        route: &Arc<OneKvNodesRoutes>,
    ) {
        let Some(bucket) = self
            .by_member
            .get(node_id.as_ref())
            .map(|entry| Arc::clone(entry.value()))
        else {
            return;
        };
        let mut state = bucket.state.lock();
        if state.generation != Some(generation) {
            return;
        }
        let index_key = ReplicaIndexKey {
            key: key.to_string(),
            put_id: route.put_id,
        };
        if state
            .replicas
            .get(&index_key)
            .is_some_and(|indexed| indexed.ptr_eq(&Arc::downgrade(route)))
        {
            state.replicas.remove(&index_key);
        }
    }

    pub(super) fn mark_absent(&self, node_id: &str, generation: i64) -> Vec<IndexedReplica> {
        let bucket = match self.by_member.entry(node_id.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(MemberReplicaBucket::departed(generation)));
                return Vec::new();
            }
        };
        let mut state = bucket.state.lock();
        if state.generation != Some(generation) {
            return Vec::new();
        }
        state.departed = true;
        state
            .replicas
            .drain()
            .map(|(key, route)| IndexedReplica {
                key: key.key,
                put_id: key.put_id,
                generation,
                route,
            })
            .collect()
    }
}

#[derive(Clone)]
pub(super) struct IndexedInflightPut {
    pub(super) key: InflightPutKey,
    pub(super) info: Weak<InflightPutInfo>,
}

struct MemberInflightPutState {
    generation: i64,
    departed: bool,
    by_put_key: HashMap<InflightPutKey, Weak<InflightPutInfo>>,
}

struct MemberInflightPutBucket {
    state: Mutex<MemberInflightPutState>,
}

#[derive(Default)]
pub(super) struct MemberInflightPutIndex {
    by_member: DashMap<NodeIDString, Arc<MemberInflightPutBucket>>,
}

impl MemberInflightPutBucket {
    fn active(generation: i64) -> Self {
        Self {
            state: Mutex::new(MemberInflightPutState {
                generation,
                departed: false,
                by_put_key: HashMap::new(),
            }),
        }
    }

    fn departed(generation: i64) -> Self {
        Self {
            state: Mutex::new(MemberInflightPutState {
                generation,
                departed: true,
                by_put_key: HashMap::new(),
            }),
        }
    }
}

impl MemberInflightPutIndex {
    fn bucket_for_admission(
        &self,
        member_id: &str,
        generation: i64,
    ) -> Arc<MemberInflightPutBucket> {
        match self.by_member.entry(member_id.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                let bucket = Arc::new(MemberInflightPutBucket::active(generation));
                entry.insert(Arc::clone(&bucket));
                bucket
            }
        }
    }

    pub(super) fn register_if_active(
        &self,
        key: &InflightPutKey,
        info: &Arc<InflightPutInfo>,
    ) -> bool {
        let mut registered = Vec::new();
        for participant in info.participants() {
            let bucket = self.bucket_for_admission(&participant.member_id, participant.generation);
            let mut state = bucket.state.lock();
            if state.departed || state.generation != participant.generation {
                drop(state);
                self.unregister_from_members(key, info, registered);
                return false;
            }
            let previous = state.by_put_key.insert(key.clone(), Arc::downgrade(info));
            assert!(
                previous.is_none(),
                "duplicate inflight PUT key in member lifecycle index: {key:?}"
            );
            registered.push(participant.member_id);
        }
        true
    }

    fn unregister_from_members(
        &self,
        key: &InflightPutKey,
        info: &Arc<InflightPutInfo>,
        member_ids: Vec<NodeIDString>,
    ) {
        let expected = Arc::downgrade(info);
        for member_id in member_ids {
            let Some(bucket) = self
                .by_member
                .get(&member_id)
                .map(|entry| Arc::clone(entry.value()))
            else {
                continue;
            };
            let mut state = bucket.state.lock();
            if state
                .by_put_key
                .get(key)
                .is_some_and(|current| current.ptr_eq(&expected))
            {
                state.by_put_key.remove(key);
            }
        }
    }

    pub(super) fn unregister(&self, key: &InflightPutKey, info: &Arc<InflightPutInfo>) {
        self.unregister_from_members(
            key,
            info,
            info.participants()
                .into_iter()
                .map(|participant| participant.member_id)
                .collect(),
        );
    }

    pub(super) fn contains_current(
        &self,
        key: &InflightPutKey,
        info: &Arc<InflightPutInfo>,
    ) -> bool {
        let expected = Arc::downgrade(info);
        info.participants().into_iter().all(|participant| {
            let Some(bucket) = self
                .by_member
                .get(&participant.member_id)
                .map(|entry| Arc::clone(entry.value()))
            else {
                return false;
            };
            let state = bucket.state.lock();
            !state.departed
                && state.generation == participant.generation
                && state
                    .by_put_key
                    .get(key)
                    .is_some_and(|current| current.ptr_eq(&expected))
        })
    }

    pub(super) fn observe_present(
        &self,
        member_id: &str,
        generation: i64,
    ) -> Vec<IndexedInflightPut> {
        let bucket = match self.by_member.entry(member_id.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(MemberInflightPutBucket::active(generation)));
                return Vec::new();
            }
        };
        let mut state = bucket.state.lock();
        if state.generation == generation {
            state.departed = false;
            return Vec::new();
        }
        let displaced = state
            .by_put_key
            .drain()
            .map(|(key, info)| IndexedInflightPut { key, info })
            .collect();
        state.generation = generation;
        state.departed = false;
        displaced
    }

    pub(super) fn mark_absent(&self, member_id: &str, generation: i64) -> Vec<IndexedInflightPut> {
        let bucket = match self.by_member.entry(member_id.to_string()) {
            Entry::Occupied(entry) => Arc::clone(entry.get()),
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(MemberInflightPutBucket::departed(generation)));
                return Vec::new();
            }
        };
        let mut state = bucket.state.lock();
        if state.generation != generation {
            return Vec::new();
        }
        state.departed = true;
        state
            .by_put_key
            .drain()
            .map(|(key, info)| IndexedInflightPut { key, info })
            .collect()
    }
}

pub(super) async fn cleanup_indexed_replicas(
    view: &MasterKvRouterView,
    node_id: &NodeID,
    replicas: Vec<IndexedReplica>,
) -> (usize, usize) {
    let mut replicas_removed = 0usize;
    let mut routes_removed = Vec::new();

    for indexed in replicas {
        let Some(route) = indexed.route.upgrade() else {
            continue;
        };
        if route.put_id != indexed.put_id
            || !route.remove_node_replicas_if_generation(node_id, indexed.generation)
        {
            continue;
        }
        replicas_removed += 1;
        if route.has_live_replica() {
            continue;
        }
        let route_for_compare = Arc::clone(&route);
        if view
            .master_kv_router()
            .inner()
            .kv_routes
            .remove_if(&indexed.key, |_, current| {
                Arc::ptr_eq(current, &route_for_compare)
                    && current.put_id == indexed.put_id
                    && !current.has_live_replica()
            })
            .is_some()
        {
            view.master_kv_router()
                .unregister_route_replicas(&indexed.key, &route);
            routes_removed.push(indexed.key);
        }
    }

    let routes_removed_count = routes_removed.len();
    if routes_removed_count > 0 && view.master_kv_router().prefix_index_enabled() {
        let inner = view.master_kv_router().inner();
        let mut tree = inner.prefix_index.write().await;
        for key in routes_removed {
            if !inner.kv_routes.contains_key(&key) {
                tree.remove(&key);
            }
        }
    }

    (replicas_removed, routes_removed_count)
}

async fn cleanup_indexed_inflight_puts(
    view: &MasterKvRouterView,
    indexed_puts: Vec<IndexedInflightPut>,
) -> usize {
    let mut removed = 0usize;
    for indexed in indexed_puts {
        if view
            .master_kv_router()
            .cleanup_indexed_inflight_put(&indexed)
            .await
        {
            removed += 1;
        }
    }
    removed
}

pub(super) async fn apply_present(view: &MasterKvRouterView, member: &ClusterMember) {
    let inner = view.master_kv_router().inner();
    let displaced = inner
        .member_binded_state_ctrl
        .observe_present(&member.id, member.node_start_time);
    let displaced_gets =
        inner.observe_inflight_member_generation(&member.id, member.node_start_time, true);
    let holdings_removed = inner
        .get_holding
        .mark_member_active(&member.id, member.node_start_time);

    let inflight_puts_removed = cleanup_indexed_inflight_puts(view, displaced.inflight_puts).await;
    let (inflight_gets_released, inflight_gets_deferred) =
        cleanup_inflight_gets_for_member(view, &member.id, displaced_gets).await;

    if !displaced.replicas.is_empty() {
        let node_id: NodeID = member.id.clone().into();
        let (replicas_removed, routes_removed) =
            cleanup_indexed_replicas(view, &node_id, displaced.replicas).await;
        warn!(
            member = %member.id,
            generation = member.node_start_time,
            replicas_removed,
            routes_removed,
            "Cleaned stale replicas before activating a new member generation"
        );
    }

    if inflight_puts_removed > 0
        || inflight_gets_released > 0
        || inflight_gets_deferred > 0
        || holdings_removed > 0
    {
        warn!(
            member = %member.id,
            generation = member.node_start_time,
            inflight_puts_removed,
            inflight_gets_released,
            inflight_gets_deferred,
            holdings_removed,
            "Cleaned stale member-bound state before activating a new generation"
        );
    }
}

pub(super) async fn apply_absent(
    view: &MasterKvRouterView,
    node_id: &str,
    generation: i64,
) -> MemberCleanupStats {
    let inner = view.master_kv_router().inner();
    let indexed = inner
        .member_binded_state_ctrl
        .mark_absent(node_id, generation);
    let inflight_gets = inner.mark_inflight_member_left(node_id, generation);
    let holdings_removed = inner
        .get_holding
        .mark_member_left_and_cleanup(node_id, generation);
    let cache_removed = inner.node_kv_cache_controller.remove(node_id).is_some();
    let reservation_state_removed = inner.cache_reserved_bytes.remove(node_id).is_some();

    let inflight_puts_removed = cleanup_indexed_inflight_puts(view, indexed.inflight_puts).await;
    let (inflight_gets_released, inflight_gets_deferred) =
        cleanup_inflight_gets_for_member(view, node_id, inflight_gets).await;
    let node: NodeID = node_id.to_string().into();
    let (replicas_removed, routes_removed) =
        cleanup_indexed_replicas(view, &node, indexed.replicas).await;

    MemberCleanupStats {
        inflight_puts_removed,
        inflight_gets_released,
        inflight_gets_deferred,
        holdings_removed,
        replicas_removed,
        routes_removed,
        cache_removed,
        reservation_state_removed,
    }
}

async fn send_transition_with_warn(
    node_id: &str,
    tx: ampsc::Sender<MemberTransition>,
    transition: MemberTransition,
) -> Result<(), MemberTransition> {
    let mut send_fut = Box::pin(tx.send(transition.clone()));
    let mut warn_sleep = Box::pin(tokio::time::sleep(SEND_BACKPRESSURE_WARN_INTERVAL));

    loop {
        tokio::select! {
            result = &mut send_fut => {
                return match result {
                    Ok(()) => Ok(()),
                    Err(_) => Err(transition),
                };
            }
            _ = &mut warn_sleep => {
                warn!(member = node_id, ?transition, "Waiting to deliver member transition");
                warn_sleep = Box::pin(tokio::time::sleep(SEND_BACKPRESSURE_WARN_INTERVAL));
            }
        }
    }
}

async fn dispatch_transition(
    view: &MasterKvRouterView,
    actors: &mut HashMap<NodeIDString, ampsc::Sender<MemberTransition>>,
    transition: MemberTransition,
) {
    let node_id = transition.node_id();
    loop {
        let tx = actors
            .entry(node_id.clone())
            .or_insert_with(|| {
                view.master_kv_router()
                    .spawn_node_segment_registration_actor()
            })
            .clone();
        match send_transition_with_warn(&node_id, tx, transition.clone()).await {
            Ok(()) => return,
            Err(_) => {
                actors.insert(
                    node_id.clone(),
                    view.master_kv_router()
                        .spawn_node_segment_registration_actor(),
                );
            }
        }
    }
}

async fn dispatch_present(
    view: &MasterKvRouterView,
    actors: &mut HashMap<NodeIDString, ampsc::Sender<MemberTransition>>,
    observed: &mut HashMap<NodeIDString, ClusterMember>,
    member: ClusterMember,
) {
    if let Some(previous) = observed.get(&member.id) {
        if previous == &member {
            return;
        }
        if previous.node_start_time != member.node_start_time {
            dispatch_transition(
                view,
                actors,
                MemberTransition::Absent {
                    node_id: previous.id.clone(),
                    generation: previous.node_start_time,
                },
            )
            .await;
        }
    }
    observed.insert(member.id.clone(), member.clone());
    dispatch_transition(view, actors, MemberTransition::Present(member)).await;
}

async fn reconcile_snapshot(
    view: &MasterKvRouterView,
    actors: &mut HashMap<NodeIDString, ampsc::Sender<MemberTransition>>,
    observed: &mut HashMap<NodeIDString, ClusterMember>,
) {
    let current = view
        .cluster_manager()
        .get_client_members()
        .into_iter()
        .map(|member| (member.id.clone(), member))
        .collect::<HashMap<_, _>>();

    let departed = observed
        .iter()
        .filter(|(node_id, _)| !current.contains_key(*node_id))
        .map(|(_, member)| member.clone())
        .collect::<Vec<_>>();
    for member in departed {
        observed.remove(&member.id);
        dispatch_transition(
            view,
            actors,
            MemberTransition::Absent {
                node_id: member.id,
                generation: member.node_start_time,
            },
        )
        .await;
    }

    for member in current.into_values() {
        dispatch_present(view, actors, observed, member).await;
    }
}

pub(super) fn spawn(view: MasterKvRouterView) {
    let view_task = view.clone();
    let _ = view.spawn("member_lifecycle", async move {
        let mut events = view_task.cluster_manager().listen();
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let mut actors: HashMap<NodeIDString, ampsc::Sender<MemberTransition>> = HashMap::new();
        let mut observed = HashMap::new();

        reconcile_snapshot(&view_task, &mut actors, &mut observed).await;
        let mut reconcile_sleep = Box::pin(tokio::time::sleep(RECONCILE_INTERVAL));

        loop {
            tokio::select! {
                _ = &mut reconcile_sleep => {
                    reconcile_snapshot(&view_task, &mut actors, &mut observed).await;
                    reconcile_sleep = Box::pin(tokio::time::sleep(RECONCILE_INTERVAL));
                }
                event = events.recv() => {
                    match event {
                        Ok(ClusterEvent::MemberJoined(member) | ClusterEvent::MemberUpdated(member)) => {
                            if let Some(current) = view_task
                                .cluster_manager()
                                .get_member_info_cached(&member.id)
                                .filter(|member| matches!(member.node_role(), NodeRole::Client))
                            {
                                dispatch_present(
                                    &view_task,
                                    &mut actors,
                                    &mut observed,
                                    current,
                                )
                                .await;
                            }
                        }
                        Ok(ClusterEvent::MemberLeft(node_id)) => {
                            if let Some(current) = view_task
                                .cluster_manager()
                                .get_member_info_cached(&node_id)
                                .filter(|member| matches!(member.node_role(), NodeRole::Client))
                            {
                                dispatch_present(
                                    &view_task,
                                    &mut actors,
                                    &mut observed,
                                    current,
                                )
                                .await;
                                continue;
                            }
                            let previous = observed
                                .remove(&node_id)
                                .or_else(|| view_task.cluster_manager().get_prev_member_info(&node_id));
                            if let Some(previous) = previous {
                                dispatch_transition(
                                    &view_task,
                                    &mut actors,
                                    MemberTransition::Absent {
                                        node_id,
                                        generation: previous.node_start_time,
                                    },
                                )
                                .await;
                            }
                        }
                        Err(err) => {
                            warn!(?err, "Cluster event receiver failed; resubscribing");
                            events = view_task.cluster_manager().listen();
                        }
                    }
                }
                _ = shutdown_waiter.wait() => break,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_inflight_put(
        requester: (&str, i64),
        source: (&str, i64),
        target: (&str, i64),
    ) -> Arc<InflightPutInfo> {
        Arc::new(InflightPutInfo {
            target_node_id: target.0.to_string().into(),
            source_node_id: source.0.to_string().into(),
            key: "key-a".to_string(),
            req_node_id: requester.0.to_string().into(),
            target_generation: target.1,
            source_generation: source.1,
            requester_generation: requester.1,
            len: 512,
            persist_to_ssd: false,
            src_target_allocation: Arc::new(Mutex::new(None)),
        })
    }

    #[test]
    fn absent_generation_drains_replicas_and_rejects_late_admission() {
        let index = MemberReplicaIndex::default();
        let node: NodeID = "owner-a".to_string().into();
        let route = Arc::new(OneKvNodesRoutes::new((10, 1), None));

        assert!(index.register_if_active(&node, 7, "key-a", &route));
        let removed = index.mark_absent(node.as_ref(), 7);
        assert_eq!(removed.len(), 1);
        assert!(!index.register_if_active(&node, 7, "late-key", &route));
    }

    #[test]
    fn stale_absence_cannot_drain_new_generation() {
        let index = MemberReplicaIndex::default();
        let node: NodeID = "owner-a".to_string().into();
        let old_route = Arc::new(OneKvNodesRoutes::new((10, 1), None));
        let new_route = Arc::new(OneKvNodesRoutes::new((11, 0), None));

        assert!(index.register_if_active(&node, 7, "old-key", &old_route));
        assert_eq!(index.observe_present(node.as_ref(), 8).len(), 1);
        assert!(index.register_if_active(&node, 8, "new-key", &new_route));
        assert!(index.mark_absent(node.as_ref(), 7).is_empty());
        assert!(index.contains_current(&node, 8, "new-key", &new_route));
    }

    #[test]
    fn inflight_put_departure_rejects_late_admission() {
        let index = MemberInflightPutIndex::default();
        let key = ("key-a".to_string(), 10, 0);
        let info = test_inflight_put(("requester", 1), ("source", 2), ("target", 3));

        assert!(index.register_if_active(&key, &info));
        let removed = index.mark_absent("source", 2);
        assert_eq!(removed.len(), 1);
        let late_key = ("late-key".to_string(), 11, 0);
        let late = test_inflight_put(("requester", 1), ("source", 2), ("target", 3));
        assert!(!index.register_if_active(&late_key, &late));
    }

    #[test]
    fn stale_absence_cannot_drain_new_inflight_put_generation() {
        let index = MemberInflightPutIndex::default();
        let old_key = ("old-key".to_string(), 10, 0);
        let old = test_inflight_put(("requester", 1), ("source", 2), ("target", 3));
        assert!(index.register_if_active(&old_key, &old));
        assert_eq!(index.observe_present("source", 4).len(), 1);

        let new_key = ("new-key".to_string(), 11, 0);
        let new = test_inflight_put(("requester", 1), ("source", 4), ("target", 3));
        assert!(index.register_if_active(&new_key, &new));
        assert!(index.mark_absent("source", 2).is_empty());
        assert!(index.contains_current(&new_key, &new));
    }
}
