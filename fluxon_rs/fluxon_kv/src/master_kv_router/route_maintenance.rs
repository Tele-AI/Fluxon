use super::{MasterKvRouterView, NodeValueReplicaDesc, put::PutIDForAKey};
use crate::cluster_manager::NodeID;
use limit_thirdparty::tokio;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const POST_ROUTE_MAINTENANCE_MAX_BATCH: usize = 512;
const POST_ROUTE_MAINTENANCE_MERGE_WINDOW: Duration = Duration::from_millis(2);

#[derive(Clone, Copy)]
enum RoutePublishKind {
    PrimaryPut,
    ReplicaAppend,
}

pub(super) struct RoutePublishEvent {
    kind: RoutePublishKind,
    key: String,
    put_id: PutIDForAKey,
    lease_id: Option<u64>,
    node_id: NodeID,
    capacity_bytes: u64,
}

impl RoutePublishEvent {
    pub(super) fn primary_put(
        key: String,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
        node_id: NodeID,
        capacity_bytes: u64,
    ) -> Self {
        Self {
            kind: RoutePublishKind::PrimaryPut,
            key,
            put_id,
            lease_id,
            node_id,
            capacity_bytes,
        }
    }

    pub(super) fn replica_append(
        key: String,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
        node_id: NodeID,
        capacity_bytes: u64,
    ) -> Self {
        Self {
            kind: RoutePublishKind::ReplicaAppend,
            key,
            put_id,
            lease_id,
            node_id,
            capacity_bytes,
        }
    }
}

fn saturating_moka_weight_bytes(key: &str, put_id: PutIDForAKey, capacity_bytes: u64) -> u32 {
    if capacity_bytes > u32::MAX as u64 {
        tracing::warn!(
            "moka weight saturation after route publish: key={} put_id=({},{}) cap={}B exceeds u32::MAX; weight set to u32::MAX",
            key,
            put_id.0,
            put_id.1,
            capacity_bytes
        );
        u32::MAX
    } else {
        capacity_bytes as u32
    }
}

fn projected_batch_weight(current_weight: u64, replaced_weight: u64, incoming_weight: u64) -> u64 {
    current_weight
        .saturating_sub(replaced_weight)
        .saturating_add(incoming_weight)
}

fn deduplicate_owner_events(
    events: Vec<(usize, String, NodeValueReplicaDesc)>,
) -> Vec<(String, NodeValueReplicaDesc)> {
    let mut latest_by_key = HashMap::with_capacity(events.len());
    for (sequence, key, desc) in events {
        latest_by_key.insert(key, (sequence, desc));
    }
    let mut latest = latest_by_key.into_iter().collect::<Vec<_>>();
    latest.sort_unstable_by_key(|(_, (sequence, _))| *sequence);
    latest
        .into_iter()
        .map(|(key, (_, desc))| (key, desc))
        .collect()
}

/// Applies index and cache work after route guards have been released.
async fn apply_post_route_maintenance_batch(
    view: &MasterKvRouterView,
    events: Vec<RoutePublishEvent>,
) {
    if view.master_kv_router().prefix_index_enabled()
        && events
            .iter()
            .any(|event| matches!(event.kind, RoutePublishKind::PrimaryPut))
    {
        let inner = view.master_kv_router().inner();
        let mut tree = inner.prefix_index.write().await;
        for event in &events {
            if matches!(event.kind, RoutePublishKind::PrimaryPut) {
                let event_is_current = inner.kv_routes.get(&event.key).is_some_and(|route| {
                    route.put_id == event.put_id
                        && route
                            .nodes_replicas
                            .read()
                            .values()
                            .any(|replica| !replica.tomb_tag.is_tomb())
                });
                if event_is_current {
                    tree.insert(&event.key, event.put_id);
                }
            }
        }
    }

    if !view.master_kv_router().replica_cache_enabled() {
        return;
    }
    let mut events_by_owner = HashMap::<String, Vec<(usize, String, NodeValueReplicaDesc)>>::new();
    for (sequence, event) in events.into_iter().enumerate() {
        if event.lease_id.is_some() {
            continue;
        }
        let weight_bytes =
            saturating_moka_weight_bytes(&event.key, event.put_id, event.capacity_bytes);
        let desc = NodeValueReplicaDesc {
            weight_bytes,
            put_id: event.put_id,
        };
        if !view.master_kv_router().eviction_cache_entry_is_current(
            event.node_id.as_ref(),
            &event.key,
            &desc,
        ) {
            continue;
        }
        events_by_owner
            .entry(event.node_id.as_ref().to_string())
            .or_default()
            .push((sequence, event.key, desc));
    }

    for (owner_node_id, owner_events) in events_by_owner {
        let entries = deduplicate_owner_events(owner_events);
        let Some(cache) = view
            .master_kv_router()
            .get_node_cache_controller(&owner_node_id)
        else {
            tracing::warn!(
                "No cache controller found for node: {}, node is not ready",
                owner_node_id
            );
            continue;
        };

        // Drain prior writes once, then admit this owner batch as one unit. This exposes the
        // aggregate incoming weight to the explicit resident-capacity policy.
        cache.run_pending_tasks();
        let weighted_size_before = cache.weighted_size();
        let replaced_weight = entries
            .iter()
            .filter_map(|(key, _)| cache.get(key))
            .map(|existing| u64::from(existing.weight_bytes))
            .fold(0u64, u64::saturating_add);
        let incoming_weight = entries
            .iter()
            .map(|(_, desc)| u64::from(desc.weight_bytes))
            .fold(0u64, u64::saturating_add);
        let projected_weight =
            projected_batch_weight(weighted_size_before, replaced_weight, incoming_weight);
        let node_space_size = view
            .master_seg_manager()
            .get_node_space_size(&owner_node_id);
        let capacity = view
            .master_kv_router()
            .replica_cache_effective_capacity(&owner_node_id, node_space_size);
        let requested_weight = projected_weight.saturating_sub(capacity);
        let incoming_keys = entries
            .iter()
            .map(|(key, _)| key.clone())
            .collect::<HashSet<_>>();

        let recoverable_selected_weight = view
            .master_kv_router()
            .evict_recoverable_cache_weight_excluding(
                &owner_node_id,
                requested_weight,
                &incoming_keys,
            );
        let fallback_requested_weight =
            requested_weight.saturating_sub(recoverable_selected_weight);
        let fallback_selected_weight = if view
            .master_kv_router()
            .owner_cache_allows_unrecoverable_eviction(&owner_node_id)
        {
            cache.evict_some_if(fallback_requested_weight, |key, _desc| {
                !incoming_keys.contains(key)
            })
        } else {
            0
        };
        if requested_weight != 0 {
            tracing::debug!(
                "recoverable-first batch admission: owner={} entries={} weighted_size_before={} replaced_weight={} incoming_weight={} projected_weight={} capacity={} requested_weight={} recoverable_selected_weight={} fallback_requested_weight={} fallback_selected_weight={} shortfall_weight={}",
                owner_node_id,
                entries.len(),
                weighted_size_before,
                replaced_weight,
                incoming_weight,
                projected_weight,
                capacity,
                requested_weight,
                recoverable_selected_weight,
                fallback_requested_weight,
                fallback_selected_weight,
                fallback_requested_weight.saturating_sub(fallback_selected_weight),
            );
        }

        let tier1_cache = view
            .master_kv_router()
            .get_node_writeback_tier1_controller(&owner_node_id);
        for (key, desc) in entries {
            tracing::debug!("Inserting key: {:?} into cache", key);
            cache.insert(key.clone(), desc.clone());
            if let Some(tier1_cache) = tier1_cache.as_ref() {
                tier1_cache.insert(key.clone(), desc);
            }
            tracing::debug!(
                "Inserted key: {:?} into cache, current cache size: {}",
                key,
                cache.weighted_size()
            );
        }
    }
}

pub(super) fn spawn_post_route_maintenance_actor(
    view: MasterKvRouterView,
    mut rx: tokio::sync::ampsc::Receiver<RoutePublishEvent>,
) {
    let view_task = view.clone();
    view.spawn("post_route_maintenance_actor", async move {
        tracing::info!(
            "post-route maintenance actor started: max_batch={} merge_window_ms={}",
            POST_ROUTE_MAINTENANCE_MAX_BATCH,
            POST_ROUTE_MAINTENANCE_MERGE_WINDOW.as_millis(),
        );
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let first = tokio::select! {
                _ = shutdown_waiter.wait() => break,
                event = rx.recv() => {
                    let Some(event) = event else { break; };
                    event
                }
            };
            let mut events = Vec::with_capacity(POST_ROUTE_MAINTENANCE_MAX_BATCH);
            events.push(first);
            let mut merge_window =
                Box::pin(tokio::time::sleep(POST_ROUTE_MAINTENANCE_MERGE_WINDOW));
            while events.len() < POST_ROUTE_MAINTENANCE_MAX_BATCH {
                tokio::select! {
                    _ = &mut merge_window => break,
                    event = rx.recv() => {
                        let Some(event) = event else { break; };
                        events.push(event);
                    }
                }
            }
            apply_post_route_maintenance_batch(&view_task, events).await;
        }
        tracing::info!("post-route maintenance actor stopped");
    });
}

/// Queues index and cache work with bounded backpressure after route publication.
pub(super) async fn enqueue_post_route_maintenance(
    view: &MasterKvRouterView,
    event: RoutePublishEvent,
) {
    let update_prefix_index = matches!(event.kind, RoutePublishKind::PrimaryPut)
        && view.master_kv_router().prefix_index_enabled();
    let insert_replica_cache =
        event.lease_id.is_none() && view.master_kv_router().replica_cache_enabled();
    if !update_prefix_index && !insert_replica_cache {
        return;
    }
    view.master_kv_router()
        .inner()
        .post_route_maintenance_tx
        .send(event)
        .await
        .expect("post-route maintenance actor stopped while master is serving requests");
}

#[cfg(test)]
mod tests {
    use super::{deduplicate_owner_events, projected_batch_weight, saturating_moka_weight_bytes};
    use crate::master_kv_router::NodeValueReplicaDesc;

    #[test]
    fn moka_weight_saturates_without_truncating() {
        assert_eq!(
            saturating_moka_weight_bytes("key", (1, 2), u32::MAX as u64),
            u32::MAX
        );
        assert_eq!(
            saturating_moka_weight_bytes("key", (1, 2), u32::MAX as u64 + 1),
            u32::MAX
        );
    }

    #[test]
    fn projected_weight_accounts_for_batch_replacements_once() {
        assert_eq!(projected_batch_weight(90, 30, 50), 110);
        assert_eq!(projected_batch_weight(10, 20, 5), 5);
    }

    #[test]
    fn owner_batch_keeps_only_the_last_event_for_each_key() {
        let desc = |weight_bytes, put_id| NodeValueReplicaDesc {
            weight_bytes,
            put_id,
        };
        let events = vec![
            (0, "a".to_string(), desc(10, (1, 0))),
            (1, "b".to_string(), desc(20, (1, 0))),
            (2, "a".to_string(), desc(30, (2, 0))),
        ];

        let deduplicated = deduplicate_owner_events(events);
        assert_eq!(deduplicated.len(), 2);
        assert_eq!(deduplicated[0].0, "b");
        assert_eq!(deduplicated[1].0, "a");
        assert_eq!(deduplicated[1].1.weight_bytes, 30);
        assert_eq!(deduplicated[1].1.put_id, (2, 0));
    }
}
