//! Read-only Radix lineage accounting over the authoritative route table.
//!
//! This module deliberately does not participate in victim selection. It
//! measures how much live physical memory is attached to pages that cannot be
//! reached through a complete root-to-page prefix. A later leaf-first policy
//! must pass an experiment gate based on these counters before changing Moka.

use super::OneKvNodesRoutes;
use crate::master_kv_router::msg_pack::RadixKvMetadata;
use dashmap::DashMap;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RadixShadowOwnerObserveSnapshot {
    pub owner_node: String,
    pub memory_entries: u64,
    pub memory_capacity_bytes: u64,
    pub described_memory_entries: u64,
    pub described_memory_capacity_bytes: u64,
    pub reachable_memory_entries: u64,
    pub reachable_memory_capacity_bytes: u64,
    pub orphan_memory_entries: u64,
    pub orphan_memory_capacity_bytes: u64,
    pub unresolved_memory_entries: u64,
    pub unresolved_memory_capacity_bytes: u64,
    pub leaf_memory_entries: u64,
    pub leaf_memory_capacity_bytes: u64,
    pub internal_memory_entries: u64,
    pub internal_memory_capacity_bytes: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RadixShadowObserveSnapshot {
    pub route_entries: u64,
    pub described_route_entries: u64,
    pub unknown_route_entries: u64,
    pub root_route_entries: u64,
    pub reachable_route_entries: u64,
    pub orphan_route_entries: u64,
    pub unresolved_route_entries: u64,
    pub direct_gap_route_entries: u64,
    pub leaf_route_entries: u64,
    pub internal_route_entries: u64,
    pub described_logical_bytes: u64,
    pub reachable_logical_bytes: u64,
    pub orphan_logical_bytes: u64,
    pub unresolved_logical_bytes: u64,
    pub owners: Vec<RadixShadowOwnerObserveSnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Reachability {
    Unknown,
    Reachable,
    Orphan,
}

#[derive(Clone, Debug)]
struct RouteShadow {
    key: String,
    radix: Option<RadixKvMetadata>,
    logical_bytes: u64,
    memory_backings: Vec<(String, u64)>,
}

pub fn observe_radix_shadow(
    routes: &DashMap<String, Arc<OneKvNodesRoutes>>,
) -> RadixShadowObserveSnapshot {
    let route_snapshots = routes
        .iter()
        .map(|entry| {
            let route = entry.value();
            let replicas = route.node_replicas.read();
            let mut logical_bytes = 0u64;
            let mut memory_backings = Vec::new();
            for (owner, node_replicas) in replicas.iter() {
                if node_replicas.tomb_tag.is_tomb() {
                    continue;
                }
                if let Some(memory) = node_replicas.memory.as_ref() {
                    logical_bytes = logical_bytes.max(memory.backing.len());
                    memory_backings.push((owner.to_string(), memory.backing.capacity_bytes()));
                }
                if let Some(ssd) = node_replicas.ssd.as_ref() {
                    logical_bytes = logical_bytes.max(ssd.len);
                }
            }
            RouteShadow {
                key: entry.key().clone(),
                radix: route.radix.clone(),
                logical_bytes,
                memory_backings,
            }
        })
        .collect();
    observe_route_snapshots(route_snapshots)
}

fn observe_route_snapshots(routes: Vec<RouteShadow>) -> RadixShadowObserveSnapshot {
    let mut snapshot = RadixShadowObserveSnapshot {
        route_entries: u64::try_from(routes.len()).unwrap_or(u64::MAX),
        ..Default::default()
    };
    let key_to_index = routes
        .iter()
        .enumerate()
        .map(|(index, route)| (route.key.as_str(), index))
        .collect::<HashMap<_, _>>();
    let referenced_parents = routes
        .iter()
        .filter_map(|route| route.radix.as_ref()?.parent_key.as_deref())
        .collect::<HashSet<_>>();
    let mut reachability = vec![Reachability::Unknown; routes.len()];
    let mut direct_gap = vec![false; routes.len()];
    let mut described_indices = routes
        .iter()
        .enumerate()
        .filter_map(|(index, route)| route.radix.as_ref().map(|radix| (radix.depth, index)))
        .collect::<Vec<_>>();
    described_indices.sort_unstable_by_key(|(depth, _)| *depth);

    for (_, index) in described_indices.iter().copied() {
        let radix = routes[index]
            .radix
            .as_ref()
            .expect("described index must retain Radix metadata");
        reachability[index] = if radix.depth == 0 && radix.parent_key.is_none() {
            Reachability::Reachable
        } else if let Some(parent_key) = radix.parent_key.as_deref() {
            match key_to_index.get(parent_key).copied() {
                None => {
                    direct_gap[index] = true;
                    Reachability::Orphan
                }
                Some(parent_index) => match routes[parent_index].radix.as_ref() {
                    Some(parent_radix)
                        if parent_radix.depth.checked_add(1) == Some(radix.depth) =>
                    {
                        match reachability[parent_index] {
                            Reachability::Reachable => Reachability::Reachable,
                            Reachability::Orphan => Reachability::Orphan,
                            Reachability::Unknown => Reachability::Unknown,
                        }
                    }
                    _ => Reachability::Unknown,
                },
            }
        } else {
            Reachability::Unknown
        };
    }

    let mut owners = BTreeMap::<String, RadixShadowOwnerObserveSnapshot>::new();
    for (index, route) in routes.iter().enumerate() {
        let described = route.radix.is_some();
        let is_internal = described && referenced_parents.contains(route.key.as_str());
        if described {
            snapshot.described_route_entries = snapshot.described_route_entries.saturating_add(1);
            snapshot.described_logical_bytes = snapshot
                .described_logical_bytes
                .saturating_add(route.logical_bytes);
            if route.radix.as_ref().is_some_and(|radix| radix.depth == 0) {
                snapshot.root_route_entries = snapshot.root_route_entries.saturating_add(1);
            }
            if is_internal {
                snapshot.internal_route_entries = snapshot.internal_route_entries.saturating_add(1);
            } else {
                snapshot.leaf_route_entries = snapshot.leaf_route_entries.saturating_add(1);
            }
            match reachability[index] {
                Reachability::Reachable => {
                    snapshot.reachable_route_entries =
                        snapshot.reachable_route_entries.saturating_add(1);
                    snapshot.reachable_logical_bytes = snapshot
                        .reachable_logical_bytes
                        .saturating_add(route.logical_bytes);
                }
                Reachability::Orphan => {
                    snapshot.orphan_route_entries = snapshot.orphan_route_entries.saturating_add(1);
                    snapshot.orphan_logical_bytes = snapshot
                        .orphan_logical_bytes
                        .saturating_add(route.logical_bytes);
                }
                Reachability::Unknown => {
                    snapshot.unresolved_route_entries =
                        snapshot.unresolved_route_entries.saturating_add(1);
                    snapshot.unresolved_logical_bytes = snapshot
                        .unresolved_logical_bytes
                        .saturating_add(route.logical_bytes);
                }
            }
            if direct_gap[index] {
                snapshot.direct_gap_route_entries =
                    snapshot.direct_gap_route_entries.saturating_add(1);
            }
        } else {
            snapshot.unknown_route_entries = snapshot.unknown_route_entries.saturating_add(1);
        }

        for (owner_node, capacity_bytes) in &route.memory_backings {
            let owner = owners.entry(owner_node.clone()).or_insert_with(|| {
                RadixShadowOwnerObserveSnapshot {
                    owner_node: owner_node.clone(),
                    ..Default::default()
                }
            });
            owner.memory_entries = owner.memory_entries.saturating_add(1);
            owner.memory_capacity_bytes =
                owner.memory_capacity_bytes.saturating_add(*capacity_bytes);
            if !described {
                continue;
            }
            owner.described_memory_entries = owner.described_memory_entries.saturating_add(1);
            owner.described_memory_capacity_bytes = owner
                .described_memory_capacity_bytes
                .saturating_add(*capacity_bytes);
            if is_internal {
                owner.internal_memory_entries = owner.internal_memory_entries.saturating_add(1);
                owner.internal_memory_capacity_bytes = owner
                    .internal_memory_capacity_bytes
                    .saturating_add(*capacity_bytes);
            } else {
                owner.leaf_memory_entries = owner.leaf_memory_entries.saturating_add(1);
                owner.leaf_memory_capacity_bytes = owner
                    .leaf_memory_capacity_bytes
                    .saturating_add(*capacity_bytes);
            }
            match reachability[index] {
                Reachability::Reachable => {
                    owner.reachable_memory_entries =
                        owner.reachable_memory_entries.saturating_add(1);
                    owner.reachable_memory_capacity_bytes = owner
                        .reachable_memory_capacity_bytes
                        .saturating_add(*capacity_bytes);
                }
                Reachability::Orphan => {
                    owner.orphan_memory_entries = owner.orphan_memory_entries.saturating_add(1);
                    owner.orphan_memory_capacity_bytes = owner
                        .orphan_memory_capacity_bytes
                        .saturating_add(*capacity_bytes);
                }
                Reachability::Unknown => {
                    owner.unresolved_memory_entries =
                        owner.unresolved_memory_entries.saturating_add(1);
                    owner.unresolved_memory_capacity_bytes = owner
                        .unresolved_memory_capacity_bytes
                        .saturating_add(*capacity_bytes);
                }
            }
        }
    }
    snapshot.owners = owners.into_values().collect();
    snapshot
}

#[cfg(test)]
mod tests {
    use super::{RouteShadow, observe_route_snapshots};
    use crate::master_kv_router::msg_pack::RadixKvMetadata;

    fn route(key: &str, parent_key: Option<&str>, depth: u32) -> RouteShadow {
        RouteShadow {
            key: key.to_string(),
            radix: Some(RadixKvMetadata {
                parent_key: parent_key.map(str::to_string),
                depth,
            }),
            logical_bytes: 4,
            memory_backings: vec![("owner".to_string(), 8)],
        }
    }

    #[test]
    fn complete_chain_is_reachable_and_tail_is_the_only_leaf() {
        let snapshot = observe_route_snapshots(vec![
            route("a", None, 0),
            route("b", Some("a"), 1),
            route("c", Some("b"), 2),
        ]);
        assert_eq!(snapshot.reachable_route_entries, 3);
        assert_eq!(snapshot.orphan_route_entries, 0);
        assert_eq!(snapshot.internal_route_entries, 2);
        assert_eq!(snapshot.leaf_route_entries, 1);
        assert_eq!(snapshot.owners[0].reachable_memory_capacity_bytes, 24);
        assert_eq!(snapshot.owners[0].leaf_memory_capacity_bytes, 8);
    }

    #[test]
    fn missing_ancestor_marks_direct_and_transitive_orphans() {
        let snapshot =
            observe_route_snapshots(vec![route("b", Some("a"), 1), route("c", Some("b"), 2)]);
        assert_eq!(snapshot.reachable_route_entries, 0);
        assert_eq!(snapshot.orphan_route_entries, 2);
        assert_eq!(snapshot.direct_gap_route_entries, 1);
        assert_eq!(snapshot.orphan_logical_bytes, 8);
        assert_eq!(snapshot.owners[0].orphan_memory_capacity_bytes, 16);
    }

    #[test]
    fn legacy_parent_keeps_child_unresolved_instead_of_false_orphan() {
        let legacy_parent = RouteShadow {
            key: "a".to_string(),
            radix: None,
            logical_bytes: 4,
            memory_backings: vec![("owner".to_string(), 8)],
        };
        let snapshot = observe_route_snapshots(vec![legacy_parent, route("b", Some("a"), 1)]);
        assert_eq!(snapshot.unknown_route_entries, 1);
        assert_eq!(snapshot.unresolved_route_entries, 1);
        assert_eq!(snapshot.orphan_route_entries, 0);
    }
}
