use super::{KvReplicaBacking, MasterKvRouterView, NodeValueReplicaDesc};
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::master_kv_router::msg_pack::{
    BatchEvictOwnerSourceReq, BatchEvictOwnerSourceResp, BatchOwnerReclaimReq, OwnerReclaimBacking,
    OwnerReclaimItem, OwnerReclaimItemResp, OwnerReclaimItemState, OwnerReclaimPhase,
    OwnerReclaimReason, OwnerSourceEvictionOutcome, OwnerSourceEvictionVictim,
    OwnerSourceEvictionVictimResp, OwnerSourceSsdPolicy, owner_source_eviction_epoch,
};
use crate::p2p::msg_pack::{MIN_EXPLICIT_RPC_TIMEOUT_SECS, MsgPack, RPCCaller};
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, OK};
use limit_thirdparty::tokio;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const OWNER_RECLAIM_RPC_TIMEOUT: Duration = Duration::from_secs(MIN_EXPLICIT_RPC_TIMEOUT_SECS);
const OWNER_RECLAIM_MAX_BATCH: usize = 256;
// Keep each transport/SSD transaction small enough to release physical slots
// continuously.  Every entry remains an independently fenced single-KV
// victim; this is only an RPC and bounded-I/O aggregation limit.
const OWNER_RECLAIM_RPC_BATCH: usize = 32;
const OWNER_RECLAIM_MERGE_WINDOW: Duration = Duration::from_millis(5);
const EVICTION_RECLAIM_RETRY_INITIAL: Duration = Duration::from_millis(100);
const EVICTION_RECLAIM_RETRY_MAX: Duration = Duration::from_secs(1);
const EVICTION_RECLAIM_MAX_RETRY_COUNT: u32 = 5;

fn should_restore_after_retry(retry_count: u32) -> bool {
    retry_count >= EVICTION_RECLAIM_MAX_RETRY_COUNT
}

fn eviction_reclaim_retry_delay(retry_count: u32) -> Duration {
    let multiplier = 1u32 << retry_count.saturating_sub(1).min(16);
    EVICTION_RECLAIM_RETRY_INITIAL
        .saturating_mul(multiplier)
        .min(EVICTION_RECLAIM_RETRY_MAX)
}

#[cfg(test)]
mod timeout_contract_tests {
    use super::*;
    use crate::p2p::msg_pack::validate_explicit_rpc_timeout;

    #[test]
    fn owner_reclaim_timeout_satisfies_rpc_contract() {
        validate_explicit_rpc_timeout(Some(OWNER_RECLAIM_RPC_TIMEOUT)).unwrap();
    }

    #[test]
    fn eviction_reclaim_retry_restoration_is_bounded() {
        assert!(!should_restore_after_retry(
            EVICTION_RECLAIM_MAX_RETRY_COUNT - 1
        ));
        assert!(should_restore_after_retry(EVICTION_RECLAIM_MAX_RETRY_COUNT));
        assert!(should_restore_after_retry(u32::MAX));
    }

    #[test]
    fn eviction_reclaim_retry_paces_restore_view_holders() {
        assert_eq!(eviction_reclaim_retry_delay(1), Duration::from_millis(100));
        assert_eq!(eviction_reclaim_retry_delay(2), Duration::from_millis(200));
        assert_eq!(eviction_reclaim_retry_delay(3), Duration::from_millis(400));
        assert_eq!(eviction_reclaim_retry_delay(4), Duration::from_millis(800));
        assert_eq!(
            eviction_reclaim_retry_delay(u32::MAX),
            Duration::from_secs(1)
        );
    }
}

#[cfg(test)]
mod single_victim_transaction_tests {
    use super::*;
    use crate::master_kv_router::msg_pack::{
        OwnerSourceEvictionVictim, PutAtomicGroup, PutAtomicGroupMember,
    };
    use crate::master_kv_router::{
        CommittedSlotReplica, KvMemoryReplica, KvNodeReplicas, KvSsdReplica, MasterKeyActivityKind,
        MasterKeyActivityTable, OneKvNodesRoutes,
    };
    use crate::master_seg_manager::NodeTombTag;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32};

    fn item(key: &str, epoch: u64) -> OwnerReclaimItem {
        OwnerReclaimItem {
            key: key.to_string(),
            put_id: (7, epoch as u32),
            epoch,
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id: 1,
                slot_index: epoch as u32,
                slot_size: 4096,
            },
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        }
    }

    #[test]
    fn single_victim_master_fence_rejects_a_busy_key() {
        let activity = Arc::new(MasterKeyActivityTable::default());
        let items = vec![item("victim", 1)];
        let _busy = activity
            .reserve("victim", MasterKeyActivityKind::Get, false)
            .unwrap();

        assert!(try_install_master_fences(&activity, &items).is_err());
        assert!(!activity.has_reclaim("victim"));
    }

    #[test]
    fn single_victim_master_fence_installs_and_clears() {
        let activity = Arc::new(MasterKeyActivityTable::default());
        let items = vec![item("victim", 1)];

        try_install_master_fences(&activity, &items).unwrap();
        assert!(activity.has_reclaim("victim"));
        for item in &items {
            assert!(activity.clear_reclaim(item));
        }
    }

    #[test]
    fn master_capacity_origin_rejects_committed_slot_backing() {
        let backing = KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
            owner_node_id: "gpu0".to_string().into(),
            grant_id: 1,
            slot_index: 2,
            slot_size: 4096,
            addr: 0,
            len: 4096,
            base_addr: 0,
        });
        assert_eq!(
            master_allocation_capacity_weight(&backing),
            Err(MasterCapacityPlanError::CommittedSlot),
        );
    }

    fn source_victim(
        key: &str,
        put_id: (u64, u32),
        grant_id: u64,
        slot_index: u32,
    ) -> OwnerSourceEvictionVictim {
        OwnerSourceEvictionVictim {
            key: key.to_string(),
            put_id,
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id,
                slot_index,
                slot_size: 4096,
            },
            ssd_backing_len: None,
            ssd_policy: OwnerSourceSsdPolicy::Drop,
        }
    }

    fn source_route(
        owner: &NodeID,
        member: &OwnerSourceEvictionVictim,
        atomic_group: Option<Arc<PutAtomicGroup>>,
        include_cpu_replica: bool,
    ) -> Arc<OneKvNodesRoutes> {
        let OwnerReclaimBacking::CommittedSlot {
            grant_id,
            slot_index,
            slot_size,
        } = &member.backing
        else {
            unreachable!()
        };
        let owner_replica = KvNodeReplicas::memory(
            NodeTombTag::new(),
            KvMemoryReplica {
                backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                    owner_node_id: owner.clone(),
                    grant_id: *grant_id,
                    slot_index: *slot_index,
                    slot_size: *slot_size,
                    addr: 0,
                    len: *slot_size,
                    base_addr: 0,
                }),
                owner_local_indexed: true,
                get_durable_reservation: None,
                capacity_reservation: None,
            },
        );
        let mut replicas = HashMap::from([(owner.clone(), owner_replica)]);
        if include_cpu_replica {
            let cpu: NodeID = "cpu0".to_string().into();
            replicas.insert(
                cpu.clone(),
                KvNodeReplicas::memory(
                    NodeTombTag::new(),
                    KvMemoryReplica {
                        backing: KvReplicaBacking::CommittedSlot(CommittedSlotReplica {
                            owner_node_id: cpu,
                            grant_id: 900 + *grant_id,
                            slot_index: *slot_index,
                            slot_size: *slot_size,
                            addr: 0,
                            len: *slot_size,
                            base_addr: 0,
                        }),
                        owner_local_indexed: false,
                        get_durable_reservation: None,
                        capacity_reservation: None,
                    },
                ),
            );
        }
        Arc::new(OneKvNodesRoutes {
            put_id: member.put_id,
            lease_id: None,
            atomic_group,
            node_replicas: RwLock::new(replicas),
            get_durable_slots_used: AtomicU32::new(0),
        })
    }

    fn source_reclaim_item(member: &OwnerSourceEvictionVictim) -> OwnerReclaimItem {
        OwnerReclaimItem {
            key: member.key.clone(),
            put_id: member.put_id,
            epoch: 1,
            backing: member.backing.clone(),
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        }
    }

    #[test]
    fn exact_source_plan_accepts_with_or_without_a_cpu_replica() {
        let owner: NodeID = "gpu0".to_string().into();
        let member = source_victim("single", (10, 1), 7, 3);

        for include_cpu_replica in [false, true] {
            let route = source_route(&owner, &member, None, include_cpu_replica);
            let plan = plan_exact_owner_source_victim_with(&owner, &member, &|key| {
                (key == "single").then(|| route.clone())
            });
            match plan {
                OwnerSourceVictimPlan::Ready(planned) => {
                    assert_eq!(planned.key, "single");
                    assert_eq!(planned.expected_backing, Some(member.backing.clone()));
                }
                _ => panic!("exact current owner source must be accepted"),
            }
        }
    }

    #[test]
    fn exact_source_removal_deletes_only_gpu_when_cpu_exists_and_last_route_otherwise() {
        let owner: NodeID = "gpu0".to_string().into();
        let member = source_victim("with-cpu", (10, 2), 7, 4);
        let routes = dashmap::DashMap::new();
        routes.insert(
            member.key.clone(),
            source_route(&owner, &member, None, true),
        );
        let removed =
            remove_exact_owner_source_route(&routes, &owner, &source_reclaim_item(&member))
                .expect("exact GPU source must be removed");
        let counters = crate::master_kv_router::EvictionReclaimCounters::default();
        record_last_route_removal(&counters, &removed);
        assert!(!removed.removed_last_route);
        assert!(!removed.ssd_survived);
        assert!(!removed.ssd_became_only_backing);
        assert_eq!(
            counters.last_route_removed_members.load(Ordering::Relaxed),
            0
        );
        assert_eq!(counters.last_route_removed_bytes.load(Ordering::Relaxed), 0);
        let remaining = routes.get(&member.key).expect("CPU route must remain");
        assert!(!remaining.node_replicas.read().contains_key(&owner));
        assert!(remaining.node_replicas.read().contains_key("cpu0"));

        let last = source_victim("last", (10, 3), 7, 5);
        routes.insert(last.key.clone(), source_route(&owner, &last, None, false));
        let removed = remove_exact_owner_source_route(&routes, &owner, &source_reclaim_item(&last))
            .expect("last exact GPU source must be removed");
        record_last_route_removal(&counters, &removed);
        assert!(removed.removed_last_route);
        assert!(!routes.contains_key(&last.key));
        assert_eq!(
            counters.last_route_removed_members.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            counters.last_route_removed_bytes.load(Ordering::Relaxed),
            removed.capacity_bytes
        );

        let stale = source_victim("stale", (10, 4), 8, 6);
        routes.insert(stale.key.clone(), source_route(&owner, &stale, None, false));
        let wrong_identity = source_victim("stale", stale.put_id, 999, 6);
        assert!(
            remove_exact_owner_source_route(
                &routes,
                &owner,
                &source_reclaim_item(&wrong_identity),
            )
            .is_none()
        );
        assert!(routes.contains_key(&stale.key));
    }

    #[test]
    fn exact_source_memory_removal_preserves_same_owner_ssd_backing() {
        let owner: NodeID = "gpu0".to_string().into();
        let member = source_victim("ssd-backed", (10, 5), 7, 7);
        let route = source_route(&owner, &member, None, false);
        route
            .node_replicas
            .write()
            .get_mut(&owner)
            .expect("owner route must exist")
            .ssd = Some(KvSsdReplica { len: 4096 });
        let routes = dashmap::DashMap::new();
        routes.insert(member.key.clone(), route.clone());

        let removed =
            remove_exact_owner_source_route(&routes, &owner, &source_reclaim_item(&member))
                .expect("exact memory backing must be removed");

        assert!(!removed.removed_last_route);
        assert!(removed.ssd_survived);
        assert!(removed.ssd_became_only_backing);
        let current = routes
            .get(&member.key)
            .expect("SSD backing must keep the key route alive");
        let replicas = current.node_replicas.read();
        let owner_backings = replicas.get(&owner).expect("owner route must remain");
        assert!(owner_backings.memory.is_none());
        assert_eq!(owner_backings.ssd.as_ref().map(|ssd| ssd.len), Some(4096));
    }

    #[test]
    fn ssd_preservation_selects_only_an_exact_last_live_backing() {
        let owner: NodeID = "gpu0".to_string().into();
        let last = source_victim("last-for-ssd", (20, 1), 11, 1);
        let last_route = source_route(&owner, &last, None, false);
        let last_item = source_reclaim_item(&last);
        assert!(exact_memory_reclaim_needs_ssd_with(
            &owner,
            &last_item,
            &|key| (key == last.key).then(|| last_route.clone()),
        ));

        let with_other = source_victim("other-live", (20, 2), 12, 2);
        let with_other_route = source_route(&owner, &with_other, None, true);
        let with_other_item = source_reclaim_item(&with_other);
        assert!(!exact_memory_reclaim_needs_ssd_with(
            &owner,
            &with_other_item,
            &|key| (key == with_other.key).then(|| with_other_route.clone()),
        ));

        let already_on_ssd = source_victim("already-on-ssd", (20, 3), 13, 3);
        let already_on_ssd_route = source_route(&owner, &already_on_ssd, None, false);
        already_on_ssd_route
            .node_replicas
            .write()
            .get_mut(&owner)
            .expect("owner route must exist")
            .ssd = Some(KvSsdReplica { len: 4096 });
        let already_on_ssd_item = source_reclaim_item(&already_on_ssd);
        assert!(!exact_memory_reclaim_needs_ssd_with(
            &owner,
            &already_on_ssd_item,
            &|key| (key == already_on_ssd.key).then(|| already_on_ssd_route.clone()),
        ));

        let stale = source_victim("stale-for-ssd", (20, 4), 14, 4);
        let stale_route = source_route(&owner, &stale, None, false);
        let wrong_identity = source_victim("stale-for-ssd", stale.put_id, 999, 4);
        assert!(!exact_memory_reclaim_needs_ssd_with(
            &owner,
            &source_reclaim_item(&wrong_identity),
            &|key| (key == stale.key).then(|| stale_route.clone()),
        ));
    }

    #[test]
    fn direct_delete_filters_before_ssd_and_drop_finishes_without_waiting() {
        let owner: NodeID = "gpu0".to_string().into();
        let mut last = source_victim("last-candidate", (21, 1), 15, 1);
        last.ssd_policy = OwnerSourceSsdPolicy::SelectLastLive;
        let mut backed = source_victim("already-backed", (21, 2), 16, 2);
        backed.ssd_policy = OwnerSourceSsdPolicy::SelectLastLive;
        let routes = dashmap::DashMap::new();
        routes.insert(last.key.clone(), source_route(&owner, &last, None, false));
        routes.insert(
            backed.key.clone(),
            source_route(&owner, &backed, None, true),
        );
        let activity = MasterKeyActivityTable::default();
        let (responses, busy) = direct_delete_exact_owner_source_batch_with(
            &activity,
            &owner,
            81,
            &[last.clone(), backed.clone()],
            &|key| routes.get(key).map(|route| route.clone()),
            |item| remove_exact_owner_source_route(&routes, &owner, item).is_some(),
        );
        assert_eq!(busy, DirectDeleteBatchBusySummary::default());
        assert_eq!(
            responses
                .iter()
                .map(|response| response.outcome)
                .collect::<Vec<_>>(),
            vec![
                OwnerSourceEvictionOutcome::SsdCandidate,
                OwnerSourceEvictionOutcome::Completed,
            ]
        );
        assert!(routes.contains_key(&last.key));
        assert!(
            routes
                .get(&backed.key)
                .unwrap()
                .node_replicas
                .read()
                .contains_key("cpu0")
        );
        assert!(!activity.has_reclaim(&last.key));

        last.ssd_policy = OwnerSourceSsdPolicy::Drop;
        let dropped = direct_delete_exact_owner_source_with(
            &activity,
            &owner,
            &last,
            owner_source_eviction_epoch(82, 0),
            &|key| routes.get(key).map(|route| route.clone()),
            |item| remove_exact_owner_source_route(&routes, &owner, item).is_some(),
        );
        assert_eq!(dropped.outcome, OwnerSourceEvictionOutcome::Completed);
        assert!(!dropped.ssd_backing_committed);
        assert!(!routes.contains_key(&last.key));
        assert!(!activity.has_reclaim(&last.key));
    }

    #[test]
    fn singleton_source_is_independent_of_atomic_group_siblings() {
        let owner: NodeID = "gpu0".to_string().into();
        let a = source_victim("a", (12, 0), 9, 0);
        let b = source_victim("b", (12, 1), 9, 1);
        let group = Arc::new(PutAtomicGroup {
            members: vec![
                PutAtomicGroupMember {
                    key: a.key.clone(),
                    put_id: a.put_id,
                },
                PutAtomicGroupMember {
                    key: b.key.clone(),
                    put_id: b.put_id,
                },
            ],
        });
        let route_a = source_route(&owner, &a, Some(group.clone()), false);
        match plan_exact_owner_source_victim_with(&owner, &a, &|key| {
            (key == "a").then(|| route_a.clone())
        }) {
            OwnerSourceVictimPlan::Ready(planned) => assert_eq!(planned.key, "a"),
            _ => panic!("one current key must be reclaimable without its siblings"),
        }

        let changed_a = source_victim("a", a.put_id, 99, 0);
        let changed_route = source_route(&owner, &changed_a, Some(group), false);
        assert!(matches!(
            plan_exact_owner_source_victim_with(&owner, &a, &|key| {
                (key == "a").then(|| changed_route.clone())
            }),
            OwnerSourceVictimPlan::Stale(_)
        ));
    }

    #[test]
    fn absent_single_source_is_already_completed() {
        let owner: NodeID = "gpu0".to_string().into();
        let victim = source_victim("gone", (13, 0), 10, 0);
        assert!(matches!(
            plan_exact_owner_source_victim_with(&owner, &victim, &|_| None),
            OwnerSourceVictimPlan::Completed(_)
        ));
    }

    #[test]
    fn direct_delete_batch_keeps_results_independent_and_replay_idempotent() {
        let owner: NodeID = "gpu0".to_string().into();
        let mut ready = source_victim("ready", (14, 0), 11, 0);
        ready.ssd_backing_len = Some(4096);
        ready.ssd_policy = OwnerSourceSsdPolicy::Persisted;
        let mut busy = source_victim("busy", (14, 1), 11, 1);
        busy.ssd_backing_len = Some(4096);
        busy.ssd_policy = OwnerSourceSsdPolicy::Persisted;
        let stale = source_victim("stale", (14, 2), 11, 2);
        let routes = dashmap::DashMap::new();
        for victim in [&ready, &busy, &stale] {
            routes.insert(
                victim.key.clone(),
                source_route(&owner, victim, None, false),
            );
        }
        let activity = Arc::new(MasterKeyActivityTable::default());
        let _busy_get = activity
            .reserve(&busy.key, MasterKeyActivityKind::Get, false)
            .expect("busy victim must hold a master Get lease");
        let mut stale_request = source_victim("stale", stale.put_id, 999, 2);
        stale_request.ssd_backing_len = Some(4096);
        stale_request.ssd_policy = OwnerSourceSsdPolicy::Persisted;
        let victims = vec![ready.clone(), busy.clone(), stale_request];
        let (responses, busy_summary) = direct_delete_exact_owner_source_batch_with(
            activity.as_ref(),
            &owner,
            77,
            &victims,
            &|key| routes.get(key).map(|route| route.clone()),
            |item| remove_exact_owner_source_route(&routes, &owner, item).is_some(),
        );

        assert_eq!(
            responses
                .iter()
                .map(|response| response.outcome)
                .collect::<Vec<_>>(),
            vec![
                OwnerSourceEvictionOutcome::Completed,
                OwnerSourceEvictionOutcome::RetryableBusy,
                OwnerSourceEvictionOutcome::Stale,
            ]
        );
        assert_eq!(
            responses
                .iter()
                .map(|response| response.victim_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2],
            "one batch response vector must stay aligned with every input victim"
        );
        assert_eq!(
            responses
                .iter()
                .map(|response| response.ssd_backing_committed)
                .collect::<Vec<_>>(),
            vec![true, false, false],
            "only the exact fenced write-back victim may publish SSD backing"
        );
        assert_eq!(
            busy_summary,
            DirectDeleteBatchBusySummary {
                activity_busy_items: 1,
                get_busy_items: 1,
                inflight_gets: 1,
                ..Default::default()
            }
        );
        let ready_route = routes
            .get(&ready.key)
            .expect("SSD write-back must keep the ready route alive");
        let ready_replicas = ready_route.node_replicas.read();
        let ready_backings = ready_replicas
            .get(&owner)
            .expect("same-owner SSD backing must remain");
        assert!(ready_backings.memory.is_none());
        assert_eq!(ready_backings.ssd.as_ref().map(|ssd| ssd.len), Some(4096));
        drop(ready_replicas);
        drop(ready_route);
        assert!(routes.contains_key(&busy.key));
        assert!(routes.contains_key(&stale.key));
        assert!(
            routes
                .get(&busy.key)
                .unwrap()
                .node_replicas
                .read()
                .get(&owner)
                .unwrap()
                .ssd
                .is_none(),
            "a busy key must not publish SSD backing before its master fence"
        );
        assert!(
            routes
                .get(&stale.key)
                .unwrap()
                .node_replicas
                .read()
                .get(&owner)
                .unwrap()
                .ssd
                .is_none(),
            "a stale exact backing must not publish SSD backing"
        );
        assert!(!activity.has_reclaim(&ready.key));

        let replay_delete_called = AtomicBool::new(false);
        let replay = direct_delete_exact_owner_source_with(
            activity.as_ref(),
            &owner,
            &ready,
            owner_source_eviction_epoch(77, 0),
            &|key| routes.get(key).map(|route| route.clone()),
            |_| {
                replay_delete_called.store(true, Ordering::Relaxed);
                false
            },
        );
        assert_eq!(replay.outcome, OwnerSourceEvictionOutcome::Completed);
        assert!(
            replay.ssd_backing_committed,
            "a lost direct-delete response must replay the already-published SSD terminal"
        );
        assert_eq!(replay.busy_cause, None);
        assert!(!replay_delete_called.load(Ordering::Relaxed));
    }
}

/// Why an entry entered the shared safe-reclaim pipeline.
///
/// Only `MasterAllocationCapacity` may originate from the master resident
/// cache's Size listener. `OwnerCapacityEviction` is an exact source-deletion
/// request selected by the owner and may resolve to a CommittedSlot.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum EvictionReclaimOrigin {
    MasterAllocationCapacity,
    OwnerCapacityEviction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnqueueEvictionReclaimResult {
    Accepted,
    AlreadyInProgress,
    PartialOverlap,
    NotInProgress,
    Closed,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct EvictionReclaimIdentity {
    owner_node_id: NodeIDString,
    owner_node_start_time: Option<i64>,
    key: String,
    put_id: (u64, u32),
    weight_bytes: u32,
    expected_backing: Option<OwnerReclaimBacking>,
}

#[derive(Clone, Debug)]
pub(crate) struct EvictionReclaimMember {
    pub key: String,
    pub desc: NodeValueReplicaDesc,
    pub expected_backing: Option<OwnerReclaimBacking>,
}

#[derive(Clone, Debug)]
pub(crate) struct EvictionReclaimRequest {
    pub owner_node_id: NodeIDString,
    pub owner_node_start_time: Option<i64>,
    pub members: Vec<EvictionReclaimMember>,
    pub origin: EvictionReclaimOrigin,
    pub retry_count: u32,
}

impl EvictionReclaimRequest {
    pub(crate) fn identities(&self) -> Vec<EvictionReclaimIdentity> {
        self.members
            .iter()
            .map(|member| EvictionReclaimIdentity {
                owner_node_id: self.owner_node_id.clone(),
                owner_node_start_time: self.owner_node_start_time,
                key: member.key.clone(),
                put_id: member.desc.put_id,
                weight_bytes: member.desc.weight_bytes,
                expected_backing: member.expected_backing.clone(),
            })
            .collect()
    }

    pub(crate) fn weight_bytes(&self) -> u64 {
        self.members
            .iter()
            .map(|member| u64::from(member.desc.weight_bytes))
            .fold(0u64, u64::saturating_add)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MasterCapacityPlanError {
    RouteChanged,
    WrongRole,
    CommittedSlot,
}

fn master_allocation_capacity_weight(
    backing: &KvReplicaBacking,
) -> Result<u32, MasterCapacityPlanError> {
    match backing {
        KvReplicaBacking::Allocation(allocation) => {
            Ok(u32::try_from(allocation.capcity()).unwrap_or(u32::MAX))
        }
        KvReplicaBacking::CommittedSlot(_) => Err(MasterCapacityPlanError::CommittedSlot),
    }
}

fn allocation_member_from_route(
    view: &MasterKvRouterView,
    owner: &NodeID,
    key: &str,
) -> Result<
    (
        EvictionReclaimMember,
        std::sync::Arc<super::OneKvNodesRoutes>,
    ),
    MasterCapacityPlanError,
> {
    let route = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(key)
        .map(|entry| entry.clone())
        .ok_or(MasterCapacityPlanError::RouteChanged)?;
    if route.lease_id.is_some() {
        return Err(MasterCapacityPlanError::RouteChanged);
    }
    let replicas = route.node_replicas.read();
    let node_replicas = replicas
        .get(owner)
        .filter(|replicas| !replicas.tomb_tag.is_tomb())
        .ok_or(MasterCapacityPlanError::RouteChanged)?;
    let replica = node_replicas
        .memory
        .as_ref()
        .ok_or(MasterCapacityPlanError::RouteChanged)?;
    if replica.owner_local_indexed {
        return Err(MasterCapacityPlanError::WrongRole);
    }
    let weight_bytes = master_allocation_capacity_weight(&replica.backing)?;
    drop(replicas);
    Ok((
        EvictionReclaimMember {
            key: key.to_string(),
            desc: NodeValueReplicaDesc {
                weight_bytes,
                put_id: route.put_id,
            },
            expected_backing: None,
        },
        route,
    ))
}

/// Validate one exact key popped by the master Allocation Moka.
fn plan_master_allocation_capacity_victim(
    view: &MasterKvRouterView,
    request: &EvictionReclaimRequest,
) -> Result<EvictionReclaimMember, MasterCapacityPlanError> {
    if request.origin != EvictionReclaimOrigin::MasterAllocationCapacity
        || request.members.len() != 1
    {
        return Err(MasterCapacityPlanError::WrongRole);
    }
    let owner: NodeID = request.owner_node_id.clone().into();
    let anchor = &request.members[0];
    let (current_anchor, _route) = allocation_member_from_route(view, &owner, &anchor.key)?;
    if current_anchor.desc.put_id != anchor.desc.put_id
        || current_anchor.desc.weight_bytes != anchor.desc.weight_bytes
    {
        return Err(MasterCapacityPlanError::RouteChanged);
    }
    Ok(current_anchor)
}

fn route_item(
    view: &MasterKvRouterView,
    owner_node_id: &NodeID,
    key: &str,
    expected_put_id: Option<(u64, u32)>,
    required_slot_size: Option<u64>,
    reason: OwnerReclaimReason,
    epoch: u64,
) -> Option<OwnerReclaimItem> {
    let route = view.master_kv_router().inner().kv_routes.get(key)?.clone();
    if expected_put_id.is_some_and(|put_id| put_id != route.put_id) || route.lease_id.is_some() {
        return None;
    }
    let replicas = route.node_replicas.read();
    let node_replicas = replicas.get(owner_node_id)?;
    if node_replicas.tomb_tag.is_tomb() {
        return None;
    }
    let target = node_replicas.memory.as_ref()?;
    let backing = match &target.backing {
        KvReplicaBacking::Allocation(allocation) if required_slot_size.is_none() => {
            if target.owner_local_indexed {
                OwnerReclaimBacking::Allocation
            } else {
                OwnerReclaimBacking::UnindexedAllocation {
                    addr: allocation.base_addr().checked_add(allocation.addr())?,
                    base_addr: allocation.base_addr(),
                    len: allocation.size(),
                    capacity_bytes: allocation.capcity(),
                }
            }
        }
        KvReplicaBacking::Allocation(_) => return None,
        KvReplicaBacking::CommittedSlot(slot)
            if slot.owner_node_id == *owner_node_id
                && required_slot_size.map_or(true, |slot_size| slot.slot_size == slot_size) =>
        {
            OwnerReclaimBacking::CommittedSlot {
                grant_id: slot.grant_id,
                slot_index: slot.slot_index,
                slot_size: slot.slot_size,
            }
        }
        KvReplicaBacking::CommittedSlot(_) => return None,
    };
    drop(replicas);
    Some(OwnerReclaimItem {
        key: key.to_string(),
        put_id: route.put_id,
        epoch,
        backing,
        reason,
    })
}

fn item_still_valid(view: &MasterKvRouterView, owner: &NodeID, item: &OwnerReclaimItem) -> bool {
    if !view
        .master_kv_router()
        .inner()
        .key_activity
        .reclaim_matches(item)
    {
        return false;
    }
    route_item(
        view,
        owner,
        &item.key,
        Some(item.put_id),
        match &item.backing {
            OwnerReclaimBacking::Allocation | OwnerReclaimBacking::UnindexedAllocation { .. } => {
                None
            }
            OwnerReclaimBacking::CommittedSlot { slot_size, .. } => Some(*slot_size),
        },
        item.reason,
        item.epoch,
    )
    .is_some_and(|current| current.backing == item.backing && current.reason == item.reason)
}

/// Publish bytes already persisted by the owner onto the existing route while
/// both master and owner Prepare fences still protect the exact memory
/// generation.  Memory remains present until the subsequent Commit succeeds,
/// so Get never observes a route gap between DRAM and SSD.
fn publish_prepared_ssd_backing(
    view: &MasterKvRouterView,
    owner: &NodeID,
    item: &OwnerReclaimItem,
    ssd_backing_len: Option<u64>,
) -> Result<bool, String> {
    let Some(len) = ssd_backing_len else {
        return Ok(false);
    };
    if !item_still_valid(view, owner, item) {
        return Err(format!(
            "exact memory route changed before SSD publication: key={} epoch={}",
            item.key, item.epoch
        ));
    }
    let route = view
        .master_kv_router()
        .inner()
        .kv_routes
        .get(&item.key)
        .map(|route| route.clone())
        .ok_or_else(|| format!("route disappeared before SSD publication: key={}", item.key))?;
    let existing_ssd_len = route
        .node_replicas
        .read()
        .get(owner)
        .and_then(|replicas| replicas.ssd.as_ref())
        .map(|ssd| ssd.len);
    if let Some(existing_len) = existing_ssd_len {
        return if existing_len == len {
            Ok(false)
        } else {
            Err(format!(
                "existing SSD backing length mismatch: key={} existing={} prepared={}",
                item.key, existing_len, len
            ))
        };
    }
    match route.commit_ssd_replica(owner, len) {
        super::SsdReplicaCommitStatus::Committed => Ok(true),
        super::SsdReplicaCommitStatus::MissingMemory => Err(format!(
            "memory backing disappeared before SSD publication: key={}",
            item.key
        )),
        super::SsdReplicaCommitStatus::TombedNode => Err(format!(
            "owner generation was tombed before SSD publication: owner={} key={}",
            owner, item.key
        )),
        super::SsdReplicaCommitStatus::LengthMismatch => Err(format!(
            "persisted SSD length mismatches memory route: key={} len={}",
            item.key, len
        )),
    }
}

fn rollback_new_ssd_backing(
    view: &MasterKvRouterView,
    owner: &NodeID,
    item: &OwnerReclaimItem,
) -> bool {
    if !item_still_valid(view, owner, item) {
        return false;
    }
    view.master_kv_router()
        .inner()
        .kv_routes
        .get(&item.key)
        .filter(|route| route.put_id == item.put_id)
        .is_some_and(|route| route.remove_ssd_replica(owner))
}

async fn call_owner_phase(
    view: &MasterKvRouterView,
    owner: &NodeID,
    phase: OwnerReclaimPhase,
    items: Vec<OwnerReclaimItem>,
) -> Result<Vec<OwnerReclaimItemResp>, String> {
    if items.is_empty() {
        return Ok(Vec::new());
    }
    debug_assert!(items.iter().all(|item| {
        !matches!(
            &item.backing,
            OwnerReclaimBacking::UnindexedAllocation { .. }
        ) || item.reason == OwnerReclaimReason::MasterAllocationCapacity
    }));
    let caller = RPCCaller::<BatchOwnerReclaimReq>::new();
    caller.regist(view.p2p_module());
    let resp = caller
        .call(
            view.p2p_module(),
            owner.clone(),
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase,
                    items: items.clone(),
                },
                raw_bytes: Vec::new(),
            },
            Some(OWNER_RECLAIM_RPC_TIMEOUT),
            1,
        )
        .await
        .map_err(|err| format!("{err:?}"))?;
    if resp.serialize_part.error_code != OK {
        return Err(format!(
            "code={} error={}",
            resp.serialize_part.error_code, resp.serialize_part.error_json
        ));
    }
    if resp.serialize_part.items.len() != items.len() {
        return Err(format!(
            "owner reclaim response length mismatch: phase={phase:?} expected={} got={}",
            items.len(),
            resp.serialize_part.items.len()
        ));
    }
    for (request, response) in items.iter().zip(resp.serialize_part.items.iter()) {
        if request.key != response.key || request.epoch != response.epoch {
            return Err(format!(
                "owner reclaim response identity mismatch: phase={phase:?} request=({}, {}) response=({}, {})",
                request.key, request.epoch, response.key, response.epoch
            ));
        }
    }
    Ok(resp.serialize_part.items)
}

fn clear_master_fence(view: &MasterKvRouterView, item: &OwnerReclaimItem) {
    let cleared = view
        .master_kv_router()
        .inner()
        .key_activity
        .clear_reclaim(item);
    if !cleared {
        tracing::warn!(
            "owner reclaim master fence did not match during cleanup: key={} epoch={}",
            item.key,
            item.epoch
        );
    }
}

async fn abort_prepared(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
    newly_published_ssd: HashSet<(String, u64)>,
) -> Vec<OwnerReclaimItem> {
    if items.is_empty() {
        return Vec::new();
    }
    match call_owner_phase(view, owner, OwnerReclaimPhase::Abort, items.clone()).await {
        Ok(responses) => {
            let mut already_committed = Vec::new();
            for (item, response) in items.into_iter().zip(responses.into_iter()) {
                match response.state {
                    OwnerReclaimItemState::Committed => already_committed.push(item),
                    OwnerReclaimItemState::Aborted => {
                        if newly_published_ssd.contains(&(item.key.clone(), item.epoch))
                            && !rollback_new_ssd_backing(view, owner, &item)
                        {
                            tracing::warn!(
                                owner = %owner,
                                key = %item.key,
                                epoch = item.epoch,
                                "new SSD backing was not removable after owner reclaim abort"
                            );
                        }
                        clear_master_fence(view, &item);
                    }
                    OwnerReclaimItemState::Stale | OwnerReclaimItemState::Finalized => {
                        clear_master_fence(view, &item)
                    }
                    state => tracing::warn!(
                        "owner reclaim abort returned unresolved state: key={} epoch={} state={:?} detail={}",
                        item.key,
                        item.epoch,
                        state,
                        response.detail
                    ),
                }
            }
            already_committed
        }
        Err(err) => {
            tracing::warn!(
                "owner reclaim abort RPC failed; retaining master fences: owner={} keys={} err={}",
                owner,
                items.len(),
                err
            );
            spawn_abort_retry(view.clone(), owner.clone(), items, newly_published_ssd);
            Vec::new()
        }
    }
}

fn spawn_abort_retry(
    view: MasterKvRouterView,
    owner: NodeID,
    items: Vec<OwnerReclaimItem>,
    newly_published_ssd: HashSet<(String, u64)>,
) {
    if items.is_empty() {
        return;
    }
    let spawn_view = view.clone();
    let _ = spawn_view.spawn("owner_reclaim_abort_retry", async move {
        let mut pending = items;
        let mut committed = Vec::new();
        let mut delay = Duration::from_millis(25);
        for _attempt in 1..=8 {
            tokio::time::sleep(delay).await;
            match call_owner_phase(&view, &owner, OwnerReclaimPhase::Abort, pending.clone()).await {
                Ok(responses) => {
                    let mut next = Vec::new();
                    for (item, response) in pending.into_iter().zip(responses.into_iter()) {
                        match response.state {
                            OwnerReclaimItemState::Committed => committed.push(item),
                            OwnerReclaimItemState::Aborted => {
                                if newly_published_ssd.contains(&(item.key.clone(), item.epoch))
                                    && !rollback_new_ssd_backing(&view, &owner, &item)
                                {
                                    tracing::warn!(
                                        owner = %owner,
                                        key = %item.key,
                                        epoch = item.epoch,
                                        "new SSD backing was not removable after retried owner reclaim abort"
                                    );
                                }
                                clear_master_fence(&view, &item);
                            }
                            OwnerReclaimItemState::Stale | OwnerReclaimItemState::Finalized => {
                                clear_master_fence(&view, &item)
                            }
                            _ => next.push(item),
                        }
                    }
                    pending = next;
                    if pending.is_empty() {
                        break;
                    }
                }
                Err(err) => tracing::warn!(
                    "owner reclaim abort retry failed: owner={} keys={} err={}",
                    owner,
                    pending.len(),
                    err
                ),
            }
            delay = (delay * 2).min(Duration::from_secs(1));
        }
        if !committed.is_empty() {
            let _ = finish_committed(&view, &owner, committed).await;
        }
        if !pending.is_empty() {
            tracing::error!(
                "owner reclaim abort retry exhausted; fences retained: owner={} keys={}",
                owner,
                pending.len()
            );
        }
    });
}

fn reclaim_backing_matches(
    replica: &super::KvMemoryReplica,
    expected: &OwnerReclaimBacking,
) -> bool {
    match (&replica.backing, expected) {
        (KvReplicaBacking::Allocation(_), OwnerReclaimBacking::Allocation) => {
            replica.owner_local_indexed
        }
        (
            KvReplicaBacking::Allocation(allocation),
            OwnerReclaimBacking::UnindexedAllocation {
                addr,
                base_addr,
                len,
                capacity_bytes,
            },
        ) => {
            !replica.owner_local_indexed
                && allocation.base_addr().checked_add(allocation.addr()) == Some(*addr)
                && allocation.base_addr() == *base_addr
                && allocation.size() == *len
                && allocation.capcity() == *capacity_bytes
        }
        (
            KvReplicaBacking::CommittedSlot(slot),
            OwnerReclaimBacking::CommittedSlot {
                grant_id,
                slot_index,
                slot_size,
            },
        ) => {
            slot.grant_id == *grant_id
                && slot.slot_index == *slot_index
                && slot.slot_size == *slot_size
        }
        _ => false,
    }
}

fn owner_source_member_weight(backing: &OwnerReclaimBacking) -> Option<u32> {
    match backing {
        OwnerReclaimBacking::CommittedSlot { slot_size, .. } => u32::try_from(*slot_size).ok(),
        // Allocation does not carry an address/generation identity in the
        // current wire contract, so accepting it would not be an exact delete.
        OwnerReclaimBacking::Allocation | OwnerReclaimBacking::UnindexedAllocation { .. } => None,
    }
}

enum OwnerSourceVictimPlan {
    Ready(EvictionReclaimMember),
    Completed(&'static str),
    Stale(String),
    Rejected(String),
}

fn plan_exact_owner_source_victim_with(
    owner: &NodeID,
    victim: &OwnerSourceEvictionVictim,
    route_lookup: &dyn Fn(&str) -> Option<Arc<super::OneKvNodesRoutes>>,
) -> OwnerSourceVictimPlan {
    let Some(weight_bytes) = owner_source_member_weight(&victim.backing) else {
        return OwnerSourceVictimPlan::Rejected(format!(
            "source backing is not an exact committed slot: key={}",
            victim.key
        ));
    };
    let desc = NodeValueReplicaDesc {
        weight_bytes,
        put_id: victim.put_id,
    };
    let planned = EvictionReclaimMember {
        key: victim.key.clone(),
        desc: desc.clone(),
        expected_backing: Some(victim.backing.clone()),
    };

    let Some(route) = route_lookup(&victim.key) else {
        return OwnerSourceVictimPlan::Completed("exact source replica is already absent");
    };
    if route.put_id != victim.put_id {
        return OwnerSourceVictimPlan::Stale(format!(
            "route version changed: key={} expected=({},{}) current=({},{})",
            victim.key, victim.put_id.0, victim.put_id.1, route.put_id.0, route.put_id.1,
        ));
    }
    if route.lease_id.is_some() {
        return OwnerSourceVictimPlan::Rejected(format!(
            "leased route is not cache-evictable: key={}",
            victim.key
        ));
    }
    let replica_matches = {
        let replicas = route.node_replicas.read();
        match replicas.get(owner) {
            Some(node_replicas) if !node_replicas.tomb_tag.is_tomb() => {
                let Some(replica) = node_replicas.memory.as_ref() else {
                    return OwnerSourceVictimPlan::Completed(
                        "exact source memory replica is already absent",
                    );
                };
                if !replica.owner_local_indexed {
                    return OwnerSourceVictimPlan::Rejected(format!(
                        "source route is not owner-local indexed: key={}",
                        victim.key
                    ));
                }
                reclaim_backing_matches(replica, &victim.backing)
            }
            _ => {
                return OwnerSourceVictimPlan::Completed("exact source replica is already absent");
            }
        }
    };
    if !replica_matches {
        return OwnerSourceVictimPlan::Stale(format!(
            "source backing changed: key={} put_id=({},{})",
            victim.key, victim.put_id.0, victim.put_id.1
        ));
    }
    OwnerSourceVictimPlan::Ready(planned)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectDeleteBusyCause {
    MasterActivity(super::MasterKeyActivitySnapshot),
    DeleteUnderFence,
}

struct DirectDeleteResult {
    outcome: OwnerSourceEvictionOutcome,
    ssd_backing_committed: bool,
    detail: String,
    busy_cause: Option<DirectDeleteBusyCause>,
}

impl DirectDeleteResult {
    fn terminal(outcome: OwnerSourceEvictionOutcome, detail: impl Into<String>) -> Self {
        Self {
            outcome,
            ssd_backing_committed: false,
            detail: detail.into(),
            busy_cause: None,
        }
    }

    fn activity_busy(snapshot: super::MasterKeyActivitySnapshot) -> Self {
        Self {
            outcome: OwnerSourceEvictionOutcome::RetryableBusy,
            ssd_backing_committed: false,
            detail: format!(
                "master key activity is busy: puts={} gets={} replicas={} reclaim_installed={}",
                snapshot.puts, snapshot.gets, snapshot.replicas, snapshot.reclaim_installed
            ),
            busy_cause: Some(DirectDeleteBusyCause::MasterActivity(snapshot)),
        }
    }

    fn delete_under_fence_busy() -> Self {
        Self {
            outcome: OwnerSourceEvictionOutcome::RetryableBusy,
            ssd_backing_committed: false,
            detail: "exact source route could not be deleted under its master fence".to_string(),
            busy_cause: Some(DirectDeleteBusyCause::DeleteUnderFence),
        }
    }
}

fn exact_ssd_writeback_is_published(
    owner: &NodeID,
    victim: &OwnerSourceEvictionVictim,
    route_lookup: &dyn Fn(&str) -> Option<Arc<super::OneKvNodesRoutes>>,
) -> bool {
    if victim.ssd_policy != OwnerSourceSsdPolicy::Persisted {
        return false;
    }
    let Some(expected_len) = victim.ssd_backing_len else {
        return false;
    };
    let Some(route) = route_lookup(&victim.key) else {
        return false;
    };
    if route.put_id != victim.put_id {
        return false;
    }
    route
        .node_replicas
        .read()
        .get(owner)
        .is_some_and(|replicas| {
            !replicas.tomb_tag.is_tomb()
                && replicas
                    .ssd
                    .as_ref()
                    .is_some_and(|ssd| ssd.len == expected_len)
        })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DirectDeleteBatchBusySummary {
    activity_busy_items: u64,
    put_busy_items: u64,
    get_busy_items: u64,
    replica_busy_items: u64,
    reclaim_busy_items: u64,
    inflight_puts: u64,
    inflight_gets: u64,
    inflight_replicas: u64,
    delete_under_fence_busy_items: u64,
}

impl DirectDeleteBatchBusySummary {
    fn record(&mut self, cause: Option<DirectDeleteBusyCause>) {
        match cause {
            Some(DirectDeleteBusyCause::MasterActivity(snapshot)) => {
                self.activity_busy_items = self.activity_busy_items.saturating_add(1);
                self.put_busy_items = self
                    .put_busy_items
                    .saturating_add(u64::from(snapshot.puts != 0));
                self.get_busy_items = self
                    .get_busy_items
                    .saturating_add(u64::from(snapshot.gets != 0));
                self.replica_busy_items = self
                    .replica_busy_items
                    .saturating_add(u64::from(snapshot.replicas != 0));
                self.reclaim_busy_items = self
                    .reclaim_busy_items
                    .saturating_add(u64::from(snapshot.reclaim_installed));
                self.inflight_puts = self.inflight_puts.saturating_add(u64::from(snapshot.puts));
                self.inflight_gets = self.inflight_gets.saturating_add(u64::from(snapshot.gets));
                self.inflight_replicas = self
                    .inflight_replicas
                    .saturating_add(u64::from(snapshot.replicas));
            }
            Some(DirectDeleteBusyCause::DeleteUnderFence) => {
                self.delete_under_fence_busy_items =
                    self.delete_under_fence_busy_items.saturating_add(1);
            }
            None => {}
        }
    }
}

fn direct_delete_exact_owner_source_with(
    activity: &super::MasterKeyActivityTable,
    owner: &NodeID,
    victim: &OwnerSourceEvictionVictim,
    epoch: u64,
    route_lookup: &dyn Fn(&str) -> Option<Arc<super::OneKvNodesRoutes>>,
    delete: impl FnOnce(&OwnerReclaimItem) -> bool,
) -> DirectDeleteResult {
    let member = match plan_exact_owner_source_victim_with(owner, victim, route_lookup) {
        OwnerSourceVictimPlan::Ready(member) => member,
        OwnerSourceVictimPlan::Completed(detail) => {
            let mut result =
                DirectDeleteResult::terminal(OwnerSourceEvictionOutcome::Completed, detail);
            result.ssd_backing_committed =
                exact_ssd_writeback_is_published(owner, victim, route_lookup);
            return result;
        }
        OwnerSourceVictimPlan::Stale(detail) => {
            return DirectDeleteResult::terminal(OwnerSourceEvictionOutcome::Stale, detail);
        }
        OwnerSourceVictimPlan::Rejected(detail) => {
            return DirectDeleteResult::terminal(
                OwnerSourceEvictionOutcome::RejectedNotEvictable,
                detail,
            );
        }
    };
    let item = OwnerReclaimItem {
        key: member.key,
        put_id: member.desc.put_id,
        epoch,
        backing: member
            .expected_backing
            .expect("exact owner source plan must retain its backing"),
        reason: OwnerReclaimReason::OwnerCapacityEviction,
    };
    if let Err(snapshot) = activity.try_install_reclaim(&item) {
        return DirectDeleteResult::activity_busy(snapshot);
    }

    let result = match plan_exact_owner_source_victim_with(owner, victim, route_lookup) {
        OwnerSourceVictimPlan::Ready(_) => {
            let ssd_ready = match victim.ssd_policy {
                OwnerSourceSsdPolicy::Drop => {
                    if victim.ssd_backing_len.is_none() {
                        Ok(())
                    } else {
                        Err(DirectDeleteResult::terminal(
                            OwnerSourceEvictionOutcome::RejectedNotEvictable,
                            format!("drop policy must not carry SSD bytes: key={}", victim.key),
                        ))
                    }
                }
                OwnerSourceSsdPolicy::SelectLastLive => {
                    if victim.ssd_backing_len.is_some() {
                        Err(DirectDeleteResult::terminal(
                            OwnerSourceEvictionOutcome::RejectedNotEvictable,
                            format!(
                                "SSD selection policy must not carry prepared bytes: key={}",
                                victim.key
                            ),
                        ))
                    } else if exact_memory_reclaim_needs_ssd_with(owner, &item, route_lookup) {
                        Err(DirectDeleteResult::terminal(
                            OwnerSourceEvictionOutcome::SsdCandidate,
                            "exact source is the last live backing; owner SSD admission required",
                        ))
                    } else {
                        Ok(())
                    }
                }
                OwnerSourceSsdPolicy::Persisted => match victim.ssd_backing_len {
                    None => Err(DirectDeleteResult::terminal(
                        OwnerSourceEvictionOutcome::RejectedNotEvictable,
                        format!(
                            "persisted SSD policy requires exact bytes: key={}",
                            victim.key
                        ),
                    )),
                    Some(len) => match route_lookup(&victim.key)
                        .map(|route| route.commit_ssd_replica(owner, len))
                    {
                        Some(super::SsdReplicaCommitStatus::Committed) => Ok(()),
                        Some(super::SsdReplicaCommitStatus::LengthMismatch) => {
                            Err(DirectDeleteResult::terminal(
                                OwnerSourceEvictionOutcome::RejectedNotEvictable,
                                format!(
                                    "SSD write-back length does not match the exact source: key={} len={}",
                                    victim.key, len
                                ),
                            ))
                        }
                        Some(
                            super::SsdReplicaCommitStatus::MissingMemory
                            | super::SsdReplicaCommitStatus::TombedNode,
                        )
                        | None => Err(DirectDeleteResult::terminal(
                            OwnerSourceEvictionOutcome::Stale,
                            format!(
                                "exact source changed before SSD write-back publication: key={}",
                                victim.key
                            ),
                        )),
                    },
                },
            };
            match ssd_ready {
                Err(result) => result,
                Ok(()) => {
                    let mut result = if delete(&item) {
                        DirectDeleteResult::terminal(
                            OwnerSourceEvictionOutcome::Completed,
                            if victim.ssd_policy == OwnerSourceSsdPolicy::Persisted {
                                "SSD backing published and exact memory source deleted by batch handler"
                            } else {
                                "exact source route deleted by batch handler"
                            },
                        )
                    } else {
                        DirectDeleteResult::delete_under_fence_busy()
                    };
                    result.ssd_backing_committed =
                        victim.ssd_policy == OwnerSourceSsdPolicy::Persisted;
                    result
                }
            }
        }
        OwnerSourceVictimPlan::Completed(detail) => {
            let mut result =
                DirectDeleteResult::terminal(OwnerSourceEvictionOutcome::Completed, detail);
            result.ssd_backing_committed =
                exact_ssd_writeback_is_published(owner, victim, route_lookup);
            result
        }
        OwnerSourceVictimPlan::Stale(detail) => {
            DirectDeleteResult::terminal(OwnerSourceEvictionOutcome::Stale, detail)
        }
        OwnerSourceVictimPlan::Rejected(detail) => {
            DirectDeleteResult::terminal(OwnerSourceEvictionOutcome::RejectedNotEvictable, detail)
        }
    };
    assert!(
        activity.clear_reclaim(&item),
        "direct-delete master fence must remain installed until route deletion completes"
    );
    result
}

fn direct_delete_exact_owner_source_batch_with(
    activity: &super::MasterKeyActivityTable,
    owner: &NodeID,
    operation_id: u64,
    victims: &[OwnerSourceEvictionVictim],
    route_lookup: &dyn Fn(&str) -> Option<Arc<super::OneKvNodesRoutes>>,
    delete: impl Fn(&OwnerReclaimItem) -> bool,
) -> (
    Vec<OwnerSourceEvictionVictimResp>,
    DirectDeleteBatchBusySummary,
) {
    let mut responses = Vec::with_capacity(victims.len());
    let mut busy = DirectDeleteBatchBusySummary::default();
    for (index, victim) in victims.iter().enumerate() {
        let result = direct_delete_exact_owner_source_with(
            activity,
            owner,
            victim,
            owner_source_eviction_epoch(operation_id, index),
            route_lookup,
            |item| delete(item),
        );
        busy.record(result.busy_cause);
        responses.push(OwnerSourceEvictionVictimResp {
            victim_index: u32::try_from(index).unwrap_or(u32::MAX),
            outcome: result.outcome,
            ssd_backing_committed: result.ssd_backing_committed,
            detail: result.detail,
        });
    }
    (responses, busy)
}

pub(crate) async fn handle_batch_evict_owner_source(
    view: &MasterKvRouterView,
    req: MsgPack<BatchEvictOwnerSourceReq>,
    owner: NodeID,
) -> MsgPack<BatchEvictOwnerSourceResp> {
    let operation_id = req.serialize_part.operation_id;
    let counters = view
        .master_kv_router()
        .eviction_reclaim_counters(owner.as_ref());
    counters
        .source_evict_rpc_requests
        .fetch_add(1, Ordering::Relaxed);
    counters.source_evict_victims.fetch_add(
        u64::try_from(req.serialize_part.victims.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let requested_bytes = req
        .serialize_part
        .victims
        .iter()
        .filter_map(|victim| owner_source_member_weight(&victim.backing))
        .map(u64::from)
        .fold(0u64, u64::saturating_add);
    counters
        .source_evict_requested_bytes
        .fetch_add(requested_bytes, Ordering::Relaxed);
    let current_generation = view
        .cluster_manager()
        .get_member_info_cached(owner.as_ref())
        .map(|member| member.node_start_time);
    if current_generation != Some(req.serialize_part.owner_node_start_time) {
        counters.source_evict_rejected.fetch_add(
            u64::try_from(req.serialize_part.victims.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let err = KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "owner source-eviction generation mismatch: owner={} requested={} current={:?}",
                owner, req.serialize_part.owner_node_start_time, current_generation
            ),
        });
        return MsgPack {
            serialize_part: BatchEvictOwnerSourceResp {
                operation_id,
                victims: Vec::new(),
                error_code: err.code(),
                error_json: err.to_json(),
            },
            raw_bytes: Vec::new(),
        };
    }

    let (responses, busy) = direct_delete_exact_owner_source_batch_with(
        &view.master_kv_router().inner().key_activity,
        &owner,
        operation_id,
        &req.serialize_part.victims,
        &|key| {
            view.master_kv_router()
                .inner()
                .kv_routes
                .get(key)
                .map(|route| route.clone())
        },
        |item| remove_reclaimed_replica(view, &owner, item),
    );
    for response in &responses {
        let outcome_counter = match response.outcome {
            OwnerSourceEvictionOutcome::Accepted => &counters.source_evict_accepted,
            OwnerSourceEvictionOutcome::AlreadyInProgress => &counters.source_evict_in_progress,
            OwnerSourceEvictionOutcome::Completed => &counters.source_evict_completed,
            OwnerSourceEvictionOutcome::SsdCandidate => &counters.source_evict_accepted,
            OwnerSourceEvictionOutcome::RetryableBusy | OwnerSourceEvictionOutcome::Unspecified => {
                &counters.source_evict_retryable_busy
            }
            OwnerSourceEvictionOutcome::Stale => &counters.source_evict_stale,
            OwnerSourceEvictionOutcome::RejectedNotEvictable => &counters.source_evict_rejected,
        };
        outcome_counter.fetch_add(1, Ordering::Relaxed);
    }

    let completed = responses
        .iter()
        .filter(|response| response.outcome == OwnerSourceEvictionOutcome::Completed)
        .count();
    let retryable = responses
        .iter()
        .filter(|response| response.outcome == OwnerSourceEvictionOutcome::RetryableBusy)
        .count();
    let ssd_candidates = responses
        .iter()
        .filter(|response| response.outcome == OwnerSourceEvictionOutcome::SsdCandidate)
        .count();
    tracing::info!(
        owner = %owner,
        operation_id,
        victims = responses.len(),
        completed,
        ssd_candidates,
        retryable,
        activity_busy_items = busy.activity_busy_items,
        put_busy_items = busy.put_busy_items,
        get_busy_items = busy.get_busy_items,
        replica_busy_items = busy.replica_busy_items,
        reclaim_busy_items = busy.reclaim_busy_items,
        inflight_puts = busy.inflight_puts,
        inflight_gets = busy.inflight_gets,
        inflight_replicas = busy.inflight_replicas,
        delete_under_fence_busy_items = busy.delete_under_fence_busy_items,
        "owner source direct-delete batch completed"
    );

    MsgPack {
        serialize_part: BatchEvictOwnerSourceResp {
            operation_id,
            victims: responses,
            error_code: OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

struct RemovedOwnerSource {
    desc: NodeValueReplicaDesc,
    capacity_bytes: u64,
    logical_bytes: u64,
    removed_last_route: bool,
    ssd_survived: bool,
    ssd_became_only_backing: bool,
}

fn record_last_route_removal(
    counters: &super::EvictionReclaimCounters,
    removed: &RemovedOwnerSource,
) {
    if !removed.removed_last_route {
        return;
    }
    counters
        .last_route_removed_members
        .fetch_add(1, Ordering::Relaxed);
    counters
        .last_route_removed_bytes
        .fetch_add(removed.capacity_bytes, Ordering::Relaxed);
}

fn remove_exact_owner_source_route(
    routes: &dashmap::DashMap<String, Arc<super::OneKvNodesRoutes>>,
    owner: &NodeID,
    item: &OwnerReclaimItem,
) -> Option<RemovedOwnerSource> {
    let route = routes.get(&item.key).map(|route| route.clone())?;
    if route.put_id != item.put_id {
        return None;
    }
    let removed_desc = {
        let mut replicas = route.node_replicas.write();
        let Some(node_replicas) = replicas.get_mut(owner) else {
            return None;
        };
        if node_replicas.tomb_tag.is_tomb() {
            return None;
        }
        let Some(replica) = node_replicas.memory.as_ref() else {
            return None;
        };
        if !reclaim_backing_matches(replica, &item.backing) {
            return None;
        }
        let capacity_bytes = replica.backing.capacity_bytes();
        let logical_bytes = replica.backing.len();
        let desc = NodeValueReplicaDesc {
            weight_bytes: u32::try_from(capacity_bytes).unwrap_or(u32::MAX),
            put_id: route.put_id,
        };
        node_replicas.memory.take();
        let ssd_survived = node_replicas.ssd.is_some();
        let remove_node_entry = !ssd_survived;
        if remove_node_entry {
            replicas.remove(owner);
        }
        let live_backings = replicas
            .values()
            .filter(|replicas| !replicas.tomb_tag.is_tomb())
            .map(|replicas| {
                usize::from(replicas.memory.is_some()) + usize::from(replicas.ssd.is_some())
            })
            .sum::<usize>();
        let ssd_became_only_backing = ssd_survived && live_backings == 1;
        Some((
            desc,
            capacity_bytes,
            logical_bytes,
            ssd_survived,
            ssd_became_only_backing,
        ))
    };
    let (removed_desc, capacity_bytes, logical_bytes, ssd_survived, ssd_became_only_backing) =
        removed_desc?;

    let removed_last_route = if route.node_replicas.read().is_empty() {
        routes
            .remove_if(&item.key, |_, current| {
                Arc::ptr_eq(current, &route)
                    && current.put_id == item.put_id
                    && current.node_replicas.read().is_empty()
            })
            .is_some()
    } else {
        false
    };
    Some(RemovedOwnerSource {
        desc: removed_desc,
        capacity_bytes,
        logical_bytes,
        removed_last_route,
        ssd_survived,
        ssd_became_only_backing,
    })
}

fn remove_reclaimed_replica(
    view: &MasterKvRouterView,
    owner: &NodeID,
    item: &OwnerReclaimItem,
) -> bool {
    if !view
        .master_kv_router()
        .inner()
        .key_activity
        .reclaim_matches(item)
    {
        return false;
    }
    let removed =
        remove_exact_owner_source_route(&view.master_kv_router().inner().kv_routes, owner, item);
    if let Some(removed) = removed {
        let ssd_tier = &view.master_kv_router().inner().ssd_tier_counters;
        if removed.ssd_survived {
            ssd_tier
                .memory_removed_ssd_survived_items
                .fetch_add(1, Ordering::Relaxed);
            ssd_tier
                .memory_removed_ssd_survived_bytes
                .fetch_add(removed.logical_bytes, Ordering::Relaxed);
        }
        if removed.ssd_became_only_backing {
            ssd_tier
                .memory_removed_ssd_became_only_items
                .fetch_add(1, Ordering::Relaxed);
            ssd_tier
                .memory_removed_ssd_became_only_bytes
                .fetch_add(removed.logical_bytes, Ordering::Relaxed);
        }
        let counters = view
            .master_kv_router()
            .eviction_reclaim_counters(owner.as_ref());
        record_last_route_removal(counters.as_ref(), &removed);
        if removed.removed_last_route && view.master_kv_router().prefix_index_enabled() {
            let view_task = view.clone();
            let key = item.key.clone();
            let put_id = item.put_id;
            let spawn_view = view.clone();
            let _ = spawn_view.spawn("owner_reclaim_remove_prefix", async move {
                let mut tree = view_task
                    .master_kv_router()
                    .inner()
                    .prefix_index
                    .write()
                    .await;
                tree.remove(&key, put_id);
            });
        }
        if let Some(cache) = view
            .master_kv_router()
            .inner()
            .node_kv_cache_controller
            .get(owner.as_ref())
        {
            let _ = super::remove_exact_cache_entry(cache.value(), &item.key, &removed.desc);
        }
        if let Some(cache) = view
            .master_kv_router()
            .inner()
            .node_writeback_tier1_controller
            .get(owner.as_ref())
        {
            let _ = super::remove_exact_cache_entry(cache.value(), &item.key, &removed.desc);
        }
    }
    true
}

fn finish_unindexed_allocations(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> u32 {
    let mut reclaimed = 0u32;
    for item in items {
        debug_assert!(matches!(
            &item.backing,
            OwnerReclaimBacking::UnindexedAllocation { .. }
        ));
        if remove_reclaimed_replica(view, owner, &item) {
            clear_master_fence(view, &item);
            reclaimed = reclaimed.saturating_add(1);
        } else {
            tracing::error!(
                "unindexed allocation reclaim backing could not be removed from master route: owner={} key={} epoch={}",
                owner,
                item.key,
                item.epoch
            );
        }
    }
    reclaimed
}

/// Return true only when removing this exact memory source would leave the key
/// with no live backing. The caller holds the per-key master reclaim fence, so
/// this route snapshot cannot race another Put/Get/replica/reclaim mutation.
fn exact_memory_reclaim_needs_ssd_with(
    owner: &NodeID,
    item: &OwnerReclaimItem,
    route_lookup: &dyn Fn(&str) -> Option<Arc<super::OneKvNodesRoutes>>,
) -> bool {
    let Some(route) = route_lookup(&item.key) else {
        return false;
    };
    if route.put_id != item.put_id || route.lease_id.is_some() {
        return false;
    }
    let replicas = route.node_replicas.read();
    let Some(target) = replicas.get(owner) else {
        return false;
    };
    if target.tomb_tag.is_tomb()
        || !target
            .memory
            .as_ref()
            .is_some_and(|memory| reclaim_backing_matches(memory, &item.backing))
    {
        return false;
    }

    replicas.iter().all(|(node_id, node_replicas)| {
        if node_replicas.tomb_tag.is_tomb() {
            return true;
        }
        if node_id == owner {
            // The exact memory above is the source being removed. An existing
            // same-owner SSD backing already preserves this route.
            node_replicas.ssd.is_none()
        } else {
            !node_replicas.has_live_backing()
        }
    })
}

fn exact_memory_reclaim_needs_ssd(
    view: &MasterKvRouterView,
    owner: &NodeID,
    item: &OwnerReclaimItem,
) -> bool {
    exact_memory_reclaim_needs_ssd_with(owner, item, &|key| {
        view.master_kv_router()
            .inner()
            .kv_routes
            .get(key)
            .map(|route| route.clone())
    })
}

fn partition_reclaim_coordination<F>(
    items: Vec<OwnerReclaimItem>,
    should_coordinate_unindexed: F,
) -> (Vec<OwnerReclaimItem>, Vec<OwnerReclaimItem>)
where
    F: Fn(&OwnerReclaimItem) -> bool,
{
    items.into_iter().partition(|item| {
        matches!(
            &item.backing,
            OwnerReclaimBacking::UnindexedAllocation { .. }
        ) && !should_coordinate_unindexed(item)
    })
}

fn owner_has_ssd_storage(view: &MasterKvRouterView, owner: &NodeID) -> bool {
    view.cluster_manager()
        .get_member_info_cached(owner.as_ref())
        .and_then(|member| {
            member
                .metadata
                .get(crate::cluster_manager::META_KEY_KV_SSD_STORAGE)
                .cloned()
        })
        .is_some_and(|value| value == "true")
}

async fn finish_committed(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> u32 {
    let mut removed = Vec::new();
    for item in items {
        if remove_reclaimed_replica(view, owner, &item) {
            removed.push(item);
        } else {
            tracing::error!(
                "owner reclaim backing could not be removed from master route: owner={} key={} epoch={}",
                owner,
                item.key,
                item.epoch
            );
        }
    }
    if removed.is_empty() {
        return 0;
    }
    match call_owner_phase(view, owner, OwnerReclaimPhase::Finalize, removed.clone()).await {
        Ok(responses) => {
            let mut finalized = 0u32;
            let mut retry = Vec::new();
            for (item, response) in removed.into_iter().zip(responses.into_iter()) {
                if response.state == OwnerReclaimItemState::Finalized {
                    clear_master_fence(view, &item);
                    finalized = finalized.saturating_add(1);
                } else {
                    tracing::warn!(
                        "owner reclaim finalize returned unresolved state: owner={} key={} epoch={} state={:?} detail={}",
                        owner,
                        item.key,
                        item.epoch,
                        response.state,
                        response.detail
                    );
                    retry.push(item);
                }
            }
            spawn_finalize_retry(view.clone(), owner.clone(), retry);
            finalized
        }
        Err(err) => {
            tracing::warn!(
                "owner reclaim finalize RPC failed; retaining both fences: owner={} keys={} err={}",
                owner,
                removed.len(),
                err
            );
            spawn_finalize_retry(view.clone(), owner.clone(), removed);
            0
        }
    }
}

fn spawn_finalize_retry(view: MasterKvRouterView, owner: NodeID, items: Vec<OwnerReclaimItem>) {
    if items.is_empty() {
        return;
    }
    let spawn_view = view.clone();
    let _ = spawn_view.spawn("owner_reclaim_finalize_retry", async move {
        let mut pending = items;
        let mut delay = Duration::from_millis(25);
        for _attempt in 1..=8 {
            tokio::time::sleep(delay).await;
            match call_owner_phase(&view, &owner, OwnerReclaimPhase::Finalize, pending.clone())
                .await
            {
                Ok(responses) => {
                    let mut next = Vec::new();
                    for (item, response) in pending.into_iter().zip(responses.into_iter()) {
                        if response.state == OwnerReclaimItemState::Finalized {
                            clear_master_fence(&view, &item);
                        } else {
                            next.push(item);
                        }
                    }
                    pending = next;
                    if pending.is_empty() {
                        return;
                    }
                }
                Err(err) => tracing::warn!(
                    "owner reclaim finalize retry failed: owner={} keys={} err={}",
                    owner,
                    pending.len(),
                    err
                ),
            }
            delay = (delay * 2).min(Duration::from_secs(1));
        }
        tracing::error!(
            "owner reclaim finalize retry exhausted; fences retained: owner={} keys={}",
            owner,
            pending.len()
        );
    });
}

async fn reclaim_items(
    view: &MasterKvRouterView,
    owner: &NodeID,
    candidates: Vec<OwnerReclaimItem>,
) -> u32 {
    let counters = view
        .master_kv_router()
        .eviction_reclaim_counters(owner.as_ref());
    let mut fenced = Vec::new();
    for item in candidates {
        match view
            .master_kv_router()
            .inner()
            .key_activity
            .try_install_reclaim(&item)
        {
            Ok(()) => {
                if item_still_valid(view, owner, &item) {
                    fenced.push(item);
                } else {
                    counters.route_changed.fetch_add(1, Ordering::Relaxed);
                    clear_master_fence(view, &item);
                }
            }
            Err(activity) => {
                counters
                    .master_activity_deferred
                    .fetch_add(1, Ordering::Relaxed);
                tracing::trace!(
                    "owner reclaim deferred by master activity: owner={} key={} puts={} gets={} replicas={} reclaim_installed={}",
                    owner,
                    item.key,
                    activity.puts,
                    activity.gets,
                    activity.replicas,
                    activity.reclaim_installed
                );
            }
        }
    }
    if fenced.is_empty() {
        return 0;
    }

    let owner_has_ssd = owner_has_ssd_storage(view, owner);
    let (master_only, owner_coordinated) = partition_reclaim_coordination(fenced, |item| {
        owner_has_ssd && exact_memory_reclaim_needs_ssd(view, owner, item)
    });
    let master_reclaimed = finish_unindexed_allocations(view, owner, master_only);
    if owner_coordinated.is_empty() {
        counters
            .completed
            .fetch_add(u64::from(master_reclaimed), Ordering::Relaxed);
        return master_reclaimed;
    }

    let prepare_responses = match call_owner_phase(
        view,
        owner,
        OwnerReclaimPhase::Prepare,
        owner_coordinated.clone(),
    )
    .await
    {
        Ok(responses) => responses,
        Err(err) => {
            tracing::warn!(
                "owner reclaim prepare RPC failed; aborting batch: owner={} keys={} err={}",
                owner,
                owner_coordinated.len(),
                err
            );
            let _ = abort_prepared(view, owner, owner_coordinated, HashSet::new()).await;
            counters
                .completed
                .fetch_add(u64::from(master_reclaimed), Ordering::Relaxed);
            return master_reclaimed;
        }
    };
    let mut prepared = Vec::new();
    let mut committed = Vec::new();
    let mut ssd_publish_failed = Vec::new();
    let mut newly_published_ssd = HashSet::new();
    for (item, response) in owner_coordinated
        .into_iter()
        .zip(prepare_responses.into_iter())
    {
        match response.state {
            OwnerReclaimItemState::Prepared => {
                match publish_prepared_ssd_backing(view, owner, &item, response.ssd_backing_len) {
                    Ok(newly_published) => {
                        if newly_published {
                            newly_published_ssd.insert((item.key.clone(), item.epoch));
                        }
                        prepared.push(item);
                    }
                    Err(detail) => {
                        counters.route_changed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            owner = %owner,
                            key = %item.key,
                            epoch = item.epoch,
                            detail = %detail,
                            "aborting owner reclaim after SSD backing publication validation failed"
                        );
                        ssd_publish_failed.push(item);
                    }
                }
            }
            OwnerReclaimItemState::Committed => committed.push(item),
            OwnerReclaimItemState::Busy => {
                if response.detail == "owner local memory still has active holders" {
                    counters
                        .owner_holder_deferred
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    counters
                        .owner_other_deferred
                        .fetch_add(1, Ordering::Relaxed);
                }
                clear_master_fence(view, &item);
            }
            _ => {
                counters
                    .owner_other_deferred
                    .fetch_add(1, Ordering::Relaxed);
                clear_master_fence(view, &item);
            }
        }
    }
    committed.extend(abort_prepared(view, owner, ssd_publish_failed, HashSet::new()).await);

    let mut invalid_prepared = Vec::new();
    prepared.retain(|item| {
        let valid = item_still_valid(view, owner, item);
        if !valid {
            counters.route_changed.fetch_add(1, Ordering::Relaxed);
            invalid_prepared.push(item.clone());
        }
        valid
    });
    let invalid_published = invalid_prepared
        .iter()
        .filter_map(|item| {
            let identity = (item.key.clone(), item.epoch);
            newly_published_ssd.contains(&identity).then_some(identity)
        })
        .collect();
    committed.extend(abort_prepared(view, owner, invalid_prepared, invalid_published).await);

    if !prepared.is_empty() {
        match call_owner_phase(view, owner, OwnerReclaimPhase::Commit, prepared.clone()).await {
            Ok(responses) => {
                let mut unresolved = Vec::new();
                for (item, response) in prepared.into_iter().zip(responses.into_iter()) {
                    if response.state == OwnerReclaimItemState::Committed {
                        committed.push(item);
                    } else {
                        unresolved.push(item);
                    }
                }
                let unresolved_published = unresolved
                    .iter()
                    .filter_map(|item| {
                        let identity = (item.key.clone(), item.epoch);
                        newly_published_ssd.contains(&identity).then_some(identity)
                    })
                    .collect();
                committed
                    .extend(abort_prepared(view, owner, unresolved, unresolved_published).await);
            }
            Err(err) => {
                tracing::warn!(
                    "owner reclaim commit RPC failed; resolving with abort: owner={} keys={} err={}",
                    owner,
                    prepared.len(),
                    err
                );
                let prepared_published = prepared
                    .iter()
                    .filter_map(|item| {
                        let identity = (item.key.clone(), item.epoch);
                        newly_published_ssd.contains(&identity).then_some(identity)
                    })
                    .collect();
                committed.extend(abort_prepared(view, owner, prepared, prepared_published).await);
            }
        }
    }
    let reclaimed = master_reclaimed.saturating_add(finish_committed(view, owner, committed).await);
    counters
        .completed
        .fetch_add(u64::from(reclaimed), Ordering::Relaxed);
    reclaimed
}

fn clear_master_fences(view: &MasterKvRouterView, items: &[OwnerReclaimItem]) {
    for item in items {
        clear_master_fence(view, item);
    }
}

fn try_install_master_fences(
    activity: &super::MasterKeyActivityTable,
    items: &[OwnerReclaimItem],
) -> Result<(), (usize, super::MasterKeyActivitySnapshot)> {
    let mut installed = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        match activity.try_install_reclaim(item) {
            Ok(()) => installed.push(item),
            Err(snapshot) => {
                for installed_item in installed {
                    assert!(activity.clear_reclaim(installed_item));
                }
                return Err((index, snapshot));
            }
        }
    }
    Ok(())
}

/// Reclaim one independently selected key.
async fn reclaim_single_victim(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> u32 {
    if items.len() != 1 {
        tracing::error!(
            owner = %owner,
            victims = items.len(),
            "single-key reclaim received a non-singleton request"
        );
        return 0;
    }
    let counters = view
        .master_kv_router()
        .eviction_reclaim_counters(owner.as_ref());
    if let Err((failed_index, activity)) =
        try_install_master_fences(&view.master_kv_router().inner().key_activity, &items)
    {
        counters
            .master_activity_deferred
            .fetch_add(1, Ordering::Relaxed);
        tracing::trace!(
            "single-key reclaim deferred by master activity: owner={} key={} puts={} gets={} replicas={} reclaim_installed={}",
            owner,
            items[failed_index].key,
            activity.puts,
            activity.gets,
            activity.replicas,
            activity.reclaim_installed,
        );
        return 0;
    }
    let fenced = items;
    if fenced
        .iter()
        .any(|item| !item_still_valid(view, owner, item))
    {
        clear_master_fences(view, &fenced);
        return 0;
    }

    let owner_has_ssd = owner_has_ssd_storage(view, owner);
    let (master_only, owner_coordinated) = partition_reclaim_coordination(fenced.clone(), |item| {
        owner_has_ssd && exact_memory_reclaim_needs_ssd(view, owner, item)
    });
    if owner_coordinated.is_empty() {
        // All master-owned allocations are fenced and revalidated before the
        // first route mutation, so no member can be admitted independently.
        if master_only.len() != fenced.len()
            || master_only
                .iter()
                .any(|item| !item_still_valid(view, owner, item))
        {
            clear_master_fences(view, &fenced);
            return 0;
        }
        let reclaimed = finish_unindexed_allocations(view, owner, master_only);
        counters
            .completed
            .fetch_add(u64::from(reclaimed), Ordering::Relaxed);
        return reclaimed;
    }
    if !master_only.is_empty() || owner_coordinated.len() != fenced.len() {
        tracing::error!(
            "BUG: one single-key reclaim mixed master-only and owner-coordinated backings: owner={} victims={} master_only={} owner_coordinated={}",
            owner,
            fenced.len(),
            master_only.len(),
            owner_coordinated.len(),
        );
        clear_master_fences(view, &fenced);
        return 0;
    }

    let all_by_key = owner_coordinated
        .iter()
        .cloned()
        .map(|item| (item.key.clone(), item))
        .collect::<HashMap<_, _>>();
    let mut committed_keys = HashSet::new();
    let mut delay = Duration::from_millis(25);
    let mut rounds = 0u32;
    loop {
        let pending = all_by_key
            .iter()
            .filter(|(key, _)| !committed_keys.contains(*key))
            .map(|(_, item)| item.clone())
            .collect::<Vec<_>>();
        if pending.is_empty() {
            let reclaimed = finish_committed(view, owner, owner_coordinated).await;
            counters
                .completed
                .fetch_add(u64::from(reclaimed), Ordering::Relaxed);
            return reclaimed;
        }

        rounds = rounds.saturating_add(1);
        let prepare =
            call_owner_phase(view, owner, OwnerReclaimPhase::Prepare, pending.clone()).await;
        let Ok(prepare_responses) = prepare else {
            // No member is known committed yet. Abort is both rollback and
            // response-loss resolution: a Committed response moves us onto
            // the mandatory roll-forward branch.
            if committed_keys.is_empty() {
                if let Ok(abort_responses) =
                    call_owner_phase(view, owner, OwnerReclaimPhase::Abort, pending.clone()).await
                {
                    for (item, response) in pending.iter().zip(abort_responses) {
                        if response.state == OwnerReclaimItemState::Committed {
                            committed_keys.insert(item.key.clone());
                        }
                    }
                    if committed_keys.is_empty() {
                        clear_master_fences(view, &fenced);
                        return 0;
                    }
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(1));
            continue;
        };

        let mut prepared = Vec::new();
        let mut blocked = false;
        for (item, response) in pending.iter().cloned().zip(prepare_responses) {
            match response.state {
                OwnerReclaimItemState::Prepared => {
                    match publish_prepared_ssd_backing(view, owner, &item, response.ssd_backing_len)
                    {
                        Ok(_) => prepared.push(item),
                        Err(detail) => {
                            counters.route_changed.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                owner = %owner,
                                key = %item.key,
                                epoch = item.epoch,
                                detail = %detail,
                                "single-key reclaim SSD publication validation failed"
                            );
                            blocked = true;
                        }
                    }
                }
                OwnerReclaimItemState::Committed => {
                    committed_keys.insert(item.key);
                }
                _ => blocked = true,
            }
        }

        if blocked && committed_keys.is_empty() {
            // Nothing irreversible happened. Abort every possibly-prepared
            // member, and only roll back after the response proves that none
            // had crossed Commit during a lost response.
            match call_owner_phase(view, owner, OwnerReclaimPhase::Abort, pending.clone()).await {
                Ok(responses) => {
                    for (item, response) in pending.iter().zip(responses) {
                        if response.state == OwnerReclaimItemState::Committed {
                            committed_keys.insert(item.key.clone());
                        }
                    }
                    if committed_keys.is_empty() {
                        clear_master_fences(view, &fenced);
                        return 0;
                    }
                }
                Err(_) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(1));
                    continue;
                }
            }
        }

        // With no blocked member this is the first atomic Commit attempt. If
        // another member was already observed Committed, this is mandatory
        // roll-forward for the rest of the transaction.
        if !prepared.is_empty() && (!blocked || !committed_keys.is_empty()) {
            if let Ok(commit_responses) =
                call_owner_phase(view, owner, OwnerReclaimPhase::Commit, prepared.clone()).await
            {
                for (item, response) in prepared.iter().zip(commit_responses) {
                    if response.state == OwnerReclaimItemState::Committed {
                        committed_keys.insert(item.key.clone());
                    }
                }
            }
        }

        if rounds == 8 && !committed_keys.is_empty() {
            tracing::warn!(
                "owner single-key reclaim is rolling forward after uncertain commit: owner={} victims={} committed={}",
                owner,
                owner_coordinated.len(),
                committed_keys.len(),
            );
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(1));
    }
}

#[cfg(test)]
mod reclaim_partition_tests {
    use super::{
        OwnerReclaimBacking, OwnerReclaimItem, OwnerReclaimReason, partition_reclaim_coordination,
    };

    fn candidate(index: u32) -> OwnerReclaimItem {
        OwnerReclaimItem {
            key: format!("candidate-{index}"),
            put_id: (u64::from(index), 0),
            epoch: u64::from(index),
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id: u64::from(index),
                slot_index: index,
                slot_size: 8 * 1024 * 1024,
            },
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        }
    }

    #[test]
    fn only_unindexed_allocations_skip_owner_coordination() {
        let mut indexed_allocation = candidate(1);
        indexed_allocation.backing = OwnerReclaimBacking::Allocation;
        indexed_allocation.reason = OwnerReclaimReason::OwnerCapacityEviction;
        let mut unindexed_allocation = candidate(2);
        unindexed_allocation.backing = OwnerReclaimBacking::UnindexedAllocation {
            addr: 0x2000,
            base_addr: 0x1000,
            len: 4096,
            capacity_bytes: 4096,
        };
        unindexed_allocation.reason = OwnerReclaimReason::MasterAllocationCapacity;
        let committed_slot = candidate(3);

        let candidates = vec![
            indexed_allocation.clone(),
            unindexed_allocation.clone(),
            committed_slot.clone(),
        ];
        let (master_only, owner_coordinated) =
            partition_reclaim_coordination(candidates.clone(), |_| false);

        assert_eq!(master_only, vec![unindexed_allocation.clone()]);
        assert_eq!(owner_coordinated.len(), 2);
        assert!(owner_coordinated.iter().all(|item| !matches!(
            &item.backing,
            OwnerReclaimBacking::UnindexedAllocation { .. }
        )));

        let (master_only, owner_coordinated) = partition_reclaim_coordination(candidates, |_| true);
        assert!(master_only.is_empty());
        assert_eq!(
            owner_coordinated,
            vec![indexed_allocation, unindexed_allocation, committed_slot]
        );
    }
}

#[cfg(test)]
mod owner_get_holding_reclaim_tests {
    use super::{OwnerReclaimBacking, OwnerReclaimReason, reclaim_items, route_item};
    use crate::client_kv_api::PutOptionalArgs;
    use crate::config::KvSsdStorageConfig;
    use crate::kv_ssd_storage::MIN_CAPACITY_BYTES;
    use crate::kvcore_test_lib::{
        integration_test_lock, start_master_and_client, start_master_and_client_with_client_config,
        stop_master_and_client,
    };
    use crate::master_kv_router::msg_pack::{PutDoneReq, PutStartReq};
    use crate::master_kv_router::put::{handle_put_done, handle_put_start};
    use crate::memholder::{MemholderManagerTrait, NodeHolderKey};
    use crate::p2p::msg_pack::MsgPack;
    use crate::rpcresp_kvresult_convert::msg_and_error::OK;
    use std::time::{Duration, Instant};

    #[limit_thirdparty::tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn completed_get_holding_does_not_block_two_sided_owner_reclaim() {
        let _test_guard = integration_test_lock().await;
        let (master, client) =
            start_master_and_client("reclaim_get_holding_master", "reclaim_get_holding_owner")
                .await;
        let key = "completed_get_holding_reclaim_key";
        let owner_view = client.client_kv_api_view();
        let owner_api = owner_view.client_kv_api();
        owner_api
            .inner()
            .put(key, &[7u8; 4096], PutOptionalArgs::default())
            .await
            .expect("owner put");
        let (holder, _get_info) = owner_api
            .inner()
            .get(key)
            .await
            .expect("owner get")
            .expect("owner get should hit");

        let owner_id = client
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        let holding_key = NodeHolderKey::new(owner_id.clone(), holder.holder_id());
        let master_view = master.master_kv_router_view().clone();
        assert!(
            master_view
                .master_kv_router()
                .inner()
                .get_holding
                .inner()
                .contains_key(&holding_key),
            "get_done must install the Allocation lifetime holder"
        );

        assert!(
            master_view
                .master_kv_router()
                .inner()
                .key_activity
                .is_quiescent(key),
            "completed get must release its master key-activity lease"
        );
        let owner_node: crate::cluster_manager::NodeID = owner_id.clone().into();
        let busy_item = route_item(
            &master_view,
            &owner_node,
            key,
            None,
            None,
            OwnerReclaimReason::OwnerCapacityEviction,
            master_view.master_kv_router().next_owner_reclaim_epoch(),
        )
        .expect("active-holder owner route should be reclaimable after the reader exits");
        assert_eq!(
            reclaim_items(&master_view, &owner_node, vec![busy_item]).await,
            0,
            "owner Prepare must reject reclaim while the user holder is live"
        );

        drop(holder);
        limit_thirdparty::tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            master_view
                .master_kv_router()
                .inner()
                .get_holding
                .inner()
                .contains_key(&holding_key),
            "the committed local index intentionally keeps MemoryInfo and its ACK holder alive"
        );

        let item = route_item(
            &master_view,
            &owner_node,
            key,
            None,
            None,
            OwnerReclaimReason::OwnerCapacityEviction,
            master_view.master_kv_router().next_owner_reclaim_epoch(),
        )
        .expect("current owner route should be reclaimable");
        assert_eq!(
            item.backing,
            OwnerReclaimBacking::Allocation,
            "reuse-replica get_done must publish the owner-local index on the route"
        );
        assert_eq!(
            reclaim_items(&master_view, &owner_node, vec![item]).await,
            1
        );

        let wait_started = Instant::now();
        while master_view
            .master_kv_router()
            .inner()
            .get_holding
            .inner()
            .contains_key(&holding_key)
        {
            assert!(
                wait_started.elapsed() < Duration::from_secs(5),
                "owner reclaim must drop MemoryInfo and deliver its delete ACK"
            );
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !master_view
                .master_kv_router()
                .inner()
                .kv_routes
                .contains_key(key),
            "the reclaimed last replica route must be removed"
        );

        stop_master_and_client(master, client).await;
    }

    #[limit_thirdparty::tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn master_unindexed_allocation_reclaim_publishes_ssd_and_reloads_exact_bytes() {
        let _test_guard = integration_test_lock().await;
        let (master, client) = start_master_and_client_with_client_config(
            "master_capacity_ssd_reclaim_master",
            "master_capacity_ssd_reclaim_owner",
            |config| {
                config.ssd_storage = Some(KvSsdStorageConfig {
                    limit_bytes: MIN_CAPACITY_BYTES,
                    write_rate_limit_bytes_per_sec: None,
                    write_burst_bytes: None,
                });
                let target = std::env::var("CARGO_TARGET_DIR")
                    .expect("SSD integration test requires the NVMe Cargo target");
                let root = format!(
                    "{target}/kv_ssd_integration/master_capacity_reclaim-{}",
                    std::process::id()
                );
                config.share_mem_path = format!("{root}/sharemem");
                config.large_file_paths.paths = vec![format!("{root}/large")];
            },
        )
        .await;
        let key = "master_capacity_ssd_reclaim_key";
        let payload = vec![0x5au8; 4096];
        let owner_view = client.client_kv_api_view();
        let owner_api = owner_view.client_kv_api();
        let owner_info = client
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info();
        let owner_id = owner_info.id;
        let owner_node: crate::cluster_manager::NodeID = owner_id.clone().into();
        let master_view = master.master_kv_router_view().clone();
        assert!(
            master_view
                .cluster_manager()
                .get_member_info_cached(&owner_id)
                .and_then(|member| {
                    member
                        .metadata
                        .get(crate::cluster_manager::META_KEY_KV_SSD_STORAGE)
                        .cloned()
                })
                .is_some_and(|value| value == "true"),
            "master must observe the owner's SSD capability before selecting coordination"
        );

        let (_put_id, start) = handle_put_start(
            master_view.clone(),
            MsgPack {
                serialize_part: PutStartReq {
                    key: key.to_string(),
                    len: payload.len() as u64,
                    reject_if_inflight_same_key: false,
                    reject_if_exist_same_key: false,
                    make_replica_task: true,
                    preferred_sub_cluster: None,
                    source_node_id: None,
                },
                raw_bytes: Vec::new(),
            },
            owner_node.clone(),
        )
        .await;
        assert_eq!(
            start.serialize_part.error_code, OK,
            "PutStart failed: {}",
            start.serialize_part.error_json
        );
        assert_eq!(start.serialize_part.node_id, owner_id);
        owner_view
            .client_seg_pool()
            .copy_into_segment(start.serialize_part.target_addr, &payload)
            .await
            .expect("copy payload into the master-owned owner segment Allocation");
        let done = handle_put_done(
            master_view.clone(),
            MsgPack {
                serialize_part: PutDoneReq {
                    key: key.to_string(),
                    put_id: start.serialize_part.put_id,
                    lease_id: None,
                    committed_slot: None,
                    publish_local_cache: false,
                    atomic_group: None,
                },
                raw_bytes: Vec::new(),
            },
            owner_node.clone(),
        )
        .await;
        assert_eq!(done.serialize_part.error_code, OK);
        assert_eq!(done.serialize_part.local_cache_holder_id, None);
        let route = master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .get(key)
            .map(|route| route.clone())
            .expect("PutDone must publish the production unindexed Allocation route");
        assert!(
            route
                .node_replicas
                .read()
                .get(&owner_node)
                .and_then(|replicas| replicas.memory.as_ref())
                .is_some_and(|memory| !memory.owner_local_indexed),
            "production remote backing must have no owner-local key index"
        );
        drop(route);

        let item = route_item(
            &master_view,
            &owner_node,
            key,
            None,
            None,
            OwnerReclaimReason::MasterAllocationCapacity,
            master_view.master_kv_router().next_owner_reclaim_epoch(),
        )
        .expect("master-capacity victim must resolve to the current owner Allocation");
        assert!(matches!(
            item.backing,
            OwnerReclaimBacking::UnindexedAllocation { .. }
        ));
        assert_eq!(
            reclaim_items(&master_view, &owner_node, vec![item]).await,
            1
        );

        let route = master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .get(key)
            .map(|route| route.clone())
            .expect("SSD backing must keep the route alive after DRAM reclaim");
        {
            let replicas = route.node_replicas.read();
            let owner_backings = replicas
                .get(&owner_node)
                .expect("the SSD-only owner route must remain");
            assert!(owner_backings.memory.is_none());
            assert_eq!(
                owner_backings.ssd.as_ref().map(|ssd| ssd.len),
                Some(payload.len() as u64)
            );
        }
        let persisted = owner_api
            .inner()
            .kv_ssd_storage_usage_snapshot()
            .expect("test owner SSD must be configured");
        assert_eq!(persisted.persist_successes, 1);
        assert_eq!(persisted.persist_failures, 0);
        assert_eq!(persisted.used_bytes, payload.len() as u64);

        let (holder, _get_info) = owner_api
            .inner()
            .get(key)
            .await
            .expect("SSD-backed get")
            .expect("SSD-only route must be readable");
        assert_eq!(holder.bytes(), payload);
        drop(holder);
        let loaded = owner_api
            .inner()
            .kv_ssd_storage_usage_snapshot()
            .expect("test owner SSD must remain configured");
        assert_eq!(loaded.load_successes, 1);
        assert_eq!(loaded.load_failures, 0);
        assert_eq!(loaded.load_bytes, payload.len() as u64);
        assert_eq!(loaded.memory_hits + loaded.disk_hits + loaded.outer_hits, 1);

        stop_master_and_client(master, client).await;
    }
}

fn request_is_current(view: &MasterKvRouterView, request: &EvictionReclaimRequest) -> bool {
    if let Some(expected_generation) = request.owner_node_start_time
        && view
            .cluster_manager()
            .get_member_info_cached(&request.owner_node_id)
            .map(|member| member.node_start_time)
            != Some(expected_generation)
    {
        return false;
    }
    match request.origin {
        EvictionReclaimOrigin::MasterAllocationCapacity => request.members.iter().all(|member| {
            view.master_kv_router().eviction_cache_entry_is_current(
                &request.owner_node_id,
                &member.key,
                &member.desc,
            )
        }),
        EvictionReclaimOrigin::OwnerCapacityEviction => {
            let owner: NodeID = request.owner_node_id.clone().into();
            request.members.iter().all(|member| {
                let Some(expected_backing) = member.expected_backing.as_ref() else {
                    return false;
                };
                let Some(route) = view
                    .master_kv_router()
                    .inner()
                    .kv_routes
                    .get(&member.key)
                    .map(|entry| entry.clone())
                else {
                    return false;
                };
                if route.put_id != member.desc.put_id || route.lease_id.is_some() {
                    return false;
                }
                route
                    .node_replicas
                    .read()
                    .get(&owner)
                    .is_some_and(|node_replicas| {
                        !node_replicas.tomb_tag.is_tomb()
                            && node_replicas.memory.as_ref().is_some_and(|replica| {
                                replica.owner_local_indexed
                                    && reclaim_backing_matches(replica, expected_backing)
                            })
                    })
            })
        }
    }
}

fn restore_request_entries(view: &MasterKvRouterView, request: &EvictionReclaimRequest) -> usize {
    request
        .members
        .iter()
        .filter(|member| {
            view.master_kv_router()
                .restore_eviction_cache_entry_if_current(
                    &request.owner_node_id,
                    member.key.clone(),
                    member.desc.clone(),
                )
        })
        .count()
}

fn spawn_eviction_reclaim_retry(view: MasterKvRouterView, requests: Vec<EvictionReclaimRequest>) {
    if requests.is_empty() {
        return;
    }
    let mut delayed = Vec::with_capacity(requests.len());
    let mut restored_count = 0usize;
    let mut restored_weight = 0u64;
    for mut request in requests {
        request.retry_count = request.retry_count.saturating_add(1);
        let counters = view
            .master_kv_router()
            .eviction_reclaim_counters(&request.owner_node_id);
        let weight = request.weight_bytes();
        if !request_is_current(&view, &request) {
            view.master_kv_router().complete_eviction_reclaim(&request);
            counters.route_changed.fetch_add(1, Ordering::Relaxed);
            counters.retry_completed.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if request.origin == EvictionReclaimOrigin::MasterAllocationCapacity
            && should_restore_after_retry(request.retry_count)
        {
            // Release the old identity before reinsertion.  A bounded remote
            // cache may synchronously produce a fresh Size event; that event
            // must own a new, independently-accounted lifecycle.
            view.master_kv_router().complete_eviction_reclaim(&request);
            let restored = restore_request_entries(&view, &request);
            if restored == request.members.len() {
                counters.retry_restored.fetch_add(1, Ordering::Relaxed);
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                restored_count += restored;
                restored_weight = restored_weight.saturating_add(weight);
                continue;
            }
            counters.route_changed.fetch_add(1, Ordering::Relaxed);
            counters.retry_completed.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        counters.retry_queued.fetch_add(1, Ordering::Relaxed);
        delayed.push(request);
    }
    if restored_count != 0 {
        tracing::info!(
            "safe eviction reclaim restored current cache entries after bounded retry: entries={} weight_bytes={} max_retry_count={}",
            restored_count,
            restored_weight,
            EVICTION_RECLAIM_MAX_RETRY_COUNT
        );
    }
    if delayed.is_empty() {
        return;
    }
    let max_retry_count = delayed
        .iter()
        .map(|request| request.retry_count)
        .max()
        .unwrap_or(1);
    let retry_delay = eviction_reclaim_retry_delay(max_retry_count);
    let spawn_view = view.clone();
    let _ = spawn_view.spawn("eviction_reclaim_retry", async move {
        tokio::time::sleep(retry_delay).await;
        let tx = view.master_kv_router().inner().eviction_reclaim_tx.clone();
        for request in delayed {
            let counters = view
                .master_kv_router()
                .eviction_reclaim_counters(&request.owner_node_id);
            if !request_is_current(&view, &request) {
                view.master_kv_router().complete_eviction_reclaim(&request);
                counters.route_changed.fetch_add(1, Ordering::Relaxed);
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if let Err(err) = tx.send(request) {
                let request = err.0;
                view.master_kv_router().complete_eviction_reclaim(&request);
                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "lossless eviction reclaim retry channel closed: owner={} members={}",
                    request.owner_node_id,
                    request.members.len(),
                );
            }
        }
    });
}

pub(crate) fn spawn_eviction_reclaim_actor(
    view: MasterKvRouterView,
    mut rx: limit_thirdparty::tokio::sync::ampsc::UnboundedReceiver<EvictionReclaimRequest>,
) {
    let view_task = view.clone();
    let _ = view.spawn("eviction_reclaim_actor", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let first = tokio::select! {
                _ = shutdown_waiter.wait() => break,
                request = rx.recv() => {
                    let Some(request) = request else { break; };
                    request
                }
            };
            let mut batch = Vec::with_capacity(OWNER_RECLAIM_MAX_BATCH);
            batch.push(first);
            let mut merge_window = Box::pin(tokio::time::sleep(OWNER_RECLAIM_MERGE_WINDOW));
            while batch.len() < OWNER_RECLAIM_MAX_BATCH {
                tokio::select! {
                    _ = &mut merge_window => break,
                    request = rx.recv() => {
                        let Some(request) = request else { break; };
                        batch.push(request);
                    }
                }
            }

            let mut groups: HashMap<NodeIDString, Vec<EvictionReclaimRequest>> = HashMap::new();
            for request in batch {
                groups
                    .entry(request.owner_node_id.clone())
                    .or_default()
                    .push(request);
            }
            for (owner_node_id, requests) in groups {
                let owner: NodeID = owner_node_id.clone().into();
                let counters = view_task
                    .master_kv_router()
                    .eviction_reclaim_counters(&owner_node_id);
                let mut pending = std::collections::VecDeque::from(requests);
                let mut retry_requests = Vec::new();
                while !pending.is_empty() {
                    let mut accounting_requests = Vec::with_capacity(OWNER_RECLAIM_RPC_BATCH);
                    let mut items = Vec::with_capacity(OWNER_RECLAIM_RPC_BATCH);
                    for _ in 0..OWNER_RECLAIM_RPC_BATCH {
                        let Some(request) = pending.pop_front() else {
                            break;
                        };
                        let (members, reason) = match request.origin {
                            EvictionReclaimOrigin::OwnerCapacityEviction => (
                                request.members.clone(),
                                OwnerReclaimReason::OwnerCapacityEviction,
                            ),
                            EvictionReclaimOrigin::MasterAllocationCapacity => {
                                let member = match plan_master_allocation_capacity_victim(
                                    &view_task,
                                    &request,
                                ) {
                                    Ok(member) => member,
                                    Err(MasterCapacityPlanError::CommittedSlot) => {
                                        counters
                                            .capacity_eviction_non_ring_b_entry_total
                                            .fetch_add(1, Ordering::Relaxed);
                                        counters
                                            .capacity_eviction_hit_committed_slot
                                            .fetch_add(1, Ordering::Relaxed);
                                        view_task
                                            .master_kv_router()
                                            .complete_eviction_reclaim(&request);
                                        let restored =
                                            restore_request_entries(&view_task, &request);
                                        tracing::error!(
                                            "BUG: master Allocation capacity event resolved to CommittedSlot; restored metadata: owner={} members={} restored={}",
                                            owner_node_id,
                                            request.members.len(),
                                            restored,
                                        );
                                        continue;
                                    }
                                    Err(MasterCapacityPlanError::RouteChanged) => {
                                        view_task
                                            .master_kv_router()
                                            .complete_eviction_reclaim(&request);
                                        counters.route_changed.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                    Err(MasterCapacityPlanError::WrongRole) => {
                                        counters
                                            .capacity_eviction_non_ring_b_entry_total
                                            .fetch_add(1, Ordering::Relaxed);
                                        view_task
                                            .master_kv_router()
                                            .complete_eviction_reclaim(&request);
                                        let restored =
                                            restore_request_entries(&view_task, &request);
                                        tracing::error!(
                                            "BUG: master Allocation Size event resolved to a non-ring-B route; restored metadata: owner={} members={} restored={}",
                                            owner_node_id,
                                            request.members.len(),
                                            restored,
                                        );
                                        continue;
                                    }
                                };
                                (vec![member], OwnerReclaimReason::MasterAllocationCapacity)
                            }
                        };

                        let planned = members
                            .iter()
                            .map(|member| {
                                let item = route_item(
                                    &view_task,
                                    &owner,
                                    &member.key,
                                    Some(member.desc.put_id),
                                    None,
                                    reason,
                                    view_task.master_kv_router().next_owner_reclaim_epoch(),
                                )?;
                                if member
                                    .expected_backing
                                    .as_ref()
                                    .is_some_and(|expected| expected != &item.backing)
                                {
                                    return None;
                                }
                                Some(item)
                            })
                            .collect::<Option<Vec<_>>>();
                        if let Some(mut planned) = planned {
                            items.append(&mut planned);
                        }
                        accounting_requests.push(request);
                    }

                    if !items.is_empty() {
                        let _ = reclaim_items(&view_task, &owner, items).await;
                    }
                    for accounting_request in accounting_requests {
                        if request_is_current(&view_task, &accounting_request) {
                            retry_requests.push(accounting_request);
                        } else {
                            view_task
                                .master_kv_router()
                                .complete_eviction_reclaim(&accounting_request);
                            counters.route_changed.fetch_add(1, Ordering::Relaxed);
                            if accounting_request.retry_count != 0 {
                                counters.retry_completed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                let retry_count = retry_requests.len();
                spawn_eviction_reclaim_retry(view_task.clone(), retry_requests);
                tracing::trace!(
                    "batched single-key eviction reclaim completed: owner={} retry_deferred={}",
                    owner_node_id,
                    retry_count,
                );
            }
        }
    });
}
