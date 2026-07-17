use super::{KvReplicaBacking, MasterKvRouterView, NodeValueReplicaDesc};
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::master_kv_router::msg_pack::{
    BatchEvictOwnerSourceReq, BatchEvictOwnerSourceResp, BatchOwnerReclaimReq, OwnerReclaimBacking,
    OwnerReclaimItem, OwnerReclaimItemResp, OwnerReclaimItemState, OwnerReclaimPhase,
    OwnerReclaimReason, OwnerSourceEvictionCohort, OwnerSourceEvictionCohortResp,
    OwnerSourceEvictionOutcome,
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
mod cohort_transaction_tests {
    use super::*;
    use crate::master_kv_router::msg_pack::{
        OwnerSourceEvictionMember, PutAtomicGroup, PutAtomicGroupMember,
    };
    use crate::master_kv_router::{
        CommittedSlotReplica, KvRouteInfo, MasterKeyActivityKind, MasterKeyActivityTable,
        OneKvNodesRoutes,
    };
    use crate::master_seg_manager::NodeTombTag;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

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
    fn cohort_master_fences_roll_back_all_members_when_one_is_busy() {
        let activity = Arc::new(MasterKeyActivityTable::default());
        let items = vec![item("rank0", 1), item("rank1", 2)];
        let _busy = activity
            .reserve("rank1", MasterKeyActivityKind::Get, false)
            .unwrap();

        assert!(try_install_cohort_master_fences(&activity, &items).is_err());
        assert!(!activity.has_reclaim("rank0"));
        assert!(!activity.has_reclaim("rank1"));
    }

    #[test]
    fn cohort_master_fences_install_and_clear_as_one_set() {
        let activity = Arc::new(MasterKeyActivityTable::default());
        let items = vec![item("rank0", 1), item("rank1", 2)];

        try_install_cohort_master_fences(&activity, &items).unwrap();
        assert!(activity.has_reclaim("rank0"));
        assert!(activity.has_reclaim("rank1"));
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

    fn source_member(
        key: &str,
        put_id: (u64, u32),
        grant_id: u64,
        slot_index: u32,
    ) -> OwnerSourceEvictionMember {
        OwnerSourceEvictionMember {
            key: key.to_string(),
            put_id,
            backing: OwnerReclaimBacking::CommittedSlot {
                grant_id,
                slot_index,
                slot_size: 4096,
            },
        }
    }

    fn source_route(
        owner: &NodeID,
        member: &OwnerSourceEvictionMember,
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
        let owner_replica = KvRouteInfo {
            node_id: owner.clone(),
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
            tomb_tag: NodeTombTag::new(),
        };
        let mut replicas = HashMap::from([(owner.clone(), owner_replica)]);
        if include_cpu_replica {
            let cpu: NodeID = "cpu0".to_string().into();
            replicas.insert(
                cpu.clone(),
                KvRouteInfo {
                    node_id: cpu.clone(),
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
                    tomb_tag: NodeTombTag::new(),
                },
            );
        }
        Arc::new(OneKvNodesRoutes {
            put_id: member.put_id,
            lease_id: None,
            atomic_group,
            nodes_replicas: RwLock::new(replicas),
            get_durable_slots_used: AtomicU32::new(0),
        })
    }

    fn source_reclaim_item(member: &OwnerSourceEvictionMember) -> OwnerReclaimItem {
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
        let member = source_member("single", (10, 1), 7, 3);
        let cohort = OwnerSourceEvictionCohort {
            members: vec![member.clone()],
        };

        for include_cpu_replica in [false, true] {
            let route = source_route(&owner, &member, None, include_cpu_replica);
            let plan = plan_exact_owner_source_cohort_with(&owner, &cohort, &|key| {
                (key == "single").then(|| route.clone())
            });
            match plan {
                OwnerSourceCohortPlan::Ready {
                    members,
                    all_sources_present,
                } => {
                    assert!(all_sources_present);
                    assert_eq!(members.len(), 1);
                    assert_eq!(members[0].key, "single");
                    assert_eq!(members[0].expected_backing, Some(member.backing.clone()));
                }
                _ => panic!("exact current owner source must be accepted"),
            }
        }
    }

    #[test]
    fn exact_source_removal_deletes_only_gpu_when_cpu_exists_and_last_route_otherwise() {
        let owner: NodeID = "gpu0".to_string().into();
        let member = source_member("with-cpu", (10, 2), 7, 4);
        let routes = dashmap::DashMap::new();
        routes.insert(
            member.key.clone(),
            source_route(&owner, &member, None, true),
        );
        let removed =
            remove_exact_owner_source_route(&routes, &owner, &source_reclaim_item(&member))
                .expect("exact GPU source must be removed");
        assert!(!removed.removed_last_route);
        let remaining = routes.get(&member.key).expect("CPU route must remain");
        assert!(!remaining.nodes_replicas.read().contains_key(&owner));
        assert!(remaining.nodes_replicas.read().contains_key("cpu0"));

        let last = source_member("last", (10, 3), 7, 5);
        routes.insert(last.key.clone(), source_route(&owner, &last, None, false));
        let removed = remove_exact_owner_source_route(&routes, &owner, &source_reclaim_item(&last))
            .expect("last exact GPU source must be removed");
        assert!(removed.removed_last_route);
        assert!(!routes.contains_key(&last.key));

        let stale = source_member("stale", (10, 4), 8, 6);
        routes.insert(stale.key.clone(), source_route(&owner, &stale, None, false));
        let wrong_identity = source_member("stale", stale.put_id, 999, 6);
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
    fn partial_source_absence_is_replay_only_and_all_absent_is_completed() {
        let owner: NodeID = "gpu0".to_string().into();
        let a = source_member("a", (11, 0), 8, 0);
        let b = source_member("b", (11, 1), 8, 1);
        let cohort = OwnerSourceEvictionCohort {
            members: vec![a.clone(), b.clone()],
        };
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
        let route_a = source_route(&owner, &a, Some(group), false);

        match plan_exact_owner_source_cohort_with(&owner, &cohort, &|key| {
            (key == "a").then(|| route_a.clone())
        }) {
            OwnerSourceCohortPlan::Ready {
                all_sources_present,
                ..
            } => assert!(!all_sources_present),
            _ => panic!("a partially removed accepted cohort must be recognized as replay-only"),
        }
        assert!(matches!(
            plan_exact_owner_source_cohort_with(&owner, &cohort, &|_| None),
            OwnerSourceCohortPlan::Completed(_)
        ));
    }

    #[test]
    fn one_stale_or_inconsistent_member_rejects_the_whole_source_cohort() {
        let owner: NodeID = "gpu0".to_string().into();
        let a = source_member("a", (12, 0), 9, 0);
        let b = source_member("b", (12, 1), 9, 1);
        let cohort = OwnerSourceEvictionCohort {
            members: vec![a.clone(), b.clone()],
        };
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
        let changed_b = source_member("b", b.put_id, 99, 1);
        let route_b_with_changed_backing =
            source_route(&owner, &changed_b, Some(group.clone()), false);
        let stale_backing_plan =
            plan_exact_owner_source_cohort_with(&owner, &cohort, &|key| match key {
                "a" => Some(route_a.clone()),
                "b" => Some(route_b_with_changed_backing.clone()),
                _ => None,
            });
        assert!(matches!(
            stale_backing_plan,
            OwnerSourceCohortPlan::Stale(_)
        ));

        let route_b_without_group = source_route(&owner, &b, None, false);
        let inconsistent_group_plan =
            plan_exact_owner_source_cohort_with(&owner, &cohort, &|key| match key {
                "a" => Some(route_a.clone()),
                "b" => Some(route_b_without_group.clone()),
                _ => None,
            });
        assert!(matches!(
            inconsistent_group_plan,
            OwnerSourceCohortPlan::Rejected(_)
        ));
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
    InvalidCohort,
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
    let replicas = route.nodes_replicas.read();
    let replica = replicas
        .get(owner)
        .filter(|replica| !replica.tomb_tag.is_tomb())
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

/// Expand one master Size event by point-looking up its atomic/TP cohort.
/// This function never iterates the resident cache or global route table.
fn plan_master_allocation_capacity_cohort(
    view: &MasterKvRouterView,
    request: &EvictionReclaimRequest,
) -> Result<Vec<EvictionReclaimMember>, MasterCapacityPlanError> {
    if request.origin != EvictionReclaimOrigin::MasterAllocationCapacity
        || request.members.is_empty()
    {
        return Err(MasterCapacityPlanError::WrongRole);
    }
    let owner: NodeID = request.owner_node_id.clone().into();
    let anchor = &request.members[0];
    let (current_anchor, anchor_route) = allocation_member_from_route(view, &owner, &anchor.key)?;
    if current_anchor.desc.put_id != anchor.desc.put_id
        || current_anchor.desc.weight_bytes != anchor.desc.weight_bytes
    {
        return Err(MasterCapacityPlanError::RouteChanged);
    }
    let mut cohort_keys =
        super::current_tp_cohort_keys(&anchor.key, &anchor_route, &current_anchor.desc)
            .map_err(|_| MasterCapacityPlanError::InvalidCohort)?;
    cohort_keys.sort_unstable();
    cohort_keys.dedup();
    if cohort_keys.is_empty() {
        return Err(MasterCapacityPlanError::InvalidCohort);
    }

    let mut members = Vec::with_capacity(cohort_keys.len());
    for key in &cohort_keys {
        let (member, route) = allocation_member_from_route(view, &owner, key)?;
        let mut peer_keys = super::current_tp_cohort_keys(key, &route, &member.desc)
            .map_err(|_| MasterCapacityPlanError::InvalidCohort)?;
        peer_keys.sort_unstable();
        peer_keys.dedup();
        if peer_keys != cohort_keys {
            return Err(MasterCapacityPlanError::InvalidCohort);
        }
        members.push(member);
    }
    Ok(members)
}

fn detach_present_capacity_cohort_siblings(
    view: &MasterKvRouterView,
    owner_node_id: &str,
    members: &[EvictionReclaimMember],
) -> Result<Vec<EvictionReclaimMember>, MasterCapacityPlanError> {
    let cache = view
        .master_kv_router()
        .get_node_cache_controller(owner_node_id)
        .ok_or(MasterCapacityPlanError::RouteChanged)?;
    let mut expected = HashMap::new();
    for member in members {
        if let Some(cached) = cache.get(&member.key) {
            if cached.put_id != member.desc.put_id
                || cached.weight_bytes != member.desc.weight_bytes
            {
                return Err(MasterCapacityPlanError::RouteChanged);
            }
            expected.insert(member.key.clone(), member.desc.clone());
        }
    }
    if expected.is_empty() {
        return Ok(Vec::new());
    }
    super::take_exact_cache_entries(&cache, &expected)
        .map(|removed| {
            removed
                .into_iter()
                .map(|(key, desc)| EvictionReclaimMember {
                    key,
                    desc,
                    expected_backing: None,
                })
                .collect()
        })
        .ok_or(MasterCapacityPlanError::RouteChanged)
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
    let replicas = route.nodes_replicas.read();
    let target = replicas.get(owner_node_id)?;
    if target.tomb_tag.is_tomb() {
        return None;
    }
    let backing = match &target.backing {
        KvReplicaBacking::Allocation(_) if required_slot_size.is_none() => {
            if target.owner_local_indexed {
                OwnerReclaimBacking::Allocation
            } else {
                OwnerReclaimBacking::UnindexedAllocation
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
            OwnerReclaimBacking::Allocation | OwnerReclaimBacking::UnindexedAllocation => None,
            OwnerReclaimBacking::CommittedSlot { slot_size, .. } => Some(*slot_size),
        },
        item.reason,
        item.epoch,
    )
    .is_some_and(|current| current.backing == item.backing && current.reason == item.reason)
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
    debug_assert!(
        items
            .iter()
            .all(|item| item.backing != OwnerReclaimBacking::UnindexedAllocation)
    );
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

#[cfg(test)]
async fn abort_prepared(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
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
                    OwnerReclaimItemState::Aborted
                    | OwnerReclaimItemState::Stale
                    | OwnerReclaimItemState::Finalized => clear_master_fence(view, &item),
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
            spawn_abort_retry(view.clone(), owner.clone(), items);
            Vec::new()
        }
    }
}

#[cfg(test)]
fn spawn_abort_retry(view: MasterKvRouterView, owner: NodeID, items: Vec<OwnerReclaimItem>) {
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
                            OwnerReclaimItemState::Aborted
                            | OwnerReclaimItemState::Stale
                            | OwnerReclaimItemState::Finalized => clear_master_fence(&view, &item),
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

fn reclaim_backing_matches(replica: &super::KvRouteInfo, expected: &OwnerReclaimBacking) -> bool {
    match (&replica.backing, expected) {
        (KvReplicaBacking::Allocation(_), OwnerReclaimBacking::Allocation) => {
            replica.owner_local_indexed
        }
        (KvReplicaBacking::Allocation(_), OwnerReclaimBacking::UnindexedAllocation) => {
            !replica.owner_local_indexed
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
        OwnerReclaimBacking::Allocation | OwnerReclaimBacking::UnindexedAllocation => None,
    }
}

enum OwnerSourceCohortPlan {
    Ready {
        members: Vec<EvictionReclaimMember>,
        all_sources_present: bool,
    },
    Completed(&'static str),
    Stale(String),
    Rejected(String),
}

fn plan_exact_owner_source_cohort(
    view: &MasterKvRouterView,
    owner: &NodeID,
    cohort: &OwnerSourceEvictionCohort,
) -> OwnerSourceCohortPlan {
    plan_exact_owner_source_cohort_with(owner, cohort, &|key| {
        view.master_kv_router()
            .inner()
            .kv_routes
            .get(key)
            .map(|entry| entry.clone())
    })
}

fn plan_exact_owner_source_cohort_with(
    owner: &NodeID,
    cohort: &OwnerSourceEvictionCohort,
    route_lookup: &dyn Fn(&str) -> Option<Arc<super::OneKvNodesRoutes>>,
) -> OwnerSourceCohortPlan {
    if cohort.members.is_empty() {
        return OwnerSourceCohortPlan::Rejected("empty source-eviction cohort".to_string());
    }

    let mut expected_keys = cohort
        .members
        .iter()
        .map(|member| member.key.clone())
        .collect::<Vec<_>>();
    expected_keys.sort_unstable();
    expected_keys.dedup();
    if expected_keys.len() != cohort.members.len() {
        return OwnerSourceCohortPlan::Rejected(
            "source-eviction cohort contains duplicate keys".to_string(),
        );
    }

    let mut planned = Vec::with_capacity(cohort.members.len());
    let mut absent_sources = 0usize;
    for member in &cohort.members {
        let Some(weight_bytes) = owner_source_member_weight(&member.backing) else {
            return OwnerSourceCohortPlan::Rejected(format!(
                "source backing is not an exact committed slot: key={}",
                member.key
            ));
        };
        let desc = NodeValueReplicaDesc {
            weight_bytes,
            put_id: member.put_id,
        };
        planned.push(EvictionReclaimMember {
            key: member.key.clone(),
            desc: desc.clone(),
            expected_backing: Some(member.backing.clone()),
        });

        let Some(route) = route_lookup(&member.key) else {
            absent_sources += 1;
            continue;
        };
        if route.put_id != member.put_id {
            return OwnerSourceCohortPlan::Stale(format!(
                "route version changed: key={} expected=({},{}) current=({},{})",
                member.key, member.put_id.0, member.put_id.1, route.put_id.0, route.put_id.1,
            ));
        }
        if route.lease_id.is_some() {
            return OwnerSourceCohortPlan::Rejected(format!(
                "leased route is not cache-evictable: key={}",
                member.key
            ));
        }
        let replica_matches = {
            let replicas = route.nodes_replicas.read();
            match replicas.get(owner) {
                Some(replica) if !replica.tomb_tag.is_tomb() => {
                    if !replica.owner_local_indexed {
                        return OwnerSourceCohortPlan::Rejected(format!(
                            "source route is not owner-local indexed: key={}",
                            member.key
                        ));
                    }
                    reclaim_backing_matches(replica, &member.backing)
                }
                _ => {
                    absent_sources += 1;
                    continue;
                }
            }
        };
        if !replica_matches {
            return OwnerSourceCohortPlan::Stale(format!(
                "source backing changed: key={} put_id=({},{})",
                member.key, member.put_id.0, member.put_id.1
            ));
        }

        let mut route_cohort = match super::current_tp_cohort_keys(&member.key, &route, &desc) {
            Ok(keys) => keys,
            Err(_) => {
                return OwnerSourceCohortPlan::Rejected(format!(
                    "master route has an invalid TP/atomic cohort: key={}",
                    member.key
                ));
            }
        };
        route_cohort.sort_unstable();
        route_cohort.dedup();
        if route_cohort != expected_keys {
            return OwnerSourceCohortPlan::Rejected(format!(
                "owner did not submit the complete TP/atomic cohort: key={} expected_members={} route_members={}",
                member.key,
                expected_keys.len(),
                route_cohort.len(),
            ));
        }
    }

    if absent_sources == cohort.members.len() {
        OwnerSourceCohortPlan::Completed("all exact source replicas are already absent")
    } else if absent_sources != 0 {
        // This can be observed while an accepted cohort is between individual
        // route removals. The exact inflight registration below distinguishes
        // that case from a malformed new request.
        OwnerSourceCohortPlan::Ready {
            members: planned,
            all_sources_present: false,
        }
    } else {
        OwnerSourceCohortPlan::Ready {
            members: planned,
            all_sources_present: true,
        }
    }
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
    counters.source_evict_cohorts.fetch_add(
        u64::try_from(req.serialize_part.cohorts.len()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let requested_bytes = req
        .serialize_part
        .cohorts
        .iter()
        .flat_map(|cohort| cohort.members.iter())
        .filter_map(|member| owner_source_member_weight(&member.backing))
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
            u64::try_from(req.serialize_part.cohorts.len()).unwrap_or(u64::MAX),
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
                cohorts: Vec::new(),
                error_code: err.code(),
                error_json: err.to_json(),
            },
            raw_bytes: Vec::new(),
        };
    }

    let mut responses = Vec::with_capacity(req.serialize_part.cohorts.len());
    for (index, cohort) in req.serialize_part.cohorts.iter().enumerate() {
        let (outcome, detail) = match plan_exact_owner_source_cohort(view, &owner, cohort) {
            OwnerSourceCohortPlan::Completed(detail) => {
                (OwnerSourceEvictionOutcome::Completed, detail.to_string())
            }
            OwnerSourceCohortPlan::Stale(detail) => (OwnerSourceEvictionOutcome::Stale, detail),
            OwnerSourceCohortPlan::Rejected(detail) => {
                (OwnerSourceEvictionOutcome::RejectedNotEvictable, detail)
            }
            OwnerSourceCohortPlan::Ready {
                members,
                all_sources_present,
            } => {
                match view
                    .master_kv_router()
                    .enqueue_owner_capacity_eviction_cohort(
                        owner.as_ref().to_string(),
                        req.serialize_part.owner_node_start_time,
                        members,
                        all_sources_present,
                    ) {
                    EnqueueEvictionReclaimResult::Accepted => (
                        OwnerSourceEvictionOutcome::Accepted,
                        "exact source cohort accepted by reclaim pipeline".to_string(),
                    ),
                    EnqueueEvictionReclaimResult::AlreadyInProgress => (
                        OwnerSourceEvictionOutcome::AlreadyInProgress,
                        "same exact source cohort is already in progress".to_string(),
                    ),
                    EnqueueEvictionReclaimResult::PartialOverlap => (
                        OwnerSourceEvictionOutcome::RetryableBusy,
                        "source cohort partially overlaps another reclaim transaction".to_string(),
                    ),
                    EnqueueEvictionReclaimResult::NotInProgress => (
                        OwnerSourceEvictionOutcome::Stale,
                        "partially absent source cohort is not an in-progress replay".to_string(),
                    ),
                    EnqueueEvictionReclaimResult::Closed => (
                        OwnerSourceEvictionOutcome::RetryableBusy,
                        "source reclaim actor is unavailable".to_string(),
                    ),
                }
            }
        };
        let outcome_counter = match outcome {
            OwnerSourceEvictionOutcome::Accepted => &counters.source_evict_accepted,
            OwnerSourceEvictionOutcome::AlreadyInProgress => &counters.source_evict_in_progress,
            OwnerSourceEvictionOutcome::Completed => &counters.source_evict_completed,
            OwnerSourceEvictionOutcome::RetryableBusy | OwnerSourceEvictionOutcome::Unspecified => {
                &counters.source_evict_retryable_busy
            }
            OwnerSourceEvictionOutcome::Stale => &counters.source_evict_stale,
            OwnerSourceEvictionOutcome::RejectedNotEvictable => &counters.source_evict_rejected,
        };
        outcome_counter.fetch_add(1, Ordering::Relaxed);
        responses.push(OwnerSourceEvictionCohortResp {
            cohort_index: u32::try_from(index).unwrap_or(u32::MAX),
            outcome,
            detail,
        });
    }

    MsgPack {
        serialize_part: BatchEvictOwnerSourceResp {
            operation_id,
            cohorts: responses,
            error_code: OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

struct RemovedOwnerSource {
    desc: NodeValueReplicaDesc,
    removed_last_route: bool,
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
        let mut replicas = route.nodes_replicas.write();
        let Some(replica) = replicas.get(owner) else {
            return None;
        };
        if !reclaim_backing_matches(replica, &item.backing) {
            return None;
        }
        let desc = NodeValueReplicaDesc {
            weight_bytes: u32::try_from(replica.backing.capacity_bytes()).unwrap_or(u32::MAX),
            put_id: route.put_id,
        };
        replicas.remove(owner).map(|_| desc)
    };
    let removed_desc = removed_desc?;

    let removed_last_route = if route.nodes_replicas.read().is_empty() {
        routes
            .remove_if(&item.key, |_, current| {
                Arc::ptr_eq(current, &route)
                    && current.put_id == item.put_id
                    && current.nodes_replicas.read().is_empty()
            })
            .is_some()
    } else {
        false
    };
    Some(RemovedOwnerSource {
        desc: removed_desc,
        removed_last_route,
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
        debug_assert_eq!(item.backing, OwnerReclaimBacking::UnindexedAllocation);
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

fn partition_reclaim_coordination(
    items: Vec<OwnerReclaimItem>,
) -> (Vec<OwnerReclaimItem>, Vec<OwnerReclaimItem>) {
    items
        .into_iter()
        .partition(|item| item.backing == OwnerReclaimBacking::UnindexedAllocation)
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

#[cfg(test)]
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

    let (master_only, owner_coordinated) = partition_reclaim_coordination(fenced);
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
            let _ = abort_prepared(view, owner, owner_coordinated).await;
            counters
                .completed
                .fetch_add(u64::from(master_reclaimed), Ordering::Relaxed);
            return master_reclaimed;
        }
    };
    let mut prepared = Vec::new();
    let mut committed = Vec::new();
    for (item, response) in owner_coordinated
        .into_iter()
        .zip(prepare_responses.into_iter())
    {
        match response.state {
            OwnerReclaimItemState::Prepared => prepared.push(item),
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

    let mut invalid_prepared = Vec::new();
    prepared.retain(|item| {
        let valid = item_still_valid(view, owner, item);
        if !valid {
            counters.route_changed.fetch_add(1, Ordering::Relaxed);
            invalid_prepared.push(item.clone());
        }
        valid
    });
    committed.extend(abort_prepared(view, owner, invalid_prepared).await);

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
                committed.extend(abort_prepared(view, owner, unresolved).await);
            }
            Err(err) => {
                tracing::warn!(
                    "owner reclaim commit RPC failed; resolving with abort: owner={} keys={} err={}",
                    owner,
                    prepared.len(),
                    err
                );
                committed.extend(abort_prepared(view, owner, prepared).await);
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

fn try_install_cohort_master_fences(
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

/// Reclaim one TP/atomic cohort as a transaction.
///
/// Before the first owner Commit, any failed fence/validation/Prepare rolls
/// the entire cohort back.  Once a response (including an Abort response)
/// proves that any member was committed, rollback is impossible; the same
/// transaction retains every master fence and keeps driving the remaining
/// members until all are committed, then removes/finalizes the whole cohort.
async fn reclaim_cohort_items(
    view: &MasterKvRouterView,
    owner: &NodeID,
    items: Vec<OwnerReclaimItem>,
) -> u32 {
    if items.is_empty() {
        return 0;
    }
    let counters = view
        .master_kv_router()
        .eviction_reclaim_counters(owner.as_ref());
    if let Err((failed_index, activity)) =
        try_install_cohort_master_fences(&view.master_kv_router().inner().key_activity, &items)
    {
        counters
            .master_activity_deferred
            .fetch_add(1, Ordering::Relaxed);
        tracing::trace!(
            "cohort reclaim deferred by master activity: owner={} key={} puts={} gets={} replicas={} reclaim_installed={}",
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

    let (master_only, owner_coordinated) = partition_reclaim_coordination(fenced.clone());
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
            "BUG: one reclaim cohort mixed master-only and owner-coordinated backings: owner={} cohort={} master_only={} owner_coordinated={}",
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
                OwnerReclaimItemState::Prepared => prepared.push(item),
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
                "owner cohort reclaim is rolling forward after partial/uncertain commit: owner={} cohort={} committed={}",
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
        unindexed_allocation.backing = OwnerReclaimBacking::UnindexedAllocation;
        unindexed_allocation.reason = OwnerReclaimReason::MasterAllocationCapacity;
        let committed_slot = candidate(3);

        let (master_only, owner_coordinated) = partition_reclaim_coordination(vec![
            indexed_allocation,
            unindexed_allocation.clone(),
            committed_slot,
        ]);

        assert_eq!(master_only, vec![unindexed_allocation]);
        assert_eq!(owner_coordinated.len(), 2);
        assert!(
            owner_coordinated
                .iter()
                .all(|item| item.backing != OwnerReclaimBacking::UnindexedAllocation)
        );
    }
}

#[cfg(test)]
mod owner_get_holding_reclaim_tests {
    use super::{OwnerReclaimBacking, OwnerReclaimReason, reclaim_items, route_item};
    use crate::client_kv_api::PutOptionalArgs;
    use crate::kvcore_test_lib::{
        integration_test_lock, start_master_and_client, stop_master_and_client,
    };
    use crate::memholder::{MemholderManagerTrait, NodeHolderKey};
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
        let owner_node = owner_id.clone().into();
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
                    .nodes_replicas
                    .read()
                    .get(&owner)
                    .is_some_and(|replica| {
                        !replica.tomb_tag.is_tomb()
                            && replica.owner_local_indexed
                            && reclaim_backing_matches(replica, expected_backing)
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
                while let Some(request) = pending.pop_front() {
                    let (members, mut accounting_requests, reason) = match request.origin {
                        EvictionReclaimOrigin::OwnerCapacityEviction => (
                            request.members.clone(),
                            vec![request],
                            OwnerReclaimReason::OwnerCapacityEviction,
                        ),
                        EvictionReclaimOrigin::MasterAllocationCapacity => {
                            let members = match plan_master_allocation_capacity_cohort(
                                &view_task,
                                &request,
                            ) {
                                Ok(members) => members,
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
                                    let restored = restore_request_entries(&view_task, &request);
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
                                Err(MasterCapacityPlanError::InvalidCohort) => {
                                    retry_requests.push(request);
                                    continue;
                                }
                                Err(MasterCapacityPlanError::WrongRole) => {
                                    counters
                                        .capacity_eviction_non_ring_b_entry_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    view_task
                                        .master_kv_router()
                                        .complete_eviction_reclaim(&request);
                                    let restored = restore_request_entries(&view_task, &request);
                                    tracing::error!(
                                        "BUG: master Allocation Size event resolved to a non-ring-B route; restored metadata: owner={} members={} restored={}",
                                        owner_node_id,
                                        request.members.len(),
                                        restored,
                                    );
                                    continue;
                                }
                            };
                            let owner_cache_lock = view_task
                                .master_kv_router()
                                .inner()
                                .owner_cache_operation_locks
                                .get_lock(owner_node_id.clone());
                            let owner_cache_guard = owner_cache_lock.lock().await;
                            let detach_result = detach_present_capacity_cohort_siblings(
                                &view_task,
                                &owner_node_id,
                                &members,
                            );
                            drop(owner_cache_guard);
                            if detach_result.is_err() {
                                retry_requests.push(request);
                                continue;
                            }

                            let planned = members
                                .iter()
                                .map(|member| {
                                    (
                                        member.key.clone(),
                                        member.desc.put_id,
                                        member.desc.weight_bytes,
                                    )
                                })
                                .collect::<HashSet<_>>();
                            let mut accounting_requests = vec![request];
                            let mut retained = std::collections::VecDeque::new();
                            while let Some(other) = pending.pop_front() {
                                let belongs = other.origin
                                    == EvictionReclaimOrigin::MasterAllocationCapacity
                                    && other.members.iter().all(|member| {
                                        planned.contains(&(
                                            member.key.clone(),
                                            member.desc.put_id,
                                            member.desc.weight_bytes,
                                        ))
                                    });
                                if belongs {
                                    accounting_requests.push(other);
                                } else {
                                    retained.push_back(other);
                                }
                            }
                            pending = retained;

                            let mut accounted = accounting_requests
                                .iter()
                                .flat_map(EvictionReclaimRequest::identities)
                                .collect::<HashSet<_>>();
                            for member in &members {
                                let synthetic = EvictionReclaimRequest {
                                    owner_node_id: owner_node_id.clone(),
                                    owner_node_start_time: None,
                                    members: vec![member.clone()],
                                    origin: EvictionReclaimOrigin::MasterAllocationCapacity,
                                    retry_count: 0,
                                };
                                let identity = synthetic.identities().pop().unwrap();
                                if accounted.contains(&identity) {
                                    continue;
                                }
                                if view_task
                                    .master_kv_router()
                                    .register_eviction_reclaim(&synthetic)
                                {
                                    accounted.insert(identity);
                                    accounting_requests.push(synthetic);
                                }
                            }
                            (
                                members,
                                accounting_requests,
                                OwnerReclaimReason::MasterAllocationCapacity,
                            )
                        }
                    };

                    let items = members
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
                    if let Some(items) = items {
                        let _ = reclaim_cohort_items(&view_task, &owner, items).await;
                    }
                    for accounting_request in accounting_requests.drain(..) {
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
                    "safe cohort eviction reclaim batch completed: owner={} retry_deferred={}",
                    owner_node_id,
                    retry_count,
                );
            }
        }
    });
}
