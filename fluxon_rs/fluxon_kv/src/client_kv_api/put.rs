use super::{
    ClientKvApiInner, OwnerHotEvictionEvent, OwnerHotEvictionPreparation,
    OwnerHotSelectionFenceOutcome, OwnerLocalReserveClassState, OwnerLocalReserveGrantState,
    OwnerLocalReservePoolState, OwnerLocalReserveSlotLease, OwnerLocalReserveSlotRef,
    OwnerRemotePutOutcome, OwnerRemotePutReservation, OwnerRemotePutSharedOp,
    local_reserve_rebalance::{owner_local_reserve_timeout_config, wait_owner_local_reserve_ready},
};
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
use crate::cluster_manager::NodeIDString;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_kv_router::put::PutIDForAKey;
// no StageScope; timestamps-based metrics only
use crate::memholder::kvclient_encode::{calc_flat_dict_encoded_len, write_flat_dict_ptrs_to_ptr};
use crate::observe_kvope::{
    obe_put_start_error_rpc, obe_put_start_error_status, obe_put_start_success,
};
use crate::{
    client_kv_api::ClientKvApiView,
    master_kv_router::msg_pack::{
        BatchEnqueueReplicaTaskReq, BatchEnqueueReplicaTaskResp, BatchEvictOwnerSourceReq,
        BatchEvictOwnerSourceResp, BatchPreparePutKeyItemReq, BatchPreparePutKeysReq,
        BatchPreparePutKeysResp, BatchPutAppendDoneItemReq, BatchPutAppendDoneReq,
        BatchPutAppendDoneResp, BatchPutAppendStartItemReq, BatchPutAppendStartReq,
        BatchPutAppendStartResp, BatchPutDoneItemReq, BatchPutDoneItemResp, BatchPutDoneReq,
        BatchPutDoneResp, BatchPutRevokeItemReq, BatchPutRevokeReq, BatchPutRevokeResp,
        BatchPutStartItemReq, BatchPutStartReq, BatchPutStartResp,
        BatchReleasePutKeyReservationsReq, BatchReleasePutKeyReservationsResp,
        EnqueueReplicaTaskItemResp, GroupedBatchPutDoneItemReq, GroupedBatchPutDoneReq,
        GroupedBatchPutDoneResp, OwnerReclaimBacking, OwnerSourceEvictionOutcome,
        OwnerSourceEvictionVictim, PutAppendDoneReq, PutAppendDoneResp, PutAppendRevokeReq,
        PutAppendStartOutcome, PutAppendStartReq, PutAppendStartResp, PutAtomicGroup,
        PutDoneCommittedSlot, PutDoneReq, PutRevokeReq, PutStartReq, PutStartResp,
        ReleaseLocalGrantReq, ReserveLocalGrantOutcome, ReserveLocalGrantReq,
        owner_source_eviction_epoch,
    },
    memholder::{UserMemHolder, UserMemHolderExposeKind},
    p2p::msg_pack::MsgPack,
    p2p::p2p_module::RpcTransportPolicy,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use chrono::Utc;
use fluxon_commu::TransferBreakdown;
use limit_thirdparty::tokio;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};
use tracing::info;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

fn owner_local_reserve_slot_size(value_len: u64) -> KvResult<u64> {
    crate::owner_local_reserve_slot_size_bytes(value_len).ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "value_len={} cannot be represented by a resident local-reserve slot no larger than {} bytes",
                value_len, OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES
            ),
        })
    })
}

fn owner_local_reserve_slots_per_grant(slot_size: u64) -> u32 {
    crate::owner_local_reserve_slots_per_grant(slot_size)
        .expect("validated local-reserve slot size must fit in a grant")
}

fn owner_local_reserve_install_grant(
    pool: &mut OwnerLocalReservePoolState,
    slot_size: u64,
    slots_per_grant: u32,
    grant: OwnerLocalReserveGrantState,
) {
    let class_state = pool
        .classes
        .entry(slot_size)
        .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
    assert!(
        class_state.slot_size == slot_size,
        "slot_size drift detected while installing local reserve grant"
    );
    assert!(
        class_state.slots_per_grant == slots_per_grant,
        "slots_per_grant drift detected while installing local reserve grant"
    );
    class_state.install_grant(grant);
}

fn owner_local_reserve_try_claim(
    pool: &mut OwnerLocalReservePoolState,
    slot_size: u64,
    slots_per_grant: u32,
    value_len: u64,
    key_count: usize,
) -> Option<OwnerLocalReserveSlotLease> {
    let free_slots = pool
        .classes
        .entry(slot_size)
        .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant))
        .free_slot_count();
    if free_slots < key_count {
        return None;
    }
    let slots = owner_local_reserve_claim_available(pool, slot_size, slots_per_grant, key_count);
    assert_eq!(
        slots.len(),
        key_count,
        "free_slot_count check and claim path diverged"
    );
    Some(OwnerLocalReserveSlotLease {
        value_len,
        slot_size,
        slots,
    })
}

fn owner_local_reserve_claim_available(
    pool: &mut OwnerLocalReservePoolState,
    slot_size: u64,
    slots_per_grant: u32,
    max_slots: usize,
) -> Vec<OwnerLocalReserveSlotRef> {
    let class_state = pool
        .classes
        .entry(slot_size)
        .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
    let claim_count = class_state.free_slot_count().min(max_slots);
    let slots = class_state.claim_available(max_slots);
    assert_eq!(
        slots.len(),
        claim_count,
        "free_slot_count check and claim path diverged"
    );
    slots
}

struct OwnerLocalReservePendingDemandGuard<'a> {
    inner: &'a ClientKvApiInner,
    slot_size: u64,
    slots_per_grant: u32,
    pending_slots: usize,
}

impl<'a> OwnerLocalReservePendingDemandGuard<'a> {
    fn new(
        inner: &'a ClientKvApiInner,
        slot_size: u64,
        slots_per_grant: u32,
        pending_slots: usize,
    ) -> Self {
        inner.owner_local_reserve_register_pending_demand(
            slot_size,
            slots_per_grant,
            pending_slots,
        );
        Self {
            inner,
            slot_size,
            slots_per_grant,
            pending_slots,
        }
    }

    fn consume(&mut self) {
        if self.pending_slots == 0 {
            return;
        }
        self.inner.owner_local_reserve_consume_pending_demand(
            self.slot_size,
            self.slots_per_grant,
            self.pending_slots,
        );
        self.pending_slots = 0;
        self.inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
    }

    fn disarm_after_locked_consume(&mut self) {
        assert!(
            self.pending_slots > 0,
            "pending-demand guard was already consumed"
        );
        self.pending_slots = 0;
        self.inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
    }
}

impl Drop for OwnerLocalReservePendingDemandGuard<'_> {
    fn drop(&mut self) {
        self.consume();
    }
}

#[cfg(test)]
mod local_reserve_claim_tests {
    use super::{
        OwnerLocalReserveGrantState, OwnerLocalReservePoolState,
        owner_local_reserve_claim_available, owner_local_reserve_install_grant,
        owner_local_reserve_slot_size, owner_local_reserve_slots_per_grant,
    };
    use crate::client_kv_api::{ClientKvApi, ClientKvApiNewArg};
    use crate::config::TestSpecConfig;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn slot_size_uses_page_aligned_exact_fit() {
        const SGLANG_KV_PAGE_BYTES: u64 = 4_718_592;

        let slot_size = owner_local_reserve_slot_size(SGLANG_KV_PAGE_BYTES).unwrap();
        assert_eq!(slot_size, SGLANG_KV_PAGE_BYTES);
        assert_eq!(owner_local_reserve_slots_per_grant(slot_size), 113);
        assert_eq!(owner_local_reserve_slot_size(4_097).unwrap(), 8_192);
    }

    #[test]
    fn partial_claim_keeps_progress_for_large_waiters() {
        let mut pool = OwnerLocalReservePoolState::default();
        owner_local_reserve_install_grant(
            &mut pool,
            8,
            4,
            OwnerLocalReserveGrantState::new(1, 1000, 1000, 32, 8, 4),
        );

        let first = owner_local_reserve_claim_available(&mut pool, 8, 4, 3);
        assert_eq!(first.len(), 3);
        assert_eq!(pool.classes.get(&8).unwrap().free_slot_count(), 1);

        let second = owner_local_reserve_claim_available(&mut pool, 8, 4, 3);
        assert_eq!(second.len(), 1);
        assert_eq!(pool.classes.get(&8).unwrap().free_slot_count(), 0);
    }

    #[test]
    fn grant_index_and_cached_counters_survive_swap_remove_and_reinstall() {
        const SLOT_SIZE: u64 = 8;
        const SLOTS_PER_GRANT: u32 = 4;
        let mut pool = OwnerLocalReservePoolState::default();
        for grant_id in 1..=3 {
            owner_local_reserve_install_grant(
                &mut pool,
                SLOT_SIZE,
                SLOTS_PER_GRANT,
                OwnerLocalReserveGrantState::new(
                    grant_id,
                    grant_id * 1000,
                    grant_id * 1000,
                    SLOT_SIZE * u64::from(SLOTS_PER_GRANT),
                    SLOT_SIZE,
                    SLOTS_PER_GRANT,
                ),
            );
        }

        let class = pool.classes.get_mut(&SLOT_SIZE).unwrap();
        assert_eq!(class.free_slot_count(), 12);
        assert_eq!(class.used_slot_count(), 0);
        let detached = class
            .detach_fully_free_grant(2)
            .expect("middle grant must be indexed");
        assert_eq!(class.free_slot_count(), 8);
        assert_eq!(class.grant_count(), 2);

        // Removing the middle Vec entry swap-moves grant 3. A subsequent state
        // transition by grant id proves that its repaired index is authoritative.
        let claimed = class.claim_available(1);
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].grant_id, 3);
        assert_eq!(class.free_slot_count(), 7);
        assert_eq!(class.prepared_slot_count(), 1);
        assert!(class.release_prepared_slot(3, claimed[0].slot_index));
        assert_eq!(class.free_slot_count(), 8);
        assert_eq!(class.prepared_slot_count(), 0);

        class.install_grant(detached);
        assert_eq!(class.free_slot_count(), 12);
        assert_eq!(class.used_slot_count(), 0);
        assert_eq!(class.grant_count(), 3);
    }

    #[test]
    fn committed_slots_are_reclaimed_and_reused_independently_across_grants() {
        const SLOT_SIZE: u64 = 8;
        const SLOTS_PER_GRANT: u32 = 4;

        let mut pool = OwnerLocalReservePoolState::default();
        for (grant_id, addr) in [(1, 1000), (2, 2000)] {
            owner_local_reserve_install_grant(
                &mut pool,
                SLOT_SIZE,
                SLOTS_PER_GRANT,
                OwnerLocalReserveGrantState::new(
                    grant_id,
                    addr,
                    addr,
                    SLOT_SIZE * u64::from(SLOTS_PER_GRANT),
                    SLOT_SIZE,
                    SLOTS_PER_GRANT,
                ),
            );
        }

        let initial = owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOTS_PER_GRANT, 8);
        assert_eq!(initial.len(), 8);
        {
            let class = pool.classes.get_mut(&SLOT_SIZE).unwrap();
            for slot in &initial {
                assert!(class.mark_prepared_slot_pending_visible(slot.grant_id, slot.slot_index));
                assert!(class.retain_resident_slot_holder(slot.grant_id, slot.slot_index));
                assert!(
                    class.promote_pending_visible_slot_to_committed(slot.grant_id, slot.slot_index)
                );
            }
        }

        let victims = [initial[1].clone(), initial[5].clone()];
        {
            let class = pool.classes.get_mut(&SLOT_SIZE).unwrap();
            for victim in &victims {
                assert!(class.release_committed_slot_route(victim.grant_id, victim.slot_index));
                assert!(class.release_resident_slot_holder(victim.grant_id, victim.slot_index));
            }
            assert_eq!(class.free_slot_count(), 2);
            assert_eq!(class.grant_count(), 2);
            assert!(class.grants.iter().all(|grant| !grant.is_fully_free()));
            assert!(class.grants.iter().all(|grant| grant.free_slots.len() == 1));
        }

        for cycle in 0..10 {
            let reused =
                owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOTS_PER_GRANT, 2);
            assert_eq!(reused.len(), 2, "cycle {cycle} did not reuse both slots");
            let mut reused_grant_ids = reused.iter().map(|slot| slot.grant_id).collect::<Vec<_>>();
            reused_grant_ids.sort_unstable();
            assert_eq!(reused_grant_ids, vec![1, 2]);

            let class = pool.classes.get_mut(&SLOT_SIZE).unwrap();
            for slot in reused {
                assert!(class.mark_prepared_slot_pending_visible(slot.grant_id, slot.slot_index));
                assert!(class.retain_resident_slot_holder(slot.grant_id, slot.slot_index));
                assert!(
                    class.promote_pending_visible_slot_to_committed(slot.grant_id, slot.slot_index)
                );
                assert!(class.release_committed_slot_route(slot.grant_id, slot.slot_index));
                assert!(class.release_resident_slot_holder(slot.grant_id, slot.slot_index));
            }
            assert_eq!(class.free_slot_count(), 2);
            assert_eq!(class.grant_count(), 2);
            assert!(class.grants.iter().all(|grant| !grant.is_fully_free()));
        }
    }

    #[limit_thirdparty::tokio::test]
    async fn later_waiter_cannot_steal_slot_before_current_claim_turn_completes() {
        const SLOT_SIZE: u64 = 4 * 1024;
        const SLOTS_PER_GRANT: u32 = 4;

        let api = Arc::new(
            ClientKvApi::construct(ClientKvApiNewArg {
                test_spec_config: TestSpecConfig::default(),
                owner_hot_cache_capacity_bytes: None,
            })
            .await
            .expect("construct test ClientKvApi"),
        );
        {
            let mut pool = api.inner().owner_local_reserve_pool.lock();
            owner_local_reserve_install_grant(
                &mut pool,
                SLOT_SIZE,
                SLOTS_PER_GRANT,
                OwnerLocalReserveGrantState::new(
                    1,
                    1000,
                    1000,
                    SLOT_SIZE * u64::from(SLOTS_PER_GRANT),
                    SLOT_SIZE,
                    SLOTS_PER_GRANT,
                ),
            );
        }

        // Model a five-slot waiter that has made partial progress while owning the claim turn.
        let claim_lock = api.inner().owner_local_reserve_claim_lock(SLOT_SIZE);
        let current_claim_turn = claim_lock.lock().await;
        let first_partial = {
            let mut pool = api.inner().owner_local_reserve_pool.lock();
            owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOTS_PER_GRANT, 3)
        };
        assert_eq!(first_partial.len(), 3);

        // A later one-slot request must queue even though one slot is currently free.
        let later_api = Arc::clone(&api);
        let mut later_waiter = tokio::spawn(async move {
            later_api
                .inner()
                .owner_claim_local_reserve_slot_lease(SLOT_SIZE, 1)
                .await
        });
        assert!(
            limit_thirdparty::tokio::time::timeout(Duration::from_millis(25), &mut later_waiter,)
                .await
                .is_err(),
            "later waiter bypassed the active claim turn"
        );
        assert_eq!(
            api.inner()
                .owner_local_reserve_pool
                .lock()
                .classes
                .get(&SLOT_SIZE)
                .unwrap()
                .free_slot_count(),
            1,
            "later waiter stole the current waiter's remaining free slot"
        );

        // Refill lets the current waiter reach all five slots before handing off the turn.
        let first_remainder = {
            let mut pool = api.inner().owner_local_reserve_pool.lock();
            owner_local_reserve_install_grant(
                &mut pool,
                SLOT_SIZE,
                SLOTS_PER_GRANT,
                OwnerLocalReserveGrantState::new(
                    2,
                    2000,
                    2000,
                    SLOT_SIZE * u64::from(SLOTS_PER_GRANT),
                    SLOT_SIZE,
                    SLOTS_PER_GRANT,
                ),
            );
            owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOTS_PER_GRANT, 2)
        };
        assert_eq!(first_partial.len() + first_remainder.len(), 5);
        drop(current_claim_turn);

        let later_lease =
            limit_thirdparty::tokio::time::timeout(Duration::from_secs(1), later_waiter)
                .await
                .expect("later waiter did not receive the next claim turn")
                .expect("later waiter task panicked")
                .expect("later waiter failed to claim a free slot");
        assert_eq!(later_lease.slots.len(), 1);
    }

    #[limit_thirdparty::tokio::test]
    async fn same_class_claim_waiters_complete_in_fifo_order() {
        const SLOT_SIZE: u64 = 4 * 1024;

        let api = Arc::new(
            ClientKvApi::construct(ClientKvApiNewArg {
                test_spec_config: TestSpecConfig::default(),
                owner_hot_cache_capacity_bytes: None,
            })
            .await
            .expect("construct test ClientKvApi"),
        );
        let claim_lock = api.inner().owner_local_reserve_claim_lock(SLOT_SIZE);
        let claim_turn = claim_lock.lock().await;
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));

        let first_queued = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_lock = api.inner().owner_local_reserve_claim_lock(SLOT_SIZE);
        let first_order = Arc::clone(&order);
        let first_queued_task = Arc::clone(&first_queued);
        let first = tokio::spawn(async move {
            first_queued_task.store(true, std::sync::atomic::Ordering::Release);
            let _turn = first_lock.lock_owned().await;
            first_order.lock().unwrap().push(1u8);
        });
        while !first_queued.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let second_queued = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_lock = api.inner().owner_local_reserve_claim_lock(SLOT_SIZE);
        let second_order = Arc::clone(&order);
        let second_queued_task = Arc::clone(&second_queued);
        let second = tokio::spawn(async move {
            second_queued_task.store(true, std::sync::atomic::Ordering::Release);
            let _turn = second_lock.lock_owned().await;
            second_order.lock().unwrap().push(2u8);
        });
        while !second_queued.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        drop(claim_turn);

        limit_thirdparty::tokio::time::timeout(Duration::from_secs(1), async {
            first.await.expect("first same-class waiter panicked");
            second.await.expect("second same-class waiter panicked");
        })
        .await
        .expect("same-class FIFO waiters did not complete");
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
    }

    #[limit_thirdparty::tokio::test]
    async fn pressured_class_does_not_block_an_unrelated_slot_class() {
        const BLOCKED_SLOT_SIZE: u64 = 4 * 1024;
        const READY_SLOT_SIZE: u64 = 8 * 1024;
        let ready_slots_per_grant = owner_local_reserve_slots_per_grant(READY_SLOT_SIZE);

        let api = Arc::new(
            ClientKvApi::construct(ClientKvApiNewArg {
                test_spec_config: TestSpecConfig::default(),
                owner_hot_cache_capacity_bytes: None,
            })
            .await
            .expect("construct test ClientKvApi"),
        );
        let blocked_lock = api
            .inner()
            .owner_local_reserve_claim_lock(BLOCKED_SLOT_SIZE);
        let _blocked_turn = blocked_lock.lock().await;
        {
            let mut pool = api.inner().owner_local_reserve_pool.lock();
            owner_local_reserve_install_grant(
                &mut pool,
                READY_SLOT_SIZE,
                ready_slots_per_grant,
                OwnerLocalReserveGrantState::new(
                    1,
                    1000,
                    1000,
                    READY_SLOT_SIZE * u64::from(ready_slots_per_grant),
                    READY_SLOT_SIZE,
                    ready_slots_per_grant,
                ),
            );
        }

        let ready_api = Arc::clone(&api);
        let ready_lease = limit_thirdparty::tokio::time::timeout(
            Duration::from_secs(1),
            ready_api
                .inner()
                .owner_claim_local_reserve_slot_lease(READY_SLOT_SIZE, 1),
        )
        .await
        .expect("an unrelated slot class was head-of-line blocked")
        .expect("ready slot class claim failed");
        assert_eq!(ready_lease.slot_size, READY_SLOT_SIZE);
        assert_eq!(ready_lease.slots.len(), 1);
    }

    #[limit_thirdparty::tokio::test]
    async fn queued_claims_publish_aggregate_demand_and_cancel_cleanly() {
        const SLOT_SIZE: u64 = 4 * 1024;

        let api = Arc::new(
            ClientKvApi::construct(ClientKvApiNewArg {
                test_spec_config: TestSpecConfig::default(),
                owner_hot_cache_capacity_bytes: None,
            })
            .await
            .expect("construct test ClientKvApi"),
        );
        let claim_lock = api.inner().owner_local_reserve_claim_lock(SLOT_SIZE);
        let claim_turn = claim_lock.lock().await;

        let first_api = Arc::clone(&api);
        let first = tokio::spawn(async move {
            first_api
                .inner()
                .owner_claim_local_reserve_slot_lease(SLOT_SIZE, 3)
                .await
        });
        let second_api = Arc::clone(&api);
        let second = tokio::spawn(async move {
            second_api
                .inner()
                .owner_claim_local_reserve_slot_lease(SLOT_SIZE, 2)
                .await
        });

        limit_thirdparty::tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let pending = api
                    .inner()
                    .owner_local_reserve_pool
                    .lock()
                    .classes
                    .get(&SLOT_SIZE)
                    .map(|class| class.pending_slot_demand)
                    .unwrap_or_default();
                if pending == 5 {
                    break;
                }
                limit_thirdparty::tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued claims did not publish aggregate demand");

        first.abort();
        second.abort();
        let _ = first.await;
        let _ = second.await;
        assert_eq!(
            api.inner()
                .owner_local_reserve_pool
                .lock()
                .classes
                .get(&SLOT_SIZE)
                .unwrap()
                .pending_slot_demand,
            0,
            "cancelled queued claims leaked pending demand"
        );
        drop(claim_turn);
    }
}

#[derive(Debug, Clone)]
pub struct OwnerReservedPutItem {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub src_addr: u64,
    pub src_base_addr: u64,
    pub target_addr: u64,
    pub target_base_addr: u64,
    pub value_len: u64,
    pub lease_id: Option<u64>,
    pub peer_node_id: Option<NodeIDString>,
    pub remember_local_snapshot: bool,
    pub make_replica_task: bool,
    pub preferred_sub_cluster: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OwnerLocalPublishItem {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub value_len: u64,
    pub lease_id: Option<u64>,
    pub committed_slot: PutDoneCommittedSlot,
    pub make_replica_task: bool,
    pub preferred_sub_cluster: Option<String>,
    pub atomic_group: Option<PutAtomicGroup>,
}

#[derive(Debug, Clone)]
pub struct OwnerLocalPublishJob {
    pub items: Vec<OwnerLocalPublishItem>,
    pub key_reservation_ids: Vec<u64>,
    /// External local-first requests keep their owner reclaim fences here until
    /// the grouped master terminal response and all local promotions complete.
    /// Native/Pyo3 jobs use master key reservations instead and leave this empty.
    pub external_pending_contexts: Vec<super::ExternalPendingPutCtx>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PutEndStats {
    pub master_put_end_rpc_us: i64,
    pub master_put_end_server_us: i64,
}

pub struct PutEndWithLocalCachePublish {
    pub stats: PutEndStats,
    pub local_cache_holder_id: Option<u64>,
}

fn owner_local_reserve_timeout_error(
    inner: &ClientKvApiInner,
    stage: &'static str,
    slot_size: u64,
    key_count: usize,
    soft_wait_timeout: std::time::Duration,
    hard_wait_timeout: std::time::Duration,
    request_started_at: Instant,
) -> KvError {
    let (used_slots, free_slots, pending_slots, grants, expected_grants) = {
        let pool = inner.owner_local_reserve_pool.lock();
        pool.classes
            .get(&slot_size)
            .map(|class_state| {
                (
                    class_state.used_slot_count(),
                    class_state.free_slot_count(),
                    class_state.pending_slot_demand,
                    class_state.grant_count(),
                    class_state.expected_grant_count,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0))
    };
    KvError::Api(ApiError::Unknown {
        detail: format!(
            "owner local reserve refill timeout: stage={} slot_size={} key_count={} remaining_slots={} used_slots={} free_slots={} pending_slots={} grants={} expected_grants={} waited_ms={} soft_wait_timeout_ms={} hard_timeout_ms={}",
            stage,
            slot_size,
            key_count,
            key_count.saturating_sub(free_slots),
            used_slots,
            free_slots,
            pending_slots,
            grants,
            expected_grants,
            request_started_at.elapsed().as_millis(),
            soft_wait_timeout.as_millis(),
            hard_wait_timeout.as_millis()
        ),
    })
}

impl ClientKvApiInner {
    pub async fn owner_claim_local_reserve_slot_lease(
        &self,
        value_len: u64,
        key_count: usize,
    ) -> KvResult<OwnerLocalReserveSlotLease> {
        let slot_size = owner_local_reserve_slot_size(value_len)?;
        let slots_per_grant = owner_local_reserve_slots_per_grant(slot_size);
        let (soft_wait_timeout, hard_wait_timeout) = owner_local_reserve_timeout_config(self);
        let request_started_at = Instant::now();
        let hard_deadline = request_started_at
            .checked_add(hard_wait_timeout)
            .ok_or_else(|| {
                KvError::Api(ApiError::Unknown {
                    detail: "owner local reserve hard timeout overflow".to_string(),
                })
            })?;
        // Publish demand before waiting for the FIFO turn so one refill can cover all queued
        // claimants. The guard also removes demand if the caller is cancelled while queued.
        let mut pending_demand =
            OwnerLocalReservePendingDemandGuard::new(self, slot_size, slots_per_grant, key_count);
        self.owner_local_reserve_rebalance_notify().notify_waiters();

        let Some(remaining_for_turn) = hard_deadline.checked_duration_since(Instant::now()) else {
            return Err(owner_local_reserve_timeout_error(
                self,
                "claim_turn",
                slot_size,
                key_count,
                soft_wait_timeout,
                hard_wait_timeout,
                request_started_at,
            ));
        };
        let claim_lock = self.owner_local_reserve_claim_lock(slot_size);
        let _claim_turn = match tokio::time::timeout(remaining_for_turn, claim_lock.lock()).await {
            Ok(claim_turn) => claim_turn,
            Err(_) => {
                return Err(owner_local_reserve_timeout_error(
                    self,
                    "claim_turn",
                    slot_size,
                    key_count,
                    soft_wait_timeout,
                    hard_wait_timeout,
                    request_started_at,
                ));
            }
        };

        loop {
            let claim = {
                let mut pool = self.owner_local_reserve_pool.lock();
                let claim = owner_local_reserve_try_claim(
                    &mut pool,
                    slot_size,
                    slots_per_grant,
                    value_len,
                    key_count,
                );
                if claim.is_some() {
                    let class_state = pool
                        .classes
                        .get_mut(&slot_size)
                        .expect("claimed local-reserve class must exist");
                    class_state.pending_slot_demand = class_state
                        .pending_slot_demand
                        .checked_sub(key_count)
                        .expect("claimed local-reserve demand underflow");
                }
                claim
            };
            if let Some(lease) = claim {
                pending_demand.disarm_after_locked_consume();
                return Ok(lease);
            }
            self.owner_local_reserve_rebalance_notify().notify_waiters();
            if !wait_owner_local_reserve_ready(
                self,
                slot_size,
                slots_per_grant,
                key_count,
                soft_wait_timeout,
                hard_deadline,
            )
            .await
            {
                break;
            }
        }
        Err(owner_local_reserve_timeout_error(
            self,
            "refill",
            slot_size,
            key_count,
            soft_wait_timeout,
            hard_wait_timeout,
            request_started_at,
        ))
    }

    pub async fn owner_release_local_reserve_slot_lease(
        &self,
        lease: OwnerLocalReserveSlotLease,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_local_reserve_pool.lock();
            let Some(class_state) = pool.classes.get_mut(&lease.slot_size) else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "resident local reserve class missing while releasing slot lease: slot_size={}",
                        lease.slot_size
                    ),
                }));
            };
            for slot_ref in &lease.slots {
                if !class_state.release_prepared_slot(slot_ref.grant_id, slot_ref.slot_index) {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "resident local reserve grant missing while releasing slot lease: grant_id={}",
                            slot_ref.grant_id
                        ),
                    }));
                }
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub fn owner_mark_local_reserve_slot_pending_visible(
        &self,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<()> {
        let mut pool = self.owner_local_reserve_pool.lock();
        let Some(class_state) = pool.classes.get_mut(&slot_size) else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve class missing while marking pending slot: slot_size={}",
                    slot_size
                ),
            }));
        };
        if !class_state.mark_prepared_slot_pending_visible(grant_id, slot_index) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve grant missing while marking pending slot: grant_id={}",
                    grant_id
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_promote_local_reserve_pending_slot_to_committed(
        &self,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<()> {
        let mut pool = self.owner_local_reserve_pool.lock();
        let Some(class_state) = pool.classes.get_mut(&slot_size) else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve class missing while promoting pending slot: slot_size={}",
                    slot_size
                ),
            }));
        };
        if !class_state.promote_pending_visible_slot_to_committed(grant_id, slot_index) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve grant missing while promoting pending slot: grant_id={}",
                    grant_id
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_retain_local_reserve_resident_slot_holder(
        &self,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<()> {
        let mut pool = self.owner_local_reserve_pool.lock();
        let Some(class_state) = pool.classes.get_mut(&slot_size) else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve class missing while retaining resident slot holder: slot_size={}",
                    slot_size
                ),
            }));
        };
        if !class_state.retain_resident_slot_holder(grant_id, slot_index) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve grant missing while retaining resident slot holder: grant_id={}",
                    grant_id
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_release_local_reserve_resident_slot_holder(
        &self,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_local_reserve_pool.lock();
            let Some(class_state) = pool.classes.get_mut(&slot_size) else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve class missing while releasing resident slot holder: slot_size={}",
                        slot_size
                    ),
                }));
            };
            if !class_state.release_resident_slot_holder(grant_id, slot_index) {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve grant missing while releasing resident slot holder: grant_id={}",
                        grant_id
                    ),
                }));
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub fn owner_release_local_reserve_committed_slot_route(
        &self,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_local_reserve_pool.lock();
            let Some(class_state) = pool.classes.get_mut(&slot_size) else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve class missing while releasing committed slot route: slot_size={}",
                        slot_size
                    ),
                }));
            };
            if !class_state.release_committed_slot_route(grant_id, slot_index) {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve grant missing while releasing committed slot route: grant_id={}",
                        grant_id
                    ),
                }));
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub fn owner_release_local_reserve_committed_resident_slot(
        &self,
        slot_size: u64,
        grant_id: u64,
        slot_index: u32,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_local_reserve_pool.lock();
            let Some(class_state) = pool.classes.get_mut(&slot_size) else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve class missing while reclaiming committed resident slot: slot_size={}",
                        slot_size
                    ),
                }));
            };
            if !class_state.release_committed_resident_slot(grant_id, slot_index) {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve grant missing while reclaiming committed resident slot: grant_id={}",
                        grant_id
                    ),
                }));
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub async fn owner_shutdown_local_reserve_pool(&self) -> KvResult<()> {
        let grants = {
            let mut pool = self.owner_local_reserve_pool.lock();
            let mut detached = Vec::new();
            for (_slot_size, mut class_state) in pool.classes.drain() {
                detached.extend(class_state.take_all_grants());
            }
            detached
        };
        let mut first_err = None;
        for grant in grants {
            if let Err(err) = self.release_local_grant(grant.grant_id).await {
                if first_err.is_none() {
                    first_err = Some(err);
                } else {
                    tracing::warn!(
                        "owner_shutdown_local_reserve_pool dropped additional release error after the first one: {}",
                        err
                    );
                }
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    pub async fn owner_batch_put_start_reserved(
        &self,
        start_items: Vec<BatchPutStartItemReq>,
        lease_id: Option<u64>,
    ) -> KvResult<Vec<OwnerReservedPutItem>> {
        if start_items.is_empty() {
            return Ok(Vec::new());
        }
        if self.short_circuit_put_payload_path_enabled() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail:
                    "owner_batch_put_start_reserved does not support short_circuit_put_payload_path"
                        .to_string(),
            }));
        }
        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();
        let start_resp = self.batch_put_start(start_items.clone()).await?;
        if start_resp.items.len() != start_items.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_put_start response length mismatch: expected={} got={}",
                    start_items.len(),
                    start_resp.items.len()
                ),
            }));
        }

        let mut prepared_items = Vec::with_capacity(start_items.len());
        let mut revoke_items = Vec::with_capacity(start_items.len());
        let mut first_error: Option<KvError> = None;

        for (start_req, start_item) in start_items.into_iter().zip(start_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                first_error = Some(err);
                break;
            }
            let peer_node_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let replica_admitted = start_item.replica_target.is_some();
            revoke_items.push(BatchPutRevokeItemReq {
                key: start_req.key.clone(),
                put_id: start_item.put_id,
            });
            prepared_items.push(OwnerReservedPutItem {
                key: start_req.key,
                put_id: start_item.put_id,
                src_addr: start_item.src_addr,
                src_base_addr: start_item.src_base_addr,
                target_addr: start_item.target_addr,
                target_base_addr: start_item.target_base_addr,
                value_len: start_req.len,
                lease_id,
                peer_node_id: peer_node_id.clone(),
                remember_local_snapshot: true,
                make_replica_task: start_req.make_replica_task && replica_admitted,
                preferred_sub_cluster: start_req.preferred_sub_cluster,
            });
        }

        if let Some(err) = first_error {
            if !revoke_items.is_empty() {
                if let Err(revoke_err) = self.batch_put_revoke(revoke_items).await {
                    tracing::warn!(
                        "owner_batch_put_start_reserved batch_put_revoke failed after partial reserve: {}",
                        revoke_err
                    );
                }
            }
            return Err(err);
        }

        Ok(prepared_items)
    }

    pub async fn owner_batch_put_commit_reserved(
        &self,
        items: Vec<OwnerReservedPutItem>,
        _transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<()>>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            item: OwnerReservedPutItem,
        }

        let metrics = self.metrics_handle();
        let mut results: Vec<Option<KvResult<()>>> = (0..items.len()).map(|_| None).collect();
        let mut done_pending = Vec::with_capacity(items.len());

        for (idx, item) in items.into_iter().enumerate() {
            metrics.record_put_io_locality(false, item.value_len, 0);
            done_pending.push(DonePending { idx, item });
        }

        if self.skip_put_end_commit_enabled() {
            for pending in done_pending {
                results[pending.idx] = Some(Ok(()));
            }
            return Ok(results
                .into_iter()
                .map(|item| {
                    item.unwrap_or_else(|| {
                        Err(KvError::Api(ApiError::Unknown {
                            detail: "owner_batch_put_commit_reserved result slot was not populated"
                                .to_string(),
                        }))
                    })
                })
                .collect());
        }

        let done_req_items = done_pending
            .iter()
            .map(|pending| BatchPutDoneItemReq {
                key: pending.item.key.clone(),
                put_id: pending.item.put_id,
                lease_id: pending.item.lease_id,
                committed_slot: None,
                publish_local_cache: false,
                atomic_group: None,
            })
            .collect::<Vec<_>>();
        let done_resp = self.batch_put_done(done_req_items).await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_put_done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(Err(err));
                continue;
            }
            if pending.item.remember_local_snapshot {
                self.remember_local_snapshot(&pending.item.key, pending.item.put_id);
            }
            if pending.item.make_replica_task {
                if let Err(err) = self
                    .ensure_remote_put(
                        &pending.item.key,
                        pending.item.put_id,
                        pending.item.preferred_sub_cluster.clone(),
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        "owner_batch_put_commit_reserved make replica task failed after local commit: key={} put_id=({},{}) err={}",
                        pending.item.key,
                        pending.item.put_id.0,
                        pending.item.put_id.1,
                        err
                    );
                }
            }
            results[pending.idx] = Some(Ok(()));
        }

        Ok(results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "owner_batch_put_commit_reserved result slot was not populated"
                            .to_string(),
                    }))
                })
            })
            .collect())
    }

    pub async fn owner_batch_put_abort_reserved(
        &self,
        items: Vec<OwnerReservedPutItem>,
    ) -> KvResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let revoke_items = items
            .into_iter()
            .map(|item| BatchPutRevokeItemReq {
                key: item.key,
                put_id: item.put_id,
            })
            .collect::<Vec<_>>();
        self.batch_put_revoke(revoke_items).await?;
        Ok(())
    }

    pub async unsafe fn batch_put_flat_dict_ptrs(
        &self,
        keys: Vec<String>,
        ptrs_groups: Vec<Vec<(u8, usize, u32, u64, u32, Option<u32>)>>,
        opts: crate::client_kv_api::PutOptionalArgs,
        _transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<()>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_put_flat_dict_ptrs"
                    .to_string(),
            }));
        }
        if keys.len() != ptrs_groups.len() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "batch_put_flat_dict_ptrs requires keys and ptrs_groups to have the same length: keys={} ptrs_groups={}",
                    keys.len(),
                    ptrs_groups.len()
                ),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let reject_if_exist_same_key = opts.reject_if_exist_same_key();
        let make_replica_task = opts.make_replica_task();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());

        let mut start_items = Vec::with_capacity(keys.len());
        let mut payload_lens = Vec::with_capacity(keys.len());
        for (key, ptrs) in keys.iter().zip(ptrs_groups.iter()) {
            let payload_len = calc_flat_dict_encoded_len(ptrs)?;
            payload_lens.push(payload_len);
            start_items.push(BatchPutStartItemReq {
                key: key.clone(),
                len: payload_len,
                reject_if_inflight_same_key,
                reject_if_exist_same_key,
                make_replica_task,
                preferred_sub_cluster: preferred_sub_cluster.clone(),
            });
        }

        let start_resp = self.batch_put_start(start_items).await?;
        if start_resp.items.len() != keys.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_put_start response length mismatch: expected={} got={}",
                    keys.len(),
                    start_resp.items.len()
                ),
            }));
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            put_id: PutIDForAKey,
            lease_id: Option<u64>,
            remember_local_snapshot: bool,
            make_replica_task: bool,
            preferred_sub_cluster: Option<String>,
        }

        let mut results: Vec<Option<KvResult<()>>> = (0..keys.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let short_circuit_payload = self.short_circuit_put_payload_path_enabled();
        let skip_put_end_commit = self.skip_put_end_commit_enabled();

        for (idx, (((key, ptrs), _payload_len), start_item)) in keys
            .into_iter()
            .zip(ptrs_groups.into_iter())
            .zip(payload_lens.into_iter())
            .zip(start_resp.items.into_iter())
            .enumerate()
        {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                results[idx] = Some(Err(err));
                continue;
            }

            let put_id = start_item.put_id;
            let remember_local_snapshot = true;
            let replica_admitted = start_item.replica_target.is_some();

            if !short_circuit_payload {
                unsafe {
                    write_flat_dict_ptrs_to_ptr(start_item.src_addr as *mut u8, &ptrs);
                }
            }

            done_pending.push(DonePending {
                idx,
                key,
                put_id,
                lease_id,
                remember_local_snapshot,
                make_replica_task: make_replica_task && replica_admitted,
                preferred_sub_cluster: preferred_sub_cluster.clone(),
            });
        }

        if skip_put_end_commit {
            for pending in done_pending {
                results[pending.idx] = Some(Ok(()));
            }
            return Ok(results
                .into_iter()
                .map(|item| {
                    item.unwrap_or_else(|| {
                        Err(KvError::Api(ApiError::Unknown {
                            detail: "batch_put result slot was not populated".to_string(),
                        }))
                    })
                })
                .collect());
        }

        let done_req_items = done_pending
            .iter()
            .map(|pending| BatchPutDoneItemReq {
                key: pending.key.clone(),
                put_id: pending.put_id,
                lease_id: pending.lease_id,
                committed_slot: None,
                publish_local_cache: false,
                atomic_group: None,
            })
            .collect::<Vec<_>>();
        let done_resp = self.batch_put_done(done_req_items).await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_put_done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }
        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(Err(err));
                continue;
            }
            if pending.remember_local_snapshot {
                self.remember_local_snapshot(&pending.key, pending.put_id);
            }
            if pending.make_replica_task {
                if let Err(err) = self
                    .ensure_remote_put(
                        &pending.key,
                        pending.put_id,
                        pending.preferred_sub_cluster.clone(),
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        "batch make replica task failed after local commit: key={} put_id=({},{}) err={}",
                        pending.key,
                        pending.put_id.0,
                        pending.put_id.1,
                        err
                    );
                }
            }
            results[pending.idx] = Some(Ok(()));
        }

        Ok(results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_put result slot was not populated".to_string(),
                    }))
                })
            })
            .collect())
    }

    /// Ensure one remote backing exists for an exact owner-local generation.
    ///
    /// Normal Put, pre-reserved replica, proactive write-back, and tier1 all
    /// enter here.  Only the flight leader pins the source and immediately
    /// launches Start/transfer/Done in its own async task; there is no actor or
    /// intermediate work queue. Followers wait for and reuse that terminal
    /// result instead of acquiring their own holder.
    pub async fn ensure_remote_put(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) -> KvResult<bool> {
        let mut follower_takeover_attempted = false;
        loop {
            match self.begin_owner_remote_put(
                key,
                put_id,
                preferred_sub_cluster.clone(),
                protect_source_on_remote_complete,
            ) {
                OwnerRemotePutReservation::Leader { op, memory_info } => {
                    let holder = Arc::new(UserMemHolder::new(
                        memory_info,
                        self.get_or_init_all_memholder_refcount(),
                        UserMemHolderExposeKind::SegPtr,
                    ));
                    let spawn_view = self.view.clone_view();
                    let task_view = spawn_view.clone();
                    let _ = spawn_view.spawn("owner_remote_put_leader", async move {
                        run_owner_remote_put(task_view, op, holder).await;
                    });
                    return Ok(true);
                }
                OwnerRemotePutReservation::Follower(op) => {
                    tracing::debug!(
                        "owner remote Put joined existing flight: key={} put_id=({},{})",
                        op.key,
                        op.put_id.0,
                        op.put_id.1
                    );
                    match op.wait().await {
                        OwnerRemotePutOutcome::Published
                        | OwnerRemotePutOutcome::AlreadySatisfied => return Ok(true),
                        OwnerRemotePutOutcome::Obsolete => return Ok(false),
                        OwnerRemotePutOutcome::Failed if !follower_takeover_attempted => {
                            follower_takeover_attempted = true;
                            continue;
                        }
                        OwnerRemotePutOutcome::Failed => return Ok(false),
                        OwnerRemotePutOutcome::InFlight => {
                            unreachable!("remote Put wait must return a terminal outcome")
                        }
                    }
                }
                OwnerRemotePutReservation::SourceUnavailable => {
                    tracing::warn!(
                        "owner remote Put source is unavailable, fenced, or version-mismatched: key={} put_id=({},{})",
                        key,
                        put_id.0,
                        put_id.1
                    );
                    return Ok(false);
                }
            }
        }
    }

    async fn put_common<F>(
        &self,
        key: &str,
        payload_len: u64,
        len_for_start: u32,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
        make_replica_task: bool,
        preferred_sub_cluster: Option<&str>,
        lease_id: Option<u64>,
        _test_payload_len_u32: u32,
        _test_remove_after_fill: bool,
        fill_abs_src: F,
        dbg_addr_summary: bool,
        info_complete_tag: Option<&'static str>,
    ) -> KvResult<()>
    where
        F: FnOnce(u64),
    {
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let metrics = self.metrics_handle();

        let t1 = Utc::now().timestamp_micros();
        let (resp, _rpc_latency) = {
            match self
                .put_start(
                    key,
                    len_for_start,
                    reject_if_inflight_same_key,
                    reject_if_exist_same_key,
                    make_replica_task,
                    preferred_sub_cluster,
                )
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    obe_put_start_error_rpc(&metrics, &client_id, &node_role, key, payload_len);
                    return Err(err);
                }
            }
        };
        let t2 = Utc::now().timestamp_micros();
        if let Err(e) =
            crate::rpcresp_kvresult_convert::try_from_code(resp.error_code, resp.error_json.clone())
        {
            obe_put_start_error_status(&metrics, &client_id, &node_role, key, payload_len);
            return Err(e);
        }
        obe_put_start_success(&metrics, &client_id, &node_role, key, t1, t2);

        let put_id = resp.put_id;
        let peer_id = if &*resp.node_id == &*self.view.cluster_manager().get_self_info().id {
            None
        } else {
            Some(resp.node_id.clone())
        };
        let abs_src = resp.src_addr;
        let abs_target = resp.target_addr;
        let replica_admitted = resp.replica_target.is_some();

        #[cfg(test)]
        {
            self.test_record.add_transfering_put(
                key.to_string(),
                _test_payload_len_u32,
                put_id.0,
                put_id.1,
                resp.node_id.to_string(),
                format!("{:#x}", resp.target_addr),
            );
        }

        if self.short_circuit_put_payload_path_enabled() {
            #[cfg(test)]
            {
                if _test_remove_after_fill {
                    self.test_record
                        .remove_transfering_put(key.to_string(), put_id);
                }
            }

            let skipped_breakdown = if peer_id.is_none() && abs_src == abs_target {
                TransferBreakdown {
                    local_noop: true,
                    ..TransferBreakdown::default()
                }
            } else {
                TransferBreakdown::default()
            };
            metrics.pending_put_set_transfer_breakdown(
                put_id,
                skipped_breakdown.submit_blocking_us,
                skipped_breakdown.create_xfer_req_us,
                skipped_breakdown.post_xfer_req_us,
                skipped_breakdown.poll_wait_us,
                skipped_breakdown.poll_iters,
                skipped_breakdown.used_fast_path,
                false,
                skipped_breakdown.local_noop,
                skipped_breakdown.remote_transfer,
            );
            self.put_end(key, put_id, lease_id).await?;
            self.remember_local_snapshot(key, put_id);
            if make_replica_task && replica_admitted {
                if let Err(err) = self
                    .ensure_remote_put(key, put_id, preferred_sub_cluster.map(str::to_string), true)
                    .await
                {
                    tracing::warn!(
                        "make replica task failed after short-circuit local commit: key={} put_id=({},{}) err={}",
                        key,
                        put_id.0,
                        put_id.1,
                        err
                    );
                }
            }
            if let Some(tag) = info_complete_tag {
                info!("{tag} complete key={} bytes={}", key, payload_len);
            }
            return Ok(());
        }

        fill_abs_src(abs_src);

        #[cfg(test)]
        {
            if _test_remove_after_fill {
                self.test_record
                    .remove_transfering_put(key.to_string(), put_id);
            }
        }

        let base_addr = self
            .view
            .client_seg_pool()
            .cpu_mem_read_guard()
            .await
            .unwrap()
            .allocated_addr;
        if dbg_addr_summary {
            tracing::debug!(
                "put path addr summary: key={}, put_id=({},{}) local_base={:#x}, abs_src={:#x}, master_target_base={:#x}, abs_target={:#x}, peer_id={:?}",
                key,
                put_id.0,
                put_id.1,
                base_addr,
                abs_src,
                resp.target_base_addr,
                abs_target,
                peer_id
            );
        }

        if self.skip_put_end_commit_enabled() {
            let _ = metrics.pending_put_remove(&put_id);
            tracing::warn!(
                "skip_put_end_commit test-only fast-path: returning success without put_end; key={} put_id=({},{}) payload_len={}",
                key,
                put_id.0,
                put_id.1,
                payload_len
            );
            if let Some(tag) = info_complete_tag {
                info!(
                    "{tag} complete_without_put_end key={} bytes={}",
                    key, payload_len
                );
            }
            return Ok(());
        }

        self.put_end(key, put_id, lease_id).await?;
        self.remember_local_snapshot(key, put_id);
        if make_replica_task && replica_admitted {
            if let Err(err) = self
                .ensure_remote_put(key, put_id, preferred_sub_cluster.map(str::to_string), true)
                .await
            {
                tracing::warn!(
                    "make replica task failed after local commit: key={} put_id=({},{}) err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    err
                );
            }
        }
        if let Some(tag) = info_complete_tag {
            info!("{tag} complete key={} bytes={}", key, payload_len);
        }
        Ok(())
    }

    /// Put a key/value by encoding a flat dict from raw pointers directly into the segment pool.
    ///
    /// # Safety
    /// The caller must guarantee the pointer ranges remain readable for the duration of this async call.
    pub async unsafe fn put_flat_dict_ptrs(
        &self,
        key: &str,
        ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let reject_if_exist_same_key = opts.reject_if_exist_same_key();
        let make_replica_task = opts.make_replica_task();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());

        let payload_len = calc_flat_dict_encoded_len(&ptrs)?;
        self.put_common(
            key,
            payload_len,
            payload_len as u32,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            make_replica_task,
            preferred_sub_cluster.as_deref(),
            lease_id,
            payload_len as u32,
            /*test_remove_after_fill=*/ false,
            move |abs_src| {
                // Fill owner's shared memory at abs_src directly from the raw pointers.
                unsafe {
                    write_flat_dict_ptrs_to_ptr(abs_src as *mut u8, &ptrs);
                }
            },
            /*dbg_addr_summary=*/ false,
            Some("put_flat_dict_ptrs"),
        )
        .await
    }

    /// Put a key/value with optional args (e.g., lease binding)
    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let reject_if_exist_same_key = opts.reject_if_exist_same_key();
        let make_replica_task = opts.make_replica_task();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());
        let payload_len = value.len() as u64;
        self.put_common(
            key,
            payload_len,
            value.len() as u32,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            make_replica_task,
            preferred_sub_cluster.as_deref(),
            lease_id,
            value.len() as u32,
            /*test_remove_after_fill=*/ true,
            |abs_src| unsafe {
                std::ptr::copy_nonoverlapping(value.as_ptr(), abs_src as *mut u8, value.len());
            },
            /*dbg_addr_summary=*/ true,
            None,
        )
        .await
    }

    /// Transfer data by offsets with instrumentation for external/owner callers.
    /// Records transfer latency (t2..t3) and emits tsbuckets pulses.
    pub async fn put_transfer(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        src_offset: u64,
        target_offset: u64,
        len: u64,
        peer_id: Option<NodeIDString>,
        target_base_addr: Option<u64>,
    ) -> KvResult<TransferBreakdown> {
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        // owner/external inner is stable after construction; base_addr must exist
        let base_addr = self
            .view
            .client_seg_pool()
            .cpu_mem_read_guard()
            .await
            .unwrap()
            .allocated_addr;
        let abs_src = base_addr + src_offset;
        let abs_target = if peer_id.is_some() {
            let Some(tb) = target_base_addr else {
                // propagate as Unreachable: invalid remote target context from distributed input
                let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Unreachable(
                    crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::RpcDecodeError {
                        rpc_input_json: format!(
                            "missing target_base_addr while peer_id present; src_off={:#x}, tgt_off={:#x}",
                            src_offset, target_offset
                        ),
                    },
                );
                return Err(err);
            };
            tb + target_offset
        } else {
            base_addr + target_offset
        };

        // Local placement can resolve to src==target, which means the payload is already in-place.
        // Skip the transfer-engine hop for this no-op path to avoid paying an extra fixed cost.
        if peer_id.is_none() && abs_src == abs_target {
            tracing::debug!(
                "put_transfer local no-op: key={}, put_id=({},{}) src==target {:#x}, len={}",
                key,
                put_id.0,
                put_id.1,
                abs_target,
                len
            );
            return Ok(TransferBreakdown {
                local_noop: true,
                ..TransferBreakdown::default()
            });
        } else {
            let breakdown = self
                .view
                .client_transfer_engine()
                .transfer_data_no_copy(peer_id.clone(), false, abs_src, abs_target, len, None)
                .await?;
            tracing::debug!(
                "put_transfer breakdown: key={}, put_id=({},{}) fast_path={} nixl={} local_noop={} remote_transfer={} submit_blocking_us={} create_xfer_req_us={} post_xfer_req_us={} poll_wait_us={} poll_iters={}",
                key,
                put_id.0,
                put_id.1,
                breakdown.used_fast_path,
                false,
                breakdown.local_noop,
                breakdown.remote_transfer,
                breakdown.submit_blocking_us,
                breakdown.create_xfer_req_us,
                breakdown.post_xfer_req_us,
                breakdown.poll_wait_us,
                breakdown.poll_iters
            );
            tracing::debug!(
                "put_transfer success: key={}, put_id=({},{}) src_off={:#x}, tgt_off={:#x}, len={}, peer_id={:?}",
                key,
                put_id.0,
                put_id.1,
                src_offset,
                target_offset,
                len,
                peer_id
            );

            // Emit transfer stage success and tsbuckets pulse (computes t2/t3 using pending)
            crate::observe_kvope::obe_put_transfer_success(
                &metrics, &client_id, &node_role, key, len, put_id,
            );
            return Ok(breakdown);
        }
        #[allow(unreachable_code)]
        Ok(TransferBreakdown::default())
    }

    /// 开始 Put 操作，分配存储空间
    pub async fn put_start_with_source_node(
        &self,
        key: &str,
        len: u32,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
        make_replica_task: bool,
        preferred_sub_cluster: Option<&str>,
        source_node_id: Option<NodeIDString>,
    ) -> KvResult<(PutStartResp, i64)> {
        let req = MsgPack {
            serialize_part: PutStartReq {
                key: key.to_string(),
                len: len as u64,
                reject_if_inflight_same_key,
                reject_if_exist_same_key,
                make_replica_task,
                preferred_sub_cluster: preferred_sub_cluster.map(|s| s.to_string()),
                source_node_id,
            },
            raw_bytes: Vec::new(),
        };

        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let rpc_started_at = Instant::now();
        let start_rpc_timestamp = Utc::now().timestamp_micros() as i64;
        let resp = self
            .rpc_caller_put_start
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(|e| KvError::P2p(e))?;
        let end_rpc_timestamp = Utc::now().timestamp_micros() as i64;
        let ser = resp.serialize_part.clone();
        if crate::rpcresp_kvresult_convert::try_from_code(ser.error_code, ser.error_json.clone())
            .is_ok()
        {
            let metrics = self.metrics_handle();
            metrics.pending_put_insert(
                ser.put_id,
                key.to_string(),
                len as u64,
                start_rpc_timestamp,
                end_rpc_timestamp,
                ser.server_process_us,
            );
        }
        let rpc_latency_us = duration_to_i64_us(rpc_started_at.elapsed());
        Ok((ser, rpc_latency_us))
    }

    /// 开始 Put 操作，分配存储空间
    pub async fn put_start(
        &self,
        key: &str,
        len: u32,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
        make_replica_task: bool,
        preferred_sub_cluster: Option<&str>,
    ) -> KvResult<(PutStartResp, i64)> {
        self.put_start_with_source_node(
            key,
            len,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            make_replica_task,
            preferred_sub_cluster,
            None,
        )
        .await
    }

    pub async fn reserve_local_grant(&self) -> KvResult<ReserveLocalGrantOutcome> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting reserve_local_grant".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: ReserveLocalGrantReq {},
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_reserve_local_grant
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        match resp.serialize_part.outcome {
            ReserveLocalGrantOutcome::None => Err(KvError::Api(ApiError::Unknown {
                detail: "reserve_local_grant returned success without an outcome".to_string(),
            })),
            outcome => Ok(outcome),
        }
    }

    pub async fn release_local_grant(&self, grant_id: u64) -> KvResult<()> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting release_local_grant".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: ReleaseLocalGrantReq { grant_id },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_release_local_grant
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(())
    }

    /// 撤销 Put 操作，释放已分配的资源
    pub async fn put_revoke(&self, key: &str, put_id: PutIDForAKey) -> KvResult<()> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutRevokeReq {
                key: key.to_string(),
                put_id,
            },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let _resp = self
            .rpc_caller_put_revoke
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        // cleanup pending stat if any
        let _ = self.metrics_handle().pending_put_remove(&put_id);
        Ok(())
    }

    pub async fn put_append_start(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        len: u32,
        preferred_sub_cluster: Option<&str>,
        protect_source_on_remote_complete: bool,
    ) -> KvResult<PutAppendStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_append_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutAppendStartReq {
                key: key.to_string(),
                put_id,
                len: len as u64,
                preferred_sub_cluster: preferred_sub_cluster.map(|s| s.to_string()),
                protect_source_on_remote_complete,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_put_append_start
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_put_append_start(
        &self,
        items: Vec<BatchPutAppendStartItemReq>,
    ) -> KvResult<BatchPutAppendStartResp> {
        if items.is_empty() {
            return Ok(BatchPutAppendStartResp {
                items: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_put_append_start"
                    .to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchPutAppendStartReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_put_append_start
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_evict_owner_source(
        &self,
        victims: Vec<OwnerSourceEvictionVictim>,
    ) -> KvResult<BatchEvictOwnerSourceResp> {
        if victims.is_empty() {
            return Ok(BatchEvictOwnerSourceResp {
                operation_id: 0,
                victims: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting owner source eviction".to_string(),
            }));
        }
        let operation_id = self
            .next_owner_source_eviction_operation_id
            .fetch_add(1, Ordering::Relaxed);
        let self_info = self.view.cluster_manager().get_self_info();
        let req = MsgPack {
            serialize_part: BatchEvictOwnerSourceReq {
                operation_id,
                owner_node_start_time: self_info.node_start_time,
                victims,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_evict_owner_source
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        if resp.serialize_part.operation_id != operation_id {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner source-eviction operation id mismatch: requested={} response={}",
                    operation_id, resp.serialize_part.operation_id
                ),
            }));
        }
        Ok(resp.serialize_part)
    }

    pub async fn put_append_revoke(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        operation_id: u64,
    ) -> KvResult<()> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_append_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutAppendRevokeReq {
                key: key.to_string(),
                put_id,
                operation_id,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let _resp = self
            .rpc_caller_put_append_revoke
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        Ok(())
    }

    pub async fn put_append_done(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        operation_id: u64,
    ) -> KvResult<PutAppendDoneResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_append_done".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutAppendDoneReq {
                key: key.to_string(),
                put_id,
                operation_id,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_put_append_done
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                0,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_put_append_done(
        &self,
        items: Vec<BatchPutAppendDoneItemReq>,
    ) -> KvResult<BatchPutAppendDoneResp> {
        if items.is_empty() {
            return Ok(BatchPutAppendDoneResp {
                items: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_put_append_done".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchPutAppendDoneReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_put_append_done
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                0,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_put_start(
        &self,
        items: Vec<BatchPutStartItemReq>,
    ) -> KvResult<BatchPutStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_put_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchPutStartReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_put_start
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_prepare_put_keys(
        &self,
        items: Vec<BatchPreparePutKeyItemReq>,
    ) -> KvResult<BatchPreparePutKeysResp> {
        if items.is_empty() {
            return Ok(BatchPreparePutKeysResp {
                reservation_ids: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_prepare_put_keys"
                    .to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchPreparePutKeysReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_prepare_put_keys
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_release_put_key_reservations(
        &self,
        reservation_ids: Vec<u64>,
    ) -> KvResult<BatchReleasePutKeyReservationsResp> {
        if reservation_ids.is_empty() {
            return Ok(BatchReleasePutKeyReservationsResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail:
                    "ClientKvApi is shutting down; rejecting batch_release_put_key_reservations"
                        .to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchReleasePutKeyReservationsReq { reservation_ids },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_release_put_key_reservations
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_put_revoke(
        &self,
        items: Vec<BatchPutRevokeItemReq>,
    ) -> KvResult<BatchPutRevokeResp> {
        if items.is_empty() {
            return Ok(BatchPutRevokeResp {
                items: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_put_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchPutRevokeReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_put_revoke
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                2,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn batch_put_done(
        &self,
        items: Vec<BatchPutDoneItemReq>,
    ) -> KvResult<BatchPutDoneResp> {
        if items.is_empty() {
            return Ok(BatchPutDoneResp {
                items: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_put_done".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchPutDoneReq { items },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_put_done
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                0,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    pub async fn grouped_batch_put_done(
        &self,
        items: Vec<GroupedBatchPutDoneItemReq>,
        atomic_group_lens: Vec<usize>,
    ) -> KvResult<GroupedBatchPutDoneResp> {
        if items.is_empty() {
            return Ok(GroupedBatchPutDoneResp {
                items: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting grouped_batch_put_done"
                    .to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GroupedBatchPutDoneReq {
                items,
                atomic_group_lens,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_grouped_batch_put_done
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                RpcTransportPolicy::ForceTransport,
                0,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    /// 完成 Put 操作，提交数据（inner，无监控）
    pub async fn put_end_inner(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
    ) -> KvResult<PutEndStats> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_end".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutDoneReq {
                key: key.to_string(),
                put_id,
                lease_id,
                committed_slot: None,
                publish_local_cache: false,
                atomic_group: None,
            },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let rpc_started_at = Instant::now();

        // 调用 RPC
        let resp = self
            .rpc_caller_put_done
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                0,
            )
            .await
            .map_err(KvError::from)?;
        if let Err(e) = crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        ) {
            return Err(e);
        }
        Ok(PutEndStats {
            master_put_end_rpc_us: duration_to_i64_us(rpc_started_at.elapsed()),
            master_put_end_server_us: resp.serialize_part.server_process_us,
        })
    }

    pub async fn put_end_inner_with_local_cache_publish(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
        publish_local_cache: bool,
    ) -> KvResult<PutEndWithLocalCachePublish> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_end".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutDoneReq {
                key: key.to_string(),
                put_id,
                lease_id,
                committed_slot: None,
                publish_local_cache,
                atomic_group: None,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let rpc_started_at = Instant::now();
        let resp = self
            .rpc_caller_put_done
            .call(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                0,
            )
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(PutEndWithLocalCachePublish {
            stats: PutEndStats {
                master_put_end_rpc_us: duration_to_i64_us(rpc_started_at.elapsed()),
                master_put_end_server_us: resp.serialize_part.server_process_us,
            },
            local_cache_holder_id: resp.serialize_part.local_cache_holder_id,
        })
    }

    /// 完成 Put 操作，提交数据（带监控）：适配 external 路径，统一聚合 t1..t4
    pub async fn put_end(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
    ) -> KvResult<PutEndStats> {
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        let end_stats = match self.put_end_inner(key, put_id, lease_id).await {
            Ok(stats) => stats,
            Err(e) => {
                // on error, emit end error using pending info if exists, then cleanup
                crate::observe_kvope::obe_put_end_error_from_pending(
                    &metrics, &client_id, &node_role, put_id,
                );
                return Err(e);
            }
        };

        // record end_handle to pending before aggregation
        metrics.pending_put_set_end_handle(put_id, end_stats.master_put_end_server_us);

        // success: aggregate with pending timestamps; this also clears pending
        crate::observe_kvope::obe_put_done_success_from_pending(
            &metrics, &client_id, &node_role, key, put_id, 0,
        );
        Ok(end_stats)
    }

    pub async fn put_end_with_local_cache_publish(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        lease_id: Option<u64>,
        publish_local_cache: bool,
    ) -> KvResult<PutEndWithLocalCachePublish> {
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        let end = match self
            .put_end_inner_with_local_cache_publish(key, put_id, lease_id, publish_local_cache)
            .await
        {
            Ok(end) => end,
            Err(e) => {
                crate::observe_kvope::obe_put_end_error_from_pending(
                    &metrics, &client_id, &node_role, put_id,
                );
                return Err(e);
            }
        };

        metrics.pending_put_set_end_handle(put_id, end.stats.master_put_end_server_us);
        crate::observe_kvope::obe_put_done_success_from_pending(
            &metrics, &client_id, &node_role, key, put_id, 0,
        );
        Ok(end)
    }
}

const OWNER_LOCAL_PUBLISH_RETRY_INITIAL: Duration = Duration::from_millis(25);
const OWNER_LOCAL_PUBLISH_RETRY_MAX: Duration = Duration::from_secs(1);

#[cfg(test)]
mod owner_hot_replica_policy_tests {
    use super::{
        OwnerLocalPublishItem, complete_owner_local_publish_group_lens,
        owner_local_publish_atomic_batch_complete,
    };
    use crate::master_kv_router::msg_pack::{
        PutAtomicGroup, PutAtomicGroupMember, PutDoneCommittedSlot,
    };

    fn publish_item(
        key: &str,
        put_id: (u64, u32),
        atomic_group: Option<PutAtomicGroup>,
    ) -> OwnerLocalPublishItem {
        OwnerLocalPublishItem {
            key: key.to_string(),
            put_id,
            value_len: 1,
            lease_id: None,
            committed_slot: PutDoneCommittedSlot::default(),
            make_replica_task: false,
            preferred_sub_cluster: None,
            atomic_group,
        }
    }

    #[test]
    fn grouped_publish_partition_requires_a_complete_ordered_group() {
        let group = PutAtomicGroup {
            members: vec![
                PutAtomicGroupMember {
                    key: "a".to_string(),
                    put_id: (1, 0),
                },
                PutAtomicGroupMember {
                    key: "b".to_string(),
                    put_id: (1, 1),
                },
                PutAtomicGroupMember {
                    key: "c".to_string(),
                    put_id: (1, 2),
                },
            ],
        };
        let complete = vec![
            publish_item("a", (1, 0), Some(group.clone())),
            publish_item("b", (1, 1), Some(group.clone())),
            publish_item("c", (1, 2), Some(group.clone())),
            publish_item("single", (2, 0), None),
        ];
        assert_eq!(
            complete_owner_local_publish_group_lens(&complete),
            Some(vec![3, 1])
        );

        let partial = vec![
            publish_item("a", (1, 0), Some(group.clone())),
            publish_item("b", (1, 1), Some(group)),
        ];
        assert_eq!(complete_owner_local_publish_group_lens(&partial), None);
    }

    #[test]
    fn hot_admission_waits_for_every_atomic_group_member_to_publish() {
        let group = PutAtomicGroup {
            members: vec![
                PutAtomicGroupMember {
                    key: "a".to_string(),
                    put_id: (1, 0),
                },
                PutAtomicGroupMember {
                    key: "b".to_string(),
                    put_id: (1, 1),
                },
            ],
        };
        let items = vec![
            publish_item("a", (1, 0), Some(group.clone())),
            publish_item("b", (1, 1), Some(group)),
            publish_item("single", (2, 0), None),
        ];
        let partial = vec![&items[0]];
        assert!(!owner_local_publish_atomic_batch_complete(
            &items[0], &partial
        ));
        let complete = vec![&items[0], &items[1]];
        assert!(owner_local_publish_atomic_batch_complete(
            &items[0], &complete
        ));
        assert!(owner_local_publish_atomic_batch_complete(
            &items[1], &complete
        ));
        assert!(owner_local_publish_atomic_batch_complete(
            &items[2], &partial
        ));
    }
}

async fn start_replica_append_with_retry(
    inner: &ClientKvApiInner,
    op: &OwnerRemotePutSharedOp,
    len: u32,
) -> KvResult<PutAppendStartResp> {
    let request = op.request();
    inner
        .put_append_start(
            &op.key,
            op.put_id,
            len,
            request.preferred_sub_cluster.as_deref(),
            request.protect_source_on_remote_complete,
        )
        .await
}

struct RemotePutTarget {
    operation_id: u64,
    node_id: String,
    target_offset: u64,
    target_base_addr: u64,
    len: u64,
}

fn owner_source_eviction_identity(
    victim: &OwnerSourceEvictionVictim,
) -> super::OwnerHotReplicaIdentity {
    super::OwnerHotReplicaIdentity {
        key: victim.key.clone(),
        put_time_ms: victim.put_id.0,
        put_version: victim.put_id.1,
    }
}

fn owner_source_eviction_victim(
    identity: &super::OwnerHotReplicaIdentity,
    memory_info: &crate::memholder::MemoryInfo,
) -> Option<OwnerSourceEvictionVictim> {
    let (slot_size, grant_id, slot_index) = memory_info.local_reserve_resident_slot_ref()?;
    Some(OwnerSourceEvictionVictim {
        key: identity.key.clone(),
        put_id: (identity.put_time_ms, identity.put_version),
        backing: OwnerReclaimBacking::CommittedSlot {
            grant_id,
            slot_index,
            slot_size,
        },
    })
}

fn finish_owner_source_selection(
    inner: &ClientKvApiInner,
    victim: &OwnerSourceEvictionVictim,
    restore_current: bool,
    reason: &str,
) {
    let identity = owner_source_eviction_identity(victim);
    if let Some(debt) = inner.owner_hot_remove_source_selection_debt(&identity) {
        debt.release();
        inner
            .owner_hot_counters
            .source_evict_restored_members
            .fetch_add(1, Ordering::Relaxed);
    }
    inner.owner_hot_retry_queue.remove(&identity);
    if !restore_current {
        return;
    }
    inner.owner_hot_restore_source_selection(&identity);
    if inner.owner_hot_admit_published_committed(&victim.key, victim.put_id) {
        tracing::warn!(
            key = victim.key,
            put_time_ms = victim.put_id.0,
            put_version = victim.put_id.1,
            reason,
            "restored current source to owner-hot after source eviction did not enter reclaim"
        );
    }
}

fn schedule_owner_source_eviction_retry(
    inner: &ClientKvApiInner,
    mut event: OwnerHotEvictionEvent,
    victim: Arc<OwnerSourceEvictionVictim>,
    reason: &'static str,
) {
    event.retry = true;
    event.source_eviction_victim = Some(victim);
    inner.owner_hot_retry_queue.schedule(event, reason);
}

fn prepare_owner_source_eviction_event(
    inner: &ClientKvApiInner,
    mut event: OwnerHotEvictionEvent,
) -> Option<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)> {
    let trigger = super::OwnerHotReplicaIdentity {
        key: event.key.clone(),
        put_time_ms: event.put_id.0,
        put_version: event.put_id.1,
    };

    if event.retry {
        let _ = inner.owner_hot_retry_queue.take_for_inflight(&trigger);
    }
    if let Some(victim) = event.source_eviction_victim.clone() {
        if inner
            .owner_source_eviction_selected
            .contains_key(&owner_source_eviction_identity(&victim))
        {
            return Some((event, victim));
        }
        // Commit/invalidation may win the race with a due retry.
        finish_owner_source_selection(inner, &victim, true, "retry victim lost selected identity");
        event.selection_debt.release();
        return None;
    }

    if inner.owner_source_eviction_selected.contains_key(&trigger) {
        event.selection_debt.release();
        inner
            .owner_hot_counters
            .victim_duplicates
            .fetch_add(1, Ordering::Relaxed);
        return None;
    }

    let (resolved_trigger, source) = match inner.owner_hot_prepare_eviction(&event) {
        OwnerHotEvictionPreparation::Ready { trigger, source } => (trigger, source),
        OwnerHotEvictionPreparation::RetryableReclaimFence => {
            inner.owner_hot_retry_queue.schedule(
                event,
                "owner reclaim fence busy before exact source selection",
            );
            return None;
        }
        OwnerHotEvictionPreparation::TemporarilyPinned => {
            // This event was removed from Moka, but its source is still
            // serving a local reader. Do not hand it to the master's reclaim
            // loop and do not retain projected selection credit. Re-admission
            // refreshes the trigger's recency so the next pressure kick can
            // choose a different, currently reclaimable victim.
            event.selection_debt.release();
            inner
                .owner_hot_counters
                .skipped_active_holders
                .fetch_add(1, Ordering::Relaxed);
            let restored = inner.owner_hot_admit_published_committed(&event.key, event.put_id);
            tracing::debug!(
                key = event.key,
                put_time_ms = event.put_id.0,
                put_version = event.put_id.1,
                restored,
                "owner pressure eviction skipped an actively held source"
            );
            return None;
        }
        OwnerHotEvictionPreparation::Obsolete => {
            event.selection_debt.release();
            inner
                .owner_hot_counters
                .source_evict_obsolete
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    debug_assert_eq!(resolved_trigger, trigger);

    let Some(victim) = owner_source_eviction_victim(&trigger, source.as_ref()) else {
        event.selection_debt.release();
        inner
            .owner_hot_counters
            .victim_invalid_backing
            .fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            key = event.key,
            put_time_ms = event.put_id.0,
            put_version = event.put_id.1,
            "owner-hot selected a source without exact CommittedSlot backing"
        );
        return None;
    };
    let victim = Arc::new(victim);

    match inner.owner_hot_install_source_selection_fence(&trigger, &source) {
        OwnerHotSelectionFenceOutcome::Fenced => {}
        OwnerHotSelectionFenceOutcome::Retryable => {
            inner
                .owner_hot_retry_queue
                .schedule(event, "owner source selection fence is temporarily busy");
            return None;
        }
        OwnerHotSelectionFenceOutcome::TemporarilyPinned => {
            event.selection_debt.release();
            inner
                .owner_hot_counters
                .skipped_active_holders
                .fetch_add(1, Ordering::Relaxed);
            inner.owner_hot_admit_published_committed(&event.key, event.put_id);
            return None;
        }
        OwnerHotSelectionFenceOutcome::Obsolete => {
            event.selection_debt.release();
            inner
                .owner_hot_counters
                .source_evict_obsolete
                .fetch_add(1, Ordering::Relaxed);
            return None;
        }
    }

    if inner.owner_source_eviction_selected.contains_key(&trigger) {
        inner.owner_hot_restore_source_selection(&trigger);
        event.selection_debt.release();
        inner
            .owner_hot_counters
            .victim_duplicates
            .fetch_add(1, Ordering::Relaxed);
        return None;
    }

    if !inner.owner_hot_install_source_selection_debt(trigger.clone(), event.selection_debt.clone())
    {
        inner.owner_hot_restore_source_selection(&trigger);
        event.selection_debt.release();
        return None;
    }
    event.source_eviction_victim = Some(victim.clone());
    Some((event, victim))
}

async fn process_owner_source_eviction_events(
    view: &ClientKvApiView,
    events: Vec<OwnerHotEvictionEvent>,
) {
    let inner = view.client_kv_api().inner();
    let prepared = events
        .into_iter()
        .filter_map(|event| prepare_owner_source_eviction_event(inner, event))
        .collect::<Vec<_>>();
    if prepared.is_empty() {
        return;
    }
    let victims = prepared
        .iter()
        .map(|(_, victim)| victim.as_ref().clone())
        .collect::<Vec<_>>();
    let response = inner.batch_evict_owner_source(victims).await;
    let Ok(response) = response else {
        for (event, victim) in prepared {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "owner source-eviction RPC failed",
            );
        }
        return;
    };
    if response.victims.len() != prepared.len() {
        for (event, victim) in prepared {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "owner source-eviction response length mismatch",
            );
        }
        return;
    }

    let retryable = response
        .victims
        .iter()
        .filter(|result| {
            matches!(
                result.outcome,
                OwnerSourceEvictionOutcome::RetryableBusy | OwnerSourceEvictionOutcome::Unspecified
            )
        })
        .count();
    if retryable != 0
        && let Some((index, result)) = response.victims.iter().enumerate().find(|(_, result)| {
            matches!(
                result.outcome,
                OwnerSourceEvictionOutcome::RetryableBusy | OwnerSourceEvictionOutcome::Unspecified
            )
        })
    {
        let victim = &prepared[index].1;
        tracing::warn!(
            operation_id = response.operation_id,
            victims = response.victims.len(),
            retryable,
            first_victim_index = result.victim_index,
            key = %victim.key,
            put_time_ms = victim.put_id.0,
            put_version = victim.put_id.1,
            detail = %result.detail,
            "owner source direct-delete batch has retryable victims"
        );
    }

    for (index, ((event, victim), result)) in prepared
        .into_iter()
        .zip(response.victims.into_iter())
        .enumerate()
    {
        if result.victim_index != u32::try_from(index).unwrap_or(u32::MAX) {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "owner source-eviction response identity mismatch",
            );
            continue;
        }
        match result.outcome {
            OwnerSourceEvictionOutcome::Accepted
            | OwnerSourceEvictionOutcome::AlreadyInProgress => {
                inner
                    .owner_hot_counters
                    .source_evict_handoff_members
                    .fetch_add(1, Ordering::Relaxed);
                // Selected debt stays live until owner reclaim Commit calls
                // owner_hot_invalidate_version for the exact victim.
            }
            OwnerSourceEvictionOutcome::Completed => {
                let epoch = owner_source_eviction_epoch(response.operation_id, index);
                match super::reclaim::complete_owner_source_eviction(inner, &victim, epoch) {
                    Ok(()) => {
                        inner
                            .owner_hot_counters
                            .source_evict_handoff_members
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(detail) => {
                        tracing::warn!(
                            key = victim.key,
                            put_time_ms = victim.put_id.0,
                            put_version = victim.put_id.1,
                            detail,
                            "master deleted source route but owner slot release must retry"
                        );
                        schedule_owner_source_eviction_retry(
                            inner,
                            event,
                            victim,
                            "owner slot release after master direct-delete is temporarily busy",
                        );
                    }
                }
            }
            OwnerSourceEvictionOutcome::RetryableBusy | OwnerSourceEvictionOutcome::Unspecified => {
                schedule_owner_source_eviction_retry(
                    inner,
                    event,
                    victim,
                    "master source reclaim is temporarily busy",
                );
            }
            OwnerSourceEvictionOutcome::Stale => {
                finish_owner_source_selection(
                    inner,
                    &victim,
                    true,
                    "master rejected stale source identity",
                );
            }
            OwnerSourceEvictionOutcome::RejectedNotEvictable => {
                tracing::error!(
                    key = victim.key,
                    detail = result.detail,
                    "owner selected a source victim that master declared non-evictable"
                );
                finish_owner_source_selection(
                    inner,
                    &victim,
                    true,
                    "master rejected non-evictable source victim",
                );
            }
        }
    }
}

pub fn spawn_owner_source_eviction_dispatcher(
    view: ClientKvApiView,
    mut rx: tokio::sync::ampsc::UnboundedReceiver<super::OwnerHotEvictionDispatch>,
) {
    const ORPHAN_MERGE_WINDOW: Duration = Duration::from_millis(2);

    let view_task = view.clone();
    let _ = view.spawn("owner_source_eviction_dispatcher", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let mut events = Vec::new();
        let mut pressure_open = false;
        loop {
            let dispatch = if events.is_empty() || pressure_open {
                tokio::select! {
                    _ = shutdown_waiter.wait() => {
                        tracing::info!("owner source-eviction dispatcher stopping due to shutdown");
                        break;
                    }
                    dispatch = rx.recv() => dispatch,
                }
            } else {
                tokio::select! {
                    _ = shutdown_waiter.wait() => {
                        tracing::info!("owner source-eviction dispatcher stopping due to shutdown");
                        break;
                    }
                    dispatch = rx.recv() => dispatch,
                    _ = tokio::time::sleep(ORPHAN_MERGE_WINDOW) => {
                        process_owner_source_eviction_events(
                            &view_task,
                            std::mem::take(&mut events),
                        )
                        .await;
                        continue;
                    }
                }
            };
            let Some(dispatch) = dispatch else {
                if !events.is_empty() {
                    process_owner_source_eviction_events(&view_task, std::mem::take(&mut events))
                        .await;
                }
                break;
            };
            match dispatch {
                super::OwnerHotEvictionDispatch::Victim(event) => events.push(event),
                super::OwnerHotEvictionDispatch::BeginPressure { requested_bytes } => {
                    if pressure_open {
                        tracing::error!(
                            requested_bytes,
                            "nested owner pressure selection batch is not allowed"
                        );
                    }
                    if !events.is_empty() {
                        process_owner_source_eviction_events(
                            &view_task,
                            std::mem::take(&mut events),
                        )
                        .await;
                    }
                    pressure_open = true;
                }
                super::OwnerHotEvictionDispatch::EndPressure { selected_bytes } => {
                    if !pressure_open {
                        tracing::warn!(
                            selected_bytes,
                            "owner pressure selection ended without a matching begin marker"
                        );
                    }
                    pressure_open = false;
                    process_owner_source_eviction_events(&view_task, std::mem::take(&mut events))
                        .await;
                }
                super::OwnerHotEvictionDispatch::Flush => {
                    if !pressure_open && !events.is_empty() {
                        process_owner_source_eviction_events(
                            &view_task,
                            std::mem::take(&mut events),
                        )
                        .await;
                    }
                }
            }
        }
    });
}

pub fn spawn_owner_hot_retry_actor(view: ClientKvApiView) {
    const RETRY_EMIT_BATCH: usize = 128;
    const RETRY_POLL_INTERVAL: Duration = Duration::from_millis(25);

    let view_task = view.clone();
    let _ = view.spawn("owner_hot_retry_actor", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let retry_queue = view_task
            .client_kv_api()
            .inner()
            .owner_hot_retry_queue
            .clone();
        let notify = retry_queue.notify.clone();
        let mut tick = tokio::time::interval(RETRY_POLL_INTERVAL);
        loop {
            tokio::select! {
                _ = shutdown_waiter.wait() => return,
                _ = tick.tick() => {},
                _ = notify.notified() => {},
            }
            let events = retry_queue.take_due_batch(Instant::now(), RETRY_EMIT_BATCH);
            if events.is_empty() {
                continue;
            }
            let inner = view_task.client_kv_api().inner();
            for event in events {
                if let Err(err) = inner
                    .owner_hot_eviction_tx
                    .send(super::OwnerHotEvictionDispatch::Victim(event))
                {
                    let super::OwnerHotEvictionDispatch::Victim(event) = err.0 else {
                        unreachable!("the retry actor only sends victim events")
                    };
                    retry_queue.schedule(event, "retry dispatcher closed");
                    return;
                }
                inner
                    .owner_hot_counters
                    .source_evict_retry_emitted
                    .fetch_add(1, Ordering::Relaxed);
            }
            if inner
                .owner_hot_eviction_tx
                .send(super::OwnerHotEvictionDispatch::Flush)
                .is_err()
            {
                return;
            }
        }
    });
}

async fn fail_owner_remote_put(
    view: &ClientKvApiView,
    op: Arc<OwnerRemotePutSharedOp>,
    operation_id: Option<u64>,
    reason: &'static str,
) {
    let inner = view.client_kv_api().inner();
    if let Some(operation_id) = operation_id {
        if let Err(revoke_err) = inner
            .put_append_revoke(&op.key, op.put_id, operation_id)
            .await
        {
            tracing::warn!(
                "owner remote Put revoke failed after {}: key={} put_id=({},{}) operation_id={} err={}",
                reason,
                op.key,
                op.put_id.0,
                op.put_id.1,
                operation_id,
                revoke_err
            );
        }
    }
    inner.finish_owner_remote_put(&op, OwnerRemotePutOutcome::Failed);
}

async fn run_owner_remote_put(
    view: ClientKvApiView,
    op: Arc<OwnerRemotePutSharedOp>,
    holder: Arc<UserMemHolder>,
) {
    let inner = view.client_kv_api().inner();
    let src_offset = holder.memory_info().offset;
    let len = holder.get_length() as u64;
    let len_u32 = match u32::try_from(len) {
        Ok(len_u32) => len_u32,
        Err(_) => {
            tracing::warn!(
                "owner remote Put length does not fit u32: key={} put_id=({},{}) len={}",
                op.key,
                op.put_id.0,
                op.put_id.1,
                len
            );
            drop(holder);
            fail_owner_remote_put(&view, op, None, "length does not fit u32").await;
            return;
        }
    };

    let append_start = match start_replica_append_with_retry(inner, &op, len_u32).await {
        Ok(resp) => resp,
        Err(err) => {
            tracing::warn!(
                "owner remote Put start failed: key={} put_id=({},{}) err={}",
                op.key,
                op.put_id.0,
                op.put_id.1,
                err
            );
            drop(holder);
            fail_owner_remote_put(&view, op, None, "start error").await;
            return;
        }
    };
    match append_start.outcome {
        PutAppendStartOutcome::Scheduled => {}
        PutAppendStartOutcome::AlreadySatisfied => {
            drop(holder);
            inner.finish_owner_remote_put(&op, OwnerRemotePutOutcome::AlreadySatisfied);
            return;
        }
        PutAppendStartOutcome::Obsolete => {
            drop(holder);
            inner.finish_owner_remote_put(&op, OwnerRemotePutOutcome::Obsolete);
            return;
        }
        PutAppendStartOutcome::RetryableNoSpace | PutAppendStartOutcome::Unspecified => {
            tracing::debug!(
                outcome = ?append_start.outcome,
                "owner remote Put deferred: key={} put_id=({},{})",
                op.key,
                op.put_id.0,
                op.put_id.1
            );
            drop(holder);
            inner.finish_owner_remote_put(&op, OwnerRemotePutOutcome::Failed);
            return;
        }
    }

    let target = RemotePutTarget {
        operation_id: append_start.operation_id,
        node_id: append_start.node_id,
        target_offset: append_start.target_addr - append_start.target_base_addr,
        target_base_addr: append_start.target_base_addr,
        len: append_start.len,
    };
    if len != target.len {
        tracing::warn!(
            "owner remote Put length mismatch: key={} put_id=({},{}) src_len={} target_len={}",
            op.key,
            op.put_id.0,
            op.put_id.1,
            len,
            target.len
        );
        drop(holder);
        fail_owner_remote_put(&view, op, Some(target.operation_id), "length mismatch").await;
        return;
    }

    inner.record_owner_remote_put_transfer();
    if let Err(err) = inner
        .put_transfer(
            &op.key,
            op.put_id,
            src_offset,
            target.target_offset,
            len,
            Some(target.node_id),
            Some(target.target_base_addr),
        )
        .await
    {
        tracing::warn!(
            "owner remote Put transfer failed: key={} put_id=({},{}) err={}",
            op.key,
            op.put_id.0,
            op.put_id.1,
            err
        );
        drop(holder);
        fail_owner_remote_put(&view, op, Some(target.operation_id), "transfer error").await;
        return;
    }

    // The payload is no longer read after transfer.  Keep the generation
    // flight installed through Done so eviction cannot cross the completion
    // protocol even though the physical source holder is now releasable.
    drop(holder);
    match inner
        .put_append_done(&op.key, op.put_id, target.operation_id)
        .await
    {
        Ok(resp) => {
            let outcome = if resp.appended {
                OwnerRemotePutOutcome::Published
            } else {
                OwnerRemotePutOutcome::AlreadySatisfied
            };
            tracing::debug!(
                "owner remote Put done: key={} put_id=({},{}) appended={}",
                op.key,
                op.put_id.0,
                op.put_id.1,
                resp.appended
            );
            inner.finish_owner_remote_put(&op, outcome);
        }
        Err(err) => {
            tracing::warn!(
                "owner remote Put done failed: key={} put_id=({},{}) err={}",
                op.key,
                op.put_id.0,
                op.put_id.1,
                err
            );
            fail_owner_remote_put(&view, op, Some(target.operation_id), "done error").await;
        }
    }
}

pub async fn handle_batch_enqueue_replica_tasks(
    view: &ClientKvApiView,
    req: MsgPack<BatchEnqueueReplicaTaskReq>,
) -> MsgPack<BatchEnqueueReplicaTaskResp> {
    let inner = view.client_kv_api().inner();
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        let accepted = match inner
            .ensure_remote_put(&item.key, item.put_id, None, false)
            .await
        {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!(
                    "tier1 write-back enqueue failed: key={} put_id=({},{}) err={}",
                    item.key,
                    item.put_id.0,
                    item.put_id.1,
                    err
                );
                false
            }
        };
        items.push(EnqueueReplicaTaskItemResp {
            key: item.key,
            put_id: item.put_id,
            accepted,
        });
    }
    MsgPack {
        serialize_part: BatchEnqueueReplicaTaskResp {
            items,
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

pub fn spawn_owner_local_publish_dispatcher(
    view: ClientKvApiView,
    mut rx: tokio::sync::ampsc::Receiver<OwnerLocalPublishJob>,
    max_inflight: usize,
) {
    let view_task = view.clone();
    let _ = view.spawn("owner_local_publish_dispatcher", async move {
        let max_inflight = max_inflight.max(1);
        let semaphore = Arc::new(::tokio::sync::Semaphore::new(max_inflight));
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let job = tokio::select! {
                _ = shutdown_waiter.wait() => {
                    tracing::info!("owner_local_publish_dispatcher stopping due to shutdown signal");
                    break;
                }
                job = rx.recv() => {
                    match job {
                        Some(job) => job,
                        None => break,
                    }
                }
            };
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(err) => {
                    tracing::warn!(
                        "owner_local_publish_dispatcher semaphore closed; dropping job key_count={} err={}",
                        job.items.len(),
                        err
                    );
                    break;
                }
            };
            let spawn_view = view_task.clone();
            let worker_view = view_task.clone();
            spawn_view.spawn("owner_local_publish_worker", async move {
                let _permit = permit;
                publish_owner_local_job(worker_view, job).await;
            });
        }
    });
}

/// Returns the linear group partition only when every multi-key group is present,
/// contiguous, and in the caller-declared member order. A partial batch must use
/// the legacy per-item descriptors so the V2 wire format never changes semantics.
fn complete_owner_local_publish_group_lens(items: &[OwnerLocalPublishItem]) -> Option<Vec<usize>> {
    let mut offset = 0usize;
    let mut group_lens = Vec::new();
    while offset < items.len() {
        let item = &items[offset];
        let Some(group) = item.atomic_group.as_ref() else {
            group_lens.push(1);
            offset += 1;
            continue;
        };
        let group_len = group.members.len();
        if group_len < 2 {
            return None;
        }
        let end = offset.checked_add(group_len)?;
        let group_items = items.get(offset..end)?;
        if group_items
            .iter()
            .zip(group.members.iter())
            .any(|(group_item, member)| {
                group_item.key != member.key
                    || group_item.put_id != member.put_id
                    || group_item.atomic_group.as_ref() != Some(group)
            })
        {
            return None;
        }
        group_lens.push(group_len);
        offset = end;
    }
    Some(group_lens)
}

fn owner_local_publish_atomic_batch_complete(
    item: &OwnerLocalPublishItem,
    promoted_items: &[&OwnerLocalPublishItem],
) -> bool {
    item.atomic_group.as_ref().map_or(true, |group| {
        group.members.iter().all(|member| {
            promoted_items
                .iter()
                .any(|published| published.key == member.key && published.put_id == member.put_id)
        })
    })
}

pub(crate) async fn publish_owner_local_job(view: ClientKvApiView, job: OwnerLocalPublishJob) {
    let inner = view.client_kv_api().inner();
    if job.items.is_empty() {
        release_owner_local_publish_reservations(inner, job.key_reservation_ids).await;
        return;
    }

    let group_lens = complete_owner_local_publish_group_lens(&job.items);
    let incomplete_declared_group =
        group_lens.is_none() && job.items.iter().any(|item| item.atomic_group.is_some());
    let mut shutdown_waiter = view.register_shutdown_waiter();
    let mut retry_delay = OWNER_LOCAL_PUBLISH_RETRY_INITIAL;
    let mut published = false;

    loop {
        let attempt: Result<(), String> = async {
            if incomplete_declared_group {
                return Err(
                    "declared atomic group is incomplete or non-contiguous in publish job"
                        .to_string(),
                );
            }
            let done_items: Vec<BatchPutDoneItemResp> =
                if let Some(atomic_group_lens) = group_lens.clone() {
                    inner
                        .owner_hot_counters
                        .grouped_put_done_batches
                        .fetch_add(1, Ordering::Relaxed);
                    inner
                        .owner_hot_counters
                        .grouped_put_done_items
                        .fetch_add(job.items.len() as u64, Ordering::Relaxed);
                    let items = job
                        .items
                        .iter()
                        .map(|item| GroupedBatchPutDoneItemReq {
                            key: item.key.clone(),
                            put_id: item.put_id,
                            lease_id: item.lease_id,
                            committed_slot: Some(item.committed_slot.clone()),
                            publish_local_cache: false,
                        })
                        .collect::<Vec<_>>();
                    inner
                        .grouped_batch_put_done(items, atomic_group_lens)
                        .await
                        .map(|resp| resp.items)
                        .map_err(|err| format!("PutDone RPC uncertain: {err}"))?
                } else {
                    inner
                        .owner_hot_counters
                        .legacy_put_done_batches
                        .fetch_add(1, Ordering::Relaxed);
                    inner
                        .owner_hot_counters
                        .legacy_put_done_items
                        .fetch_add(job.items.len() as u64, Ordering::Relaxed);
                    let items = job
                        .items
                        .iter()
                        .map(|item| BatchPutDoneItemReq {
                            key: item.key.clone(),
                            put_id: item.put_id,
                            lease_id: item.lease_id,
                            committed_slot: Some(item.committed_slot.clone()),
                            publish_local_cache: false,
                            atomic_group: item.atomic_group.clone(),
                        })
                        .collect::<Vec<_>>();
                    inner
                        .batch_put_done(items)
                        .await
                        .map(|resp| resp.items)
                        .map_err(|err| format!("PutDone RPC uncertain: {err}"))?
                };

            if done_items.len() != job.items.len() {
                return Err(format!(
                    "PutDone response length mismatch: expected={} got={}",
                    job.items.len(),
                    done_items.len()
                ));
            }
            for (item, done_item) in job.items.iter().zip(done_items.iter()) {
                if done_item.key != item.key || done_item.put_id != item.put_id {
                    return Err(format!(
                        "PutDone identity mismatch: request=({},({},{}) response=({},({},{}))",
                        item.key,
                        item.put_id.0,
                        item.put_id.1,
                        done_item.key,
                        done_item.put_id.0,
                        done_item.put_id.1,
                    ));
                }
                crate::rpcresp_kvresult_convert::try_from_code(
                    done_item.error_code,
                    done_item.error_json.clone(),
                )
                .map_err(|err| {
                    format!(
                        "PutDone item unresolved: key={} put_id=({},{}) err={}",
                        item.key, item.put_id.0, item.put_id.1, err
                    )
                })?;
            }

            // Do not expose only part of an atomic/TP atomic_batch to owner-hot.
            // First prove every master route terminal, then roll all local
            // precommit slots forward. Replayed attempts accept members that
            // were already promoted before a cancellation or partial local
            // failure.
            for item in &job.items {
                if inner.committed_local_reserve_slot_is_current(
                    &item.key,
                    item.put_id,
                    &item.committed_slot,
                ) {
                    continue;
                }
                let memory_info = inner
                    .precommit_local_visible_memory_info(&item.key)
                    .ok_or_else(|| {
                        format!(
                            "precommit slot missing before promotion: key={} put_id=({},{})",
                            item.key, item.put_id.0, item.put_id.1
                        )
                    })?;
                inner
                    .promote_precommit_local_reserve_resident_slot_if_same(
                        &item.key,
                        item.put_id,
                        memory_info,
                        item.atomic_group.as_ref(),
                    )
                    .map_err(|err| {
                        format!(
                            "local promotion unresolved: key={} put_id=({},{}) err={}",
                            item.key, item.put_id.0, item.put_id.1, err
                        )
                    })?;
            }
            Ok(())
        }
        .await;

        match attempt {
            Ok(()) => {
                published = true;
                break;
            }
            Err(reason) => {
                tracing::warn!(
                    "owner local publish retained full atomic_batch for retry: key_count={} external_fences={} retry_ms={} reason={}",
                    job.items.len(),
                    job.external_pending_contexts.len(),
                    retry_delay.as_millis(),
                    reason,
                );
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(retry_delay) => {}
            _ = shutdown_waiter.wait() => break,
        }
        retry_delay = retry_delay
            .saturating_mul(2)
            .min(OWNER_LOCAL_PUBLISH_RETRY_MAX);
    }

    if published {
        let promoted_items = job.items.iter().collect::<Vec<_>>();
        for item in &promoted_items {
            if !owner_local_publish_atomic_batch_complete(item, &promoted_items) {
                tracing::warn!(
                    "owner local publish skipped hot admission for incomplete atomic group: key={} put_id=({},{})",
                    item.key,
                    item.put_id.0,
                    item.put_id.1
                );
                continue;
            }
            let _ = inner.owner_hot_admit_published_committed(&item.key, item.put_id);
        }

        for item in promoted_items {
            if item.make_replica_task {
                if let Err(err) = inner
                    .ensure_remote_put(
                        &item.key,
                        item.put_id,
                        item.preferred_sub_cluster.clone(),
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        "owner local publish enqueue replica append failed: key={} put_id=({},{}) err={}",
                        item.key,
                        item.put_id.0,
                        item.put_id.1,
                        err
                    );
                }
            }
        }

        for context in &job.external_pending_contexts {
            context._pending_fence.mark_local_put_succeeded();
        }
    }

    release_owner_local_publish_reservations(inner, job.key_reservation_ids).await;
}

async fn release_owner_local_publish_reservations(
    inner: &ClientKvApiInner,
    key_reservation_ids: Vec<u64>,
) {
    if let Err(err) = inner
        .batch_release_put_key_reservations(key_reservation_ids)
        .await
    {
        tracing::warn!(
            "owner local publish key reservation cleanup failed: {}",
            err
        );
    }
}
