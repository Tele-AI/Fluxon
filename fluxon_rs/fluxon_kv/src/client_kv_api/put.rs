use super::external_api::HandlerForExternalClient;
use super::msg_pack::{
    ExternalBatchPutStartItemReq, ExternalBatchPutStartReq, ExternalPutRevokeReq,
};
use super::{
    ClientKvApiInner, OwnerHotEvictionEvent, OwnerHotEvictionPreparation,
    OwnerHotSelectionFenceOutcome, OwnerLocalSsdPutOutcome, OwnerLocalSsdPutReservation,
    OwnerLocalSsdPutSharedOp, OwnerPressureBatchReclaim, OwnerRemotePutAdmissionPermit,
    OwnerRemotePutOutcome, OwnerRemotePutReservation, OwnerRemotePutSharedOp,
    OwnerSegmentAllocator, OwnerSlotLease, OwnerSlotRef,
    local_reserve_rebalance::{owner_local_reserve_timeout_config, wait_owner_local_reserve_ready},
};
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::master_kv_router::put::PutIDForAKey;
use crate::p2p::control_plane_rpc::{CONTROL_PLANE_RPC_TRANSPORT_POLICY, call_control_plane_rpc};
// no StageScope; timestamps-based metrics only
use crate::memholder::kvclient_encode::{calc_flat_dict_encoded_len, write_flat_dict_ptrs_to_ptr};
use crate::observe_kvope::{obe_put_start_error_rpc, obe_put_start_success};
use crate::owner_segment::{
    OwnerRouteCommitMode, OwnerSegmentTransferItem, OwnerSegmentTransferOutcome, OwnerSlotDesc,
    OwnerSourceReadCapability, OwnerSourceRouteToken, OwnerTargetDisposition,
    OwnerTransferDirection, OwnerTransferErrorCode,
};
use crate::{
    client_kv_api::ClientKvApiView,
    master_kv_router::msg_pack::{
        BatchEnqueueReplicaTaskReq, BatchEnqueueReplicaTaskResp, BatchEvictOwnerSourceReq,
        BatchEvictOwnerSourceResp, BatchPreparePutKeyItemReq, BatchPreparePutKeysReq,
        BatchPreparePutKeysResp, BatchPublishOwnerSsdReq, BatchPublishOwnerSsdResp,
        BatchPutAppendDoneItemReq, BatchPutAppendDoneReq, BatchPutAppendDoneResp,
        BatchPutAppendStartItemReq, BatchPutAppendStartReq, BatchPutAppendStartResp,
        BatchPutDoneItemReq, BatchPutDoneItemResp, BatchPutDoneReq, BatchPutDoneResp,
        BatchPutRevokeItemReq, BatchPutRevokeReq, BatchPutRevokeResp, BatchPutStartItemReq,
        BatchPutStartReq, BatchPutStartResp, BatchReleasePutKeyReservationsReq,
        BatchReleasePutKeyReservationsResp, EnqueueReplicaTaskItemResp, GroupedBatchPutDoneItemReq,
        GroupedBatchPutDoneReq, GroupedBatchPutDoneResp, OwnerReclaimBacking,
        OwnerSourceEvictionOutcome, OwnerSourceEvictionVictim, OwnerSourceEvictionVictimResp,
        OwnerSourceSsdPolicy, OwnerSsdPublishItem, OwnerSsdPublishOutcome, PutAppendDoneReq,
        PutAppendDoneResp, PutAppendRevokeReq, PutAppendStartOutcome, PutAppendStartReq,
        PutAppendStartResp, PutAtomicGroup, PutDoneCommittedSlot, PutDoneReq, PutRevokeReq,
        PutStartReq, PutStartResp, RadixKvMetadata, owner_source_eviction_epoch,
    },
    memholder::{UserMemHolder, UserMemHolderExposeKind},
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use chrono::Utc;
use fluxon_commu::TransferBreakdown;
use limit_thirdparty::tokio;
use std::future::Future;
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

// Bound control-plane head-of-line exposure while retaining one independent
// result per KV. Batches run sequentially so a capacity contraction cannot
// flood the peer with many large reservation requests at once.
const OWNER_SOURCE_EVICTION_RPC_BATCH: usize = 256;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

fn owner_local_reserve_slot_size(value_len: u64) -> KvResult<u64> {
    crate::owner_segment_allocation_capacity_bytes(value_len).ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "value_len={} cannot be represented by the owner segment allocator",
                value_len
            ),
        })
    })
}

fn owner_local_reserve_try_claim(
    pool: &mut OwnerSegmentAllocator,
    slot_size: u64,
    value_len: u64,
    key_count: usize,
) -> Option<OwnerSlotLease> {
    let slots = owner_local_reserve_claim_available(pool, slot_size, value_len, key_count);
    if slots.len() != key_count {
        pool.record_failed_claim(slot_size);
        for slot in &slots {
            assert!(pool.release_prepared_slot(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
        }
        return None;
    }
    Some(OwnerSlotLease {
        value_len,
        slot_size,
        slots,
    })
}

fn owner_local_reserve_claim_available(
    pool: &mut OwnerSegmentAllocator,
    slot_size: u64,
    value_len: u64,
    max_slots: usize,
) -> Vec<OwnerSlotRef> {
    debug_assert_eq!(
        crate::owner_segment_allocation_capacity_bytes(value_len),
        Some(slot_size)
    );
    pool.claim_value(value_len, max_slots)
}

struct OwnerLocalReservePendingDemandGuard<'a> {
    inner: &'a ClientKvApiInner,
    slot_size: u64,
    pending_slots: usize,
}

impl<'a> OwnerLocalReservePendingDemandGuard<'a> {
    fn new(inner: &'a ClientKvApiInner, slot_size: u64, pending_slots: usize) -> Self {
        inner.owner_local_reserve_register_pending_demand(slot_size, pending_slots);
        Self {
            inner,
            slot_size,
            pending_slots,
        }
    }

    fn consume(&mut self) {
        if self.pending_slots == 0 {
            return;
        }
        self.inner
            .owner_local_reserve_consume_pending_demand(self.slot_size, self.pending_slots);
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
        OwnerSegmentAllocator, owner_local_reserve_claim_available, owner_local_reserve_slot_size,
        owner_local_reserve_try_claim,
    };
    use crate::client_kv_api::{ClientKvApi, ClientKvApiNewArg};
    use crate::config::TestSpecConfig;
    use std::sync::Arc;
    use std::time::Duration;

    fn segment_pool(bytes: u64) -> OwnerSegmentAllocator {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_test_segment("put-test-owner", 1_000_000, 1_000_000, bytes, bytes, None)
            .expect("install test owner segment");
        pool
    }

    #[test]
    fn slot_size_uses_page_aligned_exact_fit() {
        const SGLANG_KV_PAGE_BYTES: u64 = 4_718_592;

        let slot_size = owner_local_reserve_slot_size(SGLANG_KV_PAGE_BYTES).unwrap();
        assert_eq!(slot_size, SGLANG_KV_PAGE_BYTES);
        assert_eq!(owner_local_reserve_slot_size(4_097).unwrap(), 8_192);
    }

    #[test]
    fn failed_claim_is_recorded_for_the_exact_size_class() {
        const SGLANG_KV_PAGE_BYTES: u64 = 4_718_592;
        let mut pool = segment_pool(2 * SGLANG_KV_PAGE_BYTES);

        assert!(
            owner_local_reserve_try_claim(
                &mut pool,
                SGLANG_KV_PAGE_BYTES,
                SGLANG_KV_PAGE_BYTES,
                3,
            )
            .is_none()
        );
        assert!(pool.slot_size_has_failed_claim(SGLANG_KV_PAGE_BYTES));
        assert_eq!(pool.total_free_bytes(), 2 * SGLANG_KV_PAGE_BYTES);
    }

    #[test]
    fn partial_claim_keeps_progress_for_large_waiters() {
        const SLOT_SIZE: u64 = 4 * 1024;
        let mut pool = segment_pool(4 * SLOT_SIZE);

        let first = owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOT_SIZE, 3);
        assert_eq!(first.len(), 3);
        assert_eq!(pool.total_free_bytes(), SLOT_SIZE);

        let second = owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOT_SIZE, 3);
        assert_eq!(second.len(), 1);
        assert_eq!(pool.total_free_bytes(), 0);
    }

    #[test]
    fn mixed_sizes_share_one_segment_and_keep_byte_offsets() {
        const KIB: u64 = 1024;
        let mut pool = segment_pool(64 * KIB);

        let first = owner_local_reserve_try_claim(&mut pool, 12 * KIB, 12 * KIB, 1)
            .unwrap()
            .slots
            .pop()
            .unwrap();
        let second = owner_local_reserve_try_claim(&mut pool, 20 * KIB, 20 * KIB, 1)
            .unwrap()
            .slots
            .pop()
            .unwrap();

        assert_eq!(first.segment_offset, 0);
        assert_eq!(second.segment_offset, 12 * KIB);
        assert_eq!(second.addr - first.addr, 12 * KIB);
        assert_eq!(pool.total_used_bytes(), 32 * KIB);
    }

    #[test]
    fn allocatable_report_excludes_free_bubbles_smaller_than_one_slot() {
        const KIB: u64 = 1024;
        let mut pool = segment_pool(64 * KIB);

        let slots = pool.claim_available(16 * KIB, 4);
        assert_eq!(slots.len(), 4);
        for slot in [&slots[1], &slots[3]] {
            assert!(pool.release_prepared_slot(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
        }

        let report = pool.allocatable_report(24 * KIB);
        assert_eq!(report.raw_free_bytes, 32 * KIB);
        assert_eq!(report.allocatable_slots, 0);
        assert_eq!(report.allocatable_bytes, 0);
        assert_eq!(report.slot_unallocatable_bytes, 32 * KIB);
    }

    #[test]
    fn minimum_sized_allocations_can_fill_and_restore_the_segment() {
        const KIB: u64 = 1024;
        let mut pool = segment_pool(64 * KIB);

        let slots = pool.claim_available(4 * KIB, 16);
        assert_eq!(slots.len(), 16);
        assert_eq!(pool.total_free_bytes(), 0);
        for slot in slots {
            assert!(pool.release_prepared_slot(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
        }
        assert_eq!(pool.total_free_bytes(), 64 * KIB);
        assert_eq!(pool.largest_free_bytes(), 64 * KIB);
    }

    #[test]
    fn failed_batch_claim_rolls_back_every_offset_allocation() {
        const KIB: u64 = 1024;
        let mut pool = segment_pool(64 * KIB);

        assert!(owner_local_reserve_try_claim(&mut pool, 24 * KIB, 24 * KIB, 3).is_none());
        assert_eq!(pool.used_slot_count(), 0);
        assert_eq!(pool.total_free_bytes(), 64 * KIB);
        assert_eq!(pool.largest_free_bytes(), 64 * KIB);
        assert!(pool.slot_size_has_failed_claim(24 * KIB));
    }

    #[test]
    fn stale_allocation_id_cannot_release_a_reused_offset() {
        const SLOT_SIZE: u64 = 4 * 1024;
        let mut pool = segment_pool(SLOT_SIZE);
        let old = pool.claim_available(SLOT_SIZE, 1).pop().unwrap();
        assert!(pool.release_prepared_slot(
            old.allocation_id,
            old.segment_offset,
            old.capacity_bytes,
        ));
        let current = pool.claim_available(SLOT_SIZE, 1).pop().unwrap();
        assert_eq!(current.segment_offset, old.segment_offset);
        assert_ne!(current.allocation_id, old.allocation_id);
        assert!(!pool.release_prepared_slot(
            old.allocation_id,
            old.segment_offset,
            old.capacity_bytes,
        ));
        assert_eq!(pool.prepared_slot_count(), 1);
    }

    #[test]
    fn committed_slots_are_reclaimed_and_reused_independently_in_one_segment() {
        const SLOT_SIZE: u64 = 4 * 1024;
        let mut pool = segment_pool(SLOT_SIZE * 8);

        let initial = owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOT_SIZE, 8);
        assert_eq!(initial.len(), 8);
        for slot in &initial {
            assert!(pool.mark_prepared_slot_pending_visible(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
            assert!(pool.retain_resident_slot_holder(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
            assert!(pool.promote_pending_visible_slot_to_committed(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
        }

        let victims = [initial[1].clone(), initial[5].clone()];
        for victim in &victims {
            assert!(pool.release_committed_slot_route(
                victim.allocation_id,
                victim.segment_offset,
                victim.capacity_bytes,
            ));
            assert!(pool.release_resident_slot_holder(
                victim.allocation_id,
                victim.segment_offset,
                victim.capacity_bytes,
            ));
        }
        assert_eq!(pool.total_free_bytes(), SLOT_SIZE * 2);

        for cycle in 0..10 {
            let reused = owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOT_SIZE, 2);
            assert_eq!(reused.len(), 2, "cycle {cycle} did not reuse both slots");

            for slot in reused {
                assert!(pool.mark_prepared_slot_pending_visible(
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                ));
                assert!(pool.retain_resident_slot_holder(
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                ));
                assert!(pool.promote_pending_visible_slot_to_committed(
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                ));
                assert!(pool.release_committed_slot_route(
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                ));
                assert!(pool.release_resident_slot_holder(
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                ));
            }
            assert_eq!(pool.total_free_bytes(), SLOT_SIZE * 2);
        }
    }

    #[limit_thirdparty::tokio::test]
    async fn later_waiter_cannot_steal_slot_before_current_claim_turn_completes() {
        const SLOT_SIZE: u64 = 4 * 1024;

        let api = Arc::new(
            ClientKvApi::construct(ClientKvApiNewArg {
                test_spec_config: TestSpecConfig::default(),
                owner_hot_cache_capacity_bytes: None,
                owner_local_reserve_physical_capacity_bytes: 0,
                allocation_authority:
                    crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
                owner_placement_class:
                    crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
                ssd_storage: None,
            })
            .await
            .expect("construct test ClientKvApi"),
        );
        {
            let mut pool = api.inner().owner_segment_allocator.lock();
            pool.install_test_segment(
                "put-test-owner",
                1000,
                1000,
                SLOT_SIZE * 8,
                SLOT_SIZE * 8,
                None,
            )
            .unwrap();
        }

        // Model a five-slot waiter that has made partial progress while owning the claim turn.
        let claim_lock = api.inner().owner_local_reserve_claim_lock(SLOT_SIZE);
        let current_claim_turn = claim_lock.lock().await;
        let first_partial = {
            let mut pool = api.inner().owner_segment_allocator.lock();
            owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOT_SIZE, 3)
        };
        assert_eq!(first_partial.len(), 3);

        // A later one-slot request must queue even though physical slots are free.
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
                .owner_segment_allocator
                .lock()
                .total_free_bytes(),
            SLOT_SIZE * 5,
            "later waiter stole the current waiter's remaining free slot"
        );

        // The current waiter reaches all five slots before handing off the turn.
        let first_remainder = {
            let mut pool = api.inner().owner_segment_allocator.lock();
            owner_local_reserve_claim_available(&mut pool, SLOT_SIZE, SLOT_SIZE, 2)
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
                owner_local_reserve_physical_capacity_bytes: 0,
                allocation_authority:
                    crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
                owner_placement_class:
                    crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
                ssd_storage: None,
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

        let api = Arc::new(
            ClientKvApi::construct(ClientKvApiNewArg {
                test_spec_config: TestSpecConfig::default(),
                owner_hot_cache_capacity_bytes: None,
                owner_local_reserve_physical_capacity_bytes: 0,
                allocation_authority:
                    crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
                owner_placement_class:
                    crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
                ssd_storage: None,
            })
            .await
            .expect("construct test ClientKvApi"),
        );
        let blocked_lock = api
            .inner()
            .owner_local_reserve_claim_lock(BLOCKED_SLOT_SIZE);
        let _blocked_turn = blocked_lock.lock().await;
        {
            let mut pool = api.inner().owner_segment_allocator.lock();
            pool.install_test_segment(
                "put-test-owner",
                1000,
                1000,
                READY_SLOT_SIZE * 2,
                READY_SLOT_SIZE * 2,
                None,
            )
            .unwrap();
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
                owner_local_reserve_physical_capacity_bytes: 0,
                allocation_authority:
                    crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
                owner_placement_class:
                    crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
                ssd_storage: None,
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
                    .owner_segment_allocator
                    .lock()
                    .pending_demand_slots(SLOT_SIZE);
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
                .owner_segment_allocator
                .lock()
                .pending_demand_slots(SLOT_SIZE),
            0,
            "cancelled queued claims leaked pending demand"
        );
        drop(claim_turn);
    }
}

#[derive(Debug)]
pub(super) struct NativePutOperationScope {
    state: parking_lot::Mutex<NativePutOperationState>,
    changed: tokio::sync::Notify,
}

#[derive(Debug)]
struct NativePutOperationState {
    accepting: bool,
    active: usize,
    cleanup_error: Option<String>,
}

impl Default for NativePutOperationScope {
    fn default() -> Self {
        Self {
            state: parking_lot::Mutex::new(NativePutOperationState {
                accepting: true,
                active: 0,
                cleanup_error: None,
            }),
            changed: tokio::sync::Notify::new(),
        }
    }
}

impl NativePutOperationScope {
    fn admit(self: &Arc<Self>) -> KvResult<Arc<NativePutOperationGuard>> {
        let mut state = self.state.lock();
        if !state.accepting {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "native Put admission is closed".to_string(),
            }));
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("native Put count overflow");
        Ok(Arc::new(NativePutOperationGuard {
            scope: self.clone(),
        }))
    }

    pub(super) fn close_admission(&self) {
        self.state.lock().accepting = false;
    }

    fn record_cleanup_error(&self, error: &KvError) {
        self.state
            .lock()
            .cleanup_error
            .get_or_insert_with(|| error.to_string());
    }

    pub(super) async fn wait_finished(&self) -> KvResult<()> {
        fluxon_util::notify_state::wait_until(&self.changed, || self.state.lock().active == 0)
            .await;
        match self.state.lock().cleanup_error.as_ref() {
            Some(error) => Err(KvError::Api(ApiError::Unknown {
                detail: format!("native Put cleanup did not complete: {error}"),
            })),
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
struct NativePutOperationGuard {
    scope: Arc<NativePutOperationScope>,
}

impl Drop for NativePutOperationGuard {
    fn drop(&mut self) {
        let mut state = self.scope.state.lock();
        state.active = state
            .active
            .checked_sub(1)
            .expect("native Put count underflow");
        drop(state);
        self.scope.changed.notify_waiters();
    }
}

#[derive(Debug)]
pub struct OwnerReservedPutItem {
    // Retain the operation across delivery and terminal cleanup without a zero-count gap.
    _operation: Arc<NativePutOperationGuard>,
    pub key: String,
    /// Master metadata reservation; physical storage is owned by the local pending context.
    pub key_reservation_id: u64,
    pub put_id: PutIDForAKey,
    pub src_addr: u64,
    pub src_base_addr: u64,
    pub target_addr: u64,
    pub target_base_addr: u64,
    pub value_len: u64,
    pub lease_id: Option<u64>,
    pub peer_node_id: Option<NodeIDString>,
    pub remember_local_snapshot: bool,
    /// Original caller/content selection, independent of remote capacity.
    pub make_replica_task: bool,
    /// The Owner may start replica placement after the local route is published.
    pub remote_replica_admitted: bool,
    pub preferred_sub_cluster: Option<String>,
}

struct NativePutReservationDelivery {
    view: ClientKvApiView,
    items: Vec<OwnerReservedPutItem>,
}

impl Drop for NativePutReservationDelivery {
    fn drop(&mut self) {
        let items = std::mem::take(&mut self.items);
        if items.is_empty() {
            return;
        }
        let task_view = self.view.clone();
        self.view
            .spawn("native_put_undelivered_cleanup", async move {
                if let Err(error) = task_view
                    .client_kv_api()
                    .inner()
                    .owner_batch_put_abort_reserved(items)
                    .await
                {
                    tracing::error!(%error, "undelivered native Put reservation cleanup failed");
                }
            });
    }
}

#[derive(Debug, Clone)]
pub struct OwnerLocalPublishItem {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub value_len: u64,
    pub lease_id: Option<u64>,
    pub committed_slot: PutDoneCommittedSlot,
    /// Original caller/content selection, independent of remote capacity.
    pub make_replica_task: bool,
    /// This Put path admitted a remote trigger. Pre-reserved paths set this
    /// only when a target exists; owner-local fast Put admits selected items
    /// here and performs allocation inside the later append Start.
    pub remote_replica_admitted: bool,
    pub preferred_sub_cluster: Option<String>,
    pub atomic_group: Option<PutAtomicGroup>,
    pub radix: Option<RadixKvMetadata>,
}

#[derive(Debug, Clone)]
pub struct OwnerLocalPublishJob {
    pub items: Vec<OwnerLocalPublishItem>,
    pub key_reservation_ids: Vec<u64>,
    /// Native and external requests retain their owner reclaim fences until
    /// master publication and local promotion complete. Direct PyO3 slot
    /// reservations use master key reservations and leave this empty.
    pub external_pending_contexts: Vec<super::ExternalPendingPutCtx>,
}

fn owner_local_publish_starts_remote(item: &OwnerLocalPublishItem) -> bool {
    item.make_replica_task && item.remote_replica_admitted
}

#[derive(Clone)]
struct OwnerLocalSsdPutCandidate {
    key: String,
    put_id: PutIDForAKey,
    /// Exact logical value bytes known by the publishing Put path. Early SSD
    /// writes use this to reject over-budget candidates before installing a
    /// generation flight or acquiring a source holder. A zero value is kept
    /// only for legacy allocation-backed last-backing candidates whose
    /// resource admission already happened upstream.
    value_len: u64,
    selected_victim: Option<Arc<OwnerSourceEvictionVictim>>,
}

struct OwnerLocalSsdPutLeader {
    op: Arc<OwnerLocalSsdPutSharedOp>,
    memory_info: Arc<crate::memholder::MemoryInfo>,
    /// Bytes actually charged by the caller's admission pass. This can be
    /// zero for a legacy allocation-backed last-backing candidate, so gate
    /// failure must refund this value rather than minting credit from the
    /// subsequently discovered source length.
    admission_len: u64,
}

fn owner_reclaim_backing_len(backing: &OwnerReclaimBacking) -> u64 {
    match backing {
        OwnerReclaimBacking::CommittedSlot { capacity_bytes, .. } => *capacity_bytes,
        OwnerReclaimBacking::Allocation | OwnerReclaimBacking::UnindexedAllocation { .. } => 0,
    }
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
    if !inner.view.register_shutdown_poller().is_running() {
        return KvError::Api(ApiError::SystemShutdown {
            detail: "ClientKvApi is shutting down while waiting for owner segment capacity"
                .to_string(),
        });
    }
    let (
        used_slots,
        free_bytes,
        allocatable_slots,
        slot_unallocatable_bytes,
        pending_slots,
        segment_bytes,
    ) = {
        let pool = inner.owner_segment_allocator.lock();
        let allocatable = pool.allocatable_report(slot_size);
        (
            pool.used_slot_count(),
            pool.total_free_bytes(),
            allocatable.allocatable_slots,
            allocatable.slot_unallocatable_bytes,
            pool.pending_demand_slots(slot_size),
            pool.physical_capacity_bytes(),
        )
    };
    tracing::warn!(
        stage,
        slot_size,
        key_count,
        used_allocations = used_slots,
        raw_free_bytes = free_bytes,
        allocatable_slots,
        slot_unallocatable_bytes,
        pending_slots,
        segment_bytes,
        waited_ms = request_started_at.elapsed().as_millis(),
        soft_wait_timeout_ms = soft_wait_timeout.as_millis(),
        hard_timeout_ms = hard_wait_timeout.as_millis(),
        "owner segment allocation timeout"
    );
    KvError::Api(ApiError::NoSpace {
        node: inner.client_id_str(),
        segment: "owner-local-reserve".to_string(),
        total_capacity: segment_bytes,
        free_capacity: allocatable_slots.saturating_mul(slot_size),
    })
}

fn log_owner_local_reserve_claim_lifecycle(
    outcome: &'static str,
    stage: &'static str,
    value_len: u64,
    slot_size: u64,
    key_count: usize,
    claim_attempts: u64,
    claim_turn_wait_us: i64,
    refill_wait_us: i64,
    total_us: i64,
) {
    tracing::info!(
        outcome,
        stage,
        value_len,
        slot_size,
        key_count,
        claim_attempts,
        claim_turn_wait_us,
        refill_wait_us,
        total_us,
        "owner local reserve claim lifecycle"
    );
}

/// Ephemeral result of entering the existing owner-side remote Put singleflight.
/// This does not add a second state machine: the shared operation remains the
/// only generation terminal and only its leader owns the transfer task.
enum OwnerRemotePutStart {
    LeaderStarted,
    Follower(Arc<OwnerRemotePutSharedOp>),
    Rejected,
}

impl ClientKvApiInner {
    pub(crate) async fn owner_segment_transfer(
        &self,
        target: &crate::owner_segment::OwnerGeneration,
        request: crate::owner_segment::OwnerSegmentTransferReq,
    ) -> KvResult<Vec<crate::owner_segment::OwnerSegmentTransferItemResp>> {
        if request.items.is_empty() {
            return Ok(Vec::new());
        }
        let self_info = self.view.cluster_manager().get_self_info();
        let expected = request
            .items
            .iter()
            .map(|item| (item.terminal_sequence, item.item.op_id().cloned()))
            .collect::<Vec<_>>();
        if target.node_id == self_info.id && target.node_start_time == self_info.node_start_time {
            let caller: NodeID = self_info.id.clone().into();
            let response = super::handle_owner_segment_transfer(
                &self.view,
                caller,
                MsgPack {
                    serialize_part: request,
                    raw_bytes: Vec::new(),
                },
            )
            .await;
            crate::rpcresp_kvresult_convert::try_from_code(
                response.serialize_part.error_code,
                response.serialize_part.error_json.clone(),
            )?;
            return Ok(response.serialize_part.items);
        }
        let current = self
            .view
            .cluster_manager()
            .get_member_info_cached(&target.node_id)
            .ok_or_else(|| {
                KvError::Api(ApiError::NodeNotFound {
                    desc: target.node_id.clone(),
                })
            })?;
        if current.node_start_time != target.node_start_time {
            return Err(KvError::Api(ApiError::OwnerStartTimeMismatch {
                expected: target.node_start_time,
                got: current.node_start_time,
            }));
        }
        let response = call_control_plane_rpc(
            &self.rpc_caller_owner_segment_transfer,
            self.view.p2p_module(),
            target.node_id.clone().into(),
            MsgPack {
                serialize_part: request,
                raw_bytes: Vec::new(),
            },
            Some(Duration::from_secs(60)),
            2,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            response.serialize_part.error_code,
            response.serialize_part.error_json.clone(),
        )?;
        if response.serialize_part.items.len() != expected.len()
            || response
                .serialize_part
                .items
                .iter()
                .zip(expected)
                .any(|(item, expected)| {
                    item.terminal_sequence != expected.0 || item.op_id != expected.1
                })
        {
            return Err(KvError::Api(ApiError::Unknown {
                detail: "owner segment transfer response changed batch order or operation identity"
                    .to_string(),
            }));
        }
        Ok(response.serialize_part.items)
    }

    pub async fn owner_claim_local_reserve_slot_lease(
        &self,
        value_len: u64,
        key_count: usize,
    ) -> KvResult<OwnerSlotLease> {
        let slot_size = owner_local_reserve_slot_size(value_len)?;
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
            OwnerLocalReservePendingDemandGuard::new(self, slot_size, key_count);
        self.owner_local_reserve_rebalance_notify().notify_waiters();

        let Some(remaining_for_turn) = hard_deadline.checked_duration_since(Instant::now()) else {
            let total_us = duration_to_i64_us(request_started_at.elapsed());
            log_owner_local_reserve_claim_lifecycle(
                "timeout",
                "claim_turn",
                value_len,
                slot_size,
                key_count,
                0,
                total_us,
                0,
                total_us,
            );
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
                let total_us = duration_to_i64_us(request_started_at.elapsed());
                log_owner_local_reserve_claim_lifecycle(
                    "timeout",
                    "claim_turn",
                    value_len,
                    slot_size,
                    key_count,
                    0,
                    total_us,
                    0,
                    total_us,
                );
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
        let claim_turn_wait_us = duration_to_i64_us(request_started_at.elapsed());
        let mut claim_attempts = 0_u64;

        loop {
            claim_attempts = claim_attempts.saturating_add(1);
            let claim = {
                let mut pool = self.owner_segment_allocator.lock();
                let claim =
                    owner_local_reserve_try_claim(&mut pool, slot_size, value_len, key_count);
                if claim.is_some() {
                    pool.record_successful_claim(slot_size);
                    let pending = pool
                        .pending_demand_by_slot_size
                        .get_mut(&slot_size)
                        .expect("claimed local-reserve demand must exist");
                    *pending = pending
                        .checked_sub(key_count)
                        .expect("claimed local-reserve demand underflow");
                    if *pending == 0 {
                        pool.pending_demand_by_slot_size.remove(&slot_size);
                    }
                }
                claim
            };
            if let Some(lease) = claim {
                pending_demand.disarm_after_locked_consume();
                let total_us = duration_to_i64_us(request_started_at.elapsed());
                log_owner_local_reserve_claim_lifecycle(
                    "success",
                    "complete",
                    value_len,
                    slot_size,
                    key_count,
                    claim_attempts,
                    claim_turn_wait_us,
                    total_us.saturating_sub(claim_turn_wait_us),
                    total_us,
                );
                return Ok(lease);
            }
            self.owner_local_reserve_rebalance_notify().notify_waiters();
            if !wait_owner_local_reserve_ready(
                self,
                slot_size,
                key_count,
                soft_wait_timeout,
                hard_deadline,
            )
            .await
            {
                break;
            }
        }
        let total_us = duration_to_i64_us(request_started_at.elapsed());
        log_owner_local_reserve_claim_lifecycle(
            "timeout",
            "refill",
            value_len,
            slot_size,
            key_count,
            claim_attempts,
            claim_turn_wait_us,
            total_us.saturating_sub(claim_turn_wait_us),
            total_us,
        );
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
        lease: OwnerSlotLease,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_segment_allocator.lock();
            for slot_ref in &lease.slots {
                if !pool.release_prepared_slot(
                    slot_ref.allocation_id,
                    slot_ref.segment_offset,
                    slot_ref.capacity_bytes,
                ) {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "owner allocation identity mismatch while releasing slot lease: allocation_id={}",
                            slot_ref.allocation_id
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
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        let mut pool = self.owner_segment_allocator.lock();
        if !pool.mark_prepared_slot_pending_visible(allocation_id, segment_offset, capacity_bytes) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner allocation identity mismatch while marking pending slot: allocation_id={}",
                    allocation_id
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_promote_local_reserve_pending_slot_to_committed(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        let mut pool = self.owner_segment_allocator.lock();
        if !pool.promote_pending_visible_slot_to_committed(
            allocation_id,
            segment_offset,
            capacity_bytes,
        ) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner allocation identity mismatch while committing slot: allocation_id={}",
                    allocation_id
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_retain_local_reserve_resident_slot_holder(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        let mut pool = self.owner_segment_allocator.lock();
        if !pool.retain_resident_slot_holder(allocation_id, segment_offset, capacity_bytes) {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner allocation identity mismatch while retaining holder: allocation_id={}",
                    allocation_id
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_retain_global_shared_slot_holder(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        let mut allocator = self.owner_segment_allocator.lock();
        if !allocator.retain_committed_route_only_holder(
            allocation_id,
            segment_offset,
            capacity_bytes,
        ) {
            return Err(KvError::Api(ApiError::StaleGetPlan {
                get_id,
                key: key.to_string(),
                detail: format!(
                    "GlobalShared owner slot is no longer route-only: allocation_id={} offset={} capacity={} put_id=({},{})",
                    allocation_id, segment_offset, capacity_bytes, put_id.0, put_id.1,
                ),
            }));
        }
        Ok(())
    }

    pub fn owner_release_local_reserve_resident_slot_holder(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_segment_allocator.lock();
            if !pool.release_resident_slot_holder(allocation_id, segment_offset, capacity_bytes) {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "owner allocation identity mismatch while releasing holder: allocation_id={}",
                        allocation_id
                    ),
                }));
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub fn owner_release_local_reserve_committed_slot_route(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_segment_allocator.lock();
            if !pool.release_committed_slot_route(allocation_id, segment_offset, capacity_bytes) {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "owner allocation identity mismatch while releasing route: allocation_id={}",
                        allocation_id
                    ),
                }));
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub fn owner_release_local_reserve_committed_resident_slot(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<()> {
        {
            let mut pool = self.owner_segment_allocator.lock();
            if !pool.release_committed_resident_slot(allocation_id, segment_offset, capacity_bytes)
            {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "owner allocation identity mismatch while reclaiming slot: allocation_id={}",
                        allocation_id
                    ),
                }));
            }
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub async fn owner_shutdown_local_reserve_pool(&self) -> KvResult<()> {
        let pool = self.owner_segment_allocator.lock();
        if pool.used_slot_count() != 0 {
            tracing::warn!(
                live_allocations = pool.used_slot_count(),
                used_bytes = pool.total_used_bytes(),
                "owner segment allocator is shutting down with live allocations"
            );
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
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting native Put reservation"
                    .to_string(),
            }));
        }
        let mut keys = std::collections::HashSet::with_capacity(start_items.len());
        for item in &start_items {
            if item.len > u64::from(u32::MAX) || !keys.insert(item.key.clone()) {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: "native Put reservation requires distinct keys and lengths fitting u32"
                        .to_string(),
                }));
            }
        }

        // The owner finishes admission or rolls it back even if the caller is cancelled.
        let operation = self.native_put_operations.admit()?;
        let view = self.view.clone_view();
        let task_view = view.clone();
        let (mut done_tx, done_rx) = ::tokio::sync::oneshot::channel();
        view.spawn("native_put_reserve", async move {
            let inner = task_view.client_kv_api().inner();
            let mut shutdown_waiter = task_view.register_shutdown_waiter();
            let mut reserved_items = Vec::new();
            let mut unassigned_reservations = std::collections::HashSet::new();
            let result = async {
                let prepared = inner
                    .batch_prepare_put_keys(
                        start_items
                            .iter()
                            .map(|item| BatchPreparePutKeyItemReq {
                                key: item.key.clone(),
                                reject_if_inflight_same_key: item.reject_if_inflight_same_key,
                                reject_if_exist_same_key: item.reject_if_exist_same_key,
                            })
                            .collect(),
                    )
                    .await?;
                unassigned_reservations.extend(prepared.reservation_ids.iter().copied());
                if prepared.reservation_ids.len() != start_items.len() {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: "native Put key reservation response length mismatch".to_string(),
                    }));
                }
                let mut ordered_items = start_items
                    .into_iter()
                    .zip(prepared.reservation_ids)
                    .enumerate()
                    .map(|(index, (item, reservation_id))| (index, item, reservation_id))
                    .collect::<Vec<_>>();
                // Keep one key order across length groups: a later group waits while
                // earlier groups retain their fences. Length-first ordering can deadlock
                // overlapping batches whose values have different sizes.
                ordered_items.sort_unstable_by(|(_, left, _), (_, right, _)| {
                    left.key.cmp(&right.key)
                });
                let mut groups: Vec<Vec<(usize, BatchPutStartItemReq, u64)>> = Vec::new();
                for item in ordered_items {
                    if let Some(group) = groups.last_mut().filter(|group| group[0].1.len == item.1.len) {
                        group.push(item);
                    } else {
                        groups.push(vec![item]);
                    }
                }
                let started_time = inner.view.cluster_manager().get_self_info().node_start_time;
                for group in groups {
                    let admission = task_view
                        .client_kv_api()
                        .external_batch_put_start(ExternalBatchPutStartReq {
                            items: group
                                .iter()
                                .map(|(_, item, _)| ExternalBatchPutStartItemReq {
                                    key: item.key.clone(),
                                    len: item.len,
                                    reject_if_inflight_same_key: item.reject_if_inflight_same_key,
                                    reject_if_exist_same_key: item.reject_if_exist_same_key,
                                    make_replica_task: item.make_replica_task,
                                    preferred_sub_cluster: item.preferred_sub_cluster.clone(),
                                    radix: None,
                                })
                                .collect(),
                            atomic_group_lens: None,
                            started_time,
                        });
                    // Local admission owns only RAII fences while suspended;
                    // slot allocation through response delivery has no await.
                    // Keep the master reservation RPC outside this cancellation
                    // boundary so its returned identities always reach cleanup.
                    let response = tokio::select! {
                        biased;
                        _ = done_tx.closed() => {
                            return Err(KvError::Api(ApiError::Unknown {
                                detail: "native Put caller cancelled during admission".to_string(),
                            }));
                        }
                        _ = shutdown_waiter.wait() => {
                            return Err(KvError::Api(ApiError::SystemShutdown {
                                detail: "native Put admission stopped during shutdown".to_string(),
                            }));
                        }
                        response = admission => response?,
                    };
                    crate::rpcresp_kvresult_convert::try_from_code(
                        response.error_code,
                        response.error_json,
                    )?;
                    assert_eq!(response.items.len(), group.len(), "local Owner Put response shape");
                    let mut first_error = None;
                    for ((index, request, reservation_id), response) in
                        group.into_iter().zip(response.items)
                    {
                        if let Err(error) = crate::rpcresp_kvresult_convert::try_from_code(
                            response.error_code,
                            response.error_json,
                        ) {
                            first_error.get_or_insert(error);
                            continue;
                        }
                        let put_id = response.put_id.expect("successful Owner Put has an identity");
                        assert!(response.peer_id.is_none(), "native Owner Put target must be local");
                        assert!(unassigned_reservations.remove(&reservation_id));
                        reserved_items.push((index, OwnerReservedPutItem {
                            _operation: operation.clone(),
                            key: request.key,
                            key_reservation_id: reservation_id,
                            put_id,
                            src_addr: response.src_base_addr.checked_add(response.src_offset)
                                .expect("Owner Put source address fits u64"),
                            src_base_addr: response.src_base_addr,
                            target_addr: response.target_base_addr.checked_add(response.target_offset)
                                .expect("Owner Put target address fits u64"),
                            target_base_addr: response.target_base_addr,
                            value_len: request.len,
                            lease_id,
                            peer_node_id: None,
                            remember_local_snapshot: true,
                            make_replica_task: request.make_replica_task,
                            remote_replica_admitted: true,
                            preferred_sub_cluster: request.preferred_sub_cluster,
                        }));
                    }
                    if let Some(error) = first_error {
                        return Err(error);
                    }
                }
                Ok(())
            }.await;

            if let Err(error) = result {
                if let Err(cleanup_error) = inner.owner_batch_put_abort_reserved(
                    reserved_items.into_iter().map(|(_, item)| item).collect(),
                ).await {
                    inner.native_put_operations.record_cleanup_error(&cleanup_error);
                    tracing::error!(%cleanup_error, "native Put partial admission cleanup failed");
                }
                if let Err(cleanup_error) = inner.batch_release_put_key_reservations(
                    unassigned_reservations.into_iter().collect(),
                ).await {
                    inner.native_put_operations.record_cleanup_error(&cleanup_error);
                    tracing::error!(%cleanup_error, "native Put unused key reservation cleanup failed");
                }
                let _ = done_tx.send(Err(error));
                return;
            }
            assert!(unassigned_reservations.is_empty());
            reserved_items.sort_unstable_by_key(|(index, _)| *index);
            let _ = done_tx.send(Ok(NativePutReservationDelivery {
                view: task_view.clone(),
                items: reserved_items.into_iter().map(|(_, item)| item).collect(),
            }));
        });
        done_rx
            .await
            .map_err(|_| {
                KvError::Api(ApiError::SystemShutdown {
                    detail: "native Put reservation stopped before completion".to_string(),
                })
            })?
            .map(|mut delivery| std::mem::take(&mut delivery.items))
    }

    pub async fn owner_batch_put_commit_reserved(
        &self,
        items: Vec<OwnerReservedPutItem>,
        _transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<()>>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        // After admission, the owner task owns both publication and final cleanup.
        // The caller waits for master route/lease publication without owning its cancellation.
        let view = self.view.clone_view();
        let task_view = view.clone();
        let (done_tx, done_rx) = ::tokio::sync::oneshot::channel();
        view.spawn("native_put_commit", async move {
            let inner = task_view.client_kv_api().inner();
            let _operations = items
                .iter()
                .map(|item| item._operation.clone())
                .collect::<Vec<_>>();
            let item_count = items.len();
            if inner.skip_put_end_commit_enabled() {
                let result = inner
                    .owner_batch_put_abort_reserved(items)
                    .await
                    .map(|()| (0..item_count).map(|_| Ok(())).collect());
                let _ = done_tx.send(result);
                return;
            }
            let mut results = (0..item_count).map(|_| None).collect::<Vec<_>>();
            let mut publish_items = Vec::new();
            let mut publish_contexts = Vec::new();
            let mut publish_indices = Vec::new();
            let mut key_reservation_ids = Vec::new();
            let mut failed_items = Vec::new();
            for (index, item) in items.into_iter().enumerate() {
                let pending_key = (item.key.clone(), item.put_id.0, item.put_id.1);
                let Some(context) = inner.external_pending_puts.get(&pending_key) else {
                    results[index] = Some(Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "native Put has no pending Owner reservation: key={}",
                            item.key
                        ),
                    })));
                    failed_items.push(item);
                    continue;
                };
                let committed = super::external_api::commit_external_local_first_pending(
                    inner,
                    &item.key,
                    item.put_id,
                    &context,
                    item.src_addr - item.src_base_addr,
                    item.value_len,
                    "native_put_commit",
                )
                .await;
                match committed {
                    Ok(committed_slot) => {
                        inner.record_put_locality(false, item.value_len, 0);
                        inner.external_pending_puts.invalidate(&pending_key);
                        publish_indices.push((index, item.remember_local_snapshot));
                        key_reservation_ids.push(item.key_reservation_id);
                        publish_items.push(OwnerLocalPublishItem {
                            key: item.key,
                            put_id: item.put_id,
                            value_len: item.value_len,
                            lease_id: item.lease_id,
                            committed_slot,
                            make_replica_task: item.make_replica_task,
                            remote_replica_admitted: item.remote_replica_admitted,
                            preferred_sub_cluster: item.preferred_sub_cluster,
                            atomic_group: None,
                            radix: None,
                        });
                        publish_contexts.push(context);
                    }
                    Err(error) => {
                        results[index] = Some(Err(error));
                        failed_items.push(item);
                    }
                }
            }
            if let Err(error) = inner.owner_batch_put_abort_reserved(failed_items).await {
                tracing::error!(%error, "native Put rejected reservation cleanup failed");
            }
            inner.external_pending_puts.run_pending_tasks();
            let published = publish_native_owner_local_job(
                task_view.clone(),
                OwnerLocalPublishJob {
                    items: publish_items.clone(),
                    key_reservation_ids,
                    external_pending_contexts: publish_contexts,
                },
            )
            .await;
            assert_eq!(
                published.len(),
                publish_indices.len(),
                "native Put publication response shape"
            );
            for (((index, remember_snapshot), item), result) in publish_indices
                .into_iter()
                .zip(publish_items)
                .zip(published)
            {
                if result.is_ok() && remember_snapshot {
                    inner.remember_local_snapshot(&item.key, item.put_id);
                }
                results[index] = Some(result);
            }
            let _ = done_tx.send(Ok(results
                .into_iter()
                .map(|result| result.expect("every native Put has a terminal result"))
                .collect()));
        });
        done_rx.await.map_err(|_| {
            KvError::Api(ApiError::SystemShutdown {
                detail: "native Put publication stopped before completion".to_string(),
            })
        })?
    }

    pub async fn owner_batch_put_abort_reserved(
        &self,
        items: Vec<OwnerReservedPutItem>,
    ) -> KvResult<()> {
        if items.is_empty() {
            return Ok(());
        }
        let view = self.view.clone_view();
        let task_view = view.clone();
        let (done_tx, done_rx) = ::tokio::sync::oneshot::channel();
        view.spawn("native_put_abort", async move {
            let inner = task_view.client_kv_api().inner();
            let _operations = items
                .iter()
                .map(|item| item._operation.clone())
                .collect::<Vec<_>>();
            let started_time = inner.view.cluster_manager().get_self_info().node_start_time;
            let mut first_error = None;
            let mut reservation_ids = Vec::with_capacity(items.len());
            for item in items {
                reservation_ids.push(item.key_reservation_id);
                let pending_key = (item.key.clone(), item.put_id.0, item.put_id.1);
                // Publication owns committed slots; only a pending lease is revocable here.
                if inner.external_pending_puts.get(&pending_key).is_none() {
                    continue;
                }
                let result = task_view
                    .client_kv_api()
                    .external_put_revoke(ExternalPutRevokeReq {
                        key: item.key,
                        put_id: Some(item.put_id),
                        started_time,
                    })
                    .await;
                let result = result.and_then(|response| {
                    crate::rpcresp_kvresult_convert::try_from_code(
                        response.error_code,
                        response.error_json,
                    )
                });
                if let Err(error) = result {
                    first_error.get_or_insert(error);
                }
            }
            if let Err(error) = inner
                .batch_release_put_key_reservations(reservation_ids)
                .await
            {
                first_error.get_or_insert(error);
            }
            if let Some(error) = first_error.as_ref() {
                inner.native_put_operations.record_cleanup_error(error);
            }
            let _ = done_tx.send(first_error.map_or(Ok(()), Err));
        });
        done_rx.await.map_err(|_| {
            KvError::Api(ApiError::SystemShutdown {
                detail: "native Put abort stopped before completion".to_string(),
            })
        })?
    }

    pub async unsafe fn batch_put_flat_dict_ptrs(
        &self,
        keys: Vec<String>,
        ptrs_groups: Vec<Vec<(u8, usize, u32, u64, u32, Option<u32>)>>,
        opts: crate::client_kv_api::PutOptionalArgs,
        transfer_concurrency: usize,
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
                    "batch_put_flat_dict_ptrs requires matching key/payload counts: keys={} payloads={}",
                    keys.len(),
                    ptrs_groups.len(),
                ),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let start_items = keys
            .iter()
            .zip(&ptrs_groups)
            .map(|(key, ptrs)| {
                Ok(BatchPutStartItemReq {
                    key: key.clone(),
                    len: calc_flat_dict_encoded_len(ptrs)?,
                    reject_if_inflight_same_key: opts.reject_if_inflight_same_key(),
                    reject_if_exist_same_key: opts.reject_if_exist_same_key(),
                    make_replica_task: opts.make_replica_task(),
                    preferred_sub_cluster: opts.preferred_sub_cluster().map(str::to_string),
                })
            })
            .collect::<KvResult<Vec<_>>>()?;

        let reserved = match self
            .owner_batch_put_start_reserved(start_items, opts.lease_id())
            .await
        {
            Ok(items) => items,
            Err(_) => {
                // Admission is all-or-nothing; the native batch contract returns one
                // independent result per key, including repeated keys in input order.
                let mut results = Vec::with_capacity(keys.len());
                for (key, ptrs) in keys.into_iter().zip(ptrs_groups) {
                    results
                        .push(unsafe { self.put_flat_dict_ptrs(&key, ptrs, opts.clone()).await });
                }
                return Ok(results);
            }
        };
        assert_eq!(
            reserved.len(),
            ptrs_groups.len(),
            "native Put reservation count"
        );
        if !self.short_circuit_put_payload_path_enabled() {
            for (item, ptrs) in reserved.iter().zip(ptrs_groups) {
                unsafe {
                    write_flat_dict_ptrs_to_ptr(item.src_addr as *mut u8, &ptrs);
                }
            }
        }
        self.owner_batch_put_commit_reserved(reserved, transfer_concurrency)
            .await
    }

    fn start_owner_local_ssd_puts(
        &self,
        candidates: Vec<OwnerLocalSsdPutCandidate>,
        resources_pre_admitted: bool,
    ) -> Vec<Option<Arc<OwnerLocalSsdPutSharedOp>>> {
        // Early backing is optional work. Apply its independent byte budget
        // before touching the per-key generation table or pinning a source.
        // Last-backing callers have already performed the same admission while
        // deciding which capacity victims may enter durability, so they pass
        // resources_pre_admitted=true and must not be charged twice.
        let admission = if resources_pre_admitted {
            vec![true; candidates.len()]
        } else {
            let lengths = candidates
                .iter()
                .map(|candidate| candidate.value_len)
                .collect::<Vec<_>>();
            self.ssd_storage
                .as_ref()
                .map(|store| store.admit_owner_write_candidates(&lengths))
                .unwrap_or_else(|| vec![false; candidates.len()])
        };
        let mut operations = std::iter::repeat_with(|| None)
            .take(candidates.len())
            .collect::<Vec<_>>();
        let mut leaders = Vec::new();
        let mut unused_pre_admission_lengths = Vec::new();
        for (index, (candidate, admitted_by_resource)) in
            candidates.into_iter().zip(admission).enumerate()
        {
            if !admitted_by_resource {
                continue;
            }
            let pre_admitted_len = candidate.value_len;
            match self.begin_owner_local_ssd_put(
                &candidate.key,
                candidate.put_id,
                candidate.selected_victim.as_deref(),
            ) {
                OwnerLocalSsdPutReservation::Leader { op, memory_info } => {
                    operations[index] = Some(op.clone());
                    if pre_admitted_len != 0 && u64::from(memory_info.len) != pre_admitted_len {
                        tracing::warn!(
                            key = %candidate.key,
                            put_id_time = candidate.put_id.0,
                            put_id_version = candidate.put_id.1,
                            admitted_len = pre_admitted_len,
                            source_len = memory_info.len,
                            "owner local SSD pre-admission length no longer matches exact source"
                        );
                        unused_pre_admission_lengths.push(pre_admitted_len);
                        self.finish_owner_local_ssd_put(&op, OwnerLocalSsdPutOutcome::Failed);
                        continue;
                    }
                    leaders.push(OwnerLocalSsdPutLeader {
                        op,
                        memory_info,
                        admission_len: pre_admitted_len,
                    });
                }
                OwnerLocalSsdPutReservation::Follower(op) => {
                    operations[index] = Some(op);
                    unused_pre_admission_lengths.push(pre_admitted_len);
                }
                OwnerLocalSsdPutReservation::SourceUnavailable => {
                    unused_pre_admission_lengths.push(pre_admitted_len);
                }
            }
        }
        if let Some(store) = self.ssd_storage.as_ref() {
            store.refund_owner_write_admission(&unused_pre_admission_lengths);
        }
        if leaders.is_empty() {
            return operations;
        }

        let admitted_lengths = leaders
            .iter()
            .map(|leader| leader.admission_len)
            .collect::<Vec<_>>();
        let permit = match self.try_acquire_local_ssd_persist_batch(leaders.len()) {
            Ok(Some(permit)) => permit,
            Ok(None) => {
                if let Some(store) = self.ssd_storage.as_ref() {
                    store.refund_owner_write_admission(&admitted_lengths);
                }
                for leader in leaders {
                    self.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Dropped);
                }
                return operations;
            }
            Err(err) => {
                if let Some(store) = self.ssd_storage.as_ref() {
                    store.refund_owner_write_admission(&admitted_lengths);
                }
                tracing::warn!(
                    items = leaders.len(),
                    error = %err,
                    "owner local SSD durability batch admission failed"
                );
                for leader in leaders {
                    self.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
                }
                return operations;
            }
        };

        let spawn_view = self.view.clone_view();
        let task_view = spawn_view.clone();
        let _ = spawn_view.spawn("owner_local_ssd_put_batch", async move {
            run_owner_local_ssd_put_batch(task_view, leaders, permit).await;
        });
        operations
    }

    pub(crate) fn start_early_owner_local_ssd_puts(
        &self,
        candidates: Vec<(String, PutIDForAKey, u64)>,
    ) {
        let _ = self.start_owner_local_ssd_puts(
            candidates
                .into_iter()
                .map(|(key, put_id, value_len)| OwnerLocalSsdPutCandidate {
                    key,
                    put_id,
                    value_len,
                    selected_victim: None,
                })
                .collect(),
            false,
        );
    }

    pub(crate) fn start_early_owner_local_ssd_put(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        value_len: u64,
    ) {
        self.start_early_owner_local_ssd_puts(vec![(key.to_string(), put_id, value_len)]);
    }

    /// Enter the one owner-side remote Put singleflight and launch its leader.
    ///
    /// The per-key reservation remains a short synchronous critical section.
    /// Only a new leader acquires a source holder and spawns Start/transfer/Done;
    /// a follower receives the already-installed shared operation without
    /// acquiring another holder or starting another transfer.
    fn start_or_join_owner_remote_put(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) -> OwnerRemotePutStart {
        match self.begin_owner_remote_put(
            key,
            put_id,
            preferred_sub_cluster,
            protect_source_on_remote_complete,
        ) {
            OwnerRemotePutReservation::Leader {
                op,
                memory_info,
                admission_permit,
            } => {
                let holder = Arc::new(UserMemHolder::new(
                    memory_info,
                    self.get_or_init_all_memholder_refcount(),
                    UserMemHolderExposeKind::SegPtr,
                ));
                let spawn_view = self.view.clone_view();
                let task_view = spawn_view.clone();
                let task_guard =
                    OwnerRemotePutTaskGuard::new(task_view.clone(), op.clone(), admission_permit);
                let _ = spawn_view.spawn("owner_remote_put_leader", async move {
                    run_owner_remote_put(task_view, op, holder, task_guard).await;
                });
                OwnerRemotePutStart::LeaderStarted
            }
            OwnerRemotePutReservation::Follower(op) => {
                tracing::debug!(
                    "owner remote Put joined existing flight: key={} put_id=({},{})",
                    op.key,
                    op.put_id.0,
                    op.put_id.1
                );
                OwnerRemotePutStart::Follower(op)
            }
            OwnerRemotePutReservation::SourceUnavailable => {
                tracing::warn!(
                    "owner remote Put source is unavailable, fenced, or version-mismatched: key={} put_id=({},{})",
                    key,
                    put_id.0,
                    put_id.1
                );
                OwnerRemotePutStart::Rejected
            }
            OwnerRemotePutReservation::NotAdmitted => {
                tracing::debug!(
                    "owner remote Put was not admitted without queueing: key={} put_id=({},{})",
                    key,
                    put_id.0,
                    put_id.1
                );
                OwnerRemotePutStart::Rejected
            }
        }
    }

    /// Non-blocking enqueue acknowledgement for an owner-side remote Put.
    ///
    /// `true` means this call either launched the unique leader or joined its
    /// existing flight. It deliberately does not wait for Start/transfer/Done;
    /// the shared flight still publishes the eventual terminal to callers that
    /// explicitly need to wait for it.
    pub(crate) fn start_remote_put_nonblocking(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) -> bool {
        matches!(
            self.start_or_join_owner_remote_put(
                key,
                put_id,
                preferred_sub_cluster,
                protect_source_on_remote_complete,
            ),
            OwnerRemotePutStart::LeaderStarted | OwnerRemotePutStart::Follower(_)
        )
    }

    /// Ensure one remote backing exists for an exact owner-local generation.
    ///
    /// Normal Put, pre-reserved replica, proactive write-back, and tier1 all
    /// enter the same singleflight here. Only the flight leader pins the source
    /// and immediately launches Start/transfer/Done in its own async task;
    /// followers wait for and reuse that terminal result instead of acquiring
    /// their own holder.
    pub async fn ensure_remote_put(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) -> KvResult<bool> {
        let mut follower_takeover_attempted = false;
        loop {
            match self.start_or_join_owner_remote_put(
                key,
                put_id,
                preferred_sub_cluster.clone(),
                protect_source_on_remote_complete,
            ) {
                OwnerRemotePutStart::LeaderStarted => return Ok(true),
                OwnerRemotePutStart::Follower(op) => match op.wait().await {
                    OwnerRemotePutOutcome::Published | OwnerRemotePutOutcome::AlreadySatisfied => {
                        return Ok(true);
                    }
                    OwnerRemotePutOutcome::Obsolete => return Ok(false),
                    OwnerRemotePutOutcome::Failed if !follower_takeover_attempted => {
                        follower_takeover_attempted = true;
                        continue;
                    }
                    OwnerRemotePutOutcome::Failed => return Ok(false),
                    OwnerRemotePutOutcome::InFlight => {
                        unreachable!("remote Put wait must return a terminal outcome")
                    }
                },
                OwnerRemotePutStart::Rejected => return Ok(false),
            }
        }
    }

    async fn put_common<F>(
        &self,
        key: &str,
        payload_len: u64,
        opts: crate::client_kv_api::PutOptionalArgs,
        fill_abs_src: F,
    ) -> KvResult<()>
    where
        F: FnOnce(u64),
    {
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let metrics = self.metrics_handle();
        let started_at = Utc::now().timestamp_micros();
        let reserved = match self
            .owner_batch_put_start_reserved(
                vec![BatchPutStartItemReq {
                    key: key.to_string(),
                    len: payload_len,
                    reject_if_inflight_same_key: opts.reject_if_inflight_same_key(),
                    reject_if_exist_same_key: opts.reject_if_exist_same_key(),
                    make_replica_task: opts.make_replica_task(),
                    preferred_sub_cluster: opts.preferred_sub_cluster().map(str::to_string),
                }],
                opts.lease_id(),
            )
            .await
        {
            Ok(reserved) => reserved,
            Err(error) => {
                obe_put_start_error_rpc(&metrics, &client_id, &node_role, key, payload_len);
                return Err(error);
            }
        };
        assert_eq!(reserved.len(), 1, "single native Put reservation count");
        obe_put_start_success(
            &metrics,
            &client_id,
            &node_role,
            key,
            started_at,
            Utc::now().timestamp_micros(),
        );
        let item = &reserved[0];
        #[cfg(test)]
        self.test_record.add_transfering_put(
            key.to_string(),
            payload_len as u32,
            item.put_id.0,
            item.put_id.1,
            self.view.cluster_manager().get_self_info().id.to_string(),
            format!("{:#x}", item.target_addr),
        );
        if !self.short_circuit_put_payload_path_enabled() {
            fill_abs_src(item.src_addr);
        }
        #[cfg(test)]
        self.test_record
            .remove_transfering_put(key.to_string(), item.put_id);
        let mut results = self.owner_batch_put_commit_reserved(reserved, 1).await?;
        assert_eq!(results.len(), 1, "single native Put completion count");
        results.pop().expect("single native Put result exists")
    }

    /// Encode a flat dictionary directly into an Owner-managed segment.
    ///
    /// # Safety
    /// The caller must keep all pointer ranges readable until this call returns.
    pub async unsafe fn put_flat_dict_ptrs(
        &self,
        key: &str,
        ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        let payload_len = calc_flat_dict_encoded_len(&ptrs)?;
        self.put_common(key, payload_len, opts, move |address| unsafe {
            write_flat_dict_ptrs_to_ptr(address as *mut u8, &ptrs);
        })
        .await
    }

    /// Publish a value and its optional lease before returning success.
    pub async fn put(
        &self,
        key: &str,
        value: &[u8],
        opts: crate::client_kv_api::PutOptionalArgs,
    ) -> KvResult<()> {
        self.put_common(key, value.len() as u64, opts, |address| unsafe {
            std::ptr::copy_nonoverlapping(value.as_ptr(), address as *mut u8, value.len());
        })
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
        let resp = call_control_plane_rpc(
            &self.rpc_caller_put_start,
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            Some(std::time::Duration::from_secs(60)),
            2,
        )
        .await
        .map_err(KvError::P2p)?;
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
        let _resp = call_control_plane_rpc(
            &self.rpc_caller_put_revoke,
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
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
        mut victims: Vec<OwnerSourceEvictionVictim>,
        global_reclaim_requested_bytes: u64,
        reserve_global_demotion: bool,
    ) -> KvResult<BatchEvictOwnerSourceResp> {
        let mut global_demotion_reserved_bytes = 0u64;
        if reserve_global_demotion && !self.test_spec_config.legacy_a19_global_demotion_one_phase {
            let reservation_started_at = Instant::now();
            let mut allocator = self.owner_segment_allocator.lock();
            let reservation_results =
                allocator.try_reserve_global_demotions(victims.iter().filter_map(|victim| {
                    let OwnerReclaimBacking::CommittedSlot {
                        allocation_id,
                        segment_offset,
                        capacity_bytes,
                    } = &victim.backing
                    else {
                        return None;
                    };
                    Some((
                        victim.key.as_str(),
                        victim.put_id,
                        *allocation_id,
                        *segment_offset,
                        *capacity_bytes,
                    ))
                }));
            let mut reservation_results = reservation_results.into_iter();
            let mut reservation_slots = 0usize;
            let mut global_demotion_reserved_items = 0usize;
            for victim in &mut victims {
                let OwnerReclaimBacking::CommittedSlot {
                    allocation_id,
                    segment_offset: _,
                    capacity_bytes,
                } = &victim.backing
                else {
                    victim.global_demotion_reserved = false;
                    continue;
                };
                reservation_slots += 1;
                victim.global_demotion_reserved = match reservation_results
                    .next()
                    .expect("each committed owner victim must have one reservation result")
                {
                    Ok(reserved) => reserved,
                    Err(error) => {
                        tracing::debug!(
                            key = victim.key,
                            put_time_ms = victim.put_id.0,
                            put_version = victim.put_id.1,
                            allocation_id,
                            error = %error.detail,
                            "exact owner victim could not reserve GlobalShared budget"
                        );
                        false
                    }
                };
                if victim.global_demotion_reserved {
                    global_demotion_reserved_items += 1;
                    global_demotion_reserved_bytes =
                        global_demotion_reserved_bytes.saturating_add(*capacity_bytes);
                }
            }
            debug_assert!(reservation_results.next().is_none());
            drop(allocator);
            tracing::info!(
                victims = victims.len(),
                reservation_slots,
                global_demotion_reserved_items,
                global_demotion_reserved_bytes,
                duration_us = reservation_started_at
                    .elapsed()
                    .as_micros()
                    .min(u64::MAX as u128) as u64,
                "owner GlobalShared reservation batch completed"
            );
        } else {
            for victim in &mut victims {
                victim.global_demotion_reserved = false;
            }
        }
        let prepared_bytes = victims
            .iter()
            .map(|victim| owner_reclaim_backing_len(&victim.backing))
            .fold(0u64, u64::saturating_add);
        if global_reclaim_requested_bytes > prepared_bytes {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "owner source eviction requested {} GlobalShared reclaim bytes from only {} exact prepared bytes",
                    global_reclaim_requested_bytes, prepared_bytes
                ),
            }));
        }
        if victims.is_empty() {
            if global_reclaim_requested_bytes != 0 {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "owner source eviction cannot request {} GlobalShared reclaim bytes without exact victims",
                        global_reclaim_requested_bytes
                    ),
                }));
            }
            return Ok(BatchEvictOwnerSourceResp {
                operation_id: 0,
                global_reclaim_requested_bytes: 0,
                global_reclaim_selected_bytes: 0,
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
                global_reclaim_requested_bytes,
                victims,
            },
            raw_bytes: Vec::new(),
        };
        tracing::debug!(
            victims = req.serialize_part.victims.len(),
            prepared_bytes,
            global_demotion_reserved_bytes,
            global_reclaim_requested_bytes,
            reserve_global_demotion,
            "owner source eviction prepared master-screened GlobalShared reservation phase"
        );
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
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
        if resp.serialize_part.global_reclaim_requested_bytes != global_reclaim_requested_bytes {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner source-eviction GlobalShared reclaim echo mismatch: requested={} response={}",
                    global_reclaim_requested_bytes,
                    resp.serialize_part.global_reclaim_requested_bytes
                ),
            }));
        }
        Ok(resp.serialize_part)
    }

    async fn batch_publish_owner_ssd(
        &self,
        items: Vec<OwnerSsdPublishItem>,
    ) -> KvResult<BatchPublishOwnerSsdResp> {
        if items.is_empty() {
            return Ok(BatchPublishOwnerSsdResp {
                items: Vec::new(),
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting owner SSD publication".to_string(),
            }));
        }
        let self_info = self.view.cluster_manager().get_self_info();
        let req = MsgPack {
            serialize_part: BatchPublishOwnerSsdReq {
                owner_node_start_time: self_info.node_start_time,
                items,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_publish_owner_ssd
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(Duration::from_secs(60)),
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
        committed_slot: Option<crate::owner_segment::OwnerSlotDesc>,
        route_token: Option<crate::owner_segment::OwnerTargetRouteToken>,
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
                committed_slot,
                route_token,
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
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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

    async fn find_put_master_before_shutdown(&self) -> KvResult<String> {
        let mut shutdown_waiter = self.view.register_shutdown_waiter();
        // Discovery has no remote side effects. Once dispatched, callers must
        // consume their RPC response even if shutdown arrives in the meantime.
        tokio::select! {
            biased;
            _ = shutdown_waiter.wait() => Err(KvError::Api(ApiError::SystemShutdown {
                detail: "Put master discovery stopped during shutdown".to_string(),
            })),
            master = self.view.cluster_manager().find_or_wait_master_node() => Ok(master?),
        }
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
        let master_node_id = self.find_put_master_before_shutdown().await?;
        let resp = call_control_plane_rpc(
            &self.rpc_caller_batch_prepare_put_keys,
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
        // Native operation guards keep communication dependencies alive until
        // this cleanup finishes, including after admission closes.
        let req = MsgPack {
            serialize_part: BatchReleasePutKeyReservationsReq { reservation_ids },
            raw_bytes: Vec::new(),
        };
        let master_nodes = self
            .view
            .cluster_manager()
            .get_members()
            .into_iter()
            .filter(|member| member.metadata.contains_key("master"))
            .map(|member| member.id)
            .collect::<Vec<_>>();
        let master_node_id = match master_nodes.as_slice() {
            [master] => master.clone(),
            [] => return Err(
                crate::rpcresp_kvresult_convert::msg_and_error::ClusterManagerExtError::MasterNotFound {}.into(),
            ),
            _ => return Err(
                crate::rpcresp_kvresult_convert::msg_and_error::ClusterManagerExtError::MultipleMasters {
                    nodes: master_nodes,
                }.into(),
            ),
        };
        // Cleanup must remain callable after shutdown intent. Use the bounded
        // single-attempt primitive; the ordinary retry wrapper closes admission.
        let resp = fluxon_commu::p2p::rpc::call_rpc_with_transport_policy(
            self.view.p2p_module(),
            master_node_id.into(),
            req,
            Some(std::time::Duration::from_secs(60)),
            CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
        let master_node_id = self.find_put_master_before_shutdown().await?;
        let resp = self
            .rpc_caller_batch_put_done
            .call_with_transport_policy(
                self.view.p2p_module(),
                master_node_id.into(),
                req,
                Some(std::time::Duration::from_secs(60)),
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
                CONTROL_PLANE_RPC_TRANSPORT_POLICY,
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
                radix: None,
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
        let resp = call_control_plane_rpc(
            &self.rpc_caller_put_done,
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
                radix: None,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let rpc_started_at = Instant::now();
        let resp = call_control_plane_rpc(
            &self.rpc_caller_put_done,
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
        owner_local_publish_atomic_batch_complete, owner_local_publish_starts_remote,
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
            remote_replica_admitted: false,
            preferred_sub_cluster: None,
            atomic_group,
            radix: None,
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
    fn local_ssd_content_selection_does_not_bypass_remote_resource_admission() {
        let mut item = publish_item("key", (1, 0), None);
        item.make_replica_task = true;
        assert!(!owner_local_publish_starts_remote(&item));

        item.remote_replica_admitted = true;
        assert!(owner_local_publish_starts_remote(&item));

        item.make_replica_task = false;
        assert!(!owner_local_publish_starts_remote(&item));
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

fn owner_generation_is_current(
    inner: &ClientKvApiInner,
    owner: &crate::owner_segment::OwnerGeneration,
) -> bool {
    inner
        .view
        .cluster_manager()
        .get_member_info_cached(&owner.node_id)
        .is_some_and(|member| member.node_start_time == owner.node_start_time)
}

/// Retry one generation-safe owner operation until its per-item result is
/// known. A transport error is ambiguous: the remote handler may already have
/// installed a lease or committed a route, so callers must not switch targets
/// or free state based on that error.
async fn owner_segment_transfer_one_until_definitive(
    inner: &ClientKvApiInner,
    target: &crate::owner_segment::OwnerGeneration,
    item: OwnerSegmentTransferItem,
    phase: &'static str,
) -> KvResult<crate::owner_segment::OwnerSegmentTransferItemResp> {
    let mut responses =
        owner_segment_transfer_batch_until_definitive(inner, target, vec![item], phase).await?;
    responses.pop().ok_or_else(|| {
        KvError::Api(ApiError::Unknown {
            detail: format!(
                "owner segment transfer returned no item for phase={phase} target={}",
                target.node_id
            ),
        })
    })
}

/// Replay one exact owner-RPC batch after an ambiguous transport result. The
/// request vector and operation identities never change across attempts.
pub(super) async fn owner_segment_transfer_batch_until_definitive(
    inner: &ClientKvApiInner,
    target: &crate::owner_segment::OwnerGeneration,
    items: Vec<OwnerSegmentTransferItem>,
    phase: &'static str,
) -> KvResult<Vec<crate::owner_segment::OwnerSegmentTransferItemResp>> {
    let self_info = inner.view.cluster_manager().get_self_info();
    let request = inner.owner_transfer_peer_tracker.prepare_request(
        crate::owner_segment::OwnerGeneration::new(self_info.id.clone(), self_info.node_start_time),
        target,
        items,
    );
    crate::owner_segment::replay_owner_segment_batch_until_definitive(
        target,
        request,
        phase,
        |request| inner.owner_segment_transfer(target, request),
        || inner.view.register_shutdown_poller().is_running(),
        |owner| owner_generation_is_current(inner, owner),
    )
    .await
}

fn owner_remote_put_source_slot(
    inner: &ClientKvApiInner,
    op: &OwnerRemotePutSharedOp,
    holder: &UserMemHolder,
) -> KvResult<(OwnerSlotDesc, u64)> {
    let manifest = inner
        .owner_segment_allocator
        .lock()
        .manifest_entry(&op.key, op.put_id)
        .ok_or_else(|| {
            KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner remote Put source is absent from the owner manifest: key={} put_id=({},{})",
                    op.key, op.put_id.0, op.put_id.1
                ),
            })
        })?;
    let memory = holder.memory_info();
    if manifest.physical_state != crate::owner_segment::OwnerSlotPhysicalState::Committed
        || manifest.route_epoch == 0
        || manifest.slot.addr != memory.addr
        || manifest.slot.segment_offset != memory.offset
        || manifest.slot.len != u64::from(memory.len)
    {
        return Err(KvError::Api(ApiError::Unknown {
            detail: format!(
                "owner remote Put source holder does not match the committed manifest: key={} put_id=({},{}) allocation_id={} slot_addr={:#x} holder_addr={:#x} slot_len={} holder_len={}",
                op.key,
                op.put_id.0,
                op.put_id.1,
                manifest.slot.allocation_id,
                manifest.slot.addr,
                memory.addr,
                manifest.slot.len,
                memory.len,
            ),
        }));
    }
    Ok((manifest.slot, manifest.route_epoch))
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

fn owner_source_ssd_policy(capacity_writeback_enabled: bool) -> OwnerSourceSsdPolicy {
    if capacity_writeback_enabled {
        OwnerSourceSsdPolicy::SelectLastLive
    } else {
        OwnerSourceSsdPolicy::Drop
    }
}

fn owner_source_eviction_victim(
    identity: &super::OwnerHotReplicaIdentity,
    memory_info: &crate::memholder::MemoryInfo,
    ssd_capacity_writeback_enabled: bool,
) -> Option<OwnerSourceEvictionVictim> {
    let (allocation_id, segment_offset, capacity_bytes) =
        memory_info.local_reserve_resident_slot_ref()?;
    Some(OwnerSourceEvictionVictim {
        key: identity.key.clone(),
        put_id: (identity.put_time_ms, identity.put_version),
        backing: OwnerReclaimBacking::CommittedSlot {
            allocation_id,
            segment_offset,
            capacity_bytes,
        },
        global_demotion_reserved: false,
        ssd_backing_len: None,
        ssd_policy: owner_source_ssd_policy(ssd_capacity_writeback_enabled),
    })
}

fn finish_owner_source_selection(
    inner: &ClientKvApiInner,
    victim: &OwnerSourceEvictionVictim,
    restore_current: bool,
    reason: &str,
) {
    if let OwnerReclaimBacking::CommittedSlot {
        allocation_id,
        segment_offset,
        capacity_bytes,
    } = &victim.backing
    {
        inner
            .owner_segment_allocator
            .lock()
            .release_global_demotion_reservation(
                &victim.key,
                victim.put_id,
                *allocation_id,
                *segment_offset,
                *capacity_bytes,
            );
    }
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

    let ssd_capacity_writeback_enabled = inner
        .ssd_storage
        .as_ref()
        .is_some_and(|store| store.capacity_writeback_enabled());
    let Some(victim) =
        owner_source_eviction_victim(&trigger, source.as_ref(), ssd_capacity_writeback_enabled)
    else {
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

async fn finish_owner_source_eviction_result(
    inner: &ClientKvApiInner,
    operation_id: u64,
    index: usize,
    event: OwnerHotEvictionEvent,
    victim: Arc<OwnerSourceEvictionVictim>,
    result: OwnerSourceEvictionVictimResp,
) {
    if victim.ssd_policy == OwnerSourceSsdPolicy::Persisted
        && !result.ssd_backing_committed
        && matches!(
            result.outcome,
            OwnerSourceEvictionOutcome::DemotedGlobal
                | OwnerSourceEvictionOutcome::Completed
                | OwnerSourceEvictionOutcome::Stale
                | OwnerSourceEvictionOutcome::RejectedNotEvictable
        )
    {
        inner
            .discard_local_ssd_replica(&victim.key, victim.put_id)
            .await;
    }
    match result.outcome {
        OwnerSourceEvictionOutcome::Accepted | OwnerSourceEvictionOutcome::AlreadyInProgress => {
            inner
                .owner_hot_counters
                .source_evict_handoff_members
                .fetch_add(1, Ordering::Relaxed);
        }
        OwnerSourceEvictionOutcome::DemotedGlobal => {
            let epoch = owner_source_eviction_epoch(operation_id, index);
            match super::reclaim::complete_owner_source_demotion(inner, &victim, epoch) {
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
                        "master demoted source route but owner local-index release must retry"
                    );
                    schedule_owner_source_eviction_retry(
                        inner,
                        event,
                        victim,
                        "owner local-index release after metadata-only demotion is temporarily busy",
                    );
                }
            }
        }
        OwnerSourceEvictionOutcome::Completed => {
            if let OwnerReclaimBacking::CommittedSlot {
                allocation_id,
                segment_offset,
                capacity_bytes,
            } = &victim.backing
            {
                inner
                    .owner_segment_allocator
                    .lock()
                    .release_global_demotion_reservation(
                        &victim.key,
                        victim.put_id,
                        *allocation_id,
                        *segment_offset,
                        *capacity_bytes,
                    );
            }
            let epoch = owner_source_eviction_epoch(operation_id, index);
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
        OwnerSourceEvictionOutcome::SsdCandidate => {
            tracing::error!(
                key = victim.key,
                policy = ?victim.ssd_policy,
                "master returned an SSD candidate outside the selection phase"
            );
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "unexpected repeated SSD candidate response",
            );
        }
        OwnerSourceEvictionOutcome::RetryableBusy | OwnerSourceEvictionOutcome::Unspecified => {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "master source reclaim is temporarily busy",
            );
        }
        OwnerSourceEvictionOutcome::NeedsGlobalReservation => {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "master-screened last backing is waiting for exact GlobalShared reservation",
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

async fn submit_owner_source_eviction_decisions(
    inner: &ClientKvApiInner,
    prepared: Vec<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)>,
    rpc_failure_reason: &'static str,
    response_length_reason: &'static str,
    response_identity_reason: &'static str,
) {
    submit_owner_source_eviction_decisions_with_followup(
        inner,
        prepared,
        true,
        rpc_failure_reason,
        response_length_reason,
        response_identity_reason,
    )
    .await;
}

async fn submit_owner_source_eviction_decisions_with_followup(
    inner: &ClientKvApiInner,
    mut pending: Vec<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)>,
    allow_global_reservation_followup: bool,
    rpc_failure_reason: &'static str,
    response_length_reason: &'static str,
    response_identity_reason: &'static str,
) {
    let total_victims = pending.len();
    let batch_count = total_victims.div_ceil(OWNER_SOURCE_EVICTION_RPC_BATCH);
    if batch_count > 1 {
        tracing::info!(
            total_victims,
            batch_count,
            batch_limit = OWNER_SOURCE_EVICTION_RPC_BATCH,
            "splitting owner source-eviction decisions into bounded RPC batches"
        );
    }
    while !pending.is_empty() {
        let prepared = take_owner_source_eviction_rpc_batch(&mut pending);
        submit_one_owner_source_eviction_batch_with_followup(
            inner,
            prepared,
            allow_global_reservation_followup,
            rpc_failure_reason,
            response_length_reason,
            response_identity_reason,
        )
        .await;
    }
}

fn take_owner_source_eviction_rpc_batch<T>(pending: &mut Vec<T>) -> Vec<T> {
    if pending.len() <= OWNER_SOURCE_EVICTION_RPC_BATCH {
        return std::mem::take(pending);
    }
    let remainder = pending.split_off(OWNER_SOURCE_EVICTION_RPC_BATCH);
    std::mem::replace(pending, remainder)
}

async fn submit_one_owner_source_eviction_batch_with_followup(
    inner: &ClientKvApiInner,
    mut prepared: Vec<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)>,
    mut allow_global_reservation_followup: bool,
    rpc_failure_reason: &'static str,
    response_length_reason: &'static str,
    response_identity_reason: &'static str,
) {
    loop {
        if prepared.is_empty() {
            return;
        }
        let response = inner
            .batch_evict_owner_source(
                prepared
                    .iter()
                    .map(|(_, victim)| victim.as_ref().clone())
                    .collect(),
                0,
                !allow_global_reservation_followup,
            )
            .await;
        let Ok(response) = response else {
            for (event, victim) in prepared {
                schedule_owner_source_eviction_retry(inner, event, victim, rpc_failure_reason);
            }
            return;
        };
        if response.victims.len() != prepared.len() {
            for (event, victim) in prepared {
                schedule_owner_source_eviction_retry(inner, event, victim, response_length_reason);
            }
            return;
        }
        let operation_id = response.operation_id;
        let mut reservation_candidates = Vec::new();
        for (index, ((event, victim), result)) in
            prepared.into_iter().zip(response.victims).enumerate()
        {
            if result.victim_index != u32::try_from(index).unwrap_or(u32::MAX) {
                schedule_owner_source_eviction_retry(
                    inner,
                    event,
                    victim,
                    response_identity_reason,
                );
                continue;
            }
            if result.outcome == OwnerSourceEvictionOutcome::NeedsGlobalReservation {
                reservation_candidates.push((event, victim));
                continue;
            }
            finish_owner_source_eviction_result(inner, operation_id, index, event, victim, result)
                .await;
        }
        if reservation_candidates.is_empty() {
            return;
        }
        if !allow_global_reservation_followup {
            for (event, victim) in reservation_candidates {
                schedule_owner_source_eviction_retry(
                    inner,
                    event,
                    victim,
                    "master-screened last backing still lacks exact GlobalShared headroom",
                );
            }
            return;
        }
        tracing::info!(
            candidates = reservation_candidates.len(),
            "retrying only master-screened last backings after direct-delete reservations were released"
        );
        prepared = reservation_candidates;
        allow_global_reservation_followup = false;
    }
}

#[cfg(test)]
mod owner_source_eviction_rpc_batch_tests {
    use super::{OWNER_SOURCE_EVICTION_RPC_BATCH, take_owner_source_eviction_rpc_batch};

    #[test]
    fn a6_sized_followup_is_bounded_ordered_and_complete() {
        const VICTIMS: usize = 8_441;
        let mut pending = (0..VICTIMS).collect::<Vec<_>>();
        let mut observed = Vec::with_capacity(VICTIMS);
        let mut batch_sizes = Vec::new();
        while !pending.is_empty() {
            let batch = take_owner_source_eviction_rpc_batch(&mut pending);
            batch_sizes.push(batch.len());
            observed.extend(batch);
        }

        assert_eq!(observed, (0..VICTIMS).collect::<Vec<_>>());
        assert_eq!(
            batch_sizes.len(),
            VICTIMS.div_ceil(OWNER_SOURCE_EVICTION_RPC_BATCH)
        );
        assert!(
            batch_sizes
                .iter()
                .all(|size| *size > 0 && *size <= OWNER_SOURCE_EVICTION_RPC_BATCH)
        );
        assert_eq!(
            batch_sizes.last(),
            Some(&(VICTIMS % OWNER_SOURCE_EVICTION_RPC_BATCH))
        );
    }
}

async fn reclaim_admission_drops_before_ssd_persist<T, DropFuture, PersistFn, PersistFuture>(
    drop_reclaim: DropFuture,
    persist: PersistFn,
) -> T
where
    DropFuture: Future<Output = ()>,
    PersistFn: FnOnce() -> PersistFuture,
    PersistFuture: Future<Output = T>,
{
    drop_reclaim.await;
    persist().await
}

#[cfg(test)]
mod owner_ssd_fast_drop_tests {
    use super::{owner_source_ssd_policy, reclaim_admission_drops_before_ssd_persist};
    use crate::master_kv_router::msg_pack::OwnerSourceSsdPolicy;
    use std::sync::{Arc, Mutex};

    #[test]
    fn early_only_ssd_mode_keeps_capacity_eviction_on_direct_drop() {
        assert_eq!(owner_source_ssd_policy(false), OwnerSourceSsdPolicy::Drop);
        assert_eq!(
            owner_source_ssd_policy(true),
            OwnerSourceSsdPolicy::SelectLastLive
        );
    }

    #[limit_thirdparty::tokio::test]
    async fn admission_drops_finish_before_ssd_persist_starts() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let drop_order = order.clone();
        let persist_order = order.clone();

        let result = reclaim_admission_drops_before_ssd_persist(
            async move {
                drop_order.lock().unwrap().push("drop-start");
                limit_thirdparty::tokio::task::yield_now().await;
                drop_order.lock().unwrap().push("drop-finished");
            },
            move || async move {
                let mut order = persist_order.lock().unwrap();
                assert_eq!(order.last(), Some(&"drop-finished"));
                order.push("persist-start");
                17
            },
        )
        .await;

        assert_eq!(result, 17);
        assert_eq!(
            order.lock().unwrap().as_slice(),
            ["drop-start", "drop-finished", "persist-start"]
        );
    }
}

async fn process_owner_source_eviction_events(
    view: &ClientKvApiView,
    events: Vec<OwnerHotEvictionEvent>,
    pressure: bool,
) -> OwnerPressureBatchReclaim {
    let inner = view.client_kv_api().inner();
    let mut events = events.into_iter();
    let mut reclaimed = OwnerPressureBatchReclaim::default();
    let mut ssd_batch_available = true;
    while events.len() != 0 {
        // Tail candidates retain their local index and reader holders until
        // their own batch is ready to contact master. Uncertain retries keep
        // their existing exact fence and are never reopened here.
        let prepared = prepare_owner_source_eviction_batch(inner, &mut events);
        if prepared.is_empty() {
            continue;
        }
        let prepared_items = prepared.len();
        let started_at = Instant::now();
        let result = process_owner_source_eviction_batch(
            inner,
            prepared,
            pressure,
            &mut ssd_batch_available,
        )
        .await;
        reclaimed.requested_bytes = reclaimed
            .requested_bytes
            .saturating_add(result.requested_bytes);
        reclaimed.selected_bytes = reclaimed
            .selected_bytes
            .saturating_add(result.selected_bytes);
        tracing::debug!(
            prepared_items,
            remaining_candidates = events.len(),
            duration_us = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64,
            "owner source eviction bounded batch completed"
        );
    }
    reclaimed
}

fn prepare_owner_source_eviction_batch(
    inner: &ClientKvApiInner,
    events: &mut impl Iterator<Item = OwnerHotEvictionEvent>,
) -> Vec<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)> {
    events
        .by_ref()
        .take(OWNER_SOURCE_EVICTION_RPC_BATCH)
        .filter_map(|event| prepare_owner_source_eviction_event(inner, event))
        .collect()
}

async fn process_owner_source_eviction_batch(
    inner: &ClientKvApiInner,
    prepared: Vec<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)>,
    pressure: bool,
    ssd_batch_available: &mut bool,
) -> OwnerPressureBatchReclaim {
    // Previous batches may have consumed or released GlobalShared budget.
    // Read one current snapshot after fencing; candidates are not Free credit.
    let pressure_global_headroom_before = owner_source_eviction_pressure_headroom(inner, pressure);
    let prepared_bytes = prepared
        .iter()
        .map(|(_, victim)| owner_reclaim_backing_len(&victim.backing))
        .fold(0u64, u64::saturating_add);
    let global_reclaim_requested_bytes =
        owner_pressure_global_reclaim_request(prepared_bytes, pressure_global_headroom_before);
    // The first pass never touches SSD. Master deletes sources that already
    // have another live backing and returns only exact last-copy candidates.
    let response = inner
        .batch_evict_owner_source(
            prepared
                .iter()
                .map(|(_, victim)| victim.as_ref().clone())
                .collect(),
            global_reclaim_requested_bytes,
            false,
        )
        .await;
    let Ok(response) = response else {
        for (event, victim) in prepared {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "owner source-eviction RPC failed",
            );
        }
        return OwnerPressureBatchReclaim {
            requested_bytes: global_reclaim_requested_bytes,
            selected_bytes: 0,
        };
    };
    let pressure_reclaim = OwnerPressureBatchReclaim {
        requested_bytes: response.global_reclaim_requested_bytes,
        selected_bytes: response.global_reclaim_selected_bytes,
    };
    tracing::info!(
        prepared_bytes,
        pressure_global_headroom_before,
        global_reclaim_requested_bytes = response.global_reclaim_requested_bytes,
        global_reclaim_selected_bytes = response.global_reclaim_selected_bytes,
        "owner pressure batch GlobalShared selection completed"
    );
    if response.victims.len() != prepared.len() {
        for (event, victim) in prepared {
            schedule_owner_source_eviction_retry(
                inner,
                event,
                victim,
                "owner source-eviction response length mismatch",
            );
        }
        return pressure_reclaim;
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

    let first_operation_id = response.operation_id;
    let mut candidates = Vec::new();
    let mut reservation_candidates = Vec::new();
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
        if result.outcome == OwnerSourceEvictionOutcome::SsdCandidate
            && victim.ssd_policy == OwnerSourceSsdPolicy::SelectLastLive
        {
            candidates.push((event, victim));
            continue;
        }
        if result.outcome == OwnerSourceEvictionOutcome::NeedsGlobalReservation {
            reservation_candidates.push((event, victim));
            continue;
        }
        finish_owner_source_eviction_result(
            inner,
            first_operation_id,
            index,
            event,
            victim,
            result,
        )
        .await;
    }

    // The first master pass has now completed all direct deletes, so their
    // speculative reservations have been released.  Retry only the exact
    // last-live subset in one batch; batch_evict_owner_source() can reserve
    // those candidates without letting the much larger duplicate-backed set
    // consume GlobalShared headroom first.  One immediate follow-up is
    // enough; candidates still waiting for a real Free enter the bounded
    // retry queue instead of spinning here.
    submit_owner_source_eviction_decisions_with_followup(
        inner,
        reservation_candidates,
        false,
        "master-screened GlobalShared reservation follow-up RPC failed",
        "master-screened GlobalShared reservation response length mismatch",
        "master-screened GlobalShared reservation response identity mismatch",
    )
    .await;

    if candidates.is_empty() {
        return pressure_reclaim;
    }
    let candidate_lengths = candidates
        .iter()
        .map(|(_, victim)| owner_reclaim_backing_len(&victim.backing))
        .collect::<Vec<_>>();
    let admission = admit_owner_source_ssd_candidates_once(
        &candidate_lengths,
        ssd_batch_available,
        |lengths| {
            inner
                .ssd_storage
                .as_ref()
                .map(|store| store.admit_owner_write_candidates(lengths))
                .unwrap_or_else(|| vec![false; lengths.len()])
        },
    );
    let candidate_items = candidates.len();
    let mut dropped = Vec::new();
    let mut admitted_candidates = Vec::new();
    for ((mut event, victim), is_admitted) in candidates.into_iter().zip(admission) {
        let mut request = victim.as_ref().clone();
        request.ssd_backing_len = None;
        request.ssd_policy = OwnerSourceSsdPolicy::Drop;
        let request = Arc::new(request);
        event.source_eviction_victim = Some(request.clone());
        if is_admitted {
            admitted_candidates.push((event, request));
        } else {
            dropped.push((event, request));
        }
    }

    // Candidates rejected by the byte budget must release their DRAM slots
    // before the admitted durability batch starts. Otherwise one slow SSD
    // flush makes every intentional Drop in the same pressure batch wait,
    // recreating a reclaim backlog even though no SSD queue exists.
    let dropped_items = dropped.len();
    let admitted_items = admitted_candidates.len();
    // Both early Put and last-backing fallback enter the same per-generation
    // local_ssd_put flight. The fallback starts only after explicit admission
    // Drops have released their DRAM slots.
    let operations = reclaim_admission_drops_before_ssd_persist(
        submit_owner_source_eviction_decisions(
            inner,
            dropped,
            "owner SSD admission-drop direct-delete RPC failed",
            "owner SSD admission-drop response length mismatch",
            "owner SSD admission-drop response identity mismatch",
        ),
        || async {
            inner.start_owner_local_ssd_puts(
                admitted_candidates
                    .iter()
                    .map(|(_, victim)| OwnerLocalSsdPutCandidate {
                        key: victim.key.clone(),
                        put_id: victim.put_id,
                        value_len: owner_reclaim_backing_len(&victim.backing),
                        selected_victim: Some(victim.clone()),
                    })
                    .collect(),
                true,
            )
        },
    )
    .await;
    let mut admitted_decisions = Vec::with_capacity(admitted_candidates.len());
    let mut persisted_items = 0usize;
    for ((mut event, victim), operation) in admitted_candidates.into_iter().zip(operations) {
        let outcome = match operation {
            Some(operation) => operation.wait().await,
            None => OwnerLocalSsdPutOutcome::Failed,
        };
        if matches!(
            outcome,
            OwnerLocalSsdPutOutcome::Published | OwnerLocalSsdPutOutcome::AlreadyPresent
        ) {
            persisted_items += 1;
        }
        // Publication, when successful, already attached SSD to the exact
        // route. Direct-delete now removes only memory. Failed/Drop outcomes
        // retain the cache's existing fail-open eviction behavior.
        event.source_eviction_victim = Some(victim.clone());
        admitted_decisions.push((event, victim));
    }
    tracing::info!(
        candidates = candidate_items,
        admitted = admitted_items,
        persisted = persisted_items,
        admission_dropped = dropped_items,
        persist_dropped = admitted_items.saturating_sub(persisted_items),
        "owner last-backing SSD admission completed without queueing excess victims"
    );

    submit_owner_source_eviction_decisions(
        inner,
        admitted_decisions,
        "owner SSD persisted decision direct-delete RPC failed",
        "owner SSD persisted decision response length mismatch",
        "owner SSD persisted decision response identity mismatch",
    )
    .await;
    pressure_reclaim
}

fn admit_owner_source_ssd_candidates_once(
    lengths: &[u64],
    batch_available: &mut bool,
    admit: impl FnOnce(&[u64]) -> Vec<bool>,
) -> Vec<bool> {
    // Splitting source fencing must not multiply the original round's one
    // bounded durability batch, even if tokens refill between source batches.
    if !*batch_available {
        return vec![false; lengths.len()];
    }
    let admitted = admit(lengths);
    assert_eq!(admitted.len(), lengths.len());
    if admitted.iter().any(|allowed| *allowed) {
        *batch_available = false;
    }
    admitted
}

fn owner_source_eviction_pressure_headroom(
    inner: &ClientKvApiInner,
    pressure: bool,
) -> Option<u64> {
    pressure.then(|| {
        let allocator = inner.owner_segment_allocator.lock();
        super::local_reserve_rebalance::owner_pressure_global_headroom(
            allocator.physical_capacity_bytes(),
            allocator.local_target_bytes,
            allocator.global_accounted_bytes(),
        )
    })
}

#[cfg(test)]
#[path = "put_reclaim_tests.rs"]
mod put_reclaim_tests;

fn owner_pressure_global_reclaim_request(
    prepared_bytes: u64,
    pressure_global_headroom_before: Option<u64>,
) -> u64 {
    pressure_global_headroom_before
        .map(|headroom| prepared_bytes.saturating_sub(headroom))
        .unwrap_or(0)
}

#[cfg(test)]
mod owner_pressure_global_reclaim_tests {
    use super::owner_pressure_global_reclaim_request;

    #[test]
    fn only_exact_prepared_bytes_beyond_global_headroom_request_reclaim() {
        assert_eq!(owner_pressure_global_reclaim_request(64, None), 0);
        assert_eq!(owner_pressure_global_reclaim_request(64, Some(96)), 0);
        assert_eq!(owner_pressure_global_reclaim_request(64, Some(16)), 48);
        assert_eq!(owner_pressure_global_reclaim_request(64, Some(0)), 64);
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
                            false,
                        )
                        .await;
                        continue;
                    }
                }
            };
            let Some(dispatch) = dispatch else {
                if !events.is_empty() {
                    process_owner_source_eviction_events(
                        &view_task,
                        std::mem::take(&mut events),
                        false,
                    )
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
                            false,
                        )
                        .await;
                    }
                    pressure_open = true;
                }
                super::OwnerHotEvictionDispatch::EndPressure {
                    selected_bytes,
                    completion,
                } => {
                    if !pressure_open {
                        tracing::warn!(
                            selected_bytes,
                            "owner pressure selection ended without a matching begin marker"
                        );
                    }
                    let pressure = std::mem::replace(&mut pressure_open, false);
                    let global_reclaim = process_owner_source_eviction_events(
                        &view_task,
                        std::mem::take(&mut events),
                        pressure,
                    )
                    .await;
                    if completion.send(global_reclaim).is_err() {
                        tracing::debug!(
                            selected_bytes,
                            global_reclaim_requested_bytes = global_reclaim.requested_bytes,
                            global_reclaim_selected_bytes = global_reclaim.selected_bytes,
                            "owner pressure producer stopped before batch completion ACK"
                        );
                    }
                }
                super::OwnerHotEvictionDispatch::Flush => {
                    if !pressure_open && !events.is_empty() {
                        process_owner_source_eviction_events(
                            &view_task,
                            std::mem::take(&mut events),
                            false,
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

async fn run_owner_local_ssd_put_batch(
    view: ClientKvApiView,
    leaders: Vec<OwnerLocalSsdPutLeader>,
    permit: crate::kv_ssd_storage::KvSsdPersistBatchPermit,
) {
    let inner = view.client_kv_api().inner();
    let mut holders = Vec::with_capacity(leaders.len());
    let mut sources = Vec::with_capacity(leaders.len());
    for leader in &leaders {
        let holder = Arc::new(UserMemHolder::new(
            leader.memory_info.clone(),
            inner.get_or_init_all_memholder_refcount(),
            UserMemHolderExposeKind::SegPtr,
        ));
        sources.push(crate::kv_ssd_storage::KvSsdPersistSource {
            key: leader.op.key.clone(),
            put_id: leader.op.put_id,
            addr: holder.memory_info.addr,
            len: u64::from(holder.memory_info.len),
        });
        holders.push(holder);
    }

    let copies = match inner.copy_local_kvs_for_ssd(&sources).await {
        Ok(copies) => copies,
        Err(err) => {
            drop(holders);
            tracing::warn!(
                items = leaders.len(),
                error = %err,
                "owner local SSD source copy validation failed"
            );
            for leader in leaders {
                inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
            }
            return;
        }
    };
    // Durability now reads only owner-owned copies. The source Moka pins may
    // be released while this flight remains installed through route publish.
    drop(holders);

    let persisted = match inner.persist_copied_local_kvs_to_ssd(permit, copies).await {
        Ok(persisted) => persisted,
        Err(err) => {
            tracing::warn!(
                items = leaders.len(),
                error = %err,
                "owner local SSD durability setup failed"
            );
            for leader in leaders {
                inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
            }
            return;
        }
    };

    let mut publish = Vec::new();
    for ((leader, source), outcome) in leaders
        .into_iter()
        .zip(sources.into_iter())
        .zip(persisted.into_iter())
    {
        match outcome {
            Ok(Some(guard)) => {
                publish.push((leader, source, guard));
            }
            Ok(None) => {
                inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Dropped);
            }
            Err(err) => {
                tracing::warn!(
                    key = leader.op.key,
                    put_time_ms = leader.op.put_id.0,
                    put_version = leader.op.put_id.1,
                    error = %err,
                    "owner local SSD persist failed"
                );
                inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
            }
        }
    }
    if publish.is_empty() {
        return;
    }

    let response = match inner
        .batch_publish_owner_ssd(
            publish
                .iter()
                .map(|(leader, source, _)| OwnerSsdPublishItem {
                    key: leader.op.key.clone(),
                    put_id: leader.op.put_id,
                    len: source.len,
                })
                .collect(),
        )
        .await
    {
        Ok(response) => response,
        Err(err) => {
            tracing::warn!(
                items = publish.len(),
                error = %err,
                "owner local SSD publication was uncertain; retaining durable bytes for replay"
            );
            for (leader, _, _guard) in publish {
                inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
            }
            return;
        }
    };
    if response.items.len() != publish.len() {
        tracing::warn!(
            requested = publish.len(),
            received = response.items.len(),
            "owner local SSD publication response length mismatch; retaining bytes for replay"
        );
        for (leader, _, _guard) in publish {
            inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
        }
        return;
    }

    for ((leader, _, _guard), result) in publish.into_iter().zip(response.items) {
        if result.key != leader.op.key || result.put_id != leader.op.put_id {
            tracing::warn!(
                expected_key = leader.op.key,
                expected_put_time_ms = leader.op.put_id.0,
                expected_put_version = leader.op.put_id.1,
                response_key = result.key,
                response_put_time_ms = result.put_id.0,
                response_put_version = result.put_id.1,
                "owner local SSD publication identity mismatch; retaining bytes for replay"
            );
            inner.finish_owner_local_ssd_put(&leader.op, OwnerLocalSsdPutOutcome::Failed);
            continue;
        }
        let outcome = match result.outcome {
            OwnerSsdPublishOutcome::Published => OwnerLocalSsdPutOutcome::Published,
            OwnerSsdPublishOutcome::AlreadyPresent => OwnerLocalSsdPutOutcome::AlreadyPresent,
            OwnerSsdPublishOutcome::Obsolete => {
                inner
                    .discard_local_ssd_replica(&leader.op.key, leader.op.put_id)
                    .await;
                OwnerLocalSsdPutOutcome::Obsolete
            }
            OwnerSsdPublishOutcome::Rejected => {
                inner
                    .discard_local_ssd_replica(&leader.op.key, leader.op.put_id)
                    .await;
                OwnerLocalSsdPutOutcome::Failed
            }
            OwnerSsdPublishOutcome::RetryableBusy | OwnerSsdPublishOutcome::Unspecified => {
                OwnerLocalSsdPutOutcome::Failed
            }
        };
        inner.finish_owner_local_ssd_put(&leader.op, outcome);
    }
}

async fn revoke_owner_remote_put(
    view: &ClientKvApiView,
    op: &Arc<OwnerRemotePutSharedOp>,
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
}

/// Owns the unique byte/item admission credit for one leader task. Explicit terminal paths call
/// `finish`; unwinding or aborting the task publishes Failed and returns the same permit in Drop.
/// The pointer-checked `finish_owner_remote_put` keeps an old generation from clearing a newer
/// per-key flight, while the permit itself can only be dropped once.
struct OwnerRemotePutTaskGuard {
    view: ClientKvApiView,
    op: Arc<OwnerRemotePutSharedOp>,
    admission_permit: Option<OwnerRemotePutAdmissionPermit>,
    finished: bool,
}

impl OwnerRemotePutTaskGuard {
    fn new(
        view: ClientKvApiView,
        op: Arc<OwnerRemotePutSharedOp>,
        admission_permit: OwnerRemotePutAdmissionPermit,
    ) -> Self {
        Self {
            view,
            op,
            admission_permit: Some(admission_permit),
            finished: false,
        }
    }

    fn finish(&mut self, outcome: OwnerRemotePutOutcome) {
        debug_assert!(!self.finished, "remote Put task finished twice");
        self.view
            .client_kv_api()
            .inner()
            .finish_owner_remote_put(&self.op, outcome);
        self.finished = true;
        drop(self.admission_permit.take());
    }
}

impl Drop for OwnerRemotePutTaskGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        tracing::warn!(
            key = self.op.key,
            put_time_ms = self.op.put_id.0,
            put_version = self.op.put_id.1,
            "owner remote Put leader task dropped before terminal publication"
        );
        let inner = self.view.client_kv_api().inner();
        inner
            .owner_remote_put_counters
            .task_dropped
            .fetch_add(1, Ordering::Relaxed);
        inner.finish_owner_remote_put(&self.op, OwnerRemotePutOutcome::Failed);
        self.finished = true;
        drop(self.admission_permit.take());
    }
}

async fn run_owner_remote_put(
    view: ClientKvApiView,
    op: Arc<OwnerRemotePutSharedOp>,
    holder: Arc<UserMemHolder>,
    mut task_guard: OwnerRemotePutTaskGuard,
) {
    let inner = view.client_kv_api().inner();
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
            revoke_owner_remote_put(&view, &op, None, "length does not fit u32").await;
            task_guard.finish(OwnerRemotePutOutcome::Failed);
            return;
        }
    };

    let (source_slot, source_route_epoch) = match owner_remote_put_source_slot(inner, &op, &holder)
    {
        Ok(source) => source,
        Err(error) => {
            tracing::warn!(
                key = op.key,
                put_time_ms = op.put_id.0,
                put_version = op.put_id.1,
                error = %error,
                "owner remote Put cannot identify its exact source slot"
            );
            drop(holder);
            task_guard.finish(OwnerRemotePutOutcome::Failed);
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
            revoke_owner_remote_put(&view, &op, None, "start error").await;
            task_guard.finish(OwnerRemotePutOutcome::Failed);
            return;
        }
    };
    match append_start.outcome {
        PutAppendStartOutcome::Scheduled => {}
        PutAppendStartOutcome::AlreadySatisfied => {
            drop(holder);
            task_guard.finish(OwnerRemotePutOutcome::AlreadySatisfied);
            return;
        }
        PutAppendStartOutcome::Obsolete => {
            drop(holder);
            task_guard.finish(OwnerRemotePutOutcome::Obsolete);
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
            task_guard.finish(OwnerRemotePutOutcome::Failed);
            return;
        }
    }

    let operation_id = append_start.operation_id;
    if operation_id == 0 || append_start.len != len || append_start.owner_candidates.is_empty() {
        tracing::warn!(
            key = op.key,
            put_time_ms = op.put_id.0,
            put_version = op.put_id.1,
            operation_id,
            source_len = len,
            planned_len = append_start.len,
            candidate_count = append_start.owner_candidates.len(),
            "owner remote Put received an invalid owner-target plan"
        );
        drop(holder);
        revoke_owner_remote_put(&view, &op, Some(operation_id), "invalid owner-target plan").await;
        task_guard.finish(OwnerRemotePutOutcome::Failed);
        return;
    }

    let mut published = false;
    for (candidate_index, target_plan) in append_start.owner_candidates.into_iter().enumerate() {
        let Ok(target_attempt) = u32::try_from(candidate_index.saturating_add(1)) else {
            tracing::warn!(
                key = op.key,
                put_time_ms = op.put_id.0,
                put_version = op.put_id.1,
                operation_id,
                "owner remote Put candidate count exceeded target-attempt identity space"
            );
            break;
        };
        if target_plan.key != op.key
            || target_plan.put_id != op.put_id
            || target_plan.operation.sequence != operation_id
            || target_plan.operation.coordinator != source_slot.owner
            || target_plan.target_owner == source_slot.owner
            || target_plan.plan_nonce != operation_id
        {
            tracing::warn!(
                key = op.key,
                put_time_ms = op.put_id.0,
                put_version = op.put_id.1,
                operation_id,
                target_attempt,
                target_node_id = target_plan.target_owner.node_id,
                "owner remote Put candidate token does not match its source operation"
            );
            break;
        }
        let source_capability = OwnerSourceReadCapability {
            operation: target_plan.operation.clone(),
            target_owner: target_plan.target_owner.clone(),
            target_attempt,
            route: OwnerSourceRouteToken {
                key: op.key.clone(),
                put_id: op.put_id,
                route_epoch: source_route_epoch,
                source: source_slot.clone(),
                atomic_batch: target_plan.atomic_batch.clone(),
                plan_nonce: operation_id,
            },
        };
        let outcome = owner_segment_transfer_one_until_definitive(
            inner,
            &target_plan.target_owner,
            OwnerSegmentTransferItem::PutFromSource {
                op_id: target_plan.operation.clone(),
                target_attempt,
                target_plan: target_plan.clone(),
                source: source_capability,
                disposition: OwnerTargetDisposition::GlobalShared,
                route_commit_mode: OwnerRouteCommitMode::Async,
            },
            "put_from_source",
        )
        .await
        .map(|response| {
            inner
                .owner_transfer_peer_tracker
                .record_terminal(&target_plan.target_owner, response.terminal_sequence);
            response.outcome
        });
        match outcome {
            Ok(OwnerSegmentTransferOutcome::PutFromSourceAcceptedRoutePending {
                target_attempt: completed_attempt,
                source,
                target,
                receipt,
            }) if completed_attempt == target_attempt
                && source == source_slot
                && target.owner == target_plan.target_owner
                && target.len == len
                && receipt.completion_id == operation_id
                && receipt.direction == OwnerTransferDirection::RdmaRead
                && receipt.bytes == len
                && receipt.source.as_ref() == Some(&source_slot)
                && receipt.target == target
                && receipt.source_registration_epoch == source_slot.segment_registration_epoch
                && receipt.target_registration_epoch == target.segment_registration_epoch =>
            {
                tracing::debug!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    "owner remote Put payload accepted; route commit continues asynchronously"
                );
                published = true;
                break;
            }
            Ok(OwnerSegmentTransferOutcome::PutFromSourceCompleted {
                target_attempt: completed_attempt,
                source,
                target,
                receipt,
                route_epoch,
            }) if completed_attempt == target_attempt
                && source == source_slot
                && target.owner == target_plan.target_owner
                && target.len == len
                && receipt.completion_id == operation_id
                && receipt.direction == OwnerTransferDirection::RdmaRead
                && receipt.bytes == len
                && receipt.source.as_ref() == Some(&source_slot)
                && receipt.target == target
                && receipt.source_registration_epoch == source_slot.segment_registration_epoch
                && receipt.target_registration_epoch == target.segment_registration_epoch
                && route_epoch != 0 =>
            {
                tracing::debug!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    route_epoch,
                    "owner remote Put completed through one target-side PutFromSource RPC"
                );
                published = true;
                break;
            }
            Ok(OwnerSegmentTransferOutcome::TargetAborted) => {
                tracing::debug!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    "PutFromSource target aborted before route publication"
                );
            }
            Ok(OwnerSegmentTransferOutcome::Error(error))
                if matches!(
                    error.code,
                    OwnerTransferErrorCode::NoSpace
                        | OwnerTransferErrorCode::Busy
                        | OwnerTransferErrorCode::StaleGeneration
                ) =>
            {
                tracing::debug!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    code = ?error.code,
                    detail = error.detail,
                    "PutFromSource candidate rejected before a committed transfer"
                );
            }
            Ok(OwnerSegmentTransferOutcome::Error(error)) => {
                tracing::warn!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    code = ?error.code,
                    detail = error.detail,
                    "PutFromSource returned a non-retryable terminal"
                );
                break;
            }
            Ok(other) => {
                tracing::warn!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    outcome = ?other,
                    "PutFromSource returned an invalid terminal"
                );
                break;
            }
            Err(error) => {
                let target_current = owner_generation_is_current(inner, &target_plan.target_owner);
                tracing::warn!(
                    key = op.key,
                    put_time_ms = op.put_id.0,
                    put_version = op.put_id.1,
                    operation_id,
                    target_attempt,
                    target_node_id = target_plan.target_owner.node_id,
                    target_current,
                    error = %error,
                    "PutFromSource did not reach a live target generation"
                );
                if !inner.view.register_shutdown_poller().is_running() || target_current {
                    break;
                }
            }
        }
    }

    drop(holder);
    if published {
        task_guard.finish(OwnerRemotePutOutcome::Published);
        return;
    }
    revoke_owner_remote_put(
        &view,
        &op,
        Some(operation_id),
        "no PutFromSource target committed",
    )
    .await;
    task_guard.finish(OwnerRemotePutOutcome::Failed);
}

pub async fn handle_batch_enqueue_replica_tasks(
    view: &ClientKvApiView,
    req: MsgPack<BatchEnqueueReplicaTaskReq>,
) -> MsgPack<BatchEnqueueReplicaTaskResp> {
    let inner = view.client_kv_api().inner();
    let mut items = Vec::with_capacity(req.serialize_part.items.len());
    for item in req.serialize_part.items {
        // This RPC acknowledges admission into the existing per-generation
        // singleflight. Waiting for a follower's payload terminal here lets
        // one slow transfer hold the entire metadata batch past its deadline.
        let accepted = inner.start_remote_put_nonblocking(&item.key, item.put_id, None, false);
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum OwnerLocalPublishCompletion {
    Background,
    Native,
    External,
}

enum OwnerLocalPublishItemState {
    AwaitingMaster { uncertain: bool },
    Accepted,
    Published,
    Rejected(KvError),
}

pub(crate) async fn publish_owner_local_job(view: ClientKvApiView, job: OwnerLocalPublishJob) {
    let _ =
        publish_owner_local_job_with_completion(view, job, OwnerLocalPublishCompletion::Background)
            .await;
}

pub(crate) async fn publish_native_owner_local_job(
    view: ClientKvApiView,
    job: OwnerLocalPublishJob,
) -> Vec<KvResult<()>> {
    assert!(
        job.items.iter().all(|item| item.atomic_group.is_none()),
        "native publication reports independent item terminals"
    );
    assert_eq!(job.items.len(), job.external_pending_contexts.len());
    assert_eq!(job.items.len(), job.key_reservation_ids.len());
    publish_owner_local_job_with_completion(view, job, OwnerLocalPublishCompletion::Native).await
}

pub(crate) async fn publish_external_owner_local_job(
    view: ClientKvApiView,
    job: OwnerLocalPublishJob,
) -> Vec<KvResult<()>> {
    assert_eq!(job.items.len(), job.external_pending_contexts.len());
    assert!(job.key_reservation_ids.is_empty());
    publish_owner_local_job_with_completion(view, job, OwnerLocalPublishCompletion::External).await
}

fn owner_local_publish_rejection_is_terminal(error: &KvError) -> bool {
    matches!(
        error,
        KvError::LeaseMgr(_)
            | KvError::Api(
                ApiError::InvalidArgument { .. }
                    | ApiError::InvalidPutMasterState { .. }
                    | ApiError::KeyAlreadyExists { .. }
                    | ApiError::KeyBeingWritten { .. }
                    | ApiError::OwnerStartTimeMismatch { .. }
            )
    )
}

fn discard_rejected_owner_local_publish(
    inner: &ClientKvApiInner,
    item: &OwnerLocalPublishItem,
) -> Result<(), String> {
    let memory_info = inner
        .precommit_local_visible_memory_info(&item.key)
        .ok_or_else(|| format!("rejected Put precommit slot is absent: key={}", item.key))?;
    if memory_info.local_reserve_resident_slot_ref()
        != Some((
            item.committed_slot.allocation_id,
            item.committed_slot.segment_offset,
            item.committed_slot.capacity_bytes,
        ))
    {
        return Err(format!(
            "rejected Put precommit slot identity changed: key={}",
            item.key
        ));
    }
    if !inner.remove_precommit_local_reserve_resident_slot_if_same(&item.key, &memory_info) {
        return Err(format!(
            "rejected Put precommit slot disappeared during cleanup: key={}",
            item.key
        ));
    }
    // MemoryInfo owns physical release; readers keep the pending slot alive.
    drop(memory_info);
    Ok(())
}

async fn publish_owner_local_job_with_completion(
    view: ClientKvApiView,
    job: OwnerLocalPublishJob,
    completion: OwnerLocalPublishCompletion,
) -> Vec<KvResult<()>> {
    let inner = view.client_kv_api().inner();
    let group_lens = complete_owner_local_publish_group_lens(&job.items);
    let incomplete_declared_group =
        group_lens.is_none() && job.items.iter().any(|item| item.atomic_group.is_some());
    let mut states = job
        .items
        .iter()
        .map(|_| OwnerLocalPublishItemState::AwaitingMaster { uncertain: false })
        .collect::<Vec<_>>();
    let mut shutdown_waiter = view.register_shutdown_waiter();
    let mut retry_delay = OWNER_LOCAL_PUBLISH_RETRY_INITIAL;

    #[cfg(test)]
    inner.test_record.wait_owner_local_publish_admission().await;

    while states.iter().any(|state| {
        !matches!(
            state,
            OwnerLocalPublishItemState::Published | OwnerLocalPublishItemState::Rejected(_)
        )
    }) {
        let requested = (0..job.items.len())
            .filter(|&index| match completion {
                OwnerLocalPublishCompletion::Background => true,
                OwnerLocalPublishCompletion::Native => matches!(
                    states[index],
                    OwnerLocalPublishItemState::AwaitingMaster { .. }
                ),
                OwnerLocalPublishCompletion::External => !matches!(
                    states[index],
                    OwnerLocalPublishItemState::Published | OwnerLocalPublishItemState::Rejected(_)
                ),
            })
            .collect::<Vec<_>>();
        let requested_group_lens = group_lens.as_ref().map(|lens| {
            let mut offset = 0;
            lens.iter()
                .copied()
                .filter(|&len| {
                    let start = offset;
                    offset += len;
                    completion != OwnerLocalPublishCompletion::External
                        || !matches!(states[start], OwnerLocalPublishItemState::Rejected(_))
                })
                .collect::<Vec<_>>()
        });
        let mut retry_reason = None;
        let mut background_batch_accepted = false;
        if incomplete_declared_group {
            retry_reason = Some(
                "declared atomic group is incomplete or non-contiguous in publish job".to_string(),
            );
        } else if !requested.is_empty() {
            let response = if completion != OwnerLocalPublishCompletion::Native
                && let Some(atomic_group_lens) = requested_group_lens
            {
                assert_eq!(atomic_group_lens.iter().sum::<usize>(), requested.len());
                inner
                    .owner_hot_counters
                    .grouped_put_done_batches
                    .fetch_add(1, Ordering::Relaxed);
                inner
                    .owner_hot_counters
                    .grouped_put_done_items
                    .fetch_add(requested.len() as u64, Ordering::Relaxed);
                let items = requested
                    .iter()
                    .map(|&index| {
                        let item = &job.items[index];
                        GroupedBatchPutDoneItemReq {
                            key: item.key.clone(),
                            put_id: item.put_id,
                            lease_id: item.lease_id,
                            committed_slot: Some(item.committed_slot.clone()),
                            publish_local_cache: false,
                            radix: item.radix.clone(),
                        }
                    })
                    .collect();
                inner
                    .grouped_batch_put_done(items, atomic_group_lens)
                    .await
                    .map(|response| response.items)
            } else {
                inner
                    .owner_hot_counters
                    .legacy_put_done_batches
                    .fetch_add(1, Ordering::Relaxed);
                inner
                    .owner_hot_counters
                    .legacy_put_done_items
                    .fetch_add(requested.len() as u64, Ordering::Relaxed);
                let items = requested
                    .iter()
                    .map(|&index| {
                        let item = &job.items[index];
                        BatchPutDoneItemReq {
                            key: item.key.clone(),
                            put_id: item.put_id,
                            lease_id: item.lease_id,
                            committed_slot: Some(item.committed_slot.clone()),
                            publish_local_cache: false,
                            atomic_group: item.atomic_group.clone(),
                            radix: item.radix.clone(),
                        }
                    })
                    .collect();
                inner
                    .batch_put_done(items)
                    .await
                    .map(|response| response.items)
            };
            let response = response
                .map_err(|error| format!("PutDone RPC uncertain: {error}"))
                .and_then(|items| {
                    if items.len() != requested.len()
                        || requested.iter().zip(&items).any(|(&index, response)| {
                            let item = &job.items[index];
                            response.key != item.key || response.put_id != item.put_id
                        })
                    {
                        Err("PutDone response identities do not match the request".to_string())
                    } else {
                        Ok(items)
                    }
                });
            match response {
                Err(reason) => {
                    for &index in &requested {
                        if let OwnerLocalPublishItemState::AwaitingMaster { uncertain } =
                            &mut states[index]
                        {
                            *uncertain = true;
                        }
                    }
                    retry_reason = Some(reason);
                }
                Ok(responses) => {
                    background_batch_accepted = true;
                    for (&index, response) in requested.iter().zip(responses) {
                        let item = &job.items[index];
                        match crate::rpcresp_kvresult_convert::try_from_code(
                            response.error_code,
                            response.error_json,
                        ) {
                            Ok(()) => {
                                if matches!(
                                    states[index],
                                    OwnerLocalPublishItemState::AwaitingMaster { .. }
                                ) {
                                    states[index] = OwnerLocalPublishItemState::Accepted;
                                }
                            }
                            Err(error) => {
                                background_batch_accepted = false;
                                if (completion == OwnerLocalPublishCompletion::Native
                                    || (completion == OwnerLocalPublishCompletion::External
                                        && item.atomic_group.is_none()))
                                    && matches!(
                                        states[index],
                                        OwnerLocalPublishItemState::AwaitingMaster {
                                            uncertain: false
                                        }
                                    )
                                    && owner_local_publish_rejection_is_terminal(&error)
                                {
                                    match discard_rejected_owner_local_publish(inner, item) {
                                        Ok(()) => {
                                            states[index] =
                                                OwnerLocalPublishItemState::Rejected(error);
                                            continue;
                                        }
                                        Err(reason) => {
                                            retry_reason = Some(reason);
                                            continue;
                                        }
                                    }
                                }
                                retry_reason = Some(format!(
                                    "PutDone item unresolved: key={} put_id=({},{}) err={}",
                                    item.key, item.put_id.0, item.put_id.1, error
                                ));
                            }
                        }
                    }
                }
            }
        }

        if completion == OwnerLocalPublishCompletion::Native || background_batch_accepted {
            let mut promoted_indices = Vec::new();
            for (index, state) in states.iter().enumerate() {
                if !matches!(state, OwnerLocalPublishItemState::Accepted) {
                    continue;
                }
                let item = &job.items[index];
                let promotion = if inner.committed_local_reserve_slot_is_current(
                    &item.key,
                    item.put_id,
                    &item.committed_slot,
                ) {
                    Ok(())
                } else {
                    inner
                        .precommit_local_visible_memory_info(&item.key)
                        .ok_or_else(|| {
                            format!("precommit slot missing before promotion: key={}", item.key)
                        })
                        .and_then(|memory_info| {
                            inner
                                .promote_precommit_local_reserve_resident_slot_if_same(
                                    &item.key,
                                    item.put_id,
                                    memory_info,
                                    item.atomic_group.as_ref(),
                                )
                                .map_err(|error| {
                                    format!(
                                        "local promotion unresolved: key={} err={error}",
                                        item.key
                                    )
                                })
                        })
                };
                match promotion {
                    Ok(()) => promoted_indices.push(index),
                    Err(reason) => retry_reason = Some(reason),
                }
            }
            // Atomic groups expose hot/replica side effects only
            // after every member has completed master and local publication.
            if completion == OwnerLocalPublishCompletion::Native
                || promoted_indices.len()
                    == states
                        .iter()
                        .filter(|state| !matches!(state, OwnerLocalPublishItemState::Rejected(_)))
                        .count()
            {
                let promoted_items = promoted_indices
                    .iter()
                    .map(|&index| &job.items[index])
                    .collect::<Vec<_>>();
                for item in &promoted_items {
                    assert!(owner_local_publish_atomic_batch_complete(
                        item,
                        &promoted_items
                    ));
                    let _ = inner.owner_hot_admit_published_committed(&item.key, item.put_id);
                }
                let _ = inner.start_owner_local_ssd_puts(
                    promoted_items
                        .iter()
                        .filter(|item| item.make_replica_task)
                        .map(|item| OwnerLocalSsdPutCandidate {
                            key: item.key.clone(),
                            put_id: item.put_id,
                            value_len: item.value_len,
                            selected_victim: None,
                        })
                        .collect(),
                    false,
                );
                for &index in &promoted_indices {
                    let item = &job.items[index];
                    if owner_local_publish_starts_remote(item) {
                        if let Err(error) = inner
                            .ensure_remote_put(
                                &item.key,
                                item.put_id,
                                item.preferred_sub_cluster.clone(),
                                true,
                            )
                            .await
                        {
                            tracing::warn!(
                                key = item.key,
                                %error,
                                "owner local publish enqueue replica append failed"
                            );
                        }
                    }
                    if let Some(context) = job.external_pending_contexts.get(index) {
                        context._pending_fence.mark_local_put_succeeded();
                    }
                    states[index] = OwnerLocalPublishItemState::Published;
                }
            }
        }
        if states.iter().all(|state| {
            matches!(
                state,
                OwnerLocalPublishItemState::Published | OwnerLocalPublishItemState::Rejected(_)
            )
        }) {
            break;
        }
        tracing::warn!(
            key_count = job.items.len(),
            retry_ms = retry_delay.as_millis(),
            reason = retry_reason
                .as_deref()
                .unwrap_or("publication is still pending"),
            "owner local publish retains unresolved items for retry"
        );
        tokio::select! {
            _ = tokio::time::sleep(retry_delay) => {}
            _ = shutdown_waiter.wait() => break,
        }
        retry_delay = retry_delay
            .saturating_mul(2)
            .min(OWNER_LOCAL_PUBLISH_RETRY_MAX);
    }
    if let Err(error) =
        release_owner_local_publish_reservations(inner, job.key_reservation_ids).await
    {
        if completion == OwnerLocalPublishCompletion::Native {
            inner.native_put_operations.record_cleanup_error(&error);
        }
        tracing::warn!(%error, "owner local publish key reservation cleanup failed");
    }
    states
        .into_iter()
        .map(|state| match state {
            OwnerLocalPublishItemState::Published => Ok(()),
            OwnerLocalPublishItemState::Rejected(error) => Err(error),
            OwnerLocalPublishItemState::AwaitingMaster { .. }
            | OwnerLocalPublishItemState::Accepted => Err(KvError::Api(ApiError::SystemShutdown {
                detail: "owner Put publication stopped before completion".to_string(),
            })),
        })
        .collect()
}

async fn release_owner_local_publish_reservations(
    inner: &ClientKvApiInner,
    key_reservation_ids: Vec<u64>,
) -> KvResult<()> {
    inner
        .batch_release_put_key_reservations(key_reservation_ids)
        .await
        .map(|_| ())
}
