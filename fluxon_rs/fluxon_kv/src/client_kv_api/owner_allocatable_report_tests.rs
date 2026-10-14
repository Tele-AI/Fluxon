use super::{
    ClientKvApiInner, OWNER_ALLOCATABLE_REPORT_CACHE_ENTRIES, OwnerSegmentAllocator,
    OwnerSegmentState, owner_segment_protocol_tests::commit_test_target,
};
use crate::owner_segment::{
    OwnerGeneration, OwnerSegmentTransferOutcome, OwnerSourceRouteToken, OwnerTargetDisposition,
    OwnerTransferOpId, OwnerTransferOpKind, OwnerTransferOutcome,
};
use std::sync::atomic::Ordering;

const KIB: u64 = 1024;
const SLOT: u64 = 8 * KIB;

fn pool(bytes: u64) -> OwnerSegmentAllocator {
    let mut pool = OwnerSegmentAllocator::default();
    pool.install_segment(
        OwnerGeneration::new("target", 11),
        11,
        0x1000,
        0x1000,
        bytes,
        bytes / 2,
        Some(SLOT),
    )
    .unwrap();
    pool
}

fn segment(pool: &OwnerSegmentAllocator) -> &OwnerSegmentState {
    pool.segment.as_ref().unwrap()
}

fn revision(pool: &OwnerSegmentAllocator) -> u64 {
    segment(pool).allocator.layout_revision()
}

fn counters(pool: &OwnerSegmentAllocator) -> (u64, u64, u64) {
    let segment = segment(pool);
    (
        segment
            .allocatable_report_cache_hits
            .load(Ordering::Relaxed),
        segment.allocatable_report_scans.load(Ordering::Relaxed),
        segment
            .allocatable_report_free_nodes_visited
            .load(Ordering::Relaxed),
    )
}

fn assert_matches_oracle(pool: &OwnerSegmentAllocator, size: u64) {
    assert_eq!(
        pool.allocatable_report(size),
        segment(pool).allocatable_report_from_allocations(size),
        "capacity mismatch for slot size {size} at revision {}",
        revision(pool),
    );
}

#[test]
fn report_cache_preserves_send_sync_contracts() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<OwnerSegmentState>();
    send_sync::<OwnerSegmentAllocator>();
    send_sync::<ClientKvApiInner>();
}

#[test]
fn dense_allocations_visit_only_free_nodes_and_reuse_unchanged_reports() {
    const LIVE: usize = 32_768;
    let mut pool = pool((LIVE as u64 + 1) * SLOT);
    assert_eq!(pool.claim_available(SLOT, LIVE).len(), LIVE);
    let report = pool.allocatable_report(SLOT);
    assert_eq!(report.allocatable_slots, 1);
    assert_eq!(counters(&pool), (0, 1, 1));
    for _ in 0..100 {
        assert_eq!(pool.allocatable_report(SLOT), report);
    }
    assert_eq!(counters(&pool), (100, 1, 1));
    assert_eq!(
        report,
        segment(&pool).allocatable_report_from_allocations(SLOT)
    );
}

#[test]
fn fragmented_extents_are_counted_separately_for_every_size() {
    let mut pool = pool(64 * KIB);
    let slots = pool.claim_available(SLOT, 8);
    assert_eq!(slots.len(), 8);
    for index in [0, 2, 4] {
        let slot = &slots[index];
        assert!(pool.release_prepared_slot(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
    }
    for size in [4, 8, 12, 16, 24, 64] {
        assert_matches_oracle(&pool, size * KIB);
    }
    let report = pool.allocatable_report(12 * KIB);
    assert_eq!(report.raw_free_bytes, 24 * KIB);
    assert_eq!(report.allocatable_slots, 0);
    assert_eq!(report.slot_unallocatable_bytes, 24 * KIB);
    assert_eq!(pool.allocatable_report(4 * KIB).allocatable_slots, 6);
    assert_eq!(counters(&pool), (2, 6, 18));
}

#[test]
fn size_cache_is_bounded_and_preserves_earlier_sizes() {
    let pool = pool(1024 * KIB);
    for units in 1..=(OWNER_ALLOCATABLE_REPORT_CACHE_ENTRIES + 4) {
        assert_matches_oracle(&pool, units as u64 * 4 * KIB);
    }
    assert_eq!(
        segment(&pool)
            .allocatable_report_cache
            .iter()
            .filter(|entry| entry.get().is_some())
            .count(),
        OWNER_ALLOCATABLE_REPORT_CACHE_ENTRIES,
    );
    let before = counters(&pool);
    assert_matches_oracle(&pool, 4 * KIB);
    assert_eq!(counters(&pool).0, before.0 + 1);
    assert_matches_oracle(
        &pool,
        (OWNER_ALLOCATABLE_REPORT_CACHE_ENTRIES as u64 + 4) * 4 * KIB,
    );
    assert_eq!(counters(&pool).1, before.1 + 1);
}

#[test]
fn successful_allocate_and_free_invalidate_but_failed_allocate_does_not() {
    let mut pool = pool(64 * KIB);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 8);
    let initial_revision = revision(&pool);
    let initial_free_epoch = pool.physical_free_epoch();
    let slot = pool.claim_available(SLOT, 1).pop().unwrap();
    assert!(revision(&pool) > initial_revision);
    assert_eq!(pool.physical_free_epoch(), initial_free_epoch);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 7);
    let allocated_revision = revision(&pool);
    assert!(pool.claim_available(64 * KIB, 1).is_empty());
    assert_eq!(revision(&pool), allocated_revision);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 7);
    assert_eq!(counters(&pool), (1, 2, 2));
    assert!(pool.release_prepared_slot(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes,
    ));
    assert!(revision(&pool) > allocated_revision);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 8);
    assert_eq!(counters(&pool), (1, 3, 3));
    assert_matches_oracle(&pool, SLOT);
}

#[test]
fn actual_prepare_abort_and_replay_keep_cache_exact() {
    let mut pool = pool(64 * KIB);
    let initial = pool.allocatable_report(SLOT);
    let operation = OwnerTransferOpId::new(
        OwnerGeneration::new("coordinator", 21),
        1,
        OwnerTransferOpKind::Get,
    );
    let (lease_id, slot) = match pool.prepare_target(
        operation.clone(),
        OwnerGeneration::new("target", 11),
        "cancelled-target".to_string(),
        (100, 1),
        5000,
        OwnerTargetDisposition::EphemeralCaller,
        None,
    ) {
        OwnerSegmentTransferOutcome::TargetPrepared { lease_id, slot, .. } => (lease_id, slot),
        other => panic!("expected actual prepared target, got {other:?}"),
    };
    assert_eq!(
        pool.allocatable_report(SLOT).raw_free_bytes,
        initial.raw_free_bytes - slot.capacity_bytes
    );
    let prepared_revision = revision(&pool);
    assert!(matches!(
        pool.abort_target(&operation, &lease_id, "cancel".to_string()),
        OwnerSegmentTransferOutcome::TargetAborted
    ));
    assert!(revision(&pool) > prepared_revision);
    assert_eq!(pool.allocatable_report(SLOT), initial);
    let aborted_revision = revision(&pool);
    assert!(matches!(
        pool.abort_target(&operation, &lease_id, "replay".to_string()),
        OwnerSegmentTransferOutcome::TargetAborted
    ));
    assert_eq!(revision(&pool), aborted_revision);
    assert_eq!(pool.allocatable_report(SLOT), initial);
    assert_eq!(counters(&pool), (1, 3, 3));
}

#[test]
fn route_release_does_not_credit_space_until_last_resident_holder_drops() {
    let mut pool = pool(64 * KIB);
    let slot = pool.claim_available(SLOT, 1).pop().unwrap();
    assert!(pool.mark_prepared_slot_pending_visible(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes
    ));
    assert!(pool.retain_resident_slot_holder(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes
    ));
    assert!(pool.promote_pending_visible_slot_to_committed(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes
    ));
    let held = pool.allocatable_report(SLOT);
    let held_revision = revision(&pool);
    assert!(pool.release_committed_slot_route(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes
    ));
    assert_eq!(revision(&pool), held_revision);
    assert_eq!(pool.allocatable_report(SLOT), held);
    assert_eq!(counters(&pool), (1, 1, 1));
    assert!(pool.release_resident_slot_holder(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes
    ));
    assert!(revision(&pool) > held_revision);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 8);
    assert_eq!(counters(&pool), (1, 2, 2));
}

#[test]
fn source_read_lease_delays_free_and_invalidates_on_its_final_release() {
    let mut pool = pool(64 * KIB);
    let slot = commit_test_target(
        &mut pool,
        1,
        "source",
        (100, 1),
        OwnerTargetDisposition::GlobalShared,
    );
    let operation = OwnerTransferOpId::new(
        OwnerGeneration::new("coordinator", 21),
        2,
        OwnerTransferOpKind::Get,
    );
    let lease_id = match pool.acquire_source(
        operation.clone(),
        OwnerSourceRouteToken {
            key: "source".to_string(),
            put_id: (100, 1),
            route_epoch: 1,
            source: slot.clone(),
            atomic_batch: None,
            plan_nonce: 2,
        },
    ) {
        OwnerSegmentTransferOutcome::SourceAcquired { lease_id, .. } => lease_id,
        other => panic!("expected source lease, got {other:?}"),
    };
    let held = pool.allocatable_report(SLOT);
    let held_revision = revision(&pool);
    assert!(pool.release_committed_slot_route(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes
    ));
    assert_eq!(revision(&pool), held_revision);
    assert_eq!(pool.allocatable_report(SLOT), held);
    assert!(matches!(
        pool.release_source(&operation, &lease_id, OwnerTransferOutcome::Success),
        OwnerSegmentTransferOutcome::SourceReleased
    ));
    assert!(revision(&pool) > held_revision);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 8);
    assert_eq!(counters(&pool), (1, 2, 2));
}

#[test]
fn metadata_only_demotion_reuses_capacity_and_cached_report() {
    let mut pool = pool(64 * KIB);
    let slot = commit_test_target(
        &mut pool,
        1,
        "demote",
        (100, 1),
        OwnerTargetDisposition::LocalExclusive,
    );
    let initial = pool.allocatable_report(SLOT);
    let initial_revision = revision(&pool);
    assert!(
        pool.try_reserve_global_demotion(
            "demote",
            (100, 1),
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes
        )
        .unwrap()
    );
    pool.commit_reserved_global_demotion(
        "demote",
        (100, 1),
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes,
    )
    .unwrap();
    assert_eq!(revision(&pool), initial_revision);
    assert_eq!(pool.allocatable_report(SLOT), initial);
    assert_eq!(counters(&pool), (1, 1, 1));
}

#[test]
fn separate_segment_generations_never_reuse_another_segments_cache() {
    let first = pool(64 * KIB);
    assert_eq!(first.allocatable_report(SLOT).allocatable_slots, 8);
    let mut second = OwnerSegmentAllocator::default();
    second
        .install_segment(
            OwnerGeneration::new("target", 12),
            12,
            0x1000,
            0x1000,
            128 * KIB,
            64 * KIB,
            Some(SLOT),
        )
        .unwrap();
    assert_eq!(revision(&first), revision(&second));
    assert_eq!(second.allocatable_report(SLOT).allocatable_slots, 16);
    assert_eq!(counters(&first), (0, 1, 1));
    assert_eq!(counters(&second), (0, 1, 1));
}

#[test]
fn random_claims_and_rollbacks_match_full_allocation_oracle() {
    let mut pool = pool(2 * 1024 * KIB);
    let mut live = Vec::new();
    let mut random = 0xc68e_35ad_21b9_47f0u64;
    for step in 0..1000 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        if !live.is_empty() && (random % 3 == 0 || live.len() >= 128) {
            let slot: super::OwnerSlotRef = live.swap_remove(random as usize % live.len());
            assert!(pool.release_prepared_slot(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
        } else {
            let size = (1 + random % 32) * 4 * KIB;
            live.extend(pool.claim_available(size, 1));
        }
        for size in [4, 8, 12, 20, 32, 64] {
            assert_matches_oracle(&pool, size * KIB);
            // The second read must hit the cache for this unchanged layout.
            let scans = counters(&pool).1;
            assert_matches_oracle(&pool, size * KIB);
            assert_eq!(counters(&pool).1, scans, "unexpected rescan at step {step}");
        }
    }
}

#[test]
fn metadata_exhaustion_preserves_fail_closed_report_and_free_restores_it() {
    let mut pool = pool(64 * KIB);
    // Exhaust metadata with one allocation and its trailing free region,
    // while preserving the production owner's physical allocation/index path.
    pool.segment.as_mut().unwrap().allocator = offset_allocator::Allocator::with_max_allocs(16, 3);
    assert_eq!(pool.allocatable_report(SLOT).raw_free_bytes, 64 * KIB);
    let slot = pool.claim_available(SLOT, 1).pop().unwrap();
    assert_eq!(pool.total_free_bytes(), 0);
    assert_eq!(
        segment(&pool)
            .allocator
            .storage_report_for_size(2)
            .total_free_space,
        14,
    );
    let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.allocatable_report(SLOT)
    }));
    assert!(
        report.is_err(),
        "metadata exhaustion must retain the old fail-closed boundary"
    );
    assert!(
        segment(&pool)
            .allocatable_report_cache
            .iter()
            .all(|entry| entry.get().is_none()),
        "a rejected geometry report must not enter the cache",
    );
    assert!(pool.release_prepared_slot(
        slot.allocation_id,
        slot.segment_offset,
        slot.capacity_bytes,
    ));
    assert_eq!(pool.allocatable_report(SLOT).raw_free_bytes, 64 * KIB);
    assert_eq!(pool.allocatable_report(SLOT).allocatable_slots, 8);
    assert_matches_oracle(&pool, SLOT);
}
