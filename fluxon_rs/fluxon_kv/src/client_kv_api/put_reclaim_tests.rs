use super::{
    OWNER_SOURCE_EVICTION_RPC_BATCH, admit_owner_source_ssd_candidates_once,
    finish_owner_source_eviction_result, finish_owner_source_selection,
    owner_pressure_global_reclaim_request, owner_source_eviction_pressure_headroom,
    owner_source_eviction_victim, prepare_owner_source_eviction_batch,
    schedule_owner_source_eviction_retry,
};
use crate::client_kv_api::owner_segment_protocol_tests::{VALUE_LEN, commit_test_target};
use crate::client_kv_api::reclaim::complete_owner_source_demotion;
use crate::client_kv_api::{
    ClientKvApi, ClientKvApiInner, ClientKvApiNewArg, ClientKvApiView, GetCachedInfo,
    OwnerHotEvictionEvent, OwnerHotEvictionPreparation, OwnerHotReplicaIdentity,
    OwnerHotSelectionDebt, OwnerHotSelectionFenceOutcome,
};
use crate::master_kv_router::msg_pack::{
    OwnerReclaimBacking, OwnerSourceEvictionOutcome, OwnerSourceEvictionVictim,
    OwnerSourceEvictionVictimResp,
};
use crate::master_seg_manager::msg_pack::{OwnerPlacementClass, SegmentAllocationAuthority};
use crate::memholder::MemoryInfo;
use crate::owner_segment::{
    OwnerGeneration, OwnerSlotDesc, OwnerSlotScope, OwnerTargetDisposition,
};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

struct Source {
    key: String,
    put_id: (u64, u32),
    slot: OwnerSlotDesc,
}

struct Fixture {
    api: ClientKvApi,
    // Keep the backing bytes alive until all indexed holders have dropped.
    _payload: Vec<u8>,
}

fn slot_bytes() -> u64 {
    crate::owner_segment_allocation_capacity_bytes(VALUE_LEN).unwrap()
}

impl Fixture {
    async fn new(slots: usize, local_slots: usize) -> Self {
        let capacity = slots as u64 * slot_bytes();
        let payload = vec![0x5a; capacity as usize + 4096];
        let base = (payload.as_ptr() as u64 + 4095) & !4095;
        // Construct only the local module. No framework, service, transport,
        // shared-memory registration or real cluster is started by this fixture.
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: Default::default(),
            owner_hot_cache_capacity_bytes: Some(capacity),
            owner_local_reserve_physical_capacity_bytes: capacity,
            allocation_authority: SegmentAllocationAuthority::Owner,
            owner_placement_class: OwnerPlacementClass::Inference,
            ssd_storage: None,
        })
        .await
        .unwrap();
        api.inner()
            .owner_segment_allocator
            .lock()
            .install_segment(
                OwnerGeneration::new("target", 11),
                11,
                base,
                base,
                capacity,
                local_slots as u64 * slot_bytes(),
                Some(slot_bytes()),
            )
            .unwrap();
        Self {
            api,
            _payload: payload,
        }
    }

    fn publish(&self, sequence: u64, key: &str, put_id: (u64, u32)) -> Source {
        let inner = self.api.inner();
        let mut pool = inner.owner_segment_allocator.lock();
        let slot = commit_test_target(
            &mut pool,
            sequence,
            key,
            put_id,
            OwnerTargetDisposition::LocalExclusive,
        );
        assert!(pool.retain_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        drop(pool);
        let holder = Arc::new(MemoryInfo::new_local_reserve_resident_with_base(
            slot.addr,
            slot.base_addr,
            VALUE_LEN as u32,
            key.to_string(),
            "target".into(),
            ClientKvApiView {
                view: Weak::<crate::FrameworkInner>::new(),
            },
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(
            inner
                .get_cached_info
                .insert(
                    key.to_string(),
                    GetCachedInfo {
                        put_time_ms: put_id.0,
                        put_version: put_id.1,
                        mem_holder: holder
                    },
                )
                .is_none()
        );
        assert!(inner.owner_hot_admit_published_committed(key, put_id));
        Source {
            key: key.to_string(),
            put_id,
            slot,
        }
    }

    fn event(&self, source: &Source) -> OwnerHotEvictionEvent {
        let inner = self.api.inner();
        let cached = inner.get_cached_info.get(&source.key).unwrap();
        OwnerHotEvictionEvent {
            key: source.key.clone(),
            put_id: source.put_id,
            memory_info: Arc::downgrade(&cached.mem_holder),
            selection_debt: OwnerHotSelectionDebt::new(
                source.slot.capacity_bytes,
                inner.owner_hot_counters.selection_debt_bytes.clone(),
            ),
            retry: false,
            source_eviction_victim: None,
            retry_failures: 0,
        }
    }

    fn scans(&self) -> u64 {
        self.api
            .inner()
            .owner_segment_allocator
            .lock()
            .segment
            .as_ref()
            .unwrap()
            .global_accounting_scans
            .load(Ordering::Relaxed)
    }
}

fn reserve_demotions(
    inner: &ClientKvApiInner,
    prepared: &[(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)],
) {
    let result = inner
        .owner_segment_allocator
        .lock()
        .try_reserve_global_demotions(prepared.iter().map(|(_, victim)| {
            let OwnerReclaimBacking::CommittedSlot {
                allocation_id,
                segment_offset,
                capacity_bytes,
            } = victim.backing
            else {
                panic!("exact committed source required")
            };
            (
                victim.key.as_str(),
                victim.put_id,
                allocation_id,
                segment_offset,
                capacity_bytes,
            )
        }));
    assert_eq!(result.len(), prepared.len());
    assert!(result.into_iter().all(|item| matches!(item, Ok(true))));
}

async fn finish_demotions(
    inner: &ClientKvApiInner,
    prepared: Vec<(OwnerHotEvictionEvent, Arc<OwnerSourceEvictionVictim>)>,
) {
    for (index, (event, victim)) in prepared.into_iter().enumerate() {
        finish_owner_source_eviction_result(
            inner,
            10,
            index,
            event,
            victim,
            OwnerSourceEvictionVictimResp {
                victim_index: index as u32,
                outcome: OwnerSourceEvictionOutcome::DemotedGlobal,
                ssd_backing_committed: false,
                detail: String::new(),
            },
        )
        .await;
    }
}

#[limit_thirdparty::tokio::test]
async fn tail_reads_survive_a_pending_head_batch_and_active_readers_are_skipped() {
    let fixture = Fixture::new(OWNER_SOURCE_EVICTION_RPC_BATCH + 2, 0).await;
    let inner = fixture.api.inner();
    let sources = (0..=OWNER_SOURCE_EVICTION_RPC_BATCH)
        .map(|i| fixture.publish(i as u64 + 1, &format!("batch-{i}"), (100, i as u32)))
        .collect::<Vec<_>>();
    let mut events = sources
        .iter()
        .map(|source| fixture.event(source))
        .collect::<Vec<_>>()
        .into_iter();
    let first = prepare_owner_source_eviction_batch(inner, &mut events);
    assert_eq!(first.len(), OWNER_SOURCE_EVICTION_RPC_BATCH);
    assert_eq!(events.len(), 1);
    assert!(inner.local_visible_mem_holder(&sources[0].key).is_none());

    // Keep master completion pending while a Get reads the queued source.
    let tail = sources.last().unwrap();
    let reader = inner
        .local_visible_mem_holder(&tail.key)
        .expect("tail must remain readable");
    assert!(reader.bytes().iter().all(|byte| *byte == 0x5a));
    assert_eq!(
        inner.owner_source_eviction_selected.len(),
        OWNER_SOURCE_EVICTION_RPC_BATCH
    );
    let before = inner
        .owner_segment_allocator
        .lock()
        .allocatable_report(slot_bytes());
    reserve_demotions(inner, &first);
    let scans = fixture.scans();
    finish_demotions(inner, first).await;
    assert_eq!(
        fixture.scans(),
        scans,
        "256 completions must perform zero accounting scans"
    );
    assert!(inner.owner_source_eviction_selected.is_empty());

    assert!(prepare_owner_source_eviction_batch(inner, &mut events).is_empty());
    assert_eq!(events.len(), 0);
    assert_eq!(
        inner
            .owner_hot_counters
            .skipped_active_holders
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        inner
            .owner_hot_counters
            .selection_debt_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert!(inner.local_visible_mem_holder(&tail.key).is_some());
    let after = inner
        .owner_segment_allocator
        .lock()
        .allocatable_report(slot_bytes());
    assert_eq!(before.allocatable_bytes, after.allocatable_bytes);
    assert_eq!(before.allocatable_slots, after.allocatable_slots);

    drop(reader);
    let next =
        prepare_owner_source_eviction_batch(inner, &mut std::iter::once(fixture.event(tail)));
    assert_eq!(
        next.len(),
        1,
        "a released reader must allow later reselection"
    );
    reserve_demotions(inner, &next);
    finish_demotions(inner, next).await;
    assert!(inner.owner_source_eviction_selected.is_empty());
    assert_eq!(
        inner
            .owner_hot_counters
            .selection_debt_bytes
            .load(Ordering::Relaxed),
        0
    );
}

#[limit_thirdparty::tokio::test]
async fn reader_between_selection_and_fence_rolls_back_only_its_victim() {
    let fixture = Fixture::new(2, 0).await;
    let source = fixture.publish(1, "late-reader", (100, 1));
    let inner = fixture.api.inner();
    let event = fixture.event(&source);
    let OwnerHotEvictionPreparation::Ready {
        trigger,
        source: selected,
    } = inner.owner_hot_prepare_eviction(&event)
    else {
        panic!("unheld source must be selectable")
    };
    let reader = inner.local_visible_mem_holder(&source.key).unwrap();
    assert!(matches!(
        inner.owner_hot_install_source_selection_fence(&trigger, &selected),
        OwnerHotSelectionFenceOutcome::TemporarilyPinned
    ));
    assert!(reader.bytes().iter().all(|byte| *byte == 0x5a));
    assert!(inner.local_visible_mem_holder(&source.key).is_some());
    assert!(inner.owner_source_eviction_selected.is_empty());
    drop((selected, reader));
    let prepared = prepare_owner_source_eviction_batch(inner, &mut std::iter::once(event));
    assert_eq!(prepared.len(), 1);
    finish_owner_source_selection(inner, &prepared[0].1, true, "offline rollback");
    assert!(inner.local_visible_mem_holder(&source.key).is_some());
    assert_eq!(
        inner
            .owner_hot_counters
            .selection_debt_bytes
            .load(Ordering::Relaxed),
        0
    );
}

#[limit_thirdparty::tokio::test]
async fn delayed_candidate_rechecks_both_put_generation_and_allocation_identity() {
    for new_put_id in [(101, 2), (100, 1)] {
        let fixture = Fixture::new(2, 0).await;
        let old = fixture.publish(1, "reused-key", (100, 1));
        let event = fixture.event(&old);
        let inner = fixture.api.inner();
        let (_, cached) = inner.get_cached_info.remove(&old.key).unwrap();
        let mut old_holder = Arc::try_unwrap(cached.mem_holder).unwrap();
        old_holder.take_local_reserve_resident_slot_ref().unwrap();
        drop(old_holder);
        inner.owner_hot_invalidate_version(&old.key, old.put_id);
        assert!(
            inner
                .owner_segment_allocator
                .lock()
                .release_committed_resident_slot(
                    old.slot.allocation_id,
                    old.slot.segment_offset,
                    old.slot.capacity_bytes,
                )
        );
        let new = fixture.publish(2, &old.key, new_put_id);
        assert_eq!(old.slot.segment_offset, new.slot.segment_offset);
        assert_ne!(old.slot.allocation_id, new.slot.allocation_id);
        assert!(prepare_owner_source_eviction_batch(inner, &mut std::iter::once(event)).is_empty());
        assert!(inner.local_visible_mem_holder(&new.key).is_some());
        assert!(inner.owner_source_eviction_selected.is_empty());
        assert_eq!(
            inner
                .owner_hot_counters
                .selection_debt_bytes
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            inner
                .owner_hot_counters
                .source_evict_obsolete
                .load(Ordering::Relaxed),
            1
        );
    }
}

#[limit_thirdparty::tokio::test]
async fn uncertain_retry_keeps_its_fence_and_exact_reservation_in_the_tail() {
    let fixture = Fixture::new(OWNER_SOURCE_EVICTION_RPC_BATCH + 2, 0).await;
    let inner = fixture.api.inner();
    let uncertain = fixture.publish(1, "uncertain", (100, 1));
    let mut selected =
        prepare_owner_source_eviction_batch(inner, &mut std::iter::once(fixture.event(&uncertain)));
    reserve_demotions(inner, &selected);
    let (event, victim) = selected.pop().unwrap();
    let exact_victim = victim.clone();
    schedule_owner_source_eviction_retry(inner, event, victim, "offline uncertain RPC");
    let due = inner
        .owner_hot_retry_queue
        .take_due_batch(Instant::now() + Duration::from_secs(60), 1);
    assert_eq!(due.len(), 1);
    let sources = (0..OWNER_SOURCE_EVICTION_RPC_BATCH)
        .map(|i| fixture.publish(i as u64 + 2, &format!("ahead-{i}"), (101, i as u32)))
        .collect::<Vec<_>>();
    let mut events = sources
        .iter()
        .map(|source| fixture.event(source))
        .chain(due)
        .collect::<Vec<_>>()
        .into_iter();
    let first = prepare_owner_source_eviction_batch(inner, &mut events);
    assert_eq!(first.len(), OWNER_SOURCE_EVICTION_RPC_BATCH);
    assert_eq!(events.len(), 1);
    assert!(inner.local_visible_mem_holder(&uncertain.key).is_none());
    reserve_demotions(inner, &first);
    finish_demotions(inner, first).await;
    assert_eq!(inner.owner_source_eviction_selected.len(), 1);
    assert!(inner.local_visible_mem_holder(&uncertain.key).is_none());
    let retry = prepare_owner_source_eviction_batch(inner, &mut events);
    assert_eq!(retry.len(), 1);
    assert!(Arc::ptr_eq(&retry[0].1, &exact_victim));
    let scans = fixture.scans();
    finish_demotions(inner, retry).await;
    assert_eq!(fixture.scans(), scans);
    assert!(inner.owner_source_eviction_selected.is_empty());
    assert_eq!(
        inner
            .owner_hot_counters
            .selection_debt_bytes
            .load(Ordering::Relaxed),
        0
    );
}

#[limit_thirdparty::tokio::test]
async fn completion_without_reservation_fails_before_hiding_a_readable_source() {
    let fixture = Fixture::new(2, 0).await;
    let source = fixture.publish(1, "unreserved", (100, 1));
    let inner = fixture.api.inner();
    let victim = {
        let cached = inner.get_cached_info.get(&source.key).unwrap();
        owner_source_eviction_victim(
            &OwnerHotReplicaIdentity {
                key: source.key.clone(),
                put_time_ms: source.put_id.0,
                put_version: source.put_id.1,
            },
            &cached.mem_holder,
            false,
        )
        .unwrap()
    };
    let scans = fixture.scans();
    let error = complete_owner_source_demotion(inner, &victim, 10).unwrap_err();
    assert!(
        error.contains("no exact owner budget reservation"),
        "{error}"
    );
    assert_eq!(fixture.scans(), scans);
    assert!(inner.local_visible_mem_holder(&source.key).is_some());
    assert!(
        inner
            .owner_key_control
            .lock_key(&source.key)
            .get(&source.key)
            .is_none()
    );
    let pool = inner.owner_segment_allocator.lock();
    assert_eq!(pool.global_accounted_bytes(), 0);
    assert_eq!(
        pool.manifest_entry(&source.key, source.put_id)
            .unwrap()
            .scope,
        Some(OwnerSlotScope::LocalExclusive)
    );
}

#[limit_thirdparty::tokio::test]
async fn exact_demotion_rejects_stale_identities_and_replays_without_physical_reclaim() {
    let fixture = Fixture::new(2, 0).await;
    let source = fixture.publish(1, "reserved", (100, 1));
    let inner = fixture.api.inner();
    let prepared =
        prepare_owner_source_eviction_batch(inner, &mut std::iter::once(fixture.event(&source)));
    reserve_demotions(inner, &prepared);
    let victim = &prepared[0].1;
    let mut bad = Vec::new();
    let mut wrong_key = victim.as_ref().clone();
    wrong_key.key.push_str("-other");
    bad.push(wrong_key);
    let mut wrong_put = victim.as_ref().clone();
    wrong_put.put_id.1 += 1;
    bad.push(wrong_put);
    for (allocation_id, segment_offset, capacity_bytes) in [
        (
            source.slot.allocation_id + 1,
            source.slot.segment_offset,
            source.slot.capacity_bytes,
        ),
        (
            source.slot.allocation_id,
            source.slot.segment_offset + 4096,
            source.slot.capacity_bytes,
        ),
        (
            source.slot.allocation_id,
            source.slot.segment_offset,
            source.slot.capacity_bytes + 4096,
        ),
    ] {
        let mut wrong_slot = victim.as_ref().clone();
        wrong_slot.backing = OwnerReclaimBacking::CommittedSlot {
            allocation_id,
            segment_offset,
            capacity_bytes,
        };
        bad.push(wrong_slot);
    }
    let scans = fixture.scans();
    for wrong in bad {
        assert!(complete_owner_source_demotion(inner, &wrong, 10).is_err());
    }
    assert_eq!(fixture.scans(), scans);
    assert!(inner.local_visible_mem_holder(&source.key).is_none());
    let before_free_epoch = inner.owner_segment_allocator.lock().physical_free_epoch();
    finish_demotions(inner, prepared).await;
    assert_eq!(fixture.scans(), scans);
    let mut pool = inner.owner_segment_allocator.lock();
    pool.commit_reserved_global_demotion(
        &source.key,
        source.put_id,
        source.slot.allocation_id,
        source.slot.segment_offset,
        source.slot.capacity_bytes,
    )
    .unwrap();
    assert_eq!(pool.physical_free_epoch(), before_free_epoch);
    assert_eq!(pool.total_free_bytes(), slot_bytes());
    assert_eq!(pool.used_slot_count(), 1);
    assert_eq!(pool.global_accounted_bytes(), slot_bytes());
    let manifest = pool.manifest_entry(&source.key, source.put_id).unwrap();
    assert_eq!(manifest.slot, source.slot);
    assert_eq!(manifest.scope, Some(OwnerSlotScope::GlobalShared));
    assert_eq!(manifest.route_epoch, 1);
    drop(pool);
    assert!(inner.owner_source_eviction_selected.is_empty());
    assert!(
        inner
            .owner_key_control
            .lock_key(&source.key)
            .get(&source.key)
            .is_none()
    );
}

#[limit_thirdparty::tokio::test]
async fn pressure_refreshes_consumed_headroom_and_only_real_free_restores_capacity() {
    let fixture = Fixture::new(3, 1).await;
    let sources = (0..3)
        .map(|i| fixture.publish(i + 1, &format!("pressure-{i}"), (100, i as u32)))
        .collect::<Vec<_>>();
    let inner = fixture.api.inner();
    let scans = fixture.scans();
    assert_eq!(owner_source_eviction_pressure_headroom(inner, false), None);
    assert_eq!(
        fixture.scans(),
        scans,
        "ordinary Moka eviction needs no pressure snapshot"
    );
    let first = prepare_owner_source_eviction_batch(
        inner,
        &mut sources[..2].iter().map(|source| fixture.event(source)),
    );
    assert_eq!(
        owner_pressure_global_reclaim_request(
            2 * slot_bytes(),
            owner_source_eviction_pressure_headroom(inner, true)
        ),
        0
    );
    let free_epoch = inner.owner_segment_allocator.lock().physical_free_epoch();
    reserve_demotions(inner, &first);
    assert_eq!(
        owner_source_eviction_pressure_headroom(inner, true),
        Some(0)
    );
    finish_demotions(inner, first).await;
    assert_eq!(
        inner.owner_segment_allocator.lock().physical_free_epoch(),
        free_epoch
    );
    assert_eq!(inner.owner_segment_allocator.lock().total_free_bytes(), 0);

    let next = prepare_owner_source_eviction_batch(
        inner,
        &mut std::iter::once(fixture.event(&sources[2])),
    );
    assert_eq!(
        owner_pressure_global_reclaim_request(
            slot_bytes(),
            owner_source_eviction_pressure_headroom(inner, true)
        ),
        slot_bytes()
    );
    let global = &sources[0].slot;
    assert!(
        inner
            .owner_segment_allocator
            .lock()
            .release_committed_slot_route(
                global.allocation_id,
                global.segment_offset,
                global.capacity_bytes
            )
    );
    assert_eq!(
        inner.owner_segment_allocator.lock().total_free_bytes(),
        slot_bytes()
    );
    assert!(inner.owner_segment_allocator.lock().physical_free_epoch() > free_epoch);
    assert_eq!(
        owner_pressure_global_reclaim_request(
            slot_bytes(),
            owner_source_eviction_pressure_headroom(inner, true)
        ),
        0
    );
    reserve_demotions(inner, &next);
    finish_demotions(inner, next).await;
    assert!(inner.owner_source_eviction_selected.is_empty());
}

#[limit_thirdparty::tokio::test]
async fn reservation_rollback_and_budget_changes_refresh_logical_headroom_only() {
    let fixture = Fixture::new(3, 1).await;
    let source = fixture.publish(1, "rollback", (100, 1));
    let inner = fixture.api.inner();
    let prepared =
        prepare_owner_source_eviction_batch(inner, &mut std::iter::once(fixture.event(&source)));
    let free_before = inner.owner_segment_allocator.lock().total_free_bytes();
    reserve_demotions(inner, &prepared);
    assert_eq!(
        owner_source_eviction_pressure_headroom(inner, true),
        Some(slot_bytes())
    );
    finish_owner_source_selection(inner, &prepared[0].1, true, "offline rollback");
    assert_eq!(
        owner_source_eviction_pressure_headroom(inner, true),
        Some(2 * slot_bytes())
    );
    inner
        .owner_segment_allocator
        .lock()
        .apply_target_control(2, 3 * slot_bytes())
        .unwrap();
    assert_eq!(
        owner_source_eviction_pressure_headroom(inner, true),
        Some(0)
    );
    assert_eq!(
        inner.owner_segment_allocator.lock().total_free_bytes(),
        free_before
    );
    assert!(inner.local_visible_mem_holder(&source.key).is_some());
    assert!(inner.owner_source_eviction_selected.is_empty());
    assert_eq!(
        inner
            .owner_hot_counters
            .selection_debt_bytes
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn source_batches_share_one_ssd_durability_admission_even_after_a_failed_persist() {
    let mut available = true;
    let mut admission_calls = 0;
    assert_eq!(
        admit_owner_source_ssd_candidates_once(&[16, 32], &mut available, |_| {
            admission_calls += 1;
            vec![false, false]
        }),
        [false, false]
    );
    assert!(
        available,
        "a rejected byte admission consumes no durability batch"
    );
    assert_eq!(
        admit_owner_source_ssd_candidates_once(&[8, 16], &mut available, |_| {
            admission_calls += 1;
            vec![true, false]
        }),
        [true, false]
    );
    assert!(!available);
    // A busy gate or failed persist after this admission does not grant a
    // second durability attempt to another source batch in the same round.
    for _ in 0..3 {
        assert_eq!(
            admit_owner_source_ssd_candidates_once(&[1, 2, 3], &mut available, |_| {
                admission_calls += 1;
                vec![true; 3]
            }),
            [false; 3]
        );
    }
    assert_eq!(
        admission_calls, 2,
        "later batches must not debit SSD byte tokens"
    );
}
