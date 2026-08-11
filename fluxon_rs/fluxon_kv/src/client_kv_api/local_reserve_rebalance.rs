use super::{
    ClientKvApiInner, ClientKvApiView, OwnerHotEvictionDispatch, OwnerLocalReserveGrantState,
    OwnerLocalReservePoolState,
};
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
use crate::master_kv_router::msg_pack::ReserveLocalGrantOutcome;
use limit_thirdparty::tokio;
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

const OWNER_LOCAL_RESERVE_REBALANCE_INTERVAL: Duration = Duration::from_millis(200);
const OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN: Duration = Duration::from_secs(5);
const OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN: Duration = Duration::from_secs(1);
const OWNER_LOCAL_RESERVE_DEFAULT_SOFT_WAIT_TIMEOUT: Duration = Duration::from_millis(10);
// AtomicBatch-safe master route deletion plus owner slot release is asynchronous.
// Thirty seconds turns a recoverable pressure burst into a storage exception that
// can wedge SGLang's detokenizer.  This remains well below the external request
// timeout while allowing bounded backpressure to finish reclaiming slots.
const OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT: Duration = Duration::from_secs(30);
const OWNER_LOCAL_RESERVE_REPORT_INTERVAL: Duration = Duration::from_secs(30);
const OWNER_SLOT_PRESSURE_INTERVAL: Duration = Duration::from_millis(10);
// Source deletion now has an exact, idempotent owner -> master transaction and
// selection debt accounts for candidates until their slots are physically Free.
// Keep pressure retries responsive: a 200 ms gate used to surface directly as
// local_fast_put_start tail latency whenever one bounded kick was insufficient.
const OWNER_SLOT_PRESSURE_MIN_KICK_INTERVAL: Duration = Duration::from_millis(25);
// Start with enough exact missing bytes, but avoid tiny victim-at-a-time loops
// by selecting up to 64 MiB for a small claimant. A still-failed real claim
// doubles the next round; no one round may select more than eight grants.
const OWNER_SLOT_PRESSURE_INITIAL_COARSE_BYTES: u64 = 64 * 1024 * 1024;
const OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES: u64 = OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 8;

#[derive(Debug, Clone, Copy)]
struct ExpectedCapacityLayout {
    value_len: u64,
    slot_size: u64,
    payload_capacity_bytes: u64,
    payload_grant_count: usize,
    physical_capacity_bytes: u64,
    physical_grant_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct RebalanceSnapshot {
    used_bytes: u64,
    free_bytes: u64,
    largest_free_bytes: u64,
    pending_bytes: u64,
    largest_pending_slot_size: u64,
    grant_size_bytes: u64,
    accounting_slot_size: u64,
    allocatable_slots: u64,
    allocatable_bytes: u64,
    slot_unallocatable_bytes: u64,
    pending_size_classes: usize,
    pressure_initial_pop_bytes: u64,
    claim_progress_epoch: u64,
    grant_count: usize,
    expected_grant_count: usize,
    forced_refill_grants: usize,
}

fn div_ceil_usize_bytes(bytes: u64, quantum: u64) -> usize {
    let count = bytes / quantum + u64::from(bytes % quantum != 0);
    usize::try_from(count).unwrap_or(usize::MAX)
}

fn owner_slot_pressure_round_bytes(initial_bytes: u64, round: u32) -> u64 {
    let multiplier = 1u64.checked_shl(round.min(63)).unwrap_or(u64::MAX);
    initial_bytes
        .saturating_mul(multiplier)
        .min(OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES)
}

fn owner_slot_pressure_initial_bytes(snapshot: RebalanceSnapshot) -> u64 {
    // `slot_pressure_capacity` includes only size classes with a failed real
    // OffsetAllocator claim, so queue visibility alone leaves this at zero.
    snapshot.pressure_initial_pop_bytes
}

fn owner_slot_pressure_selected_fence_bytes(inner: &ClientKvApiInner) -> u64 {
    // Candidate debt starts in Moka's synchronous eviction listener. A
    // candidate that is stale, pinned, or waiting in the retry queue has
    // no installed owner fence and cannot release a slot. Only exact selected
    // source debt is valid projected reclaim credit.
    inner
        .owner_hot_counters
        .source_eviction_selected_bytes
        .load(std::sync::atomic::Ordering::Acquire)
}

fn owner_slot_pressure_can_select(selected_fence_bytes: u64) -> bool {
    selected_fence_bytes == 0
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct OwnerPressureBackoff {
    round: u32,
    base_bytes: u64,
    observed_claim_progress_epoch: u64,
}

impl OwnerPressureBackoff {
    fn reset(&mut self, claim_progress_epoch: u64) {
        self.round = 0;
        self.base_bytes = 0;
        self.observed_claim_progress_epoch = claim_progress_epoch;
    }

    fn sync_claim_progress(&mut self, claim_progress_epoch: u64) {
        if self.observed_claim_progress_epoch != claim_progress_epoch {
            self.reset(claim_progress_epoch);
        }
    }

    fn request_bytes(&mut self, initial_bytes: u64) -> u64 {
        self.base_bytes = self.base_bytes.max(initial_bytes);
        owner_slot_pressure_round_bytes(self.base_bytes, self.round)
    }

    fn finish_round(&mut self, selected_bytes: u64) {
        if selected_bytes != 0 {
            self.round = self.round.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum OwnerPressureBatchDispatchError {
    DispatcherClosed,
    CompletionDropped,
}

/// Close one Moka pressure-selection batch and wait until the dispatcher has
/// processed it.  The synchronous eviction listener still uses a nonblocking
/// metadata channel, but the pressure producer may have only one selected
/// batch outstanding at a time.
async fn finish_owner_pressure_batch(
    tx: &limit_thirdparty::tokio::sync::ampsc::UnboundedSender<OwnerHotEvictionDispatch>,
    selected_bytes: u64,
) -> Result<(), OwnerPressureBatchDispatchError> {
    let (completion, completed) = ::tokio::sync::oneshot::channel();
    tx.send(OwnerHotEvictionDispatch::EndPressure {
        selected_bytes,
        completion,
    })
    .map_err(|_| OwnerPressureBatchDispatchError::DispatcherClosed)?;
    completed
        .await
        .map_err(|_| OwnerPressureBatchDispatchError::CompletionDropped)
}

fn expected_capacity_layout(
    value_len: u64,
    payload_capacity_bytes: u64,
    physical_capacity_bytes: u64,
) -> ExpectedCapacityLayout {
    let payload_grant_count = usize::try_from(
        crate::owner_local_reserve_expected_grant_count(value_len, payload_capacity_bytes)
            .expect("owner local-reserve expected capacity must be config-validated"),
    )
    .expect("owner local-reserve expected grant count must fit usize");
    let slot_size = crate::owner_local_reserve_slot_size_bytes(value_len)
        .expect("owner local-reserve expected slot size must be config-validated");
    let physical_grant_count = usize::try_from(crate::owner_local_reserve_physical_grant_count(
        physical_capacity_bytes,
    ))
    .expect("owner local-reserve physical grant count must fit usize");
    assert!(
        physical_grant_count >= payload_grant_count,
        "config validation must fit the expected payload layout in physical owner DRAM"
    );
    ExpectedCapacityLayout {
        value_len,
        slot_size,
        payload_capacity_bytes,
        payload_grant_count,
        physical_capacity_bytes,
        physical_grant_count,
    }
}

fn configured_expected_capacity(inner: &ClientKvApiInner) -> Option<ExpectedCapacityLayout> {
    let expected = inner
        .test_spec_config
        .owner_local_reserve_expected_capacity
        .as_ref()?;
    Some(expected_capacity_layout(
        expected.value_len,
        expected.payload_capacity_bytes,
        inner.owner_local_reserve_physical_capacity_bytes,
    ))
}

fn install_expected_capacity(inner: &ClientKvApiInner) -> Option<ExpectedCapacityLayout> {
    let expected = configured_expected_capacity(inner)?;
    let mut pool = inner.owner_local_reserve_pool.lock();
    assert!(
        pool.grants.is_empty(),
        "expected local-reserve layout must be installed before grants"
    );
    // Prewarm every whole grant in the physical owner contribution. Moka's
    // smaller byte capacity remains the logical resident-set boundary; the
    // extra grants absorb publication/reclaim overlap without becoming cache
    // entries or projected reclaim credit.
    pool.expected_grant_count = expected.physical_grant_count;
    pool.expected_slot_size = Some(expected.slot_size);
    pool.grant_size_bytes = Some(OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES);
    Some(expected)
}

#[derive(Debug, Clone, Copy, Default)]
struct SlotPressureCapacity {
    accounting_slot_size: u64,
    allocatable_slots: u64,
    allocatable_bytes: u64,
    slot_unallocatable_bytes: u64,
    pending_size_classes: usize,
    initial_pop_bytes: u64,
}

fn slot_pressure_capacity(pool: &OwnerLocalReservePoolState) -> SlotPressureCapacity {
    let slot_sizes = BTreeSet::from_iter(pool.pending_demand_by_slot_size.iter().filter_map(
        |(slot_size, count)| {
            (*count != 0 && pool.slot_size_has_failed_claim(*slot_size)).then_some(*slot_size)
        },
    ));
    let Some(accounting_slot_size) = pool
        .expected_slot_size
        .filter(|slot_size| slot_sizes.contains(slot_size))
        .or_else(|| slot_sizes.iter().next_back().copied())
    else {
        return SlotPressureCapacity::default();
    };
    let accounting_report = pool.allocatable_report(accounting_slot_size);
    let mut initial_pop_bytes = 0u64;

    for slot_size in slot_sizes {
        let report = if slot_size == accounting_slot_size {
            accounting_report
        } else {
            pool.allocatable_report(slot_size)
        };
        let pending_slots = u64::try_from(pool.pending_demand_slots(slot_size)).unwrap_or(u64::MAX);
        if pending_slots > report.allocatable_slots {
            let missing_bytes = pending_slots
                .saturating_sub(report.allocatable_slots)
                .saturating_mul(slot_size);
            let coarse_bytes = pending_slots
                .saturating_mul(slot_size)
                .min(OWNER_SLOT_PRESSURE_INITIAL_COARSE_BYTES);
            // All size classes share the same free extents, so their byte
            // shortages are alternative views of one pool. Summing them would
            // double-count capacity; the largest class budget is sufficient
            // for this coarse round.
            initial_pop_bytes = initial_pop_bytes.max(missing_bytes.max(coarse_bytes));
        }
    }

    SlotPressureCapacity {
        accounting_slot_size,
        allocatable_slots: accounting_report.allocatable_slots,
        allocatable_bytes: accounting_report.allocatable_bytes,
        slot_unallocatable_bytes: accounting_report.slot_unallocatable_bytes,
        pending_size_classes: pool
            .pending_demand_by_slot_size
            .values()
            .filter(|count| **count != 0)
            .count(),
        initial_pop_bytes,
    }
}

fn snapshot_rebalance(inner: &ClientKvApiInner) -> RebalanceSnapshot {
    let pool = inner.owner_local_reserve_pool.lock();
    let pressure = slot_pressure_capacity(&pool);
    RebalanceSnapshot {
        used_bytes: pool.total_used_bytes(),
        free_bytes: pool.total_free_bytes(),
        largest_free_bytes: pool.largest_free_bytes(),
        pending_bytes: pool.total_pending_bytes(),
        largest_pending_slot_size: pool.largest_pending_slot_size(),
        grant_size_bytes: pool
            .grant_size_bytes
            .unwrap_or(OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES),
        accounting_slot_size: pressure.accounting_slot_size,
        allocatable_slots: pressure.allocatable_slots,
        allocatable_bytes: pressure.allocatable_bytes,
        slot_unallocatable_bytes: pressure.slot_unallocatable_bytes,
        pending_size_classes: pressure.pending_size_classes,
        pressure_initial_pop_bytes: pressure.initial_pop_bytes,
        claim_progress_epoch: pool.claim_progress_epoch,
        grant_count: pool.grant_count(),
        expected_grant_count: pool.expected_grant_count,
        forced_refill_grants: pool.failed_claim_refill_grants(),
    }
}

fn target_grant_count(snapshot: RebalanceSnapshot) -> usize {
    if snapshot.expected_grant_count != 0 {
        return snapshot.expected_grant_count;
    }
    let demand_target = div_ceil_usize_bytes(
        snapshot.used_bytes.saturating_add(snapshot.pending_bytes),
        snapshot.grant_size_bytes,
    );
    let fragmentation_target = if snapshot.pending_bytes != 0
        && snapshot.largest_free_bytes < snapshot.largest_pending_slot_size
    {
        snapshot.grant_count.saturating_add(1)
    } else {
        snapshot.grant_count
    };
    demand_target.max(fragmentation_target).max(
        snapshot
            .grant_count
            .saturating_add(snapshot.forced_refill_grants),
    )
}

pub(crate) fn owner_local_reserve_timeout_config(inner: &ClientKvApiInner) -> (Duration, Duration) {
    let soft_wait_timeout = inner
        .test_spec_config
        .owner_local_reserve_soft_wait_timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(OWNER_LOCAL_RESERVE_DEFAULT_SOFT_WAIT_TIMEOUT);
    let hard_timeout = inner
        .test_spec_config
        .owner_local_reserve_hard_timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT);
    (soft_wait_timeout, hard_timeout)
}

async fn try_refill_once(inner: &ClientKvApiInner, snapshot: RebalanceSnapshot) {
    let target_grants = target_grant_count(snapshot);
    if snapshot.grant_count >= target_grants {
        return;
    }
    let additional_grants = target_grants - snapshot.grant_count;
    let batch_started_at = Instant::now();
    let mut added_grants = 0usize;
    let mut total_rpc_latency = Duration::ZERO;
    let mut max_rpc_latency = Duration::ZERO;
    for _ in 0..additional_grants {
        let rpc_started_at = Instant::now();
        let outcome = match inner.reserve_local_grant().await {
            Ok(outcome) => {
                let rpc_latency = rpc_started_at.elapsed();
                total_rpc_latency = total_rpc_latency.saturating_add(rpc_latency);
                max_rpc_latency = max_rpc_latency.max(rpc_latency);
                outcome
            }
            Err(err) => {
                let rpc_latency = rpc_started_at.elapsed();
                tracing::warn!(
                    grants = snapshot.grant_count,
                    target_grants,
                    added_grants,
                    pending_bytes = snapshot.pending_bytes,
                    rpc_latency_ms = rpc_latency.as_millis(),
                    error = %err,
                    "owner local reserve offset-pool refill failed"
                );
                return;
            }
        };
        let (grant_id, base_addr, addr, len) = match outcome {
            ReserveLocalGrantOutcome::Granted {
                grant_id,
                node_id: _,
                addr,
                base_addr,
                len,
            } => (grant_id, base_addr, addr, len),
            ReserveLocalGrantOutcome::None => {
                unreachable!("reserve_local_grant filters empty successful outcomes")
            }
        };
        if len != snapshot.grant_size_bytes {
            tracing::warn!(
                requested = snapshot.grant_size_bytes,
                received = len,
                grant_id,
                "owner local reserve offset-pool received an unexpected grant size"
            );
            if let Err(release_err) = inner.release_local_grant(grant_id).await {
                tracing::warn!(
                    grant_id,
                    error = %release_err,
                    "failed to release mismatched local-reserve grant"
                );
            }
            return;
        }
        {
            let mut pool = inner.owner_local_reserve_pool.lock();
            pool.install_grant(OwnerLocalReserveGrantState::new(
                grant_id, base_addr, addr, len,
            ));
            pool.record_refill_grant_installed();
        }
        added_grants += 1;
        inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
    }
    if added_grants != 0 {
        let average_rpc_latency_ms = total_rpc_latency.as_secs_f64() * 1000.0 / added_grants as f64;
        tracing::info!(
            added_grants,
            grants_after = snapshot.grant_count.saturating_add(added_grants),
            target_grants,
            pending_bytes = snapshot.pending_bytes,
            elapsed_ms = batch_started_at.elapsed().as_millis(),
            rpc_avg_ms = average_rpc_latency_ms,
            rpc_max_ms = max_rpc_latency.as_millis(),
            "owner local reserve offset-pool refill completed"
        );
    }
}

fn shrink_target_grants(snapshot: RebalanceSnapshot) -> usize {
    if snapshot.expected_grant_count != 0 {
        snapshot.expected_grant_count
    } else {
        div_ceil_usize_bytes(
            snapshot.used_bytes.saturating_add(snapshot.pending_bytes),
            snapshot.grant_size_bytes,
        )
    }
}

async fn try_shrink_once(inner: &ClientKvApiInner, snapshot: RebalanceSnapshot) {
    if snapshot.grant_count <= shrink_target_grants(snapshot) {
        return;
    }
    let detached = {
        let mut pool = inner.owner_local_reserve_pool.lock();
        if pool
            .last_grow_at
            .is_some_and(|last| last.elapsed() < OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN)
        {
            return;
        }
        let candidate = pool.grants.iter().rev().find_map(|grant| {
            (grant.is_fully_free()
                && grant.fully_free_since.is_some_and(|since| {
                    since.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN
                }))
            .then_some(grant.grant_id)
        });
        candidate.and_then(|grant_id| pool.detach_fully_free_grant(grant_id))
    };
    let Some(grant) = detached else {
        return;
    };
    let grant_id = grant.grant_id;
    if let Err(err) = inner.release_local_grant(grant_id).await {
        tracing::warn!(
            grant_id,
            error = %err,
            "owner offset-pool grant release failed"
        );
        inner.owner_local_reserve_pool.lock().install_grant(grant);
    } else {
        tracing::info!(grant_id, "owner offset-pool released an idle grant");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT, OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN,
        OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES, OwnerPressureBackoff, RebalanceSnapshot,
        expected_capacity_layout, finish_owner_pressure_batch, owner_slot_pressure_can_select,
        owner_slot_pressure_initial_bytes, owner_slot_pressure_round_bytes, slot_pressure_capacity,
        target_grant_count,
    };
    use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
    use crate::client_kv_api::{
        OwnerHotEvictionDispatch, OwnerLocalReserveGrantState, OwnerLocalReservePoolState,
    };
    use std::time::Duration;

    fn empty_snapshot() -> RebalanceSnapshot {
        RebalanceSnapshot {
            used_bytes: 0,
            free_bytes: 0,
            largest_free_bytes: 0,
            pending_bytes: 0,
            largest_pending_slot_size: 0,
            grant_size_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
            accounting_slot_size: 0,
            allocatable_slots: 0,
            allocatable_bytes: 0,
            slot_unallocatable_bytes: 0,
            pending_size_classes: 0,
            pressure_initial_pop_bytes: 0,
            claim_progress_epoch: 0,
            grant_count: 0,
            expected_grant_count: 0,
            forced_refill_grants: 0,
        }
    }

    #[test]
    fn target_grants_cover_used_and_pending_without_double_reserving() {
        assert_eq!(
            target_grant_count(RebalanceSnapshot {
                used_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 2,
                pending_bytes: 1,
                grant_count: 2,
                ..empty_snapshot()
            }),
            3
        );
        assert_eq!(
            target_grant_count(RebalanceSnapshot {
                used_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 2,
                grant_count: 2,
                ..empty_snapshot()
            }),
            2
        );
        assert_eq!(target_grant_count(empty_snapshot()), 0);
    }

    #[test]
    fn exact_fit_target_counts_whole_slots_per_generic_grant() {
        const VALUE_LEN: u64 = 4_718_592;
        const EXPECTED_PAYLOAD_BYTES: u64 = 109_951_162_777;
        let slot_size = crate::owner_local_reserve_slot_size_bytes(VALUE_LEN).unwrap();
        let slots_per_grant = crate::owner_local_reserve_slots_per_grant(slot_size).unwrap();
        let expected_grants =
            crate::owner_local_reserve_expected_grant_count(VALUE_LEN, EXPECTED_PAYLOAD_BYTES)
                .unwrap();
        assert_eq!(slot_size, VALUE_LEN);
        assert_eq!(slots_per_grant, 113);
        assert_eq!(expected_grants, 207);
        assert_eq!(
            target_grant_count(RebalanceSnapshot {
                expected_grant_count: expected_grants as usize,
                ..empty_snapshot()
            }),
            207
        );
    }

    #[test]
    fn logical_payload_and_physical_burst_pool_have_independent_boundaries() {
        const VALUE_LEN: u64 = 4_718_592;
        const PHYSICAL_CAPACITY_BYTES: u64 = 128 * 1024 * 1024 * 1024;
        const PAYLOAD_CAPACITY_BYTES: u64 = 123_695_058_124;

        let layout =
            expected_capacity_layout(VALUE_LEN, PAYLOAD_CAPACITY_BYTES, PHYSICAL_CAPACITY_BYTES);
        assert_eq!(layout.payload_grant_count, 232);
        assert_eq!(layout.physical_grant_count, 256);
        assert_eq!(layout.physical_grant_count - layout.payload_grant_count, 24);
        assert_eq!(
            target_grant_count(RebalanceSnapshot {
                expected_grant_count: layout.physical_grant_count,
                ..empty_snapshot()
            }),
            256
        );
    }

    #[test]
    fn generic_grant_pressure_counts_real_slots_and_size_relative_tail() {
        const SLOT_SIZE: u64 = 4_718_592;
        let mut pool = OwnerLocalReservePoolState {
            expected_slot_size: Some(SLOT_SIZE),
            grant_size_bytes: Some(OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES),
            ..Default::default()
        };
        pool.install_grant(OwnerLocalReserveGrantState::new(
            1,
            1_000_000,
            1_000_000,
            OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
        ));
        let claimed = pool.claim_available(SLOT_SIZE, 110);
        assert_eq!(claimed.len(), 110);
        pool.pending_demand_by_slot_size.insert(SLOT_SIZE, 13);
        pool.record_failed_claim(SLOT_SIZE, 1);
        let pressure = slot_pressure_capacity(&pool);
        assert_eq!(pressure.allocatable_slots, 3);
        assert_eq!(pressure.allocatable_bytes, 3 * SLOT_SIZE);
        assert_eq!(pressure.slot_unallocatable_bytes, 3_670_016);
        assert_eq!(pressure.initial_pop_bytes, 13 * SLOT_SIZE);

        // The 3.5 MiB tail is unavailable to a 4.5 MiB request, but remains
        // useful generic extent capacity for a smaller request.
        assert_eq!(pool.claim_available(3 * 1024 * 1024, 1).len(), 1);
    }

    #[test]
    fn default_hard_timeout_covers_owner_source_eviction_transaction() {
        assert_eq!(
            OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT,
            Duration::from_secs(30)
        );
        assert!(
            OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT > OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN
        );
    }

    #[test]
    fn configured_pool_refills_before_pending_claims_consume_headroom() {
        let configured = RebalanceSnapshot {
            used_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 228,
            free_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 4,
            largest_free_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
            grant_count: 232,
            expected_grant_count: 232,
            ..empty_snapshot()
        };
        assert_eq!(target_grant_count(configured), 232);

        let pressured = RebalanceSnapshot {
            used_bytes: configured.used_bytes + 4 * 1024,
            free_bytes: configured.free_bytes - 4 * 1024,
            ..configured
        };
        assert_eq!(target_grant_count(pressured), 232);
    }

    #[test]
    fn configured_expected_grants_are_a_hard_upper_bound() {
        assert_eq!(
            target_grant_count(RebalanceSnapshot {
                used_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 300,
                pending_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
                grant_count: 300,
                expected_grant_count: 232,
                forced_refill_grants: 20,
                ..empty_snapshot()
            }),
            232
        );
        assert_eq!(
            target_grant_count(RebalanceSnapshot {
                used_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 300,
                pending_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
                largest_pending_slot_size: 64 * 1024 * 1024,
                grant_count: 300,
                ..empty_snapshot()
            }),
            301
        );
    }

    #[test]
    fn fragmented_pending_request_forces_another_grant() {
        let snapshot = RebalanceSnapshot {
            used_bytes: OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES - 64 * 1024 * 1024,
            free_bytes: 64 * 1024 * 1024,
            largest_free_bytes: 32 * 1024 * 1024,
            pending_bytes: 48 * 1024 * 1024,
            largest_pending_slot_size: 48 * 1024 * 1024,
            grant_count: 1,
            ..empty_snapshot()
        };
        assert_eq!(target_grant_count(snapshot), 2);
    }

    #[test]
    fn raw_free_bubble_uses_exact_allocatable_slots_for_pressure() {
        const SLOT_SIZE: u64 = 4_718_592;
        const PENDING_SLOTS: u64 = 261;
        const ALLOCATABLE_SLOTS: u64 = 150;
        let snapshot = RebalanceSnapshot {
            used_bytes: 122_994_819_072,
            free_bytes: 1_559_232_512,
            largest_free_bytes: 218_103_808,
            pending_bytes: PENDING_SLOTS * SLOT_SIZE,
            largest_pending_slot_size: SLOT_SIZE,
            accounting_slot_size: SLOT_SIZE,
            allocatable_slots: ALLOCATABLE_SLOTS,
            allocatable_bytes: ALLOCATABLE_SLOTS * SLOT_SIZE,
            slot_unallocatable_bytes: 851_443_712,
            pending_size_classes: 1,
            pressure_initial_pop_bytes: (PENDING_SLOTS - ALLOCATABLE_SLOTS) * SLOT_SIZE,
            grant_count: 232,
            expected_grant_count: 232,
            forced_refill_grants: 1,
            ..empty_snapshot()
        };
        assert!(snapshot.pending_bytes < snapshot.free_bytes);
        assert!(snapshot.largest_free_bytes > snapshot.largest_pending_slot_size);
        assert!(snapshot.allocatable_bytes < snapshot.pending_bytes);
        assert_eq!(
            owner_slot_pressure_initial_bytes(snapshot),
            523_763_712,
            "raw free bytes must not suppress size-relative slot pressure"
        );
    }

    #[test]
    fn pending_visibility_without_a_real_claim_failure_does_not_start_pressure() {
        const SLOT_SIZE: u64 = 4 * 1024 * 1024;
        let mut pool = OwnerLocalReservePoolState::default();
        pool.pending_demand_by_slot_size.insert(SLOT_SIZE, 16);
        assert_eq!(slot_pressure_capacity(&pool).initial_pop_bytes, 0);

        pool.record_failed_claim(SLOT_SIZE, 1);
        assert_eq!(
            slot_pressure_capacity(&pool).initial_pop_bytes,
            64 * 1024 * 1024
        );
    }

    #[test]
    fn multiple_size_classes_take_the_maximum_instead_of_summing_shared_free_space() {
        const KIB: u64 = 1024;
        let mut pool = OwnerLocalReservePoolState::default();
        pool.install_grant(OwnerLocalReserveGrantState::new(
            1,
            1_000_000,
            1_000_000,
            64 * KIB,
        ));
        pool.pending_demand_by_slot_size.insert(20 * KIB, 4);
        pool.pending_demand_by_slot_size.insert(24 * KIB, 3);
        pool.record_failed_claim(20 * KIB, 1);

        let only_failed_class = slot_pressure_capacity(&pool);
        assert_eq!(only_failed_class.accounting_slot_size, 20 * KIB);
        assert_eq!(only_failed_class.initial_pop_bytes, 80 * KIB);

        // A queued class that has not attempted a real claim cannot borrow
        // another class's failure authorization.
        pool.record_failed_claim(24 * KIB, 1);

        let pressure = slot_pressure_capacity(&pool);
        // 20 KiB: 4 pending vs 3 allocatable => max(20, 80) KiB.
        // 24 KiB: 3 pending vs 2 allocatable => max(24, 72) KiB.
        // Shared capacity means max(80, 72), not their sum.
        assert_eq!(pressure.initial_pop_bytes, 80 * KIB);
    }

    #[test]
    fn pressure_backoff_doubles_and_saturates_then_resets_on_claim_progress() {
        const BASE: u64 = 64 * 1024 * 1024;
        assert_eq!(owner_slot_pressure_round_bytes(BASE, 0), BASE);
        assert_eq!(owner_slot_pressure_round_bytes(BASE, 1), 2 * BASE);
        assert_eq!(owner_slot_pressure_round_bytes(BASE, 2), 4 * BASE);
        assert_eq!(
            owner_slot_pressure_round_bytes(BASE, 20),
            OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES
        );

        let mut backoff = OwnerPressureBackoff::default();
        backoff.finish_round(BASE);
        backoff.finish_round(2 * BASE);
        assert_eq!(backoff.request_bytes(BASE), 4 * BASE);
        backoff.sync_claim_progress(7);
        assert_eq!(backoff.request_bytes(BASE), BASE);
        assert_eq!(backoff.observed_claim_progress_epoch, 7);
    }

    #[test]
    fn installed_source_fence_debt_blocks_the_next_round() {
        assert!(owner_slot_pressure_can_select(0));
        assert!(!owner_slot_pressure_can_select(1));
    }

    #[test]
    fn successful_real_claim_clears_old_failure_debt_and_advances_progress() {
        let mut pool = OwnerLocalReservePoolState {
            claim_progress_epoch: 9,
            ..Default::default()
        };
        pool.record_failed_claim(4 * 1024 * 1024, 3);
        pool.record_failed_claim(8 * 1024 * 1024, 2);
        pool.record_successful_claim(4 * 1024 * 1024);
        assert!(!pool.slot_size_has_failed_claim(4 * 1024 * 1024));
        assert!(pool.slot_size_has_failed_claim(8 * 1024 * 1024));
        assert_eq!(pool.failed_claim_refill_grants(), 2);
        assert_eq!(pool.claim_progress_epoch, 10);
    }

    #[test]
    fn installed_refill_grant_consumes_count_but_keeps_failure_authorization() {
        const SLOT_SIZE: u64 = 4 * 1024 * 1024;
        let mut pool = OwnerLocalReservePoolState::default();
        pool.record_failed_claim(SLOT_SIZE, 1);
        pool.record_refill_grant_installed();

        assert_eq!(pool.failed_claim_refill_grants(), 0);
        assert!(pool.slot_size_has_failed_claim(SLOT_SIZE));
        pool.record_successful_claim(SLOT_SIZE);
        assert!(!pool.slot_size_has_failed_claim(SLOT_SIZE));
    }

    #[limit_thirdparty::tokio::test]
    async fn pressure_batch_finish_waits_for_dispatcher_ack() {
        let (dispatch_tx, mut dispatch_rx) =
            limit_thirdparty::tokio::sync::ampsc::unbounded_channel();
        let producer_tx = dispatch_tx.clone();
        let producer = ::tokio::spawn(async move {
            assert_eq!(
                finish_owner_pressure_batch(&producer_tx, 4096).await,
                Ok(())
            );
            assert!(
                producer_tx
                    .send(OwnerHotEvictionDispatch::BeginPressure {
                        requested_bytes: 8192,
                    })
                    .is_ok()
            );
        });

        let first = dispatch_rx.recv().await.expect("pressure batch end");
        let OwnerHotEvictionDispatch::EndPressure {
            selected_bytes,
            completion,
        } = first
        else {
            panic!("first dispatch must close the selected pressure batch");
        };
        assert_eq!(selected_bytes, 4096);
        ::tokio::task::yield_now().await;
        assert!(
            dispatch_rx.try_recv().is_err(),
            "producer must not start another pressure batch before completion"
        );

        assert!(completion.send(()).is_ok());
        let second = dispatch_rx.recv().await.expect("next pressure batch begin");
        assert!(matches!(
            second,
            OwnerHotEvictionDispatch::BeginPressure {
                requested_bytes: 8192
            }
        ));
        producer.await.expect("pressure producer task");
    }
}

async fn owner_local_reserve_rebalance_once(view: &ClientKvApiView) {
    let inner = view.client_kv_api().inner();
    let snapshot = snapshot_rebalance(inner);
    try_refill_once(inner, snapshot).await;
    try_shrink_once(inner, snapshot_rebalance(inner)).await;
}

fn report_owner_local_reserve_state(inner: &ClientKvApiInner) {
    let snapshot = snapshot_rebalance(inner);
    let expected = configured_expected_capacity(inner);
    let moka_weighted_bytes = inner
        .owner_hot_cache
        .as_ref()
        .map(|cache| cache.weighted_size())
        .unwrap_or(0);
    let pool = inner.owner_local_reserve_pool.lock();
    let accounting_slot_size = pool
        .expected_slot_size
        .unwrap_or(snapshot.accounting_slot_size);
    let capacity_report = pool.allocatable_report(accounting_slot_size);
    let slot_unallocatable_ratio_ppm = if accounting_slot_size == 0 || snapshot.free_bytes == 0 {
        0
    } else {
        capacity_report
            .slot_unallocatable_bytes
            .saturating_mul(1_000_000)
            / snapshot.free_bytes
    };
    tracing::info!(
        grants = snapshot.grant_count,
        expected_grants = snapshot.expected_grant_count,
        payload_grants = expected
            .map(|layout| layout.payload_grant_count)
            .unwrap_or(0),
        physical_target_grants = expected
            .map(|layout| layout.physical_grant_count)
            .unwrap_or(0),
        burst_grants = expected
            .map(|layout| {
                layout
                    .physical_grant_count
                    .saturating_sub(layout.payload_grant_count)
            })
            .unwrap_or(0),
        logical_payload_capacity_bytes = expected
            .map(|layout| layout.payload_capacity_bytes)
            .unwrap_or(0),
        moka_weighted_bytes,
        allocator_over_moka_bytes = snapshot.used_bytes.saturating_sub(moka_weighted_bytes),
        allocator_over_logical_payload_bytes = snapshot.used_bytes.saturating_sub(
            expected
                .map(|layout| layout.payload_capacity_bytes)
                .unwrap_or(0),
        ),
        used_allocations = pool.used_slot_count(),
        prepared_allocations = pool.prepared_slot_count(),
        pending_visible_allocations = pool.pending_visible_slot_count(),
        committed_allocations = pool.committed_slot_count(),
        used_bytes = snapshot.used_bytes,
        free_bytes = snapshot.free_bytes,
        largest_free_bytes = snapshot.largest_free_bytes,
        pending_bytes = snapshot.pending_bytes,
        largest_pending_slot_size = snapshot.largest_pending_slot_size,
        grant_size_bytes = snapshot.grant_size_bytes,
        accounting_slot_size,
        allocatable_slots = capacity_report.allocatable_slots,
        allocatable_bytes = capacity_report.allocatable_bytes,
        slot_unallocatable_bytes = capacity_report.slot_unallocatable_bytes,
        slot_unallocatable_ratio_ppm,
        pending_size_classes = snapshot.pending_size_classes,
        pressure_initial_pop_bytes = snapshot.pressure_initial_pop_bytes,
        claim_progress_epoch = snapshot.claim_progress_epoch,
        forced_refill_grants = snapshot.forced_refill_grants,
        "owner local reserve offset-pool state"
    );
}

pub fn spawn_owner_local_reserve_rebalance_actor(view: ClientKvApiView) {
    let view_task = view.clone();
    view.spawn("owner_local_reserve_rebalance_actor", async move {
        let expected = install_expected_capacity(view_task.client_kv_api().inner());
        if let Some(expected) = expected {
            let physical_reserved_target_bytes = u64::try_from(expected.physical_grant_count)
                .unwrap_or(u64::MAX)
                .saturating_mul(OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES);
            let burst_grants = expected
                .physical_grant_count
                .saturating_sub(expected.payload_grant_count);
            tracing::info!(
                value_len = expected.value_len,
                slot_size = expected.slot_size,
                grant_size_bytes = OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
                payload_capacity_bytes = expected.payload_capacity_bytes,
                payload_grants = expected.payload_grant_count,
                physical_capacity_bytes = expected.physical_capacity_bytes,
                physical_target_grants = expected.physical_grant_count,
                expected_grants = expected.physical_grant_count,
                physical_reserved_target_bytes,
                physical_capacity_tail_bytes = expected
                    .physical_capacity_bytes
                    .saturating_sub(physical_reserved_target_bytes),
                burst_grants,
                logical_physical_headroom_bytes =
                    physical_reserved_target_bytes.saturating_sub(expected.payload_capacity_bytes),
                "owner local reserve offset-pool expected capacity configured"
            );
        }
        let shutdown_poller = view_task.register_shutdown_poller();
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let notify = view_task
            .client_kv_api()
            .inner()
            .owner_local_reserve_rebalance_notify();
        let mut tick = tokio::time::interval(OWNER_LOCAL_RESERVE_REBALANCE_INTERVAL);
        let mut last_report_at = Instant::now();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => {
                    tracing::info!("owner local reserve rebalance actor stopped by shutdown");
                    return;
                }
                _ = tick.tick() => {}
                _ = notify.notified() => {}
            }

            if !shutdown_poller.is_running() {
                tracing::info!("owner local reserve rebalance actor stopped by shutdown");
                return;
            }

            owner_local_reserve_rebalance_once(&view_task).await;
            if last_report_at.elapsed() >= OWNER_LOCAL_RESERVE_REPORT_INTERVAL {
                report_owner_local_reserve_state(view_task.client_kv_api().inner());
                last_report_at = Instant::now();
            }
        }
    });
}

/// Drive owner slot pressure outside RPC handlers and outside Tokio's worker
/// threads.  This is the sole production call site for Moka's synchronous
/// `evict_some`: selection is deliberately bounded, and the returned weight
/// means only "source-eviction candidates selected", never "slots reclaimed".
pub fn spawn_owner_slot_pressure_actor(view: ClientKvApiView) {
    let view_task = view.clone();
    view.spawn("owner_slot_pressure_actor", async move {
        let shutdown_poller = view_task.register_shutdown_poller();
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let notify = view_task
            .client_kv_api()
            .inner()
            .owner_local_reserve_rebalance_notify();
        let mut tick = tokio::time::interval(OWNER_SLOT_PRESSURE_INTERVAL);
        let mut last_kick_at: Option<Instant> = None;
        let mut backoff = OwnerPressureBackoff::default();

        loop {
            tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => {
                    tracing::info!("owner slot pressure actor stopped by shutdown");
                    return;
                }
                _ = tick.tick() => {}
                _ = notify.notified() => {}
            }
            if !shutdown_poller.is_running() {
                return;
            }

            let inner = view_task.client_kv_api().inner();
            let pressure_snapshot = snapshot_rebalance(inner);
            backoff.sync_claim_progress(pressure_snapshot.claim_progress_epoch);
            let initial_bytes = owner_slot_pressure_initial_bytes(pressure_snapshot);
            if initial_bytes == 0 {
                backoff.reset(pressure_snapshot.claim_progress_epoch);
                continue;
            }
            let selected_fence_bytes = owner_slot_pressure_selected_fence_bytes(inner);
            if !owner_slot_pressure_can_select(selected_fence_bytes) {
                // These bytes already have exact source fences and may still
                // be in master retry/owner release handling. They are not
                // projected free capacity, but selecting another round before
                // they finish would duplicate pressure work.
                continue;
            }
            if last_kick_at
                .is_some_and(|last| last.elapsed() < OWNER_SLOT_PRESSURE_MIN_KICK_INTERVAL)
            {
                continue;
            }
            let pressure_round = backoff.round;
            let requested_bytes = backoff.request_bytes(initial_bytes);
            let Some(cache) = inner.owner_hot_cache.clone() else {
                continue;
            };
            if inner
                .owner_hot_eviction_tx
                .send(OwnerHotEvictionDispatch::BeginPressure { requested_bytes })
                .is_err()
            {
                tracing::warn!(
                    requested_bytes,
                    "owner slot pressure dispatcher is closed before Moka selection"
                );
                return;
            }
            let selected_bytes = match limit_thirdparty::tokio::task::spawn_blocking(move || {
                cache.evict_some(requested_bytes)
            })
            .await
            {
                Ok(selected_bytes) => selected_bytes,
                Err(err) => {
                    tracing::warn!(
                        requested_bytes,
                        err = ?err,
                        "owner slot pressure Moka selection task failed"
                    );
                    0
                }
            };
            last_kick_at = Some(Instant::now());
            let dispatch_started_at = Instant::now();
            let dispatch_result = tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => {
                    tracing::info!(
                        requested_bytes,
                        selected_bytes,
                        "owner slot pressure actor stopped while waiting for batch completion"
                    );
                    return;
                }
                result = finish_owner_pressure_batch(
                    &inner.owner_hot_eviction_tx,
                    selected_bytes,
                ) => result,
            };
            if let Err(err) = dispatch_result {
                tracing::warn!(
                    requested_bytes,
                    selected_bytes,
                    err = ?err,
                    "owner slot pressure dispatcher failed to complete selected batch"
                );
                return;
            }
            backoff.finish_round(selected_bytes);
            tracing::debug!(
                requested_bytes,
                selected_bytes,
                pressure_round,
                pressure_initial_pop_bytes = initial_bytes,
                pressure_base_bytes = backoff.base_bytes,
                accounting_slot_size = pressure_snapshot.accounting_slot_size,
                raw_free_bytes = pressure_snapshot.free_bytes,
                allocatable_slots = pressure_snapshot.allocatable_slots,
                allocatable_bytes = pressure_snapshot.allocatable_bytes,
                slot_unallocatable_bytes = pressure_snapshot.slot_unallocatable_bytes,
                slot_unallocatable_ratio_ppm = if pressure_snapshot.free_bytes == 0 {
                    0
                } else {
                    pressure_snapshot
                        .slot_unallocatable_bytes
                        .saturating_mul(1_000_000)
                        / pressure_snapshot.free_bytes
                },
                pending_bytes = pressure_snapshot.pending_bytes,
                pending_size_classes = pressure_snapshot.pending_size_classes,
                claim_progress_epoch = pressure_snapshot.claim_progress_epoch,
                dispatch_elapsed_ms = dispatch_started_at.elapsed().as_millis(),
                "owner slot pressure completed exponential source-eviction round"
            );
        }
    });
}

pub async fn wait_owner_local_reserve_ready(
    inner: &ClientKvApiInner,
    slot_size: u64,
    key_count: usize,
    soft_wait_timeout: Duration,
    hard_deadline: Instant,
) -> bool {
    let notify = inner.owner_local_reserve_rebalance_notify();
    let mut shutdown_waiter = inner.view.register_shutdown_waiter();
    let required_slots = u64::try_from(key_count).unwrap_or(u64::MAX);
    loop {
        let notified = notify.notified();
        {
            let pool = inner.owner_local_reserve_pool.lock();
            if pool.allocatable_report(slot_size).allocatable_slots >= required_slots {
                return true;
            }
        }

        let Some(remaining_hard) = hard_deadline.checked_duration_since(Instant::now()) else {
            return false;
        };
        let wait_budget = remaining_hard.min(soft_wait_timeout);

        tokio::select! {
            _ = shutdown_waiter.wait() => return false,
            _ = tokio::time::sleep(wait_budget) => {
                inner.owner_local_reserve_rebalance_notify().notify_waiters();
            }
            _ = notified => {}
        }
    }
}
