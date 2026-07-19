use super::{
    ClientKvApiInner, ClientKvApiView, OwnerHotEvictionDispatch, OwnerLocalReserveClassState,
    OwnerLocalReservePoolState,
};
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
use crate::master_kv_router::msg_pack::ReserveLocalGrantOutcome;
use limit_thirdparty::tokio;
use std::time::{Duration, Instant};

const OWNER_LOCAL_RESERVE_REBALANCE_INTERVAL: Duration = Duration::from_millis(200);
const OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN: Duration = Duration::from_secs(5);
const OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN: Duration = Duration::from_secs(1);
const OWNER_LOCAL_RESERVE_DEFAULT_SOFT_WAIT_TIMEOUT: Duration = Duration::from_millis(10);
// AtomicBatch-safe master route deletion plus owner slot release is asynchronous.
// Thirty seconds turns a recoverable pressure burst into a storage exception that
// can wedge SGLang's detokenizer.  This remains well below the external request
// timeout while allowing bounded backpressure to finish reclaiming slots.
const OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT: Duration = Duration::from_secs(30);
const OWNER_LOCAL_RESERVE_MIN_GRANTS_PER_CLASS: usize = 0;
const OWNER_LOCAL_RESERVE_REPORT_INTERVAL: Duration = Duration::from_secs(30);
const OWNER_LOCAL_RESERVE_FREE_SLOT_HEADROOM_GRANTS: usize = 4;
const OWNER_SLOT_PRESSURE_INTERVAL: Duration = Duration::from_millis(10);
// Source deletion now has an exact, idempotent owner -> master transaction and
// selection debt accounts for candidates until their slots are physically Free.
// Keep pressure retries responsive: a 200 ms gate used to surface directly as
// local_fast_put_start tail latency whenever one bounded kick was insufficient.
const OWNER_SLOT_PRESSURE_MIN_KICK_INTERVAL: Duration = Duration::from_millis(25);
// One 512 MiB grant contains 113 production KV slots.  The old 256 MiB cap was
// smaller than a single grant and could never refill the configured four-grant
// high watermark in one pass.  Bound a kick at eight grants so it covers the
// four-grant retained headroom plus a normal large claimant, while selection
// debt still prevents repeated over-selection before physical reclaim lands.
const OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES: u64 = OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES * 8;

#[derive(Debug, Clone, Copy)]
struct ExpectedCapacityLayout {
    value_len: u64,
    payload_capacity_bytes: u64,
    slot_size: u64,
    slots_per_grant: u32,
    grant_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct RebalanceClassSnapshot {
    slot_size: u64,
    slots_per_grant: u32,
    used_slots: usize,
    free_slots: usize,
    grant_count: usize,
    pending_slot_demand: usize,
    max_observed_claim_slots: usize,
    expected_grant_count: usize,
}

fn owner_local_reserve_target_grant_count(
    slots_per_grant: u32,
    used_slots: usize,
    pending_slot_demand: usize,
    expected_grant_count: usize,
) -> usize {
    assert!(
        slots_per_grant > 0,
        "local-reserve grant must contain slots"
    );
    let required_slots = used_slots.saturating_add(pending_slot_demand);
    let slots_per_grant = slots_per_grant as usize;
    let demand_grants =
        required_slots / slots_per_grant + usize::from(required_slots % slots_per_grant != 0);
    if expected_grant_count == 0 {
        demand_grants
    } else {
        // A configured expected capacity is the owner's physical slot budget,
        // not a minimum that demand may grow past.  Once all expected grants
        // are installed, additional demand must be served by owner-local
        // source eviction/reclaim; asking the master for another grant silently
        // changes the configured owner capacity and can starve the remote
        // allocation domain.
        expected_grant_count
    }
}

fn owner_slot_pressure_request_bytes(
    snapshot: RebalanceClassSnapshot,
    source_eviction_selected_bytes: u64,
) -> u64 {
    let low_watermark = (snapshot.slots_per_grant as usize)
        .saturating_mul(2)
        .max(snapshot.max_observed_claim_slots);
    let high_watermark = (snapshot.slots_per_grant as usize)
        .saturating_mul(OWNER_LOCAL_RESERVE_FREE_SLOT_HEADROOM_GRANTS);
    let projected_eviction_slots =
        usize::try_from(source_eviction_selected_bytes / snapshot.slot_size.max(1))
            .unwrap_or(usize::MAX);
    let projected_free_slots = snapshot.free_slots.saturating_add(projected_eviction_slots);
    let pressure_active =
        snapshot.pending_slot_demand > snapshot.free_slots || projected_free_slots < low_watermark;
    if !pressure_active {
        return 0;
    }
    let desired_free_slots = snapshot.pending_slot_demand.saturating_add(high_watermark);
    let selection_slots = desired_free_slots.saturating_sub(projected_free_slots);
    u64::try_from(selection_slots)
        .unwrap_or(u64::MAX)
        .saturating_mul(snapshot.slot_size)
        .min(OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES)
}

fn owner_slot_pressure_projected_reclaim_bytes(inner: &ClientKvApiInner) -> u64 {
    // Candidate debt starts in Moka's synchronous eviction listener. A
    // candidate that is stale, pinned, or waiting in the retry queue has
    // no installed owner fence and cannot release a slot. Only exact selected
    // source debt is valid projected reclaim credit.
    inner
        .owner_hot_counters
        .source_eviction_selected_bytes
        .load(std::sync::atomic::Ordering::Acquire)
}

fn owner_local_reserve_desired_free_slots(snapshot: RebalanceClassSnapshot) -> usize {
    let configured_headroom = if snapshot.expected_grant_count == 0 {
        0
    } else {
        (snapshot.slots_per_grant as usize)
            .saturating_mul(OWNER_LOCAL_RESERVE_FREE_SLOT_HEADROOM_GRANTS)
    };
    snapshot
        .pending_slot_demand
        .saturating_add(configured_headroom)
}

fn configured_expected_capacity(inner: &ClientKvApiInner) -> Option<ExpectedCapacityLayout> {
    let expected = inner
        .test_spec_config
        .owner_local_reserve_expected_capacity
        .as_ref()?;
    let slot_size = crate::owner_local_reserve_slot_size_bytes(expected.value_len)
        .expect("owner local-reserve expected capacity must be config-validated");
    let slots_per_grant = crate::owner_local_reserve_slots_per_grant(slot_size)
        .expect("validated local-reserve slot size must fit in a grant");
    let grant_count = usize::try_from(
        crate::owner_local_reserve_expected_grant_count(
            expected.value_len,
            expected.payload_capacity_bytes,
        )
        .expect("owner local-reserve expected capacity must be config-validated"),
    )
    .expect("owner local-reserve expected grant count must fit usize");
    Some(ExpectedCapacityLayout {
        value_len: expected.value_len,
        payload_capacity_bytes: expected.payload_capacity_bytes,
        slot_size,
        slots_per_grant,
        grant_count,
    })
}

fn install_expected_capacity_class(inner: &ClientKvApiInner) -> Option<ExpectedCapacityLayout> {
    let expected = configured_expected_capacity(inner)?;
    let mut pool = inner.owner_local_reserve_pool.lock();
    let class_state = pool.classes.entry(expected.slot_size).or_insert_with(|| {
        OwnerLocalReserveClassState::new(expected.slot_size, expected.slots_per_grant)
    });
    assert_eq!(class_state.slots_per_grant, expected.slots_per_grant);
    class_state.expected_grant_count = expected.grant_count;
    Some(expected)
}

fn snapshot_rebalance_classes(pool: &OwnerLocalReservePoolState) -> Vec<RebalanceClassSnapshot> {
    pool.classes
        .values()
        .map(|class_state| RebalanceClassSnapshot {
            slot_size: class_state.slot_size,
            slots_per_grant: class_state.slots_per_grant,
            used_slots: class_state.used_slot_count(),
            free_slots: class_state.free_slot_count(),
            grant_count: class_state.grant_count(),
            pending_slot_demand: class_state.pending_slot_demand,
            max_observed_claim_slots: class_state.max_observed_claim_slots,
            expected_grant_count: class_state.expected_grant_count,
        })
        .collect()
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

async fn try_refill_once(inner: &ClientKvApiInner, snapshot: RebalanceClassSnapshot) {
    if snapshot.slot_size > OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES {
        tracing::error!(
            "owner local reserve refill rejected oversized slot_size={} quantum={} pending_slots={}",
            snapshot.slot_size,
            OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
            snapshot.pending_slot_demand
        );
        return;
    }
    let desired_free_slots = owner_local_reserve_desired_free_slots(snapshot);
    let target_grants = owner_local_reserve_target_grant_count(
        snapshot.slots_per_grant,
        snapshot.used_slots,
        desired_free_slots,
        snapshot.expected_grant_count,
    );
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
                    "owner local reserve refill failed: slot_size={} pending_slots={} grants={} target_grants={} expected_grants={} added_grants={} rpc_latency_ms={} err={}",
                    snapshot.slot_size,
                    snapshot.pending_slot_demand,
                    snapshot.grant_count,
                    target_grants,
                    snapshot.expected_grant_count,
                    added_grants,
                    rpc_latency.as_millis(),
                    err
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
        if len != OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES {
            tracing::warn!(
                "owner local reserve refill got unexpected grant size: requested={} got={}",
                OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
                len
            );
            if let Err(release_err) = inner.release_local_grant(grant_id).await {
                tracing::warn!(
                    "owner local reserve refill failed to release mismatched grant_id={} err={}",
                    grant_id,
                    release_err
                );
            }
            return;
        }
        let detached_grant = super::OwnerLocalReserveGrantState::new(
            grant_id,
            base_addr,
            addr,
            len,
            snapshot.slot_size,
            snapshot.slots_per_grant,
        );
        {
            let mut pool = inner.owner_local_reserve_pool.lock();
            let class_state = pool.classes.entry(snapshot.slot_size).or_insert_with(|| {
                OwnerLocalReserveClassState::new(snapshot.slot_size, snapshot.slots_per_grant)
            });
            class_state.last_grow_at = Some(Instant::now());
            class_state.install_grant(detached_grant);
        }
        added_grants += 1;
        inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
    }
    if added_grants != 0 {
        let average_rpc_latency_ms = total_rpc_latency.as_secs_f64() * 1000.0 / added_grants as f64;
        tracing::info!(
            "owner local reserve refill batch completed: slot_size={} added_grants={} grants_after={} target_grants={} expected_grants={} pending_slots={} elapsed_ms={} rpc_avg_ms={:.3} rpc_max_ms={}",
            snapshot.slot_size,
            added_grants,
            snapshot.grant_count.saturating_add(added_grants),
            target_grants,
            snapshot.expected_grant_count,
            snapshot.pending_slot_demand,
            batch_started_at.elapsed().as_millis(),
            average_rpc_latency_ms,
            max_rpc_latency.as_millis()
        );
    }
}

fn detach_excess_fully_free_grant(
    class_state: &mut OwnerLocalReserveClassState,
) -> Option<super::OwnerLocalReserveGrantState> {
    let grow_cooldown_ok = class_state
        .last_grow_at
        .map(|last_grow_at| last_grow_at.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN)
        .unwrap_or(true);
    if !grow_cooldown_ok {
        return None;
    }
    let shrink_keep_grants = owner_local_reserve_target_grant_count(
        class_state.slots_per_grant,
        class_state.used_slot_count(),
        class_state.pending_slot_demand,
        class_state.expected_grant_count,
    )
    .max(OWNER_LOCAL_RESERVE_MIN_GRANTS_PER_CLASS);
    if class_state.grant_count() <= shrink_keep_grants {
        return None;
    }

    let candidate_grant_id = class_state.grants.last().and_then(|grant| {
        (grant.is_fully_free()
            && grant
                .fully_free_since
                .map(|since| since.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN)
                .unwrap_or(false))
        .then_some(grant.grant_id)
    })?;
    class_state.detach_fully_free_grant(candidate_grant_id)
}

async fn try_shrink_once(inner: &ClientKvApiInner, snapshot: RebalanceClassSnapshot) {
    let detached = {
        let mut pool = inner.owner_local_reserve_pool.lock();
        let Some(class_state) = pool.classes.get_mut(&snapshot.slot_size) else {
            return;
        };
        detach_excess_fully_free_grant(class_state)
    };

    let Some(grant) = detached else {
        return;
    };

    if let Err(err) = inner.release_local_grant(grant.grant_id).await {
        tracing::warn!(
            "owner local reserve shrink failed to release grant_id={} err={}",
            grant.grant_id,
            err
        );
        let mut pool = inner.owner_local_reserve_pool.lock();
        let class_state = pool.classes.entry(snapshot.slot_size).or_insert_with(|| {
            OwnerLocalReserveClassState::new(snapshot.slot_size, snapshot.slots_per_grant)
        });
        class_state.install_grant(grant);
    } else {
        tracing::info!(
            "owner local reserve released excess grant: slot_size={} grant_id={} pending_slots={}",
            snapshot.slot_size,
            grant.grant_id,
            snapshot.pending_slot_demand
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_kv_api::OwnerHotCacheCounters;
    use std::sync::atomic::Ordering;

    const SLOT_SIZE: u64 = 8 * 1024 * 1024;

    #[test]
    fn target_grants_cover_used_and_pending_without_double_reserving() {
        let slots_per_grant = crate::owner_local_reserve_slots_per_grant(SLOT_SIZE).unwrap();
        assert_eq!(
            owner_local_reserve_target_grant_count(slots_per_grant, 128, 1, 0),
            3
        );
        assert_eq!(
            owner_local_reserve_target_grant_count(slots_per_grant, 128, 0, 0),
            2
        );
        assert_eq!(
            owner_local_reserve_target_grant_count(slots_per_grant, 0, 0, 0),
            0
        );
    }

    #[test]
    fn exact_fit_target_accounts_for_unusable_grant_tail() {
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
            owner_local_reserve_target_grant_count(slots_per_grant, 0, 0, expected_grants as usize,),
            207
        );
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
        let snapshot = RebalanceClassSnapshot {
            slot_size: SLOT_SIZE,
            slots_per_grant: 64,
            used_slots: 64 * 228,
            free_slots: 64 * 4,
            grant_count: 232,
            pending_slot_demand: 0,
            max_observed_claim_slots: 0,
            expected_grant_count: 232,
        };
        assert_eq!(owner_local_reserve_desired_free_slots(snapshot), 64 * 4);
        assert_eq!(
            owner_local_reserve_target_grant_count(
                snapshot.slots_per_grant,
                snapshot.used_slots,
                owner_local_reserve_desired_free_slots(snapshot),
                snapshot.expected_grant_count,
            ),
            232
        );

        let pressured = RebalanceClassSnapshot {
            used_slots: snapshot.used_slots + 1,
            free_slots: snapshot.free_slots - 1,
            ..snapshot
        };
        assert_eq!(
            owner_local_reserve_target_grant_count(
                pressured.slots_per_grant,
                pressured.used_slots,
                owner_local_reserve_desired_free_slots(pressured),
                pressured.expected_grant_count,
            ),
            232
        );
    }

    #[test]
    fn configured_expected_grants_are_a_hard_upper_bound() {
        assert_eq!(
            owner_local_reserve_target_grant_count(64, 64 * 300, 64, 232),
            232
        );
        assert_eq!(
            owner_local_reserve_target_grant_count(64, 64 * 300, 64, 0),
            301
        );
    }

    #[test]
    fn owner_slot_pressure_targets_pending_plus_high_watermark() {
        let snapshot = RebalanceClassSnapshot {
            slot_size: 1024 * 1024,
            slots_per_grant: 4,
            used_slots: 4,
            free_slots: 1,
            grant_count: 1,
            pending_slot_demand: 3,
            max_observed_claim_slots: 3,
            expected_grant_count: 1,
        };
        assert_eq!(
            owner_slot_pressure_request_bytes(snapshot, 0),
            18 * 1024 * 1024,
            "3 pending slots plus 16 slots of retained headroom minus one physical Free"
        );
        assert_eq!(
            owner_slot_pressure_request_bytes(snapshot, 10 * 1024 * 1024),
            8 * 1024 * 1024,
            "exact selected debt contributes projected slots of the same class"
        );
        assert_eq!(
            owner_slot_pressure_request_bytes(
                RebalanceClassSnapshot {
                    free_slots: 16,
                    pending_slot_demand: 0,
                    ..snapshot
                },
                0,
            ),
            0,
            "pressure stops after the high watermark is physically available"
        );

        let clamped = RebalanceClassSnapshot {
            slot_size: 8 * 1024 * 1024,
            slots_per_grant: 64,
            used_slots: 64,
            free_slots: 1,
            grant_count: 1,
            pending_slot_demand: 1_000,
            max_observed_claim_slots: 1_000,
            expected_grant_count: 1,
        };
        assert_eq!(
            owner_slot_pressure_request_bytes(clamped, 0),
            OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES
        );
    }

    #[test]
    fn pre_fence_candidate_debt_is_not_projected_reclaim_credit() {
        let snapshot = RebalanceClassSnapshot {
            slot_size: 1024 * 1024,
            slots_per_grant: 4,
            used_slots: 4,
            free_slots: 1,
            grant_count: 1,
            pending_slot_demand: 3,
            max_observed_claim_slots: 3,
            expected_grant_count: 1,
        };
        let counters = OwnerHotCacheCounters::default();
        counters
            .selection_debt_bytes
            .store(10 * 1024 * 1024, Ordering::Release);

        assert_eq!(
            counters
                .source_eviction_selected_bytes
                .load(Ordering::Acquire),
            0
        );
        assert_eq!(
            owner_slot_pressure_request_bytes(
                snapshot,
                counters
                    .source_eviction_selected_bytes
                    .load(Ordering::Acquire),
            ),
            18 * 1024 * 1024,
            "retry-only candidate debt must not suppress physical victim selection"
        );

        counters.add_source_eviction_selected_bytes(10 * 1024 * 1024);
        assert_eq!(
            owner_slot_pressure_request_bytes(
                snapshot,
                counters
                    .source_eviction_selected_bytes
                    .load(Ordering::Acquire),
            ),
            8 * 1024 * 1024,
            "only installed source-selection debt is valid projected reclaim credit"
        );
        counters.remove_source_eviction_selected_bytes(10 * 1024 * 1024);
        assert_eq!(
            counters
                .source_eviction_selected_bytes
                .load(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn production_pressure_fills_high_watermark_in_one_kick() {
        const VALUE_LEN: u64 = 4_718_592;
        let slots_per_grant = crate::owner_local_reserve_slots_per_grant(VALUE_LEN).unwrap();
        assert_eq!(slots_per_grant, 113);
        let snapshot = RebalanceClassSnapshot {
            slot_size: VALUE_LEN,
            slots_per_grant,
            used_slots: 113 * 174 - 8,
            free_slots: 8,
            grant_count: 174,
            pending_slot_demand: 128,
            max_observed_claim_slots: 128,
            expected_grant_count: 174,
        };
        let requested = owner_slot_pressure_request_bytes(snapshot, 0);
        assert_eq!(
            requested,
            u64::from(128_u32 + slots_per_grant * 4 - 8) * VALUE_LEN,
            "one pressure kick must cover the claimant and retained four-grant headroom"
        );
        assert!(requested < OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES);
    }
}

async fn owner_local_reserve_rebalance_once(view: &ClientKvApiView) {
    let inner = view.client_kv_api().inner();
    let snapshots = {
        let pool = inner.owner_local_reserve_pool.lock();
        snapshot_rebalance_classes(&pool)
    };
    for snapshot in snapshots {
        try_refill_once(inner, snapshot).await;
        try_shrink_once(inner, snapshot).await;
    }
}

fn report_owner_local_reserve_state(inner: &ClientKvApiInner) {
    let snapshots = {
        let pool = inner.owner_local_reserve_pool.lock();
        snapshot_rebalance_classes(&pool)
    };
    for snapshot in snapshots {
        let grant_count = u64::try_from(snapshot.grant_count).unwrap_or(u64::MAX);
        let used_slots = u64::try_from(snapshot.used_slots).unwrap_or(u64::MAX);
        let free_slots = u64::try_from(snapshot.free_slots).unwrap_or(u64::MAX);
        let reserved_slots = grant_count.saturating_mul(u64::from(snapshot.slots_per_grant));
        tracing::info!(
            "owner local reserve state: slot_size={} used_slots={} free_slots={} pending_slots={} grants={} expected_grants={} reserved_slots={} used_slot_bytes={} usable_slot_bytes={} physical_reserved_bytes={}",
            snapshot.slot_size,
            snapshot.used_slots,
            snapshot.free_slots,
            snapshot.pending_slot_demand,
            snapshot.grant_count,
            snapshot.expected_grant_count,
            reserved_slots,
            used_slots.saturating_mul(snapshot.slot_size),
            used_slots
                .saturating_add(free_slots)
                .saturating_mul(snapshot.slot_size),
            grant_count.saturating_mul(OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES),
        );
    }
}

pub fn spawn_owner_local_reserve_rebalance_actor(view: ClientKvApiView) {
    let view_task = view.clone();
    view.spawn("owner_local_reserve_rebalance_actor", async move {
        let expected = install_expected_capacity_class(view_task.client_kv_api().inner());
        if let Some(expected) = expected {
            tracing::info!(
                "owner local reserve expected capacity configured: value_len={} payload_capacity_bytes={} slot_size={} slots_per_grant={} expected_grants={} physical_reserved_target_bytes={}",
                expected.value_len,
                expected.payload_capacity_bytes,
                expected.slot_size,
                expected.slots_per_grant,
                expected.grant_count,
                u64::try_from(expected.grant_count)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES),
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
        let mut last_unsupported_class_report_at: Option<Instant> = None;

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
            if last_kick_at
                .is_some_and(|last| last.elapsed() < OWNER_SLOT_PRESSURE_MIN_KICK_INTERVAL)
            {
                continue;
            }
            let source_eviction_selected_bytes =
                owner_slot_pressure_projected_reclaim_bytes(inner);
            let requested_bytes = {
                let pool = inner.owner_local_reserve_pool.lock();
                let snapshots = snapshot_rebalance_classes(&pool);
                match snapshots.as_slice() {
                    // No local allocation class has been observed yet.  This is
                    // the normal idle state, not a configuration error.
                    [] => 0,
                    [snapshot] => owner_slot_pressure_request_bytes(
                        *snapshot,
                        source_eviction_selected_bytes,
                    ),
                    _ => {
                        // Selection is intentionally disabled until the victim
                        // cache is class-indexed.  Rate-limit the diagnostic so
                        // an unsupported configuration cannot become a 100 Hz
                        // logging/CPU loop.
                        if last_unsupported_class_report_at.is_none_or(|last| {
                            last.elapsed() >= OWNER_LOCAL_RESERVE_REPORT_INTERVAL
                        }) {
                            tracing::error!(
                                active_classes = snapshots.len(),
                                "owner slot pressure requires a class-indexed victim domain when more than one size class is active"
                            );
                            last_unsupported_class_report_at = Some(Instant::now());
                        }
                        0
                    }
                }
            };
            if requested_bytes == 0 {
                continue;
            }
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
            if inner
                .owner_hot_eviction_tx
                .send(OwnerHotEvictionDispatch::EndPressure { selected_bytes })
                .is_err()
            {
                tracing::warn!(
                    requested_bytes,
                    selected_bytes,
                    "owner slot pressure dispatcher closed after Moka selection"
                );
                return;
            }
            last_kick_at = Some(Instant::now());
            tracing::debug!(
                requested_bytes,
                selected_bytes,
                "owner slot pressure selected bounded source-eviction candidates"
            );
        }
    });
}

pub async fn wait_owner_local_reserve_ready(
    inner: &ClientKvApiInner,
    slot_size: u64,
    slots_per_grant: u32,
    key_count: usize,
    soft_wait_timeout: Duration,
    hard_deadline: Instant,
) -> bool {
    let notify = inner.owner_local_reserve_rebalance_notify();
    let mut shutdown_waiter = inner.view.register_shutdown_waiter();
    loop {
        let notified = notify.notified();
        {
            let mut pool = inner.owner_local_reserve_pool.lock();
            let class_state = pool
                .classes
                .entry(slot_size)
                .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
            if class_state.free_slot_count() >= key_count {
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
