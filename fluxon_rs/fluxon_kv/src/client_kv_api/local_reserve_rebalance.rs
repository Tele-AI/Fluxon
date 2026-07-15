use super::{
    ClientKvApiInner, ClientKvApiView, OwnerLocalReserveClassState, OwnerLocalReservePoolState,
};
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
use crate::master_kv_router::msg_pack::ReserveLocalGrantOutcome;
use limit_thirdparty::tokio;
use std::time::{Duration, Instant};

const OWNER_LOCAL_RESERVE_REBALANCE_INTERVAL: Duration = Duration::from_millis(200);
const OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN: Duration = Duration::from_secs(5);
const OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN: Duration = Duration::from_secs(1);
const OWNER_LOCAL_RESERVE_DEFAULT_SOFT_WAIT_TIMEOUT: Duration = Duration::from_millis(10);
const OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT: Duration = Duration::from_secs(10);
const OWNER_LOCAL_RESERVE_MIN_GRANTS_PER_CLASS: usize = 0;
const OWNER_LOCAL_RESERVE_REPORT_INTERVAL: Duration = Duration::from_secs(30);
const OWNER_LOCAL_RESERVE_FREE_SLOT_HEADROOM_GRANTS: usize = 4;

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
    demand_grants.max(expected_grant_count)
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
        let missing_slots = desired_free_slots
            .saturating_sub(snapshot.free_slots)
            .max(1);
        let required_free_slots = u32::try_from(missing_slots).unwrap_or(u32::MAX);
        let reclaim_before_grow = snapshot.expected_grant_count != 0
            && snapshot.grant_count >= snapshot.expected_grant_count;
        let rpc_started_at = Instant::now();
        let outcome = match inner
            .reserve_local_grant(snapshot.slot_size, required_free_slots, reclaim_before_grow)
            .await
        {
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
            ReserveLocalGrantOutcome::Reclaimed {
                slot_size,
                reclaimed_slots,
            } => {
                assert_eq!(slot_size, snapshot.slot_size);
                tracing::info!(
                    "owner local reserve reused reclaimed slots: slot_size={} reclaimed_slots={} pending_slots={}",
                    slot_size,
                    reclaimed_slots,
                    snapshot.pending_slot_demand
                );
                inner
                    .owner_local_reserve_rebalance_notify()
                    .notify_waiters();
                return;
            }
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
            class_state.grants.push(detached_grant);
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
    get_target_pressure: bool,
) -> Option<(usize, super::OwnerLocalReserveGrantState)> {
    if !get_target_pressure {
        let grow_cooldown_ok = class_state
            .last_grow_at
            .map(|last_grow_at| last_grow_at.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN)
            .unwrap_or(true);
        if !grow_cooldown_ok {
            return None;
        }
    }
    let shrink_keep_grants = owner_local_reserve_target_grant_count(
        class_state.slots_per_grant,
        class_state.used_slot_count(),
        class_state.pending_slot_demand,
        class_state.expected_grant_count,
    )
    .max(OWNER_LOCAL_RESERVE_MIN_GRANTS_PER_CLASS);
    if class_state.grants.len() <= shrink_keep_grants {
        return None;
    }

    let candidate_index = if get_target_pressure {
        // A physical Get-target allocation can be blocked while reclaimed slots remain trapped in
        // a detached 512-MiB reserve grant. Under explicit pressure, any fully-free excess grant is
        // safe to return; restricting the actor to the tail would leave allocator space stranded.
        class_state
            .grants
            .iter()
            .rposition(super::OwnerLocalReserveGrantState::is_fully_free)
    } else {
        let tail_index = class_state.grants.len().checked_sub(1)?;
        class_state.grants.get(tail_index).and_then(|grant| {
            (grant.is_fully_free()
                && grant
                    .fully_free_since
                    .map(|since| since.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN)
                    .unwrap_or(false))
            .then_some(tail_index)
        })
    }?;
    Some((candidate_index, class_state.grants.remove(candidate_index)))
}

async fn try_shrink_once(inner: &ClientKvApiInner, snapshot: RebalanceClassSnapshot) {
    let detached = {
        let mut pool = inner.owner_local_reserve_pool.lock();
        let Some(class_state) = pool.classes.get_mut(&snapshot.slot_size) else {
            return;
        };
        detach_excess_fully_free_grant(class_state, false)
    };

    let Some((grant_index, grant)) = detached else {
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
        class_state
            .grants
            .insert(grant_index.min(class_state.grants.len()), grant);
    } else {
        tracing::info!(
            "owner local reserve released excess grant: slot_size={} grant_id={} pending_slots={}",
            snapshot.slot_size,
            grant.grant_id,
            snapshot.pending_slot_demand
        );
    }
}

/// Return one fully-free reserve grant immediately when an ordinary local Get-target allocation
/// reports physical NoSpace. This never shrinks below the configured/active reserve target and does
/// not evict a live slot; it only makes already-free physical capacity visible to the master
/// allocator without waiting for the periodic idle cooldown.
pub(crate) async fn release_one_excess_reserve_grant_for_get_target_pressure(
    inner: &ClientKvApiInner,
) -> bool {
    let detached = {
        let mut pool = inner.owner_local_reserve_pool.lock();
        let mut detached = None;
        for class_state in pool.classes.values_mut() {
            if let Some((grant_index, grant)) = detach_excess_fully_free_grant(class_state, true) {
                detached = Some((
                    class_state.slot_size,
                    class_state.slots_per_grant,
                    class_state.pending_slot_demand,
                    grant_index,
                    grant,
                ));
                break;
            }
        }
        detached
    };
    let Some((slot_size, slots_per_grant, pending_slots, grant_index, grant)) = detached else {
        return false;
    };
    let grant_id = grant.grant_id;

    if let Err(err) = inner.release_local_grant(grant_id).await {
        tracing::warn!(
            "owner local reserve get-target pressure failed to release grant_id={} err={}",
            grant_id,
            err
        );
        let mut pool = inner.owner_local_reserve_pool.lock();
        let class_state = pool
            .classes
            .entry(slot_size)
            .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
        class_state
            .grants
            .insert(grant_index.min(class_state.grants.len()), grant);
        inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
        false
    } else {
        tracing::info!(
            "owner local reserve released excess grant for get-target pressure: slot_size={} grant_id={} pending_slots={}",
            slot_size,
            grant_id,
            pending_slots
        );
        inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_kv_api::OwnerLocalReserveGrantState;

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
    fn default_hard_timeout_covers_pressure_reclaim_transaction() {
        assert_eq!(
            OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT,
            Duration::from_secs(10)
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
            233
        );
    }

    #[test]
    fn get_target_pressure_can_detach_a_non_tail_fully_free_excess_grant() {
        let mut class_state = OwnerLocalReserveClassState::new(SLOT_SIZE, 2);
        class_state.expected_grant_count = 1;
        for grant_id in 1..=3 {
            class_state.grants.push(OwnerLocalReserveGrantState::new(
                grant_id,
                0,
                grant_id * 1024,
                1024,
                SLOT_SIZE,
                2,
            ));
        }
        class_state.grants[0]
            .claim_prepared_slot()
            .expect("first grant must have a slot");
        class_state.grants[2]
            .claim_prepared_slot()
            .expect("tail grant must have a slot");

        assert!(detach_excess_fully_free_grant(&mut class_state, false).is_none());
        let (index, grant) = detach_excess_fully_free_grant(&mut class_state, true)
            .expect("pressure shrink must find the free middle grant");
        assert_eq!(index, 1);
        assert_eq!(grant.grant_id, 2);
        assert_eq!(class_state.grants.len(), 2);
    }

    #[test]
    fn get_target_pressure_never_shrinks_below_active_or_expected_target() {
        let mut class_state = OwnerLocalReserveClassState::new(SLOT_SIZE, 2);
        class_state.expected_grant_count = 2;
        for grant_id in 1..=2 {
            class_state.grants.push(OwnerLocalReserveGrantState::new(
                grant_id,
                0,
                grant_id * 1024,
                1024,
                SLOT_SIZE,
                2,
            ));
        }
        assert!(detach_excess_fully_free_grant(&mut class_state, true).is_none());

        class_state.expected_grant_count = 0;
        for grant in &mut class_state.grants {
            grant
                .claim_prepared_slot()
                .expect("each grant must have a slot");
        }
        assert!(detach_excess_fully_free_grant(&mut class_state, true).is_none());
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
