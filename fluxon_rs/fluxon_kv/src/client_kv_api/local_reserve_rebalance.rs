use super::{
    ClientKvApiInner, ClientKvApiView, OwnerLocalReserveClassState, OwnerLocalReservePoolState,
};
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
use limit_thirdparty::tokio;
use std::time::{Duration, Instant};

const OWNER_LOCAL_RESERVE_REBALANCE_INTERVAL: Duration = Duration::from_millis(200);
const OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN: Duration = Duration::from_secs(5);
const OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN: Duration = Duration::from_secs(1);
const OWNER_LOCAL_RESERVE_DEFAULT_SOFT_WAIT_TIMEOUT: Duration = Duration::from_millis(10);
const OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT: Duration = Duration::from_secs(1);
const OWNER_LOCAL_RESERVE_GROW_FACTOR_NUM: usize = 2;
const OWNER_LOCAL_RESERVE_MIN_GRANTS_PER_CLASS: usize = 0;

#[derive(Debug, Clone, Copy)]
struct RebalanceClassSnapshot {
    slot_size: u64,
    slots_per_grant: u32,
    used_slots: usize,
    grant_count: usize,
    pending_slot_demand: usize,
}

fn owner_local_reserve_round_up_grants(bytes: u64) -> u64 {
    let quantum = OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
    if bytes == 0 {
        return 0;
    }
    let rem = bytes % quantum;
    if rem == 0 {
        bytes
    } else {
        bytes.checked_add(quantum - rem).unwrap_or(u64::MAX)
    }
}

fn owner_local_reserve_target_bytes(
    slot_size: u64,
    used_slots: usize,
    pending_slot_demand: usize,
) -> u64 {
    let required_slots = used_slots.saturating_add(pending_slot_demand);
    let required_bytes = (required_slots as u64).saturating_mul(slot_size);
    let doubled_used = (used_slots as u64)
        .saturating_mul(slot_size)
        .saturating_mul(OWNER_LOCAL_RESERVE_GROW_FACTOR_NUM as u64);
    required_bytes.max(doubled_used)
}

fn owner_local_reserve_target_grant_count(
    slot_size: u64,
    used_slots: usize,
    pending_slot_demand: usize,
) -> usize {
    let target_bytes = owner_local_reserve_target_bytes(slot_size, used_slots, pending_slot_demand);
    let rounded = owner_local_reserve_round_up_grants(target_bytes);
    if rounded == 0 {
        return 0;
    }
    (rounded / OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES) as usize
}

fn snapshot_rebalance_classes(pool: &OwnerLocalReservePoolState) -> Vec<RebalanceClassSnapshot> {
    pool.classes
        .values()
        .map(|class_state| RebalanceClassSnapshot {
            slot_size: class_state.slot_size,
            slots_per_grant: class_state.slots_per_grant,
            used_slots: class_state.used_slot_count(),
            grant_count: class_state.grant_count(),
            pending_slot_demand: class_state.pending_slot_demand,
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
    if snapshot.pending_slot_demand == 0 {
        return;
    }
    if snapshot.slot_size > OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES {
        tracing::error!(
            "owner local reserve refill rejected oversized slot_size={} quantum={} pending_slots={}",
            snapshot.slot_size,
            OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
            snapshot.pending_slot_demand
        );
        return;
    }
    let target_grants = owner_local_reserve_target_grant_count(
        snapshot.slot_size,
        snapshot.used_slots,
        snapshot.pending_slot_demand,
    );
    if snapshot.grant_count >= target_grants {
        return;
    }
    let additional_grants = target_grants - snapshot.grant_count;
    for _ in 0..additional_grants {
        let grant = match inner.reserve_local_grant().await {
            Ok(grant) => grant,
            Err(err) => {
                tracing::warn!(
                    "owner local reserve refill failed: slot_size={} pending_slots={} err={}",
                    snapshot.slot_size,
                    snapshot.pending_slot_demand,
                    err
                );
                return;
            }
        };
        if grant.len != OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES {
            tracing::warn!(
                "owner local reserve refill got unexpected grant size: requested={} got={}",
                OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES,
                grant.len
            );
            if let Err(release_err) = inner.release_local_grant(grant.grant_id).await {
                tracing::warn!(
                    "owner local reserve refill failed to release mismatched grant_id={} err={}",
                    grant.grant_id,
                    release_err
                );
            }
            return;
        }
        let detached_grant = super::OwnerLocalReserveGrantState::new(
            grant.grant_id,
            grant.base_addr,
            grant.addr,
            grant.len,
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
        inner
            .owner_local_reserve_rebalance_notify()
            .notify_waiters();
    }
}

async fn try_shrink_once(inner: &ClientKvApiInner, snapshot: RebalanceClassSnapshot) {
    let detached = {
        let mut pool = inner.owner_local_reserve_pool.lock();
        let Some(class_state) = pool.classes.get_mut(&snapshot.slot_size) else {
            return;
        };
        if class_state.pending_slot_demand > 0 {
            return;
        }
        let grow_cooldown_ok = class_state
            .last_grow_at
            .map(|last_grow_at| last_grow_at.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_GROW_COOLDOWN)
            .unwrap_or(true);
        if !grow_cooldown_ok {
            return;
        }
        let shrink_keep_grants = owner_local_reserve_target_grant_count(
            class_state.slot_size,
            class_state.used_slot_count(),
            class_state.pending_slot_demand,
        )
        .max(OWNER_LOCAL_RESERVE_MIN_GRANTS_PER_CLASS);
        if class_state.grants.len() <= shrink_keep_grants {
            return;
        }
        let should_detach_tail = class_state
            .grants
            .last()
            .map(|grant| {
                grant.is_fully_free()
                    && grant
                        .fully_free_since
                        .map(|since| since.elapsed() >= OWNER_LOCAL_RESERVE_SHRINK_IDLE_COOLDOWN)
                        .unwrap_or(false)
            })
            .unwrap_or(false);
        if !should_detach_tail {
            return;
        }
        Some(
            class_state
                .grants
                .pop()
                .expect("tail grant must exist when shrinking local reserve"),
        )
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
        class_state.grants.push(grant);
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

pub fn spawn_owner_local_reserve_rebalance_actor(view: ClientKvApiView) {
    let view_task = view.clone();
    view.spawn("owner_local_reserve_rebalance_actor", async move {
        let shutdown_poller = view_task.register_shutdown_poller();
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let notify = view_task
            .client_kv_api()
            .inner()
            .owner_local_reserve_rebalance_notify();
        let mut tick = tokio::time::interval(OWNER_LOCAL_RESERVE_REBALANCE_INTERVAL);

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
