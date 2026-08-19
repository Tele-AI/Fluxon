use super::{
    ClientKvApiInner, ClientKvApiView, OwnerHotEvictionDispatch, OwnerPressureBatchReclaim,
    OwnerSegmentAllocator,
};
use crate::client_seg_pool::ClientSegPoolAccessTrait;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_seg_manager::msg_pack::{
    OwnerCapacityReport, OwnerCapacityReportReq, OwnerPlacementClass, OwnerSizeClassCapacity,
    SegmentAllocationAuthority,
};
use crate::p2p::control_plane_rpc::call_control_plane_rpc;
use crate::p2p::msg_pack::MsgPack;
use crate::p2p::p2p_module::P2pModuleAccessTrait;
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, OK};
use limit_thirdparty::tokio;
use std::{
    collections::BTreeSet,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

const OWNER_SEGMENT_REPORT_INTERVAL: Duration = Duration::from_secs(30);
const OWNER_CAPACITY_REPORT_INTERVAL: Duration = Duration::from_secs(1);
// The unified RPC contract rejects explicit timeouts below ten seconds.
// Successful periodic reports still return immediately; this is only the
// transport failure bound and is independent from the five-second freshness
// gate used by placement.
const OWNER_CAPACITY_REPORT_TIMEOUT: Duration = Duration::from_secs(10);
const OWNER_LOCAL_RESERVE_DEFAULT_SOFT_WAIT_TIMEOUT: Duration = Duration::from_millis(10);
const OWNER_LOCAL_RESERVE_DEFAULT_HARD_TIMEOUT: Duration = Duration::from_secs(30);
const OWNER_SLOT_PRESSURE_INTERVAL: Duration = Duration::from_millis(10);
const OWNER_SLOT_PRESSURE_MIN_KICK_INTERVAL: Duration = Duration::from_millis(25);
const OWNER_SLOT_PRESSURE_INITIAL_COARSE_BYTES: u64 = 64 * 1024 * 1024;
// Bound one pressure round in bytes. Physical ownership is one complete segment.
const OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct ExpectedCapacityLayout {
    value_len: u64,
    slot_size: u64,
    payload_capacity_bytes: u64,
    physical_capacity_bytes: u64,
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

#[derive(Debug, Clone, Copy, Default)]
struct RebalanceSnapshot {
    used_bytes: u64,
    free_bytes: u64,
    largest_free_bytes: u64,
    pending_bytes: u64,
    largest_pending_slot_size: u64,
    accounting_slot_size: u64,
    allocatable_slots: u64,
    allocatable_bytes: u64,
    slot_unallocatable_bytes: u64,
    pending_size_classes: usize,
    pressure_initial_pop_bytes: u64,
    claim_progress_epoch: u64,
    physical_free_epoch: u64,
    physical_capacity_bytes: u64,
    local_target_bytes: u64,
    global_accounted_bytes: u64,
}

fn expected_capacity_layout(
    value_len: u64,
    payload_capacity_bytes: u64,
    physical_capacity_bytes: u64,
) -> ExpectedCapacityLayout {
    let slot_size = crate::owner_segment_allocation_capacity_bytes(value_len)
        .expect("owner expected slot size must be config-validated");
    assert!(
        payload_capacity_bytes <= physical_capacity_bytes,
        "logical owner-local capacity must fit the physical segment"
    );
    ExpectedCapacityLayout {
        value_len,
        slot_size,
        payload_capacity_bytes,
        physical_capacity_bytes,
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

async fn install_owner_segment_allocator(view: &ClientKvApiView) -> Result<(), String> {
    let inner = view.client_kv_api().inner();
    if inner.allocation_authority != SegmentAllocationAuthority::Owner {
        if inner.owner_hot_cache.is_some() {
            return Err(
                "owner-local Moka requires owner allocation authority for the complete segment"
                    .to_string(),
            );
        }
        return Ok(());
    }
    if !inner.owner_placement_class.is_valid() {
        return Err(
            "owner allocation authority requires an explicit inference or remote_cpu placement class"
                .to_string(),
        );
    }

    let segment = view
        .client_seg_pool()
        .cpu_mem_read_guard()
        .await
        .map_err(|err| format!("owner allocator cannot read the registered segment: {err}"))?;
    let base_addr = segment.allocated_addr;
    let segment_bytes = segment.allocated_size;
    drop(segment);
    if segment_bytes != inner.owner_local_reserve_physical_capacity_bytes {
        return Err(format!(
            "owner segment/config mismatch: segment_bytes={} configured_bytes={}",
            segment_bytes, inner.owner_local_reserve_physical_capacity_bytes
        ));
    }
    if segment_bytes == 0 || segment_bytes % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
    {
        return Err(format!(
            "owner segment must be a non-zero multiple of {} bytes: segment_bytes={}",
            crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES,
            segment_bytes
        ));
    }

    let local_target_bytes = match inner.owner_placement_class {
        OwnerPlacementClass::Inference => inner
            .owner_hot_cache
            .as_ref()
            .and_then(|cache| cache.max_capacity())
            .ok_or_else(|| "inference owner requires owner-local Moka".to_string())?,
        OwnerPlacementClass::RemoteCpu => {
            if inner.owner_hot_cache.is_some() {
                return Err("remote_cpu owner must not create owner-local Moka".to_string());
            }
            0
        }
        OwnerPlacementClass::Invalid => unreachable!("validated owner placement class"),
    };
    let expected_slot_size = configured_expected_capacity(inner).map(|layout| layout.slot_size);
    let self_info = view.cluster_manager().get_self_info();
    let registration_epoch = u64::try_from(self_info.node_start_time).map_err(|_| {
        format!(
            "owner segment registration epoch must be non-negative: owner={} node_start_time={}",
            self_info.id, self_info.node_start_time
        )
    })?;
    inner.owner_segment_allocator.lock().install_segment(
        crate::owner_segment::OwnerGeneration::new(
            self_info.id.to_string(),
            self_info.node_start_time,
        ),
        registration_epoch,
        base_addr,
        base_addr,
        segment_bytes,
        local_target_bytes,
        expected_slot_size,
    )?;
    tracing::info!(
        base_addr,
        segment_bytes,
        local_target_bytes,
        global_target_bytes = segment_bytes.saturating_sub(local_target_bytes),
        allocator_unit_bytes = crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES,
        allocator_max_metadata_nodes = 128 * 1024,
        "installed one allocator over the complete owner-authoritative DRAM segment"
    );
    Ok(())
}

fn slot_pressure_capacity(pool: &OwnerSegmentAllocator) -> SlotPressureCapacity {
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
            let pending_batch_bytes = pending_slots
                .saturating_mul(slot_size)
                .min(OWNER_SLOT_PRESSURE_INITIAL_COARSE_BYTES);
            // Size classes view the same free extents, so the maximum shortage
            // is the one shared pressure budget. Summing would double count.
            // Reclaim one additional bounded copy of the currently pending
            // batch.  This leaves real allocator headroom for the next
            // request instead of forcing every request to start a separate
            // pressure RPC.  Victims remain independent single KVs; this is
            // only byte-budget coalescing, never atomic-batch eviction.
            let pressure_bytes = missing_bytes.saturating_add(pending_batch_bytes);
            initial_pop_bytes = initial_pop_bytes.max(pressure_bytes);
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
    let pool = inner.owner_segment_allocator.lock();
    let pressure = slot_pressure_capacity(&pool);
    RebalanceSnapshot {
        used_bytes: pool.total_used_bytes(),
        free_bytes: pool.total_free_bytes(),
        largest_free_bytes: pool.largest_free_bytes(),
        pending_bytes: pool.total_pending_bytes(),
        largest_pending_slot_size: pool.largest_pending_slot_size(),
        accounting_slot_size: pressure.accounting_slot_size,
        allocatable_slots: pressure.allocatable_slots,
        allocatable_bytes: pressure.allocatable_bytes,
        slot_unallocatable_bytes: pressure.slot_unallocatable_bytes,
        pending_size_classes: pressure.pending_size_classes,
        pressure_initial_pop_bytes: pressure.initial_pop_bytes,
        claim_progress_epoch: pool.claim_progress_epoch,
        physical_free_epoch: pool.physical_free_epoch(),
        physical_capacity_bytes: pool.physical_capacity_bytes(),
        local_target_bytes: pool.local_target_bytes,
        global_accounted_bytes: pool.global_accounted_bytes(),
    }
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

fn report_owner_segment_state(inner: &ClientKvApiInner) {
    let snapshot = snapshot_rebalance(inner);
    let expected = configured_expected_capacity(inner);
    let moka_weighted_bytes = inner
        .owner_hot_cache
        .as_ref()
        .map(|cache| cache.weighted_size())
        .unwrap_or(0);
    let global_target_bytes = snapshot
        .physical_capacity_bytes
        .saturating_sub(snapshot.local_target_bytes);
    let global_budget_overshoot_bytes = snapshot
        .global_accounted_bytes
        .saturating_sub(global_target_bytes);
    tracing::info!(
        physical_capacity_bytes = snapshot.physical_capacity_bytes,
        local_target_bytes = snapshot.local_target_bytes,
        global_target_bytes,
        global_accounted_bytes = snapshot.global_accounted_bytes,
        global_budget_overshoot_bytes,
        expected_value_len = expected.map(|layout| layout.value_len).unwrap_or(0),
        expected_slot_size = expected.map(|layout| layout.slot_size).unwrap_or(0),
        logical_payload_capacity_bytes = expected
            .map(|layout| layout.payload_capacity_bytes)
            .unwrap_or(0),
        configured_physical_capacity_bytes = expected
            .map(|layout| layout.physical_capacity_bytes)
            .unwrap_or(0),
        moka_weighted_bytes,
        used_bytes = snapshot.used_bytes,
        free_bytes = snapshot.free_bytes,
        largest_free_bytes = snapshot.largest_free_bytes,
        pending_bytes = snapshot.pending_bytes,
        largest_pending_slot_size = snapshot.largest_pending_slot_size,
        accounting_slot_size = snapshot.accounting_slot_size,
        allocatable_slots = snapshot.allocatable_slots,
        allocatable_bytes = snapshot.allocatable_bytes,
        slot_unallocatable_bytes = snapshot.slot_unallocatable_bytes,
        pending_size_classes = snapshot.pending_size_classes,
        pressure_initial_pop_bytes = snapshot.pressure_initial_pop_bytes,
        claim_progress_epoch = snapshot.claim_progress_epoch,
        physical_free_epoch = snapshot.physical_free_epoch,
        "owner segment allocator state"
    );
}

fn build_owner_capacity_report(inner: &ClientKvApiInner, report_epoch: u64) -> OwnerCapacityReport {
    let local_weighted_bytes = inner
        .owner_hot_cache
        .as_ref()
        .map(|cache| cache.weighted_size())
        .unwrap_or(0);
    let selected_fence_bytes = inner
        .owner_hot_counters
        .source_eviction_selected_bytes
        .load(Ordering::Acquire);
    let pool = inner.owner_segment_allocator.lock();
    let physical_capacity_bytes = pool.physical_capacity_bytes();
    let local_target_bytes = pool.local_target_bytes;
    let raw_free_bytes = pool.total_free_bytes();
    let size_classes = pool
        .capacity_report_size_classes()
        .into_iter()
        .map(|allocation_size_bytes| OwnerSizeClassCapacity {
            allocation_size_bytes,
            allocatable_bytes: pool
                .allocatable_report(allocation_size_bytes)
                .allocatable_bytes,
        })
        .collect();
    OwnerCapacityReport {
        owner_node_start_time: inner.view.cluster_manager().get_self_info().node_start_time,
        placement_class: inner.owner_placement_class,
        controller_epoch: pool.controller_epoch,
        report_epoch,
        physical_capacity_bytes,
        local_target_bytes,
        global_target_bytes: physical_capacity_bytes.saturating_sub(local_target_bytes),
        allocated_bytes: pool.total_used_bytes(),
        raw_free_bytes,
        largest_free_bytes: pool.largest_free_bytes(),
        global_accounted_bytes: pool.global_accounted_bytes(),
        local_weighted_bytes,
        settled: pool.controller_epoch != 0
            && match inner.owner_placement_class {
                OwnerPlacementClass::Inference => {
                    local_target_bytes
                        == inner
                            .owner_hot_cache
                            .as_ref()
                            .and_then(|cache| cache.max_capacity())
                            .unwrap_or(0)
                        && local_weighted_bytes <= local_target_bytes
                }
                OwnerPlacementClass::RemoteCpu => {
                    local_target_bytes == 0 && local_weighted_bytes == 0
                }
                OwnerPlacementClass::Invalid => false,
            }
            && selected_fence_bytes == 0
            && inner.owner_hot_retry_queue.len() == 0,
        size_classes,
    }
}

pub(super) async fn publish_owner_capacity_report(view: &ClientKvApiView) -> Result<u64, String> {
    let inner = view.client_kv_api().inner();
    if inner.allocation_authority != SegmentAllocationAuthority::Owner {
        return Err("capacity reports require owner allocation authority".to_string());
    }
    let report_epoch = inner
        .owner_capacity_report_epoch
        .fetch_add(1, Ordering::AcqRel)
        .checked_add(1)
        .expect("owner capacity report epoch exhausted");
    let report = build_owner_capacity_report(inner, report_epoch);
    if report.physical_capacity_bytes == 0 {
        return Err("owner capacity report cannot publish a zero-sized segment".to_string());
    }
    let master_node_id = view
        .cluster_manager()
        .find_or_wait_master_node()
        .await
        .map_err(|error| format!("owner capacity report is waiting for master: {error}"))?;
    let response = call_control_plane_rpc(
        &inner.rpc_caller_owner_capacity_report,
        view.p2p_module(),
        master_node_id.into(),
        MsgPack {
            serialize_part: OwnerCapacityReportReq { report },
            raw_bytes: Vec::new(),
        },
        Some(OWNER_CAPACITY_REPORT_TIMEOUT),
        0,
    )
    .await
    .map_err(|error| format!("owner capacity report transport failed: {error}"))?;
    match response {
        response
            if response.serialize_part.error_code == OK
                && response.serialize_part.accepted_report_epoch == report_epoch =>
        {
            let requested = &response.serialize_part.requested_size_classes;
            let learned = inner
                .owner_segment_allocator
                .lock()
                .track_capacity_report_size_classes(requested)
                .map_err(|error| {
                    format!("master returned an invalid owner capacity-report size class: {error}")
                })?;
            if learned {
                tracing::info!(
                    report_epoch,
                    requested_size_classes = ?requested,
                    "owner learned new exact capacity-report size classes"
                );
            }
            Ok(report_epoch)
        }
        response => {
            let error = KvError::from_json(
                response.serialize_part.error_code,
                &response.serialize_part.error_json,
            );
            Err(format!(
                "master rejected owner capacity report: report_epoch={} accepted_report_epoch={} error={}",
                report_epoch, response.serialize_part.accepted_report_epoch, error
            ))
        }
    }
}

pub fn spawn_owner_local_reserve_rebalance_actor(view: ClientKvApiView) {
    let view_task = view.clone();
    view.spawn("owner_segment_allocator_actor", async move {
        if let Err(err) = install_owner_segment_allocator(&view_task).await {
            tracing::error!(error = %err, "failed to initialize owner segment allocator");
            return;
        }
        let shutdown_poller = view_task.register_shutdown_poller();
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        let mut log_tick = tokio::time::interval(OWNER_SEGMENT_REPORT_INTERVAL);
        let mut capacity_tick = tokio::time::interval(OWNER_CAPACITY_REPORT_INTERVAL);
        capacity_tick.set_missed_tick_behavior(::tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => return,
                _ = capacity_tick.tick() => {
                    if !shutdown_poller.is_running() {
                        return;
                    }
                    tokio::select! {
                        biased;
                        _ = shutdown_waiter.wait() => return,
                        result = publish_owner_capacity_report(&view_task) => {
                            if let Err(error) = result {
                                tracing::debug!(%error, "periodic owner capacity report did not converge");
                            }
                        }
                    }
                }
                _ = log_tick.tick() => {
                    if !shutdown_poller.is_running() {
                        return;
                    }
                    report_owner_segment_state(view_task.client_kv_api().inner());
                }
            }
        }
    });
}

fn owner_slot_pressure_round_bytes(initial_bytes: u64, round: u32) -> u64 {
    let multiplier = 1u64.checked_shl(round.min(63)).unwrap_or(u64::MAX);
    initial_bytes
        .saturating_mul(multiplier)
        .min(OWNER_SLOT_PRESSURE_MAX_EVICT_BYTES)
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
struct OwnerPressureBackoff {
    round: u32,
    base_bytes: u64,
    observed_claim_progress_epoch: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct OwnerPressureProgressBaseline {
    claim_progress_epoch: u64,
    physical_free_epoch: u64,
    free_bytes: u64,
    allocatable_slots: u64,
}

impl OwnerPressureProgressBaseline {
    fn from_snapshot(snapshot: RebalanceSnapshot) -> Self {
        Self {
            claim_progress_epoch: snapshot.claim_progress_epoch,
            physical_free_epoch: snapshot.physical_free_epoch,
            free_bytes: snapshot.free_bytes,
            allocatable_slots: snapshot.allocatable_slots,
        }
    }

    fn observed_physical_progress(self, snapshot: RebalanceSnapshot) -> bool {
        snapshot.physical_free_epoch != self.physical_free_epoch
            || snapshot.claim_progress_epoch != self.claim_progress_epoch
            || snapshot.free_bytes > self.free_bytes
            || snapshot.allocatable_slots > self.allocatable_slots
    }
}

fn owner_pressure_global_headroom(snapshot: RebalanceSnapshot) -> u64 {
    snapshot
        .physical_capacity_bytes
        .saturating_sub(snapshot.local_target_bytes)
        .saturating_sub(snapshot.global_accounted_bytes)
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

async fn finish_owner_pressure_batch(
    tx: &limit_thirdparty::tokio::sync::ampsc::UnboundedSender<OwnerHotEvictionDispatch>,
    selected_bytes: u64,
    global_headroom_before: u64,
) -> Result<OwnerPressureBatchReclaim, OwnerPressureBatchDispatchError> {
    let (completion, completed) = ::tokio::sync::oneshot::channel();
    tx.send(OwnerHotEvictionDispatch::EndPressure {
        selected_bytes,
        global_headroom_before,
        completion,
    })
    .map_err(|_| OwnerPressureBatchDispatchError::DispatcherClosed)?;
    completed
        .await
        .map_err(|_| OwnerPressureBatchDispatchError::CompletionDropped)
}

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
                _ = shutdown_waiter.wait() => return,
                _ = tick.tick() => {},
                _ = notify.notified() => {}
            }
            if !shutdown_poller.is_running() {
                return;
            }

            let inner = view_task.client_kv_api().inner();
            let snapshot = snapshot_rebalance(inner);
            backoff.sync_claim_progress(snapshot.claim_progress_epoch);
            let initial_bytes = snapshot.pressure_initial_pop_bytes;
            if initial_bytes == 0 {
                backoff.reset(snapshot.claim_progress_epoch);
                continue;
            }
            if last_kick_at
                .is_some_and(|last| last.elapsed() < OWNER_SLOT_PRESSURE_MIN_KICK_INTERVAL)
            {
                continue;
            }
            let pressure_round = backoff.round;
            let exponential_request_bytes = backoff.request_bytes(initial_bytes);
            let global_headroom_bytes = owner_pressure_global_headroom(snapshot);
            // One closed batch that did not make an allocation possible is
            // followed by B, 2B, 4B... selection.  Logical scope changes,
            // Moka selection, retry debt and an installed source fence are
            // deliberately not capacity credit.  Only a real allocator Free
            // or a successful claim can reset this exponential loop.
            let requested_bytes = exponential_request_bytes;
            let Some(cache) = inner.owner_hot_cache.clone() else {
                continue;
            };
            let _selection_guard = inner.owner_hot_selection_lock.lock().await;
            if inner
                .owner_hot_eviction_tx
                .send(OwnerHotEvictionDispatch::BeginPressure { requested_bytes })
                .is_err()
            {
                return;
            }
            let selected_bytes = limit_thirdparty::tokio::task::spawn_blocking(move || {
                cache.evict_some(requested_bytes)
            })
            .await
            .unwrap_or(0);
            last_kick_at = Some(Instant::now());
            let result = tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => return,
                result = finish_owner_pressure_batch(
                    &inner.owner_hot_eviction_tx,
                    selected_bytes,
                    global_headroom_bytes,
                ) => result,
            };
            let global_reclaim = match result {
                Ok(global_reclaim) => global_reclaim,
                Err(err) => {
                    tracing::warn!(
                        ?err,
                        requested_bytes,
                        selected_bytes,
                        "owner pressure batch failed"
                    );
                    return;
                }
            };
            let completed_snapshot = snapshot_rebalance(inner);
            let physical_progress = OwnerPressureProgressBaseline::from_snapshot(snapshot)
                .observed_physical_progress(completed_snapshot);
            let logical_progress =
                completed_snapshot.global_accounted_bytes != snapshot.global_accounted_bytes;
            // Advance after every closed non-empty pop. If this batch really
            // enabled a claim, claim_progress_epoch resets the backoff on the
            // next observation. If it only selected busy candidates, the next
            // round must continue instead of waiting on imaginary Free slots.
            backoff.finish_round(selected_bytes);
            tracing::debug!(
                requested_bytes,
                exponential_request_bytes,
                selected_bytes,
                pressure_round,
                pressure_initial_pop_bytes = initial_bytes,
                accounting_slot_size = snapshot.accounting_slot_size,
                raw_free_bytes = snapshot.free_bytes,
                allocatable_slots = snapshot.allocatable_slots,
                allocatable_bytes = snapshot.allocatable_bytes,
                slot_unallocatable_bytes = snapshot.slot_unallocatable_bytes,
                pending_bytes = snapshot.pending_bytes,
                pending_size_classes = snapshot.pending_size_classes,
                claim_progress_epoch = snapshot.claim_progress_epoch,
                physical_free_epoch = snapshot.physical_free_epoch,
                global_accounted_bytes = snapshot.global_accounted_bytes,
                global_headroom_bytes,
                global_reclaim_requested_bytes = global_reclaim.requested_bytes,
                global_reclaim_selected_bytes = global_reclaim.selected_bytes,
                physical_progress,
                logical_progress,
                candidate_only_selection = global_reclaim.selected_bytes != 0 && !physical_progress,
                "owner segment pressure completed exponential single-KV eviction round"
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
        if inner
            .owner_segment_allocator
            .lock()
            .allocatable_report(slot_size)
            .allocatable_slots
            >= required_slots
        {
            return true;
        }
        let Some(remaining_hard) = hard_deadline.checked_duration_since(Instant::now()) else {
            return false;
        };
        let wait_budget = remaining_hard.min(soft_wait_timeout);
        tokio::select! {
            _ = shutdown_waiter.wait() => return false,
            _ = tokio::time::sleep(wait_budget) => {
                notify.notify_waiters();
            }
            _ = notified => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_backoff_is_byte_bounded() {
        assert_eq!(
            owner_slot_pressure_round_bytes(64 * 1024 * 1024, 0),
            64 * 1024 * 1024
        );
        assert_eq!(
            owner_slot_pressure_round_bytes(64 * 1024 * 1024, 3),
            512 * 1024 * 1024
        );
        assert_eq!(
            owner_slot_pressure_round_bytes(64 * 1024 * 1024, 20),
            4 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn first_pressure_round_reclaims_one_bounded_pending_batch_of_headroom() {
        let slot_size = 4 * 1024 * 1024;
        let mut pool = OwnerSegmentAllocator::default();
        pool.expected_slot_size = Some(slot_size);
        pool.pending_demand_by_slot_size.insert(slot_size, 13);
        pool.failed_claim_slot_sizes.insert(slot_size);

        // With no currently allocatable slot, the exact shortage is 52 MiB
        // and the bounded next-batch headroom is another 52 MiB.
        let pressure = slot_pressure_capacity(&pool);
        assert_eq!(pressure.initial_pop_bytes, 104 * 1024 * 1024);

        // Large batches add at most 64 MiB of look-ahead headroom.
        pool.pending_demand_by_slot_size.insert(slot_size, 32);
        let pressure = slot_pressure_capacity(&pool);
        assert_eq!(pressure.initial_pop_bytes, 192 * 1024 * 1024);
    }

    fn pressure_snapshot(
        free_bytes: u64,
        allocatable_slots: u64,
        claim_progress_epoch: u64,
        physical_free_epoch: u64,
        global_accounted_bytes: u64,
    ) -> RebalanceSnapshot {
        RebalanceSnapshot {
            free_bytes,
            allocatable_slots,
            claim_progress_epoch,
            physical_free_epoch,
            physical_capacity_bytes: 128 * 1024 * 1024,
            local_target_bytes: 96 * 1024 * 1024,
            global_accounted_bytes,
            ..RebalanceSnapshot::default()
        }
    }

    #[test]
    fn metadata_only_demotion_is_not_allocator_progress() {
        let before = pressure_snapshot(4096, 0, 7, 11, 24 * 1024 * 1024);
        let after = pressure_snapshot(4096, 0, 7, 11, 25 * 1024 * 1024);
        let baseline = OwnerPressureProgressBaseline::from_snapshot(before);
        assert!(!baseline.observed_physical_progress(after));

        let freed = pressure_snapshot(8192, 1, 7, 12, 25 * 1024 * 1024);
        assert!(baseline.observed_physical_progress(freed));
        let claimed = pressure_snapshot(4096, 0, 8, 11, 25 * 1024 * 1024);
        assert!(baseline.observed_physical_progress(claimed));

        // Another allocator user may consume the newly freed extent before
        // the pressure actor samples aggregate bytes.  Exact Free still has
        // to count as progress so the remaining shortage is replanned.
        let free_masked_by_claim = pressure_snapshot(2048, 0, 7, 12, 25 * 1024 * 1024);
        assert!(baseline.observed_physical_progress(free_masked_by_claim));
    }

    #[test]
    fn candidate_only_round_keeps_exponential_replan_open() {
        let base = 64 * 1024 * 1024;
        let mut backoff = OwnerPressureBackoff::default();
        assert_eq!(backoff.request_bytes(base), base);

        // Closing a batch with selected candidates is not a reason to wait.
        // If no claim follows, the next round doubles even when those
        // candidates later become retry-only debt.
        backoff.finish_round(base);
        assert_eq!(backoff.request_bytes(base), 2 * base);
        backoff.finish_round(base);
        assert_eq!(backoff.request_bytes(base), 4 * base);

        // Only allocator claim progress resets the pressure sequence.
        backoff.sync_claim_progress(1);
        assert_eq!(backoff.request_bytes(base), base);
    }

    #[test]
    fn expected_layout_keeps_logical_and_physical_bytes_separate() {
        let layout = expected_capacity_layout(
            4_718_592,
            100 * 1024 * 1024 * 1024,
            128 * 1024 * 1024 * 1024,
        );
        assert_eq!(layout.slot_size, 4_718_592);
        assert_eq!(layout.payload_capacity_bytes, 100 * 1024 * 1024 * 1024);
        assert_eq!(layout.physical_capacity_bytes, 128 * 1024 * 1024 * 1024);
    }
}
