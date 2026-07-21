use super::{
    ClientKvApiInner, ClientKvApiView, KvMetrics, OwnerLocalReserveSlotLease,
    OwnerLocalReserveSlotRef,
};
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::memholder::{MemoryInfo, UserMemHolder, UserMemHolderExposeKind};
// no StageScope; timestamps-based metrics only
use crate::observe_kvope::{
    obe_get_cache_hit, obe_get_cache_miss, obe_get_done_error_status, obe_get_done_success,
    obe_get_end_error_rpc, obe_get_start_error_rpc, obe_get_start_error_status,
    obe_get_start_not_found, obe_get_start_success, obe_get_transfer_error,
    obe_get_transfer_success,
};
use crate::{
    cluster_manager::NodeID,
    master_kv_router::msg_pack::{
        BatchGetDoneReq, BatchGetDoneResp, BatchGetRevokeReq, BatchGetRevokeResp,
        BatchGetStartItemResp, BatchGetStartReq, BatchGetStartResp, BatchIsExistReq,
        GetAllocationMode, GetDoneReq, GetDoneResp, GetMetaReq, GetMetaResp,
        GetPreparedLocalReserveTarget, GetRevokeReq, GetStartReq, GetStartResp,
    },
    p2p::msg_pack::MsgPack,
    rpcresp_kvresult_convert::msg_and_error::codes_api,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult, OK},
};
use ::tokio::sync::Semaphore;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use limit_thirdparty::tokio;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::Instant;

const BATCH_GET_DONE_MAX_INFLIGHT: usize = 4;

fn batch_get_done_rpc_limiter() -> &'static Semaphore {
    static LIMITER: OnceLock<Semaphore> = OnceLock::new();
    LIMITER.get_or_init(|| Semaphore::new(BATCH_GET_DONE_MAX_INFLIGHT))
}

async fn release_prepared_get_target(
    inner: &ClientKvApiInner,
    target: &GetPreparedLocalReserveTarget,
) -> KvResult<()> {
    inner
        .owner_release_local_reserve_slot_lease(OwnerLocalReserveSlotLease {
            value_len: target.slot_size,
            slot_size: target.slot_size,
            slots: vec![OwnerLocalReserveSlotRef {
                grant_id: target.grant_id,
                slot_index: target.slot_index,
                ptr: target.addr,
                base_addr: target.base_addr,
            }],
        })
        .await
}

fn batch_get_done_response_matches(get_ids: &[u64], response: &BatchGetDoneResp) -> bool {
    response.items.len() == get_ids.len()
        && response
            .items
            .iter()
            .zip(get_ids)
            .all(|(item, expected_get_id)| item.get_id == *expected_get_id)
}

#[derive(Clone)]
pub(crate) struct StartedGetRevokeCleanup {
    pub(crate) get_id: u64,
    pub(crate) prepared_target: Option<GetPreparedLocalReserveTarget>,
}

async fn run_started_get_revoke_cleanup(
    view: ClientKvApiView,
    pending: Vec<StartedGetRevokeCleanup>,
    context: &'static str,
) {
    if pending.is_empty() {
        return;
    }
    let mut attempt = 1u32;
    loop {
        let get_ids = pending.iter().map(|item| item.get_id).collect::<Vec<_>>();
        let response = view.client_kv_api().inner().batch_get_revoke(get_ids).await;
        let resp = match response {
            Ok(resp)
                if resp.items.len() == pending.len()
                    && resp
                        .items
                        .iter()
                        .zip(&pending)
                        .all(|(resp, expected)| resp.get_id == expected.get_id) =>
            {
                resp
            }
            Ok(resp) => {
                tracing::warn!(
                    "{} Revoke response shape/identity mismatch; retaining prepared slots for retry: expected={} got={} attempt={}",
                    context,
                    pending.len(),
                    resp.items.len(),
                    attempt
                );
                tokio::time::sleep(Duration::from_millis(
                    (50u64.saturating_mul(1u64 << attempt.min(6))).min(2_000),
                ))
                .await;
                attempt = attempt.saturating_add(1);
                continue;
            }
            Err(err) => {
                if matches!(&err, KvError::Api(ApiError::SystemShutdown { .. })) {
                    tracing::warn!(
                        "{} Revoke cleanup stopped during owner shutdown: items={}",
                        context,
                        pending.len()
                    );
                    return;
                }
                tracing::warn!(
                    "{} Revoke uncertain; retaining get ids and prepared slots for retry: items={} attempt={} err={}",
                    context,
                    pending.len(),
                    attempt,
                    err
                );
                tokio::time::sleep(Duration::from_millis(
                    (50u64.saturating_mul(1u64 << attempt.min(6))).min(2_000),
                ))
                .await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        for (expected, item_resp) in pending.iter().zip(resp.items) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                item_resp.error_code,
                item_resp.error_json,
            ) {
                // A terminal Done may have won.  Never release a possibly
                // committed slot from the losing Revoke path.
                tracing::warn!(
                    "{} Revoke reached non-releasable terminal: get_id={} err={}",
                    context,
                    expected.get_id,
                    err
                );
                continue;
            }
            let Some(target) = expected.prepared_target.as_ref() else {
                continue;
            };
            let mut release_attempt = 1u32;
            loop {
                match release_prepared_get_target(view.client_kv_api().inner(), target).await {
                    Ok(()) => break,
                    Err(err) => {
                        tracing::error!(
                            "{} Revoke confirmed but prepared slot release failed; retrying: get_id={} attempt={} err={}",
                            context,
                            expected.get_id,
                            release_attempt,
                            err
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        release_attempt = release_attempt.saturating_add(1);
                    }
                }
            }
        }
        return;
    }
}

/// Move cleanup ownership to a registered task before awaiting it.  If the
/// caller future is cancelled, the task still drives Revoke to a definite
/// terminal and releases only confirmed-uncommitted prepared slots.
pub(crate) async fn finish_started_get_revoke_cleanup(
    inner: &ClientKvApiInner,
    pending: Vec<StartedGetRevokeCleanup>,
    context: &'static str,
) {
    if pending.is_empty() {
        return;
    }
    let (done_tx, done_rx) = ::tokio::sync::oneshot::channel::<()>();
    let spawn_view = inner.view.clone_view();
    let worker_view = spawn_view.clone();
    spawn_view.spawn("started_get_revoke_cleanup", async move {
        run_started_get_revoke_cleanup(worker_view, pending, context).await;
        let _ = done_tx.send(());
    });
    let _ = done_rx.await;
}

#[derive(Debug, Clone)]
pub struct RemoteGetInfo {
    get_id: u64,
    data_len: usize,
    src_addr: u64,
    target_addr: u64,
    node_id: NodeID,
    peer_is_src_or_target: bool,
}

impl std::fmt::Display for RemoteGetInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetInfo{{ get_id: {}, data_len: {} bytes, src_addr: {:#x}, target_addr: {:#x}, node_id: {:?}, remote_transfer: {} }}",
            self.get_id,
            self.data_len,
            self.src_addr,
            self.target_addr,
            self.node_id,
            self.peer_is_src_or_target
        )
    }
}

impl RemoteGetInfo {
    pub fn data_len(&self) -> usize {
        self.data_len
    }

    pub fn is_remote_transfer(&self) -> bool {
        self.peer_is_src_or_target
    }
}

impl ClientKvApiInner {
    pub async fn batch_get_finish_started(
        &self,
        keys: Vec<String>,
        start_items: Vec<BatchGetStartItemResp>,
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_finish_started"
                    .to_string(),
            }));
        }
        if keys.len() != start_items.len() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "batch_get_finish_started length mismatch: keys={} start_items={}",
                    keys.len(),
                    start_items.len()
                ),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let lifecycle_started_at = Instant::now();
        let lifecycle_requested_keys = keys.len();

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            start_item: BatchGetStartItemResp,
            peer_is_remote: bool,
            transfer_us: i64,
            prepared_memory_info: Option<Arc<MemoryInfo>>,
        }

        let transfer_concurrency = transfer_concurrency.max(1);
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();

        let mut results: Vec<
            Option<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>,
        > = (0..keys.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut transfer_error_cleanup = Vec::new();
        let mut transfer_futures = Vec::new();
        let mut lifecycle_zero_copy_items = 0usize;
        let mut lifecycle_transfer_items = 0usize;
        let mut lifecycle_remote_transfer_items = 0usize;
        let mut lifecycle_transfer_bytes = 0u64;
        let mut lifecycle_remote_transfer_bytes = 0u64;

        for (idx, (key, start_item)) in keys.into_iter().zip(start_items.into_iter()).enumerate() {
            if start_item.error_code == codes_api::API_KEY_NOT_FOUND {
                if let Some(target) = start_item.prepared_target.as_ref() {
                    release_prepared_get_target(self, target).await?;
                }
                results[idx] = Some(Ok(None));
                continue;
            }
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                if let Some(target) = start_item.prepared_target.as_ref() {
                    release_prepared_get_target(self, target).await?;
                }
                results[idx] = Some(Err(err));
                continue;
            }

            let peer_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let peer_is_remote = peer_id.is_some();
            let get_id = start_item.get_id;
            let src_addr = start_item.src_addr;
            let target_addr = start_item.target_addr;
            let len = start_item.len;

            if peer_id.is_none() && src_addr == target_addr {
                lifecycle_zero_copy_items = lifecycle_zero_copy_items.saturating_add(1);
                done_pending.push(DonePending {
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    transfer_us: 0,
                    prepared_memory_info: None,
                });
                continue;
            }

            lifecycle_transfer_items = lifecycle_transfer_items.saturating_add(1);
            lifecycle_transfer_bytes = lifecycle_transfer_bytes.saturating_add(len);
            if peer_is_remote {
                lifecycle_remote_transfer_items = lifecycle_remote_transfer_items.saturating_add(1);
                lifecycle_remote_transfer_bytes =
                    lifecycle_remote_transfer_bytes.saturating_add(len);
            }

            transfer_futures.push(async move {
                let transfer_started_at = Instant::now();
                let transfer_result = self
                    .view
                    .client_transfer_engine()
                    .transfer_data_no_copy(peer_id, true, src_addr, target_addr, len, None)
                    .await
                    .map_err(|err| {
                        KvError::Api(ApiError::Transfer {
                            from_addr: src_addr,
                            to_addr: target_addr,
                            len,
                            error: err.to_string(),
                        })
                    });
                let transfer_us = transfer_started_at
                    .elapsed()
                    .as_micros()
                    .min(i64::MAX as u128) as i64;
                (
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    get_id,
                    transfer_us,
                    transfer_result,
                )
            });
        }

        let lifecycle_plan_us = lifecycle_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        let transfer_wall_started_at = Instant::now();
        let mut lifecycle_transfer_sum_us = 0u64;
        let mut lifecycle_transfer_max_us = 0u64;
        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (idx, key, start_item, peer_is_remote, _get_id, transfer_us, Ok(_breakdown)) => {
                    let transfer_us_u64 = transfer_us.max(0) as u64;
                    lifecycle_transfer_sum_us =
                        lifecycle_transfer_sum_us.saturating_add(transfer_us_u64);
                    lifecycle_transfer_max_us = lifecycle_transfer_max_us.max(transfer_us_u64);
                    done_pending.push(DonePending {
                        idx,
                        key,
                        start_item,
                        peer_is_remote,
                        transfer_us,
                        prepared_memory_info: None,
                    });
                }
                (idx, _key, start_item, _peer_is_remote, get_id, _transfer_us, Err(err)) => {
                    results[idx] = Some(Err(err));
                    transfer_error_cleanup.push(StartedGetRevokeCleanup {
                        get_id,
                        prepared_target: start_item.prepared_target,
                    });
                }
            }
        }
        let lifecycle_transfer_wall_us = transfer_wall_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;

        let transfer_cleanup_started_at = Instant::now();
        finish_started_get_revoke_cleanup(
            self,
            transfer_error_cleanup,
            "batch_get transfer failure",
        )
        .await;
        let lifecycle_transfer_cleanup_us = transfer_cleanup_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;

        let install_started_at = Instant::now();
        let mut ready_done_pending = Vec::with_capacity(done_pending.len());
        let mut install_failed_cleanup = Vec::new();
        for mut pending in done_pending {
            let install_result = if let Some(target) = pending.start_item.prepared_target.as_ref() {
                match u32::try_from(pending.start_item.len) {
                    Ok(len) => self
                        .install_hidden_pending_local_get(
                            &pending.key,
                            pending.start_item.get_id,
                            pending.start_item.put_id,
                            target.addr,
                            target.base_addr,
                            len,
                            target.slot_size,
                            target.grant_id,
                            target.slot_index,
                        )
                        .map(Some),
                    Err(_) => Err(KvError::Api(ApiError::InvalidArgument {
                        detail: format!(
                            "local-reserve Get value length exceeds u32: key={} len={}",
                            pending.key, pending.start_item.len
                        ),
                    })),
                }
            } else {
                Ok(None)
            };
            match install_result {
                Ok(memory_info) => {
                    pending.prepared_memory_info = memory_info;
                    ready_done_pending.push(pending);
                }
                Err(err) => {
                    install_failed_cleanup.push(StartedGetRevokeCleanup {
                        get_id: pending.start_item.get_id,
                        prepared_target: pending.start_item.prepared_target.clone(),
                    });
                    results[pending.idx] = Some(Err(err));
                }
            }
        }
        done_pending = ready_done_pending;

        finish_started_get_revoke_cleanup(
            self,
            install_failed_cleanup,
            "batch_get pending install failure",
        )
        .await;
        let lifecycle_install_us = install_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;

        let done_get_ids = done_pending
            .iter()
            .map(|pending| pending.start_item.get_id)
            .collect::<Vec<_>>();
        let lifecycle_done_items = done_get_ids.len();
        let done_first_get_id = done_get_ids.first().copied();
        let done_last_get_id = done_get_ids.last().copied();
        let mut done_attempt = 1u32;
        let done_started_at = Instant::now();
        let done_resp = loop {
            match self.batch_get_done(done_get_ids.clone()).await {
                Ok(resp) if batch_get_done_response_matches(&done_get_ids, &resp) => break resp,
                Ok(resp) => {
                    tracing::warn!(
                        "batch_get_done response shape/identity mismatch; retaining pending-visible slots and retrying the same idempotent get_ids: items={} first_get_id={:?} last_get_id={:?} got_items={} got_first_get_id={:?} got_last_get_id={:?} attempt={}",
                        done_get_ids.len(),
                        done_first_get_id,
                        done_last_get_id,
                        resp.items.len(),
                        resp.items.first().map(|item| item.get_id),
                        resp.items.last().map(|item| item.get_id),
                        done_attempt
                    );
                }
                Err(err) => {
                    if matches!(&err, KvError::Api(ApiError::SystemShutdown { .. })) {
                        return Err(err);
                    }
                    tracing::warn!(
                        "batch_get_done transport uncertain; retaining pending-visible slots and retrying the same idempotent get_ids: items={} first_get_id={:?} last_get_id={:?} attempt={} err={}",
                        done_get_ids.len(),
                        done_first_get_id,
                        done_last_get_id,
                        done_attempt,
                        err
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(
                (10u64.saturating_mul(1u64 << done_attempt.min(8))).min(2_000),
            ))
            .await;
            done_attempt = done_attempt.saturating_add(1);
        };
        let lifecycle_done_us = done_started_at.elapsed().as_micros().min(i64::MAX as u128) as i64;
        let publish_started_at = Instant::now();
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();
        let mut local_hot_admissions = Vec::new();

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                if let Some(target) = pending.start_item.prepared_target.as_ref() {
                    self.abort_hidden_pending_local_get(&pending.key, pending.start_item.get_id);
                    let canonical = self.local_committed_mem_holder_for_put_id(
                        &pending.key,
                        pending.start_item.put_id,
                    );
                    if let Err(release_err) = release_prepared_get_target(self, target).await {
                        results[pending.idx] = Some(Err(release_err));
                        continue;
                    }
                    // A same-version PutDone/GetDone may have won while this
                    // transfer was in flight.  Converge on the owner's
                    // canonical local backing instead of turning an already
                    // available KV page into a prefix miss.
                    if let Some(memory_info) = canonical {
                        let user_mem_holder = Arc::new(UserMemHolder::new(
                            memory_info,
                            self.get_or_init_all_memholder_refcount(),
                            UserMemHolderExposeKind::SegPtr,
                        ));
                        results[pending.idx] = Some(Ok(Some((user_mem_holder, None))));
                        continue;
                    }
                }
                results[pending.idx] = Some(Err(err));
                continue;
            }
            let expose_kind = if done_item.allocation_mode == GetAllocationMode::Temporary {
                UserMemHolderExposeKind::OwnedCopy
            } else {
                UserMemHolderExposeKind::SegPtr
            };
            let data_len = pending.start_item.len as usize;
            metrics.record_l2_hit_locality(pending.peer_is_remote, data_len as u64);
            metrics.record_get_io_locality(
                pending.peer_is_remote,
                data_len as u64,
                pending.transfer_us,
            );
            let memory_info = if pending.start_item.prepared_target.is_some() {
                if done_item.allocation_mode != GetAllocationMode::LocalCommittedSlot {
                    self.abort_hidden_pending_local_get(&pending.key, pending.start_item.get_id);
                    results[pending.idx] = Some(Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "prepared local-reserve Get completed with unexpected allocation mode: key={} mode={:?}",
                            pending.key, done_item.allocation_mode
                        ),
                    })));
                    continue;
                }
                let memory_info = match self.promote_hidden_pending_local_get(
                    &pending.key,
                    pending.start_item.get_id,
                    pending.start_item.put_id,
                ) {
                    Ok(memory_info) => memory_info,
                    Err(err) => {
                        results[pending.idx] = Some(Err(err));
                        continue;
                    }
                };
                if let Some(prepared) = pending.prepared_memory_info.as_ref() {
                    assert!(
                        Arc::ptr_eq(prepared, &memory_info),
                        "Get promotion must retain the unique prepared MemoryInfo"
                    );
                }
                local_hot_admissions.push((
                    pending.key.clone(),
                    pending.start_item.put_id,
                    memory_info.clone(),
                    pending.start_item.atomic_group.clone(),
                ));
                memory_info
            } else {
                if done_item.allocation_mode == GetAllocationMode::LocalCommittedSlot {
                    results[pending.idx] = Some(Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "master returned local committed-slot mode without a prepared target: key={}",
                            pending.key
                        ),
                    })));
                    continue;
                }
                let offset = pending.start_item.target_addr - pending.start_item.target_base_addr;
                let memory_info = Arc::new(
                    MemoryInfo::new(
                        offset,
                        pending.start_item.len as u32,
                        done_item.holder_id,
                        pending.key.clone(),
                        master_node_id.clone(),
                        self.view.clone(),
                    )
                    .await,
                );
                if done_item.allocation_mode != GetAllocationMode::Temporary
                    && self.install_get_cached_info_if_unfenced(
                        &pending.key,
                        pending.start_item.put_id,
                        memory_info.clone(),
                    )
                {
                    metrics.observe_cache_value_size(
                        &client_id,
                        node_role.as_str(),
                        data_len as u64,
                    );
                }
                memory_info
            };
            let get_info = RemoteGetInfo {
                get_id: pending.start_item.get_id,
                data_len,
                src_addr: pending.start_item.src_addr,
                target_addr: pending.start_item.target_addr,
                node_id: pending.start_item.node_id.clone().into(),
                peer_is_src_or_target: pending.peer_is_remote,
            };
            if done_item.allocation_mode == GetAllocationMode::LocalCommittedSlot {
                metrics.observe_cache_value_size(&client_id, node_role.as_str(), data_len as u64);
            }
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info,
                self.get_or_init_all_memholder_refcount(),
                expose_kind,
            ));
            results[pending.idx] = Some(Ok(Some((user_mem_holder, Some(get_info)))));
        }

        // Publish every local index before Moka admission starts selecting
        // individual capacity victims.
        let lifecycle_local_hot_admissions = local_hot_admissions.len();
        for (key, put_id, memory_info, _atomic_group) in local_hot_admissions {
            self.owner_hot_track_committed(&key, put_id, &memory_info);
        }

        let output = results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_get_finish_started result slot was not populated"
                            .to_string(),
                    }))
                })
            })
            .collect::<Vec<_>>();
        let lifecycle_hits = output
            .iter()
            .filter(|item| matches!(item, Ok(Some(_))))
            .count();
        let lifecycle_misses = output
            .iter()
            .filter(|item| matches!(item, Ok(None)))
            .count();
        let lifecycle_errors = output.iter().filter(|item| matches!(item, Err(_))).count();
        let lifecycle_publish_us = publish_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        let lifecycle_total_us = lifecycle_started_at
            .elapsed()
            .as_micros()
            .min(i64::MAX as u128) as i64;
        tracing::info!(
            "external Get finish lifecycle: requested={} transfer_concurrency={} zero_copy_items={} transfer_items={} remote_transfer_items={} transfer_bytes={} remote_transfer_bytes={} plan_us={} transfer_wall_us={} transfer_sum_us={} transfer_max_us={} transfer_cleanup_us={} install_us={} done_items={} done_attempts={} done_us={} local_hot_admissions={} publish_us={} hits={} misses={} errors={} total_us={}",
            lifecycle_requested_keys,
            transfer_concurrency,
            lifecycle_zero_copy_items,
            lifecycle_transfer_items,
            lifecycle_remote_transfer_items,
            lifecycle_transfer_bytes,
            lifecycle_remote_transfer_bytes,
            lifecycle_plan_us,
            lifecycle_transfer_wall_us,
            lifecycle_transfer_sum_us,
            lifecycle_transfer_max_us,
            lifecycle_transfer_cleanup_us,
            lifecycle_install_us,
            lifecycle_done_items,
            done_attempt,
            lifecycle_done_us,
            lifecycle_local_hot_admissions,
            lifecycle_publish_us,
            lifecycle_hits,
            lifecycle_misses,
            lifecycle_errors,
            lifecycle_total_us,
        );
        Ok(output)
    }

    pub async fn batch_get(
        &self,
        keys: Vec<String>,
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get".to_string(),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            key: String,
            start_item: crate::master_kv_router::msg_pack::BatchGetStartItemResp,
            peer_is_remote: bool,
            transfer_us: i64,
        }

        let transfer_concurrency = transfer_concurrency.max(1);
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();
        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();

        let mut results: Vec<
            Option<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>,
        > = (0..keys.len()).map(|_| None).collect();
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();

        for (idx, key) in keys.iter().enumerate() {
            if let Some(memory_info) = self.local_visible_mem_holder(key) {
                tracing::debug!(
                    "batch_get local visible hit for key: {}, directly return",
                    key
                );
                let user_mem_holder = Arc::new(UserMemHolder::new(
                    memory_info.clone(),
                    self.get_or_init_all_memholder_refcount(),
                    UserMemHolderExposeKind::SegPtr,
                ));
                obe_get_cache_hit(
                    &metrics,
                    &client_id,
                    &node_role,
                    key,
                    memory_info.len as u64,
                );
                metrics.record_get_io_locality(false, memory_info.len as u64, 0);
                results[idx] = Some(Ok(Some((user_mem_holder, None))));
            } else {
                obe_get_cache_miss(&metrics, &client_id, &node_role, key);
                missing_indices.push(idx);
                missing_keys.push(key.clone());
            }
        }

        if missing_keys.is_empty() {
            return Ok(results
                .into_iter()
                .map(|item| {
                    item.unwrap_or_else(|| {
                        Err(KvError::Api(ApiError::Unknown {
                            detail: "batch_get result slot was not populated".to_string(),
                        }))
                    })
                })
                .collect());
        }

        let start_resp = self.batch_get_start(missing_keys.clone()).await?;
        if start_resp.items.len() != missing_keys.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_get_start response length mismatch: expected={} got={}",
                    missing_keys.len(),
                    start_resp.items.len()
                ),
            }));
        }

        let mut done_pending = Vec::new();
        let mut revoke_get_ids = Vec::new();
        let mut transfer_futures = Vec::new();

        for ((idx, key), start_item) in missing_indices
            .into_iter()
            .zip(missing_keys.into_iter())
            .zip(start_resp.items.into_iter())
        {
            if start_item.error_code == codes_api::API_KEY_NOT_FOUND {
                results[idx] = Some(Ok(None));
                continue;
            }
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                start_item.error_code,
                start_item.error_json.clone(),
            ) {
                results[idx] = Some(Err(err));
                continue;
            }

            let peer_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let peer_is_remote = peer_id.is_some();
            let get_id = start_item.get_id;
            let src_addr = start_item.src_addr;
            let target_addr = start_item.target_addr;
            let len = start_item.len;

            if peer_id.is_none() && src_addr == target_addr {
                done_pending.push(DonePending {
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    transfer_us: 0,
                });
                continue;
            }

            transfer_futures.push(async move {
                let transfer_started_at = Instant::now();
                let transfer_result = self
                    .view
                    .client_transfer_engine()
                    .transfer_data_no_copy(peer_id, true, src_addr, target_addr, len, None)
                    .await
                    .map_err(|err| {
                        KvError::Api(ApiError::Transfer {
                            from_addr: src_addr,
                            to_addr: target_addr,
                            len,
                            error: err.to_string(),
                        })
                    });
                let transfer_us = transfer_started_at
                    .elapsed()
                    .as_micros()
                    .min(i64::MAX as u128) as i64;
                (
                    idx,
                    key,
                    start_item,
                    peer_is_remote,
                    get_id,
                    transfer_us,
                    transfer_result,
                )
            });
        }

        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (idx, key, start_item, peer_is_remote, _get_id, transfer_us, Ok(_breakdown)) => {
                    done_pending.push(DonePending {
                        idx,
                        key,
                        start_item,
                        peer_is_remote,
                        transfer_us,
                    });
                }
                (idx, _key, _start_item, _peer_is_remote, get_id, _transfer_us, Err(err)) => {
                    results[idx] = Some(Err(err));
                    revoke_get_ids.push(get_id);
                }
            }
        }

        if !revoke_get_ids.is_empty() {
            if let Err(err) = self.batch_get_revoke(revoke_get_ids).await {
                tracing::warn!("batch_get_revoke failed after transfer errors: {}", err);
            }
        }

        let done_resp = self
            .batch_get_done(
                done_pending
                    .iter()
                    .map(|pending| pending.start_item.get_id)
                    .collect(),
            )
            .await?;
        if done_resp.items.len() != done_pending.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_get_done response length mismatch: expected={} got={}",
                    done_pending.len(),
                    done_resp.items.len()
                ),
            }));
        }
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();

        for (pending, done_item) in done_pending.into_iter().zip(done_resp.items.into_iter()) {
            if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                done_item.error_code,
                done_item.error_json.clone(),
            ) {
                results[pending.idx] = Some(Err(err));
                continue;
            }
            let expose_kind = if done_item.allocation_mode == GetAllocationMode::Temporary {
                UserMemHolderExposeKind::OwnedCopy
            } else {
                UserMemHolderExposeKind::SegPtr
            };
            let offset = pending.start_item.target_addr - pending.start_item.target_base_addr;
            let data_len = pending.start_item.len as usize;
            metrics.record_l2_hit_locality(pending.peer_is_remote, data_len as u64);
            metrics.record_get_io_locality(
                pending.peer_is_remote,
                data_len as u64,
                pending.transfer_us,
            );
            let memory_info = Arc::new(
                MemoryInfo::new(
                    offset,
                    pending.start_item.len as u32,
                    done_item.holder_id,
                    pending.key.clone(),
                    master_node_id.clone(),
                    self.view.clone(),
                )
                .await,
            );
            let get_info = RemoteGetInfo {
                get_id: pending.start_item.get_id,
                data_len,
                src_addr: pending.start_item.src_addr,
                target_addr: pending.start_item.target_addr,
                node_id: pending.start_item.node_id.clone().into(),
                peer_is_src_or_target: pending.peer_is_remote,
            };
            if done_item.allocation_mode != GetAllocationMode::Temporary {
                if self.install_get_cached_info_if_unfenced(
                    &pending.key,
                    pending.start_item.put_id,
                    memory_info.clone(),
                ) {
                    metrics.observe_cache_value_size(
                        &client_id,
                        node_role.as_str(),
                        data_len as u64,
                    );
                }
            }
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info,
                self.get_or_init_all_memholder_refcount(),
                expose_kind,
            ));
            results[pending.idx] = Some(Ok(Some((user_mem_holder, Some(get_info)))));
        }

        Ok(results
            .into_iter()
            .map(|item| {
                item.unwrap_or_else(|| {
                    Err(KvError::Api(ApiError::Unknown {
                        detail: "batch_get result slot was not populated".to_string(),
                    }))
                })
            })
            .collect())
    }

    pub async fn batch_is_exist(
        &self,
        keys: Vec<String>,
        allow_local_snapshot: bool,
    ) -> KvResult<Vec<bool>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_is_exist".to_string(),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = vec![false; keys.len()];
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        for (idx, key) in keys.iter().enumerate() {
            if allow_local_snapshot && self.has_local_snapshot(key) {
                results[idx] = true;
                continue;
            }
            missing_indices.push(idx);
            missing_keys.push(key.clone());
        }
        if missing_keys.is_empty() {
            return Ok(results);
        }

        let req = MsgPack {
            serialize_part: BatchIsExistReq { keys: missing_keys },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_is_exist
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        let resp_part = resp.serialize_part;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp_part.error_code,
            resp_part.error_json.clone(),
        )?;
        if resp_part.exists_list.len() != missing_indices.len() {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "batch_is_exist response length mismatch: expected={} got={}",
                    missing_indices.len(),
                    resp_part.exists_list.len()
                ),
            }));
        }
        for (idx, exists) in missing_indices
            .into_iter()
            .zip(resp_part.exists_list.into_iter())
        {
            results[idx] = exists;
        }
        Ok(results)
    }

    pub async fn is_exist_with_local_snapshot(
        &self,
        key: &str,
        allow_local_snapshot: bool,
    ) -> KvResult<bool> {
        let mut results = self
            .batch_is_exist(vec![key.to_string()], allow_local_snapshot)
            .await?;
        Ok(results.pop().unwrap_or(false))
    }

    /// becaused we cached local kv metadata, so we make `MemHolder` with Arc here
    pub async fn get(
        &self,
        key: &str,
    ) -> KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get".to_string(),
            }));
        }
        let metrics = self.metrics_handle();
        let client_id = self.client_id_str();
        let node_role = self.node_role();

        if let Some(memory_info) = self.local_visible_mem_holder(key) {
            // exist, directly return
            tracing::debug!("local visible cache hit for key: {}, directly return", key);
            // Build a fresh UserMemHolder from cached MemoryInfo
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info.clone(),
                self.get_or_init_all_memholder_refcount(),
                UserMemHolderExposeKind::SegPtr,
            ));
            obe_get_cache_hit(
                &metrics,
                &client_id,
                &node_role,
                key,
                memory_info.len as u64,
            );
            return Ok(Some((user_mem_holder, None)));
        }

        let lock = self.get_remote_kv_lock.get_lock(key.to_owned());
        let _guard = lock.lock().await;

        // Recheck after acquiring the miss lock so concurrent cache-fillers can collapse here
        // without forcing every cache hit through the async lock path.
        if let Some(memory_info) = self.local_visible_mem_holder(key) {
            tracing::debug!(
                "local visible cache hit after miss-lock for key: {}, directly return",
                key
            );
            let user_mem_holder = Arc::new(UserMemHolder::new(
                memory_info.clone(),
                self.get_or_init_all_memholder_refcount(),
                UserMemHolderExposeKind::SegPtr,
            ));
            obe_get_cache_hit(
                &metrics,
                &client_id,
                &node_role,
                key,
                memory_info.len as u64,
            );
            return Ok(Some((user_mem_holder, None)));
        }

        obe_get_cache_miss(&metrics, &client_id, &node_role, key);
        let t1 = Utc::now().timestamp_micros();
        let resp = {
            match self.get_start(key).await {
                Ok(resp) => resp,
                Err(err) => {
                    obe_get_start_error_rpc(&metrics, &client_id, &node_role, key);
                    return Err(err);
                }
            }
        };
        let start_handle_us = resp.server_process_us;
        let t2 = Utc::now().timestamp_micros();
        // start stage success
        // Note: only record timestamps; no scope begin/end
        //       errors handled above and below
        if resp.error_code != OK {
            if resp.error_code == codes_api::API_KEY_NOT_FOUND {
                obe_get_start_not_found(&metrics, &client_id, &node_role, key);
                return Ok(None);
            }
            obe_get_start_error_status(&metrics, &client_id, &node_role, key);
            crate::rpcresp_kvresult_convert::try_from_code(
                resp.error_code,
                resp.error_json.clone(),
            )?;
            unreachable!("try_from_code should have returned Err for non-OK, unreachable");
        }
        obe_get_start_success(&metrics, &client_id, &node_role, key, t1, t2);

        let put_id = resp.put_id;
        let get_id = resp.get_id;
        let data_len = resp.len as usize;

        let abs_src = resp.src_addr;
        let abs_target = resp.target_addr;

        // debug get slice from src_addr and len
        tracing::debug!(
            "kv get src addr {:#x} to target addr {:#x}",
            abs_src,
            abs_target
        );

        let peer_id = if &*resp.node_id == &*self.view.cluster_manager().get_self_info().id {
            None
        } else {
            Some(resp.node_id.clone())
        };

        #[cfg(test)]
        {
            self.test_record.add_transfering_get(
                get_id,
                key.to_string(),
                data_len as u32,
                abs_target,
                resp.node_id.to_string(),
                peer_id.is_some(),
            );
        }

        // transfer data (skip if local and src==target to avoid redundant copy)
        if peer_id.is_none() && abs_src == abs_target {
            tracing::debug!(
                "kv get local no-op: src==target {:#x}, len={} (skip transfer)",
                abs_target,
                data_len
            );
        } else {
            // tracing::debug!(
            //     "kv get transfer in transfer engine path from {}",
            //     peer_id.as_ref().map(|v| &**v).unwrap_or("self")
            // );
            tracing::debug!(
                "p2p get transfer: key={}, remote_src={:#x} -> local_target={:#x}, len={}, peer={:?}",
                key,
                abs_src,
                abs_target,
                data_len,
                peer_id
            );
            if let Err(e) = self
                .view
                .client_transfer_engine()
                .transfer_data_no_copy(
                    peer_id.clone(),
                    true,
                    abs_src,
                    abs_target,
                    data_len as u64,
                    None,
                )
                .await
            {
                tracing::warn!("transfer data failed: {:?}", e);

                #[cfg(test)]
                {
                    self.test_record.remove_transfering_get(get_id);
                }

                obe_get_transfer_error(&metrics, &client_id, &node_role, key, data_len as u64);
                self.get_revoke(get_id).await?;
                return Err(KvError::Api(ApiError::Transfer {
                    from_addr: abs_src,
                    to_addr: abs_target,
                    len: data_len as u64,
                    error: e.to_string(),
                }));
            } else {
                tracing::debug!(
                    "get_transfer success key={}, src_addr={:#x}, target_addr={:#x}, len={}, peer_id={:?}",
                    key,
                    abs_src,
                    abs_target,
                    data_len,
                    peer_id
                );
            }
        }
        let t3 = Utc::now().timestamp_micros();
        obe_get_transfer_success(
            &metrics,
            &client_id,
            &node_role,
            key,
            data_len as u64,
            t2,
            t3,
        );

        // Removed post-transfer zero-header verification per request.

        // Complete the get operation and get holder_id
        let done_resp = match self.get_done(get_id).await {
            Ok(resp) => resp,
            Err(err) => {
                obe_get_end_error_rpc(&metrics, &client_id, &node_role, key, data_len as u64);
                return Err(err);
            }
        };
        let end_handle_us = done_resp.server_process_us;
        let t4 = Utc::now().timestamp_micros();
        if done_resp.error_code != OK {
            obe_get_done_error_status(&metrics, &client_id, &node_role, key, data_len as u64);
            #[cfg(test)]
            {
                self.test_record.remove_transfering_get(get_id);
            }

            crate::rpcresp_kvresult_convert::try_from_code(
                done_resp.error_code,
                done_resp.error_json.clone(),
            )?;
            unreachable!("error path should have returned above");
        }
        // end/done stage success and push detailed metrics
        obe_get_done_success(
            &metrics,
            &client_id,
            &node_role,
            key,
            data_len as u64,
            get_id,
            t1,
            t2,
            t3,
            t4,
            start_handle_us,
            end_handle_us,
        );

        #[cfg(test)]
        {
            self.test_record.remove_transfering_get(get_id);
        }

        // pulses and network bytes emitted inside obe_get_done_success

        let holder_id = done_resp.holder_id;
        let expose_kind = if done_resp.allocation_mode == GetAllocationMode::Temporary {
            UserMemHolderExposeKind::OwnedCopy
        } else {
            UserMemHolderExposeKind::SegPtr
        };
        let master_node_id: NodeID = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?
            .into();

        // Create MemHolder with keep alive functionality
        // Convert target_addr to offset using base address from master response
        let offset = resp.target_addr - resp.target_base_addr;
        let memory_info = Arc::new(
            MemoryInfo::new(
                offset,
                data_len as u32,
                holder_id,
                key.to_string(),
                master_node_id,
                self.view.clone(),
            )
            .await,
        );
        // Create GetInfo with information from the response
        let get_info = RemoteGetInfo {
            get_id,
            data_len,
            src_addr: abs_src,
            target_addr: abs_target,
            node_id: resp.node_id.into(),
            peer_is_src_or_target: true,
        };

        if done_resp.allocation_mode != GetAllocationMode::Temporary {
            if self.install_get_cached_info_if_unfenced(key, put_id, memory_info.clone()) {
                metrics.observe_cache_value_size(&client_id, node_role.as_str(), data_len as u64);
            }
        }
        let user_mem_holder = Arc::new(UserMemHolder::new(
            memory_info,
            self.get_or_init_all_memholder_refcount(),
            expose_kind,
        ));
        // let partial_hex=&user_mem_holder.bytes()[..std::cmp::min(16, user_mem_holder.bytes().len())];
        // tracing::debug!("external get done, key={}, partial_hex={:?}", key, partial_hex);
        Ok(Some((user_mem_holder, Some(get_info))))
    }

    pub async fn is_exist(&self, key: &str) -> KvResult<bool> {
        self.is_exist_with_local_snapshot(key, false).await
    }

    /// Get metadata for a key without transferring data
    pub async fn get_meta(&self, key: &str) -> KvResult<GetMetaResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get_meta".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GetMetaReq {
                key: key.to_string(),
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
        let resp = self
            .rpc_caller_get_meta
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    /// 开始 Get 操作，获取数据位置和信息
    pub async fn get_start(&self, key: &str) -> KvResult<GetStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting get_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: GetStartReq {
                key: key.to_string(),
                prepared_target: None,
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
        let resp = self
            .rpc_caller_get_start
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    pub async fn batch_get_start(&self, keys: Vec<String>) -> KvResult<BatchGetStartResp> {
        self.batch_get_start_with_prepared_targets(keys, Vec::new())
            .await
    }

    pub(crate) async fn batch_get_start_with_prepared_targets(
        &self,
        keys: Vec<String>,
        prepared_targets: Vec<Option<GetPreparedLocalReserveTarget>>,
    ) -> KvResult<BatchGetStartResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_start".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchGetStartReq {
                keys,
                prepared_targets,
            },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_get_start
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    /// 撤销 Get 操作，释放已分配的资源
    pub async fn get_revoke(&self, get_id: u64) -> KvResult<()> {
        let req = MsgPack {
            serialize_part: GetRevokeReq { get_id },
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
            .rpc_caller_get_revoke
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(())
    }

    pub async fn batch_get_revoke(&self, get_ids: Vec<u64>) -> KvResult<BatchGetRevokeResp> {
        if get_ids.is_empty() {
            return Ok(BatchGetRevokeResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: BatchGetRevokeReq { get_ids },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_get_revoke
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }

    /// 完成 Get 操作，清理资源
    pub async fn get_done(&self, get_id: u64) -> KvResult<GetDoneResp> {
        let req = MsgPack {
            serialize_part: GetDoneReq { get_id },
            raw_bytes: Vec::new(),
        };

        // 获取 master 节点 ID
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;

        // 调用 RPC
        let resp = self
            .rpc_caller_get_done
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 0)
            .await
            .map_err(KvError::from)?;

        Ok(resp.serialize_part)
    }

    pub async fn batch_get_done(&self, get_ids: Vec<u64>) -> KvResult<BatchGetDoneResp> {
        if get_ids.is_empty() {
            return Ok(BatchGetDoneResp {
                items: Vec::new(),
                error_code: OK,
                error_json: String::new(),
                server_process_us: 0,
            });
        }
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting batch_get_done".to_string(),
            }));
        }
        // The master performs synchronous Moka policy work before acknowledging
        // committed-slot Done.  Bound the number of callers entering that path
        // so a capacity scan cannot park every master Tokio worker on Moka's
        // blocking housekeeper lock.  The permit covers one RPC attempt only;
        // idempotent retry backoff releases it and lets other atomic_batches converge.
        let _done_rpc_permit = batch_get_done_rpc_limiter()
            .acquire()
            .await
            .expect("the process-wide BatchGetDone limiter is never closed");
        let req = MsgPack {
            serialize_part: BatchGetDoneReq { get_ids },
            raw_bytes: Vec::new(),
        };
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let resp = self
            .rpc_caller_batch_get_done
            .call(self.view.p2p_module(), master_node_id.into(), req, None, 2)
            .await
            .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            resp.serialize_part.error_code,
            resp.serialize_part.error_json.clone(),
        )?;
        Ok(resp.serialize_part)
    }
}

#[cfg(test)]
mod tests {
    use super::batch_get_done_response_matches;
    use crate::master_kv_router::msg_pack::{BatchGetDoneItemResp, BatchGetDoneResp};

    fn response(get_ids: &[u64]) -> BatchGetDoneResp {
        BatchGetDoneResp {
            items: get_ids
                .iter()
                .map(|get_id| BatchGetDoneItemResp {
                    get_id: *get_id,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn batch_get_done_requires_exact_response_identity() {
        assert!(batch_get_done_response_matches(
            &[11, 22],
            &response(&[11, 22])
        ));
        assert!(!batch_get_done_response_matches(
            &[11, 22],
            &response(&[22, 11])
        ));
        assert!(!batch_get_done_response_matches(
            &[11, 22],
            &response(&[11])
        ));
    }
}
