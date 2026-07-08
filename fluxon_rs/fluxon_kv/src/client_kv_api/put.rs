use super::{
    local_reserve_rebalance::{owner_local_reserve_timeout_config, wait_owner_local_reserve_ready},
    ClientKvApiInner, OwnerLocalReserveClassState, OwnerLocalReserveGrantState,
    OwnerLocalReservePoolState, OwnerLocalReserveSlotLease, WriteBackAppendJob,
};
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::cluster_manager::NodeIDString;
use crate::master_kv_router::put::PutIDForAKey;
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
// no StageScope; timestamps-based metrics only
use crate::memholder::kvclient_encode::{calc_flat_dict_encoded_len, write_flat_dict_ptrs_to_ptr};
use crate::observe_kvope::{
    obe_put_start_error_rpc, obe_put_start_error_status, obe_put_start_success,
    obe_put_transfer_error,
};
use crate::{
    client_kv_api::ClientKvApiView,
    master_kv_router::msg_pack::{
        BatchPreparePutKeyItemReq, BatchPreparePutKeysReq, BatchPreparePutKeysResp,
        BatchPutDoneItemReq, BatchPutDoneReq, BatchPutDoneResp, BatchPutRevokeItemReq,
        BatchPutRevokeReq, BatchPutRevokeResp, BatchPutStartItemReq, BatchPutStartReq,
        BatchPutStartResp, BatchReleasePutKeyReservationsReq, BatchReleasePutKeyReservationsResp,
        PutAppendDoneReq, PutAppendDoneResp, PutAppendRevokeReq, PutAppendStartReq,
        PutAppendStartResp, PutDoneReq, PutRevokeReq, PutStartReq, PutStartResp,
        ReleaseLocalGrantReq, ReserveLocalGrantReq, ReserveLocalGrantResp,
    },
    p2p::msg_pack::MsgPack,
    p2p::p2p_module::RpcTransportPolicy,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use chrono::Utc;
use fluxon_commu::TransferBreakdown;
use futures::stream::{self, StreamExt};
use limit_thirdparty::tokio;
use std::time::Instant;
use tracing::info;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

const OWNER_LOCAL_RESERVE_MIN_SLOT_SIZE_BYTES: u64 = 4 * 1024;
fn owner_local_reserve_slot_size(value_len: u64) -> KvResult<u64> {
    let normalized = value_len.max(OWNER_LOCAL_RESERVE_MIN_SLOT_SIZE_BYTES);
    let slot_size = normalized.checked_next_power_of_two().ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "value_len={} exceeds resident local reserve slot size range",
                value_len
            ),
        })
    })?;
    if slot_size > OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!(
                "value_len={} requires slot_size={} larger than local reserve grant quantum={}",
                value_len, slot_size, OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES
            ),
        }));
    }
    Ok(slot_size)
}

fn owner_local_reserve_slots_per_grant(slot_size: u64) -> u32 {
    let slots = (OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES / slot_size).max(1);
    slots.min(u32::MAX as u64) as u32
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
    class_state.grants.push(grant);
}

fn owner_local_reserve_try_claim(
    pool: &mut OwnerLocalReservePoolState,
    slot_size: u64,
    slots_per_grant: u32,
    value_len: u64,
    key_count: usize,
) -> Option<OwnerLocalReserveSlotLease> {
    let class_state = pool
        .classes
        .entry(slot_size)
        .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
    if class_state.free_slot_count() < key_count {
        return None;
    }
    let mut slots = Vec::with_capacity(key_count);
    for grant in class_state.grants.iter_mut() {
        while slots.len() < key_count {
            match grant.claim_prepared_slot() {
                Some(slot) => slots.push(slot),
                None => break,
            }
        }
        if slots.len() == key_count {
            break;
        }
    }
    assert!(
        slots.len() == key_count,
        "free_slot_count check and claim path diverged"
    );
    Some(OwnerLocalReserveSlotLease {
        value_len,
        slot_size,
        slots,
    })
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
    pub write_through: bool,
    pub remember_local_snapshot: bool,
    pub enqueue_write_back_append: bool,
    pub preferred_sub_cluster: Option<String>,
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

impl ClientKvApiInner {
    pub async fn owner_claim_local_reserve_slot_lease(
        &self,
        value_len: u64,
        key_count: usize,
    ) -> KvResult<OwnerLocalReserveSlotLease> {
        let slot_size = owner_local_reserve_slot_size(value_len)?;
        let slots_per_grant = owner_local_reserve_slots_per_grant(slot_size);
        let (soft_wait_timeout, hard_wait_timeout) = owner_local_reserve_timeout_config(self);
        let hard_deadline = Instant::now()
            .checked_add(hard_wait_timeout)
            .ok_or_else(|| {
                KvError::Api(ApiError::Unknown {
                    detail: "owner local reserve hard timeout overflow".to_string(),
                })
            })?;
        let mut pending_demand_registered = false;
        let claim_result = {
            let mut pool = self.owner_local_reserve_pool.lock();
            let class_state = pool
                .classes
                .entry(slot_size)
                .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
            if class_state.free_slot_count() >= key_count {
                Some(owner_local_reserve_try_claim(
                    &mut pool,
                    slot_size,
                    slots_per_grant,
                    value_len,
                    key_count,
                ))
            } else {
                None
            }
        };
        if let Some(Some(lease)) = claim_result {
            return Ok(lease);
        }
        self.owner_local_reserve_register_pending_demand(slot_size, slots_per_grant, key_count);
        pending_demand_registered = true;
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
            let soft_wait_timeout_ms = soft_wait_timeout.as_millis();
            let hard_wait_timeout_ms = hard_wait_timeout.as_millis();
            if pending_demand_registered {
                self.owner_local_reserve_consume_pending_demand(
                    slot_size,
                    slots_per_grant,
                    key_count,
                );
            }
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner local reserve refill timeout: slot_size={} key_count={} soft_wait_timeout_ms={} hard_timeout_ms={}",
                    slot_size, key_count, soft_wait_timeout_ms, hard_wait_timeout_ms
                ),
            }));
        }
        let claim_result = {
            let mut pool = self.owner_local_reserve_pool.lock();
            let class_state = pool
                .classes
                .entry(slot_size)
                .or_insert_with(|| OwnerLocalReserveClassState::new(slot_size, slots_per_grant));
            if class_state.free_slot_count() >= key_count {
                Some(owner_local_reserve_try_claim(
                    &mut pool,
                    slot_size,
                    slots_per_grant,
                    value_len,
                    key_count,
                ))
            } else {
                None
            }
        };
        if let Some(Some(lease)) = claim_result {
            self.owner_local_reserve_consume_pending_demand(slot_size, slots_per_grant, key_count);
            return Ok(lease);
        }
        self.owner_local_reserve_consume_pending_demand(slot_size, slots_per_grant, key_count);
        Err(KvError::Api(ApiError::Unknown {
            detail: format!(
                "owner local reserve ready check returned without available slots: slot_size={} key_count={}",
                slot_size, key_count
            ),
        }))
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
                let Some(grant) = class_state
                    .grants
                    .iter_mut()
                    .find(|grant| grant.grant_id == slot_ref.grant_id)
                else {
                    return Err(KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "resident local reserve grant missing while releasing slot lease: grant_id={}",
                            slot_ref.grant_id
                        ),
                    }));
                };
                grant.release_prepared_slot(slot_ref.slot_index);
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
        let Some(grant) = class_state
            .grants
            .iter_mut()
            .find(|grant| grant.grant_id == grant_id)
        else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve grant missing while marking pending slot: grant_id={}",
                    grant_id
                ),
            }));
        };
        grant.mark_prepared_slot_pending_visible(slot_index);
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
        let Some(grant) = class_state
            .grants
            .iter_mut()
            .find(|grant| grant.grant_id == grant_id)
        else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve grant missing while promoting pending slot: grant_id={}",
                    grant_id
                ),
            }));
        };
        grant.promote_pending_visible_slot_to_committed(slot_index);
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
        let Some(grant) = class_state
            .grants
            .iter_mut()
            .find(|grant| grant.grant_id == grant_id)
        else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "local reserve grant missing while retaining resident slot holder: grant_id={}",
                    grant_id
                ),
            }));
        };
        grant.retain_resident_slot_holder(slot_index);
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
            let Some(grant) = class_state
                .grants
                .iter_mut()
                .find(|grant| grant.grant_id == grant_id)
            else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve grant missing while releasing resident slot holder: grant_id={}",
                        grant_id
                    ),
                }));
            };
            grant.release_resident_slot_holder(slot_index);
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
            let Some(grant) = class_state
                .grants
                .iter_mut()
                .find(|grant| grant.grant_id == grant_id)
            else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "local reserve grant missing while releasing committed slot route: grant_id={}",
                        grant_id
                    ),
                }));
            };
            grant.release_committed_slot_route(slot_index);
        }
        self.owner_local_reserve_rebalance_notify().notify_waiters();
        Ok(())
    }

    pub async fn owner_shutdown_local_reserve_pool(&self) -> KvResult<()> {
        let grants = {
            let mut pool = self.owner_local_reserve_pool.lock();
            let mut detached = Vec::new();
            for (_slot_size, class_state) in pool.classes.drain() {
                detached.extend(class_state.grants);
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
                write_through: start_req.write_through,
                remember_local_snapshot: !start_req.write_through || peer_node_id.is_none(),
                enqueue_write_back_append: !start_req.write_through && peer_node_id.is_some(),
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
        transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<()>>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            item: OwnerReservedPutItem,
        }

        let transfer_concurrency = transfer_concurrency.max(1);
        let metrics = self.metrics_handle();
        let mut results: Vec<Option<KvResult<()>>> = (0..items.len()).map(|_| None).collect();
        let mut done_pending = Vec::with_capacity(items.len());
        let mut revoke_pending = Vec::new();
        let mut transfer_futures = Vec::new();

        for (idx, item) in items.into_iter().enumerate() {
            if !item.write_through {
                metrics.record_put_io_locality(false, item.value_len, 0);
                done_pending.push(DonePending { idx, item });
                continue;
            }
            if item.peer_node_id.is_none() && item.src_addr == item.target_addr {
                metrics.record_put_io_locality(false, item.value_len, 0);
                done_pending.push(DonePending { idx, item });
                continue;
            }

            let src_offset = item.src_addr - item.src_base_addr;
            let target_offset = if item.peer_node_id.is_some() {
                item.target_addr - item.target_base_addr
            } else {
                item.target_addr - item.src_base_addr
            };
            transfer_futures.push(async move {
                let transfer_started_at = Instant::now();
                let transfer_result = self
                    .put_transfer(
                        &item.key,
                        item.put_id,
                        src_offset,
                        target_offset,
                        item.value_len,
                        item.peer_node_id.clone(),
                        item.peer_node_id.as_ref().map(|_| item.target_base_addr),
                    )
                    .await;
                (
                    idx,
                    item,
                    duration_to_i64_us(transfer_started_at.elapsed()),
                    transfer_result,
                )
            });
        }

        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (idx, item, wall_transfer_us, Ok(breakdown)) => {
                    let breakdown_transfer_us = breakdown.submit_blocking_us
                        + breakdown.create_xfer_req_us
                        + breakdown.post_xfer_req_us
                        + breakdown.poll_wait_us;
                    let transfer_us = breakdown_transfer_us.max(wall_transfer_us);
                    metrics.record_put_io_locality(
                        breakdown.remote_transfer,
                        item.value_len,
                        transfer_us,
                    );
                    done_pending.push(DonePending { idx, item });
                }
                (idx, item, _wall_transfer_us, Err(err)) => {
                    results[idx] = Some(Err(err));
                    revoke_pending.push(BatchPutRevokeItemReq {
                        key: item.key,
                        put_id: item.put_id,
                    });
                }
            }
        }

        if !revoke_pending.is_empty() {
            if let Err(err) = self.batch_put_revoke(revoke_pending).await {
                tracing::warn!(
                    "owner_batch_put_commit_reserved batch_put_revoke failed after transfer errors: {}",
                    err
                );
            }
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
            if pending.item.enqueue_write_back_append {
                if let Err(err) = self
                    .enqueue_write_back_remote_append(
                        &pending.item.key,
                        pending.item.put_id,
                        pending.item.preferred_sub_cluster.as_deref(),
                    )
                    .await
                {
                    tracing::warn!(
                        "owner_batch_put_commit_reserved write-back remote append enqueue failed after local commit: key={} put_id=({},{}) err={}",
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
                    "batch_put_flat_dict_ptrs requires keys and ptrs_groups to have the same length: keys={} ptrs_groups={}",
                    keys.len(),
                    ptrs_groups.len()
                ),
            }));
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let transfer_concurrency = transfer_concurrency.max(1);
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let reject_if_exist_same_key = opts.reject_if_exist_same_key();
        let write_through = opts.write_through();
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
                write_through,
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
            enqueue_write_back_append: bool,
        }

        let self_node_id = self.view.cluster_manager().get_self_info().id.clone();
        let mut results: Vec<Option<KvResult<()>>> = (0..keys.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
        let mut revoke_pending = Vec::new();
        let mut transfer_futures = Vec::new();
        let short_circuit_payload = self.short_circuit_put_payload_path_enabled();
        let skip_put_end_commit = self.skip_put_end_commit_enabled();

        for (idx, (((key, ptrs), payload_len), start_item)) in keys
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
            let peer_id = if start_item.node_id == self_node_id {
                None
            } else {
                Some(start_item.node_id.clone())
            };
            let remember_local_snapshot = !write_through || peer_id.is_none();

            if !short_circuit_payload {
                unsafe {
                    write_flat_dict_ptrs_to_ptr(start_item.src_addr as *mut u8, &ptrs);
                }
            }

            if !write_through {
                done_pending.push(DonePending {
                    idx,
                    key,
                    put_id,
                    lease_id,
                    remember_local_snapshot,
                    enqueue_write_back_append: !short_circuit_payload && peer_id.is_some(),
                });
                continue;
            }

            if short_circuit_payload
                || (peer_id.is_none() && start_item.src_addr == start_item.target_addr)
            {
                done_pending.push(DonePending {
                    idx,
                    key,
                    put_id,
                    lease_id,
                    remember_local_snapshot,
                    enqueue_write_back_append: false,
                });
                continue;
            }

            let target_base_addr_opt = peer_id.as_ref().map(|_| start_item.target_base_addr);
            let src_offset = start_item.src_addr - start_item.src_base_addr;
            let target_offset = start_item.target_addr - start_item.target_base_addr;
            transfer_futures.push(async move {
                let transfer_result = self
                    .put_transfer(
                        &key,
                        put_id,
                        src_offset,
                        target_offset,
                        payload_len,
                        peer_id,
                        target_base_addr_opt,
                    )
                    .await;
                (idx, key, put_id, remember_local_snapshot, transfer_result)
            });
        }

        let mut transfer_stream =
            stream::iter(transfer_futures).buffer_unordered(transfer_concurrency);
        while let Some(joined) = transfer_stream.next().await {
            match joined {
                (idx, key, put_id, remember_local_snapshot, Ok(_breakdown)) => {
                    done_pending.push(DonePending {
                        idx,
                        key,
                        put_id,
                        lease_id,
                        remember_local_snapshot,
                        enqueue_write_back_append: false,
                    });
                }
                (idx, key, put_id, _remember_local_snapshot, Err(err)) => {
                    results[idx] = Some(Err(err));
                    revoke_pending.push(BatchPutRevokeItemReq { key, put_id });
                }
            }
        }

        if !revoke_pending.is_empty() {
            if let Err(err) = self.batch_put_revoke(revoke_pending).await {
                tracing::warn!("batch_put_revoke failed after transfer errors: {}", err);
            }
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
            if pending.enqueue_write_back_append {
                if let Err(err) = self
                    .enqueue_write_back_remote_append(
                        &pending.key,
                        pending.put_id,
                        preferred_sub_cluster.as_deref(),
                    )
                    .await
                {
                    tracing::warn!(
                        "batch write-back remote append enqueue failed after local commit: key={} put_id=({},{}) err={}",
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

    pub async fn enqueue_write_back_remote_append(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        preferred_sub_cluster: Option<&str>,
    ) -> KvResult<()> {
        let Some((holder, _)) = self.get(key).await? else {
            return Err(KvError::Api(ApiError::Unknown {
                detail: format!(
                    "write-back local commit did not produce a readable current replica: key={} put_id=({},{})",
                    key, put_id.0, put_id.1
                ),
            }));
        };
        self.write_back_append_tx
            .send(WriteBackAppendJob {
                key: key.to_string(),
                put_id,
                holder,
                preferred_sub_cluster: preferred_sub_cluster.map(|s| s.to_string()),
            })
            .await
            .map_err(|err| {
                KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "write-back append actor queue send failed: key={} put_id=({},{}) err={}",
                        key, put_id.0, put_id.1, err
                    ),
                })
            })
    }

    async fn put_common<F>(
        &self,
        key: &str,
        payload_len: u64,
        len_for_start: u32,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
        write_through: bool,
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
                    write_through,
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
            if !write_through || peer_id.is_none() {
                self.remember_local_snapshot(key, put_id);
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
        if !write_through {
            self.put_end(key, put_id, lease_id).await?;
            self.remember_local_snapshot(key, put_id);
            if peer_id.is_some() {
                if let Err(err) = self
                    .enqueue_write_back_remote_append(key, put_id, preferred_sub_cluster)
                    .await
                {
                    tracing::warn!(
                        "write-back remote append enqueue failed after local commit: key={} put_id=({},{}) err={}",
                        key,
                        put_id.0,
                        put_id.1,
                        err
                    );
                }
            }
            if let Some(tag) = info_complete_tag {
                info!(
                    "{tag} local_commit_complete key={} bytes={}",
                    key, payload_len
                );
            }
            return Ok(());
        }
        let src_offset = abs_src - base_addr;
        let (target_offset, target_base_addr_opt) = match &peer_id {
            Some(_) => (
                abs_target - resp.target_base_addr,
                Some(resp.target_base_addr),
            ),
            None => (abs_target - base_addr, None),
        };
        if dbg_addr_summary {
            tracing::debug!(
                "put path addr summary: key={}, put_id=({},{}) local_base={:#x}, abs_src={:#x}, src_off={:#x}, master_target_base={:#x}, abs_target={:#x}, tgt_off={:#x}, peer_id={:?}",
                key,
                put_id.0,
                put_id.1,
                base_addr,
                abs_src,
                src_offset,
                target_base_addr_opt.unwrap_or(base_addr),
                abs_target,
                target_offset,
                peer_id
            );
        }

        let transfer_breakdown = match self
            .put_transfer(
                key,
                put_id,
                src_offset,
                target_offset,
                payload_len,
                peer_id.clone(),
                target_base_addr_opt,
            )
            .await
        {
            Ok(breakdown) => breakdown,
            Err(e) => {
                self.put_revoke(key, put_id).await?;
                obe_put_transfer_error(&metrics, &client_id, &node_role, key, payload_len);
                return Err(e);
            }
        };
        metrics.pending_put_set_transfer_breakdown(
            put_id,
            transfer_breakdown.submit_blocking_us,
            transfer_breakdown.create_xfer_req_us,
            transfer_breakdown.post_xfer_req_us,
            transfer_breakdown.poll_wait_us,
            transfer_breakdown.poll_iters,
            transfer_breakdown.used_fast_path,
            false,
            transfer_breakdown.local_noop,
            transfer_breakdown.remote_transfer,
        );

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
        if peer_id.is_none() {
            self.remember_local_snapshot(key, put_id);
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
        let write_through = opts.write_through();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());

        let payload_len = calc_flat_dict_encoded_len(&ptrs)?;
        self.put_common(
            key,
            payload_len,
            payload_len as u32,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            write_through,
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
        let write_through = opts.write_through();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());
        let payload_len = value.len() as u64;
        self.put_common(
            key,
            payload_len,
            value.len() as u32,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            write_through,
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
        write_through: bool,
        preferred_sub_cluster: Option<&str>,
        source_node_id: Option<NodeIDString>,
    ) -> KvResult<(PutStartResp, i64)> {
        let req = MsgPack {
            serialize_part: PutStartReq {
                key: key.to_string(),
                len: len as u64,
                reject_if_inflight_same_key,
                reject_if_exist_same_key,
                write_through,
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
        write_through: bool,
        preferred_sub_cluster: Option<&str>,
    ) -> KvResult<(PutStartResp, i64)> {
        self.put_start_with_source_node(
            key,
            len,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            write_through,
            preferred_sub_cluster,
            None,
        )
        .await
    }

    pub async fn reserve_local_grant(&self) -> KvResult<ReserveLocalGrantResp> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting reserve_local_grant".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: ReserveLocalGrantReq::default(),
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
        Ok(resp.serialize_part)
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

    pub async fn put_append_revoke(&self, key: &str, put_id: PutIDForAKey) -> KvResult<()> {
        if !self.view.register_shutdown_poller().is_running() {
            return Err(KvError::Api(ApiError::SystemShutdown {
                detail: "ClientKvApi is shutting down; rejecting put_append_revoke".to_string(),
            }));
        }
        let req = MsgPack {
            serialize_part: PutAppendRevokeReq {
                key: key.to_string(),
                put_id,
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

pub fn spawn_write_back_append_actor(
    view: ClientKvApiView,
    mut rx: tokio::sync::ampsc::Receiver<WriteBackAppendJob>,
) {
    let view_task = view.clone();
    let _ = view.spawn("write_back_append_actor", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let job = tokio::select! {
                _ = shutdown_waiter.wait() => {
                    tracing::info!("write_back_append_actor stopping due to shutdown signal");
                    break;
                }
                job = rx.recv() => {
                    match job {
                        Some(job) => job,
                        None => break,
                    }
                }
            };
            let inner = view_task.client_kv_api().inner();
            let src_offset = job.holder.memory_info().offset;
            let len = job.holder.get_length() as u64;
            let append_start = match inner
                .put_append_start(
                    &job.key,
                    job.put_id,
                    job.holder.get_length(),
                    job.preferred_sub_cluster.as_deref(),
                )
                .await
            {
                Ok(resp) => resp,
                Err(err) => {
                    tracing::warn!(
                        "write-back append start failed: key={} put_id=({},{}) err={}",
                        job.key,
                        job.put_id.0,
                        job.put_id.1,
                        err
                    );
                    continue;
                }
            };
            if !append_start.scheduled {
                tracing::debug!(
                    "write-back append skipped because current version no longer needs remote replica: key={} put_id=({},{})",
                    job.key,
                    job.put_id.0,
                    job.put_id.1
                );
                continue;
            }
            let target_offset = append_start.target_addr - append_start.target_base_addr;
            if let Err(err) = inner
                .put_transfer(
                    &job.key,
                    job.put_id,
                    src_offset,
                    target_offset,
                    len,
                    Some(append_start.node_id.clone()),
                    Some(append_start.target_base_addr),
                )
                .await
            {
                tracing::warn!(
                    "write-back append transfer failed: key={} put_id=({},{}) err={}",
                    job.key,
                    job.put_id.0,
                    job.put_id.1,
                    err
                );
                if let Err(revoke_err) = inner.put_append_revoke(&job.key, job.put_id).await {
                    tracing::warn!(
                        "write-back append revoke failed after transfer error: key={} put_id=({},{}) err={}",
                        job.key,
                        job.put_id.0,
                        job.put_id.1,
                        revoke_err
                    );
                }
                continue;
            }
            match inner.put_append_done(&job.key, job.put_id).await {
                Ok(resp) => {
                    tracing::debug!(
                        "write-back append done: key={} put_id=({},{}) appended={}",
                        job.key,
                        job.put_id.0,
                        job.put_id.1,
                        resp.appended
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "write-back append done failed: key={} put_id=({},{}) err={}",
                        job.key,
                        job.put_id.0,
                        job.put_id.1,
                        err
                    );
                }
            }
        }
    });
}
