use super::{
    ClientKvApiInner, OwnerLocalReserveClassState, OwnerLocalReserveGrantState,
    OwnerLocalReservePoolState, OwnerLocalReserveSlotLease, ReplicaTaskJob, ReplicaTaskTarget,
    local_reserve_rebalance::{owner_local_reserve_timeout_config, wait_owner_local_reserve_ready},
};
use crate::OWNER_LOCAL_RESERVE_GRANT_QUANTUM_BYTES;
use crate::cluster_manager::NodeIDString;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_kv_router::put::PutIDForAKey;
// no StageScope; timestamps-based metrics only
use crate::memholder::kvclient_encode::{calc_flat_dict_encoded_len, write_flat_dict_ptrs_to_ptr};
use crate::observe_kvope::{
    obe_put_start_error_rpc, obe_put_start_error_status, obe_put_start_success,
};
use crate::{
    client_kv_api::ClientKvApiView,
    master_kv_router::msg_pack::{
        BatchPreparePutKeyItemReq, BatchPreparePutKeysReq, BatchPreparePutKeysResp,
        BatchPutDoneItemReq, BatchPutDoneReq, BatchPutDoneResp, BatchPutRevokeItemReq,
        BatchPutRevokeReq, BatchPutRevokeResp, BatchPutStartItemReq, BatchPutStartReq,
        BatchPutStartResp, BatchReleasePutKeyReservationsReq, BatchReleasePutKeyReservationsResp,
        PutAppendDoneReq, PutAppendDoneResp, PutAppendRevokeReq, PutAppendStartReq,
        PutAppendStartResp, PutDoneCommittedSlot, PutDoneReq, PutRevokeReq, PutStartReq,
        PutStartResp, ReleaseLocalGrantReq, ReserveLocalGrantReq, ReserveLocalGrantResp,
    },
    memholder::{UserMemHolder, UserMemHolderExposeKind},
    p2p::msg_pack::MsgPack,
    p2p::p2p_module::RpcTransportPolicy,
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use chrono::Utc;
use fluxon_commu::TransferBreakdown;
use limit_thirdparty::tokio;
use std::sync::Arc;
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
    pub remember_local_snapshot: bool,
    pub make_replica_task: bool,
    pub replica_target: Option<ReplicaTaskTarget>,
}

#[derive(Debug, Clone)]
pub struct OwnerLocalPublishItem {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub value_len: u64,
    pub lease_id: Option<u64>,
    pub committed_slot: PutDoneCommittedSlot,
    pub make_replica_task: bool,
    pub preferred_sub_cluster: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OwnerLocalPublishJob {
    pub items: Vec<OwnerLocalPublishItem>,
    pub key_reservation_ids: Vec<u64>,
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
            let replica_target =
                start_item
                    .replica_target
                    .as_ref()
                    .map(|target| ReplicaTaskTarget {
                        node_id: target.node_id.clone(),
                        target_offset: target.target_addr - target.target_base_addr,
                        target_base_addr: target.target_base_addr,
                        len: target.len,
                    });
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
                remember_local_snapshot: true,
                make_replica_task: start_req.make_replica_task && replica_target.is_some(),
                replica_target,
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
        _transfer_concurrency: usize,
    ) -> KvResult<Vec<KvResult<()>>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        #[derive(Clone)]
        struct DonePending {
            idx: usize,
            item: OwnerReservedPutItem,
        }

        let metrics = self.metrics_handle();
        let mut results: Vec<Option<KvResult<()>>> = (0..items.len()).map(|_| None).collect();
        let mut done_pending = Vec::with_capacity(items.len());

        for (idx, item) in items.into_iter().enumerate() {
            metrics.record_put_io_locality(false, item.value_len, 0);
            done_pending.push(DonePending { idx, item });
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
            if pending.item.make_replica_task {
                let target = pending
                    .item
                    .replica_target
                    .clone()
                    .expect("make_replica_task requires pre-reserved replica target");
                if let Err(err) = self
                    .make_replica_task(&pending.item.key, pending.item.put_id, target)
                    .await
                {
                    tracing::warn!(
                        "owner_batch_put_commit_reserved make replica task failed after local commit: key={} put_id=({},{}) err={}",
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
        _transfer_concurrency: usize,
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
        let lease_id = opts.lease_id();
        let reject_if_inflight_same_key = opts.reject_if_inflight_same_key();
        let reject_if_exist_same_key = opts.reject_if_exist_same_key();
        let make_replica_task = opts.make_replica_task();
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
                make_replica_task,
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
            make_replica_task: bool,
            replica_target: Option<ReplicaTaskTarget>,
        }

        let mut results: Vec<Option<KvResult<()>>> = (0..keys.len()).map(|_| None).collect();
        let mut done_pending = Vec::new();
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
            let remember_local_snapshot = true;
            let replica_target =
                start_item
                    .replica_target
                    .as_ref()
                    .map(|target| ReplicaTaskTarget {
                        node_id: target.node_id.clone(),
                        target_offset: target.target_addr - target.target_base_addr,
                        target_base_addr: target.target_base_addr,
                        len: target.len,
                    });

            if !short_circuit_payload {
                unsafe {
                    write_flat_dict_ptrs_to_ptr(start_item.src_addr as *mut u8, &ptrs);
                }
            }

            done_pending.push(DonePending {
                idx,
                key,
                put_id,
                lease_id,
                remember_local_snapshot,
                make_replica_task: make_replica_task && replica_target.is_some(),
                replica_target,
            });
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
            if pending.make_replica_task {
                let target = pending
                    .replica_target
                    .clone()
                    .expect("make_replica_task requires pre-reserved replica target");
                if let Err(err) = self
                    .make_replica_task(&pending.key, pending.put_id, target)
                    .await
                {
                    tracing::warn!(
                        "batch make replica task failed after local commit: key={} put_id=({},{}) err={}",
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

    pub async fn make_replica_task(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        target: ReplicaTaskTarget,
    ) -> KvResult<()> {
        let Some(memory_info) = self.local_visible_mem_holder(key) else {
            tracing::warn!(
                "replica task source holder is unavailable after local commit; dropping replica task: key={} put_id=({},{})",
                key,
                put_id.0,
                put_id.1
            );
            return Ok(());
        };

        let holder = Arc::new(UserMemHolder::new(
            memory_info,
            self.get_or_init_all_memholder_refcount(),
            UserMemHolderExposeKind::SegPtr,
        ));

        match self.replica_task_tx.try_send(ReplicaTaskJob {
            key: key.to_string(),
            put_id,
            holder,
            target: Some(target),
            preferred_sub_cluster: None,
        }) {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::warn!(
                    "replica task actor enqueue failed; dropping replica task: key={} put_id=({},{}) err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    err
                );
                Ok(())
            }
        }
    }

    pub async fn make_replica_append_task(
        &self,
        key: &str,
        put_id: PutIDForAKey,
        preferred_sub_cluster: Option<String>,
    ) -> KvResult<()> {
        let Some(memory_info) = self.local_visible_mem_holder(key) else {
            tracing::warn!(
                "replica append task source holder is unavailable after local publish; dropping replica task: key={} put_id=({},{})",
                key,
                put_id.0,
                put_id.1
            );
            return Ok(());
        };

        let holder = Arc::new(UserMemHolder::new(
            memory_info,
            self.get_or_init_all_memholder_refcount(),
            UserMemHolderExposeKind::SegPtr,
        ));

        match self.replica_task_tx.try_send(ReplicaTaskJob {
            key: key.to_string(),
            put_id,
            holder,
            target: None,
            preferred_sub_cluster,
        }) {
            Ok(()) => Ok(()),
            Err(err) => {
                tracing::warn!(
                    "replica append task actor enqueue failed; dropping replica task: key={} put_id=({},{}) err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    err
                );
                Ok(())
            }
        }
    }

    async fn put_common<F>(
        &self,
        key: &str,
        payload_len: u64,
        len_for_start: u32,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
        make_replica_task: bool,
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
                    make_replica_task,
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
        let replica_target = resp
            .replica_target
            .as_ref()
            .map(|target| ReplicaTaskTarget {
                node_id: target.node_id.clone(),
                target_offset: target.target_addr - target.target_base_addr,
                target_base_addr: target.target_base_addr,
                len: target.len,
            });

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
            self.remember_local_snapshot(key, put_id);
            if make_replica_task && replica_target.is_some() {
                if let Err(err) = self
                    .make_replica_task(
                        key,
                        put_id,
                        replica_target
                            .clone()
                            .expect("make_replica_task requires pre-reserved replica target"),
                    )
                    .await
                {
                    tracing::warn!(
                        "make replica task failed after short-circuit local commit: key={} put_id=({},{}) err={}",
                        key,
                        put_id.0,
                        put_id.1,
                        err
                    );
                }
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
        if dbg_addr_summary {
            tracing::debug!(
                "put path addr summary: key={}, put_id=({},{}) local_base={:#x}, abs_src={:#x}, master_target_base={:#x}, abs_target={:#x}, peer_id={:?}",
                key,
                put_id.0,
                put_id.1,
                base_addr,
                abs_src,
                resp.target_base_addr,
                abs_target,
                peer_id
            );
        }

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
        self.remember_local_snapshot(key, put_id);
        if make_replica_task && replica_target.is_some() {
            if let Err(err) = self
                .make_replica_task(
                    key,
                    put_id,
                    replica_target
                        .clone()
                        .expect("make_replica_task requires pre-reserved replica target"),
                )
                .await
            {
                tracing::warn!(
                    "make replica task failed after local commit: key={} put_id=({},{}) err={}",
                    key,
                    put_id.0,
                    put_id.1,
                    err
                );
            }
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
        let make_replica_task = opts.make_replica_task();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());

        let payload_len = calc_flat_dict_encoded_len(&ptrs)?;
        self.put_common(
            key,
            payload_len,
            payload_len as u32,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            make_replica_task,
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
        let make_replica_task = opts.make_replica_task();
        let preferred_sub_cluster = opts.preferred_sub_cluster().map(|s| s.to_string());
        let payload_len = value.len() as u64;
        self.put_common(
            key,
            payload_len,
            value.len() as u32,
            reject_if_inflight_same_key,
            reject_if_exist_same_key,
            make_replica_task,
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

pub fn spawn_replica_task_actor(
    view: ClientKvApiView,
    mut rx: tokio::sync::ampsc::Receiver<ReplicaTaskJob>,
) {
    let view_task = view.clone();
    let _ = view.spawn("replica_task_actor", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let job = tokio::select! {
                _ = shutdown_waiter.wait() => {
                    tracing::info!("replica_task_actor stopping due to shutdown signal");
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
            let target = if let Some(target) = job.target.clone() {
                target
            } else {
                let len_u32 = match u32::try_from(len) {
                    Ok(len_u32) => len_u32,
                    Err(_) => {
                        tracing::warn!(
                            "replica append task length does not fit u32: key={} put_id=({},{}) len={}",
                            job.key,
                            job.put_id.0,
                            job.put_id.1,
                            len
                        );
                        continue;
                    }
                };
                let append_start = match inner
                    .put_append_start(
                        &job.key,
                        job.put_id,
                        len_u32,
                        job.preferred_sub_cluster.as_deref(),
                    )
                    .await
                {
                    Ok(resp) => resp,
                    Err(err) => {
                        tracing::warn!(
                            "replica append task start failed: key={} put_id=({},{}) err={}",
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
                        "replica append task not scheduled: key={} put_id=({},{})",
                        job.key,
                        job.put_id.0,
                        job.put_id.1
                    );
                    continue;
                }
                ReplicaTaskTarget {
                    node_id: append_start.node_id,
                    target_offset: append_start.target_addr - append_start.target_base_addr,
                    target_base_addr: append_start.target_base_addr,
                    len: append_start.len,
                }
            };
            if len != target.len {
                tracing::warn!(
                    "replica task length mismatch: key={} put_id=({},{}) src_len={} target_len={}",
                    job.key,
                    job.put_id.0,
                    job.put_id.1,
                    len,
                    target.len
                );
                spawn_replica_task_revoke(
                    view_task.clone(),
                    job.key,
                    job.put_id,
                    "length mismatch",
                );
                continue;
            }
            if let Err(err) = inner
                .put_transfer(
                    &job.key,
                    job.put_id,
                    src_offset,
                    target.target_offset,
                    len,
                    Some(target.node_id.clone()),
                    Some(target.target_base_addr),
                )
                .await
            {
                tracing::warn!(
                    "replica task transfer failed: key={} put_id=({},{}) err={}",
                    job.key,
                    job.put_id.0,
                    job.put_id.1,
                    err
                );
                spawn_replica_task_revoke(view_task.clone(), job.key, job.put_id, "transfer error");
                continue;
            }
            match inner.put_append_done(&job.key, job.put_id).await {
                Ok(resp) => {
                    tracing::debug!(
                        "replica task append done: key={} put_id=({},{}) appended={}",
                        job.key,
                        job.put_id.0,
                        job.put_id.1,
                        resp.appended
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "replica task append done failed: key={} put_id=({},{}) err={}",
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

async fn publish_owner_local_job(view: ClientKvApiView, job: OwnerLocalPublishJob) {
    let inner = view.client_kv_api().inner();
    if job.items.is_empty() {
        release_owner_local_publish_reservations(inner, job.key_reservation_ids).await;
        return;
    }

    let done_req_items = job
        .items
        .iter()
        .map(|item| BatchPutDoneItemReq {
            key: item.key.clone(),
            put_id: item.put_id,
            lease_id: item.lease_id,
            committed_slot: Some(item.committed_slot.clone()),
            publish_local_cache: false,
        })
        .collect::<Vec<_>>();

    match inner.batch_put_done(done_req_items).await {
        Ok(done_resp) if done_resp.items.len() == job.items.len() => {
            for (item, done_item) in job.items.iter().zip(done_resp.items.into_iter()) {
                if let Err(err) = crate::rpcresp_kvresult_convert::try_from_code(
                    done_item.error_code,
                    done_item.error_json.clone(),
                ) {
                    tracing::warn!(
                        "owner local publish failed: key={} put_id=({},{}) err={}",
                        item.key,
                        item.put_id.0,
                        item.put_id.1,
                        err
                    );
                    continue;
                }
                if item.make_replica_task {
                    if let Err(err) = inner
                        .make_replica_append_task(
                            &item.key,
                            item.put_id,
                            item.preferred_sub_cluster.clone(),
                        )
                        .await
                    {
                        tracing::warn!(
                            "owner local publish enqueue replica append failed: key={} put_id=({},{}) err={}",
                            item.key,
                            item.put_id.0,
                            item.put_id.1,
                            err
                        );
                    }
                }
            }
        }
        Ok(done_resp) => {
            tracing::warn!(
                "owner local publish response length mismatch: expected={} got={}",
                job.items.len(),
                done_resp.items.len()
            );
        }
        Err(err) => {
            tracing::warn!(
                "owner local publish batch_put_done rpc failed: key_count={} err={}",
                job.items.len(),
                err
            );
        }
    }

    release_owner_local_publish_reservations(inner, job.key_reservation_ids).await;
}

async fn release_owner_local_publish_reservations(
    inner: &ClientKvApiInner,
    key_reservation_ids: Vec<u64>,
) {
    if let Err(err) = inner
        .batch_release_put_key_reservations(key_reservation_ids)
        .await
    {
        tracing::warn!(
            "owner local publish key reservation cleanup failed: {}",
            err
        );
    }
}

fn spawn_replica_task_revoke(
    view: ClientKvApiView,
    key: String,
    put_id: PutIDForAKey,
    reason: &'static str,
) {
    let _ = view.clone().spawn("replica_task_revoke", async move {
        let inner = view.client_kv_api().inner();
        if let Err(revoke_err) = inner.put_append_revoke(&key, put_id).await {
            tracing::warn!(
                "replica task append revoke failed after {}: key={} put_id=({},{}) err={}",
                reason,
                key,
                put_id.0,
                put_id.1,
                revoke_err
            );
        }
    });
}
