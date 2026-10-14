use crate::client_kv_api::delete::handle_batch_delete_client_kv_meta_cache;
use crate::client_kv_api::local_reserve_rebalance::{
    spawn_owner_local_reserve_rebalance_actor, spawn_owner_slot_pressure_actor,
};
use crate::client_kv_api::msg_pack::{
    ExternalBatchDeleteAckReq, ExternalBatchDeleteAckResp, ExternalBatchGetCancelReq,
    ExternalBatchGetCancelResp, ExternalBatchGetLocalProbeReq, ExternalBatchGetLocalProbeResp,
    ExternalBatchGetReq, ExternalBatchGetResp, ExternalBatchGetStartReq, ExternalBatchGetStartResp,
    ExternalBatchGetTransferReq, ExternalBatchGetTransferResp, ExternalBatchIsExistReq,
    ExternalBatchIsExistResp, ExternalBatchPutCommitReq, ExternalBatchPutCommitResp,
    ExternalBatchPutStartReq, ExternalBatchPutStartResp, ExternalBatchPutTransferEndReq,
    ExternalBatchPutTransferEndResp, ExternalDeleteAckReq, ExternalDeleteAckResp,
    ExternalDeleteReq, ExternalDeleteResp, ExternalExecutePlannedGetReq,
    ExternalExecutePlannedGetResp, ExternalGetReq, ExternalGetResp, ExternalIsExistReq,
    ExternalIsExistResp, ExternalObservabilitySnapshotReq, ExternalObservabilitySnapshotResp,
    ExternalPutCommitReq, ExternalPutCommitResp, ExternalPutRevokeReq, ExternalPutRevokeResp,
    ExternalPutStartReq, ExternalPutStartResp, ExternalPutTransferEndReq,
    ExternalPutTransferEndResp, SsdStageReadReq, SsdStageReadResp, SyncKvToFileReq,
    SyncKvToFileResp, TestPutPhaseTrace,
};
use crate::client_kv_api::reclaim::handle_batch_owner_reclaim;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::config::TestSpecConfig;
use crate::kv_ssd_storage::{
    KvSsdPersistBatchPermit, KvSsdPersistCopy, KvSsdPersistSource, KvSsdStorage, KvSsdStorageInit,
};
use crate::master_kv_router::msg_pack::{
    BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, BatchEnqueueReplicaTaskReq,
    BatchEvictOwnerSourceReq, BatchGetBindReq, BatchGetDoneReq, BatchGetPlanReq, BatchGetRevokeReq,
    BatchGetStartItemResp, BatchGetStartReq, BatchIsExistReq, BatchOwnerReclaimReq,
    BatchPreparePutKeysReq, BatchPublishOwnerSsdReq, BatchPutAppendDoneReq, BatchPutAppendStartReq,
    BatchPutDoneReq, BatchPutRevokeReq, BatchPutStartReq, BatchReleasePutKeyReservationsReq,
    DeleteClientKvMetaCacheItem, GetBindTarget, GroupedBatchPutDoneReq, RadixKvMetadata,
};
use crate::master_lease_manager::msg_pack::{AllocateClientLeaseReq, ClientLeaseKeepaliveReq};
use crate::memholder::{AllMemholderRefCount, ExternalMemHolderInfo, MemoryInfo, UserMemHolder};
use crate::memholder::{
    EnsureMemholderMgmtDeleteHandle, MemholderManagerTrait, NodeHolderKey, OwnerDeleteAckItem,
    OwnerDeleteAckMemMgr, OwnerExternalMemMgr,
};
use crate::owner_segment::{
    OWNER_TRANSFER_CLIENT_ACK_STREAM, OwnerSegmentTransferItem, OwnerSegmentTransferItemResp,
    OwnerSegmentTransferOutcome, OwnerSegmentTransferReq, OwnerSegmentTransferResp,
    OwnerTransferErrorCode, OwnerTransferItemError, OwnerTransferPeerTracker,
};
use crate::{
    client_seg_pool::{ClientSegPool, ClientSegPoolAccessTrait},
    client_transfer_engine::{ClientTransferEngine, ClientTransferEngineAccessTrait},
    cluster_manager::{ClusterEvent, ClusterManager, ClusterManagerAccessTrait},
    master_kv_router::msg_pack::{
        DeleteReq, GetDoneReq, GetMetaReq, GetRevokeReq, GetStartReq, OwnerLocalReserveControlOp,
        OwnerLocalReserveControlReq, OwnerLocalReserveControlResp, PutAppendDoneReq,
        PutAppendRevokeReq, PutAppendStartReq, PutDoneReq, PutRevokeReq, PutStartReq,
        SsdStageBeginReq, SsdStageDoneReq,
    },
    metric_reporter::{MetricReporter, MetricReporterAccessTrait},
    metrics::{KvLocalitySnapshot, MetricsHandle, OperationKind, RequestStage},
    p2p::{
        control_plane_rpc::{call_control_plane_rpc, send_control_plane_rpc_response},
        msg_pack::{RPCCaller, RPCHandler},
        p2p_module::{P2pModule, P2pModuleAccessTrait},
    },
    rpcresp_kvresult_convert::msg_and_error::{ApiError, ErrorCode, KvError, KvResult},
};
use ::tokio::sync::watch;
use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry as DashMapEntry};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_util::map_lock::AMapLock;
use fluxon_util::pin_aware_moka::{PinAwareMoka, PinGuard};
use limit_thirdparty::tokio;
use moka::notification::RemovalCause;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;

const OWNER_LOCAL_PUBLISH_QUEUE_CAPACITY: usize = 4096;
const OWNER_LOCAL_PUBLISH_MAX_INFLIGHT: usize = 64;
const SSD_STAGE_RPC_TIMEOUT: Duration = Duration::from_secs(300);
const SSD_STAGE_TERMINAL_TTL: Duration = Duration::from_secs(10 * 60);
const SSD_STAGE_DONE_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(50);
const SSD_STAGE_DONE_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct CompletedSsdStage {
    request: SsdStageReadReq,
    response: SsdStageReadResp,
}

struct SsdStageSharedOp {
    request: SsdStageReadReq,
    terminal: watch::Sender<Option<SsdStageReadResp>>,
    completed: AtomicBool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnerGetToTargetRequest {
    ack_stream_id: u64,
    terminal_sequence: u64,
    source: crate::owner_segment::OwnerGetSourceCapability,
    destination: crate::owner_segment::OwnerGetDestinationCapability,
}

#[derive(Clone)]
enum OwnerGetToTargetSourceHold {
    Memory(crate::owner_segment::OwnerLeaseId),
    Ssd { lease: OwnerSlotLease, get_id: u64 },
}

/// Process-scoped Get transfer singleflight. The leader is spawned outside
/// the inbound RPC future, so a lost control-plane response cannot cancel an
/// already-issued WRITE or strand its source read lease. Entries deliberately
/// remain replayable for the owner generation; cumulative acknowledgement
/// watermarks, rather than a per-Get RPC, reclaim cached terminals.
struct OwnerGetToTargetSharedOp {
    request: OwnerGetToTargetRequest,
    terminal: watch::Sender<Option<OwnerSegmentTransferOutcome>>,
    completed: AtomicBool,
    /// Populated only when a local source-release operation itself fails. A
    /// successful WRITE releases before publishing its transfer terminal.
    unreleased_source_hold: Mutex<Option<OwnerGetToTargetSourceHold>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnerPutFromSourceRequest {
    ack_stream_id: u64,
    terminal_sequence: u64,
    target_attempt: u32,
    target_plan: crate::owner_segment::OwnerTargetRouteToken,
    source: crate::owner_segment::OwnerSourceReadCapability,
    disposition: crate::owner_segment::OwnerTargetDisposition,
    route_commit_mode: crate::owner_segment::OwnerRouteCommitMode,
}

/// Target-side singleflight for one exact Put/replica attempt.  The leader is
/// detached from the inbound RPC lifetime, so an ambiguous response cannot
/// cancel an already-issued RDMA READ or make a retry allocate/read again.
struct OwnerPutFromSourceSharedOp {
    request: OwnerPutFromSourceRequest,
    terminal: watch::Sender<Option<OwnerSegmentTransferOutcome>>,
    completed: AtomicBool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnerTargetRouteCommitRequest {
    lease_id: crate::owner_segment::OwnerLeaseId,
    receipt: crate::owner_segment::OwnerTransferReceipt,
    route_token: Option<crate::owner_segment::OwnerTargetRouteToken>,
    route_commit_mode: crate::owner_segment::OwnerRouteCommitMode,
}

/// One detached metadata commit per target operation. Both Async and Sync
/// callers use this exact task; only the caller's wait point differs.
struct OwnerTargetRouteCommitSharedOp {
    request: OwnerTargetRouteCommitRequest,
    terminal: watch::Sender<Option<OwnerSegmentTransferOutcome>>,
    completed: AtomicBool,
}

impl OwnerTargetRouteCommitSharedOp {
    fn new(request: OwnerTargetRouteCommitRequest) -> Arc<Self> {
        let (terminal, _receiver) = watch::channel(None);
        Arc::new(Self {
            request,
            terminal,
            completed: AtomicBool::new(false),
        })
    }

    fn complete(&self, outcome: OwnerSegmentTransferOutcome) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.terminal.send_replace(Some(outcome));
        true
    }

    async fn wait(&self) -> OwnerSegmentTransferOutcome {
        let mut terminal = self.terminal.subscribe();
        loop {
            if let Some(outcome) = terminal.borrow_and_update().clone() {
                return outcome;
            }
            if terminal.changed().await.is_err() {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Internal,
                    "CommitTarget task closed without a terminal",
                ));
            }
        }
    }
}

impl OwnerPutFromSourceSharedOp {
    fn new(request: OwnerPutFromSourceRequest) -> Arc<Self> {
        let (terminal, _receiver) = watch::channel(None);
        Arc::new(Self {
            request,
            terminal,
            completed: AtomicBool::new(false),
        })
    }

    fn complete(&self, outcome: OwnerSegmentTransferOutcome) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.terminal.send_replace(Some(outcome));
        true
    }

    async fn wait(&self) -> OwnerSegmentTransferOutcome {
        let mut terminal = self.terminal.subscribe();
        loop {
            if let Some(outcome) = terminal.borrow_and_update().clone() {
                return outcome;
            }
            if terminal.changed().await.is_err() {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Internal,
                    "PutFromSource singleflight closed without a terminal",
                ));
            }
        }
    }
}

impl OwnerGetToTargetSharedOp {
    fn new(request: OwnerGetToTargetRequest) -> Arc<Self> {
        let (terminal, _receiver) = watch::channel(None);
        Arc::new(Self {
            request,
            terminal,
            completed: AtomicBool::new(false),
            unreleased_source_hold: Mutex::new(None),
        })
    }

    fn retain_unreleased_source_hold(&self, hold: OwnerGetToTargetSourceHold) {
        let previous = self.unreleased_source_hold.lock().replace(hold);
        assert!(
            previous.is_none(),
            "one GetToTarget operation must retain at most one failed-cleanup source hold"
        );
    }

    fn complete(&self, outcome: OwnerSegmentTransferOutcome) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.terminal.send_replace(Some(outcome));
        true
    }

    async fn wait(&self) -> OwnerSegmentTransferOutcome {
        let mut terminal = self.terminal.subscribe();
        loop {
            if let Some(outcome) = terminal.borrow_and_update().clone() {
                return outcome;
            }
            if terminal.changed().await.is_err() {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Internal,
                    "GetToTarget singleflight closed without a terminal",
                ));
            }
        }
    }
}

impl SsdStageSharedOp {
    fn new(request: SsdStageReadReq) -> Arc<Self> {
        let (terminal, _receiver) = watch::channel(None);
        Arc::new(Self {
            request,
            terminal,
            completed: AtomicBool::new(false),
        })
    }

    fn complete(&self, response: SsdStageReadResp) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.terminal.send_replace(Some(response));
        true
    }

    async fn wait(&self) -> SsdStageReadResp {
        let mut terminal = self.terminal.subscribe();
        loop {
            if let Some(response) = terminal.borrow_and_update().clone() {
                return response;
            }
            if terminal.changed().await.is_err() {
                let err = KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "SSD stage singleflight closed without a terminal result: get_id={}",
                        self.request.get_id
                    ),
                });
                return ssd_stage_error_response(err);
            }
        }
    }
}

fn ssd_stage_error_response(err: KvError) -> SsdStageReadResp {
    SsdStageReadResp {
        error_code: err.code(),
        error_json: err.to_json(),
    }
}

fn ssd_stage_request_mismatch_response(
    expected: &SsdStageReadReq,
    actual: &SsdStageReadReq,
) -> SsdStageReadResp {
    ssd_stage_error_response(KvError::Api(ApiError::InvalidArgument {
        detail: format!(
            "SSD stage get_id was reused with a different operation identity: get_id={} expected={:?} actual={:?}",
            actual.get_id, expected, actual
        ),
    }))
}

#[cfg(test)]
mod ssd_stage_singleflight_tests {
    use super::{SsdStageReadReq, SsdStageReadResp, SsdStageSharedOp};
    use crate::rpcresp_kvresult_convert::msg_and_error::OK;
    use std::sync::Arc;

    fn request(get_id: u64) -> SsdStageReadReq {
        SsdStageReadReq {
            key: "ssd-key".to_string(),
            put_id: (17, 2),
            get_id,
            stage_addr: 0x1000,
            stage_capacity: 8192,
            len: 4096,
        }
    }

    #[limit_thirdparty::tokio::test]
    async fn all_ssd_stage_followers_reuse_one_terminal_result() {
        let op = SsdStageSharedOp::new(request(91));
        let waiters = futures::future::join_all((0..64).map(|_| {
            let waiter = op.clone();
            async move { waiter.wait().await }
        }));
        let leader = async {
            assert!(op.complete(SsdStageReadResp {
                error_code: OK,
                error_json: String::new(),
            }));
            assert!(
                !op.complete(SsdStageReadResp {
                    error_code: 1,
                    error_json: "second terminal".to_string(),
                }),
                "the source operation must publish only one terminal result"
            );
        };
        let (responses, ()) = futures::future::join(waiters, leader).await;

        for response in responses {
            assert_eq!(response.error_code, OK);
            assert!(response.error_json.is_empty());
        }
        assert_eq!(Arc::strong_count(&op), 1);
    }
}

#[cfg(test)]
mod owner_get_to_target_singleflight_tests {
    use super::{
        ClientKvApi, ClientKvApiNewArg, OwnerGetToTargetRequest, OwnerGetToTargetSharedOp,
        OwnerGetToTargetSourceHold, apply_owner_transfer_terminal_ack,
        release_owner_get_to_target_source_hold,
    };
    use crate::config::TestSpecConfig;
    use crate::master_seg_manager::msg_pack::SegmentAllocationAuthority;
    use crate::owner_segment::{
        OwnerGeneration, OwnerGetDestinationCapability, OwnerGetTransferReceipt,
        OwnerSegmentTransferOutcome, OwnerSlotScope, OwnerSourceRouteToken, OwnerTransferOpId,
        OwnerTransferOpKind, OwnerTransferOutcome,
    };

    #[limit_thirdparty::tokio::test]
    async fn all_followers_reuse_one_get_to_target_terminal() {
        let op = OwnerGetToTargetSharedOp::new(OwnerGetToTargetRequest {
            ack_stream_id: 1,
            terminal_sequence: 1,
            source: crate::owner_segment::OwnerGetSourceCapability::Memory(
                OwnerSourceRouteToken::default(),
            ),
            destination: OwnerGetDestinationCapability::Invalid,
        });
        let followers = futures::future::join_all((0..8).map(|_| {
            let op = op.clone();
            async move { op.wait().await }
        }));
        let expected = OwnerSegmentTransferOutcome::GetToTargetCompleted {
            receipt: OwnerGetTransferReceipt::default(),
        };
        let leader = async {
            assert!(op.complete(expected.clone()));
            assert!(!op.complete(OwnerSegmentTransferOutcome::Invalid));
        };
        let (followers, ()) = futures::future::join(followers, leader).await;
        for follower in followers {
            assert_eq!(follower, expected);
        }
    }

    #[limit_thirdparty::tokio::test]
    async fn ack_watermark_reclaims_only_completed_get_terminals() {
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: TestSpecConfig::default(),
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority: SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: None,
        })
        .await
        .expect("construct terminal ACK test client");
        let caller = OwnerGeneration::new("requester", 19);
        let completed_id = OwnerTransferOpId::new(caller.clone(), 5, OwnerTransferOpKind::Get);
        let pending_id = OwnerTransferOpId::new(caller.clone(), 6, OwnerTransferOpKind::Get);
        let completed = OwnerGetToTargetSharedOp::new(OwnerGetToTargetRequest {
            ack_stream_id: 1,
            terminal_sequence: 1,
            source: crate::owner_segment::OwnerGetSourceCapability::Invalid,
            destination: OwnerGetDestinationCapability::Invalid,
        });
        assert!(
            completed.complete(OwnerSegmentTransferOutcome::GetToTargetCompleted {
                receipt: OwnerGetTransferReceipt::default(),
            })
        );
        let pending = OwnerGetToTargetSharedOp::new(OwnerGetToTargetRequest {
            ack_stream_id: 1,
            terminal_sequence: 2,
            source: crate::owner_segment::OwnerGetSourceCapability::Invalid,
            destination: OwnerGetDestinationCapability::Invalid,
        });
        api.inner()
            .owner_get_to_target_flights
            .insert(completed_id.clone(), completed);
        api.inner()
            .owner_get_to_target_flights
            .insert(pending_id.clone(), pending.clone());

        assert_eq!(
            apply_owner_transfer_terminal_ack(api.inner(), &caller, 1, 2),
            2
        );
        assert!(
            !api.inner()
                .owner_get_to_target_flights
                .contains_key(&completed_id)
        );
        assert!(
            api.inner()
                .owner_get_to_target_flights
                .contains_key(&pending_id)
        );

        assert!(pending.complete(OwnerSegmentTransferOutcome::Error(
            crate::owner_segment::OwnerTransferItemError::new(
                crate::owner_segment::OwnerTransferErrorCode::Internal,
                "test terminal",
            ),
        )));
        assert_eq!(
            apply_owner_transfer_terminal_ack(api.inner(), &caller, 1, 2),
            2
        );
        assert!(
            !api.inner()
                .owner_get_to_target_flights
                .contains_key(&pending_id)
        );
    }

    #[limit_thirdparty::tokio::test]
    async fn successful_get_releases_source_on_write_completion() {
        const SEGMENT_BYTES: u64 = 64 * 1024;
        const VALUE_LEN: u64 = 5000;
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: TestSpecConfig::default(),
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority: SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: None,
        })
        .await
        .expect("construct GetToTarget acknowledgement test client");
        let source_owner = OwnerGeneration::new("source-owner", 11);
        let source_slot = {
            let mut allocator = api.inner().owner_segment_allocator.lock();
            allocator
                .install_segment(
                    source_owner.clone(),
                    17,
                    0x1000,
                    0x1000,
                    SEGMENT_BYTES,
                    SEGMENT_BYTES,
                    None,
                )
                .expect("install source segment");
            let slot = allocator
                .claim_value(VALUE_LEN, 1)
                .pop()
                .expect("claim source slot");
            assert!(allocator.mark_prepared_slot_pending_visible(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
            assert!(allocator.promote_pending_visible_slot_to_committed(
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            ));
            allocator
                .install_committed_manifest(
                    "source-key",
                    (7, 1),
                    slot.allocation_id,
                    OwnerSlotScope::GlobalShared,
                    23,
                )
                .expect("install source manifest");
            slot
        };
        let requester = OwnerGeneration::new("requester", 19);
        let operation = OwnerTransferOpId::new(requester, 5, OwnerTransferOpKind::Get);
        let source_token = OwnerSourceRouteToken {
            key: "source-key".to_string(),
            put_id: (7, 1),
            route_epoch: 23,
            source: source_slot.clone(),
            atomic_batch: None,
            plan_nonce: operation.sequence,
        };
        let source_lease = match api
            .inner()
            .owner_segment_allocator
            .lock()
            .acquire_source(operation.clone(), source_token.clone())
        {
            OwnerSegmentTransferOutcome::SourceAcquired { lease_id, .. } => lease_id,
            other => panic!("expected source lease, got {other:?}"),
        };
        assert!(
            api.inner()
                .owner_segment_allocator
                .lock()
                .release_committed_slot_route(
                    source_slot.allocation_id,
                    source_slot.segment_offset,
                    source_slot.capacity_bytes,
                )
        );
        assert_eq!(
            api.inner()
                .owner_segment_allocator
                .lock()
                .total_free_bytes(),
            SEGMENT_BYTES - source_slot.capacity_bytes,
            "source read lease must block reclaim before WRITE completion"
        );

        release_owner_get_to_target_source_hold(
            api.inner(),
            &operation,
            &OwnerGetToTargetSourceHold::Memory(source_lease),
            OwnerTransferOutcome::Success,
        )
        .await
        .expect("release source lease after WRITE completion");
        assert_eq!(
            api.inner()
                .owner_segment_allocator
                .lock()
                .total_free_bytes(),
            SEGMENT_BYTES
        );
    }
}

#[cfg(test)]
mod owner_put_from_source_singleflight_tests {
    use super::{OwnerPutFromSourceRequest, OwnerPutFromSourceSharedOp};
    use crate::owner_segment::{
        OwnerRouteCommitMode, OwnerSegmentTransferOutcome, OwnerSourceReadCapability,
        OwnerTargetDisposition, OwnerTargetRouteToken,
    };

    #[limit_thirdparty::tokio::test]
    async fn all_followers_reuse_one_put_from_source_terminal() {
        let op = OwnerPutFromSourceSharedOp::new(OwnerPutFromSourceRequest {
            ack_stream_id: 1,
            terminal_sequence: 1,
            target_attempt: 1,
            target_plan: OwnerTargetRouteToken::default(),
            source: OwnerSourceReadCapability::default(),
            disposition: OwnerTargetDisposition::GlobalShared,
            route_commit_mode: OwnerRouteCommitMode::Async,
        });
        let followers = futures::future::join_all((0..8).map(|_| {
            let op = op.clone();
            async move { op.wait().await }
        }));
        let expected = OwnerSegmentTransferOutcome::PutFromSourceCompleted {
            target_attempt: 1,
            source: Default::default(),
            target: Default::default(),
            receipt: Default::default(),
            route_epoch: 7,
        };
        let leader = async {
            assert!(op.complete(expected.clone()));
            assert!(!op.complete(OwnerSegmentTransferOutcome::Invalid));
        };
        let (followers, ()) = futures::future::join(followers, leader).await;
        for follower in followers {
            assert_eq!(follower, expected);
        }
    }
}

#[cfg(test)]
mod owner_route_commit_mode_tests {
    use super::{
        OwnerPutAppendDoneDisposition, OwnerTargetRouteCommitRequest,
        OwnerTargetRouteCommitSharedOp, OwnerTargetRouteCommitStart, OwnerTargetRouteCommitWait,
        classify_owner_put_append_done, owner_put_append_done_is_definitively_rejected,
        wait_owner_target_route_commit,
    };
    use crate::master_kv_router::msg_pack::PutAppendDoneResp;
    use crate::owner_segment::{
        OwnerRouteCommitMode, OwnerSegmentTransferOutcome, OwnerTransferErrorCode,
        OwnerTransferItemError,
    };
    use std::time::Duration;

    fn route_flight() -> std::sync::Arc<OwnerTargetRouteCommitSharedOp> {
        OwnerTargetRouteCommitSharedOp::new(OwnerTargetRouteCommitRequest {
            lease_id: Default::default(),
            receipt: Default::default(),
            route_token: None,
            route_commit_mode: OwnerRouteCommitMode::Async,
        })
    }

    #[test]
    fn rejected_exact_append_is_an_obsolete_terminal_not_a_retry() {
        let obsolete = PutAppendDoneResp {
            appended: false,
            route_epoch: 0,
            ..Default::default()
        };
        assert_eq!(
            classify_owner_put_append_done(&obsolete),
            OwnerPutAppendDoneDisposition::Obsolete
        );
        let committed = PutAppendDoneResp {
            appended: true,
            route_epoch: 91,
            ..Default::default()
        };
        assert_eq!(
            classify_owner_put_append_done(&committed),
            OwnerPutAppendDoneDisposition::Committed(91)
        );

        let rejected = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidPutMasterState {
                detail: "append reservation expired".to_string(),
            },
        );
        assert!(owner_put_append_done_is_definitively_rejected(&rejected));
    }

    #[limit_thirdparty::tokio::test]
    async fn async_detaches_while_sync_waits_for_the_same_route_task() {
        let flight = route_flight();
        let async_result = ::tokio::time::timeout(
            Duration::from_millis(50),
            wait_owner_target_route_commit(
                OwnerRouteCommitMode::Async,
                OwnerTargetRouteCommitStart::Running(flight.clone()),
            ),
        )
        .await
        .expect("Async must not wait for the route terminal");
        assert!(matches!(async_result, OwnerTargetRouteCommitWait::Detached));

        let sync_wait = wait_owner_target_route_commit(
            OwnerRouteCommitMode::Sync,
            OwnerTargetRouteCommitStart::Running(flight.clone()),
        );
        ::tokio::pin!(sync_wait);
        assert!(
            ::tokio::time::timeout(Duration::from_millis(20), &mut sync_wait)
                .await
                .is_err(),
            "Sync must remain blocked before the shared route task is terminal"
        );

        let terminal = OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::RouteCommitRequired,
            "test route terminal",
        ));
        assert!(flight.complete(terminal.clone()));
        match ::tokio::time::timeout(Duration::from_secs(1), &mut sync_wait)
            .await
            .expect("Sync must observe the shared route terminal")
        {
            OwnerTargetRouteCommitWait::Terminal(actual) => assert_eq!(actual, terminal),
            OwnerTargetRouteCommitWait::Detached => panic!("Sync detached its route task"),
        }
    }
}

#[cfg(test)]
mod owner_put_from_source_integration_tests {
    use super::{
        ExternalGetKeySharedOp, ExternalGetKeySharedPhase, ExternalGetStartSharedItemResult,
    };
    use crate::client_kv_api::put::{OwnerLocalPublishItem, OwnerLocalPublishJob};
    use crate::config::OwnerLocalReserveExpectedCapacity;
    use crate::kvcore_test_lib::{
        integration_test_lock, start_additional_client_with_config,
        start_master_and_client_with_client_config, stop_master_and_client,
    };
    use crate::master_kv_router::msg_pack::{
        BatchPreparePutKeyItemReq, PutAtomicGroup, PutAtomicGroupMember,
    };
    use crate::master_seg_manager::msg_pack::OwnerPlacementClass;
    use parking_lot::RwLock;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };
    use std::time::{Duration, Instant};

    #[limit_thirdparty::tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn remote_replica_uses_one_target_read_and_publishes_exact_bytes() {
        let _test_guard = integration_test_lock().await;
        let (master, source) = start_master_and_client_with_client_config(
            "put_from_source_master",
            "put_from_source_source",
            |config| {
                config.owner_placement_class = Some(OwnerPlacementClass::Inference);
                config.replica_writeback_hot_capacity_ratio = Some(0.5);
                config
                    .test_spec_config
                    .owner_local_reserve_expected_capacity =
                    Some(OwnerLocalReserveExpectedCapacity {
                        value_len: 4096,
                        payload_capacity_bytes: config.contribute_to_cluster_pool_size.dram / 2,
                    });
            },
        )
        .await;
        let target = start_additional_client_with_config(
            "put_from_source_master",
            "put_from_source_target",
            |config| {
                config.owner_placement_class = Some(OwnerPlacementClass::RemoteCpu);
                config.replica_writeback_hot_capacity_ratio = None;
                config.fluxonkv_spec.sub_cluster = Some("remote_cache".to_string());
                config
                    .test_spec_config
                    .owner_local_reserve_expected_capacity = None;
            },
        )
        .await;

        let key = "put_from_source_exact_bytes";
        let payload = vec![0x6du8; 4096];
        let source_view = source.client_kv_api_view();
        let source_inner = source_view.client_kv_api().inner();
        let target_view = target.client_kv_api_view();
        let target_inner = target_view.client_kv_api().inner();
        let blocked_get = Arc::new(ExternalGetKeySharedOp::new(key.to_string()));
        {
            let mut controls = target_inner.owner_key_control.lock_key(key);
            controls.entry(key.to_string()).or_default().external_get = Some(blocked_get.clone());
        }
        target_inner.track_external_get_flight(&blocked_get);
        let target_node: crate::cluster_manager::NodeID = target
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id
            .into();
        let source_node: crate::cluster_manager::NodeID = source
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id
            .into();
        let master_view = master.master_kv_router_view();
        let registration_deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let source_ready = master_view
                .master_seg_manager()
                .get_node_registered_segments(&source_node)
                .is_some_and(|segments| !segments.is_empty());
            let target_ready = master_view
                .master_seg_manager()
                .get_node_registered_segments(&target_node)
                .is_some_and(|segments| !segments.is_empty());
            if source_ready && target_ready {
                break;
            }
            assert!(
                Instant::now() < registration_deadline,
                "source/target segment registration did not converge: source_ready={} target_ready={}",
                source_ready,
                target_ready,
            );
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }
        super::local_reserve_rebalance::publish_owner_capacity_report(&target_view)
            .await
            .expect("target owner capacity report RPC must converge after registration");
        let expected_allocation_size =
            crate::owner_segment_allocation_capacity_bytes(payload.len() as u64)
                .expect("test payload must fit the owner allocator");
        loop {
            let target_capacity_ready = master_view
                .master_seg_manager()
                .get_owner_capacity_report(target_node.as_ref())
                .is_some_and(|(report, _age)| {
                    report.settled
                        && report.size_classes.iter().any(|size_class| {
                            size_class.allocation_size_bytes == expected_allocation_size
                                && size_class.allocatable_bytes >= expected_allocation_size
                        })
                });
            if target_capacity_ready {
                break;
            }
            assert!(
                Instant::now() < registration_deadline,
                "target owner capacity report did not converge for allocation_size={}",
                expected_allocation_size,
            );
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let prepared = source_inner
            .batch_prepare_put_keys(vec![BatchPreparePutKeyItemReq {
                key: key.to_string(),
                reject_if_inflight_same_key: false,
                reject_if_exist_same_key: false,
            }])
            .await
            .expect("reserve owner-local source key");
        assert_eq!(prepared.reservation_ids.len(), 1);
        let slot_lease = source_inner
            .owner_claim_local_reserve_slot_lease(payload.len() as u64, 1)
            .await
            .expect("claim owner-local source slot");
        let source_slot = slot_lease.slots[0].clone();
        unsafe {
            std::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                source_slot.addr as *mut u8,
                payload.len(),
            );
        }
        let put_id = source_inner.next_owner_local_first_put_id();
        let memory_info = source_inner
            .build_local_reserve_resident_memory_info(
                key,
                source_slot.addr,
                payload.len() as u32,
                source_slot.allocation_id,
                source_slot.segment_offset,
                source_slot.capacity_bytes,
            )
            .await;
        source_inner.install_precommit_local_visible_memory_info(key, memory_info);
        source_inner
            .enqueue_owner_local_publish(OwnerLocalPublishJob {
                items: vec![OwnerLocalPublishItem {
                    key: key.to_string(),
                    put_id,
                    value_len: payload.len() as u64,
                    lease_id: None,
                    committed_slot: source_slot,
                    make_replica_task: true,
                    remote_replica_admitted: true,
                    preferred_sub_cluster: None,
                    atomic_group: None,
                    radix: None,
                }],
                key_reservation_ids: prepared.reservation_ids,
                external_pending_contexts: Vec::new(),
            })
            .await
            .expect("enqueue owner-local source publication");

        let deadline = Instant::now() + Duration::from_secs(15);
        let published_put_id = loop {
            if let Some(route) = master_view
                .master_kv_router()
                .inner()
                .kv_routes
                .get(key)
                .map(|route| route.clone())
            {
                break route.put_id;
            }
            assert!(Instant::now() < deadline, "source route was not published");
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(published_put_id, put_id);
        let target_flight = loop {
            if let Some(flight) = target_inner
                .owner_put_from_source_flights
                .iter()
                .next()
                .map(|entry| entry.value().clone())
            {
                break flight;
            }
            assert!(
                Instant::now() < deadline,
                "PutFromSource Async target flight was not installed"
            );
            let _ = source_inner
                .ensure_remote_put(key, put_id, None, true)
                .await
                .expect("joining the exact remote Put flight must not fail");
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(
            target_inner.runtime_observe_snapshot().remote_put_transfers,
            0,
            "PutFromSource must not transfer while planned Get owns the key flight"
        );
        assert!(
            target_inner
                .owner_segment_allocator
                .lock()
                .manifest_entry(key, put_id)
                .is_none(),
            "PutFromSource must not install a second target manifest behind Get"
        );
        {
            let mut controls = target_inner.owner_key_control.lock_key(key);
            let mut get_state = blocked_get.state.lock();
            get_state.phase = ExternalGetKeySharedPhase::Ready {
                result: ExternalGetStartSharedItemResult::Miss,
            };
            get_state.terminal_at = Some(Instant::now());
            let remove_control = if let Some(control) = controls.get_mut(key) {
                assert!(
                    control
                        .external_get
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &blocked_get))
                );
                control.external_get = None;
                control.is_idle()
            } else {
                false
            };
            if remove_control {
                controls.remove(key);
            }
        }
        target_inner.untrack_external_get_flight(&blocked_get);
        blocked_get.notify.notify_waiters();
        let accepted = ::tokio::time::timeout(Duration::from_secs(5), target_flight.wait())
            .await
            .expect("Async PutFromSource must publish its outward terminal");
        assert!(matches!(
            &accepted,
            crate::owner_segment::OwnerSegmentTransferOutcome::PutFromSourceAcceptedRoutePending { .. }
        ));
        loop {
            let target_published = master_view
                .master_kv_router()
                .inner()
                .kv_routes
                .get(key)
                .is_some_and(|route| {
                    route
                        .node_replicas
                        .read()
                        .get(&target_node)
                        .is_some_and(|replicas| {
                            !replicas.tomb_tag.is_tomb()
                                && replicas.memory.as_ref().is_some_and(|memory| {
                                    matches!(
                                        &memory.backing,
                                        crate::master_kv_router::KvReplicaBacking::CommittedSlot(
                                            slot
                                        ) if slot.owner.node_id.as_str() == target_node.as_ref()
                                    )
                                })
                        })
                });
            if target_published {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PutFromSource did not publish the target route"
            );
            let _ = source_inner
                .ensure_remote_put(key, put_id, None, true)
                .await
                .expect("joining the exact remote Put flight must not fail");
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            target_inner.runtime_observe_snapshot().remote_put_transfers,
            1,
            "one target attempt must issue exactly one target-side READ"
        );
        assert_eq!(
            target_inner.owner_put_from_source_flights.len(),
            1,
            "retries and the explicit waiter must reuse one target flight"
        );
        assert_eq!(
            target_flight.wait().await,
            accepted,
            "a replay after background commit must retain the original Async outward terminal"
        );
        let route_flight = target_inner
            .owner_target_route_commit_flights
            .iter()
            .next()
            .map(|entry| entry.value().clone())
            .expect("Async PutFromSource must install one route task");
        assert!(matches!(
            route_flight.wait().await,
            crate::owner_segment::OwnerSegmentTransferOutcome::TargetCommitted {
                route_epoch,
                ..
            } if route_epoch != 0
        ));
        let target_manifest = target_inner
            .owner_segment_allocator
            .lock()
            .manifest_entry(key, put_id)
            .expect("target owner manifest must contain the committed replica");
        assert_eq!(
            target_manifest.physical_state,
            crate::owner_segment::OwnerSlotPhysicalState::Committed
        );
        assert_eq!(target_manifest.slot.len, payload.len() as u64);
        let target_bytes = unsafe {
            std::slice::from_raw_parts(
                target_manifest.slot.addr as *const u8,
                target_manifest.slot.len as usize,
            )
        };
        assert_eq!(target_bytes, payload.as_slice());

        // Reproduce the inference workload's dense atomic_batch: all members
        // publish locally first, then independently enter the common remote
        // Put singleflight and commit concurrently at one RemoteCpu-like
        // target. This catches token/item swaps that a one-key test cannot.
        let batch_keys = (0..13)
            .map(|index| format!("put_from_source_atomic_{index}"))
            .collect::<Vec<_>>();
        let prepared = source_inner
            .batch_prepare_put_keys(
                batch_keys
                    .iter()
                    .map(|key| BatchPreparePutKeyItemReq {
                        key: key.clone(),
                        reject_if_inflight_same_key: false,
                        reject_if_exist_same_key: false,
                    })
                    .collect(),
            )
            .await
            .expect("reserve atomic owner-local source keys");
        assert_eq!(prepared.reservation_ids.len(), batch_keys.len());
        let batch_slots = source_inner
            .owner_claim_local_reserve_slot_lease(payload.len() as u64, batch_keys.len())
            .await
            .expect("claim atomic owner-local source slots")
            .slots;
        let batch_put_ids = batch_keys
            .iter()
            .map(|_| source_inner.next_owner_local_first_put_id())
            .collect::<Vec<_>>();
        let atomic_group = PutAtomicGroup {
            members: batch_keys
                .iter()
                .zip(&batch_put_ids)
                .map(|(key, put_id)| PutAtomicGroupMember {
                    key: key.clone(),
                    put_id: *put_id,
                })
                .collect(),
        };
        let blocked_atomic_key = batch_keys[0].clone();
        let blocked_atomic_get = Arc::new(ExternalGetKeySharedOp::new(blocked_atomic_key.clone()));
        {
            let mut controls = target_inner.owner_key_control.lock_key(&blocked_atomic_key);
            controls
                .entry(blocked_atomic_key.clone())
                .or_default()
                .external_get = Some(blocked_atomic_get.clone());
        }
        target_inner.track_external_get_flight(&blocked_atomic_get);
        let mut publish_items = Vec::with_capacity(batch_keys.len());
        for (((key, put_id), slot), fill) in batch_keys
            .iter()
            .zip(&batch_put_ids)
            .zip(batch_slots)
            .zip(0x70u8..)
        {
            unsafe {
                std::ptr::write_bytes(slot.addr as *mut u8, fill, payload.len());
            }
            let memory_info = source_inner
                .build_local_reserve_resident_memory_info(
                    key,
                    slot.addr,
                    payload.len() as u32,
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                )
                .await;
            source_inner.install_precommit_local_visible_memory_info(key, memory_info);
            publish_items.push(OwnerLocalPublishItem {
                key: key.clone(),
                put_id: *put_id,
                value_len: payload.len() as u64,
                lease_id: None,
                committed_slot: slot,
                make_replica_task: true,
                remote_replica_admitted: true,
                preferred_sub_cluster: None,
                atomic_group: Some(atomic_group.clone()),
                radix: None,
            });
        }
        source_inner
            .enqueue_owner_local_publish(OwnerLocalPublishJob {
                items: publish_items,
                key_reservation_ids: prepared.reservation_ids,
                external_pending_contexts: Vec::new(),
            })
            .await
            .expect("enqueue atomic owner-local source publication");

        let atomic_deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let target_waiting = target_inner
                .owner_put_from_source_flights
                .iter()
                .any(|entry| entry.value().request.target_plan.key == blocked_atomic_key);
            let inflight_reserved = master_view
                .master_kv_router()
                .inner()
                .inflight_replica_tasks
                .get(&(
                    blocked_atomic_key.clone(),
                    batch_put_ids[0].0,
                    batch_put_ids[0].1,
                ))
                .await
                .is_some();
            if target_waiting && inflight_reserved {
                break;
            }
            assert!(
                Instant::now() < atomic_deadline,
                "blocked atomic PutFromSource did not reserve its immutable master operation"
            );
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Reproduce the a6 race: a replay arrives after the route visible at
        // Start has changed. The replay must reuse the reservation's frozen
        // atomic_batch, never rebuild its token from the later route object.
        let original_route = master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .get(&blocked_atomic_key)
            .map(|entry| entry.value().clone())
            .expect("blocked atomic source route must be visible");
        let drifted_route = Arc::new(crate::master_kv_router::OneKvNodesRoutes {
            put_id: original_route.put_id,
            radix: original_route.radix.clone(),
            lease_id: original_route.lease_id,
            atomic_group: None,
            node_replicas: RwLock::new(original_route.node_replicas.read().clone()),
            get_durable_slots_used: AtomicU32::new(
                original_route
                    .get_durable_slots_used
                    .load(Ordering::Acquire),
            ),
        });
        master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .insert(blocked_atomic_key.clone(), drifted_route);
        let replay_plan = source_inner
            .put_append_start(
                &blocked_atomic_key,
                batch_put_ids[0],
                payload.len() as u32,
                None,
                true,
            )
            .await
            .expect("replica Start replay must reuse the reserved operation");
        assert_eq!(
            replay_plan.owner_candidates[0].atomic_batch.as_ref(),
            Some(&atomic_group),
            "Start replay rebuilt atomic_batch from a later route instead of the inflight reservation"
        );
        master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .insert(blocked_atomic_key.clone(), original_route);
        {
            let mut controls = target_inner.owner_key_control.lock_key(&blocked_atomic_key);
            let mut get_state = blocked_atomic_get.state.lock();
            get_state.phase = ExternalGetKeySharedPhase::Ready {
                result: ExternalGetStartSharedItemResult::Miss,
            };
            get_state.terminal_at = Some(Instant::now());
            let remove_control = if let Some(control) = controls.get_mut(&blocked_atomic_key) {
                control.external_get = None;
                control.is_idle()
            } else {
                false
            };
            if remove_control {
                controls.remove(&blocked_atomic_key);
            }
        }
        target_inner.untrack_external_get_flight(&blocked_atomic_get);
        blocked_atomic_get.notify.notify_waiters();

        loop {
            let published = batch_keys.iter().zip(&batch_put_ids).all(|(key, put_id)| {
                master_view
                    .master_kv_router()
                    .inner()
                    .kv_routes
                    .get(key)
                    .is_some_and(|route| {
                        route.put_id == *put_id
                            && route.node_replicas.read().get(&target_node).is_some_and(
                                |replicas| {
                                    !replicas.tomb_tag.is_tomb()
                                        && replicas.memory.as_ref().is_some_and(|memory| {
                                            matches!(
                                                memory.backing,
                                                crate::master_kv_router::KvReplicaBacking::CommittedSlot(_)
                                            )
                                        })
                                },
                            )
                    })
            });
            if published {
                break;
            }
            assert!(
                Instant::now() < atomic_deadline,
                "atomic PutFromSource routes did not all commit"
            );
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
        }

        target.shutdown().await.expect("stop target owner");
        stop_master_and_client(master, source).await;
    }
}

/// Information about a memholder held by external client
#[derive(Clone)]
pub struct ExternalHoldingGetInfo {
    pub key: String,
    pub req_node_id: Arc<str>,
    /// Requester membership generation observed when this holding was
    /// installed. Unknown generations are never removed by generation-scoped
    /// MemberLeft cleanup.
    pub requester_node_start_time: Option<i64>,
    pub memory_info: Arc<MemoryInfo>, // The actual memholder being held
    _owner_hot_pin: Option<PinGuard>,
}

/// Requester identity and holder-id reservation shared by one external Get
/// batch.  The reservation may contain gaps when some probe items miss; holder
/// ids are opaque and only need to remain non-zero and generation-unique.
pub(crate) struct ExternalGetHoldingBatch {
    req_node_id: Arc<str>,
    requester_node_start_time: Option<i64>,
    first_holder_id: u64,
    reserved_len: usize,
}

impl ExternalGetHoldingBatch {
    fn holder_id_at(&self, index: usize) -> u64 {
        assert!(
            index < self.reserved_len,
            "external holding batch index must stay inside its reservation"
        );
        self.first_holder_id
            .checked_add(u64::try_from(index).expect("holder index must fit u64"))
            .expect("external holding id space exhausted")
    }
}

#[derive(Clone, Debug, Default)]
pub struct OwnerRuntimeObserveSnapshot {
    pub ssd_capacity_bytes: u64,
    pub ssd_used_bytes: u64,
    pub ssd_persist_requests: u64,
    pub ssd_persist_successes: u64,
    pub ssd_persist_failures: u64,
    pub ssd_persist_bytes: u64,
    pub ssd_persist_duration_us: u64,
    pub ssd_persist_batch_requests: u64,
    pub ssd_persist_batch_items: u64,
    pub ssd_persist_flush_batches: u64,
    pub ssd_persist_busy_batches: u64,
    pub ssd_persist_admission_skips: u64,
    pub ssd_persist_batch_duration_us: u64,
    pub ssd_write_candidate_items: u64,
    pub ssd_write_candidate_bytes: u64,
    pub ssd_write_admitted_items: u64,
    pub ssd_write_admitted_bytes: u64,
    pub ssd_write_dropped_items: u64,
    pub ssd_write_dropped_bytes: u64,
    pub ssd_write_refunded_items: u64,
    pub ssd_write_refunded_bytes: u64,
    pub ssd_load_requests: u64,
    pub ssd_load_successes: u64,
    pub ssd_load_misses: u64,
    pub ssd_load_failures: u64,
    pub ssd_load_bytes: u64,
    pub ssd_load_duration_us: u64,
    pub ssd_memory_hits: u64,
    pub ssd_disk_hits: u64,
    pub ssd_outer_hits: u64,
    pub ssd_removals: u64,
    pub ssd_stage_flights: u64,
    pub ssd_stage_terminals: u64,
    pub ssd_stage_ready_requests: u64,
    pub ssd_stage_ready_successes: u64,
    pub ssd_stage_ready_failures: u64,
    pub ssd_stage_ready_duration_us: u64,
    pub ssd_stage_execute_completions: u64,
    pub ssd_stage_terminal_published: u64,
    pub ssd_stage_terminal_cache_inserts: u64,
    pub ssd_stage_terminal_cache_duration_us: u64,
    pub ssd_stage_response_send_attempts: u64,
    pub ssd_stage_response_send_successes: u64,
    pub ssd_stage_response_send_failures: u64,
    pub ssd_stage_response_send_duration_us: u64,
    pub ssd_source_ready_wait_requests: u64,
    pub ssd_source_ready_wait_successes: u64,
    pub ssd_source_ready_wait_failures: u64,
    pub ssd_source_ready_wait_duration_us: u64,
    pub ssd_target_pull_requests: u64,
    pub ssd_target_pull_successes: u64,
    pub ssd_target_pull_failures: u64,
    pub ssd_target_pull_duration_us: u64,
    pub ssd_stage_done_detached: u64,
    pub external_get_holding_entries: u64,
    pub external_get_holding_bytes: u64,
    pub external_get_start_handles: u64,
    pub external_get_flights: u64,
    pub external_get_flights_starting: u64,
    pub external_get_flights_finishing: u64,
    pub external_get_flights_revoking: u64,
    pub external_get_undecided_interests: u64,
    pub external_get_retained_interests: u64,
    pub owner_local_probe_batches: u64,
    pub owner_local_probe_items: u64,
    pub owner_local_probe_local_items: u64,
    pub owner_local_probe_remote_items: u64,
    pub planned_cpu_get_batches: u64,
    pub planned_cpu_get_local_items: u64,
    pub planned_cpu_get_leader_items: u64,
    pub planned_cpu_get_follower_items: u64,
    pub external_pending_put_entries: u64,
    pub remote_put_flights_active: u64,
    pub remote_put_flight_leaders: u64,
    pub remote_put_flight_followers: u64,
    pub remote_put_source_unavailable: u64,
    pub remote_put_source_fenced: u64,
    pub remote_put_source_missing: u64,
    pub remote_put_source_version_mismatch: u64,
    pub remote_put_transfers: u64,
    pub remote_put_published: u64,
    pub remote_put_already_satisfied: u64,
    pub remote_put_obsolete: u64,
    pub remote_put_failed: u64,
    pub remote_put_task_dropped: u64,
    /// Zero means unbounded for the two configured limits below.
    pub remote_put_admission_limit_bytes: u64,
    pub remote_put_admission_limit_items: u64,
    pub remote_put_admission_active_bytes: u64,
    pub remote_put_admission_active_items: u64,
    pub remote_put_admission_peak_bytes: u64,
    pub remote_put_admission_peak_items: u64,
    pub remote_put_admission_admitted: u64,
    pub remote_put_admission_not_admitted: u64,
    pub remote_put_admission_not_admitted_bytes: u64,
    pub local_ssd_put_flights_active: u64,
    pub local_ssd_put_flight_leaders: u64,
    pub local_ssd_put_flight_followers: u64,
    pub local_ssd_put_source_unavailable: u64,
    pub local_ssd_put_published: u64,
    pub local_ssd_put_already_present: u64,
    pub local_ssd_put_dropped: u64,
    pub local_ssd_put_obsolete: u64,
    pub local_ssd_put_failed: u64,
    pub owner_segment_capacity_bytes: u64,
    pub local_reserve_accounting_slot_size: u64,
    pub local_reserve_raw_free_bytes: u64,
    pub local_reserve_allocatable_slots: u64,
    pub local_reserve_allocatable_bytes: u64,
    pub local_reserve_slot_unallocatable_bytes: u64,
    pub local_reserve_slot_unallocatable_ratio_ppm: u64,
    pub local_reserve_slots_free: u64,
    pub local_reserve_slots_prepared: u64,
    pub local_reserve_slots_pending_visible: u64,
    pub local_reserve_slots_committed: u64,
    pub local_reserve_controller_epoch: u64,
    pub local_reserve_target_bytes: u64,
    pub global_shared_target_bytes: u64,
    pub owner_segment_allocated_bytes: u64,
    pub local_reserve_applied_moka_bytes: u64,
    pub local_reserve_moka_capacity_delta_bytes: u64,
    pub local_reserve_settled: bool,
    pub hot_cache_capacity_bytes: u64,
    pub hot_cache_entries: u64,
    pub hot_cache_weighted_bytes: u64,
    pub hot_size_evictions: u64,
    pub hot_source_evict_handoff_members: u64,
    pub hot_source_evict_committed_members: u64,
    pub hot_source_evict_restored_members: u64,
    pub hot_source_evict_obsolete: u64,
    pub hot_source_evict_dispatch_failed: u64,
    pub hot_source_eviction_selected: u64,
    pub hot_source_evict_retry_entries: u64,
    pub hot_source_evict_retry_scheduled: u64,
    pub hot_source_evict_retry_emitted: u64,
    pub hot_selection_debt_bytes: u64,
    pub hot_source_eviction_selected_bytes: u64,
    pub hot_eviction_skipped_stale: u64,
    pub hot_eviction_skipped_reclaim: u64,
    pub hot_eviction_skipped_active_holders: u64,
    pub hot_victim_duplicates: u64,
    pub hot_victim_invalid_backing: u64,
    pub grouped_put_done_batches: u64,
    pub grouped_put_done_items: u64,
    pub legacy_put_done_batches: u64,
    pub legacy_put_done_items: u64,
}

pub use get::RemoteGetInfo;
pub use put::{OwnerLocalPublishItem, OwnerLocalPublishJob, OwnerReservedPutItem};
pub mod external_api;
mod local_reserve_rebalance;
mod reclaim;
pub use external_api::HandlerForExternalClient;
pub type TestObservePutPhaseSink = Arc<Mutex<Option<TestPutPhaseTrace>>>;
pub type ExternalGetStartTransferOutput =
    Vec<KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>>>;

pub enum ExternalGetStartOwnerItem {
    Local { memory_info: Arc<MemoryInfo> },
    Shared { interest: ExternalGetKeyInterest },
}

/// One request's interest in a per-key Get flight.
///
/// A pending prefix decision is owned by this guard, not by control flow.  If
/// the request future is cancelled at any await point, Drop retires the
/// undecided count so the atomic_batch task can still choose Finish or Revoke.
pub struct ExternalGetKeyInterest {
    op: Arc<ExternalGetKeySharedOp>,
    decision_pending: bool,
}

impl ExternalGetKeyInterest {
    pub fn new(op: Arc<ExternalGetKeySharedOp>, decision_pending: bool) -> Self {
        Self {
            op,
            decision_pending,
        }
    }

    pub fn op(&self) -> &Arc<ExternalGetKeySharedOp> {
        &self.op
    }

    pub fn decide(&mut self, retain: bool) {
        if !self.decision_pending {
            return;
        }
        self.decision_pending = false;

        let wake = {
            let mut state = self.op.state.lock();
            if state.undecided == 0 {
                tracing::error!(
                    "external Get interest observed undecided underflow: key={}",
                    self.op.key
                );
                return;
            }
            state.undecided -= 1;
            if retain
                && matches!(
                    state.phase,
                    ExternalGetKeySharedPhase::Starting | ExternalGetKeySharedPhase::Started { .. }
                )
            {
                state.retained = state
                    .retained
                    .checked_add(1)
                    .expect("external Get singleflight retained overflow");
            }
            state.undecided == 0
        };
        if wake {
            self.op.notify.notify_waiters();
        }
    }
}

impl Drop for ExternalGetKeyInterest {
    fn drop(&mut self) {
        self.decide(false);
    }
}

#[derive(Clone)]
pub enum ExternalGetKeySharedPhase {
    Starting,
    Started {
        item: BatchGetStartItemResp,
        late_target: Option<GetBindTarget>,
    },
    /// At least one request plan's prefix retained this key.  The leader atomic_batch is
    /// transferring/completing it and later publishes one canonical result.
    Finishing {
        item: BatchGetStartItemResp,
        late_target: Option<GetBindTarget>,
    },
    /// No request plan's prefix retained this prepared Get.  Keep the marker in
    /// the owner key fence until BatchGetRevoke and local-slot release finish,
    /// so a new overlapping batch cannot race the revoke.
    Revoking {
        /// Keep the exact master operation and prepared target reachable until
        /// Revoke reaches a definite terminal response.
        item: BatchGetStartItemResp,
    },
    Ready {
        result: ExternalGetStartSharedItemResult,
    },
    Failed {
        error_code: ErrorCode,
        error_json: String,
    },
}

pub struct ExternalGetKeySharedState {
    /// Exact-batch operations which joined while the key was Starting/Started
    /// and have not yet applied their own atomic-prefix decision.
    pub undecided: usize,
    /// Number of those operations whose transferable prefix retained the key.
    pub retained: usize,
    pub phase: ExternalGetKeySharedPhase,
    /// Monotonic publication time for Ready/Failed.  Observation-only users
    /// compare this with the later handle-consume time to distinguish data
    /// that was already ready from time actually spent waiting in Transfer.
    pub terminal_at: Option<Instant>,
}

pub struct ExternalGetKeySharedOp {
    pub key: String,
    pub state: Mutex<ExternalGetKeySharedState>,
    pub notify: Arc<limit_thirdparty::tokio::sync::Notify>,
}

impl ExternalGetKeySharedOp {
    pub fn new(key: String) -> Self {
        Self {
            key,
            state: Mutex::new(ExternalGetKeySharedState {
                undecided: 1,
                retained: 0,
                phase: ExternalGetKeySharedPhase::Starting,
                terminal_at: None,
            }),
            notify: Arc::new(limit_thirdparty::tokio::sync::Notify::new()),
        }
    }

    pub(crate) async fn wait_terminal(&self) {
        loop {
            let notified = self.notify.notified();
            futures::pin_mut!(notified);
            let should_wait = {
                let state = self.state.lock();
                match state.phase {
                    ExternalGetKeySharedPhase::Ready { .. }
                    | ExternalGetKeySharedPhase::Failed { .. } => false,
                    _ => {
                        notified.as_mut().enable();
                        true
                    }
                }
            };
            if !should_wait {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExternalGetStartPrefixResult {
    pub raw_prefix_hit_len: usize,
    pub transferable_len: usize,
    pub first_miss_index: Option<usize>,
    pub first_error_kind: Option<String>,
}

#[derive(Clone)]
pub enum ExternalGetStartSharedItemResult {
    Hit {
        memholder: Arc<UserMemHolder>,
    },
    Miss,
    Error {
        error_code: ErrorCode,
        error_json: String,
    },
}

pub struct ExternalGetStartEntry {
    pub req_node_id: String,
    /// Requester membership generation observed when this handle was created.
    /// `None` is retained only for compatibility with a temporarily incomplete
    /// cluster cache; generation cleanup must never guess in that case.
    pub requester_node_start_time: Option<i64>,
    pub keys: Vec<String>,
    pub items: Vec<ExternalGetStartOwnerItem>,
    pub atomic_group_lens: Vec<usize>,
    pub created_at: Instant,
}

/// Optional arguments for put operations
#[derive(Clone, Debug)]
pub enum PutOptionalArg {
    /// Attach the written key to the specified lease on commit
    LeaseId(u64),
    /// Ask the master to fail-fast when the same key already has an inflight put.
    RejectIfInflightSameKey,
    /// Ask the master to fail-fast when the key already has a committed live replica.
    RejectIfExistSameKey,
    /// Disable asynchronous remote replica task after local commit in write-back mode.
    SkipMakeReplicaTask,
    /// Prefer placing the target allocation on a kvclient within this sub_cluster.
    PreferredSubCluster(String),
    /// Hidden test-only side-channel for collecting per-put phase timings.
    TestObservePutPhases(TestObservePutPhaseSink),
}

/// Container for optional put arguments
#[derive(Clone, Debug, Default)]
pub struct PutOptionalArgs(pub Vec<PutOptionalArg>);

impl PutOptionalArgs {
    pub fn new() -> Self {
        Self(Vec::new())
    }
    /// Get the last provided lease_id if any
    pub fn lease_id(&self) -> Option<u64> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::LeaseId(id) => Some(*id),
            PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::RejectIfExistSameKey
            | PutOptionalArg::SkipMakeReplicaTask
            | PutOptionalArg::PreferredSubCluster(_)
            | PutOptionalArg::TestObservePutPhases(_) => None,
        })
    }

    pub fn reject_if_inflight_same_key(&self) -> bool {
        self.0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::RejectIfInflightSameKey))
    }

    pub fn reject_if_exist_same_key(&self) -> bool {
        self.0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::RejectIfExistSameKey))
    }

    pub fn make_replica_task(&self) -> bool {
        !self
            .0
            .iter()
            .any(|arg| matches!(arg, PutOptionalArg::SkipMakeReplicaTask))
    }

    /// Get the last provided preferred_sub_cluster if any.
    pub fn preferred_sub_cluster(&self) -> Option<&str> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::PreferredSubCluster(sc) => Some(sc.as_str()),
            PutOptionalArg::LeaseId(_)
            | PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::RejectIfExistSameKey
            | PutOptionalArg::SkipMakeReplicaTask
            | PutOptionalArg::TestObservePutPhases(_) => None,
        })
    }

    pub fn test_observe_put_phases(&self) -> Option<TestObservePutPhaseSink> {
        self.0.iter().rev().find_map(|a| match a {
            PutOptionalArg::TestObservePutPhases(sink) => Some(sink.clone()),
            PutOptionalArg::LeaseId(_)
            | PutOptionalArg::RejectIfInflightSameKey
            | PutOptionalArg::RejectIfExistSameKey
            | PutOptionalArg::SkipMakeReplicaTask
            | PutOptionalArg::PreferredSubCluster(_) => None,
        })
    }
}

/// KV operation timestamp kind with Begin/End events for Grafana state visualization
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MetricTimestampKind {
    // Put operation phases
    PutWholeBegin,
    PutWholeEnd,
    PutStartBegin,
    PutStartEnd,
    PutTransferBegin,
    PutTransferEnd,
    PutEndBegin,
    PutEndEnd,
    PutRpcBegin,
    PutRpcEnd,

    // Get operation phases
    GetWholeBegin,
    GetWholeEnd,
    GetStartBegin,
    GetStartEnd,
    GetTransferBegin,
    GetTransferEnd,
    GetEndBegin,
    GetEndEnd,
}

/// Timestamp for KV operation metrics with enhanced tracking
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricTimestamp {
    pub time: i64,
    pub kind: MetricTimestampKind,
    pub key_opt: Option<String>,
    pub ope_id_opt: Option<String>,
}

impl MetricTimestampKind {
    /// Get the corresponding value for Prometheus (1 for Begin, 0 for End)
    pub fn to_prometheus_value(&self) -> i32 {
        match self {
            Self::PutWholeBegin
            | Self::PutStartBegin
            | Self::PutTransferBegin
            | Self::PutEndBegin
            | Self::PutRpcBegin
            | Self::GetWholeBegin
            | Self::GetStartBegin
            | Self::GetTransferBegin
            | Self::GetEndBegin => 1,

            Self::PutWholeEnd
            | Self::PutStartEnd
            | Self::PutTransferEnd
            | Self::PutEndEnd
            | Self::PutRpcEnd
            | Self::GetWholeEnd
            | Self::GetStartEnd
            | Self::GetTransferEnd
            | Self::GetEndEnd => 0,
        }
    }

    /// Get the operation phase name (without Begin/End suffix)
    pub fn get_phase_name(&self) -> &'static str {
        match self {
            Self::PutWholeBegin | Self::PutWholeEnd => "put_whole",
            Self::PutStartBegin | Self::PutStartEnd => "put_start",
            Self::PutTransferBegin | Self::PutTransferEnd => "put_transfer",
            Self::PutEndBegin | Self::PutEndEnd => "put_end",
            Self::PutRpcBegin | Self::PutRpcEnd => "put_rpc",
            Self::GetWholeBegin | Self::GetWholeEnd => "get_whole",
            Self::GetStartBegin | Self::GetStartEnd => "get_start",
            Self::GetTransferBegin | Self::GetTransferEnd => "get_transfer",
            Self::GetEndBegin | Self::GetEndEnd => "get_end",
        }
    }

    /// Get the base operation name (put/get)
    pub fn get_operation_name(&self) -> &'static str {
        match self {
            Self::PutWholeBegin
            | Self::PutWholeEnd
            | Self::PutStartBegin
            | Self::PutStartEnd
            | Self::PutTransferBegin
            | Self::PutTransferEnd
            | Self::PutEndBegin
            | Self::PutEndEnd
            | Self::PutRpcBegin
            | Self::PutRpcEnd => "put",

            Self::GetWholeBegin
            | Self::GetWholeEnd
            | Self::GetStartBegin
            | Self::GetStartEnd
            | Self::GetTransferBegin
            | Self::GetTransferEnd
            | Self::GetEndBegin
            | Self::GetEndEnd => "get",
        }
    }

    /// Check if this is a begin event
    pub fn is_begin(&self) -> bool {
        self.to_prometheus_value() == 1
    }

    /// Check if this is an end event
    pub fn is_end(&self) -> bool {
        self.to_prometheus_value() == 0
    }
}

/// KV operation metrics type enum
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum KvMetrics {
    /// Various phases of Put operation
    Put {
        whole_put: i64,
        start: i64,

        transfer: i64,
        end: i64,
        rpc_of_put_start: i64,
        /// Server handling time for PutStart RPC (microseconds)
        start_handle: i64,
        /// Server handling time for PutDone RPC (microseconds)
        end_handle: i64,
        /// Key associated with the put operation
        key: String,
        /// Put operation ID formatted as "{}.{}"
        put_id: String,
        /// ✅ 源头时间戳：操作真正开始的时间 (微秒) - t1
        start_timestamp_us: i64,
        /// ✅ 源头时间戳：start阶段结束/transfer阶段开始的时间 (微秒) - t2
        transfer_start_timestamp_us: i64,
        /// ✅ 源头时间戳：transfer阶段结束/end阶段开始的时间 (微秒) - t3
        end_start_timestamp_us: i64,
        /// ✅ 源头时间戳：操作真正结束的时间 (微秒) - t4
        end_timestamp_us: i64,
        transfer_submit_blocking_us: i64,
        transfer_create_xfer_req_us: i64,
        transfer_post_xfer_req_us: i64,
        transfer_poll_wait_us: i64,
        transfer_poll_iters: i64,
        transfer_used_fast_path: bool,
        transfer_used_nixl: bool,
        transfer_local_noop: bool,
        transfer_remote_transfer: bool,
    },
    /// Various phases of Get operation
    Get {
        whole_get: i64,
        start: i64,
        transfer: i64,
        end: i64,
        /// Server handling time for GetStart RPC (microseconds)
        start_handle: i64,
        /// Server handling time for GetDone RPC (microseconds)
        end_handle: i64,
        /// Key associated with the get operation
        key: String,
        /// Get operation ID formatted as "{}.{}"
        get_id: String,
        /// ✅ 源头时间戳：操作真正开始的时间 (微秒) - t1
        start_timestamp_us: i64,
        /// ✅ 源头时间戳：start阶段结束/transfer阶段开始的时间 (微秒) - t2
        transfer_start_timestamp_us: i64,
        /// ✅ 源头时间戳：transfer阶段结束/end阶段开始的时间 (微秒) - t3
        end_start_timestamp_us: i64,
        /// ✅ 源头时间戳：操作真正结束的时间 (微秒) - t4
        end_timestamp_us: i64,
    },
}

#[cfg(test)]
pub mod client_test_record;
mod delete;
mod get;
pub mod msg_pack;
mod put;

// --- External RPC Handlers ---
use crate::p2p::msg_pack::MsgPack;
use crate::rpcresp_kvresult_convert::FromError;

// External handlers that use the ExternalApi trait on ClientKvApi
async fn handle_external_get(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalGetReq>,
) -> MsgPack<ExternalGetResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let dbg_req_node_id = req.req_node_id.clone();
    let resp = view
        .client_kv_api()
        .external_get(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_get error: {e}; key={key}, req_node_id={req_node_id}",
                key = dbg_key,
                req_node_id = dbg_req_node_id
            );
            ExternalGetResp {
                external_memholder_info: None,
                ..crate::rpcresp_kvresult_convert::FromError::from_error(&e)
            }
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetReq>,
) -> MsgPack<ExternalBatchGetResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_get(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchGetResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_local_probe(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetLocalProbeReq>,
) -> MsgPack<ExternalBatchGetLocalProbeResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_get_local_probe(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_local_probe error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            ExternalBatchGetLocalProbeResp {
                items: Vec::new(),
                error_code: e.code(),
                error_json: e.to_json(),
            }
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_start(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetStartReq>,
) -> MsgPack<ExternalBatchGetStartResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_get_start(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_start error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_transfer(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetTransferReq>,
) -> MsgPack<ExternalBatchGetTransferResp> {
    let req = msg.serialize_part.clone();
    let dbg_handle = req.handle;
    let resp = view
        .client_kv_api()
        .external_batch_get_transfer(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_transfer error: {e}; handle={handle}",
                handle = dbg_handle
            );
            let mut r: ExternalBatchGetTransferResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_execute_planned_get(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalExecutePlannedGetReq>,
) -> MsgPack<ExternalExecutePlannedGetResp> {
    let req = msg.serialize_part.clone();
    let plan_handle = req.plan_handle;
    let item_count = req.items.len();
    let resp = view
        .client_kv_api()
        .external_execute_planned_get(req)
        .await
        .unwrap_or_else(|err| {
            tracing::error!(
                "handle_external_execute_planned_get error: {}; plan_handle={} items={}",
                err,
                plan_handle,
                item_count
            );
            ExternalExecutePlannedGetResp {
                items: Vec::new(),
                error_code: err.code(),
                error_json: err.to_json(),
            }
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_get_cancel(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchGetCancelReq>,
) -> MsgPack<ExternalBatchGetCancelResp> {
    let req = msg.serialize_part.clone();
    let dbg_handle = req.handle;
    let resp = view
        .client_kv_api()
        .external_batch_get_cancel(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_get_cancel error: {e}; handle={handle}",
                handle = dbg_handle
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_start(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutStartReq>,
) -> MsgPack<ExternalPutStartResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let dbg_len = req.len;
    let resp = view
        .client_kv_api()
        .external_put_start(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_start error: {e}; key={key}, len={len}",
                key = dbg_key,
                len = dbg_len
            );
            let mut r: ExternalPutStartResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.src_offset = 0;
            r.target_offset = 0;
            r.transfer_target_offset = None;
            r.peer_id = None;
            r.put_id = None;
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_put_start(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchPutStartReq>,
) -> MsgPack<ExternalBatchPutStartResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.items.len();
    let resp = view
        .client_kv_api()
        .external_batch_put_start(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_put_start error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchPutStartResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_transfer_end(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutTransferEndReq>,
) -> MsgPack<ExternalPutTransferEndResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let dbg_put_id = req.put_id.clone();
    let resp = view
        .client_kv_api()
        .external_put_transfer_end(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_transfer_end error: {e}; key={key}, put_id={put_id:?}",
                key = dbg_key,
                put_id = dbg_put_id
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_put_transfer_end(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchPutTransferEndReq>,
) -> MsgPack<ExternalBatchPutTransferEndResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.items.len();
    let resp = view
        .client_kv_api()
        .external_batch_put_transfer_end(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_put_transfer_end error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchPutTransferEndResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_commit(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutCommitReq>,
) -> MsgPack<ExternalPutCommitResp> {
    let req = msg.serialize_part.clone();
    let dbg_key = req.key.clone();
    let dbg_put_id = req.put_id.clone();
    let resp = view
        .client_kv_api()
        .external_put_commit(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_commit error: {e}; key={key}, put_id={put_id:?}",
                key = dbg_key,
                put_id = dbg_put_id
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_put_commit(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchPutCommitReq>,
) -> MsgPack<ExternalBatchPutCommitResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.items.len();
    let resp = view
        .client_kv_api()
        .external_batch_put_commit(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_put_commit error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchPutCommitResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.items = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_put_revoke(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalPutRevokeReq>,
) -> MsgPack<ExternalPutRevokeResp> {
    let req = msg.serialize_part.clone();
    let dbg_key = req.key.clone();
    let dbg_put_id = req.put_id.clone();
    let resp = view
        .client_kv_api()
        .external_put_revoke(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_put_revoke error: {e}; key={key}, put_id={put_id:?}",
                key = dbg_key,
                put_id = dbg_put_id
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_delete_ack(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalDeleteAckReq>,
) -> MsgPack<ExternalDeleteAckResp> {
    let req = msg.serialize_part.clone();
    // Validate owner's start_time (allow 0 for legacy callers)
    let expected = view.cluster_manager().get_self_info().node_start_time;
    if req.started_time != 0 && req.started_time != expected {
        let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                expected,
                got: req.started_time,
            },
        );
        return MsgPack {
            serialize_part: ExternalDeleteAckResp::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }
    let inner = view.client_kv_api().inner();
    // Try to remove the holding record for this external client and holder_id
    let mut success = false;
    let mut error_msg = String::new();

    match inner.external_get_holding.remove(&NodeHolderKey::new(
        req.external_client_id.clone(),
        req.holder_id,
    )) {
        Some(_) => success = true,
        None => {
            error_msg = format!(
                "holding id {} not found for client {}",
                req.holder_id, req.external_client_id
            );
        }
    }

    MsgPack {
        serialize_part: ExternalDeleteAckResp {
            error_code: if success {
                crate::rpcresp_kvresult_convert::msg_and_error::OK
            } else {
                crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND
            },
            error_json: error_msg,
        },
        raw_bytes: Vec::new(),
    }
}

fn release_external_holder_ids_with(
    holder_ids: Vec<u64>,
    mut remove: impl FnMut(u64) -> bool,
) -> (u32, u32) {
    let mut seen = HashSet::with_capacity(holder_ids.len());
    let mut released = 0u32;
    let mut missing = 0u32;
    for holder_id in holder_ids {
        if !seen.insert(holder_id) {
            continue;
        }
        if remove(holder_id) {
            released = released.saturating_add(1);
        } else {
            missing = missing.saturating_add(1);
        }
    }
    (released, missing)
}

async fn handle_external_batch_delete_ack(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchDeleteAckReq>,
) -> MsgPack<ExternalBatchDeleteAckResp> {
    let req = msg.serialize_part.clone();
    let expected = view.cluster_manager().get_self_info().node_start_time;
    if req.started_time != 0 && req.started_time != expected {
        let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                expected,
                got: req.started_time,
            },
        );
        return MsgPack {
            serialize_part: ExternalBatchDeleteAckResp::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }

    let inner = view.client_kv_api().inner();
    let external_client_id = req.external_client_id;
    let requested_count = req.holder_ids.len();
    let (released_count, missing_count) =
        release_external_holder_ids_with(req.holder_ids, |holder_id| {
            inner
                .external_get_holding
                .remove(&NodeHolderKey::new(external_client_id.clone(), holder_id))
                .is_some()
        });
    tracing::debug!(
        external_client_id,
        requested_count,
        released_count,
        missing_count,
        "processed external holder ACK batch"
    );
    MsgPack {
        serialize_part: ExternalBatchDeleteAckResp {
            released_count,
            missing_count,
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}

#[cfg(test)]
mod external_delete_ack_batch_tests {
    use super::release_external_holder_ids_with;
    use std::collections::HashSet;

    #[test]
    fn duplicate_and_missing_holder_ids_are_idempotent() {
        let mut live = HashSet::from([3u64, 5u64]);
        let (released, missing) =
            release_external_holder_ids_with(vec![3, 3, 4, 5], |holder_id| live.remove(&holder_id));
        assert_eq!((released, missing), (2, 1));
        assert!(live.is_empty());
    }
}

async fn handle_external_delete(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalDeleteReq>,
) -> MsgPack<ExternalDeleteResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let resp = view
        .client_kv_api()
        .external_delete(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_delete error: {e}; key={key}",
                key = dbg_key
            );
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_is_exist(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalIsExistReq>,
) -> MsgPack<ExternalIsExistResp> {
    let req = msg.serialize_part.clone();
    // Handler only registers in client mode
    let dbg_key = req.key.clone();
    let resp = view
        .client_kv_api()
        .external_is_exist(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_is_exist error: {e}; key={key}",
                key = dbg_key
            );
            let mut r: ExternalIsExistResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.exists = false;
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_batch_is_exist(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalBatchIsExistReq>,
) -> MsgPack<ExternalBatchIsExistResp> {
    let req = msg.serialize_part.clone();
    let dbg_len = req.keys.len();
    let resp = view
        .client_kv_api()
        .external_batch_is_exist(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(
                "handle_external_batch_is_exist error: {e}; batch_len={batch_len}",
                batch_len = dbg_len
            );
            let mut r: ExternalBatchIsExistResp =
                crate::rpcresp_kvresult_convert::FromError::from_error(&e);
            r.exists_list = Vec::new();
            r
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

async fn handle_external_observability_snapshot(
    view: &ClientKvApiView,
    msg: &MsgPack<ExternalObservabilitySnapshotReq>,
) -> MsgPack<ExternalObservabilitySnapshotResp> {
    let req = msg.serialize_part.clone();
    let resp = view
        .client_kv_api()
        .external_observability_snapshot(req)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("handle_external_observability_snapshot error: {e}");
            crate::rpcresp_kvresult_convert::FromError::from_error(&e)
        });
    MsgPack {
        serialize_part: resp,
        raw_bytes: Vec::new(),
    }
}

fn write_all_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::io::{Error, ErrorKind};
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let n = file.write_at(buf, offset)?;
        if n == 0 {
            return Err(Error::new(ErrorKind::WriteZero, "write_at returned 0"));
        }
        offset = offset
            .checked_add(n as u64)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "offset overflow"))?;
        buf = &buf[n..];
    }
    Ok(())
}

fn sync_kv_bytes_field_to_file(
    encoded_flat_dict: &[u8],
    bytes_field_key: &str,
    filepath: &str,
    file_offset: u64,
) -> KvResult<()> {
    use crate::memholder::kvclient_encode::FlatKvValueRange;

    if bytes_field_key.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "bytes_field_key must be non-empty".to_string(),
        }));
    }
    if filepath.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "filepath must be non-empty".to_string(),
        }));
    }

    let entries = crate::memholder::kvclient_encode::flat_kv_decode_ranges(encoded_flat_dict)
        .map_err(|e| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!("flat dict decode failed: {}", e),
            })
        })?;

    let mut found: Option<(usize, usize)> = None;
    for (k, v) in entries {
        if k != bytes_field_key {
            continue;
        }
        match v {
            FlatKvValueRange::BytesRange { start, len } => {
                found = Some((start, len));
            }
            _ => {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("field is not bytes: {}", bytes_field_key),
                }));
            }
        }
        break;
    }

    let Some((start, len)) = found else {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: format!("missing bytes field: {}", bytes_field_key),
        }));
    };

    let end = start.checked_add(len).ok_or_else(|| {
        KvError::Api(ApiError::InvalidArgument {
            detail: "bytes range overflow".to_string(),
        })
    })?;
    if end > encoded_flat_dict.len() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "bytes range out of bounds".to_string(),
        }));
    }

    let data = &encoded_flat_dict[start..end];

    let path = std::path::Path::new(filepath);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| {
                KvError::Api(ApiError::FileWriteError {
                    path: filepath.to_string(),
                    offset: file_offset,
                    detail: format!("create parent dir failed: {}", e),
                })
            })?;
        }
    }

    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .map_err(|e| {
            KvError::Api(ApiError::FileWriteError {
                path: filepath.to_string(),
                offset: file_offset,
                detail: e.to_string(),
            })
        })?;

    write_all_at(&f, data, file_offset).map_err(|e| {
        KvError::Api(ApiError::FileWriteError {
            path: filepath.to_string(),
            offset: file_offset,
            detail: e.to_string(),
        })
    })?;

    Ok(())
}

async fn handle_sync_kv_to_file_client(
    view: &ClientKvApiView,
    msg: &MsgPack<SyncKvToFileReq>,
) -> MsgPack<SyncKvToFileResp> {
    let req = msg.serialize_part.clone();
    let key = req.key.clone();

    let result: KvResult<()> = async {
        if req.key.is_empty() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "key must be non-empty".to_string(),
            }));
        }

        let got = view.client_kv_api().get(&req.key).await?;
        let Some((holder, _remote)) = got else {
            return Err(KvError::Api(ApiError::KeyNotFound { key }));
        };

        sync_kv_bytes_field_to_file(
            holder.bytes(),
            req.bytes_field_key.as_str(),
            req.filepath.as_str(),
            req.file_offset,
        )?;
        Ok(())
    }
    .await;

    let (error_code, error_json) = match result {
        Ok(()) => (
            crate::rpcresp_kvresult_convert::msg_and_error::OK,
            String::new(),
        ),
        Err(e) => (e.code(), e.to_json()),
    };

    MsgPack {
        serialize_part: SyncKvToFileResp {
            error_code,
            error_json,
        },
        raw_bytes: Vec::new(),
    }
}

define_module!(
    ClientKvApi,
    (cluster_manager, ClusterManager),
    (p2p, P2pModule),
    (client_kv_api, ClientKvApi),
    (client_transfer_engine, ClientTransferEngine),
    (client_seg_pool, ClientSegPool),
    (metric_reporter, MetricReporter)
);

// Use unified conversion in msg_and_error.rs: ClusterManagerExtError -> KvError::ClusterManagerExt

/// ClientKvApi module creation parameters
#[derive(Clone, Debug)]
pub struct ClientKvApiNewArg {
    pub test_spec_config: TestSpecConfig,
    /// Logical hot-tier capacity only. This does not resize the owner segment.
    pub owner_hot_cache_capacity_bytes: Option<u64>,
    /// Physical owner DRAM already declared by `contribute_to_cluster_pool_size.dram`.
    /// The one owner allocator manages this complete segment while Moka
    /// continues to enforce the smaller logical hot-tier capacity.
    pub owner_local_reserve_physical_capacity_bytes: u64,
    pub allocation_authority: crate::master_seg_manager::msg_pack::SegmentAllocationAuthority,
    pub owner_placement_class: crate::master_seg_manager::msg_pack::OwnerPlacementClass,
    pub ssd_storage: Option<KvSsdStorageInit>,
}

pub struct ClientKvApi(ClientKvApiInner);

#[derive(Debug)]
pub struct GetCachedInfo {
    put_time_ms: u64,
    put_version: u32,
    mem_holder: Arc<MemoryInfo>,
}

#[derive(Debug)]
pub struct PrecommitLocalVisibleInfo {
    mem_holder: Arc<MemoryInfo>,
}

#[derive(Debug)]
pub(crate) struct PendingLocalGetInfo {
    get_id: u64,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    mem_holder: Arc<MemoryInfo>,
    source: PendingLocalGetSource,
    target_write_capability: Option<crate::owner_segment::OwnerTargetWriteCapability>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PendingLocalGetSource {
    PreparedDestination,
    ExistingGlobalShared,
}

#[derive(Debug)]
pub(crate) struct LocalSnapshotInfo {
    put_time_ms: u64,
    put_version: u32,
}

#[derive(Clone)]
struct OwnerHotCacheEntry {
    put_id: crate::master_kv_router::put::PutIDForAKey,
    memory_info: Weak<MemoryInfo>,
    weight_bytes: u32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct OwnerHotPinAlias {
    key: String,
    memory_info_ptr: usize,
}

impl OwnerHotPinAlias {
    fn new(memory_info: &Arc<MemoryInfo>) -> Self {
        Self {
            key: memory_info.key.clone(),
            memory_info_ptr: Arc::as_ptr(memory_info) as usize,
        }
    }
}

type OwnerHotCache = PinAwareMoka<String, OwnerHotPinAlias, OwnerHotCacheEntry>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct OwnerHotReplicaIdentity {
    key: String,
    put_time_ms: u64,
    put_version: u32,
}

#[derive(Default)]
struct OwnerHotCacheCounters {
    size_evictions: AtomicU64,
    source_evict_handoff_members: AtomicU64,
    source_evict_committed_members: AtomicU64,
    source_evict_restored_members: AtomicU64,
    source_evict_obsolete: AtomicU64,
    source_evict_dispatch_failed: AtomicU64,
    source_evict_retry_scheduled: AtomicU64,
    source_evict_retry_emitted: AtomicU64,
    selection_debt_bytes: Arc<AtomicU64>,
    /// Bytes behind an installed source-selection fence. Unlike candidate
    /// debt, every byte here can become a physical Free slot after reclaim.
    source_eviction_selected_bytes: AtomicU64,
    skipped_stale: AtomicU64,
    skipped_reclaim: AtomicU64,
    skipped_active_holders: AtomicU64,
    victim_duplicates: AtomicU64,
    victim_invalid_backing: AtomicU64,
    grouped_put_done_batches: AtomicU64,
    grouped_put_done_items: AtomicU64,
    legacy_put_done_batches: AtomicU64,
    legacy_put_done_items: AtomicU64,
}

#[derive(Default)]
struct OwnerRemotePutCounters {
    active: AtomicU64,
    leaders: AtomicU64,
    followers: AtomicU64,
    source_unavailable: AtomicU64,
    source_fenced: AtomicU64,
    source_missing: AtomicU64,
    source_version_mismatch: AtomicU64,
    transfers: AtomicU64,
    published: AtomicU64,
    already_satisfied: AtomicU64,
    obsolete: AtomicU64,
    failed: AtomicU64,
    task_dropped: AtomicU64,
}

/// Direct remote Put admission is a no-queue resource boundary. The exact source bytes are the
/// primary budget; an optional item ceiling is only a safety bound against tiny-value task storms.
/// Both counters are reserved before the generation flight is installed or a UserMemHolder pins
/// the source. A failed `try_acquire` returns immediately and never creates deferred work.
struct OwnerRemotePutAdmission {
    max_bytes: Option<u64>,
    max_items: Option<u64>,
    active_bytes: AtomicU64,
    active_items: AtomicU64,
    peak_bytes: AtomicU64,
    peak_items: AtomicU64,
    admitted: AtomicU64,
    not_admitted: AtomicU64,
    not_admitted_bytes: AtomicU64,
}

impl OwnerRemotePutAdmission {
    fn new(max_bytes: Option<u64>, max_items: Option<u64>) -> Arc<Self> {
        debug_assert!(max_bytes != Some(0));
        debug_assert!(max_items != Some(0));
        debug_assert!(max_items.is_none() || max_bytes.is_some());
        Arc::new(Self {
            max_bytes,
            max_items,
            active_bytes: AtomicU64::new(0),
            active_items: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            peak_items: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            not_admitted: AtomicU64::new(0),
            not_admitted_bytes: AtomicU64::new(0),
        })
    }

    fn try_add_with_limit(current: &AtomicU64, amount: u64, limit: Option<u64>) -> Option<u64> {
        let mut observed = current.load(Ordering::Acquire);
        loop {
            let next = observed.checked_add(amount)?;
            if limit.is_some_and(|limit| next > limit) {
                return None;
            }
            match current.compare_exchange_weak(observed, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(next),
                Err(actual) => observed = actual,
            }
        }
    }

    fn record_not_admitted(&self, bytes: u64) {
        self.not_admitted.fetch_add(1, Ordering::Relaxed);
        self.not_admitted_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn try_acquire(self: &Arc<Self>, bytes: u64) -> Option<OwnerRemotePutAdmissionPermit> {
        let Some(active_bytes) =
            Self::try_add_with_limit(&self.active_bytes, bytes, self.max_bytes)
        else {
            self.record_not_admitted(bytes);
            return None;
        };
        let Some(active_items) = Self::try_add_with_limit(&self.active_items, 1, self.max_items)
        else {
            let previous = self.active_bytes.fetch_sub(bytes, Ordering::AcqRel);
            debug_assert!(
                previous >= bytes,
                "remote Put admission byte refund underflow"
            );
            self.record_not_admitted(bytes);
            return None;
        };

        self.peak_bytes.fetch_max(active_bytes, Ordering::Relaxed);
        self.peak_items.fetch_max(active_items, Ordering::Relaxed);
        self.admitted.fetch_add(1, Ordering::Relaxed);
        Some(OwnerRemotePutAdmissionPermit {
            admission: self.clone(),
            bytes: Some(bytes),
        })
    }

    fn release_bytes(&self, bytes: u64) {
        let previous_bytes = self.active_bytes.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(
            previous_bytes >= bytes,
            "remote Put admission byte release underflow"
        );
    }

    fn release_item(&self) {
        let previous_items = self.active_items.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(
            previous_items >= 1,
            "remote Put admission item release underflow"
        );
    }
}

/// Unique admission credits for one exact `(key, put_id)` leader generation. Bytes cover only the
/// transfer and can be released as soon as `put_transfer` returns. The item remains held until the
/// permit is dropped at Done/Revoke terminal publication. RAII returns whichever credits remain
/// after spawn unwind, panic, or task abort.
pub(crate) struct OwnerRemotePutAdmissionPermit {
    admission: Arc<OwnerRemotePutAdmission>,
    bytes: Option<u64>,
}

impl OwnerRemotePutAdmissionPermit {
    pub(crate) fn release_transfer_bytes(&mut self) -> bool {
        let Some(bytes) = self.bytes.take() else {
            return false;
        };
        self.admission.release_bytes(bytes);
        true
    }
}

impl Drop for OwnerRemotePutAdmissionPermit {
    fn drop(&mut self) {
        self.release_transfer_bytes();
        self.admission.release_item();
    }
}

#[derive(Default)]
struct OwnerLocalSsdPutCounters {
    active: AtomicU64,
    leaders: AtomicU64,
    followers: AtomicU64,
    source_unavailable: AtomicU64,
    published: AtomicU64,
    already_present: AtomicU64,
    dropped: AtomicU64,
    obsolete: AtomicU64,
    failed: AtomicU64,
}

#[derive(Default)]
struct OwnerPlannedGetCounters {
    local_probe_batches: AtomicU64,
    local_probe_items: AtomicU64,
    local_probe_local_items: AtomicU64,
    local_probe_remote_items: AtomicU64,
    batches: AtomicU64,
    local_items: AtomicU64,
    leader_items: AtomicU64,
    follower_items: AtomicU64,
}

#[derive(Default)]
struct OwnerSsdStageCounters {
    ready_requests: AtomicU64,
    ready_successes: AtomicU64,
    ready_failures: AtomicU64,
    ready_duration_us: AtomicU64,
    execute_completions: AtomicU64,
    terminal_published: AtomicU64,
    terminal_cache_inserts: AtomicU64,
    terminal_cache_duration_us: AtomicU64,
    response_send_attempts: AtomicU64,
    response_send_successes: AtomicU64,
    response_send_failures: AtomicU64,
    response_send_duration_us: AtomicU64,
    source_ready_wait_requests: AtomicU64,
    source_ready_wait_successes: AtomicU64,
    source_ready_wait_failures: AtomicU64,
    source_ready_wait_duration_us: AtomicU64,
    target_pull_requests: AtomicU64,
    target_pull_successes: AtomicU64,
    target_pull_failures: AtomicU64,
    target_pull_duration_us: AtomicU64,
    done_detached: AtomicU64,
}

struct OwnerHotSelectionDebt {
    weight_bytes: u64,
    outstanding_bytes: Arc<AtomicU64>,
    released: AtomicU32,
}

impl OwnerHotSelectionDebt {
    fn new(weight_bytes: u64, outstanding_bytes: Arc<AtomicU64>) -> Arc<Self> {
        outstanding_bytes.fetch_add(weight_bytes, Ordering::AcqRel);
        Arc::new(Self {
            weight_bytes,
            outstanding_bytes,
            released: AtomicU32::new(0),
        })
    }

    fn release(&self) {
        if self
            .released
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.outstanding_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(self.weight_bytes))
                })
                .expect("owner hot selection debt update cannot fail");
        }
    }
}

impl OwnerHotCacheCounters {
    fn add_source_eviction_selected_bytes(&self, bytes: u64) {
        self.source_eviction_selected_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes)
            })
            .expect("owner source-selection byte credit overflowed");
    }

    fn remove_source_eviction_selected_bytes(&self, bytes: u64) {
        self.source_eviction_selected_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_sub(bytes)
            })
            .expect("owner source-selection byte credit underflowed");
    }
}

#[derive(Clone)]
pub(crate) struct OwnerHotEvictionEvent {
    key: String,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    memory_info: Weak<MemoryInfo>,
    selection_debt: Arc<OwnerHotSelectionDebt>,
    /// The event was dispatched from the bounded retry queue.
    retry: bool,
    /// Exact single-key source-delete transaction retained across RPC retries.
    source_eviction_victim:
        Option<Arc<crate::master_kv_router::msg_pack::OwnerSourceEvictionVictim>>,
    /// Failure count follows the event while it is dispatched, preserving
    /// exponential backoff across queue take/reinsert cycles.
    retry_failures: u32,
}

pub(crate) enum OwnerHotEvictionDispatch {
    Victim(OwnerHotEvictionEvent),
    BeginPressure {
        requested_bytes: u64,
    },
    EndPressure {
        selected_bytes: u64,
        /// The pressure producer must not select another Moka batch until
        /// all bounded source batches have finished fencing, master
        /// direct-delete, and owner slot-release handling. Each source batch
        /// re-reads GlobalShared headroom before requesting reclaim.
        /// Candidate debt before that point is
        /// deliberately not projected reclaim credit.  The dispatcher
        /// returns both the requested bytes and the bytes actually selected
        /// from the master's GlobalShared Moka for observability only.  A
        /// selected candidate, retry-only debt or source fence never closes
        /// owner pressure; only the owner allocator's exact Free/claim state
        /// does.
        completion: ::tokio::sync::oneshot::Sender<OwnerPressureBatchReclaim>,
    },
    Flush,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OwnerPressureBatchReclaim {
    pub requested_bytes: u64,
    pub selected_bytes: u64,
}

pub(crate) enum OwnerHotEvictionPreparation {
    Ready {
        trigger: OwnerHotReplicaIdentity,
        source: Arc<MemoryInfo>,
    },
    RetryableReclaimFence,
    TemporarilyPinned,
    Obsolete,
}

pub(crate) enum OwnerHotSelectionFenceOutcome {
    Fenced,
    Retryable,
    TemporarilyPinned,
    Obsolete,
}

struct OwnerHotRetryEntry {
    event: OwnerHotEvictionEvent,
    failures: u32,
    next_attempt_at: Instant,
    dispatched: bool,
}

#[derive(Default)]
struct OwnerHotRetryState {
    entries: HashMap<OwnerHotReplicaIdentity, OwnerHotRetryEntry>,
    /// Exactly one deadline for each non-dispatched entry.  Unlike a lazy
    /// generation heap, rescheduling replaces the old tuple, so both memory
    /// and lock-held work are bounded by the live retry set, not its history.
    deadlines: BTreeSet<(Instant, OwnerHotReplicaIdentity)>,
}

/// Exactly-once owner-local retry state.  It is physically bounded by the
/// owner committed-slot pool: one identity can occupy at most one entry, and
/// obsolete identities are removed when their local version is invalidated.
/// The actor emits only a small due batch and applies exponential backoff.
struct OwnerHotRetryQueue {
    state: Mutex<OwnerHotRetryState>,
    notify: Arc<limit_thirdparty::tokio::sync::Notify>,
    counters: Arc<OwnerHotCacheCounters>,
}

impl OwnerHotRetryQueue {
    fn new(counters: Arc<OwnerHotCacheCounters>) -> Self {
        Self {
            state: Mutex::new(OwnerHotRetryState::default()),
            notify: Arc::new(limit_thirdparty::tokio::sync::Notify::new()),
            counters,
        }
    }

    fn retry_delay(failures: u32) -> Duration {
        let shift = failures.saturating_sub(1).min(8);
        Duration::from_millis(25u64.saturating_mul(1u64 << shift)).min(Duration::from_secs(5))
    }

    fn schedule(&self, mut event: OwnerHotEvictionEvent, reason: &'static str) {
        let identity = OwnerHotReplicaIdentity {
            key: event.key.clone(),
            put_time_ms: event.put_id.0,
            put_version: event.put_id.1,
        };
        let now = Instant::now();
        let mut state = self.state.lock();
        let previous_deadline = state
            .entries
            .get(&identity)
            .and_then(|entry| (!entry.dispatched).then_some(entry.next_attempt_at));
        if let Some(previous_deadline) = previous_deadline {
            state
                .deadlines
                .remove(&(previous_deadline, identity.clone()));
        }
        let entry = state
            .entries
            .entry(identity.clone())
            .or_insert_with(|| OwnerHotRetryEntry {
                event: event.clone(),
                failures: event.retry_failures,
                next_attempt_at: now,
                dispatched: false,
            });
        if !Arc::ptr_eq(&entry.event.selection_debt, &event.selection_debt) {
            entry.event.selection_debt.release();
        }
        if event.source_eviction_victim.is_none() {
            event.source_eviction_victim = entry.event.source_eviction_victim.clone();
        }
        entry.failures = entry.failures.max(event.retry_failures).saturating_add(1);
        event.retry = true;
        event.retry_failures = entry.failures;
        entry.event = event;
        entry.next_attempt_at = now + Self::retry_delay(entry.failures);
        entry.dispatched = false;
        let next_attempt_at = entry.next_attempt_at;
        let inserted = state.deadlines.insert((next_attempt_at, identity.clone()));
        debug_assert!(inserted, "owner retry deadline must be unique per identity");
        drop(state);
        self.counters
            .source_evict_retry_scheduled
            .fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            key = identity.key,
            put_time_ms = identity.put_time_ms,
            put_version = identity.put_version,
            reason,
            "owner writeback entered retryable local state"
        );
        self.notify.notify_waiters();
    }

    fn take_due_batch(&self, now: Instant, limit: usize) -> Vec<OwnerHotEvictionEvent> {
        let mut state = self.state.lock();
        let mut due = Vec::with_capacity(limit);
        while due.len() < limit {
            let Some((deadline, identity)) = state.deadlines.iter().next().cloned() else {
                break;
            };
            if deadline > now {
                break;
            }
            state.deadlines.remove(&(deadline, identity.clone()));
            let Some(entry) = state.entries.get_mut(&identity) else {
                debug_assert!(false, "owner retry deadline must reference a live entry");
                continue;
            };
            if entry.dispatched || entry.next_attempt_at != deadline {
                debug_assert!(false, "owner retry deadline and entry must agree");
                continue;
            }
            due.push(entry.event.clone());
            // Keep the authoritative retry record until the dispatcher has
            // atomically pinned the source and installed an inflight guard,
            // but emit it only once. A failed dispatcher attempt explicitly
            // reschedules it with the next backoff.
            entry.dispatched = true;
        }
        due
    }

    fn remove(&self, identity: &OwnerHotReplicaIdentity) {
        let entry = {
            let mut state = self.state.lock();
            let entry = state.entries.remove(identity);
            if let Some(entry) = entry.as_ref()
                && !entry.dispatched
            {
                state
                    .deadlines
                    .remove(&(entry.next_attempt_at, identity.clone()));
            }
            entry
        };
        if let Some(entry) = entry {
            entry.event.selection_debt.release();
        }
    }

    fn take_for_inflight(
        &self,
        identity: &OwnerHotReplicaIdentity,
    ) -> Option<OwnerHotEvictionEvent> {
        // The inflight guard takes over the same debt token; do not release it.
        let mut state = self.state.lock();
        let entry = state.entries.remove(identity)?;
        if !entry.dispatched {
            state
                .deadlines
                .remove(&(entry.next_attempt_at, identity.clone()));
        }
        Some(entry.event)
    }

    fn len(&self) -> usize {
        self.state.lock().entries.len()
    }
}

pub(crate) struct OwnerPreparedReclaim {
    item: crate::master_kv_router::msg_pack::OwnerReclaimItem,
    source: OwnerPreparedReclaimSource,
    ssd_prepare_lock: Arc<tokio::sync::AMutex<()>>,
    ssd_prepare_complete: bool,
    ssd_backing: Option<OwnerPreparedSsdBacking>,
}

pub(crate) enum OwnerPreparedReclaimSource {
    /// An owner-indexed source detached from the local Moka while the reclaim fence is active.
    Indexed {
        cached_info: GetCachedInfo,
        local_snapshot: Option<LocalSnapshotInfo>,
    },
    /// A master-owned allocation that has no owner-local key index. The master route owns the
    /// physical allocation through Commit; the owner only reads it under the reclaim fence.
    UnindexedAllocation { addr: u64, len: u64 },
    /// A GlobalShared owner slot has no local key index, but its exact
    /// allocation remains owned by the one owner segment allocator.
    UnindexedOwnerSlot {
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    },
}

pub(crate) struct OwnerPreparedSsdBacking {
    len: u64,
    _persist_guard: crate::kv_ssd_storage::KvSsdPersistGuard,
}

pub(crate) struct OwnerSourceEvictionSelection {
    put_id: crate::master_kv_router::put::PutIDForAKey,
    cached_info: GetCachedInfo,
}

pub(crate) enum OwnerReclaimRecord {
    Prepared(OwnerPreparedReclaim),
    /// The local index is fenced and the Commit handler owns the detached
    /// backing while it updates the slot pool. Keeping this marker in the
    /// per-key table lets that O(1) pool update happen without nesting the
    /// pool mutex under the key-shard mutex.
    Releasing(crate::master_kv_router::msg_pack::OwnerReclaimItem),
    Committed(crate::master_kv_router::msg_pack::OwnerReclaimItem),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExternalPutKeyOutcome {
    InFlight,
    Succeeded,
    Failed,
}

pub(crate) struct ExternalPutKeySharedOp {
    outcome: watch::Sender<ExternalPutKeyOutcome>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerRemotePutOutcome {
    InFlight,
    Published,
    AlreadySatisfied,
    Obsolete,
    Failed,
}

/// Shared leader/follower terminal publication used by owner backing writes.
/// Target-specific requests and executors stay outside this type.
struct OwnerTargetPutFlight<T: Copy> {
    terminal: watch::Sender<Option<T>>,
    completed: AtomicBool,
}

impl<T: Copy> OwnerTargetPutFlight<T> {
    fn new() -> Self {
        let (terminal, _receiver) = watch::channel(None);
        Self {
            terminal,
            completed: AtomicBool::new(false),
        }
    }

    fn terminal(&self) -> Option<T> {
        *self.terminal.borrow()
    }

    fn complete(&self, terminal: T) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.terminal.send_replace(Some(terminal));
        true
    }

    async fn wait(&self) -> Option<T> {
        let mut terminal = self.terminal.subscribe();
        loop {
            if let Some(terminal) = *terminal.borrow_and_update() {
                return Some(terminal);
            }
            if terminal.changed().await.is_err() {
                return None;
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OwnerRemotePutRequest {
    pub preferred_sub_cluster: Option<String>,
    pub protect_source_on_remote_complete: bool,
}

/// One owner-initiated remote write for an exact local generation.
///
/// Every trigger (normal Put, pre-reserved replica, proactive write-back, and
/// tier1) joins this same operation.  The trigger is deliberately absent from
/// the identity so policy labels cannot create a second payload transfer.
pub(crate) struct OwnerRemotePutSharedOp {
    pub key: String,
    pub put_id: crate::master_kv_router::put::PutIDForAKey,
    request: Mutex<OwnerRemotePutRequest>,
    flight: OwnerTargetPutFlight<OwnerRemotePutOutcome>,
}

impl OwnerRemotePutSharedOp {
    fn new(
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_string(),
            put_id,
            request: Mutex::new(OwnerRemotePutRequest {
                preferred_sub_cluster,
                protect_source_on_remote_complete,
            }),
            flight: OwnerTargetPutFlight::new(),
        })
    }

    fn merge_request(
        &self,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) {
        let mut request = self.request.lock();
        if request.preferred_sub_cluster.is_none() {
            request.preferred_sub_cluster = preferred_sub_cluster;
        }
        request.protect_source_on_remote_complete |= protect_source_on_remote_complete;
    }

    pub(crate) fn request(&self) -> OwnerRemotePutRequest {
        self.request.lock().clone()
    }

    pub(crate) fn outcome(&self) -> OwnerRemotePutOutcome {
        self.flight
            .terminal()
            .unwrap_or(OwnerRemotePutOutcome::InFlight)
    }

    fn complete(&self, outcome: OwnerRemotePutOutcome) -> bool {
        debug_assert_ne!(outcome, OwnerRemotePutOutcome::InFlight);
        self.flight.complete(outcome)
    }

    pub(crate) async fn wait(&self) -> OwnerRemotePutOutcome {
        self.flight
            .wait()
            .await
            .unwrap_or(OwnerRemotePutOutcome::Failed)
    }
}

pub(crate) enum OwnerRemotePutReservation {
    Leader {
        op: Arc<OwnerRemotePutSharedOp>,
        memory_info: Arc<MemoryInfo>,
        admission_permit: OwnerRemotePutAdmissionPermit,
    },
    Follower(Arc<OwnerRemotePutSharedOp>),
    SourceUnavailable,
    NotAdmitted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerLocalSsdPutOutcome {
    Published,
    AlreadyPresent,
    Dropped,
    Obsolete,
    Failed,
}

/// One same-owner SSD write for an exact local generation. The common flight
/// owns only terminal publication; admission, copy, durability, and route
/// publication remain in the SSD executor.
pub(crate) struct OwnerLocalSsdPutSharedOp {
    pub key: String,
    pub put_id: crate::master_kv_router::put::PutIDForAKey,
    flight: OwnerTargetPutFlight<OwnerLocalSsdPutOutcome>,
}

impl OwnerLocalSsdPutSharedOp {
    fn new(key: &str, put_id: crate::master_kv_router::put::PutIDForAKey) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_string(),
            put_id,
            flight: OwnerTargetPutFlight::new(),
        })
    }

    fn outcome(&self) -> Option<OwnerLocalSsdPutOutcome> {
        self.flight.terminal()
    }

    fn complete(&self, outcome: OwnerLocalSsdPutOutcome) -> bool {
        self.flight.complete(outcome)
    }

    pub(crate) async fn wait(&self) -> OwnerLocalSsdPutOutcome {
        self.flight
            .wait()
            .await
            .unwrap_or(OwnerLocalSsdPutOutcome::Failed)
    }
}

pub(crate) enum OwnerLocalSsdPutReservation {
    Leader {
        op: Arc<OwnerLocalSsdPutSharedOp>,
        memory_info: Arc<MemoryInfo>,
    },
    Follower(Arc<OwnerLocalSsdPutSharedOp>),
    SourceUnavailable,
}

impl ExternalPutKeySharedOp {
    fn new() -> Arc<Self> {
        let (outcome, _receiver) = watch::channel(ExternalPutKeyOutcome::InFlight);
        Arc::new(Self { outcome })
    }

    fn complete(&self, outcome: ExternalPutKeyOutcome) {
        debug_assert_ne!(outcome, ExternalPutKeyOutcome::InFlight);
        if *self.outcome.borrow() == ExternalPutKeyOutcome::InFlight {
            self.outcome.send_replace(outcome);
        }
    }

    pub(crate) async fn wait(&self) -> ExternalPutKeyOutcome {
        let mut outcome = self.outcome.subscribe();
        loop {
            let current = *outcome.borrow_and_update();
            if current != ExternalPutKeyOutcome::InFlight {
                return current;
            }
            if outcome.changed().await.is_err() {
                return ExternalPutKeyOutcome::Failed;
            }
        }
    }
}

pub(crate) enum ExternalLocalFirstPutKeyReservation {
    Leader(Arc<ExternalPendingPutFenceGuard>),
    Wait(Arc<ExternalPutKeySharedOp>),
    /// An incoming PutFromSource or a detached Get CommitTarget already owns
    /// the canonical owner-slot materialization for this key.  A reusable
    /// local-first Put waits without claiming a slot, then re-evaluates its
    /// complete atomic_batch after that materialization reaches terminal.
    WaitForMaterialization(Arc<OwnerKeyMaterializationOp>),
    /// A Get for the same owner-local key won the per-key fence first.  The
    /// Put must not claim a second destination while that Get is materializing
    /// its canonical local result.  The caller drops every partial
    /// `atomic_batch` reservation, waits asynchronously, then re-evaluates the
    /// whole batch under the same key fences.
    WaitForGet(Arc<ExternalGetKeySharedOp>),
    /// A committed owner-local source is between precise source selection and
    /// reclaim completion/rollback.  The caller owns only a watch receiver:
    /// it holds no key fence or physical slot while asynchronously waiting to
    /// re-evaluate the complete atomic_batch.
    WaitForLocalAccess(watch::Receiver<bool>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerKeyMaterializationOutcome {
    InFlight,
    Committed,
    Failed,
}

/// One physical owner-slot materialization for a key generation.
///
/// This is deliberately a small cross-operation fence rather than another
/// transfer state machine.  Planned Get and incoming PutFromSource keep their
/// existing operation identities and replay tables; this watch only elects
/// which operation may allocate/transfer and lets the loser reuse the
/// winner's committed owner manifest.
pub(crate) struct OwnerKeyMaterializationOp {
    pub key: String,
    pub put_id: crate::master_kv_router::put::PutIDForAKey,
    outcome: watch::Sender<OwnerKeyMaterializationOutcome>,
    completed: AtomicBool,
}

impl OwnerKeyMaterializationOp {
    fn new(key: &str, put_id: crate::master_kv_router::put::PutIDForAKey) -> Arc<Self> {
        let (outcome, _receiver) = watch::channel(OwnerKeyMaterializationOutcome::InFlight);
        Arc::new(Self {
            key: key.to_string(),
            put_id,
            outcome,
            completed: AtomicBool::new(false),
        })
    }

    fn complete(&self, outcome: OwnerKeyMaterializationOutcome) -> bool {
        debug_assert_ne!(outcome, OwnerKeyMaterializationOutcome::InFlight);
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.outcome.send_replace(outcome);
        true
    }

    pub(crate) async fn wait(&self) -> OwnerKeyMaterializationOutcome {
        let mut outcome = self.outcome.subscribe();
        loop {
            let current = *outcome.borrow_and_update();
            if current != OwnerKeyMaterializationOutcome::InFlight {
                return current;
            }
            if outcome.changed().await.is_err() {
                return OwnerKeyMaterializationOutcome::Failed;
            }
        }
    }
}

pub(crate) enum OwnerKeyMaterializationReservation {
    Leader(Arc<OwnerKeyMaterializationOp>),
    Wait(Arc<OwnerKeyMaterializationOp>),
    WaitForGet(Arc<ExternalGetKeySharedOp>),
    WaitForExternalPut(Arc<ExternalPutKeySharedOp>),
    WaitForLocalAccess(watch::Receiver<bool>),
}

#[derive(Default)]
pub(crate) struct OwnerKeyControlState {
    local_puts: u32,
    /// External Put contexts that may still expose or commit owner-local
    /// backing for this key.  This counter is maintained by an Arc-backed
    /// guard stored in every context, so cache invalidation cannot clear the
    /// reclaim fence while a cloned context is still in use.
    external_pending_puts: u32,
    /// The reject-on-inflight local-first Put leader for this key. Followers
    /// subscribe to its terminal result without claiming another slot.
    external_put: Option<Arc<ExternalPutKeySharedOp>>,
    /// Exact-generation owner-side remote Put singleflight.  Unlike
    /// `external_put`, this remains active after local publication until the
    /// remote Start/transfer/Done state machine reaches a terminal outcome.
    remote_put: Option<Arc<OwnerRemotePutSharedOp>>,
    /// Exact-generation same-owner SSD write. This is independent from
    /// `remote_put`, so both backing writes may run concurrently.
    local_ssd_put: Option<Arc<OwnerLocalSsdPutSharedOp>>,
    /// Owner-local pre-Prepare fence installed by the Moka source-eviction
    /// dispatcher.  The matching committed index is moved into this record,
    /// so a new local Get cannot acquire the source between victim selection
    /// and the master's reclaim Prepare RPC.
    source_eviction_selection: Option<OwnerSourceEvictionSelection>,
    reclaim: Option<OwnerReclaimRecord>,
    /// Per-key owner-side Get singleflight marker.  It deliberately lives in
    /// the same fence as local visibility and reclaim so `R ∩ local`,
    /// `R ∩ inflight`, and new leaders are classified atomically.
    external_get: Option<Arc<ExternalGetKeySharedOp>>,
    /// Cross-operation slot materialization fence.  The operation-specific
    /// Get/Put state machines remain authoritative; this marker only prevents
    /// them from allocating and transferring the same key generation twice.
    owner_materialization: Option<Arc<OwnerKeyMaterializationOp>>,
    /// Completion channel for the exact source-selection/reclaim fence.  A
    /// receiver subscribed under the key-shard lock cannot miss completion,
    /// even if the fence clears before the waiter is first polled.
    local_access_fence: Option<watch::Sender<bool>>,
}

impl OwnerKeyControlState {
    fn local_access_fenced(&self) -> bool {
        self.source_eviction_selection.is_some() || self.reclaim.is_some()
    }

    fn is_idle(&self) -> bool {
        self.local_puts == 0
            && self.external_pending_puts == 0
            && self.external_put.is_none()
            && self.remote_put.is_none()
            && self.local_ssd_put.is_none()
            && self.source_eviction_selection.is_none()
            && self.reclaim.is_none()
            && self.external_get.is_none()
            && self.owner_materialization.is_none()
            && self.local_access_fence.is_none()
    }

    fn begin_local_access_fence(&mut self) {
        assert!(
            self.local_access_fence.is_none(),
            "one key cannot install two owner local-access fence generations"
        );
        let (completion, _receiver) = watch::channel(false);
        self.local_access_fence = Some(completion);
    }

    fn subscribe_local_access_fence(&self) -> watch::Receiver<bool> {
        assert!(
            self.local_access_fenced(),
            "local-access waiter requires an active source/reclaim fence"
        );
        self.local_access_fence
            .as_ref()
            .expect("active source/reclaim fence must own a completion channel")
            .subscribe()
    }

    fn finish_local_access_fence(&mut self) {
        assert!(
            !self.local_access_fenced(),
            "local-access completion cannot publish before the source/reclaim fence clears"
        );
        if let Some(completion) = self.local_access_fence.take() {
            completion.send_replace(true);
        }
    }

    fn install_remote_put_leader(&mut self, op: Arc<OwnerRemotePutSharedOp>) {
        if let Some(displaced) = self.remote_put.replace(op.clone()) {
            assert_ne!(
                displaced.put_id, op.put_id,
                "a matching remote Put generation must join instead of being replaced"
            );
        }
    }

    fn install_local_ssd_put_leader(&mut self, op: Arc<OwnerLocalSsdPutSharedOp>) {
        if let Some(displaced) = self.local_ssd_put.replace(op.clone()) {
            assert_ne!(
                displaced.put_id, op.put_id,
                "a matching local SSD Put generation must join instead of being replaced"
            );
        }
    }
}

const OWNER_KEY_CONTROL_SHARDS: usize = 256;

/// Per-key owner fencing without a process-wide mutex.
///
/// Every operation for one key hashes to the same shard, so local index
/// publication, Get singleflight registration, and reclaim remain linearized.
/// Callers must hold a shard only for one key and must not await, perform RPC,
/// or walk a request batch while holding it.  Unrelated keys normally proceed
/// on independent shards; a hash collision only adds a short O(1) critical
/// section and does not change correctness.
pub(crate) struct OwnerKeyControlTable {
    shards: Box<[Mutex<HashMap<String, OwnerKeyControlState>>]>,
}

impl Default for OwnerKeyControlTable {
    fn default() -> Self {
        Self {
            shards: (0..OWNER_KEY_CONTROL_SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }
}

impl OwnerKeyControlTable {
    fn shard_index(key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % OWNER_KEY_CONTROL_SHARDS
    }

    pub(crate) fn lock_key(
        &self,
        key: &str,
    ) -> parking_lot::MutexGuard<'_, HashMap<String, OwnerKeyControlState>> {
        self.shards[Self::shard_index(key)].lock()
    }
}

pub(crate) struct OwnerKeyMaterializationGuard {
    view: ClientKvApiView,
    op: Arc<OwnerKeyMaterializationOp>,
    finished: bool,
}

impl OwnerKeyMaterializationGuard {
    fn new(view: ClientKvApiView, op: Arc<OwnerKeyMaterializationOp>) -> Self {
        Self {
            view,
            op,
            finished: false,
        }
    }

    pub(crate) fn finish(&mut self, outcome: OwnerKeyMaterializationOutcome) {
        if self.finished {
            return;
        }
        self.view
            .client_kv_api()
            .inner()
            .finish_owner_key_materialization(&self.op, outcome);
        self.finished = true;
    }
}

impl Drop for OwnerKeyMaterializationGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.view
            .client_kv_api()
            .inner()
            .finish_owner_key_materialization(&self.op, OwnerKeyMaterializationOutcome::Failed);
        self.finished = true;
    }
}

pub(crate) struct ExternalPendingPutFenceGuard {
    key: String,
    owner_key_control: Arc<OwnerKeyControlTable>,
    owns_local_put: bool,
    local_put_op: Option<Arc<ExternalPutKeySharedOp>>,
    local_put_succeeded: std::sync::atomic::AtomicBool,
    local_slot_cleanup_view: Option<ClientKvApiView>,
    local_slot_lease: Mutex<Option<OwnerSlotLease>>,
    local_slot_release_failed: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for ExternalPendingPutFenceGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalPendingPutFenceGuard")
            .field("key", &self.key)
            .field("owns_local_put", &self.owns_local_put)
            .finish_non_exhaustive()
    }
}

impl ExternalPendingPutFenceGuard {
    pub(crate) fn mark_local_put_succeeded(&self) {
        assert!(
            self.owns_local_put,
            "only a local-first Put can publish a reusable terminal result"
        );
        self.local_put_succeeded.store(true, Ordering::Release);
        if let Some(op) = self.local_put_op.as_ref() {
            op.complete(ExternalPutKeyOutcome::Succeeded);
        }
    }

    pub(crate) fn attach_local_slot_lease(&self, lease: OwnerSlotLease) {
        assert!(
            self.owns_local_put,
            "only local-first Put owns a slot lease"
        );
        assert_eq!(
            lease.slots.len(),
            1,
            "a pending local-first Put fence must own exactly one slot"
        );
        let mut current = self.local_slot_lease.lock();
        assert!(
            current.is_none(),
            "a pending local-first Put fence cannot replace its slot lease"
        );
        *current = Some(lease);
    }

    /// Transfer the prepared slot to the precommit/committed MemoryInfo.  Once
    /// disarmed, dropping the pending context must not return that resident slot
    /// to the free list.
    pub(crate) fn disarm_local_slot_lease(&self) {
        assert!(
            self.local_slot_lease.lock().take().is_some(),
            "pending local-first Put slot lease is absent while committing"
        );
    }

    pub(crate) async fn release_local_slot_lease_now(
        &self,
        inner: &ClientKvApiInner,
    ) -> KvResult<()> {
        let lease = self.local_slot_lease.lock().take();
        if let Some(lease) = lease {
            if let Err(err) = inner.owner_release_local_reserve_slot_lease(lease).await {
                // The lease object has been consumed and the physical state is
                // now uncertain.  Keep the per-key fence permanently rather
                // than allow reclaim to cross a possibly-live prepared slot.
                self.local_slot_release_failed
                    .store(true, Ordering::Release);
                return Err(err);
            }
        }
        Ok(())
    }
}

fn release_external_pending_put_counts(
    owner_key_control: &Arc<OwnerKeyControlTable>,
    key: &str,
    owns_local_put: bool,
    local_put_op: Option<Arc<ExternalPutKeySharedOp>>,
    local_put_succeeded: bool,
) {
    let mut controls = owner_key_control.lock_key(key);
    let remove = {
        let state = controls
            .get_mut(key)
            .expect("external pending Put fence state missing on release");
        state.external_pending_puts = state
            .external_pending_puts
            .checked_sub(1)
            .expect("external pending Put fence counter underflow");
        if owns_local_put {
            state.local_puts = state
                .local_puts
                .checked_sub(1)
                .expect("owner local-first Put fence counter underflow");
            if let Some(op) = local_put_op.as_ref()
                && state
                    .external_put
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, op))
            {
                state.external_put = None;
            }
        }
        state.is_idle()
    };
    if remove {
        controls.remove(key);
    }
    drop(controls);
    if let Some(op) = local_put_op {
        op.complete(if local_put_succeeded {
            ExternalPutKeyOutcome::Succeeded
        } else {
            ExternalPutKeyOutcome::Failed
        });
    }
}

fn acquire_external_pending_put_fence_for_key(
    owner_key_control: &Arc<OwnerKeyControlTable>,
    key: &str,
) -> KvResult<Arc<ExternalPendingPutFenceGuard>> {
    let mut controls = owner_key_control.lock_key(key);
    if controls
        .get(key)
        .is_some_and(|state| state.local_access_fenced())
    {
        return Err(KvError::Api(ApiError::KeyBeingWritten {
            key: key.to_string(),
        }));
    }
    let state = controls.entry(key.to_string()).or_default();
    state.external_pending_puts = state
        .external_pending_puts
        .checked_add(1)
        .expect("external pending Put fence counter overflow");
    Ok(Arc::new(ExternalPendingPutFenceGuard {
        key: key.to_string(),
        owner_key_control: owner_key_control.clone(),
        owns_local_put: false,
        local_put_op: None,
        local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
        local_slot_cleanup_view: None,
        local_slot_lease: Mutex::new(None),
        local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
    }))
}

impl Drop for ExternalPendingPutFenceGuard {
    fn drop(&mut self) {
        let abandoned_slot_lease = self.local_slot_lease.get_mut().take();
        if let Some(lease) = abandoned_slot_lease {
            let view = self
                .local_slot_cleanup_view
                .as_ref()
                .expect("local-first Put slot cleanup requires an attached owner view")
                .clone();
            let worker_view = view.clone();
            let key = self.key.clone();
            let owner_key_control = self.owner_key_control.clone();
            let owns_local_put = self.owns_local_put;
            let local_put_op = self.local_put_op.clone();
            let local_put_succeeded = self.local_put_succeeded.load(Ordering::Acquire);
            view.spawn("external_pending_put_slot_drop_cleanup", async move {
                if let Err(err) = worker_view
                    .client_kv_api()
                    .inner()
                    .owner_release_local_reserve_slot_lease(lease)
                    .await
                {
                    tracing::error!("pending local-first Put slot Drop cleanup failed: {}", err);
                    return;
                }
                release_external_pending_put_counts(
                    &owner_key_control,
                    &key,
                    owns_local_put,
                    local_put_op,
                    local_put_succeeded,
                );
            });
        } else if !self.local_slot_release_failed.load(Ordering::Acquire) {
            release_external_pending_put_counts(
                &self.owner_key_control,
                &self.key,
                self.owns_local_put,
                self.local_put_op.clone(),
                self.local_put_succeeded.load(Ordering::Acquire),
            );
        } else {
            tracing::error!(
                "retaining pending Put fence after local slot release failure: key={}",
                self.key
            );
        }
    }
}

fn allocate_external_holding_ids(counter: &AtomicU64, count: usize) -> u64 {
    assert!(count > 0, "external holding reservation cannot be empty");
    let count = u64::try_from(count).expect("external holding reservation must fit u64");
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(count)
        })
        .expect("external holding id space exhausted")
}

fn owner_hot_weight_bytes(memory_info: &MemoryInfo) -> u32 {
    let bytes = memory_info
        .local_reserve_resident_slot_ref()
        .map(|(_, _, capacity_bytes)| capacity_bytes)
        .unwrap_or(memory_info.len as u64);
    u32::try_from(bytes).unwrap_or(u32::MAX)
}

fn clone_if_owner_hot_entry_matches<T>(
    current_put_id: crate::master_kv_router::put::PutIDForAKey,
    current: &Arc<T>,
    entry_put_id: crate::master_kv_router::put::PutIDForAKey,
    entry: &Weak<T>,
) -> Option<Arc<T>> {
    (current_put_id == entry_put_id && Weak::ptr_eq(entry, &Arc::downgrade(current)))
        .then(|| current.clone())
}

fn owner_hot_source_has_active_holders<T>(selected_source: &Arc<T>) -> bool {
    // A reclaimable committed source has exactly two strong references here:
    // one in get_cached_info and one temporary selection pin. Any additional
    // reference belongs to an active local reader/transfer. Sending such a
    // source to the master would only make Prepare return Busy while its
    // selection debt suppresses choosing a different, reclaimable victim.
    Arc::strong_count(selected_source) > 2
}

enum OwnerHotPinResult<T> {
    Pinned(Arc<T>),
    ReclaimBusy,
    Stale,
}

fn pin_current_owner_hot_source_from_index<T>(
    entry_put_id: crate::master_kv_router::put::PutIDForAKey,
    entry: &Weak<T>,
    resolve_current: impl FnOnce() -> Option<(crate::master_kv_router::put::PutIDForAKey, Arc<T>)>,
) -> OwnerHotPinResult<T> {
    let Some((current_put_id, current)) = resolve_current() else {
        // Do not upgrade the listener's Weak after the local index has
        // disappeared.  A reclaim Prepare may already own the sole Arc and
        // Commit relies on that ownership.  A still-live Weak therefore means
        // "retry after the transient owner transition"; a dead Weak is
        // definitively obsolete.
        return if entry.strong_count() == 0 {
            OwnerHotPinResult::Stale
        } else {
            OwnerHotPinResult::ReclaimBusy
        };
    };
    let Some(pinned) =
        clone_if_owner_hot_entry_matches(current_put_id, &current, entry_put_id, entry)
    else {
        return OwnerHotPinResult::Stale;
    };

    // `resolve_current` clones from the DashMap entry while its shard read
    // guard is held. Reclaim Prepare cannot remove that entry until the clone
    // exists, and its strong-count check will consequently return Busy. This
    // gives us the required per-key pin without the global owner-control lock.
    Some(pinned).map_or(OwnerHotPinResult::Stale, OwnerHotPinResult::Pinned)
}

fn pin_current_owner_hot_source(
    key: &str,
    entry: &OwnerHotCacheEntry,
    get_cached_info: &DashMap<String, GetCachedInfo>,
    counters: &OwnerHotCacheCounters,
) -> OwnerHotPinResult<MemoryInfo> {
    let result = pin_current_owner_hot_source_from_index(entry.put_id, &entry.memory_info, || {
        get_cached_info.get(key).map(|cached| {
            (
                (cached.put_time_ms, cached.put_version),
                cached.mem_holder.clone(),
            )
        })
    });
    match result {
        OwnerHotPinResult::Pinned(pinned) => OwnerHotPinResult::Pinned(pinned),
        OwnerHotPinResult::ReclaimBusy => {
            counters.skipped_reclaim.fetch_add(1, Ordering::Relaxed);
            OwnerHotPinResult::ReclaimBusy
        }
        OwnerHotPinResult::Stale => {
            counters.skipped_stale.fetch_add(1, Ordering::Relaxed);
            OwnerHotPinResult::Stale
        }
    }
}

fn build_owner_hot_cache(
    capacity_bytes: u64,
    counters: Arc<OwnerHotCacheCounters>,
    retry_queue: Arc<OwnerHotRetryQueue>,
    eviction_tx: tokio::sync::ampsc::UnboundedSender<OwnerHotEvictionDispatch>,
) -> OwnerHotCache {
    assert!(
        capacity_bytes > 0,
        "owner hot-cache capacity must be positive"
    );
    OwnerHotCache::builder(capacity_bytes)
        .weigher(|_key: &String, entry: &OwnerHotCacheEntry| entry.weight_bytes)
        .eviction_listener(move |key, entry, cause| {
            if cause != RemovalCause::Size {
                return;
            }
            counters.size_evictions.fetch_add(1, Ordering::Relaxed);
            let identity = OwnerHotReplicaIdentity {
                key: (*key).clone(),
                put_time_ms: entry.put_id.0,
                put_version: entry.put_id.1,
            };
            let selection_debt = OwnerHotSelectionDebt::new(
                u64::from(entry.weight_bytes),
                counters.selection_debt_bytes.clone(),
            );
            let event = OwnerHotEvictionEvent {
                key: identity.key.clone(),
                put_id: entry.put_id,
                memory_info: entry.memory_info.clone(),
                selection_debt,
                retry: false,
                source_eviction_victim: None,
                retry_failures: 0,
            };
            if let Err(err) = eviction_tx.send(OwnerHotEvictionDispatch::Victim(event)) {
                let OwnerHotEvictionDispatch::Victim(event) = err.0 else {
                    unreachable!("the Moka listener only sends victim events")
                };
                counters
                    .source_evict_dispatch_failed
                    .fetch_add(1, Ordering::Relaxed);
                retry_queue.schedule(event, "eviction dispatcher closed");
            }
        })
        .build()
}

struct ClientKvApiViewHolder {
    view: OnceLock<ClientKvApiView>,
}

impl ClientKvApiViewHolder {
    fn new() -> Self {
        Self {
            view: OnceLock::new(),
        }
    }

    fn attach(&self, view: ClientKvApiView) {
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("ClientKvApi view attached twice"));
    }

    fn clone_view(&self) -> ClientKvApiView {
        self.view.get().unwrap().clone()
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

impl std::ops::Deref for ClientKvApiViewHolder {
    type Target = ClientKvApiView;

    fn deref(&self) -> &Self::Target {
        self.view.get().unwrap()
    }
}

pub struct ClientKvApiInner {
    view: ClientKvApiViewHolder,
    test_spec_config: TestSpecConfig,
    owner_local_reserve_physical_capacity_bytes: u64,
    allocation_authority: crate::master_seg_manager::msg_pack::SegmentAllocationAuthority,
    owner_placement_class: crate::master_seg_manager::msg_pack::OwnerPlacementClass,
    ssd_storage: Option<Arc<KvSsdStorage>>,
    metrics: OnceLock<Arc<MetricsHandle>>,

    /// make sure each remote kv get run in order
    pub get_remote_kv_lock: AMapLock<String>,
    /// key -> value info on this node
    /// we can only remove value if it's put_time_ms and put_version match remote eviction command
    get_cached_info: Arc<DashMap<String, GetCachedInfo>>,
    /// key -> locally readable resident slot before backend put_start/put_done finishes.
    precommit_local_visible_info: DashMap<String, PrecommitLocalVisibleInfo>,
    /// Transferred Get targets awaiting an idempotent master GetDone result.
    /// The payload is already DataReady/RoutePending, so external local probes
    /// may reuse its exact `MemoryInfo` under the per-key fence.  It is still
    /// excluded from the committed local index and from Moka hot-touch until
    /// the master terminal promotes it.
    pending_local_get_info: DashMap<String, PendingLocalGetInfo>,
    /// key -> local replica version remembered from local put/get durable-replica success.
    /// This authority is positive-only: hit means "can answer exists=true immediately when
    /// allow_local_snapshot is enabled"; miss does not imply non-existence.
    local_snapshot_info: DashMap<String, LocalSnapshotInfo>,
    /// One allocator over the complete owner-managed DRAM segment.
    owner_segment_allocator: Mutex<OwnerSegmentAllocator>,
    /// Serialize claims only within one slot-size class. Tokio's mutex provides FIFO
    /// acquisition order for equal-size waiters without making an unrelated class wait
    /// behind a pressured class.
    owner_local_reserve_claim_locks: DashMap<u64, Arc<limit_thirdparty::tokio::sync::AMutex<()>>>,
    /// Wake the background reserve actor after demand/free-slot changes.
    owner_local_reserve_rebalance_notify: Arc<limit_thirdparty::tokio::sync::Notify>,
    /// Serializes bounded Moka pressure selection batches. It does not
    /// serialize per-key reclaim or unrelated transfers.
    owner_hot_selection_lock: limit_thirdparty::tokio::sync::AMutex<()>,
    /// Owner-local write-back put ids for external local-first path.
    external_local_first_put_id_counter: AtomicU32,
    /// Correlates idempotent owner source-eviction batches in logs and RPC responses.
    next_owner_source_eviction_operation_id: AtomicU64,
    /// Sharded per-key gate for local-first puts, local index access, and reclaim fencing.
    owner_key_control: Arc<OwnerKeyControlTable>,
    /// A weak-value admission/recency tier. It never owns resident memory and
    /// therefore cannot become a second physical-reclaim authority.
    owner_hot_cache: Option<OwnerHotCache>,
    /// Exact selected identities remain here until their backing is physically
    /// Free or the source is restored to owner-hot.
    owner_source_eviction_selected:
        Arc<DashMap<OwnerHotReplicaIdentity, Arc<OwnerHotSelectionDebt>>>,
    owner_hot_counters: Arc<OwnerHotCacheCounters>,
    owner_remote_put_counters: Arc<OwnerRemotePutCounters>,
    owner_remote_put_admission: Arc<OwnerRemotePutAdmission>,
    owner_local_ssd_put_counters: Arc<OwnerLocalSsdPutCounters>,
    planned_get_counters: OwnerPlannedGetCounters,
    ssd_stage_counters: OwnerSsdStageCounters,
    owner_hot_retry_queue: Arc<OwnerHotRetryQueue>,
    owner_hot_eviction_tx: tokio::sync::ampsc::UnboundedSender<OwnerHotEvictionDispatch>,
    owner_hot_eviction_rx:
        Mutex<Option<tokio::sync::ampsc::UnboundedReceiver<OwnerHotEvictionDispatch>>>,

    /// Shared delete actor input for owner -> external weak-index invalidation.
    pub external_invalidate_delete: EnsureMemholderMgmtDeleteHandle<DeleteClientKvMetaCacheItem>,
    /// Shared delete actor input for owner -> master delete-ack batching.
    pub delete_ack_batch: EnsureMemholderMgmtDeleteHandle<OwnerDeleteAckItem>,
    /// Shared manager for owner -> master delete-ack batching.
    pub owner_delete_ack_mgr: OwnerDeleteAckMemMgr,

    // record external_client get_holding info (owned, flattened manager)
    pub external_get_holding: OwnerExternalMemMgr,
    pub external_get_start_registry: DashMap<u64, ExternalGetStartEntry>,
    /// Metrics-only weak index of active per-key Get flights. Correctness
    /// remains in the sharded key-control table; observing metrics must never
    /// scan or hold those fences.
    external_get_flight_registry: DashMap<String, Weak<ExternalGetKeySharedOp>>,
    external_get_local_probe_locks: AMapLock<(String, i64, u64)>,
    completed_external_get_local_probes:
        moka::future::Cache<(String, i64, u64), (Vec<String>, ExternalBatchGetLocalProbeResp)>,
    planned_external_get_execute_locks: AMapLock<(String, i64, u64)>,
    completed_planned_external_get_executes:
        moka::future::Cache<(String, i64, u64), ExternalExecutePlannedGetResp>,
    /// Exact `get_id` SSD source operations. RPC retransmits and concurrent
    /// callers join one disk read plus one payload transfer.
    ssd_stage_flights: DashMap<u64, Arc<SsdStageSharedOp>>,
    /// Exact source-side Get operations. One leader performs the WRITE;
    /// followers and transport retries reuse the same terminal.
    owner_get_to_target_flights:
        DashMap<crate::owner_segment::OwnerTransferOpId, Arc<OwnerGetToTargetSharedOp>>,
    /// Exact target attempt -> one detached target-side READ/route terminal.
    owner_put_from_source_flights:
        DashMap<(crate::owner_segment::OwnerTransferOpId, u32), Arc<OwnerPutFromSourceSharedOp>>,
    /// DataReady target -> one detached metadata route task. Async callers
    /// return after this entry and its task guard exist; Sync callers await
    /// the same terminal.
    owner_target_route_commit_flights:
        DashMap<crate::owner_segment::OwnerTransferOpId, Arc<OwnerTargetRouteCommitSharedOp>>,
    /// Outbound terminal sequence/ACK state. ACKs are piggybacked on the next
    /// owner batch and never add a foreground control-plane round trip.
    owner_transfer_peer_tracker: OwnerTransferPeerTracker,
    /// Monotonic ACK watermark received from each caller generation/stream.
    /// It rejects late duplicate operations after their full terminal was GC'd.
    owner_transfer_inbound_ack_watermarks:
        DashMap<(crate::owner_segment::OwnerGeneration, u64), u64>,
    /// Short-lived terminal replay closes the active-map removal race and
    /// prevents a lost response from re-reading or re-transferring the value.
    completed_ssd_stages: moka::future::Cache<u64, CompletedSsdStage>,
    next_external_get_start_handle: AtomicU64,
    /// External holding identities are independent from upstream and resident holder ids.
    next_external_holding_id: AtomicU64,
    /// Weak handle to a shared refcount tracker for all UserMemHolder of this client.
    ///
    /// - A strong `Arc<AllMemholderRefCount>` is given to every `UserMemHolder` created by this client.
    /// - When the last `UserMemHolder` is dropped, the strong `Arc<AllMemholderRefCount>` is dropped too,
    ///   and this weak handle will no longer upgrade, meaning the client can be safely dropped.
    /// - Stored as `Weak` in `OnceLock` to avoid cycles and allow lazy initialization.
    pub all_memholder_refcount: OnceLock<Weak<AllMemholderRefCount>>,
    /// External API is implemented directly on ClientKvApi; no handler stored here

    #[cfg(test)]
    test_record: crate::client_kv_api::client_test_record::ClientTestRecord,

    rpc_caller_get_start: RPCCaller<GetStartReq>,
    rpc_caller_get_revoke: RPCCaller<GetRevokeReq>,
    rpc_caller_get_done: RPCCaller<GetDoneReq>,
    rpc_caller_batch_get_start: RPCCaller<BatchGetStartReq>,
    rpc_caller_batch_get_plan: RPCCaller<BatchGetPlanReq>,
    rpc_caller_batch_get_bind: RPCCaller<BatchGetBindReq>,
    rpc_caller_batch_get_revoke: RPCCaller<BatchGetRevokeReq>,
    rpc_caller_batch_get_done: RPCCaller<BatchGetDoneReq>,
    rpc_caller_put_start: RPCCaller<PutStartReq>,
    rpc_caller_put_revoke: RPCCaller<PutRevokeReq>,
    rpc_caller_put_done: RPCCaller<PutDoneReq>,
    rpc_caller_batch_put_start: RPCCaller<BatchPutStartReq>,
    rpc_caller_batch_put_revoke: RPCCaller<BatchPutRevokeReq>,
    rpc_caller_batch_put_done: RPCCaller<BatchPutDoneReq>,
    rpc_caller_grouped_batch_put_done: RPCCaller<GroupedBatchPutDoneReq>,
    rpc_caller_batch_prepare_put_keys: RPCCaller<BatchPreparePutKeysReq>,
    rpc_caller_batch_release_put_key_reservations: RPCCaller<BatchReleasePutKeyReservationsReq>,
    rpc_caller_put_append_start: RPCCaller<PutAppendStartReq>,
    rpc_caller_batch_put_append_start: RPCCaller<BatchPutAppendStartReq>,
    rpc_caller_put_append_revoke: RPCCaller<PutAppendRevokeReq>,
    rpc_caller_put_append_done: RPCCaller<PutAppendDoneReq>,
    rpc_caller_batch_put_append_done: RPCCaller<BatchPutAppendDoneReq>,
    rpc_caller_batch_evict_owner_source: RPCCaller<BatchEvictOwnerSourceReq>,
    rpc_caller_batch_publish_owner_ssd: RPCCaller<BatchPublishOwnerSsdReq>,
    rpc_caller_delete: RPCCaller<DeleteReq>,
    rpc_caller_batch_delete_ack: RPCCaller<BatchDeleteAckReq>,
    rpc_caller_batch_is_exist: RPCCaller<BatchIsExistReq>,
    rpc_caller_get_meta: RPCCaller<GetMetaReq>,
    rpc_caller_allocate_client_lease: RPCCaller<AllocateClientLeaseReq>,
    rpc_caller_client_lease_keepalive: RPCCaller<ClientLeaseKeepaliveReq>,
    rpc_caller_ssd_stage_begin: RPCCaller<SsdStageBeginReq>,
    rpc_caller_ssd_stage_done: RPCCaller<SsdStageDoneReq>,
    rpc_caller_external_put_commit: RPCCaller<ExternalPutCommitReq>,
    rpc_caller_external_put_revoke: RPCCaller<ExternalPutRevokeReq>,
    rpc_caller_owner_segment_transfer: RPCCaller<OwnerSegmentTransferReq>,
    rpc_caller_owner_capacity_report:
        RPCCaller<crate::master_seg_manager::msg_pack::OwnerCapacityReportReq>,
    owner_capacity_report_epoch: AtomicU64,

    /// Default lease id recorded for inspection/convenience, but NOT auto-applied.
    /// Callers must explicitly pass `Some(lease_id)` to attach a put to a lease.
    default_lease_id: parking_lot::RwLock<Option<u64>>,
    /// External put (remote target) pending context keyed by (key, put_time_ms, put_version).
    /// 注意：put_id (time_ms,version) 在不同 key 上并不全局唯一，因此必须携带 key 作为索引的一部分，避免碰撞。
    /// 使用 moka::sync::SegmentedCache 并设置 30 分钟 TTL，避免异常路径未清理导致的泄漏；不设置容量上限，纯 TTL 控制。
    external_pending_puts: moka::sync::SegmentedCache<(String, u64, u32), ExternalPendingPutCtx>,
    owner_local_publish_tx: tokio::sync::ampsc::Sender<OwnerLocalPublishJob>,
    owner_local_publish_rx: Mutex<Option<tokio::sync::ampsc::Receiver<OwnerLocalPublishJob>>>,
}

impl ClientKvApiInner {
    fn view(&self) -> &ClientKvApiView {
        &self.view
    }

    pub(crate) async fn persist_local_kvs_to_ssd(
        &self,
        sources: &[KvSsdPersistSource],
    ) -> KvResult<Vec<KvResult<Option<crate::kv_ssd_storage::KvSsdPersistGuard>>>> {
        let Some(store) = self.ssd_storage.as_ref() else {
            return Ok(sources.iter().map(|_| Ok(None)).collect());
        };
        let segment_guard = self.view.client_seg_pool().cpu_mem_read_guard().await?;
        for source in sources {
            if !segment_guard.contains_rw_or_ro(source.addr, source.len) {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "SSD persist source is outside the local segment: key={} put_id=({},{}) addr={:#x} len={}",
                        source.key, source.put_id.0, source.put_id.1, source.addr, source.len
                    ),
                }));
            }
        }
        let results = store.persist_batch_from_addrs(sources).await;
        drop(segment_guard);
        Ok(results)
    }

    pub(crate) async fn copy_local_kvs_for_ssd(
        &self,
        sources: &[KvSsdPersistSource],
    ) -> KvResult<Vec<KvResult<KvSsdPersistCopy>>> {
        if self.ssd_storage.is_none() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "local SSD storage is disabled".to_string(),
            }));
        }
        let segment_guard = self.view.client_seg_pool().cpu_mem_read_guard().await?;
        for source in sources {
            if !segment_guard.contains_rw_or_ro(source.addr, source.len) {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "SSD persist source is outside the local segment: key={} put_id=({},{}) addr={:#x} len={}",
                        source.key, source.put_id.0, source.put_id.1, source.addr, source.len
                    ),
                }));
            }
        }
        let copies = KvSsdStorage::copy_batch_from_addrs(sources);
        drop(segment_guard);
        Ok(copies)
    }

    pub(crate) async fn persist_copied_local_kvs_to_ssd(
        &self,
        permit: KvSsdPersistBatchPermit,
        copies: Vec<KvResult<KvSsdPersistCopy>>,
    ) -> KvResult<Vec<KvResult<Option<crate::kv_ssd_storage::KvSsdPersistGuard>>>> {
        let Some(store) = self.ssd_storage.as_ref() else {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "local SSD storage is disabled".to_string(),
            }));
        };
        Ok(store
            .persist_batch_from_copies_with_permit(permit, copies)
            .await)
    }

    pub(crate) fn try_acquire_local_ssd_persist_batch(
        &self,
        item_count: usize,
    ) -> KvResult<Option<KvSsdPersistBatchPermit>> {
        let Some(store) = self.ssd_storage.as_ref() else {
            return Ok(None);
        };
        store.try_acquire_persist_batch(item_count)
    }

    async fn discard_local_ssd_replica(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> bool {
        let Some(store) = self.ssd_storage.as_ref() else {
            return false;
        };
        store.remove_exact(key, put_id).await
    }

    async fn begin_ssd_stage(&self, get_id: u64) -> KvResult<bool> {
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let response = call_control_plane_rpc(
            &self.rpc_caller_ssd_stage_begin,
            self.view.p2p_module(),
            master_node_id.into(),
            MsgPack {
                serialize_part: SsdStageBeginReq { get_id },
                raw_bytes: Vec::new(),
            },
            Some(SSD_STAGE_RPC_TIMEOUT),
            2,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            response.serialize_part.error_code,
            response.serialize_part.error_json,
        )?;
        Ok(response.serialize_part.started)
    }

    async fn finish_ssd_stage_once(&self, get_id: u64, drop_ssd_source: bool) -> KvResult<()> {
        let master_node_id = self
            .view
            .cluster_manager()
            .find_or_wait_master_node()
            .await?;
        let response = call_control_plane_rpc(
            &self.rpc_caller_ssd_stage_done,
            self.view.p2p_module(),
            master_node_id.into(),
            MsgPack {
                serialize_part: SsdStageDoneReq {
                    get_id,
                    drop_ssd_source,
                },
                raw_bytes: Vec::new(),
            },
            Some(SSD_STAGE_RPC_TIMEOUT),
            2,
        )
        .await
        .map_err(KvError::from)?;
        crate::rpcresp_kvresult_convert::try_from_code(
            response.serialize_part.error_code,
            response.serialize_part.error_json,
        )
    }

    async fn finish_ssd_stage_until_acked(&self, get_id: u64, drop_ssd_source: bool) {
        let mut attempt = 0_u64;
        let mut backoff = SSD_STAGE_DONE_RETRY_INITIAL_BACKOFF;
        loop {
            attempt = attempt.saturating_add(1);
            match self.finish_ssd_stage_once(get_id, drop_ssd_source).await {
                Ok(()) => return,
                Err(err) => {
                    if attempt == 1 || attempt.is_power_of_two() {
                        tracing::warn!(
                            get_id,
                            drop_ssd_source,
                            attempt,
                            retry_delay_ms = backoff.as_millis(),
                            error = %err,
                            "SSD StageDone failed; retaining the source flight and retrying"
                        );
                    }
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = backoff
                .saturating_mul(2)
                .min(SSD_STAGE_DONE_RETRY_MAX_BACKOFF);
        }
    }

    fn finish_ssd_stage_detached(&self, get_id: u64) {
        self.ssd_stage_counters
            .done_detached
            .fetch_add(1, Ordering::Relaxed);
        let spawn_view = self.view.clone_view();
        let task_view = spawn_view.clone();
        spawn_view.spawn("ssd_stage_done_retry", async move {
            task_view
                .client_kv_api()
                .inner()
                .finish_ssd_stage_until_acked(get_id, false)
                .await;
        });
    }

    async fn run_ssd_stage_once(&self, req: &SsdStageReadReq) -> SsdStageReadResp {
        self.ssd_stage_counters
            .ready_requests
            .fetch_add(1, Ordering::Relaxed);
        let ready_started_at = Instant::now();
        match self.begin_ssd_stage(req.get_id).await {
            Ok(true) => {}
            Ok(false) => {
                let response = ssd_stage_error_response(KvError::Api(ApiError::InvalidArgument {
                    detail: format!("SSD stage is not startable: get_id={}", req.get_id),
                }));
                self.ssd_stage_counters
                    .ready_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.ssd_stage_counters.ready_duration_us.fetch_add(
                    u64::try_from(ready_started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                return response;
            }
            Err(err) => {
                let response = ssd_stage_error_response(err);
                self.ssd_stage_counters
                    .ready_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.ssd_stage_counters.ready_duration_us.fetch_add(
                    u64::try_from(ready_started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                return response;
            }
        }

        let load_result = async {
            let Some(store) = self.ssd_storage.as_ref() else {
                return Err(KvError::Api(ApiError::KeyNotFound {
                    key: req.key.clone(),
                }));
            };
            let segment_guard = self.view.client_seg_pool().cpu_mem_read_guard().await?;
            if !segment_guard.contains_rw(req.stage_addr, req.stage_capacity) {
                return Err(KvError::Api(ApiError::InvalidArgument {
                    detail: format!(
                        "SSD stage is outside the local writable segment: get_id={} addr={:#x} capacity={}",
                        req.get_id, req.stage_addr, req.stage_capacity
                    ),
                }));
            }
            store
                .load_into_addr(
                    &req.key,
                    req.put_id,
                    req.stage_addr,
                    req.len,
                    req.stage_capacity,
                )
                .await?;
            drop(segment_guard);
            Ok(())
        }
        .await;

        let response = match load_result {
            Ok(()) => SsdStageReadResp {
                error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
                error_json: String::new(),
            },
            Err(load_err) => {
                let stale_ssd_source =
                    matches!(&load_err, KvError::Api(ApiError::KeyNotFound { .. }));
                // A failed load never becomes pull-ready. The source owner is
                // therefore the only side that can safely close the stage. A
                // true miss also removes the exact stale SSD route.
                self.finish_ssd_stage_until_acked(req.get_id, stale_ssd_source)
                    .await;
                ssd_stage_error_response(load_err)
            }
        };
        self.ssd_stage_counters.ready_duration_us.fetch_add(
            u64::try_from(ready_started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if response.error_code == crate::rpcresp_kvresult_convert::msg_and_error::OK {
            self.ssd_stage_counters
                .ready_successes
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.ssd_stage_counters
                .ready_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        response
    }

    async fn execute_ssd_stage(&self, req: &SsdStageReadReq) -> SsdStageReadResp {
        if let Some(completed) = self.completed_ssd_stages.get(&req.get_id).await {
            return if completed.request == *req {
                completed.response
            } else {
                ssd_stage_request_mismatch_response(&completed.request, req)
            };
        }

        let (op, is_leader) = match self.ssd_stage_flights.entry(req.get_id) {
            DashMapEntry::Occupied(entry) => (entry.get().clone(), false),
            DashMapEntry::Vacant(entry) => {
                let op = SsdStageSharedOp::new(req.clone());
                entry.insert(op.clone());
                (op, true)
            }
        };
        if op.request != *req {
            return ssd_stage_request_mismatch_response(&op.request, req);
        }

        if is_leader {
            let spawn_view = self.view.clone_view();
            let task_view = spawn_view.clone();
            let task_op = op.clone();
            spawn_view.spawn("ssd_stage_singleflight", async move {
                let inner = task_view.client_kv_api().inner();
                let response = inner.run_ssd_stage_once(&task_op.request).await;
                inner
                    .ssd_stage_counters
                    .execute_completions
                    .fetch_add(1, Ordering::Relaxed);
                assert!(
                    task_op.complete(response.clone()),
                    "one SSD stage flight must publish exactly one terminal result"
                );
                inner
                    .ssd_stage_counters
                    .terminal_published
                    .fetch_add(1, Ordering::Relaxed);

                // Wake the foreground RPC before maintaining the replay cache.
                // The completed flight remains indexed until the cache insert
                // finishes, so retransmits cannot start a second disk read.
                let cache_started_at = Instant::now();
                inner
                    .completed_ssd_stages
                    .insert(
                        task_op.request.get_id,
                        CompletedSsdStage {
                            request: task_op.request.clone(),
                            response: response.clone(),
                        },
                    )
                    .await;
                inner
                    .ssd_stage_counters
                    .terminal_cache_duration_us
                    .fetch_add(
                        u64::try_from(cache_started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
                        Ordering::Relaxed,
                    );
                inner
                    .ssd_stage_counters
                    .terminal_cache_inserts
                    .fetch_add(1, Ordering::Relaxed);
                inner
                    .ssd_stage_flights
                    .remove_if(&task_op.request.get_id, |_, current| {
                        Arc::ptr_eq(current, &task_op)
                    });
            });
        }
        op.wait().await
    }

    pub(crate) fn kv_ssd_storage_usage_snapshot(
        &self,
    ) -> Option<crate::kv_ssd_storage::KvSsdStorageDeviceUsage> {
        self.ssd_storage
            .as_ref()
            .map(|store| store.usage_snapshot())
    }

    pub(crate) fn track_external_get_flight(&self, op: &Arc<ExternalGetKeySharedOp>) {
        self.external_get_flight_registry
            .insert(op.key.clone(), Arc::downgrade(op));
    }

    pub(crate) fn untrack_external_get_flight(&self, op: &Arc<ExternalGetKeySharedOp>) {
        let weak = Arc::downgrade(op);
        self.external_get_flight_registry
            .remove_if(&op.key, |_, current| Weak::ptr_eq(current, &weak));
    }

    fn external_get_flight_snapshot(&self) -> Vec<Arc<ExternalGetKeySharedOp>> {
        let mut ops = Vec::new();
        let mut stale = Vec::new();
        for entry in &self.external_get_flight_registry {
            if let Some(op) = entry.value().upgrade() {
                ops.push(op);
            } else {
                stale.push(entry.key().clone());
            }
        }
        for key in stale {
            self.external_get_flight_registry
                .remove_if(&key, |_, weak| weak.strong_count() == 0);
        }
        ops
    }

    pub(crate) fn owner_hot_prepare_eviction(
        &self,
        event: &OwnerHotEvictionEvent,
    ) -> OwnerHotEvictionPreparation {
        let trigger = OwnerHotReplicaIdentity {
            key: event.key.clone(),
            put_time_ms: event.put_id.0,
            put_version: event.put_id.1,
        };
        let cache_entry = OwnerHotCacheEntry {
            put_id: event.put_id,
            memory_info: event.memory_info.clone(),
            weight_bytes: 0,
        };
        let memory_info = match pin_current_owner_hot_source(
            event.key.as_str(),
            &cache_entry,
            self.get_cached_info.as_ref(),
            self.owner_hot_counters.as_ref(),
        ) {
            OwnerHotPinResult::Pinned(memory_info) => memory_info,
            OwnerHotPinResult::ReclaimBusy => {
                return OwnerHotEvictionPreparation::RetryableReclaimFence;
            }
            OwnerHotPinResult::Stale => return OwnerHotEvictionPreparation::Obsolete,
        };

        if owner_hot_source_has_active_holders(&memory_info) {
            OwnerHotEvictionPreparation::TemporarilyPinned
        } else {
            OwnerHotEvictionPreparation::Ready {
                trigger,
                source: memory_info,
            }
        }
    }

    pub(crate) fn owner_hot_restore_source_selection(
        &self,
        identity: &OwnerHotReplicaIdentity,
    ) -> bool {
        let mut controls = self.owner_key_control.lock_key(&identity.key);
        let Some(state) = controls.get_mut(&identity.key) else {
            return false;
        };
        let matches = state
            .source_eviction_selection
            .as_ref()
            .is_some_and(|selection| {
                selection.put_id == (identity.put_time_ms, identity.put_version)
            });
        if !matches {
            return false;
        }
        let selection = state
            .source_eviction_selection
            .take()
            .expect("matching owner source selection must exist");
        let replaced = self
            .get_cached_info
            .insert(identity.key.clone(), selection.cached_info);
        assert!(
            replaced.is_none(),
            "rolling back an owner source selection must restore an empty local index"
        );
        state.finish_local_access_fence();
        if state.is_idle() {
            controls.remove(&identity.key);
        }
        true
    }

    fn owner_hot_install_source_selection_debt(
        &self,
        identity: OwnerHotReplicaIdentity,
        debt: Arc<OwnerHotSelectionDebt>,
    ) -> bool {
        match self.owner_source_eviction_selected.entry(identity) {
            DashMapEntry::Vacant(entry) => {
                entry.insert(debt.clone());
                self.owner_hot_counters
                    .add_source_eviction_selected_bytes(debt.weight_bytes);
                true
            }
            DashMapEntry::Occupied(_) => false,
        }
    }

    fn owner_hot_remove_source_selection_debt(
        &self,
        identity: &OwnerHotReplicaIdentity,
    ) -> Option<Arc<OwnerHotSelectionDebt>> {
        let debt = self
            .owner_source_eviction_selected
            .remove(identity)
            .map(|(_, debt)| debt)?;
        self.owner_hot_counters
            .remove_source_eviction_selected_bytes(debt.weight_bytes);
        Some(debt)
    }

    pub(crate) fn owner_hot_install_source_selection_fence(
        &self,
        identity: &OwnerHotReplicaIdentity,
        source: &Arc<MemoryInfo>,
    ) -> OwnerHotSelectionFenceOutcome {
        let mut controls = self.owner_key_control.lock_key(&identity.key);
        let control_busy = controls.get(&identity.key).is_some_and(|state| {
            state.local_puts != 0
                || state.external_pending_puts != 0
                || state.remote_put.is_some()
                || state.local_ssd_put.is_some()
                || state.external_get.is_some()
                || state.local_access_fenced()
        });
        if control_busy
            || self
                .precommit_local_visible_info
                .contains_key(&identity.key)
            || self.pending_local_get_info.contains_key(&identity.key)
        {
            return OwnerHotSelectionFenceOutcome::Retryable;
        }

        let cached_info = self
            .get_cached_info
            .remove_if(&identity.key, |_, cached| {
                (cached.put_time_ms, cached.put_version)
                    == (identity.put_time_ms, identity.put_version)
                    && Arc::ptr_eq(&cached.mem_holder, source)
            })
            .map(|(_, cached)| cached);
        let Some(cached_info) = cached_info else {
            return OwnerHotSelectionFenceOutcome::Obsolete;
        };

        let state = controls.entry(identity.key.clone()).or_default();
        assert!(state.source_eviction_selection.is_none());
        assert!(state.reclaim.is_none());
        state.begin_local_access_fence();
        state.source_eviction_selection = Some(OwnerSourceEvictionSelection {
            put_id: (identity.put_time_ms, identity.put_version),
            cached_info,
        });
        drop(controls);

        // The index Arc moved into source_eviction_selection. The temporary
        // source Arc is the second expected reference; any extra Arc is an
        // active reader or transfer that arrived before the fence was installed.
        if owner_hot_source_has_active_holders(source) {
            assert!(
                self.owner_hot_restore_source_selection(identity),
                "a pinned single-key victim must restore its source fence"
            );
            OwnerHotSelectionFenceOutcome::TemporarilyPinned
        } else {
            OwnerHotSelectionFenceOutcome::Fenced
        }
    }

    fn owner_hot_track_committed(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: &Arc<MemoryInfo>,
    ) -> bool {
        let Some(cache) = self.owner_hot_cache.as_ref() else {
            return false;
        };
        if !self.owner_hot_source_is_current(key, put_id, memory_info) {
            return false;
        }
        cache.insert(
            key.to_string(),
            [OwnerHotPinAlias::new(memory_info)],
            OwnerHotCacheEntry {
                put_id,
                memory_info: Arc::downgrade(memory_info),
                weight_bytes: owner_hot_weight_bytes(memory_info.as_ref()),
            },
        );
        if !self.owner_hot_source_is_current(key, put_id, memory_info) {
            self.owner_hot_invalidate_version(key, put_id);
            return false;
        }
        true
    }

    pub(crate) fn owner_hot_admit_published_committed(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> bool {
        let memory_info = self.get_cached_info.get(key).and_then(|cached| {
            ((cached.put_time_ms, cached.put_version) == put_id).then(|| cached.mem_holder.clone())
        });
        let Some(memory_info) = memory_info else {
            return false;
        };
        self.owner_hot_track_committed(key, put_id, &memory_info)
    }

    fn owner_hot_touch_or_promote(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: &Arc<MemoryInfo>,
    ) {
        let Some(cache) = self.owner_hot_cache.as_ref() else {
            return;
        };
        if cache.get(&key.to_string()).is_some_and(|entry| {
            entry.put_id == put_id && Weak::ptr_eq(&entry.memory_info, &Arc::downgrade(memory_info))
        }) {
            return;
        }
        let _ = self.owner_hot_track_committed(key, put_id, memory_info);
    }

    pub(crate) fn owner_hot_source_is_current(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: &Arc<MemoryInfo>,
    ) -> bool {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            return false;
        }
        self.get_cached_info.get(key).is_some_and(|cached| {
            cached.put_time_ms == put_id.0
                && cached.put_version == put_id.1
                && Arc::ptr_eq(&cached.mem_holder, memory_info)
        })
    }

    pub(crate) fn owner_hot_invalidate_version(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) {
        let identity = OwnerHotReplicaIdentity {
            key: key.to_string(),
            put_time_ms: put_id.0,
            put_version: put_id.1,
        };
        if let Some(cache) = self.owner_hot_cache.as_ref() {
            cache.invalidate_if(&key.to_string(), |entry| entry.put_id == put_id);
        }
        if let Some(debt) = self.owner_hot_remove_source_selection_debt(&identity) {
            debt.release();
            self.owner_hot_counters
                .source_evict_committed_members
                .fetch_add(1, Ordering::Relaxed);
        }
        self.owner_hot_retry_queue.remove(&identity);
    }

    pub(crate) fn release_local_reserve_route_for_memory_info(&self, memory_info: &MemoryInfo) {
        let Some((allocation_id, segment_offset, capacity_bytes)) =
            memory_info.local_reserve_resident_slot_ref()
        else {
            return;
        };
        if let Err(err) = self.owner_release_local_reserve_committed_slot_route(
            allocation_id,
            segment_offset,
            capacity_bytes,
        ) {
            tracing::warn!(
                "failed to release owner committed slot route: key={} allocation_id={} segment_offset={} capacity_bytes={} err={}",
                memory_info.key,
                allocation_id,
                segment_offset,
                capacity_bytes,
                err
            );
        }
    }

    pub(crate) fn owner_hot_pin_memory_info(
        &self,
        memory_info: &Arc<MemoryInfo>,
    ) -> Option<PinGuard> {
        self.owner_hot_cache
            .as_ref()?
            .try_pin_alias(OwnerHotPinAlias::new(memory_info))
    }

    pub(crate) fn short_circuit_put_payload_path_enabled(&self) -> bool {
        self.test_spec_config.short_circuit_put_payload_path
    }

    pub(crate) fn skip_put_end_commit_enabled(&self) -> bool {
        self.test_spec_config.skip_put_end_commit
    }

    pub(crate) fn next_external_local_first_put_id(
        &self,
    ) -> crate::master_kv_router::put::PutIDForAKey {
        (
            now_unix_ms(),
            self.external_local_first_put_id_counter
                .fetch_add(1, Ordering::Relaxed),
        )
    }

    pub fn next_owner_local_first_put_id(&self) -> crate::master_kv_router::put::PutIDForAKey {
        self.next_external_local_first_put_id()
    }

    pub async fn enqueue_owner_local_publish(&self, job: OwnerLocalPublishJob) -> KvResult<()> {
        let key_count = job.items.len();
        let first_key = job
            .items
            .first()
            .map(|item| item.key.as_str())
            .unwrap_or("<empty>")
            .to_string();
        self.owner_local_publish_tx.try_send(job).map_err(|err| {
            KvError::Api(ApiError::Unknown {
                detail: format!(
                    "owner local publish queue is full or closed: first_key={} key_count={} err={}",
                    first_key, key_count, err
                ),
            })
        })
    }

    pub(crate) fn reserve_owner_key_materialization(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> OwnerKeyMaterializationReservation {
        let mut controls = self.owner_key_control.lock_key(key);
        if let Some(state) = controls.get(key) {
            if state.local_access_fenced() {
                return OwnerKeyMaterializationReservation::WaitForLocalAccess(
                    state.subscribe_local_access_fence(),
                );
            }
            if let Some(op) = state.owner_materialization.clone() {
                if op.put_id != put_id {
                    tracing::debug!(
                        key,
                        active_put_time_ms = op.put_id.0,
                        active_put_version = op.put_id.1,
                        waiting_put_time_ms = put_id.0,
                        waiting_put_version = put_id.1,
                        "serializing different key generations behind one owner materialization"
                    );
                }
                return OwnerKeyMaterializationReservation::Wait(op);
            }
            if let Some(op) = state.external_get.clone() {
                return OwnerKeyMaterializationReservation::WaitForGet(op);
            }
            if let Some(op) = state.external_put.clone() {
                return OwnerKeyMaterializationReservation::WaitForExternalPut(op);
            }
        }
        let op = OwnerKeyMaterializationOp::new(key, put_id);
        let state = controls.entry(key.to_string()).or_default();
        assert!(state.owner_materialization.replace(op.clone()).is_none());
        OwnerKeyMaterializationReservation::Leader(op)
    }

    /// Extend a Get's existing per-key admission fence through its detached
    /// Async CommitTarget task.  If another materialization somehow already
    /// exists, the caller must keep this Get on the synchronous Done path;
    /// silently joining after it already transferred would retain two slots.
    pub(crate) fn begin_async_get_key_materialization(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> Option<OwnerKeyMaterializationGuard> {
        let mut controls = self.owner_key_control.lock_key(key);
        let state = controls.entry(key.to_string()).or_default();
        if state.owner_materialization.is_some() || state.external_put.is_some() {
            return None;
        }
        let op = OwnerKeyMaterializationOp::new(key, put_id);
        state.owner_materialization = Some(op.clone());
        Some(OwnerKeyMaterializationGuard::new(
            self.view.clone_view(),
            op,
        ))
    }

    pub(crate) fn finish_owner_key_materialization(
        &self,
        op: &Arc<OwnerKeyMaterializationOp>,
        outcome: OwnerKeyMaterializationOutcome,
    ) -> bool {
        if !op.complete(outcome) {
            return false;
        }
        let mut controls = self.owner_key_control.lock_key(&op.key);
        let remove_control = if let Some(state) = controls.get_mut(&op.key) {
            if state
                .owner_materialization
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, op))
            {
                state.owner_materialization = None;
            }
            state.is_idle()
        } else {
            false
        };
        if remove_control {
            controls.remove(&op.key);
        }
        true
    }

    pub(crate) fn reserve_external_local_first_put_key(
        &self,
        key: &str,
        reject_if_inflight_same_key: bool,
        reject_if_exist_same_key: bool,
    ) -> KvResult<ExternalLocalFirstPutKeyReservation> {
        let mut controls = self.owner_key_control.lock_key(key);
        let reusable_singleflight = reject_if_inflight_same_key && reject_if_exist_same_key;
        if let Some(state) = controls.get(key)
            && state.local_access_fenced()
        {
            return if reusable_singleflight {
                Ok(ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(
                    state.subscribe_local_access_fence(),
                ))
            } else {
                Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }))
            };
        }
        if reject_if_exist_same_key
            && (self.precommit_local_visible_info.contains_key(key)
                || self.pending_local_get_info.contains_key(key)
                || self.get_cached_info.contains_key(key)
                || self.local_snapshot_info.contains_key(key))
        {
            return Err(KvError::Api(ApiError::KeyAlreadyExists {
                key: key.to_string(),
            }));
        }
        let state = controls.entry(key.to_string()).or_default();
        if let Some(op) = state.owner_materialization.clone() {
            return if reusable_singleflight {
                Ok(ExternalLocalFirstPutKeyReservation::WaitForMaterialization(
                    op,
                ))
            } else {
                Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }))
            };
        }
        if let Some(op) = state.external_get.clone() {
            return if reusable_singleflight {
                Ok(ExternalLocalFirstPutKeyReservation::WaitForGet(op))
            } else {
                Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }))
            };
        }
        if reject_if_inflight_same_key && state.local_puts > 0 {
            return (reusable_singleflight)
                .then(|| state.external_put.clone())
                .flatten()
                .map_or_else(
                    || {
                        Err(KvError::Api(ApiError::KeyBeingWritten {
                            key: key.to_string(),
                        }))
                    },
                    |op| Ok(ExternalLocalFirstPutKeyReservation::Wait(op)),
                );
        }
        let local_put_op = reusable_singleflight.then(ExternalPutKeySharedOp::new);
        if let Some(op) = local_put_op.as_ref() {
            assert!(
                state.external_put.replace(op.clone()).is_none(),
                "a reject-on-inflight Put leader must own an empty shared-op slot"
            );
        }
        state.local_puts = state
            .local_puts
            .checked_add(1)
            .expect("owner local-first put counter overflow");
        state.external_pending_puts = state
            .external_pending_puts
            .checked_add(1)
            .expect("external pending Put fence counter overflow");
        Ok(ExternalLocalFirstPutKeyReservation::Leader(Arc::new(
            ExternalPendingPutFenceGuard {
                key: key.to_string(),
                owner_key_control: self.owner_key_control.clone(),
                owns_local_put: true,
                local_put_op,
                local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
                local_slot_cleanup_view: Some(self.view.clone_view()),
                local_slot_lease: Mutex::new(None),
                local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
            },
        )))
    }

    pub(crate) fn acquire_external_pending_put_fence(
        &self,
        key: &str,
    ) -> KvResult<Arc<ExternalPendingPutFenceGuard>> {
        acquire_external_pending_put_fence_for_key(&self.owner_key_control, key)
    }

    pub(crate) fn remember_local_snapshot(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            tracing::debug!(
                "skip local snapshot publication behind owner reclaim fence: key={} put_id=({},{})",
                key,
                put_id.0,
                put_id.1
            );
            return;
        }
        self.local_snapshot_info.insert(
            key.to_string(),
            LocalSnapshotInfo {
                put_time_ms: put_id.0,
                put_version: put_id.1,
            },
        );
    }

    pub(crate) fn has_local_snapshot(&self, key: &str) -> bool {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            return false;
        }
        self.precommit_local_visible_info.contains_key(key)
            || self.get_cached_info.contains_key(key)
            || self.local_snapshot_info.contains_key(key)
    }

    pub(crate) fn local_visible_mem_holder(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        let (memory_info, hot_put_id) = {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                return None;
            }
            let memory_info = self.local_visible_mem_holder_unfenced(key);
            let hot_put_id = memory_info.as_ref().and_then(|memory_info| {
                self.get_cached_info
                    .get(key)
                    .filter(|cached| Arc::ptr_eq(&cached.mem_holder, memory_info))
                    .map(|cached| (cached.put_time_ms, cached.put_version))
            });
            (memory_info, hot_put_id)
        };
        if let Some(put_id) = hot_put_id {
            self.owner_hot_touch_or_promote(
                key,
                put_id,
                memory_info
                    .as_ref()
                    .expect("hot touch requires a local memory holder"),
            );
        }
        memory_info
    }

    pub(crate) async fn local_visible_mem_holder_waiting(
        &self,
        key: &str,
    ) -> Option<Arc<MemoryInfo>> {
        self.local_visible_mem_holder(key)
    }

    pub(crate) async fn local_committed_mem_holder_for_put_id(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> Option<Arc<MemoryInfo>> {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            return None;
        }
        self.get_cached_info.get(key).and_then(|info| {
            (info.put_time_ms == put_id.0 && info.put_version == put_id.1)
                .then(|| info.mem_holder.clone())
        })
    }

    pub(crate) fn begin_owner_remote_put(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        preferred_sub_cluster: Option<String>,
        protect_source_on_remote_complete: bool,
    ) -> OwnerRemotePutReservation {
        let mut controls = self.owner_key_control.lock_key(key);

        if let Some(existing) = controls.get(key).and_then(|state| state.remote_put.clone()) {
            if existing.put_id == put_id && existing.outcome() == OwnerRemotePutOutcome::InFlight {
                existing.merge_request(preferred_sub_cluster, protect_source_on_remote_complete);
                self.owner_remote_put_counters
                    .followers
                    .fetch_add(1, Ordering::Relaxed);
                return OwnerRemotePutReservation::Follower(existing);
            }
            if existing.outcome() != OwnerRemotePutOutcome::InFlight {
                let state = controls
                    .get_mut(key)
                    .expect("terminal remote Put flight control state disappeared");
                if state
                    .remote_put
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &existing))
                {
                    state.remote_put = None;
                }
            }
            // A newer local generation may publish before the old remote task
            // observes Obsolete.  The new generation may replace the visible
            // per-key flight slot after its exact source is verified below;
            // the displaced task keeps its own holder and its pointer-checked
            // completion cannot clear the replacement.
        }

        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
        {
            self.owner_remote_put_counters
                .source_unavailable
                .fetch_add(1, Ordering::Relaxed);
            self.owner_remote_put_counters
                .source_fenced
                .fetch_add(1, Ordering::Relaxed);
            return OwnerRemotePutReservation::SourceUnavailable;
        }
        let memory_info = match self.get_cached_info.get(key) {
            Some(info) if (info.put_time_ms, info.put_version) == put_id => info.mem_holder.clone(),
            Some(_) => {
                self.owner_remote_put_counters
                    .source_unavailable
                    .fetch_add(1, Ordering::Relaxed);
                self.owner_remote_put_counters
                    .source_version_mismatch
                    .fetch_add(1, Ordering::Relaxed);
                return OwnerRemotePutReservation::SourceUnavailable;
            }
            None => {
                self.owner_remote_put_counters
                    .source_unavailable
                    .fetch_add(1, Ordering::Relaxed);
                self.owner_remote_put_counters
                    .source_missing
                    .fetch_add(1, Ordering::Relaxed);
                return OwnerRemotePutReservation::SourceUnavailable;
            }
        };
        let Some(admission_permit) = self
            .owner_remote_put_admission
            .try_acquire(u64::from(memory_info.len))
        else {
            return OwnerRemotePutReservation::NotAdmitted;
        };

        let op = OwnerRemotePutSharedOp::new(
            key,
            put_id,
            preferred_sub_cluster,
            protect_source_on_remote_complete,
        );
        let state = controls.entry(key.to_string()).or_default();
        state.install_remote_put_leader(op.clone());
        self.owner_remote_put_counters
            .active
            .fetch_add(1, Ordering::Relaxed);
        self.owner_remote_put_counters
            .leaders
            .fetch_add(1, Ordering::Relaxed);
        OwnerRemotePutReservation::Leader {
            op,
            memory_info,
            admission_permit,
        }
    }

    pub(crate) fn finish_owner_remote_put(
        &self,
        op: &Arc<OwnerRemotePutSharedOp>,
        outcome: OwnerRemotePutOutcome,
    ) -> bool {
        if !op.complete(outcome) {
            return false;
        }

        let mut controls = self.owner_key_control.lock_key(&op.key);
        let remove_control = if let Some(state) = controls.get_mut(&op.key) {
            if state
                .remote_put
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, op))
            {
                state.remote_put = None;
            }
            state.is_idle()
        } else {
            false
        };
        if remove_control {
            controls.remove(&op.key);
        }
        drop(controls);

        self.owner_remote_put_counters
            .active
            .fetch_sub(1, Ordering::Relaxed);
        let terminal_counter = match outcome {
            OwnerRemotePutOutcome::InFlight => {
                unreachable!("remote Put cannot finish with an inflight outcome")
            }
            OwnerRemotePutOutcome::Published => &self.owner_remote_put_counters.published,
            OwnerRemotePutOutcome::AlreadySatisfied => {
                &self.owner_remote_put_counters.already_satisfied
            }
            OwnerRemotePutOutcome::Obsolete => &self.owner_remote_put_counters.obsolete,
            OwnerRemotePutOutcome::Failed => &self.owner_remote_put_counters.failed,
        };
        terminal_counter.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(crate) fn begin_owner_local_ssd_put(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        selected_victim: Option<&crate::master_kv_router::msg_pack::OwnerSourceEvictionVictim>,
    ) -> OwnerLocalSsdPutReservation {
        if self.ssd_storage.is_none() {
            return OwnerLocalSsdPutReservation::SourceUnavailable;
        }
        let mut controls = self.owner_key_control.lock_key(key);

        if let Some(existing) = controls
            .get(key)
            .and_then(|state| state.local_ssd_put.clone())
        {
            if existing.put_id == put_id && existing.outcome().is_none() {
                self.owner_local_ssd_put_counters
                    .followers
                    .fetch_add(1, Ordering::Relaxed);
                return OwnerLocalSsdPutReservation::Follower(existing);
            }
            if existing.outcome().is_some() {
                let state = controls
                    .get_mut(key)
                    .expect("terminal local SSD Put control state disappeared");
                if state
                    .local_ssd_put
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &existing))
                {
                    state.local_ssd_put = None;
                }
            }
        }

        let memory_info = if let Some(victim) = selected_victim {
            let selected = controls
                .get(key)
                .and_then(|state| state.source_eviction_selection.as_ref())
                .filter(|selection| selection.put_id == put_id)
                .map(|selection| selection.cached_info.mem_holder.clone());
            selected.filter(|source| {
                source.local_reserve_resident_slot_ref().is_some_and(
                    |(allocation_id, segment_offset, capacity_bytes)| {
                        matches!(
                            &victim.backing,
                            crate::master_kv_router::msg_pack::OwnerReclaimBacking::CommittedSlot {
                                allocation_id: expected_allocation_id,
                                segment_offset: expected_segment_offset,
                                capacity_bytes: expected_capacity_bytes,
                            } if *expected_allocation_id == allocation_id
                                && *expected_segment_offset == segment_offset
                                && *expected_capacity_bytes == capacity_bytes
                        )
                    },
                )
            })
        } else {
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                None
            } else {
                self.get_cached_info.get(key).and_then(|info| {
                    ((info.put_time_ms, info.put_version) == put_id)
                        .then(|| info.mem_holder.clone())
                })
            }
        };
        let Some(memory_info) = memory_info else {
            self.owner_local_ssd_put_counters
                .source_unavailable
                .fetch_add(1, Ordering::Relaxed);
            return OwnerLocalSsdPutReservation::SourceUnavailable;
        };

        let op = OwnerLocalSsdPutSharedOp::new(key, put_id);
        controls
            .entry(key.to_string())
            .or_default()
            .install_local_ssd_put_leader(op.clone());
        self.owner_local_ssd_put_counters
            .active
            .fetch_add(1, Ordering::Relaxed);
        self.owner_local_ssd_put_counters
            .leaders
            .fetch_add(1, Ordering::Relaxed);
        OwnerLocalSsdPutReservation::Leader { op, memory_info }
    }

    pub(crate) fn finish_owner_local_ssd_put(
        &self,
        op: &Arc<OwnerLocalSsdPutSharedOp>,
        outcome: OwnerLocalSsdPutOutcome,
    ) -> bool {
        if !op.complete(outcome) {
            return false;
        }

        let mut controls = self.owner_key_control.lock_key(&op.key);
        let remove_control = if let Some(state) = controls.get_mut(&op.key) {
            if state
                .local_ssd_put
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, op))
            {
                state.local_ssd_put = None;
            }
            state.is_idle()
        } else {
            false
        };
        if remove_control {
            controls.remove(&op.key);
        }
        drop(controls);

        self.owner_local_ssd_put_counters
            .active
            .fetch_sub(1, Ordering::Relaxed);
        let terminal_counter = match outcome {
            OwnerLocalSsdPutOutcome::Published => &self.owner_local_ssd_put_counters.published,
            OwnerLocalSsdPutOutcome::AlreadyPresent => {
                &self.owner_local_ssd_put_counters.already_present
            }
            OwnerLocalSsdPutOutcome::Dropped => &self.owner_local_ssd_put_counters.dropped,
            OwnerLocalSsdPutOutcome::Obsolete => &self.owner_local_ssd_put_counters.obsolete,
            OwnerLocalSsdPutOutcome::Failed => &self.owner_local_ssd_put_counters.failed,
        };
        terminal_counter.fetch_add(1, Ordering::Relaxed);
        true
    }

    pub(crate) fn record_owner_remote_put_transfer(&self) {
        self.owner_remote_put_counters
            .transfers
            .fetch_add(1, Ordering::Relaxed);
    }

    fn local_visible_mem_holder_unfenced(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        if let Some(info) = self.precommit_local_visible_info.get(key) {
            return Some(info.mem_holder.clone());
        }
        self.get_cached_info
            .get(key)
            .map(|info| info.mem_holder.clone())
    }

    /// Resolve payloads that an external local probe can consume immediately.
    ///
    /// A pending local Get has completed its physical transfer and already
    /// returned the same resident `MemoryInfo` to its leader.  Reusing that Arc
    /// prevents a concurrent request from claiming a second owner target while
    /// the asynchronous master route commit is still in flight.  Callers hold
    /// the per-key fence across this lookup, so promotion or abort cannot race
    /// the selection.  Pending entries deliberately remain outside the normal
    /// committed lookup helper so they cannot receive a Moka hot-touch.
    fn external_local_probe_mem_holder_unfenced(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        self.local_visible_mem_holder_unfenced(key).or_else(|| {
            self.pending_local_get_info
                .get(key)
                .map(|pending| pending.mem_holder.clone())
        })
    }

    pub(crate) fn local_visible_mem_holders(
        &self,
        keys: &[String],
    ) -> Vec<Option<Arc<MemoryInfo>>> {
        // Resolve each page under only its own short sharded fence.  The batch
        // itself never owns a synchronous lock.  A cloned MemoryInfo pins the
        // selected backing if reclaim starts after this point.
        let resolved = keys
            .iter()
            .map(|key| {
                let controls = self.owner_key_control.lock_key(key);
                if controls
                    .get(key)
                    .is_some_and(|state| state.local_access_fenced())
                {
                    return None;
                }
                let memory_info = self.local_visible_mem_holder_unfenced(key)?;
                let hot_put_id = self
                    .get_cached_info
                    .get(key)
                    .filter(|cached| Arc::ptr_eq(&cached.mem_holder, &memory_info))
                    .map(|cached| (cached.put_time_ms, cached.put_version));
                Some((memory_info, hot_put_id))
            })
            .collect::<Vec<_>>();
        resolved
            .into_iter()
            .zip(keys)
            .map(|(resolved, key)| {
                let (memory_info, hot_put_id) = resolved?;
                if let Some(put_id) = hot_put_id {
                    self.owner_hot_touch_or_promote(key, put_id, &memory_info);
                }
                Some(memory_info)
            })
            .collect()
    }

    pub(crate) fn install_external_get_holding(
        &self,
        req_node_id: &str,
        memory_info: Arc<MemoryInfo>,
    ) -> ExternalMemHolderInfo {
        let batch = self.prepare_external_get_holding_batch(req_node_id, 1);
        self.install_external_get_holding_from_batch(&batch, 0, memory_info)
    }

    pub(crate) fn prepare_external_get_holding_batch(
        &self,
        req_node_id: &str,
        reserved_len: usize,
    ) -> ExternalGetHoldingBatch {
        ExternalGetHoldingBatch {
            req_node_id: Arc::from(req_node_id),
            requester_node_start_time: self
                .view
                .cluster_manager()
                .get_member_info_cached(req_node_id)
                .map(|member| member.node_start_time),
            first_holder_id: allocate_external_holding_ids(
                &self.next_external_holding_id,
                reserved_len,
            ),
            reserved_len,
        }
    }

    pub(crate) fn install_external_get_holding_from_batch(
        &self,
        batch: &ExternalGetHoldingBatch,
        reservation_index: usize,
        memory_info: Arc<MemoryInfo>,
    ) -> ExternalMemHolderInfo {
        let external_holder_id = batch.holder_id_at(reservation_index);
        let key = NodeHolderKey::new(batch.req_node_id.to_string(), external_holder_id);
        let external_memholder_info = ExternalMemHolderInfo {
            offset: memory_info.offset,
            len: memory_info.len,
            holder_id: external_holder_id,
        };
        let owner_hot_pin = self.owner_hot_pin_memory_info(&memory_info);
        let previous = self.external_get_holding.inner().insert(
            key,
            ExternalHoldingGetInfo {
                key: memory_info.key.clone(),
                req_node_id: batch.req_node_id.clone(),
                requester_node_start_time: batch.requester_node_start_time,
                memory_info,
                _owner_hot_pin: owner_hot_pin,
            },
        );
        assert!(
            previous.is_none(),
            "fresh external holding id unexpectedly replaced a live holding"
        );
        external_memholder_info
    }

    pub async fn build_local_reserve_resident_memory_info(
        &self,
        key: &str,
        addr: u64,
        len: u32,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> Arc<MemoryInfo> {
        let resident_owner_node_id: NodeID = self.view.cluster_manager().get_self_info().id.into();
        Arc::new(
            MemoryInfo::new_local_reserve_resident(
                addr,
                len,
                key.to_string(),
                resident_owner_node_id,
                self.view.clone(),
                allocation_id,
                segment_offset,
                capacity_bytes,
            )
            .await,
        )
    }

    pub(crate) fn install_hidden_pending_local_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        addr: u64,
        base_addr: u64,
        len: u32,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<Arc<MemoryInfo>> {
        self.install_hidden_owner_slot_get(
            key,
            get_id,
            put_id,
            addr,
            base_addr,
            len,
            allocation_id,
            segment_offset,
            capacity_bytes,
            PendingLocalGetSource::PreparedDestination,
            None,
        )
    }

    pub(crate) fn install_hidden_leased_pending_local_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        capability: &crate::owner_segment::OwnerTargetWriteCapability,
    ) -> KvResult<Arc<MemoryInfo>> {
        let target = &capability.slot;
        let len = u32::try_from(target.len).map_err(|_| {
            KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "owner Get target length exceeds u32: key={} len={}",
                    key, target.len
                ),
            })
        })?;
        self.install_hidden_owner_slot_get(
            key,
            get_id,
            put_id,
            target.addr,
            target.base_addr,
            len,
            target.allocation_id,
            target.segment_offset,
            target.capacity_bytes,
            PendingLocalGetSource::PreparedDestination,
            Some(capability),
        )
    }

    pub(crate) fn install_hidden_global_shared_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        addr: u64,
        base_addr: u64,
        len: u32,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> KvResult<Arc<MemoryInfo>> {
        self.install_hidden_owner_slot_get(
            key,
            get_id,
            put_id,
            addr,
            base_addr,
            len,
            allocation_id,
            segment_offset,
            capacity_bytes,
            PendingLocalGetSource::ExistingGlobalShared,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn install_hidden_owner_slot_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        addr: u64,
        base_addr: u64,
        len: u32,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
        source: PendingLocalGetSource,
        target_write_capability: Option<&crate::owner_segment::OwnerTargetWriteCapability>,
    ) -> KvResult<Arc<MemoryInfo>> {
        let controls = self.owner_key_control.lock_key(key);
        if controls
            .get(key)
            .is_some_and(|state| state.local_access_fenced())
            || self.pending_local_get_info.contains_key(key)
        {
            return Err(KvError::Api(ApiError::KeyBeingWritten {
                key: key.to_string(),
            }));
        }
        match source {
            PendingLocalGetSource::PreparedDestination => {
                if let Some(capability) = target_write_capability {
                    if capability.slot.allocation_id != allocation_id
                        || capability.slot.segment_offset != segment_offset
                        || capability.slot.capacity_bytes != capacity_bytes
                    {
                        return Err(KvError::Api(ApiError::InvalidArgument {
                            detail: "Get target capability geometry changed before hidden install"
                                .to_string(),
                        }));
                    }
                    self.owner_segment_allocator
                        .lock()
                        .mark_data_ready_get_target_pending_visible(capability)
                        .map_err(|error| {
                            KvError::Api(ApiError::Unknown {
                                detail: format!(
                                    "failed to install DataReady Get target: {}",
                                    error.detail
                                ),
                            })
                        })?;
                } else {
                    self.owner_mark_local_reserve_slot_pending_visible(
                        allocation_id,
                        segment_offset,
                        capacity_bytes,
                    )?;
                }
                self.owner_retain_local_reserve_resident_slot_holder(
                    allocation_id,
                    segment_offset,
                    capacity_bytes,
                )?;
            }
            PendingLocalGetSource::ExistingGlobalShared => {
                self.owner_retain_global_shared_slot_holder(
                    allocation_id,
                    segment_offset,
                    capacity_bytes,
                )?;
            }
        }
        let resident_owner_node_id: NodeID = self.view.cluster_manager().get_self_info().id.into();
        let memory_info = Arc::new(MemoryInfo::new_local_reserve_resident_with_base(
            addr,
            base_addr,
            len,
            key.to_string(),
            resident_owner_node_id,
            self.view.clone(),
            allocation_id,
            segment_offset,
            capacity_bytes,
        ));
        let previous = self.pending_local_get_info.insert(
            key.to_string(),
            PendingLocalGetInfo {
                get_id,
                put_id,
                mem_holder: memory_info.clone(),
                source,
                target_write_capability: target_write_capability.cloned(),
            },
        );
        assert!(
            previous.is_none(),
            "pending local Get must be unique per key"
        );
        drop(controls);
        Ok(memory_info)
    }

    pub(crate) fn abort_hidden_pending_local_get(&self, key: &str, get_id: u64) -> bool {
        let _controls = self.owner_key_control.lock_key(key);
        let capability = self.pending_local_get_info.get(key).and_then(|pending| {
            (pending.get_id == get_id)
                .then(|| pending.target_write_capability.clone())
                .flatten()
        });
        if let Some(capability) = capability {
            let outcome = self
                .owner_segment_allocator
                .lock()
                .abort_data_ready_visible_get_target(
                    &capability.operation,
                    &capability.lease_id,
                    "master GetDone rejected the hidden target".to_string(),
                );
            if !matches!(
                outcome,
                crate::owner_segment::OwnerSegmentTransferOutcome::TargetAborted
            ) {
                tracing::error!(
                    key,
                    get_id,
                    outcome = ?outcome,
                    "refusing to remove a hidden Get target whose operation did not abort"
                );
                return false;
            }
        }
        self.pending_local_get_info
            .remove_if(key, |_, pending| pending.get_id == get_id)
            .is_some()
    }

    pub(crate) fn promote_hidden_owner_slot_get(
        &self,
        key: &str,
        get_id: u64,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> KvResult<Arc<MemoryInfo>> {
        let memory_info = {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                return Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }));
            }
            let Some((pending_memory_info, source, target_write_capability)) =
                self.pending_local_get_info.get(key).and_then(|pending| {
                    (pending.get_id == get_id && pending.put_id == put_id).then(|| {
                        (
                            pending.mem_holder.clone(),
                            pending.source,
                            pending.target_write_capability.clone(),
                        )
                    })
                })
            else {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "hidden pending local Get is absent: key={} get_id={}",
                        key, get_id
                    ),
                }));
            };
            let (allocation_id, segment_offset, capacity_bytes) = pending_memory_info
                .local_reserve_resident_slot_ref()
                .expect("pending local Get must carry a local-reserve slot");
            if source == PendingLocalGetSource::PreparedDestination {
                if let Some(capability) = target_write_capability.as_ref() {
                    let outcome = self
                        .owner_segment_allocator
                        .lock()
                        .finish_data_ready_get_target(
                            &capability.operation,
                            &capability.lease_id,
                            allocation_id,
                        );
                    if !matches!(
                        outcome,
                        crate::owner_segment::OwnerSegmentTransferOutcome::TargetCommitted { .. }
                    ) {
                        return Err(KvError::Api(ApiError::Unknown {
                            detail: format!("failed to commit DataReady Get target: {outcome:?}"),
                        }));
                    }
                } else {
                    self.owner_promote_local_reserve_pending_slot_to_committed(
                        allocation_id,
                        segment_offset,
                        capacity_bytes,
                    )?;
                    self.owner_segment_allocator
                        .lock()
                        .install_committed_manifest(
                            key,
                            put_id,
                            allocation_id,
                            crate::owner_segment::OwnerSlotScope::LocalExclusive,
                            allocation_id,
                        )
                        .map_err(|error| {
                            KvError::Api(ApiError::Unknown {
                                detail: format!(
                                    "failed to install committed Get target manifest: {}",
                                    error.detail
                                ),
                            })
                        })?;
                }
            } else {
                self.owner_segment_allocator
                    .lock()
                    .update_committed_manifest_scope(
                        key,
                        put_id,
                        allocation_id,
                        segment_offset,
                        capacity_bytes,
                        crate::owner_segment::OwnerSlotScope::LocalExclusive,
                    )
                    .map_err(|error| {
                        KvError::Api(ApiError::Unknown {
                            detail: format!(
                                "failed to promote GlobalShared owner manifest: {}",
                                error.detail
                            ),
                        })
                    })?;
            }
            let removed = self
                .pending_local_get_info
                .remove_if(key, |_, pending| {
                    pending.get_id == get_id
                        && pending.put_id == put_id
                        && Arc::ptr_eq(&pending.mem_holder, &pending_memory_info)
                })
                .is_some();
            if !removed {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "hidden pending local Get changed while promoting: key={} get_id={}",
                        key, get_id
                    ),
                }));
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: pending_memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
            drop(controls);
            pending_memory_info
        };
        Ok(memory_info)
    }

    pub async fn install_local_committed_memory_info(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        offset: u64,
        len: u32,
        holder_id: u64,
    ) -> KvResult<()> {
        let master_node_id: NodeID = self.view.cluster_manager().get_self_info().id.into();
        let memory_info = Arc::new(
            MemoryInfo::new(
                offset,
                len,
                holder_id,
                key.to_string(),
                master_node_id,
                self.view.clone(),
            )
            .await,
        );
        {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                return Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }));
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
        }
        self.owner_hot_track_committed(key, put_id, &memory_info);
        Ok(())
    }

    pub(crate) fn install_get_cached_info_if_unfenced(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: Arc<MemoryInfo>,
    ) -> bool {
        {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                tracing::debug!(
                    "skip get cache publication behind owner reclaim fence: key={} put_id=({},{})",
                    key,
                    put_id.0,
                    put_id.1
                );
                return false;
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
        }
        self.owner_hot_track_committed(key, put_id, &memory_info);
        true
    }

    pub fn install_precommit_local_visible_memory_info(
        &self,
        key: &str,
        memory_info: Arc<MemoryInfo>,
    ) {
        let controls = self.owner_key_control.lock_key(key);
        assert!(
            !controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced()),
            "precommit local index publication must not cross an owner reclaim fence"
        );
        let (allocation_id, segment_offset, capacity_bytes) = memory_info
            .local_reserve_resident_slot_ref()
            .expect("resident memory_info must carry local reserve slot ref");
        self.owner_mark_local_reserve_slot_pending_visible(
            allocation_id,
            segment_offset,
            capacity_bytes,
        )
        .expect("marking local reserve slot pending visible must succeed");
        self.owner_retain_local_reserve_resident_slot_holder(
            allocation_id,
            segment_offset,
            capacity_bytes,
        )
        .expect("retaining local reserve resident holder must succeed");
        let replaced = self.precommit_local_visible_info.insert(
            key.to_string(),
            PrecommitLocalVisibleInfo {
                mem_holder: memory_info.clone(),
            },
        );
        assert!(
            replaced.is_none(),
            "precommit local visible cache must not be replaced for the same key"
        );
    }

    pub fn remove_precommit_local_reserve_resident_slot_if_same(
        &self,
        key: &str,
        expected_mem_holder: &Arc<MemoryInfo>,
    ) -> bool {
        let _controls = self.owner_key_control.lock_key(key);
        self.precommit_local_visible_info
            .remove_if(key, |_, info| {
                Arc::ptr_eq(&info.mem_holder, expected_mem_holder)
            })
            .is_some()
    }

    pub fn precommit_local_visible_memory_info(&self, key: &str) -> Option<Arc<MemoryInfo>> {
        self.precommit_local_visible_info
            .get(key)
            .map(|info| info.mem_holder.clone())
    }

    pub(crate) fn committed_local_reserve_slot_is_current(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        expected: &crate::master_kv_router::msg_pack::PutDoneCommittedSlot,
    ) -> bool {
        self.get_cached_info.get(key).is_some_and(|cached| {
            (cached.put_time_ms, cached.put_version) == put_id
                && cached.mem_holder.local_reserve_resident_slot_ref()
                    == Some((
                        expected.allocation_id,
                        expected.segment_offset,
                        expected.capacity_bytes,
                    ))
        })
    }

    pub fn promote_precommit_local_reserve_resident_slot_if_same(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        memory_info: Arc<MemoryInfo>,
        _atomic_group: Option<&crate::master_kv_router::msg_pack::PutAtomicGroup>,
    ) -> KvResult<()> {
        {
            let controls = self.owner_key_control.lock_key(key);
            if controls
                .get(key)
                .is_some_and(|state| state.local_access_fenced())
            {
                return Err(KvError::Api(ApiError::KeyBeingWritten {
                    key: key.to_string(),
                }));
            }
            let is_same_pending = self
                .precommit_local_visible_info
                .get(key)
                .map(|info| Arc::ptr_eq(&info.mem_holder, &memory_info))
                .unwrap_or(false);
            if !is_same_pending {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "precommit local visible cache missing while promoting key={}",
                        key
                    ),
                }));
            }
            let (allocation_id, segment_offset, capacity_bytes) = memory_info
                .local_reserve_resident_slot_ref()
                .expect("resident memory_info must carry local reserve slot ref");
            self.owner_promote_local_reserve_pending_slot_to_committed(
                allocation_id,
                segment_offset,
                capacity_bytes,
            )?;
            self.owner_segment_allocator
                .lock()
                .install_committed_manifest(
                    key,
                    put_id,
                    allocation_id,
                    crate::owner_segment::OwnerSlotScope::LocalExclusive,
                    allocation_id,
                )
                .map_err(|error| {
                    KvError::Api(ApiError::Unknown {
                        detail: format!(
                            "failed to install committed Put target manifest: {}",
                            error.detail
                        ),
                    })
                })?;
            let removed = self
                .precommit_local_visible_info
                .remove_if(key, |_, info| Arc::ptr_eq(&info.mem_holder, &memory_info))
                .is_some();
            if !removed {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "precommit local visible cache disappeared while promoting key={}",
                        key
                    ),
                }));
            }
            let replaced = self.get_cached_info.insert(
                key.to_string(),
                GetCachedInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                    mem_holder: memory_info.clone(),
                },
            );
            if let Some(previous) = replaced {
                self.release_local_reserve_route_for_memory_info(previous.mem_holder.as_ref());
            }
            self.local_snapshot_info.insert(
                key.to_string(),
                LocalSnapshotInfo {
                    put_time_ms: put_id.0,
                    put_version: put_id.1,
                },
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExternalPendingPutCtx {
    pub peer_id: Option<NodeIDString>,
    pub src_offset: u64,
    pub target_base_addr: u64,
    pub target_offset: u64,
    pub len: u64,
    /// Original content-selection signal from the caller/adapter.
    pub make_replica_task: bool,
    /// A remote memory target was pre-reserved, or this local-first path may
    /// perform normal append-time remote admission.
    pub remote_replica_admitted: bool,
    pub preferred_sub_cluster: Option<String>,
    pub local_reserve_slot: Option<OwnerSlotRef>,
    pub local_reserve_slot_size: Option<u64>,
    pub atomic_group: Option<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    pub radix: Option<RadixKvMetadata>,
    /// Keep the per-key reclaim fence alive for every cache/user clone of this
    /// pending context.  The counter is released only by the final Arc drop.
    pub(crate) _pending_fence: Arc<ExternalPendingPutFenceGuard>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum OwnerSlotState {
    Prepared,
    PendingLocalVisible {
        holder_ref_count: u32,
    },
    Committed {
        route_live: bool,
        holder_ref_count: u32,
    },
}

pub type OwnerSlotRef = crate::owner_segment::OwnerSlotDesc;

#[derive(Debug, Clone)]
pub struct OwnerSlotLease {
    pub value_len: u64,
    pub slot_size: u64,
    pub slots: Vec<OwnerSlotRef>,
}

impl OwnerSlotLease {
    pub fn value_ptrs(&self) -> Vec<u64> {
        self.slots.iter().map(|slot| slot.addr).collect()
    }
}

#[derive(Clone)]
struct OwnerAllocationState {
    allocation_id: u64,
    allocation: offset_allocator::Allocation,
    segment_offset: u64,
    capacity_bytes: u64,
    value_len: u64,
    source_read_lease_count: u32,
    state: OwnerSlotState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnerSourceLeaseState {
    Active,
    Released(crate::owner_segment::OwnerTransferOutcome),
}

#[derive(Debug, Clone)]
struct OwnerSourceLeaseRecord {
    lease_id: crate::owner_segment::OwnerLeaseId,
    route_token: crate::owner_segment::OwnerSourceRouteToken,
    state: OwnerSourceLeaseState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnerTargetLeaseState {
    Prepared,
    DataReady {
        receipt: crate::owner_segment::OwnerTransferReceipt,
    },
    RoutePending {
        receipt: crate::owner_segment::OwnerTransferReceipt,
        route_token: Option<crate::owner_segment::OwnerTargetRouteToken>,
    },
    Committed {
        route_epoch: u64,
    },
    Aborted {
        reason: String,
    },
}

#[derive(Debug, Clone)]
struct OwnerTargetLeaseRecord {
    lease_id: crate::owner_segment::OwnerLeaseId,
    key: String,
    put_id: crate::master_kv_router::put::PutIDForAKey,
    len: u64,
    disposition: crate::owner_segment::OwnerTargetDisposition,
    atomic_batch: Option<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    slot: OwnerSlotRef,
    state: OwnerTargetLeaseState,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OwnerSegmentAllocatableReport {
    pub raw_free_bytes: u64,
    pub allocatable_slots: u64,
    pub allocatable_bytes: u64,
    /// Free bytes that cannot satisfy this report's requested slot size. This
    /// is demand-relative capacity, not permanently stranded storage: a
    /// smaller allocation may still consume it.
    pub slot_unallocatable_bytes: u64,
}

const OWNER_ALLOCATABLE_REPORT_CACHE_ENTRIES: usize = 16;

#[derive(Clone, Copy)]
struct OwnerCachedAllocatableReport {
    layout_revision: u64,
    slot_size: u64,
    report: OwnerSegmentAllocatableReport,
}

/// The single physical allocator for one owner's complete registered DRAM segment.
///
/// Local-exclusive and global-shared slots use this same allocator.  A logical
/// scope change never replaces this object and never moves payload bytes.
struct OwnerSegmentState {
    owner: crate::owner_segment::OwnerGeneration,
    registration_epoch: u64,
    pub base_addr: u64,
    pub addr: u64,
    pub len: u64,
    allocator: offset_allocator::Allocator,
    /// Read-only derived geometry, scoped to this segment generation. Runtime
    /// access remains under the existing owner allocator mutex. OnceLock keeps
    /// the shared report API Send + Sync without another report-cache mutex.
    /// Physical allocate/free clears these bounded entries; the allocator's
    /// own revision additionally prevents a stale layout from ever matching.
    allocatable_report_cache:
        [OnceLock<OwnerCachedAllocatableReport>; OWNER_ALLOCATABLE_REPORT_CACHE_ENTRIES],
    #[cfg(test)]
    allocatable_report_cache_hits: AtomicU64,
    #[cfg(test)]
    allocatable_report_scans: AtomicU64,
    #[cfg(test)]
    allocatable_report_free_nodes_visited: AtomicU64,
    /// ABA-safe primary identity. Allocation ids are never reused within an
    /// owner process generation.
    allocations: HashMap<u64, OwnerAllocationState>,
    /// Ordered physical offsets retain exact identity checks and the test
    /// oracle. Production capacity reports read OffsetAllocator's free nodes.
    allocations_by_offset: BTreeMap<u32, u64>,
    manifest_by_key: HashMap<
        crate::owner_segment::OwnerManifestKey,
        crate::owner_segment::OwnerSlotManifestEntry,
    >,
    manifest_key_by_allocation: HashMap<u64, crate::owner_segment::OwnerManifestKey>,
    source_leases: HashMap<crate::owner_segment::OwnerTransferOpId, OwnerSourceLeaseRecord>,
    target_leases: HashMap<crate::owner_segment::OwnerTransferOpId, OwnerTargetLeaseRecord>,
    /// Logical GlobalShared capacity reserved by exact LocalExclusive
    /// generations before their metadata-only demotion RPC.  These bytes are
    /// admission state, not projected physical reclaim credit.  A retry of
    /// the same fenced generation reuses the same entry.
    global_demotion_reservations: HashMap<crate::owner_segment::OwnerManifestKey, OwnerSlotRef>,
    #[cfg(test)]
    global_accounting_scans: AtomicU64,
    /// Advances only after the unique allocator has actually returned one
    /// allocation to its free extents.  Aggregate free bytes can stay flat
    /// when another claimant consumes the extent immediately, so pressure
    /// progress must not infer exact Free from that aggregate.
    physical_free_epoch: u64,
}

impl std::fmt::Debug for OwnerSegmentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let report = self.allocator.storage_report();
        f.debug_struct("OwnerSegmentState")
            .field("owner", &self.owner)
            .field("registration_epoch", &self.registration_epoch)
            .field("base_addr", &format_args!("{:#x}", self.base_addr))
            .field("addr", &format_args!("{:#x}", self.addr))
            .field("len", &self.len)
            .field("allocations", &self.allocations.len())
            .field("manifest_entries", &self.manifest_by_key.len())
            .field("source_leases", &self.source_leases.len())
            .field("target_leases", &self.target_leases.len())
            .field(
                "global_demotion_reservations",
                &self.global_demotion_reservations.len(),
            )
            .field("physical_free_epoch", &self.physical_free_epoch)
            .field(
                "free_bytes",
                &(u64::from(report.total_free_space)
                    * crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES),
            )
            .field(
                "largest_free_bytes",
                &(u64::from(report.largest_free_region)
                    * crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES),
            )
            .finish()
    }
}

impl OwnerSegmentState {
    fn new(
        owner: crate::owner_segment::OwnerGeneration,
        registration_epoch: u64,
        base_addr: u64,
        addr: u64,
        len: u64,
    ) -> Result<Self, String> {
        if !owner.is_initialized() {
            return Err("owner segment generation must be initialized".to_string());
        }
        if registration_epoch == 0 {
            return Err("owner segment registration epoch must be non-zero".to_string());
        }
        assert!(
            len > 0 && len % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES == 0,
            "owner segment length must be a non-zero multiple of 4 KiB"
        );
        let allocation_units = u32::try_from(
            len / crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES,
        )
        .map_err(|_| {
            format!(
                "owner segment exceeds OffsetAllocator address space: len={} unit_bytes={}",
                len,
                crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES
            )
        })?;
        // Match the master allocator's bounded metadata policy.  Hundreds of
        // GiB of address space must not preallocate one metadata node per 4 KiB.
        let max_allocation_nodes = allocation_units.saturating_add(2).min(128 * 1024);
        Ok(Self {
            owner,
            registration_epoch,
            base_addr,
            addr,
            len,
            allocator: offset_allocator::Allocator::with_max_allocs(
                allocation_units,
                max_allocation_nodes,
            ),
            allocatable_report_cache: std::array::from_fn(|_| OnceLock::new()),
            #[cfg(test)]
            allocatable_report_cache_hits: AtomicU64::new(0),
            #[cfg(test)]
            allocatable_report_scans: AtomicU64::new(0),
            #[cfg(test)]
            allocatable_report_free_nodes_visited: AtomicU64::new(0),
            allocations: HashMap::new(),
            allocations_by_offset: BTreeMap::new(),
            manifest_by_key: HashMap::new(),
            manifest_key_by_allocation: HashMap::new(),
            source_leases: HashMap::new(),
            target_leases: HashMap::new(),
            global_demotion_reservations: HashMap::new(),
            #[cfg(test)]
            global_accounting_scans: AtomicU64::new(0),
            physical_free_epoch: 0,
        })
    }

    fn claim_prepared_slot(
        &mut self,
        capacity_bytes: u64,
        value_len: u64,
        allocation_id: u64,
    ) -> Option<OwnerSlotRef> {
        if allocation_id == 0 {
            return None;
        }
        if value_len == 0
            || capacity_bytes == 0
            || value_len > capacity_bytes
            || capacity_bytes > self.len
            || capacity_bytes % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        {
            return None;
        }
        let allocation_units =
            u32::try_from(capacity_bytes / crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES)
                .ok()?;
        let allocation = self.allocator.allocate(allocation_units)?;
        self.clear_allocatable_report_cache();
        let offset_units = allocation.offset;
        let segment_offset = crate::owner_segment_offset_bytes(offset_units)?;
        let previous = self.allocations.insert(
            allocation_id,
            OwnerAllocationState {
                allocation_id,
                allocation,
                segment_offset,
                capacity_bytes,
                value_len,
                source_read_lease_count: 0,
                state: OwnerSlotState::Prepared,
            },
        );
        assert!(previous.is_none(), "owner allocation id was reused");
        assert!(
            self.allocations_by_offset
                .insert(offset_units, allocation_id)
                .is_none(),
            "OffsetAllocator returned a live segment offset"
        );
        Some(OwnerSlotRef {
            owner: self.owner.clone(),
            allocation_id,
            segment_offset,
            capacity_bytes,
            addr: self.addr.checked_add(segment_offset)?,
            base_addr: self.base_addr,
            len: value_len,
            segment_registration_epoch: self.registration_epoch,
        })
    }

    fn allocation(&self, allocation_id: u64) -> Option<&OwnerAllocationState> {
        self.allocations.get(&allocation_id)
    }

    fn allocation_mut(&mut self, allocation_id: u64) -> Option<&mut OwnerAllocationState> {
        self.allocations.get_mut(&allocation_id)
    }

    fn identity_matches(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        self.allocation(allocation_id).is_some_and(|allocation| {
            allocation.allocation_id == allocation_id
                && allocation.segment_offset == segment_offset
                && allocation.capacity_bytes == capacity_bytes
        })
    }

    fn slot_desc(&self, allocation_id: u64) -> Option<OwnerSlotRef> {
        let allocation = self.allocation(allocation_id)?;
        Some(OwnerSlotRef {
            owner: self.owner.clone(),
            allocation_id,
            segment_offset: allocation.segment_offset,
            capacity_bytes: allocation.capacity_bytes,
            addr: self.addr.checked_add(allocation.segment_offset)?,
            base_addr: self.base_addr,
            len: allocation.value_len,
            segment_registration_epoch: self.registration_epoch,
        })
    }

    fn identity_matches_desc(&self, slot: &OwnerSlotRef) -> bool {
        slot.owner == self.owner
            && slot.segment_registration_epoch == self.registration_epoch
            && slot.addr
                == self
                    .addr
                    .checked_add(slot.segment_offset)
                    .unwrap_or(u64::MAX)
            && slot.base_addr == self.base_addr
            && self
                .allocation(slot.allocation_id)
                .is_some_and(|allocation| {
                    allocation.segment_offset == slot.segment_offset
                        && allocation.capacity_bytes == slot.capacity_bytes
                        && allocation.value_len == slot.len
                })
    }

    fn install_manifest_entry(
        &mut self,
        entry: crate::owner_segment::OwnerSlotManifestEntry,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{OwnerManifestKey, OwnerTransferErrorCode};

        if entry.key.is_empty()
            || !entry.slot.is_valid()
            || !self.identity_matches_desc(&entry.slot)
        {
            return Err(crate::owner_segment::OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "owner manifest entry has an invalid key or slot descriptor",
            ));
        }
        let manifest_key = OwnerManifestKey::new(entry.key.clone(), entry.put_id);
        if let Some(existing) = self.manifest_by_key.get(&manifest_key) {
            if existing.slot.geometry_matches(&entry.slot)
                && existing.slot.len == entry.slot.len
                && existing.slot.segment_registration_epoch == entry.slot.segment_registration_epoch
            {
                return Ok(());
            }
            return Err(crate::owner_segment::OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                format!(
                    "owner manifest key already names another slot: key={} put_id=({},{})",
                    entry.key, entry.put_id.0, entry.put_id.1
                ),
            ));
        }
        if let Some(existing_key) = self
            .manifest_key_by_allocation
            .get(&entry.slot.allocation_id)
        {
            return Err(crate::owner_segment::OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                format!(
                    "owner allocation is already bound to another manifest key: allocation_id={} key={} put_id=({},{})",
                    entry.slot.allocation_id,
                    existing_key.key,
                    existing_key.put_id.0,
                    existing_key.put_id.1
                ),
            ));
        }
        self.manifest_key_by_allocation
            .insert(entry.slot.allocation_id, manifest_key.clone());
        self.manifest_by_key.insert(manifest_key, entry);
        Ok(())
    }

    fn manifest_entry(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> Option<&crate::owner_segment::OwnerSlotManifestEntry> {
        self.manifest_by_key
            .get(&crate::owner_segment::OwnerManifestKey::new(key, put_id))
    }

    fn manifest_entry_mut_by_allocation(
        &mut self,
        allocation_id: u64,
    ) -> Option<&mut crate::owner_segment::OwnerSlotManifestEntry> {
        let key = self.manifest_key_by_allocation.get(&allocation_id)?.clone();
        self.manifest_by_key.get_mut(&key)
    }

    fn remove_manifest_for_allocation(
        &mut self,
        allocation_id: u64,
    ) -> Option<crate::owner_segment::OwnerSlotManifestEntry> {
        let key = self.manifest_key_by_allocation.remove(&allocation_id)?;
        self.manifest_by_key.remove(&key)
    }

    fn acquire_source(
        &mut self,
        op_id: crate::owner_segment::OwnerTransferOpId,
        route_token: crate::owner_segment::OwnerSourceRouteToken,
        lease_id: crate::owner_segment::OwnerLeaseId,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        if let Some(existing) = self.source_leases.get(&op_id) {
            if existing.route_token != route_token {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "source lease replay changed the route token",
                ));
            }
            return match existing.state {
                OwnerSourceLeaseState::Active => OwnerSegmentTransferOutcome::SourceAcquired {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.route_token.source.clone(),
                },
                OwnerSourceLeaseState::Released(_) => OwnerSegmentTransferOutcome::SourceReleased,
            };
        }
        if !op_id.is_initialized() || !lease_id.is_initialized() || route_token.plan_nonce == 0 {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "source lease acquire requires initialized operation, lease and plan identities",
            ));
        }
        if route_token.source.owner != self.owner
            || route_token.source.segment_registration_epoch != self.registration_epoch
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::StaleGeneration,
                "source lease acquire names a stale owner or segment registration generation",
            ));
        }
        let manifest_key =
            crate::owner_segment::OwnerManifestKey::new(&route_token.key, route_token.put_id);
        let Some(manifest) = self.manifest_by_key.get(&manifest_key) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "source lease acquire key generation is absent from the owner manifest",
            ));
        };
        if manifest.physical_state != OwnerSlotPhysicalState::Committed
            || manifest.route_epoch != route_token.route_epoch
            || !manifest.slot.geometry_matches(&route_token.source)
            || manifest.slot.len != route_token.source.len
            || !self.identity_matches_desc(&route_token.source)
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "source lease route token does not match the committed owner manifest",
            ));
        }
        let Some(allocation) = self.allocation_mut(route_token.source.allocation_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "source lease slot disappeared from the owner allocator",
            ));
        };
        if !matches!(
            allocation.state,
            OwnerSlotState::Committed {
                route_live: true,
                ..
            }
        ) {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Reclaiming,
                "source lease slot is no longer readable",
            ));
        }
        allocation.source_read_lease_count = allocation
            .source_read_lease_count
            .checked_add(1)
            .expect("source read lease count overflow");
        self.source_leases.insert(
            op_id,
            OwnerSourceLeaseRecord {
                lease_id: lease_id.clone(),
                route_token: route_token.clone(),
                state: OwnerSourceLeaseState::Active,
            },
        );
        OwnerSegmentTransferOutcome::SourceAcquired {
            lease_id,
            slot: route_token.source,
        }
    }

    fn release_source(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        outcome: crate::owner_segment::OwnerTransferOutcome,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerTransferErrorCode, OwnerTransferItemError,
        };

        let Some(record) = self.source_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "source lease release operation is absent",
            ));
        };
        if &record.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "source lease release identity mismatch",
            ));
        }
        match record.state {
            OwnerSourceLeaseState::Released(previous) => {
                if previous != outcome {
                    return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Conflict,
                        "source lease release replay changed the transfer outcome",
                    ));
                }
                return OwnerSegmentTransferOutcome::SourceReleased;
            }
            OwnerSourceLeaseState::Active => {}
        }
        let allocation_id = record.route_token.source.allocation_id;
        let _ = record;
        let Some(allocation) = self.allocation_mut(allocation_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Internal,
                "active source lease lost its allocator slot",
            ));
        };
        let Some(next_count) = allocation.source_read_lease_count.checked_sub(1) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Internal,
                "source read lease count underflow",
            ));
        };
        allocation.source_read_lease_count = next_count;
        self.source_leases
            .get_mut(op_id)
            .expect("validated source lease disappeared")
            .state = OwnerSourceLeaseState::Released(outcome);
        let should_free = self.allocation(allocation_id).is_some_and(|allocation| {
            allocation.source_read_lease_count == 0
                && matches!(
                    allocation.state,
                    OwnerSlotState::Committed {
                        route_live: false,
                        holder_ref_count: 0,
                    }
                )
        });
        if should_free {
            self.free_allocation(allocation_id);
        }
        OwnerSegmentTransferOutcome::SourceReleased
    }

    fn committed_route_only_matches(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        self.allocation(allocation_id).is_some_and(|allocation| {
            allocation.segment_offset == segment_offset
                && allocation.capacity_bytes == capacity_bytes
                && matches!(
                    allocation.state,
                    OwnerSlotState::Committed {
                        route_live: true,
                        holder_ref_count: 0,
                    }
                )
        })
    }

    fn retain_committed_route_only_holder(&mut self, allocation_id: u64) -> bool {
        let Some(state) = self
            .allocation_mut(allocation_id)
            .map(|allocation| &mut allocation.state)
        else {
            return false;
        };
        match state {
            OwnerSlotState::Committed {
                route_live: true,
                holder_ref_count,
            } if *holder_ref_count == 0 => {
                *holder_ref_count = 1;
                true
            }
            OwnerSlotState::Prepared
            | OwnerSlotState::PendingLocalVisible { .. }
            | OwnerSlotState::Committed { .. } => false,
        }
    }

    fn free_allocation(&mut self, allocation_id: u64) {
        let allocation = self
            .allocations
            .remove(&allocation_id)
            .expect("free_allocation expects a live allocation");
        assert_eq!(allocation.allocation_id, allocation_id);
        assert_eq!(
            allocation.source_read_lease_count, 0,
            "free_allocation cannot release a slot with active source read leases"
        );
        if let Some(manifest) = self.remove_manifest_for_allocation(allocation_id) {
            self.global_demotion_reservations
                .remove(&crate::owner_segment::OwnerManifestKey::new(
                    manifest.key,
                    manifest.put_id,
                ));
        }
        assert_eq!(
            self.allocations_by_offset
                .remove(&allocation.allocation.offset),
            Some(allocation_id),
            "owner segment offset index diverged from allocation identity"
        );
        self.allocator.free(allocation.allocation);
        self.clear_allocatable_report_cache();
        self.physical_free_epoch = self.physical_free_epoch.saturating_add(1);
    }

    fn mark_prepared_slot_pending_visible(&mut self, allocation_id: u64) {
        let state = self
            .allocation_mut(allocation_id)
            .map(|allocation| &mut allocation.state)
            .expect("owner allocation id is not live");
        assert!(
            matches!(*state, OwnerSlotState::Prepared),
            "mark_prepared_slot_pending_visible expects a prepared slot"
        );
        *state = OwnerSlotState::PendingLocalVisible {
            holder_ref_count: 0,
        };
    }

    fn promote_pending_visible_slot_to_committed(&mut self, allocation_id: u64) {
        let state = self
            .allocation_mut(allocation_id)
            .map(|allocation| &mut allocation.state)
            .expect("owner allocation id is not live");
        let holder_ref_count = match *state {
            OwnerSlotState::PendingLocalVisible { holder_ref_count } => holder_ref_count,
            _ => {
                unreachable!("promote_pending_visible_slot_to_committed expects a pending slot");
            }
        };
        *state = OwnerSlotState::Committed {
            route_live: true,
            holder_ref_count,
        };
    }

    fn release_prepared_slot(&mut self, allocation_id: u64) {
        let state = &self
            .allocation(allocation_id)
            .expect("owner allocation id is not live")
            .state;
        assert!(
            matches!(*state, OwnerSlotState::Prepared),
            "release_prepared_slot expects a prepared slot"
        );
        self.free_allocation(allocation_id);
    }

    fn retain_resident_slot_holder(&mut self, allocation_id: u64) {
        let state = self
            .allocation_mut(allocation_id)
            .map(|allocation| &mut allocation.state)
            .expect("owner allocation id is not live");
        match state {
            OwnerSlotState::PendingLocalVisible { holder_ref_count }
            | OwnerSlotState::Committed {
                holder_ref_count, ..
            } => {
                *holder_ref_count = holder_ref_count
                    .checked_add(1)
                    .expect("retain_resident_slot_holder overflow");
            }
            _ => {
                unreachable!("retain_resident_slot_holder expects a resident slot");
            }
        }
    }

    fn release_resident_slot_holder(&mut self, allocation_id: u64) {
        let should_free = {
            let allocation = self
                .allocation_mut(allocation_id)
                .expect("owner allocation id is not live");
            let source_read_lease_count = allocation.source_read_lease_count;
            match &mut allocation.state {
                OwnerSlotState::PendingLocalVisible { holder_ref_count } => {
                    assert!(
                        *holder_ref_count > 0,
                        "release_resident_slot_holder expects holder_ref_count > 0"
                    );
                    *holder_ref_count -= 1;
                    *holder_ref_count == 0 && source_read_lease_count == 0
                }
                OwnerSlotState::Committed {
                    route_live,
                    holder_ref_count,
                } => {
                    *holder_ref_count = holder_ref_count
                        .checked_sub(1)
                        .expect("release_resident_slot_holder expects holder_ref_count > 0");
                    !*route_live && *holder_ref_count == 0 && source_read_lease_count == 0
                }
                _ => {
                    unreachable!("release_resident_slot_holder expects a resident slot");
                }
            }
        };
        if should_free {
            self.free_allocation(allocation_id);
        }
    }

    fn release_committed_slot_route(&mut self, allocation_id: u64) {
        let should_free = {
            let allocation = self
                .allocation_mut(allocation_id)
                .expect("owner allocation id is not live");
            let source_read_lease_count = allocation.source_read_lease_count;
            match &mut allocation.state {
                OwnerSlotState::Committed {
                    route_live,
                    holder_ref_count,
                } => {
                    assert!(
                        *route_live,
                        "release_committed_slot_route expects a live route"
                    );
                    *route_live = false;
                    *holder_ref_count == 0 && source_read_lease_count == 0
                }
                _ => {
                    unreachable!("release_committed_slot_route expects a committed slot");
                }
            }
        };
        if let Some(manifest) = self.manifest_entry_mut_by_allocation(allocation_id) {
            manifest.physical_state = crate::owner_segment::OwnerSlotPhysicalState::Reclaiming;
        }
        if should_free {
            self.free_allocation(allocation_id);
        }
    }

    fn release_committed_resident_slot(&mut self, allocation_id: u64) {
        let should_free = {
            let allocation = self
                .allocation_mut(allocation_id)
                .expect("owner allocation id is not live");
            let source_read_lease_count = allocation.source_read_lease_count;
            match &mut allocation.state {
                OwnerSlotState::Committed {
                    route_live,
                    holder_ref_count,
                } => {
                    assert!(
                        *route_live,
                        "release_committed_resident_slot expects a live route"
                    );
                    assert!(
                        *holder_ref_count > 0,
                        "release_committed_resident_slot expects holder_ref_count > 0"
                    );
                    *route_live = false;
                    *holder_ref_count -= 1;
                    *holder_ref_count == 0 && source_read_lease_count == 0
                }
                _ => {
                    unreachable!("release_committed_resident_slot expects a committed slot");
                }
            }
        };
        if let Some(manifest) = self.manifest_entry_mut_by_allocation(allocation_id) {
            manifest.physical_state = crate::owner_segment::OwnerSlotPhysicalState::Reclaiming;
        }
        if should_free {
            self.free_allocation(allocation_id);
        }
    }

    fn used_slot_count(&self) -> usize {
        self.allocations.len()
    }

    fn total_free_bytes(&self) -> u64 {
        u64::from(self.allocator.storage_report().total_free_space)
            * crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES
    }

    fn largest_free_bytes(&self) -> u64 {
        u64::from(self.allocator.storage_report().largest_free_region)
            * crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES
    }

    fn allocatable_report(&self, slot_size: u64) -> OwnerSegmentAllocatableReport {
        let raw_free_bytes = self.total_free_bytes();
        if slot_size == 0
            || slot_size > self.len
            || slot_size % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        {
            return OwnerSegmentAllocatableReport {
                raw_free_bytes,
                slot_unallocatable_bytes: raw_free_bytes,
                ..Default::default()
            };
        }

        let layout_revision = self.allocator.layout_revision();
        for cached in self
            .allocatable_report_cache
            .iter()
            .filter_map(OnceLock::get)
        {
            if cached.layout_revision == layout_revision && cached.slot_size == slot_size {
                #[cfg(test)]
                self.allocatable_report_cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                return cached.report;
            }
        }

        let slot_units =
            u32::try_from(slot_size / crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES)
                .expect("validated owner slot size exceeds the allocator address space");
        let exact = self.allocator.storage_report_for_size(slot_units);
        #[cfg(test)]
        {
            self.allocatable_report_scans
                .fetch_add(1, Ordering::Relaxed);
            self.allocatable_report_free_nodes_visited
                .fetch_add(u64::from(exact.free_regions), Ordering::Relaxed);
        }
        let exact_raw_free_bytes =
            u64::from(exact.total_free_space) * crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES;
        // Preserve the previous report's fail-closed boundary when allocator
        // metadata is exhausted: physical geometry alone must not turn a real
        // failed allocation into an immediately-ready retry loop.
        assert_eq!(
            exact_raw_free_bytes, raw_free_bytes,
            "owner allocation index diverged from OffsetAllocator free accounting"
        );
        let allocatable_bytes = exact.allocatable_slots * slot_size;
        let report = OwnerSegmentAllocatableReport {
            raw_free_bytes,
            allocatable_slots: exact.allocatable_slots,
            allocatable_bytes,
            slot_unallocatable_bytes: raw_free_bytes - allocatable_bytes,
        };
        let cached = OwnerCachedAllocatableReport {
            layout_revision,
            slot_size,
            report,
        };
        // Preserve the first (usually configured) sizes for this layout. More
        // than sixteen queried sizes still get an exact uncached report.
        for entry in &self.allocatable_report_cache {
            if entry.set(cached).is_ok() {
                break;
            }
        }
        report
    }

    fn clear_allocatable_report_cache(&mut self) {
        for entry in &mut self.allocatable_report_cache {
            entry.take();
        }
    }

    /// Independent baseline for tests: reconstruct free extents from all live
    /// allocation identities rather than trusting the allocator report/cache.
    #[cfg(test)]
    fn allocatable_report_from_allocations(&self, slot_size: u64) -> OwnerSegmentAllocatableReport {
        let raw_free_bytes = self.total_free_bytes();
        if slot_size == 0
            || slot_size > self.len
            || slot_size % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        {
            return OwnerSegmentAllocatableReport {
                raw_free_bytes,
                slot_unallocatable_bytes: raw_free_bytes,
                ..Default::default()
            };
        }

        let mut cursor = 0u64;
        let mut exact_free_bytes = 0u64;
        let mut allocatable_slots = 0u64;
        for (offset_units, allocation_id) in &self.allocations_by_offset {
            let allocation = self
                .allocations
                .get(allocation_id)
                .expect("owner segment offset index references a missing allocation");
            let allocation_start = crate::owner_segment_offset_bytes(*offset_units)
                .expect("owner segment allocation offset overflow");
            assert!(
                allocation_start >= cursor,
                "local-reserve allocation intervals overlap"
            );
            let free_extent = allocation_start - cursor;
            exact_free_bytes = exact_free_bytes.saturating_add(free_extent);
            allocatable_slots = allocatable_slots.saturating_add(free_extent / slot_size);
            cursor = allocation_start
                .checked_add(allocation.capacity_bytes)
                .expect("owner segment allocation end overflow");
        }
        assert!(cursor <= self.len, "owner allocation exceeds segment");
        let trailing_free = self.len - cursor;
        exact_free_bytes = exact_free_bytes.saturating_add(trailing_free);
        allocatable_slots = allocatable_slots.saturating_add(trailing_free / slot_size);
        assert_eq!(
            exact_free_bytes, raw_free_bytes,
            "owner allocation index diverged from OffsetAllocator free accounting"
        );

        let allocatable_bytes = allocatable_slots.saturating_mul(slot_size);
        OwnerSegmentAllocatableReport {
            raw_free_bytes,
            allocatable_slots,
            allocatable_bytes,
            slot_unallocatable_bytes: raw_free_bytes.saturating_sub(allocatable_bytes),
        }
    }

    fn global_committed_bytes(&self) -> u64 {
        self.allocations
            .iter()
            .filter_map(|(allocation_id, allocation)| {
                let manifest_key = self.manifest_key_by_allocation.get(allocation_id)?;
                let manifest = self.manifest_by_key.get(manifest_key)?;
                (manifest.disposition == crate::owner_segment::OwnerTargetDisposition::GlobalShared)
                    .then_some(allocation.capacity_bytes)
            })
            .fold(0u64, u64::saturating_add)
    }

    fn global_reserved_demotion_bytes(&self) -> u64 {
        self.global_demotion_reservations
            .values()
            .map(|slot| slot.capacity_bytes)
            .fold(0u64, u64::saturating_add)
    }

    fn global_accounted_bytes(&self) -> u64 {
        #[cfg(test)]
        self.global_accounting_scans.fetch_add(1, Ordering::Relaxed);
        self.global_committed_bytes()
            .saturating_add(self.global_reserved_demotion_bytes())
    }

    fn try_reserve_global_demotion(
        &mut self,
        local_target_bytes: u64,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
        global_accounted_bytes: &mut u64,
    ) -> Result<bool, crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{
            OwnerManifestKey, OwnerSlotPhysicalState, OwnerSlotScope, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let manifest_key = OwnerManifestKey::new(key, put_id);
        let manifest = self.manifest_by_key.get(&manifest_key).ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "owner manifest entry is absent during GlobalShared reservation",
            )
        })?;
        if manifest.slot.allocation_id != allocation_id
            || manifest.slot.segment_offset != segment_offset
            || manifest.slot.capacity_bytes != capacity_bytes
            || manifest.physical_state != OwnerSlotPhysicalState::Committed
        {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "owner manifest slot changed during GlobalShared reservation",
            ));
        }
        if manifest.scope == Some(OwnerSlotScope::GlobalShared) {
            return Ok(true);
        }
        if manifest.scope != Some(OwnerSlotScope::LocalExclusive) {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "only a committed LocalExclusive slot can reserve GlobalShared budget",
            ));
        }
        let slot = manifest.slot.clone();
        if let Some(existing) = self.global_demotion_reservations.get(&manifest_key) {
            if existing.geometry_matches(&slot)
                && existing.len == slot.len
                && existing.segment_registration_epoch == slot.segment_registration_epoch
            {
                return Ok(true);
            }
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "GlobalShared reservation replay changed the exact owner slot",
            ));
        }
        let global_target_bytes = self.len.saturating_sub(local_target_bytes);
        let Some(next_global_accounted_bytes) = global_accounted_bytes.checked_add(capacity_bytes)
        else {
            return Ok(false);
        };
        if next_global_accounted_bytes > global_target_bytes {
            return Ok(false);
        }
        self.global_demotion_reservations.insert(manifest_key, slot);
        *global_accounted_bytes = next_global_accounted_bytes;
        Ok(true)
    }

    fn allocation_size_classes(&self) -> impl Iterator<Item = u64> + '_ {
        self.allocations
            .values()
            .map(|allocation| allocation.capacity_bytes)
    }
}

/// Owner-side authority for the complete DRAM segment.  The optional inner
/// state exists only until segment registration finishes; it is never split
/// into independently owned sub-pools.
#[derive(Debug, Default)]
pub(crate) struct OwnerSegmentAllocator {
    segment: Option<OwnerSegmentState>,
    next_allocation_id: u64,
    next_lease_sequence: u64,
    prepared_slots: usize,
    pending_visible_slots: usize,
    committed_slots: usize,
    pub pending_demand_by_slot_size: HashMap<u64, usize>,
    /// A pressure round is authorized only after this exact size class has
    /// attempted and failed a real claim.
    failed_claim_slot_sizes: HashSet<u64>,
    /// Monotonic progress marker used only to reset pressure backoff. Partial
    /// claims that are rolled back do not advance it.
    pub claim_progress_epoch: u64,
    /// Logical local-exclusive target. Physical ownership always remains the
    /// full segment; global-shared capacity is the complement.
    pub local_target_bytes: u64,
    pub controller_epoch: u64,
    pub expected_slot_size: Option<u64>,
    /// Size classes requested by the master through capacity-report replies.
    /// This is allocator metadata only; it does not reserve physical bytes.
    capacity_report_requested_size_classes: std::collections::BTreeSet<u64>,
}

impl OwnerSegmentAllocator {
    pub fn install_segment(
        &mut self,
        owner: crate::owner_segment::OwnerGeneration,
        registration_epoch: u64,
        base_addr: u64,
        addr: u64,
        len: u64,
        local_target_bytes: u64,
        expected_slot_size: Option<u64>,
    ) -> Result<(), String> {
        if self.segment.is_some() {
            return Err("owner segment allocator must be installed exactly once".to_string());
        }
        if local_target_bytes > len {
            return Err(format!(
                "owner local target must be in [0, segment_bytes]: target={} segment={}",
                local_target_bytes, len
            ));
        }
        self.segment = Some(OwnerSegmentState::new(
            owner,
            registration_epoch,
            base_addr,
            addr,
            len,
        )?);
        self.local_target_bytes = local_target_bytes;
        self.controller_epoch = 1;
        self.expected_slot_size = expected_slot_size;
        self.capacity_report_requested_size_classes.clear();
        self.capacity_report_requested_size_classes
            .extend(expected_slot_size);
        Ok(())
    }

    #[cfg(test)]
    pub fn install_test_segment(
        &mut self,
        owner_id: &str,
        base_addr: u64,
        addr: u64,
        len: u64,
        local_target_bytes: u64,
        expected_slot_size: Option<u64>,
    ) -> Result<(), String> {
        self.install_segment(
            crate::owner_segment::OwnerGeneration::for_test(owner_id),
            1,
            base_addr,
            addr,
            len,
            local_target_bytes,
            expected_slot_size,
        )
    }

    pub fn apply_target_control(
        &mut self,
        controller_epoch: u64,
        local_target_bytes: u64,
    ) -> Result<bool, String> {
        let segment_bytes = self
            .segment
            .as_ref()
            .map(|segment| segment.len)
            .ok_or_else(|| "owner segment allocator is not initialized".to_string())?;
        if self.controller_epoch == 0 {
            return Err("owner scope budget is not initialized".to_string());
        }
        if controller_epoch == self.controller_epoch {
            if local_target_bytes != self.local_target_bytes {
                return Err(format!(
                    "controller epoch {} was replayed with a different local target: current={} requested={}",
                    controller_epoch, self.local_target_bytes, local_target_bytes
                ));
            }
            return Ok(false);
        }
        let next_epoch = self
            .controller_epoch
            .checked_add(1)
            .ok_or_else(|| "owner local-reserve controller epoch exhausted".to_string())?;
        if controller_epoch != next_epoch {
            return Err(format!(
                "owner local-reserve controller epoch must advance exactly once: current={} requested={}",
                self.controller_epoch, controller_epoch
            ));
        }
        if local_target_bytes == 0 || local_target_bytes > segment_bytes {
            return Err(format!(
                "inference owner local target must be in [1, {}], got {}",
                segment_bytes, local_target_bytes,
            ));
        }
        self.local_target_bytes = local_target_bytes;
        self.controller_epoch = controller_epoch;
        Ok(true)
    }

    fn claim_available_with_len(
        &mut self,
        slot_size: u64,
        value_len: u64,
        max_slots: usize,
    ) -> Vec<OwnerSlotRef> {
        let mut slots = Vec::with_capacity(max_slots);
        while slots.len() < max_slots {
            let Some(segment) = self.segment.as_mut() else {
                break;
            };
            self.next_allocation_id = self
                .next_allocation_id
                .checked_add(1)
                .expect("owner allocation id exhausted");
            let allocation_id = self.next_allocation_id;
            let Some(slot) = segment.claim_prepared_slot(slot_size, value_len, allocation_id)
            else {
                break;
            };
            self.prepared_slots = self
                .prepared_slots
                .checked_add(1)
                .expect("prepared slot overflow");
            slots.push(slot);
        }
        slots
    }

    pub fn claim_value(&mut self, value_len: u64, max_slots: usize) -> Vec<OwnerSlotRef> {
        let Some(slot_size) = crate::owner_segment_allocation_capacity_bytes(value_len) else {
            return Vec::new();
        };
        self.claim_available_with_len(slot_size, value_len, max_slots)
    }

    #[cfg(test)]
    pub fn claim_available(&mut self, slot_size: u64, max_slots: usize) -> Vec<OwnerSlotRef> {
        self.claim_available_with_len(slot_size, slot_size, max_slots)
    }

    fn allocate_lease_id(
        &mut self,
    ) -> Result<crate::owner_segment::OwnerLeaseId, crate::owner_segment::OwnerTransferItemError>
    {
        use crate::owner_segment::{OwnerTransferErrorCode, OwnerTransferItemError};

        let owner = self
            .segment
            .as_ref()
            .map(|segment| segment.owner.clone())
            .ok_or_else(|| {
                OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Busy,
                    "owner segment allocator is not initialized",
                )
            })?;
        self.next_lease_sequence = self.next_lease_sequence.checked_add(1).ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::Internal,
                "owner lease sequence exhausted",
            )
        })?;
        Ok(crate::owner_segment::OwnerLeaseId {
            owner,
            sequence: self.next_lease_sequence,
        })
    }

    pub fn acquire_source(
        &mut self,
        op_id: crate::owner_segment::OwnerTransferOpId,
        route_token: crate::owner_segment::OwnerSourceRouteToken,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::OwnerSegmentTransferOutcome;

        let existing_lease = self.segment.as_ref().and_then(|segment| {
            segment
                .source_leases
                .get(&op_id)
                .map(|record| record.lease_id.clone())
        });
        let lease_id = match existing_lease {
            Some(lease_id) => lease_id,
            None => match self.allocate_lease_id() {
                Ok(lease_id) => lease_id,
                Err(error) => return OwnerSegmentTransferOutcome::Error(error),
            },
        };
        let Some(segment) = self.segment.as_mut() else {
            unreachable!("allocate_lease_id verified the owner segment")
        };
        segment.acquire_source(op_id, route_token, lease_id)
    }

    pub fn release_source(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        outcome: crate::owner_segment::OwnerTransferOutcome,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        let Some(segment) = self.segment.as_mut() else {
            return crate::owner_segment::OwnerSegmentTransferOutcome::Error(
                crate::owner_segment::OwnerTransferItemError::new(
                    crate::owner_segment::OwnerTransferErrorCode::Busy,
                    "owner segment allocator is not initialized",
                ),
            );
        };
        let allocations_before = segment.allocations.len();
        let result = segment.release_source(op_id, lease_id, outcome);
        if segment.allocations.len() < allocations_before {
            self.committed_slots = self
                .committed_slots
                .checked_sub(1)
                .expect("committed slot counter underflow after source release");
        }
        result
    }

    pub fn prepare_target(
        &mut self,
        op_id: crate::owner_segment::OwnerTransferOpId,
        expected_target: crate::owner_segment::OwnerGeneration,
        key: String,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        len: u64,
        disposition: crate::owner_segment::OwnerTargetDisposition,
        atomic_batch: Option<crate::master_kv_router::msg_pack::PutAtomicGroup>,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotManifestEntry, OwnerSlotPhysicalState,
            OwnerSlotScope, OwnerTargetLeaseStateView, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_ref() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        if expected_target != segment.owner {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::StaleGeneration,
                "PrepareTarget expected another owner generation",
            ));
        }
        if let Some(existing) = segment.target_leases.get(&op_id) {
            if existing.key != key
                || existing.put_id != put_id
                || existing.len != len
                || existing.disposition != disposition
                || existing.atomic_batch != atomic_batch
            {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "PrepareTarget replay changed operation parameters",
                ));
            }
            return match &existing.state {
                OwnerTargetLeaseState::Prepared => OwnerSegmentTransferOutcome::TargetPrepared {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                    state: OwnerTargetLeaseStateView::Prepared,
                },
                OwnerTargetLeaseState::DataReady { .. } => {
                    OwnerSegmentTransferOutcome::TargetDataReady {
                        lease_id: existing.lease_id.clone(),
                        slot: existing.slot.clone(),
                    }
                }
                OwnerTargetLeaseState::RoutePending { .. } => {
                    OwnerSegmentTransferOutcome::TargetCommitPending {
                        lease_id: existing.lease_id.clone(),
                        slot: existing.slot.clone(),
                    }
                }
                OwnerTargetLeaseState::Committed { route_epoch } => {
                    OwnerSegmentTransferOutcome::TargetCommitted {
                        lease_id: existing.lease_id.clone(),
                        slot: existing.slot.clone(),
                        route_epoch: *route_epoch,
                    }
                }
                OwnerTargetLeaseState::Aborted { .. } => OwnerSegmentTransferOutcome::TargetAborted,
            };
        }
        if !op_id.is_initialized() || key.is_empty() || len == 0 {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "PrepareTarget requires initialized operation, non-empty key and non-zero length",
            ));
        }
        if segment.manifest_entry(&key, put_id).is_some() {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "PrepareTarget key generation already has an owner manifest entry",
            ));
        }

        let Some(slot_size) = crate::owner_segment_allocation_capacity_bytes(len) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                format!("PrepareTarget length cannot be represented: {len}"),
            ));
        };
        if disposition == crate::owner_segment::OwnerTargetDisposition::GlobalShared {
            let global_target_bytes = segment.len.saturating_sub(self.local_target_bytes);
            let global_accounted_bytes = segment.global_accounted_bytes();
            if global_accounted_bytes
                .checked_add(slot_size)
                .is_none_or(|next| next > global_target_bytes)
            {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::NoSpace,
                    format!(
                        "PrepareTarget GlobalShared budget is full: accounted={} requested={} target={}",
                        global_accounted_bytes, slot_size, global_target_bytes,
                    ),
                ));
            }
        }

        let mut slots = self.claim_available_with_len(slot_size, len, 1);
        let Some(slot) = slots.pop() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NoSpace,
                format!("PrepareTarget cannot allocate {} bytes", len),
            ));
        };
        let lease_id = match self.allocate_lease_id() {
            Ok(lease_id) => lease_id,
            Err(error) => {
                assert!(self.release_prepared_slot(
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                ));
                return OwnerSegmentTransferOutcome::Error(error);
            }
        };
        let scope = match disposition {
            crate::owner_segment::OwnerTargetDisposition::LocalExclusive => {
                Some(OwnerSlotScope::LocalExclusive)
            }
            crate::owner_segment::OwnerTargetDisposition::GlobalShared => {
                Some(OwnerSlotScope::GlobalShared)
            }
            crate::owner_segment::OwnerTargetDisposition::EphemeralCaller
            | crate::owner_segment::OwnerTargetDisposition::TransientSsdRead => None,
        };
        let entry = OwnerSlotManifestEntry {
            key: key.clone(),
            put_id,
            slot: slot.clone(),
            scope,
            disposition,
            route_epoch: 0,
            physical_state: OwnerSlotPhysicalState::Reserved,
        };
        let segment = self
            .segment
            .as_mut()
            .expect("target allocation requires an installed segment");
        if let Err(error) = segment.install_manifest_entry(entry) {
            segment.release_prepared_slot(slot.allocation_id);
            self.prepared_slots = self
                .prepared_slots
                .checked_sub(1)
                .expect("prepared slot counter underflow");
            return OwnerSegmentTransferOutcome::Error(error);
        }
        segment.target_leases.insert(
            op_id,
            OwnerTargetLeaseRecord {
                lease_id: lease_id.clone(),
                key,
                put_id,
                len,
                disposition,
                atomic_batch,
                slot: slot.clone(),
                state: OwnerTargetLeaseState::Prepared,
            },
        );
        OwnerSegmentTransferOutcome::TargetPrepared {
            lease_id,
            slot,
            state: OwnerTargetLeaseStateView::Prepared,
        }
    }

    /// Bind an already-claimed requester-local slot to one Get operation.
    ///
    /// The pressure path remains the sole allocator admission authority. This
    /// method only turns that exact Prepared allocation into a generation-safe
    /// target lease before its capability is sent to a source owner.
    pub fn prepare_claimed_get_target(
        &mut self,
        op_id: crate::owner_segment::OwnerTransferOpId,
        key: String,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        atomic_batch: Option<crate::master_kv_router::msg_pack::PutAtomicGroup>,
        slot: OwnerSlotRef,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotManifestEntry, OwnerSlotPhysicalState,
            OwnerSlotScope, OwnerTargetDisposition, OwnerTargetLeaseStateView,
            OwnerTransferErrorCode, OwnerTransferItemError, OwnerTransferOpKind,
        };

        let Some(segment) = self.segment.as_ref() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        if let Some(existing) = segment.target_leases.get(&op_id) {
            if existing.key != key
                || existing.put_id != put_id
                || existing.len != slot.len
                || existing.disposition != OwnerTargetDisposition::LocalExclusive
                || existing.atomic_batch != atomic_batch
                || existing.slot != slot
            {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "claimed Get target replay changed operation parameters",
                ));
            }
            return match &existing.state {
                OwnerTargetLeaseState::Prepared => OwnerSegmentTransferOutcome::TargetPrepared {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                    state: OwnerTargetLeaseStateView::Prepared,
                },
                OwnerTargetLeaseState::DataReady { .. } => {
                    OwnerSegmentTransferOutcome::TargetDataReady {
                        lease_id: existing.lease_id.clone(),
                        slot: existing.slot.clone(),
                    }
                }
                OwnerTargetLeaseState::RoutePending { .. } => {
                    OwnerSegmentTransferOutcome::TargetCommitPending {
                        lease_id: existing.lease_id.clone(),
                        slot: existing.slot.clone(),
                    }
                }
                OwnerTargetLeaseState::Committed { route_epoch } => {
                    OwnerSegmentTransferOutcome::TargetCommitted {
                        lease_id: existing.lease_id.clone(),
                        slot: existing.slot.clone(),
                        route_epoch: *route_epoch,
                    }
                }
                OwnerTargetLeaseState::Aborted { .. } => OwnerSegmentTransferOutcome::TargetAborted,
            };
        }
        if !op_id.is_initialized()
            || op_id.kind != OwnerTransferOpKind::Get
            || key.is_empty()
            || !slot.is_valid()
            || slot.owner != segment.owner
            || slot.len == 0
            || !segment.identity_matches_desc(&slot)
            || !segment
                .allocation(slot.allocation_id)
                .is_some_and(|allocation| matches!(allocation.state, OwnerSlotState::Prepared))
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "claimed Get target is not the exact live Prepared owner slot",
            ));
        }
        let key_generation_conflict = segment.manifest_entry(&key, put_id).is_some();
        let allocation_identity_conflict = segment
            .manifest_key_by_allocation
            .contains_key(&slot.allocation_id);
        if key_generation_conflict && !allocation_identity_conflict {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::StalePlan,
                format!(
                    "claimed Get target became stale because the exact key generation materialized locally: allocation_id={}",
                    slot.allocation_id
                ),
            ));
        }
        if allocation_identity_conflict {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                format!(
                    "claimed Get target already belongs to an owner manifest: key_generation_conflict={} allocation_identity_conflict={} allocation_id={}",
                    key_generation_conflict, allocation_identity_conflict, slot.allocation_id
                ),
            ));
        }

        let lease_id = match self.allocate_lease_id() {
            Ok(lease_id) => lease_id,
            Err(error) => return OwnerSegmentTransferOutcome::Error(error),
        };
        let entry = OwnerSlotManifestEntry {
            key: key.clone(),
            put_id,
            slot: slot.clone(),
            scope: Some(OwnerSlotScope::LocalExclusive),
            disposition: OwnerTargetDisposition::LocalExclusive,
            route_epoch: 0,
            physical_state: OwnerSlotPhysicalState::Reserved,
        };
        let segment = self
            .segment
            .as_mut()
            .expect("claimed Get target validation requires an installed segment");
        if let Err(error) = segment.install_manifest_entry(entry) {
            return OwnerSegmentTransferOutcome::Error(error);
        }
        segment.target_leases.insert(
            op_id,
            OwnerTargetLeaseRecord {
                lease_id: lease_id.clone(),
                key,
                put_id,
                len: slot.len,
                disposition: OwnerTargetDisposition::LocalExclusive,
                atomic_batch,
                slot: slot.clone(),
                state: OwnerTargetLeaseState::Prepared,
            },
        );
        OwnerSegmentTransferOutcome::TargetPrepared {
            lease_id,
            slot,
            state: OwnerTargetLeaseStateView::Prepared,
        }
    }

    /// Record a source-generated completion receipt before exposing or
    /// publishing the target. Replays must present the same exact receipt.
    pub fn mark_target_data_ready(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        receipt: crate::owner_segment::OwnerTransferReceipt,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "Get target operation is absent",
            ));
        };
        if &existing.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get target lease identity mismatch",
            ));
        }
        match &existing.state {
            OwnerTargetLeaseState::DataReady {
                receipt: previous_receipt,
            } => {
                if previous_receipt != &receipt {
                    return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Conflict,
                        "Get target replay changed the transfer receipt",
                    ));
                }
                return OwnerSegmentTransferOutcome::TargetDataReady {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                };
            }
            OwnerTargetLeaseState::Committed { route_epoch } => {
                return OwnerSegmentTransferOutcome::TargetCommitted {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                    route_epoch: *route_epoch,
                };
            }
            OwnerTargetLeaseState::RoutePending { .. } => {
                return OwnerSegmentTransferOutcome::TargetCommitPending {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                };
            }
            OwnerTargetLeaseState::Aborted { .. } => {
                return OwnerSegmentTransferOutcome::TargetAborted;
            }
            OwnerTargetLeaseState::Prepared => {}
        }
        if receipt.completion_id == 0
            || receipt.bytes != existing.len
            || receipt.target != existing.slot
            || receipt.target_registration_epoch != existing.slot.segment_registration_epoch
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "Get receipt does not prove completion into the exact target lease",
            ));
        }
        let allocation_id = existing.slot.allocation_id;
        let response_lease = existing.lease_id.clone();
        let response_slot = existing.slot.clone();
        segment
            .target_leases
            .get_mut(op_id)
            .expect("validated Get target lease disappeared")
            .state = OwnerTargetLeaseState::DataReady { receipt };
        segment
            .manifest_entry_mut_by_allocation(allocation_id)
            .expect("prepared Get target must have a manifest entry")
            .physical_state = OwnerSlotPhysicalState::DataReady;
        OwnerSegmentTransferOutcome::TargetDataReady {
            lease_id: response_lease,
            slot: response_slot,
        }
    }

    pub fn begin_target_commit(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        receipt: crate::owner_segment::OwnerTransferReceipt,
        route_token: Option<crate::owner_segment::OwnerTargetRouteToken>,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTargetDisposition,
            OwnerTransferErrorCode, OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "CommitTarget operation is absent",
            ));
        };
        if &existing.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "CommitTarget lease identity mismatch",
            ));
        }
        match &existing.state {
            OwnerTargetLeaseState::Committed { route_epoch } => {
                return OwnerSegmentTransferOutcome::TargetCommitted {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                    route_epoch: *route_epoch,
                };
            }
            OwnerTargetLeaseState::Aborted { .. } => {
                return OwnerSegmentTransferOutcome::TargetAborted;
            }
            OwnerTargetLeaseState::RoutePending {
                receipt: previous_receipt,
                route_token: previous_token,
            } => {
                if previous_receipt != &receipt || previous_token != &route_token {
                    return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Conflict,
                        "CommitTarget replay changed receipt or route token",
                    ));
                }
                return OwnerSegmentTransferOutcome::TargetCommitPending {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                };
            }
            OwnerTargetLeaseState::Prepared | OwnerTargetLeaseState::DataReady { .. } => {}
        }
        if receipt.completion_id == 0
            || receipt.bytes != existing.len
            || receipt.target != existing.slot
            || receipt.target_registration_epoch != existing.slot.segment_registration_epoch
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "CommitTarget receipt does not prove completion into the exact target slot",
            ));
        }
        let persistent = matches!(
            existing.disposition,
            OwnerTargetDisposition::LocalExclusive | OwnerTargetDisposition::GlobalShared
        );
        if persistent {
            let Some(token) = route_token.as_ref() else {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::InvalidArgument,
                    "persistent CommitTarget requires a master route token",
                ));
            };
            if token.operation != *op_id
                || token.key != existing.key
                || token.put_id != existing.put_id
                || token.target_owner != existing.slot.owner
                || token.atomic_batch != existing.atomic_batch
                || token.plan_nonce == 0
            {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "CommitTarget route token does not match the prepared target",
                ));
            }
        } else if route_token.is_some() {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "ephemeral target must not publish a master route",
            ));
        }
        let allocation_id = existing.slot.allocation_id;
        let response_lease = existing.lease_id.clone();
        let response_slot = existing.slot.clone();
        let record = segment
            .target_leases
            .get_mut(op_id)
            .expect("validated target lease disappeared");
        record.state = OwnerTargetLeaseState::RoutePending {
            receipt,
            route_token,
        };
        let manifest = segment
            .manifest_entry_mut_by_allocation(allocation_id)
            .expect("prepared target must have a manifest entry");
        manifest.physical_state = OwnerSlotPhysicalState::RoutePending;
        OwnerSegmentTransferOutcome::TargetCommitPending {
            lease_id: response_lease,
            slot: response_slot,
        }
    }

    pub fn finish_target_commit(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        route_epoch: u64,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTargetDisposition,
            OwnerTransferErrorCode, OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "target commit operation is absent",
            ));
        };
        if &existing.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "target commit lease identity mismatch",
            ));
        }
        if let OwnerTargetLeaseState::Committed {
            route_epoch: previous_epoch,
        } = existing.state
        {
            if previous_epoch != route_epoch {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "target commit replay changed route epoch",
                ));
            }
            return OwnerSegmentTransferOutcome::TargetCommitted {
                lease_id: existing.lease_id.clone(),
                slot: existing.slot.clone(),
                route_epoch,
            };
        }
        if !matches!(existing.state, OwnerTargetLeaseState::RoutePending { .. }) {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "target is not waiting for route commit",
            ));
        }
        let persistent = matches!(
            existing.disposition,
            OwnerTargetDisposition::LocalExclusive | OwnerTargetDisposition::GlobalShared
        );
        if persistent != (route_epoch != 0) {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "persistent targets require a non-zero route epoch; ephemeral targets require zero",
            ));
        }
        let allocation_id = existing.slot.allocation_id;
        let response_lease = existing.lease_id.clone();
        let response_slot = existing.slot.clone();
        let disposition = existing.disposition;
        let allocation = segment
            .allocation_mut(allocation_id)
            .expect("target commit lost its allocation");
        if !matches!(allocation.state, OwnerSlotState::Prepared) {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "target allocation left Prepared before commit",
            ));
        }
        allocation.state = OwnerSlotState::Committed {
            route_live: persistent,
            holder_ref_count: u32::from(!persistent),
        };
        let manifest = segment
            .manifest_entry_mut_by_allocation(allocation_id)
            .expect("target commit lost its manifest entry");
        manifest.physical_state = OwnerSlotPhysicalState::Committed;
        manifest.route_epoch = route_epoch;
        let record = segment
            .target_leases
            .get_mut(op_id)
            .expect("target commit record disappeared");
        record.state = OwnerTargetLeaseState::Committed { route_epoch };
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        self.committed_slots = self
            .committed_slots
            .checked_add(1)
            .expect("committed slot overflow");
        let _ = disposition;
        OwnerSegmentTransferOutcome::TargetCommitted {
            lease_id: response_lease,
            slot: response_slot,
            route_epoch,
        }
    }

    /// Complete the requester-local half of a Get after master GetDone has
    /// published the route. The payload was already proven DataReady by the
    /// source terminal; this transition only changes owner metadata/state.
    pub fn begin_data_ready_get_target_commit(
        &mut self,
        capability: &crate::owner_segment::OwnerTargetWriteCapability,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTransferErrorCode,
            OwnerTransferItemError, OwnerTransferOpKind,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(&capability.operation) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "Get CommitTarget operation is absent",
            ));
        };
        if capability.operation.kind != OwnerTransferOpKind::Get
            || existing.lease_id != capability.lease_id
            || existing.slot != capability.slot
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get CommitTarget capability identity mismatch",
            ));
        }
        match &existing.state {
            OwnerTargetLeaseState::Committed { route_epoch } => {
                return OwnerSegmentTransferOutcome::TargetCommitted {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                    route_epoch: *route_epoch,
                };
            }
            OwnerTargetLeaseState::RoutePending { route_token, .. } => {
                if route_token.is_some() {
                    return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Conflict,
                        "Get CommitTarget replay found a Put route token",
                    ));
                }
                return OwnerSegmentTransferOutcome::TargetCommitPending {
                    lease_id: existing.lease_id.clone(),
                    slot: existing.slot.clone(),
                };
            }
            OwnerTargetLeaseState::Aborted { .. } => {
                return OwnerSegmentTransferOutcome::TargetAborted;
            }
            OwnerTargetLeaseState::Prepared => {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::RouteCommitRequired,
                    "Get CommitTarget cannot start before payload DataReady",
                ));
            }
            OwnerTargetLeaseState::DataReady { .. } => {}
        }
        if !segment
            .allocation(existing.slot.allocation_id)
            .is_some_and(|allocation| {
                matches!(allocation.state, OwnerSlotState::PendingLocalVisible { .. })
            })
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get CommitTarget requires its hidden PendingLocalVisible slot",
            ));
        }
        let receipt = match &existing.state {
            OwnerTargetLeaseState::DataReady { receipt } => receipt.clone(),
            _ => unreachable!("validated DataReady Get target"),
        };
        let allocation_id = existing.slot.allocation_id;
        let response_lease = existing.lease_id.clone();
        let response_slot = existing.slot.clone();
        segment
            .target_leases
            .get_mut(&capability.operation)
            .expect("validated Get target lease disappeared")
            .state = OwnerTargetLeaseState::RoutePending {
            receipt,
            route_token: None,
        };
        segment
            .manifest_entry_mut_by_allocation(allocation_id)
            .expect("DataReady Get target must have a manifest entry")
            .physical_state = OwnerSlotPhysicalState::RoutePending;
        OwnerSegmentTransferOutcome::TargetCommitPending {
            lease_id: response_lease,
            slot: response_slot,
        }
    }

    pub fn finish_data_ready_get_target(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        route_epoch: u64,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "Get target commit operation is absent",
            ));
        };
        if &existing.lease_id != lease_id
            || op_id.kind != crate::owner_segment::OwnerTransferOpKind::Get
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get target commit identity mismatch",
            ));
        }
        if let OwnerTargetLeaseState::Committed {
            route_epoch: previous_epoch,
        } = existing.state
        {
            if previous_epoch != route_epoch {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "Get target commit replay changed route epoch",
                ));
            }
            return OwnerSegmentTransferOutcome::TargetCommitted {
                lease_id: existing.lease_id.clone(),
                slot: existing.slot.clone(),
                route_epoch,
            };
        }
        let state_can_commit = matches!(
            existing.state,
            OwnerTargetLeaseState::DataReady { .. }
                | OwnerTargetLeaseState::RoutePending {
                    route_token: None,
                    ..
                }
        );
        if route_epoch == 0 || !state_can_commit {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::RouteCommitRequired,
                "Get target is not DataReady/RoutePending or lacks a committed route epoch",
            ));
        }
        let allocation_id = existing.slot.allocation_id;
        let response_lease = existing.lease_id.clone();
        let response_slot = existing.slot.clone();
        if !segment.allocation(allocation_id).is_some_and(|allocation| {
            matches!(allocation.state, OwnerSlotState::PendingLocalVisible { .. })
        }) {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get target must be hidden PendingLocalVisible before commit",
            ));
        }
        segment.promote_pending_visible_slot_to_committed(allocation_id);
        let manifest = segment
            .manifest_entry_mut_by_allocation(allocation_id)
            .expect("DataReady Get target must have a manifest entry");
        manifest.physical_state = OwnerSlotPhysicalState::Committed;
        manifest.route_epoch = route_epoch;
        segment
            .target_leases
            .get_mut(op_id)
            .expect("validated Get target lease disappeared")
            .state = OwnerTargetLeaseState::Committed { route_epoch };
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_sub(1)
            .expect("pending-visible slot counter underflow");
        self.committed_slots = self
            .committed_slots
            .checked_add(1)
            .expect("committed slot overflow");
        OwnerSegmentTransferOutcome::TargetCommitted {
            lease_id: response_lease,
            slot: response_slot,
            route_epoch,
        }
    }

    /// Abort an unpublished Get target after it has become hidden resident
    /// state.  DataReady is used by the synchronous error path; RoutePending
    /// with no Put route token is used when an asynchronous Get receives an
    /// explicit master rejection.  The final MemoryInfo holder drop performs
    /// the physical free, so a caller already consuming Async-Get bytes remains
    /// safe; this method only closes the operation and manifest.
    pub fn abort_data_ready_visible_get_target(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        reason: String,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerSlotPhysicalState, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "Get target abort operation is absent",
            ));
        };
        if &existing.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get target abort lease identity mismatch",
            ));
        }
        if matches!(existing.state, OwnerTargetLeaseState::Aborted { .. }) {
            return OwnerSegmentTransferOutcome::TargetAborted;
        }
        let is_unpublished_get = matches!(
            existing.state,
            OwnerTargetLeaseState::DataReady { .. }
                | OwnerTargetLeaseState::RoutePending {
                    route_token: None,
                    ..
                }
        );
        if op_id.kind != crate::owner_segment::OwnerTransferOpKind::Get || !is_unpublished_get {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "only an unpublished DataReady/RoutePending Get target can use visible abort",
            ));
        }
        let allocation_id = existing.slot.allocation_id;
        if !segment.allocation(allocation_id).is_some_and(|allocation| {
            matches!(allocation.state, OwnerSlotState::PendingLocalVisible { .. })
        }) {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "visible Get target no longer owns PendingLocalVisible state",
            ));
        }
        segment
            .target_leases
            .get_mut(op_id)
            .expect("validated Get target lease disappeared")
            .state = OwnerTargetLeaseState::Aborted { reason };
        segment
            .manifest_entry_mut_by_allocation(allocation_id)
            .expect("unpublished Get target must have a manifest entry")
            .physical_state = OwnerSlotPhysicalState::Reclaiming;
        OwnerSegmentTransferOutcome::TargetAborted
    }

    pub fn abort_target(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        reason: String,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerTransferErrorCode, OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "AbortTarget operation is absent",
            ));
        };
        if &existing.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "AbortTarget lease identity mismatch",
            ));
        }
        match existing.state {
            OwnerTargetLeaseState::Aborted { .. } => {
                return OwnerSegmentTransferOutcome::TargetAborted;
            }
            OwnerTargetLeaseState::Committed { .. } => {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "AbortTarget cannot revoke a committed target",
                ));
            }
            OwnerTargetLeaseState::RoutePending { .. } => {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::RouteCommitRequired,
                    "RoutePending target requires master terminal reconciliation before abort",
                ));
            }
            OwnerTargetLeaseState::Prepared | OwnerTargetLeaseState::DataReady { .. } => {}
        }
        let allocation_id = existing.slot.allocation_id;
        let record = segment
            .target_leases
            .get_mut(op_id)
            .expect("validated target lease disappeared");
        record.state = OwnerTargetLeaseState::Aborted { reason };
        segment.release_prepared_slot(allocation_id);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        OwnerSegmentTransferOutcome::TargetAborted
    }

    pub fn abort_route_rejected_target(
        &mut self,
        op_id: &crate::owner_segment::OwnerTransferOpId,
        lease_id: &crate::owner_segment::OwnerLeaseId,
        reason: String,
    ) -> crate::owner_segment::OwnerSegmentTransferOutcome {
        use crate::owner_segment::{
            OwnerSegmentTransferOutcome, OwnerTransferErrorCode, OwnerTransferItemError,
        };

        let Some(segment) = self.segment.as_mut() else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            ));
        };
        let Some(existing) = segment.target_leases.get(op_id) else {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "route-rejected target operation is absent",
            ));
        };
        if &existing.lease_id != lease_id {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "route-rejected target lease identity mismatch",
            ));
        }
        match existing.state {
            OwnerTargetLeaseState::Aborted { .. } => {
                return OwnerSegmentTransferOutcome::TargetAborted;
            }
            OwnerTargetLeaseState::RoutePending { .. } => {}
            _ => {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "master route rejection can abort only a RoutePending target",
                ));
            }
        }
        let allocation_id = existing.slot.allocation_id;
        segment
            .target_leases
            .get_mut(op_id)
            .expect("validated target lease disappeared")
            .state = OwnerTargetLeaseState::Aborted { reason };
        segment.release_prepared_slot(allocation_id);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        OwnerSegmentTransferOutcome::TargetAborted
    }

    pub fn install_committed_manifest(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        scope: crate::owner_segment::OwnerSlotScope,
        route_epoch: u64,
    ) -> Result<OwnerSlotRef, crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{
            OwnerSlotManifestEntry, OwnerSlotPhysicalState, OwnerTargetDisposition,
            OwnerTransferErrorCode, OwnerTransferItemError,
        };

        let segment = self.segment.as_mut().ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            )
        })?;
        let slot = segment.slot_desc(allocation_id).ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "committed manifest allocation is absent",
            )
        })?;
        if !matches!(
            segment
                .allocation(allocation_id)
                .map(|allocation| &allocation.state),
            Some(OwnerSlotState::Committed {
                route_live: true,
                ..
            })
        ) {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "committed manifest requires a live committed owner slot",
            ));
        }
        let disposition = match scope {
            crate::owner_segment::OwnerSlotScope::LocalExclusive => {
                OwnerTargetDisposition::LocalExclusive
            }
            crate::owner_segment::OwnerSlotScope::GlobalShared => {
                OwnerTargetDisposition::GlobalShared
            }
        };
        segment.install_manifest_entry(OwnerSlotManifestEntry {
            key: key.to_string(),
            put_id,
            slot: slot.clone(),
            scope: Some(scope),
            disposition,
            route_epoch,
            physical_state: OwnerSlotPhysicalState::Committed,
        })?;
        Ok(slot)
    }

    pub fn manifest_entry(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
    ) -> Option<crate::owner_segment::OwnerSlotManifestEntry> {
        self.segment
            .as_ref()
            .and_then(|segment| segment.manifest_entry(key, put_id))
            .cloned()
    }

    pub fn update_committed_manifest_scope(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
        scope: crate::owner_segment::OwnerSlotScope,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        self.update_committed_manifest_scope_inner(
            key,
            put_id,
            allocation_id,
            segment_offset,
            capacity_bytes,
            scope,
            false,
        )
    }

    /// Restore the sealed a19 one-RPC demotion semantics for an explicitly
    /// gated performance run. This still validates the exact committed slot;
    /// it only bypasses the later per-key GlobalShared budget reservation.
    pub fn commit_legacy_a19_global_demotion(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        self.update_committed_manifest_scope_inner(
            key,
            put_id,
            allocation_id,
            segment_offset,
            capacity_bytes,
            crate::owner_segment::OwnerSlotScope::GlobalShared,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_committed_manifest_scope_inner(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
        scope: crate::owner_segment::OwnerSlotScope,
        allow_unreserved_global: bool,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{
            OwnerSlotPhysicalState, OwnerTargetDisposition, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let segment = self.segment.as_mut().ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            )
        })?;
        let manifest = segment
            .manifest_by_key
            .get_mut(&crate::owner_segment::OwnerManifestKey::new(key, put_id))
            .ok_or_else(|| {
                OwnerTransferItemError::new(
                    OwnerTransferErrorCode::NotFound,
                    "owner manifest entry is absent during scope conversion",
                )
            })?;
        if manifest.slot.allocation_id != allocation_id
            || manifest.slot.segment_offset != segment_offset
            || manifest.slot.capacity_bytes != capacity_bytes
            || manifest.physical_state != OwnerSlotPhysicalState::Committed
        {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "owner manifest slot changed during scope conversion",
            ));
        }
        if scope == crate::owner_segment::OwnerSlotScope::GlobalShared
            && manifest.scope != Some(crate::owner_segment::OwnerSlotScope::GlobalShared)
            && !allow_unreserved_global
        {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "LocalExclusive -> GlobalShared requires an owner budget reservation",
            ));
        }
        manifest.scope = Some(scope);
        manifest.disposition = match scope {
            crate::owner_segment::OwnerSlotScope::LocalExclusive => {
                OwnerTargetDisposition::LocalExclusive
            }
            crate::owner_segment::OwnerSlotScope::GlobalShared => {
                OwnerTargetDisposition::GlobalShared
            }
        };
        Ok(())
    }

    /// Reserve the logical GlobalShared budget for one exact, already-fenced
    /// LocalExclusive slot.  The payload stays in place and no physical bytes
    /// are treated as free.  Replays of the same generation and slot reuse the
    /// reservation; another claimant observes it through
    /// `global_accounted_bytes` while holding this allocator's single mutex.
    pub fn try_reserve_global_demotion(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> Result<bool, crate::owner_segment::OwnerTransferItemError> {
        self.try_reserve_global_demotions(std::iter::once((
            key,
            put_id,
            allocation_id,
            segment_offset,
            capacity_bytes,
        )))
        .pop()
        .expect("one GlobalShared reservation request must return one result")
    }

    /// Reserve a batch under one allocator lock and one accounting snapshot.
    /// Each item is still validated and recorded independently; the shared
    /// running total only avoids rescanning every resident allocation for
    /// every victim in the same RPC batch.
    pub fn try_reserve_global_demotions<'a, I>(
        &mut self,
        requests: I,
    ) -> Vec<Result<bool, crate::owner_segment::OwnerTransferItemError>>
    where
        I: IntoIterator<
            Item = (
                &'a str,
                crate::master_kv_router::put::PutIDForAKey,
                u64,
                u64,
                u64,
            ),
        >,
    {
        use crate::owner_segment::{OwnerTransferErrorCode, OwnerTransferItemError};

        let requests = requests.into_iter().collect::<Vec<_>>();
        let local_target_bytes = self.local_target_bytes;
        let Some(segment) = self.segment.as_mut() else {
            return requests
                .into_iter()
                .map(|_| {
                    Err(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Busy,
                        "owner segment allocator is not initialized",
                    ))
                })
                .collect();
        };
        let mut global_accounted_bytes = segment.global_accounted_bytes();
        requests
            .into_iter()
            .map(
                |(key, put_id, allocation_id, segment_offset, capacity_bytes)| {
                    segment.try_reserve_global_demotion(
                        local_target_bytes,
                        key,
                        put_id,
                        allocation_id,
                        segment_offset,
                        capacity_bytes,
                        &mut global_accounted_bytes,
                    )
                },
            )
            .collect()
    }

    pub fn release_global_demotion_reservation(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        let manifest_key = crate::owner_segment::OwnerManifestKey::new(key, put_id);
        let matches = segment
            .global_demotion_reservations
            .get(&manifest_key)
            .is_some_and(|slot| {
                slot.allocation_id == allocation_id
                    && slot.segment_offset == segment_offset
                    && slot.capacity_bytes == capacity_bytes
            });
        if matches {
            segment.global_demotion_reservations.remove(&manifest_key);
        }
        matches
    }

    /// Validate an already reserved exact slot without scanning capacity or
    /// admitting a new reservation. A committed scope is a valid replay.
    pub fn validate_global_demotion_reservation(
        &self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{
            OwnerManifestKey, OwnerSlotPhysicalState, OwnerSlotScope, OwnerTransferErrorCode,
            OwnerTransferItemError,
        };

        let segment = self.segment.as_ref().ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            )
        })?;
        let manifest_key = OwnerManifestKey::new(key, put_id);
        let manifest = segment.manifest_by_key.get(&manifest_key).ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::NotFound,
                "owner manifest entry is absent while committing GlobalShared reservation",
            )
        })?;
        if manifest.slot.allocation_id != allocation_id
            || manifest.slot.segment_offset != segment_offset
            || manifest.slot.capacity_bytes != capacity_bytes
            || manifest.physical_state != OwnerSlotPhysicalState::Committed
        {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "owner manifest slot changed while committing GlobalShared reservation",
            ));
        }
        if manifest.scope == Some(OwnerSlotScope::GlobalShared) {
            return Ok(());
        }
        let reserved = segment
            .global_demotion_reservations
            .get(&manifest_key)
            .is_some_and(|slot| {
                slot.allocation_id == allocation_id
                    && slot.segment_offset == segment_offset
                    && slot.capacity_bytes == capacity_bytes
            });
        if !reserved {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::NoSpace,
                "GlobalShared scope commit has no exact owner budget reservation",
            ));
        }
        Ok(())
    }

    pub fn commit_reserved_global_demotion(
        &mut self,
        key: &str,
        put_id: crate::master_kv_router::put::PutIDForAKey,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{OwnerManifestKey, OwnerSlotScope, OwnerTargetDisposition};

        self.validate_global_demotion_reservation(
            key,
            put_id,
            allocation_id,
            segment_offset,
            capacity_bytes,
        )?;
        let segment = self
            .segment
            .as_mut()
            .expect("validated owner segment disappeared");
        let manifest_key = OwnerManifestKey::new(key, put_id);
        let manifest = segment
            .manifest_by_key
            .get_mut(&manifest_key)
            .expect("validated GlobalShared manifest disappeared");
        manifest.scope = Some(OwnerSlotScope::GlobalShared);
        manifest.disposition = OwnerTargetDisposition::GlobalShared;
        segment.global_demotion_reservations.remove(&manifest_key);
        Ok(())
    }

    pub fn record_failed_claim(&mut self, slot_size: u64) {
        self.failed_claim_slot_sizes.insert(slot_size);
    }

    pub fn record_successful_claim(&mut self, slot_size: u64) {
        self.claim_progress_epoch = self.claim_progress_epoch.saturating_add(1);
        self.failed_claim_slot_sizes.remove(&slot_size);
    }

    pub fn slot_size_has_failed_claim(&self, slot_size: u64) -> bool {
        self.failed_claim_slot_sizes.contains(&slot_size)
    }

    pub fn clear_failed_claim(&mut self, slot_size: u64) {
        self.failed_claim_slot_sizes.remove(&slot_size);
    }

    pub fn release_prepared_slot(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.release_prepared_slot(allocation_id);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        true
    }

    pub fn committed_route_only_matches(
        &self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        self.segment.as_ref().is_some_and(|segment| {
            segment.committed_route_only_matches(allocation_id, segment_offset, capacity_bytes)
        })
    }

    pub fn retain_committed_route_only_holder(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.retain_committed_route_only_holder(allocation_id)
    }

    pub fn mark_prepared_slot_pending_visible(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.mark_prepared_slot_pending_visible(allocation_id);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_add(1)
            .expect("pending-visible slot overflow");
        true
    }

    pub fn mark_data_ready_get_target_pending_visible(
        &mut self,
        capability: &crate::owner_segment::OwnerTargetWriteCapability,
    ) -> Result<(), crate::owner_segment::OwnerTransferItemError> {
        use crate::owner_segment::{OwnerTransferErrorCode, OwnerTransferItemError};

        let segment = self.segment.as_mut().ok_or_else(|| {
            OwnerTransferItemError::new(
                OwnerTransferErrorCode::Busy,
                "owner segment allocator is not initialized",
            )
        })?;
        let record = segment
            .target_leases
            .get(&capability.operation)
            .ok_or_else(|| {
                OwnerTransferItemError::new(
                    OwnerTransferErrorCode::NotFound,
                    "DataReady Get target lease is absent",
                )
            })?;
        if record.lease_id != capability.lease_id
            || record.slot != capability.slot
            || !matches!(record.state, OwnerTargetLeaseState::DataReady { .. })
            || !segment.identity_matches_desc(&capability.slot)
            || !segment
                .allocation(capability.slot.allocation_id)
                .is_some_and(|allocation| matches!(allocation.state, OwnerSlotState::Prepared))
        {
            return Err(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "Get target capability no longer names the exact DataReady Prepared slot",
            ));
        }
        segment.mark_prepared_slot_pending_visible(capability.slot.allocation_id);
        self.prepared_slots = self
            .prepared_slots
            .checked_sub(1)
            .expect("prepared slot counter underflow");
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_add(1)
            .expect("pending-visible slot overflow");
        Ok(())
    }

    pub fn promote_pending_visible_slot_to_committed(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.promote_pending_visible_slot_to_committed(allocation_id);
        self.pending_visible_slots = self
            .pending_visible_slots
            .checked_sub(1)
            .expect("pending-visible slot counter underflow");
        self.committed_slots = self
            .committed_slots
            .checked_add(1)
            .expect("committed slot overflow");
        true
    }

    pub fn retain_resident_slot_holder(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.retain_resident_slot_holder(allocation_id);
        true
    }

    pub fn release_resident_slot_holder(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        let prior_state = segment
            .allocation(allocation_id)
            .expect("validated owner allocation disappeared")
            .state
            .clone();
        segment.release_resident_slot_holder(allocation_id);
        let became_free = segment.allocation(allocation_id).is_none();
        if became_free {
            match prior_state {
                OwnerSlotState::PendingLocalVisible { .. } => {
                    self.pending_visible_slots = self
                        .pending_visible_slots
                        .checked_sub(1)
                        .expect("pending-visible slot counter underflow");
                }
                OwnerSlotState::Committed { .. } => {
                    self.committed_slots = self
                        .committed_slots
                        .checked_sub(1)
                        .expect("committed slot counter underflow");
                }
                OwnerSlotState::Prepared => {
                    unreachable!("resident holder must belong to a resident slot")
                }
            }
        }
        true
    }

    pub fn release_committed_slot_route(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.release_committed_slot_route(allocation_id);
        let became_free = segment.allocation(allocation_id).is_none();
        if became_free {
            self.committed_slots = self
                .committed_slots
                .checked_sub(1)
                .expect("committed slot counter underflow");
        }
        true
    }

    /// Drop the route reference and the resident `MemoryInfo` reference as one
    /// slot-pool transaction. This is the normal owner-reclaim transition and
    /// avoids exposing an intermediate state across two pool lock acquisitions.
    pub fn release_committed_resident_slot(
        &mut self,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    ) -> bool {
        let Some(segment) = self.segment.as_mut() else {
            return false;
        };
        if !segment.identity_matches(allocation_id, segment_offset, capacity_bytes) {
            return false;
        }
        segment.release_committed_resident_slot(allocation_id);
        let became_free = segment.allocation(allocation_id).is_none();
        if became_free {
            self.committed_slots = self
                .committed_slots
                .checked_sub(1)
                .expect("committed slot counter underflow");
        }
        true
    }

    pub fn physical_capacity_bytes(&self) -> u64 {
        self.segment
            .as_ref()
            .map(|segment| segment.len)
            .unwrap_or(0)
    }

    pub fn physical_free_epoch(&self) -> u64 {
        self.segment
            .as_ref()
            .map(|segment| segment.physical_free_epoch)
            .unwrap_or(0)
    }

    pub fn used_slot_count(&self) -> usize {
        self.prepared_slots + self.pending_visible_slots + self.committed_slots
    }

    pub fn prepared_slot_count(&self) -> usize {
        self.prepared_slots
    }

    pub fn pending_visible_slot_count(&self) -> usize {
        self.pending_visible_slots
    }

    pub fn committed_slot_count(&self) -> usize {
        self.committed_slots
    }

    pub fn total_free_bytes(&self) -> u64 {
        self.segment
            .as_ref()
            .map(OwnerSegmentState::total_free_bytes)
            .unwrap_or(0)
    }

    pub fn allocatable_report(&self, slot_size: u64) -> OwnerSegmentAllocatableReport {
        self.segment
            .as_ref()
            .map(|segment| segment.allocatable_report(slot_size))
            .unwrap_or_default()
    }

    pub fn total_used_bytes(&self) -> u64 {
        self.physical_capacity_bytes()
            .saturating_sub(self.total_free_bytes())
    }

    pub fn largest_free_bytes(&self) -> u64 {
        self.segment
            .as_ref()
            .map(OwnerSegmentState::largest_free_bytes)
            .unwrap_or(0)
    }

    pub fn global_accounted_bytes(&self) -> u64 {
        self.segment
            .as_ref()
            .map(OwnerSegmentState::global_accounted_bytes)
            .unwrap_or(0)
    }

    pub fn capacity_report_size_classes(&self) -> Vec<u64> {
        let mut classes = std::collections::BTreeSet::new();
        classes.extend(self.expected_slot_size);
        classes.extend(self.capacity_report_requested_size_classes.iter().copied());
        classes.extend(
            self.pending_demand_by_slot_size
                .iter()
                .filter_map(|(slot_size, count)| (*count != 0).then_some(*slot_size)),
        );
        if let Some(segment) = self.segment.as_ref() {
            classes.extend(segment.allocation_size_classes());
        }
        classes.into_iter().collect()
    }

    pub fn track_capacity_report_size_classes(
        &mut self,
        size_classes: &[u64],
    ) -> Result<bool, String> {
        for &allocation_size in size_classes {
            if allocation_size == 0
                || allocation_size % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
            {
                return Err(format!(
                    "master requested an invalid owner capacity size class: {}",
                    allocation_size
                ));
            }
        }
        let before = self.capacity_report_requested_size_classes.len();
        self.capacity_report_requested_size_classes
            .extend(size_classes.iter().copied());
        Ok(self.capacity_report_requested_size_classes.len() != before)
    }

    pub fn pending_demand_slots(&self, slot_size: u64) -> usize {
        self.pending_demand_by_slot_size
            .get(&slot_size)
            .copied()
            .unwrap_or(0)
    }

    pub fn total_pending_bytes(&self) -> u64 {
        self.pending_demand_by_slot_size
            .iter()
            .map(|(slot_size, count)| {
                slot_size.saturating_mul(u64::try_from(*count).unwrap_or(u64::MAX))
            })
            .sum()
    }

    pub fn largest_pending_slot_size(&self) -> u64 {
        self.pending_demand_by_slot_size
            .iter()
            .filter_map(|(slot_size, count)| (*count != 0).then_some(*slot_size))
            .max()
            .unwrap_or(0)
    }
}

fn owner_transfer_error(
    terminal_sequence: u64,
    op_id: Option<crate::owner_segment::OwnerTransferOpId>,
    code: OwnerTransferErrorCode,
    detail: impl Into<String>,
) -> OwnerSegmentTransferItemResp {
    OwnerSegmentTransferItemResp {
        terminal_sequence,
        op_id,
        outcome: OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(code, detail)),
    }
}

fn apply_owner_transfer_terminal_ack(
    inner: &ClientKvApiInner,
    caller_generation: &crate::owner_segment::OwnerGeneration,
    ack_stream_id: u64,
    requested_watermark: u64,
) -> u64 {
    if ack_stream_id == 0 || requested_watermark == 0 {
        return 0;
    }
    let key = (caller_generation.clone(), ack_stream_id);
    let applied = match inner.owner_transfer_inbound_ack_watermarks.entry(key) {
        DashMapEntry::Occupied(mut current) => {
            if requested_watermark > *current.get() {
                current.insert(requested_watermark);
            }
            *current.get()
        }
        DashMapEntry::Vacant(vacant) => {
            vacant.insert(requested_watermark);
            requested_watermark
        }
    };
    inner.owner_get_to_target_flights.retain(|op_id, flight| {
        !(op_id.coordinator == *caller_generation
            && flight.request.ack_stream_id == ack_stream_id
            && flight.request.terminal_sequence <= applied
            && flight.completed.load(Ordering::Acquire))
    });
    inner
        .owner_put_from_source_flights
        .retain(|(op_id, _), flight| {
            !(op_id.coordinator == *caller_generation
                && flight.request.ack_stream_id == ack_stream_id
                && flight.request.terminal_sequence <= applied
                && flight.completed.load(Ordering::Acquire))
        });
    applied
}

async fn release_owner_get_to_target_source_hold(
    inner: &ClientKvApiInner,
    op_id: &crate::owner_segment::OwnerTransferOpId,
    hold: &OwnerGetToTargetSourceHold,
    outcome: crate::owner_segment::OwnerTransferOutcome,
) -> Result<(), String> {
    match hold {
        OwnerGetToTargetSourceHold::Memory(source_lease_id) => {
            let released = inner.owner_segment_allocator.lock().release_source(
                op_id,
                source_lease_id,
                outcome,
            );
            if matches!(released, OwnerSegmentTransferOutcome::SourceReleased) {
                Ok(())
            } else {
                Err(format!(
                    "GetToTarget failed to release its source lease: {released:?}"
                ))
            }
        }
        OwnerGetToTargetSourceHold::Ssd { lease, get_id } => {
            // The master SSD-stage lifecycle is replay-safe and deliberately
            // detached. Physical transient DRAM remains protected until this
            // exact owner slot release succeeds.
            inner.finish_ssd_stage_detached(*get_id);
            inner
                .owner_release_local_reserve_slot_lease(lease.clone())
                .await
                .map_err(|error| format!("GetToTarget failed to release SSD staging: {error}"))
        }
    }
}

async fn execute_owner_get_to_target(
    view: ClientKvApiView,
    op_id: crate::owner_segment::OwnerTransferOpId,
    request: OwnerGetToTargetRequest,
    flight: &OwnerGetToTargetSharedOp,
) -> OwnerSegmentTransferOutcome {
    use crate::owner_segment::{
        OwnerGetDestinationCapability, OwnerGetSourceCapability, OwnerGetTransferReceipt,
        OwnerSegmentTransferOutcome, OwnerTransferDirection, OwnerTransferOpKind,
        OwnerTransferOutcome,
    };

    let inner = view.client_kv_api().inner();
    let destination = &request.destination;
    let source_len = request.source.len();
    if op_id.kind != OwnerTransferOpKind::Get
        || request.source.plan_nonce() != op_id.sequence
        || !destination.is_valid_for(&op_id, source_len)
        || source_len == 0
    {
        return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::InvalidArgument,
            "GetToTarget operation, source token and destination capability do not match",
        ));
    }

    let (target_owner, target_addr, is_external_gpu) = match destination {
        OwnerGetDestinationCapability::OwnerSlot(capability) => {
            if capability.slot.owner != op_id.coordinator {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::InvalidArgument,
                    "GetToTarget owner destination is not owned by the coordinator",
                ));
            }
            (capability.slot.owner.clone(), capability.slot.addr, false)
        }
        OwnerGetDestinationCapability::ExternalGpu(capability) => {
            (capability.requester.clone(), capability.addr, true)
        }
        OwnerGetDestinationCapability::Invalid => unreachable!("validated destination"),
    };

    let (exact_source, source_hold) = match &request.source {
        OwnerGetSourceCapability::Memory(route_token) => {
            if route_token.key.is_empty() || route_token.route_epoch == 0 {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::InvalidArgument,
                    "GetToTarget memory source token is incomplete",
                ));
            }
            match inner
                .owner_segment_allocator
                .lock()
                .acquire_source(op_id.clone(), route_token.clone())
            {
                OwnerSegmentTransferOutcome::SourceAcquired { lease_id, slot } => {
                    (slot, OwnerGetToTargetSourceHold::Memory(lease_id))
                }
                OwnerSegmentTransferOutcome::Error(error) => {
                    return OwnerSegmentTransferOutcome::Error(error);
                }
                other => {
                    return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Conflict,
                        format!("GetToTarget could not acquire its exact source: {other:?}"),
                    ));
                }
            }
        }
        OwnerGetSourceCapability::Ssd(token) => {
            let self_info = view.cluster_manager().get_self_info();
            if token.key.is_empty()
                || token.owner.node_id != self_info.id
                || token.owner.node_start_time != self_info.node_start_time
                || token.plan_nonce == 0
            {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::StaleGeneration,
                    "GetToTarget SSD source token names another owner generation",
                ));
            }
            let lease = match inner
                .owner_claim_local_reserve_slot_lease(token.len, 1)
                .await
            {
                Ok(lease) => lease,
                Err(error) => {
                    return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                        OwnerTransferErrorCode::NoSpace,
                        format!("SSD transient target claim failed: {error}"),
                    ));
                }
            };
            let slot = lease
                .slots
                .first()
                .expect("one-slot SSD transient claim must be dense")
                .clone();
            let get_id = token.plan_nonce - 1;
            let response = inner
                .execute_ssd_stage(&SsdStageReadReq {
                    key: token.key.clone(),
                    put_id: token.put_id,
                    get_id,
                    stage_addr: slot.addr,
                    stage_capacity: slot.capacity_bytes,
                    len: token.len,
                })
                .await;
            if let Err(error) = crate::rpcresp_kvresult_convert::try_from_code(
                response.error_code,
                response.error_json,
            ) {
                if let Err(release_error) = inner
                    .owner_release_local_reserve_slot_lease(lease.clone())
                    .await
                {
                    flight.retain_unreleased_source_hold(OwnerGetToTargetSourceHold::Ssd {
                        lease,
                        get_id,
                    });
                    tracing::error!(
                        get_id,
                        error = %release_error,
                        "failed to release an unsuccessful SSD transient source"
                    );
                }
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::NotFound,
                    format!("SSD source load failed: {error}"),
                ));
            }
            (slot, OwnerGetToTargetSourceHold::Ssd { lease, get_id })
        }
        OwnerGetSourceCapability::Invalid => {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::InvalidArgument,
                "GetToTarget source capability is invalid",
            ));
        }
    };

    let same_owner = exact_source.owner == target_owner;
    let direction = if same_owner && exact_source.addr == target_addr {
        OwnerTransferDirection::ZeroCopy
    } else if same_owner {
        OwnerTransferDirection::LocalCopy
    } else {
        OwnerTransferDirection::RdmaWrite
    };
    let transfer = if is_external_gpu {
        view.client_transfer_engine()
            .transfer_data_no_copy_to_remote_gpu(
                target_owner.node_id.clone(),
                exact_source.addr,
                target_addr,
                exact_source.len,
            )
            .await
    } else {
        let peer = (!same_owner).then(|| target_owner.node_id.clone());
        view.client_transfer_engine()
            .transfer_data_no_copy(
                peer,
                false,
                exact_source.addr,
                target_addr,
                exact_source.len,
                None,
            )
            .await
    };
    if let Err(error) = transfer {
        if let Err(cleanup_error) = release_owner_get_to_target_source_hold(
            inner,
            &op_id,
            &source_hold,
            OwnerTransferOutcome::Failed,
        )
        .await
        {
            flight.retain_unreleased_source_hold(source_hold);
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Internal,
                format!(
                    "GetToTarget payload WRITE failed ({error}); source cleanup also failed ({cleanup_error})"
                ),
            ));
        }
        return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::Internal,
            format!("GetToTarget payload WRITE failed: {error}"),
        ));
    }

    // Transfer completion is the final access to the source range. The
    // requester-owned Reserved target now protects the copied bytes, so source
    // release does not require another per-Get owner RPC.
    if let Err(cleanup_error) = release_owner_get_to_target_source_hold(
        inner,
        &op_id,
        &source_hold,
        OwnerTransferOutcome::Success,
    )
    .await
    {
        flight.retain_unreleased_source_hold(source_hold);
        return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::Internal,
            format!(
                "GetToTarget payload WRITE completed but source cleanup failed: {cleanup_error}"
            ),
        ));
    }

    OwnerSegmentTransferOutcome::GetToTargetCompleted {
        receipt: OwnerGetTransferReceipt {
            completion_id: op_id.sequence,
            direction,
            bytes: exact_source.len,
            source: exact_source.clone(),
            destination: request.destination,
        },
    }
}

async fn handle_owner_get_to_target(
    inner: &ClientKvApiInner,
    op_id: crate::owner_segment::OwnerTransferOpId,
    ack_stream_id: u64,
    terminal_sequence: u64,
    source: crate::owner_segment::OwnerGetSourceCapability,
    destination: crate::owner_segment::OwnerGetDestinationCapability,
) -> OwnerSegmentTransferOutcome {
    let request = OwnerGetToTargetRequest {
        ack_stream_id,
        terminal_sequence,
        source,
        destination,
    };
    let (flight, leader) = match inner.owner_get_to_target_flights.entry(op_id.clone()) {
        DashMapEntry::Occupied(existing) => {
            if existing.get().request != request {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "GetToTarget replay changed source or destination capability",
                ));
            }
            (existing.get().clone(), false)
        }
        DashMapEntry::Vacant(vacant) => {
            let flight = OwnerGetToTargetSharedOp::new(request.clone());
            vacant.insert(flight.clone());
            (flight, true)
        }
    };
    if leader {
        let spawn_view = inner.view.clone_view();
        let worker_view = spawn_view.clone();
        let worker_flight = flight.clone();
        let worker_op = op_id.clone();
        spawn_view.spawn(
            format!(
                "owner_get_to_target_{}_{}",
                worker_op.coordinator.node_id, worker_op.sequence
            ),
            async move {
                let outcome = execute_owner_get_to_target(
                    worker_view,
                    worker_op,
                    request,
                    worker_flight.as_ref(),
                )
                .await;
                assert!(
                    worker_flight.complete(outcome),
                    "GetToTarget leader published more than one terminal"
                );
            },
        );
    }
    flight.wait().await
}

enum OwnerTargetRouteCommitStart {
    Running(Arc<OwnerTargetRouteCommitSharedOp>),
    Terminal(OwnerSegmentTransferOutcome),
}

enum OwnerTargetRouteCommitWait {
    /// The unique route task owns the RoutePending target. The Async caller
    /// may publish its outward acceptance without waiting for master.
    Detached,
    /// Either the task was already terminal or a Sync caller waited for that
    /// same task. No second commit operation is created here.
    Terminal(OwnerSegmentTransferOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerPutAppendDoneDisposition {
    Committed(u64),
    Obsolete,
}

fn classify_owner_put_append_done(
    response: &crate::master_kv_router::msg_pack::PutAppendDoneResp,
) -> OwnerPutAppendDoneDisposition {
    if response.appended && response.route_epoch != 0 {
        OwnerPutAppendDoneDisposition::Committed(response.route_epoch)
    } else {
        OwnerPutAppendDoneDisposition::Obsolete
    }
}

/// An operation-state rejection is a definitive answer for this immutable
/// route token. Replaying it cannot recreate an expired, revoked, or
/// superseded append reservation and would otherwise retain the target slot
/// and the per-key materialization fence forever.
fn owner_put_append_done_is_definitively_rejected(
    error: &crate::rpcresp_kvresult_convert::msg_and_error::KvError,
) -> bool {
    matches!(
        error,
        crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidPutMasterState { .. }
        )
    )
}

async fn wait_owner_target_route_commit(
    mode: crate::owner_segment::OwnerRouteCommitMode,
    commit: OwnerTargetRouteCommitStart,
) -> OwnerTargetRouteCommitWait {
    match (mode, commit) {
        (
            crate::owner_segment::OwnerRouteCommitMode::Async,
            OwnerTargetRouteCommitStart::Running(_),
        ) => OwnerTargetRouteCommitWait::Detached,
        (
            crate::owner_segment::OwnerRouteCommitMode::Async,
            OwnerTargetRouteCommitStart::Terminal(terminal),
        ) => OwnerTargetRouteCommitWait::Terminal(terminal),
        (
            crate::owner_segment::OwnerRouteCommitMode::Sync,
            OwnerTargetRouteCommitStart::Running(flight),
        ) => OwnerTargetRouteCommitWait::Terminal(flight.wait().await),
        (
            crate::owner_segment::OwnerRouteCommitMode::Sync,
            OwnerTargetRouteCommitStart::Terminal(terminal),
        ) => OwnerTargetRouteCommitWait::Terminal(terminal),
    }
}

async fn run_owner_target_route_commit(
    view: ClientKvApiView,
    op_id: crate::owner_segment::OwnerTransferOpId,
    request: OwnerTargetRouteCommitRequest,
) -> OwnerSegmentTransferOutcome {
    if request.route_token.is_some()
        && op_id.kind != crate::owner_segment::OwnerTransferOpKind::ReplicaAppend
    {
        return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::RouteCommitRequired,
            "this persistent owner transfer kind has not migrated its master route adapter",
        ));
    }
    let inner = view.client_kv_api().inner();
    let Some(route_token) = request.route_token.clone() else {
        return inner.owner_segment_allocator.lock().finish_target_commit(
            &op_id,
            &request.lease_id,
            0,
        );
    };
    let key = route_token.key.clone();
    let mut retry_delay = crate::owner_segment::OWNER_SEGMENT_TRANSFER_RETRY_INITIAL;
    loop {
        match inner
            .put_append_done(
                &key,
                route_token.put_id,
                route_token.operation.sequence,
                Some(request.receipt.target.clone()),
                Some(route_token.clone()),
            )
            .await
        {
            Ok(response) => match classify_owner_put_append_done(&response) {
                OwnerPutAppendDoneDisposition::Committed(route_epoch) => {
                    return inner.owner_segment_allocator.lock().finish_target_commit(
                        &op_id,
                        &request.lease_id,
                        route_epoch,
                    );
                }
                OwnerPutAppendDoneDisposition::Obsolete => {
                    // The request and immutable reservation identity were already
                    // validated by the master.  A successful response with no
                    // appended route therefore means the exact source generation
                    // is no longer current (or an equivalent generation fence
                    // rejected publication). Replaying cannot make that old
                    // generation current again. Free only this RoutePending slot
                    // and let a later lookup observe the new route or recompute.
                    tracing::debug!(
                        operation_id = op_id.sequence,
                        key,
                        appended = response.appended,
                        route_epoch = response.route_epoch,
                        "CommitTarget became obsolete before route publication; aborting the exact target"
                    );
                    return inner
                    .owner_segment_allocator
                    .lock()
                    .abort_route_rejected_target(
                        &op_id,
                        &request.lease_id,
                        format!(
                            "master route generation advanced before owner target publication: appended={} route_epoch={}",
                            response.appended, response.route_epoch
                        ),
                    );
                }
            },
            Err(error) if owner_put_append_done_is_definitively_rejected(&error) => {
                tracing::warn!(
                    operation_id = op_id.sequence,
                    key,
                    error = %error,
                    "CommitTarget metadata operation was definitively rejected; aborting the exact target instead of replaying"
                );
                return inner
                    .owner_segment_allocator
                    .lock()
                    .abort_route_rejected_target(
                        &op_id,
                        &request.lease_id,
                        format!("master definitively rejected owner target publication: {error}"),
                    );
            }
            Err(error) if view.register_shutdown_poller().is_running() => {
                tracing::warn!(
                    operation_id = op_id.sequence,
                    key,
                    retry_delay_ms = retry_delay.as_millis(),
                    error = %error,
                    "CommitTarget metadata RPC is uncertain; replaying the same route operation"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay
                    .saturating_mul(2)
                    .min(crate::owner_segment::OWNER_SEGMENT_TRANSFER_RETRY_MAX);
            }
            Err(error) => {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Internal,
                    format!("CommitTarget stopped during shutdown: {error}"),
                ));
            }
        }
    }
}

fn start_owner_target_route_commit(
    inner: &ClientKvApiInner,
    op_id: crate::owner_segment::OwnerTransferOpId,
    request: OwnerTargetRouteCommitRequest,
) -> OwnerTargetRouteCommitStart {
    let pending = inner.owner_segment_allocator.lock().begin_target_commit(
        &op_id,
        &request.lease_id,
        request.receipt.clone(),
        request.route_token.clone(),
    );
    match pending {
        OwnerSegmentTransferOutcome::TargetCommitPending { .. } => {}
        terminal => return OwnerTargetRouteCommitStart::Terminal(terminal),
    }

    let (flight, leader) = match inner.owner_target_route_commit_flights.entry(op_id.clone()) {
        DashMapEntry::Occupied(existing) => {
            if existing.get().request != request {
                return OwnerTargetRouteCommitStart::Terminal(OwnerSegmentTransferOutcome::Error(
                    OwnerTransferItemError::new(
                        OwnerTransferErrorCode::Conflict,
                        "CommitTarget replay changed lease, receipt or route token",
                    ),
                ));
            }
            (existing.get().clone(), false)
        }
        DashMapEntry::Vacant(vacant) => {
            let flight = OwnerTargetRouteCommitSharedOp::new(request.clone());
            vacant.insert(flight.clone());
            (flight, true)
        }
    };
    if leader {
        let spawn_view = inner.view.clone_view();
        let worker_view = spawn_view.clone();
        let worker_flight = flight.clone();
        let worker_op = op_id.clone();
        spawn_view.spawn(
            format!(
                "owner_target_route_commit_{}_{}",
                worker_op.coordinator.node_id, worker_op.sequence
            ),
            async move {
                let terminal = run_owner_target_route_commit(worker_view, worker_op, request).await;
                assert!(
                    worker_flight.complete(terminal),
                    "CommitTarget task published more than one terminal"
                );
            },
        );
    }
    OwnerTargetRouteCommitStart::Running(flight)
}

async fn commit_owner_target_route(
    inner: &ClientKvApiInner,
    op_id: &crate::owner_segment::OwnerTransferOpId,
    lease_id: &crate::owner_segment::OwnerLeaseId,
    receipt: crate::owner_segment::OwnerTransferReceipt,
    route_token: Option<crate::owner_segment::OwnerTargetRouteToken>,
) -> OwnerSegmentTransferOutcome {
    match start_owner_target_route_commit(
        inner,
        op_id.clone(),
        OwnerTargetRouteCommitRequest {
            lease_id: lease_id.clone(),
            receipt,
            route_token,
            route_commit_mode: crate::owner_segment::OwnerRouteCommitMode::Sync,
        },
    ) {
        OwnerTargetRouteCommitStart::Running(flight) => flight.wait().await,
        OwnerTargetRouteCommitStart::Terminal(terminal) => terminal,
    }
}

fn put_from_source_completed(
    target_attempt: u32,
    source: &crate::owner_segment::OwnerSourceReadCapability,
    target: crate::owner_segment::OwnerSlotDesc,
    receipt: crate::owner_segment::OwnerTransferReceipt,
    route_epoch: u64,
) -> OwnerSegmentTransferOutcome {
    OwnerSegmentTransferOutcome::PutFromSourceCompleted {
        target_attempt,
        source: source.route.source.clone(),
        target,
        receipt,
        route_epoch,
    }
}

fn put_from_source_accepted_route_pending(
    target_attempt: u32,
    source: &crate::owner_segment::OwnerSourceReadCapability,
    target: crate::owner_segment::OwnerSlotDesc,
    receipt: crate::owner_segment::OwnerTransferReceipt,
) -> OwnerSegmentTransferOutcome {
    OwnerSegmentTransferOutcome::PutFromSourceAcceptedRoutePending {
        target_attempt,
        source: source.route.source.clone(),
        target,
        receipt,
    }
}

async fn wait_owner_local_access_fence(mut completion: watch::Receiver<bool>) {
    loop {
        if *completion.borrow_and_update() {
            return;
        }
        if completion.changed().await.is_err() {
            return;
        }
    }
}

async fn complete_put_from_existing_owner_manifest(
    inner: &ClientKvApiInner,
    op_id: &crate::owner_segment::OwnerTransferOpId,
    request: &OwnerPutFromSourceRequest,
    source_slot: &crate::owner_segment::OwnerSlotDesc,
    target_slot: crate::owner_segment::OwnerSlotDesc,
) -> OwnerSegmentTransferOutcome {
    use crate::owner_segment::{OwnerTransferDirection, OwnerTransferReceipt};

    let response = match inner
        .put_append_done(
            &request.target_plan.key,
            request.target_plan.put_id,
            op_id.sequence,
            Some(target_slot.clone()),
            Some(request.target_plan.clone()),
        )
        .await
    {
        Ok(response) if response.appended && response.route_epoch != 0 => response,
        Ok(response) => {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::RouteCommitRequired,
                format!(
                    "PutFromSource could not adopt an existing exact owner manifest: appended={} route_epoch={}",
                    response.appended, response.route_epoch
                ),
            ));
        }
        Err(error) => {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::RouteCommitRequired,
                format!("PutFromSource existing-manifest route commit failed: {error}"),
            ));
        }
    };
    let receipt = OwnerTransferReceipt {
        completion_id: op_id.sequence,
        direction: OwnerTransferDirection::RdmaRead,
        bytes: source_slot.len,
        source: Some(source_slot.clone()),
        target: target_slot.clone(),
        source_registration_epoch: source_slot.segment_registration_epoch,
        target_registration_epoch: target_slot.segment_registration_epoch,
    };
    match request.route_commit_mode {
        crate::owner_segment::OwnerRouteCommitMode::Async => {
            put_from_source_accepted_route_pending(
                request.target_attempt,
                &request.source,
                target_slot,
                receipt,
            )
        }
        crate::owner_segment::OwnerRouteCommitMode::Sync => put_from_source_completed(
            request.target_attempt,
            &request.source,
            target_slot,
            receipt,
            response.route_epoch,
        ),
    }
}

async fn execute_owner_put_from_source(
    view: ClientKvApiView,
    op_id: crate::owner_segment::OwnerTransferOpId,
    request: OwnerPutFromSourceRequest,
) -> OwnerSegmentTransferOutcome {
    use crate::owner_segment::{OwnerTransferDirection, OwnerTransferOpKind, OwnerTransferReceipt};

    let inner = view.client_kv_api().inner();
    let self_info = view.cluster_manager().get_self_info();
    let target_owner =
        crate::owner_segment::OwnerGeneration::new(self_info.id.clone(), self_info.node_start_time);
    let source_slot = &request.source.route.source;
    let target_plan = &request.target_plan;
    if op_id.kind != OwnerTransferOpKind::ReplicaAppend
        || request.target_attempt == 0
        || target_plan.operation != op_id
        || target_plan.target_owner != target_owner
        || target_plan.plan_nonce != op_id.sequence
        || target_plan.key != request.source.route.key
        || target_plan.put_id != request.source.route.put_id
        || target_plan.atomic_batch != request.source.route.atomic_batch
        || source_slot.owner == target_owner
        || !request.source.is_valid_for(
            &op_id,
            &target_owner,
            request.target_attempt,
            source_slot.len,
        )
    {
        return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::InvalidArgument,
            "PutFromSource operation, source capability and target plan do not match",
        ));
    }

    let materialization_op = loop {
        match inner.reserve_owner_key_materialization(&target_plan.key, target_plan.put_id) {
            OwnerKeyMaterializationReservation::Leader(op) => break op,
            OwnerKeyMaterializationReservation::Wait(op) => {
                let _ = op.wait().await;
            }
            OwnerKeyMaterializationReservation::WaitForGet(op) => {
                op.wait_terminal().await;
            }
            OwnerKeyMaterializationReservation::WaitForExternalPut(op) => {
                let _ = op.wait().await;
            }
            OwnerKeyMaterializationReservation::WaitForLocalAccess(completion) => {
                wait_owner_local_access_fence(completion).await;
            }
        }
    };
    let mut materialization_guard =
        OwnerKeyMaterializationGuard::new(view.clone(), materialization_op);

    let existing_manifest = inner
        .owner_segment_allocator
        .lock()
        .manifest_entry(&target_plan.key, target_plan.put_id);
    if let Some(existing) = existing_manifest {
        if existing.physical_state != crate::owner_segment::OwnerSlotPhysicalState::Committed {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                format!(
                    "PutFromSource found an owner manifest without its materialization fence: state={:?} allocation_id={}",
                    existing.physical_state, existing.slot.allocation_id
                ),
            ));
        }
        if existing.slot.owner != target_owner
            || existing.slot.len != source_slot.len
            || !existing.slot.is_valid()
        {
            return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                OwnerTransferErrorCode::Conflict,
                "PutFromSource existing owner manifest has incompatible geometry",
            ));
        }
        let outcome = complete_put_from_existing_owner_manifest(
            inner,
            &op_id,
            &request,
            source_slot,
            existing.slot,
        )
        .await;
        if matches!(
            outcome,
            OwnerSegmentTransferOutcome::PutFromSourceAcceptedRoutePending { .. }
                | OwnerSegmentTransferOutcome::PutFromSourceCompleted { .. }
        ) {
            materialization_guard.finish(OwnerKeyMaterializationOutcome::Committed);
        }
        return outcome;
    }

    let prepared = inner.owner_segment_allocator.lock().prepare_target(
        op_id.clone(),
        target_owner.clone(),
        target_plan.key.clone(),
        target_plan.put_id,
        source_slot.len,
        request.disposition,
        target_plan.atomic_batch.clone(),
    );
    let (lease_id, target_slot, transfer_required) = match prepared {
        OwnerSegmentTransferOutcome::TargetPrepared { lease_id, slot, .. } => {
            (lease_id, slot, true)
        }
        OwnerSegmentTransferOutcome::TargetDataReady { lease_id, slot }
        | OwnerSegmentTransferOutcome::TargetCommitPending { lease_id, slot } => {
            (lease_id, slot, false)
        }
        OwnerSegmentTransferOutcome::TargetCommitted {
            lease_id: _,
            slot,
            route_epoch,
        } if route_epoch != 0 => {
            let receipt = OwnerTransferReceipt {
                completion_id: op_id.sequence,
                direction: OwnerTransferDirection::RdmaRead,
                bytes: source_slot.len,
                source: Some(source_slot.clone()),
                target: slot.clone(),
                source_registration_epoch: source_slot.segment_registration_epoch,
                target_registration_epoch: slot.segment_registration_epoch,
            };
            materialization_guard.finish(OwnerKeyMaterializationOutcome::Committed);
            return match request.route_commit_mode {
                crate::owner_segment::OwnerRouteCommitMode::Async => {
                    put_from_source_accepted_route_pending(
                        request.target_attempt,
                        &request.source,
                        slot,
                        receipt,
                    )
                }
                crate::owner_segment::OwnerRouteCommitMode::Sync => put_from_source_completed(
                    request.target_attempt,
                    &request.source,
                    slot,
                    receipt,
                    route_epoch,
                ),
            };
        }
        terminal => return terminal,
    };

    if target_slot.owner != target_owner
        || target_slot.len != source_slot.len
        || !target_slot.is_valid()
    {
        return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
            OwnerTransferErrorCode::Conflict,
            "PutFromSource target claim returned invalid geometry",
        ));
    }
    let receipt = OwnerTransferReceipt {
        completion_id: op_id.sequence,
        direction: OwnerTransferDirection::RdmaRead,
        bytes: source_slot.len,
        source: Some(source_slot.clone()),
        target: target_slot.clone(),
        source_registration_epoch: source_slot.segment_registration_epoch,
        target_registration_epoch: target_slot.segment_registration_epoch,
    };

    if transfer_required {
        inner.record_owner_remote_put_transfer();
        if let Err(error) = view
            .client_transfer_engine()
            .transfer_data_no_copy(
                Some(source_slot.owner.node_id.clone()),
                true,
                source_slot.addr,
                target_slot.addr,
                source_slot.len,
                None,
            )
            .await
        {
            tracing::warn!(
                key = target_plan.key,
                put_time_ms = target_plan.put_id.0,
                put_version = target_plan.put_id.1,
                target_attempt = request.target_attempt,
                source_node_id = source_slot.owner.node_id,
                target_node_id = target_owner.node_id,
                error = %error,
                "PutFromSource target-side RDMA READ failed"
            );
            return inner.owner_segment_allocator.lock().abort_target(
                &op_id,
                &lease_id,
                format!("PutFromSource RDMA READ failed: {error}"),
            );
        }
    }

    let commit = start_owner_target_route_commit(
        inner,
        op_id.clone(),
        OwnerTargetRouteCommitRequest {
            lease_id: lease_id.clone(),
            receipt: receipt.clone(),
            route_token: Some(request.target_plan.clone()),
            route_commit_mode: request.route_commit_mode,
        },
    );
    if request.route_commit_mode == crate::owner_segment::OwnerRouteCommitMode::Async {
        return match commit {
            OwnerTargetRouteCommitStart::Running(flight) => {
                let spawn_view = view.clone();
                let expected_slot = target_slot.clone();
                spawn_view.spawn("owner_key_materialization_route_wait", async move {
                    let terminal = flight.wait().await;
                    let outcome = match terminal {
                        OwnerSegmentTransferOutcome::TargetCommitted {
                            slot, route_epoch, ..
                        } if route_epoch != 0 && slot.geometry_matches(&expected_slot) => {
                            OwnerKeyMaterializationOutcome::Committed
                        }
                        _ => OwnerKeyMaterializationOutcome::Failed,
                    };
                    materialization_guard.finish(outcome);
                });
                put_from_source_accepted_route_pending(
                    request.target_attempt,
                    &request.source,
                    target_slot,
                    receipt,
                )
            }
            OwnerTargetRouteCommitStart::Terminal(
                OwnerSegmentTransferOutcome::TargetCommitted {
                    slot, route_epoch, ..
                },
            ) if route_epoch != 0 && slot.geometry_matches(&target_slot) => {
                materialization_guard.finish(OwnerKeyMaterializationOutcome::Committed);
                put_from_source_accepted_route_pending(
                    request.target_attempt,
                    &request.source,
                    slot,
                    receipt,
                )
            }
            OwnerTargetRouteCommitStart::Terminal(terminal) => terminal,
        };
    }

    let terminal = match commit {
        OwnerTargetRouteCommitStart::Running(flight) => flight.wait().await,
        OwnerTargetRouteCommitStart::Terminal(terminal) => terminal,
    };
    match terminal {
        OwnerSegmentTransferOutcome::TargetCommitted {
            lease_id: committed_lease,
            slot,
            route_epoch,
        } if committed_lease == lease_id
            && slot.geometry_matches(&target_slot)
            && route_epoch != 0 =>
        {
            materialization_guard.finish(OwnerKeyMaterializationOutcome::Committed);
            put_from_source_completed(
                request.target_attempt,
                &request.source,
                slot,
                receipt,
                route_epoch,
            )
        }
        OwnerSegmentTransferOutcome::Error(OwnerTransferItemError {
            code: OwnerTransferErrorCode::RouteCommitRequired,
            detail,
        }) => inner
            .owner_segment_allocator
            .lock()
            .abort_route_rejected_target(
                &op_id,
                &lease_id,
                format!("Sync CommitTarget failed before outward success: {detail}"),
            ),
        terminal => terminal,
    }
}

async fn handle_owner_put_from_source(
    inner: &ClientKvApiInner,
    op_id: crate::owner_segment::OwnerTransferOpId,
    request: OwnerPutFromSourceRequest,
) -> OwnerSegmentTransferOutcome {
    let key = (op_id.clone(), request.target_attempt);
    let (flight, leader) = match inner.owner_put_from_source_flights.entry(key) {
        DashMapEntry::Occupied(existing) => {
            if existing.get().request != request {
                return OwnerSegmentTransferOutcome::Error(OwnerTransferItemError::new(
                    OwnerTransferErrorCode::Conflict,
                    "PutFromSource replay changed source, target plan or attempt",
                ));
            }
            (existing.get().clone(), false)
        }
        DashMapEntry::Vacant(vacant) => {
            let flight = OwnerPutFromSourceSharedOp::new(request.clone());
            vacant.insert(flight.clone());
            (flight, true)
        }
    };
    if leader {
        let spawn_view = inner.view.clone_view();
        let worker_view = spawn_view.clone();
        let worker_flight = flight.clone();
        let worker_op = op_id.clone();
        spawn_view.spawn(
            format!(
                "owner_put_from_source_{}_{}_{}",
                worker_op.coordinator.node_id, worker_op.sequence, request.target_attempt,
            ),
            async move {
                let outcome = execute_owner_put_from_source(worker_view, worker_op, request).await;
                assert!(
                    worker_flight.complete(outcome),
                    "PutFromSource leader published more than one terminal"
                );
            },
        );
    }
    flight.wait().await
}

async fn handle_owner_segment_transfer_item(
    inner: &ClientKvApiInner,
    caller: &NodeID,
    caller_generation: &crate::owner_segment::OwnerGeneration,
    ack_stream_id: u64,
    acknowledged_watermark: u64,
    request_item: crate::owner_segment::OwnerSegmentTransferRequestItem,
) -> OwnerSegmentTransferItemResp {
    let terminal_sequence = request_item.terminal_sequence;
    let item = request_item.item;
    let op_id = item.op_id().cloned();
    let Some(operation) = op_id.clone() else {
        return owner_transfer_error(
            terminal_sequence,
            None,
            OwnerTransferErrorCode::InvalidArgument,
            "owner segment transfer item is invalid",
        );
    };
    if operation.coordinator != *caller_generation
        || operation.coordinator.node_id.as_str() != caller.as_ref()
    {
        return owner_transfer_error(
            terminal_sequence,
            op_id,
            OwnerTransferErrorCode::StaleGeneration,
            format!(
                "owner transfer caller does not match coordinator: caller={} coordinator={}",
                caller, operation.coordinator.node_id
            ),
        );
    }
    let current_caller = inner
        .view
        .cluster_manager()
        .get_member_info_cached(caller.as_ref());
    if current_caller
        .as_ref()
        .is_none_or(|member| member.node_start_time != operation.coordinator.node_start_time)
    {
        return owner_transfer_error(
            terminal_sequence,
            op_id,
            OwnerTransferErrorCode::StaleGeneration,
            format!(
                "owner transfer coordinator generation is stale: caller={} requested_start={} current_start={:?}",
                caller,
                operation.coordinator.node_start_time,
                current_caller.map(|member| member.node_start_time),
            ),
        );
    }
    let caches_terminal = item.caches_replay_terminal();
    if ack_stream_id == 0
        || caches_terminal != (terminal_sequence != 0)
        || (terminal_sequence != 0 && terminal_sequence <= acknowledged_watermark)
    {
        return owner_transfer_error(
            terminal_sequence,
            op_id,
            OwnerTransferErrorCode::Conflict,
            if terminal_sequence != 0 && terminal_sequence <= acknowledged_watermark {
                format!(
                    "owner transfer terminal was already acknowledged: stream={} sequence={} watermark={}",
                    ack_stream_id, terminal_sequence, acknowledged_watermark
                )
            } else {
                format!(
                    "owner transfer terminal sequence is invalid: stream={} sequence={} caches_terminal={}",
                    ack_stream_id, terminal_sequence, caches_terminal
                )
            },
        );
    }

    let outcome = match item {
        OwnerSegmentTransferItem::Invalid => unreachable!(),
        OwnerSegmentTransferItem::GetToTarget {
            op_id,
            source,
            destination,
        } => {
            handle_owner_get_to_target(
                inner,
                op_id,
                ack_stream_id,
                terminal_sequence,
                source,
                destination,
            )
            .await
        }
        OwnerSegmentTransferItem::PutFromSource {
            op_id,
            target_attempt,
            target_plan,
            source,
            disposition,
            route_commit_mode,
        } => {
            handle_owner_put_from_source(
                inner,
                op_id,
                OwnerPutFromSourceRequest {
                    ack_stream_id,
                    terminal_sequence,
                    target_attempt,
                    target_plan,
                    source,
                    disposition,
                    route_commit_mode,
                },
            )
            .await
        }
        OwnerSegmentTransferItem::PrepareTarget {
            op_id,
            expected_target,
            key,
            put_id,
            len,
            disposition,
            atomic_batch,
        } => inner.owner_segment_allocator.lock().prepare_target(
            op_id,
            expected_target,
            key,
            put_id,
            len,
            disposition,
            atomic_batch,
        ),
        OwnerSegmentTransferItem::CommitTarget {
            op_id,
            lease_id,
            receipt,
            route_token,
        } => commit_owner_target_route(inner, &op_id, &lease_id, receipt, route_token).await,
        OwnerSegmentTransferItem::AbortTarget {
            op_id,
            lease_id,
            reason,
        } => inner
            .owner_segment_allocator
            .lock()
            .abort_target(&op_id, &lease_id, reason),
    };
    OwnerSegmentTransferItemResp {
        terminal_sequence,
        op_id,
        outcome,
    }
}

async fn handle_owner_segment_transfer(
    view: &ClientKvApiView,
    caller: NodeID,
    request: MsgPack<OwnerSegmentTransferReq>,
) -> MsgPack<OwnerSegmentTransferResp> {
    let started_at = Instant::now();
    let inner = view.client_kv_api().inner();
    let caller_generation = request.serialize_part.caller_generation.clone();
    let get_to_target_requested = request
        .serialize_part
        .items
        .iter()
        .filter(|item| matches!(&item.item, OwnerSegmentTransferItem::GetToTarget { .. }))
        .count();
    let get_to_target_request_bytes = request
        .serialize_part
        .items
        .iter()
        .filter_map(|item| match &item.item {
            OwnerSegmentTransferItem::GetToTarget { source, .. } => Some(source.len()),
            _ => None,
        })
        .fold(0_u64, u64::saturating_add);
    let ack_stream_id = request.serialize_part.ack_stream_id;
    let caller_is_current = caller_generation.node_id.as_str() == caller.as_ref()
        && inner
            .view
            .cluster_manager()
            .get_member_info_cached(caller.as_ref())
            .is_some_and(|member| member.node_start_time == caller_generation.node_start_time);
    let acknowledged_watermark = if caller_is_current {
        apply_owner_transfer_terminal_ack(
            inner,
            &caller_generation,
            ack_stream_id,
            request.serialize_part.terminal_ack_watermark,
        )
    } else {
        0
    };
    let items = futures::future::join_all(request.serialize_part.items.into_iter().map(|item| {
        handle_owner_segment_transfer_item(
            inner,
            &caller,
            &caller_generation,
            ack_stream_id,
            acknowledged_watermark,
            item,
        )
    }))
    .await;
    inner
        .owner_local_reserve_rebalance_notify()
        .notify_waiters();
    let server_process_us = started_at.elapsed().as_micros().min(u64::MAX as u128) as u64;
    if get_to_target_requested > 0 {
        let mut completed = 0_usize;
        let mut completed_bytes = 0_u64;
        let mut rdma_write_items = 0_usize;
        let mut ipc_copy_items = 0_usize;
        let mut local_copy_items = 0_usize;
        let mut zero_copy_items = 0_usize;
        for item in &items {
            if let OwnerSegmentTransferOutcome::GetToTargetCompleted { receipt } = &item.outcome {
                completed = completed.saturating_add(1);
                completed_bytes = completed_bytes.saturating_add(receipt.bytes);
                match receipt.direction {
                    crate::owner_segment::OwnerTransferDirection::RdmaWrite => {
                        rdma_write_items = rdma_write_items.saturating_add(1)
                    }
                    crate::owner_segment::OwnerTransferDirection::IpcCopy => {
                        ipc_copy_items = ipc_copy_items.saturating_add(1)
                    }
                    crate::owner_segment::OwnerTransferDirection::LocalCopy => {
                        local_copy_items = local_copy_items.saturating_add(1)
                    }
                    crate::owner_segment::OwnerTransferDirection::ZeroCopy => {
                        zero_copy_items = zero_copy_items.saturating_add(1)
                    }
                    crate::owner_segment::OwnerTransferDirection::RdmaRead => {}
                }
            }
        }
        tracing::info!(
            caller = %caller_generation.node_id,
            get_to_target_requested,
            get_to_target_completed = completed,
            get_to_target_request_bytes,
            get_to_target_completed_bytes = completed_bytes,
            rdma_write_items,
            ipc_copy_items,
            local_copy_items,
            zero_copy_items,
            server_process_us,
            "owner GetToTarget batch lifecycle"
        );
    }
    MsgPack {
        serialize_part: OwnerSegmentTransferResp {
            items,
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            server_process_us: i64::try_from(server_process_us).unwrap_or(i64::MAX),
        },
        raw_bytes: Vec::new(),
    }
}

#[cfg(test)]
mod owner_allocatable_report_tests;

#[cfg(test)]
mod owner_segment_protocol_tests {
    use super::OwnerSegmentAllocator;
    use crate::owner_segment::{
        OwnerGeneration, OwnerSegmentTransferOutcome, OwnerSlotPhysicalState,
        OwnerSourceRouteToken, OwnerTargetDisposition, OwnerTargetRouteToken,
        OwnerTargetWriteCapability, OwnerTransferDirection, OwnerTransferErrorCode,
        OwnerTransferOpId, OwnerTransferOpKind, OwnerTransferOutcome, OwnerTransferReceipt,
    };

    const SEGMENT_BYTES: u64 = 64 * 1024;
    pub(super) const VALUE_LEN: u64 = 5000;

    fn generation(node_id: &str, node_start_time: i64) -> OwnerGeneration {
        OwnerGeneration::new(node_id, node_start_time)
    }

    fn pool() -> OwnerSegmentAllocator {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_segment(
            generation("target", 11),
            11,
            0x1000,
            0x1000,
            SEGMENT_BYTES,
            0,
            None,
        )
        .expect("install owner protocol test segment");
        pool
    }

    fn pool_with_local_target(local_target_bytes: u64) -> OwnerSegmentAllocator {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_segment(
            generation("target", 11),
            11,
            0x1000,
            0x1000,
            SEGMENT_BYTES,
            local_target_bytes,
            None,
        )
        .expect("install owner protocol test segment with local target");
        pool
    }

    fn op(sequence: u64, kind: OwnerTransferOpKind) -> OwnerTransferOpId {
        OwnerTransferOpId::new(generation("coordinator", 21), sequence, kind)
    }

    fn prepared(
        outcome: OwnerSegmentTransferOutcome,
    ) -> (
        crate::owner_segment::OwnerLeaseId,
        crate::owner_segment::OwnerSlotDesc,
    ) {
        match outcome {
            OwnerSegmentTransferOutcome::TargetPrepared { lease_id, slot, .. } => (lease_id, slot),
            other => panic!("expected prepared target, got {other:?}"),
        }
    }

    pub(super) fn commit_test_target(
        pool: &mut OwnerSegmentAllocator,
        sequence: u64,
        key: &str,
        put_id: (u64, u32),
        disposition: OwnerTargetDisposition,
    ) -> crate::owner_segment::OwnerSlotDesc {
        let operation = op(sequence, OwnerTransferOpKind::ReplicaAppend);
        let (lease_id, slot) = prepared(pool.prepare_target(
            operation.clone(),
            generation("target", 11),
            key.to_string(),
            put_id,
            VALUE_LEN,
            disposition,
            None,
        ));
        let receipt = OwnerTransferReceipt {
            completion_id: sequence,
            direction: OwnerTransferDirection::RdmaRead,
            bytes: VALUE_LEN,
            source: None,
            target: slot.clone(),
            source_registration_epoch: 0,
            target_registration_epoch: slot.segment_registration_epoch,
        };
        let route_token = OwnerTargetRouteToken {
            key: key.to_string(),
            put_id,
            operation: operation.clone(),
            target_owner: generation("target", 11),
            prior_route_epoch: 0,
            policy_epoch: 1,
            atomic_batch: None,
            plan_nonce: sequence,
        };
        assert!(matches!(
            pool.begin_target_commit(&operation, &lease_id, receipt, Some(route_token)),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert!(matches!(
            pool.finish_target_commit(&operation, &lease_id, sequence),
            OwnerSegmentTransferOutcome::TargetCommitted { .. }
        ));
        slot
    }

    #[test]
    fn target_prepare_commit_and_response_loss_replay_do_not_reallocate() {
        let mut pool = pool();
        let operation = op(1, OwnerTransferOpKind::ReplicaAppend);
        let prepare = || {
            (
                operation.clone(),
                generation("target", 11),
                "key".to_string(),
                (100, 3),
                VALUE_LEN,
                OwnerTargetDisposition::GlobalShared,
                None,
            )
        };
        let (lease_id, slot) = prepared(pool.prepare_target(
            prepare().0,
            prepare().1,
            prepare().2,
            prepare().3,
            prepare().4,
            prepare().5,
            prepare().6,
        ));
        let (replay_lease, replay_slot) = prepared(pool.prepare_target(
            prepare().0,
            prepare().1,
            prepare().2,
            prepare().3,
            prepare().4,
            prepare().5,
            prepare().6,
        ));
        assert_eq!(replay_lease, lease_id);
        assert_eq!(replay_slot, slot);
        assert_eq!(pool.used_slot_count(), 1);
        assert!(matches!(
            pool.prepare_target(
                operation.clone(),
                generation("target", 11),
                "key".to_string(),
                (100, 3),
                VALUE_LEN + 1,
                OwnerTargetDisposition::GlobalShared,
                None,
            ),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::Conflict
        ));

        let receipt = OwnerTransferReceipt {
            completion_id: 99,
            direction: OwnerTransferDirection::RdmaWrite,
            bytes: VALUE_LEN,
            source: None,
            target: slot.clone(),
            source_registration_epoch: 0,
            target_registration_epoch: slot.segment_registration_epoch,
        };
        let route_token = OwnerTargetRouteToken {
            key: "key".to_string(),
            put_id: (100, 3),
            operation: operation.clone(),
            target_owner: generation("target", 11),
            prior_route_epoch: 0,
            policy_epoch: 7,
            atomic_batch: None,
            plan_nonce: 123,
        };
        assert!(matches!(
            pool.begin_target_commit(
                &operation,
                &lease_id,
                receipt.clone(),
                Some(route_token.clone()),
            ),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert!(matches!(
            pool.begin_target_commit(&operation, &lease_id, receipt, Some(route_token),),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert!(matches!(
            pool.finish_target_commit(&operation, &lease_id, 77),
            OwnerSegmentTransferOutcome::TargetCommitted {
                route_epoch: 77,
                ..
            }
        ));
        assert!(matches!(
            pool.finish_target_commit(&operation, &lease_id, 77),
            OwnerSegmentTransferOutcome::TargetCommitted {
                route_epoch: 77,
                ..
            }
        ));
        assert_eq!(pool.used_slot_count(), 1);
        let manifest = pool.manifest_entry("key", (100, 3)).unwrap();
        assert_eq!(manifest.slot, slot);
        assert_eq!(manifest.route_epoch, 77);
        assert_eq!(manifest.physical_state, OwnerSlotPhysicalState::Committed);
    }

    #[test]
    fn global_target_prepare_is_hard_bounded_before_transfer() {
        let slot_size = crate::owner_segment_allocation_capacity_bytes(VALUE_LEN).unwrap();
        let global_target_bytes = slot_size * 2;
        let mut pool = pool_with_local_target(SEGMENT_BYTES - global_target_bytes);

        for sequence in 1..=2 {
            assert!(matches!(
                pool.prepare_target(
                    op(sequence, OwnerTransferOpKind::ReplicaAppend),
                    generation("target", 11),
                    format!("global-{sequence}"),
                    (100 + sequence, 1),
                    VALUE_LEN,
                    OwnerTargetDisposition::GlobalShared,
                    None,
                ),
                OwnerSegmentTransferOutcome::TargetPrepared { .. }
            ));
        }
        assert_eq!(pool.global_accounted_bytes(), global_target_bytes);
        assert!(matches!(
            pool.prepare_target(
                op(3, OwnerTransferOpKind::ReplicaAppend),
                generation("target", 11),
                "global-3".to_string(),
                (103, 1),
                VALUE_LEN,
                OwnerTargetDisposition::GlobalShared,
                None,
            ),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::NoSpace
        ));
        assert_eq!(pool.global_accounted_bytes(), global_target_bytes);
        assert_eq!(pool.used_slot_count(), 2);

        assert!(matches!(
            pool.prepare_target(
                op(4, OwnerTransferOpKind::Get),
                generation("target", 11),
                "local-still-admitted".to_string(),
                (104, 1),
                VALUE_LEN,
                OwnerTargetDisposition::LocalExclusive,
                None,
            ),
            OwnerSegmentTransferOutcome::TargetPrepared { .. }
        ));
    }

    #[test]
    fn global_demotion_reservation_blocks_transfer_and_commits_without_double_counting() {
        let slot_size = crate::owner_segment_allocation_capacity_bytes(VALUE_LEN).unwrap();
        let mut pool = pool_with_local_target(SEGMENT_BYTES - slot_size);
        let operation = op(20, OwnerTransferOpKind::ReplicaAppend);
        let (lease_id, slot) = prepared(pool.prepare_target(
            operation.clone(),
            generation("target", 11),
            "local-to-global".to_string(),
            (200, 1),
            VALUE_LEN,
            OwnerTargetDisposition::LocalExclusive,
            None,
        ));
        let receipt = OwnerTransferReceipt {
            completion_id: 20,
            direction: OwnerTransferDirection::RdmaRead,
            bytes: VALUE_LEN,
            source: None,
            target: slot.clone(),
            source_registration_epoch: 0,
            target_registration_epoch: slot.segment_registration_epoch,
        };
        let route_token = OwnerTargetRouteToken {
            key: "local-to-global".to_string(),
            put_id: (200, 1),
            operation: operation.clone(),
            target_owner: generation("target", 11),
            prior_route_epoch: 0,
            policy_epoch: 1,
            atomic_batch: None,
            plan_nonce: 20,
        };
        assert!(matches!(
            pool.begin_target_commit(&operation, &lease_id, receipt, Some(route_token)),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert!(matches!(
            pool.finish_target_commit(&operation, &lease_id, 20),
            OwnerSegmentTransferOutcome::TargetCommitted { .. }
        ));

        assert!(
            pool.try_reserve_global_demotion(
                "local-to-global",
                (200, 1),
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            )
            .unwrap()
        );
        assert_eq!(pool.global_accounted_bytes(), slot_size);
        assert!(
            pool.try_reserve_global_demotion(
                "local-to-global",
                (200, 1),
                slot.allocation_id,
                slot.segment_offset,
                slot.capacity_bytes,
            )
            .unwrap(),
            "reservation replay must be idempotent"
        );
        assert_eq!(pool.global_accounted_bytes(), slot_size);

        assert!(matches!(
            pool.prepare_target(
                op(21, OwnerTransferOpKind::ReplicaAppend),
                generation("target", 11),
                "remote-transfer".to_string(),
                (201, 1),
                VALUE_LEN,
                OwnerTargetDisposition::GlobalShared,
                None,
            ),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::NoSpace
        ));

        pool.commit_reserved_global_demotion(
            "local-to-global",
            (200, 1),
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        )
        .unwrap();
        assert_eq!(pool.global_accounted_bytes(), slot_size);
        assert_eq!(
            pool.manifest_entry("local-to-global", (200, 1))
                .unwrap()
                .disposition,
            OwnerTargetDisposition::GlobalShared
        );
        assert!(!pool.release_global_demotion_reservation(
            "local-to-global",
            (200, 1),
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
    }

    #[test]
    fn batched_global_demotion_reservation_preserves_the_exact_hard_bound() {
        let slot_size = crate::owner_segment_allocation_capacity_bytes(VALUE_LEN).unwrap();
        let global_target_bytes = slot_size * 2;
        let mut pool = pool_with_local_target(SEGMENT_BYTES - global_target_bytes);
        let slots = (0..4u64)
            .map(|index| {
                let key = format!("batched-local-{index}");
                let put_id = (300 + index, 1);
                let slot = commit_test_target(
                    &mut pool,
                    30 + index,
                    &key,
                    put_id,
                    OwnerTargetDisposition::LocalExclusive,
                );
                (key, put_id, slot)
            })
            .collect::<Vec<_>>();

        fn reserve(
            pool: &mut OwnerSegmentAllocator,
            slots: &[(String, (u64, u32), crate::owner_segment::OwnerSlotDesc)],
        ) -> Vec<bool> {
            pool.try_reserve_global_demotions(slots.iter().map(|(key, put_id, slot)| {
                (
                    key.as_str(),
                    *put_id,
                    slot.allocation_id,
                    slot.segment_offset,
                    slot.capacity_bytes,
                )
            }))
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
        }
        assert_eq!(reserve(&mut pool, &slots), vec![true, true, false, false]);
        assert_eq!(pool.global_accounted_bytes(), global_target_bytes);
        assert_eq!(
            reserve(&mut pool, &slots),
            vec![true, true, false, false],
            "batch replay must reuse exact reservations without double counting"
        );
        assert_eq!(pool.global_accounted_bytes(), global_target_bytes);
    }

    #[test]
    fn legacy_a19_demotion_still_requires_the_exact_committed_slot() {
        let slot_size = crate::owner_segment_allocation_capacity_bytes(VALUE_LEN).unwrap();
        let mut pool = pool_with_local_target(SEGMENT_BYTES - slot_size);
        let slot = commit_test_target(
            &mut pool,
            22,
            "legacy-local-to-global",
            (202, 1),
            OwnerTargetDisposition::LocalExclusive,
        );
        assert_eq!(pool.global_accounted_bytes(), 0);
        assert!(!pool.release_global_demotion_reservation(
            "legacy-local-to-global",
            (202, 1),
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));

        pool.commit_legacy_a19_global_demotion(
            "legacy-local-to-global",
            (202, 1),
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        )
        .unwrap();
        assert_eq!(pool.global_accounted_bytes(), slot_size);
        assert_eq!(
            pool.manifest_entry("legacy-local-to-global", (202, 1))
                .unwrap()
                .disposition,
            OwnerTargetDisposition::GlobalShared
        );

        let error = pool
            .commit_legacy_a19_global_demotion(
                "legacy-local-to-global",
                (202, 1),
                slot.allocation_id + 1,
                slot.segment_offset,
                slot.capacity_bytes,
            )
            .unwrap_err();
        assert_eq!(error.code, OwnerTransferErrorCode::Conflict);
    }

    #[test]
    fn claimed_get_target_is_operation_bound_data_ready_and_committed_in_place() {
        let mut pool = pool();
        let operation =
            OwnerTransferOpId::new(generation("target", 11), 41, OwnerTransferOpKind::Get);
        let slot = pool
            .claim_value(VALUE_LEN, 1)
            .pop()
            .expect("claim requester-local Get target");
        let (lease_id, prepared_slot) = prepared(pool.prepare_claimed_get_target(
            operation.clone(),
            "get-key".to_string(),
            (300, 4),
            None,
            slot.clone(),
        ));
        assert_eq!(prepared_slot, slot);
        assert_eq!(pool.prepared_slot_count(), 1);
        let capability = OwnerTargetWriteCapability {
            operation: operation.clone(),
            lease_id: lease_id.clone(),
            slot: slot.clone(),
        };
        let receipt = OwnerTransferReceipt {
            completion_id: operation.sequence,
            direction: OwnerTransferDirection::RdmaWrite,
            bytes: VALUE_LEN,
            source: None,
            target: slot.clone(),
            source_registration_epoch: 17,
            target_registration_epoch: slot.segment_registration_epoch,
        };
        assert!(matches!(
            pool.mark_target_data_ready(&operation, &lease_id, receipt.clone()),
            OwnerSegmentTransferOutcome::TargetDataReady { .. }
        ));
        assert!(matches!(
            pool.mark_target_data_ready(&operation, &lease_id, receipt),
            OwnerSegmentTransferOutcome::TargetDataReady { .. }
        ));
        pool.mark_data_ready_get_target_pending_visible(&capability)
            .expect("DataReady target becomes hidden resident");
        assert_eq!(pool.prepared_slot_count(), 0);
        assert_eq!(pool.pending_visible_slot_count(), 1);
        assert_eq!(
            pool.manifest_entry("get-key", (300, 4))
                .expect("DataReady Get target manifest")
                .physical_state,
            OwnerSlotPhysicalState::DataReady
        );
        assert!(matches!(
            pool.begin_data_ready_get_target_commit(&capability),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert!(matches!(
            pool.begin_data_ready_get_target_commit(&capability),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert_eq!(
            pool.manifest_entry("get-key", (300, 4))
                .expect("RoutePending Get target manifest")
                .physical_state,
            OwnerSlotPhysicalState::RoutePending
        );
        assert!(matches!(
            pool.finish_data_ready_get_target(&operation, &lease_id, slot.allocation_id),
            OwnerSegmentTransferOutcome::TargetCommitted { .. }
        ));
        assert_eq!(pool.pending_visible_slot_count(), 0);
        assert_eq!(pool.committed_slot_count(), 1);
        let manifest = pool.manifest_entry("get-key", (300, 4)).unwrap();
        assert_eq!(manifest.slot, slot);
        assert_eq!(manifest.physical_state, OwnerSlotPhysicalState::Committed);
    }

    #[test]
    fn rejected_async_get_target_keeps_caller_bytes_until_holder_drop() {
        let mut pool = pool();
        let operation =
            OwnerTransferOpId::new(generation("target", 11), 42, OwnerTransferOpKind::Get);
        let slot = pool
            .claim_value(VALUE_LEN, 1)
            .pop()
            .expect("claim requester-local Get target");
        let (lease_id, prepared_slot) = prepared(pool.prepare_claimed_get_target(
            operation.clone(),
            "rejected-async-get".to_string(),
            (301, 4),
            None,
            slot.clone(),
        ));
        let capability = OwnerTargetWriteCapability {
            operation: operation.clone(),
            lease_id: lease_id.clone(),
            slot: prepared_slot.clone(),
        };
        let receipt = OwnerTransferReceipt {
            completion_id: operation.sequence,
            direction: OwnerTransferDirection::RdmaWrite,
            bytes: VALUE_LEN,
            source: None,
            target: prepared_slot.clone(),
            source_registration_epoch: 17,
            target_registration_epoch: prepared_slot.segment_registration_epoch,
        };
        assert!(matches!(
            pool.mark_target_data_ready(&operation, &lease_id, receipt),
            OwnerSegmentTransferOutcome::TargetDataReady { .. }
        ));
        pool.mark_data_ready_get_target_pending_visible(&capability)
            .expect("DataReady target becomes hidden resident");
        assert!(pool.retain_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(matches!(
            pool.begin_data_ready_get_target_commit(&capability),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));

        assert!(matches!(
            pool.abort_data_ready_visible_get_target(
                &operation,
                &lease_id,
                "master rejected expired Get Plan".to_string(),
            ),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert!(matches!(
            pool.abort_data_ready_visible_get_target(
                &operation,
                &lease_id,
                "terminal replay".to_string(),
            ),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert_eq!(pool.used_slot_count(), 1);
        assert_eq!(pool.pending_visible_slot_count(), 1);
        assert_eq!(
            pool.manifest_entry("rejected-async-get", (301, 4))
                .expect("aborted holder remains generation-safe until drop")
                .physical_state,
            OwnerSlotPhysicalState::Reclaiming
        );

        assert!(pool.release_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.used_slot_count(), 0);
        assert_eq!(pool.pending_visible_slot_count(), 0);
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
    }

    #[test]
    fn failed_get_target_aborts_the_exact_data_ready_prepared_slot() {
        let mut pool = pool();
        let operation =
            OwnerTransferOpId::new(generation("target", 11), 42, OwnerTransferOpKind::Get);
        let slot = pool.claim_value(VALUE_LEN, 1).pop().unwrap();
        let (lease_id, _) = prepared(pool.prepare_claimed_get_target(
            operation.clone(),
            "failed-get".to_string(),
            (301, 1),
            None,
            slot.clone(),
        ));
        let receipt = OwnerTransferReceipt {
            completion_id: operation.sequence,
            direction: OwnerTransferDirection::RdmaWrite,
            bytes: VALUE_LEN,
            source: None,
            target: slot,
            source_registration_epoch: 17,
            target_registration_epoch: 11,
        };
        assert!(matches!(
            pool.mark_target_data_ready(&operation, &lease_id, receipt),
            OwnerSegmentTransferOutcome::TargetDataReady { .. }
        ));
        assert!(matches!(
            pool.abort_target(&operation, &lease_id, "source failed".to_string()),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert_eq!(pool.used_slot_count(), 0);
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
    }

    #[test]
    fn get_target_stale_after_claim_keeps_the_claim_releasable() {
        let mut pool = pool();
        let claimed_target = pool.claim_value(VALUE_LEN, 1).pop().unwrap();
        let resident = pool.claim_value(VALUE_LEN, 1).pop().unwrap();
        assert!(pool.mark_prepared_slot_pending_visible(
            resident.allocation_id,
            resident.segment_offset,
            resident.capacity_bytes,
        ));
        assert!(pool.promote_pending_visible_slot_to_committed(
            resident.allocation_id,
            resident.segment_offset,
            resident.capacity_bytes,
        ));
        pool.install_committed_manifest(
            "raced-get",
            (302, 1),
            resident.allocation_id,
            crate::owner_segment::OwnerSlotScope::LocalExclusive,
            51,
        )
        .unwrap();

        let operation =
            OwnerTransferOpId::new(generation("target", 11), 43, OwnerTransferOpKind::Get);
        assert!(matches!(
            pool.prepare_claimed_get_target(
                operation,
                "raced-get".to_string(),
                (302, 1),
                None,
                claimed_target.clone(),
            ),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::StalePlan
        ));
        assert!(pool.release_prepared_slot(
            claimed_target.allocation_id,
            claimed_target.segment_offset,
            claimed_target.capacity_bytes,
        ));
        assert_eq!(pool.used_slot_count(), 1);
        assert!(pool.manifest_entry("raced-get", (302, 1)).is_some());
    }

    #[test]
    fn ssd_transient_source_claim_has_no_route_and_requires_exact_release() {
        let mut pool = pool();
        let slot = pool
            .claim_value(VALUE_LEN, 1)
            .pop()
            .expect("claim SSD transient source slot from the owner allocator");
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES - slot.capacity_bytes);
        assert!(
            pool.manifest_entry("ssd-source", (1, 1)).is_none(),
            "transient SSD staging must not publish an owner route"
        );
        assert!(!pool.release_prepared_slot(
            slot.allocation_id + 1,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(
            pool.total_free_bytes(),
            SEGMENT_BYTES - slot.capacity_bytes,
            "a stale allocation id must not free the transient slot"
        );
        assert!(pool.release_prepared_slot(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
        assert!(!pool.release_prepared_slot(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
    }

    #[test]
    fn target_abort_replays_and_stale_generation_never_allocates() {
        let mut pool = pool();
        let stale = op(2, OwnerTransferOpKind::Get);
        assert!(matches!(
            pool.prepare_target(
                stale,
                generation("target", 10),
                "stale".to_string(),
                (1, 1),
                VALUE_LEN,
                OwnerTargetDisposition::EphemeralCaller,
                None,
            ),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::StaleGeneration
        ));
        assert_eq!(pool.used_slot_count(), 0);

        let operation = op(3, OwnerTransferOpKind::Get);
        let (lease_id, old_slot) = prepared(pool.prepare_target(
            operation.clone(),
            generation("target", 11),
            "ephemeral".to_string(),
            (2, 1),
            VALUE_LEN,
            OwnerTargetDisposition::EphemeralCaller,
            None,
        ));
        assert!(matches!(
            pool.abort_target(&operation, &lease_id, "cancelled".to_string()),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert!(matches!(
            pool.abort_target(&operation, &lease_id, "retry".to_string()),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);

        let next = op(4, OwnerTransferOpKind::Get);
        let (_, new_slot) = prepared(pool.prepare_target(
            next,
            generation("target", 11),
            "next".to_string(),
            (3, 1),
            VALUE_LEN,
            OwnerTargetDisposition::EphemeralCaller,
            None,
        ));
        assert_eq!(new_slot.segment_offset, old_slot.segment_offset);
        assert_ne!(new_slot.allocation_id, old_slot.allocation_id);
    }

    #[test]
    fn master_route_rejection_rolls_back_only_the_exact_pending_target() {
        let mut pool = pool();
        let operation = op(7, OwnerTransferOpKind::ReplicaAppend);
        let (lease_id, slot) = prepared(pool.prepare_target(
            operation.clone(),
            generation("target", 11),
            "route-rejected".to_string(),
            (44, 1),
            VALUE_LEN,
            OwnerTargetDisposition::GlobalShared,
            None,
        ));
        let receipt = OwnerTransferReceipt {
            completion_id: 7,
            direction: OwnerTransferDirection::RdmaWrite,
            bytes: VALUE_LEN,
            source: None,
            target: slot.clone(),
            source_registration_epoch: 0,
            target_registration_epoch: slot.segment_registration_epoch,
        };
        let route_token = OwnerTargetRouteToken {
            key: "route-rejected".to_string(),
            put_id: (44, 1),
            operation: operation.clone(),
            target_owner: generation("target", 11),
            prior_route_epoch: 0,
            policy_epoch: 3,
            atomic_batch: None,
            plan_nonce: 7,
        };
        assert!(matches!(
            pool.begin_target_commit(&operation, &lease_id, receipt, Some(route_token),),
            OwnerSegmentTransferOutcome::TargetCommitPending { .. }
        ));
        assert!(matches!(
            pool.abort_target(&operation, &lease_id, "unsafe generic abort".to_string()),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::RouteCommitRequired
        ));
        assert!(matches!(
            pool.abort_route_rejected_target(
                &operation,
                &lease_id,
                "master rejected route".to_string(),
            ),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
        assert!(pool.manifest_entry("route-rejected", (44, 1)).is_none());
        assert!(matches!(
            pool.abort_route_rejected_target(&operation, &lease_id, "replay".to_string(),),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
        assert!(matches!(
            pool.prepare_target(
                operation,
                generation("target", 11),
                "route-rejected".to_string(),
                (44, 1),
                VALUE_LEN,
                OwnerTargetDisposition::GlobalShared,
                None,
            ),
            OwnerSegmentTransferOutcome::TargetAborted
        ));
    }

    #[test]
    fn source_lease_replay_blocks_reclaim_until_exact_release() {
        let mut pool = pool();
        let slot = pool.claim_value(VALUE_LEN, 1).pop().unwrap();
        assert!(pool.mark_prepared_slot_pending_visible(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(pool.promote_pending_visible_slot_to_committed(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        pool.install_committed_manifest(
            "source",
            (9, 2),
            slot.allocation_id,
            crate::owner_segment::OwnerSlotScope::GlobalShared,
            41,
        )
        .unwrap();

        let token = OwnerSourceRouteToken {
            key: "source".to_string(),
            put_id: (9, 2),
            route_epoch: 41,
            source: slot.clone(),
            atomic_batch: None,
            plan_nonce: 5,
        };
        let operation = op(5, OwnerTransferOpKind::Get);
        let (lease_id, acquired_slot) = match pool.acquire_source(operation.clone(), token.clone())
        {
            OwnerSegmentTransferOutcome::SourceAcquired { lease_id, slot } => (lease_id, slot),
            other => panic!("expected source lease, got {other:?}"),
        };
        assert_eq!(acquired_slot, slot);
        assert!(matches!(
            pool.acquire_source(operation.clone(), token),
            OwnerSegmentTransferOutcome::SourceAcquired { lease_id: ref replay_id, .. }
                if replay_id == &lease_id
        ));

        assert!(pool.release_committed_slot_route(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES - slot.capacity_bytes);
        let stale_reader = op(6, OwnerTransferOpKind::Get);
        let stale_token = OwnerSourceRouteToken {
            key: "source".to_string(),
            put_id: (9, 2),
            route_epoch: 41,
            source: slot,
            atomic_batch: None,
            plan_nonce: 6,
        };
        assert!(matches!(
            pool.acquire_source(stale_reader, stale_token),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::Conflict
        ));
        assert!(matches!(
            pool.release_source(&operation, &lease_id, OwnerTransferOutcome::Success),
            OwnerSegmentTransferOutcome::SourceReleased
        ));
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
        assert!(matches!(
            pool.release_source(&operation, &lease_id, OwnerTransferOutcome::Success),
            OwnerSegmentTransferOutcome::SourceReleased
        ));
    }

    #[test]
    fn plan_snapshot_cannot_acquire_a_source_reclaimed_before_get_to_target() {
        let mut pool = pool();
        let slot = pool.claim_value(VALUE_LEN, 1).pop().unwrap();
        assert!(pool.mark_prepared_slot_pending_visible(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(pool.promote_pending_visible_slot_to_committed(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        pool.install_committed_manifest(
            "stale-plan-source",
            (10, 1),
            slot.allocation_id,
            crate::owner_segment::OwnerSlotScope::GlobalShared,
            51,
        )
        .unwrap();

        let operation = op(7, OwnerTransferOpKind::Get);
        let plan_snapshot = OwnerSourceRouteToken {
            key: "stale-plan-source".to_string(),
            put_id: (10, 1),
            route_epoch: 51,
            source: slot.clone(),
            atomic_batch: None,
            plan_nonce: operation.sequence,
        };

        assert!(pool.release_committed_slot_route(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
        assert!(matches!(
            pool.acquire_source(operation, plan_snapshot),
            OwnerSegmentTransferOutcome::Error(ref error)
                if error.code == OwnerTransferErrorCode::NotFound
        ));
        assert_eq!(pool.used_slot_count(), 0);
        assert_eq!(pool.total_free_bytes(), SEGMENT_BYTES);
    }
}

#[cfg(test)]
mod owner_reclaim_slot_tests {
    use super::{
        ApiError, ClientKvApi, ClientKvApiNewArg, ExternalLocalFirstPutKeyReservation,
        ExternalPendingPutCtx, ExternalPendingPutFenceGuard, ExternalPutKeyOutcome,
        ExternalPutKeySharedOp, KvError, OwnerHotCacheCounters, OwnerHotCacheEntry,
        OwnerHotEvictionDispatch, OwnerHotEvictionEvent, OwnerHotPinAlias, OwnerHotReplicaIdentity,
        OwnerHotRetryQueue, OwnerHotSelectionDebt, OwnerHotSelectionFenceOutcome,
        OwnerKeyControlState, OwnerKeyControlTable, OwnerLocalSsdPutOutcome,
        OwnerLocalSsdPutSharedOp, OwnerReclaimRecord, OwnerRemotePutAdmission,
        OwnerRemotePutOutcome, OwnerRemotePutReservation, OwnerRemotePutSharedOp,
        OwnerSegmentAllocator, OwnerSlotLease, OwnerSlotRef,
        acquire_external_pending_put_fence_for_key, allocate_external_holding_ids,
        build_owner_hot_cache, clone_if_owner_hot_entry_matches,
        owner_hot_source_has_active_holders, pin_current_owner_hot_source_from_index,
    };
    use crate::config::TestSpecConfig;
    use crate::kv_ssd_storage::{KvSsdStorageInit, KvSsdStorageRootLimit, MIN_CAPACITY_BYTES};
    use crate::master_kv_router::msg_pack::{
        BatchOwnerReclaimReq, OwnerReclaimBacking, OwnerReclaimItem, OwnerReclaimItemState,
        OwnerReclaimPhase, OwnerReclaimReason, OwnerSourceEvictionVictim, OwnerSourceSsdPolicy,
    };
    use crate::p2p::msg_pack::MsgPack;
    use dashmap::DashMap;
    use parking_lot::Mutex;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Weak};
    use std::time::{Duration, Instant};

    fn pending_put_count(controls: &Arc<OwnerKeyControlTable>, key: &str) -> Option<u32> {
        controls
            .lock_key(key)
            .get(key)
            .map(|state| state.external_pending_puts)
    }

    fn controls_with_state(key: &str, state: OwnerKeyControlState) -> Arc<OwnerKeyControlTable> {
        let controls = Arc::new(OwnerKeyControlTable::default());
        controls.lock_key(key).insert(key.to_string(), state);
        controls
    }

    #[test]
    fn external_pending_put_guard_clone_releases_only_after_last_drop() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let guard = acquire_external_pending_put_fence_for_key(&controls, "clone-key")
            .expect("pending fence acquisition must succeed");
        let clone = guard.clone();
        assert_eq!(pending_put_count(&controls, "clone-key"), Some(1));

        drop(guard);
        assert_eq!(pending_put_count(&controls, "clone-key"), Some(1));
        drop(clone);
        assert_eq!(pending_put_count(&controls, "clone-key"), None);
    }

    #[test]
    fn stale_pending_put_guard_drop_cannot_erase_new_generation() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let old = acquire_external_pending_put_fence_for_key(&controls, "aba-key")
            .expect("old pending fence acquisition must succeed");
        let new = acquire_external_pending_put_fence_for_key(&controls, "aba-key")
            .expect("new pending fence acquisition must succeed");
        assert_eq!(pending_put_count(&controls, "aba-key"), Some(2));

        drop(old);
        assert_eq!(pending_put_count(&controls, "aba-key"), Some(1));
        drop(new);
        assert_eq!(pending_put_count(&controls, "aba-key"), None);
    }

    #[test]
    fn local_first_pending_guard_releases_both_key_counters() {
        let controls = controls_with_state(
            "local-key",
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                external_put: None,
                remote_put: None,
                local_ssd_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
                owner_materialization: None,
                local_access_fence: None,
            },
        );
        let guard = Arc::new(ExternalPendingPutFenceGuard {
            key: "local-key".to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_put_op: None,
            local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        });

        drop(guard);
        assert!(controls.lock_key("local-key").get("local-key").is_none());
    }

    #[limit_thirdparty::tokio::test]
    async fn same_key_put_waiter_reuses_one_leader_terminal_result() {
        let key = "put-singleflight";
        let op = ExternalPutKeySharedOp::new();
        let controls = controls_with_state(
            key,
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                external_put: Some(op.clone()),
                remote_put: None,
                local_ssd_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
                owner_materialization: None,
                local_access_fence: None,
            },
        );
        let leader = Arc::new(ExternalPendingPutFenceGuard {
            key: key.to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_put_op: Some(op.clone()),
            local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        });

        leader.mark_local_put_succeeded();
        assert_eq!(
            ::tokio::time::timeout(Duration::from_secs(1), op.wait())
                .await
                .expect("the follower must observe the leader terminal result"),
            ExternalPutKeyOutcome::Succeeded
        );
        drop(leader);
        assert!(controls.lock_key(key).get(key).is_none());
    }

    #[limit_thirdparty::tokio::test]
    async fn failed_put_leader_wakes_waiter_only_after_its_fence_is_released() {
        let key = "put-singleflight-failed";
        let op = ExternalPutKeySharedOp::new();
        let controls = controls_with_state(
            key,
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                external_put: Some(op.clone()),
                remote_put: None,
                local_ssd_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
                owner_materialization: None,
                local_access_fence: None,
            },
        );
        let leader = Arc::new(ExternalPendingPutFenceGuard {
            key: key.to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_put_op: Some(op.clone()),
            local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        });

        drop(leader);
        assert_eq!(
            ::tokio::time::timeout(Duration::from_secs(1), op.wait())
                .await
                .expect("the failed leader must wake its follower"),
            ExternalPutKeyOutcome::Failed
        );
        assert!(controls.lock_key(key).get(key).is_none());
    }

    #[limit_thirdparty::tokio::test]
    async fn every_remote_put_trigger_joins_one_owner_generation_flight() {
        let key = "remote-put-singleflight";
        let put_id = (77, 3);
        let test_spec_config = TestSpecConfig {
            owner_remote_put_max_inflight_bytes: Some(1),
            owner_remote_put_max_inflight_items: Some(1),
            ..Default::default()
        };
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config,
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority:
                crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: None,
        })
        .await
        .expect("construct test ClientKvApi");
        let saturation = api
            .inner()
            .owner_remote_put_admission
            .try_acquire(1)
            .expect("saturate new-leader admission");
        let op = OwnerRemotePutSharedOp::new(key, put_id, None, false);
        api.inner().owner_key_control.lock_key(key).insert(
            key.to_string(),
            OwnerKeyControlState {
                remote_put: Some(op.clone()),
                ..Default::default()
            },
        );
        api.inner()
            .owner_remote_put_counters
            .active
            .store(1, Ordering::Relaxed);
        api.inner()
            .owner_remote_put_counters
            .leaders
            .store(1, Ordering::Relaxed);

        for follower in 0..64 {
            let preferred = (follower == 0).then(|| "tier1".to_string());
            let protect_source = follower == 63;
            match api
                .inner()
                .begin_owner_remote_put(key, put_id, preferred, protect_source)
            {
                OwnerRemotePutReservation::Follower(joined) => {
                    assert!(Arc::ptr_eq(&joined, &op));
                }
                OwnerRemotePutReservation::Leader { .. } => {
                    panic!("a follower created a second remote Put leader")
                }
                OwnerRemotePutReservation::SourceUnavailable => {
                    panic!("a matching active remote Put flight was not reusable")
                }
                OwnerRemotePutReservation::NotAdmitted => {
                    panic!("a matching follower must bypass new-leader admission")
                }
            }
        }

        let request = op.request();
        assert_eq!(request.preferred_sub_cluster.as_deref(), Some("tier1"));
        assert!(request.protect_source_on_remote_complete);
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .followers
                .load(Ordering::Relaxed),
            64
        );
        assert!(matches!(
            api.inner()
                .begin_owner_remote_put(key, (78, 0), None, false),
            OwnerRemotePutReservation::SourceUnavailable
        ));

        assert!(
            api.inner()
                .finish_owner_remote_put(&op, OwnerRemotePutOutcome::Published)
        );
        assert!(
            !api.inner()
                .finish_owner_remote_put(&op, OwnerRemotePutOutcome::Failed)
        );
        assert_eq!(
            ::tokio::time::timeout(Duration::from_secs(1), op.wait())
                .await
                .expect("remote Put followers must observe the terminal result"),
            OwnerRemotePutOutcome::Published
        );
        assert!(
            api.inner()
                .owner_key_control
                .lock_key(key)
                .get(key)
                .is_none()
        );
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .active
                .load(Ordering::Relaxed),
            0
        );
        drop(saturation);
        assert_eq!(
            api.inner()
                .owner_remote_put_admission
                .active_bytes
                .load(Ordering::Relaxed),
            0
        );
    }

    #[limit_thirdparty::tokio::test]
    async fn tier1_batch_ack_joins_inflight_remote_puts_without_waiting_or_retransfer() {
        const BATCH_ITEMS: usize = 256;
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: TestSpecConfig::default(),
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority:
                crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: None,
        })
        .await
        .expect("construct test ClientKvApi");
        let mut flights = Vec::with_capacity(BATCH_ITEMS);

        for index in 0..BATCH_ITEMS {
            let key = format!("tier1-batch-follower-{index}");
            let put_id = (1000 + index as u64, 1);
            let op = OwnerRemotePutSharedOp::new(&key, put_id, None, false);
            api.inner().owner_key_control.lock_key(&key).insert(
                key.clone(),
                OwnerKeyControlState {
                    remote_put: Some(op.clone()),
                    ..Default::default()
                },
            );
            api.inner()
                .owner_remote_put_counters
                .active
                .fetch_add(1, Ordering::Relaxed);
            api.inner()
                .owner_remote_put_counters
                .leaders
                .fetch_add(1, Ordering::Relaxed);
            flights.push((key, put_id, op));
        }

        let started_at = Instant::now();
        for (key, put_id, _) in &flights {
            assert!(
                api.inner()
                    .start_remote_put_nonblocking(key, *put_id, None, false),
                "an existing exact-generation flight must be acknowledged"
            );
        }
        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "metadata-only batch acknowledgement must not wait for payload terminals"
        );
        assert!(
            flights
                .iter()
                .all(|(_, _, op)| op.outcome() == OwnerRemotePutOutcome::InFlight),
            "acknowledgement must return while every payload flight is still in progress"
        );
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .leaders
                .load(Ordering::Relaxed),
            BATCH_ITEMS as u64,
            "followers must not create replacement leaders"
        );
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .followers
                .load(Ordering::Relaxed),
            BATCH_ITEMS as u64
        );
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .transfers
                .load(Ordering::Relaxed),
            0,
            "followers must not start duplicate payload transfers"
        );

        for (_, _, op) in flights {
            assert!(
                api.inner()
                    .finish_owner_remote_put(&op, OwnerRemotePutOutcome::Published)
            );
        }
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .active
                .load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn remote_put_admission_is_exact_no_queue_and_raii_refunded() {
        let admission = OwnerRemotePutAdmission::new(Some(10), Some(2));
        let first = admission.try_acquire(4).expect("first permit");
        let second = admission.try_acquire(6).expect("second permit");
        assert!(
            admission.try_acquire(1).is_none(),
            "exhaustion must reject instead of queueing"
        );
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 2);
        assert_eq!(admission.peak_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(admission.peak_items.load(Ordering::Relaxed), 2);
        assert_eq!(admission.admitted.load(Ordering::Relaxed), 2);
        assert_eq!(admission.not_admitted.load(Ordering::Relaxed), 1);
        assert_eq!(admission.not_admitted_bytes.load(Ordering::Relaxed), 1);

        drop(first);
        let replacement = admission
            .try_acquire(4)
            .expect("a terminal permit refund must be immediately reusable");
        assert!(
            admission.try_acquire(1).is_none(),
            "the item safety ceiling remains exact after a byte refund"
        );
        drop((second, replacement));
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remote_put_transfer_releases_bytes_but_retains_item_credit() {
        let admission = OwnerRemotePutAdmission::new(Some(4), Some(1));
        let mut permit = admission.try_acquire(4).expect("transfer permit");

        assert!(permit.release_transfer_bytes());
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 1);
        assert!(
            admission.try_acquire(4).is_none(),
            "Done/Revoke must retain the item safety credit"
        );

        drop(permit);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 0);
        drop(
            admission
                .try_acquire(4)
                .expect("terminal item refund must be reusable"),
        );
    }

    #[test]
    fn remote_put_transfer_byte_release_is_exactly_once() {
        let admission = OwnerRemotePutAdmission::new(Some(4), Some(1));
        let mut permit = admission.try_acquire(4).expect("transfer permit");

        assert!(permit.release_transfer_bytes());
        assert!(!permit.release_transfer_bytes());
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 1);

        drop(permit);
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn remote_put_abort_raii_refunds_credits_before_and_after_transfer() {
        let admission = OwnerRemotePutAdmission::new(Some(8), Some(2));
        let before_transfer = admission.try_acquire(4).expect("pre-transfer permit");
        let mut after_transfer = admission.try_acquire(4).expect("post-transfer permit");
        assert!(after_transfer.release_transfer_bytes());
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 4);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 2);

        drop(before_transfer);
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 1);

        drop(after_transfer);
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn concurrent_remote_put_admission_never_oversubscribes_either_limit() {
        let admission = OwnerRemotePutAdmission::new(Some(32), Some(5));
        let entered = Arc::new(std::sync::Barrier::new(33));
        let release = Arc::new(std::sync::Barrier::new(33));
        let accepted = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::new();
        for _ in 0..32 {
            let admission = admission.clone();
            let entered = entered.clone();
            let release = release.clone();
            let accepted = accepted.clone();
            workers.push(std::thread::spawn(move || {
                let permit = admission.try_acquire(4);
                if permit.is_some() {
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
                entered.wait();
                release.wait();
                drop(permit);
            }));
        }
        entered.wait();
        assert_eq!(accepted.load(Ordering::Relaxed), 5);
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 5);
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 20);
        assert!(admission.peak_items.load(Ordering::Relaxed) <= 5);
        assert!(admission.peak_bytes.load(Ordering::Relaxed) <= 32);
        release.wait();
        for worker in workers {
            worker.join().expect("admission worker");
        }
        assert_eq!(admission.active_items.load(Ordering::Relaxed), 0);
        assert_eq!(admission.active_bytes.load(Ordering::Relaxed), 0);
    }

    #[limit_thirdparty::tokio::test]
    async fn new_remote_put_generation_replaces_old_without_aba_cleanup() {
        let key = "remote-put-generation-aba";
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: TestSpecConfig::default(),
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority:
                crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: None,
        })
        .await
        .expect("construct test ClientKvApi");
        let old = OwnerRemotePutSharedOp::new(key, (80, 0), None, false);
        let new = OwnerRemotePutSharedOp::new(key, (81, 0), None, false);
        api.inner().owner_key_control.lock_key(key).insert(
            key.to_string(),
            OwnerKeyControlState {
                remote_put: Some(old.clone()),
                ..Default::default()
            },
        );
        api.inner()
            .owner_remote_put_counters
            .active
            .store(2, Ordering::Relaxed);
        api.inner()
            .owner_key_control
            .lock_key(key)
            .get_mut(key)
            .expect("old generation control state")
            .install_remote_put_leader(new.clone());
        let current = api
            .inner()
            .owner_key_control
            .lock_key(key)
            .get(key)
            .and_then(|state| state.remote_put.clone())
            .expect("new generation must own the visible flight slot");
        assert!(Arc::ptr_eq(&current, &new));
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .active
                .load(Ordering::Relaxed),
            2
        );

        assert!(
            api.inner()
                .finish_owner_remote_put(&old, OwnerRemotePutOutcome::Obsolete)
        );
        let current = api
            .inner()
            .owner_key_control
            .lock_key(key)
            .get(key)
            .and_then(|state| state.remote_put.clone())
            .expect("old completion must retain the new generation flight");
        assert!(Arc::ptr_eq(&current, &new));
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .active
                .load(Ordering::Relaxed),
            1
        );

        assert!(
            api.inner()
                .finish_owner_remote_put(&new, OwnerRemotePutOutcome::Published)
        );
        assert!(
            api.inner()
                .owner_key_control
                .lock_key(key)
                .get(key)
                .is_none()
        );
        assert_eq!(
            api.inner()
                .owner_remote_put_counters
                .active
                .load(Ordering::Relaxed),
            0
        );
    }

    #[limit_thirdparty::tokio::test]
    async fn remote_and_local_ssd_flights_publish_independent_terminals() {
        let key = "parallel-backing-flights";
        let put_id = (82, 4);
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: TestSpecConfig::default(),
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority:
                crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: None,
        })
        .await
        .expect("construct test ClientKvApi");
        let remote = OwnerRemotePutSharedOp::new(key, put_id, None, false);
        let local_ssd = OwnerLocalSsdPutSharedOp::new(key, put_id);
        api.inner().owner_key_control.lock_key(key).insert(
            key.to_string(),
            OwnerKeyControlState {
                remote_put: Some(remote.clone()),
                local_ssd_put: Some(local_ssd.clone()),
                ..Default::default()
            },
        );
        api.inner()
            .owner_remote_put_counters
            .active
            .store(1, Ordering::Relaxed);
        api.inner()
            .owner_local_ssd_put_counters
            .active
            .store(1, Ordering::Relaxed);

        assert!(
            api.inner()
                .finish_owner_remote_put(&remote, OwnerRemotePutOutcome::Published)
        );
        {
            let controls = api.inner().owner_key_control.lock_key(key);
            let state = controls.get(key).expect("SSD flight must remain installed");
            assert!(state.remote_put.is_none());
            assert!(
                state
                    .local_ssd_put
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &local_ssd))
            );
        }
        assert_eq!(
            local_ssd.outcome(),
            None,
            "remote completion must not publish the SSD terminal"
        );
        assert!(
            api.inner()
                .finish_owner_local_ssd_put(&local_ssd, OwnerLocalSsdPutOutcome::Published)
        );
        assert_eq!(local_ssd.wait().await, OwnerLocalSsdPutOutcome::Published);
        assert!(
            api.inner()
                .owner_key_control
                .lock_key(key)
                .get(key)
                .is_none()
        );
    }

    #[limit_thirdparty::tokio::test]
    async fn early_ssd_byte_rejection_does_not_install_generation_flight() {
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/mnt/nvme0/mjq_build/push_sglang_fluxon_target"));
        let root = target.join("kv_ssd_tests").join(format!(
            "early-pre-admission-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let api = ClientKvApi::construct(ClientKvApiNewArg {
            test_spec_config: TestSpecConfig::default(),
            owner_hot_cache_capacity_bytes: None,
            owner_local_reserve_physical_capacity_bytes: 0,
            allocation_authority:
                crate::master_seg_manager::msg_pack::SegmentAllocationAuthority::Master,
            owner_placement_class:
                crate::master_seg_manager::msg_pack::OwnerPlacementClass::Invalid,
            ssd_storage: Some(KvSsdStorageInit {
                roots: vec![KvSsdStorageRootLimit {
                    root_dir: root.clone(),
                    limit_bytes: MIN_CAPACITY_BYTES,
                }],
                write_rate_limit_bytes_per_sec: Some(1),
                write_burst_bytes: Some(4),
                capacity_writeback_enabled: true,
            }),
        })
        .await
        .expect("construct SSD-enabled test ClientKvApi");
        let key = "early-pre-admission-drop";

        api.inner()
            .start_early_owner_local_ssd_puts(vec![(key.to_string(), (83, 1), 8)]);

        let usage = api
            .inner()
            .ssd_storage
            .as_ref()
            .expect("SSD store")
            .usage_snapshot();
        assert_eq!(usage.write_candidate_items, 1);
        assert_eq!(usage.write_admitted_items, 0);
        assert_eq!(usage.write_dropped_items, 1);
        assert_eq!(
            api.inner()
                .owner_local_ssd_put_counters
                .leaders
                .load(Ordering::Relaxed),
            0,
            "a byte-rejected early candidate must never enter singleflight"
        );
        assert!(
            api.inner()
                .owner_key_control
                .lock_key(key)
                .get(key)
                .is_none(),
            "a byte-rejected early candidate must not create per-key state"
        );

        api.inner()
            .ssd_storage
            .as_ref()
            .expect("SSD store")
            .close()
            .await
            .expect("close SSD store");
        drop(api);
        fs::remove_dir_all(root).expect("remove SSD test root");
    }

    #[test]
    fn failed_local_slot_release_keeps_key_fence_closed() {
        let controls = controls_with_state(
            "failed-slot",
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                external_put: None,
                remote_put: None,
                local_ssd_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
                owner_materialization: None,
                local_access_fence: None,
            },
        );
        let guard = Arc::new(ExternalPendingPutFenceGuard {
            key: "failed-slot".to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_put_op: None,
            local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(true),
        });

        drop(guard);
        let controls = controls.lock_key("failed-slot");
        assert_eq!(controls["failed-slot"].local_puts, 1);
        assert_eq!(controls["failed-slot"].external_pending_puts, 1);
    }

    #[test]
    fn committed_local_first_slot_disarms_drop_cleanup_before_fence_release() {
        let controls = controls_with_state(
            "committed-slot",
            OwnerKeyControlState {
                local_puts: 1,
                external_pending_puts: 1,
                external_put: None,
                remote_put: None,
                local_ssd_put: None,
                source_eviction_selection: None,
                reclaim: None,
                external_get: None,
                owner_materialization: None,
                local_access_fence: None,
            },
        );
        let guard = Arc::new(ExternalPendingPutFenceGuard {
            key: "committed-slot".to_string(),
            owner_key_control: controls.clone(),
            owns_local_put: true,
            local_put_op: None,
            local_put_succeeded: std::sync::atomic::AtomicBool::new(false),
            local_slot_cleanup_view: None,
            local_slot_lease: Mutex::new(None),
            local_slot_release_failed: std::sync::atomic::AtomicBool::new(false),
        });
        guard.attach_local_slot_lease(OwnerSlotLease {
            value_len: 8,
            slot_size: 8,
            slots: vec![OwnerSlotRef {
                owner: crate::owner_segment::OwnerGeneration::for_test("guard-test-owner"),
                allocation_id: 7,
                segment_offset: 8,
                capacity_bytes: 8,
                addr: 0x1008,
                base_addr: 0x1000,
                len: 8,
                segment_registration_epoch: 1,
            }],
        });

        guard.disarm_local_slot_lease();
        drop(guard);
        assert!(
            controls
                .lock_key("committed-slot")
                .get("committed-slot")
                .is_none()
        );
    }

    #[test]
    fn pending_ctx_clone_keeps_fence_after_explicit_cache_invalidation() {
        let controls = Arc::new(OwnerKeyControlTable::default());
        let fence = acquire_external_pending_put_fence_for_key(&controls, "cached-key")
            .expect("pending fence acquisition must succeed");
        let cache = moka::sync::Cache::new(1);
        let identity = ("cached-key".to_string(), 10, 2);
        cache.insert(
            identity.clone(),
            ExternalPendingPutCtx {
                peer_id: None,
                src_offset: 0,
                target_base_addr: 0,
                target_offset: 0,
                len: 1,
                make_replica_task: false,
                remote_replica_admitted: false,
                preferred_sub_cluster: None,
                local_reserve_slot: None,
                local_reserve_slot_size: None,
                atomic_group: None,
                radix: None,
                _pending_fence: fence,
            },
        );
        let clone = cache.get(&identity).expect("pending ctx must exist");
        cache.invalidate(&identity);
        cache.run_pending_tasks();
        assert_eq!(pending_put_count(&controls, "cached-key"), Some(1));

        drop(clone);
        assert_eq!(pending_put_count(&controls, "cached-key"), None);
    }

    #[test]
    fn external_holding_ids_are_nonzero_and_unique_for_resident_pages() {
        let counter = AtomicU64::new(1);
        let first = allocate_external_holding_ids(&counter, 1);
        let second = allocate_external_holding_ids(&counter, 1);
        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_ne!(first, second);
    }

    #[test]
    fn external_holding_batch_reserves_one_contiguous_unique_range() {
        let counter = AtomicU64::new(1);
        let first = allocate_external_holding_ids(&counter, 3);
        let following = allocate_external_holding_ids(&counter, 1);
        let second = allocate_external_holding_ids(&counter, 2);
        assert_eq!(first, 1);
        assert_eq!(following, 4);
        assert_eq!(second, 5);
        assert_eq!(counter.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn hot_source_pin_requires_the_same_version_and_allocation() {
        let current = Arc::new(7u64);
        let current_weak = Arc::downgrade(&current);
        let other = Arc::new(7u64);
        let other_weak = Arc::downgrade(&other);

        let pinned = clone_if_owner_hot_entry_matches((10, 2), &current, (10, 2), &current_weak)
            .expect("matching source should be pinned");
        assert!(Arc::ptr_eq(&pinned, &current));
        assert_eq!(Arc::strong_count(&current), 2);
        drop(pinned);

        assert!(
            clone_if_owner_hot_entry_matches((10, 2), &current, (10, 3), &current_weak).is_none()
        );
        assert!(
            clone_if_owner_hot_entry_matches((10, 2), &current, (10, 2), &other_weak).is_none()
        );
    }

    #[test]
    fn pressure_victim_rejects_an_extra_active_holder() {
        let indexed = Arc::new(7u64);
        let selected = indexed.clone();
        assert_eq!(Arc::strong_count(&selected), 2);
        assert!(
            !owner_hot_source_has_active_holders(&selected),
            "index plus the temporary selection pin is reclaimable"
        );

        let active_reader = indexed.clone();
        assert_eq!(Arc::strong_count(&selected), 3);
        assert!(owner_hot_source_has_active_holders(&selected));

        drop(active_reader);
        assert!(!owner_hot_source_has_active_holders(&selected));
    }

    #[limit_thirdparty::tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn single_key_source_selection_fence_closes_late_get_and_rolls_back() {
        use crate::client_kv_api::PutOptionalArgs;
        use crate::kvcore_test_lib::{
            integration_test_lock, start_master_and_client, stop_master_and_client,
        };

        let _test_guard = integration_test_lock().await;
        let (master, client) = start_master_and_client(
            "source_selection_fence_master",
            "source_selection_fence_owner",
        )
        .await;
        let owner_view = client.client_kv_api_view();
        let inner = owner_view.client_kv_api().inner();
        let join_probe_leader = match inner
            .reserve_external_local_first_put_key("put-join-probe", true, true)
            .expect("the first same-key Put must become leader")
        {
            ExternalLocalFirstPutKeyReservation::Leader(leader) => leader,
            ExternalLocalFirstPutKeyReservation::Wait(_) => {
                panic!("the first same-key Put cannot be a follower")
            }
            ExternalLocalFirstPutKeyReservation::WaitForMaterialization(_) => {
                panic!("the first same-key Put has no owner materialization to join")
            }
            ExternalLocalFirstPutKeyReservation::WaitForGet(_) => {
                panic!("the first same-key Put has no Get leader to join")
            }
            ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(_) => {
                panic!("an unfenced key cannot wait for local access")
            }
        };
        let join_probe_waiter = match inner
            .reserve_external_local_first_put_key("put-join-probe", true, true)
            .expect("the second same-key Put must join the leader")
        {
            ExternalLocalFirstPutKeyReservation::Wait(waiter) => waiter,
            ExternalLocalFirstPutKeyReservation::Leader(_) => {
                panic!("the second same-key Put must not claim a second leader fence")
            }
            ExternalLocalFirstPutKeyReservation::WaitForMaterialization(_) => {
                panic!("a local-Put follower must not wait for an absent materialization")
            }
            ExternalLocalFirstPutKeyReservation::WaitForGet(_) => {
                panic!("a local-Put follower must not wait for an absent Get")
            }
            ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(_) => {
                panic!("a local-Put follower must wait on the leader result")
            }
        };
        join_probe_leader.mark_local_put_succeeded();
        assert_eq!(
            join_probe_waiter.wait().await,
            ExternalPutKeyOutcome::Succeeded
        );
        drop(join_probe_leader);

        let key = "selection-single";
        inner
            .put(key, &[7u8; 4096], PutOptionalArgs::default())
            .await
            .expect("owner put must publish a committed route");
        let (holder, _remote) = inner
            .get(key)
            .await
            .expect("owner get must succeed")
            .expect("committed route must be readable");
        drop(holder);

        let cached = inner
            .get_cached_info
            .get(key)
            .expect("committed owner source must be indexed");
        let identity = OwnerHotReplicaIdentity {
            key: key.to_string(),
            put_time_ms: cached.put_time_ms,
            put_version: cached.put_version,
        };
        let source = cached.mem_holder.clone();
        drop(cached);
        assert!(
            !owner_hot_source_has_active_holders(&source),
            "the initial victim check must see only index plus selection pins"
        );

        // Reproduce the r11 TOCTOU: a local Get acquires the single victim after
        // the dispatcher's first holder check but before its fence is installed.
        let late_reader = inner
            .local_visible_mem_holder(key)
            .expect("late local Get must acquire the source before fencing");
        assert!(matches!(
            inner.owner_hot_install_source_selection_fence(&identity, &source),
            OwnerHotSelectionFenceOutcome::TemporarilyPinned
        ));
        assert!(inner.get_cached_info.contains_key(key));
        assert!(
            inner
                .owner_key_control
                .lock_key(key)
                .get(key)
                .is_none_or(|state| state.source_eviction_selection.is_none()),
            "a pinned victim must roll back its source fence"
        );

        drop(late_reader);
        assert!(matches!(
            inner.owner_hot_install_source_selection_fence(&identity, &source),
            OwnerHotSelectionFenceOutcome::Fenced
        ));
        assert!(!inner.get_cached_info.contains_key(key));
        assert!(inner.local_visible_mem_holder(key).is_none());
        assert!(
            acquire_external_pending_put_fence_for_key(&inner.owner_key_control, key).is_err(),
            "a new local Put must not cross a source-selection fence"
        );
        let rollback_waiter = match inner
            .reserve_external_local_first_put_key(key, true, true)
            .expect("idempotent local-first Put must asynchronously wait for source selection")
        {
            ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(waiter) => waiter,
            ExternalLocalFirstPutKeyReservation::Leader(_) => {
                panic!("a source-fenced key cannot claim a local-Put leader")
            }
            ExternalLocalFirstPutKeyReservation::Wait(_) => {
                panic!("a source-fenced key has no local-Put leader to join")
            }
            ExternalLocalFirstPutKeyReservation::WaitForMaterialization(_) => {
                panic!("a source-fenced key has no owner materialization to join")
            }
            ExternalLocalFirstPutKeyReservation::WaitForGet(_) => {
                panic!("a source-fenced key has no owner Get to join")
            }
        };
        let second_rollback_waiter = match inner
            .reserve_external_local_first_put_key(key, true, true)
            .expect("all idempotent local-first Puts must share the fence completion")
        {
            ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(waiter) => waiter,
            _ => panic!("a second source-fence waiter must not claim a Put fence"),
        };
        let cancelled_rollback_waiter = match inner
            .reserve_external_local_first_put_key(key, true, true)
            .expect("a cancellable Put may subscribe to the same fence completion")
        {
            ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(waiter) => waiter,
            _ => panic!("a cancellable source-fence waiter must not claim a Put fence"),
        };
        // Simulate an external RPC being cancelled before reclaim completes.
        // Dropping one receiver must neither retain the physical source nor
        // prevent the shared generation from waking the remaining waiters.
        drop(cancelled_rollback_waiter);
        assert!(matches!(
            inner.reserve_external_local_first_put_key(key, true, false),
            Err(KvError::Api(ApiError::KeyBeingWritten { .. }))
        ));
        drop(source);
        let master_id = master
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;

        let mismatched = OwnerReclaimItem {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version.wrapping_add(1)),
            epoch: 90,
            backing: OwnerReclaimBacking::Allocation,
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        };
        let mismatch_resp = super::reclaim::handle_batch_owner_reclaim(
            &owner_view,
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase: OwnerReclaimPhase::Prepare,
                    items: vec![mismatched],
                },
                raw_bytes: Vec::new(),
            },
            master_id.clone().into(),
        )
        .await;
        assert_eq!(
            mismatch_resp.serialize_part.items[0].state,
            OwnerReclaimItemState::Busy
        );
        assert!(
            inner
                .owner_key_control
                .lock_key(&identity.key)
                .get(&identity.key)
                .is_some_and(|state| state.source_eviction_selection.is_some()),
            "a mismatched Prepare must not consume the owner selection"
        );

        let matching = OwnerReclaimItem {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version),
            epoch: 100,
            backing: OwnerReclaimBacking::Allocation,
            reason: OwnerReclaimReason::OwnerCapacityEviction,
        };
        let prepare_resp = super::reclaim::handle_batch_owner_reclaim(
            &owner_view,
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase: OwnerReclaimPhase::Prepare,
                    items: vec![matching.clone()],
                },
                raw_bytes: Vec::new(),
            },
            master_id.clone().into(),
        )
        .await;
        assert!(
            prepare_resp.serialize_part.items[0].state == OwnerReclaimItemState::Prepared,
            "matching Prepare must promote the single selection into reclaim"
        );
        assert!(inner.local_visible_mem_holder(key).is_none());

        let abort_resp = super::reclaim::handle_batch_owner_reclaim(
            &owner_view,
            MsgPack {
                serialize_part: BatchOwnerReclaimReq {
                    phase: OwnerReclaimPhase::Abort,
                    items: vec![matching],
                },
                raw_bytes: Vec::new(),
            },
            master_id.into(),
        )
        .await;
        assert!(abort_resp.serialize_part.items[0].state == OwnerReclaimItemState::Aborted);
        assert!(
            inner.local_visible_mem_holder(key).is_some(),
            "Abort must restore the exact committed local index"
        );
        for mut waiter in [rollback_waiter, second_rollback_waiter] {
            ::tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if *waiter.borrow_and_update() {
                        break;
                    }
                    waiter
                        .changed()
                        .await
                        .expect("rollback completion sender must remain live");
                }
            })
            .await
            .expect("Abort must wake every live source-fence Put waiter");
        }
        assert!(matches!(
            inner.reserve_external_local_first_put_key(key, true, true),
            Err(KvError::Api(ApiError::KeyAlreadyExists { .. }))
        ));

        let trigger_weak = {
            let cached = inner
                .get_cached_info
                .get(key)
                .expect("Abort-restored trigger must be indexed");
            Arc::downgrade(&cached.mem_holder)
        };
        let retry_event = OwnerHotEvictionEvent {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version),
            memory_info: trigger_weak,
            selection_debt: OwnerHotSelectionDebt::new(
                4096,
                inner.owner_hot_counters.selection_debt_bytes.clone(),
            ),
            retry: true,
            source_eviction_victim: None,
            retry_failures: 1,
        };
        match inner.owner_hot_prepare_eviction(&retry_event) {
            super::OwnerHotEvictionPreparation::Ready { trigger, source } => {
                assert_eq!(trigger, identity);
                assert_eq!(source.key, key);
            }
            _ => panic!("a current single-key retry must be ready"),
        }
        retry_event.selection_debt.release();

        let cached = inner
            .get_cached_info
            .get(key)
            .expect("the aborted source must still be locally indexed");
        let source = cached.mem_holder.clone();
        drop(cached);
        assert!(matches!(
            inner.owner_hot_install_source_selection_fence(&identity, &source),
            OwnerHotSelectionFenceOutcome::Fenced
        ));
        drop(source);
        assert!(
            inner.owner_hot_install_source_selection_debt(
                identity.clone(),
                OwnerHotSelectionDebt::new(
                    4096,
                    inner.owner_hot_counters.selection_debt_bytes.clone(),
                ),
            )
        );
        let mut direct_delete_waiter = match inner
            .reserve_external_local_first_put_key(key, true, true)
            .expect("direct-delete source fence must expose an async waiter")
        {
            ExternalLocalFirstPutKeyReservation::WaitForLocalAccess(waiter) => waiter,
            _ => panic!("direct-delete source fence must not admit a local Put"),
        };
        super::reclaim::complete_owner_source_eviction(
            inner,
            &OwnerSourceEvictionVictim {
                key: key.to_string(),
                put_id: (identity.put_time_ms, identity.put_version),
                backing: OwnerReclaimBacking::Allocation,
                global_demotion_reserved: false,
                ssd_backing_len: None,
                ssd_policy: OwnerSourceSsdPolicy::Drop,
            },
            101,
        )
        .expect("one direct-delete response must release and finalize the local source");
        assert!(inner.local_visible_mem_holder(key).is_none());
        assert!(!inner.owner_source_eviction_selected.contains_key(&identity));
        assert!(
            inner.owner_key_control.lock_key(key).get(key).is_none(),
            "direct local completion must clear the source fence in one call"
        );
        ::tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if *direct_delete_waiter.borrow_and_update() {
                    break;
                }
                direct_delete_waiter
                    .changed()
                    .await
                    .expect("finalize completion sender must remain live");
            }
        })
        .await
        .expect("direct-delete Finalize must wake the source-fence Put waiter");
        let post_delete_leader = match inner
            .reserve_external_local_first_put_key(key, true, true)
            .expect("a physically reclaimed key must be eligible for a new local Put")
        {
            ExternalLocalFirstPutKeyReservation::Leader(leader) => leader,
            _ => panic!("a reclaimed key must re-evaluate to a fresh leader"),
        };
        drop(post_delete_leader);

        stop_master_and_client(master, client).await;
    }

    #[test]
    fn dispatcher_pin_and_reclaim_prepare_are_serialized_by_the_current_index() {
        let current = Mutex::new(Some(((10, 2), Arc::new(7u64))));
        let weak = Arc::downgrade(&current.lock().as_ref().unwrap().1);

        let pinned = match pin_current_owner_hot_source_from_index((10, 2), &weak, || {
            current
                .lock()
                .as_ref()
                .map(|(put_id, value)| (*put_id, value.clone()))
        }) {
            super::OwnerHotPinResult::Pinned(pinned) => pinned,
            _ => panic!("current source must pin under the owner fence"),
        };
        let prepared_after_pin = current.lock().take().unwrap().1;
        assert_eq!(Arc::strong_count(&prepared_after_pin), 2);
        drop(pinned);
        assert_eq!(Arc::strong_count(&prepared_after_pin), 1);

        // If Prepare wins and moves the sole Arc out of the local index, the
        // dispatcher observes an absent current entry. It must not upgrade the
        // Weak that now points into Prepared, or Commit's try_unwrap could race
        // an unexpected second holder.
        let current = Mutex::new(Some(((11, 3), Arc::new(9u64))));
        let weak = Arc::downgrade(&current.lock().as_ref().unwrap().1);
        let prepared_before_pin = current.lock().take().unwrap().1;
        assert!(matches!(
            pin_current_owner_hot_source_from_index((11, 3), &weak, || {
                current
                    .lock()
                    .as_ref()
                    .map(|(put_id, value)| (*put_id, value.clone()))
            }),
            super::OwnerHotPinResult::ReclaimBusy
        ));
        assert_eq!(Arc::strong_count(&prepared_before_pin), 1);

        drop(prepared_before_pin);
        assert!(matches!(
            pin_current_owner_hot_source_from_index((11, 3), &weak, || None),
            super::OwnerHotPinResult::Stale
        ));
    }

    #[test]
    fn owner_hot_retry_queue_is_exactly_once_and_keeps_selection_debt() {
        let counters = Arc::new(OwnerHotCacheCounters::default());
        let retry_queue = OwnerHotRetryQueue::new(counters.clone());
        let identity = OwnerHotReplicaIdentity {
            key: "retry-key".to_string(),
            put_time_ms: 12,
            put_version: 3,
        };
        let debt = OwnerHotSelectionDebt::new(64, counters.selection_debt_bytes.clone());
        let event = OwnerHotEvictionEvent {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version),
            memory_info: Weak::new(),
            selection_debt: debt,
            retry: true,
            source_eviction_victim: None,
            retry_failures: 0,
        };
        retry_queue.schedule(event.clone(), "first failure");
        retry_queue.schedule(event, "duplicate failure");
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 64);

        let due = retry_queue.take_due_batch(Instant::now() + Duration::from_secs(10), 128);
        assert_eq!(due.len(), 1);
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 64);
        assert!(
            retry_queue
                .take_due_batch(Instant::now() + Duration::from_secs(30), 128)
                .is_empty(),
            "a dispatched retry stays exactly once until dispatcher acknowledgement"
        );
        let accepted = retry_queue
            .take_for_inflight(&identity)
            .expect("dispatcher acknowledgement must take the authoritative event");
        accepted.selection_debt.release();
        assert_eq!(retry_queue.len(), 0);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn owner_hot_retry_queue_deadlines_stay_bounded_under_high_churn() {
        const CHURN: usize = 20_000;

        let counters = Arc::new(OwnerHotCacheCounters::default());
        let retry_queue = OwnerHotRetryQueue::new(counters.clone());
        let identity = OwnerHotReplicaIdentity {
            key: "retry-churn".to_string(),
            put_time_ms: 21,
            put_version: 5,
        };
        let debt = OwnerHotSelectionDebt::new(64, counters.selection_debt_bytes.clone());
        let event = OwnerHotEvictionEvent {
            key: identity.key.clone(),
            put_id: (identity.put_time_ms, identity.put_version),
            memory_info: Weak::new(),
            selection_debt: debt,
            retry: true,
            source_eviction_victim: None,
            retry_failures: 0,
        };

        for _ in 0..CHURN {
            retry_queue.schedule(event.clone(), "repeated failure");
        }
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 1);

        let due = retry_queue.take_due_batch(Instant::now() + Duration::from_secs(10), 1);
        assert_eq!(due.len(), 1);
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 0);

        for _ in 0..CHURN {
            retry_queue.schedule(due[0].clone(), "repeated dispatcher failure");
        }
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 1);

        let event = retry_queue
            .take_for_inflight(&identity)
            .expect("live retry must remain available to the dispatcher");
        assert_eq!(retry_queue.len(), 0);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 0);
        event.selection_debt.release();
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 0);

        let remove_identity = OwnerHotReplicaIdentity {
            key: "retry-remove-churn".to_string(),
            put_time_ms: 22,
            put_version: 6,
        };
        let remove_debt = OwnerHotSelectionDebt::new(32, counters.selection_debt_bytes.clone());
        let remove_event = OwnerHotEvictionEvent {
            key: remove_identity.key.clone(),
            put_id: (remove_identity.put_time_ms, remove_identity.put_version),
            memory_info: Weak::new(),
            selection_debt: remove_debt,
            retry: true,
            source_eviction_victim: None,
            retry_failures: 0,
        };
        for _ in 0..CHURN {
            retry_queue.schedule(remove_event.clone(), "remove churn");
        }
        assert_eq!(retry_queue.len(), 1);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 1);
        retry_queue.remove(&remove_identity);
        assert_eq!(retry_queue.len(), 0);
        assert_eq!(retry_queue.state.lock().deadlines.len(), 0);
        assert_eq!(counters.selection_debt_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn hot_cache_only_dispatches_size_removals_and_keeps_capacity() {
        let counters = Arc::new(OwnerHotCacheCounters::default());
        let retry_queue = Arc::new(OwnerHotRetryQueue::new(counters.clone()));
        let (tx, mut rx) = limit_thirdparty::tokio::sync::ampsc::unbounded_channel();
        let cache = build_owner_hot_cache(10, counters.clone(), retry_queue, tx);
        let entry = |put_version| OwnerHotCacheEntry {
            put_id: (10, put_version),
            memory_info: Weak::new(),
            weight_bytes: 6,
        };
        let alias = |key: &str, put_version: usize| OwnerHotPinAlias {
            key: key.to_string(),
            memory_info_ptr: put_version,
        };

        cache.insert("explicit".to_string(), [alias("explicit", 0)], entry(0));
        cache.run_pending_tasks();
        cache.invalidate(&"explicit".to_string());
        cache.run_pending_tasks();
        assert_eq!(counters.size_evictions.load(Ordering::Relaxed), 0);

        cache.insert("size-a".to_string(), [alias("size-a", 1)], entry(1));
        cache.insert("size-b".to_string(), [alias("size-b", 2)], entry(2));
        cache.run_pending_tasks();
        assert!(counters.size_evictions.load(Ordering::Relaxed) >= 1);
        assert_eq!(cache.max_capacity(), Some(10));
        let dispatch = rx
            .try_recv()
            .expect("the Moka listener must emit lightweight metadata without pinning");
        let OwnerHotEvictionDispatch::Victim(event) = dispatch else {
            panic!("the Moka listener must emit a victim event")
        };
        assert!(event.memory_info.upgrade().is_none());
    }

    #[test]
    fn pointwise_batch_visibility_skips_reclaim_fenced_keys() {
        let keys = vec![
            "local-a".to_string(),
            "fenced".to_string(),
            "local-b".to_string(),
        ];
        let controls = OwnerKeyControlTable::default();
        controls.lock_key("fenced").insert(
            "fenced".to_string(),
            OwnerKeyControlState {
                local_puts: 0,
                external_pending_puts: 0,
                external_put: None,
                remote_put: None,
                local_ssd_put: None,
                source_eviction_selection: None,
                reclaim: Some(OwnerReclaimRecord::Committed(OwnerReclaimItem {
                    key: "fenced".to_string(),
                    ..OwnerReclaimItem::default()
                })),
                external_get: None,
                owner_materialization: None,
                local_access_fence: Some(::tokio::sync::watch::channel(false).0),
            },
        );
        let mut resolved_keys = Vec::new();
        let visible = keys
            .iter()
            .map(|key| {
                let shard = controls.lock_key(key);
                if shard
                    .get(key)
                    .is_some_and(|state| state.local_access_fenced())
                {
                    None
                } else {
                    resolved_keys.push(key.to_string());
                    Some(key.to_string())
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(
            visible,
            vec![
                Some("local-a".to_string()),
                None,
                Some("local-b".to_string())
            ]
        );
        assert_eq!(resolved_keys, vec!["local-a", "local-b"]);
    }

    #[test]
    fn sharded_owner_control_does_not_globally_block_unrelated_keys() {
        let controls = OwnerKeyControlTable::default();
        let first = "shard-key-a";
        let first_shard = OwnerKeyControlTable::shard_index(first);
        let second = (0..10_000)
            .map(|idx| format!("shard-key-b-{idx}"))
            .find(|key| OwnerKeyControlTable::shard_index(key) != first_shard)
            .expect("a key on another owner-control shard must exist");

        let _first_guard = controls.lock_key(first);
        assert!(
            controls.shards[OwnerKeyControlTable::shard_index(&second)]
                .try_lock()
                .is_some(),
            "one key fence must not block an unrelated shard"
        );
        assert!(
            controls.shards[first_shard].try_lock().is_none(),
            "the same shard must remain serialized while its guard is held"
        );
    }

    #[test]
    fn committed_slot_becomes_free_only_after_route_and_holder_are_released() {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_test_segment("allocator-test-owner", 0, 0, 16 * 1024, 16 * 1024, None)
            .unwrap();
        let slot = pool.claim_available(8 * 1024, 1).pop().unwrap();
        assert!(pool.mark_prepared_slot_pending_visible(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(pool.retain_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(pool.promote_pending_visible_slot_to_committed(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));

        assert!(pool.release_committed_slot_route(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.used_slot_count(), 1);
        assert_eq!(pool.total_free_bytes(), 8 * 1024);

        assert!(pool.release_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.used_slot_count(), 0);
        assert_eq!(pool.largest_free_bytes(), 16 * 1024);
    }

    #[test]
    fn global_shared_to_local_exclusive_reuses_the_same_segment_allocation() {
        let mut allocator = OwnerSegmentAllocator::default();
        allocator
            .install_test_segment(
                "allocator-test-owner",
                0x1000,
                0x1000,
                16 * 1024,
                8 * 1024,
                None,
            )
            .unwrap();
        let slot = allocator.claim_available(8 * 1024, 1).pop().unwrap();
        assert!(allocator.mark_prepared_slot_pending_visible(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(allocator.retain_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(allocator.promote_pending_visible_slot_to_committed(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));

        // LocalExclusive -> GlobalShared drops only the local resident holder.
        assert!(allocator.release_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(allocator.committed_route_only_matches(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(allocator.total_free_bytes(), 8 * 1024);

        // A requester-local Get takes a holder on that exact route-owned slot;
        // no new OffsetAllocator allocation or payload address is involved.
        assert!(allocator.retain_committed_route_only_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(allocator.used_slot_count(), 1);
        assert_eq!(allocator.total_free_bytes(), 8 * 1024);
        assert!(allocator.release_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(allocator.committed_route_only_matches(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));

        // Only GlobalShared eviction removes the final route reference and
        // returns the physical extent to the same segment allocator.
        assert!(allocator.release_committed_slot_route(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(allocator.used_slot_count(), 0);
        assert_eq!(allocator.total_free_bytes(), 16 * 1024);
    }

    #[test]
    fn owner_reclaim_releases_committed_route_and_resident_holder_as_one_pool_update() {
        const SLOT_SIZE: u64 = 8 * 1024;
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_test_segment("allocator-test-owner", 0, 0, 16 * 1024, 16 * 1024, None)
            .unwrap();
        let slot = pool
            .claim_available(SLOT_SIZE, 1)
            .pop()
            .expect("slot should be free");
        assert!(pool.mark_prepared_slot_pending_visible(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(pool.retain_resident_slot_holder(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert!(pool.promote_pending_visible_slot_to_committed(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.committed_slot_count(), 1);

        assert!(pool.release_committed_resident_slot(
            slot.allocation_id,
            slot.segment_offset,
            slot.capacity_bytes,
        ));
        assert_eq!(pool.committed_slot_count(), 0);
        assert_eq!(pool.largest_free_bytes(), 16 * 1024);
    }

    #[test]
    fn failed_pending_get_releases_only_its_slot() {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_test_segment("allocator-test-owner", 0, 0, 24 * 1024, 24 * 1024, None)
            .unwrap();
        let slots = pool.claim_available(8 * 1024, 2);
        let failed = &slots[0];
        let unrelated = &slots[1];
        assert!(pool.mark_prepared_slot_pending_visible(
            failed.allocation_id,
            failed.segment_offset,
            failed.capacity_bytes,
        ));
        assert!(pool.retain_resident_slot_holder(
            failed.allocation_id,
            failed.segment_offset,
            failed.capacity_bytes,
        ));
        assert!(pool.release_resident_slot_holder(
            failed.allocation_id,
            failed.segment_offset,
            failed.capacity_bytes,
        ));
        assert!(!pool.release_prepared_slot(
            failed.allocation_id,
            failed.segment_offset,
            failed.capacity_bytes,
        ));
        assert!(pool.release_prepared_slot(
            unrelated.allocation_id,
            unrelated.segment_offset,
            unrelated.capacity_bytes,
        ));
        assert_eq!(pool.used_slot_count(), 0);
    }

    #[test]
    fn owner_segment_is_installed_once_and_never_detached() {
        let mut pool = OwnerSegmentAllocator::default();
        assert!(
            pool.install_test_segment("allocator-test-owner", 0, 0, 16 * 1024, 12 * 1024, None,)
                .is_ok()
        );
        assert!(
            pool.install_test_segment("allocator-test-owner", 0, 0, 16 * 1024, 8 * 1024, None,)
                .is_err()
        );
        assert_eq!(pool.physical_capacity_bytes(), 16 * 1024);
    }

    #[test]
    fn target_control_is_epoch_idempotent_and_byte_bounded() {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_test_segment("allocator-test-owner", 0, 0, 16 * 1024, 12 * 1024, None)
            .unwrap();

        assert_eq!(pool.apply_target_control(1, 12 * 1024), Ok(false));
        assert!(pool.apply_target_control(1, 8 * 1024).is_err());
        assert!(pool.apply_target_control(3, 8 * 1024).is_err());
        assert!(pool.apply_target_control(2, 0).is_err());
        assert!(pool.apply_target_control(2, 20 * 1024).is_err());
        assert_eq!(pool.apply_target_control(2, 8 * 1024), Ok(true));
        assert_eq!(pool.controller_epoch, 2);
        assert_eq!(pool.local_target_bytes, 8 * 1024);
        assert_eq!(pool.apply_target_control(3, 4 * 1024), Ok(true));
    }

    #[test]
    fn capacity_report_size_classes_merge_master_interest_without_reservation() {
        let mut pool = OwnerSegmentAllocator::default();
        pool.install_test_segment("allocator-test-owner", 0, 0, 32 * 1024, 0, Some(4 * 1024))
            .unwrap();
        assert_eq!(pool.capacity_report_size_classes(), vec![4 * 1024]);
        assert_eq!(pool.total_free_bytes(), 32 * 1024);

        assert_eq!(
            pool.track_capacity_report_size_classes(&[12 * 1024, 8 * 1024]),
            Ok(true)
        );
        assert_eq!(
            pool.capacity_report_size_classes(),
            vec![4 * 1024, 8 * 1024, 12 * 1024]
        );
        assert_eq!(pool.total_free_bytes(), 32 * 1024);
        assert_eq!(
            pool.track_capacity_report_size_classes(&[8 * 1024]),
            Ok(false)
        );
        assert!(pool.track_capacity_report_size_classes(&[123]).is_err());
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsSet {
    pub mean: f64,
    pub p99: i64,
    pub p95: i64,
    pub min: i64,
    pub max: i64,
    pub timestamps: Vec<MetricTimestamp>,
}

// Removed StageScope: no longer using stage-scoped gauges; we record
// timestamps (t1..t4) and emit stage success/error directly.

impl MetricsSet {
    /// Convert to Prometheus format string
    pub fn to_prometheus_format(&self, metric_name: &str, client_id: &str) -> String {
        let mut result = String::new();

        // Traditional aggregated metrics (mean, p99, p95, min, max)
        result.push_str(&format!(
            "kvcache_{}_mean{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.mean
        ));

        result.push_str(&format!(
            "kvcache_{}_p99{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.p99
        ));

        result.push_str(&format!(
            "kvcache_{}_p95{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.p95
        ));

        result.push_str(&format!(
            "kvcache_{}_min{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.min
        ));

        result.push_str(&format!(
            "kvcache_{}_max{{client=\"{}\"}} {}\n",
            metric_name, client_id, self.max
        ));

        result.push_str(&format!(
            "kvcache_{}_sample_count{{client=\"{}\"}} {}\n",
            metric_name,
            client_id,
            self.timestamps.len()
        ));

        // Add metrics for unique keys and operations
        let unique_keys: std::collections::HashSet<_> = self
            .timestamps
            .iter()
            .filter_map(|ts| ts.key_opt.as_ref())
            .collect();
        result.push_str(&format!(
            "kvcache_{}_unique_keys_count{{client=\"{}\"}} {}\n",
            metric_name,
            client_id,
            unique_keys.len()
        ));

        let unique_ops: std::collections::HashSet<_> = self
            .timestamps
            .iter()
            .filter_map(|ts| ts.ope_id_opt.as_ref())
            .collect();
        result.push_str(&format!(
            "kvcache_{}_unique_operations_count{{client=\"{}\"}} {}\n",
            metric_name,
            client_id,
            unique_ops.len()
        ));

        // Generate individual timestamp events for Grafana state visualization
        for timestamp in &self.timestamps {
            let phase_name = timestamp.kind.get_phase_name();
            let state_value = timestamp.kind.to_prometheus_value();
            let event_type = if timestamp.kind.is_begin() {
                "begin"
            } else {
                "end"
            };

            // Create a metric for each timestamp event
            result.push_str(&format!(
                "kvcache_operation_event{{client=\"{}\",phase=\"{}\",event=\"{}\",key=\"{}\",op_id=\"{}\"}} {} {}\n",
                client_id,
                phase_name,
                event_type,
                timestamp.key_opt.as_deref().unwrap_or("unknown"),
                timestamp.ope_id_opt.as_deref().unwrap_or("unknown"),
                state_value,
                timestamp.time
            ));
        }

        result
    }

    /// Get the most recent timestamp for this metric type
    pub fn get_latest_timestamp(&self) -> Option<&MetricTimestamp> {
        self.timestamps.iter().max_by_key(|ts| ts.time)
    }

    /// Get operation timeline grouped by operation ID
    pub fn get_operation_timeline(
        &self,
    ) -> std::collections::HashMap<String, Vec<&MetricTimestamp>> {
        let mut timeline = std::collections::HashMap::new();

        for ts in &self.timestamps {
            if let Some(op_id) = &ts.ope_id_opt {
                timeline
                    .entry(op_id.clone())
                    .or_insert_with(Vec::new)
                    .push(ts);
            }
        }

        // Sort each operation's timeline by timestamp
        for events in timeline.values_mut() {
            events.sort_by_key(|ts| ts.time);
        }

        timeline
    }

    /// Generate timeline events for Grafana visualization
    pub fn to_prometheus_timeline_format(&self, client_id: &str) -> String {
        let mut result = String::new();
        let timeline = self.get_operation_timeline();

        for (op_id, events) in timeline {
            for event in events {
                let phase_name = event.kind.get_phase_name();
                let state_value = event.kind.to_prometheus_value();
                let event_type = if event.kind.is_begin() {
                    "begin"
                } else {
                    "end"
                };

                result.push_str(&format!(
                    "kvcache_operation_timeline{{client=\"{}\",op_id=\"{}\",phase=\"{}\",event=\"{}\",key=\"{}\"}} {} {}\n",
                    client_id,
                    op_id,
                    phase_name,
                    event_type,
                    event.key_opt.as_deref().unwrap_or("unknown"),
                    state_value,
                    event.time
                ));
            }
        }

        result
    }
}

fn format_metrics_snapshot_prometheus(
    client_id: &str,
    timestamp_ms: i64,
    metrics: &std::collections::HashMap<String, MetricsSet>,
) -> String {
    let mut result = String::new();

    for (metric_name, metric_set) in metrics {
        result.push_str(&metric_set.to_prometheus_format(metric_name, client_id));
        result.push_str(&metric_set.to_prometheus_timeline_format(client_id));
    }

    result.push_str(&format!(
        "kvcache_metrics_report_timestamp{{client=\"{}\"}} {}\n",
        client_id, timestamp_ms
    ));

    result
}

impl ClientKvApiInner {
    pub(crate) fn owner_local_reserve_claim_lock(
        &self,
        slot_size: u64,
    ) -> Arc<limit_thirdparty::tokio::sync::AMutex<()>> {
        self.owner_local_reserve_claim_locks
            .entry(slot_size)
            .or_insert_with(|| Arc::new(limit_thirdparty::tokio::sync::AMutex::new(())))
            .clone()
    }

    pub(crate) fn owner_local_reserve_control_snapshot(&self) -> OwnerLocalReserveControlResp {
        let (
            controller_epoch,
            physical_capacity_bytes,
            local_target_bytes,
            allocated_bytes,
            free_bytes,
        ) = {
            let pool = self.owner_segment_allocator.lock();
            (
                pool.controller_epoch,
                pool.physical_capacity_bytes(),
                pool.local_target_bytes,
                pool.total_used_bytes(),
                pool.total_free_bytes(),
            )
        };
        let (applied_moka_bytes, moka_weighted_bytes) = self
            .owner_hot_cache
            .as_ref()
            .map(|cache| (cache.max_capacity().unwrap_or(0), cache.weighted_size()))
            .unwrap_or_default();
        let owner_node_start_time = self.view.cluster_manager().get_self_info().node_start_time;
        let global_target_bytes = physical_capacity_bytes.saturating_sub(local_target_bytes);
        let selected_fence_bytes = self
            .owner_hot_counters
            .source_eviction_selected_bytes
            .load(Ordering::Acquire);
        let settled = controller_epoch != 0
            && local_target_bytes == applied_moka_bytes
            && moka_weighted_bytes <= local_target_bytes
            && selected_fence_bytes == 0
            && self.owner_hot_retry_queue.len() == 0;
        OwnerLocalReserveControlResp {
            owner_node_start_time,
            controller_epoch,
            physical_capacity_bytes,
            local_target_bytes,
            global_target_bytes,
            allocated_bytes,
            free_bytes,
            applied_moka_bytes,
            moka_weighted_bytes,
            settled,
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 0,
        }
    }

    pub(crate) fn apply_owner_local_reserve_control(
        &self,
        req: &OwnerLocalReserveControlReq,
    ) -> KvResult<OwnerLocalReserveControlResp> {
        let actual_start_time = self.view.cluster_manager().get_self_info().node_start_time;
        if req.expected_owner_node_start_time != actual_start_time {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "owner local-reserve generation mismatch: expected={} actual={}",
                    req.expected_owner_node_start_time, actual_start_time
                ),
            }));
        }
        let OwnerLocalReserveControlOp::SetLocalTarget {
            controller_epoch,
            local_target_bytes,
        } = req.operation
        else {
            return Ok(self.owner_local_reserve_control_snapshot());
        };
        if self.owner_hot_cache.is_none() {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "owner scope budget requires owner-local Moka".to_string(),
            }));
        }
        let changed = {
            let mut pool = self.owner_segment_allocator.lock();
            pool.apply_target_control(controller_epoch, local_target_bytes)
                .map_err(|detail| KvError::Api(ApiError::InvalidArgument { detail }))?
        };
        if changed {
            let cache = self
                .owner_hot_cache
                .as_ref()
                .expect("validated owner-local Moka");
            cache.set_max_capacity(local_target_bytes).map_err(|err| {
                KvError::Api(ApiError::Unknown {
                    detail: format!("failed to apply owner local target bytes: {err}"),
                })
            })?;
            cache.run_pending_tasks();
            self.owner_local_reserve_rebalance_notify.notify_waiters();
        }
        Ok(self.owner_local_reserve_control_snapshot())
    }

    pub fn get_holding_len(&self) -> usize {
        self.external_get_holding.total()
    }

    pub fn runtime_observe_snapshot(&self) -> OwnerRuntimeObserveSnapshot {
        let ssd = self.kv_ssd_storage_usage_snapshot().unwrap_or_default();
        let mut external_get_holding_bytes = 0u64;
        for entry in self.external_get_holding.inner().iter() {
            external_get_holding_bytes =
                external_get_holding_bytes.saturating_add(entry.value().memory_info.len as u64);
        }
        let (hot_cache_capacity_bytes, hot_cache_entries, hot_cache_weighted_bytes) = self
            .owner_hot_cache
            .as_ref()
            .map(|cache| {
                (
                    cache.max_capacity().unwrap_or(0),
                    cache.entry_count(),
                    cache.weighted_size(),
                )
            })
            .unwrap_or_default();
        let (
            external_get_flights,
            external_get_flights_starting,
            external_get_flights_finishing,
            external_get_flights_revoking,
            external_get_undecided_interests,
            external_get_retained_interests,
        ) = {
            let mut flights = 0u64;
            let mut starting = 0u64;
            let mut finishing = 0u64;
            let mut revoking = 0u64;
            let mut undecided = 0u64;
            let mut retained = 0u64;
            // Metrics use a weak side index and never scan correctness fences.
            for op in self.external_get_flight_snapshot() {
                flights = flights.saturating_add(1);
                let state = op.state.lock();
                undecided = undecided.saturating_add(state.undecided as u64);
                retained = retained.saturating_add(state.retained as u64);
                match &state.phase {
                    ExternalGetKeySharedPhase::Starting
                    | ExternalGetKeySharedPhase::Started { .. } => {
                        starting = starting.saturating_add(1)
                    }
                    ExternalGetKeySharedPhase::Finishing { .. } => {
                        finishing = finishing.saturating_add(1)
                    }
                    ExternalGetKeySharedPhase::Revoking { .. } => {
                        revoking = revoking.saturating_add(1)
                    }
                    ExternalGetKeySharedPhase::Ready { .. }
                    | ExternalGetKeySharedPhase::Failed { .. } => {}
                }
            }
            (flights, starting, finishing, revoking, undecided, retained)
        };
        let (
            owner_segment_capacity_bytes,
            local_reserve_accounting_slot_size,
            local_reserve_raw_free_bytes,
            local_reserve_allocatable_slots,
            local_reserve_allocatable_bytes,
            local_reserve_slot_unallocatable_bytes,
            local_reserve_slot_unallocatable_ratio_ppm,
            local_reserve_slots_free,
            local_reserve_slots_prepared,
            local_reserve_slots_pending_visible,
            local_reserve_slots_committed,
        ) = {
            let pool = self.owner_segment_allocator.lock();
            let accounting_slot_size = pool
                .expected_slot_size
                .unwrap_or_else(|| pool.largest_pending_slot_size());
            let raw_free_bytes = pool.total_free_bytes();
            let allocatable = pool.allocatable_report(accounting_slot_size);
            let slot_unallocatable_ratio_ppm = if accounting_slot_size == 0 || raw_free_bytes == 0 {
                0
            } else {
                allocatable
                    .slot_unallocatable_bytes
                    .saturating_mul(1_000_000)
                    / raw_free_bytes
            };
            (
                pool.physical_capacity_bytes(),
                accounting_slot_size,
                raw_free_bytes,
                allocatable.allocatable_slots,
                allocatable.allocatable_bytes,
                allocatable.slot_unallocatable_bytes,
                slot_unallocatable_ratio_ppm,
                raw_free_bytes / crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES,
                u64::try_from(pool.prepared_slot_count()).unwrap_or(u64::MAX),
                u64::try_from(pool.pending_visible_slot_count()).unwrap_or(u64::MAX),
                u64::try_from(pool.committed_slot_count()).unwrap_or(u64::MAX),
            )
        };
        let local_reserve_control = self.owner_local_reserve_control_snapshot();
        OwnerRuntimeObserveSnapshot {
            ssd_capacity_bytes: ssd.capacity_bytes,
            ssd_used_bytes: ssd.used_bytes,
            ssd_persist_requests: ssd.persist_requests,
            ssd_persist_successes: ssd.persist_successes,
            ssd_persist_failures: ssd.persist_failures,
            ssd_persist_bytes: ssd.persist_bytes,
            ssd_persist_duration_us: ssd.persist_duration_us,
            ssd_persist_batch_requests: ssd.persist_batch_requests,
            ssd_persist_batch_items: ssd.persist_batch_items,
            ssd_persist_flush_batches: ssd.persist_flush_batches,
            ssd_persist_busy_batches: ssd.persist_busy_batches,
            ssd_persist_admission_skips: ssd.persist_admission_skips,
            ssd_persist_batch_duration_us: ssd.persist_batch_duration_us,
            ssd_write_candidate_items: ssd.write_candidate_items,
            ssd_write_candidate_bytes: ssd.write_candidate_bytes,
            ssd_write_admitted_items: ssd.write_admitted_items,
            ssd_write_admitted_bytes: ssd.write_admitted_bytes,
            ssd_write_dropped_items: ssd.write_dropped_items,
            ssd_write_dropped_bytes: ssd.write_dropped_bytes,
            ssd_write_refunded_items: ssd.write_refunded_items,
            ssd_write_refunded_bytes: ssd.write_refunded_bytes,
            ssd_load_requests: ssd.load_requests,
            ssd_load_successes: ssd.load_successes,
            ssd_load_misses: ssd.load_misses,
            ssd_load_failures: ssd.load_failures,
            ssd_load_bytes: ssd.load_bytes,
            ssd_load_duration_us: ssd.load_duration_us,
            ssd_memory_hits: ssd.memory_hits,
            ssd_disk_hits: ssd.disk_hits,
            ssd_outer_hits: ssd.outer_hits,
            ssd_removals: ssd.removals,
            ssd_stage_flights: self.ssd_stage_flights.len() as u64,
            ssd_stage_terminals: self.completed_ssd_stages.entry_count(),
            ssd_stage_ready_requests: self
                .ssd_stage_counters
                .ready_requests
                .load(Ordering::Relaxed),
            ssd_stage_ready_successes: self
                .ssd_stage_counters
                .ready_successes
                .load(Ordering::Relaxed),
            ssd_stage_ready_failures: self
                .ssd_stage_counters
                .ready_failures
                .load(Ordering::Relaxed),
            ssd_stage_ready_duration_us: self
                .ssd_stage_counters
                .ready_duration_us
                .load(Ordering::Relaxed),
            ssd_stage_execute_completions: self
                .ssd_stage_counters
                .execute_completions
                .load(Ordering::Relaxed),
            ssd_stage_terminal_published: self
                .ssd_stage_counters
                .terminal_published
                .load(Ordering::Relaxed),
            ssd_stage_terminal_cache_inserts: self
                .ssd_stage_counters
                .terminal_cache_inserts
                .load(Ordering::Relaxed),
            ssd_stage_terminal_cache_duration_us: self
                .ssd_stage_counters
                .terminal_cache_duration_us
                .load(Ordering::Relaxed),
            ssd_stage_response_send_attempts: self
                .ssd_stage_counters
                .response_send_attempts
                .load(Ordering::Relaxed),
            ssd_stage_response_send_successes: self
                .ssd_stage_counters
                .response_send_successes
                .load(Ordering::Relaxed),
            ssd_stage_response_send_failures: self
                .ssd_stage_counters
                .response_send_failures
                .load(Ordering::Relaxed),
            ssd_stage_response_send_duration_us: self
                .ssd_stage_counters
                .response_send_duration_us
                .load(Ordering::Relaxed),
            ssd_source_ready_wait_requests: self
                .ssd_stage_counters
                .source_ready_wait_requests
                .load(Ordering::Relaxed),
            ssd_source_ready_wait_successes: self
                .ssd_stage_counters
                .source_ready_wait_successes
                .load(Ordering::Relaxed),
            ssd_source_ready_wait_failures: self
                .ssd_stage_counters
                .source_ready_wait_failures
                .load(Ordering::Relaxed),
            ssd_source_ready_wait_duration_us: self
                .ssd_stage_counters
                .source_ready_wait_duration_us
                .load(Ordering::Relaxed),
            ssd_target_pull_requests: self
                .ssd_stage_counters
                .target_pull_requests
                .load(Ordering::Relaxed),
            ssd_target_pull_successes: self
                .ssd_stage_counters
                .target_pull_successes
                .load(Ordering::Relaxed),
            ssd_target_pull_failures: self
                .ssd_stage_counters
                .target_pull_failures
                .load(Ordering::Relaxed),
            ssd_target_pull_duration_us: self
                .ssd_stage_counters
                .target_pull_duration_us
                .load(Ordering::Relaxed),
            ssd_stage_done_detached: self
                .ssd_stage_counters
                .done_detached
                .load(Ordering::Relaxed),
            external_get_holding_entries: self.external_get_holding.total() as u64,
            external_get_holding_bytes,
            external_get_start_handles: self.external_get_start_registry.len() as u64,
            external_get_flights,
            external_get_flights_starting,
            external_get_flights_finishing,
            external_get_flights_revoking,
            external_get_undecided_interests,
            external_get_retained_interests,
            owner_local_probe_batches: self
                .planned_get_counters
                .local_probe_batches
                .load(Ordering::Relaxed),
            owner_local_probe_items: self
                .planned_get_counters
                .local_probe_items
                .load(Ordering::Relaxed),
            owner_local_probe_local_items: self
                .planned_get_counters
                .local_probe_local_items
                .load(Ordering::Relaxed),
            owner_local_probe_remote_items: self
                .planned_get_counters
                .local_probe_remote_items
                .load(Ordering::Relaxed),
            planned_cpu_get_batches: self.planned_get_counters.batches.load(Ordering::Relaxed),
            planned_cpu_get_local_items: self
                .planned_get_counters
                .local_items
                .load(Ordering::Relaxed),
            planned_cpu_get_leader_items: self
                .planned_get_counters
                .leader_items
                .load(Ordering::Relaxed),
            planned_cpu_get_follower_items: self
                .planned_get_counters
                .follower_items
                .load(Ordering::Relaxed),
            external_pending_put_entries: self.external_pending_puts.entry_count(),
            remote_put_flights_active: self
                .owner_remote_put_counters
                .active
                .load(Ordering::Relaxed),
            remote_put_flight_leaders: self
                .owner_remote_put_counters
                .leaders
                .load(Ordering::Relaxed),
            remote_put_flight_followers: self
                .owner_remote_put_counters
                .followers
                .load(Ordering::Relaxed),
            remote_put_source_unavailable: self
                .owner_remote_put_counters
                .source_unavailable
                .load(Ordering::Relaxed),
            remote_put_source_fenced: self
                .owner_remote_put_counters
                .source_fenced
                .load(Ordering::Relaxed),
            remote_put_source_missing: self
                .owner_remote_put_counters
                .source_missing
                .load(Ordering::Relaxed),
            remote_put_source_version_mismatch: self
                .owner_remote_put_counters
                .source_version_mismatch
                .load(Ordering::Relaxed),
            remote_put_transfers: self
                .owner_remote_put_counters
                .transfers
                .load(Ordering::Relaxed),
            remote_put_published: self
                .owner_remote_put_counters
                .published
                .load(Ordering::Relaxed),
            remote_put_already_satisfied: self
                .owner_remote_put_counters
                .already_satisfied
                .load(Ordering::Relaxed),
            remote_put_obsolete: self
                .owner_remote_put_counters
                .obsolete
                .load(Ordering::Relaxed),
            remote_put_failed: self
                .owner_remote_put_counters
                .failed
                .load(Ordering::Relaxed),
            remote_put_task_dropped: self
                .owner_remote_put_counters
                .task_dropped
                .load(Ordering::Relaxed),
            remote_put_admission_limit_bytes: self
                .owner_remote_put_admission
                .max_bytes
                .unwrap_or(0),
            remote_put_admission_limit_items: self
                .owner_remote_put_admission
                .max_items
                .unwrap_or(0),
            remote_put_admission_active_bytes: self
                .owner_remote_put_admission
                .active_bytes
                .load(Ordering::Relaxed),
            remote_put_admission_active_items: self
                .owner_remote_put_admission
                .active_items
                .load(Ordering::Relaxed),
            remote_put_admission_peak_bytes: self
                .owner_remote_put_admission
                .peak_bytes
                .load(Ordering::Relaxed),
            remote_put_admission_peak_items: self
                .owner_remote_put_admission
                .peak_items
                .load(Ordering::Relaxed),
            remote_put_admission_admitted: self
                .owner_remote_put_admission
                .admitted
                .load(Ordering::Relaxed),
            remote_put_admission_not_admitted: self
                .owner_remote_put_admission
                .not_admitted
                .load(Ordering::Relaxed),
            remote_put_admission_not_admitted_bytes: self
                .owner_remote_put_admission
                .not_admitted_bytes
                .load(Ordering::Relaxed),
            local_ssd_put_flights_active: self
                .owner_local_ssd_put_counters
                .active
                .load(Ordering::Relaxed),
            local_ssd_put_flight_leaders: self
                .owner_local_ssd_put_counters
                .leaders
                .load(Ordering::Relaxed),
            local_ssd_put_flight_followers: self
                .owner_local_ssd_put_counters
                .followers
                .load(Ordering::Relaxed),
            local_ssd_put_source_unavailable: self
                .owner_local_ssd_put_counters
                .source_unavailable
                .load(Ordering::Relaxed),
            local_ssd_put_published: self
                .owner_local_ssd_put_counters
                .published
                .load(Ordering::Relaxed),
            local_ssd_put_already_present: self
                .owner_local_ssd_put_counters
                .already_present
                .load(Ordering::Relaxed),
            local_ssd_put_dropped: self
                .owner_local_ssd_put_counters
                .dropped
                .load(Ordering::Relaxed),
            local_ssd_put_obsolete: self
                .owner_local_ssd_put_counters
                .obsolete
                .load(Ordering::Relaxed),
            local_ssd_put_failed: self
                .owner_local_ssd_put_counters
                .failed
                .load(Ordering::Relaxed),
            owner_segment_capacity_bytes,
            local_reserve_accounting_slot_size,
            local_reserve_raw_free_bytes,
            local_reserve_allocatable_slots,
            local_reserve_allocatable_bytes,
            local_reserve_slot_unallocatable_bytes,
            local_reserve_slot_unallocatable_ratio_ppm,
            local_reserve_slots_free,
            local_reserve_slots_prepared,
            local_reserve_slots_pending_visible,
            local_reserve_slots_committed,
            local_reserve_controller_epoch: local_reserve_control.controller_epoch,
            local_reserve_target_bytes: local_reserve_control.local_target_bytes,
            global_shared_target_bytes: local_reserve_control.global_target_bytes,
            owner_segment_allocated_bytes: local_reserve_control.allocated_bytes,
            local_reserve_applied_moka_bytes: local_reserve_control.applied_moka_bytes,
            local_reserve_moka_capacity_delta_bytes: local_reserve_control
                .local_target_bytes
                .abs_diff(local_reserve_control.applied_moka_bytes),
            local_reserve_settled: local_reserve_control.settled,
            hot_cache_capacity_bytes,
            hot_cache_entries,
            hot_cache_weighted_bytes,
            hot_size_evictions: self
                .owner_hot_counters
                .size_evictions
                .load(Ordering::Relaxed),
            hot_source_evict_handoff_members: self
                .owner_hot_counters
                .source_evict_handoff_members
                .load(Ordering::Relaxed),
            hot_source_evict_committed_members: self
                .owner_hot_counters
                .source_evict_committed_members
                .load(Ordering::Relaxed),
            hot_source_evict_restored_members: self
                .owner_hot_counters
                .source_evict_restored_members
                .load(Ordering::Relaxed),
            hot_source_evict_obsolete: self
                .owner_hot_counters
                .source_evict_obsolete
                .load(Ordering::Relaxed),
            hot_source_evict_dispatch_failed: self
                .owner_hot_counters
                .source_evict_dispatch_failed
                .load(Ordering::Relaxed),
            hot_source_eviction_selected: self.owner_source_eviction_selected.len() as u64,
            hot_source_evict_retry_entries: self.owner_hot_retry_queue.len() as u64,
            hot_source_evict_retry_scheduled: self
                .owner_hot_counters
                .source_evict_retry_scheduled
                .load(Ordering::Relaxed),
            hot_source_evict_retry_emitted: self
                .owner_hot_counters
                .source_evict_retry_emitted
                .load(Ordering::Relaxed),
            hot_selection_debt_bytes: self
                .owner_hot_counters
                .selection_debt_bytes
                .load(Ordering::Relaxed),
            hot_source_eviction_selected_bytes: self
                .owner_hot_counters
                .source_eviction_selected_bytes
                .load(Ordering::Relaxed),
            hot_eviction_skipped_stale: self
                .owner_hot_counters
                .skipped_stale
                .load(Ordering::Relaxed),
            hot_eviction_skipped_reclaim: self
                .owner_hot_counters
                .skipped_reclaim
                .load(Ordering::Relaxed),
            hot_eviction_skipped_active_holders: self
                .owner_hot_counters
                .skipped_active_holders
                .load(Ordering::Relaxed),
            hot_victim_duplicates: self
                .owner_hot_counters
                .victim_duplicates
                .load(Ordering::Relaxed),
            hot_victim_invalid_backing: self
                .owner_hot_counters
                .victim_invalid_backing
                .load(Ordering::Relaxed),
            grouped_put_done_batches: self
                .owner_hot_counters
                .grouped_put_done_batches
                .load(Ordering::Relaxed),
            grouped_put_done_items: self
                .owner_hot_counters
                .grouped_put_done_items
                .load(Ordering::Relaxed),
            legacy_put_done_batches: self
                .owner_hot_counters
                .legacy_put_done_batches
                .load(Ordering::Relaxed),
            legacy_put_done_items: self
                .owner_hot_counters
                .legacy_put_done_items
                .load(Ordering::Relaxed),
        }
    }

    pub fn get_cache_len(&self) -> usize {
        self.precommit_local_visible_info.len() + self.get_cached_info.len()
    }
    fn metrics_handle(&self) -> Arc<MetricsHandle> {
        self.metrics
            .get()
            .cloned()
            .expect("metrics handle not initialized")
    }

    pub fn locality_snapshot(&self) -> KvLocalitySnapshot {
        self.metrics_handle().get_locality_snapshot()
    }

    pub fn record_put_locality(&self, remote: bool, bytes: u64, transfer_us: i64) {
        self.metrics_handle()
            .record_put_io_locality(remote, bytes, transfer_us);
    }

    fn client_id_str(&self) -> String {
        self.view.cluster_manager().get_self_info().id.to_string()
    }

    fn node_role(&self) -> crate::cluster_manager::NodeRole {
        let member = self.view.cluster_manager().get_self_info();
        member.node_role()
    }

    /// Drain pending metric events, compute aggregates and update snapshot.
    pub fn drain_and_compute_metrics(&self) -> std::collections::HashMap<String, MetricsSet> {
        let mut results = std::collections::HashMap::new();

        // Helper to compute avg, p99, p95, min, max and collect timestamps
        let compute = |data: &mut Vec<i64>, timestamps: Vec<MetricTimestamp>| -> MetricsSet {
            if data.is_empty() {
                return MetricsSet {
                    mean: 0.0,
                    p99: 0,
                    p95: 0,
                    min: 0,
                    max: 0,
                    timestamps, // ✅ 保留timestamps，即使没有延迟数据也要上报时间节点
                };
            }
            data.sort_unstable();
            let len = data.len();
            let sum: i64 = data.iter().sum();
            let avg = sum as f64 / len as f64;
            let idx99 = ((len * 99 + 99) / 100).saturating_sub(1);
            let idx95 = ((len * 95 + 99) / 100).saturating_sub(1);
            let p99 = data[idx99.min(len - 1)];
            let p95 = data[idx95.min(len - 1)];
            let min = data[0];
            let max = data[len - 1];
            MetricsSet {
                mean: avg,
                p99,
                p95,
                min,
                max,
                timestamps,
            }
        };

        let metrics_handle = self.metrics_handle();

        // Drain put metrics
        let mut put_whole = Vec::new();
        let mut put_start = Vec::new();
        let mut put_transfer = Vec::new();
        let mut put_end = Vec::new();
        let mut put_rpc = Vec::new();
        let mut put_start_handle = Vec::new();
        let mut put_end_handle = Vec::new();
        let mut put_whole_timestamps = Vec::new();
        let mut put_start_timestamps = Vec::new();
        let mut put_transfer_timestamps = Vec::new();
        let mut put_end_timestamps = Vec::new();
        let mut put_rpc_timestamps = Vec::new();

        for m in metrics_handle.drain_put_metrics() {
            if let KvMetrics::Put {
                whole_put,
                start,
                transfer,
                end,
                rpc_of_put_start,
                start_handle,
                end_handle,
                key,
                put_id,
                start_timestamp_us,
                transfer_start_timestamp_us,
                end_start_timestamp_us,
                end_timestamp_us,
                ..
            } = m
            {
                if whole_put > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Total,
                        whole_put as f64 / 1_000_000.0,
                    );
                }
                if start > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Start,
                        start as f64 / 1_000_000.0,
                    );
                }
                if transfer > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Transfer,
                        transfer as f64 / 1_000_000.0,
                    );
                }
                if end > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::End,
                        end as f64 / 1_000_000.0,
                    );
                }
                if rpc_of_put_start > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Put,
                        RequestStage::Rpc,
                        rpc_of_put_start as f64 / 1_000_000.0,
                    );
                }
                // ✅ 使用源头时间戳，转换为毫秒
                let t1_ms = start_timestamp_us / 1000; // 操作开始
                let t2_ms = transfer_start_timestamp_us / 1000; // start结束/transfer开始
                let t3_ms = end_start_timestamp_us / 1000; // transfer结束/end开始
                let t4_ms = end_timestamp_us / 1000; // 操作结束

                put_whole.push(whole_put);
                put_start.push(start);
                put_transfer.push(transfer);
                put_end.push(end);
                put_rpc.push(rpc_of_put_start);
                if start_handle > 0 {
                    put_start_handle.push(start_handle);
                }
                if end_handle > 0 {
                    put_end_handle.push(end_handle);
                }

                // 使用真实的源头时间戳生成各阶段的Begin/End事件
                // Put Whole phase: t1 -> t4
                put_whole_timestamps.push(MetricTimestamp {
                    time: t1_ms, // Begin time - 真实源头时间戳
                    kind: MetricTimestampKind::PutWholeBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_whole_timestamps.push(MetricTimestamp {
                    time: t4_ms, // End time - 真实源头时间戳
                    kind: MetricTimestampKind::PutWholeEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put Start phase: t1 -> t2
                put_start_timestamps.push(MetricTimestamp {
                    time: t1_ms, // 真实的start开始时间
                    kind: MetricTimestampKind::PutStartBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_start_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的start结束时间
                    kind: MetricTimestampKind::PutStartEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put Transfer phase: t2 -> t3
                put_transfer_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的transfer开始时间
                    kind: MetricTimestampKind::PutTransferBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_transfer_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的transfer结束时间
                    kind: MetricTimestampKind::PutTransferEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put End phase: t3 -> t4
                put_end_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的end开始时间
                    kind: MetricTimestampKind::PutEndBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_end_timestamps.push(MetricTimestamp {
                    time: t4_ms, // 真实的end结束时间
                    kind: MetricTimestampKind::PutEndEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });

                // Put RPC phase: 通常与start阶段重合 t1 -> t2
                put_rpc_timestamps.push(MetricTimestamp {
                    time: t1_ms, // RPC开始时间
                    kind: MetricTimestampKind::PutRpcBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(put_id.clone()),
                });
                put_rpc_timestamps.push(MetricTimestamp {
                    time: t2_ms, // RPC结束时间 (大概在start阶段结束)
                    kind: MetricTimestampKind::PutRpcEnd,
                    key_opt: Some(key),
                    ope_id_opt: Some(put_id),
                });
            }
        }
        results.insert(
            "put_whole".to_string(),
            compute(&mut put_whole, put_whole_timestamps),
        );
        results.insert(
            "put_start".to_string(),
            compute(&mut put_start, put_start_timestamps),
        );
        results.insert(
            "put_transfer".to_string(),
            compute(&mut put_transfer, put_transfer_timestamps),
        );
        results.insert(
            "put_end".to_string(),
            compute(&mut put_end, put_end_timestamps),
        );
        results.insert(
            "put_rpc".to_string(),
            compute(&mut put_rpc, put_rpc_timestamps),
        );
        results.insert(
            "put_start_handle".to_string(),
            compute(&mut put_start_handle, vec![]),
        );
        results.insert(
            "put_end_handle".to_string(),
            compute(&mut put_end_handle, vec![]),
        );

        // Drain get metrics
        let mut get_whole = Vec::new();
        let mut get_start = Vec::new();
        let mut get_transfer = Vec::new();
        let mut get_end = Vec::new();
        let mut get_start_handle = Vec::new();
        let mut get_end_handle = Vec::new();
        let mut get_whole_timestamps = Vec::new();
        let mut get_start_timestamps = Vec::new();
        let mut get_transfer_timestamps = Vec::new();
        let mut get_end_timestamps = Vec::new();

        for m in metrics_handle.drain_get_metrics() {
            if let KvMetrics::Get {
                whole_get,
                start,
                transfer,
                end,
                start_handle,
                end_handle,
                key,
                get_id,
                start_timestamp_us,
                transfer_start_timestamp_us,
                end_start_timestamp_us,
                end_timestamp_us,
            } = m
            {
                if whole_get > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::Total,
                        whole_get as f64 / 1_000_000.0,
                    );
                }
                if start > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::Start,
                        start as f64 / 1_000_000.0,
                    );
                }
                if transfer > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::Transfer,
                        transfer as f64 / 1_000_000.0,
                    );
                }
                if end > 0 {
                    metrics_handle.observe_request_duration_with_labels(
                        OperationKind::Get,
                        RequestStage::End,
                        end as f64 / 1_000_000.0,
                    );
                }
                // ✅ 使用源头时间戳，转换为毫秒
                let t1_ms = start_timestamp_us / 1000; // 操作开始
                let t2_ms = transfer_start_timestamp_us / 1000; // start结束/transfer开始
                let t3_ms = end_start_timestamp_us / 1000; // transfer结束/end开始
                let t4_ms = end_timestamp_us / 1000; // 操作结束

                get_whole.push(whole_get);
                get_start.push(start);
                get_transfer.push(transfer);
                get_end.push(end);
                if start_handle > 0 {
                    get_start_handle.push(start_handle);
                }
                if end_handle > 0 {
                    get_end_handle.push(end_handle);
                }

                // 使用真实的源头时间戳生成各阶段的Begin/End事件
                // Get Whole phase: t1 -> t4
                get_whole_timestamps.push(MetricTimestamp {
                    time: t1_ms, // Begin time - 真实源头时间戳
                    kind: MetricTimestampKind::GetWholeBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_whole_timestamps.push(MetricTimestamp {
                    time: t4_ms, // End time - 真实源头时间戳
                    kind: MetricTimestampKind::GetWholeEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });

                // Get Start phase: t1 -> t2
                get_start_timestamps.push(MetricTimestamp {
                    time: t1_ms, // 真实的start开始时间
                    kind: MetricTimestampKind::GetStartBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_start_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的start结束时间
                    kind: MetricTimestampKind::GetStartEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });

                // Get Transfer phase: t2 -> t3
                get_transfer_timestamps.push(MetricTimestamp {
                    time: t2_ms, // 真实的transfer开始时间
                    kind: MetricTimestampKind::GetTransferBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_transfer_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的transfer结束时间
                    kind: MetricTimestampKind::GetTransferEnd,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });

                // Get End phase: t3 -> t4
                get_end_timestamps.push(MetricTimestamp {
                    time: t3_ms, // 真实的end开始时间
                    kind: MetricTimestampKind::GetEndBegin,
                    key_opt: Some(key.clone()),
                    ope_id_opt: Some(get_id.clone()),
                });
                get_end_timestamps.push(MetricTimestamp {
                    time: t4_ms, // 真实的end结束时间
                    kind: MetricTimestampKind::GetEndEnd,
                    key_opt: Some(key),
                    ope_id_opt: Some(get_id),
                });
            }
        }
        results.insert(
            "get_whole".to_string(),
            compute(&mut get_whole, get_whole_timestamps),
        );
        results.insert(
            "get_start".to_string(),
            compute(&mut get_start, get_start_timestamps),
        );
        results.insert(
            "get_transfer".to_string(),
            compute(&mut get_transfer, get_transfer_timestamps),
        );
        results.insert(
            "get_end".to_string(),
            compute(&mut get_end, get_end_timestamps),
        );
        results.insert(
            "get_start_handle".to_string(),
            compute(&mut get_start_handle, vec![]),
        );
        results.insert(
            "get_end_handle".to_string(),
            compute(&mut get_end_handle, vec![]),
        );

        // Update in MetricsHandle for non-draining readers
        let metrics_handle = self.metrics_handle();
        metrics_handle.set_latest_metrics_snapshot(results.clone());

        results
    }

    /// Returns a shared `Arc<AllMemholderRefCount>`, creating and storing its `Weak` in
    /// `all_memholder_refcount` if absent. All created `UserMemHolder`s share the same
    /// refcount tracker to coordinate drop lifecycle.
    pub fn get_or_init_all_memholder_refcount(&self) -> Arc<AllMemholderRefCount> {
        // Check if the OnceLock already contains a value
        if let Some(existing) = self.all_memholder_refcount.get() {
            if let Some(upgraded) = existing.upgrade() {
                return upgraded;
            }
        }

        // Create a new Arc<AllMemholderRefCount> and store its Weak reference in the OnceLock
        let new_ref = Arc::new(AllMemholderRefCount::new(self.view.clone_view()));
        let weak_ref = Arc::downgrade(&new_ref);
        if self.all_memholder_refcount.set(weak_ref).is_err() {
            // If setting the OnceLock fails, retrieve the existing value
            if let Some(existing) = self.all_memholder_refcount.get() {
                if let Some(upgraded) = existing.upgrade() {
                    return upgraded;
                }
            }
        }

        new_ref
    }

    pub(crate) fn owner_local_reserve_rebalance_notify(
        &self,
    ) -> Arc<limit_thirdparty::tokio::sync::Notify> {
        self.owner_local_reserve_rebalance_notify.clone()
    }

    pub(crate) fn owner_local_reserve_register_pending_demand(
        &self,
        slot_size: u64,
        demand_slots: usize,
    ) {
        let mut pool = self.owner_segment_allocator.lock();
        if pool.pending_demand_slots(slot_size) == 0 {
            // Do not let a completed/cancelled generation of this size
            // authorize pressure for a newly arriving claimant.
            pool.clear_failed_claim(slot_size);
        }
        let pending = pool
            .pending_demand_by_slot_size
            .entry(slot_size)
            .or_default();
        *pending = pending.saturating_add(demand_slots);
    }

    pub(crate) fn owner_local_reserve_consume_pending_demand(
        &self,
        slot_size: u64,
        demand_slots: usize,
    ) {
        let mut pool = self.owner_segment_allocator.lock();
        let Some(pending) = pool.pending_demand_by_slot_size.get_mut(&slot_size) else {
            return;
        };
        *pending = pending.saturating_sub(demand_slots);
        if *pending == 0 {
            pool.pending_demand_by_slot_size.remove(&slot_size);
            pool.clear_failed_claim(slot_size);
        }
    }
}
impl ClientKvApi {
    pub fn inner(&self) -> &ClientKvApiInner {
        &self.0
    }

    fn spawn_runtime_observe_reporter(&self) {
        let view = self.0.view.clone_view();
        let view_task = view.clone();
        view.spawn("client_runtime_observe_reporter", async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            let mut shutdown_waiter = view_task.register_shutdown_waiter();
            loop {
                tokio::select! {
                    _ = shutdown_waiter.wait() => break,
                    _ = interval.tick() => {
                        let snapshot = view_task.client_kv_api().inner().runtime_observe_snapshot();
                        if snapshot.ssd_capacity_bytes > 0 {
                            tracing::info!(
                                capacity_bytes = snapshot.ssd_capacity_bytes,
                                used_bytes = snapshot.ssd_used_bytes,
                                persist_requests = snapshot.ssd_persist_requests,
                                persist_successes = snapshot.ssd_persist_successes,
                                persist_failures = snapshot.ssd_persist_failures,
                                persist_bytes = snapshot.ssd_persist_bytes,
                                persist_duration_us = snapshot.ssd_persist_duration_us,
                                persist_batch_requests = snapshot.ssd_persist_batch_requests,
                                persist_batch_items = snapshot.ssd_persist_batch_items,
                                persist_flush_batches = snapshot.ssd_persist_flush_batches,
                                persist_busy_batches = snapshot.ssd_persist_busy_batches,
                                persist_admission_skips = snapshot.ssd_persist_admission_skips,
                                persist_batch_duration_us = snapshot.ssd_persist_batch_duration_us,
                                write_candidate_items = snapshot.ssd_write_candidate_items,
                                write_candidate_bytes = snapshot.ssd_write_candidate_bytes,
                                write_admitted_items = snapshot.ssd_write_admitted_items,
                                write_admitted_bytes = snapshot.ssd_write_admitted_bytes,
                                write_dropped_items = snapshot.ssd_write_dropped_items,
                                write_dropped_bytes = snapshot.ssd_write_dropped_bytes,
                                write_refunded_items = snapshot.ssd_write_refunded_items,
                                write_refunded_bytes = snapshot.ssd_write_refunded_bytes,
                                load_requests = snapshot.ssd_load_requests,
                                load_successes = snapshot.ssd_load_successes,
                                load_misses = snapshot.ssd_load_misses,
                                load_failures = snapshot.ssd_load_failures,
                                load_bytes = snapshot.ssd_load_bytes,
                                load_duration_us = snapshot.ssd_load_duration_us,
                                memory_hits = snapshot.ssd_memory_hits,
                                disk_hits = snapshot.ssd_disk_hits,
                                outer_hits = snapshot.ssd_outer_hits,
                                removals = snapshot.ssd_removals,
                                active_stage_flights = snapshot.ssd_stage_flights,
                                retained_stage_terminals = snapshot.ssd_stage_terminals,
                                "owner KV SSD storage snapshot"
                            );
                        }
                        let metrics = view_task.metric_reporter().metrics();
                        metrics.set_kv_holding_entries(
                            "owner_external_get_holding",
                            snapshot.external_get_holding_entries,
                        );
                        metrics.set_kv_holding_bytes(
                            "owner_external_get_holding",
                            snapshot.external_get_holding_bytes,
                        );
                        metrics.set_kv_external_pending_put_entries(
                            snapshot.external_pending_put_entries,
                        );
                        if snapshot.local_reserve_controller_epoch != 0 {
                            tracing::info!(
                                controller_epoch = snapshot.local_reserve_controller_epoch,
                                segment_capacity_bytes = snapshot.owner_segment_capacity_bytes,
                                local_target_bytes = snapshot.local_reserve_target_bytes,
                                global_target_bytes = snapshot.global_shared_target_bytes,
                                allocated_bytes = snapshot.owner_segment_allocated_bytes,
                                applied_moka_bytes = snapshot.local_reserve_applied_moka_bytes,
                                moka_capacity_delta_bytes = snapshot.local_reserve_moka_capacity_delta_bytes,
                                settled = snapshot.local_reserve_settled,
                                "owner segment scope-budget snapshot"
                            );
                        }
                        tracing::info!(
                            active_handles = snapshot.external_get_start_handles,
                            active_flights = snapshot.external_get_flights,
                            starting_flights = snapshot.external_get_flights_starting,
                            finishing_flights = snapshot.external_get_flights_finishing,
                            revoking_flights = snapshot.external_get_flights_revoking,
                            undecided_interests = snapshot.external_get_undecided_interests,
                            retained_interests = snapshot.external_get_retained_interests,
                            local_probe_batches = snapshot.owner_local_probe_batches,
                            local_probe_items = snapshot.owner_local_probe_items,
                            local_probe_local_items = snapshot.owner_local_probe_local_items,
                            local_probe_remote_items = snapshot.owner_local_probe_remote_items,
                            planned_cpu_batches = snapshot.planned_cpu_get_batches,
                            planned_cpu_local_items = snapshot.planned_cpu_get_local_items,
                            planned_cpu_leader_items = snapshot.planned_cpu_get_leader_items,
                            planned_cpu_follower_items = snapshot.planned_cpu_get_follower_items,
                            ssd_stage_ready_requests = snapshot.ssd_stage_ready_requests,
                            ssd_stage_ready_successes = snapshot.ssd_stage_ready_successes,
                            ssd_stage_ready_failures = snapshot.ssd_stage_ready_failures,
                            ssd_stage_ready_duration_us = snapshot.ssd_stage_ready_duration_us,
                            ssd_stage_execute_completions = snapshot.ssd_stage_execute_completions,
                            ssd_stage_terminal_published = snapshot.ssd_stage_terminal_published,
                            ssd_stage_terminal_cache_inserts = snapshot.ssd_stage_terminal_cache_inserts,
                            ssd_stage_terminal_cache_duration_us = snapshot.ssd_stage_terminal_cache_duration_us,
                            ssd_stage_response_send_attempts = snapshot.ssd_stage_response_send_attempts,
                            ssd_stage_response_send_successes = snapshot.ssd_stage_response_send_successes,
                            ssd_stage_response_send_failures = snapshot.ssd_stage_response_send_failures,
                            ssd_stage_response_send_duration_us = snapshot.ssd_stage_response_send_duration_us,
                            ssd_source_ready_wait_requests = snapshot.ssd_source_ready_wait_requests,
                            ssd_source_ready_wait_successes = snapshot.ssd_source_ready_wait_successes,
                            ssd_source_ready_wait_failures = snapshot.ssd_source_ready_wait_failures,
                            ssd_source_ready_wait_duration_us = snapshot.ssd_source_ready_wait_duration_us,
                            ssd_target_pull_requests = snapshot.ssd_target_pull_requests,
                            ssd_target_pull_successes = snapshot.ssd_target_pull_successes,
                            ssd_target_pull_failures = snapshot.ssd_target_pull_failures,
                            ssd_target_pull_duration_us = snapshot.ssd_target_pull_duration_us,
                            ssd_stage_done_detached = snapshot.ssd_stage_done_detached,
                            owner_segment_capacity_bytes = snapshot.owner_segment_capacity_bytes,
                            reserve_accounting_slot_size = snapshot.local_reserve_accounting_slot_size,
                            reserve_raw_free_bytes = snapshot.local_reserve_raw_free_bytes,
                            reserve_allocatable_slots = snapshot.local_reserve_allocatable_slots,
                            reserve_allocatable_bytes = snapshot.local_reserve_allocatable_bytes,
                            reserve_slot_unallocatable_bytes = snapshot.local_reserve_slot_unallocatable_bytes,
                            reserve_slot_unallocatable_ratio_ppm = snapshot.local_reserve_slot_unallocatable_ratio_ppm,
                            reserve_free = snapshot.local_reserve_slots_free,
                            reserve_prepared = snapshot.local_reserve_slots_prepared,
                            reserve_pending_visible = snapshot.local_reserve_slots_pending_visible,
                            reserve_committed = snapshot.local_reserve_slots_committed,
                            "owner Get lifecycle snapshot"
                        );
                        tracing::info!(
                            active = snapshot.remote_put_flights_active,
                            leaders = snapshot.remote_put_flight_leaders,
                            followers = snapshot.remote_put_flight_followers,
                            source_unavailable = snapshot.remote_put_source_unavailable,
                            source_fenced = snapshot.remote_put_source_fenced,
                            source_missing = snapshot.remote_put_source_missing,
                            source_version_mismatch = snapshot.remote_put_source_version_mismatch,
                            transfers = snapshot.remote_put_transfers,
                            published = snapshot.remote_put_published,
                            already_satisfied = snapshot.remote_put_already_satisfied,
                            obsolete = snapshot.remote_put_obsolete,
                            failed = snapshot.remote_put_failed,
                            task_dropped = snapshot.remote_put_task_dropped,
                            admission_limit_bytes = snapshot.remote_put_admission_limit_bytes,
                            admission_limit_items = snapshot.remote_put_admission_limit_items,
                            admission_active_bytes = snapshot.remote_put_admission_active_bytes,
                            admission_active_items = snapshot.remote_put_admission_active_items,
                            admission_peak_bytes = snapshot.remote_put_admission_peak_bytes,
                            admission_peak_items = snapshot.remote_put_admission_peak_items,
                            admission_admitted = snapshot.remote_put_admission_admitted,
                            admission_not_admitted = snapshot.remote_put_admission_not_admitted,
                            admission_not_admitted_bytes = snapshot.remote_put_admission_not_admitted_bytes,
                            "owner unified remote Put flight snapshot"
                        );
                        tracing::info!(
                            active = snapshot.local_ssd_put_flights_active,
                            leaders = snapshot.local_ssd_put_flight_leaders,
                            followers = snapshot.local_ssd_put_flight_followers,
                            source_unavailable = snapshot.local_ssd_put_source_unavailable,
                            published = snapshot.local_ssd_put_published,
                            already_present = snapshot.local_ssd_put_already_present,
                            dropped = snapshot.local_ssd_put_dropped,
                            obsolete = snapshot.local_ssd_put_obsolete,
                            failed = snapshot.local_ssd_put_failed,
                            "owner local SSD Put flight snapshot"
                        );
                        if snapshot.hot_cache_capacity_bytes > 0 {
                            tracing::info!(
                                capacity_bytes = snapshot.hot_cache_capacity_bytes,
                                entries = snapshot.hot_cache_entries,
                                weighted_bytes = snapshot.hot_cache_weighted_bytes,
                                size_evictions = snapshot.hot_size_evictions,
                                source_evict_handoff_members = snapshot.hot_source_evict_handoff_members,
                                source_evict_committed_members = snapshot.hot_source_evict_committed_members,
                                source_evict_restored_members = snapshot.hot_source_evict_restored_members,
                                source_evict_obsolete = snapshot.hot_source_evict_obsolete,
                                source_evict_dispatch_failed = snapshot.hot_source_evict_dispatch_failed,
                                source_eviction_selected = snapshot.hot_source_eviction_selected,
                                source_evict_retry_entries = snapshot.hot_source_evict_retry_entries,
                                source_evict_retry_scheduled = snapshot.hot_source_evict_retry_scheduled,
                                source_evict_retry_emitted = snapshot.hot_source_evict_retry_emitted,
                                selection_debt_bytes = snapshot.hot_selection_debt_bytes,
                                source_eviction_selected_bytes = snapshot.hot_source_eviction_selected_bytes,
                                skipped_stale = snapshot.hot_eviction_skipped_stale,
                                skipped_reclaim = snapshot.hot_eviction_skipped_reclaim,
                                skipped_active_holders = snapshot.hot_eviction_skipped_active_holders,
                                victim_duplicates = snapshot.hot_victim_duplicates,
                                victim_invalid_backing = snapshot.hot_victim_invalid_backing,
                                grouped_put_done_batches = snapshot.grouped_put_done_batches,
                                grouped_put_done_items = snapshot.grouped_put_done_items,
                                legacy_put_done_batches = snapshot.legacy_put_done_batches,
                                legacy_put_done_items = snapshot.legacy_put_done_items,
                                "owner hot source-eviction policy snapshot"
                            );
                        }
                    }
                }
            }
        });
    }

    pub fn attach_view(&self, view: ClientKvApiView) {
        self.0.view.attach(view);
    }

    pub async fn construct(arg: ClientKvApiNewArg) -> Result<Self, KvError> {
        tracing::info!("Constructing ClientKvApi in Client mode (PreView)");
        let ClientKvApiNewArg {
            test_spec_config,
            owner_hot_cache_capacity_bytes,
            owner_local_reserve_physical_capacity_bytes,
            allocation_authority,
            owner_placement_class,
            ssd_storage,
        } = arg;
        let ssd_storage = match ssd_storage {
            Some(init) => Some(Arc::new(KvSsdStorage::new(init).await?)),
            None => None,
        };
        let (owner_local_publish_tx, owner_local_publish_rx) =
            tokio::sync::ampsc::channel(OWNER_LOCAL_PUBLISH_QUEUE_CAPACITY);
        // The Moka eviction listener is synchronous and must never block while
        // holding Moka's housekeeper lock. Events contain only weak payload
        // references and are deduplicated by exact selected identities, so use
        // a lossless metadata channel instead of dropping victims when the old
        // bounded queue briefly filled under cache pressure.
        let (owner_hot_eviction_tx, owner_hot_eviction_rx) =
            tokio::sync::ampsc::unbounded_channel();
        let get_cached_info = Arc::new(DashMap::new());
        let owner_key_control = Arc::new(OwnerKeyControlTable::default());
        let owner_source_eviction_selected = Arc::new(DashMap::new());
        let owner_hot_counters = Arc::new(OwnerHotCacheCounters::default());
        let owner_remote_put_counters = Arc::new(OwnerRemotePutCounters::default());
        let owner_remote_put_admission = OwnerRemotePutAdmission::new(
            test_spec_config.owner_remote_put_max_inflight_bytes,
            test_spec_config.owner_remote_put_max_inflight_items,
        );
        let owner_local_ssd_put_counters = Arc::new(OwnerLocalSsdPutCounters::default());
        let owner_hot_retry_queue = Arc::new(OwnerHotRetryQueue::new(owner_hot_counters.clone()));
        let owner_hot_cache = owner_hot_cache_capacity_bytes.map(|capacity_bytes| {
            build_owner_hot_cache(
                capacity_bytes,
                owner_hot_counters.clone(),
                owner_hot_retry_queue.clone(),
                owner_hot_eviction_tx.clone(),
            )
        });

        let inner = ClientKvApiInner {
            view: ClientKvApiViewHolder::new(),
            test_spec_config,
            owner_local_reserve_physical_capacity_bytes,
            allocation_authority,
            owner_placement_class,
            ssd_storage,
            metrics: OnceLock::new(),
            all_memholder_refcount: OnceLock::new(),
            get_remote_kv_lock: AMapLock::new(Duration::from_secs(60)),
            get_cached_info,
            precommit_local_visible_info: DashMap::new(),
            pending_local_get_info: DashMap::new(),
            local_snapshot_info: DashMap::new(),
            owner_segment_allocator: Mutex::new(OwnerSegmentAllocator::default()),
            owner_local_reserve_claim_locks: DashMap::new(),
            owner_local_reserve_rebalance_notify: Arc::new(
                limit_thirdparty::tokio::sync::Notify::new(),
            ),
            owner_hot_selection_lock: limit_thirdparty::tokio::sync::AMutex::new(()),
            external_local_first_put_id_counter: AtomicU32::new(0),
            next_owner_source_eviction_operation_id: AtomicU64::new(1),
            owner_key_control,
            owner_hot_cache,
            owner_source_eviction_selected,
            owner_hot_counters,
            owner_remote_put_counters,
            owner_remote_put_admission,
            owner_local_ssd_put_counters,
            planned_get_counters: OwnerPlannedGetCounters::default(),
            ssd_stage_counters: OwnerSsdStageCounters::default(),
            owner_hot_retry_queue,
            owner_hot_eviction_tx,
            owner_hot_eviction_rx: Mutex::new(Some(owner_hot_eviction_rx)),
            external_invalidate_delete: EnsureMemholderMgmtDeleteHandle::new(
                OwnerExternalMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            delete_ack_batch: EnsureMemholderMgmtDeleteHandle::new(
                OwnerDeleteAckMemMgr::DELETE_SUBMIT_QUEUE_CAPACITY,
            ),
            owner_delete_ack_mgr: OwnerDeleteAckMemMgr::default(),
            external_get_holding: OwnerExternalMemMgr::default(),
            external_get_start_registry: DashMap::new(),
            external_get_flight_registry: DashMap::new(),
            external_get_local_probe_locks: AMapLock::new(Duration::from_secs(120)),
            completed_external_get_local_probes: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(120))
                .build(),
            planned_external_get_execute_locks: AMapLock::new(Duration::from_secs(120)),
            completed_planned_external_get_executes: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(120))
                .build(),
            ssd_stage_flights: DashMap::new(),
            owner_get_to_target_flights: DashMap::new(),
            owner_put_from_source_flights: DashMap::new(),
            owner_target_route_commit_flights: DashMap::new(),
            owner_transfer_peer_tracker: OwnerTransferPeerTracker::new(
                OWNER_TRANSFER_CLIENT_ACK_STREAM,
            ),
            owner_transfer_inbound_ack_watermarks: DashMap::new(),
            completed_ssd_stages: moka::future::Cache::builder()
                .time_to_live(SSD_STAGE_TERMINAL_TTL)
                .build(),
            next_external_get_start_handle: AtomicU64::new(1),
            next_external_holding_id: AtomicU64::new(1),
            external_pending_puts: moka::sync::Cache::builder()
                .time_to_live(Duration::from_secs(30 * 60))
                .segments(16)
                .build(),
            #[cfg(test)]
            test_record: crate::client_kv_api::client_test_record::ClientTestRecord::new(),
            rpc_caller_get_start: RPCCaller::new(),
            rpc_caller_get_revoke: RPCCaller::new(),
            rpc_caller_get_done: RPCCaller::new(),
            rpc_caller_batch_get_start: RPCCaller::new(),
            rpc_caller_batch_get_plan: RPCCaller::new(),
            rpc_caller_batch_get_bind: RPCCaller::new(),
            rpc_caller_batch_get_revoke: RPCCaller::new(),
            rpc_caller_batch_get_done: RPCCaller::new(),
            rpc_caller_put_start: RPCCaller::new(),
            rpc_caller_put_revoke: RPCCaller::new(),
            rpc_caller_put_done: RPCCaller::new(),
            rpc_caller_batch_put_start: RPCCaller::new(),
            rpc_caller_batch_put_revoke: RPCCaller::new(),
            rpc_caller_batch_put_done: RPCCaller::new(),
            rpc_caller_grouped_batch_put_done: RPCCaller::new(),
            rpc_caller_batch_prepare_put_keys: RPCCaller::new(),
            rpc_caller_batch_release_put_key_reservations: RPCCaller::new(),
            rpc_caller_put_append_start: RPCCaller::new(),
            rpc_caller_batch_put_append_start: RPCCaller::new(),
            rpc_caller_put_append_revoke: RPCCaller::new(),
            rpc_caller_put_append_done: RPCCaller::new(),
            rpc_caller_batch_put_append_done: RPCCaller::new(),
            rpc_caller_batch_evict_owner_source: RPCCaller::new(),
            rpc_caller_batch_publish_owner_ssd: RPCCaller::new(),
            rpc_caller_delete: RPCCaller::new(),
            rpc_caller_batch_delete_ack: RPCCaller::new(),
            rpc_caller_batch_is_exist: RPCCaller::new(),
            rpc_caller_get_meta: RPCCaller::new(),
            rpc_caller_allocate_client_lease: RPCCaller::new(),
            rpc_caller_client_lease_keepalive: RPCCaller::new(),
            rpc_caller_ssd_stage_begin: RPCCaller::new(),
            rpc_caller_ssd_stage_done: RPCCaller::new(),
            rpc_caller_external_put_commit: RPCCaller::new(),
            rpc_caller_external_put_revoke: RPCCaller::new(),
            rpc_caller_owner_segment_transfer: RPCCaller::new(),
            rpc_caller_owner_capacity_report: RPCCaller::new(),
            owner_capacity_report_epoch: AtomicU64::new(0),
            default_lease_id: parking_lot::RwLock::new(None),
            owner_local_publish_tx,
            owner_local_publish_rx: Mutex::new(Some(owner_local_publish_rx)),
        };
        Ok(Self(inner))
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), KvError> {
        let inner = &self.0;

        let metrics_arc = inner.view.metric_reporter().metrics();
        if inner.metrics.set(metrics_arc.clone()).is_err() {
            tracing::warn!("metrics handle already initialized for ClientKvApi");
        }

        inner.rpc_caller_get_start.regist(inner.view.p2p_module());
        inner.rpc_caller_get_revoke.regist(inner.view.p2p_module());
        inner.rpc_caller_get_done.regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_plan
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_bind
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_get_done
            .regist(inner.view.p2p_module());
        inner.rpc_caller_put_start.regist(inner.view.p2p_module());
        inner.rpc_caller_put_revoke.regist(inner.view.p2p_module());
        inner.rpc_caller_put_done.regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_grouped_batch_put_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_prepare_put_keys
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_release_put_key_reservations
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_append_start
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_put_append_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_put_append_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_evict_owner_source
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_publish_owner_ssd
            .regist(inner.view.p2p_module());
        inner.rpc_caller_delete.regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_delete_ack
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_batch_is_exist
            .regist(inner.view.p2p_module());
        inner.rpc_caller_get_meta.regist(inner.view.p2p_module());
        inner
            .rpc_caller_ssd_stage_begin
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_ssd_stage_done
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_external_put_commit
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_external_put_revoke
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_owner_segment_transfer
            .regist(inner.view.p2p_module());
        inner
            .rpc_caller_owner_capacity_report
            .regist(inner.view.p2p_module());
        crate::key_prefix::init_for_p2p_owner(inner.view.p2p_module());
        crate::kvlease::init_for_p2p_owner(inner.view.p2p_module());
        // Register master-only metric RPC callers
        crate::metrics::client::init_for_p2p_owner(inner.view.p2p_module());
        RPCCaller::<BatchDeleteAckReq>::new().regist(inner.view.p2p_module());
        RPCCaller::<BatchIsExistReq>::new().regist(inner.view.p2p_module());
        RPCCaller::<BatchDeleteClientKvMetaCacheReq>::new().regist(inner.view.p2p_module());
        spawn_owner_local_reserve_rebalance_actor(inner.view.clone_view());
        spawn_owner_slot_pressure_actor(inner.view.clone_view());
        external_api::spawn_external_get_start_handle_sweeper(inner.view.clone_view());
        self.spawn_runtime_observe_reporter();

        let view_owner_transfer = inner.view.clone_view();
        RPCHandler::<OwnerSegmentTransferReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let caller = resp.node_id().clone();
                let view = view_owner_transfer.clone();
                let task_view = view.clone();
                view.spawn("rpc_owner_segment_transfer", async move {
                    let result = handle_owner_segment_transfer(&task_view, caller, msg).await;
                    if let Err(error) = send_control_plane_rpc_response(&resp, result).await {
                        warn!("Failed to send OwnerSegmentTransferResp: {:?}", error);
                    }
                });
                Ok(())
            },
        );

        // External RPC handlers
        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalGetReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            view.spawn("rpc_external_get", async move {
                let result = handle_external_get(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get", async move {
                    let result = handle_external_batch_get(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetLocalProbeReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_local_probe", async move {
                    let result = handle_external_batch_get_local_probe(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetStartReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_start", async move {
                    let result = handle_external_batch_get_start(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetTransferReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_transfer", async move {
                    let result = handle_external_batch_get_transfer(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchGetCancelReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_get_cancel", async move {
                    let result = handle_external_batch_get_cancel(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalExecutePlannedGetReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_execute_planned_get", async move {
                    let result = handle_external_execute_planned_get(&view_task, &msg).await;
                    if let Err(error) = send_control_plane_rpc_response(&resp, result).await {
                        warn!(
                            "Failed to send ExternalExecutePlannedGetResp over control transport: {:?}",
                            error
                        );
                    }
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutStartReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            view.spawn("rpc_external_put_start", async move {
                let req = msg.serialize_part.clone();
                tracing::info!(
                    "rpc_external_put_start received: self={} peer={} task_id={} key={} len={} started_time={}",
                    view_task.cluster_manager().get_self_info().id,
                    resp.node_id(),
                    resp.task_id(),
                    req.key,
                    req.len,
                    req.started_time
                );
                let result = handle_external_put_start(&view_task, &msg).await;
                if let Err(err) = resp.send_resp(result).await {
                    tracing::warn!(
                        "rpc_external_put_start send_resp failed: self={} peer={} task_id={} key={} err={:?}",
                        view_task.cluster_manager().get_self_info().id,
                        resp.node_id(),
                        resp.task_id(),
                        req.key,
                        err
                    );
                } else {
                    tracing::info!(
                        "rpc_external_put_start response sent: self={} peer={} task_id={} key={}",
                        view_task.cluster_manager().get_self_info().id,
                        resp.node_id(),
                        resp.task_id(),
                        req.key
                    );
                }
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchPutStartReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_put_start", async move {
                    let result = handle_external_batch_put_start(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutTransferEndReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_put_transfer_end", async move {
                    let result = handle_external_put_transfer_end(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchPutTransferEndReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_put_transfer_end", async move {
                    let result = handle_external_batch_put_transfer_end(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutCommitReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_put_commit", async move {
                    let result = handle_external_put_commit(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchPutCommitReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_put_commit", async move {
                    let result = handle_external_batch_put_commit(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalPutRevokeReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_put_revoke", async move {
                    let result = handle_external_put_revoke(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalDeleteReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            view.spawn("rpc_external_delete", async move {
                let result = handle_external_delete(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalIsExistReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_is_exist", async move {
                    let result = handle_external_is_exist(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchIsExistReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_is_exist", async move {
                    let result = handle_external_batch_is_exist(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalObservabilitySnapshotReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_observability_snapshot", async move {
                    let result = handle_external_observability_snapshot(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalDeleteAckReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_delete_ack", async move {
                    let result = handle_external_delete_ack(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        let view_ext = inner.view.clone_view();
        RPCHandler::<ExternalBatchDeleteAckReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view_ext.clone();
                let view_task = view.clone();
                view.spawn("rpc_external_batch_delete_ack", async move {
                    let result = handle_external_batch_delete_ack(&view_task, &msg).await;
                    let _ = resp.send_resp(result).await;
                });
                Ok(())
            },
        );

        // KV->file sync RPC (bytes field -> file@offset)
        RPCCaller::<SyncKvToFileReq>::new().regist(inner.view.p2p_module());
        let view_ext = inner.view.clone_view();
        RPCHandler::<SyncKvToFileReq>::new().regist(inner.view.p2p_module(), move |resp, msg| {
            let view = view_ext.clone();
            let view_task = view.clone();
            view.spawn("rpc_sync_kv_to_file", async move {
                let result = handle_sync_kv_to_file_client(&view_task, &msg).await;
                let _ = resp.send_resp(result).await;
            });
            Ok(())
        });

        // client rpc handler register
        let view = inner.view.clone_view();
        RPCHandler::<OwnerLocalReserveControlReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let req_node_id = resp.node_id().clone();
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_owner_local_reserve_control", async move {
                    let started_at = Instant::now();
                    let result = async {
                        let master_node_id = view_task
                            .cluster_manager()
                            .find_or_wait_master_node()
                            .await?;
                        if req_node_id.as_ref() != master_node_id {
                            return Err(KvError::Api(ApiError::InvalidArgument {
                                detail: format!(
                                    "owner local-reserve control accepts only the master: caller={} master={}",
                                    req_node_id, master_node_id
                                ),
                            }));
                        }
                        view_task
                            .client_kv_api()
                            .inner()
                            .apply_owner_local_reserve_control(&msg.serialize_part)
                    }
                    .await;
                    let mut ack = match result {
                        Ok(response) => response,
                        Err(err) => crate::rpcresp_kvresult_convert::FromError::from_error(&err),
                    };
                    ack.server_process_us = i64::try_from(started_at.elapsed().as_micros())
                        .unwrap_or(i64::MAX);
                    if let Err(err) = send_control_plane_rpc_response(
                        &resp,
                        MsgPack {
                            serialize_part: ack,
                            raw_bytes: Vec::new(),
                        },
                    )
                    .await
                    {
                        warn!("Failed to send OwnerLocalReserveControlResp: {:?}", err);
                    }
                });
                Ok(())
            },
        );

        let view = inner.view.clone_view();
        RPCHandler::<BatchEnqueueReplicaTaskReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_batch_enqueue_replica_tasks", async move {
                    let ack = put::handle_batch_enqueue_replica_tasks(&view_task, msg).await;
                    if let Err(e) = send_control_plane_rpc_response(&resp, ack).await {
                        warn!("Failed to send BatchEnqueueReplicaTaskResp: {:?}", e);
                    }
                });
                Ok(())
            },
        );

        let view = inner.view.clone_view();
        RPCHandler::<BatchOwnerReclaimReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let req_node_id = resp.node_id().clone();
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_batch_owner_reclaim", async move {
                    let ack = handle_batch_owner_reclaim(&view_task, msg, req_node_id).await;
                    if let Err(e) = send_control_plane_rpc_response(&resp, ack).await {
                        warn!("Failed to send BatchOwnerReclaimResp: {:?}", e);
                    }
                });
                Ok(())
            },
        );

        let view = inner.view.clone_view();
        RPCHandler::<BatchDeleteClientKvMetaCacheReq>::new().regist(
            inner.view.p2p_module(),
            move |resp, msg| {
                let req_node_id = resp.node_id().clone();
                let view = view.clone();
                let view_task = view.clone();
                view.spawn("rpc_batch_delete_client_kv_meta_cache", async move {
                    let ack =
                        handle_batch_delete_client_kv_meta_cache(&view_task, msg, req_node_id)
                            .await;
                    if let Err(e) = send_control_plane_rpc_response(&resp, ack).await {
                        warn!("Failed to send BatchDeleteClientKvMetaCacheResp: {:?}", e);
                    }
                });
                Ok(())
            },
        );

        let external_invalidate_delete_rx = inner
            .external_invalidate_delete
            .take_rx()
            .expect("external_invalidate_delete rx already taken, that's impossible");
        delete::spawn_external_invalidate_delete(
            inner.view.clone_view(),
            external_invalidate_delete_rx,
        );

        let delete_ack_batch_rx = inner
            .delete_ack_batch
            .take_rx()
            .expect("delete_ack_batch rx already taken, that's impossible");
        delete::spawn_owner_delete_ack_batch(inner.view.clone_view(), delete_ack_batch_rx);

        if inner.owner_hot_cache.is_some() {
            if let Some(owner_hot_eviction_rx) = inner.owner_hot_eviction_rx.lock().take() {
                put::spawn_owner_source_eviction_dispatcher(
                    inner.view.clone_view(),
                    owner_hot_eviction_rx,
                );
                put::spawn_owner_hot_retry_actor(inner.view.clone_view());
            } else {
                tracing::warn!("owner_hot_eviction_rx already taken for ClientKvApi");
            }
        }
        if let Some(owner_local_publish_rx) = inner.owner_local_publish_rx.lock().take() {
            put::spawn_owner_local_publish_dispatcher(
                inner.view.clone_view(),
                owner_local_publish_rx,
                OWNER_LOCAL_PUBLISH_MAX_INFLIGHT,
            );
        } else {
            tracing::warn!("owner_local_publish_rx already taken for ClientKvApi");
        }

        // Spawn cluster listener to retire generation-scoped external requester state.
        let view = inner.view.clone_view();
        let view2 = view.clone();
        let view_task = view2.clone();
        view.spawn("client_cluster_listener", async move {
            let mut listen_cluster_event = view_task.cluster_manager().listen();
            let mut shutdown_waiter = view_task.register_shutdown_waiter();

            loop {
                tokio::select! {
                    event = listen_cluster_event.recv() => {
                        match event {
                            Ok(event) => {
                                match event {
                                    ClusterEvent::MemberLeft(node_id) => {
                                        let departed_epoch = view_task
                                            .cluster_manager()
                                            .get_prev_member_info(&node_id)
                                            .map(|member| member.node_start_time);
                                        let current_epoch = view_task
                                            .cluster_manager()
                                            .get_member_info_cached(&node_id)
                                            .map(|member| member.node_start_time);
                                        let Some(departed_epoch) = external_api::external_member_left_departed_epoch(
                                            departed_epoch,
                                            current_epoch,
                                        ) else {
                                            tracing::debug!(
                                                "Ignoring ambiguous/delayed external MemberLeft: node={} departed_epoch={:?} current_epoch={:?}",
                                                node_id,
                                                departed_epoch,
                                                current_epoch,
                                            );
                                            continue;
                                        };

                                        let inner = view_task.client_kv_api().inner();
                                        let removed_handles = external_api::cleanup_external_get_start_handles_for_generation(
                                            &inner.external_get_start_registry,
                                            &node_id,
                                            departed_epoch,
                                        );
                                        let removed_holdings = inner
                                            .external_get_holding
                                            .cleanup_node_generation(&node_id, departed_epoch);
                                        if removed_handles > 0 || removed_holdings > 0 {
                                            tracing::info!(
                                                "Cleaned up departed external requester state: node={} epoch={} handles={} holdings={}",
                                                node_id,
                                                departed_epoch,
                                                removed_handles,
                                                removed_holdings,
                                            );
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Client cluster event receiver error (will resubscribe): {}",
                                    e
                                );
                                listen_cluster_event = view_task.cluster_manager().listen();
                            }
                        }
                    }
                    _ = shutdown_waiter.wait() => {
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    pub fn can_be_dropped(&self) -> bool {
        // 如果没有初始化 refcount，返回 true
        if self.inner().all_memholder_refcount.get().is_none() {
            return true;
        }
        // 判断 AllMemholderRefCount 能否 upgrade
        if let Some(ref_weak) = self.inner().all_memholder_refcount.get() {
            if ref_weak.upgrade().is_none() {
                return true;
            }
        }
        false
    }

    /// Drain pending metric events and compute a fresh snapshot.
    pub fn drain_and_compute_metrics(&self) -> std::collections::HashMap<String, MetricsSet> {
        self.inner().drain_and_compute_metrics()
    }

    pub fn client_id(&self) -> NodeIDString {
        self.inner().view.cluster_manager().get_self_info().id
    }

    // Removed thin wrappers: get/put/delete/is_exist/send_delete_ack; call via inner()

    /// Convenience wrapper: get KV
    pub async fn get(
        &self,
        key: &str,
    ) -> KvResult<Option<(Arc<UserMemHolder>, Option<RemoteGetInfo>)>> {
        self.inner().get(key).await
    }

    /// Convenience wrapper: put KV with optional lease_id
    /// NOTE: If `lease_id` is None, it MUST remain a pure non-lease put.
    ///       We do NOT fallback to any default lease here to avoid surprising behavior.
    pub async fn put(&self, key: &str, value: &[u8], lease_id: Option<u64>) -> KvResult<()> {
        let mut opts = PutOptionalArgs::new();
        // Only attach lease when caller explicitly provides it.
        if let Some(id) = lease_id {
            opts.0.push(PutOptionalArg::LeaseId(id));
        }
        self.inner().put(key, value, opts).await
    }

    /// Allocate a client lease with the given TTL seconds.
    ///
    /// Semantics:
    /// - `ttl_seconds` must be >= the master-side minimum client lease TTL
    ///   (see MasterLeaseManager::MIN_CLIENT_TTL_SECONDS).
    /// - Values smaller than this minimum (including 0) are invalid and will
    ///   cause `LeaseMgrError::InvalidTTL` to be returned from the master.
    pub async fn allocate_lease(&self, ttl_seconds: u64) -> KvResult<u64> {
        let inner = self.inner();
        let lease_id = crate::kvlease::allocate_lease(
            inner.view.p2p_module(),
            inner.view.cluster_manager(),
            ttl_seconds,
        )
        .await?;
        // store as default
        {
            let mut g = inner.default_lease_id.write();
            *g = Some(lease_id);
        }
        Ok(lease_id)
    }

    /// Keepalive a client lease using its existing TTL on the master.
    pub async fn keepalive_lease(&self, lease_id: u64) -> KvResult<()> {
        let inner = self.inner();
        crate::kvlease::keepalive_lease(
            inner.view.p2p_module(),
            inner.view.cluster_manager(),
            lease_id,
        )
        .await
    }

    /// Get current default lease id (set by allocate_lease)
    pub fn get_lease_id(&self) -> Option<u64> {
        self.inner().default_lease_id.read().clone()
    }

    #[cfg(test)]
    pub fn test_record(&self) -> &crate::client_kv_api::client_test_record::ClientTestRecord {
        &self.inner().test_record
    }

    #[cfg(test)]
    pub fn debug_cached_meta(&self) {
        tracing::info!("--- debug cached meta --------------------------------------");
        for entry in self.inner().get_cached_info.iter() {
            tracing::info!("- cached meta: {:?}", entry.value());
        }
        tracing::info!("------------------------------------------------------------");
    }

    pub fn has_cached_key(&self, key: &str) -> bool {
        self.inner().has_local_snapshot(key)
    }

    // Removed is_client_mode(): ClientKvApi is owner-only and always constructed.
}

#[async_trait]
impl LogicalModule for ClientKvApi {
    type View = ClientKvApiView;
    type NewArg = ClientKvApiNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "ClientKvApi"
    }

    fn attach_view(&self, view: Self::View) {
        ClientKvApi::attach_view(self, view);
    }

    async fn before_shutdown(&self) -> Result<(), Self::Error> {
        // High cohesion: handle KV client drop readiness here
        tracing::info!("ClientKvApi before_shutdown: waiting until safe to drop");
        loop {
            if self.can_be_dropped() {
                tracing::info!("ClientKvApi can be dropped");
                break;
            }
            tracing::info!(
                "ClientKvApi not ready to drop; retry in 3s (some user memholder may still be in use)"
            );
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        Ok(())
    }
    async fn shutdown(&self) -> Result<(), Self::Error> {
        tracing::info!("ClientKvApi shutting down...");
        if let Some(store) = self.0.ssd_storage.as_ref() {
            store.close().await?;
        }
        tracing::info!(
            "ClientKvApi final: holding_len={} , cache_len={}",
            self.0.get_holding_len(),
            self.0.get_cache_len()
        );
        Ok(())
    }
}

impl ClientKvApiInner {
    #[cfg(any(test, feature = "test_bins"))]
    pub fn get_view(&self) -> &ClientKvApiView {
        &self.view
    }
}
