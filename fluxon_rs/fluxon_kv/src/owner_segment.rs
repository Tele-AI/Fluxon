#![allow(unused_assignments)]

use crate::cluster_manager::NodeIDString;
use crate::master_kv_router::msg_pack::PutAtomicGroup;
use crate::p2p::msg_pack::{MsgPackSerializePart, RPCReq};
use crate::rpcresp_kvresult_convert::msg_and_error::{ErrorCode, MsgId};
use bitcode::{Decode, Encode};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(crate) const OWNER_TRANSFER_CLIENT_ACK_STREAM: u64 = 1;
pub(crate) const OWNER_TRANSFER_EXTERNAL_ACK_STREAM: u64 = 2;

/// Stable identity of one owner process generation.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct OwnerGeneration {
    pub node_id: NodeIDString,
    pub node_start_time: i64,
}

impl OwnerGeneration {
    pub fn new(node_id: impl Into<NodeIDString>, node_start_time: i64) -> Self {
        Self {
            node_id: node_id.into(),
            node_start_time,
        }
    }

    pub fn is_initialized(&self) -> bool {
        !self.node_id.is_empty() && self.node_start_time != 0
    }

    #[cfg(test)]
    pub fn for_test(node_id: &str) -> Self {
        Self::new(node_id, 1)
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OwnerSlotScope {
    #[default]
    LocalExclusive,
    GlobalShared,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OwnerTargetDisposition {
    #[default]
    EphemeralCaller,
    LocalExclusive,
    GlobalShared,
    TransientSsdRead,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum OwnerTransferOpKind {
    #[default]
    Get,
    Put,
    ReplicaAppend,
    SsdLoad,
    Repair,
}

/// Generation-safe identity shared by source and target owner RPCs.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct OwnerTransferOpId {
    pub coordinator: OwnerGeneration,
    pub sequence: u64,
    pub kind: OwnerTransferOpKind,
}

impl OwnerTransferOpId {
    pub fn new(coordinator: OwnerGeneration, sequence: u64, kind: OwnerTransferOpKind) -> Self {
        Self {
            coordinator,
            sequence,
            kind,
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.coordinator.is_initialized() && self.sequence != 0
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct OwnerLeaseId {
    pub owner: OwnerGeneration,
    pub sequence: u64,
}

impl OwnerLeaseId {
    pub fn is_initialized(&self) -> bool {
        self.owner.is_initialized() && self.sequence != 0
    }
}

/// The only physical slot descriptor used by owner allocator, owner RPC and
/// master route metadata. `allocation_id` is never reused within one owner
/// generation; an offset alone is never a release capability.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSlotDesc {
    pub owner: OwnerGeneration,
    pub allocation_id: u64,
    /// Byte offset inside the owner's complete registered segment.
    pub segment_offset: u64,
    /// 4 KiB-aligned physical allocation capacity.
    pub capacity_bytes: u64,
    pub addr: u64,
    pub base_addr: u64,
    pub len: u64,
    /// Generation of the segment registration used by the transfer backend.
    pub segment_registration_epoch: u64,
}

impl OwnerSlotDesc {
    pub fn end_addr(&self) -> Option<u64> {
        self.addr.checked_add(self.capacity_bytes)
    }

    pub fn geometry_matches(&self, other: &Self) -> bool {
        self.owner == other.owner
            && self.allocation_id == other.allocation_id
            && self.segment_offset == other.segment_offset
            && self.capacity_bytes == other.capacity_bytes
            && self.addr == other.addr
            && self.base_addr == other.base_addr
    }

    pub fn is_valid(&self) -> bool {
        self.owner.is_initialized()
            && self.allocation_id != 0
            && self.capacity_bytes != 0
            && self.len <= self.capacity_bytes
            && self.segment_registration_epoch != 0
            && self.addr
                == self
                    .base_addr
                    .checked_add(self.segment_offset)
                    .unwrap_or(u64::MAX)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnerManifestKey {
    pub key: String,
    pub put_id: (u64, u32),
}

impl OwnerManifestKey {
    pub fn new(key: impl Into<String>, put_id: (u64, u32)) -> Self {
        Self {
            key: key.into(),
            put_id,
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerSlotPhysicalState {
    #[default]
    Reserved,
    DataReady,
    RoutePending,
    Committed,
    Reclaiming,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct OwnerSlotManifestEntry {
    pub key: String,
    pub put_id: (u64, u32),
    pub slot: OwnerSlotDesc,
    pub scope: Option<OwnerSlotScope>,
    pub disposition: OwnerTargetDisposition,
    pub route_epoch: u64,
    pub physical_state: OwnerSlotPhysicalState,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSourceRouteToken {
    pub key: String,
    pub put_id: (u64, u32),
    pub route_epoch: u64,
    pub source: OwnerSlotDesc,
    pub atomic_batch: Option<PutAtomicGroup>,
    pub plan_nonce: u64,
}

/// Source-owner authorization for one exact target-initiated Put/replica
/// READ.  The source keeps its local holder alive while this capability is in
/// flight; the target may use it only for the named owner generation and
/// attempt.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSourceReadCapability {
    pub operation: OwnerTransferOpId,
    pub target_owner: OwnerGeneration,
    pub target_attempt: u32,
    pub route: OwnerSourceRouteToken,
}

impl OwnerSourceReadCapability {
    pub fn is_valid_for(
        &self,
        operation: &OwnerTransferOpId,
        target_owner: &OwnerGeneration,
        target_attempt: u32,
        len: u64,
    ) -> bool {
        self.operation == *operation
            && self.target_owner == *target_owner
            && self.target_attempt == target_attempt
            && target_attempt != 0
            && self.route.source.owner == operation.coordinator
            && self.route.source.is_valid()
            && self.route.source.len == len
            && self.route.route_epoch != 0
            && self.route.plan_nonce == operation.sequence
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSsdSourceRouteToken {
    pub key: String,
    pub put_id: (u64, u32),
    pub owner: OwnerGeneration,
    pub len: u64,
    pub atomic_batch: Option<PutAtomicGroup>,
    pub plan_nonce: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OwnerGetSourceCapability {
    #[default]
    Invalid,
    Memory(OwnerSourceRouteToken),
    Ssd(OwnerSsdSourceRouteToken),
}

impl OwnerGetSourceCapability {
    pub fn owner(&self) -> Option<&OwnerGeneration> {
        match self {
            Self::Invalid => None,
            Self::Memory(token) => Some(&token.source.owner),
            Self::Ssd(token) => Some(&token.owner),
        }
    }

    pub fn len(&self) -> u64 {
        match self {
            Self::Invalid => 0,
            Self::Memory(token) => token.source.len,
            Self::Ssd(token) => token.len,
        }
    }

    pub fn plan_nonce(&self) -> u64 {
        match self {
            Self::Invalid => 0,
            Self::Memory(token) => token.plan_nonce,
            Self::Ssd(token) => token.plan_nonce,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerTargetRouteToken {
    pub key: String,
    pub put_id: (u64, u32),
    pub operation: OwnerTransferOpId,
    pub target_owner: OwnerGeneration,
    pub prior_route_epoch: u64,
    pub policy_epoch: u64,
    pub atomic_batch: Option<PutAtomicGroup>,
    pub plan_nonce: u64,
}

/// Whether the caller waits for the metadata route terminal after payload
/// completion. Async is the wire/default behavior: once a replay-safe route
/// task owns the DataReady target, the payload producer may leave the
/// foreground path. Sync waits for that same task; it never creates a second
/// commit state machine.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerRouteCommitMode {
    #[default]
    Async,
    Sync,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerTransferDirection {
    #[default]
    RdmaRead,
    RdmaWrite,
    IpcCopy,
    LocalCopy,
    ZeroCopy,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerTransferReceipt {
    pub completion_id: u64,
    pub direction: OwnerTransferDirection,
    pub bytes: u64,
    pub source: Option<OwnerSlotDesc>,
    pub target: OwnerSlotDesc,
    pub source_registration_epoch: u64,
    pub target_registration_epoch: u64,
}

/// Exact requester-owner authorization for one source-initiated Get WRITE.
///
/// The target owner allocates the slot and lease before this capability is
/// sent to the source.  Binding both identities to the operation prevents a
/// replay from redirecting an already-started WRITE to another allocation.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerTargetWriteCapability {
    pub operation: OwnerTransferOpId,
    pub lease_id: OwnerLeaseId,
    pub slot: OwnerSlotDesc,
}

impl OwnerTargetWriteCapability {
    pub fn is_valid_for(&self, operation: &OwnerTransferOpId, len: u64) -> bool {
        self.operation == *operation
            && self.lease_id.is_initialized()
            && self.lease_id.owner == self.slot.owner
            && self.slot.is_valid()
            && self.slot.len == len
            && self.slot.capacity_bytes >= len
    }
}

/// Caller-owned GPU registration used as a direct Get destination. Unlike an
/// owner slot this capability never enters the owner allocator or route
/// manifest; its lifetime is retained by the caller-side GPU guard.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerExternalGpuWriteCapability {
    pub operation: OwnerTransferOpId,
    pub requester: OwnerGeneration,
    pub addr: u64,
    pub capacity_bytes: u64,
    pub registration_id: u64,
}

impl OwnerExternalGpuWriteCapability {
    pub fn is_valid_for(&self, operation: &OwnerTransferOpId, len: u64) -> bool {
        self.operation == *operation
            && self.requester == operation.coordinator
            && self.requester.is_initialized()
            && self.addr != 0
            && self.capacity_bytes >= len
            && self.addr.checked_add(self.capacity_bytes).is_some()
            && self.registration_id != 0
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OwnerGetDestinationCapability {
    #[default]
    Invalid,
    OwnerSlot(OwnerTargetWriteCapability),
    ExternalGpu(OwnerExternalGpuWriteCapability),
}

impl OwnerGetDestinationCapability {
    pub fn is_valid_for(&self, operation: &OwnerTransferOpId, len: u64) -> bool {
        match self {
            Self::Invalid => false,
            Self::OwnerSlot(capability) => capability.is_valid_for(operation, len),
            Self::ExternalGpu(capability) => capability.is_valid_for(operation, len),
        }
    }
}

/// Source-issued terminal for one Get WRITE. The destination capability is
/// echoed exactly, allowing the requester to reject a terminal for another
/// slot or GPU registration without interpreting a raw address.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerGetTransferReceipt {
    pub completion_id: u64,
    pub direction: OwnerTransferDirection,
    pub bytes: u64,
    pub source: OwnerSlotDesc,
    pub destination: OwnerGetDestinationCapability,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerTransferOutcome {
    #[default]
    Success,
    Failed,
    Cancelled,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerTargetLeaseStateView {
    #[default]
    Prepared,
    DataReady,
    RoutePending,
    Committed,
    Aborted,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerTransferErrorCode {
    #[default]
    InvalidArgument,
    StaleGeneration,
    NotFound,
    Conflict,
    Busy,
    NoSpace,
    Reclaiming,
    RouteCommitRequired,
    Internal,
    /// A metadata Plan was valid when issued, but the exact key generation
    /// materialized locally before its preclaimed target could be bound.
    /// This is a cache miss/replan terminal, not a protocol identity conflict.
    StalePlan,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerTransferItemError {
    pub code: OwnerTransferErrorCode,
    pub detail: String,
}

impl OwnerTransferItemError {
    pub fn new(code: OwnerTransferErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OwnerSegmentTransferItem {
    #[default]
    Invalid,
    /// Execute one replay-safe Get directly into a requester-owned target.
    /// The source acquires/releases its read lease inside this operation and
    /// publishes one cached terminal.
    GetToTarget {
        op_id: OwnerTransferOpId,
        source: OwnerGetSourceCapability,
        destination: OwnerGetDestinationCapability,
    },
    /// Execute one replay-safe Put/replica by letting the target owner claim
    /// locally, READ the exact source capability and publish the route before
    /// replying.  PrepareTarget/CommitTarget remain internal primitives, not
    /// extra wire round trips on this path.
    PutFromSource {
        op_id: OwnerTransferOpId,
        target_attempt: u32,
        target_plan: OwnerTargetRouteToken,
        source: OwnerSourceReadCapability,
        disposition: OwnerTargetDisposition,
        route_commit_mode: OwnerRouteCommitMode,
    },
    PrepareTarget {
        op_id: OwnerTransferOpId,
        expected_target: OwnerGeneration,
        key: String,
        put_id: (u64, u32),
        len: u64,
        disposition: OwnerTargetDisposition,
        atomic_batch: Option<PutAtomicGroup>,
    },
    CommitTarget {
        op_id: OwnerTransferOpId,
        lease_id: OwnerLeaseId,
        receipt: OwnerTransferReceipt,
        route_token: Option<OwnerTargetRouteToken>,
    },
    AbortTarget {
        op_id: OwnerTransferOpId,
        lease_id: OwnerLeaseId,
        reason: String,
    },
}

impl OwnerSegmentTransferItem {
    pub fn op_id(&self) -> Option<&OwnerTransferOpId> {
        match self {
            Self::Invalid => None,
            Self::GetToTarget { op_id, .. }
            | Self::PutFromSource { op_id, .. }
            | Self::PrepareTarget { op_id, .. }
            | Self::CommitTarget { op_id, .. }
            | Self::AbortTarget { op_id, .. } => Some(op_id),
        }
    }

    pub fn caches_replay_terminal(&self) -> bool {
        matches!(self, Self::GetToTarget { .. } | Self::PutFromSource { .. })
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OwnerSegmentTransferOutcome {
    #[default]
    Invalid,
    SourceAcquired {
        lease_id: OwnerLeaseId,
        slot: OwnerSlotDesc,
    },
    SourceReleased,
    GetToTargetCompleted {
        receipt: OwnerGetTransferReceipt,
    },
    PutFromSourceCompleted {
        target_attempt: u32,
        source: OwnerSlotDesc,
        target: OwnerSlotDesc,
        receipt: OwnerTransferReceipt,
        route_epoch: u64,
    },
    /// The target owns the payload and a detached CommitTarget task before
    /// this terminal is published. Replays of an Async request keep returning
    /// this outward terminal even after the background task commits.
    PutFromSourceAcceptedRoutePending {
        target_attempt: u32,
        source: OwnerSlotDesc,
        target: OwnerSlotDesc,
        receipt: OwnerTransferReceipt,
    },
    TargetPrepared {
        lease_id: OwnerLeaseId,
        slot: OwnerSlotDesc,
        state: OwnerTargetLeaseStateView,
    },
    TargetDataReady {
        lease_id: OwnerLeaseId,
        slot: OwnerSlotDesc,
    },
    TargetCommitPending {
        lease_id: OwnerLeaseId,
        slot: OwnerSlotDesc,
    },
    TargetCommitted {
        lease_id: OwnerLeaseId,
        slot: OwnerSlotDesc,
        route_epoch: u64,
    },
    TargetAborted,
    Error(OwnerTransferItemError),
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSegmentTransferItemResp {
    /// Caller-assigned sequence in one generation-safe peer ACK stream. Zero
    /// is reserved for operations that do not retain a replay terminal.
    pub terminal_sequence: u64,
    pub op_id: Option<OwnerTransferOpId>,
    pub outcome: OwnerSegmentTransferOutcome,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSegmentTransferRequestItem {
    pub terminal_sequence: u64,
    pub item: OwnerSegmentTransferItem,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct OwnerSegmentTransferReq {
    pub caller_generation: OwnerGeneration,
    pub ack_stream_id: u64,
    /// Highest continuous caller-assigned terminal sequence whose response is
    /// already durably reflected in the caller's local operation state.
    pub terminal_ack_watermark: u64,
    pub items: Vec<OwnerSegmentTransferRequestItem>,
}

impl MsgPackSerializePart for OwnerSegmentTransferReq {
    fn msg_id(&self) -> u32 {
        MsgId::OwnerSegmentTransferReq as u32
    }
}

impl RPCReq for OwnerSegmentTransferReq {
    type Resp = OwnerSegmentTransferResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct OwnerSegmentTransferResp {
    pub items: Vec<OwnerSegmentTransferItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}

impl MsgPackSerializePart for OwnerSegmentTransferResp {
    fn msg_id(&self) -> u32 {
        MsgId::OwnerSegmentTransferResp as u32
    }
}

pub(crate) const OWNER_SEGMENT_TRANSFER_RETRY_INITIAL: Duration = Duration::from_millis(10);
pub(crate) const OWNER_SEGMENT_TRANSFER_RETRY_MAX: Duration = Duration::from_secs(1);

#[derive(Default)]
struct OwnerTransferAckWindow {
    watermark: u64,
    completed_after_gap: BTreeSet<u64>,
}

#[derive(Default)]
struct OwnerTransferPeerState {
    next_terminal_sequence: AtomicU64,
    ack: Mutex<OwnerTransferAckWindow>,
}

/// Caller-side state for one logical owner-transfer stream. Sequence numbers
/// are allocated before the first RPC attempt and remain embedded in every
/// replay of that request. Responses may arrive out of order; only a complete
/// prefix advances the watermark piggybacked on a later batch.
pub(crate) struct OwnerTransferPeerTracker {
    ack_stream_id: u64,
    peers: DashMap<OwnerGeneration, OwnerTransferPeerState>,
}

impl OwnerTransferPeerTracker {
    pub(crate) fn new(ack_stream_id: u64) -> Self {
        assert!(
            ack_stream_id != 0,
            "owner transfer ACK stream id must be non-zero"
        );
        Self {
            ack_stream_id,
            peers: DashMap::new(),
        }
    }

    pub(crate) fn prepare_request(
        &self,
        caller_generation: OwnerGeneration,
        target: &OwnerGeneration,
        items: Vec<OwnerSegmentTransferItem>,
    ) -> OwnerSegmentTransferReq {
        let peer = self.peers.entry(target.clone()).or_default();
        let terminal_ack_watermark = peer.ack.lock().watermark;
        let items = items
            .into_iter()
            .map(|item| {
                let terminal_sequence = if item.caches_replay_terminal() {
                    peer.next_terminal_sequence
                        .fetch_add(1, Ordering::Relaxed)
                        .checked_add(1)
                        .expect("owner transfer terminal sequence overflow")
                } else {
                    0
                };
                OwnerSegmentTransferRequestItem {
                    terminal_sequence,
                    item,
                }
            })
            .collect();
        OwnerSegmentTransferReq {
            caller_generation,
            ack_stream_id: self.ack_stream_id,
            terminal_ack_watermark,
            items,
        }
    }

    pub(crate) fn record_terminal(&self, target: &OwnerGeneration, terminal_sequence: u64) -> u64 {
        if terminal_sequence == 0 {
            return self.ack_watermark(target);
        }
        let peer = self.peers.entry(target.clone()).or_default();
        let mut ack = peer.ack.lock();
        if terminal_sequence <= ack.watermark {
            return ack.watermark;
        }
        ack.completed_after_gap.insert(terminal_sequence);
        loop {
            let Some(next) = ack.watermark.checked_add(1) else {
                break;
            };
            if !ack.completed_after_gap.remove(&next) {
                break;
            }
            ack.watermark = next;
        }
        ack.watermark
    }

    pub(crate) fn ack_watermark(&self, target: &OwnerGeneration) -> u64 {
        self.peers
            .get(target)
            .map(|peer| peer.ack.lock().watermark)
            .unwrap_or(0)
    }
}

/// Replay the same generation-safe owner batch after an ambiguous transport
/// result. Callers provide only the transport adapter and liveness checks;
/// operation identity, backoff, and the "never switch target after ambiguity"
/// rule stay shared by owner and external coordinators.
pub(crate) async fn replay_owner_segment_batch_until_definitive<
    Call,
    CallFuture,
    Running,
    Current,
>(
    target: &OwnerGeneration,
    request: OwnerSegmentTransferReq,
    phase: &'static str,
    mut call: Call,
    is_running: Running,
    owner_is_current: Current,
) -> crate::rpcresp_kvresult_convert::msg_and_error::KvResult<Vec<OwnerSegmentTransferItemResp>>
where
    Call: FnMut(OwnerSegmentTransferReq) -> CallFuture,
    CallFuture: Future<
        Output = crate::rpcresp_kvresult_convert::msg_and_error::KvResult<
            Vec<OwnerSegmentTransferItemResp>,
        >,
    >,
    Running: Fn() -> bool,
    Current: Fn(&OwnerGeneration) -> bool,
{
    let mut retry_delay = OWNER_SEGMENT_TRANSFER_RETRY_INITIAL;
    loop {
        match call(request.clone()).await {
            Ok(responses) => return Ok(responses),
            Err(error) => {
                if !is_running() || !owner_is_current(target) {
                    return Err(error);
                }
                tracing::warn!(
                    phase,
                    target_node_id = target.node_id,
                    target_node_start_time = target.node_start_time,
                    retry_delay_ms = retry_delay.as_millis(),
                    error = %error,
                    "owner segment transfer transport is uncertain; replaying the same operation batch"
                );
                limit_thirdparty::tokio::time::sleep(retry_delay).await;
                retry_delay = retry_delay
                    .saturating_mul(2)
                    .min(OWNER_SEGMENT_TRANSFER_RETRY_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source_token(get_id: u64) -> OwnerSourceRouteToken {
        let owner = OwnerGeneration::new("source", 23);
        OwnerSourceRouteToken {
            key: "key".to_string(),
            put_id: (101, 2),
            route_epoch: 7,
            source: OwnerSlotDesc {
                owner,
                allocation_id: 7,
                segment_offset: 4096,
                capacity_bytes: 8192,
                addr: 0x2000,
                base_addr: 0x1000,
                len: 5000,
                segment_registration_epoch: 23,
            },
            atomic_batch: None,
            plan_nonce: get_id + 1,
        }
    }

    fn test_request(
        caller: OwnerGeneration,
        target: &OwnerGeneration,
        items: Vec<OwnerSegmentTransferItem>,
    ) -> OwnerSegmentTransferReq {
        OwnerTransferPeerTracker::new(9).prepare_request(caller, target, items)
    }

    #[test]
    fn peer_terminal_ack_advances_only_the_contiguous_prefix() {
        let caller = OwnerGeneration::new("caller", 11);
        let target = OwnerGeneration::new("target", 13);
        let tracker = OwnerTransferPeerTracker::new(9);
        let operation = OwnerTransferOpId::new(caller.clone(), 7, OwnerTransferOpKind::Get);
        let item = OwnerSegmentTransferItem::GetToTarget {
            op_id: operation,
            source: OwnerGetSourceCapability::Invalid,
            destination: OwnerGetDestinationCapability::Invalid,
        };
        let first =
            tracker.prepare_request(caller.clone(), &target, vec![item.clone(), item.clone()]);
        assert_eq!(first.terminal_ack_watermark, 0);
        assert_eq!(first.items[0].terminal_sequence, 1);
        assert_eq!(first.items[1].terminal_sequence, 2);

        assert_eq!(tracker.record_terminal(&target, 2), 0);
        assert_eq!(tracker.ack_watermark(&target), 0);
        assert_eq!(tracker.record_terminal(&target, 1), 2);

        let next = tracker.prepare_request(caller, &target, vec![item]);
        assert_eq!(next.terminal_ack_watermark, 2);
        assert_eq!(next.items[0].terminal_sequence, 3);
    }

    #[test]
    fn owner_segment_transfer_batch_wire_preserves_generation_and_operation_identity() {
        let coordinator = OwnerGeneration::new("coordinator", 17);
        let target = OwnerGeneration::new("target", 23);
        let operation =
            OwnerTransferOpId::new(coordinator.clone(), 9, OwnerTransferOpKind::ReplicaAppend);
        let request = test_request(
            coordinator,
            &target,
            vec![OwnerSegmentTransferItem::PrepareTarget {
                op_id: operation.clone(),
                expected_target: target.clone(),
                key: "key".to_string(),
                put_id: (101, 2),
                len: 4718592,
                disposition: OwnerTargetDisposition::GlobalShared,
                atomic_batch: None,
            }],
        );
        let decoded: OwnerSegmentTransferReq =
            bitcode::decode(&bitcode::encode(&request)).expect("decode owner transfer request");
        assert_eq!(decoded.items, request.items);
        assert_eq!(request.msg_id(), 3083);

        let response = OwnerSegmentTransferResp {
            items: vec![OwnerSegmentTransferItemResp {
                terminal_sequence: 0,
                op_id: Some(operation),
                outcome: OwnerSegmentTransferOutcome::TargetPrepared {
                    lease_id: OwnerLeaseId {
                        owner: target.clone(),
                        sequence: 3,
                    },
                    slot: OwnerSlotDesc {
                        owner: target,
                        allocation_id: 7,
                        segment_offset: 4096,
                        capacity_bytes: 8192,
                        addr: 0x2000,
                        base_addr: 0x1000,
                        len: 5000,
                        segment_registration_epoch: 23,
                    },
                    state: OwnerTargetLeaseStateView::Prepared,
                },
            }],
            error_code: crate::rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
            server_process_us: 11,
        };
        let decoded: OwnerSegmentTransferResp =
            bitcode::decode(&bitcode::encode(&response)).expect("decode owner transfer response");
        assert_eq!(decoded.items, response.items);
        assert_eq!(response.msg_id(), 3084);
    }

    #[test]
    fn put_from_source_wire_binds_source_target_and_attempt() {
        let source_route = test_source_token(8);
        let source_owner = source_route.source.owner.clone();
        let target_owner = OwnerGeneration::new("target", 29);
        let operation = OwnerTransferOpId::new(
            source_owner.clone(),
            source_route.plan_nonce,
            OwnerTransferOpKind::ReplicaAppend,
        );
        let target_attempt = 3;
        let source = OwnerSourceReadCapability {
            operation: operation.clone(),
            target_owner: target_owner.clone(),
            target_attempt,
            route: source_route.clone(),
        };
        assert!(source.is_valid_for(
            &operation,
            &target_owner,
            target_attempt,
            source_route.source.len,
        ));
        let target_plan = OwnerTargetRouteToken {
            key: source_route.key.clone(),
            put_id: source_route.put_id,
            operation: operation.clone(),
            target_owner: target_owner.clone(),
            prior_route_epoch: 0,
            policy_epoch: 7,
            atomic_batch: None,
            plan_nonce: operation.sequence,
        };
        assert_eq!(OwnerRouteCommitMode::default(), OwnerRouteCommitMode::Async);
        for route_commit_mode in [OwnerRouteCommitMode::Async, OwnerRouteCommitMode::Sync] {
            let request = test_request(
                source_owner.clone(),
                &target_owner,
                vec![OwnerSegmentTransferItem::PutFromSource {
                    op_id: operation.clone(),
                    target_attempt,
                    target_plan: target_plan.clone(),
                    source: source.clone(),
                    disposition: OwnerTargetDisposition::GlobalShared,
                    route_commit_mode,
                }],
            );
            let decoded: OwnerSegmentTransferReq =
                bitcode::decode(&bitcode::encode(&request)).expect("decode PutFromSource request");
            assert_eq!(decoded.items, request.items);
        }

        let accepted = OwnerSegmentTransferOutcome::PutFromSourceAcceptedRoutePending {
            target_attempt,
            source: source.route.source.clone(),
            target: OwnerSlotDesc {
                owner: target_owner,
                ..source.route.source.clone()
            },
            receipt: OwnerTransferReceipt::default(),
        };
        let decoded: OwnerSegmentTransferOutcome =
            bitcode::decode(&bitcode::encode(&accepted)).expect("decode Async Put terminal");
        assert_eq!(decoded, accepted);
    }

    #[test]
    fn get_to_target_wire_binds_exact_source_and_destination_operation() {
        let source = test_source_token(8);
        let coordinator = OwnerGeneration::new("requester", 31);
        let operation = OwnerTransferOpId::new(
            coordinator.clone(),
            source.plan_nonce,
            OwnerTransferOpKind::Get,
        );
        let destination = OwnerTargetWriteCapability {
            operation: operation.clone(),
            lease_id: OwnerLeaseId {
                owner: coordinator.clone(),
                sequence: 5,
            },
            slot: OwnerSlotDesc {
                owner: coordinator,
                allocation_id: 9,
                segment_offset: 8192,
                capacity_bytes: 8192,
                addr: 0x5000,
                base_addr: 0x3000,
                len: source.source.len,
                segment_registration_epoch: 31,
            },
        };
        assert!(destination.is_valid_for(&operation, source.source.len));
        let source_owner = source.source.owner.clone();
        let request = test_request(
            operation.coordinator.clone(),
            &source_owner,
            vec![OwnerSegmentTransferItem::GetToTarget {
                op_id: operation,
                source: OwnerGetSourceCapability::Memory(source),
                destination: OwnerGetDestinationCapability::OwnerSlot(destination),
            }],
        );
        let decoded: OwnerSegmentTransferReq =
            bitcode::decode(&bitcode::encode(&request)).expect("decode GetToTarget request");
        assert_eq!(decoded.items, request.items);
    }

    #[test]
    fn ssd_get_to_target_wire_carries_metadata_token() {
        let source_owner = OwnerGeneration::new("ssd-owner", 37);
        let requester = OwnerGeneration::new("requester", 41);
        let operation = OwnerTransferOpId::new(requester.clone(), 13, OwnerTransferOpKind::Get);
        let source = OwnerSsdSourceRouteToken {
            key: "ssd-key".to_string(),
            put_id: (17, 3),
            owner: source_owner.clone(),
            len: 5000,
            atomic_batch: None,
            plan_nonce: operation.sequence,
        };
        let target = OwnerTargetWriteCapability {
            operation: operation.clone(),
            lease_id: OwnerLeaseId {
                owner: requester.clone(),
                sequence: 7,
            },
            slot: OwnerSlotDesc {
                owner: requester,
                allocation_id: 19,
                segment_offset: 4096,
                capacity_bytes: 8192,
                addr: 0x3000,
                base_addr: 0x2000,
                len: source.len,
                segment_registration_epoch: 43,
            },
        };
        let destination = OwnerGetDestinationCapability::OwnerSlot(target);
        let request = test_request(
            operation.coordinator.clone(),
            &source_owner,
            vec![OwnerSegmentTransferItem::GetToTarget {
                op_id: operation,
                source: OwnerGetSourceCapability::Ssd(source.clone()),
                destination,
            }],
        );
        let decoded: OwnerSegmentTransferReq =
            bitcode::decode(&bitcode::encode(&request)).expect("decode SSD GetToTarget");
        assert_eq!(decoded.items, request.items);
        assert_eq!(
            OwnerGetSourceCapability::Ssd(source.clone()).owner(),
            Some(&source_owner)
        );
        assert_eq!(OwnerGetSourceCapability::Ssd(source).len(), 5000);
    }

    #[test]
    fn external_gpu_destination_is_operation_and_generation_bound() {
        let requester = OwnerGeneration::new("external", 43);
        let operation = OwnerTransferOpId::new(requester.clone(), 17, OwnerTransferOpKind::Get);
        let capability = OwnerExternalGpuWriteCapability {
            operation: operation.clone(),
            requester,
            addr: 0x8000,
            capacity_bytes: 16 * 1024,
            registration_id: 9,
        };
        assert!(capability.is_valid_for(&operation, 8192));
        let mut changed = capability.clone();
        changed.registration_id = 0;
        assert!(!changed.is_valid_for(&operation, 8192));
        let destination = OwnerGetDestinationCapability::ExternalGpu(capability);
        assert!(destination.is_valid_for(&operation, 8192));
        let decoded: OwnerGetDestinationCapability =
            bitcode::decode(&bitcode::encode(&destination)).expect("decode GPU capability");
        assert_eq!(decoded, destination);
    }
}
