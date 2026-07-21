use crate::{
    cluster_manager::NodeIDString,
    p2p::msg_pack::{MsgPackSerializePart, RPCReq},
    rpcresp_kvresult_convert::msg_and_error::{ErrorCode, MsgId, OK},
};
use bitcode::{Decode, Encode};
use std::collections::HashMap;
use std::sync::Arc;

use super::put::PutIDForAKey;

// --- RPC for Get ---

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum GetAllocationMode {
    #[default]
    Temporary = 0,
    ReuseReplica = 1,
    DurableReplica = 2,
    LocalCommittedSlot = 3,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct GetPreparedLocalReserveTarget {
    pub grant_id: u64,
    pub slot_index: u32,
    pub slot_size: u64,
    pub addr: u64,
    pub base_addr: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetStartReq {
    pub key: String,
    pub prepared_target: Option<GetPreparedLocalReserveTarget>,
}
impl MsgPackSerializePart for GetStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::GetStartReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetStartResp {
    pub get_id: u64,
    pub node_id: NodeIDString,
    pub put_id: PutIDForAKey,
    // absolute addresses because Mooncake transfer engine requires absolute addresses (not offsets)
    pub target_addr: u64,
    pub src_addr: u64,
    // base addresses to allow callers to convert abs->offset when needed
    pub target_base_addr: u64,
    pub src_base_addr: u64,
    pub len: u64,
    /// Echoes the owner-local slot accepted as this Get's target.
    pub prepared_target: Option<GetPreparedLocalReserveTarget>,
    pub atomic_group: Option<PutAtomicGroup>,
    pub error_code: ErrorCode,
    pub error_json: String,
    /// Server-side processing time in microseconds for this RPC handler
    pub server_process_us: i64,
}
impl MsgPackSerializePart for GetStartResp {
    fn msg_id(&self) -> u32 {
        MsgId::GetStartResp as u32
    }
}
impl RPCReq for GetStartReq {
    type Resp = GetStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetRevokeReq {
    pub get_id: u64,
}
impl MsgPackSerializePart for GetRevokeReq {
    fn msg_id(&self) -> u32 {
        MsgId::GetRevokeReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetRevokeResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for GetRevokeResp {
    fn msg_id(&self) -> u32 {
        MsgId::GetRevokeResp as u32
    }
}
impl RPCReq for GetRevokeReq {
    type Resp = GetRevokeResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetDoneReq {
    pub get_id: u64,
}
impl MsgPackSerializePart for GetDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::GetDoneReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetDoneResp {
    pub holder_id: u64,
    pub allocation_mode: GetAllocationMode,
    pub error_code: ErrorCode,
    pub error_json: String,
    /// Server-side processing time in microseconds for this RPC handler
    pub server_process_us: i64,
}
impl MsgPackSerializePart for GetDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::GetDoneResp as u32
    }
}
impl RPCReq for GetDoneReq {
    type Resp = GetDoneResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetStartReq {
    pub keys: Vec<String>,
    /// Empty selects ordinary master allocations. Otherwise this must contain
    /// exactly one entry per key.
    pub prepared_targets: Vec<Option<GetPreparedLocalReserveTarget>>,
}
impl MsgPackSerializePart for BatchGetStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetStartReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetStartItemResp {
    pub get_id: u64,
    pub node_id: NodeIDString,
    pub put_id: PutIDForAKey,
    pub target_addr: u64,
    pub src_addr: u64,
    pub target_base_addr: u64,
    pub src_base_addr: u64,
    pub len: u64,
    pub prepared_target: Option<GetPreparedLocalReserveTarget>,
    pub atomic_group: Option<PutAtomicGroup>,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetStartResp {
    pub items: Vec<BatchGetStartItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchGetStartResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetStartResp as u32
    }
}
impl RPCReq for BatchGetStartReq {
    type Resp = BatchGetStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetRevokeReq {
    pub get_ids: Vec<u64>,
}
impl MsgPackSerializePart for BatchGetRevokeReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetRevokeReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetRevokeItemResp {
    pub get_id: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetRevokeResp {
    pub items: Vec<BatchGetRevokeItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for BatchGetRevokeResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetRevokeResp as u32
    }
}
impl RPCReq for BatchGetRevokeReq {
    type Resp = BatchGetRevokeResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetDoneReq {
    pub get_ids: Vec<u64>,
}
impl MsgPackSerializePart for BatchGetDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetDoneReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetDoneItemResp {
    pub get_id: u64,
    pub holder_id: u64,
    pub allocation_mode: GetAllocationMode,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetDoneResp {
    pub items: Vec<BatchGetDoneItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchGetDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetDoneResp as u32
    }
}
impl RPCReq for BatchGetDoneReq {
    type Resp = BatchGetDoneResp;
}

// --- RPC for CountPrefix ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct CountPrefixReq {
    pub prefix: String,
}
impl MsgPackSerializePart for CountPrefixReq {
    fn msg_id(&self) -> u32 {
        MsgId::CountPrefixReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct CountPrefixResp {
    pub count: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for CountPrefixResp {
    fn msg_id(&self) -> u32 {
        MsgId::CountPrefixResp as u32
    }
}
impl RPCReq for CountPrefixReq {
    type Resp = CountPrefixResp;
}

// --- RPC for Master-only metric parts (authoritative snapshots) ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetMasterOnlyMetricPartReq {
    pub part: String, // e.g. "segment_bytes"
}
impl MsgPackSerializePart for GetMasterOnlyMetricPartReq {
    fn msg_id(&self) -> u32 {
        MsgId::GetMasterOnlyMetricPartReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetMasterOnlyMetricPartResp {
    pub seg_bytes_map: HashMap<String, (u64, u64)>, // used when part=="segment_bytes"
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for GetMasterOnlyMetricPartResp {
    fn msg_id(&self) -> u32 {
        MsgId::GetMasterOnlyMetricPartResp as u32
    }
}
impl RPCReq for GetMasterOnlyMetricPartReq {
    type Resp = GetMasterOnlyMetricPartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ReserveLocalGrantReq {}
impl MsgPackSerializePart for ReserveLocalGrantReq {
    fn msg_id(&self) -> u32 {
        MsgId::ReserveLocalGrantReq as u32
    }
}

#[allow(unused_assignments)]
#[derive(Default, Debug, Clone, Encode, Decode)]
pub enum ReserveLocalGrantOutcome {
    #[default]
    None,
    Granted {
        grant_id: u64,
        node_id: NodeIDString,
        addr: u64,
        base_addr: u64,
        len: u64,
    },
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ReserveLocalGrantResp {
    pub outcome: ReserveLocalGrantOutcome,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for ReserveLocalGrantResp {
    fn msg_id(&self) -> u32 {
        MsgId::ReserveLocalGrantResp as u32
    }
}
impl RPCReq for ReserveLocalGrantReq {
    type Resp = ReserveLocalGrantResp;
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerReclaimPhase {
    #[default]
    Prepare,
    Commit,
    Abort,
    Finalize,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerReclaimItemState {
    #[default]
    Busy,
    Prepared,
    Committed,
    Aborted,
    Finalized,
    Stale,
}

#[derive(Default, Debug, Clone, Hash, PartialEq, Eq, Encode, Decode)]
pub enum OwnerReclaimBacking {
    #[default]
    Allocation,
    CommittedSlot {
        grant_id: u64,
        slot_index: u32,
        slot_size: u64,
    },
    /// A master-owned allocation with no owner-side key index. The master reclaims this
    /// backing directly after installing its key-activity fence and never sends it to the owner.
    UnindexedAllocation,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerReclaimReason {
    #[default]
    OwnerCapacityEviction,
    MasterAllocationCapacity,
}

/// One exact owner-local source selected for capacity eviction.
#[derive(Default, Debug, Clone, Hash, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSourceEvictionVictim {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub backing: OwnerReclaimBacking,
}

pub(crate) fn owner_source_eviction_epoch(operation_id: u64, victim_index: usize) -> u64 {
    operation_id
        .rotate_left(32)
        .wrapping_add(u64::try_from(victim_index).unwrap_or(u64::MAX))
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchEvictOwnerSourceReq {
    pub operation_id: u64,
    /// Membership generation of the authenticated source owner.
    pub owner_node_start_time: i64,
    pub victims: Vec<OwnerSourceEvictionVictim>,
}

impl MsgPackSerializePart for BatchEvictOwnerSourceReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchEvictOwnerSourceReq as u32
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerSourceEvictionOutcome {
    #[default]
    Unspecified,
    Accepted,
    AlreadyInProgress,
    Completed,
    RetryableBusy,
    Stale,
    RejectedNotEvictable,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSourceEvictionVictimResp {
    pub victim_index: u32,
    pub outcome: OwnerSourceEvictionOutcome,
    pub detail: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchEvictOwnerSourceResp {
    pub operation_id: u64,
    pub victims: Vec<OwnerSourceEvictionVictimResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for BatchEvictOwnerSourceResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchEvictOwnerSourceResp as u32
    }
}

impl RPCReq for BatchEvictOwnerSourceReq {
    type Resp = BatchEvictOwnerSourceResp;
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerReclaimItem {
    pub key: String,
    pub put_id: (u64, u32),
    pub epoch: u64,
    pub backing: OwnerReclaimBacking,
    pub reason: OwnerReclaimReason,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchOwnerReclaimReq {
    pub phase: OwnerReclaimPhase,
    pub items: Vec<OwnerReclaimItem>,
}

impl MsgPackSerializePart for BatchOwnerReclaimReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchOwnerReclaimReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct OwnerReclaimItemResp {
    pub key: String,
    pub epoch: u64,
    pub state: OwnerReclaimItemState,
    pub detail: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchOwnerReclaimResp {
    pub items: Vec<OwnerReclaimItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for BatchOwnerReclaimResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchOwnerReclaimResp as u32
    }
}

impl RPCReq for BatchOwnerReclaimReq {
    type Resp = BatchOwnerReclaimResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct EnqueueReplicaTaskItem {
    pub key: String,
    pub put_id: PutIDForAKey,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchEnqueueReplicaTaskReq {
    pub items: Vec<EnqueueReplicaTaskItem>,
}

impl MsgPackSerializePart for BatchEnqueueReplicaTaskReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchEnqueueReplicaTaskReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct EnqueueReplicaTaskItemResp {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub accepted: bool,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchEnqueueReplicaTaskResp {
    pub items: Vec<EnqueueReplicaTaskItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for BatchEnqueueReplicaTaskResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchEnqueueReplicaTaskResp as u32
    }
}

impl RPCReq for BatchEnqueueReplicaTaskReq {
    type Resp = BatchEnqueueReplicaTaskResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ReleaseLocalGrantReq {
    pub grant_id: u64,
}
impl MsgPackSerializePart for ReleaseLocalGrantReq {
    fn msg_id(&self) -> u32 {
        MsgId::ReleaseLocalGrantReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ReleaseLocalGrantResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for ReleaseLocalGrantResp {
    fn msg_id(&self) -> u32 {
        MsgId::ReleaseLocalGrantResp as u32
    }
}
impl RPCReq for ReleaseLocalGrantReq {
    type Resp = ReleaseLocalGrantResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPreparePutKeyItemReq {
    pub key: String,
    pub reject_if_inflight_same_key: bool,
    pub reject_if_exist_same_key: bool,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPreparePutKeysReq {
    pub items: Vec<BatchPreparePutKeyItemReq>,
}
impl MsgPackSerializePart for BatchPreparePutKeysReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPreparePutKeysReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPreparePutKeysResp {
    pub reservation_ids: Vec<u64>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchPreparePutKeysResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPreparePutKeysResp as u32
    }
}
impl RPCReq for BatchPreparePutKeysReq {
    type Resp = BatchPreparePutKeysResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchReleasePutKeyReservationsReq {
    pub reservation_ids: Vec<u64>,
}
impl MsgPackSerializePart for BatchReleasePutKeyReservationsReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchReleasePutKeyReservationsReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchReleasePutKeyReservationsResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchReleasePutKeyReservationsResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchReleasePutKeyReservationsResp as u32
    }
}
impl RPCReq for BatchReleasePutKeyReservationsReq {
    type Resp = BatchReleasePutKeyReservationsResp;
}

// --- RPC for Put ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutStartReq {
    pub key: String,
    pub len: u64,
    pub reject_if_inflight_same_key: bool,
    pub reject_if_exist_same_key: bool,
    pub make_replica_task: bool,
    /// Prefer placing the target allocation on any kvclient within this sub_cluster.
    pub preferred_sub_cluster: Option<String>,
    /// Optional source-node override for side-transfer workers that share an owner's mmap.
    pub source_node_id: Option<NodeIDString>,
}
impl MsgPackSerializePart for PutStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutStartReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutReplicaTarget {
    pub node_id: NodeIDString,
    pub target_addr: u64,
    pub target_base_addr: u64,
    pub len: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutStartResp {
    pub put_id: PutIDForAKey,
    pub node_id: NodeIDString,
    // absolute addresses because Mooncake transfer engine requires absolute addresses (not offsets)
    pub target_addr: u64,
    pub src_addr: u64,
    // base addresses to allow callers to convert abs->offset when needed
    pub target_base_addr: u64,
    pub src_base_addr: u64,
    pub len: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
    /// Server-side processing time in microseconds for this RPC handler
    pub server_process_us: i64,
    pub replica_target: Option<PutReplicaTarget>,
}
impl MsgPackSerializePart for PutStartResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutStartResp as u32
    }
}
impl RPCReq for PutStartReq {
    type Resp = PutStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutRevokeReq {
    pub key: String,
    pub put_id: PutIDForAKey,
}
impl MsgPackSerializePart for PutRevokeReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutRevokeReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutRevokeResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for PutRevokeResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutRevokeResp as u32
    }
}
impl RPCReq for PutRevokeReq {
    type Resp = PutRevokeResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutDoneCommittedSlot {
    pub grant_id: u64,
    pub slot_index: u32,
    pub slot_size: u64,
    pub addr: u64,
    pub base_addr: u64,
    pub len: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PutAtomicGroupMember {
    pub key: String,
    pub put_id: PutIDForAKey,
}

/// Version-scoped members of one caller-declared atomic put group.
///
/// Groups with one member are represented as `None` on the route. Multi-member
/// groups let eviction require one common remote-cache owner to hold every member.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PutAtomicGroup {
    pub members: Vec<PutAtomicGroupMember>,
}

pub fn build_put_atomic_group_assignments(
    keys_and_put_ids: &[(String, PutIDForAKey)],
    atomic_group_lens: &[usize],
) -> Result<Vec<Option<PutAtomicGroup>>, String> {
    build_shared_put_atomic_group_assignments(keys_and_put_ids, atomic_group_lens).map(
        |assignments| {
            assignments
                .into_iter()
                .map(|group| group.map(|group| group.as_ref().clone()))
                .collect()
        },
    )
}

/// Builds one shared descriptor per multi-key group and assigns cheap `Arc`
/// clones to its members. This is the grouped-put representation used by the
/// V2 route-publish protocol; unlike the legacy wire representation, it is
/// linear in the number of keys rather than the sum of squared group sizes.
pub fn build_shared_put_atomic_group_assignments(
    keys_and_put_ids: &[(String, PutIDForAKey)],
    atomic_group_lens: &[usize],
) -> Result<Vec<Option<Arc<PutAtomicGroup>>>, String> {
    if atomic_group_lens.is_empty() && !keys_and_put_ids.is_empty() {
        return Err("atomic_group_lens must be non-empty".to_string());
    }
    let mut offset = 0usize;
    let mut assignments = Vec::with_capacity(keys_and_put_ids.len());
    for (group_index, group_len) in atomic_group_lens.iter().copied().enumerate() {
        if group_len == 0 {
            return Err(format!(
                "atomic_group_lens entries must be > 0; index={group_index}"
            ));
        }
        if group_len > 4096 {
            return Err(format!(
                "atomic_group_lens entries must be <= 4096; index={group_index} len={group_len}"
            ));
        }
        let end = offset
            .checked_add(group_len)
            .ok_or_else(|| "atomic_group_lens sum overflowed usize".to_string())?;
        let members = keys_and_put_ids.get(offset..end).ok_or_else(|| {
            format!(
                "atomic_group_lens exceeds keys length; end={} keys={}",
                end,
                keys_and_put_ids.len()
            )
        })?;
        if group_len == 1 {
            assignments.push(None);
        } else {
            let group = Arc::new(PutAtomicGroup {
                members: members
                    .iter()
                    .map(|(key, put_id)| PutAtomicGroupMember {
                        key: key.clone(),
                        put_id: *put_id,
                    })
                    .collect(),
            });
            assignments.extend((0..group_len).map(|_| Some(group.clone())));
        }
        offset = end;
    }
    if offset != keys_and_put_ids.len() {
        return Err(format!(
            "atomic_group_lens must sum to keys length; sum={} keys={}",
            offset,
            keys_and_put_ids.len()
        ));
    }
    Ok(assignments)
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutDoneReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    /// Optional lease to attach this key to on commit
    pub lease_id: Option<u64>,
    /// Optional local committed slot descriptor for local-first publish path.
    pub committed_slot: Option<PutDoneCommittedSlot>,
    /// Ask master to keep a local read holder for the committing node.
    pub publish_local_cache: bool,
    /// Multi-key atomic group for this exact key version.
    pub atomic_group: Option<PutAtomicGroup>,
}
impl MsgPackSerializePart for PutDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutDoneReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutDoneResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    /// Server-side processing time in microseconds for this RPC handler
    pub server_process_us: i64,
    /// Holder id for an owner-local cache view, present only when requested.
    pub local_cache_holder_id: Option<u64>,
}
impl MsgPackSerializePart for PutDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutDoneResp as u32
    }
}
impl RPCReq for PutDoneReq {
    type Resp = PutDoneResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutStartItemReq {
    pub key: String,
    pub len: u64,
    pub reject_if_inflight_same_key: bool,
    pub reject_if_exist_same_key: bool,
    pub make_replica_task: bool,
    pub preferred_sub_cluster: Option<String>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutStartReq {
    pub items: Vec<BatchPutStartItemReq>,
}
impl MsgPackSerializePart for BatchPutStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutStartReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutStartItemResp {
    pub put_id: PutIDForAKey,
    pub node_id: NodeIDString,
    pub target_addr: u64,
    pub src_addr: u64,
    pub target_base_addr: u64,
    pub src_base_addr: u64,
    pub len: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub replica_target: Option<PutReplicaTarget>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutStartResp {
    pub items: Vec<BatchPutStartItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchPutStartResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutStartResp as u32
    }
}
impl RPCReq for BatchPutStartReq {
    type Resp = BatchPutStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutRevokeItemReq {
    pub key: String,
    pub put_id: PutIDForAKey,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutRevokeReq {
    pub items: Vec<BatchPutRevokeItemReq>,
}
impl MsgPackSerializePart for BatchPutRevokeReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutRevokeReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutRevokeItemResp {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutRevokeResp {
    pub items: Vec<BatchPutRevokeItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for BatchPutRevokeResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutRevokeResp as u32
    }
}
impl RPCReq for BatchPutRevokeReq {
    type Resp = BatchPutRevokeResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutDoneItemReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub lease_id: Option<u64>,
    pub committed_slot: Option<PutDoneCommittedSlot>,
    pub publish_local_cache: bool,
    pub atomic_group: Option<PutAtomicGroup>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutDoneReq {
    pub items: Vec<BatchPutDoneItemReq>,
}
impl MsgPackSerializePart for BatchPutDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutDoneReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutDoneItemResp {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub local_cache_holder_id: Option<u64>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutDoneResp {
    pub items: Vec<BatchPutDoneItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchPutDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutDoneResp as u32
    }
}
impl RPCReq for BatchPutDoneReq {
    type Resp = BatchPutDoneResp;
}

/// Linear-size V2 batch route publication. `atomic_group_lens` partitions the
/// ordered items; the master reconstructs each group once from the item keys
/// and put ids, then shares one interned descriptor across member routes.
///
/// The V1 `BatchPutDoneReq` remains registered unchanged for rolling and API
/// compatibility.
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GroupedBatchPutDoneItemReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub lease_id: Option<u64>,
    pub committed_slot: Option<PutDoneCommittedSlot>,
    pub publish_local_cache: bool,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GroupedBatchPutDoneReq {
    pub items: Vec<GroupedBatchPutDoneItemReq>,
    pub atomic_group_lens: Vec<usize>,
}
impl MsgPackSerializePart for GroupedBatchPutDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::GroupedBatchPutDoneReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GroupedBatchPutDoneResp {
    pub items: Vec<BatchPutDoneItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for GroupedBatchPutDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::GroupedBatchPutDoneResp as u32
    }
}
impl RPCReq for GroupedBatchPutDoneReq {
    type Resp = GroupedBatchPutDoneResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendStartReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub len: u64,
    pub preferred_sub_cluster: Option<String>,
    pub protect_source_on_remote_complete: bool,
}
impl MsgPackSerializePart for PutAppendStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendStartReq as u32
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum PutAppendStartOutcome {
    /// Missing/old peers must not accidentally interpret a zero value as
    /// successful completion.
    #[default]
    Unspecified,
    Scheduled,
    /// A complete non-source replica already exists for this exact put_id.
    AlreadySatisfied,
    /// The source route/version no longer exists; retry would target stale data.
    Obsolete,
    /// No remote allocation is available now. The owner keeps its local slot
    /// and retries with backoff; this is never a demotion/drop instruction.
    RetryableNoSpace,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendStartResp {
    pub outcome: PutAppendStartOutcome,
    /// Master-issued identity for this concrete replica append attempt.
    ///
    /// `put_id` identifies the KV generation, but one generation may need to
    /// be copied remotely more than once after an earlier remote route is
    /// reclaimed.  Done/Revoke must echo this value so an old replayable
    /// terminal result cannot complete a later reservation.
    pub operation_id: u64,
    pub node_id: NodeIDString,
    pub target_addr: u64,
    pub target_base_addr: u64,
    pub len: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for PutAppendStartResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendStartResp as u32
    }
}
impl RPCReq for PutAppendStartReq {
    type Resp = PutAppendStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendStartItemReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub len: u64,
    pub preferred_sub_cluster: Option<String>,
    pub protect_source_on_remote_complete: bool,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendStartReq {
    pub items: Vec<BatchPutAppendStartItemReq>,
}
impl MsgPackSerializePart for BatchPutAppendStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutAppendStartReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendStartItemResp {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub outcome: PutAppendStartOutcome,
    pub operation_id: u64,
    pub node_id: NodeIDString,
    pub target_addr: u64,
    pub target_base_addr: u64,
    pub len: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendStartResp {
    pub items: Vec<BatchPutAppendStartItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchPutAppendStartResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutAppendStartResp as u32
    }
}
impl RPCReq for BatchPutAppendStartReq {
    type Resp = BatchPutAppendStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendRevokeReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub operation_id: u64,
}
impl MsgPackSerializePart for PutAppendRevokeReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendRevokeReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendRevokeResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for PutAppendRevokeResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendRevokeResp as u32
    }
}
impl RPCReq for PutAppendRevokeReq {
    type Resp = PutAppendRevokeResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendDoneReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub operation_id: u64,
}
impl MsgPackSerializePart for PutAppendDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendDoneReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendDoneResp {
    pub appended: bool,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for PutAppendDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendDoneResp as u32
    }
}
impl RPCReq for PutAppendDoneReq {
    type Resp = PutAppendDoneResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendDoneItemReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub operation_id: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendDoneReq {
    pub items: Vec<BatchPutAppendDoneItemReq>,
}
impl MsgPackSerializePart for BatchPutAppendDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutAppendDoneReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendDoneItemResp {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub appended: bool,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPutAppendDoneResp {
    pub items: Vec<BatchPutAppendDoneItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchPutAppendDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPutAppendDoneResp as u32
    }
}
impl RPCReq for BatchPutAppendDoneReq {
    type Resp = BatchPutAppendDoneResp;
}

// --- RPC for MemHolder KeepAlive ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct MemHolderKeepAliveReq {
    pub holder_id: u64,
}
impl MsgPackSerializePart for MemHolderKeepAliveReq {
    fn msg_id(&self) -> u32 {
        MsgId::MemHolderKeepAliveReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct MemHolderKeepAliveResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for MemHolderKeepAliveResp {
    fn msg_id(&self) -> u32 {
        MsgId::MemHolderKeepAliveResp as u32
    }
}
impl RPCReq for MemHolderKeepAliveReq {
    type Resp = MemHolderKeepAliveResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct MemHolderReleaseReq {
    pub holder_id: u64,
}
impl MsgPackSerializePart for MemHolderReleaseReq {
    fn msg_id(&self) -> u32 {
        MsgId::MemHolderReleaseReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct MemHolderReleaseResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for MemHolderReleaseResp {
    fn msg_id(&self) -> u32 {
        MsgId::MemHolderReleaseResp as u32
    }
}
impl RPCReq for MemHolderReleaseReq {
    type Resp = MemHolderReleaseResp;
}

// --- RPC for Delete ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct DeleteReq {
    pub key: String,
}
impl MsgPackSerializePart for DeleteReq {
    fn msg_id(&self) -> u32 {
        MsgId::DeleteReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct DeleteResp {
    pub deleted_put_time_ms: u64,
    pub deleted_put_version: u32,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for DeleteResp {
    fn msg_id(&self) -> u32 {
        MsgId::DeleteResp as u32
    }
}
impl RPCReq for DeleteReq {
    type Resp = DeleteResp;
}

// --- RPC for DeleteAck ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct DeleteAckReq {
    pub key: String,
    pub client_id: String,
    pub holder_id: u64,
}
impl MsgPackSerializePart for DeleteAckReq {
    fn msg_id(&self) -> u32 {
        MsgId::DeleteAckReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct DeleteAckResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for DeleteAckResp {
    fn msg_id(&self) -> u32 {
        MsgId::DeleteAckResp as u32
    }
}
impl RPCReq for DeleteAckReq {
    type Resp = DeleteAckResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct DeleteAckItem {
    pub key: String,
    pub client_id: String,
    pub holder_id: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchDeleteAckReq {
    pub delete_acks: Vec<DeleteAckItem>,
}

impl MsgPackSerializePart for BatchDeleteAckReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchDeleteAckReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchDeleteAckResp {
    pub deleted_count: u32,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for BatchDeleteAckResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchDeleteAckResp as u32
    }
}

impl RPCReq for BatchDeleteAckReq {
    type Resp = BatchDeleteAckResp;
}

// --- RPC for GetMeta ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetMetaReq {
    pub key: String,
}
impl MsgPackSerializePart for GetMetaReq {
    fn msg_id(&self) -> u32 {
        MsgId::GetMetaReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetMetaResp {
    pub exists: bool,
    pub len: u64,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for GetMetaResp {
    fn msg_id(&self) -> u32 {
        MsgId::GetMetaResp as u32
    }
}
impl RPCReq for GetMetaReq {
    type Resp = GetMetaResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchIsExistReq {
    pub keys: Vec<String>,
}
impl MsgPackSerializePart for BatchIsExistReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchIsExistReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchIsExistResp {
    pub exists_list: Vec<bool>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for BatchIsExistResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchIsExistResp as u32
    }
}
impl RPCReq for BatchIsExistReq {
    type Resp = BatchIsExistResp;
}

// --- RPC for Batch Delete Client KV Meta Cache ---

#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct BatchDeleteClientKvMetaCacheReq {
    /// List of keys with their metadata for batch deletion
    pub delete_items: Vec<DeleteClientKvMetaCacheItem>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct DeleteClientKvMetaCacheItem {
    pub key: String,
    pub put_time_ms: u64,
    pub put_version: u32,
}

impl MsgPackSerializePart for BatchDeleteClientKvMetaCacheReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchDeleteClientKvMetaCacheReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchDeleteClientKvMetaCacheResp {
    pub deleted_count: u32,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for BatchDeleteClientKvMetaCacheResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchDeleteClientKvMetaCacheResp as u32
    }
}

impl RPCReq for BatchDeleteClientKvMetaCacheReq {
    type Resp = BatchDeleteClientKvMetaCacheResp;
}

#[cfg(test)]
mod put_atomic_group_tests {
    use super::*;

    #[test]
    fn assignments_expand_only_multi_member_groups() {
        let keys_and_put_ids = vec![
            ("a".to_string(), (1, 0)),
            ("b".to_string(), (1, 1)),
            ("c".to_string(), (1, 2)),
        ];
        let assignments = build_put_atomic_group_assignments(&keys_and_put_ids, &[2, 1]).unwrap();
        assert_eq!(assignments.len(), 3);
        assert_eq!(assignments[0], assignments[1]);
        assert_eq!(assignments[0].as_ref().unwrap().members.len(), 2);
        assert!(assignments[2].is_none());
    }

    #[test]
    fn assignments_reject_invalid_partitions() {
        let keys_and_put_ids = vec![("a".to_string(), (1, 0)), ("b".to_string(), (1, 1))];
        assert!(build_put_atomic_group_assignments(&keys_and_put_ids, &[1]).is_err());
        assert!(build_put_atomic_group_assignments(&keys_and_put_ids, &[0, 2]).is_err());
    }

    #[test]
    fn batch_put_done_group_round_trips_on_wire() {
        let group = PutAtomicGroup {
            members: vec![
                PutAtomicGroupMember {
                    key: "a".to_string(),
                    put_id: (1, 0),
                },
                PutAtomicGroupMember {
                    key: "b".to_string(),
                    put_id: (1, 1),
                },
            ],
        };
        let req = BatchPutDoneReq {
            items: vec![BatchPutDoneItemReq {
                key: "a".to_string(),
                put_id: (1, 0),
                lease_id: None,
                committed_slot: None,
                publish_local_cache: false,
                atomic_group: Some(group.clone()),
            }],
        };
        let decoded: BatchPutDoneReq =
            bitcode::decode(&bitcode::encode(&req)).expect("decode atomic put group");
        assert_eq!(decoded.items[0].atomic_group.as_ref(), Some(&group));
    }

    #[test]
    fn grouped_batch_put_done_wire_is_linear_and_round_trips() {
        let keys_and_put_ids = (0..128)
            .map(|index| (format!("page-{index:03}"), (7, index)))
            .collect::<Vec<_>>();
        let group = PutAtomicGroup {
            members: keys_and_put_ids
                .iter()
                .map(|(key, put_id)| PutAtomicGroupMember {
                    key: key.clone(),
                    put_id: *put_id,
                })
                .collect(),
        };
        let legacy = BatchPutDoneReq {
            items: keys_and_put_ids
                .iter()
                .map(|(key, put_id)| BatchPutDoneItemReq {
                    key: key.clone(),
                    put_id: *put_id,
                    lease_id: None,
                    committed_slot: None,
                    publish_local_cache: false,
                    atomic_group: Some(group.clone()),
                })
                .collect(),
        };
        let grouped = GroupedBatchPutDoneReq {
            items: keys_and_put_ids
                .iter()
                .map(|(key, put_id)| GroupedBatchPutDoneItemReq {
                    key: key.clone(),
                    put_id: *put_id,
                    lease_id: None,
                    committed_slot: None,
                    publish_local_cache: false,
                })
                .collect(),
            atomic_group_lens: vec![128],
        };
        let legacy_bytes = bitcode::encode(&legacy);
        let grouped_bytes = bitcode::encode(&grouped);
        assert!(
            grouped_bytes.len() * 16 < legacy_bytes.len(),
            "grouped={} legacy={}",
            grouped_bytes.len(),
            legacy_bytes.len()
        );
        let decoded: GroupedBatchPutDoneReq =
            bitcode::decode(&grouped_bytes).expect("decode grouped put done");
        assert_eq!(decoded.items.len(), 128);
        assert_eq!(decoded.atomic_group_lens, vec![128]);
    }
}
