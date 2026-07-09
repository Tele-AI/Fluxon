use crate::{
    cluster_manager::NodeIDString,
    p2p::msg_pack::{MsgPackSerializePart, RPCReq},
    rpcresp_kvresult_convert::msg_and_error::{ErrorCode, MsgId, OK},
};
use bitcode::{Decode, Encode};
use std::collections::HashMap;

use super::put::PutIDForAKey;

// --- RPC for Get ---

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum GetAllocationMode {
    #[default]
    Temporary = 0,
    ReuseReplica = 1,
    DurableReplica = 2,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetStartReq {
    pub key: String,
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
pub struct ReserveLocalGrantReq;
impl MsgPackSerializePart for ReserveLocalGrantReq {
    fn msg_id(&self) -> u32 {
        MsgId::ReserveLocalGrantReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ReserveLocalGrantResp {
    pub grant_id: u64,
    pub node_id: NodeIDString,
    pub addr: u64,
    pub base_addr: u64,
    pub len: u64,
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

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendStartReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub len: u64,
    pub preferred_sub_cluster: Option<String>,
}
impl MsgPackSerializePart for PutAppendStartReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendStartReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendStartResp {
    pub scheduled: bool,
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
pub struct PutAppendRevokeReq {
    pub key: String,
    pub put_id: PutIDForAKey,
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
