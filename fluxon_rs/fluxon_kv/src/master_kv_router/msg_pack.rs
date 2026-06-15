use crate::{
    cluster_manager::NodeIDString,
    p2p::msg_pack::{MsgPackSerializePart, RPCReq},
    rpcresp_kvresult_convert::msg_and_error::{ErrorCode, MsgId},
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

// --- RPC for Put ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutStartReq {
    pub key: String,
    pub len: u64,
    pub reject_if_inflight_same_key: bool,
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
pub struct PutDoneReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    /// Optional lease to attach this key to on commit
    pub lease_id: Option<u64>,
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
}
impl MsgPackSerializePart for PutDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::PutDoneResp as u32
    }
}
impl RPCReq for PutDoneReq {
    type Resp = PutDoneResp;
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
