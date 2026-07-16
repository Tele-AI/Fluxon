use crate::master_kv_router::msg_pack::GetAllocationMode;
use crate::master_kv_router::put::PutIDForAKey;
use crate::p2p::msg_pack::{MsgPackSerializePart, RPCReq};
use crate::rpcresp_kvresult_convert::msg_and_error::ErrorCode;
use bitcode::{Decode, Encode};

use crate::cluster_manager::NodeIDString;
use crate::memholder::ExternalMemHolderInfo;

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct TestPutPhaseTrace {
    pub external_put_start_rpc_us: i64,
    pub external_write_payload_us: i64,
    pub external_put_transfer_end_rpc_us: i64,
    pub external_total_us: i64,
    pub external_side_transfer_peer_id: Option<String>,
    pub external_side_transfer_lane_idx: Option<u16>,
    pub owner_external_put_start_total_us: i64,
    pub owner_put_start_total_us: i64,
    pub owner_master_put_start_rpc_us: i64,
    pub owner_master_put_start_server_us: i64,
    pub owner_external_put_transfer_end_total_us: i64,
    pub owner_put_transfer_total_us: i64,
    pub owner_put_transfer_peer_id: Option<String>,
    pub owner_put_end_total_us: i64,
    pub owner_master_put_end_rpc_us: i64,
    pub owner_master_put_end_server_us: i64,
}

impl TestPutPhaseTrace {
    pub fn merge_from(&mut self, rhs: &Self) {
        macro_rules! merge_i64_field {
            ($field:ident) => {
                if rhs.$field != 0 {
                    self.$field = rhs.$field;
                }
            };
        }

        merge_i64_field!(external_put_start_rpc_us);
        merge_i64_field!(external_write_payload_us);
        merge_i64_field!(external_put_transfer_end_rpc_us);
        merge_i64_field!(external_total_us);
        if let Some(peer_id) = rhs.external_side_transfer_peer_id.as_ref() {
            self.external_side_transfer_peer_id = Some(peer_id.clone());
        }
        if let Some(lane_idx) = rhs.external_side_transfer_lane_idx {
            self.external_side_transfer_lane_idx = Some(lane_idx);
        }
        merge_i64_field!(owner_external_put_start_total_us);
        merge_i64_field!(owner_put_start_total_us);
        merge_i64_field!(owner_master_put_start_rpc_us);
        merge_i64_field!(owner_master_put_start_server_us);
        merge_i64_field!(owner_external_put_transfer_end_total_us);
        merge_i64_field!(owner_put_transfer_total_us);
        if let Some(peer_id) = rhs.owner_put_transfer_peer_id.as_ref() {
            self.owner_put_transfer_peer_id = Some(peer_id.clone());
        }
        merge_i64_field!(owner_put_end_total_us);
        merge_i64_field!(owner_master_put_end_rpc_us);
        merge_i64_field!(owner_master_put_end_server_us);
    }
}
// --- RPC for Physical Node Shared Memory ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalGetReq {
    pub key: String,
    pub req_node_id: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalGetReq {
    fn msg_id(&self) -> u32 {
        4001
    }
}
impl RPCReq for ExternalGetReq {
    type Resp = ExternalGetResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalGetResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub external_memholder_info: Option<ExternalMemHolderInfo>,
}
impl MsgPackSerializePart for ExternalGetResp {
    fn msg_id(&self) -> u32 {
        4002
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdStageReadReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub get_id: u64,
    pub stage_addr: u64,
    pub stage_len: u64,
    pub target_node_id: NodeIDString,
    pub target_addr: u64,
    pub len: u64,
}

impl MsgPackSerializePart for SsdStageReadReq {
    fn msg_id(&self) -> u32 {
        4020
    }
}

impl RPCReq for SsdStageReadReq {
    type Resp = SsdStageReadResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdStageReadResp {
    pub done_holder_id: u64,
    pub done_allocation_mode: GetAllocationMode,
    pub done_error_code: ErrorCode,
    pub done_error_json: String,
    pub done_server_process_us: i64,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for SsdStageReadResp {
    fn msg_id(&self) -> u32 {
        4021
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdReplicaPersistReq {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub target_addr: u64,
    pub len: u64,
}

impl MsgPackSerializePart for SsdReplicaPersistReq {
    fn msg_id(&self) -> u32 {
        4022
    }
}

impl RPCReq for SsdReplicaPersistReq {
    type Resp = SsdReplicaPersistResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdReplicaPersistResp {
    pub persisted: bool,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for SsdReplicaPersistResp {
    fn msg_id(&self) -> u32 {
        4023
    }
}

// #[derive(Default, Debug, Clone, Encode, Decode)]
// pub struct ExternalPutReq {
//     pub key: String,
//     pub len: u64,
// }
// impl MsgPackSerializePart for ExternalPutReq {
//     fn msg_id(&self) -> u32 { 4003 }
// }
// impl RPCReq for ExternalPutReq {
//     type Resp = ExternalPutResp;
// }

// #[derive(Default, Debug, Clone, Encode, Decode)]
// pub struct ExternalPutResp {
//     pub success: bool,
//     pub error_msg: String,
// }
// impl MsgPackSerializePart for ExternalPutResp {
//     fn msg_id(&self) -> u32 { 4004 }
// }
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutStartReq {
    pub key: String,
    pub len: u64,
    pub reject_if_inflight_same_key: bool,
    pub reject_if_exists: bool,
    /// Prefer placing the target allocation on any kvclient within this sub_cluster.
    pub preferred_sub_cluster: Option<String>,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    /// Hidden test-only switch for latency composition observation.
    pub test_observe_put_phases: bool,
}
impl MsgPackSerializePart for ExternalPutStartReq {
    fn msg_id(&self) -> u32 {
        4003
    }
}
impl RPCReq for ExternalPutStartReq {
    type Resp = ExternalPutStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutStartResp {
    pub error_code: ErrorCode,
    pub src_offset: u64,
    pub target_offset: u64,
    pub transfer_target_offset: Option<u64>,
    pub peer_id: Option<String>,
    // base addrs to allow owner to reconstruct abs addrs without internal state
    pub src_base_addr: u64,
    pub target_base_addr: u64,
    pub error_json: String,
    pub put_id: Option<PutIDForAKey>,
    pub test_put_phase_trace: Option<TestPutPhaseTrace>,
}
impl MsgPackSerializePart for ExternalPutStartResp {
    fn msg_id(&self) -> u32 {
        4004
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutTransferEndReq {
    pub key: String,
    pub len: u64,
    pub src_offset: u64,
    pub target_offset: u64,
    pub peer_id: Option<String>,
    pub target_base_addr: Option<u64>,
    pub put_id: Option<PutIDForAKey>,
    /// Optional lease to attach this key to when committing
    pub lease_id: Option<u64>,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    /// Hidden test-only switch for latency composition observation.
    pub test_observe_put_phases: bool,
}
impl MsgPackSerializePart for ExternalPutTransferEndReq {
    fn msg_id(&self) -> u32 {
        4005
    }
}
impl RPCReq for ExternalPutTransferEndReq {
    type Resp = ExternalPutTransferEndResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutTransferEndResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub test_put_phase_trace: Option<TestPutPhaseTrace>,
}
impl MsgPackSerializePart for ExternalPutTransferEndResp {
    fn msg_id(&self) -> u32 {
        4006
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutCommitReq {
    pub key: String,
    pub put_id: Option<PutIDForAKey>,
    pub lease_id: Option<u64>,
    /// Owner node_start_time observed by the caller when request starts
    pub started_time: i64,
    pub test_observe_put_phases: bool,
}
impl MsgPackSerializePart for ExternalPutCommitReq {
    fn msg_id(&self) -> u32 {
        4016
    }
}
impl RPCReq for ExternalPutCommitReq {
    type Resp = ExternalPutCommitResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutCommitResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub test_put_phase_trace: Option<TestPutPhaseTrace>,
}
impl MsgPackSerializePart for ExternalPutCommitResp {
    fn msg_id(&self) -> u32 {
        4017
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutRevokeReq {
    pub key: String,
    pub put_id: Option<PutIDForAKey>,
    /// Owner node_start_time observed by the caller when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalPutRevokeReq {
    fn msg_id(&self) -> u32 {
        4018
    }
}
impl RPCReq for ExternalPutRevokeReq {
    type Resp = ExternalPutRevokeResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalPutRevokeResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalPutRevokeResp {
    fn msg_id(&self) -> u32 {
        4019
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalDeleteReq {
    pub key: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalDeleteReq {
    fn msg_id(&self) -> u32 {
        4009
    }
}
impl RPCReq for ExternalDeleteReq {
    type Resp = ExternalDeleteResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalDeleteResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalDeleteResp {
    fn msg_id(&self) -> u32 {
        4010
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalIsExistReq {
    pub key: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalIsExistReq {
    fn msg_id(&self) -> u32 {
        4011
    }
}
impl RPCReq for ExternalIsExistReq {
    type Resp = ExternalIsExistResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalIsExistResp {
    pub error_code: ErrorCode,
    pub exists: bool,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalIsExistResp {
    fn msg_id(&self) -> u32 {
        4012
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalDeleteAckReq {
    pub key: String,
    pub external_client_id: String,
    pub holder_id: u64,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalDeleteAckReq {
    fn msg_id(&self) -> u32 {
        4013
    }
}
impl RPCReq for ExternalDeleteAckReq {
    type Resp = ExternalDeleteAckResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalDeleteAckResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalDeleteAckResp {
    fn msg_id(&self) -> u32 {
        4014
    }
}

// --- RPC: Owner -> External to invalidate weak-index cache for keys ---
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalInvalidateWeakIndexReq {
    /// Keys whose weak cache entries should be invalidated on external client
    pub keys: Vec<String>,
}
impl MsgPackSerializePart for ExternalInvalidateWeakIndexReq {
    fn msg_id(&self) -> u32 {
        4015
    }
}
impl RPCReq for ExternalInvalidateWeakIndexReq {
    type Resp = ExternalInvalidateWeakIndexResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalInvalidateWeakIndexResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalInvalidateWeakIndexResp {
    fn msg_id(&self) -> u32 {
        4016
    }
}

// --- RPC: Sync a KV bytes field to a file at an explicit offset on the target node ---
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SyncKvToFileReq {
    pub key: String,
    pub bytes_field_key: String,
    pub filepath: String,
    pub file_offset: u64,
}
impl MsgPackSerializePart for SyncKvToFileReq {
    fn msg_id(&self) -> u32 {
        4111
    }
}
impl RPCReq for SyncKvToFileReq {
    type Resp = SyncKvToFileResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SyncKvToFileResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for SyncKvToFileResp {
    fn msg_id(&self) -> u32 {
        4112
    }
}
