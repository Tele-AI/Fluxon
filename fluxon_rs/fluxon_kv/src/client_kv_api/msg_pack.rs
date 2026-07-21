use crate::master_kv_router::put::PutIDForAKey;
use crate::p2p::msg_pack::{MsgPackSerializePart, RPCReq};
use crate::rpcresp_kvresult_convert::msg_and_error::{ErrorCode, OK};
use bitcode::{Decode, Encode};

use crate::memholder::{ExternalMemHolder, ExternalMemHolderInfo};

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
pub struct ExternalBatchGetReq {
    pub keys: Vec<String>,
    pub req_node_id: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    pub transfer_concurrency: usize,
}
impl MsgPackSerializePart for ExternalBatchGetReq {
    fn msg_id(&self) -> u32 {
        4020
    }
}
impl RPCReq for ExternalBatchGetReq {
    type Resp = ExternalBatchGetResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetItemResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub external_memholder_info: Option<ExternalMemHolderInfo>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetResp {
    pub items: Vec<ExternalBatchGetItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchGetResp {
    fn msg_id(&self) -> u32 {
        4021
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetStartReq {
    pub keys: Vec<String>,
    pub req_node_id: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    pub prefix_best_effort: bool,
    pub atomic_group_lens: Option<Vec<usize>>,
    pub transfer_concurrency: usize,
}
impl MsgPackSerializePart for ExternalBatchGetStartReq {
    fn msg_id(&self) -> u32 {
        4030
    }
}
impl RPCReq for ExternalBatchGetStartReq {
    type Resp = ExternalBatchGetStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetStartResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub handle: u64,
    pub raw_prefix_hit_len: usize,
    pub transfer_plan: ExternalBatchGetStartTransferPlan,
}
impl MsgPackSerializePart for ExternalBatchGetStartResp {
    fn msg_id(&self) -> u32 {
        4031
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub enum ExternalBatchGetStartTransferPlan {
    #[default]
    OwnerRpc,
    InlineLocal {
        items: Vec<ExternalBatchGetItemResp>,
    },
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetTransferReq {
    pub handle: u64,
    pub req_node_id: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    /// Complete atomic-group prefix selected by the consumer from the live
    /// get-start handle.
    pub consume_prefix_len: usize,
}
impl MsgPackSerializePart for ExternalBatchGetTransferReq {
    fn msg_id(&self) -> u32 {
        4032
    }
}
impl RPCReq for ExternalBatchGetTransferReq {
    type Resp = ExternalBatchGetTransferResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetTransferResp {
    pub items: Vec<ExternalBatchGetItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchGetTransferResp {
    fn msg_id(&self) -> u32 {
        4033
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetCancelReq {
    pub handle: u64,
    pub req_node_id: String,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    pub transfer_plan: ExternalBatchGetCancelPlan,
}
impl MsgPackSerializePart for ExternalBatchGetCancelReq {
    fn msg_id(&self) -> u32 {
        4034
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub enum ExternalBatchGetCancelPlan {
    #[default]
    OwnerRpc,
    InlineLocal {
        holder_ids: Vec<u64>,
    },
}
impl RPCReq for ExternalBatchGetCancelReq {
    type Resp = ExternalBatchGetCancelResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchGetCancelResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchGetCancelResp {
    fn msg_id(&self) -> u32 {
        4035
    }
}

#[cfg(test)]
mod external_batch_get_plan_wire_tests {
    use super::{
        ExternalBatchGetCancelPlan, ExternalBatchGetCancelReq, ExternalBatchGetItemResp,
        ExternalBatchGetStartResp, ExternalBatchGetStartTransferPlan, ExternalBatchGetTransferReq,
    };
    use crate::memholder::ExternalMemHolderInfo;
    use crate::rpcresp_kvresult_convert::msg_and_error::OK;

    #[test]
    fn inline_start_and_cancel_plans_round_trip() {
        let start = ExternalBatchGetStartResp {
            error_code: OK,
            error_json: String::new(),
            handle: 19,
            raw_prefix_hit_len: 2,
            transfer_plan: ExternalBatchGetStartTransferPlan::InlineLocal {
                items: vec![ExternalBatchGetItemResp {
                    error_code: OK,
                    error_json: String::new(),
                    external_memholder_info: Some(ExternalMemHolderInfo {
                        offset: 4096,
                        len: 8192,
                        holder_id: 23,
                    }),
                }],
            },
        };
        let decoded_start: ExternalBatchGetStartResp =
            bitcode::decode(&bitcode::encode(&start)).expect("decode inline start plan");
        assert_eq!(decoded_start.handle, 19);
        assert!(matches!(
            decoded_start.transfer_plan,
            ExternalBatchGetStartTransferPlan::InlineLocal { items }
                if items.len() == 1
                    && items[0]
                        .external_memholder_info
                        .as_ref()
                        .is_some_and(|info| info.holder_id == 23)
        ));

        let transfer = ExternalBatchGetTransferReq {
            handle: 19,
            req_node_id: "external-a".to_string(),
            started_time: 29,
            consume_prefix_len: 1,
        };
        let decoded_transfer: ExternalBatchGetTransferReq =
            bitcode::decode(&bitcode::encode(&transfer)).expect("decode transfer request");
        assert_eq!(decoded_transfer.consume_prefix_len, 1);

        let cancel = ExternalBatchGetCancelReq {
            handle: 19,
            req_node_id: "external-a".to_string(),
            started_time: 29,
            transfer_plan: ExternalBatchGetCancelPlan::InlineLocal {
                holder_ids: vec![23, 24],
            },
        };
        let decoded_cancel: ExternalBatchGetCancelReq =
            bitcode::decode(&bitcode::encode(&cancel)).expect("decode inline cancel plan");
        assert!(matches!(
            decoded_cancel.transfer_plan,
            ExternalBatchGetCancelPlan::InlineLocal { holder_ids }
                if holder_ids == vec![23, 24]
        ));
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutStartItemReq {
    pub key: String,
    pub len: u64,
    pub reject_if_inflight_same_key: bool,
    pub reject_if_exist_same_key: bool,
    pub make_replica_task: bool,
    pub preferred_sub_cluster: Option<String>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutStartReq {
    pub items: Vec<ExternalBatchPutStartItemReq>,
    /// Positive lengths partitioning `items`; omitted means one group per item.
    pub atomic_group_lens: Option<Vec<usize>>,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalBatchPutStartReq {
    fn msg_id(&self) -> u32 {
        4022
    }
}
impl RPCReq for ExternalBatchPutStartReq {
    type Resp = ExternalBatchPutStartResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutStartItemResp {
    pub error_code: ErrorCode,
    pub src_offset: u64,
    pub target_offset: u64,
    pub transfer_target_offset: Option<u64>,
    pub peer_id: Option<String>,
    pub src_base_addr: u64,
    pub target_base_addr: u64,
    pub error_json: String,
    pub put_id: Option<PutIDForAKey>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutStartResp {
    pub items: Vec<ExternalBatchPutStartItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchPutStartResp {
    fn msg_id(&self) -> u32 {
        4023
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutTransferEndItemReq {
    pub key: String,
    pub len: u64,
    pub src_offset: u64,
    pub target_offset: u64,
    pub peer_id: Option<String>,
    pub target_base_addr: Option<u64>,
    pub put_id: Option<PutIDForAKey>,
    pub lease_id: Option<u64>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutTransferEndReq {
    pub items: Vec<ExternalBatchPutTransferEndItemReq>,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
    pub transfer_concurrency: usize,
}
impl MsgPackSerializePart for ExternalBatchPutTransferEndReq {
    fn msg_id(&self) -> u32 {
        4024
    }
}
impl RPCReq for ExternalBatchPutTransferEndReq {
    type Resp = ExternalBatchPutTransferEndResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutTransferEndItemResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutTransferEndResp {
    pub items: Vec<ExternalBatchPutTransferEndItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchPutTransferEndResp {
    fn msg_id(&self) -> u32 {
        4025
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutCommitItemReq {
    pub key: String,
    pub len: u64,
    pub src_offset: u64,
    pub remote_target: bool,
    pub put_id: Option<PutIDForAKey>,
    pub lease_id: Option<u64>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutCommitReq {
    pub items: Vec<ExternalBatchPutCommitItemReq>,
    /// Owner node_start_time observed by the caller when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalBatchPutCommitReq {
    fn msg_id(&self) -> u32 {
        4026
    }
}
impl RPCReq for ExternalBatchPutCommitReq {
    type Resp = ExternalBatchPutCommitResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutCommitItemResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchPutCommitResp {
    pub items: Vec<ExternalBatchPutCommitItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchPutCommitResp {
    fn msg_id(&self) -> u32 {
        4027
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalIoLocalitySnapshot {
    pub op_count: u64,
    pub bytes: u64,
    pub transfer_us: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalObservabilitySnapshotReq {
    /// Owner node_start_time observed by external when request starts.
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalObservabilitySnapshotReq {
    fn msg_id(&self) -> u32 {
        4028
    }
}
impl RPCReq for ExternalObservabilitySnapshotReq {
    type Resp = ExternalObservabilitySnapshotResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalObservabilitySnapshotResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub l2_local_hit_pages: u64,
    pub l2_local_hit_bytes: u64,
    pub l2_remote_hit_pages: u64,
    pub l2_remote_hit_bytes: u64,
    pub put_local: ExternalIoLocalitySnapshot,
    pub put_remote: ExternalIoLocalitySnapshot,
    pub get_local: ExternalIoLocalitySnapshot,
    pub get_remote: ExternalIoLocalitySnapshot,
}
impl ExternalObservabilitySnapshotResp {
    pub fn success(snapshot: crate::metrics::KvLocalitySnapshot) -> Self {
        Self {
            error_code: OK,
            error_json: String::new(),
            l2_local_hit_pages: snapshot.l2_local_hit_pages,
            l2_local_hit_bytes: snapshot.l2_local_hit_bytes,
            l2_remote_hit_pages: snapshot.l2_remote_hit_pages,
            l2_remote_hit_bytes: snapshot.l2_remote_hit_bytes,
            put_local: ExternalIoLocalitySnapshot {
                op_count: snapshot.put_local.op_count,
                bytes: snapshot.put_local.bytes,
                transfer_us: snapshot.put_local.transfer_us,
            },
            put_remote: ExternalIoLocalitySnapshot {
                op_count: snapshot.put_remote.op_count,
                bytes: snapshot.put_remote.bytes,
                transfer_us: snapshot.put_remote.transfer_us,
            },
            get_local: ExternalIoLocalitySnapshot {
                op_count: snapshot.get_local.op_count,
                bytes: snapshot.get_local.bytes,
                transfer_us: snapshot.get_local.transfer_us,
            },
            get_remote: ExternalIoLocalitySnapshot {
                op_count: snapshot.get_remote.op_count,
                bytes: snapshot.get_remote.bytes,
                transfer_us: snapshot.get_remote.transfer_us,
            },
        }
    }

    pub fn into_snapshot(self) -> crate::metrics::KvLocalitySnapshot {
        crate::metrics::KvLocalitySnapshot {
            l2_local_hit_pages: self.l2_local_hit_pages,
            l2_local_hit_bytes: self.l2_local_hit_bytes,
            l2_remote_hit_pages: self.l2_remote_hit_pages,
            l2_remote_hit_bytes: self.l2_remote_hit_bytes,
            put_local: crate::metrics::KvIoLocalitySnapshot {
                op_count: self.put_local.op_count,
                bytes: self.put_local.bytes,
                transfer_us: self.put_local.transfer_us,
            },
            put_remote: crate::metrics::KvIoLocalitySnapshot {
                op_count: self.put_remote.op_count,
                bytes: self.put_remote.bytes,
                transfer_us: self.put_remote.transfer_us,
            },
            get_local: crate::metrics::KvIoLocalitySnapshot {
                op_count: self.get_local.op_count,
                bytes: self.get_local.bytes,
                transfer_us: self.get_local.transfer_us,
            },
            get_remote: crate::metrics::KvIoLocalitySnapshot {
                op_count: self.get_remote.op_count,
                bytes: self.get_remote.bytes,
                transfer_us: self.get_remote.transfer_us,
            },
        }
    }
}
impl MsgPackSerializePart for ExternalObservabilitySnapshotResp {
    fn msg_id(&self) -> u32 {
        4029
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
    pub reject_if_exist_same_key: bool,
    pub make_replica_task: bool,
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
    pub len: u64,
    pub src_offset: u64,
    pub remote_target: bool,
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
pub struct ExternalBatchIsExistReq {
    pub keys: Vec<String>,
    pub allow_local_snapshot: bool,
    /// Owner node_start_time observed by external when request starts
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalBatchIsExistReq {
    fn msg_id(&self) -> u32 {
        crate::rpcresp_kvresult_convert::msg_and_error::MsgId::ExternalBatchIsExistReq as u32
    }
}
impl RPCReq for ExternalBatchIsExistReq {
    type Resp = ExternalBatchIsExistResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchIsExistResp {
    pub error_code: ErrorCode,
    pub exists_list: Vec<bool>,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchIsExistResp {
    fn msg_id(&self) -> u32 {
        crate::rpcresp_kvresult_convert::msg_and_error::MsgId::ExternalBatchIsExistResp as u32
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

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchDeleteAckReq {
    pub external_client_id: String,
    pub holder_ids: Vec<u64>,
    /// Owner node_start_time observed when these holders were created.
    pub started_time: i64,
}
impl MsgPackSerializePart for ExternalBatchDeleteAckReq {
    fn msg_id(&self) -> u32 {
        4036
    }
}
impl RPCReq for ExternalBatchDeleteAckReq {
    type Resp = ExternalBatchDeleteAckResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalBatchDeleteAckResp {
    pub released_count: u32,
    pub missing_count: u32,
    pub error_code: ErrorCode,
    pub error_json: String,
}
impl MsgPackSerializePart for ExternalBatchDeleteAckResp {
    fn msg_id(&self) -> u32 {
        4037
    }
}

#[cfg(test)]
mod external_batch_delete_ack_wire_tests {
    use super::ExternalBatchDeleteAckReq;

    #[test]
    fn compact_holder_id_batch_round_trips() {
        let request = ExternalBatchDeleteAckReq {
            external_client_id: "external-a".to_string(),
            holder_ids: vec![3, 5, 8],
            started_time: 17,
        };
        let decoded: ExternalBatchDeleteAckReq =
            bitcode::decode(&bitcode::encode(&request)).expect("decode holder ACK batch");
        assert_eq!(decoded.external_client_id, "external-a");
        assert_eq!(decoded.holder_ids, vec![3, 5, 8]);
        assert_eq!(decoded.started_time, 17);
    }
}

// --- RPC: Owner -> External to invalidate weak-index cache for keys ---
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalInvalidateWeakIndexItem {
    pub key: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ExternalInvalidateWeakIndexReq {
    /// Keys whose weak cache entries should be invalidated on external client.
    /// Kept for compatibility with older senders; new senders should use `items`.
    pub keys: Vec<String>,
    pub items: Vec<ExternalInvalidateWeakIndexItem>,
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
