use std::collections::HashMap;

use crate::rpcresp_kvresult_convert::msg_and_error::ErrorCode;
use crate::{
    p2p::msg_pack::{MsgPackSerializePart, RPCReq},
    rpcresp_kvresult_convert::msg_and_error::MsgId,
};
use bitcode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum SegmentDeviceDescription {
    Uninitialized,
    Cpu,
    Gpu,
    Nvme,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentDeviceMemInfo {
    pub addr: u64,
    pub len: u64,
}

impl Default for SegmentDeviceDescription {
    fn default() -> Self {
        SegmentDeviceDescription::Uninitialized
    }
}

pub type SegmentDeviceID = String;

// --- RPC for RequestSegmentRegistration (Master -> Client) ---

#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct RequestSegmentRegistrationReq {
    /// Master-side member-generation guard.
    ///
    /// The master sets this to the target member's `node_start_time` from cluster membership.
    /// The client must reject requests whose expected generation does not match its current
    /// `ClusterMember.node_start_time`.
    ///
    /// Note: `Default` is required by the RPC dispatch registry (type-only); the value is
    /// ignored in that context.
    pub expected_node_start_time: i64,
}

impl MsgPackSerializePart for RequestSegmentRegistrationReq {
    fn msg_id(&self) -> u32 {
        MsgId::RequestSegmentRegistrationReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RequestSegmentRegistrationResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub seg_map: HashMap<SegmentDeviceID, (SegmentDeviceDescription, SegmentDeviceMemInfo)>,
}

impl MsgPackSerializePart for RequestSegmentRegistrationResp {
    fn msg_id(&self) -> u32 {
        MsgId::RequestSegmentRegistrationResp as u32
    }
}

impl RPCReq for RequestSegmentRegistrationReq {
    type Resp = RequestSegmentRegistrationResp;
}

// Removed: QuerySegBaseReq/Resp — no longer supported
