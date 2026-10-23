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

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum SegmentAllocationAuthority {
    #[default]
    Master,
    Owner,
}

/// Placement role of one owner-authoritative DRAM contributor.
///
/// This is deliberately independent from hostname, GPU enumeration,
/// `sub_cluster` and free-form member metadata.  `Invalid` exists only for
/// error/default wire values and legacy master-authoritative registrations;
/// it is never an eligible owner placement class.
#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OwnerPlacementClass {
    #[default]
    Invalid,
    Inference,
    RemoteCpu,
}

impl OwnerPlacementClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Inference => "inference",
            Self::RemoteCpu => "remote_cpu",
        }
    }

    pub fn is_valid(self) -> bool {
        !matches!(self, Self::Invalid)
    }
}

impl Default for SegmentDeviceDescription {
    fn default() -> Self {
        SegmentDeviceDescription::Uninitialized
    }
}

pub type SegmentDeviceID = String;

/// Exact geometry and ownership contract for one owner generation.
///
/// The same payload is used by the owner-DAG push and the master-pull repair
/// path so registration validation has one wire representation.
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct NodeSegmentRegistration {
    pub allocation_authority: SegmentAllocationAuthority,
    pub owner_placement_class: OwnerPlacementClass,
    pub owner_local_target_bytes: Option<u64>,
    pub seg_map: HashMap<SegmentDeviceID, (SegmentDeviceDescription, SegmentDeviceMemInfo)>,
}

// --- Recovery RPC for RequestSegmentRegistration (Master -> Client) ---

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
    pub registration: Option<NodeSegmentRegistration>,
}

impl MsgPackSerializePart for RequestSegmentRegistrationResp {
    fn msg_id(&self) -> u32 {
        MsgId::RequestSegmentRegistrationResp as u32
    }
}

impl RPCReq for RequestSegmentRegistrationReq {
    type Resp = RequestSegmentRegistrationResp;
}

// --- Canonical owner init-DAG registration RPC (Owner -> Master) ---

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RegisterNodeSegmentsReq {
    pub owner_node_start_time: i64,
    pub registration: NodeSegmentRegistration,
}

impl MsgPackSerializePart for RegisterNodeSegmentsReq {
    fn msg_id(&self) -> u32 {
        MsgId::RegisterNodeSegmentsReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct RegisterNodeSegmentsResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub accepted_owner_node_start_time: i64,
}

impl MsgPackSerializePart for RegisterNodeSegmentsResp {
    fn msg_id(&self) -> u32 {
        MsgId::RegisterNodeSegmentsResp as u32
    }
}

impl RPCReq for RegisterNodeSegmentsReq {
    type Resp = RegisterNodeSegmentsResp;
}

/// Master -> owner terminal for the recovery-pull branch of the distributed
/// registration DAG.  The master sends this only after it has installed the
/// exact owner generation returned by `RequestSegmentRegistration`.
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ConfirmNodeSegmentsRegisteredReq {
    pub owner_node_start_time: i64,
    pub master_node_start_time: i64,
}

impl MsgPackSerializePart for ConfirmNodeSegmentsRegisteredReq {
    fn msg_id(&self) -> u32 {
        MsgId::ConfirmNodeSegmentsRegisteredReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ConfirmNodeSegmentsRegisteredResp {
    pub error_code: ErrorCode,
    pub error_json: String,
    pub accepted_owner_node_start_time: i64,
    pub accepted_master_node_start_time: i64,
}

impl MsgPackSerializePart for ConfirmNodeSegmentsRegisteredResp {
    fn msg_id(&self) -> u32 {
        MsgId::ConfirmNodeSegmentsRegisteredResp as u32
    }
}

impl RPCReq for ConfirmNodeSegmentsRegisteredReq {
    type Resp = ConfirmNodeSegmentsRegisteredResp;
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSizeClassCapacity {
    pub allocation_size_bytes: u64,
    pub allocatable_bytes: u64,
}

/// Generation-fenced capacity summary produced by the owner allocator.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerCapacityReport {
    pub owner_node_start_time: i64,
    pub placement_class: OwnerPlacementClass,
    pub controller_epoch: u64,
    pub report_epoch: u64,
    pub physical_capacity_bytes: u64,
    pub local_target_bytes: u64,
    pub global_target_bytes: u64,
    pub allocated_bytes: u64,
    pub raw_free_bytes: u64,
    pub largest_free_bytes: u64,
    pub global_accounted_bytes: u64,
    pub local_weighted_bytes: u64,
    pub settled: bool,
    pub size_classes: Vec<OwnerSizeClassCapacity>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct OwnerCapacityReportReq {
    pub report: OwnerCapacityReport,
}

impl MsgPackSerializePart for OwnerCapacityReportReq {
    fn msg_id(&self) -> u32 {
        MsgId::OwnerCapacityReportReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct OwnerCapacityReportResp {
    pub accepted_report_epoch: u64,
    /// Exact allocation size classes currently tracked by the master.
    ///
    /// Owners merge these into their next periodic report. This lets an empty
    /// RemoteCpu owner bootstrap exact allocatable-byte reporting without a
    /// guessed-capacity fallback or an extra probe RPC.
    pub requested_size_classes: Vec<u64>,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for OwnerCapacityReportResp {
    fn msg_id(&self) -> u32 {
        MsgId::OwnerCapacityReportResp as u32
    }
}

impl RPCReq for OwnerCapacityReportReq {
    type Resp = OwnerCapacityReportResp;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_class_and_capacity_report_roundtrip() {
        let request = OwnerCapacityReportReq {
            report: OwnerCapacityReport {
                owner_node_start_time: 17,
                placement_class: OwnerPlacementClass::RemoteCpu,
                controller_epoch: 3,
                report_epoch: 9,
                physical_capacity_bytes: 400,
                local_target_bytes: 0,
                global_target_bytes: 400,
                allocated_bytes: 100,
                raw_free_bytes: 300,
                largest_free_bytes: 250,
                global_accounted_bytes: 100,
                local_weighted_bytes: 0,
                settled: true,
                size_classes: vec![OwnerSizeClassCapacity {
                    allocation_size_bytes: 50,
                    allocatable_bytes: 250,
                }],
            },
        };
        let encoded = bitcode::encode(&request);
        let decoded: OwnerCapacityReportReq = bitcode::decode(&encoded).unwrap();
        assert_eq!(decoded.report, request.report);

        let response = OwnerCapacityReportResp {
            accepted_report_epoch: 9,
            requested_size_classes: vec![50, 100],
            error_code: 0,
            error_json: String::new(),
        };
        let encoded = bitcode::encode(&response);
        let decoded: OwnerCapacityReportResp = bitcode::decode(&encoded).unwrap();
        assert_eq!(decoded.accepted_report_epoch, 9);
        assert_eq!(decoded.requested_size_classes, vec![50, 100]);
    }
}

// Removed: QuerySegBaseReq/Resp — no longer supported
