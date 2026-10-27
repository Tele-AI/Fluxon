use crate::owner_segment::{
    OwnerSlotDesc, OwnerSourceRouteToken, OwnerSsdSourceRouteToken, OwnerTargetRouteToken,
};
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
    /// Caller-owned memory used only as the terminal data sink. The master
    /// neither allocates it nor publishes it as a cache route on GetDone.
    ExternalSink = 4,
    /// The selected global Allocation already belongs to the requester's
    /// share-group owner. The owner borrows that exact backing for this Get;
    /// no payload moves and the route remains in the global/ring-B domain.
    RequesterLocalBorrow = 5,
    /// The selected GlobalShared CommittedSlot already belongs to the
    /// requester's owner segment.  The owner retains that exact slot and
    /// GetDone changes only its route/index scope back to LocalExclusive.
    RequesterLocalPromote = 6,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum GetSourceKind {
    #[default]
    Memory = 0,
    Ssd = 1,
}

/// Exact owner-issued target capability. Keeping the wire type identical to
/// route metadata prevents generation or registration identity from being
/// dropped between local claim, master validation and transfer completion.
pub type GetPreparedLocalReserveTarget = crate::owner_segment::OwnerSlotDesc;

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct GetExternalSinkTarget {
    /// Exact destination address in the requester's registered memory.
    pub addr: u64,
    /// Caller-validated writable bytes starting at `addr`.
    pub capacity: u64,
    /// Opaque requester-side registration generation, retained for identity
    /// and observability. The requester remains authoritative for MR lifetime.
    pub registration_id: u64,
    /// Requester membership generation captured with the GPU registration.
    pub requester_node_start_time: i64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct GetStartReq {
    pub key: String,
    pub prepared_target: Option<GetPreparedLocalReserveTarget>,
    pub external_sink_target: Option<GetExternalSinkTarget>,
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
    pub source_kind: GetSourceKind,
    /// Exact owner-managed source capability. `None` means the source still
    /// uses the transitional master-Allocation or SSD staging path.
    pub source_route_token: Option<OwnerSourceRouteToken>,
    /// Exact owner/SSD generation selected by master. The SSD owner allocates
    /// transient DRAM locally only when GetToTarget executes.
    pub ssd_source_route_token: Option<OwnerSsdSourceRouteToken>,
    /// Echoes the owner-local slot accepted as this Get's target.
    pub prepared_target: Option<GetPreparedLocalReserveTarget>,
    /// Exact existing GlobalShared owner slot reused as both source and
    /// destination. Unlike `prepared_target`, this slot already owns a live
    /// route and must be retained/promoted instead of released as an
    /// uncommitted destination.
    pub reused_committed_slot: Option<GetPreparedLocalReserveTarget>,
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
pub struct SsdStageBeginReq {
    pub get_id: u64,
}

impl MsgPackSerializePart for SsdStageBeginReq {
    fn msg_id(&self) -> u32 {
        MsgId::SsdStageBeginReq as u32
    }
}

impl RPCReq for SsdStageBeginReq {
    type Resp = SsdStageBeginResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdStageBeginResp {
    pub started: bool,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for SsdStageBeginResp {
    fn msg_id(&self) -> u32 {
        MsgId::SsdStageBeginResp as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdStageDoneReq {
    pub get_id: u64,
    pub drop_ssd_source: bool,
}

impl MsgPackSerializePart for SsdStageDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::SsdStageDoneReq as u32
    }
}

impl RPCReq for SsdStageDoneReq {
    type Resp = SsdStageDoneResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct SsdStageDoneResp {
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for SsdStageDoneResp {
    fn msg_id(&self) -> u32 {
        MsgId::SsdStageDoneResp as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetStartReq {
    pub keys: Vec<String>,
    /// Empty selects ordinary master allocations. Otherwise this must contain
    /// exactly one entry per key.
    pub prepared_targets: Vec<Option<GetPreparedLocalReserveTarget>>,
    /// Empty selects no external sinks. Otherwise this must contain exactly
    /// one entry per key and is mutually exclusive with prepared targets.
    pub external_sink_targets: Vec<Option<GetExternalSinkTarget>>,
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
    pub source_kind: GetSourceKind,
    /// Exact owner-managed source capability. The requester must Acquire it
    /// before transfer and Release it only after the target terminal.
    pub source_route_token: Option<OwnerSourceRouteToken>,
    pub ssd_source_route_token: Option<OwnerSsdSourceRouteToken>,
    pub prepared_target: Option<GetPreparedLocalReserveTarget>,
    /// Exact existing GlobalShared owner slot reused as both source and
    /// destination.  Unlike `prepared_target`, this allocation already owns a
    /// live route and must never be released as an uncommitted target.
    pub reused_committed_slot: Option<GetPreparedLocalReserveTarget>,
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

/// Target-free Get planning. Successful items retain one exact source
/// generation until Bind, Revoke, or expiry.
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetPlanReq {
    pub keys: Vec<String>,
}
impl MsgPackSerializePart for BatchGetPlanReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetPlanReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetPlanItemResp {
    pub get_id: u64,
    pub node_id: NodeIDString,
    pub put_id: PutIDForAKey,
    pub src_addr: u64,
    pub src_base_addr: u64,
    pub len: u64,
    pub source_kind: GetSourceKind,
    /// Exact owner-managed source capability selected by this plan. This is
    /// metadata only; GetToTarget installs the owner read lease internally.
    pub source_route_token: Option<OwnerSourceRouteToken>,
    pub ssd_source_route_token: Option<OwnerSsdSourceRouteToken>,
    pub atomic_group: Option<PutAtomicGroup>,
    /// True only for remote owner-authoritative memory. Requester-local
    /// memory, transitional master allocations and every SSD source must
    /// materialize through an owner CPU holder instead of using late-bound
    /// direct GPU transfer.
    pub gpu_direct_eligible: bool,
    /// The selected memory source is an Allocation on the requester's owner.
    /// A CPU execution may bind that source in place instead of claiming a
    /// local-reserve destination and copying DRAM to DRAM.
    pub requester_local_borrow_eligible: bool,
    pub error_code: ErrorCode,
    pub error_json: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetPlanResp {
    pub items: Vec<BatchGetPlanItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchGetPlanResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetPlanResp as u32
    }
}
impl RPCReq for BatchGetPlanReq {
    type Resp = BatchGetPlanResp;
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum GetBindTarget {
    #[default]
    Invalid,
    PreparedLocalReserve(GetPreparedLocalReserveTarget),
    ExternalSink(GetExternalSinkTarget),
    /// Bind the exact planned requester-local backing as both source and
    /// target. Bind revalidates the route generation and backing geometry. A
    /// master-owned Allocation remains a borrow; an owner-managed
    /// CommittedSlot is promoted metadata-only on GetDone.
    RequesterLocalSource,
}

impl BatchGetPlanItemResp {
    /// Materialize a transfer descriptor without a separate Bind RPC when the
    /// memory source already has an exact owner capability and the target is
    /// owned by the caller. Master validation still runs later from BatchGetDone's
    /// `late_target`; this helper only preserves the exact Plan/target identity
    /// used by GetToTarget and the data transfer. SSD sources require Bind first
    /// to establish the stage lifecycle before loading their transient source.
    pub(crate) fn materialize_owner_source_late_target(
        &self,
        target: &GetBindTarget,
        requester_owner: Option<&crate::owner_segment::OwnerGeneration>,
    ) -> Result<BatchGetStartItemResp, &'static str> {
        if self.error_code != OK {
            return Err("late target requires a successful Plan");
        }
        let plan_nonce = self
            .get_id
            .checked_add(1)
            .ok_or("master Get id overflows the owner operation identity")?;
        let memory_source = match self.source_kind {
            GetSourceKind::Memory => {
                let source = self
                    .source_route_token
                    .as_ref()
                    .ok_or("memory Plan requires an exact owner source token")?;
                if self.ssd_source_route_token.is_some()
                    || source.put_id != self.put_id
                    || source.route_epoch == 0
                    || !source.source.is_valid()
                    || source.source.owner.node_id != self.node_id
                    || source.source.addr != self.src_addr
                    || source.source.base_addr != self.src_base_addr
                    || source.source.len != self.len
                    || source.atomic_batch != self.atomic_group
                    || source.plan_nonce != plan_nonce
                {
                    return Err("owner memory source token does not match the Plan");
                }
                Some(source)
            }
            GetSourceKind::Ssd => {
                let source = self
                    .ssd_source_route_token
                    .as_ref()
                    .ok_or("SSD Plan requires an exact owner source token")?;
                if self.source_route_token.is_some()
                    || source.put_id != self.put_id
                    || source.owner.node_id != self.node_id
                    || source.len != self.len
                    || source.atomic_batch != self.atomic_group
                    || source.plan_nonce != plan_nonce
                {
                    return Err("owner SSD source token does not match the Plan");
                }
                return Err("SSD source requires Bind before staging");
            }
        };

        let (target_addr, target_base_addr, prepared_target, reused_committed_slot) = match target {
            GetBindTarget::PreparedLocalReserve(target) => {
                if !target.is_valid() || target.len != self.len || target.capacity_bytes < self.len
                {
                    return Err("prepared owner target does not match the Plan length");
                }
                (target.addr, target.base_addr, Some(target.clone()), None)
            }
            GetBindTarget::ExternalSink(target) => {
                if target.addr == 0
                    || target.capacity < self.len
                    || target.registration_id == 0
                    || target.addr.checked_add(target.capacity).is_none()
                {
                    return Err("external sink target is invalid or too small");
                }
                (target.addr, target.addr, None, None)
            }
            GetBindTarget::RequesterLocalSource => {
                let source =
                    memory_source.ok_or("SSD source cannot be reused as requester-local memory")?;
                if !self.requester_local_borrow_eligible {
                    return Err("Plan did not authorize requester-local source reuse");
                }
                let requester_owner = requester_owner
                    .ok_or("requester-local reuse requires the current owner generation")?;
                if &source.source.owner != requester_owner {
                    return Err("requester-local source belongs to another owner generation");
                }
                (
                    self.src_addr,
                    self.src_base_addr,
                    None,
                    Some(source.source.clone()),
                )
            }
            GetBindTarget::Invalid => return Err("late target must be concrete"),
        };

        Ok(BatchGetStartItemResp {
            get_id: self.get_id,
            node_id: self.node_id.clone(),
            put_id: self.put_id,
            target_addr,
            src_addr: self.src_addr,
            target_base_addr,
            src_base_addr: self.src_base_addr,
            len: self.len,
            source_kind: self.source_kind,
            source_route_token: self.source_route_token.clone(),
            ssd_source_route_token: self.ssd_source_route_token.clone(),
            prepared_target,
            reused_committed_slot,
            atomic_group: self.atomic_group.clone(),
            error_code: self.error_code,
            error_json: self.error_json.clone(),
        })
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetBindItemReq {
    pub get_id: u64,
    pub target: GetBindTarget,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetBindReq {
    pub items: Vec<BatchGetBindItemReq>,
}
impl MsgPackSerializePart for BatchGetBindReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetBindReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetBindResp {
    pub items: Vec<BatchGetStartItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for BatchGetBindResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchGetBindResp as u32
    }
}
impl RPCReq for BatchGetBindReq {
    type Resp = BatchGetBindResp;
}

#[cfg(test)]
mod planned_get_wire_tests {
    use super::{
        BatchGetBindItemReq, BatchGetBindReq, BatchGetDoneItemReq, BatchGetDoneReq,
        BatchGetPlanItemResp, BatchGetPlanResp, BatchGetStartItemResp, GetBindTarget,
        GetExternalSinkTarget, GetPreparedLocalReserveTarget, GetSourceKind,
    };
    use crate::owner_segment::{
        OwnerGeneration, OwnerSlotDesc, OwnerSourceRouteToken, OwnerSsdSourceRouteToken,
    };
    use crate::rpcresp_kvresult_convert::msg_and_error::OK;

    #[test]
    fn first_get_id_and_late_gpu_binding_round_trip() {
        let source_slot = OwnerSlotDesc {
            owner: OwnerGeneration::new("source-a", 9),
            allocation_id: 17,
            segment_offset: 0x800,
            capacity_bytes: 4096,
            addr: 0x1000,
            base_addr: 0x800,
            len: 4096,
            segment_registration_epoch: 3,
        };
        let plan = BatchGetPlanResp {
            items: vec![BatchGetPlanItemResp {
                get_id: 0,
                node_id: "source-a".to_string(),
                src_addr: 0x1000,
                src_base_addr: 0x800,
                len: 4096,
                source_route_token: Some(OwnerSourceRouteToken {
                    key: "key".to_string(),
                    put_id: (4, 2),
                    route_epoch: source_slot.allocation_id,
                    source: source_slot.clone(),
                    atomic_batch: None,
                    plan_nonce: 1,
                }),
                gpu_direct_eligible: true,
                requester_local_borrow_eligible: false,
                error_code: OK,
                ..Default::default()
            }],
            error_code: OK,
            ..Default::default()
        };
        let decoded: BatchGetPlanResp =
            bitcode::decode(&bitcode::encode(&plan)).expect("decode GetPlan response");
        assert_eq!(decoded.items[0].get_id, 0);
        assert!(decoded.items[0].gpu_direct_eligible);
        assert!(!decoded.items[0].requester_local_borrow_eligible);
        assert_eq!(decoded.items[0].src_addr, 0x1000);
        assert_eq!(
            decoded.items[0]
                .source_route_token
                .as_ref()
                .expect("source token must survive GetPlan encoding")
                .source,
            source_slot
        );

        let bind = BatchGetBindReq {
            items: vec![BatchGetBindItemReq {
                get_id: 0,
                target: GetBindTarget::ExternalSink(GetExternalSinkTarget {
                    addr: 0x2000,
                    capacity: 4096,
                    registration_id: 7,
                    requester_node_start_time: 11,
                }),
            }],
        };
        let decoded: BatchGetBindReq =
            bitcode::decode(&bitcode::encode(&bind)).expect("decode GetBind request");
        assert!(matches!(
            &decoded.items[0].target,
            GetBindTarget::ExternalSink(target)
                if decoded.items[0].get_id == 0
                    && target.registration_id == 7
                    && target.requester_node_start_time == 11
        ));

        let late_target = GetBindTarget::ExternalSink(GetExternalSinkTarget {
            addr: 0x2000,
            capacity: 4096,
            registration_id: 7,
            requester_node_start_time: 11,
        });
        let done = BatchGetDoneReq {
            items: vec![BatchGetDoneItemReq {
                get_id: 0,
                late_target: Some(late_target.clone()),
            }],
        };
        let encoded = bitcode::encode(&done);
        let decoded: BatchGetDoneReq =
            bitcode::decode(&encoded).expect("decode late-target GetDone request");
        assert_eq!(decoded.items[0].get_id, 0);
        assert_eq!(decoded.items[0].late_target.as_ref(), Some(&late_target));
        let replayed: BatchGetDoneReq =
            bitcode::decode(&encoded).expect("replay the exact late-target GetDone request");
        assert_eq!(replayed.items[0].get_id, decoded.items[0].get_id);
        assert_eq!(replayed.items[0].late_target, decoded.items[0].late_target);

        let local_borrow = BatchGetBindReq {
            items: vec![BatchGetBindItemReq {
                get_id: 1,
                target: GetBindTarget::RequesterLocalSource,
            }],
        };
        let decoded: BatchGetBindReq = bitcode::decode(&bitcode::encode(&local_borrow))
            .expect("decode requester-local GetBind request");
        assert!(matches!(
            decoded.items[0].target,
            GetBindTarget::RequesterLocalSource
        ));

        let reused = BatchGetStartItemResp {
            get_id: 2,
            src_addr: 0x5000,
            target_addr: 0x5000,
            reused_committed_slot: Some(GetPreparedLocalReserveTarget {
                owner: crate::owner_segment::OwnerGeneration::for_test("requester"),
                allocation_id: 9,
                segment_offset: 0x4000,
                capacity_bytes: 8192,
                addr: 0x5000,
                base_addr: 0x1000,
                len: 4096,
                segment_registration_epoch: 1,
            }),
            ..Default::default()
        };
        let decoded: BatchGetStartItemResp = bitcode::decode(&bitcode::encode(&reused))
            .expect("decode requester-local CommittedSlot response");
        let slot = decoded
            .reused_committed_slot
            .expect("reused slot identity must survive the wire");
        assert_eq!(slot.allocation_id, 9);
        assert_eq!(slot.segment_offset, 0x4000);
        assert_eq!(decoded.src_addr, decoded.target_addr);
    }

    #[test]
    fn owner_plan_materializes_every_caller_owned_target_and_rejects_legacy_sources() {
        let source = OwnerSlotDesc {
            owner: OwnerGeneration::new("source-owner", 13),
            allocation_id: 17,
            segment_offset: 0x2000,
            capacity_bytes: 8192,
            addr: 0x5000,
            base_addr: 0x3000,
            len: 4096,
            segment_registration_epoch: 3,
        };
        let mut plan = BatchGetPlanItemResp {
            get_id: 7,
            node_id: source.owner.node_id.clone(),
            put_id: (4, 2),
            src_addr: source.addr,
            src_base_addr: source.base_addr,
            len: source.len,
            source_kind: GetSourceKind::Memory,
            source_route_token: Some(OwnerSourceRouteToken {
                key: "key".to_string(),
                put_id: (4, 2),
                route_epoch: source.allocation_id,
                source: source.clone(),
                atomic_batch: None,
                plan_nonce: 8,
            }),
            error_code: OK,
            ..Default::default()
        };

        let prepared = GetPreparedLocalReserveTarget {
            owner: OwnerGeneration::new("requester-owner", 19),
            allocation_id: 23,
            segment_offset: 0x6000,
            capacity_bytes: 8192,
            addr: 0xa000,
            base_addr: 0x4000,
            len: plan.len,
            segment_registration_epoch: 5,
        };
        let prepared_start = plan
            .materialize_owner_source_late_target(
                &GetBindTarget::PreparedLocalReserve(prepared.clone()),
                None,
            )
            .expect("owner source must materialize into a prepared owner slot");
        assert_eq!(prepared_start.prepared_target.as_ref(), Some(&prepared));
        assert!(prepared_start.reused_committed_slot.is_none());
        assert_eq!(prepared_start.target_addr, prepared.addr);

        let sink = GetExternalSinkTarget {
            addr: 0xc000,
            capacity: 8192,
            registration_id: 29,
            requester_node_start_time: 31,
        };
        let sink_start = plan
            .materialize_owner_source_late_target(&GetBindTarget::ExternalSink(sink.clone()), None)
            .expect("owner source must materialize into an external sink");
        assert_eq!(sink_start.target_addr, sink.addr);
        assert_eq!(sink_start.target_base_addr, sink.addr);
        assert!(sink_start.prepared_target.is_none());
        assert!(sink_start.reused_committed_slot.is_none());

        plan.requester_local_borrow_eligible = true;
        let reused_start = plan
            .materialize_owner_source_late_target(
                &GetBindTarget::RequesterLocalSource,
                Some(&source.owner),
            )
            .expect("authorized requester-local owner source must materialize in place");
        assert_eq!(reused_start.src_addr, reused_start.target_addr);
        assert_eq!(reused_start.reused_committed_slot.as_ref(), Some(&source));
        assert!(
            plan.materialize_owner_source_late_target(
                &GetBindTarget::RequesterLocalSource,
                Some(&OwnerGeneration::new("source-owner", 14)),
            )
            .is_err(),
            "requester-local reuse must match the current owner generation"
        );

        let mut mismatched = plan.clone();
        mismatched
            .source_route_token
            .as_mut()
            .expect("owner token")
            .plan_nonce += 1;
        assert!(
            mismatched
                .materialize_owner_source_late_target(
                    &GetBindTarget::RequesterLocalSource,
                    Some(&source.owner),
                )
                .is_err(),
            "a mismatched owner capability must fail closed"
        );

        let mut master_allocation = plan.clone();
        master_allocation.source_route_token = None;
        assert!(
            master_allocation
                .materialize_owner_source_late_target(
                    &GetBindTarget::PreparedLocalReserve(prepared.clone()),
                    None
                )
                .is_err(),
            "master Allocation sources must stay on the pre-bound compatibility path"
        );

        let mut ssd = plan;
        ssd.source_kind = GetSourceKind::Ssd;
        ssd.src_addr = 0;
        ssd.src_base_addr = 0;
        ssd.source_route_token = None;
        ssd.ssd_source_route_token = Some(OwnerSsdSourceRouteToken {
            key: "key".to_string(),
            put_id: ssd.put_id,
            owner: source.owner,
            len: ssd.len,
            atomic_batch: None,
            plan_nonce: ssd.get_id + 1,
        });
        assert_eq!(
            ssd.materialize_owner_source_late_target(
                &GetBindTarget::PreparedLocalReserve(prepared.clone()),
                None,
            )
            .expect_err("SSD reads must Bind before starting their source stage"),
            "SSD source requires Bind before staging"
        );
        assert!(
            ssd.materialize_owner_source_late_target(
                &GetBindTarget::RequesterLocalSource,
                Some(&OwnerGeneration::new("source-owner", 13)),
            )
            .is_err(),
            "SSD metadata cannot masquerade as requester-local DRAM"
        );

        let mut changed_ssd = ssd.clone();
        changed_ssd
            .ssd_source_route_token
            .as_mut()
            .expect("SSD token")
            .plan_nonce += 1;
        assert!(
            changed_ssd
                .materialize_owner_source_late_target(
                    &GetBindTarget::PreparedLocalReserve(prepared.clone()),
                    None,
                )
                .is_err(),
            "SSD Plan nonce drift must fail closed"
        );
        let mut changed_ssd = ssd;
        changed_ssd
            .ssd_source_route_token
            .as_mut()
            .expect("SSD token")
            .owner = OwnerGeneration::new("another-owner", 13);
        assert!(
            changed_ssd
                .materialize_owner_source_late_target(
                    &GetBindTarget::PreparedLocalReserve(prepared),
                    None,
                )
                .is_err(),
            "SSD owner drift must fail closed"
        );
    }
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
pub struct BatchGetDoneItemReq {
    pub get_id: u64,
    /// A caller-owned target submitted together with Done. `None` replays the
    /// transitional pre-bound path. External GPU sinks use this field to
    /// remove the separate physical BatchGetBind RTT.
    pub late_target: Option<GetBindTarget>,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchGetDoneReq {
    pub items: Vec<BatchGetDoneItemReq>,
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
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
    },
    /// A master-owned allocation with no owner-side key index.
    ///
    /// SSD-capable owners use this exact source identity to persist the bytes while the
    /// master's route and key-activity fence keep the allocation alive. Owners without SSD
    /// still skip owner coordination and let the master reclaim the allocation directly.
    UnindexedAllocation {
        /// Absolute address in the owner's registered CPU segment.
        addr: u64,
        /// Base address of the exact registered segment generation.
        base_addr: u64,
        /// Logical KV payload length.
        len: u64,
        /// Physical allocator capacity released when the master drops the allocation.
        capacity_bytes: u64,
    },
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerReclaimReason {
    #[default]
    OwnerCapacityEviction,
    MasterAllocationCapacity,
    /// Exact remote-only DRAM source made redundant by a committed local Get target.
    PostReadDuplicate,
}

/// Per-victim SSD action for owner-local capacity reclaim. Selection and
/// deletion remain single-KV decisions even when one RPC carries a vector.
#[derive(Default, Debug, Clone, Copy, Hash, PartialEq, Eq, Encode, Decode)]
pub enum OwnerSourceSsdPolicy {
    /// Delete the exact memory source without preserving it on SSD.
    #[default]
    Drop,
    /// Ask the master to return `SsdCandidate` only if this is the last live
    /// backing. Sources with another backing are deleted immediately.
    SelectLastLive,
    /// The owner has durably persisted this generation and supplied its exact
    /// length in `ssd_backing_len`.
    Persisted,
}

/// One exact owner-local source selected for capacity eviction.
#[derive(Default, Debug, Clone, Hash, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSourceEvictionVictim {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub backing: OwnerReclaimBacking,
    /// The source owner atomically reserved this exact slot's logical
    /// GlobalShared budget before issuing the master RPC.  The reservation is
    /// retained across transport/retry uncertainty and is consumed only when
    /// the owner commits the LocalExclusive -> GlobalShared scope change.
    /// Without it the master may delete the replica or wait for an already
    /// selected GlobalShared victim to become a real owner-side Free, but it
    /// must not publish a new GlobalShared route.
    pub global_demotion_reserved: bool,
    /// Durable owner-local SSD bytes prepared under the exact source fence.
    /// The master installs this backing immediately before deleting `memory`.
    pub ssd_backing_len: Option<u64>,
    pub ssd_policy: OwnerSourceSsdPolicy,
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
    /// Bytes of already-GlobalShared entries that the master must explicitly
    /// select before demoting this batch.  This is bounded by the exact source
    /// victims in the same request and is never physical reclaim credit.
    pub global_reclaim_requested_bytes: u64,
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
    /// The exact physical slot remains live; only its scope changed from
    /// LocalExclusive to GlobalShared.
    DemotedGlobal,
    Completed,
    SsdCandidate,
    RetryableBusy,
    Stale,
    RejectedNotEvictable,
    /// Master proved that this exact source is the last live memory backing,
    /// but the owner did not reserve GlobalShared budget for it.  The owner
    /// must first finish the rest of the batch (releasing speculative
    /// reservations held by direct-delete victims), reserve only these
    /// screened generations, and retry them in one follow-up batch.
    NeedsGlobalReservation,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSourceEvictionVictimResp {
    pub victim_index: u32,
    pub outcome: OwnerSourceEvictionOutcome,
    pub ssd_backing_committed: bool,
    pub detail: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchEvictOwnerSourceResp {
    pub operation_id: u64,
    pub global_reclaim_requested_bytes: u64,
    pub global_reclaim_selected_bytes: u64,
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

/// One durable same-owner SSD generation to publish without removing memory.
#[derive(Default, Debug, Clone, Hash, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSsdPublishItem {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub len: u64,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPublishOwnerSsdReq {
    /// Membership generation of the authenticated owner.
    pub owner_node_start_time: i64,
    pub items: Vec<OwnerSsdPublishItem>,
}

impl MsgPackSerializePart for BatchPublishOwnerSsdReq {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPublishOwnerSsdReq as u32
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum OwnerSsdPublishOutcome {
    #[default]
    Unspecified,
    Published,
    AlreadyPresent,
    RetryableBusy,
    Obsolete,
    Rejected,
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OwnerSsdPublishItemResp {
    pub key: String,
    pub put_id: PutIDForAKey,
    pub outcome: OwnerSsdPublishOutcome,
    pub detail: String,
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct BatchPublishOwnerSsdResp {
    pub items: Vec<OwnerSsdPublishItemResp>,
    pub error_code: ErrorCode,
    pub error_json: String,
}

impl MsgPackSerializePart for BatchPublishOwnerSsdResp {
    fn msg_id(&self) -> u32 {
        MsgId::BatchPublishOwnerSsdResp as u32
    }
}

impl RPCReq for BatchPublishOwnerSsdReq {
    type Resp = BatchPublishOwnerSsdResp;
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
    /// Durable bytes persisted by the owner while the exact memory source is
    /// hidden behind the Prepare fence.  The master publishes this backing on
    /// the existing route before it asks the owner to Commit/free DRAM.
    pub ssd_backing_len: Option<u64>,
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

#[derive(
    Default, Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, serde::Serialize, serde::Deserialize,
)]
pub enum OwnerLocalReserveControlOp {
    #[default]
    Get,
    SetLocalTarget {
        controller_epoch: u64,
        local_target_bytes: u64,
    },
}

#[derive(Default, Debug, Clone, Encode, Decode, serde::Serialize, serde::Deserialize)]
pub struct OwnerLocalReserveControlReq {
    pub expected_owner_node_start_time: i64,
    pub operation: OwnerLocalReserveControlOp,
}
impl MsgPackSerializePart for OwnerLocalReserveControlReq {
    fn msg_id(&self) -> u32 {
        MsgId::OwnerLocalReserveControlReq as u32
    }
}

#[derive(Default, Debug, Clone, Encode, Decode, serde::Serialize, serde::Deserialize)]
pub struct OwnerLocalReserveControlResp {
    pub owner_node_start_time: i64,
    pub controller_epoch: u64,
    pub physical_capacity_bytes: u64,
    pub local_target_bytes: u64,
    pub global_target_bytes: u64,
    pub allocated_bytes: u64,
    pub free_bytes: u64,
    pub applied_moka_bytes: u64,
    pub moka_weighted_bytes: u64,
    pub settled: bool,
    pub error_code: ErrorCode,
    pub error_json: String,
    pub server_process_us: i64,
}
impl MsgPackSerializePart for OwnerLocalReserveControlResp {
    fn msg_id(&self) -> u32 {
        MsgId::OwnerLocalReserveControlResp as u32
    }
}
impl RPCReq for OwnerLocalReserveControlReq {
    type Resp = OwnerLocalReserveControlResp;
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

/// The committed target returned by an owner is the same generation-safe
/// descriptor used by the owner allocator and master route.
pub type PutDoneCommittedSlot = crate::owner_segment::OwnerSlotDesc;

/// Exact prefix dependency for one immutable KV page.
///
/// `depth == 0` denotes a direct child of the Radix root and requires
/// `parent_key == None`. Every deeper page names its exact parent. This
/// metadata does not change the KV key, atomic group, or capacity-victim
/// boundary; the first implementation only observes prefix-closure waste.
#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RadixKvMetadata {
    pub parent_key: Option<String>,
    pub depth: u32,
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
    /// Optional exact Radix lineage for this immutable key.
    pub radix: Option<RadixKvMetadata>,
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
    pub radix: Option<RadixKvMetadata>,
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
    pub radix: Option<RadixKvMetadata>,
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
    /// Ordered owner generations. These are placement hints; each target
    /// performs the authoritative PrepareTarget claim.
    pub owner_candidates: Vec<OwnerTargetRouteToken>,
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
    pub owner_candidates: Vec<OwnerTargetRouteToken>,
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
    /// Present only for the owner-authoritative direct-transfer path.
    pub committed_slot: Option<OwnerSlotDesc>,
    pub route_token: Option<OwnerTargetRouteToken>,
}
impl MsgPackSerializePart for PutAppendDoneReq {
    fn msg_id(&self) -> u32 {
        MsgId::PutAppendDoneReq as u32
    }
}
#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct PutAppendDoneResp {
    pub appended: bool,
    pub route_epoch: u64,
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
    pub committed_slot: Option<OwnerSlotDesc>,
    pub route_token: Option<OwnerTargetRouteToken>,
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
    pub route_epoch: u64,
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
                radix: Some(RadixKvMetadata {
                    parent_key: None,
                    depth: 0,
                }),
            }],
        };
        let decoded: BatchPutDoneReq =
            bitcode::decode(&bitcode::encode(&req)).expect("decode atomic put group");
        assert_eq!(decoded.items[0].atomic_group.as_ref(), Some(&group));
        assert_eq!(decoded.items[0].radix, req.items[0].radix);
    }

    #[test]
    fn owner_ssd_publish_only_batch_round_trips_exact_generation() {
        let req = BatchPublishOwnerSsdReq {
            owner_node_start_time: 41,
            items: vec![OwnerSsdPublishItem {
                key: "ssd-key".to_string(),
                put_id: (17, 3),
                len: 4_718_592,
            }],
        };
        let decoded: BatchPublishOwnerSsdReq =
            bitcode::decode(&bitcode::encode(&req)).expect("decode owner SSD publication");
        assert_eq!(decoded.owner_node_start_time, 41);
        assert_eq!(decoded.items, req.items);
    }

    #[test]
    fn owner_scope_budget_control_round_trips_epoch_target_and_snapshot() {
        let request = OwnerLocalReserveControlReq {
            expected_owner_node_start_time: 41,
            operation: OwnerLocalReserveControlOp::SetLocalTarget {
                controller_epoch: 7,
                local_target_bytes: 96 * 1024 * 1024 * 1024,
            },
        };
        let decoded: OwnerLocalReserveControlReq = bitcode::decode(&bitcode::encode(&request))
            .expect("decode owner local-reserve control request");
        assert_eq!(decoded.expected_owner_node_start_time, 41);
        assert_eq!(decoded.operation, request.operation);

        let response = OwnerLocalReserveControlResp {
            owner_node_start_time: 41,
            controller_epoch: 7,
            physical_capacity_bytes: 128 * 1024 * 1024 * 1024,
            local_target_bytes: 96 * 1024 * 1024 * 1024,
            global_target_bytes: 32 * 1024 * 1024 * 1024,
            allocated_bytes: 80 * 1024 * 1024 * 1024,
            free_bytes: 48 * 1024 * 1024 * 1024,
            ..Default::default()
        };
        let decoded: OwnerLocalReserveControlResp = bitcode::decode(&bitcode::encode(&response))
            .expect("decode owner local-reserve control response");
        assert_eq!(decoded.owner_node_start_time, 41);
        assert_eq!(decoded.controller_epoch, 7);
        assert_eq!(decoded.physical_capacity_bytes, 128 * 1024 * 1024 * 1024);
        assert_eq!(decoded.local_target_bytes, 96 * 1024 * 1024 * 1024);
        assert_eq!(decoded.global_target_bytes, 32 * 1024 * 1024 * 1024);
        assert_eq!(decoded.allocated_bytes, 80 * 1024 * 1024 * 1024);
        assert_eq!(decoded.free_bytes, 48 * 1024 * 1024 * 1024);
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
                    radix: None,
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
                    radix: None,
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
