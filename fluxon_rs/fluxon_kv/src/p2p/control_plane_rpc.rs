use crate::cluster_manager::NodeID;
use crate::master_kv_router::msg_pack::{
    BatchDeleteAckReq, BatchDeleteClientKvMetaCacheReq, BatchEnqueueReplicaTaskReq,
    BatchEvictOwnerSourceReq, BatchGetBindReq, BatchGetDoneReq, BatchGetPlanReq, BatchGetRevokeReq,
    BatchGetStartReq, BatchIsExistReq, BatchOwnerReclaimReq, BatchPreparePutKeysReq,
    BatchPublishOwnerSsdReq, BatchPutAppendDoneReq, BatchPutAppendStartReq, BatchPutDoneReq,
    BatchPutRevokeReq, BatchPutStartReq, BatchReleasePutKeyReservationsReq, CountPrefixReq,
    DeleteAckReq, DeleteReq, GetDoneReq, GetMasterOnlyMetricPartReq, GetMetaReq, GetRevokeReq,
    GetStartReq, GroupedBatchPutDoneReq, MemHolderKeepAliveReq, MemHolderReleaseReq,
    PutAppendDoneReq, PutAppendRevokeReq, PutAppendStartReq, PutDoneReq, PutRevokeReq, PutStartReq,
    ReleaseLocalGrantReq, ReserveLocalGrantReq, SsdStageBeginReq, SsdStageDoneReq,
};
use crate::master_seg_manager::msg_pack::RequestSegmentRegistrationReq;
use crate::p2p::P2PResult;
use crate::p2p::msg_pack::{MsgPack, RPCCaller, RPCReq, RPCResponsor};
use crate::p2p::p2p_module::{P2pModule, RpcTransportPolicy};
use std::time::Duration;

pub(crate) const CONTROL_PLANE_RPC_TRANSPORT_POLICY: RpcTransportPolicy =
    RpcTransportPolicy::ForceTransport;

/// RPCs whose request and response are metadata-only control traffic.
///
/// The master has a control-only transfer runtime. Letting these messages use the optional
/// transfer-RPC fast path makes the two directions depend on endpoint-specific segment state and
/// has repeatedly produced asymmetric cross-node timeouts. Keeping the request types in one
/// marker list makes both directions share one transport decision.
pub(crate) trait ControlPlaneRpcReq: RPCReq {}

macro_rules! impl_control_plane_rpc_req {
    ($($req:ty),+ $(,)?) => {
        $(impl ControlPlaneRpcReq for $req {})+
    };
}

impl_control_plane_rpc_req!(
    RequestSegmentRegistrationReq,
    GetStartReq,
    GetRevokeReq,
    GetDoneReq,
    SsdStageBeginReq,
    SsdStageDoneReq,
    BatchGetStartReq,
    BatchGetPlanReq,
    BatchGetBindReq,
    BatchGetRevokeReq,
    BatchGetDoneReq,
    CountPrefixReq,
    GetMasterOnlyMetricPartReq,
    ReserveLocalGrantReq,
    ReleaseLocalGrantReq,
    BatchPreparePutKeysReq,
    BatchReleasePutKeyReservationsReq,
    BatchEvictOwnerSourceReq,
    BatchPublishOwnerSsdReq,
    BatchOwnerReclaimReq,
    BatchEnqueueReplicaTaskReq,
    BatchDeleteClientKvMetaCacheReq,
    PutStartReq,
    PutRevokeReq,
    PutDoneReq,
    BatchPutStartReq,
    BatchPutRevokeReq,
    BatchPutDoneReq,
    GroupedBatchPutDoneReq,
    PutAppendStartReq,
    BatchPutAppendStartReq,
    PutAppendRevokeReq,
    PutAppendDoneReq,
    BatchPutAppendDoneReq,
    MemHolderKeepAliveReq,
    MemHolderReleaseReq,
    DeleteReq,
    DeleteAckReq,
    BatchDeleteAckReq,
    GetMetaReq,
    BatchIsExistReq,
);

pub(crate) async fn call_control_plane_rpc<R: ControlPlaneRpcReq>(
    caller: &RPCCaller<R>,
    p2p: &P2pModule,
    node_id: NodeID,
    request: MsgPack<R>,
    timeout: Option<Duration>,
    retry: usize,
) -> P2PResult<MsgPack<R::Resp>> {
    caller
        .call_with_transport_policy(
            p2p,
            node_id,
            request,
            timeout,
            CONTROL_PLANE_RPC_TRANSPORT_POLICY,
            retry,
        )
        .await
}

pub(crate) async fn send_control_plane_rpc_response<R: ControlPlaneRpcReq>(
    responder: &RPCResponsor<R>,
    response: MsgPack<R::Resp>,
) -> P2PResult<()> {
    responder
        .send_resp_with_transport_policy(response, CONTROL_PLANE_RPC_TRANSPORT_POLICY)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_control_plane_rpc<R: ControlPlaneRpcReq>() {}

    #[test]
    fn high_volume_master_control_rpcs_share_one_force_transport_policy() {
        assert_eq!(
            CONTROL_PLANE_RPC_TRANSPORT_POLICY,
            RpcTransportPolicy::ForceTransport
        );
        assert_control_plane_rpc::<RequestSegmentRegistrationReq>();
        assert_control_plane_rpc::<GetStartReq>();
        assert_control_plane_rpc::<GetRevokeReq>();
        assert_control_plane_rpc::<GetDoneReq>();
        assert_control_plane_rpc::<SsdStageBeginReq>();
        assert_control_plane_rpc::<SsdStageDoneReq>();
        assert_control_plane_rpc::<BatchGetStartReq>();
        assert_control_plane_rpc::<BatchGetPlanReq>();
        assert_control_plane_rpc::<BatchGetBindReq>();
        assert_control_plane_rpc::<BatchGetRevokeReq>();
        assert_control_plane_rpc::<BatchGetDoneReq>();
        assert_control_plane_rpc::<CountPrefixReq>();
        assert_control_plane_rpc::<GetMasterOnlyMetricPartReq>();
        assert_control_plane_rpc::<ReserveLocalGrantReq>();
        assert_control_plane_rpc::<ReleaseLocalGrantReq>();
        assert_control_plane_rpc::<BatchPreparePutKeysReq>();
        assert_control_plane_rpc::<BatchReleasePutKeyReservationsReq>();
        assert_control_plane_rpc::<BatchEvictOwnerSourceReq>();
        assert_control_plane_rpc::<BatchPublishOwnerSsdReq>();
        assert_control_plane_rpc::<BatchOwnerReclaimReq>();
        assert_control_plane_rpc::<BatchEnqueueReplicaTaskReq>();
        assert_control_plane_rpc::<BatchDeleteClientKvMetaCacheReq>();
        assert_control_plane_rpc::<PutStartReq>();
        assert_control_plane_rpc::<PutRevokeReq>();
        assert_control_plane_rpc::<PutDoneReq>();
        assert_control_plane_rpc::<BatchPutStartReq>();
        assert_control_plane_rpc::<BatchPutRevokeReq>();
        assert_control_plane_rpc::<BatchPutDoneReq>();
        assert_control_plane_rpc::<GroupedBatchPutDoneReq>();
        assert_control_plane_rpc::<PutAppendStartReq>();
        assert_control_plane_rpc::<BatchPutAppendStartReq>();
        assert_control_plane_rpc::<PutAppendRevokeReq>();
        assert_control_plane_rpc::<PutAppendDoneReq>();
        assert_control_plane_rpc::<BatchPutAppendDoneReq>();
        assert_control_plane_rpc::<MemHolderKeepAliveReq>();
        assert_control_plane_rpc::<MemHolderReleaseReq>();
        assert_control_plane_rpc::<DeleteReq>();
        assert_control_plane_rpc::<DeleteAckReq>();
        assert_control_plane_rpc::<BatchDeleteAckReq>();
        assert_control_plane_rpc::<GetMetaReq>();
        assert_control_plane_rpc::<BatchIsExistReq>();
    }
}
