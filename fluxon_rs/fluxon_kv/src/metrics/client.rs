use std::collections::HashMap;
use std::time::Duration;

use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_kv_router::msg_pack::{GetMasterOnlyMetricPartReq, GetMasterOnlyMetricPartResp};
use crate::p2p::control_plane_rpc::call_control_plane_rpc;
use crate::p2p::msg_pack::{MsgPack, RPCCaller};
use crate::p2p::p2p_module::P2pModule;
use crate::rpcresp_kvresult_convert::msg_and_error::KvError;
use crate::rpcresp_kvresult_convert::msg_and_error::KvResult;
use crate::rpcresp_kvresult_convert::{self};

/// Register RPC caller for owner/external clients (p2p owner)
pub fn init_for_p2p_owner(p2p: &P2pModule) {
    RPCCaller::<GetMasterOnlyMetricPartReq>::new().regist(p2p);
}

// Master-only datasource handler moved to metrics/datasource.rs

/// Client-side helper to query the master for an authoritative metric part.
pub async fn get_master_only_metric_map(
    fw: &crate::Framework,
    part: &str,
) -> KvResult<HashMap<String, (u64, u64)>> {
    let master_node_id = fw
        .cluster_manager_view()
        .cluster_manager()
        .find_or_wait_master_node()
        .await?;

    let req = GetMasterOnlyMetricPartReq {
        part: part.to_string(),
    };
    let caller = RPCCaller::<GetMasterOnlyMetricPartReq>::new();
    let resp: MsgPack<GetMasterOnlyMetricPartResp> = call_control_plane_rpc(
        &caller,
        fw.p2p_view().p2p_module(),
        master_node_id.into(),
        MsgPack {
            serialize_part: req,
            raw_bytes: Vec::new(),
        },
        Some(Duration::from_secs(30)),
        0,
    )
    .await
    .map_err(KvError::from)?;

    rpcresp_kvresult_convert::try_from_code(
        resp.serialize_part.error_code,
        resp.serialize_part.error_json.clone(),
    )?;
    Ok(resp.serialize_part.seg_bytes_map)
}
