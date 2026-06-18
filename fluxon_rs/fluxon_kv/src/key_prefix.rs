use std::time::Duration;

use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_kv_router::msg_pack::CountPrefixReq;
use crate::p2p::msg_pack::{MsgPack, RPCCaller};
use crate::p2p::p2p_module::P2pModule;
use crate::rpcresp_kvresult_convert;
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, KvResult};

/// Helper for counting keys by prefix via master node.
///
/// This is shared by client/external roles and uses the master-side
/// prefix index maintained in `MasterKvRouter`.
///
/// The index is derived asynchronously from `kv_routes`, so CountPrefix is
/// intended for aggregate prefix counting rather than as an immediate
/// strong-consistency visibility probe for a just-committed put.
pub async fn count_prefix_for_framework(fw: &crate::Framework, prefix: &str) -> KvResult<u64> {
    // Locate master
    let master_node_id = fw
        .cluster_manager_view()
        .cluster_manager()
        .find_or_wait_master_node()
        .await?;

    let req = MsgPack {
        serialize_part: CountPrefixReq {
            prefix: prefix.to_string(),
        },
        raw_bytes: Vec::new(),
    };

    let caller = RPCCaller::<CountPrefixReq>::new();
    let resp = caller
        .call(
            fw.p2p_view().p2p_module(),
            master_node_id.into(),
            req,
            Some(Duration::from_secs(60)),
            0,
        )
        .await
        .map_err(KvError::from)?;

    if let Err(e) = rpcresp_kvresult_convert::try_from_code(
        resp.serialize_part.error_code,
        resp.serialize_part.error_json.clone(),
    ) {
        return Err(e);
    }

    Ok(resp.serialize_part.count)
}

/// Register CountPrefix RPC caller for any module that has p2p access.
///
/// Owner(client) 与 external 模式都调用本函数完成 CountPrefix 的
/// RPC caller 注册，避免在多个模块里重复写注册逻辑。
pub fn init_for_p2p_owner(p2p: &P2pModule) {
    RPCCaller::<CountPrefixReq>::new().regist(p2p);
}
