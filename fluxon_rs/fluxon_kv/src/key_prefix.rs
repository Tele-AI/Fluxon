use std::time::Duration;

use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::master_kv_router::msg_pack::CountPrefixReq;
use crate::p2p::msg_pack::{MIN_EXPLICIT_RPC_TIMEOUT_SECS, MsgPack, RPCCaller};
use crate::p2p::p2p_module::P2pModule;
use crate::rpcresp_kvresult_convert;
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, KvResult, P2pError};

// CountPrefix runs synchronously in MQ capacity checks. Bound both blocking
// control steps so their combined maximum stays below MQ worker shutdown grace.
const COUNT_PREFIX_MASTER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(4);
const COUNT_PREFIX_RPC_TIMEOUT: Duration = Duration::from_secs(MIN_EXPLICIT_RPC_TIMEOUT_SECS);

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
    let master_node_id = match limit_thirdparty::tokio::time::timeout(
        COUNT_PREFIX_MASTER_LOOKUP_TIMEOUT,
        fw.cluster_manager_view()
            .cluster_manager()
            .find_or_wait_master_node(),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            return Err(KvError::P2p(P2pError::Timeout {
                detail: format!(
                    "CountPrefix master lookup timed out after {} ms",
                    COUNT_PREFIX_MASTER_LOOKUP_TIMEOUT.as_millis()
                ),
            }));
        }
    };

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
            Some(COUNT_PREFIX_RPC_TIMEOUT),
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
/// Owner-client and external modes share this registration path so the
/// CountPrefix RPC caller has one canonical setup point.
pub fn init_for_p2p_owner(p2p: &P2pModule) {
    RPCCaller::<CountPrefixReq>::new().regist(p2p);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_prefix_rpc_timeout_respects_p2p_contract() {
        assert!(COUNT_PREFIX_RPC_TIMEOUT >= Duration::from_secs(MIN_EXPLICIT_RPC_TIMEOUT_SECS));
    }

    #[test]
    fn count_prefix_total_timeout_stays_below_worker_shutdown_grace() {
        let worker_shutdown_grace = Duration::from_secs(15);
        assert!(
            COUNT_PREFIX_MASTER_LOOKUP_TIMEOUT + COUNT_PREFIX_RPC_TIMEOUT < worker_shutdown_grace
        );
    }
}
