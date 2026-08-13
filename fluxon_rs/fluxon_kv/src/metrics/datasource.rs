use std::collections::HashMap;

use crate::master_kv_router::MasterKvRouterView;
use crate::master_kv_router::msg_pack::{GetMasterOnlyMetricPartReq, GetMasterOnlyMetricPartResp};
use crate::p2p::control_plane_rpc::send_control_plane_rpc_response;
use crate::p2p::msg_pack::{MsgPack, RPCHandler};
use crate::rpcresp_kvresult_convert::msg_and_error::KvError;
use crate::rpcresp_kvresult_convert::{self};

/// Register RPC handler on master to serve master-only metric parts.
pub fn register_master_only_metric_handler(view: &MasterKvRouterView) {
    let p2p = view.p2p_module();
    let view = view.clone();
    RPCHandler::<GetMasterOnlyMetricPartReq>::new().regist(p2p, move |resp, msg| {
        let view_task = view.clone();
        let _ = view.spawn("rpc_get_master_only_metric_part", async move {
            let ack = handle_get_master_only_metric_part(&view_task, msg).await;
            let _ = send_control_plane_rpc_response(&resp, ack).await;
        });
        Ok(())
    });
}

async fn handle_get_master_only_metric_part(
    view: &MasterKvRouterView,
    msg: MsgPack<GetMasterOnlyMetricPartReq>,
) -> MsgPack<GetMasterOnlyMetricPartResp> {
    // Only 'segment_bytes' supported for now
    if msg.serialize_part.part != "segment_bytes" {
        let err = KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::Unknown {
                detail: format!("unsupported metric part: {}", msg.serialize_part.part),
            },
        );
        return MsgPack {
            serialize_part: GetMasterOnlyMetricPartResp {
                seg_bytes_map: Default::default(),
                error_code: err.code(),
                error_json: err.to_json(),
            },
            raw_bytes: Vec::new(),
        };
    }

    // Read from MasterSegManager directly instead of scraping gauges
    let segs = view.master_seg_manager().get_all_segments_allocator();
    let mut map: HashMap<String, (u64, u64)> = HashMap::new();
    for (node_id, allocator) in segs.into_iter() {
        let total = allocator.total_size_bytes();
        let used = allocator.used_size_bytes();
        let available = total.saturating_sub(used);
        map.insert(
            format!("{}:{}", node_id, allocator.seg_device_id),
            (available, total),
        );
    }
    MsgPack {
        serialize_part: GetMasterOnlyMetricPartResp {
            seg_bytes_map: map,
            error_code: rpcresp_kvresult_convert::msg_and_error::OK,
            error_json: String::new(),
        },
        raw_bytes: Vec::new(),
    }
}
