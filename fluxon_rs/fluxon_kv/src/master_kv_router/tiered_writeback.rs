use super::{MasterKvRouterView, NodeValueReplicaDesc};
use crate::cluster_manager::{NodeID, NodeIDString};
use crate::master_kv_router::msg_pack::{BatchEnqueueReplicaTaskReq, EnqueueReplicaTaskItem};
use crate::p2p::msg_pack::{MIN_EXPLICIT_RPC_TIMEOUT_SECS, MsgPack, RPCCaller};
use crate::rpcresp_kvresult_convert::msg_and_error::OK;
use limit_thirdparty::tokio;
use std::collections::HashMap;
use std::time::Duration;

const TIER1_WRITEBACK_RPC_TIMEOUT: Duration = Duration::from_secs(MIN_EXPLICIT_RPC_TIMEOUT_SECS);
const TIER1_WRITEBACK_MAX_BATCH: usize = 256;
const TIER1_WRITEBACK_MERGE_WINDOW: Duration = Duration::from_millis(2);

#[derive(Clone, Debug)]
pub(crate) struct Tier1WritebackRequest {
    pub source_node_id: NodeIDString,
    pub key: String,
    pub desc: NodeValueReplicaDesc,
}

async fn dispatch_owner_batch(
    view: &MasterKvRouterView,
    source_node_id: NodeIDString,
    requests: Vec<Tier1WritebackRequest>,
) {
    let mut current = Vec::with_capacity(requests.len());
    for request in requests {
        if view.master_kv_router().tier1_writeback_entry_is_current(
            &request.source_node_id,
            &request.key,
            &request.desc,
        ) {
            current.push(request);
        } else {
            view.master_kv_router()
                .finish_tier1_writeback_request(request);
        }
    }
    if current.is_empty() {
        return;
    }

    let items = current
        .iter()
        .map(|request| EnqueueReplicaTaskItem {
            key: request.key.clone(),
            put_id: request.desc.put_id,
        })
        .collect::<Vec<_>>();
    let caller = RPCCaller::<BatchEnqueueReplicaTaskReq>::new();
    caller.regist(view.p2p_module());
    let result = caller
        .call(
            view.p2p_module(),
            NodeID::from(source_node_id.clone()),
            MsgPack {
                serialize_part: BatchEnqueueReplicaTaskReq { items },
                raw_bytes: Vec::new(),
            },
            Some(TIER1_WRITEBACK_RPC_TIMEOUT),
            1,
        )
        .await;

    let response = match result {
        Ok(response)
            if response.serialize_part.error_code == OK
                && response.serialize_part.items.len() == current.len() =>
        {
            response.serialize_part.items
        }
        Ok(response) => {
            tracing::warn!(
                "tier1 write-back owner response rejected: owner={} requested={} returned={} code={} error={}",
                source_node_id,
                current.len(),
                response.serialize_part.items.len(),
                response.serialize_part.error_code,
                response.serialize_part.error_json
            );
            view.master_kv_router().record_tier1_writeback_failed(
                &source_node_id,
                u64::try_from(current.len()).unwrap_or(u64::MAX),
            );
            for request in current {
                view.master_kv_router()
                    .finish_tier1_writeback_request(request);
            }
            return;
        }
        Err(err) => {
            tracing::warn!(
                "tier1 write-back owner RPC failed: owner={} requested={} err={:?}",
                source_node_id,
                current.len(),
                err
            );
            view.master_kv_router().record_tier1_writeback_failed(
                &source_node_id,
                u64::try_from(current.len()).unwrap_or(u64::MAX),
            );
            for request in current {
                view.master_kv_router()
                    .finish_tier1_writeback_request(request);
            }
            return;
        }
    };

    let requested = current.len();
    let mut accepted = 0usize;
    for (request, item) in current.into_iter().zip(response.into_iter()) {
        let identity_matches = request.key == item.key && request.desc.put_id == item.put_id;
        if identity_matches && item.accepted {
            accepted += 1;
            continue;
        }
        view.master_kv_router()
            .record_tier1_writeback_failed(&source_node_id, 1);
        if !identity_matches {
            tracing::warn!(
                "tier1 write-back owner response identity mismatch: owner={} request_key={} request_put_id=({},{}) response_key={} response_put_id=({},{})",
                source_node_id,
                request.key,
                request.desc.put_id.0,
                request.desc.put_id.1,
                item.key,
                item.put_id.0,
                item.put_id.1
            );
        }
        view.master_kv_router()
            .finish_tier1_writeback_request(request);
    }
    view.master_kv_router()
        .record_tier1_writeback_owner_accepted(
            &source_node_id,
            u64::try_from(accepted).unwrap_or(u64::MAX),
        );
    tracing::debug!(
        "tier1 write-back owner batch dispatched: owner={} requested={} accepted={}",
        source_node_id,
        requested,
        accepted
    );
}

pub(crate) fn spawn_tier1_writeback_actor(
    view: MasterKvRouterView,
    mut rx: tokio::sync::ampsc::Receiver<Tier1WritebackRequest>,
) {
    let view_task = view.clone();
    let _ = view.spawn("tier1_writeback_actor", async move {
        let mut shutdown_waiter = view_task.register_shutdown_waiter();
        loop {
            let first = tokio::select! {
                _ = shutdown_waiter.wait() => break,
                request = rx.recv() => {
                    let Some(request) = request else { break; };
                    request
                }
            };
            let mut batch = Vec::with_capacity(TIER1_WRITEBACK_MAX_BATCH);
            batch.push(first);
            let mut merge_window = Box::pin(tokio::time::sleep(TIER1_WRITEBACK_MERGE_WINDOW));
            while batch.len() < TIER1_WRITEBACK_MAX_BATCH {
                tokio::select! {
                    _ = &mut merge_window => break,
                    request = rx.recv() => {
                        let Some(request) = request else { break; };
                        batch.push(request);
                    }
                }
            }

            let mut groups: HashMap<NodeIDString, Vec<Tier1WritebackRequest>> = HashMap::new();
            for request in batch {
                groups
                    .entry(request.source_node_id.clone())
                    .or_default()
                    .push(request);
            }
            for (source_node_id, requests) in groups {
                dispatch_owner_batch(&view_task, source_node_id, requests).await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::msg_pack::validate_explicit_rpc_timeout;

    #[test]
    fn tier1_writeback_timeout_satisfies_rpc_contract() {
        validate_explicit_rpc_timeout(Some(TIER1_WRITEBACK_RPC_TIMEOUT)).unwrap();
    }

    #[test]
    fn inclusive_hot_tier_does_not_reduce_resident_capacity() {
        let build = |capacity| {
            moka::sync::SegmentedCache::builder(1)
                .max_capacity(capacity)
                .weigher(Box::new(|_key: &String, weight: &u32| *weight))
                .build()
        };
        let resident = build(100);
        let tier1 = build(60);

        for key in ["a", "b", "c"] {
            resident.insert(key.to_string(), 30);
            tier1.insert(key.to_string(), 30);
        }
        resident.run_pending_tasks();
        tier1.run_pending_tasks();

        assert_eq!(resident.policy().max_capacity(), Some(100));
        assert_eq!(resident.weighted_size(), 90);
        assert_eq!(tier1.policy().max_capacity(), Some(60));
        assert!(tier1.weighted_size() <= 60);
        assert!(resident.entry_count() > tier1.entry_count());
    }
}
