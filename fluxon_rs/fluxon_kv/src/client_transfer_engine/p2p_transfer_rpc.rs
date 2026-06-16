use super::*;
use crate::client_seg_pool::ClientCpuMemReadGuard;
use crate::p2p::msg_pack::{MsgPack, RPCCaller, RPCHandler};
use crate::p2p::p2p_module::RpcTransportPolicy;
use crate::rpcresp_kvresult_convert::msg_and_error::OK;
use fluxon_commu::{
    P2pRawMemReadHandler, P2pRawMemWriteHandler, RawMemReadReq, RawMemReadRespWire, RawMemWriteReq,
    RawMemWriteRespWire,
};
use prost::bytes::Bytes;
use std::pin::Pin;
use std::time::Instant;

fn duration_to_i64_us(duration: std::time::Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

pub fn register_transfer_rpc(view: &ClientTransferEngineView) {
    register_raw_mem_callers(view);

    let view_read = view.clone();
    register_raw_mem_read_handler(
        view,
        std::sync::Arc::new(move |req| {
            let view = view_read.clone();
            Box::pin(async move { handle_raw_mem_read(&view, req).await })
        }),
    );

    let view_write = view.clone();
    register_raw_mem_write_handler(
        view,
        std::sync::Arc::new(move |req, payload| {
            let view = view_write.clone();
            Box::pin(async move { handle_raw_mem_write(&view, req, payload).await })
        }),
    );
}

pub async fn ensure_local_segment_guard(
    view: &ClientTransferEngineView,
    local_addr: u64,
    seg_guard: Option<ClientCpuMemReadGuard>,
) -> Result<ClientCpuMemReadGuard, String> {
    match seg_guard {
        Some(guard) => {
            validate_guard_covers_addr(&guard, local_addr)?;
            Ok(guard)
        }
        None => view
            .client_seg_pool()
            .get_guard_of_address(local_addr)
            .await
            .map_err(|e| e.to_string()),
    }
}

pub async fn p2p_read_to_local(
    view: &ClientTransferEngineView,
    peer: NodeIDString,
    remote_src: u64,
    local_target: u64,
    len: u64,
    seg_guard: ClientCpuMemReadGuard,
) -> Result<(), String> {
    if len == 0 {
        return Ok(());
    }

    let bytes = call_raw_mem_read(
        view,
        peer.clone(),
        RawMemReadReq {
            src_addr: remote_src,
            len,
        },
    )
    .await
    .map_err(|e| e.to_string())?;

    let len_usize = u64_to_usize(len)?;
    if bytes.len() != len_usize {
        return Err(format!(
            "p2p_read_to_local: payload length mismatch: expected={}, actual={}",
            len,
            bytes.len()
        ));
    }

    seg_guard.validate_layout("p2p_read_to_local")?;
    if !seg_guard.contains_rw(local_target, len) {
        return Err(format!(
            "p2p_read_to_local: local_target not in local RW segment: local_target={:#x}, len={}, {}",
            local_target,
            len,
            segment_ranges(&seg_guard)?
        ));
    }

    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ref().as_ptr(),
            local_target as *mut u8,
            bytes.len(),
        );
    }
    let copied_prefix_len = std::cmp::min(8, bytes.len());
    tracing::info!(
        "p2p_read_to_local copied: peer={} remote_src={:#x} local_target={:#x} len={} prefix={:?}",
        peer,
        remote_src,
        local_target,
        len,
        &bytes[..copied_prefix_len]
    );
    Ok(())
}

pub async fn p2p_write_from_local(
    view: &ClientTransferEngineView,
    peer: NodeIDString,
    local_src: u64,
    remote_target: u64,
    len: u64,
    copy_from: Option<Pin<&[u8]>>,
    seg_guard: ClientCpuMemReadGuard,
) -> Result<(), String> {
    if len == 0 {
        return Ok(());
    }

    let payload = if let Some(p) = copy_from.as_ref() {
        Bytes::copy_from_slice(p.get_ref())
    } else {
        let len_usize = u64_to_usize(len)?;
        seg_guard.validate_layout("p2p_write_from_local")?;
        if !seg_guard.contains_rw_or_ro(local_src, len) {
            return Err(format!(
                "p2p_write_from_local: local_src not in local segment: local_src={:#x}, len={}, {}",
                local_src,
                len,
                segment_ranges(&seg_guard)?
            ));
        }
        // Snapshot the current payload before issuing the async RPC send. The
        // shared segment is aggressively reused by concurrent put/get traffic,
        // so borrowing it as a zero-copy network buffer can race with later
        // writes and corrupt the remote value.
        let bytes = unsafe { std::slice::from_raw_parts(local_src as *const u8, len_usize) };
        Bytes::copy_from_slice(bytes)
    };

    call_raw_mem_write(
        view,
        peer,
        RawMemWriteReq {
            target_addr: remote_target,
            len,
        },
        payload,
    )
    .await
    .map_err(|e| e.to_string())
}

pub fn register_raw_mem_callers(view: &ClientTransferEngineView) {
    RPCCaller::<RawMemReadReq>::new().regist(view.p2p_module());
    RPCCaller::<RawMemWriteReq>::new().regist(view.p2p_module());
}

pub fn register_raw_mem_read_handler(
    view: &ClientTransferEngineView,
    handler: P2pRawMemReadHandler,
) {
    let view_read = view.clone();
    RPCHandler::<RawMemReadReq>::new().regist(view.p2p_module(), move |resp, msg| {
        let view_task = view_read.clone();
        let handler = handler.clone();
        let _ = view_read.spawn("p2p_raw_read", async move {
            let req = msg.serialize_part;
            let resp_pack = match handler(req).await {
                Ok(bytes) => MsgPack {
                    serialize_part: RawMemReadRespWire {
                        error_code: OK,
                        error_json: String::new(),
                    },
                    raw_bytes: vec![Bytes::from(bytes)],
                },
                Err(err) => MsgPack {
                    serialize_part: RawMemReadRespWire {
                        error_code: crate::rpcresp_kvresult_convert::msg_and_error::codes_p2p_transfer::P2P_TRANSFER_INVALID_ARG,
                        error_json: serde_json::to_string(
                            &crate::rpcresp_kvresult_convert::msg_and_error::P2pTransferError::InvalidArg {
                                detail: err,
                            },
                        )
                        .unwrap(),
                    },
                    raw_bytes: Vec::new(),
                },
            };
            if let Err(err) = resp
                .send_resp_with_transport_policy(resp_pack, RpcTransportPolicy::ForceTransport)
                .await
            {
                tracing::warn!("p2p_raw_read send_resp failed: {:?}", err);
            }
            drop(view_task);
        });
        Ok(())
    });
}

pub fn register_raw_mem_write_handler(
    view: &ClientTransferEngineView,
    handler: P2pRawMemWriteHandler,
) {
    let view_write = view.clone();
    RPCHandler::<RawMemWriteReq>::new().regist(view.p2p_module(), move |resp, msg| {
        let view_task = view_write.clone();
        let handler = handler.clone();
        let queued_at = Instant::now();
        let from_node = resp.node_id().to_string();
        let task_id = resp.task_id();
        let _ = view_write.spawn("p2p_raw_write", async move {
            let task_started_at = Instant::now();
            let queue_delay_us = duration_to_i64_us(queued_at.elapsed());
            let req = msg.serialize_part;
            let target_addr = req.target_addr;
            let req_len = req.len;
            let payload_len = msg.raw_bytes.first().map(|payload| payload.len() as u64).unwrap_or(0);
            let handler_started_at = Instant::now();
            let serialize_part = match msg.raw_bytes.first().cloned() {
                Some(payload) => match handler(req, payload).await {
                    Ok(()) => RawMemWriteRespWire {
                        error_code: OK,
                        error_json: String::new(),
                    },
                    Err(err) => RawMemWriteRespWire {
                        error_code: crate::rpcresp_kvresult_convert::msg_and_error::codes_p2p_transfer::P2P_TRANSFER_INVALID_ARG,
                        error_json: serde_json::to_string(
                            &crate::rpcresp_kvresult_convert::msg_and_error::P2pTransferError::InvalidArg {
                                detail: err,
                            },
                        )
                        .unwrap(),
                    },
                },
                None => RawMemWriteRespWire {
                    error_code: crate::rpcresp_kvresult_convert::msg_and_error::codes_p2p_transfer::P2P_TRANSFER_MISSING_PAYLOAD,
                    error_json: serde_json::to_string(
                        &crate::rpcresp_kvresult_convert::msg_and_error::P2pTransferError::MissingPayload {
                            detail: "p2p_raw_write: missing raw_bytes[0]".to_string(),
                        },
                    )
                    .unwrap(),
                },
            };
            let handler_us = duration_to_i64_us(handler_started_at.elapsed());
            let resp_error_code = serialize_part.error_code;
            let send_resp_started_at = Instant::now();
            if let Err(err) = resp
                .send_resp_with_transport_policy(
                    MsgPack {
                        serialize_part,
                        raw_bytes: Vec::new(),
                    },
                    RpcTransportPolicy::ForceTransport,
                )
                .await
            {
                tracing::warn!(
                    "p2p_raw_write timing send_resp_failed: task_id={} from_node={} target_addr={:#x} req_len={} payload_len={} queue_delay_us={} handler_us={} send_resp_us={} total_us={} resp_error_code={} err={:?}",
                    task_id,
                    from_node,
                    target_addr,
                    req_len,
                    payload_len,
                    queue_delay_us,
                    handler_us,
                    duration_to_i64_us(send_resp_started_at.elapsed()),
                    duration_to_i64_us(task_started_at.elapsed()),
                    resp_error_code,
                    err
                );
            } else {
                tracing::info!(
                    "p2p_raw_write timing: task_id={} from_node={} target_addr={:#x} req_len={} payload_len={} queue_delay_us={} handler_us={} send_resp_us={} total_us={} resp_error_code={}",
                    task_id,
                    from_node,
                    target_addr,
                    req_len,
                    payload_len,
                    queue_delay_us,
                    handler_us,
                    duration_to_i64_us(send_resp_started_at.elapsed()),
                    duration_to_i64_us(task_started_at.elapsed()),
                    resp_error_code
                );
            }
            drop(view_task);
        });
        Ok(())
    });
}

pub async fn call_raw_mem_read(
    view: &ClientTransferEngineView,
    peer: NodeIDString,
    req: RawMemReadReq,
) -> KvResult<Bytes> {
    let caller = RPCCaller::<RawMemReadReq>::new();
    let resp = caller
        .call_with_transport_policy(
            view.p2p_module(),
            peer.into(),
            MsgPack {
                serialize_part: req,
                raw_bytes: Vec::new(),
            },
            None,
            RpcTransportPolicy::ForceTransport,
            0,
        )
        .await
        .map_err(KvError::from)?;
    crate::rpcresp_kvresult_convert::try_from_code(
        resp.serialize_part.error_code,
        resp.serialize_part.error_json,
    )?;
    let bytes = resp.raw_bytes.get(0).ok_or_else(|| {
        KvError::from(
            crate::rpcresp_kvresult_convert::msg_and_error::P2pTransferError::MissingPayload {
                detail: "call_raw_mem_read: missing raw_bytes".to_string(),
            },
        )
    })?;
    Ok(bytes.clone())
}

pub async fn call_raw_mem_write(
    view: &ClientTransferEngineView,
    peer: NodeIDString,
    req: RawMemWriteReq,
    payload: Bytes,
) -> KvResult<()> {
    let caller = RPCCaller::<RawMemWriteReq>::new();
    let resp = caller
        .call_with_transport_policy(
            view.p2p_module(),
            peer.into(),
            MsgPack {
                serialize_part: req,
                raw_bytes: vec![payload],
            },
            None,
            RpcTransportPolicy::ForceTransport,
            0,
        )
        .await
        .map_err(KvError::from)?;
    crate::rpcresp_kvresult_convert::try_from_code(
        resp.serialize_part.error_code,
        resp.serialize_part.error_json,
    )?;
    Ok(())
}

async fn handle_raw_mem_read(
    view: &ClientTransferEngineView,
    req: RawMemReadReq,
) -> Result<Bytes, String> {
    if req.len == 0 {
        return Err("len must be > 0".to_string());
    }
    let len = u64_to_usize(req.len)?;
    let bytes = view
        .client_seg_pool()
        .read_from_segment(req.src_addr, len)
        .await?;
    let prefix_len = std::cmp::min(8, bytes.len());
    tracing::info!(
        "p2p_raw_read snapshot: src_addr={:#x} len={} prefix={:?}",
        req.src_addr,
        len,
        &bytes[..prefix_len]
    );
    Ok(bytes)
}

async fn handle_raw_mem_write(
    view: &ClientTransferEngineView,
    req: RawMemWriteReq,
    payload: Bytes,
) -> Result<(), String> {
    if req.len == 0 {
        return Err("len must be > 0".to_string());
    }
    let len = u64_to_usize(req.len)?;
    if payload.len() != len {
        return Err(format!(
            "payload length mismatch: expected={}, actual={}",
            req.len,
            payload.len()
        ));
    }
    view.client_seg_pool()
        .copy_into_segment(req.target_addr, payload.as_ref())
        .await
}

fn validate_guard_covers_addr(
    guard: &ClientCpuMemReadGuard,
    local_addr: u64,
) -> Result<(), String> {
    let rw_end = guard
        .allocated_addr
        .checked_add(guard.allocated_size)
        .ok_or_else(|| {
            format!(
                "segment range overflow: rw_base={:#x}, len={}",
                guard.allocated_addr, guard.allocated_size
            )
        })?;
    let ro_end = guard
        .allocated_addr_ro
        .checked_add(guard.allocated_size)
        .ok_or_else(|| {
            format!(
                "segment range overflow: ro_base={:#x}, len={}",
                guard.allocated_addr_ro, guard.allocated_size
            )
        })?;

    let in_rw = local_addr >= guard.allocated_addr && local_addr < rw_end;
    let in_ro = local_addr >= guard.allocated_addr_ro && local_addr < ro_end;
    if !in_rw && !in_ro {
        return Err(format!(
            "segment guard does not cover local_addr: local_addr={:#x}, rw=[{:#x},{:#x}), ro=[{:#x},{:#x})",
            local_addr, guard.allocated_addr, rw_end, guard.allocated_addr_ro, ro_end
        ));
    }
    Ok(())
}

fn segment_ranges(guard: &ClientCpuMemReadGuard) -> Result<String, String> {
    let rw_end = guard
        .allocated_addr
        .checked_add(guard.allocated_size)
        .ok_or_else(|| {
            format!(
                "segment range overflow: rw_base={:#x}, seg_len={}",
                guard.allocated_addr, guard.allocated_size
            )
        })?;
    let ro_end = guard
        .allocated_addr_ro
        .checked_add(guard.allocated_size)
        .ok_or_else(|| {
            format!(
                "segment range overflow: ro_base={:#x}, seg_len={}",
                guard.allocated_addr_ro, guard.allocated_size
            )
        })?;
    Ok(format!(
        "rw=[{:#x},{:#x}), ro=[{:#x},{:#x})",
        guard.allocated_addr, rw_end, guard.allocated_addr_ro, ro_end
    ))
}

fn u64_to_usize(len: u64) -> Result<usize, String> {
    if len <= usize::MAX as u64 {
        Ok(len as usize)
    } else {
        Err(format!("len is too large for this process: len={}", len))
    }
}
