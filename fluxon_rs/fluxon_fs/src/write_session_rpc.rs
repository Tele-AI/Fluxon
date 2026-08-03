use bitcode::{Decode, Encode};
use fluxon_commu::p2p::RpcTransportPolicy;
use fluxon_commu::p2p::rpc::{MsgPack, MsgPackSerializePart, RPCCaller, RPCHandler};
use fluxon_kv::Framework as KvFramework;
use fluxon_kv::cluster_manager::NodeID;
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use fluxon_util::run_async_from_sync::spawn_blocking_allow_sync_async_bridge;
use prost::bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const FS_WRITE_SESSION_CHUNK_REQ_MSG_ID: u32 = 7101;
pub const FS_WRITE_SESSION_CHUNK_RESP_MSG_ID: u32 = 7102;
pub const FS_OPEN_WRITE_SESSION_REQ_MSG_ID: u32 = 7103;
pub const FS_OPEN_WRITE_SESSION_RESP_MSG_ID: u32 = 7104;
pub const FS_CLOSE_WRITE_SESSION_REQ_MSG_ID: u32 = 7105;
pub const FS_CLOSE_WRITE_SESSION_RESP_MSG_ID: u32 = 7106;
pub const FS_WRITE_SESSION_DATA_MSG_ID: u32 = 7107;
pub const FS_ABORT_WRITE_SESSION_REQ_MSG_ID: u32 = 7108;
pub const FS_ABORT_WRITE_SESSION_RESP_MSG_ID: u32 = 7109;
pub const FS_WAIT_WRITE_SESSION_PAYLOADS_REQ_MSG_ID: u32 = 7110;
pub const FS_WAIT_WRITE_SESSION_PAYLOADS_RESP_MSG_ID: u32 = 7111;
pub const FS_WRITE_SESSION_DATA_ACK_MSG_ID: u32 = 7112;
pub const FS_WRITE_SESSION_DATA_REF_MSG_ID: u32 = 7113;
pub const FS_FINALIZE_WRITE_SESSION_REQ_MSG_ID: u32 = 7114;
pub const FS_FINALIZE_WRITE_SESSION_RESP_MSG_ID: u32 = 7115;
pub const FS_PUT_SMALL_OBJECT_REQ_MSG_ID: u32 = 7116;
pub const FS_PUT_SMALL_OBJECT_RESP_MSG_ID: u32 = 7117;
pub const FS_WRITE_SESSION_KV_REF_CAPABILITY_METADATA_KEY: &str =
    "fluxon_fs_write_session_kv_ref_v1";

pub(crate) fn write_session_kv_payload_key(
    from_node: &str,
    source_start: i64,
    session_id: &str,
    lease_id: u64,
    seq_no: u64,
    nonce: u128,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(from_node.as_bytes());
    hasher.update(b"\0");
    hasher.update(source_start.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(session_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(lease_id.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(seq_no.to_string().as_bytes());
    hasher.update(b"\0");
    hasher.update(nonce.to_string().as_bytes());
    format!(
        "__fluxon_fs/write-session/v1/{}",
        hex::encode(hasher.finalize())
    )
}

fn write_session_trace_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn spawn_on_fs_framework<F>(framework: &crate::Framework, fut: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let _ = framework.spawn_registered_boxed("write_session_rpc", Box::pin(fut));
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWriteSessionChunkReq {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub offset: i64,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWriteSessionChunkResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsOpenWriteSessionReq {
    pub export: String,
    pub relpath: String,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
    pub create_parents: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsOpenWriteSessionResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
    pub session_id: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub chunk_bytes: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsPutSmallObjectReq {
    pub export: String,
    pub relpath: String,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsPutSmallObjectResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
    pub size: i64,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsCloseWriteSessionReq {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub expected_data_frames: u64,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsCloseWriteSessionResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
    pub size: i64,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsFinalizeWriteSessionReq {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub expected_data_frames: u64,
    pub final_size: i64,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsFinalizeWriteSessionResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
    pub size: i64,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsAbortWriteSessionReq {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsAbortWriteSessionResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWaitWriteSessionPayloadsReq {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub expected_data_frames: u64,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWaitWriteSessionPayloadsResp {
    pub ok: bool,
    pub err_kind: i64,
    pub err_detail: String,
    pub err_errno: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWriteSessionDataFrame {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub seq_no: u64,
    pub offset: i64,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWriteSessionDataRefFrame {
    pub export: String,
    pub relpath: String,
    pub session_id: String,
    pub seq_no: u64,
    pub offset: i64,
    pub kv_key: String,
    pub total_len: u64,
    pub part_lengths: Vec<u32>,
    pub source_node_start_time: i64,
    pub owner_id: String,
    pub owner_start_time: i64,
    pub lease_id: u64,
    pub nonce: u128,
    pub fs_rpc_token: Option<String>,
    pub allow_s3_internal_multipart: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Encode, Decode)]
pub struct FsWriteSessionDataAck {
    pub session_id: String,
    pub seq_no: u64,
    pub frame_count: u64,
    pub ok: bool,
    pub err_detail: String,
}

impl MsgPackSerializePart for FsWriteSessionChunkReq {
    fn msg_id(&self) -> u32 {
        FS_WRITE_SESSION_CHUNK_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsWriteSessionChunkReq {
    type Resp = FsWriteSessionChunkResp;
}

impl MsgPackSerializePart for FsWriteSessionChunkResp {
    fn msg_id(&self) -> u32 {
        FS_WRITE_SESSION_CHUNK_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsOpenWriteSessionReq {
    fn msg_id(&self) -> u32 {
        FS_OPEN_WRITE_SESSION_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsOpenWriteSessionReq {
    type Resp = FsOpenWriteSessionResp;
}

impl MsgPackSerializePart for FsOpenWriteSessionResp {
    fn msg_id(&self) -> u32 {
        FS_OPEN_WRITE_SESSION_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsPutSmallObjectReq {
    fn msg_id(&self) -> u32 {
        FS_PUT_SMALL_OBJECT_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsPutSmallObjectReq {
    type Resp = FsPutSmallObjectResp;
}

impl MsgPackSerializePart for FsPutSmallObjectResp {
    fn msg_id(&self) -> u32 {
        FS_PUT_SMALL_OBJECT_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsCloseWriteSessionReq {
    fn msg_id(&self) -> u32 {
        FS_CLOSE_WRITE_SESSION_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsCloseWriteSessionReq {
    type Resp = FsCloseWriteSessionResp;
}

impl MsgPackSerializePart for FsCloseWriteSessionResp {
    fn msg_id(&self) -> u32 {
        FS_CLOSE_WRITE_SESSION_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsFinalizeWriteSessionReq {
    fn msg_id(&self) -> u32 {
        FS_FINALIZE_WRITE_SESSION_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsFinalizeWriteSessionReq {
    type Resp = FsFinalizeWriteSessionResp;
}

impl MsgPackSerializePart for FsFinalizeWriteSessionResp {
    fn msg_id(&self) -> u32 {
        FS_FINALIZE_WRITE_SESSION_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsAbortWriteSessionReq {
    fn msg_id(&self) -> u32 {
        FS_ABORT_WRITE_SESSION_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsAbortWriteSessionReq {
    type Resp = FsAbortWriteSessionResp;
}

impl MsgPackSerializePart for FsAbortWriteSessionResp {
    fn msg_id(&self) -> u32 {
        FS_ABORT_WRITE_SESSION_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsWaitWriteSessionPayloadsReq {
    fn msg_id(&self) -> u32 {
        FS_WAIT_WRITE_SESSION_PAYLOADS_REQ_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsWaitWriteSessionPayloadsReq {
    type Resp = FsWaitWriteSessionPayloadsResp;
}

impl MsgPackSerializePart for FsWaitWriteSessionPayloadsResp {
    fn msg_id(&self) -> u32 {
        FS_WAIT_WRITE_SESSION_PAYLOADS_RESP_MSG_ID
    }
}

impl MsgPackSerializePart for FsWriteSessionDataFrame {
    fn msg_id(&self) -> u32 {
        FS_WRITE_SESSION_DATA_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsWriteSessionDataFrame {
    type Resp = FsWriteSessionDataAck;
}

impl MsgPackSerializePart for FsWriteSessionDataRefFrame {
    fn msg_id(&self) -> u32 {
        FS_WRITE_SESSION_DATA_REF_MSG_ID
    }
}

impl fluxon_commu::p2p::rpc::RPCReq for FsWriteSessionDataRefFrame {
    type Resp = FsWriteSessionDataAck;
}

impl MsgPackSerializePart for FsWriteSessionDataAck {
    fn msg_id(&self) -> u32 {
        FS_WRITE_SESSION_DATA_ACK_MSG_ID
    }
}

pub fn register_callers(fw: &KvFramework) {
    RPCCaller::<FsWriteSessionChunkReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsOpenWriteSessionReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsPutSmallObjectReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsCloseWriteSessionReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsFinalizeWriteSessionReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsAbortWriteSessionReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsWaitWriteSessionPayloadsReq>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsWriteSessionDataFrame>::new().regist(fw.p2p_view().p2p_module());
    RPCCaller::<FsWriteSessionDataRefFrame>::new().regist(fw.p2p_view().p2p_module());
}

pub fn register_chunk_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsWriteSessionChunkReq, Vec<u8>) -> FsWriteSessionChunkResp
        + Send
        + Sync
        + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsWriteSessionChunkReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let payload = msg
                .raw_bytes
                .first()
                .map(|v| v.to_vec())
                .unwrap_or_default();
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response = match spawn_blocking_allow_sync_async_bridge(move || {
                    handler(from_node, req, payload)
                })
                .await
                {
                    Ok(v) => v,
                    Err(err) => FsWriteSessionChunkResp {
                        ok: false,
                        err_kind: 0,
                        err_detail: format!(
                            "fluxon_fs write-session chunk handler panicked: {}",
                            err
                        ),
                        err_errno: 0,
                    },
                };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs write-session chunk typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_open_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsOpenWriteSessionReq) -> FsOpenWriteSessionResp + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsOpenWriteSessionReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response =
                    match spawn_blocking_allow_sync_async_bridge(move || handler(from_node, req))
                        .await
                    {
                        Ok(v) => v,
                        Err(err) => open_handler_panic_response(err.to_string()),
                    };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs open_write_session typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

fn open_handler_panic_response(err: String) -> FsOpenWriteSessionResp {
    FsOpenWriteSessionResp {
        ok: false,
        err_kind: 4,
        err_detail: format!("fluxon_fs open_write_session handler panicked: {}", err),
        err_errno: 0,
        session_id: String::new(),
        size: 0,
        mtime_ns: 0,
        chunk_bytes: 0,
    }
}

pub fn register_put_small_object_handler<F>(
    fw: &KvFramework,
    lifecycle: crate::Framework,
    handler: F,
) where
    F: Fn(NodeID, FsPutSmallObjectReq, Bytes) -> FsPutSmallObjectResp + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsPutSmallObjectReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let payload = if msg.raw_bytes.len() == 1 {
                msg.raw_bytes.first().cloned()
            } else {
                None
            };
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response = match payload {
                    Some(payload) => {
                        match spawn_blocking_allow_sync_async_bridge(move || {
                            handler(from_node, req, payload)
                        })
                        .await
                        {
                            Ok(v) => v,
                            Err(err) => FsPutSmallObjectResp {
                                ok: false,
                                err_kind: 4,
                                err_detail: format!(
                                    "fluxon_fs put_small_object handler panicked: {}",
                                    err
                                ),
                                err_errno: 0,
                                size: 0,
                                mtime_ns: 0,
                            },
                        }
                    }
                    None => FsPutSmallObjectResp {
                        ok: false,
                        err_kind: 1,
                        err_detail: "put_small_object requires exactly one raw payload".to_string(),
                        err_errno: 0,
                        size: 0,
                        mtime_ns: 0,
                    },
                };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs put_small_object typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_close_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsCloseWriteSessionReq) -> FsCloseWriteSessionResp + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsCloseWriteSessionReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response =
                    match spawn_blocking_allow_sync_async_bridge(move || handler(from_node, req))
                        .await
                    {
                        Ok(v) => v,
                        Err(err) => FsCloseWriteSessionResp {
                            ok: false,
                            err_kind: 0,
                            err_detail: format!(
                                "fluxon_fs close_write_session handler panicked: {}",
                                err
                            ),
                            err_errno: 0,
                            size: 0,
                            mtime_ns: 0,
                        },
                    };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs close_write_session typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_finalize_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsFinalizeWriteSessionReq) -> FsFinalizeWriteSessionResp + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsFinalizeWriteSessionReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response =
                    match spawn_blocking_allow_sync_async_bridge(move || handler(from_node, req))
                        .await
                    {
                        Ok(v) => v,
                        Err(err) => FsFinalizeWriteSessionResp {
                            ok: false,
                            err_kind: 0,
                            err_detail: format!(
                                "fluxon_fs finalize_write_session handler panicked: {}",
                                err
                            ),
                            err_errno: 0,
                            size: 0,
                            mtime_ns: 0,
                        },
                    };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs finalize_write_session typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_abort_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsAbortWriteSessionReq) -> FsAbortWriteSessionResp + Send + Sync + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsAbortWriteSessionReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response =
                    match spawn_blocking_allow_sync_async_bridge(move || handler(from_node, req))
                        .await
                    {
                        Ok(v) => v,
                        Err(err) => FsAbortWriteSessionResp {
                            ok: false,
                            err_kind: 0,
                            err_detail: format!(
                                "fluxon_fs abort_write_session handler panicked: {}",
                                err
                            ),
                            err_errno: 0,
                        },
                    };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs abort_write_session typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_wait_payloads_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsWaitWriteSessionPayloadsReq) -> FsWaitWriteSessionPayloadsResp
        + Send
        + Sync
        + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsWaitWriteSessionPayloadsReq>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response =
                    match spawn_blocking_allow_sync_async_bridge(move || handler(from_node, req))
                        .await
                    {
                        Ok(v) => v,
                        Err(err) => FsWaitWriteSessionPayloadsResp {
                            ok: false,
                            err_kind: 0,
                            err_detail: format!(
                                "fluxon_fs wait_write_session_payloads handler panicked: {}",
                                err
                            ),
                            err_errno: 0,
                        },
                    };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs wait_write_session_payloads typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_data_handler<F>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsWriteSessionDataFrame, Vec<Bytes>) -> FsWriteSessionDataAck
        + Send
        + Sync
        + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsWriteSessionDataFrame>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let payloads = msg.raw_bytes;
            let req = msg.serialize_part;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let payload_frame_count = payloads.len() as u64;
                let session_id = req.session_id.clone();
                let seq_no = req.seq_no;
                let response = match spawn_blocking_allow_sync_async_bridge(move || {
                    handler(from_node, req, payloads)
                })
                .await
                {
                    Ok(v) => v,
                    Err(err) => FsWriteSessionDataAck {
                        session_id,
                        seq_no,
                        frame_count: payload_frame_count,
                        ok: false,
                        err_detail: format!(
                            "fluxon_fs write_session data handler panicked: {}",
                            err
                        ),
                    },
                };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs write_session data typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub fn register_data_ref_handler<F, Fut>(fw: &KvFramework, lifecycle: crate::Framework, handler: F)
where
    F: Fn(NodeID, FsWriteSessionDataRefFrame) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = FsWriteSessionDataAck> + Send + 'static,
{
    let handler = Arc::new(handler);
    RPCHandler::<FsWriteSessionDataRefFrame>::new().regist(
        fw.p2p_view().p2p_module(),
        move |resp, msg| {
            let handler = handler.clone();
            let lifecycle = lifecycle.clone();
            let has_raw_bytes = !msg.raw_bytes.is_empty();
            let req = msg.serialize_part;
            let session_id = req.session_id.clone();
            let seq_no = req.seq_no;
            let frame_count = req.part_lengths.len() as u64;
            let from_node = resp.node_id();
            spawn_on_fs_framework(&lifecycle, async move {
                let response = if has_raw_bytes {
                    FsWriteSessionDataAck {
                        session_id,
                        seq_no,
                        frame_count,
                        ok: false,
                        err_detail: "write_session data-ref request must not contain raw bytes"
                            .to_string(),
                    }
                } else {
                    match tokio::spawn(async move { handler(from_node, req).await }).await {
                        Ok(value) => value,
                        Err(err) => FsWriteSessionDataAck {
                            session_id,
                            seq_no,
                            frame_count,
                            ok: false,
                            err_detail: format!(
                                "fluxon_fs write_session data-ref handler panicked or was cancelled: {}",
                                err
                            ),
                        },
                    }
                };
                let out = MsgPack {
                    serialize_part: response,
                    raw_bytes: Vec::new(),
                };
                if let Err(err) = resp.send_resp(out).await {
                    tracing::warn!(
                        "fluxon_fs write_session data-ref typed rpc send_resp failed: {:?}",
                        err
                    );
                }
            });
            Ok(())
        },
    );
}

pub async fn call_write_session_chunk(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsWriteSessionChunkReq,
    data: Vec<u8>,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsWriteSessionChunkResp> {
    let resp = RPCCaller::<FsWriteSessionChunkReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: vec![Bytes::from(data)],
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    if resp.serialize_part.ok {
        return Ok(resp.serialize_part);
    }
    let detail = if resp.serialize_part.err_detail.trim().is_empty() {
        "remote write_session_chunk failed".to_string()
    } else {
        resp.serialize_part.err_detail.clone()
    };
    Err(KvError::Api(ApiError::Unknown { detail }))
}

pub async fn call_open_write_session(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsOpenWriteSessionReq,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsOpenWriteSessionResp> {
    let resp = RPCCaller::<FsOpenWriteSessionReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: Vec::new(),
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    Ok(resp.serialize_part)
}

pub async fn call_put_small_object(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsPutSmallObjectReq,
    data: Bytes,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsPutSmallObjectResp> {
    let resp = RPCCaller::<FsPutSmallObjectReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: vec![data],
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    Ok(resp.serialize_part)
}

pub async fn call_close_write_session(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsCloseWriteSessionReq,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsCloseWriteSessionResp> {
    let resp = RPCCaller::<FsCloseWriteSessionReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: Vec::new(),
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    if resp.serialize_part.ok {
        return Ok(resp.serialize_part);
    }
    let detail = if resp.serialize_part.err_detail.trim().is_empty() {
        "remote close_write_session failed".to_string()
    } else {
        resp.serialize_part.err_detail.clone()
    };
    Err(KvError::Api(ApiError::Unknown { detail }))
}

pub async fn call_finalize_write_session(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsFinalizeWriteSessionReq,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsFinalizeWriteSessionResp> {
    let resp = RPCCaller::<FsFinalizeWriteSessionReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: Vec::new(),
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    if resp.serialize_part.ok {
        return Ok(resp.serialize_part);
    }
    let detail = if resp.serialize_part.err_detail.trim().is_empty() {
        "remote finalize_write_session failed".to_string()
    } else {
        resp.serialize_part.err_detail.clone()
    };
    Err(KvError::Api(ApiError::Unknown { detail }))
}

pub async fn call_abort_write_session(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsAbortWriteSessionReq,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsAbortWriteSessionResp> {
    let resp = RPCCaller::<FsAbortWriteSessionReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: Vec::new(),
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    if resp.serialize_part.ok {
        return Ok(resp.serialize_part);
    }
    let detail = if resp.serialize_part.err_detail.trim().is_empty() {
        "remote abort_write_session failed".to_string()
    } else {
        resp.serialize_part.err_detail.clone()
    };
    Err(KvError::Api(ApiError::Unknown { detail }))
}

pub async fn call_wait_write_session_payloads(
    fw: &KvFramework,
    node_id: NodeID,
    req: FsWaitWriteSessionPayloadsReq,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsWaitWriteSessionPayloadsResp> {
    let resp = RPCCaller::<FsWaitWriteSessionPayloadsReq>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: req,
                raw_bytes: Vec::new(),
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    if resp.serialize_part.ok {
        return Ok(resp.serialize_part);
    }
    let detail = if resp.serialize_part.err_detail.trim().is_empty() {
        "remote wait_write_session_payloads failed".to_string()
    } else {
        resp.serialize_part.err_detail.clone()
    };
    Err(KvError::Api(ApiError::Unknown { detail }))
}

pub async fn send_write_session_data(
    fw: &KvFramework,
    node_id: NodeID,
    frame: FsWriteSessionDataFrame,
    data: Vec<u8>,
) -> KvResult<FsWriteSessionDataAck> {
    send_write_session_data_bytes(
        fw,
        node_id,
        frame,
        vec![Bytes::from(data)],
        RpcTransportPolicy::AllowTransferRpcFastPath,
    )
    .await
}

pub async fn send_write_session_data_bytes(
    fw: &KvFramework,
    node_id: NodeID,
    frame: FsWriteSessionDataFrame,
    data: Vec<Bytes>,
    transport_policy: RpcTransportPolicy,
) -> KvResult<FsWriteSessionDataAck> {
    let raw_bytes: Vec<Bytes> = data.into_iter().filter(|value| !value.is_empty()).collect();
    if raw_bytes.is_empty() {
        return Ok(FsWriteSessionDataAck {
            session_id: frame.session_id,
            seq_no: frame.seq_no,
            frame_count: 0,
            ok: true,
            err_detail: String::new(),
        });
    }
    let payload_len: usize = raw_bytes.iter().map(|value| value.len()).sum();
    let payload_frames = raw_bytes.len();
    let started = std::time::Instant::now();
    let node_id_for_log = node_id.clone();
    let seq_no = frame.seq_no;
    let offset = frame.offset;
    let session_id_for_log = frame.session_id.clone();
    let result = RPCCaller::<FsWriteSessionDataFrame>::new()
        .call_with_transport_policy(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: frame,
                raw_bytes,
            },
            Some(std::time::Duration::from_millis(
                remote_write_session_data_timeout_ms(payload_len),
            )),
            transport_policy,
            0,
        )
        .await
        .map_err(KvError::from)
        .and_then(|resp| {
            if resp.serialize_part.ok {
                Ok(resp.serialize_part)
            } else {
                let detail = if resp.serialize_part.err_detail.trim().is_empty() {
                    "remote write_session_data failed".to_string()
                } else {
                    resp.serialize_part.err_detail.clone()
                };
                Err(KvError::Api(ApiError::Unknown { detail }))
            }
        });
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    if elapsed_ms >= 50.0 {
        let ok = result.is_ok();
        tracing::info!(
            "[fluxon-write-session-send-prof] wall_ms={} session_id={} policy={:?} node={} bytes={} frames={} seq_no={} offset={} elapsed_ms={:.3} ok={}",
            write_session_trace_now_ms(),
            session_id_for_log,
            transport_policy,
            node_id_for_log,
            payload_len,
            payload_frames,
            seq_no,
            offset,
            elapsed_ms,
            ok,
        );
    }
    result
}

pub async fn call_write_session_data_ref(
    fw: &KvFramework,
    node_id: NodeID,
    frame: FsWriteSessionDataRefFrame,
    timeout: Option<std::time::Duration>,
) -> KvResult<FsWriteSessionDataAck> {
    let resp = RPCCaller::<FsWriteSessionDataRefFrame>::new()
        .call(
            fw.p2p_view().p2p_module(),
            node_id,
            MsgPack {
                serialize_part: frame,
                raw_bytes: Vec::new(),
            },
            timeout,
            0,
        )
        .await
        .map_err(KvError::from)?;
    if !resp.raw_bytes.is_empty() {
        return Err(KvError::Api(ApiError::InvalidArgument {
            detail: "write_session data-ref response must not contain raw bytes".to_string(),
        }));
    }
    if resp.serialize_part.ok {
        return Ok(resp.serialize_part);
    }
    let detail = if resp.serialize_part.err_detail.trim().is_empty() {
        "remote write_session_data_ref failed".to_string()
    } else {
        resp.serialize_part.err_detail.clone()
    };
    Err(KvError::Api(ApiError::Unknown { detail }))
}

fn remote_write_session_data_timeout_ms(payload_len_bytes: usize) -> u64 {
    let mib = (payload_len_bytes.saturating_add((1 << 20) - 1) / (1 << 20)) as u64;
    let timeout_ms = 10_000u64.saturating_add(mib.saturating_mul(500));
    timeout_ms.max(10_000).min(120_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_session_data_ref_frame_has_stable_message_id_and_round_trips() {
        let frame = FsWriteSessionDataRefFrame {
            export: "export-a".to_string(),
            relpath: "dir/file.bin".to_string(),
            session_id: "session-a".to_string(),
            seq_no: 7,
            offset: 4096,
            kv_key: "/fluxon/fs/write-session/session-a/7".to_string(),
            total_len: 8192,
            part_lengths: vec![4096, 4096],
            source_node_start_time: 101,
            owner_id: "owner-a".to_string(),
            owner_start_time: 202,
            lease_id: 303,
            nonce: 404,
            fs_rpc_token: Some("token-a".to_string()),
            allow_s3_internal_multipart: true,
        };

        assert_eq!(frame.msg_id(), FS_WRITE_SESSION_DATA_REF_MSG_ID);
        assert_eq!(
            FsWriteSessionDataAck::default().msg_id(),
            FS_WRITE_SESSION_DATA_ACK_MSG_ID
        );
        let encoded = bitcode::encode(&frame);
        let decoded: FsWriteSessionDataRefFrame = bitcode::decode(&encoded).unwrap();
        assert_eq!(decoded.export, frame.export);
        assert_eq!(decoded.relpath, frame.relpath);
        assert_eq!(decoded.session_id, frame.session_id);
        assert_eq!(decoded.seq_no, frame.seq_no);
        assert_eq!(decoded.offset, frame.offset);
        assert_eq!(decoded.kv_key, frame.kv_key);
        assert_eq!(decoded.total_len, frame.total_len);
        assert_eq!(decoded.part_lengths, frame.part_lengths);
        assert_eq!(decoded.source_node_start_time, frame.source_node_start_time);
        assert_eq!(decoded.owner_id, frame.owner_id);
        assert_eq!(decoded.owner_start_time, frame.owner_start_time);
        assert_eq!(decoded.lease_id, frame.lease_id);
        assert_eq!(decoded.nonce, frame.nonce);
        assert_eq!(decoded.fs_rpc_token, frame.fs_rpc_token);
        assert_eq!(
            decoded.allow_s3_internal_multipart,
            frame.allow_s3_internal_multipart
        );
    }

    #[test]
    fn write_session_kv_payload_key_is_stable() {
        let first = write_session_kv_payload_key("node-a", 101, "session-a", 303, 7, 404);
        assert_eq!(
            first,
            write_session_kv_payload_key("node-a", 101, "session-a", 303, 7, 404)
        );
        assert!(first.starts_with("__fluxon_fs/write-session/v1/"));
        assert_ne!(
            first,
            write_session_kv_payload_key("node-a", 101, "session-a", 303, 7, 405)
        );
    }

    #[test]
    fn finalize_write_session_request_has_stable_message_id_and_round_trips() {
        let req = FsFinalizeWriteSessionReq {
            export: "export-a".to_string(),
            relpath: "dir/file.bin".to_string(),
            session_id: "session-a".to_string(),
            expected_data_frames: 7,
            final_size: 12345,
            fs_rpc_token: Some("token-a".to_string()),
            allow_s3_internal_multipart: true,
        };

        assert_eq!(req.msg_id(), FS_FINALIZE_WRITE_SESSION_REQ_MSG_ID);
        assert_eq!(
            FsFinalizeWriteSessionResp::default().msg_id(),
            FS_FINALIZE_WRITE_SESSION_RESP_MSG_ID
        );
        let encoded = bitcode::encode(&req);
        let decoded: FsFinalizeWriteSessionReq = bitcode::decode(&encoded).unwrap();
        assert_eq!(decoded.export, req.export);
        assert_eq!(decoded.relpath, req.relpath);
        assert_eq!(decoded.session_id, req.session_id);
        assert_eq!(decoded.expected_data_frames, req.expected_data_frames);
        assert_eq!(decoded.final_size, req.final_size);
        assert_eq!(decoded.fs_rpc_token, req.fs_rpc_token);
        assert_eq!(
            decoded.allow_s3_internal_multipart,
            req.allow_s3_internal_multipart
        );
    }

    #[test]
    fn put_small_object_request_has_stable_message_id_and_round_trips() {
        let req = FsPutSmallObjectReq {
            export: "export-a".to_string(),
            relpath: "nested/dir/file.bin".to_string(),
            fs_rpc_token: Some("token-a".to_string()),
            allow_s3_internal_multipart: true,
        };

        assert_eq!(req.msg_id(), FS_PUT_SMALL_OBJECT_REQ_MSG_ID);
        assert_eq!(
            FsPutSmallObjectResp::default().msg_id(),
            FS_PUT_SMALL_OBJECT_RESP_MSG_ID
        );
        let encoded = bitcode::encode(&req);
        let decoded: FsPutSmallObjectReq = bitcode::decode(&encoded).unwrap();
        assert_eq!(decoded.export, req.export);
        assert_eq!(decoded.relpath, req.relpath);
        assert_eq!(decoded.fs_rpc_token, req.fs_rpc_token);
        assert_eq!(
            decoded.allow_s3_internal_multipart,
            req.allow_s3_internal_multipart
        );
    }

    #[test]
    fn open_write_session_create_parents_round_trips() {
        let req = FsOpenWriteSessionReq {
            export: "export-a".to_string(),
            relpath: "nested/dir/file.bin".to_string(),
            fs_rpc_token: Some("token-a".to_string()),
            allow_s3_internal_multipart: false,
            create_parents: true,
        };

        let encoded = bitcode::encode(&req);
        let decoded: FsOpenWriteSessionReq = bitcode::decode(&encoded).unwrap();
        assert!(decoded.create_parents);
        assert_eq!(decoded.export, req.export);
        assert_eq!(decoded.relpath, req.relpath);
    }

    #[test]
    fn open_handler_panic_is_reported_as_internal_error() {
        let resp = open_handler_panic_response("boom".to_string());
        assert!(!resp.ok);
        assert_eq!(resp.err_kind, 4);
        assert!(resp.err_detail.contains("handler panicked"));
    }
}
