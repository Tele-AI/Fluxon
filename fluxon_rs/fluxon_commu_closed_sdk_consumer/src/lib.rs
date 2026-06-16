use std::any::Any;
use std::error::Error;
use std::ffi::{CStr, c_char, c_void};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use fluxon_commu_contract::{
    ClosedRuntimeCallRawObservedOutputView, ClosedRuntimeClusterEventStreamItem,
    ClosedRuntimeClusterManagerCall, ClosedRuntimeClusterManagerResponse,
    ClosedRuntimeClusterRdmaResolvedConfigStreamItem, ClosedRuntimeDesiredTransferPeer,
    ClosedRuntimeDispatchRequestView,
    ClosedRuntimeDispatchResponse, ClosedRuntimeDispatchTransportPolicy, ClosedRuntimeError,
    ClosedRuntimeHandle, ClosedRuntimeHostCallbackHandle, ClosedRuntimeP2pCall,
    ClosedRuntimeP2pCallRawObservedRequestView, ClosedRuntimeP2pResponse,
    ClosedRuntimeP2pSendResponseRawRequestView, ClosedRuntimePeerGen, ClosedRuntimeRawSlice,
    ClosedRuntimeRequest, ClosedRuntimeResponse, ClosedRuntimeTransferEngineCall,
    ClosedRuntimeTransferEngineOpenRuntimeRequest, ClosedRuntimeTransferEngineOpenRuntimeResponse,
    ClosedRuntimeTransferEngineResponse, ClosedRuntimeUserRpcBytesRequestView,
    ClosedRuntimeUserRpcHandlerLocalObserveView, ClosedRuntimeWireBodyView,
    ClosedRuntimeWireTransportLocalObserveView, P2pError, UserRpcBytesAsyncHandler,
    UserRpcBytesError, UserRpcBytesHandler, WireMessageBody,
};

pub mod rdma_probe;

pub const FLUXON_COMMU_CLOSED_SDK_SCHEMA_VERSION: u32 = 5;
pub const FLUXON_COMMU_CLOSED_ABI_VERSION: u32 = 8;
pub const FLUXON_COMMU_CLOSED_HOST_CALLBACKS_ABI_VERSION: u32 = 8;
pub const FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK: i32 = 0;
pub const FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR: i32 = 1;
pub const FLUXON_COMMU_CLOSED_RUNTIME_RESULT_USER_RPC_ERR: i32 = 2;

type HostAsyncBytesFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FluxonCommuClosedVersion {
    pub abi_version: u32,
    pub sdk_schema_version: u32,
    pub sdk_version: *const c_char,
    pub required_open_surface_version: *const c_char,
    pub boundary_mode: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FluxonCommuClosedRuntimeAnchor {
    pub cluster_manager_size: usize,
    pub p2p_module_size: usize,
    pub transfer_engine_core_size: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FluxonCommuClosedOwnedBytes {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FluxonCommuClosedRuntimeCompletionResult {
    pub status_code: i32,
    pub payload: FluxonCommuClosedOwnedBytes,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FluxonCommuClosedUserRpcCompletionResult {
    pub status_code: i32,
    pub error_code: u32,
    pub _reserved: u32,
    pub payload: FluxonCommuClosedOwnedBytes,
    pub local_observe: ClosedRuntimeUserRpcHandlerLocalObserveView,
}

pub type FluxonCommuClosedHostUserRpcBytesCompletionCallback =
    extern "C" fn(*mut c_void, FluxonCommuClosedUserRpcCompletionResult);

pub type FluxonCommuClosedHostUserRpcBytesHandleAsync = extern "C" fn(
    u64,
    *const ClosedRuntimeUserRpcBytesRequestView,
    *mut c_void,
    Option<FluxonCommuClosedHostUserRpcBytesCompletionCallback>,
) -> i32;

pub type FluxonCommuClosedHostUserRpcBytesHandleSync = extern "C" fn(
    u64,
    *const ClosedRuntimeUserRpcBytesRequestView,
    *mut FluxonCommuClosedUserRpcCompletionResult,
) -> i32;

pub type FluxonCommuClosedHostAsyncBytesHandleAsync = extern "C" fn(
    u64,
    *const u8,
    usize,
    *mut c_void,
    Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
) -> i32;

pub type FluxonCommuClosedHostDispatchHandleAsync = extern "C" fn(
    u64,
    *const u8,
    usize,
    *mut c_void,
    Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
) -> i32;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FluxonCommuClosedHostCallbacksV1 {
    pub abi_version: u32,
    pub user_rpc_bytes_handle_sync: Option<FluxonCommuClosedHostUserRpcBytesHandleSync>,
    pub user_rpc_bytes_handle_async: Option<FluxonCommuClosedHostUserRpcBytesHandleAsync>,
    pub async_bytes_handle_async: Option<FluxonCommuClosedHostAsyncBytesHandleAsync>,
    pub dispatch_handle_async: Option<FluxonCommuClosedHostDispatchHandleAsync>,
    pub retain_host_body_owner: Option<extern "C" fn(u64) -> i32>,
    pub release_host_body_owner: Option<extern "C" fn(u64)>,
    pub release_host_owned_bytes: Option<extern "C" fn(FluxonCommuClosedOwnedBytes)>,
    pub release_host_callback: Option<extern "C" fn(u64)>,
}

unsafe extern "C" {
    fn fluxon_commu_closed_sdk_schema_version() -> u32;
    fn fluxon_commu_closed_abi_version() -> u32;
    fn fluxon_commu_closed_sdk_version() -> *const c_char;
    fn fluxon_commu_closed_required_open_surface_version() -> *const c_char;
    fn fluxon_commu_closed_boundary_mode() -> *const c_char;
    fn fluxon_commu_closed_check_abi_compatible(requested_abi_version: u32) -> i32;
    fn fluxon_commu_closed_query_version() -> FluxonCommuClosedVersion;
    fn fluxon_commu_closed_runtime_anchor() -> FluxonCommuClosedRuntimeAnchor;
    fn fluxon_commu_closed_runtime_anchor_checksum() -> u64;
    fn fluxon_commu_closed_runtime_call_async(
        request_ptr: *const u8,
        request_len: usize,
        user_data: *mut c_void,
        callback: Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
    ) -> i32;
    fn fluxon_commu_closed_p2p_call_raw_observed_async(
        request_ptr: *const u8,
        request_len: usize,
        user_data: *mut c_void,
        callback: Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
    ) -> i32;
    fn fluxon_commu_closed_p2p_send_response_raw_async(
        request_ptr: *const u8,
        request_len: usize,
        user_data: *mut c_void,
        callback: Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
    ) -> i32;
    fn fluxon_commu_closed_runtime_release_owned_bytes(bytes: FluxonCommuClosedOwnedBytes);
    fn fluxon_commu_closed_dispatch_body_owner_retain(owner_handle: u64) -> i32;
    fn fluxon_commu_closed_dispatch_body_owner_release(owner_handle: u64) -> i32;
    fn fluxon_commu_closed_call_raw_observed_output_owner_release(owner_handle: u64) -> i32;
    fn fluxon_commu_closed_install_host_callbacks_v1(
        callbacks: FluxonCommuClosedHostCallbacksV1,
    ) -> i32;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedSdkVersionInfo {
    pub abi_version: u32,
    pub sdk_schema_version: u32,
    pub sdk_version: String,
    pub required_open_surface_version: String,
    pub boundary_mode: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedSdkRuntimeAnchor {
    pub cluster_manager_size: usize,
    pub p2p_module_size: usize,
    pub transfer_engine_core_size: usize,
}

#[derive(Debug, Clone)]
pub struct ClosedRuntimeWireIncomingMessage {
    pub from_node: String,
    pub head: fluxon_commu_contract::p2p::MsgPackHeadMeta,
    pub body: Bytes,
    pub local_observe: fluxon_commu_contract::p2p::WireTransportLocalObserve,
}

#[derive(Debug, Clone)]
pub struct ClosedRuntimeCallRawObservedOutput {
    pub message: ClosedRuntimeWireIncomingMessage,
    pub observe: fluxon_commu_contract::ClosedRuntimeRpcCallTransportObserveTrace,
}

impl From<fluxon_commu_contract::ClosedRuntimeCallRawObservedOutput>
    for ClosedRuntimeCallRawObservedOutput
{
    fn from(value: fluxon_commu_contract::ClosedRuntimeCallRawObservedOutput) -> Self {
        Self {
            message: ClosedRuntimeWireIncomingMessage {
                from_node: value.message.from_node,
                head: value.message.head,
                body: Bytes::from(value.message.body),
                local_observe: value.message.local_observe,
            },
            observe: value.observe,
        }
    }
}

#[derive(Debug)]
pub enum ClosedSdkConsumerError {
    NullStaticString {
        field: &'static str,
    },
    InvalidUtf8 {
        field: &'static str,
        source: std::str::Utf8Error,
    },
    AbiMismatch {
        expected: u32,
        actual: u32,
    },
    RuntimeSubmitRejected {
        status: i32,
    },
    RuntimeCompletionDropped,
    RuntimeDecode {
        detail: String,
    },
    RuntimeError {
        error: ClosedRuntimeError,
    },
    RuntimeUnexpectedResponse {
        detail: String,
    },
    HostCallbackInstallRejected {
        status: i32,
    },
    HostCallbackMissingRuntime {
        detail: String,
    },
}

impl Display for ClosedSdkConsumerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NullStaticString { field } => {
                write!(
                    f,
                    "closed SDK returned a null static string pointer for {field}"
                )
            }
            Self::InvalidUtf8 { field, source } => {
                write!(f, "closed SDK returned invalid UTF-8 for {field}: {source}")
            }
            Self::AbiMismatch { expected, actual } => {
                write!(
                    f,
                    "closed SDK ABI mismatch: expected abi_version={}, actual abi_version={}",
                    expected, actual
                )
            }
            Self::RuntimeSubmitRejected { status } => {
                write!(
                    f,
                    "closed SDK runtime call submission failed: status={status}"
                )
            }
            Self::RuntimeCompletionDropped => {
                write!(f, "closed SDK runtime completion channel dropped")
            }
            Self::RuntimeDecode { detail } => {
                write!(f, "closed SDK runtime payload decode failed: {detail}")
            }
            Self::RuntimeError { error } => {
                write!(f, "closed SDK runtime call failed: {error:?}")
            }
            Self::RuntimeUnexpectedResponse { detail } => {
                write!(
                    f,
                    "closed SDK runtime returned an unexpected response: {detail}"
                )
            }
            Self::HostCallbackInstallRejected { status } => {
                write!(
                    f,
                    "closed SDK host callback installation failed: status={status}"
                )
            }
            Self::HostCallbackMissingRuntime { detail } => {
                write!(
                    f,
                    "closed SDK host callback registration requires a Tokio runtime: {detail}"
                )
            }
        }
    }
}

impl Error for ClosedSdkConsumerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidUtf8 { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone)]
enum RegisteredHostCallback {
    UserRpcBytesSync(Arc<dyn UserRpcBytesHandler>),
    UserRpcBytesAsync(Arc<dyn UserRpcBytesAsyncHandler>),
    AsyncBytes(Arc<dyn Fn(Vec<u8>) -> HostAsyncBytesFuture + Send + Sync + 'static>),
    DispatchRaw(
        Arc<
            dyn for<'a> Fn(ClosedRuntimeDispatchRequestRef<'a>, Bytes) -> Result<(), P2pError>
                + Send
                + Sync
                + 'static,
        >,
    ),
}

struct RegisteredHostCallbackEntry {
    callback: RegisteredHostCallback,
    runtime: Option<tokio::runtime::Handle>,
}

fn alloc_host_callback_entry(
    callback: RegisteredHostCallback,
) -> Result<ClosedRuntimeHostCallbackHandle, ClosedSdkConsumerError> {
    let runtime = Some(tokio::runtime::Handle::try_current().map_err(|error| {
        ClosedSdkConsumerError::HostCallbackMissingRuntime {
            detail: error.to_string(),
        }
    })?);
    let entry = Arc::new(RegisteredHostCallbackEntry { callback, runtime });
    Ok(ClosedRuntimeHostCallbackHandle {
        raw: Arc::into_raw(entry) as u64,
    })
}

static HOST_CALLBACK_INSTALL_RESULT: OnceLock<Result<(), i32>> = OnceLock::new();

unsafe fn retain_host_callback_entry(raw: u64) -> Option<Arc<RegisteredHostCallbackEntry>> {
    if raw == 0 {
        None
    } else {
        let ptr = raw as *const RegisteredHostCallbackEntry;
        unsafe {
            Arc::increment_strong_count(ptr);
            Some(Arc::from_raw(ptr))
        }
    }
}

unsafe fn drop_host_callback_entry(raw: u64) {
    if raw == 0 {
        return;
    }
    unsafe {
        drop(Arc::from_raw(raw as *const RegisteredHostCallbackEntry));
    }
}

struct DispatchBodyOwner {
    owner_handle: u64,
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for DispatchBodyOwner {}
unsafe impl Sync for DispatchBodyOwner {}

impl AsRef<[u8]> for DispatchBodyOwner {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for DispatchBodyOwner {
    fn drop(&mut self) {
        if self.owner_handle == 0 {
            return;
        }
        unsafe {
            let _ = fluxon_commu_closed_dispatch_body_owner_release(self.owner_handle);
        }
    }
}

struct CallRawObservedOutputBodyOwner {
    owner_handle: u64,
    ptr: *const u8,
    len: usize,
}

unsafe impl Send for CallRawObservedOutputBodyOwner {}
unsafe impl Sync for CallRawObservedOutputBodyOwner {}

impl AsRef<[u8]> for CallRawObservedOutputBodyOwner {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for CallRawObservedOutputBodyOwner {
    fn drop(&mut self) {
        if self.owner_handle == 0 {
            return;
        }
        unsafe {
            let _ = fluxon_commu_closed_call_raw_observed_output_owner_release(self.owner_handle);
        }
    }
}

struct WireBodyPartsOwner {
    serialized: Bytes,
    raw_lengths: WireBodyRawLengths,
    raw_payload: WireBodyRawPayload,
}

enum WireBodyRawLengths {
    Empty,
    Single([u32; 1]),
    Many(Vec<u32>),
}

enum WireBodyRawPayload {
    Empty,
    Single(Bytes),
    Concat(Bytes),
}

fn into_wire_body_owner_handle(owner: WireBodyPartsOwner) -> u64 {
    Arc::into_raw(Arc::new(owner)) as u64
}

unsafe fn retain_wire_body_owner_ref(raw: u64) -> bool {
    if raw == 0 {
        return false;
    }
    unsafe {
        Arc::increment_strong_count(raw as *const WireBodyPartsOwner);
    }
    true
}

unsafe fn release_wire_body_owner_ref(raw: u64) -> bool {
    if raw == 0 {
        return false;
    }
    unsafe {
        drop(Arc::from_raw(raw as *const WireBodyPartsOwner));
    }
    true
}

#[derive(Clone, Copy)]
pub struct ClosedRuntimeDispatchRequestRef<'a> {
    pub reply_next_hop: &'a str,
    pub msg_id: u32,
    pub task_id: fluxon_commu_contract::p2p::TaskId,
    pub logical_source_peer_id: &'a str,
    pub logical_source_node_start_time: i64,
    pub logical_target_peer_id: &'a str,
    pub logical_target_node_start_time: i64,
    pub remaining_hops: u8,
    pub default_resp_transport_policy: ClosedRuntimeDispatchTransportPolicy,
    pub incoming_frame_recv_done_ts_us: i64,
    pub incoming_dispatch_enqueued_ts_us: i64,
    pub incoming_dispatch_started_ts_us: i64,
    pub incoming_complete_pending_call_ts_us: i64,
    pub body_serialize_part_len: usize,
    pub body_raw_bytes_lengths: &'a [u32],
}

#[derive(Clone, Copy)]
struct ClosedRuntimeUserRpcBytesRequestRef<'a> {
    from_node: &'a str,
    payload: &'a [u8],
}

impl WireBodyPartsOwner {
    fn new(body: WireMessageBody) -> Result<Self, ClosedSdkConsumerError> {
        let WireMessageBody {
            serialize_part,
            raw_bytes,
        } = body;
        let (raw_lengths, raw_payload) = match raw_bytes.len() {
            0 => (WireBodyRawLengths::Empty, WireBodyRawPayload::Empty),
            1 => {
                let part = raw_bytes.into_iter().next().expect("single raw part missing");
                let len =
                    u32::try_from(part.len()).map_err(|_| ClosedSdkConsumerError::RuntimeDecode {
                        detail: format!("wire raw part too large for u32 length: {}", part.len()),
                    })?;
                (
                    WireBodyRawLengths::Single([len]),
                    WireBodyRawPayload::Single(part),
                )
            }
            _ => {
                let mut raw_lengths = Vec::with_capacity(raw_bytes.len());
                let mut total_raw_len = 0usize;
                for part in &raw_bytes {
                    raw_lengths.push(u32::try_from(part.len()).map_err(|_| {
                        ClosedSdkConsumerError::RuntimeDecode {
                            detail: format!(
                                "wire raw part too large for u32 length: {}",
                                part.len()
                            ),
                        }
                    })?);
                    total_raw_len = total_raw_len.checked_add(part.len()).ok_or_else(|| {
                        ClosedSdkConsumerError::RuntimeDecode {
                            detail: "wire raw parts length overflow".to_string(),
                        }
                    })?;
                }
                let mut raw_concat = Vec::with_capacity(total_raw_len);
                for part in raw_bytes {
                    raw_concat.extend_from_slice(&part);
                }
                (
                    WireBodyRawLengths::Many(raw_lengths),
                    WireBodyRawPayload::Concat(Bytes::from(raw_concat)),
                )
            }
        };
        Ok(Self {
            serialized: serialize_part,
            raw_lengths,
            raw_payload,
        })
    }

    fn view_with_owner_handle(&self, owner_handle: u64) -> ClosedRuntimeWireBodyView {
        let (raw_bytes_ptr, raw_bytes_len) = match &self.raw_payload {
            WireBodyRawPayload::Empty => (0, 0),
            WireBodyRawPayload::Single(bytes) | WireBodyRawPayload::Concat(bytes) => {
                (bytes.as_ptr() as u64, bytes.len())
            }
        };
        let (raw_bytes_lengths_ptr, raw_bytes_lengths_len) = match &self.raw_lengths {
            WireBodyRawLengths::Empty => (0, 0),
            WireBodyRawLengths::Single(length) => (length.as_ptr() as u64, 1),
            WireBodyRawLengths::Many(lengths) => (lengths.as_ptr() as u64, lengths.len()),
        };
        ClosedRuntimeWireBodyView {
            owner_handle,
            serialize_part: ClosedRuntimeRawSlice {
                ptr: self.serialized.as_ptr() as u64,
                len: self.serialized.len(),
            },
            raw_bytes_ptr,
            raw_bytes_len,
            raw_bytes_lengths_ptr,
            raw_bytes_lengths_len,
        }
    }
}

fn decode_dispatch_body_bytes(
    request_view: &ClosedRuntimeDispatchRequestView,
) -> Result<Bytes, P2pError> {
    let body_view = request_view.body;
    let full_body = if body_view.full_body.len == 0 {
        Bytes::new()
    } else {
        if body_view.owner_handle == 0 {
            return Err(P2pError::InvalidMessage {
                detail: "closed SDK dispatch body owner_handle is zero".to_string(),
            });
        }
        let retain_status =
            unsafe { fluxon_commu_closed_dispatch_body_owner_retain(body_view.owner_handle) };
        if retain_status != 0 {
            return Err(P2pError::Other {
                detail: format!(
                    "closed SDK dispatch body owner retain failed: owner_handle={} status={retain_status}",
                    body_view.owner_handle
                ),
            });
        }
        Bytes::from_owner(DispatchBodyOwner {
            owner_handle: body_view.owner_handle,
            ptr: body_view.full_body.ptr as *const u8,
            len: body_view.full_body.len,
        })
    };

    if body_view.serialize_part.len > full_body.len() {
        return Err(P2pError::InvalidMessage {
            detail: format!(
                "closed SDK dispatch serialize_part length overflow: serialize_len={} full_len={}",
                body_view.serialize_part.len,
                full_body.len()
            ),
        });
    }
    let raw_lengths = if body_view.raw_bytes_lengths_len == 0 {
        Vec::new()
    } else {
        if body_view.raw_bytes_lengths_ptr == 0 {
            return Err(P2pError::InvalidMessage {
                detail: "closed SDK dispatch raw_bytes_lengths_ptr is null while len > 0"
                    .to_string(),
            });
        }
        unsafe {
            std::slice::from_raw_parts(
                body_view.raw_bytes_lengths_ptr as *const u32,
                body_view.raw_bytes_lengths_len,
            )
        }
        .to_vec()
    };
    let mut current = body_view.serialize_part.len;
    for raw_len in &raw_lengths {
        let raw_len = *raw_len as usize;
        let end = current
            .checked_add(raw_len)
            .ok_or_else(|| P2pError::InvalidMessage {
                detail: "closed SDK dispatch raw_bytes length overflow".to_string(),
            })?;
        if end > full_body.len() {
            return Err(P2pError::InvalidMessage {
                detail: format!(
                    "closed SDK dispatch raw_bytes out of bounds: end={} full_len={}",
                    end,
                    full_body.len()
                ),
            });
        }
        current = end;
    }
    if current != full_body.len() {
        return Err(P2pError::InvalidMessage {
            detail: format!(
                "closed SDK dispatch body length mismatch: consumed={} full_len={}",
                current,
                full_body.len()
            ),
        });
    }
    Ok(full_body)
}

fn decode_dispatch_request_str<'a>(
    field: &'static str,
    slice: ClosedRuntimeRawSlice,
) -> Result<&'a str, P2pError> {
    std::str::from_utf8(unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) })
        .map_err(|error| P2pError::InvalidMessage {
            detail: format!("closed SDK dispatch {field} is not valid UTF-8: {error}"),
        })
}

fn decode_dispatch_request_ref<'a>(
    request_view: &'a ClosedRuntimeDispatchRequestView,
) -> Result<ClosedRuntimeDispatchRequestRef<'a>, P2pError> {
    let reply_next_hop =
        decode_dispatch_request_str("reply_next_hop", request_view.reply_next_hop)?;
    let logical_source_peer_id = decode_dispatch_request_str(
        "logical_source_peer_id",
        request_view.logical_source_peer_id,
    )?;
    let logical_target_peer_id = decode_dispatch_request_str(
        "logical_target_peer_id",
        request_view.logical_target_peer_id,
    )?;
    let body_raw_bytes_lengths = if request_view.body.raw_bytes_lengths_len == 0 {
        &[]
    } else {
        unsafe {
            std::slice::from_raw_parts(
                request_view.body.raw_bytes_lengths_ptr as *const u32,
                request_view.body.raw_bytes_lengths_len,
            )
        }
    };
    Ok(ClosedRuntimeDispatchRequestRef {
        reply_next_hop,
        msg_id: request_view.msg_id,
        task_id: request_view.task_id,
        logical_source_peer_id,
        logical_source_node_start_time: request_view.logical_source_node_start_time,
        logical_target_peer_id,
        logical_target_node_start_time: request_view.logical_target_node_start_time,
        remaining_hops: request_view.remaining_hops,
        default_resp_transport_policy: request_view.default_resp_transport_policy,
        incoming_frame_recv_done_ts_us: request_view.incoming_frame_recv_done_ts_us,
        incoming_dispatch_enqueued_ts_us: request_view.incoming_dispatch_enqueued_ts_us,
        incoming_dispatch_started_ts_us: request_view.incoming_dispatch_started_ts_us,
        incoming_complete_pending_call_ts_us: request_view.incoming_complete_pending_call_ts_us,
        body_serialize_part_len: request_view.body.serialize_part.len,
        body_raw_bytes_lengths,
    })
}

fn decode_user_rpc_request_bytes<'a>(
    field: &'static str,
    slice: ClosedRuntimeRawSlice,
) -> Result<&'a [u8], String> {
    if slice.len == 0 {
        return Ok(&[]);
    }
    if slice.ptr == 0 {
        return Err(format!(
            "closed SDK host user-rpc {field} ptr is null while len > 0"
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(slice.ptr as *const u8, slice.len) })
}

fn decode_user_rpc_request_ref<'a>(
    request_view: &'a ClosedRuntimeUserRpcBytesRequestView,
) -> Result<ClosedRuntimeUserRpcBytesRequestRef<'a>, String> {
    let from_node_bytes = decode_user_rpc_request_bytes("from_node", request_view.from_node)?;
    let from_node = std::str::from_utf8(from_node_bytes).map_err(|error| {
        format!("closed SDK host user-rpc from_node is not valid UTF-8: {error}")
    })?;
    let payload = decode_user_rpc_request_bytes("payload", request_view.payload)?;
    Ok(ClosedRuntimeUserRpcBytesRequestRef { from_node, payload })
}

struct CallRawObservedOutputOwnerGuard {
    owner_handle: u64,
}

impl CallRawObservedOutputOwnerGuard {
    fn disarm(mut self) -> u64 {
        let owner_handle = self.owner_handle;
        self.owner_handle = 0;
        owner_handle
    }
}

impl Drop for CallRawObservedOutputOwnerGuard {
    fn drop(&mut self) {
        if self.owner_handle == 0 {
            return;
        }
        unsafe {
            let _ = fluxon_commu_closed_call_raw_observed_output_owner_release(self.owner_handle);
        }
    }
}

fn decode_call_raw_observed_output_view(
    payload: &[u8],
) -> Result<ClosedRuntimeCallRawObservedOutput, ClosedSdkConsumerError> {
    if payload.len() != std::mem::size_of::<ClosedRuntimeCallRawObservedOutputView>() {
        return Err(ClosedSdkConsumerError::RuntimeDecode {
            detail: format!(
                "closed SDK call_raw_observed output size mismatch: expected={} actual={}",
                std::mem::size_of::<ClosedRuntimeCallRawObservedOutputView>(),
                payload.len(),
            ),
        });
    }
    let view = unsafe {
        std::ptr::read_unaligned(payload.as_ptr() as *const ClosedRuntimeCallRawObservedOutputView)
    };
    let message_view = view.message;
    if message_view.owner_handle == 0 {
        return Err(ClosedSdkConsumerError::RuntimeDecode {
            detail: "closed SDK call_raw_observed owner_handle is zero".to_string(),
        });
    }
    let owner_guard = CallRawObservedOutputOwnerGuard {
        owner_handle: message_view.owner_handle,
    };
    let from_node = std::str::from_utf8(unsafe {
        std::slice::from_raw_parts(
            message_view.from_node.ptr as *const u8,
            message_view.from_node.len,
        )
    })
    .map(str::to_string)
    .map_err(|error| ClosedSdkConsumerError::RuntimeDecode {
        detail: format!("closed SDK call_raw_observed from_node is not valid UTF-8: {error}"),
    })?;
    let logical_source_peer_id = std::str::from_utf8(unsafe {
        std::slice::from_raw_parts(
            message_view.logical_source_peer_id.ptr as *const u8,
            message_view.logical_source_peer_id.len,
        )
    })
    .map(str::to_string)
    .map_err(|error| ClosedSdkConsumerError::RuntimeDecode {
        detail: format!(
            "closed SDK call_raw_observed logical_source_peer_id is not valid UTF-8: {error}"
        ),
    })?;
    let logical_target_peer_id = std::str::from_utf8(unsafe {
        std::slice::from_raw_parts(
            message_view.logical_target_peer_id.ptr as *const u8,
            message_view.logical_target_peer_id.len,
        )
    })
    .map(str::to_string)
    .map_err(|error| ClosedSdkConsumerError::RuntimeDecode {
        detail: format!(
            "closed SDK call_raw_observed logical_target_peer_id is not valid UTF-8: {error}"
        ),
    })?;
    let raw_bytes_lengths = if message_view.body.raw_bytes_lengths_len == 0 {
        Vec::new()
    } else {
        if message_view.body.raw_bytes_lengths_ptr == 0 {
            return Err(ClosedSdkConsumerError::RuntimeDecode {
                detail: "closed SDK call_raw_observed raw_bytes_lengths_ptr is null while len > 0"
                    .to_string(),
            });
        }
        unsafe {
            std::slice::from_raw_parts(
                message_view.body.raw_bytes_lengths_ptr as *const u32,
                message_view.body.raw_bytes_lengths_len,
            )
        }
        .to_vec()
    };
    if message_view.body.full_body.len == 0 {
        if message_view.body.serialize_part.len != 0 || !raw_bytes_lengths.is_empty() {
            return Err(ClosedSdkConsumerError::RuntimeDecode {
                detail: "closed SDK call_raw_observed empty body has non-empty subviews"
                    .to_string(),
            });
        }
    } else {
        if message_view.body.full_body.ptr == 0 {
            return Err(ClosedSdkConsumerError::RuntimeDecode {
                detail: "closed SDK call_raw_observed full_body ptr is null while len > 0"
                    .to_string(),
            });
        }
        if message_view.body.serialize_part.ptr != message_view.body.full_body.ptr {
            return Err(ClosedSdkConsumerError::RuntimeDecode {
                detail: "closed SDK call_raw_observed serialize_part does not start at full_body"
                    .to_string(),
            });
        }
    }
    if message_view.body.serialize_part.len > message_view.body.full_body.len {
        return Err(ClosedSdkConsumerError::RuntimeDecode {
            detail: format!(
                "closed SDK call_raw_observed serialize_part overflow: serialize_len={} full_len={}",
                message_view.body.serialize_part.len,
                message_view.body.full_body.len,
            ),
        });
    }
    let raw_total = raw_bytes_lengths
        .iter()
        .try_fold(0usize, |acc, raw_len| acc.checked_add(*raw_len as usize))
        .ok_or_else(|| ClosedSdkConsumerError::RuntimeDecode {
            detail: "closed SDK call_raw_observed raw_bytes length overflow".to_string(),
        })?;
    let expected_full_len =
        message_view
            .body
            .serialize_part
            .len
            .checked_add(raw_total)
            .ok_or_else(|| ClosedSdkConsumerError::RuntimeDecode {
                detail: "closed SDK call_raw_observed body length overflow".to_string(),
            })?;
    if expected_full_len != message_view.body.full_body.len {
        return Err(ClosedSdkConsumerError::RuntimeDecode {
            detail: format!(
                "closed SDK call_raw_observed body length mismatch: expected={} full_len={}",
                expected_full_len,
                message_view.body.full_body.len,
            ),
        });
    }

    let body = if message_view.body.full_body.len == 0 {
        Bytes::new()
    } else {
        let owner_handle = owner_guard.disarm();
        Bytes::from_owner(CallRawObservedOutputBodyOwner {
            owner_handle,
            ptr: message_view.body.full_body.ptr as *const u8,
            len: message_view.body.full_body.len,
        })
    };
    let request_path_kind = match view.observe.request_path_kind {
        0 => fluxon_commu_contract::p2p::rpc::UserRpcTransportPathKind::Unknown,
        1 => fluxon_commu_contract::p2p::rpc::UserRpcTransportPathKind::Fast,
        2 => fluxon_commu_contract::p2p::rpc::UserRpcTransportPathKind::Slow,
        other => {
            return Err(ClosedSdkConsumerError::RuntimeDecode {
                detail: format!(
                    "closed SDK call_raw_observed request_path_kind is invalid: {}",
                    other
                ),
            });
        }
    };
    Ok(ClosedRuntimeCallRawObservedOutput {
        message: ClosedRuntimeWireIncomingMessage {
            from_node,
            head: fluxon_commu_contract::p2p::MsgPackHeadMeta {
                msg_id: message_view.msg_id,
                task_id: message_view.task_id,
                relay: fluxon_commu_contract::p2p::MsgPackRelay {
                    logical_source_peer_id,
                    logical_source_node_start_time: message_view.logical_source_node_start_time,
                    logical_target_peer_id,
                    logical_target_node_start_time: message_view.logical_target_node_start_time,
                    remaining_hops: message_view.remaining_hops,
                },
                serialize_part_length: message_view.body.serialize_part.len as u32,
                raw_bytes_length: raw_bytes_lengths,
            },
            body,
            local_observe: fluxon_commu_contract::p2p::WireTransportLocalObserve {
                frame_recv_done_ts_us: message_view.local_observe.frame_recv_done_ts_us,
                dispatch_enqueued_ts_us: message_view.local_observe.dispatch_enqueued_ts_us,
                dispatch_started_ts_us: message_view.local_observe.dispatch_started_ts_us,
                complete_pending_call_ts_us: message_view
                    .local_observe
                    .complete_pending_call_ts_us,
            },
        },
        observe: fluxon_commu_contract::ClosedRuntimeRpcCallTransportObserveTrace {
            caller_submit_us: view.observe.caller_submit_us,
            caller_submit_ts_us: view.observe.caller_submit_ts_us,
            request_path_kind,
        },
    })
}

fn c_static_string(
    ptr: *const c_char,
    field: &'static str,
) -> Result<String, ClosedSdkConsumerError> {
    if ptr.is_null() {
        return Err(ClosedSdkConsumerError::NullStaticString { field });
    }

    let value = unsafe { CStr::from_ptr(ptr) };
    value
        .to_str()
        .map(str::to_string)
        .map_err(|source| ClosedSdkConsumerError::InvalidUtf8 { field, source })
}

pub fn sdk_schema_version() -> u32 {
    unsafe { fluxon_commu_closed_sdk_schema_version() }
}

pub fn abi_version() -> u32 {
    unsafe { fluxon_commu_closed_abi_version() }
}

pub fn sdk_version() -> Result<String, ClosedSdkConsumerError> {
    c_static_string(unsafe { fluxon_commu_closed_sdk_version() }, "sdk_version")
}

pub fn required_open_surface_version() -> Result<String, ClosedSdkConsumerError> {
    c_static_string(
        unsafe { fluxon_commu_closed_required_open_surface_version() },
        "required_open_surface_version",
    )
}

pub fn boundary_mode() -> Result<String, ClosedSdkConsumerError> {
    c_static_string(
        unsafe { fluxon_commu_closed_boundary_mode() },
        "boundary_mode",
    )
}

pub fn assert_abi_compatible() -> Result<(), ClosedSdkConsumerError> {
    let actual = abi_version();
    let status =
        unsafe { fluxon_commu_closed_check_abi_compatible(FLUXON_COMMU_CLOSED_ABI_VERSION) };
    if status == 0 {
        Ok(())
    } else {
        Err(ClosedSdkConsumerError::AbiMismatch {
            expected: FLUXON_COMMU_CLOSED_ABI_VERSION,
            actual,
        })
    }
}

pub fn query_version() -> Result<ClosedSdkVersionInfo, ClosedSdkConsumerError> {
    let raw = unsafe { fluxon_commu_closed_query_version() };
    Ok(ClosedSdkVersionInfo {
        abi_version: raw.abi_version,
        sdk_schema_version: raw.sdk_schema_version,
        sdk_version: c_static_string(raw.sdk_version, "sdk_version")?,
        required_open_surface_version: c_static_string(
            raw.required_open_surface_version,
            "required_open_surface_version",
        )?,
        boundary_mode: c_static_string(raw.boundary_mode, "boundary_mode")?,
    })
}

pub fn runtime_anchor() -> ClosedSdkRuntimeAnchor {
    let raw = unsafe { fluxon_commu_closed_runtime_anchor() };
    ClosedSdkRuntimeAnchor {
        cluster_manager_size: raw.cluster_manager_size,
        p2p_module_size: raw.p2p_module_size,
        transfer_engine_core_size: raw.transfer_engine_core_size,
    }
}

pub fn runtime_anchor_checksum() -> u64 {
    unsafe { fluxon_commu_closed_runtime_anchor_checksum() }
}

struct ClosedRuntimeOwnedBytesOwner {
    bytes: FluxonCommuClosedOwnedBytes,
}

unsafe impl Send for ClosedRuntimeOwnedBytesOwner {}
unsafe impl Sync for ClosedRuntimeOwnedBytesOwner {}

impl AsRef<[u8]> for ClosedRuntimeOwnedBytesOwner {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.bytes.ptr, self.bytes.len) }
    }
}

impl Drop for ClosedRuntimeOwnedBytesOwner {
    fn drop(&mut self) {
        unsafe {
            fluxon_commu_closed_runtime_release_owned_bytes(self.bytes);
        }
    }
}

fn owned_bytes_into_bytes(bytes: FluxonCommuClosedOwnedBytes) -> Bytes {
    if bytes.ptr.is_null() {
        return Bytes::new();
    }
    Bytes::from_owner(ClosedRuntimeOwnedBytesOwner { bytes })
}

fn encode_owned_bytes(bytes: Vec<u8>) -> FluxonCommuClosedOwnedBytes {
    let mut bytes = bytes;
    let payload = FluxonCommuClosedOwnedBytes {
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
        cap: bytes.capacity(),
    };
    std::mem::forget(bytes);
    payload
}

extern "C" fn release_host_owned_bytes(bytes: FluxonCommuClosedOwnedBytes) {
    if bytes.ptr.is_null() {
        return;
    }
    unsafe {
        drop(Vec::from_raw_parts(bytes.ptr, bytes.len, bytes.cap));
    }
}

extern "C" fn retain_host_body_owner(owner_handle: u64) -> i32 {
    if unsafe { retain_wire_body_owner_ref(owner_handle) } {
        0
    } else {
        -1
    }
}

extern "C" fn release_host_body_owner(owner_handle: u64) {
    unsafe {
        let _ = release_wire_body_owner_ref(owner_handle);
    }
}

extern "C" fn release_host_callback(callback_handle: u64) {
    unsafe {
        drop_host_callback_entry(callback_handle);
    }
}

fn host_user_rpc_error(detail: impl ToString) -> UserRpcBytesError {
    let error = P2pError::Other {
        detail: detail.to_string(),
    };
    UserRpcBytesError {
        error_code: error.code(),
        error_json: serde_json::to_string(&error)
            .unwrap_or_else(|_| "{\"type\":\"Other\"}".to_string()),
    }
}

fn host_user_rpc_completion_result(
    result: Result<fluxon_commu_contract::p2p::rpc::UserRpcBytesOutput, UserRpcBytesError>,
) -> FluxonCommuClosedUserRpcCompletionResult {
    match result {
        Ok(output) => FluxonCommuClosedUserRpcCompletionResult {
            status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK,
            error_code: 0,
            _reserved: 0,
            payload: encode_owned_bytes(output.payload),
            local_observe: output.local_observe.into(),
        },
        Err(error) => FluxonCommuClosedUserRpcCompletionResult {
            status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_USER_RPC_ERR,
            error_code: error.error_code,
            _reserved: 0,
            payload: encode_owned_bytes(error.error_json.into_bytes()),
            local_observe: ClosedRuntimeUserRpcHandlerLocalObserveView::default(),
        },
    }
}

fn host_async_bytes_completion_result(
    result: Result<Vec<u8>, String>,
) -> FluxonCommuClosedRuntimeCompletionResult {
    match result {
        Ok(payload) => FluxonCommuClosedRuntimeCompletionResult {
            status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK,
            payload: encode_owned_bytes(payload),
        },
        Err(detail) => FluxonCommuClosedRuntimeCompletionResult {
            status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR,
            payload: encode_owned_bytes(detail.into_bytes()),
        },
    }
}

fn host_callback_failure(detail: impl ToString) -> FluxonCommuClosedRuntimeCompletionResult {
    FluxonCommuClosedRuntimeCompletionResult {
        status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR,
        payload: encode_owned_bytes(detail.to_string().into_bytes()),
    }
}

fn host_user_rpc_callback_failure(
    detail: impl ToString,
) -> FluxonCommuClosedUserRpcCompletionResult {
    FluxonCommuClosedUserRpcCompletionResult {
        status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR,
        error_code: 0,
        _reserved: 0,
        payload: encode_owned_bytes(detail.to_string().into_bytes()),
        local_observe: ClosedRuntimeUserRpcHandlerLocalObserveView::default(),
    }
}

fn host_dispatch_completion_result(
    result: Result<(), P2pError>,
) -> FluxonCommuClosedRuntimeCompletionResult {
    let response = match result {
        Ok(()) => ClosedRuntimeDispatchResponse::Ok,
        Err(error) => ClosedRuntimeDispatchResponse::Err {
            error_code: error.code(),
            error_json: serde_json::to_string(&error)
                .unwrap_or_else(|_| "{\"type\":\"Other\"}".to_string()),
        },
    };
    FluxonCommuClosedRuntimeCompletionResult {
        status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK,
        payload: encode_owned_bytes(bitcode::encode(&response)),
    }
}

fn ensure_host_callbacks_installed() -> Result<(), ClosedSdkConsumerError> {
    let install_result = HOST_CALLBACK_INSTALL_RESULT.get_or_init(|| {
        let status = unsafe {
            fluxon_commu_closed_install_host_callbacks_v1(FluxonCommuClosedHostCallbacksV1 {
                abi_version: FLUXON_COMMU_CLOSED_HOST_CALLBACKS_ABI_VERSION,
                user_rpc_bytes_handle_sync: Some(host_user_rpc_bytes_handler_invoke_sync),
                user_rpc_bytes_handle_async: Some(host_user_rpc_bytes_handler_invoke),
                async_bytes_handle_async: Some(host_async_bytes_handler_invoke),
                dispatch_handle_async: Some(host_dispatch_handler_invoke),
                retain_host_body_owner: Some(retain_host_body_owner),
                release_host_body_owner: Some(release_host_body_owner),
                release_host_owned_bytes: Some(release_host_owned_bytes),
                release_host_callback: Some(release_host_callback),
            })
        };
        if status == 0 { Ok(()) } else { Err(status) }
    });
    install_result
        .as_ref()
        .map(|_| ())
        .map_err(|status| ClosedSdkConsumerError::HostCallbackInstallRejected { status: *status })
}

fn run_host_user_rpc_sync_callback(
    handler: Arc<dyn UserRpcBytesHandler>,
    from_node: fluxon_commu_contract::NodeID,
    payload: &[u8],
) -> Result<fluxon_commu_contract::p2p::rpc::UserRpcBytesOutput, UserRpcBytesError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        handler.handle(from_node, payload)
    })) {
        Ok(result) => result,
        Err(_) => Err(host_user_rpc_error(
            "closed SDK host sync user-rpc callback panicked",
        )),
    }
}

async fn run_host_user_rpc_callback(
    callback: Arc<dyn UserRpcBytesAsyncHandler>,
    from_node: fluxon_commu_contract::NodeID,
    payload: Vec<u8>,
) -> Result<fluxon_commu_contract::p2p::rpc::UserRpcBytesOutput, UserRpcBytesError> {
    callback.handle(from_node, payload).await
}

fn run_host_dispatch_callback(
    callback: RegisteredHostCallback,
    request: ClosedRuntimeDispatchRequestRef<'_>,
    body: Bytes,
) -> Result<(), P2pError> {
    match callback {
        RegisteredHostCallback::DispatchRaw(handler) => handler(request, body),
        RegisteredHostCallback::UserRpcBytesSync(_)
        | RegisteredHostCallback::UserRpcBytesAsync(_)
        | RegisteredHostCallback::AsyncBytes(_) => Err(P2pError::Other {
            detail:
                "closed SDK user-rpc callback was invoked through the dispatch callback entrypoint"
                    .to_string(),
        }),
    }
}

extern "C" fn host_user_rpc_bytes_handler_invoke(
    callback_handle: u64,
    request_ptr: *const ClosedRuntimeUserRpcBytesRequestView,
    user_data: *mut c_void,
    callback: Option<FluxonCommuClosedHostUserRpcBytesCompletionCallback>,
) -> i32 {
    let Some(callback) = callback else {
        return -1;
    };
    let Some(registered) = (unsafe { retain_host_callback_entry(callback_handle) }) else {
        callback(
            user_data,
            host_user_rpc_callback_failure(format!(
                "closed SDK host callback handle not found: {}",
                callback_handle
            )),
        );
        return 0;
    };
    if request_ptr.is_null() {
        callback(
            user_data,
            host_user_rpc_callback_failure("request_ptr is null"),
        );
        return 0;
    }
    let request = unsafe { &*request_ptr };
    let user_data = user_data as usize;
    let runtime = registered.runtime.clone();
    match &registered.callback {
        RegisteredHostCallback::UserRpcBytesSync(handler) => {
            let request = match decode_user_rpc_request_ref(request) {
                Ok(request) => request,
                Err(error) => {
                    callback(
                        user_data as *mut c_void,
                        host_user_rpc_callback_failure(error),
                    );
                    return 0;
                }
            };
            let result = run_host_user_rpc_sync_callback(
                Arc::clone(handler),
                request.from_node.to_string().into(),
                request.payload,
            );
            callback(
                user_data as *mut c_void,
                host_user_rpc_completion_result(result),
            );
        }
        RegisteredHostCallback::UserRpcBytesAsync(handler) => {
            let runtime =
                runtime.expect("user-rpc callback runtime must be captured at registration");
            let request = match decode_user_rpc_request_ref(request) {
                Ok(request) => request,
                Err(error) => {
                    callback(
                        user_data as *mut c_void,
                        host_user_rpc_callback_failure(error),
                    );
                    return 0;
                }
            };
            let handler = Arc::clone(handler);
            let from_node = request.from_node.to_string();
            let payload = request.payload.to_vec();
            runtime.spawn(async move {
                let result = run_host_user_rpc_callback(handler, from_node.into(), payload).await;
                callback(
                    user_data as *mut c_void,
                    host_user_rpc_completion_result(result),
                );
            });
        }
        RegisteredHostCallback::AsyncBytes(_) | RegisteredHostCallback::DispatchRaw(_) => {
            callback(
                user_data as *mut c_void,
                host_user_rpc_callback_failure(
                    "closed SDK non-user-rpc callback was invoked through the user-rpc callback entrypoint",
                ),
            );
        }
    }
    0
}

extern "C" fn host_user_rpc_bytes_handler_invoke_sync(
    callback_handle: u64,
    request_ptr: *const ClosedRuntimeUserRpcBytesRequestView,
    out_result: *mut FluxonCommuClosedUserRpcCompletionResult,
) -> i32 {
    if out_result.is_null() {
        return -1;
    }
    let result = if request_ptr.is_null() {
        host_user_rpc_callback_failure("request_ptr is null")
    } else if let Some(registered) = unsafe { retain_host_callback_entry(callback_handle) } {
        let request = unsafe { &*request_ptr };
        match &registered.callback {
            RegisteredHostCallback::UserRpcBytesSync(handler) => {
                match decode_user_rpc_request_ref(request) {
                    Ok(request) => {
                        host_user_rpc_completion_result(run_host_user_rpc_sync_callback(
                            Arc::clone(handler),
                            request.from_node.to_string().into(),
                            request.payload,
                        ))
                    }
                    Err(error) => host_user_rpc_callback_failure(error),
                }
            }
            RegisteredHostCallback::UserRpcBytesAsync(_)
            | RegisteredHostCallback::AsyncBytes(_)
            | RegisteredHostCallback::DispatchRaw(_) => host_user_rpc_callback_failure(
                "closed SDK non-sync-user-rpc callback was invoked through the sync user-rpc callback entrypoint",
            ),
        }
    } else {
        host_user_rpc_callback_failure(format!(
            "closed SDK host callback handle not found: {}",
            callback_handle
        ))
    };
    unsafe {
        out_result.write(result);
    }
    0
}

extern "C" fn host_async_bytes_handler_invoke(
    callback_handle: u64,
    request_ptr: *const u8,
    request_len: usize,
    user_data: *mut c_void,
    callback: Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
) -> i32 {
    let Some(callback) = callback else {
        return -1;
    };
    let Some(registered) = (unsafe { retain_host_callback_entry(callback_handle) }) else {
        callback(
            user_data,
            host_callback_failure(format!(
                "closed SDK host callback handle not found: {}",
                callback_handle
            )),
        );
        return 0;
    };
    if request_ptr.is_null() {
        callback(user_data, host_callback_failure("request_ptr is null"));
        return 0;
    }
    let request_bytes = unsafe { std::slice::from_raw_parts(request_ptr, request_len) };
    let user_data = user_data as usize;
    let runtime = registered
        .runtime
        .clone()
        .expect("async-bytes callback runtime must be captured at registration");
    match &registered.callback {
        RegisteredHostCallback::AsyncBytes(handler) => {
            let handler = Arc::clone(handler);
            let request_bytes = request_bytes.to_vec();
            runtime.spawn(async move {
                let result = handler(request_bytes).await;
                callback(
                    user_data as *mut c_void,
                    host_async_bytes_completion_result(result),
                );
            });
        }
        RegisteredHostCallback::UserRpcBytesSync(_)
        | RegisteredHostCallback::UserRpcBytesAsync(_)
        | RegisteredHostCallback::DispatchRaw(_) => {
            callback(
                user_data as *mut c_void,
                host_callback_failure(
                    "closed SDK non-async-bytes callback was invoked through the async-bytes callback entrypoint",
                ),
            );
        }
    }
    0
}

extern "C" fn host_dispatch_handler_invoke(
    callback_handle: u64,
    request_ptr: *const u8,
    request_len: usize,
    user_data: *mut c_void,
    callback: Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
) -> i32 {
    let Some(callback) = callback else {
        return -1;
    };
    if request_ptr.is_null() {
        callback(user_data, host_callback_failure("request_ptr is null"));
        return 0;
    }
    if request_len != std::mem::size_of::<ClosedRuntimeDispatchRequestView>() {
        callback(
            user_data,
            host_callback_failure(format!(
                "closed SDK host dispatch request size mismatch: expected={} actual={}",
                std::mem::size_of::<ClosedRuntimeDispatchRequestView>(),
                request_len
            )),
        );
        return 0;
    }
    let request_view = unsafe { &*(request_ptr as *const ClosedRuntimeDispatchRequestView) };
    let request = match decode_dispatch_request_ref(request_view) {
        Ok(request) => request,
        Err(error) => {
            callback(user_data, host_dispatch_completion_result(Err(error)));
            return 0;
        }
    };
    let body = match decode_dispatch_body_bytes(request_view) {
        Ok(body) => body,
        Err(error) => {
            callback(user_data, host_dispatch_completion_result(Err(error)));
            return 0;
        }
    };
    let Some(registered) = (unsafe { retain_host_callback_entry(callback_handle) }) else {
        callback(
            user_data,
            host_callback_failure(format!(
                "closed SDK host callback handle not found: {}",
                callback_handle
            )),
        );
        return 0;
    };
    let _runtime = registered
        .runtime
        .clone()
        .expect("dispatch callback runtime must be captured at registration");
    let result = run_host_dispatch_callback(registered.callback.clone(), request, body);
    callback(
        user_data,
        match result {
            Ok(()) => FluxonCommuClosedRuntimeCompletionResult {
                status_code: FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK,
                payload: FluxonCommuClosedOwnedBytes {
                    ptr: std::ptr::null_mut(),
                    len: 0,
                    cap: 0,
                },
            },
            Err(error) => host_dispatch_completion_result(Err(error)),
        },
    );
    0
}

fn register_host_user_rpc_callback(
    callback: RegisteredHostCallback,
) -> Result<ClosedRuntimeHostCallbackHandle, ClosedSdkConsumerError> {
    ensure_host_callbacks_installed()?;
    alloc_host_callback_entry(callback)
}

fn register_host_dispatch_callback(
    callback: Arc<
        dyn for<'a> Fn(ClosedRuntimeDispatchRequestRef<'a>, Bytes) -> Result<(), P2pError>
            + Send
            + Sync,
    >,
) -> Result<ClosedRuntimeHostCallbackHandle, ClosedSdkConsumerError> {
    ensure_host_callbacks_installed()?;
    alloc_host_callback_entry(RegisteredHostCallback::DispatchRaw(callback))
}

pub fn register_host_async_bytes_callback<F, Fut>(
    handler: F,
) -> Result<ClosedRuntimeHostCallbackHandle, ClosedSdkConsumerError>
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>, String>> + Send + 'static,
{
    ensure_host_callbacks_installed()?;
    let wrapped =
        Arc::new(move |request: Vec<u8>| -> HostAsyncBytesFuture { Box::pin(handler(request)) });
    alloc_host_callback_entry(RegisteredHostCallback::AsyncBytes(wrapped))
}

pub fn release_host_callback_handle(handle: ClosedRuntimeHostCallbackHandle) {
    unsafe {
        drop_host_callback_entry(handle.raw);
    }
}

struct RuntimeCompletionState {
    sender: tokio::sync::oneshot::Sender<(i32, Bytes)>,
    keepalive: Option<Box<dyn Any + Send>>,
}

extern "C" fn runtime_completion_callback(
    user_data: *mut c_void,
    result: FluxonCommuClosedRuntimeCompletionResult,
) {
    let state = unsafe { Box::from_raw(user_data.cast::<RuntimeCompletionState>()) };
    let RuntimeCompletionState { sender, keepalive } = *state;
    let _keepalive = keepalive;
    let _ = sender.send((result.status_code, owned_bytes_into_bytes(result.payload)));
}

async fn invoke_completion_async_with_keepalive(
    keepalive: Option<Box<dyn Any + Send>>,
    submit: impl FnOnce(
        *mut c_void,
        Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
    ) -> i32,
) -> Result<(i32, Bytes), ClosedSdkConsumerError> {
    let (sender, receiver) = tokio::sync::oneshot::channel::<(i32, Bytes)>();
    let user_data = Box::into_raw(Box::new(RuntimeCompletionState { sender, keepalive }))
        .cast::<c_void>();
    let submit_status = submit(user_data, Some(runtime_completion_callback));
    if submit_status != 0 {
        unsafe {
            drop(Box::from_raw(user_data.cast::<RuntimeCompletionState>()));
        }
        return Err(ClosedSdkConsumerError::RuntimeSubmitRejected {
            status: submit_status,
        });
    }
    receiver
        .await
        .map_err(|_| ClosedSdkConsumerError::RuntimeCompletionDropped)
}

async fn invoke_completion_async_with_post_submit(
    submit: impl FnOnce(
        *mut c_void,
        Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
    ) -> i32,
    on_submit_accepted: impl FnOnce(),
) -> Result<(i32, Bytes), ClosedSdkConsumerError> {
    let (sender, receiver) = tokio::sync::oneshot::channel::<(i32, Bytes)>();
    let user_data = Box::into_raw(Box::new(RuntimeCompletionState {
        sender,
        keepalive: None,
    }))
    .cast::<c_void>();
    let submit_status = submit(user_data, Some(runtime_completion_callback));
    if submit_status != 0 {
        unsafe {
            drop(Box::from_raw(user_data.cast::<RuntimeCompletionState>()));
        }
        return Err(ClosedSdkConsumerError::RuntimeSubmitRejected {
            status: submit_status,
        });
    }
    on_submit_accepted();
    receiver
        .await
        .map_err(|_| ClosedSdkConsumerError::RuntimeCompletionDropped)
}

async fn invoke_completion_async(
    submit: impl FnOnce(
        *mut c_void,
        Option<extern "C" fn(*mut c_void, FluxonCommuClosedRuntimeCompletionResult)>,
    ) -> i32,
) -> Result<(i32, Bytes), ClosedSdkConsumerError> {
    invoke_completion_async_with_keepalive(None, submit).await
}

pub async fn runtime_invoke(
    request: &ClosedRuntimeRequest,
) -> Result<ClosedRuntimeResponse, ClosedSdkConsumerError> {
    let encoded_request = bitcode::encode(request);
    let (status_code, payload) = invoke_completion_async(|user_data, callback| unsafe {
        fluxon_commu_closed_runtime_call_async(
            encoded_request.as_ptr(),
            encoded_request.len(),
            user_data,
            callback,
        )
    })
    .await?;
    match status_code {
        FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK => {
            bitcode::decode::<ClosedRuntimeResponse>(payload.as_ref()).map_err(|error| {
                ClosedSdkConsumerError::RuntimeDecode {
                    detail: error.to_string(),
                }
            })
        }
        FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR => {
            let error = bitcode::decode::<ClosedRuntimeError>(payload.as_ref()).map_err(
                |decode_error| ClosedSdkConsumerError::RuntimeDecode {
                    detail: decode_error.to_string(),
                },
            )?;
            Err(ClosedSdkConsumerError::RuntimeError { error })
        }
        status => Err(ClosedSdkConsumerError::RuntimeSubmitRejected { status }),
    }
}

pub async fn construct_cluster_manager_handle(
    arg: fluxon_commu_contract::ClusterManagerNewArg,
) -> Result<ClosedRuntimeHandle, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ConstructClusterManager { arg }).await? {
        ClosedRuntimeResponse::Constructed { handle } => Ok(handle),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn construct_p2p_module_handle(
    cluster_manager: ClosedRuntimeHandle,
    arg: fluxon_commu_contract::P2pModuleNewArg,
) -> Result<ClosedRuntimeHandle, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ConstructP2pModule {
        cluster_manager,
        arg,
    })
    .await?
    {
        ClosedRuntimeResponse::Constructed { handle } => Ok(handle),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn construct_transfer_engine_handle(
    cluster_manager: ClosedRuntimeHandle,
    p2p_module: ClosedRuntimeHandle,
    arg: fluxon_commu_contract::ClientTransferEngineNewArg,
) -> Result<ClosedRuntimeHandle, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ConstructClientTransferEngineCore {
        cluster_manager,
        p2p_module,
        arg,
    })
    .await?
    {
        ClosedRuntimeResponse::Constructed { handle } => Ok(handle),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn cluster_manager_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeClusterManagerCall,
) -> Result<ClosedRuntimeClusterManagerResponse, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ClusterManagerCall { handle, call }).await? {
        ClosedRuntimeResponse::ClusterManager { response } => Ok(response),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn p2p_module_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeP2pCall,
) -> Result<ClosedRuntimeP2pResponse, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::P2pModuleCall { handle, call }).await? {
        ClosedRuntimeResponse::P2p { response } => Ok(response),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_call(
    handle: ClosedRuntimeHandle,
    call: ClosedRuntimeTransferEngineCall,
) -> Result<ClosedRuntimeTransferEngineResponse, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::TransferEngineCall { handle, call }).await? {
        ClosedRuntimeResponse::TransferEngine { response } => Ok(response),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub fn register_transfer_engine_open_runtime_callback<F, Fut>(
    handler: F,
) -> Result<ClosedRuntimeHostCallbackHandle, ClosedSdkConsumerError>
where
    F: Fn(ClosedRuntimeTransferEngineOpenRuntimeRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ClosedRuntimeTransferEngineOpenRuntimeResponse, String>>
        + Send
        + 'static,
{
    let handler = Arc::new(handler);
    register_host_async_bytes_callback(move |request_bytes| {
        let handler = handler.clone();
        async move {
            let request =
                bitcode::decode::<ClosedRuntimeTransferEngineOpenRuntimeRequest>(&request_bytes)
                    .map_err(|error| {
                        format!(
                            "closed SDK transfer-engine open-runtime request decode failed: {error}"
                        )
                    })?;
            let response = (handler.as_ref())(request).await?;
            Ok(bitcode::encode(&response))
        }
    })
}

pub async fn transfer_engine_init2_for_init_dag<F, Fut>(
    handle: ClosedRuntimeHandle,
    supports_local_segment_transfer: bool,
    handler: F,
) -> Result<(), ClosedSdkConsumerError>
where
    F: Fn(ClosedRuntimeTransferEngineOpenRuntimeRequest) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<ClosedRuntimeTransferEngineOpenRuntimeResponse, String>>
        + Send
        + 'static,
{
    let callback = register_transfer_engine_open_runtime_callback(handler)?;
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::Init2ForInitDag {
            open_runtime_callback: callback,
            supports_local_segment_transfer,
        },
    )
    .await
    {
        Ok(ClosedRuntimeTransferEngineResponse::Unit) => Ok(()),
        Ok(other) => {
            release_host_callback_handle(callback);
            Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
                detail: format!("{other:?}"),
            })
        }
        Err(error) => {
            release_host_callback_handle(callback);
            Err(error)
        }
    }
}

pub async fn transfer_engine_ensure_started_if_needed(
    handle: ClosedRuntimeHandle,
) -> Result<(), ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::EnsureStartedIfNeeded,
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_current_runtime_config(
    handle: ClosedRuntimeHandle,
) -> Result<fluxon_commu_contract::ClientTransferEngineRuntimeConfig, ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::CurrentRuntimeConfig,
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::RuntimeConfigValue(config) => Ok(config),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_update_runtime_config(
    handle: ClosedRuntimeHandle,
    config: fluxon_commu_contract::ClientTransferEngineRuntimeConfig,
) -> Result<(), ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::UpdateRuntimeConfig { config },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_update_enabled_rdma_devices(
    handle: ClosedRuntimeHandle,
    enabled_devices: Vec<String>,
) -> Result<(), ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::UpdateEnabledRdmaDevices { enabled_devices },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_sync_desired_peers(
    handle: ClosedRuntimeHandle,
    desired_peers: Vec<ClosedRuntimeDesiredTransferPeer>,
) -> Result<(), ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::SyncDesiredPeers { desired_peers },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_register_local_segment(
    handle: ClosedRuntimeHandle,
    allocated_addr: u64,
    allocated_size: u64,
) -> Result<(), ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::RegisterLocalSegment {
            allocated_addr,
            allocated_size,
        },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_unregister_local_segment(
    handle: ClosedRuntimeHandle,
    allocated_addr: u64,
    allocated_size: u64,
) -> Result<(), ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::UnregisterLocalSegment {
            allocated_addr,
            allocated_size,
        },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_transfer_data_no_copy(
    handle: ClosedRuntimeHandle,
    peer_node: Option<String>,
    peer_src_or_target: bool,
    src_addr: u64,
    target_addr: u64,
    len: u64,
    initial_local_segment_guard_handle: Option<u64>,
) -> Result<fluxon_commu_contract::TransferBreakdown, ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::TransferDataNoCopy {
            peer_node,
            peer_src_or_target,
            src_addr,
            target_addr,
            len,
            initial_local_segment_guard_handle,
        },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::TransferBreakdownValue(breakdown) => Ok(breakdown),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_try_send_wire_direct(
    handle: ClosedRuntimeHandle,
    peer_gen: ClosedRuntimePeerGen,
    peer_transfer_backend_epoch: u64,
    wire_bytes: Vec<u8>,
) -> Result<bool, ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::TrySendWireDirect {
            peer_gen,
            peer_transfer_backend_epoch,
            wire_bytes,
        },
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::BoolValue(value) => Ok(value),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn transfer_engine_drain_inbound_fast_path_messages(
    handle: ClosedRuntimeHandle,
) -> Result<Vec<fluxon_commu_contract::TransferRpcFastPathInbound>, ClosedSdkConsumerError> {
    match transfer_engine_call(
        handle,
        ClosedRuntimeTransferEngineCall::DrainInboundFastPathMessages,
    )
    .await?
    {
        ClosedRuntimeTransferEngineResponse::InboundFastPathMessagesValue(messages) => Ok(messages),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn p2p_attach_transfer_engine(
    p2p_module: ClosedRuntimeHandle,
    transfer_engine: ClosedRuntimeHandle,
) -> Result<(), ClosedSdkConsumerError> {
    match p2p_module_call(
        p2p_module,
        ClosedRuntimeP2pCall::AttachTransferEngine { transfer_engine },
    )
    .await?
    {
        ClosedRuntimeP2pResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn p2p_register_dispatch(
    p2p_module: ClosedRuntimeHandle,
    msg_id: u32,
    handler: Arc<
        dyn for<'a> Fn(ClosedRuntimeDispatchRequestRef<'a>, Bytes) -> Result<(), P2pError>
            + Send
            + Sync,
    >,
) -> Result<(), ClosedSdkConsumerError> {
    let callback = register_host_dispatch_callback(handler)?;
    match p2p_module_call(
        p2p_module,
        ClosedRuntimeP2pCall::RegisterDispatch { msg_id, callback },
    )
    .await
    {
        Ok(ClosedRuntimeP2pResponse::Unit) => Ok(()),
        Ok(other) => {
            release_host_callback_handle(callback);
            Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
                detail: format!("{other:?}"),
            })
        }
        Err(error) => {
            release_host_callback_handle(callback);
            Err(error)
        }
    }
}

pub async fn p2p_register_rpc_response_msg_id(
    p2p_module: ClosedRuntimeHandle,
    msg_id: u32,
) -> Result<(), ClosedSdkConsumerError> {
    match p2p_module_call(
        p2p_module,
        ClosedRuntimeP2pCall::RegisterRpcResponseMsgId { msg_id },
    )
    .await?
    {
        ClosedRuntimeP2pResponse::Unit => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn p2p_call_raw_observed(
    p2p_module: ClosedRuntimeHandle,
    node: String,
    msg_id: u32,
    body: WireMessageBody,
    timeout_ms: Option<u64>,
    transport_policy: fluxon_commu_contract::RpcTransportPolicy,
) -> Result<ClosedRuntimeCallRawObservedOutput, ClosedSdkConsumerError> {
    let body_owner = WireBodyPartsOwner::new(body)?;
    let body_owner_handle = into_wire_body_owner_handle(body_owner);
    let body_owner_ref = unsafe { &*(body_owner_handle as *const WireBodyPartsOwner) };
    let request = ClosedRuntimeP2pCallRawObservedRequestView {
        handle: p2p_module,
        node: ClosedRuntimeRawSlice {
            ptr: node.as_ptr() as u64,
            len: node.len(),
        },
        msg_id,
        timeout_ms: timeout_ms.unwrap_or(0),
        has_timeout: u8::from(timeout_ms.is_some()),
        transport_policy: match transport_policy {
            fluxon_commu_contract::p2p::RpcTransportPolicy::AllowTransferRpcFastPath => {
                ClosedRuntimeDispatchTransportPolicy::AllowTransferRpcFastPath
            }
            fluxon_commu_contract::p2p::RpcTransportPolicy::ForceTransport => {
                ClosedRuntimeDispatchTransportPolicy::ForceTransport
            }
        },
        body: body_owner_ref.view_with_owner_handle(body_owner_handle),
    };
    let (status_code, payload) = invoke_completion_async_with_post_submit(
        |user_data, callback| unsafe {
            fluxon_commu_closed_p2p_call_raw_observed_async(
                (&request as *const ClosedRuntimeP2pCallRawObservedRequestView).cast(),
                std::mem::size_of::<ClosedRuntimeP2pCallRawObservedRequestView>(),
                user_data,
                callback,
            )
        },
        || unsafe {
            let _ = release_wire_body_owner_ref(body_owner_handle);
        },
    )
    .await?;
    match status_code {
        FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK => decode_call_raw_observed_output_view(payload.as_ref()),
        FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR => {
            let error = bitcode::decode::<ClosedRuntimeError>(payload.as_ref()).map_err(
                |decode_error| ClosedSdkConsumerError::RuntimeDecode {
                    detail: decode_error.to_string(),
                },
            )?;
            Err(ClosedSdkConsumerError::RuntimeError { error })
        }
        status => Err(ClosedSdkConsumerError::RuntimeSubmitRejected { status }),
    }
}

pub async fn p2p_send_response_raw(
    p2p_module: ClosedRuntimeHandle,
    logical_target: String,
    reply_next_hop: String,
    task_id: fluxon_commu_contract::TaskId,
    msg_id: u32,
    body: WireMessageBody,
    transport_policy: fluxon_commu_contract::RpcTransportPolicy,
    incoming_local_observe: fluxon_commu_contract::WireTransportLocalObserve,
) -> Result<(), ClosedSdkConsumerError> {
    let body_owner = WireBodyPartsOwner::new(body)?;
    let body_owner_handle = into_wire_body_owner_handle(body_owner);
    let body_owner_ref = unsafe { &*(body_owner_handle as *const WireBodyPartsOwner) };
    let request = ClosedRuntimeP2pSendResponseRawRequestView {
        handle: p2p_module,
        logical_target: ClosedRuntimeRawSlice {
            ptr: logical_target.as_ptr() as u64,
            len: logical_target.len(),
        },
        reply_next_hop: ClosedRuntimeRawSlice {
            ptr: reply_next_hop.as_ptr() as u64,
            len: reply_next_hop.len(),
        },
        task_id,
        msg_id,
        transport_policy: match transport_policy {
            fluxon_commu_contract::p2p::RpcTransportPolicy::AllowTransferRpcFastPath => {
                ClosedRuntimeDispatchTransportPolicy::AllowTransferRpcFastPath
            }
            fluxon_commu_contract::p2p::RpcTransportPolicy::ForceTransport => {
                ClosedRuntimeDispatchTransportPolicy::ForceTransport
            }
        },
        incoming_local_observe: ClosedRuntimeWireTransportLocalObserveView {
            frame_recv_done_ts_us: incoming_local_observe.frame_recv_done_ts_us,
            dispatch_enqueued_ts_us: incoming_local_observe.dispatch_enqueued_ts_us,
            dispatch_started_ts_us: incoming_local_observe.dispatch_started_ts_us,
            complete_pending_call_ts_us: incoming_local_observe.complete_pending_call_ts_us,
        },
        body: body_owner_ref.view_with_owner_handle(body_owner_handle),
    };
    let (status_code, payload) = invoke_completion_async_with_post_submit(
        |user_data, callback| unsafe {
            fluxon_commu_closed_p2p_send_response_raw_async(
                (&request as *const ClosedRuntimeP2pSendResponseRawRequestView).cast(),
                std::mem::size_of::<ClosedRuntimeP2pSendResponseRawRequestView>(),
                user_data,
                callback,
            )
        },
        || unsafe {
            let _ = release_wire_body_owner_ref(body_owner_handle);
        },
    )
    .await?;
    match status_code {
        FLUXON_COMMU_CLOSED_RUNTIME_RESULT_OK => Ok(()),
        FLUXON_COMMU_CLOSED_RUNTIME_RESULT_ERR => {
            let error = bitcode::decode::<ClosedRuntimeError>(payload.as_ref()).map_err(
                |decode_error| ClosedSdkConsumerError::RuntimeDecode {
                    detail: decode_error.to_string(),
                },
            )?;
            Err(ClosedSdkConsumerError::RuntimeError { error })
        }
        status => Err(ClosedSdkConsumerError::RuntimeSubmitRejected { status }),
    }
}

pub async fn p2p_register_user_rpc_bytes_handler(
    p2p_module: ClosedRuntimeHandle,
    path: String,
    handler: Arc<dyn UserRpcBytesHandler>,
) -> Result<(), ClosedSdkConsumerError> {
    let callback =
        register_host_user_rpc_callback(RegisteredHostCallback::UserRpcBytesSync(handler))?;
    match p2p_module_call(
        p2p_module,
        ClosedRuntimeP2pCall::RegisterUserRpcBytesHandler {
            path,
            callback,
            is_async: false,
        },
    )
    .await
    {
        Ok(ClosedRuntimeP2pResponse::Unit) => Ok(()),
        Ok(other) => {
            release_host_callback_handle(callback);
            Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
                detail: format!("{other:?}"),
            })
        }
        Err(error) => {
            release_host_callback_handle(callback);
            Err(error)
        }
    }
}

pub async fn p2p_register_user_rpc_bytes_handler_async(
    p2p_module: ClosedRuntimeHandle,
    path: String,
    handler: Arc<dyn UserRpcBytesAsyncHandler>,
) -> Result<(), ClosedSdkConsumerError> {
    let callback =
        register_host_user_rpc_callback(RegisteredHostCallback::UserRpcBytesAsync(handler))?;
    match p2p_module_call(
        p2p_module,
        ClosedRuntimeP2pCall::RegisterUserRpcBytesHandler {
            path,
            callback,
            is_async: true,
        },
    )
    .await
    {
        Ok(ClosedRuntimeP2pResponse::Unit) => Ok(()),
        Ok(other) => {
            release_host_callback_handle(callback);
            Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
                detail: format!("{other:?}"),
            })
        }
        Err(error) => {
            release_host_callback_handle(callback);
            Err(error)
        }
    }
}

pub async fn subscribe_cluster_manager_events(
    handle: ClosedRuntimeHandle,
) -> Result<ClosedRuntimeHandle, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ClusterManagerCall {
        handle,
        call: ClosedRuntimeClusterManagerCall::SubscribeEvents,
    })
    .await?
    {
        ClosedRuntimeResponse::Constructed { handle } => Ok(handle),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn current_cluster_manager_self_rdma_resolved_config(
    handle: ClosedRuntimeHandle,
) -> Result<fluxon_commu_contract::MemberRdmaResolvedConfig, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ClusterManagerCall {
        handle,
        call: ClosedRuntimeClusterManagerCall::CurrentSelfRdmaResolvedConfig,
    })
    .await?
    {
        ClosedRuntimeResponse::ClusterManager {
            response: ClosedRuntimeClusterManagerResponse::MemberRdmaResolvedConfigValue(config),
        } => Ok(config),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn watch_cluster_manager_self_rdma_resolved_config(
    handle: ClosedRuntimeHandle,
) -> Result<ClosedRuntimeHandle, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ClusterManagerCall {
        handle,
        call: ClosedRuntimeClusterManagerCall::WatchSelfRdmaResolvedConfig,
    })
    .await?
    {
        ClosedRuntimeResponse::Constructed { handle } => Ok(handle),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn recv_cluster_event_stream(
    handle: ClosedRuntimeHandle,
) -> Result<ClosedRuntimeClusterEventStreamItem, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ClusterEventStreamRecv { handle }).await? {
        ClosedRuntimeResponse::ClusterEventStreamItem { item } => Ok(item),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn recv_cluster_rdma_resolved_config_stream(
    handle: ClosedRuntimeHandle,
) -> Result<ClosedRuntimeClusterRdmaResolvedConfigStreamItem, ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::ClusterRdmaResolvedConfigStreamRecv { handle })
        .await?
    {
        ClosedRuntimeResponse::ClusterRdmaResolvedConfigStreamItem { item } => Ok(item),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}

pub async fn drop_runtime_handle(
    handle: ClosedRuntimeHandle,
) -> Result<(), ClosedSdkConsumerError> {
    match runtime_invoke(&ClosedRuntimeRequest::DropHandle { handle }).await? {
        ClosedRuntimeResponse::Dropped => Ok(()),
        other => Err(ClosedSdkConsumerError::RuntimeUnexpectedResponse {
            detail: format!("{other:?}"),
        }),
    }
}
