use pyo3::PyErr;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use fluxon_mq::MpscError as CoreMpscError;
// Re-export the core MPSC error type for callers who want to depend on a single error hub.
pub use fluxon_mq::MpscError as CoreMpscErrorReExport;

// NOTE: We avoid a central conversion function. Instead, expose explicit
// constructors for each Python-side error we need to raise from Rust.

fn api_ext_module(py: Python<'_>) -> Bound<'_, PyModule> {
    // Build errors from fluxon_py.api_error (ApiError subclasses)
    py.import_bound("fluxon_py.api_error").unwrap()
}

fn build_ext_error(
    py: Python<'_>,
    class_name: &str,
    message: &str,
    fill: impl FnOnce(&Bound<'_, PyDict>),
) -> PyErr {
    let m = api_ext_module(py);
    let cls = m.getattr(class_name).unwrap();
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    fill(&kwargs);
    let inst = cls.call((), Some(&kwargs)).unwrap();
    // PyO3 0.21+ exposes `from_value_bound` for creating a PyErr from a Bound<PyAny>.
    // `from_value` no longer exists (or has a different signature), which caused E0599/E0061.
    // Use the bound variant directly to match the current API.
    PyErr::from_value_bound(inst)
}

// Explicit Python-side ApiError constructors (return PyErr ready to raise)
pub(crate) fn pyerr_message_consumption_no_new_message(
    py: Python<'_>,
    message: &str,
    channel_id: i64,
    producer_idx: Option<&str>,
    message_id: Option<i64>,
) -> PyErr {
    build_ext_error(py, "MessageConsumptionNoNewMessageError", message, |kw| {
        kw.set_item("channel_id", channel_id).unwrap();
        if let Some(p) = producer_idx {
            kw.set_item("producer_idx", p).unwrap();
        }
        if let Some(id) = message_id {
            kw.set_item("message_id", id).unwrap();
        }
    })
}

pub(crate) fn pyerr_message_consumption(
    py: Python<'_>,
    message: &str,
    channel_id: i64,
    producer_idx: Option<&str>,
    message_id: Option<i64>,
) -> PyErr {
    build_ext_error(py, "MessageConsumptionError", message, |kw| {
        kw.set_item("channel_id", channel_id).unwrap();
        if let Some(p) = producer_idx {
            kw.set_item("producer_idx", p).unwrap();
        }
        if let Some(id) = message_id {
            kw.set_item("message_id", id).unwrap();
        }
    })
}

pub(crate) fn pyerr_chan_message_produce(
    py: Python<'_>,
    message: &str,
    chan_id: i64,
    producer_idx: Option<&str>,
    message_id: Option<i64>,
) -> PyErr {
    build_ext_error(py, "ChanMessageProduceError", message, |kw| {
        kw.set_item("chan_id", chan_id).unwrap();
        if let Some(p) = producer_idx {
            kw.set_item("producer_idx", p).unwrap();
        }
        if let Some(id) = message_id {
            kw.set_item("message_id", id).unwrap();
        }
    })
}

// System/bridge category constructors (distinct helpers for clarity)
pub(crate) fn pyerr_etcd(py: Python<'_>, message: &str, component: &str) -> PyErr {
    build_ext_error(py, "EtcdError", message, |kw| {
        kw.set_item("component", component).unwrap();
    })
}

pub(crate) fn pyerr_join_error(py: Python<'_>, message: &str, component: &str) -> PyErr {
    build_ext_error(py, "JoinError", message, |kw| {
        kw.set_item("component", component).unwrap();
    })
}

pub(crate) fn pyerr_internal(py: Python<'_>, message: &str, component: &str) -> PyErr {
    build_ext_error(py, "InternalError", message, |kw| {
        kw.set_item("component", component).unwrap();
    })
}

// -------- Base ApiError constructors and Result wrappers (centralized) --------

/// Create Python SUCCESS instance (from fluxon_py.api_error)
pub(crate) fn new_none_success_instance(py: Python<'_>) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    api_error_module.getattr("OK_NONE").unwrap().into()
}

/// Helper to construct base ApiError (fluxon_py.api_error)
fn new_api_error_base(py: Python<'_>, error_type: &str, message: &str) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr(error_type).unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_general_error(py: Python<'_>, message: &str) -> PyObject {
    new_api_error_base(py, "GeneralError", message)
}

pub(crate) fn new_invalid_argument_error(py: Python<'_>, message: &str) -> PyObject {
    new_api_error_base(py, "InvalidArgumentError", message)
}

pub(crate) fn new_backend_init_failed_error(
    py: Python<'_>,
    message: &str,
    backend_name: Option<&str>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("BackendInitFailedError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(name) = backend_name {
        kwargs.set_item("backend_name", name).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_network_error(py: Python<'_>, message: &str, endpoint: Option<&str>) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("NetworkError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(ep) = endpoint {
        kwargs.set_item("endpoint", ep).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_network_error_with_transport(
    py: Python<'_>,
    message: &str,
    endpoint: Option<&str>,
    transport: Option<&str>,
    transport_user: Option<&str>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("NetworkError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(ep) = endpoint {
        kwargs.set_item("endpoint", ep).unwrap();
    }
    if let Some(value) = transport {
        kwargs.set_item("transport", value).unwrap();
    }
    if let Some(value) = transport_user {
        kwargs.set_item("transport_user", value).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_key_not_found_error(
    py: Python<'_>,
    message: &str,
    key: Option<&str>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("KeyNotFoundError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(k) = key {
        kwargs.set_item("key", k).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_key_being_written_error(
    py: Python<'_>,
    message: &str,
    key: Option<&str>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("KeyBeingWrittenError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(k) = key {
        kwargs.set_item("key", k).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_key_already_exists_error(
    py: Python<'_>,
    message: &str,
    key: Option<&str>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("KeyAlreadyExistsError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(k) = key {
        kwargs.set_item("key", k).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_storage_full_error(
    py: Python<'_>,
    message: &str,
    available_space: Option<i64>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("StorageFullError").unwrap();
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(space) = available_space {
        kwargs.set_item("available_space", space).unwrap();
    }
    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_file_write_error(
    py: Python<'_>,
    message: &str,
    filepath: Option<&str>,
    offset: Option<i64>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("FileWriteError").unwrap();

    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(p) = filepath {
        kwargs.set_item("filepath", p).unwrap();
    }
    if let Some(o) = offset {
        kwargs.set_item("offset", o).unwrap();
    }

    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_store_closed_error(py: Python<'_>, message: &str) -> PyObject {
    new_api_error_base(py, "StoreClosedError", message)
}

pub(crate) fn new_result_success(py: Python<'_>, value: PyObject) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let result_class = api_error_module.getattr("Result").unwrap();
    result_class
        .call_method1("new_ok", (value,))
        .unwrap()
        .into()
}

pub(crate) fn new_result_error(py: Python<'_>, error: PyObject) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let result_class = api_error_module.getattr("Result").unwrap();
    result_class
        .call_method1("new_error", (error,))
        .unwrap()
        .into()
}

// -------- Extra Python ApiError constructors used by KV mapping --------

pub(crate) fn new_transfer_block_failed_error(
    py: Python<'_>,
    message: &str,
    endpoint: Option<&str>,
    task_id: Option<u64>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module
        .getattr("TransferBlockFailedError")
        .unwrap();
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(ep) = endpoint {
        kwargs.set_item("endpoint", ep).unwrap();
    }
    if let Some(tid) = task_id {
        kwargs.set_item("task_id", tid).unwrap();
    }
    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_put_done_failed_error(
    py: Python<'_>,
    message: &str,
    channel_id: Option<i64>,
    producer_idx: Option<&str>,
    message_id: Option<i64>,
    detail: Option<&str>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module.getattr("PutDoneFailedError").unwrap();
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(cid) = channel_id {
        kwargs.set_item("channel_id", cid).unwrap();
    }
    if let Some(p) = producer_idx {
        kwargs.set_item("producer_idx", p).unwrap();
    }
    if let Some(mid) = message_id {
        kwargs.set_item("message_id", mid).unwrap();
    }
    if let Some(d) = detail {
        kwargs.set_item("detail", d).unwrap();
    }
    error_class.call((), Some(&kwargs)).unwrap().into()
}

pub(crate) fn new_payload_lease_not_found_error(
    py: Python<'_>,
    message: &str,
    lease_id: Option<i64>,
) -> PyObject {
    let api_error_module = py.import_bound("fluxon_py.api_error").unwrap();
    let error_class = api_error_module
        .getattr("PayloadLeaseNotFoundError")
        .unwrap();
    let kwargs = PyDict::new_bound(py);
    kwargs.set_item("message", message).unwrap();
    if let Some(id) = lease_id {
        kwargs.set_item("lease_id", id).unwrap();
    }
    error_class.call((), Some(&kwargs)).unwrap().into()
}

/// Typed mapping: map Rust-side KvError to Python ApiError instance.
///
/// 收束规则（最小必要）：
/// - TransferEngine::TransferFailedForBlock -> TransferBlockFailedError（可重试）
/// - Api::InvalidPutMasterState -> PutDoneFailedError
/// - Api::KeyNotFound -> KeyNotFoundError（携带 key）
/// - Api::KeyAlreadyExists -> KeyAlreadyExistsError（携带 key）
/// - Api::KeyBeingWritten -> KeyBeingWrittenError（携带 key）
/// - Api::NoSpace -> StorageFullError（available_space=free_capacity）
/// - 其他 -> NetworkError（携带格式化消息）
pub(crate) fn py_error_from_kv_error(
    py: Python<'_>,
    e: &fluxon_kv::rpcresp_kvresult_convert::msg_and_error::KvError,
    prefix: &str,
) -> PyObject {
    use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{
        ApiError as CoreApiError, KvError, LeaseMgrError, P2pError as CoreP2pError,
        TransferEngineError,
    };
    let msg = format!("{}: {}", prefix, e);
    match e {
        KvError::TransferEngine(TransferEngineError::TransferFailedForBlock {
            task_id, ..
        }) => new_transfer_block_failed_error(py, &msg, None, Some(*task_id)),
        KvError::Api(CoreApiError::InvalidPutMasterState { detail }) => new_put_done_failed_error(
            py,
            &format!("{}: InvalidPutMasterState: {}", prefix, detail),
            None,
            None,
            None,
            Some(detail),
        ),
        KvError::Api(CoreApiError::KeyNotFound { key }) => new_key_not_found_error(
            py,
            &format!("{}: Key not found: {}", prefix, key),
            Some(key),
        ),
        KvError::Api(CoreApiError::KeyAlreadyExists { key }) => new_key_already_exists_error(
            py,
            &format!("{}: Key already exists: {}", prefix, key),
            Some(key),
        ),
        KvError::Api(CoreApiError::KeyBeingWritten { key }) => new_key_being_written_error(
            py,
            &format!("{}: Key is currently being written: {}", prefix, key),
            Some(key),
        ),
        KvError::Api(CoreApiError::InvalidArgument { detail }) => {
            new_invalid_argument_error(py, &format!("{}: Invalid argument: {}", prefix, detail))
        }
        KvError::P2p(CoreP2pError::InvalidRpcTimeout { .. }) => {
            new_invalid_argument_error(py, &msg)
        }
        KvError::Api(CoreApiError::FileWriteError {
            path,
            offset,
            detail,
        }) => {
            let off = i64::try_from(*offset).ok();
            new_file_write_error(
                py,
                &format!("{}: File write failed: {}", prefix, detail),
                Some(path.as_str()),
                off,
            )
        }
        KvError::Api(CoreApiError::NoSpace {
            node,
            segment,
            total_capacity,
            free_capacity,
        }) => {
            let msg = format!(
                "{}: No space left: node={}, segment={}, total={}, free={}",
                prefix, node, segment, total_capacity, free_capacity
            );
            new_storage_full_error(py, &msg, Some(*free_capacity as i64))
        }
        KvError::Api(CoreApiError::UserRpcMissingPayload { path }) => new_general_error(
            py,
            &format!(
                "{}: UserRpcResp missing payload raw_bytes[0] (path={})",
                prefix, path
            ),
        ),
        // Lease manager: 收束 LeaseNotFound / LeaseExpired 到专门的 PayloadLeaseNotFoundError，
        // 方便上层通过类型判断 payload lease 丢失/过期，而不是依赖字符串 contains。
        KvError::LeaseMgr(LeaseMgrError::LeaseNotFound { lease_id, .. }) => {
            new_payload_lease_not_found_error(py, &msg, Some(*lease_id as i64))
        }
        KvError::LeaseMgr(LeaseMgrError::LeaseExpired { lease_id, .. }) => {
            let expired_msg = format!("{} (expired)", msg);
            new_payload_lease_not_found_error(py, &expired_msg, Some(*lease_id as i64))
        }
        KvError::Api(CoreApiError::TransportError {
            transport,
            transport_user,
            detail,
        }) => new_network_error_with_transport(
            py,
            &format!("{}: {}", prefix, detail),
            None,
            Some(transport.as_str()),
            Some(transport_user.as_str()),
        ),
        _ => new_network_error(py, &msg, None),
    }
}
