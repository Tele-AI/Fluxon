use std::collections::{BTreeMap, BTreeSet};
use std::os::raw::c_void;
use std::sync::Arc;

use fluxon_kv::memholder::kvclient_encode::{
    BorrowedFlatKvValueRange, FLAT_KV_TYPE_BOOL, FLAT_KV_TYPE_BYTES, FLAT_KV_TYPE_FLOAT64,
    FLAT_KV_TYPE_INT64, FLAT_KV_TYPE_STRING, calc_flat_dict_encoded_len, flat_kv_decode_borrowed,
    write_flat_dict_ptrs_to_ptr,
};
use fluxon_kv::memholder::{
    ExternalMemHolder as RustExternalMemHolder, UserMemHolder as RustMemHolder,
};
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{
    ApiError as CoreApiError, KvError as CoreKvError,
};
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyCapsule, PyCapsuleMethods, PyDict, PyFloat, PyInt, PyString,
};

pub(crate) const INTERNAL_DLPACK_META_KEY: &str = "__fluxon_internal_dlpack_meta__";
pub(crate) const DLPACK_CAPSULE_NAME: &[u8] = b"dltensor";
pub(crate) const DLPACK_USED_CAPSULE_NAME: &[u8] = b"used_dltensor";
pub(crate) const DLPACK_DEVICE_CPU: i32 = 1;

const DLPACK_CAPSULE_NAME_CSTR: &[u8] = b"dltensor\0";
const DLPACK_USED_CAPSULE_NAME_CSTR: &[u8] = b"used_dltensor\0";

#[derive(Clone)]
pub(crate) enum FlatDictDataOwner {
    OwnedBytes(Arc<[u8]>),
    UserMemHolder(Arc<RustMemHolder>),
    ExternalMemHolder(Arc<RustExternalMemHolder>),
}

impl FlatDictDataOwner {
    pub(crate) fn from_owned_bytes(bytes: Vec<u8>) -> Self {
        Self::OwnedBytes(Arc::<[u8]>::from(bytes))
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::OwnedBytes(bytes) => bytes.as_ref(),
            Self::UserMemHolder(holder) => holder.bytes(),
            Self::ExternalMemHolder(holder) => holder.bytes(),
        }
    }
}

pub(crate) struct FlatDictEncodePlan {
    ptrs: Vec<(u8, usize, u32, u64, u32, Option<u32>)>,
    key_storage: Vec<Vec<u8>>,
    owned_value_storage: Vec<Vec<u8>>,
    keepalive_objects: Vec<PyObject>,
    dlpack_meta_entries: Vec<DlpackMetaEntry>,
}

struct BorrowedDlpackEncodeField {
    keepalive_object: PyObject,
    data_ptr: usize,
    nbytes: u32,
    meta_entry: DlpackMetaEntry,
}

impl FlatDictEncodePlan {
    fn new(entry_capacity: usize) -> Self {
        Self {
            ptrs: Vec::with_capacity(entry_capacity),
            key_storage: Vec::with_capacity(entry_capacity),
            owned_value_storage: Vec::new(),
            keepalive_objects: Vec::new(),
            dlpack_meta_entries: Vec::new(),
        }
    }

    fn push_inline_entry(
        &mut self,
        type_id: u8,
        key_utf8: Vec<u8>,
        inline_value: u64,
        value_len_u32: u32,
    ) -> Result<(), CoreKvError> {
        let key_len_u32 = u32::try_from(key_utf8.len()).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: "flat dict key too large".to_string(),
            })
        })?;
        self.key_storage.push(key_utf8);
        let key_storage = self
            .key_storage
            .last()
            .expect("key storage must contain the just-pushed key");
        self.ptrs.push((
            type_id,
            key_storage.as_ptr() as usize,
            key_len_u32,
            inline_value,
            value_len_u32,
            None,
        ));
        Ok(())
    }

    fn push_owned_bytes_entry(
        &mut self,
        type_id: u8,
        key_utf8: Vec<u8>,
        value_bytes: Vec<u8>,
        value_kind: &str,
    ) -> Result<(), CoreKvError> {
        let key_len_u32 = u32::try_from(key_utf8.len()).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: "flat dict key too large".to_string(),
            })
        })?;
        let value_len_u32 = u32::try_from(value_bytes.len()).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("flat dict {value_kind} value too large"),
            })
        })?;
        self.key_storage.push(key_utf8);
        self.owned_value_storage.push(value_bytes);
        let key_storage = self
            .key_storage
            .last()
            .expect("key storage must contain the just-pushed key");
        let value_storage = self
            .owned_value_storage
            .last()
            .expect("value storage must contain the just-pushed bytes");
        self.ptrs.push((
            type_id,
            key_storage.as_ptr() as usize,
            key_len_u32,
            value_storage.as_ptr() as u64,
            value_len_u32,
            None,
        ));
        Ok(())
    }

    fn push_borrowed_bytes_entry(
        &mut self,
        type_id: u8,
        key_utf8: Vec<u8>,
        value_ptr: usize,
        value_len_u32: u32,
        keepalive_object: PyObject,
    ) -> Result<(), CoreKvError> {
        let key_len_u32 = u32::try_from(key_utf8.len()).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: "flat dict key too large".to_string(),
            })
        })?;
        self.keepalive_objects.push(keepalive_object);
        self.key_storage.push(key_utf8);
        let key_storage = self
            .key_storage
            .last()
            .expect("key storage must contain the just-pushed key");
        self.ptrs.push((
            type_id,
            key_storage.as_ptr() as usize,
            key_len_u32,
            value_ptr as u64,
            value_len_u32,
            None,
        ));
        Ok(())
    }

    pub(crate) fn encode(mut self) -> Result<Vec<u8>, CoreKvError> {
        if !self.dlpack_meta_entries.is_empty() {
            let dlpack_meta_payload =
                encode_dlpack_meta_entries(self.dlpack_meta_entries.as_slice())?;
            self.push_owned_bytes_entry(
                FLAT_KV_TYPE_BYTES,
                INTERNAL_DLPACK_META_KEY.as_bytes().to_vec(),
                dlpack_meta_payload,
                "dlpack meta",
            )?;
        }
        let payload_len_u64 = calc_flat_dict_encoded_len(self.ptrs.as_slice())?;
        let payload_len = usize::try_from(payload_len_u64).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: "flat dict encoded payload too large".to_string(),
            })
        })?;
        let mut payload = Vec::<u8>::with_capacity(payload_len);
        unsafe {
            payload.set_len(payload_len);
            write_flat_dict_ptrs_to_ptr(payload.as_mut_ptr(), self.ptrs.as_slice());
        }
        Ok(payload)
    }
}

#[repr(C)]
pub(crate) struct DlpackDevice {
    pub(crate) device_type: i32,
    pub(crate) device_id: i32,
}

#[repr(C)]
pub(crate) struct DlpackDType {
    pub(crate) code: u8,
    pub(crate) bits: u8,
    pub(crate) lanes: u16,
}

#[repr(C)]
pub(crate) struct DlpackTensor {
    pub(crate) data: *mut c_void,
    pub(crate) device: DlpackDevice,
    pub(crate) ndim: i32,
    pub(crate) dtype: DlpackDType,
    pub(crate) shape: *const i64,
    pub(crate) strides: *const i64,
    pub(crate) byte_offset: u64,
}

#[repr(C)]
pub(crate) struct DlManagedTensor {
    pub(crate) dl_tensor: DlpackTensor,
    pub(crate) manager_ctx: *mut c_void,
    pub(crate) deleter: *mut c_void,
}

pub(crate) struct DlpackMetaEntry {
    pub(crate) key_utf8: Vec<u8>,
    pub(crate) dtype_code: u8,
    pub(crate) bits: u8,
    pub(crate) lanes: u16,
    pub(crate) shape: Vec<i64>,
}

#[derive(Clone)]
struct DecodedDlpackMeta {
    dtype_code: u8,
    bits: u8,
    lanes: u16,
    shape: Arc<[i64]>,
    nbytes: usize,
}

#[pyclass]
pub(crate) struct FlatDictDLPackView {
    owner: FlatDictDataOwner,
    data_offset: usize,
    nbytes: usize,
    dtype_code: u8,
    bits: u8,
    lanes: u16,
    shape: Arc<[i64]>,
}

#[pymethods]
impl FlatDictDLPackView {
    fn __dlpack_device__(&self) -> (i32, i32) {
        (DLPACK_DEVICE_CPU, 0)
    }

    #[pyo3(signature = (stream=None))]
    fn __dlpack__(&self, py: Python<'_>, stream: Option<PyObject>) -> PyResult<PyObject> {
        if stream.is_some() {
            return Err(PyValueError::new_err(
                "Only __dlpack__(stream=None) is supported",
            ));
        }
        new_dlpack_capsule(
            py,
            self.owner.clone(),
            self.data_offset,
            self.nbytes,
            self.dtype_code,
            self.bits,
            self.lanes,
            self.shape.clone(),
        )
    }
}

impl FlatDictDLPackView {
    fn new(
        owner: FlatDictDataOwner,
        data_offset: usize,
        nbytes: usize,
        dtype_code: u8,
        bits: u8,
        lanes: u16,
        shape: Arc<[i64]>,
    ) -> Self {
        Self {
            owner,
            data_offset,
            nbytes,
            dtype_code,
            bits,
            lanes,
            shape,
        }
    }
}

#[repr(C)]
struct FlatDictDLPackCapsulePayload {
    managed: DlManagedTensor,
    owner: FlatDictDataOwner,
    shape: Arc<[i64]>,
}

unsafe extern "C" fn flatdict_dlpack_managed_deleter(managed: *mut DlManagedTensor) {
    if managed.is_null() {
        return;
    }
    let payload = managed as *mut FlatDictDLPackCapsulePayload;
    drop(unsafe { Box::from_raw(payload) });
}

unsafe extern "C" fn flatdict_dlpack_capsule_destructor(capsule: *mut ffi::PyObject) {
    if capsule.is_null() {
        return;
    }
    if unsafe { ffi::PyCapsule_IsValid(capsule, DLPACK_USED_CAPSULE_NAME_CSTR.as_ptr().cast()) }
        == 1
    {
        return;
    }
    if unsafe { ffi::PyCapsule_IsValid(capsule, DLPACK_CAPSULE_NAME_CSTR.as_ptr().cast()) } != 1 {
        unsafe { ffi::PyErr_Clear() };
        return;
    }
    let managed = unsafe {
        ffi::PyCapsule_GetPointer(capsule, DLPACK_CAPSULE_NAME_CSTR.as_ptr().cast())
            as *mut DlManagedTensor
    };
    if managed.is_null() {
        unsafe { ffi::PyErr_Clear() };
        return;
    }
    let deleter_ptr = unsafe { (*managed).deleter };
    if deleter_ptr.is_null() {
        return;
    }
    let deleter: unsafe extern "C" fn(*mut DlManagedTensor) =
        unsafe { std::mem::transmute(deleter_ptr) };
    unsafe { deleter(managed) };
}

fn new_dlpack_capsule(
    py: Python<'_>,
    owner: FlatDictDataOwner,
    data_offset: usize,
    nbytes: usize,
    dtype_code: u8,
    bits: u8,
    lanes: u16,
    shape: Arc<[i64]>,
) -> PyResult<PyObject> {
    let owner_bytes = owner.bytes();
    let data_end = data_offset
        .checked_add(nbytes)
        .ok_or_else(|| PyValueError::new_err("DLPack data range overflow"))?;
    if data_end > owner_bytes.len() {
        return Err(PyValueError::new_err(
            "DLPack data range exceeds payload bounds",
        ));
    }
    let ndim =
        i32::try_from(shape.len()).map_err(|_| PyValueError::new_err("DLPack ndim too large"))?;
    let data_ptr = unsafe { owner_bytes.as_ptr().add(data_offset) } as *mut c_void;
    let shape_ptr = if shape.is_empty() {
        std::ptr::null()
    } else {
        shape.as_ptr()
    };
    let mut payload = Box::new(FlatDictDLPackCapsulePayload {
        managed: DlManagedTensor {
            dl_tensor: DlpackTensor {
                data: data_ptr,
                device: DlpackDevice {
                    device_type: DLPACK_DEVICE_CPU,
                    device_id: 0,
                },
                ndim,
                dtype: DlpackDType {
                    code: dtype_code,
                    bits,
                    lanes,
                },
                shape: shape_ptr,
                strides: std::ptr::null(),
                byte_offset: 0,
            },
            manager_ctx: std::ptr::null_mut(),
            deleter: flatdict_dlpack_managed_deleter as *mut c_void,
        },
        owner,
        shape,
    });
    let payload_ptr = payload.as_mut() as *mut FlatDictDLPackCapsulePayload;
    payload.managed.manager_ctx = payload_ptr.cast();
    let raw_payload = Box::into_raw(payload);
    let capsule_ptr = unsafe {
        ffi::PyCapsule_New(
            raw_payload.cast(),
            DLPACK_CAPSULE_NAME_CSTR.as_ptr().cast(),
            Some(flatdict_dlpack_capsule_destructor),
        )
    };
    if capsule_ptr.is_null() {
        drop(unsafe { Box::from_raw(raw_payload) });
        return Err(PyErr::fetch(py));
    }
    Ok(unsafe { PyObject::from_owned_ptr(py, capsule_ptr) })
}

fn range_slice<'a>(data: &'a [u8], start: usize, len: usize) -> Result<&'a [u8], CoreKvError> {
    let end = start.checked_add(len).ok_or_else(|| {
        CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "flat dict bytes range overflow".to_string(),
        })
    })?;
    if end > data.len() {
        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "flat dict bytes range out of bounds".to_string(),
        }));
    }
    Ok(&data[start..end])
}

fn calc_dlpack_nbytes(bits: u8, lanes: u16, shape: &[i64]) -> Result<usize, CoreKvError> {
    if bits == 0 || (bits % 8) != 0 {
        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: format!("dlpack dtype.bits must be a positive multiple of 8 (bits={bits})"),
        }));
    }
    if lanes == 0 {
        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "dlpack dtype.lanes must be > 0".to_string(),
        }));
    }
    let mut numel: u64 = 1;
    for dim in shape {
        if *dim < 0 {
            return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack shape contains negative dimension".to_string(),
            }));
        }
        let dim_u64 = u64::try_from(*dim).map_err(|_| {
            CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack dimension out of range".to_string(),
            })
        })?;
        numel = numel.checked_mul(dim_u64).ok_or_else(|| {
            CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack element count overflow".to_string(),
            })
        })?;
    }
    let itemsize = u64::from(bits / 8)
        .checked_mul(u64::from(lanes))
        .ok_or_else(|| {
            CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack itemsize overflow".to_string(),
            })
        })?;
    let nbytes = numel.checked_mul(itemsize).ok_or_else(|| {
        CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "dlpack nbytes overflow".to_string(),
        })
    })?;
    usize::try_from(nbytes).map_err(|_| {
        CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "dlpack nbytes out of range".to_string(),
        })
    })
}

fn decode_dlpack_meta(data: &[u8]) -> Result<BTreeMap<String, DecodedDlpackMeta>, CoreKvError> {
    let mut pos = 0usize;
    if data.len() < 4 {
        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "dlpack meta decode failed: truncated count".to_string(),
        }));
    }
    let count = u32::from_le_bytes(data[pos..(pos + 4)].try_into().unwrap()) as usize;
    pos += 4;
    let mut out = BTreeMap::<String, DecodedDlpackMeta>::new();
    for _ in 0..count {
        if pos + 4 > data.len() {
            return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack meta decode failed: truncated key length".to_string(),
            }));
        }
        let key_len = u32::from_le_bytes(data[pos..(pos + 4)].try_into().unwrap()) as usize;
        pos += 4;
        if pos + key_len > data.len() {
            return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack meta decode failed: truncated key bytes".to_string(),
            }));
        }
        let key = std::str::from_utf8(&data[pos..(pos + key_len)]).map_err(|err| {
            CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: format!("dlpack meta key is not valid utf-8: {err}"),
            })
        })?;
        pos += key_len;
        if pos + 8 > data.len() {
            return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: "dlpack meta decode failed: truncated dtype header".to_string(),
            }));
        }
        let dtype_code = data[pos];
        pos += 1;
        let bits = data[pos];
        pos += 1;
        let lanes = u16::from_le_bytes(data[pos..(pos + 2)].try_into().unwrap());
        pos += 2;
        let ndim = u32::from_le_bytes(data[pos..(pos + 4)].try_into().unwrap()) as usize;
        pos += 4;
        let mut shape = Vec::<i64>::with_capacity(ndim);
        for _ in 0..ndim {
            if pos + 8 > data.len() {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: "dlpack meta decode failed: truncated shape".to_string(),
                }));
            }
            let dim = i64::from_le_bytes(data[pos..(pos + 8)].try_into().unwrap());
            pos += 8;
            shape.push(dim);
        }
        let nbytes = calc_dlpack_nbytes(bits, lanes, shape.as_slice())?;
        let previous = out.insert(
            key.to_string(),
            DecodedDlpackMeta {
                dtype_code,
                bits,
                lanes,
                shape: Arc::<[i64]>::from(shape),
                nbytes,
            },
        );
        if previous.is_some() {
            return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: format!("dlpack meta duplicate field key: {key:?}"),
            }));
        }
    }
    if pos != data.len() {
        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
            detail: "dlpack meta decode failed: trailing bytes present".to_string(),
        }));
    }
    Ok(out)
}

pub(crate) fn encode_dlpack_meta_entries(
    entries: &[DlpackMetaEntry],
) -> Result<Vec<u8>, CoreKvError> {
    let mut out = Vec::<u8>::new();
    let count = u32::try_from(entries.len()).map_err(|_| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: "dlpack meta entry count too large".to_string(),
        })
    })?;
    out.extend_from_slice(count.to_le_bytes().as_slice());
    for entry in entries {
        let key_len = u32::try_from(entry.key_utf8.len()).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: "dlpack meta key too large".to_string(),
            })
        })?;
        let ndim = u32::try_from(entry.shape.len()).map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: "dlpack meta ndim too large".to_string(),
            })
        })?;
        out.extend_from_slice(key_len.to_le_bytes().as_slice());
        out.extend_from_slice(entry.key_utf8.as_slice());
        out.push(entry.dtype_code);
        out.push(entry.bits);
        out.extend_from_slice(entry.lanes.to_le_bytes().as_slice());
        out.extend_from_slice(ndim.to_le_bytes().as_slice());
        for dim in &entry.shape {
            out.extend_from_slice(dim.to_le_bytes().as_slice());
        }
    }
    Ok(out)
}

fn py_type_name(value: &Bound<'_, PyAny>) -> String {
    value
        .get_type()
        .name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "unknown".to_string())
}

fn is_c_contiguous_dlpack(shape: &[i64], strides: &[i64]) -> bool {
    if shape.len() != strides.len() {
        return false;
    }
    let mut expected = 1i64;
    for (&dim, &stride) in shape.iter().rev().zip(strides.iter().rev()) {
        if dim == 0 {
            return true;
        }
        if dim > 1 && stride != expected {
            return false;
        }
        match expected.checked_mul(dim) {
            Some(next) => expected = next,
            None => return false,
        }
    }
    true
}

fn extract_py_dlpack_encode_field(
    py: Python<'_>,
    key: &str,
    value: &Bound<'_, PyAny>,
) -> Result<BorrowedDlpackEncodeField, CoreKvError> {
    let _ = py;
    let capsule_obj = value.call_method0("__dlpack__").map_err(|err| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} __dlpack__() failed: {err}"),
        })
    })?;
    let keepalive_object = capsule_obj.clone().unbind();
    let capsule = capsule_obj.downcast::<PyCapsule>().map_err(|_| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} __dlpack__ returned non-capsule"),
        })
    })?;
    let capsule_name = capsule.name().map_err(|err| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack capsule name read failed: {err}"),
        })
    })?;
    let capsule_name = capsule_name.ok_or_else(|| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack capsule missing name"),
        })
    })?;
    if capsule_name.to_bytes() == DLPACK_USED_CAPSULE_NAME {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!(
                "rpc handler field {key:?} dlpack capsule already consumed (name='used_dltensor')"
            ),
        }));
    }
    if capsule_name.to_bytes() != DLPACK_CAPSULE_NAME {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!(
                "rpc handler field {key:?} invalid dlpack capsule name {:?}",
                capsule_name.to_string_lossy()
            ),
        }));
    }
    let managed_ptr = capsule.pointer() as *const DlManagedTensor;
    if managed_ptr.is_null() {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack capsule pointer is null"),
        }));
    }
    let managed = unsafe { &*managed_ptr };
    let tensor = &managed.dl_tensor;
    if tensor.device.device_type != DLPACK_DEVICE_CPU {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!(
                "rpc handler field {key:?} only supports CPU dlpack (device_type={})",
                tensor.device.device_type
            ),
        }));
    }
    if tensor.byte_offset != 0 {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack byte_offset must be 0"),
        }));
    }
    if tensor.ndim < 0 {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack ndim must be >= 0"),
        }));
    }
    let mut shape = Vec::<i64>::new();
    let mut numel: u64 = 1;
    if tensor.ndim > 0 {
        if tensor.shape.is_null() {
            return Err(CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("rpc handler field {key:?} dlpack shape pointer is null"),
            }));
        }
        for idx in 0..(tensor.ndim as usize) {
            let dim = unsafe { *tensor.shape.add(idx) };
            if dim < 0 {
                return Err(CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!(
                        "rpc handler field {key:?} dlpack shape contains negative dimension"
                    ),
                }));
            }
            let dim_u64 = u64::try_from(dim).map_err(|_| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!("rpc handler field {key:?} dlpack dimension out of range"),
                })
            })?;
            numel = numel.checked_mul(dim_u64).ok_or_else(|| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!("rpc handler field {key:?} dlpack element count overflow"),
                })
            })?;
            shape.push(dim);
        }
    }
    let bits = tensor.dtype.bits;
    if bits == 0 || (bits % 8) != 0 {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!(
                "rpc handler field {key:?} dlpack dtype.bits must be a positive multiple of 8 (bits={bits})"
            ),
        }));
    }
    let lanes = tensor.dtype.lanes;
    if lanes == 0 {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack dtype.lanes must be > 0"),
        }));
    }
    let itemsize = u64::from(bits / 8)
        .checked_mul(u64::from(lanes))
        .ok_or_else(|| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("rpc handler field {key:?} dlpack itemsize overflow"),
            })
        })?;
    let nbytes = numel.checked_mul(itemsize).ok_or_else(|| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack nbytes overflow"),
        })
    })?;
    if nbytes > (u32::MAX as u64) {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack tensor too large (nbytes={nbytes})"),
        }));
    }
    let nbytes_usize = usize::try_from(nbytes).map_err(|_| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack nbytes out of range"),
        })
    })?;
    let nbytes_u32 = u32::try_from(nbytes_usize).map_err(|_| {
        CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack nbytes out of u32 range"),
        })
    })?;
    if !tensor.strides.is_null() {
        let mut strides = Vec::<i64>::with_capacity(shape.len());
        for idx in 0..shape.len() {
            strides.push(unsafe { *tensor.strides.add(idx) });
        }
        if !is_c_contiguous_dlpack(shape.as_slice(), strides.as_slice()) {
            return Err(CoreKvError::Api(CoreApiError::Unknown {
                detail: format!(
                    "rpc handler field {key:?} only supports C-contiguous CPU dlpack tensors; call contiguous() or ascontiguousarray() first"
                ),
            }));
        }
    }
    if tensor.data.is_null() && nbytes_usize != 0 {
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!("rpc handler field {key:?} dlpack tensor data pointer is null"),
        }));
    }
    Ok(BorrowedDlpackEncodeField {
        keepalive_object,
        data_ptr: tensor.data as usize,
        nbytes: nbytes_u32,
        meta_entry: DlpackMetaEntry {
            key_utf8: key.as_bytes().to_vec(),
            dtype_code: tensor.dtype.code,
            bits,
            lanes,
            shape,
        },
    })
}

pub(crate) fn build_py_flat_dict_encode_plan(
    dict: &Bound<'_, PyDict>,
) -> Result<FlatDictEncodePlan, CoreKvError> {
    let mut plan = FlatDictEncodePlan::new(dict.len().saturating_add(1));
    for (key_obj, value_obj) in dict.iter() {
        let key_py = key_obj.downcast::<PyString>().map_err(|_| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: format!(
                    "KV put() requires string keys only; got key type {}",
                    py_type_name(&key_obj)
                ),
            })
        })?;
        let key = key_py.extract::<String>().map_err(|err| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("rpc handler dict key extract failed: {err}"),
            })
        })?;
        if key == INTERNAL_DLPACK_META_KEY {
            return Err(CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("Reserved key not allowed: {INTERNAL_DLPACK_META_KEY:?}"),
            }));
        }
        let key_utf8 = key.as_bytes().to_vec();
        if value_obj.is_instance_of::<PyBool>() {
            let value = value_obj.extract::<bool>().map_err(|err| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!("rpc handler bool field {key:?} extract failed: {err}"),
                })
            })?;
            plan.push_inline_entry(FLAT_KV_TYPE_BOOL, key_utf8, if value { 1 } else { 0 }, 1)?;
            continue;
        }
        if value_obj.is_instance_of::<PyInt>() {
            let value = value_obj.extract::<i64>().map_err(|err| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!("KV put() int out of int64 range: key={key:?} error={err}"),
                })
            })?;
            plan.push_inline_entry(
                FLAT_KV_TYPE_INT64,
                key_utf8,
                u64::from_le_bytes(value.to_le_bytes()),
                8,
            )?;
            continue;
        }
        if value_obj.is_instance_of::<PyFloat>() {
            let value = value_obj.extract::<f64>().map_err(|err| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: format!("rpc handler float field {key:?} extract failed: {err}"),
                })
            })?;
            plan.push_inline_entry(
                FLAT_KV_TYPE_FLOAT64,
                key_utf8,
                u64::from_le_bytes(value.to_le_bytes()),
                8,
            )?;
            continue;
        }
        if let Ok(py_string) = value_obj.downcast::<PyString>() {
            let value_utf8 = py_string
                .extract::<String>()
                .map_err(|err| {
                    CoreKvError::Api(CoreApiError::Unknown {
                        detail: format!("rpc handler string field {key:?} extract failed: {err}"),
                    })
                })?
                .into_bytes();
            plan.push_owned_bytes_entry(FLAT_KV_TYPE_STRING, key_utf8, value_utf8, "string")?;
            continue;
        }
        if let Ok(py_bytes) = value_obj.downcast::<PyBytes>() {
            let value_len_u32 = u32::try_from(py_bytes.as_bytes().len()).map_err(|_| {
                CoreKvError::Api(CoreApiError::Unknown {
                    detail: "flat dict bytes value too large".to_string(),
                })
            })?;
            plan.push_borrowed_bytes_entry(
                FLAT_KV_TYPE_BYTES,
                key_utf8,
                py_bytes.as_bytes().as_ptr() as usize,
                value_len_u32,
                value_obj.clone().unbind(),
            )?;
            continue;
        }
        if value_obj.hasattr("__dlpack__").map_err(|err| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("rpc handler field {key:?} __dlpack__ check failed: {err}"),
            })
        })? {
            let dlpack_field = extract_py_dlpack_encode_field(dict.py(), key.as_str(), &value_obj)?;
            plan.dlpack_meta_entries.push(dlpack_field.meta_entry);
            plan.push_borrowed_bytes_entry(
                FLAT_KV_TYPE_BYTES,
                key_utf8,
                dlpack_field.data_ptr,
                dlpack_field.nbytes,
                dlpack_field.keepalive_object,
            )?;
            continue;
        }
        return Err(CoreKvError::Api(CoreApiError::Unknown {
            detail: format!(
                "KV put() only supports a flat dict value: Dict[str, Union[int, float, bool, str, bytes, dlpack]]. Field {key:?} has unsupported type {}",
                py_type_name(&value_obj)
            ),
        }));
    }
    Ok(plan)
}

pub(crate) fn decode_flat_dict_to_wrapped_py_object(
    py: Python<'_>,
    data_owner: FlatDictDataOwner,
) -> Result<PyObject, CoreKvError> {
    let data = data_owner.bytes();
    let items = py
        .allow_threads(|| flat_kv_decode_borrowed(data))
        .map_err(|err| {
            CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: format!("flat dict decode failed: {}", err),
            })
        })?;
    let mut dlpack_meta = None::<BTreeMap<String, DecodedDlpackMeta>>;
    for item in &items {
        if item.key != INTERNAL_DLPACK_META_KEY {
            continue;
        }
        if dlpack_meta.is_some() {
            return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                detail: format!("duplicate reserved key not allowed: {INTERNAL_DLPACK_META_KEY:?}"),
            }));
        }
        let meta_bytes = match item.value {
            BorrowedFlatKvValueRange::BytesRange { start, len } => range_slice(data, start, len)?,
            _ => {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: "dlpack meta field must be bytes".to_string(),
                }));
            }
        };
        dlpack_meta = Some(decode_dlpack_meta(meta_bytes)?);
    }

    let dict = PyDict::new_bound(py);
    let mut wrapped_tensor_keys = BTreeSet::<String>::new();
    for item in items {
        let key = item.key;
        if key == INTERNAL_DLPACK_META_KEY {
            continue;
        }
        let key_obj = PyString::new_bound(py, key);
        let value_obj: PyObject = match item.value {
            BorrowedFlatKvValueRange::Bool(value) => value.into_py(py),
            BorrowedFlatKvValueRange::Int64(value) => value.into_py(py),
            BorrowedFlatKvValueRange::Float64(value) => value.into_py(py),
            BorrowedFlatKvValueRange::String(value) => PyString::new_bound(py, value).into(),
            BorrowedFlatKvValueRange::BytesRange { start, len } => {
                if let Some(meta) = dlpack_meta.as_ref().and_then(|all_meta| all_meta.get(key)) {
                    if len != meta.nbytes {
                        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                            detail: format!(
                                "dlpack field {key:?} payload size mismatch: expected {} bytes, got {}",
                                meta.nbytes, len
                            ),
                        }));
                    }
                    let _ = range_slice(data, start, len)?;
                    wrapped_tensor_keys.insert(key.to_string());
                    Py::new(
                        py,
                        FlatDictDLPackView::new(
                            data_owner.clone(),
                            start,
                            len,
                            meta.dtype_code,
                            meta.bits,
                            meta.lanes,
                            meta.shape.clone(),
                        ),
                    )
                    .map(|obj| obj.into_py(py))
                    .map_err(|err| {
                        CoreKvError::Api(CoreApiError::Unknown {
                            detail: format!("build dlpack view failed: {err}"),
                        })
                    })?
                } else {
                    PyBytes::new_bound(py, range_slice(data, start, len)?).into()
                }
            }
        };
        dict.set_item(key_obj, value_obj).map_err(|err| {
            CoreKvError::Api(CoreApiError::Unknown {
                detail: format!("build payload dict failed: {}", err),
            })
        })?;
    }
    if let Some(meta) = dlpack_meta {
        for meta_key in meta.keys() {
            if !wrapped_tensor_keys.contains(meta_key) {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!("dlpack field {meta_key:?} is missing or not bytes"),
                }));
            }
        }
    }
    Ok(dict.into())
}
