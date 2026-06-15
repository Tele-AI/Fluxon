use std::sync::Arc;

use fluxon_kv::memholder::{
    ExternalMemHolder as RustExternalMemHolder, UserMemHolder as RustMemHolder,
};
use parking_lot::RwLock;
use pyo3::prelude::*;

use super::{ApiResult, new_general_error};
use crate::flatdict_zerocopy::{FlatDictDataOwner, decode_flat_dict_to_wrapped_py_object};

enum MemHolderInner {
    Seg(Arc<RustMemHolder>),
    Owned(Arc<[u8]>),
}

/// Python wrapper for MemHolder with a stable top-level API.
#[pyclass]
pub struct MemHolder {
    holder: MemHolderInner,
    access_cache: RwLock<Option<PyObject>>,
}

#[pymethods]
impl MemHolder {
    /// Decode the held flat dict as a Python dict.
    ///
    /// The binary parsing runs without the Python GIL; only Python object creation holds the GIL.
    fn access(&self, py: Python) -> PyObject {
        fn access_inner(holder: &MemHolder, py: Python) -> ApiResult<PyObject> {
            if let Some(cached) = holder.access_cache.read().as_ref() {
                return ApiResult::new_success(cached.clone_ref(py).into_py(py));
            }

            let data_owner = match &holder.holder {
                MemHolderInner::Seg(seg_holder) => {
                    FlatDictDataOwner::UserMemHolder(seg_holder.clone())
                }
                MemHolderInner::Owned(bytes) => FlatDictDataOwner::OwnedBytes(bytes.clone()),
            };
            match decode_flat_dict_to_wrapped_py_object(py, data_owner) {
                Ok(obj) => {
                    *holder.access_cache.write() = Some(obj.clone_ref(py));
                    ApiResult::new_success(obj)
                }
                Err(err) => ApiResult::new_error(crate::error::py_error_from_kv_error(
                    py,
                    &err,
                    "flat dict decode failed",
                )),
            }
        }
        access_inner(self, py).into_py_object(py)
    }
}

impl MemHolder {
    pub(crate) fn new(holder: Arc<RustMemHolder>) -> Self {
        Self {
            holder: MemHolderInner::Seg(holder),
            access_cache: RwLock::new(None),
        }
    }

    pub(crate) fn new_owned(bytes: Vec<u8>) -> Self {
        Self {
            holder: MemHolderInner::Owned(Arc::<[u8]>::from(bytes)),
            access_cache: RwLock::new(None),
        }
    }

    pub(crate) fn into_py_mem_holder(self, py: Python) -> ApiResult<PyObject> {
        // Import fluxon_py; FluxonMemHolder is re-exported at the top level
        let unified_module = match py.import_bound("fluxon_py") {
            Ok(module) => module,
            Err(e) => {
                // Fallback: create a simple wrapper if unified module is not available
                return ApiResult::new_error(new_general_error(
                    py,
                    &format!("Unified module not found: {:?}", e),
                ));
            }
        };

        // Get the FluxonMemHolder class
        let unified_mem_holder_class = match unified_module.getattr("FluxonMemHolder") {
            Ok(class) => class,
            Err(e) => {
                // Fallback: create a simple wrapper if UnifiedMemHolder is not available
                return ApiResult::new_error(new_general_error(
                    py,
                    &format!("FluxonMemHolder class not found: {:?}", e),
                ));
            }
        };

        // Create a new FluxonMemHolder instance with self as the inner holder
        match unified_mem_holder_class.call1((self,)) {
            Ok(unified_holder) => ApiResult::new_success(unified_holder.into_py(py)),
            Err(e) => {
                // Fallback: return an error if creation fails
                ApiResult::new_error(new_general_error(
                    py,
                    &format!("Failed to create FluxonMemHolder: {:?}", e),
                ))
            }
        }
    }
}

/// Python wrapper for external memory holder
#[pyclass]
pub struct ExternalMemHolder {
    pub(crate) holder: Arc<RustExternalMemHolder>,
    access_cache: RwLock<Option<PyObject>>,
}

#[pymethods]
impl ExternalMemHolder {
    /// Decode the held flat dict as a Python dict.
    ///
    /// The binary parsing runs without the Python GIL; only Python object creation holds the GIL.
    fn access(&self, py: Python) -> PyObject {
        fn access_inner(holder: &ExternalMemHolder, py: Python) -> ApiResult<PyObject> {
            if let Some(cached) = holder.access_cache.read().as_ref() {
                return ApiResult::new_success(cached.clone_ref(py).into_py(py));
            }

            let data_owner = FlatDictDataOwner::ExternalMemHolder(holder.holder.clone());
            match decode_flat_dict_to_wrapped_py_object(py, data_owner) {
                Ok(obj) => {
                    *holder.access_cache.write() = Some(obj.clone_ref(py));
                    ApiResult::new_success(obj)
                }
                Err(err) => ApiResult::new_error(crate::error::py_error_from_kv_error(
                    py,
                    &err,
                    "flat dict decode failed",
                )),
            }
        }
        access_inner(self, py).into_py_object(py)
    }
}

impl ExternalMemHolder {
    pub(crate) fn new(holder: Arc<RustExternalMemHolder>) -> Self {
        Self {
            holder,
            access_cache: RwLock::new(None),
        }
    }

    pub(crate) fn into_py_mem_holder(self, py: Python) -> ApiResult<PyObject> {
        // Import fluxon_py; FluxonMemHolder is re-exported at the top level
        let unified_module = match py.import_bound("fluxon_py") {
            Ok(module) => module,
            Err(e) => {
                return ApiResult::new_error(new_general_error(
                    py,
                    &format!("Unified module not found: {:?}", e),
                ));
            }
        };

        // Get the FluxonMemHolder class
        let unified_mem_holder_class = match unified_module.getattr("FluxonMemHolder") {
            Ok(class) => class,
            Err(e) => {
                return ApiResult::new_error(new_general_error(
                    py,
                    &format!("FluxonMemHolder class not found: {:?}", e),
                ));
            }
        };

        // Create a new FluxonMemHolder instance with self as the inner holder
        match unified_mem_holder_class.call1((self,)) {
            Ok(unified_holder) => ApiResult::new_success(unified_holder.into_py(py)),
            Err(e) => ApiResult::new_error(new_general_error(
                py,
                &format!("Failed to create FluxonMemHolder: {:?}", e),
            )),
        }
    }
}
