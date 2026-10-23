use fluxon_util::fixed_slab_allocator::FixedSlabAllocator as RustFixedSlabAllocator;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Thread-safe fixed-slot slab allocator.
///
/// `try_reserve` is all-or-none. `release` rejects invalid input atomically.
#[pyclass(module = "fluxon_pyo3")]
pub struct FixedSlabAllocator {
    inner: RustFixedSlabAllocator,
}

#[pymethods]
impl FixedSlabAllocator {
    #[new]
    fn new(slot_count: u32) -> PyResult<Self> {
        let inner = RustFixedSlabAllocator::new(slot_count)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }

    /// Reserve exactly `count` slots, returning `None` if capacity is unavailable.
    fn try_reserve(&self, count: u32) -> Option<Vec<u32>> {
        self.inner.try_reserve(count)
    }

    /// Release all slots atomically after bounds, duplicate, and live-state checks.
    fn release(&self, slots: Vec<u32>) -> PyResult<()> {
        self.inner
            .release(&slots)
            .map_err(|error| PyValueError::new_err(error.to_string()))
    }

    #[getter]
    fn capacity(&self) -> u32 {
        self.inner.capacity()
    }

    #[getter]
    fn free_count(&self) -> u32 {
        self.inner.free_count()
    }

    #[getter]
    fn live_count(&self) -> u32 {
        self.inner.live_count()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
