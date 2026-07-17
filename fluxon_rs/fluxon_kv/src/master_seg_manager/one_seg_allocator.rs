use fluxon_util::vallocator::VirtualAllocator;

use crate::rpcresp_kvresult_convert::msg_and_error::KvResult;
use std::sync::Arc;

use super::msg_pack::{SegmentDeviceDescription, SegmentDeviceID};

/// An RAII guard for a memory allocation from a `OneSegAllocator`.
///
/// When this guard is dropped, it attempts to free the memory block
/// it represents from its parent allocator.
///
/// size bytes value stored in capcity bytes allocated memory block
pub struct Allocation {
    addr: u64,
    size: u64,
    capcity: u64,

    /// Used to free the allocation with RAII deref
    /// There's no circular reference, so we use arc
    allocator: Arc<OneSegAllocator>,
    /// Optional callback invoked when this allocation is dropped.
    /// Used by upper layers to perform side effects (e.g., capacity restoration).
    on_drop: Option<Box<dyn Fn() + Send + Sync + 'static>>,
}

// Custom Debug to avoid requiring Debug on the callback closure.
impl std::fmt::Debug for Allocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Allocation")
            .field("addr", &self.addr)
            .field("size", &self.size)
            .field("capcity", &self.capcity)
            // Avoid printing the closure; just indicate presence
            .field("on_drop", &self.on_drop.as_ref().map(|_| "<callback>"))
            // Show allocator base addr for quick identification
            .field("allocator_base_addr", &self.allocator.base_addr)
            .finish()
    }
}

impl Allocation {
    /// Creates a new allocation guard. This is typically done by the allocator.
    pub fn new(addr: u64, size: u64, capcity: u64, allocator: Arc<OneSegAllocator>) -> Self {
        Self {
            addr,
            size,
            capcity,
            allocator,
            on_drop: None,
        }
    }

    /// Returns the addr of the allocation.
    pub fn addr(&self) -> u64 {
        self.addr
    }

    /// Returns the value size of the allocation.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns the capacity size of the allocation.
    pub fn capcity(&self) -> u64 {
        self.capcity
    }

    /// Returns the base address of the underlying segment allocator.
    /// Direct access is safe: `Allocation` holds a strong `Arc` to allocator.
    pub fn base_addr(&self) -> u64 {
        self.allocator.base_addr
    }

    /// Returns whether this allocation was created by the exact allocator instance.
    ///
    /// Segment names and node ids are reusable after a node reconnects.  Pointer identity is
    /// therefore the only safe way for master-side completion paths to bind an allocation back
    /// to the registration generation that created it.
    pub(crate) fn belongs_to_allocator(&self, allocator: &Arc<OneSegAllocator>) -> bool {
        Arc::ptr_eq(&self.allocator, allocator)
    }

    /// Attach an on-drop callback. It will be executed exactly once
    /// when this allocation is dropped.
    pub fn set_on_drop<F>(&mut self, f: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_drop = Some(Box::new(f));
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        // Run user-defined on-drop hook first (if any)
        if let Some(f) = self.on_drop.take() {
            (f)();
        }
        self.allocator.free(self.addr, self.capcity);
    }
}

/// A thread-safe allocator for a single contiguous memory region using VirtualAllocator.
#[derive(Debug)]
pub struct OneSegAllocator {
    pub seg_device_id: SegmentDeviceID,
    pub seg_device_desc: SegmentDeviceDescription,
    pub base_addr: u64,
    inner: VirtualAllocator,
}

impl OneSegAllocator {
    /// Creates a new allocator for a region.
    pub fn new(
        seg_device_id: SegmentDeviceID,
        seg_device_desc: SegmentDeviceDescription,
        base_addr: u64,
        size: u64,
    ) -> KvResult<Self> {
        let inner = VirtualAllocator::new(size as u64)?;
        Ok(Self {
            seg_device_id,
            seg_device_desc,
            base_addr,
            inner,
        })
    }

    /// Allocates a block of memory of `size` bytes.
    /// Returns an RAII guard for the allocation.
    pub fn allocate(self: &Arc<Self>, size: u64) -> KvResult<Allocation> {
        let region = self.inner.alloc(size as u64)?;
        // return base0 offset in addr (pure offset); base address is carried separately
        Ok(Allocation::new(
            region.start_addr,
            size,
            region.size,
            Arc::clone(self),
        ))
    }

    /// Frees a block of memory.
    fn free(&self, addr: u64, capcity: u64) {
        // addr is offset (base0); free directly
        let _ = self.inner.free(addr, capcity as u64);
    }

    /// Returns total capacity (bytes) of this segment.
    pub fn total_size_bytes(&self) -> u64 {
        self.inner.get_total_size() as u64
    }

    /// Returns currently allocated bytes in this segment.
    pub fn used_size_bytes(&self) -> u64 {
        self.inner.get_allocated_size() as u64
    }
}
