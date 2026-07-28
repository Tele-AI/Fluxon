use fluxon_util::vallocator::VirtualAllocator;
use parking_lot::Mutex;

use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use std::sync::Arc;

use super::msg_pack::{SegmentDeviceDescription, SegmentDeviceID};

/// Runtime capacity state shared by every segment registered by one node generation.
///
/// Physical capacity describes the already allocated and registered memory. Active capacity is
/// the allocation budget currently exposed to Fluxon; the remainder is parked without changing
/// the underlying mapping or registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePoolCapacitySnapshot {
    pub physical_capacity_bytes: u64,
    pub active_capacity_bytes: u64,
    pub used_capacity_bytes: u64,
    pub parked_capacity_bytes: u64,
    pub draining_capacity_bytes: u64,
    pub available_capacity_bytes: u64,
    pub capacity_epoch: u64,
}

#[derive(Debug)]
struct NodePoolCapacityState {
    physical_capacity_bytes: u64,
    active_capacity_bytes: u64,
    used_capacity_bytes: u64,
    capacity_epoch: u64,
}

/// One generation-scoped allocation budget shared by all of a node's segment allocators.
#[derive(Debug)]
pub(crate) struct NodePoolCapacityBudget {
    state: Mutex<NodePoolCapacityState>,
}

impl NodePoolCapacityBudget {
    pub(crate) fn new(physical_capacity_bytes: u64) -> KvResult<Self> {
        if physical_capacity_bytes == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "node pool physical capacity must be greater than zero".to_string(),
            }));
        }
        Ok(Self {
            state: Mutex::new(NodePoolCapacityState {
                physical_capacity_bytes,
                active_capacity_bytes: physical_capacity_bytes,
                used_capacity_bytes: 0,
                capacity_epoch: 1,
            }),
        })
    }

    fn snapshot_locked(state: &NodePoolCapacityState) -> NodePoolCapacitySnapshot {
        NodePoolCapacitySnapshot {
            physical_capacity_bytes: state.physical_capacity_bytes,
            active_capacity_bytes: state.active_capacity_bytes,
            used_capacity_bytes: state.used_capacity_bytes,
            parked_capacity_bytes: state
                .physical_capacity_bytes
                .saturating_sub(state.active_capacity_bytes),
            draining_capacity_bytes: state
                .used_capacity_bytes
                .saturating_sub(state.active_capacity_bytes),
            available_capacity_bytes: state
                .active_capacity_bytes
                .saturating_sub(state.used_capacity_bytes),
            capacity_epoch: state.capacity_epoch,
        }
    }

    pub(crate) fn snapshot(&self) -> NodePoolCapacitySnapshot {
        Self::snapshot_locked(&self.state.lock())
    }

    /// Change only the active allocation budget. Existing allocations above a lower target are
    /// accounted as draining bytes and remain valid until the normal single-KV reclaim path drops
    /// them.
    pub(crate) fn set_active_capacity(
        &self,
        expected_capacity_epoch: u64,
        active_capacity_bytes: u64,
    ) -> KvResult<NodePoolCapacitySnapshot> {
        let mut state = self.state.lock();
        if state.capacity_epoch != expected_capacity_epoch {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "stale node pool capacity epoch: expected={} current={}",
                    expected_capacity_epoch, state.capacity_epoch
                ),
            }));
        }
        if active_capacity_bytes == 0 || active_capacity_bytes > state.physical_capacity_bytes {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: format!(
                    "active capacity must be in 1..={}: got {}",
                    state.physical_capacity_bytes, active_capacity_bytes
                ),
            }));
        }
        if state.active_capacity_bytes != active_capacity_bytes {
            state.active_capacity_bytes = active_capacity_bytes;
            state.capacity_epoch = state
                .capacity_epoch
                .checked_add(1)
                .expect("node pool capacity epoch overflow");
        }
        Ok(Self::snapshot_locked(&state))
    }

    /// Extend a live generation when it registers an additional segment. A fully active pool
    /// grows with its physical mapping; an already parked pool preserves its active byte target.
    pub(crate) fn extend_physical_capacity(&self, additional_bytes: u64) -> KvResult<()> {
        if additional_bytes == 0 {
            return Err(KvError::Api(ApiError::InvalidArgument {
                detail: "additional physical capacity must be greater than zero".to_string(),
            }));
        }
        let mut state = self.state.lock();
        let was_fully_active = state.active_capacity_bytes == state.physical_capacity_bytes;
        state.physical_capacity_bytes = state
            .physical_capacity_bytes
            .checked_add(additional_bytes)
            .ok_or_else(|| {
                KvError::Api(ApiError::InvalidArgument {
                    detail: "node pool physical capacity overflow".to_string(),
                })
            })?;
        if was_fully_active {
            state.active_capacity_bytes = state.physical_capacity_bytes;
        }
        state.capacity_epoch = state
            .capacity_epoch
            .checked_add(1)
            .expect("node pool capacity epoch overflow");
        Ok(())
    }

    fn allocate(&self, allocator: &VirtualAllocator, size: u64) -> KvResult<(u64, u64)> {
        // The budget lock spans the physical allocator mutation. Capacity changes and allocations
        // therefore have one total order; a resize cannot be bypassed by a stale pre-check.
        let mut state = self.state.lock();
        if size
            > state
                .active_capacity_bytes
                .saturating_sub(state.used_capacity_bytes)
        {
            return Err(KvError::Api(ApiError::Allocator {
                detail: format!(
                    "node pool active capacity exhausted: requested={} active={} used={} epoch={}",
                    size,
                    state.active_capacity_bytes,
                    state.used_capacity_bytes,
                    state.capacity_epoch
                ),
            }));
        }
        let region = allocator.alloc(size)?;
        let Some(next_used) = state.used_capacity_bytes.checked_add(region.size) else {
            let _ = allocator.free(region.start_addr, region.size);
            return Err(KvError::Api(ApiError::Allocator {
                detail: "node pool used capacity overflow".to_string(),
            }));
        };
        if next_used > state.active_capacity_bytes {
            // VirtualAllocator rounds allocations to its alignment. Roll the physical mutation
            // back when the aligned size crosses the active byte boundary.
            let _ = allocator.free(region.start_addr, region.size);
            return Err(KvError::Api(ApiError::Allocator {
                detail: format!(
                    "node pool active capacity exhausted after alignment: requested={} aligned={} active={} used={} epoch={}",
                    size,
                    region.size,
                    state.active_capacity_bytes,
                    state.used_capacity_bytes,
                    state.capacity_epoch
                ),
            }));
        }
        state.used_capacity_bytes = next_used;
        Ok((region.start_addr, region.size))
    }

    fn free(&self, allocator: &VirtualAllocator, addr: u64, capacity: u64) {
        let mut state = self.state.lock();
        match allocator.free(addr, capacity) {
            Ok(freed) => {
                state.used_capacity_bytes = state
                    .used_capacity_bytes
                    .checked_sub(freed)
                    .expect("node pool used capacity underflow");
            }
            Err(err) => {
                tracing::error!(
                    addr,
                    capacity,
                    error = %err,
                    "failed to free segment allocation; preserving node pool used accounting"
                );
            }
        }
    }
}

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
    capacity_budget: Arc<NodePoolCapacityBudget>,
}

impl OneSegAllocator {
    /// Creates a new allocator for a region.
    pub fn new(
        seg_device_id: SegmentDeviceID,
        seg_device_desc: SegmentDeviceDescription,
        base_addr: u64,
        size: u64,
    ) -> KvResult<Self> {
        let capacity_budget = Arc::new(NodePoolCapacityBudget::new(size)?);
        Self::new_with_capacity_budget(
            seg_device_id,
            seg_device_desc,
            base_addr,
            size,
            capacity_budget,
        )
    }

    pub(crate) fn new_with_capacity_budget(
        seg_device_id: SegmentDeviceID,
        seg_device_desc: SegmentDeviceDescription,
        base_addr: u64,
        size: u64,
        capacity_budget: Arc<NodePoolCapacityBudget>,
    ) -> KvResult<Self> {
        let inner = VirtualAllocator::new(size as u64)?;
        Ok(Self {
            seg_device_id,
            seg_device_desc,
            base_addr,
            inner,
            capacity_budget,
        })
    }

    /// Allocates a block of memory of `size` bytes.
    /// Returns an RAII guard for the allocation.
    pub fn allocate(self: &Arc<Self>, size: u64) -> KvResult<Allocation> {
        let (start_addr, capacity) = self.capacity_budget.allocate(&self.inner, size)?;
        // return base0 offset in addr (pure offset); base address is carried separately
        Ok(Allocation::new(
            start_addr,
            size,
            capacity,
            Arc::clone(self),
        ))
    }

    /// Frees a block of memory.
    fn free(&self, addr: u64, capcity: u64) {
        // addr is offset (base0); free directly
        self.capacity_budget.free(&self.inner, addr, capcity);
    }

    /// Returns total capacity (bytes) of this segment.
    pub fn total_size_bytes(&self) -> u64 {
        self.inner.get_total_size() as u64
    }

    /// Returns currently allocated bytes in this segment.
    pub fn used_size_bytes(&self) -> u64 {
        self.inner.get_allocated_size() as u64
    }

    /// Returns the active/parked capacity state shared by this node generation.
    pub fn node_pool_capacity_snapshot(&self) -> NodePoolCapacitySnapshot {
        self.capacity_budget.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocator_with_budget(
        id: &str,
        size: u64,
        budget: Arc<NodePoolCapacityBudget>,
    ) -> Arc<OneSegAllocator> {
        Arc::new(
            OneSegAllocator::new_with_capacity_budget(
                id.to_string(),
                SegmentDeviceDescription::Cpu,
                0,
                size,
                budget,
            )
            .unwrap(),
        )
    }

    #[test]
    fn shrinking_below_used_blocks_new_allocations_until_normal_drop_drains() {
        let budget = Arc::new(NodePoolCapacityBudget::new(16 * 1024).unwrap());
        let allocator = allocator_with_budget("cpu", 16 * 1024, budget.clone());
        let first = allocator.allocate(8 * 1024).unwrap();

        let shrinking = budget.set_active_capacity(1, 4 * 1024).unwrap();
        assert_eq!(shrinking.capacity_epoch, 2);
        assert_eq!(shrinking.draining_capacity_bytes, 4 * 1024);
        assert!(allocator.allocate(1).is_err());

        drop(first);
        let drained = budget.snapshot();
        assert_eq!(drained.used_capacity_bytes, 0);
        assert_eq!(drained.draining_capacity_bytes, 0);
        assert!(allocator.allocate(4 * 1024).is_ok());
    }

    #[test]
    fn shared_node_budget_limits_allocations_across_segments_and_expands_immediately() {
        let budget = Arc::new(NodePoolCapacityBudget::new(16 * 1024).unwrap());
        let first_allocator = allocator_with_budget("cpu0", 8 * 1024, budget.clone());
        let second_allocator = allocator_with_budget("cpu1", 8 * 1024, budget.clone());
        let first = first_allocator.allocate(8 * 1024).unwrap();
        let _second = second_allocator.allocate(8 * 1024).unwrap();
        assert!(first_allocator.allocate(1).is_err());

        drop(first);
        budget.set_active_capacity(1, 12 * 1024).unwrap();
        assert!(first_allocator.allocate(4 * 1024).is_ok());
        assert!(second_allocator.allocate(1).is_err());

        let expanded = budget.set_active_capacity(2, 16 * 1024).unwrap();
        assert_eq!(expanded.capacity_epoch, 3);
        assert!(first_allocator.allocate(4 * 1024).is_ok());
    }

    #[test]
    fn stale_epoch_and_out_of_physical_range_updates_are_rejected() {
        let budget = NodePoolCapacityBudget::new(16 * 1024).unwrap();
        assert!(budget.set_active_capacity(0, 8 * 1024).is_err());
        assert!(budget.set_active_capacity(1, 0).is_err());
        assert!(budget.set_active_capacity(1, 32 * 1024).is_err());
        assert_eq!(budget.snapshot().capacity_epoch, 1);

        let updated = budget.set_active_capacity(1, 8 * 1024).unwrap();
        assert_eq!(updated.capacity_epoch, 2);
        assert!(budget.set_active_capacity(1, 4 * 1024).is_err());
        assert_eq!(budget.snapshot().active_capacity_bytes, 8 * 1024);
    }
}
