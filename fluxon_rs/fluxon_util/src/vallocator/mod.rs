mod frame;
pub use frame::*;

pub mod test;

use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};

crate::define_error_code_enum_with_from! {
    #[repr(i32)]
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum AllocErrorCode {
        Success = 0,
        OutOfMemory = 1,
        InvalidSize = 2,
        InvalidPointer = 3,
        DoubleFree = 4,
        InvalidAddress = 5,
        NewException = 6,
        DeallocationException = 7,
        DeallocationUnknownException = 8,
        AllocationFailed = 9,
        AllocationException = 10,
        AllocationUnknownException = 11,
        SizeNotAligned = 12,
        InvalidCode = 10000000,
    }
    default: AllocErrorCode::InvalidCode
}

#[derive(Debug, Clone)]
pub struct AllocError {
    pub code: AllocErrorCode,
    pub message: String,
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AllocError({:?}): {}", self.code, self.message)
    }
}

impl std::error::Error for AllocError {}

pub const ORDER: usize = 64;
const CONTIGUOUS_ALLOCATION_ALIGNMENT: u64 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocRegion {
    pub start_addr: u64,
    pub size: u64,
}

#[derive(Debug)]
struct ContiguousRangeAllocator {
    free_by_start: BTreeMap<u64, u64>,
    free_by_size: BTreeSet<(u64, u64)>,
    allocated: u64,
    total: u64,
}

impl ContiguousRangeAllocator {
    fn new(total: u64) -> Self {
        let mut allocator = Self {
            free_by_start: BTreeMap::new(),
            free_by_size: BTreeSet::new(),
            allocated: 0,
            total,
        };
        if total != 0 {
            allocator.insert_free_range(0, total);
        }
        allocator
    }

    fn aligned_capacity(size: u64) -> Option<u64> {
        size.checked_add(CONTIGUOUS_ALLOCATION_ALIGNMENT - 1)
            .map(|value| value / CONTIGUOUS_ALLOCATION_ALIGNMENT * CONTIGUOUS_ALLOCATION_ALIGNMENT)
    }

    fn insert_free_range(&mut self, start: u64, len: u64) {
        if len == 0 {
            return;
        }
        assert!(self.free_by_start.insert(start, len).is_none());
        assert!(self.free_by_size.insert((len, start)));
    }

    fn remove_free_range(&mut self, start: u64, len: u64) {
        assert_eq!(self.free_by_start.remove(&start), Some(len));
        assert!(self.free_by_size.remove(&(len, start)));
    }

    fn alloc(&mut self, requested: u64) -> Option<(u64, u64)> {
        let capacity = Self::aligned_capacity(requested)?;
        let (range_len, range_start) = self.free_by_size.range((capacity, 0)..).next().copied()?;
        self.remove_free_range(range_start, range_len);
        let remaining = range_len - capacity;
        if remaining != 0 {
            self.insert_free_range(range_start + capacity, remaining);
        }
        self.allocated = self.allocated.checked_add(capacity)?;
        Some((range_start, capacity))
    }

    fn free(&mut self, start: u64, capacity: u64) -> Result<u64, AllocError> {
        let end = start.checked_add(capacity).ok_or_else(|| AllocError {
            code: AllocErrorCode::InvalidAddress,
            message: format!("Allocation range overflows: start={start} capacity={capacity}"),
        })?;
        if capacity == 0
            || capacity % CONTIGUOUS_ALLOCATION_ALIGNMENT != 0
            || start % CONTIGUOUS_ALLOCATION_ALIGNMENT != 0
            || end > self.total
        {
            return Err(AllocError {
                code: AllocErrorCode::InvalidAddress,
                message: format!(
                    "Invalid aligned allocation range: start={start} capacity={capacity} total={}",
                    self.total
                ),
            });
        }

        let previous = self
            .free_by_start
            .range(..start)
            .next_back()
            .map(|(&range_start, &range_len)| (range_start, range_len));
        if previous
            .is_some_and(|(range_start, range_len)| range_start.saturating_add(range_len) > start)
        {
            return Err(AllocError {
                code: AllocErrorCode::DoubleFree,
                message: format!(
                    "Range overlaps previous free range: start={start} capacity={capacity}"
                ),
            });
        }
        let next = self
            .free_by_start
            .range(start..)
            .next()
            .map(|(&range_start, &range_len)| (range_start, range_len));
        if next.is_some_and(|(range_start, _)| range_start < end) {
            return Err(AllocError {
                code: AllocErrorCode::DoubleFree,
                message: format!(
                    "Range overlaps next free range: start={start} capacity={capacity}"
                ),
            });
        }

        let mut merged_start = start;
        let mut merged_len = capacity;
        if let Some((previous_start, previous_len)) = previous
            && previous_start + previous_len == start
        {
            self.remove_free_range(previous_start, previous_len);
            merged_start = previous_start;
            merged_len += previous_len;
        }
        if let Some((next_start, next_len)) = next
            && end == next_start
        {
            self.remove_free_range(next_start, next_len);
            merged_len += next_len;
        }
        self.insert_free_range(merged_start, merged_len);
        self.allocated = self
            .allocated
            .checked_sub(capacity)
            .ok_or_else(|| AllocError {
                code: AllocErrorCode::DoubleFree,
                message: format!(
                    "Allocated byte counter underflow: allocated={} capacity={capacity}",
                    self.allocated
                ),
            })?;
        Ok(capacity)
    }
}

pub struct VirtualAllocator {
    inner: RwLock<ContiguousRangeAllocator>,
}

impl std::fmt::Debug for VirtualAllocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let allocator = self.inner.read();
        f.debug_struct("VirtualAllocator")
            .field("total_size", &allocator.total)
            .field("allocated_size", &allocator.allocated)
            .finish()
    }
}

impl VirtualAllocator {
    pub fn new(total_size: u64) -> Result<Self, AllocError> {
        Ok(Self {
            inner: RwLock::new(ContiguousRangeAllocator::new(total_size)),
        })
    }

    pub fn get_allocated_size(&self) -> u64 {
        self.inner.read().allocated
    }

    pub fn get_total_size(&self) -> u64 {
        self.inner.read().total
    }

    pub fn get_free_size(&self) -> u64 {
        let allocator = self.inner.read();
        allocator.total.saturating_sub(allocator.allocated)
    }

    pub fn alloc(&self, size: u64) -> Result<AllocRegion, AllocError> {
        if size == 0 {
            return Err(AllocError {
                code: AllocErrorCode::InvalidSize,
                message: format!("Invalid allocation size: {}", size),
            });
        }

        let mut allocator = self.inner.write();
        if let Some((addr, actual_size)) = allocator.alloc(size) {
            if actual_size < size {
                return Err(AllocError {
                    code: AllocErrorCode::AllocationFailed,
                    message: format!(
                        "Allocated size {} is less than requested size {}",
                        actual_size, size
                    ),
                });
            }
            return Ok(AllocRegion {
                start_addr: addr,
                size: actual_size,
            });
        }

        Err(AllocError {
            code: AllocErrorCode::OutOfMemory,
            message: "Out of memory".to_string(),
        })
    }

    pub fn free(&self, ptr: u64, size: u64) -> Result<u64, AllocError> {
        if size == 0 {
            return Err(AllocError {
                code: AllocErrorCode::InvalidSize,
                message: format!("Invalid free size: {}", size),
            });
        }

        let mut allocator = self.inner.write();
        let freed_size = allocator.free(ptr, size)?;
        if freed_size != size {
            return Err(AllocError {
                code: AllocErrorCode::DeallocationException,
                message: format!(
                    "Freed size {} is not equal to requested size {}",
                    freed_size, size
                ),
            });
        }
        Ok(freed_size)
    }
}
