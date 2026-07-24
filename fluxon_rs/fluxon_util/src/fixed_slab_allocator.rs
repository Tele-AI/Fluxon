use parking_lot::Mutex;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FixedSlabAllocatorError {
    ZeroCapacity,
    DuplicateSlot { slot: u32 },
    SlotOutOfBounds { slot: u32, capacity: u32 },
    SlotNotAllocated { slot: u32 },
}

impl Display for FixedSlabAllocatorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroCapacity => write!(formatter, "fixed slab capacity must be positive"),
            Self::DuplicateSlot { slot } => {
                write!(
                    formatter,
                    "fixed slab release contains duplicate slot {slot}"
                )
            }
            Self::SlotOutOfBounds { slot, capacity } => write!(
                formatter,
                "fixed slab slot {slot} is outside capacity {capacity}"
            ),
            Self::SlotNotAllocated { slot } => {
                write!(
                    formatter,
                    "fixed slab slot {slot} is not currently allocated"
                )
            }
        }
    }
}

impl Error for FixedSlabAllocatorError {}

#[derive(Debug)]
struct FixedSlabState {
    free_slots: Vec<u32>,
    allocated: Vec<bool>,
    validation_marks: Vec<u64>,
    validation_epoch: u64,
}

/// Thread-safe allocator for a fixed number of equally sized slab slots.
///
/// Reservations are all-or-none. Releases validate every supplied slot before
/// changing allocator state, so an invalid release cannot partially free slots.
#[derive(Debug)]
pub struct FixedSlabAllocator {
    capacity: u32,
    state: Mutex<FixedSlabState>,
}

impl FixedSlabAllocator {
    pub fn new(capacity: u32) -> Result<Self, FixedSlabAllocatorError> {
        if capacity == 0 {
            return Err(FixedSlabAllocatorError::ZeroCapacity);
        }
        Ok(Self {
            capacity,
            state: Mutex::new(FixedSlabState {
                free_slots: (0..capacity).rev().collect(),
                allocated: vec![false; capacity as usize],
                validation_marks: vec![0; capacity as usize],
                validation_epoch: 0,
            }),
        })
    }

    /// Reserves exactly `count` slots, or returns `None` without changing state.
    pub fn try_reserve(&self, count: u32) -> Option<Vec<u32>> {
        let mut state = self.state.lock();
        let count = count as usize;
        if count > state.free_slots.len() {
            return None;
        }

        let mut slots = Vec::with_capacity(count);
        for _ in 0..count {
            let slot = state
                .free_slots
                .pop()
                .expect("fixed slab free count was checked before reservation");
            let was_allocated = std::mem::replace(&mut state.allocated[slot as usize], true);
            assert!(!was_allocated, "fixed slab freelist contained a live slot");
            slots.push(slot);
        }
        Some(slots)
    }

    /// Atomically releases all supplied slots after validating the full input.
    pub fn release(&self, slots: &[u32]) -> Result<(), FixedSlabAllocatorError> {
        let mut state = self.state.lock();
        state.validation_epoch = state.validation_epoch.wrapping_add(1);
        if state.validation_epoch == 0 {
            state.validation_marks.fill(0);
            state.validation_epoch = 1;
        }
        let validation_epoch = state.validation_epoch;

        for &slot in slots {
            if slot >= self.capacity {
                return Err(FixedSlabAllocatorError::SlotOutOfBounds {
                    slot,
                    capacity: self.capacity,
                });
            }
            let slot_index = slot as usize;
            if state.validation_marks[slot_index] == validation_epoch {
                return Err(FixedSlabAllocatorError::DuplicateSlot { slot });
            }
            state.validation_marks[slot_index] = validation_epoch;
        }
        for &slot in slots {
            if !state.allocated[slot as usize] {
                return Err(FixedSlabAllocatorError::SlotNotAllocated { slot });
            }
        }

        for &slot in slots {
            state.allocated[slot as usize] = false;
            state.free_slots.push(slot);
        }
        Ok(())
    }

    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    pub fn free_count(&self) -> u32 {
        self.state.lock().free_slots.len() as u32
    }

    pub fn live_count(&self) -> u32 {
        self.capacity - self.free_count()
    }

    pub fn is_empty(&self) -> bool {
        self.free_count() == self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::{FixedSlabAllocator, FixedSlabAllocatorError};
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn rejects_zero_capacity() {
        assert!(matches!(
            FixedSlabAllocator::new(0),
            Err(FixedSlabAllocatorError::ZeroCapacity)
        ));
    }

    #[test]
    fn reserve_is_ordered_and_all_or_none() {
        let allocator = FixedSlabAllocator::new(4).unwrap();
        assert_eq!(allocator.try_reserve(3), Some(vec![0, 1, 2]));
        assert_eq!(allocator.free_count(), 1);
        assert_eq!(allocator.live_count(), 3);

        assert_eq!(allocator.try_reserve(2), None);
        assert_eq!(allocator.free_count(), 1);
        assert_eq!(allocator.try_reserve(1), Some(vec![3]));
        assert_eq!(allocator.try_reserve(0), Some(Vec::new()));
        assert!(!allocator.is_empty());
    }

    #[test]
    fn released_slots_are_reused_and_counts_close() {
        let allocator = FixedSlabAllocator::new(4).unwrap();
        let slots = allocator.try_reserve(3).unwrap();
        allocator.release(&slots[2..]).unwrap();
        assert_eq!(allocator.try_reserve(1), Some(vec![2]));
        allocator.release(&slots[..2]).unwrap();
        allocator.release(&[2]).unwrap();

        assert_eq!(allocator.capacity(), 4);
        assert_eq!(allocator.free_count(), 4);
        assert_eq!(allocator.live_count(), 0);
        assert!(allocator.is_empty());
    }

    #[test]
    fn invalid_release_is_atomic() {
        let allocator = FixedSlabAllocator::new(4).unwrap();
        assert_eq!(allocator.try_reserve(2), Some(vec![0, 1]));

        assert_eq!(
            allocator.release(&[0, 0]),
            Err(FixedSlabAllocatorError::DuplicateSlot { slot: 0 })
        );
        assert_eq!(allocator.live_count(), 2);
        assert_eq!(
            allocator.release(&[0, 4]),
            Err(FixedSlabAllocatorError::SlotOutOfBounds {
                slot: 4,
                capacity: 4,
            })
        );
        assert_eq!(allocator.live_count(), 2);

        allocator.release(&[0]).unwrap();
        assert_eq!(
            allocator.release(&[0]),
            Err(FixedSlabAllocatorError::SlotNotAllocated { slot: 0 })
        );
        assert_eq!(allocator.live_count(), 1);
        allocator.release(&[1]).unwrap();
        assert!(allocator.is_empty());
    }

    #[test]
    fn concurrent_single_slot_reservations_are_unique() {
        let allocator = Arc::new(FixedSlabAllocator::new(64).unwrap());
        let reserved = Arc::new(Mutex::new(Vec::new()));
        let workers = (0..8)
            .map(|_| {
                let allocator = Arc::clone(&allocator);
                let reserved = Arc::clone(&reserved);
                thread::spawn(move || {
                    while let Some(mut slots) = allocator.try_reserve(1) {
                        reserved.lock().unwrap().append(&mut slots);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }

        let slots = reserved.lock().unwrap();
        assert_eq!(slots.len(), 64);
        assert_eq!(slots.iter().copied().collect::<HashSet<_>>().len(), 64);
        allocator.release(&slots).unwrap();
        assert!(allocator.is_empty());
    }
}
