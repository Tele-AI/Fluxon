use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::{Allocation, Allocator, ExactStorageReport};

fn bitmap_report(occupied: &[bool], size: u32) -> ExactStorageReport {
    let mut report = ExactStorageReport::default();
    let mut run = 0;
    for used in occupied.iter().copied().chain(std::iter::once(true)) {
        if !used {
            run += 1;
        } else if run != 0 {
            report.total_free_space += run;
            report.largest_free_region = report.largest_free_region.max(run);
            report.allocatable_slots += u64::from(run / size);
            report.free_regions += 1;
            run = 0;
        }
    }
    report
}

#[test]
fn fragmented_regions_cannot_combine_to_make_a_slot() {
    let mut allocator: Allocator = Allocator::new(14);
    let left = allocator.allocate(6).unwrap();
    let separator = allocator.allocate(1).unwrap();
    let right = allocator.allocate(6).unwrap();
    let tail = allocator.allocate(1).unwrap();
    assert_eq!(
        allocator.storage_report_for_size(8),
        ExactStorageReport::default()
    );
    allocator.free(left);
    allocator.free(right);
    assert_eq!(
        allocator.storage_report_for_size(8),
        ExactStorageReport {
            total_free_space: 12,
            largest_free_region: 6,
            allocatable_slots: 0,
            free_regions: 2,
        }
    );
    assert!(allocator.allocate(8).is_none());

    allocator.free(separator);
    assert_eq!(
        allocator.storage_report_for_size(8),
        ExactStorageReport {
            total_free_space: 13,
            largest_free_region: 13,
            allocatable_slots: 1,
            free_regions: 1,
        }
    );
    allocator.free(tail);
    assert_eq!(allocator.storage_report_for_size(7).allocatable_slots, 2);
}

#[test]
fn reports_actual_lengths_instead_of_coarse_bin_sizes() {
    let mut allocator: Allocator = Allocator::new(128);
    let mut holes = Vec::new();
    let mut occupied = vec![true; 128];
    for length in [17, 19, 21, 23] {
        holes.push((allocator.allocate(length).unwrap(), length));
        allocator.allocate(1).unwrap();
    }
    allocator.allocate(44).unwrap();
    for (allocation, length) in holes {
        occupied[allocation.offset as usize..(allocation.offset + length) as usize].fill(false);
        allocator.free(allocation);
    }
    for size in [1, 7, 8, 17, 19, 23, 24] {
        assert_eq!(
            allocator.storage_report_for_size(size),
            bitmap_report(&occupied, size)
        );
    }
    assert_eq!(allocator.storage_report_for_size(7).allocatable_slots, 10);
    assert_eq!(allocator.storage_report_for_size(7).largest_free_region, 23);
    let rounded_slots: u64 = allocator
        .storage_report_full()
        .free_regions
        .iter()
        .map(|bin| u64::from(bin.size / 7) * u64::from(bin.count))
        .sum();
    assert_eq!(rounded_slots, 9);
}

#[test]
fn physical_report_does_not_change_bin_rounding_or_metadata_admission() {
    let mut rounded: Allocator = Allocator::new(17);
    let revision = rounded.layout_revision();
    assert_eq!(rounded.storage_report_for_size(17).allocatable_slots, 1);
    assert_eq!(rounded.storage_report_for_size(17).largest_free_region, 17);
    assert_eq!(rounded.storage_report().largest_free_region, 16);
    assert!(rounded.allocate(17).is_none());
    assert_eq!(rounded.layout_revision(), revision);

    let mut exhausted: Allocator = Allocator::with_max_allocs(64, 4);
    let first = exhausted.allocate(1).unwrap();
    let second = exhausted.allocate(1).unwrap();
    let revision = exhausted.layout_revision();
    assert!(exhausted.allocate(1).is_none());
    assert_eq!(exhausted.layout_revision(), revision);
    assert_eq!(exhausted.storage_report().total_free_space, 0);
    assert_eq!(
        exhausted.storage_report_for_size(8),
        ExactStorageReport {
            total_free_space: 62,
            largest_free_region: 62,
            allocatable_slots: 7,
            free_regions: 1,
        }
    );
    exhausted.free(first);
    assert_eq!(exhausted.layout_revision(), revision + 1);
    assert_eq!(exhausted.storage_report_for_size(1).total_free_space, 63);
    // An isolated free region still consumes the returned metadata node.
    assert!(exhausted.allocate(1).is_none());
    assert_eq!(exhausted.layout_revision(), revision + 1);
    // Coalescing all three physical regions releases spare metadata nodes.
    exhausted.free(second);
    assert_eq!(exhausted.layout_revision(), revision + 2);
    assert_eq!(exhausted.storage_report_for_size(1).total_free_space, 64);
    assert!(exhausted.allocate(1).is_some());
}

#[test]
fn revisions_cover_allocations_rollbacks_and_resets_without_reuse() {
    let mut allocator: Allocator = Allocator::new(32);
    let initial_revision = allocator.layout_revision();
    let initial_report = allocator.storage_report_for_size(8);
    let mut allocations = Vec::new();
    for index in 1..=3 {
        allocations.push(allocator.allocate(8).unwrap());
        assert_eq!(allocator.layout_revision(), initial_revision + index);
    }
    assert!(allocator.allocate(16).is_none());
    assert_eq!(allocator.layout_revision(), initial_revision + 3);
    for allocation in allocations {
        let before = allocator.layout_revision();
        allocator.free(allocation);
        assert_eq!(allocator.layout_revision(), before + 1);
    }
    assert_eq!(allocator.storage_report_for_size(8), initial_report);
    assert_eq!(allocator.layout_revision(), initial_revision + 6);
    allocator.reset();
    assert_eq!(allocator.storage_report_for_size(8), initial_report);
    assert_eq!(allocator.layout_revision(), initial_revision + 7);
    allocator.reset();
    assert_eq!(allocator.layout_revision(), initial_revision + 8);
    let zero = allocator.allocate(0).unwrap();
    assert_eq!(allocator.layout_revision(), initial_revision + 9);
    allocator.free(zero);
    assert_eq!(allocator.layout_revision(), initial_revision + 10);
}

#[test]
fn revision_overflow_rejects_mutation_before_changing_layout() {
    let mut allocator: Allocator = Allocator::new(32);
    let allocation = allocator.allocate(8).unwrap();
    let before = allocator.storage_report_for_size(8);
    allocator.layout_revision = u64::MAX;
    assert!(catch_unwind(AssertUnwindSafe(|| allocator.allocate(8))).is_err());
    assert_eq!(allocator.storage_report_for_size(8), before);
    assert!(catch_unwind(AssertUnwindSafe(|| allocator.free(allocation))).is_err());
    assert_eq!(allocator.storage_report_for_size(8), before);
    assert!(catch_unwind(AssertUnwindSafe(|| allocator.reset())).is_err());
    assert_eq!(allocator.storage_report_for_size(8), before);
    assert_eq!(allocator.layout_revision(), u64::MAX);
}

#[test]
fn randomized_reports_match_independent_occupancy_bitmap() {
    const CAPACITY: usize = 513;
    let mut allocator: Allocator = Allocator::with_max_allocs(CAPACITY as u32, 1024);
    let mut occupied = vec![false; CAPACITY];
    let mut allocations: Vec<(Allocation, u32)> = Vec::new();
    let mut random = 0x80d8_e474_9343_58bdu64;
    for step in 0..10_000 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let revision = allocator.layout_revision();
        if !allocations.is_empty() && random % 3 == 0 {
            let index = ((random >> 16) as usize) % allocations.len();
            let (allocation, size) = allocations.swap_remove(index);
            let span =
                &mut occupied[allocation.offset as usize..(allocation.offset + size) as usize];
            assert!(span.iter().all(|&used| used));
            span.fill(false);
            allocator.free(allocation);
            assert_eq!(allocator.layout_revision(), revision + 1);
        } else {
            let size = (random % 63 + 1) as u32;
            if let Some(allocation) = allocator.allocate(size) {
                let span =
                    &mut occupied[allocation.offset as usize..(allocation.offset + size) as usize];
                assert!(span.iter().all(|&used| !used));
                span.fill(true);
                allocations.push((allocation, size));
                assert_eq!(allocator.layout_revision(), revision + 1);
            } else {
                assert_eq!(allocator.layout_revision(), revision);
            }
        }
        let revision = allocator.layout_revision();
        for size in [1, 7, 8, 17, 32, 64, 514] {
            assert_eq!(
                allocator.storage_report_for_size(size),
                bitmap_report(&occupied, size),
                "step={step}, size={size}"
            );
        }
        assert_eq!(allocator.layout_revision(), revision);
    }
    for (allocation, _) in allocations {
        allocator.free(allocation);
    }
    assert_eq!(
        allocator.storage_report_for_size(8),
        bitmap_report(&[false; CAPACITY], 8)
    );
}

#[test]
fn reports_support_u16_indices_and_empty_geometry() {
    let mut allocator: Allocator<u16> = Allocator::with_max_allocs(1024, 128);
    let allocation = allocator.allocate(17).unwrap();
    assert_eq!(allocator.storage_report_for_size(8).total_free_space, 1007);
    assert_eq!(allocator.storage_report_for_size(8).allocatable_slots, 125);
    allocator.free(allocation);
    assert_eq!(allocator.storage_report_for_size(8).allocatable_slots, 128);
    let empty: Allocator = Allocator::new(0);
    assert_eq!(
        empty.storage_report_for_size(8),
        ExactStorageReport::default()
    );
}

#[test]
#[should_panic(expected = "exact storage report requires a nonzero size")]
fn zero_accounting_size_is_rejected() {
    let allocator: Allocator = Allocator::new(32);
    allocator.storage_report_for_size(0);
}
