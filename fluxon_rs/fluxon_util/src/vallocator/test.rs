use crate::vallocator::VirtualAllocator;

#[test]
fn virtual_allocator_keeps_sglang_pages_exact_instead_of_power_of_two_rounding() {
    const PAGE_BYTES: u64 = 4_718_592;
    let allocator = VirtualAllocator::new(PAGE_BYTES * 3).unwrap();
    let first = allocator.alloc(PAGE_BYTES).unwrap();
    let second = allocator.alloc(PAGE_BYTES).unwrap();
    let third = allocator.alloc(PAGE_BYTES).unwrap();

    assert_eq!(first.size, PAGE_BYTES);
    assert_eq!(second.size, PAGE_BYTES);
    assert_eq!(third.size, PAGE_BYTES);
    assert_eq!(allocator.get_allocated_size(), PAGE_BYTES * 3);
    assert!(allocator.alloc(PAGE_BYTES).is_err());

    allocator.free(second.start_addr, second.size).unwrap();
    allocator.free(first.start_addr, first.size).unwrap();
    allocator.free(third.start_addr, third.size).unwrap();
    assert_eq!(allocator.get_allocated_size(), 0);

    let whole = allocator.alloc(PAGE_BYTES * 3).unwrap();
    assert_eq!(whole.start_addr, 0);
    assert_eq!(whole.size, PAGE_BYTES * 3);
}

#[test]
fn virtual_allocator_rounds_only_to_transfer_page_alignment() {
    let allocator = VirtualAllocator::new(16 * 1024).unwrap();
    let region = allocator.alloc(4097).unwrap();
    assert_eq!(region.start_addr, 0);
    assert_eq!(region.size, 8192);
    assert_eq!(allocator.get_allocated_size(), 8192);
    allocator.free(region.start_addr, region.size).unwrap();
    assert_eq!(allocator.get_allocated_size(), 0);
}

#[test]
fn test_empty_frame_allocator() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();
    assert!(frame.alloc(1).0.is_none());
}

#[test]
fn test_frame_allocator_add() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();
    assert!(frame.alloc(1).0.is_none());

    frame.insert(0..3);
    let num = frame.alloc(1);
    assert_eq!(num.0, Some(2));
    let num = frame.alloc(2);
    assert_eq!(num.0, Some(0));
    assert!(frame.alloc(1).0.is_none());
    assert!(frame.alloc(2).0.is_none());
}

#[test]
fn test_frame_allocator_allocate_large() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();
    assert_eq!(frame.alloc(10_000_000_000).0, None);
}

#[test]
fn test_frame_allocator_add_large_size_split() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();

    frame.insert(0..10_000_000_000);

    assert_eq!(frame.alloc(0x8000_0001).0, None);
    assert_eq!(frame.alloc(0x8000_0000).0, Some(0));
    assert_eq!(frame.alloc(0x8000_0000).0, Some(0x8000_0000));
}

#[test]
fn test_frame_allocator_add_large_size() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<33>::new();

    frame.insert(0..10_000_000_000);
    assert_eq!(frame.alloc(0x8000_0001).1, 0x1_0000_0000_u64);
}

#[test]
fn test_frame_allocator_alloc_and_free() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();
    assert!(frame.alloc(1).0.is_none());

    frame.add_frame(0, 1024);
    for _ in 0..100 {
        let addr = frame.alloc(512).0.unwrap();
        frame.dealloc(addr, 512);
    }
}

#[test]
fn test_frame_allocator_alloc_and_free_complex() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();
    frame.add_frame(100, 1024);
    for _ in 0..10 {
        let addr = frame.alloc(1).0.unwrap();
        frame.dealloc(addr, 1);
    }
    let addr1 = frame.alloc(1).0.unwrap();
    let addr2 = frame.alloc(1).0.unwrap();
    assert_ne!(addr1, addr2);
}

#[test]
fn test_frame_allocator_aligned() {
    let mut frame = crate::vallocator::frame::FrameAllocator::<32>::new();
    frame.add_frame(1, 64);
    assert_eq!(
        frame
            .alloc_aligned(std::alloc::Layout::from_size_align(2, 4).unwrap())
            .0,
        Some(4)
    );
    assert_eq!(
        frame
            .alloc_aligned(std::alloc::Layout::from_size_align(2, 2).unwrap())
            .0,
        Some(2)
    );
    assert_eq!(
        frame
            .alloc_aligned(std::alloc::Layout::from_size_align(2, 1).unwrap())
            .0,
        Some(8)
    );
    assert_eq!(
        frame
            .alloc_aligned(std::alloc::Layout::from_size_align(1, 16).unwrap())
            .0,
        Some(16)
    );
}

#[test]
fn test_basic_allocation() {
    test_once();
    test_once();
    test_once();
}

pub fn test_once() {
    println!("=== Testing Basic Allocation ===");

    let total_size = 32 << 30;
    let allocator = VirtualAllocator::new(total_size).expect("Should be able to create allocator");

    assert_eq!(allocator.get_total_size(), total_size);
    assert_eq!(allocator.get_free_size(), total_size);
    assert_eq!(allocator.get_allocated_size(), 0);

    let mut sizes = vec![1, 4, 16, 64, 256, 1024, 4096, 16_384];
    sizes.reverse();
    let mut allocations = Vec::with_capacity(sizes.len());
    let mut allocated_size = 0;
    let mut free_size = total_size;

    println!("\n=== Starting allocations ===");
    for &s in &sizes {
        let size = s << 20;
        println!("Allocating {} MB", s);
        let region = allocator
            .alloc(size)
            .unwrap_or_else(|_| panic!("Should be able to allocate {size} bytes"));
        let ptr = region.start_addr;
        let actual_size = region.size;

        println!(
            "Allocated {} MB at address {:x} (with required size: {} MB)",
            actual_size >> 20,
            ptr,
            size >> 20
        );
        free_size -= actual_size;
        allocated_size += actual_size;
        assert_eq!(allocator.get_allocated_size(), allocated_size);
        assert_eq!(allocator.get_free_size(), free_size);
        assert!(actual_size >= size);
        assert!(actual_size.is_power_of_two());
        assert!(actual_size + ptr <= total_size);

        allocations.push((ptr, size));
    }

    let total_allocated_byte: u64 = sizes.iter().sum();
    let total_allocated = total_allocated_byte << 20;
    assert_eq!(allocator.get_allocated_size(), total_allocated);
    assert_eq!(allocator.get_free_size(), total_size - total_allocated);

    println!(
        "\nTotal allocated after all allocations: {} bytes ({}MB)",
        total_allocated,
        total_allocated / (1024 * 1024)
    );
    println!(
        "Free after all allocations: {} bytes ({}MB)",
        total_size - total_allocated,
        (total_size - total_allocated) / (1024 * 1024)
    );

    println!("\n=== Starting deallocations ===");
    let mut remaining_allocated = total_allocated;
    for &(ptr, size) in &allocations {
        allocator
            .free(ptr, size)
            .unwrap_or_else(|_| panic!("Should be able to free {} MB at {:x}", size >> 20, ptr));

        remaining_allocated -= size;
        println!(
            "Freed {} MB - Remaining allocated: {} MB",
            size >> 20,
            remaining_allocated >> 20
        );

        assert_eq!(allocator.get_allocated_size(), remaining_allocated);
        assert_eq!(allocator.get_free_size(), total_size - remaining_allocated);
    }

    assert_eq!(allocator.get_allocated_size(), 0);
    assert_eq!(allocator.get_free_size(), total_size);

    println!("\nBasic allocation tests passed - All memory freed");
}
