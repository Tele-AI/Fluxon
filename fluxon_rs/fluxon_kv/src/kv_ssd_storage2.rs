use crate::kv_ssd_storage::SSD_ALIGNMENT;
use io_uring::{IoUring, opcode, types::Fd};
use std::fs::{self, OpenOptions};
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::ptr::NonNull;

#[derive(Clone, Copy, Debug)]
enum DirectOpcode {
    BaselineReadv,
    BaselineWritev,
    OptimizedRead,
    OptimizedWrite,
}

#[derive(Clone, Copy, Debug)]
struct DirectIoPerfResult {
    opcode: DirectOpcode,
    ops: usize,
    bytes_per_op: usize,
    elapsed: std::time::Duration,
}

impl DirectIoPerfResult {
    fn ns_per_op(&self) -> f64 {
        self.elapsed.as_nanos() as f64 / self.ops as f64
    }

    fn mib_per_sec(&self) -> f64 {
        let bytes = self.ops as f64 * self.bytes_per_op as f64;
        bytes / self.elapsed.as_secs_f64() / 1024.0 / 1024.0
    }
}

struct DirectAlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
}

impl DirectAlignedBuffer {
    fn zeroed(len: usize) -> io::Result<Self> {
        if len == 0 || !len.is_multiple_of(SSD_ALIGNMENT) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "direct buffer len must be positive and {}-byte aligned: {}",
                    SSD_ALIGNMENT, len
                ),
            ));
        }
        let mut raw = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut raw, SSD_ALIGNMENT, len) };
        if rc != 0 || raw.is_null() {
            return Err(io::Error::other(format!("posix_memalign failed: rc={rc}")));
        }
        unsafe {
            std::ptr::write_bytes(raw as *mut u8, 0, len);
        }
        Ok(Self {
            ptr: NonNull::new(raw as *mut u8).expect("posix_memalign returned non-null"),
            len,
        })
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for DirectAlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr.as_ptr() as *mut libc::c_void);
        }
    }
}

fn baseline_current_sqe(
    opcode: DirectOpcode,
    fd: RawFd,
    iovec: &libc::iovec,
    offset: u64,
) -> io_uring::squeue::Entry {
    match opcode {
        DirectOpcode::BaselineReadv => opcode::Readv::new(Fd(fd), iovec, 1).offset(offset).build(),
        DirectOpcode::BaselineWritev => {
            opcode::Writev::new(Fd(fd), iovec, 1).offset(offset).build()
        }
        DirectOpcode::OptimizedRead | DirectOpcode::OptimizedWrite => {
            unreachable!("optimized opcodes do not use iovec SQEs")
        }
    }
}

fn optimized_single_buffer_sqe(
    opcode: DirectOpcode,
    fd: RawFd,
    buffer: &mut DirectAlignedBuffer,
    offset: u64,
) -> io_uring::squeue::Entry {
    match opcode {
        DirectOpcode::OptimizedRead => {
            opcode::Read::new(Fd(fd), buffer.as_mut_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
        }
        DirectOpcode::OptimizedWrite => {
            opcode::Write::new(Fd(fd), buffer.as_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
        }
        DirectOpcode::BaselineReadv | DirectOpcode::BaselineWritev => {
            unreachable!("baseline opcodes use iovec SQEs")
        }
    }
}

fn run_direct_io_perf(
    opcode: DirectOpcode,
    fd: RawFd,
    buffer: &mut DirectAlignedBuffer,
    ops: usize,
    bytes_per_op: usize,
    offset_slots: usize,
) -> io::Result<DirectIoPerfResult> {
    assert!(offset_slots > 0);
    assert_eq!(buffer.len(), bytes_per_op);
    let mut ring = IoUring::builder().build(64)?;
    let start = std::time::Instant::now();
    for idx in 0..ops {
        let offset = u64::try_from((idx % offset_slots) * bytes_per_op).unwrap();
        let iovec = libc::iovec {
            iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
            iov_len: bytes_per_op,
        };
        let sqe = match opcode {
            DirectOpcode::BaselineReadv | DirectOpcode::BaselineWritev => {
                baseline_current_sqe(opcode, fd, &iovec, offset)
            }
            DirectOpcode::OptimizedRead | DirectOpcode::OptimizedWrite => {
                optimized_single_buffer_sqe(opcode, fd, buffer, offset)
            }
        }
        .user_data((idx + 1) as u64);
        unsafe {
            ring.submission()
                .push(&sqe)
                .map_err(|_| io::Error::other("submission queue full"))?;
        }
        ring.submit_and_wait(1)?;
        let mut cq = ring.completion();
        let cqe: io_uring::cqueue::Entry = cq
            .next()
            .ok_or_else(|| io::Error::other("missing completion"))?;
        if cqe.result() != bytes_per_op as i32 {
            return Err(io::Error::other(format!(
                "short direct I/O completion: {} != {}",
                cqe.result(),
                bytes_per_op
            )));
        }
    }
    Ok(DirectIoPerfResult {
        opcode,
        ops,
        bytes_per_op,
        elapsed: start.elapsed(),
    })
}

fn best_result(results: &[DirectIoPerfResult]) -> DirectIoPerfResult {
    results
        .iter()
        .min_by_key(|result| result.elapsed)
        .copied()
        .unwrap()
}

fn median_result(results: &[DirectIoPerfResult]) -> DirectIoPerfResult {
    let mut sorted = results.to_vec();
    sorted.sort_by_key(|result| result.elapsed);
    sorted[sorted.len() / 2]
}

fn print_pair(
    label: &str,
    baseline_results: &[DirectIoPerfResult],
    optimized_results: &[DirectIoPerfResult],
) {
    assert_eq!(baseline_results.len(), optimized_results.len());
    let rounds = baseline_results.len();
    for (stat, baseline, optimized) in [
        (
            "best",
            best_result(baseline_results),
            best_result(optimized_results),
        ),
        (
            "median",
            median_result(baseline_results),
            median_result(optimized_results),
        ),
    ] {
        println!(
            "kv_ssd_storage2 {stat}-of-{rounds} {label} baseline: {:?}: ops={} bytes/op={} elapsed={:?} ns/op={:.1} MiB/s={:.1}",
            baseline.opcode,
            baseline.ops,
            baseline.bytes_per_op,
            baseline.elapsed,
            baseline.ns_per_op(),
            baseline.mib_per_sec()
        );
        println!(
            "kv_ssd_storage2 {stat}-of-{rounds} {label} optimized: {:?}: ops={} bytes/op={} elapsed={:?} ns/op={:.1} MiB/s={:.1}",
            optimized.opcode,
            optimized.ops,
            optimized.bytes_per_op,
            optimized.elapsed,
            optimized.ns_per_op(),
            optimized.mib_per_sec()
        );
        println!(
            "kv_ssd_storage2 delta {stat}-of-{rounds} {label}: optimized relative to baseline = {:.2}%",
            (baseline.ns_per_op() - optimized.ns_per_op()) / baseline.ns_per_op() * 100.0
        );
    }
}

fn seed_direct_file(fd: RawFd, bytes_per_op: usize, offset_slots: usize) -> io::Result<()> {
    let mut seed = DirectAlignedBuffer::zeroed(bytes_per_op)?;
    unsafe {
        std::ptr::write_bytes(seed.as_mut_ptr(), 0x5a, seed.len());
    }
    for slot in 0..offset_slots {
        let offset = u64::try_from(slot * bytes_per_op).unwrap();
        let written = unsafe {
            libc::pwrite(
                fd,
                seed.as_ptr() as *const _,
                bytes_per_op,
                offset as libc::off_t,
            )
        };
        if written != bytes_per_op as isize {
            return Err(io::Error::other(format!(
                "short direct seed write: {written} != {bytes_per_op}"
            )));
        }
    }
    Ok(())
}

#[test]
#[ignore = "manual perf comparison for kv_ssd_storage2 single-buffer direct I/O fast path"]
fn perf_compare_current_iovec_with_storage2_single_buffer_fast_path() {
    let dir = std::env::temp_dir().join(format!("fluxon-kv-ssd-storage2-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    for (bytes_per_op, ops, offset_slots, rounds) in [
        (4096usize, 256usize, 128usize, 3usize),
        (1024 * 1024usize, 12usize, 12usize, 3usize),
        (10 * 1024 * 1024usize, 3usize, 3usize, 3usize),
    ] {
        assert!(bytes_per_op.is_multiple_of(SSD_ALIGNMENT));
        let file_len = bytes_per_op.checked_mul(offset_slots).unwrap();
        let path = dir.join(format!("direct-{bytes_per_op}.dat"));
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)
            .unwrap();
        file.set_len(file_len as u64).unwrap();
        seed_direct_file(file.as_raw_fd(), bytes_per_op, offset_slots).unwrap();

        let mut baseline_read = DirectAlignedBuffer::zeroed(bytes_per_op).unwrap();
        let mut optimized_read = DirectAlignedBuffer::zeroed(bytes_per_op).unwrap();
        let mut baseline_write = DirectAlignedBuffer::zeroed(bytes_per_op).unwrap();
        let mut optimized_write = DirectAlignedBuffer::zeroed(bytes_per_op).unwrap();
        unsafe {
            std::ptr::write_bytes(baseline_write.as_mut_ptr(), 0xa5, baseline_write.len());
            std::ptr::write_bytes(optimized_write.as_mut_ptr(), 0x3c, optimized_write.len());
        }

        let warmup_ops = ops.min(16);
        let _ = run_direct_io_perf(
            DirectOpcode::BaselineReadv,
            file.as_raw_fd(),
            &mut baseline_read,
            warmup_ops,
            bytes_per_op,
            offset_slots,
        )
        .unwrap();
        let _ = run_direct_io_perf(
            DirectOpcode::OptimizedRead,
            file.as_raw_fd(),
            &mut optimized_read,
            warmup_ops,
            bytes_per_op,
            offset_slots,
        )
        .unwrap();
        let _ = run_direct_io_perf(
            DirectOpcode::BaselineWritev,
            file.as_raw_fd(),
            &mut baseline_write,
            warmup_ops,
            bytes_per_op,
            offset_slots,
        )
        .unwrap();
        let _ = run_direct_io_perf(
            DirectOpcode::OptimizedWrite,
            file.as_raw_fd(),
            &mut optimized_write,
            warmup_ops,
            bytes_per_op,
            offset_slots,
        )
        .unwrap();

        let mut baseline_read_results = Vec::with_capacity(rounds);
        let mut optimized_read_results = Vec::with_capacity(rounds);
        let mut baseline_write_results = Vec::with_capacity(rounds);
        let mut optimized_write_results = Vec::with_capacity(rounds);
        for round_idx in 0..rounds {
            if round_idx % 2 == 0 {
                baseline_read_results.push(
                    run_direct_io_perf(
                        DirectOpcode::BaselineReadv,
                        file.as_raw_fd(),
                        &mut baseline_read,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
                optimized_read_results.push(
                    run_direct_io_perf(
                        DirectOpcode::OptimizedRead,
                        file.as_raw_fd(),
                        &mut optimized_read,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
            } else {
                optimized_read_results.push(
                    run_direct_io_perf(
                        DirectOpcode::OptimizedRead,
                        file.as_raw_fd(),
                        &mut optimized_read,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
                baseline_read_results.push(
                    run_direct_io_perf(
                        DirectOpcode::BaselineReadv,
                        file.as_raw_fd(),
                        &mut baseline_read,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
            }

            if round_idx % 2 == 0 {
                baseline_write_results.push(
                    run_direct_io_perf(
                        DirectOpcode::BaselineWritev,
                        file.as_raw_fd(),
                        &mut baseline_write,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
                optimized_write_results.push(
                    run_direct_io_perf(
                        DirectOpcode::OptimizedWrite,
                        file.as_raw_fd(),
                        &mut optimized_write,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
            } else {
                optimized_write_results.push(
                    run_direct_io_perf(
                        DirectOpcode::OptimizedWrite,
                        file.as_raw_fd(),
                        &mut optimized_write,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
                baseline_write_results.push(
                    run_direct_io_perf(
                        DirectOpcode::BaselineWritev,
                        file.as_raw_fd(),
                        &mut baseline_write,
                        ops,
                        bytes_per_op,
                        offset_slots,
                    )
                    .unwrap(),
                );
            }
        }

        print_pair("read", &baseline_read_results, &optimized_read_results);
        print_pair("write", &baseline_write_results, &optimized_write_results);

        fs::remove_file(&path).ok();
    }
    fs::remove_dir(&dir).ok();
}
