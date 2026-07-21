#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use std::collections::{BTreeMap, VecDeque};
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use std::ffi::{c_char, c_int, c_void};
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use std::panic::{self, AssertUnwindSafe};
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use std::ptr;
use std::sync::Arc;
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use fluxon_fs::config::FluxonFsRequestIdentity;
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
use pyo3::types::{PyAny, PyBytes, PyDict, PyTuple};

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
const VIDEO_PAGE_CACHE_MAX_PAGES: usize = 64;
#[cfg(feature = "fluxon_fs_video_ffmpeg")]
const VIDEO_ERR_BUF_BYTES: usize = 4096;

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
#[repr(C)]
struct FluxonVideoIo {
    user_data: *mut c_void,
    read_at: Option<
        unsafe extern "C" fn(
            user_data: *mut c_void,
            offset: i64,
            buf: *mut u8,
            buf_size: c_int,
            out_len: *mut c_int,
            err_buf: *mut c_char,
            err_buf_len: usize,
        ) -> c_int,
    >,
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
unsafe extern "C" {
    fn fluxon_fs_video_decode_frames(
        io: *const FluxonVideoIo,
        file_size: i64,
        indices: *const i64,
        indices_len: c_int,
        out_width: c_int,
        out_height: c_int,
        num_threads: c_int,
        out_data: *mut u8,
        out_data_len: i64,
        err_buf: *mut c_char,
        err_buf_len: usize,
    ) -> c_int;
}

pub fn open_video_reader_from_agent(
    py: Python<'_>,
    agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
    export_name: String,
    relpath: String,
    height: i64,
    width: i64,
    num_threads: i64,
    request_identity: Option<(String, String)>,
) -> PyResult<PyObject> {
    #[cfg(not(feature = "fluxon_fs_video_ffmpeg"))]
    {
        let _ = (
            py,
            agent,
            export_name,
            relpath,
            height,
            width,
            num_threads,
            request_identity,
        );
        Err(PyRuntimeError::new_err(
            "FluxonFS VideoReader requires building fluxon_pyo3 with --features fluxon_fs_video_ffmpeg",
        ))
    }

    #[cfg(feature = "fluxon_fs_video_ffmpeg")]
    {
        let request_identity = crate::py_request_identity_tuple_to_core(request_identity)?;
        let reader = FluxonFsVideoReader::open(
            agent,
            export_name,
            relpath,
            height,
            width,
            num_threads,
            request_identity,
            py,
        )?;
        Ok(Py::new(py, reader)?.into_py(py))
    }
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
#[pyclass]
pub struct FluxonFsVideoReader {
    agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
    export_name: String,
    relpath: String,
    path_for_err: String,
    size: i64,
    mtime_ns: i64,
    height: i64,
    width: i64,
    num_threads: i64,
    request_identity: Option<FluxonFsRequestIdentity>,
    page_bytes: i64,
    page_cache: Mutex<VideoPageCache>,
    decode_calls: AtomicU64,
    decode_frames_requested: AtomicU64,
    decode_errors: AtomicU64,
    read_at_calls: AtomicU64,
    read_at_requested_bytes: AtomicU64,
    read_at_returned_bytes: AtomicU64,
    page_cache_hits: AtomicU64,
    page_cache_misses: AtomicU64,
    remote_read_calls: AtomicU64,
    remote_read_bytes: AtomicU64,
    closed: AtomicBool,
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
struct VideoPageCache {
    pages: BTreeMap<i64, Vec<u8>>,
    lru: VecDeque<i64>,
    max_pages: usize,
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
impl VideoPageCache {
    fn new(max_pages: usize) -> Self {
        Self {
            pages: BTreeMap::new(),
            lru: VecDeque::new(),
            max_pages: max_pages.max(1),
        }
    }

    fn get(&mut self, page_idx: i64) -> Option<Vec<u8>> {
        let data = self.pages.get(&page_idx)?.clone();
        self.touch(page_idx);
        Some(data)
    }

    fn insert(&mut self, page_idx: i64, data: Vec<u8>) {
        self.pages.insert(page_idx, data);
        self.touch(page_idx);
        while self.pages.len() > self.max_pages {
            let Some(evict_idx) = self.lru.pop_front() else {
                break;
            };
            if self.pages.remove(&evict_idx).is_some() {
                continue;
            }
        }
    }

    fn touch(&mut self, page_idx: i64) {
        self.lru.retain(|idx| *idx != page_idx);
        self.lru.push_back(page_idx);
    }
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
impl FluxonFsVideoReader {
    fn open(
        agent: Arc<fluxon_fs::agent::FluxonFsAgent>,
        export_name: String,
        relpath: String,
        height: i64,
        width: i64,
        num_threads: i64,
        request_identity: Option<FluxonFsRequestIdentity>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        validate_non_empty(&export_name, "export_name")?;
        validate_non_empty(&relpath, "relpath")?;
        validate_positive_c_int(height, "height")?;
        validate_positive_c_int(width, "width")?;
        validate_positive_c_int(num_threads, "num_threads")?;

        let path_for_err = format!("fluxonfs://{}/{}", export_name, relpath);
        let stat = py
            .allow_threads(|| {
                agent.remote_stat_by_handle_with_identity(
                    &export_name,
                    &relpath,
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .map_err(crate::pyerr_from_fs_agent_error)?;

        if !stat.exists {
            return Err(PyRuntimeError::new_err(format!(
                "FluxonFS video not found: {}",
                path_for_err
            )));
        }
        if !stat.is_file || stat.is_dir {
            return Err(PyRuntimeError::new_err(format!(
                "FluxonFS video path is not a file: {}",
                path_for_err
            )));
        }
        if stat.size < 0 || stat.mtime_ns < 0 {
            return Err(PyRuntimeError::new_err(format!(
                "FluxonFS video stat returned invalid size/mtime: {} size={} mtime_ns={}",
                path_for_err, stat.size, stat.mtime_ns
            )));
        }

        Ok(Self {
            agent,
            export_name,
            relpath,
            path_for_err,
            size: stat.size,
            mtime_ns: stat.mtime_ns,
            height,
            width,
            num_threads,
            request_identity,
            page_bytes: fluxon_fs::agent::REMOTE_CHUNK_BYTES as i64,
            page_cache: Mutex::new(VideoPageCache::new(VIDEO_PAGE_CACHE_MAX_PAGES)),
            decode_calls: AtomicU64::new(0),
            decode_frames_requested: AtomicU64::new(0),
            decode_errors: AtomicU64::new(0),
            read_at_calls: AtomicU64::new(0),
            read_at_requested_bytes: AtomicU64::new(0),
            read_at_returned_bytes: AtomicU64::new(0),
            page_cache_hits: AtomicU64::new(0),
            page_cache_misses: AtomicU64::new(0),
            remote_read_calls: AtomicU64::new(0),
            remote_read_bytes: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        })
    }

    fn read_at_bytes(&self, offset: i64, n: usize) -> Result<Vec<u8>, String> {
        self.read_at_calls.fetch_add(1, Ordering::Relaxed);
        self.read_at_requested_bytes
            .fetch_add(n as u64, Ordering::Relaxed);
        if self.closed.load(Ordering::SeqCst) {
            return Err("FluxonFS VideoReader is closed".to_string());
        }
        if offset < 0 {
            return Err(format!("read offset must be non-negative: {}", offset));
        }
        if n == 0 || offset >= self.size {
            return Ok(Vec::new());
        }
        let available = self.size.saturating_sub(offset) as usize;
        let want = n.min(available);
        let mut out = Vec::with_capacity(want);
        let mut pos = offset;
        while out.len() < want {
            let page_idx = pos / self.page_bytes;
            let page_start = page_idx * self.page_bytes;
            let page_off = (pos - page_start) as usize;
            let page = self.read_page(page_idx, page_start)?;
            if page_off >= page.len() {
                break;
            }
            let take = (want - out.len()).min(page.len() - page_off);
            out.extend_from_slice(&page[page_off..page_off + take]);
            pos = pos.saturating_add(take as i64);
        }
        self.read_at_returned_bytes
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        Ok(out)
    }

    fn read_page(&self, page_idx: i64, page_start: i64) -> Result<Vec<u8>, String> {
        {
            let mut cache = self.page_cache.lock();
            if let Some(page) = cache.get(page_idx) {
                self.page_cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(page);
            }
        }
        self.page_cache_misses.fetch_add(1, Ordering::Relaxed);

        let n = self.page_bytes.min(self.size.saturating_sub(page_start));
        if n <= 0 {
            return Ok(Vec::new());
        }
        self.remote_read_calls.fetch_add(1, Ordering::Relaxed);
        let page = self
            .agent
            .remote_read_chunk_by_handle_with_identity(
                &self.export_name,
                &self.relpath,
                page_start,
                n,
                self.size,
                self.mtime_ns,
                true,
                &self.path_for_err,
                self.request_identity.as_ref(),
            )
            .map_err(|err| err.to_string())?;
        self.remote_read_bytes
            .fetch_add(page.len() as u64, Ordering::Relaxed);

        let mut cache = self.page_cache.lock();
        cache.insert(page_idx, page.clone());
        Ok(page)
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.closed.load(Ordering::SeqCst) {
            Err(PyRuntimeError::new_err("FluxonFS VideoReader is closed"))
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
#[pymethods]
impl FluxonFsVideoReader {
    fn read_frames_numpy(&self, indices: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<PyObject> {
        self.ensure_open()?;
        let frame_indices: Vec<i64> = indices.extract()?;
        for idx in &frame_indices {
            if *idx < 0 {
                return Err(PyValueError::new_err(
                    "FluxonFS VideoReader frame indices must be non-negative",
                ));
            }
        }

        let frame_count = frame_indices.len();
        if frame_count > c_int::MAX as usize {
            return Err(PyValueError::new_err(
                "FluxonFS VideoReader frame count is too large",
            ));
        }
        self.decode_calls.fetch_add(1, Ordering::Relaxed);
        self.decode_frames_requested
            .fetch_add(frame_count as u64, Ordering::Relaxed);
        let frame_bytes = checked_frame_bytes(self.height, self.width)?;
        let out_len = frame_bytes
            .checked_mul(frame_count)
            .ok_or_else(|| PyValueError::new_err("FluxonFS VideoReader output is too large"))?;
        let mut out = vec![0u8; out_len];

        if frame_count > 0 {
            let mut err_buf = vec![0i8; VIDEO_ERR_BUF_BYTES];
            let decode_res = py.allow_threads(|| {
                let mut state = VideoReadCallbackState { reader: self };
                let io = FluxonVideoIo {
                    user_data: (&mut state as *mut VideoReadCallbackState<'_>).cast::<c_void>(),
                    read_at: Some(fluxon_video_read_at),
                };
                unsafe {
                    fluxon_fs_video_decode_frames(
                        &io as *const FluxonVideoIo,
                        self.size,
                        frame_indices.as_ptr(),
                        frame_count as c_int,
                        self.width as c_int,
                        self.height as c_int,
                        self.num_threads as c_int,
                        out.as_mut_ptr(),
                        out.len() as i64,
                        err_buf.as_mut_ptr(),
                        err_buf.len(),
                    )
                }
            });
            if decode_res != 0 {
                self.decode_errors.fetch_add(1, Ordering::Relaxed);
                let msg = c_err_buf_to_string(&err_buf);
                return Err(PyRuntimeError::new_err(if msg.is_empty() {
                    "FluxonFS VideoReader decode failed".to_string()
                } else {
                    format!("FluxonFS VideoReader decode failed: {}", msg)
                }));
            }
        }

        numpy_array_from_vec(py, out, frame_count, self.height, self.width)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<PyObject> {
        let cache_pages = {
            let cache = self.page_cache.lock();
            cache.pages.len() as u64
        };
        let stats = PyDict::new_bound(py);
        stats.set_item("file_size", self.size)?;
        stats.set_item("mtime_ns", self.mtime_ns)?;
        stats.set_item("height", self.height)?;
        stats.set_item("width", self.width)?;
        stats.set_item("num_threads", self.num_threads)?;
        stats.set_item("page_bytes", self.page_bytes)?;
        stats.set_item("page_cache_pages", cache_pages)?;
        stats.set_item("page_cache_max_pages", VIDEO_PAGE_CACHE_MAX_PAGES as u64)?;
        stats.set_item("decode_calls", self.decode_calls.load(Ordering::Relaxed))?;
        stats.set_item(
            "decode_frames_requested",
            self.decode_frames_requested.load(Ordering::Relaxed),
        )?;
        stats.set_item("decode_errors", self.decode_errors.load(Ordering::Relaxed))?;
        stats.set_item("read_at_calls", self.read_at_calls.load(Ordering::Relaxed))?;
        stats.set_item(
            "read_at_requested_bytes",
            self.read_at_requested_bytes.load(Ordering::Relaxed),
        )?;
        stats.set_item(
            "read_at_returned_bytes",
            self.read_at_returned_bytes.load(Ordering::Relaxed),
        )?;
        stats.set_item(
            "page_cache_hits",
            self.page_cache_hits.load(Ordering::Relaxed),
        )?;
        stats.set_item(
            "page_cache_misses",
            self.page_cache_misses.load(Ordering::Relaxed),
        )?;
        stats.set_item(
            "remote_read_calls",
            self.remote_read_calls.load(Ordering::Relaxed),
        )?;
        stats.set_item(
            "remote_read_bytes",
            self.remote_read_bytes.load(Ordering::Relaxed),
        )?;
        Ok(stats.into_py(py))
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc=None, _tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        self.close();
        false
    }
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
struct VideoReadCallbackState<'a> {
    reader: &'a FluxonFsVideoReader,
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
unsafe extern "C" fn fluxon_video_read_at(
    user_data: *mut c_void,
    offset: i64,
    buf: *mut u8,
    buf_size: c_int,
    out_len: *mut c_int,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> c_int {
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        if user_data.is_null() || buf.is_null() || out_len.is_null() || buf_size < 0 {
            return Err("invalid FluxonFS video read callback argument".to_string());
        }
        let state = unsafe { &mut *(user_data as *mut VideoReadCallbackState<'_>) };
        let data = state.reader.read_at_bytes(offset, buf_size as usize)?;
        let data_len = data.len();
        if data_len > 0 {
            unsafe {
                ptr::copy_nonoverlapping(data.as_ptr(), buf, data_len);
            }
        }
        unsafe {
            *out_len = data_len as c_int;
        }
        Ok(())
    }));

    match result {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => {
            write_c_err(err_buf, err_buf_len, &err);
            -1
        }
        Err(_) => {
            write_c_err(
                err_buf,
                err_buf_len,
                "FluxonFS video read callback panicked",
            );
            -1
        }
    }
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
fn validate_non_empty(value: &str, name: &str) -> PyResult<()> {
    if value.trim().is_empty() {
        Err(PyValueError::new_err(format!("{} must be non-empty", name)))
    } else {
        Ok(())
    }
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
fn validate_positive_c_int(value: i64, name: &str) -> PyResult<()> {
    if value <= 0 {
        return Err(PyValueError::new_err(format!("{} must be positive", name)));
    }
    if value > c_int::MAX as i64 {
        return Err(PyValueError::new_err(format!(
            "{} must be <= {}",
            name,
            c_int::MAX
        )));
    }
    Ok(())
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
fn checked_frame_bytes(height: i64, width: i64) -> PyResult<usize> {
    let bytes = height
        .checked_mul(width)
        .and_then(|v| v.checked_mul(3))
        .ok_or_else(|| PyValueError::new_err("FluxonFS VideoReader frame shape is too large"))?;
    usize::try_from(bytes)
        .map_err(|_| PyValueError::new_err("FluxonFS VideoReader frame shape is too large"))
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
fn numpy_array_from_vec(
    py: Python<'_>,
    data: Vec<u8>,
    frame_count: usize,
    height: i64,
    width: i64,
) -> PyResult<PyObject> {
    let np = py.import_bound("numpy")?;
    let bytes = PyBytes::new_bound(py, &data);
    let arr = np.call_method1("frombuffer", (bytes, "uint8"))?;
    let shape = PyTuple::new_bound(
        py,
        [
            (frame_count as i64).into_py(py),
            height.into_py(py),
            width.into_py(py),
            3i64.into_py(py),
        ],
    );
    let reshaped = arr.call_method1("reshape", (shape,))?;
    let owned = reshaped.call_method0("copy")?;
    Ok(owned.into_py(py))
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
fn c_err_buf_to_string(buf: &[i8]) -> String {
    let bytes: Vec<u8> = buf
        .iter()
        .copied()
        .take_while(|b| *b != 0)
        .map(|b| b as u8)
        .collect();
    String::from_utf8_lossy(&bytes).trim().to_string()
}

#[cfg(feature = "fluxon_fs_video_ffmpeg")]
fn write_c_err(err_buf: *mut c_char, err_buf_len: usize, msg: &str) {
    if err_buf.is_null() || err_buf_len == 0 {
        return;
    }
    let bytes = msg.as_bytes();
    let take = bytes.len().min(err_buf_len.saturating_sub(1));
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), err_buf, take);
        *err_buf.add(take) = 0;
    }
}
