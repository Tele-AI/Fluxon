from __future__ import annotations

import builtins
import errno
import io
import importlib
import linecache
import tokenize
import types
import genericpath
import os
import pathlib
import stat as _stat
import sys
import threading
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Callable, Dict, Optional, Tuple


def _normalize_abs(path: Any) -> str:
    if isinstance(path, pathlib.Path):
        path = str(path)
    if not isinstance(path, str):
        raise TypeError(f"path must be str or Path, got {type(path)}")
    return os.path.abspath(path)


def _is_write_mode(mode: str) -> bool:
    return any(m in str(mode) for m in ("w", "a", "x", "+"))


# Rust open_plan returns a small tagged tuple for performance.
_OPEN_PLAN_KIND_BYPASS = 0
_OPEN_PLAN_KIND_BYTES = 1
_OPEN_PLAN_KIND_REMOTE_HANDLE = 2
_OPEN_PLAN_KIND_FD = 3

# os.open() is an "open-only" primitive. We must not trigger any content IO at
# os.open time. We only consult agent.open_plan for write-intent paths to support write-through
# tracking (close-time push to KV for local files).
_OS_OPEN_WRITE_INTENT_FLAGS = (
    os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC
)

_REMOTE_READ_CHUNK_BYTES = 8 * 1024 * 1024
_REMOTE_WRITE_SESSION_THRESHOLD_BYTES = 4 * 1024 * 1024
_REMOTE_WRITE_SESSION_SUBMIT_MULTIPLIER = 4
_REMOTE_DEFAULT_WRITE_BUFFER_BYTES_MAX = 32 * 1024 * 1024
_REMOTE_WRITE_SESSION_TARGET_INFLIGHT_BYTES_DEFAULT = 128 * 1024 * 1024


class _FluxonFileProxy:
    def __init__(
        self,
        inner: Any,
        *,
        file_abs: str,
        mode: str,
        on_close: Callable[[str, str], None],
    ) -> None:
        self._inner = inner
        self._file_abs = file_abs
        self._mode = mode
        self._on_close = on_close
        self._synced = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def __enter__(self) -> "_FluxonFileProxy":
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Write-through is bound to close/exit semantics. Close the underlying file first so
        # buffered writes are flushed to disk before we read and push content to KV.
        ret = self._inner.__exit__(exc_type, exc, tb)
        if not self._synced:
            self._on_close(self._file_abs, self._mode)
            self._synced = True
        return ret

    def close(self) -> None:
        already = bool(getattr(self._inner, "closed", False))
        if already:
            return
        self._inner.close()
        if not self._synced:
            self._on_close(self._file_abs, self._mode)
            self._synced = True

    def __iter__(self):
        return iter(self._inner)


class _FluxonRemoteFileRaw(io.RawIOBase):
    def __init__(
        self,
        agent: Any,
        *,
        export_name: str,
        relpath: str,
        mode: str,
        file_abs: str,
        initial_size: int,
        initial_mtime_ns: int,
        chunk_bytes: int,
        request_identity: Optional[Tuple[str, str]] = None,
    ) -> None:
        # English note: this object intentionally does NOT expose a real OS fd.
        super().__init__()
        self._agent = agent
        self._export_name = export_name
        self._relpath = relpath
        self._mode = mode
        self._file_abs = file_abs
        self._identity = tuple(request_identity) if request_identity is not None else None

        self._readable = ("r" in mode) or ("+" in mode)
        self._writable = any(m in mode for m in ("w", "a", "x", "+"))
        self._append = "a" in mode

        cb = int(chunk_bytes)
        if cb <= 0:
            raise ValueError("chunk_bytes must be positive")
        # Large initial streaming uses disk-cache path with KV cache disabled.
        self._chunk_bytes = cb
        self._write_chunk_bytes = cb
        self._write_session_frame_bytes = cb
        self._write_submit_bytes = cb
        self._write_session_max_inflight_chunks = 1
        if self._writable:
            self._write_session_frame_bytes = max(
                1,
                int(self._agent.remote_write_session_chunk_bytes()),
            )
            self._write_chunk_bytes = max(
                cb,
                int(self._write_session_frame_bytes),
            )
            write_session_target_inflight_bytes = max(
                int(self._write_session_frame_bytes),
                int(self._agent.remote_write_session_target_inflight_bytes()),
            )
            self._write_submit_bytes = max(
                self._write_chunk_bytes,
                int(self._write_chunk_bytes) * int(_REMOTE_WRITE_SESSION_SUBMIT_MULTIPLIER),
            )
            self._write_session_max_inflight_chunks = max(
                1,
                (write_session_target_inflight_bytes + self._write_session_frame_bytes - 1)
                // self._write_session_frame_bytes,
            )

        init_size = int(initial_size)
        self._size = init_size
        self._mtime_ns = int(initial_mtime_ns)
        self._pos = init_size if self._append else 0
        self._append_pos: Optional[int] = init_size if self._append else None
        self._allow_kv_cache = bool(self._readable) and (not bool(self._writable))
        self._write_session_id: Optional[str] = None
        self._write_session_eligible = bool(self._writable) and (not bool(self._readable))
        self._write_session_threshold_bytes = max(
            int(self._chunk_bytes),
            int(_REMOTE_WRITE_SESSION_THRESHOLD_BYTES),
        )
        self._write_path_total_bytes = 0
        self._pre_session_buffer = bytearray()
        self._pre_session_buffer_off: Optional[int] = None
        self._write_session_error: Optional[BaseException] = None
        self._write_session_chunks_submitted = 0

    def fileno(self) -> int:
        raise io.UnsupportedOperation("fluxon_fs remote file does not provide fileno()")

    def readable(self) -> bool:
        return bool(self._readable)

    def writable(self) -> bool:
        return bool(self._writable)

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return int(self._pos)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        self._flush_write_session_buffer()
        self._wait_write_session_barrier()
        off = int(offset)
        if whence == io.SEEK_SET:
            new_pos = off
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + off
        elif whence == io.SEEK_END:
            st = self._agent.remote_stat_by_handle_with_identity(
                self._export_name,
                self._relpath,
                self._file_abs,
                self._identity,
            )
            _exists, _is_file, _is_dir, size, mtime_ns, _mode = st
            self._size = int(size)
            self._mtime_ns = int(mtime_ns)
            new_pos = self._size + off
        else:
            raise ValueError(f"invalid whence: {whence}")
        if new_pos < 0:
            raise ValueError("negative seek position")
        self._pos = int(new_pos)
        if self._append:
            # English note: append writes always go to the end (best-effort). Refresh append pos
            # when we have a fresh end offset (SEEK_END path).
            if whence == io.SEEK_END:
                self._append_pos = int(self._size)
        return int(self._pos)

    def readinto(self, b: Any) -> int:
        if not self._readable:
            raise io.UnsupportedOperation("not readable")
        mv = memoryview(b).cast("B")
        if len(mv) == 0:
            return 0
        n = min(len(mv), self._chunk_bytes)
        data = self._agent.remote_read_chunk_by_handle_with_identity(
            self._export_name,
            self._relpath,
            int(self._pos),
            int(n),
            int(self._size),
            int(self._mtime_ns),
            bool(self._allow_kv_cache),
            self._file_abs,
            self._identity,
        )
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("remote_read_chunk_by_handle must return bytes")
        mv[: len(data)] = data
        self._pos += len(data)
        return int(len(data))

    def remote_read_chunk_by_handle_remote_read(
        self,
        export_name: str,
        relpath: str,
        pos: int,
        want: int,
        size: int,
        mtime_ns: int,
        file_abs: str,
    ) -> bytes:
        data = self._agent.remote_read_chunk_by_handle_remote_read_with_identity(
            export_name,
            relpath,
            int(pos),
            int(want),
            int(size),
            int(mtime_ns),
            file_abs,
            self._identity,
        )
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("remote_read_chunk_by_handle_remote_read must return bytes")
        return bytes(data)

    def _ensure_write_session(self) -> None:
        if self._write_session_id is not None:
            return
        session_id, size, mtime_ns = self._agent.remote_open_write_session_by_handle_with_identity(
            self._export_name,
            self._relpath,
            self._file_abs,
            self._identity,
        )
        if not isinstance(session_id, str) or not session_id:
            raise TypeError("remote_open_write_session_by_handle must return non-empty session_id")
        self._write_session_id = session_id
        self._size = max(self._size, int(size))
        self._mtime_ns = max(self._mtime_ns, int(mtime_ns))

    def _submit_write_session_payload(self, payload: Any, start_off: int) -> None:
        assert self._write_session_id is not None
        payload_len = len(payload)
        if payload_len == 0:
            return
        frame_bytes = max(1, int(self._write_session_frame_bytes))
        frame_count = (payload_len + frame_bytes - 1) // frame_bytes
        self._write_session_chunks_submitted += frame_count
        self._agent.remote_buffer_write_session_payload_by_handle_with_identity(
            self._export_name,
            self._relpath,
            self._write_session_id,
            int(start_off),
            payload,
            int(self._write_submit_bytes),
            int(self._write_session_max_inflight_chunks),
            self._file_abs,
            self._identity,
        )

    def _flush_write_session_buffer(self) -> None:
        if self._write_session_error is not None:
            raise self._write_session_error
        if self._write_session_id is None:
            return
        try:
            self._agent.remote_flush_write_session_buffer_by_handle_with_identity(
                self._export_name,
                self._relpath,
                self._write_session_id,
                self._file_abs,
                self._identity,
            )
        except Exception as exc:
            self._write_session_error = exc
            raise

    def _wait_write_session_barrier(self) -> None:
        if self._write_session_error is not None:
            raise self._write_session_error
        if self._write_session_id is None:
            return
        try:
            self._agent.remote_wait_write_session_payloads_by_handle_with_identity(
                self._export_name,
                self._relpath,
                self._write_session_id,
                self._file_abs,
                self._identity,
            )
        except Exception as exc:
            self._write_session_error = exc
            raise

    def _flush_pre_session_buffer_via_chunk_rpc(self) -> None:
        if not self._pre_session_buffer:
            self._pre_session_buffer_off = None
            return
        if self._pre_session_buffer_off is None:
            raise RuntimeError("pre-session buffer offset missing")
        off = int(self._pre_session_buffer_off)
        payload = bytes(self._pre_session_buffer)
        self._pre_session_buffer.clear()
        self._pre_session_buffer_off = None
        self._write_chunk_payload(payload, start_off=off)

    def _promote_pre_session_buffer_into_session(self) -> None:
        if not self._pre_session_buffer:
            self._pre_session_buffer_off = None
            return
        if self._pre_session_buffer_off is None:
            raise RuntimeError("pre-session buffer offset missing")
        payload = bytes(self._pre_session_buffer)
        off = int(self._pre_session_buffer_off)
        self._pre_session_buffer.clear()
        self._pre_session_buffer_off = None
        self._ensure_write_session()
        self._submit_write_session_payload(payload, off)

    def _buffer_pre_session_payload(self, mv: memoryview, start_off: int) -> int:
        if len(mv) == 0:
            return 0
        if self._pre_session_buffer_off is None:
            self._pre_session_buffer_off = int(start_off)
        else:
            expected_off = int(self._pre_session_buffer_off) + len(self._pre_session_buffer)
            if expected_off != int(start_off):
                self._flush_pre_session_buffer_via_chunk_rpc()
                self._pre_session_buffer_off = int(start_off)
        self._pre_session_buffer.extend(mv)
        self._advance_write_position(len(mv))
        self._write_path_total_bytes += int(len(mv))
        return int(len(mv))

    def _write_chunk_payload(self, payload: bytes, *, start_off: int) -> None:
        total = 0
        while total < len(payload):
            chunk = payload[total : total + self._chunk_bytes]
            off = int(start_off) + total
            self._agent.remote_write_chunk_by_handle_with_identity(
                self._export_name,
                self._relpath,
                off,
                chunk,
                self._file_abs,
                self._identity,
            )
            total += len(chunk)

    def _buffer_write_session_payload(self, payload: Any, start_off: int) -> None:
        if self._write_session_error is not None:
            raise self._write_session_error
        if len(payload) == 0:
            return
        self._submit_write_session_payload(payload, start_off)

    def _should_use_write_session(self, write_len: int) -> bool:
        if self._write_session_id is not None:
            return True
        if not self._write_session_eligible:
            return False
        projected_total = int(self._write_path_total_bytes) + max(0, int(write_len))
        return projected_total >= self._write_session_threshold_bytes

    def _write_via_chunk_rpc(self, mv: memoryview) -> int:
        if self._append:
            # Keep append conservative: avoid buffering before session because we
            # do not have a stable absolute offset contract across concurrent
            # appenders.
            total = 0
            while total < len(mv):
                chunk = bytes(mv[total : total + self._chunk_bytes])
                if self._append_pos is None:
                    raise RuntimeError("append_pos must be set for append mode")
                off = int(self._append_pos)
                self._agent.remote_write_chunk_by_handle_with_identity(
                    self._export_name,
                    self._relpath,
                    off,
                    chunk,
                    self._file_abs,
                    self._identity,
                )
                self._append_pos += len(chunk)
                self._pos = int(self._append_pos)
                self._size = max(self._size, int(self._append_pos))
                total += len(chunk)
            self._write_path_total_bytes += int(total)
            return int(total)
        return self._buffer_pre_session_payload(mv, int(self._pos))

    def _write_via_session(self, payload: Any, mv: memoryview) -> int:
        if self._write_session_id is None and self._pre_session_buffer:
            self._promote_pre_session_buffer_into_session()
        if self._append:
            if self._append_pos is None:
                raise RuntimeError("append_pos must be set for append mode")
            start_off = int(self._append_pos)
        else:
            start_off = int(self._pos)
        self._ensure_write_session()
        if not isinstance(payload, (bytes, bytearray)):
            payload = mv.tobytes()
        self._buffer_write_session_payload(payload, start_off)
        total = len(mv)
        self._advance_write_position(total)
        self._write_path_total_bytes += int(total)
        return int(total)

    def _advance_write_position(self, written: int) -> None:
        if written <= 0:
            return
        if self._append:
            if self._append_pos is None:
                raise RuntimeError("append_pos must be set for append mode")
            self._append_pos += int(written)
            self._pos = int(self._append_pos)
            self._size = max(self._size, int(self._append_pos))
            return
        self._pos += int(written)
        self._size = max(self._size, self._pos)

    def write(self, b: Any) -> int:
        if not self._writable:
            raise io.UnsupportedOperation("not writable")
        mv = memoryview(b).cast("B")
        if len(mv) == 0:
            return 0
        if self._should_use_write_session(len(mv)):
            return self._write_via_session(b, mv)
        return self._write_via_chunk_rpc(mv)

    def truncate(self, size: Optional[int] = None) -> int:
        if not self._writable:
            raise io.UnsupportedOperation("not writable")
        self._flush_pre_session_buffer_via_chunk_rpc()
        self._flush_write_session_buffer()
        new_size = self._pos if size is None else int(size)
        if new_size < 0:
            raise ValueError("negative truncate size")
        if self._write_session_id is not None:
            self._agent.remote_truncate_write_session_by_handle_with_identity(
                self._export_name,
                self._relpath,
                self._write_session_id,
                int(new_size),
                self._file_abs,
                self._identity,
            )
        else:
            self._agent.remote_truncate_by_handle_with_identity(
                self._export_name,
                self._relpath,
                int(new_size),
                self._file_abs,
                self._identity,
            )
        self._size = int(new_size)
        if self._append and self._append_pos is not None:
            self._append_pos = min(self._append_pos, self._size)
        if self._pos > self._size:
            self._pos = int(self._size)
        return int(self._size)

    def flush(self) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        self._flush_pre_session_buffer_via_chunk_rpc()
        self._flush_write_session_buffer()
        self._wait_write_session_barrier()
        return None

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self._write_session_id is None:
                self._flush_pre_session_buffer_via_chunk_rpc()
            if self._write_session_id is not None:
                try:
                    self._flush_write_session_buffer()
                    size, mtime_ns = self._agent.remote_close_write_session_by_handle_with_identity(
                        self._export_name,
                        self._relpath,
                        self._write_session_id,
                        self._file_abs,
                        self._identity,
                    )
                    self._size = int(size)
                    self._mtime_ns = int(mtime_ns)
                    if self._append:
                        self._append_pos = int(size)
                except Exception:
                    try:
                        self._agent.remote_abort_write_session_by_handle_with_identity(
                            self._export_name,
                            self._relpath,
                            self._write_session_id,
                            self._file_abs,
                            self._identity,
                        )
                    except Exception:
                        pass
                    raise
                finally:
                    self._write_session_id = None
                    self._pre_session_buffer.clear()
                    self._pre_session_buffer_off = None
                    self._write_session_error = None
            else:
                self._pre_session_buffer.clear()
                self._pre_session_buffer_off = None
        finally:
            super().close()


class _RemoteDirEntry:
    def __init__(
        self,
        *,
        patcher: "FluxonFsPatcher",
        parent_abs: str,
        name: str,
        is_file: bool,
        is_dir: bool,
    ) -> None:
        self._patcher = patcher
        self.name = name
        self.path = os.path.join(parent_abs, name)
        self._is_file = bool(is_file)
        self._is_dir = bool(is_dir)

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        return self._is_file

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        return self._is_dir

    def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        if follow_symlinks:
            return self._patcher._os_stat(self.path)
        return self._patcher._os_lstat(self.path)


@dataclass
class _OrigFns:
    open: Any
    io_open: Any
    os_open: Any
    os_close: Any
    path_open: Any
    path_exists: Any
    path_is_file: Any
    path_is_dir: Any
    path_stat: Any
    path_lstat: Any
    os_stat: Any
    os_lstat: Any
    os_listdir: Any
    os_scandir: Any
    os_mkdir: Any
    os_rmdir: Any
    os_unlink: Any
    os_rename: Any
    os_replace: Any
    os_chmod: Any
    os_utime: Any
    os_path_exists: Any
    os_path_isfile: Any
    os_path_isdir: Any
    os_path_getsize: Any


class FluxonFsPatcher:
    """Global FS interceptor.

    It is intentionally process-wide, similar to pyfakefs.Patcher.
    """

    def __init__(self, kv_store: Any):
        self._kv_store = kv_store
        self._lock = threading.RLock()
        self._orig: Optional[_OrigFns] = None
        self._installed = False
        self._ref_count = 0
        self._request_identity: Optional[Tuple[str, str]] = None

        inner = getattr(kv_store, "_client", None)
        if inner is None:
            raise RuntimeError(
                "fluxon_fs requires a fluxon backend store exposing _client (fluxon_pyo3.KvClient); "
                "mooncake backend is not supported"
            )
        import fluxon_pyo3  # type: ignore

        self._agent = fluxon_pyo3.FluxonFsAgent(inner)

        self._patched_module_attrs: list[tuple[ModuleType, str, Any]] = []

        # fd -> (path, os_open_flags)
        self._fd_track: Dict[int, Tuple[str, int]] = {}

        self._skip_modules: Tuple[ModuleType, ...] = (
            sys,
            os,
            io,
            pathlib,
            linecache,
            tokenize,
            importlib,
            types,
            genericpath,
        )

    def install(self) -> None:
        with self._lock:
            if self._installed:
                self._ref_count += 1
                return

            self._orig = _OrigFns(
                open=builtins.open,
                io_open=io.open,
                os_open=os.open,
                os_close=os.close,
                path_open=pathlib.Path.open,
                path_exists=pathlib.Path.exists,
                path_is_file=pathlib.Path.is_file,
                path_is_dir=pathlib.Path.is_dir,
                path_stat=pathlib.Path.stat,
                path_lstat=pathlib.Path.lstat,
                os_stat=os.stat,
                os_lstat=os.lstat,
                os_listdir=os.listdir,
                os_scandir=os.scandir,
                os_mkdir=os.mkdir,
                os_rmdir=os.rmdir,
                os_unlink=os.unlink,
                os_rename=os.rename,
                os_replace=os.replace,
                os_chmod=os.chmod,
                os_utime=os.utime,
                os_path_exists=os.path.exists,
                os_path_isfile=os.path.isfile,
                os_path_isdir=os.path.isdir,
                os_path_getsize=os.path.getsize,
            )

            builtins.open = self._open  # type: ignore[assignment]
            io.open = self._open  # type: ignore[assignment]
            os.open = self._os_open  # type: ignore[assignment]
            os.close = self._os_close  # type: ignore[assignment]
            os.stat = self._os_stat  # type: ignore[assignment]
            os.lstat = self._os_lstat  # type: ignore[assignment]
            os.listdir = self._os_listdir  # type: ignore[assignment]
            os.scandir = self._os_scandir  # type: ignore[assignment]
            os.mkdir = self._os_mkdir  # type: ignore[assignment]
            os.rmdir = self._os_rmdir  # type: ignore[assignment]
            os.unlink = self._os_unlink  # type: ignore[assignment]
            os.remove = self._os_unlink  # type: ignore[assignment]
            os.rename = self._os_rename  # type: ignore[assignment]
            os.replace = self._os_replace  # type: ignore[assignment]
            os.chmod = self._os_chmod  # type: ignore[assignment]
            os.utime = self._os_utime  # type: ignore[assignment]

            os.path.exists = self._os_path_exists  # type: ignore[assignment]
            os.path.isfile = self._os_path_isfile  # type: ignore[assignment]
            os.path.isdir = self._os_path_isdir  # type: ignore[assignment]
            os.path.getsize = self._os_path_getsize  # type: ignore[assignment]

            def _path_open(path_self: pathlib.Path, *args: Any, **kwargs: Any) -> Any:
                return self._open(path_self, *args, **kwargs)

            pathlib.Path.open = _path_open  # type: ignore[assignment]
            pathlib.Path.exists = lambda path_self: self._os_path_exists(path_self)  # type: ignore[assignment]
            pathlib.Path.is_file = lambda path_self: self._os_path_isfile(path_self)  # type: ignore[assignment]
            pathlib.Path.is_dir = lambda path_self: self._os_path_isdir(path_self)  # type: ignore[assignment]
            pathlib.Path.stat = lambda path_self, *args, **kwargs: self._os_stat(path_self, *args, **kwargs)  # type: ignore[assignment]
            pathlib.Path.lstat = lambda path_self, *args, **kwargs: self._os_lstat(path_self, *args, **kwargs)  # type: ignore[assignment]

            self._patch_loaded_modules()
            self._installed = True
            self._ref_count = 1

    def __enter__(self) -> "FluxonFsPatcher":
        self.install()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.uninstall()

    def uninstall(self) -> None:
        with self._lock:
            if not self._installed:
                return
            assert self._orig is not None
            self._ref_count -= 1
            if self._ref_count > 0:
                return

            builtins.open = self._orig.open  # type: ignore[assignment]
            io.open = self._orig.io_open  # type: ignore[assignment]
            os.open = self._orig.os_open  # type: ignore[assignment]
            os.close = self._orig.os_close  # type: ignore[assignment]
            os.stat = self._orig.os_stat  # type: ignore[assignment]
            os.lstat = self._orig.os_lstat  # type: ignore[assignment]
            os.listdir = self._orig.os_listdir  # type: ignore[assignment]
            os.scandir = self._orig.os_scandir  # type: ignore[assignment]
            os.mkdir = self._orig.os_mkdir  # type: ignore[assignment]
            os.rmdir = self._orig.os_rmdir  # type: ignore[assignment]
            os.unlink = self._orig.os_unlink  # type: ignore[assignment]
            os.remove = self._orig.os_unlink  # type: ignore[assignment]
            os.rename = self._orig.os_rename  # type: ignore[assignment]
            os.replace = self._orig.os_replace  # type: ignore[assignment]
            os.chmod = self._orig.os_chmod  # type: ignore[assignment]
            os.utime = self._orig.os_utime  # type: ignore[assignment]

            os.path.exists = self._orig.os_path_exists  # type: ignore[assignment]
            os.path.isfile = self._orig.os_path_isfile  # type: ignore[assignment]
            os.path.isdir = self._orig.os_path_isdir  # type: ignore[assignment]
            os.path.getsize = self._orig.os_path_getsize  # type: ignore[assignment]
            pathlib.Path.open = self._orig.path_open  # type: ignore[assignment]
            pathlib.Path.exists = self._orig.path_exists  # type: ignore[assignment]
            pathlib.Path.is_file = self._orig.path_is_file  # type: ignore[assignment]
            pathlib.Path.is_dir = self._orig.path_is_dir  # type: ignore[assignment]
            pathlib.Path.stat = self._orig.path_stat  # type: ignore[assignment]
            pathlib.Path.lstat = self._orig.path_lstat  # type: ignore[assignment]

            for m, k, v in self._patched_module_attrs:
                d = getattr(m, "__dict__", None)
                if isinstance(d, dict):
                    d[k] = v
            self._patched_module_attrs.clear()

            self._installed = False
            self._orig = None
            self._ref_count = 0
            self._fd_track.clear()

    def set_cache_config_yaml(self, cache_yaml: str) -> None:
        self._agent.set_cache_config_yaml(cache_yaml)

    def load_cache_config_from_master_config_file(self, config_path: pathlib.Path) -> None:
        self._agent.load_cache_config_from_master_config_file(str(config_path))

    def start_cache_config_fetch_from_master_config_file(self, config_path: pathlib.Path) -> None:
        self._agent.start_cache_config_fetch_from_master_config_file(str(config_path))

    def wait_cache_config_loaded(self) -> None:
        self._agent.wait_cache_config_loaded()

    def set_request_identity(self, username: str, password: str) -> None:
        self._agent.set_request_identity(username, password)
        self._request_identity = (str(username), str(password))

    def clear_request_identity(self) -> None:
        self._agent.clear_request_identity()
        self._request_identity = None

    def mount_remote_dir(
        self,
        *,
        local_mount_dir_abs: str,
        export_name: str,
    ) -> None:
        self._agent.mount_remote_dir(local_mount_dir_abs, export_name)

    def open_video_reader(
        self,
        *,
        export_name: str,
        relpath: str,
        height: int,
        width: int,
        num_threads: int = 8,
    ) -> Any:
        from .video import FluxonFsVideoReader

        return FluxonFsVideoReader._open(
            agent=self._agent,
            export_name=export_name,
            relpath=relpath,
            height=height,
            width=width,
            num_threads=num_threads,
            request_identity=self._request_identity,
        )

    def open_video_reader_pool(self, *, max_readers: int = 32) -> Any:
        from .video import FluxonFsVideoReaderPool

        return FluxonFsVideoReaderPool(
            agent=self._agent,
            request_identity=self._request_identity,
            max_readers=max_readers,
        )

    def publish_export(
        self,
        *,
        schema_version: int,
        export_name: str,
        export: Any,
    ) -> None:
        from .config_types import export_to_json_text
        instance_key_res = self._kv_store.instance_key()
        if not instance_key_res.is_ok():
            raise RuntimeError(
                f"patcher publish_export failed to get instance key: {instance_key_res.unwrap_error()}"
            )
        target_instance_key = instance_key_res.unwrap()
        inner = getattr(self._kv_store, "_client", None)
        if inner is None:
            raise RuntimeError(
                "patcher publish_export requires kv_store to expose _client (fluxon_pyo3.KvClient)"
            )
        import fluxon_pyo3  # type: ignore

        result = fluxon_pyo3.fluxon_fs_agent_publish_export(
            inner,
            target_instance_key,
            int(schema_version),
            export_name,
            export_to_json_text(export),
        )
        if not result.is_ok():
            raise RuntimeError(f"patcher publish_export failed: {result.unwrap_error()}")
        _ = result.unwrap()

    def unpublish_export(
        self,
        *,
        schema_version: int,
        export_name: str,
    ) -> None:
        instance_key_res = self._kv_store.instance_key()
        if not instance_key_res.is_ok():
            raise RuntimeError(
                f"patcher unpublish_export failed to get instance key: {instance_key_res.unwrap_error()}"
            )
        target_instance_key = instance_key_res.unwrap()
        inner = getattr(self._kv_store, "_client", None)
        if inner is None:
            raise RuntimeError(
                "patcher unpublish_export requires kv_store to expose _client (fluxon_pyo3.KvClient)"
            )
        import fluxon_pyo3  # type: ignore

        result = fluxon_pyo3.fluxon_fs_agent_unpublish_export(
            inner,
            target_instance_key,
            int(schema_version),
            export_name,
        )
        if not result.is_ok():
            raise RuntimeError(f"patcher unpublish_export failed: {result.unwrap_error()}")
        _ = result.unwrap()

    def _patch_loaded_modules(self) -> None:
        assert self._orig is not None
        for m in list(sys.modules.values()):
            if not isinstance(m, ModuleType):
                continue
            if m in self._skip_modules:
                continue
            d = getattr(m, "__dict__", None)
            if not isinstance(d, dict):
                continue

            for k, v in list(d.items()):
                if v is self._orig.open or v is self._orig.io_open:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._open
                elif v is self._orig.os_open:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_open
                elif v is self._orig.os_close:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_close
                elif v is self._orig.os_stat:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_stat
                elif v is self._orig.os_lstat:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_lstat
                elif v is self._orig.os_listdir:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_listdir
                elif v is self._orig.os_scandir:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_scandir
                elif v is self._orig.os_mkdir:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_mkdir
                elif v is self._orig.os_rmdir:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_rmdir
                elif v is self._orig.os_unlink:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_unlink
                elif v is self._orig.os_rename:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_rename
                elif v is self._orig.os_replace:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_replace
                elif v is self._orig.os_chmod:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_chmod
                elif v is self._orig.os_utime:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_utime
                elif v is self._orig.os_path_exists:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_path_exists
                elif v is self._orig.os_path_isfile:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_path_isfile
                elif v is self._orig.os_path_isdir:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_path_isdir
                elif v is self._orig.os_path_getsize:
                    self._patched_module_attrs.append((m, k, v))
                    d[k] = self._os_path_getsize

    def _default_remote_open_buffer_size(self, raw: Any, mode: str) -> int:
        if _is_write_mode(mode) and ("r" not in mode) and ("+" not in mode):
            preferred = int(getattr(raw, "_write_submit_bytes", io.DEFAULT_BUFFER_SIZE))
            return max(
                io.DEFAULT_BUFFER_SIZE,
                min(preferred, _REMOTE_DEFAULT_WRITE_BUFFER_BYTES_MAX),
            )
        return io.DEFAULT_BUFFER_SIZE

    def _open(
        self,
        file: Any,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        closefd: bool = True,
        opener: Optional[Callable[..., Any]] = None,
    ) -> Any:
        assert self._orig is not None
        if isinstance(file, int):
            return self._orig.open(
                file,
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                closefd=closefd,
                opener=opener,
            )

        file_abs = _normalize_abs(file)
        request_identity = self._request_identity

        if opener is not None and self._agent.is_remote_path(file_abs):
            raise ValueError("fluxon_fs remote open does not support opener")

        # English note: open() is an open-only primitive. For local paths, we must not trigger
        # any content IO at open-time (local Bytes plan reads the whole file), so read-only open()
        # bypasses agent.open_plan.
        if (not _is_write_mode(mode)) and (not bool(self._agent.is_remote_path(file_abs))):
            return self._orig.open(
                file,
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                closefd=closefd,
                opener=opener,
            )

        kind, plan_payload, _mirror_path, local_write_through, export_name, relpath, _upload_on_close = (
            self._agent.open_plan(file_abs, mode)
        )

        if kind == _OPEN_PLAN_KIND_BYPASS:
            inner = self._orig.open(
                file,
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                closefd=closefd,
                opener=opener,
            )
            if bool(local_write_through) and _is_write_mode(mode):
                return _FluxonFileProxy(
                    inner,
                    file_abs=file_abs,
                    mode=mode,
                    on_close=lambda p, m: self._agent.local_write_through_on_close(p, m),
                )
            return inner

        if kind == _OPEN_PLAN_KIND_FD:
            if not isinstance(plan_payload, int):
                raise TypeError("open_plan: fd payload must be raw_fd int")
            fd = plan_payload
            os.lseek(fd, 0, os.SEEK_SET)
            inner = self._orig.open(
                fd,
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                closefd=True,
                opener=None,
            )
            if (
                bool(_upload_on_close)
                and _is_write_mode(mode)
                and isinstance(export_name, str)
                and export_name
                and isinstance(relpath, str)
                and relpath
            ):
                return _FluxonFileProxy(
                    inner,
                    file_abs=file_abs,
                    mode=mode,
                    on_close=lambda _p, _m: self._agent.direct_write_fd_on_close(export_name, relpath),
                )
            return inner

        if kind == _OPEN_PLAN_KIND_BYTES:
            if not isinstance(plan_payload, (bytes, bytearray)):
                raise TypeError("open_plan: bytes payload must be bytes")
            fd = os.memfd_create("fluxon_fs_agent")
            os.write(fd, bytes(plan_payload))
            os.lseek(fd, 0, os.SEEK_SET)
            return self._orig.open(
                fd,
                mode,
                buffering=buffering,
                encoding=encoding,
                errors=errors,
                newline=newline,
                closefd=True,
                opener=None,
            )

        if kind == _OPEN_PLAN_KIND_REMOTE_HANDLE:
            if not isinstance(export_name, str) or not isinstance(relpath, str):
                raise TypeError("open_plan: export_name/relpath must be str")
            if not (isinstance(plan_payload, tuple) and len(plan_payload) == 2):
                raise TypeError("open_plan: remote payload must be (size:int, mtime_ns:int)")
            initial_size, initial_mtime_ns = plan_payload
            if not isinstance(initial_size, int) or not isinstance(initial_mtime_ns, int):
                raise TypeError("open_plan: remote payload must be (size:int, mtime_ns:int)")

            chunk_bytes = self._agent.remote_chunk_bytes()

            raw = _FluxonRemoteFileRaw(
                self._agent,
                export_name=export_name,
                relpath=relpath,
                mode=mode,
                file_abs=file_abs,
                initial_size=int(initial_size),
                initial_mtime_ns=int(initial_mtime_ns),
                chunk_bytes=int(chunk_bytes),
                request_identity=request_identity,
            )
            if "b" in mode:
                if buffering == 0:
                    return raw
                if int(buffering) >= 0:
                    bufsize = int(buffering)
                else:
                    bufsize = self._default_remote_open_buffer_size(raw, mode)
                if _is_write_mode(mode) and ("r" in mode or "+" in mode):
                    return io.BufferedRandom(raw, buffer_size=bufsize)
                if _is_write_mode(mode):
                    return io.BufferedWriter(raw, buffer_size=bufsize)
                return io.BufferedReader(raw, buffer_size=bufsize)

            # Text mode: always wrap a buffered binary layer.
            if buffering == 0:
                raise ValueError("can't have unbuffered text I/O")
            if int(buffering) >= 0:
                bufsize = int(buffering)
            else:
                bufsize = self._default_remote_open_buffer_size(raw, mode)
            if _is_write_mode(mode) and ("r" in mode or "+" in mode):
                base = io.BufferedRandom(raw, buffer_size=bufsize)
            elif _is_write_mode(mode):
                base = io.BufferedWriter(raw, buffer_size=bufsize)
            else:
                base = io.BufferedReader(raw, buffer_size=bufsize)
            return io.TextIOWrapper(
                base,
                encoding=encoding,
                errors=errors,
                newline=newline,
                line_buffering=False,
                write_through=False,
            )

        raise RuntimeError(f"unknown open_plan kind: {kind}")

    def _os_open(self, path: Any, flags: int, mode: int = 0o777, *, dir_fd: Any = None) -> int:
        with self._lock:
            assert self._orig is not None
            file_abs = _normalize_abs(path)

            if bool(self._agent.is_remote_path(file_abs)):
                raise OSError(errno.ENOTSUP, "fluxon_fs remote path does not support os.open()", file_abs)

            # English note: read-only os.open must bypass agent.open_plan to avoid content IO
            # (local Bytes plan reads the whole file) at open-time.
            if (int(flags) & _OS_OPEN_WRITE_INTENT_FLAGS) == 0:
                return int(self._orig.os_open(path, flags, mode, dir_fd=dir_fd))

            prep_mode = "r"
            if (flags & os.O_CREAT) and (flags & os.O_EXCL):
                prep_mode = "x"
            elif flags & os.O_TRUNC:
                prep_mode = "w"
            elif flags & os.O_APPEND:
                prep_mode = "a"
            elif flags & (os.O_WRONLY | os.O_RDWR):
                prep_mode = "a"

            kind, _plan_payload, _mirror_path, local_write_through, _export_name, _relpath, _upload_on_close = (
                self._agent.open_plan(file_abs, prep_mode)
            )

            fd = self._orig.os_open(path, flags, mode, dir_fd=dir_fd)
            if kind == _OPEN_PLAN_KIND_BYPASS and bool(local_write_through) and (
                (int(flags) & _OS_OPEN_WRITE_INTENT_FLAGS) != 0
            ):
                self._fd_track[int(fd)] = (file_abs, int(flags))
            return int(fd)

    def _os_close(self, fd: int) -> None:
        with self._lock:
            assert self._orig is not None
            tracked_local = self._fd_track.pop(int(fd), None)

        try:
            self._orig.os_close(fd)
        finally:
            if tracked_local is not None:
                file_abs, _flags = tracked_local
                self._agent.local_write_through_on_close(file_abs, "w")

    def _is_remote(self, file_abs: str) -> bool:
        return bool(self._agent.is_remote_path(file_abs))

    def _os_stat(self, path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_stat(path, *args, **kwargs)

        st = self._agent.path_stat(file_abs)
        exists, is_file, is_dir, size, mtime_ns, mode = st
        if not bool(exists):
            raise FileNotFoundError(file_abs)

        st_mode = int(mode) if isinstance(mode, int) and mode != 0 else 0
        if st_mode == 0:
            if bool(is_dir):
                st_mode |= _stat.S_IFDIR | 0o555
            elif bool(is_file):
                st_mode |= _stat.S_IFREG | 0o444
            else:
                st_mode |= _stat.S_IFREG | 0o444

        mtime_ns_i = int(mtime_ns)
        mtime = mtime_ns_i / 1_000_000_000.0
        return os.stat_result((st_mode, 0, 0, 1, 0, 0, int(size), mtime, mtime, mtime))

    def _os_lstat(self, path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_lstat(path, *args, **kwargs)

        st = self._agent.path_lstat(file_abs)
        exists, is_file, is_dir, size, mtime_ns, mode = st
        if not bool(exists):
            raise FileNotFoundError(file_abs)

        st_mode = int(mode) if isinstance(mode, int) and mode != 0 else 0
        if st_mode == 0:
            if bool(is_dir):
                st_mode |= _stat.S_IFDIR | 0o555
            elif bool(is_file):
                st_mode |= _stat.S_IFREG | 0o444
            else:
                st_mode |= _stat.S_IFREG | 0o444

        mtime_ns_i = int(mtime_ns)
        mtime = mtime_ns_i / 1_000_000_000.0
        return os.stat_result((st_mode, 0, 0, 1, 0, 0, int(size), mtime, mtime, mtime))

    def _os_path_exists(self, path: Any) -> bool:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return bool(self._orig.os_path_exists(path))
        st = self._agent.path_stat(file_abs)
        exists, _is_file, _is_dir, _size, _mtime_ns, _mode = st
        return bool(exists)

    def _os_path_isfile(self, path: Any) -> bool:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return bool(self._orig.os_path_isfile(path))
        st = self._agent.path_stat(file_abs)
        exists, is_file, _is_dir, _size, _mtime_ns, _mode = st
        return bool(exists) and bool(is_file)

    def _os_path_isdir(self, path: Any) -> bool:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return bool(self._orig.os_path_isdir(path))
        st = self._agent.path_stat(file_abs)
        exists, _is_file, is_dir, _size, _mtime_ns, _mode = st
        return bool(exists) and bool(is_dir)

    def _os_path_getsize(self, path: Any) -> int:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return int(self._orig.os_path_getsize(path))
        st = self._agent.path_stat(file_abs)
        exists, _is_file, _is_dir, size, _mtime_ns, _mode = st
        if not bool(exists):
            raise FileNotFoundError(file_abs)
        return int(size)

    def _os_listdir(self, path: Any) -> list[str]:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return list(self._orig.os_listdir(path))
        entries = self._agent.path_list_dir(file_abs)
        names: list[str] = []
        for name, _is_file, _is_dir in entries:
            if isinstance(name, str):
                names.append(name)
        return names

    def _os_scandir(self, path: Any) -> Any:
        assert self._orig is not None
        # shutil.rmtree() (and other stdlib helpers) may call os.scandir() with an int
        # directory fd. That path is always local; do not normalize/redirect it.
        if isinstance(path, int):
            return self._orig.os_scandir(path)
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_scandir(path)

        items = self._agent.path_list_dir(file_abs)
        entries: list[_RemoteDirEntry] = []
        for n, is_file, is_dir in items:
            if not isinstance(n, str) or not n:
                continue
            entries.append(
                _RemoteDirEntry(
                    patcher=self,
                    parent_abs=file_abs,
                    name=n,
                    is_file=bool(is_file),
                    is_dir=bool(is_dir),
                )
            )

        class _Iter:
            def __init__(self, it: list[_RemoteDirEntry]):
                self._it = it
                self._idx = 0

            def __iter__(self):
                return self

            def __next__(self):
                if self._idx >= len(self._it):
                    raise StopIteration
                v = self._it[self._idx]
                self._idx += 1
                return v

            def close(self) -> None:
                return

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                self.close()

        return _Iter(entries)

    def _os_mkdir(self, path: Any, mode: int = 0o777, *, dir_fd: Any = None) -> None:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_mkdir(path, mode, dir_fd=dir_fd)
        self._agent.path_mkdir(file_abs, int(mode))
        return None

    def _os_rmdir(self, path: Any, *, dir_fd: Any = None) -> None:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_rmdir(path, dir_fd=dir_fd)
        self._agent.path_rmdir(file_abs)
        return None

    def _os_unlink(self, path: Any, *, dir_fd: Any = None) -> None:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_unlink(path, dir_fd=dir_fd)
        self._agent.path_unlink(file_abs)
        return None

    def _os_rename(self, src: Any, dst: Any, *, src_dir_fd: Any = None, dst_dir_fd: Any = None) -> None:
        assert self._orig is not None
        src_abs = _normalize_abs(src)
        dst_abs = _normalize_abs(dst)
        if not (self._is_remote(src_abs) or self._is_remote(dst_abs)):
            return self._orig.os_rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)
        self._agent.path_rename(src_abs, dst_abs)
        return None

    def _os_replace(self, src: Any, dst: Any, *, src_dir_fd: Any = None, dst_dir_fd: Any = None) -> None:
        return self._os_rename(src, dst, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    def _os_chmod(self, path: Any, mode: int, *, dir_fd: Any = None, follow_symlinks: bool = True) -> None:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_chmod(path, mode, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        self._agent.path_chmod(file_abs, int(mode))
        return None

    def _os_utime(self, path: Any, times: Any = None, *, ns: Any = None, dir_fd: Any = None, follow_symlinks: bool = True) -> None:
        assert self._orig is not None
        file_abs = _normalize_abs(path)
        if not self._is_remote(file_abs):
            return self._orig.os_utime(path, times=times, ns=ns, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

        atime_ns = None
        mtime_ns = None
        if ns is not None:
            if not isinstance(ns, (tuple, list)) or len(ns) != 2:
                raise TypeError("ns must be a (atime_ns, mtime_ns) tuple")
            atime_ns = int(ns[0])
            mtime_ns = int(ns[1])
        elif times is not None:
            if not isinstance(times, (tuple, list)) or len(times) != 2:
                raise TypeError("times must be a (atime, mtime) tuple")
            atime_ns = int(float(times[0]) * 1_000_000_000)
            mtime_ns = int(float(times[1]) * 1_000_000_000)

        self._agent.path_utime(file_abs, atime_ns, mtime_ns)
        return None
