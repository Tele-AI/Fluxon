from __future__ import annotations

import base64
import fcntl
import hashlib
import http.server
import importlib.util
import json
import os
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import urllib.error
import urllib.parse
import urllib.request
import yaml

from fluxon_py.fluxon_fs import (
    FluxonFsTransferSkipEntry,
    FluxonFsTransferSkipEntryKind,
    FluxonFsTransferStateStoreConfig,
    FluxonFsTransferStateStoreKind,
    FluxonFsTransferStateStoreTiKvConfig,
    transfer_inspect_local_job_blocking,
    transfer_inspect_local_job_status_blocking,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
MIB = 1024 * 1024
GIB = 1024 * MIB
TRANSFER_CHUNK_BYTES = 1 * MIB
SCAN_BATCH_READY_BYTES = 10 * GIB
WHOLE_FLOW_STACK_STABILIZATION_SECS = 10.0
FIXTURE_VERSION = "v1"
FIXTURE_BASE_DIR = Path("/dev/shm/fluxon_fs_transfer_tikv_fixture_v1")
FIXTURE_LOCK_PATH = FIXTURE_BASE_DIR / "build.lock"
FIXTURE_READY_PATH = FIXTURE_BASE_DIR / "ready.txt"
FIXTURE_MANIFEST_PATH = FIXTURE_BASE_DIR / "manifest.json"
FIXTURE_SRC_ROOT = FIXTURE_BASE_DIR / "src"
FIXTURE_BUILD_ROOT = FIXTURE_BASE_DIR / "src_build"
TEST_DIR_BASE = Path("/tmp/fxfs_t")
TEST_DIR_CLEANUP_LOCK_PATH = TEST_DIR_BASE / ".cleanup.lock"
SHM_TEST_DIR_BASE = Path("/dev/shm/fxfs_t")
SHM_TEST_DIR_CLEANUP_LOCK_PATH = SHM_TEST_DIR_BASE / ".cleanup.lock"
TRANSFER_REPORT_BASE = Path("/tmp/fluxon_fs_transfer_reports")
ETCD_HARNESS_WORK_ROOT = Path("/mnt/nvme0/fluxon_fs_transfer_etcd")
ETCD_HARNESS_LOCK_NAME = ".lock"
TIKV_HARNESS_WORK_ROOT = Path("/mnt/nvme0/fluxon_fs_transfer_tikv")
TIKV_HARNESS_LOCK_NAME = ".lock"
TIKV_HARNESS_PD_LEASE_SECS = 60
TIKV_HARNESS_UNIFIED_READPOOL_MAX_THREADS = 4
TIKV_HARNESS_STORAGE_READPOOL_CONCURRENCY = 2
TIKV_HARNESS_COPROCESSOR_READPOOL_CONCURRENCY = 2
TIKV_HARNESS_ENDPOINT_MAX_CONCURRENCY = 8
TIKV_HARNESS_BACKGROUND_THREAD_COUNT = 2
TIKV_HARNESS_SCHEDULER_CONCURRENCY = 2048
TIKV_HARNESS_SCHEDULER_WORKER_POOL_SIZE = 2
TIKV_HARNESS_APPLY_POOL_SIZE = 1
TIKV_HARNESS_STORE_POOL_SIZE = 1
TIKV_HARNESS_ROCKSDB_MAX_BACKGROUND_JOBS = 2
TIKV_HARNESS_RAFTDB_MAX_BACKGROUND_JOBS = 2


@dataclass(frozen=True)
class PreparedTransferFixture:
    src_root: Path
    skip_entries: list[FluxonFsTransferSkipEntry]
    expected_entries: dict[str, int]
    expected_sha256: dict[str, str]
    expected_empty_dirs: list[str]


def new_test_dir(tag: str) -> Path:
    return _new_test_dir_under(
        base_dir=TEST_DIR_BASE,
        cleanup_lock_path=TEST_DIR_CLEANUP_LOCK_PATH,
        tag=tag,
    )


def new_shm_test_dir(tag: str) -> Path:
    return _new_test_dir_under(
        base_dir=SHM_TEST_DIR_BASE,
        cleanup_lock_path=SHM_TEST_DIR_CLEANUP_LOCK_PATH,
        tag=tag,
    )


def _new_test_dir_under(
    *,
    base_dir: Path,
    cleanup_lock_path: Path,
    tag: str,
) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_stale_test_dirs(
        base_dir=base_dir,
        cleanup_lock_path=cleanup_lock_path,
    )
    short_tag = hashlib.sha1(tag.encode("utf-8")).hexdigest()[:8]
    path = base_dir / f"{short_tag}_{int(time.time() * 1000):x}_{os.getpid():x}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def _extract_test_dir_pid(path: Path) -> int | None:
    parts = path.name.split("_")
    if len(parts) != 3:
        return None
    try:
        return int(parts[2], 16)
    except ValueError:
        return None


def _process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _cleanup_stale_test_dirs(
    *,
    base_dir: Path,
    cleanup_lock_path: Path,
) -> None:
    with cleanup_lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        for child in sorted(base_dir.iterdir(), key=lambda path: path.name):
            if not child.is_dir():
                continue
            pid = _extract_test_dir_pid(child)
            if pid is None or _process_is_alive(pid):
                continue
            _print_fixture_progress(f"removing stale test dir path={child} creator_pid={pid}")
            shutil.rmtree(child, ignore_errors=False)


def build_skip_entries() -> list[FluxonFsTransferSkipEntry]:
    return [
        FluxonFsTransferSkipEntry(
            kind=FluxonFsTransferSkipEntryKind.DIR,
            relpath="root/skipdir",
        ),
        FluxonFsTransferSkipEntry(
            kind=FluxonFsTransferSkipEntryKind.FILE,
            relpath="root/keep/skip.bin",
        ),
    ]


def _print_fixture_progress(detail: str) -> None:
    print(f"[fluxon_fs_transfer_fixture] {detail}", flush=True)


def _seed_byte_for_relpath(relpath: str) -> int:
    return hashlib.sha256(relpath.encode("utf-8")).digest()[0]


def _write_pattern_file(root: Path, relpath: str, size_bytes: int) -> str:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    chunk_bytes = 8 * MIB
    seed = _seed_byte_for_relpath(relpath)
    full_chunk = bytes([seed]) * chunk_bytes
    hasher = hashlib.sha256()
    remaining = size_bytes
    with path.open("wb") as handle:
        while remaining > 0:
            if remaining >= chunk_bytes:
                chunk = full_chunk
            else:
                chunk = full_chunk[:remaining]
            handle.write(chunk)
            hasher.update(chunk)
            remaining -= len(chunk)
    return hasher.hexdigest()


def _write_symlink(root: Path, relpath: str, target: str) -> None:
    path = root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(target)


def _build_dense_file_specs(
    *,
    prefix: str,
    group_count: int,
    files_per_group: int,
    file_size_bytes: int,
) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for group_index in range(group_count):
        for file_index in range(files_per_group):
            relpath = (
                f"{prefix}/group_{group_index:02d}/"
                f"file_{group_index:02d}_{file_index:04d}.bin"
            )
            out.append((relpath, file_size_bytes))
    return out


def _build_fixture_regular_file_specs() -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    out.extend(
        [
            ("root/root-direct-0.bin", 16 * MIB),
            ("root/root-direct-1.bin", 24 * MIB),
            ("root/keep/keep-a.bin", 64 * MIB),
            ("root/keep/keep-b.bin", 96 * MIB),
            ("root/keep/skip.bin", 256 * MIB),
            ("root/skipdir/hidden-a.bin", 512 * MIB),
            ("root/skipdir/hidden-b.bin", 512 * MIB),
            ("root/full_dir_a/huge-a.bin", 6 * GIB),
            ("root/full_dir_a/huge-b.bin", 4 * GIB),
        ]
    )
    for medium_index in range(8):
        out.append((f"root/full_dir_a/medium/medium_{medium_index:02d}.bin", 16 * MIB))
    out.extend(
        _build_dense_file_specs(
            prefix="root/full_dir_a/dense",
            group_count=16,
            files_per_group=128,
            file_size_bytes=64 * 1024,
        )
    )
    branch_sizes_mib = [3072, 3072, 2048, 1536, 1024]
    for branch_index, size_mib in enumerate(branch_sizes_mib):
        out.append(
            (
                f"root/full_dir_b/branch_{branch_index:02d}/payload.bin",
                size_mib * MIB,
            )
        )
    out.extend(
        _build_dense_file_specs(
            prefix="root/full_dir_b/dense",
            group_count=8,
            files_per_group=128,
            file_size_bytes=128 * 1024,
        )
    )
    out.extend(
        [
            ("root/split_tail/direct/direct-0.bin", 16 * MIB),
            ("root/split_tail/direct/direct-1.bin", 24 * MIB),
            ("root/split_tail/child_a/payload-0.bin", 2 * GIB),
            ("root/split_tail/child_a/payload-1.bin", 1 * GIB),
            ("root/split_tail/child_b/payload-0.bin", 2 * GIB),
            ("root/split_tail/child_b/payload-1.bin", 1536 * MIB),
            ("root/split_tail/child_c/payload-0.bin", 768 * MIB),
        ]
    )
    out.extend(
        _build_dense_file_specs(
            prefix="root/split_tail/child_a/dense",
            group_count=8,
            files_per_group=64,
            file_size_bytes=128 * 1024,
        )
    )
    for medium_index in range(8):
        out.append((f"root/split_tail/child_b/medium/medium_{medium_index:02d}.bin", 16 * MIB))
    out.extend(
        _build_dense_file_specs(
            prefix="root/split_tail/child_c/dense",
            group_count=8,
            files_per_group=128,
            file_size_bytes=128 * 1024,
        )
    )
    return out


def _fixture_empty_dirs() -> tuple[str, ...]:
    return (
        "root/emptydir",
        "root/full_dir_a/emptydir",
        "root/full_dir_b/emptydir",
        "root/split_tail/emptydir",
        "root/split_tail/child_b/emptydir",
    )


def _fixture_symlinks() -> tuple[tuple[str, str], ...]:
    return (
        ("root/link-root-direct", "root-direct-0.bin"),
        ("root/link-full-dir-b", "full_dir_b"),
        ("root/full_dir_b/link-branch-0", "branch_00/payload.bin"),
        ("root/split_tail/child_c/link-dense", "dense"),
    )


def _build_large_transfer_fixture(root: Path) -> dict[str, object]:
    skip_entries = build_skip_entries()
    expected_entries: dict[str, int] = {}
    expected_sha256: dict[str, str] = {}
    specs = _build_fixture_regular_file_specs()
    _print_fixture_progress(
        f"building fixture_version={FIXTURE_VERSION} path={root} regular_file_count={len(specs)}"
    )
    for relpath in _fixture_empty_dirs():
        (root / relpath).mkdir(parents=True, exist_ok=True)
    progress_roots = (
        "root/keep/",
        "root/skipdir/",
        "root/full_dir_a/",
        "root/full_dir_b/",
        "root/split_tail/",
    )
    current_progress_root = ""
    for relpath, size_bytes in specs:
        matched_progress_root = next(
            (
                progress_root
                for progress_root in progress_roots
                if relpath.startswith(progress_root)
            ),
            "root/",
        )
        if matched_progress_root != current_progress_root:
            current_progress_root = matched_progress_root
            _print_fixture_progress(f"writing subtree={current_progress_root}")
        sha256 = _write_pattern_file(root, relpath, size_bytes)
        if is_relpath_skipped(skip_entries, relpath):
            continue
        expected_entries[relpath] = size_bytes
        expected_sha256[relpath] = sha256
    for relpath, target in _fixture_symlinks():
        _write_symlink(root, relpath, target)
    return {
        "version": FIXTURE_VERSION,
        "expected_regular_files": [
            {
                "relpath": relpath,
                "size": expected_entries[relpath],
                "sha256": expected_sha256[relpath],
            }
            for relpath in sorted(expected_entries)
        ],
    }


def _remove_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    shutil.rmtree(path, ignore_errors=False)


def _clear_directory_children(root: Path, *, preserved_names: set[str]) -> None:
    if not root.is_dir():
        raise AssertionError(f"expected directory root: {root}")
    for child in sorted(root.iterdir(), key=lambda path: path.name):
        if child.name in preserved_names:
            continue
        _remove_path(child)


def _atomic_write_text(path: Path, text: str) -> None:
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(text, encoding="utf-8")
    os.replace(temp_path, path)


def _atomic_write_json(path: Path, value: object) -> None:
    _atomic_write_text(
        path,
        json.dumps(value, sort_keys=True, separators=(",", ":")),
    )


def _fixture_is_ready() -> bool:
    return (
        FIXTURE_READY_PATH.is_file()
        and FIXTURE_READY_PATH.read_text(encoding="utf-8") == FIXTURE_VERSION
        and FIXTURE_MANIFEST_PATH.is_file()
        and FIXTURE_SRC_ROOT.is_dir()
    )


def _load_prepared_transfer_fixture() -> PreparedTransferFixture:
    manifest = json.loads(FIXTURE_MANIFEST_PATH.read_text(encoding="utf-8"))
    expected_entries: dict[str, int] = {}
    expected_sha256: dict[str, str] = {}
    for entry in manifest["expected_regular_files"]:
        relpath = str(entry["relpath"])
        expected_entries[relpath] = int(entry["size"])
        expected_sha256[relpath] = str(entry["sha256"])
    skip_entries = build_skip_entries()
    return PreparedTransferFixture(
        src_root=FIXTURE_SRC_ROOT,
        skip_entries=skip_entries,
        expected_entries=dict(sorted(expected_entries.items())),
        expected_sha256=dict(sorted(expected_sha256.items())),
        expected_empty_dirs=collect_expected_subtree_empty_dir_entries(
            src_root=FIXTURE_SRC_ROOT,
            dir_relpath=".",
            skip_entries=skip_entries,
        ),
    )


def prepare_transfer_fixture_once() -> PreparedTransferFixture:
    FIXTURE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    with FIXTURE_LOCK_PATH.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if _fixture_is_ready():
            fixture = _load_prepared_transfer_fixture()
            _print_fixture_progress(
                "reusing ready fixture "
                f"path={fixture.src_root} file_count={len(fixture.expected_entries)}"
            )
            return fixture
        _print_fixture_progress(f"rebuilding fixture base_dir={FIXTURE_BASE_DIR}")
        _remove_path(FIXTURE_READY_PATH)
        _remove_path(FIXTURE_MANIFEST_PATH)
        _remove_path(FIXTURE_BUILD_ROOT)
        _remove_path(FIXTURE_SRC_ROOT)
        FIXTURE_BUILD_ROOT.mkdir(parents=True, exist_ok=False)
        manifest = _build_large_transfer_fixture(FIXTURE_BUILD_ROOT)
        os.replace(FIXTURE_BUILD_ROOT, FIXTURE_SRC_ROOT)
        _atomic_write_json(FIXTURE_MANIFEST_PATH, manifest)
        _atomic_write_text(FIXTURE_READY_PATH, FIXTURE_VERSION)
        fixture = _load_prepared_transfer_fixture()
        total_bytes = sum(fixture.expected_entries.values())
        _print_fixture_progress(
            "fixture ready "
            f"path={fixture.src_root} file_count={len(fixture.expected_entries)} total_bytes={total_bytes}"
        )
        return fixture


def _join_relpath(parent: str, child_name: str) -> str:
    if parent == ".":
        return child_name
    return f"{parent.rstrip('/')}/{child_name}"


def _abs_path_for_relpath(root: Path, relpath: str) -> Path:
    if relpath == ".":
        return root
    return root / relpath


def _skip_entry_matches_relpath(entry: FluxonFsTransferSkipEntry, relpath: str) -> bool:
    if entry.kind == FluxonFsTransferSkipEntryKind.DIR:
        return relpath == entry.relpath or relpath.startswith(f"{entry.relpath}/")
    if entry.kind == FluxonFsTransferSkipEntryKind.FILE:
        return relpath == entry.relpath
    raise ValueError(f"unsupported skip entry kind: {entry.kind}")


def is_relpath_skipped(
    skip_entries: Iterable[FluxonFsTransferSkipEntry],
    relpath: str,
) -> bool:
    for entry in skip_entries:
        if _skip_entry_matches_relpath(entry, relpath):
            return True
    return False


def collect_expected_direct_file_entries(
    *,
    src_root: Path,
    dir_relpath: str,
    skip_entries: list[FluxonFsTransferSkipEntry],
) -> dict[str, int]:
    if is_relpath_skipped(skip_entries, dir_relpath):
        return {}
    dir_abs = _abs_path_for_relpath(src_root, dir_relpath)
    if not dir_abs.is_dir():
        raise AssertionError(f"batch root must exist as directory: {dir_relpath}")
    out: dict[str, int] = {}
    for child in sorted(dir_abs.iterdir(), key=lambda path: path.name):
        child_relpath = _join_relpath(dir_relpath, child.name)
        if is_relpath_skipped(skip_entries, child_relpath):
            continue
        if child.is_symlink():
            continue
        if child.is_file():
            out[child_relpath] = child.stat().st_size
    return out


def collect_expected_subtree_file_entries(
    *,
    src_root: Path,
    dir_relpath: str,
    skip_entries: list[FluxonFsTransferSkipEntry],
) -> dict[str, int]:
    if is_relpath_skipped(skip_entries, dir_relpath):
        return {}
    dir_abs = _abs_path_for_relpath(src_root, dir_relpath)
    if dir_abs.is_symlink():
        return {}
    if dir_abs.is_file():
        return {dir_relpath: dir_abs.stat().st_size}
    if not dir_abs.is_dir():
        raise AssertionError(f"batch root must exist as directory: {dir_relpath}")
    out: dict[str, int] = {}
    stack: list[tuple[Path, str]] = [(dir_abs, dir_relpath)]
    while stack:
        current_abs, current_rel = stack.pop()
        for child in sorted(current_abs.iterdir(), key=lambda path: path.name, reverse=True):
            child_relpath = _join_relpath(current_rel, child.name)
            if is_relpath_skipped(skip_entries, child_relpath):
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                stack.append((child, child_relpath))
                continue
            if child.is_file():
                out[child_relpath] = child.stat().st_size
    return dict(sorted(out.items()))


def collect_expected_subtree_empty_dir_entries(
    *,
    src_root: Path,
    dir_relpath: str,
    skip_entries: list[FluxonFsTransferSkipEntry],
) -> list[str]:
    if is_relpath_skipped(skip_entries, dir_relpath):
        return []
    dir_abs = _abs_path_for_relpath(src_root, dir_relpath)
    if dir_abs.is_symlink() or dir_abs.is_file():
        return []
    if not dir_abs.is_dir():
        raise AssertionError(f"batch root must exist as directory: {dir_relpath}")
    out: list[str] = []
    stack: list[tuple[Path, str]] = [(dir_abs, dir_relpath)]
    while stack:
        current_abs, current_rel = stack.pop()
        has_child_coverage = False
        child_dirs: list[tuple[Path, str]] = []
        for child in sorted(current_abs.iterdir(), key=lambda path: path.name, reverse=True):
            child_relpath = _join_relpath(current_rel, child.name)
            if is_relpath_skipped(skip_entries, child_relpath):
                continue
            if child.is_symlink():
                has_child_coverage = True
                continue
            if child.is_dir():
                has_child_coverage = True
                child_dirs.append((child, child_relpath))
                continue
            if child.is_file():
                has_child_coverage = True
                continue
        if not has_child_coverage:
            out.append(current_rel)
            continue
        stack.extend(child_dirs)
    return sorted(out)


def validate_transfer_job_batches_against_source(
    *,
    job: dict[str, object],
    src_root: Path,
    skip_entries: list[FluxonFsTransferSkipEntry],
) -> dict[str, object]:
    expected_all = collect_expected_subtree_file_entries(
        src_root=src_root,
        dir_relpath=".",
        skip_entries=skip_entries,
    )
    expected_all_empty_dirs = collect_expected_subtree_empty_dir_entries(
        src_root=src_root,
        dir_relpath=".",
        skip_entries=skip_entries,
    )
    covered_entries: dict[str, int] = {}
    covered_empty_dirs: set[str] = set()
    batch_total_sizes: dict[str, int] = {}
    batch_kind_counts = {"full_dir": 0, "direct_files_only": 0}
    for batch in job["batches"]:
        batch_id = str(batch["batch_id"])
        root_relpath = str(batch["root_relpath"])
        batch_kind = str(batch["batch_kind"])
        actual_empty_dirs = sorted(str(relpath) for relpath in batch["empty_dir_relpaths"])
        if batch_kind == "full_dir":
            batch_kind_counts["full_dir"] += 1
            expected_entries = collect_expected_subtree_file_entries(
                src_root=src_root,
                dir_relpath=root_relpath,
                skip_entries=skip_entries,
            )
            expected_empty_dirs = collect_expected_subtree_empty_dir_entries(
                src_root=src_root,
                dir_relpath=root_relpath,
                skip_entries=skip_entries,
            )
        elif batch_kind == "direct_files_only":
            batch_kind_counts["direct_files_only"] += 1
            expected_entries = collect_expected_direct_file_entries(
                src_root=src_root,
                dir_relpath=root_relpath,
                skip_entries=skip_entries,
            )
            expected_empty_dirs = []
        else:
            raise AssertionError(f"unsupported batch kind in inspect output: {batch_kind}")

        actual_entries: dict[str, int] = {}
        for entry in batch["entries"]:
            relpath = str(entry["relpath"])
            size = int(entry["size"])
            if relpath in actual_entries:
                raise AssertionError(f"duplicate relpath inside one batch: {batch_id} {relpath}")
            actual_entries[relpath] = size
            if relpath in covered_entries:
                raise AssertionError(
                    f"duplicate relpath across batches: {relpath} "
                    f"first_size={covered_entries[relpath]} second_batch={batch_id}"
                )
            covered_entries[relpath] = size
        for relpath in actual_empty_dirs:
            if relpath in covered_empty_dirs:
                raise AssertionError(
                    f"duplicate empty dir relpath across batches: {relpath} second_batch={batch_id}"
                )
            covered_empty_dirs.add(relpath)
        if actual_entries != expected_entries:
            raise AssertionError(
                f"batch manifest does not match batch contract: "
                f"batch_id={batch_id} root_relpath={root_relpath} batch_kind={batch_kind} "
                f"expected={expected_entries} actual={actual_entries}"
            )
        if actual_empty_dirs != expected_empty_dirs:
            raise AssertionError(
                f"batch empty dir manifest does not match batch contract: "
                f"batch_id={batch_id} root_relpath={root_relpath} batch_kind={batch_kind} "
                f"expected={expected_empty_dirs} actual={actual_empty_dirs}"
            )
        batch_total_sizes[batch_id] = sum(actual_entries.values())

    if covered_entries != expected_all:
        raise AssertionError(
            f"job coverage mismatch: expected={expected_all} actual={covered_entries}"
        )
    if sorted(covered_empty_dirs) != expected_all_empty_dirs:
        raise AssertionError(
            f"job empty dir coverage mismatch: expected={expected_all_empty_dirs} "
            f"actual={sorted(covered_empty_dirs)}"
        )

    return {
        "expected_entries": expected_all,
        "expected_empty_dirs": expected_all_empty_dirs,
        "batch_total_sizes": batch_total_sizes,
        "batch_kind_counts": batch_kind_counts,
    }


def collect_regular_file_entries(
    root: Path,
    *,
    ignored_top_level_dirs: tuple[str, ...] = (),
) -> dict[str, int]:
    ignored = set(ignored_top_level_dirs)
    out: dict[str, int] = {}
    stack = [root]
    while stack:
        current = stack.pop()
        for child in sorted(current.iterdir(), key=lambda path: path.name, reverse=True):
            relpath = child.relative_to(root).as_posix()
            top_level = relpath.split("/", 1)[0]
            if top_level in ignored:
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                stack.append(child)
                continue
            if child.is_file():
                out[relpath] = child.stat().st_size
    return dict(sorted(out.items()))


def collect_directory_relpaths(
    root: Path,
    *,
    ignored_top_level_dirs: tuple[str, ...] = (),
) -> list[str]:
    ignored = set(ignored_top_level_dirs)
    out: list[str] = []
    stack = [root]
    while stack:
        current = stack.pop()
        for child in sorted(current.iterdir(), key=lambda path: path.name, reverse=True):
            relpath = child.relative_to(root).as_posix()
            if relpath == ".":
                continue
            top_level = relpath.split("/", 1)[0]
            if top_level in ignored:
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                out.append(relpath)
                stack.append(child)
    return sorted(out)


def sha256_for_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def collect_regular_file_sha256(
    root: Path,
    *,
    ignored_top_level_dirs: tuple[str, ...] = (),
) -> dict[str, str]:
    ignored = set(ignored_top_level_dirs)
    out: dict[str, str] = {}
    stack = [root]
    while stack:
        current = stack.pop()
        for child in sorted(current.iterdir(), key=lambda path: path.name, reverse=True):
            relpath = child.relative_to(root).as_posix()
            top_level = relpath.split("/", 1)[0]
            if top_level in ignored:
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                stack.append(child)
                continue
            if child.is_file():
                out[relpath] = sha256_for_file(child)
    return dict(sorted(out.items()))


def collect_regular_file_signatures(
    root: Path,
    *,
    ignored_top_level_dirs: tuple[str, ...] = (),
) -> dict[str, tuple[int, str]]:
    ignored = set(ignored_top_level_dirs)
    out: dict[str, tuple[int, str]] = {}
    stack = [root]
    while stack:
        current = stack.pop()
        for child in sorted(current.iterdir(), key=lambda path: path.name, reverse=True):
            relpath = child.relative_to(root).as_posix()
            top_level = relpath.split("/", 1)[0]
            if top_level in ignored:
                continue
            if child.is_symlink():
                continue
            if child.is_dir():
                stack.append(child)
                continue
            if child.is_file():
                out[relpath] = (child.stat().st_size, sha256_for_file(child))
    return dict(sorted(out.items()))


def collect_file_sha256_for_relpaths(root: Path, relpaths: Iterable[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for relpath in sorted(relpaths):
        out[relpath] = sha256_for_file(root / relpath)
    return out


def collect_collect_info_output_paths(dst_root: Path) -> list[str]:
    collect_root = dst_root / "fluxon_collect_info" / "batches"
    if not collect_root.exists():
        return []
    out: list[str] = []
    for path in sorted(collect_root.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        out.append(path.relative_to(dst_root).as_posix())
    return out


def wait_for_staging_file_at_least_bytes(
    dst_root: Path,
    *,
    min_size_bytes: int,
    timeout_secs: float,
) -> Path:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        staging_root = dst_root / ".fluxon.stage"
        if staging_root.exists():
            for path in sorted(staging_root.rglob("*.fluxon.part")):
                if path.is_symlink() or not path.is_file():
                    continue
                if path.stat().st_size >= min_size_bytes:
                    return path
        time.sleep(0.2)
    raise AssertionError(
        "timed out waiting for staged transfer file to reach "
        f"{min_size_bytes} bytes under dst_root={dst_root}"
    )


def wait_for_staging_root_gone(
    dst_root: Path,
    *,
    timeout_secs: float,
) -> None:
    deadline = time.time() + timeout_secs
    last_entries: list[str] = []
    while time.time() < deadline:
        staging_root = dst_root / ".fluxon.stage"
        if not staging_root.exists():
            return
        last_entries = []
        for path in sorted(staging_root.rglob("*")):
            last_entries.append(path.relative_to(dst_root).as_posix())
            if len(last_entries) >= 16:
                break
        time.sleep(0.2)
    raise AssertionError(
        "timed out waiting for transfer staging cleanup under "
        f"dst_root={dst_root}; sample_entries={last_entries}"
    )


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_tcp(host: str, port: int, *, label: str, proc: subprocess.Popen, log_path: Path) -> None:
    deadline = time.time() + 40.0
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) == 0:
                return
        rc = proc.poll()
        if rc is not None:
            raise RuntimeError(
                f"{label} exited early: rc={rc} log={log_path.read_text(encoding='utf-8', errors='replace')}"
            )
        if time.time() >= deadline:
            raise RuntimeError(
                f"timed out waiting for {label} on {host}:{port} log={log_path.read_text(encoding='utf-8', errors='replace')}"
            )
        time.sleep(0.1)


def _tikv_runtime_dir() -> Path:
    return REPO_ROOT / "fluxon_release" / "ext_images" / "tikv"


def ensure_tikv_runtime_paths() -> tuple[Path, Path]:
    runtime_dir = _tikv_runtime_dir()
    pd_start = runtime_dir / "start_pd.sh"
    tikv_start = runtime_dir / "start_tikv.sh"
    if not pd_start.is_file() or not tikv_start.is_file():
        raise FileNotFoundError(
            "missing TiKV ext runtime. Run `python3 setup_and_pack/pack_release_ext.py --release-dir fluxon_release` first."
        )
    return pd_start, tikv_start


class TiKvHarness:
    def __init__(self, *, tag: str):
        _ = tag
        pd_start, tikv_start = ensure_tikv_runtime_paths()
        self._temp_root = TIKV_HARNESS_WORK_ROOT
        self._lock_path = self._temp_root / TIKV_HARNESS_LOCK_NAME
        self._lock_handle = None
        self._prepare_work_root()
        self._pd_port = _pick_free_port()
        self._pd_peer_port = _pick_free_port()
        self._tikv_port = _pick_free_port()
        self._tikv_status_port = _pick_free_port()
        self._pd_config = self._temp_root / "pd_config.sh"
        self._pd_runtime_config = self._temp_root / "pd.toml"
        self._tikv_config = self._temp_root / "tikv_config.sh"
        self._tikv_runtime_config = self._temp_root / "tikv.toml"
        self._pd_log = self._temp_root / "pd.log"
        self._tikv_log = self._temp_root / "tikv.log"
        self._pd_endpoint = f"127.0.0.1:{self._pd_port}"
        self._pd_http_base_url = f"http://127.0.0.1:{self._pd_port}"
        self._write_pd_runtime_config()
        self._pd_config.write_text(
            "\n".join(
                [
                    "declare -a PD_ARGS=(",
                    '  --config "$WORKDIR/pd.toml"',
                    "  --name pd0",
                    '  --data-dir "$WORKDIR/pd-data"',
                    f'  --client-urls "http://127.0.0.1:{self._pd_port}"',
                    f'  --advertise-client-urls "http://127.0.0.1:{self._pd_port}"',
                    f'  --peer-urls "http://127.0.0.1:{self._pd_peer_port}"',
                    f'  --advertise-peer-urls "http://127.0.0.1:{self._pd_peer_port}"',
                    f'  --initial-cluster "pd0=http://127.0.0.1:{self._pd_peer_port}"',
                    '  --log-file "$WORKDIR/pd.log"',
                    ")",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self._write_tikv_runtime_config()
        self._tikv_config.write_text(
            "\n".join(
                [
                    "declare -a TIKV_ARGS=(",
                    '  --config "$WORKDIR/tikv.toml"',
                    f'  --pd-endpoints "{self._pd_endpoint}"',
                    f'  --addr "127.0.0.1:{self._tikv_port}"',
                    f'  --advertise-addr "127.0.0.1:{self._tikv_port}"',
                    f'  --status-addr "127.0.0.1:{self._tikv_status_port}"',
                    '  --data-dir "$WORKDIR/tikv-data"',
                    '  --log-file "$WORKDIR/tikv.log"',
                    ")",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self._pd_stdout = self._pd_log.open("w", encoding="utf-8")
        self._pd_proc = subprocess.Popen(
            [str(pd_start), "--config", str(self._pd_config), "--workdir", str(self._temp_root)],
            stdin=subprocess.DEVNULL,
            stdout=self._pd_stdout,
            stderr=subprocess.STDOUT,
        )
        _wait_for_tcp("127.0.0.1", self._pd_port, label="pd-server", proc=self._pd_proc, log_path=self._pd_log)
        self._tikv_stdout = self._tikv_log.open("w", encoding="utf-8")
        self._tikv_proc = subprocess.Popen(
            [str(tikv_start), "--config", str(self._tikv_config), "--workdir", str(self._temp_root)],
            stdin=subprocess.DEVNULL,
            stdout=self._tikv_stdout,
            stderr=subprocess.STDOUT,
        )
        _wait_for_tcp(
            "127.0.0.1",
            self._tikv_port,
            label="tikv-server",
            proc=self._tikv_proc,
            log_path=self._tikv_log,
        )

    def _prepare_work_root(self) -> None:
        self._temp_root.mkdir(parents=True, exist_ok=True)
        self._lock_handle = self._lock_path.open("a+", encoding="utf-8")
        try:
            # The fixed work root is a single authoritative harness instance.
            # A second concurrent user must fail immediately instead of racing
            # with directory cleanup and corrupting the shared PD/TiKV state.
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as err:
            self._lock_handle.close()
            self._lock_handle = None
            raise RuntimeError(
                f"TiKV harness work root is already locked: {self._temp_root}"
            ) from err
        _clear_directory_children(
            self._temp_root,
            preserved_names={TIKV_HARNESS_LOCK_NAME},
        )

    def _wait_for_pd_http_ready(self) -> None:
        # PD opening the TCP port is not enough for TiKV bootstrap. The harness
        # must wait until PD's HTTP API is serving a leader-backed response so
        # TiKV does not race against single-node PD election and TSO startup.
        _wait_for_http_status(
            url=f"{self._pd_http_base_url}/pd/api/v1/members",
            accepted_statuses=(200,),
            label="pd-http-ready",
            proc=self._pd_proc,
            log_path=self._pd_log,
        )

    def _write_pd_runtime_config(self) -> None:
        # The local integration stack uses a single PD member on a temporary disk.
        # The default 3-second leader lease is too short once fsync occasionally
        # stretches into the 1-2 second range, so the harness must make the leader
        # lease tolerant to transient local stalls instead of changing transfer logic.
        self._pd_runtime_config.write_text(
            "\n".join(
                [
                    f"lease = {TIKV_HARNESS_PD_LEASE_SECS}",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def _write_tikv_runtime_config(self) -> None:
        # The local single-node test stack must not inherit production-scale
        # host-wide concurrency defaults. When TiKV auto-scales itself against a
        # large host, the temporary-disk integration stack becomes self-induced
        # IO bound and destabilizes PD/TSO instead of exercising transfer logic.
        self._tikv_runtime_config.write_text(
            "\n".join(
                [
                    "[readpool.unified]",
                    f"max-thread-count = {TIKV_HARNESS_UNIFIED_READPOOL_MAX_THREADS}",
                    "",
                    "[readpool.storage]",
                    f"high-concurrency = {TIKV_HARNESS_STORAGE_READPOOL_CONCURRENCY}",
                    f"normal-concurrency = {TIKV_HARNESS_STORAGE_READPOOL_CONCURRENCY}",
                    f"low-concurrency = {TIKV_HARNESS_STORAGE_READPOOL_CONCURRENCY}",
                    "",
                    "[readpool.coprocessor]",
                    f"high-concurrency = {TIKV_HARNESS_COPROCESSOR_READPOOL_CONCURRENCY}",
                    f"normal-concurrency = {TIKV_HARNESS_COPROCESSOR_READPOOL_CONCURRENCY}",
                    f"low-concurrency = {TIKV_HARNESS_COPROCESSOR_READPOOL_CONCURRENCY}",
                    "",
                    "[server]",
                    f"end-point-max-concurrency = {TIKV_HARNESS_ENDPOINT_MAX_CONCURRENCY}",
                    f"background-thread-count = {TIKV_HARNESS_BACKGROUND_THREAD_COUNT}",
                    "",
                    "[storage]",
                    f"scheduler-concurrency = {TIKV_HARNESS_SCHEDULER_CONCURRENCY}",
                    f"scheduler-worker-pool-size = {TIKV_HARNESS_SCHEDULER_WORKER_POOL_SIZE}",
                    "",
                    "[raftstore]",
                    f"apply-pool-size = {TIKV_HARNESS_APPLY_POOL_SIZE}",
                    f"store-pool-size = {TIKV_HARNESS_STORE_POOL_SIZE}",
                    "",
                    "[rocksdb]",
                    f"max-background-jobs = {TIKV_HARNESS_ROCKSDB_MAX_BACKGROUND_JOBS}",
                    "",
                    "[raftdb]",
                    f"max-background-jobs = {TIKV_HARNESS_RAFTDB_MAX_BACKGROUND_JOBS}",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def close(self) -> None:
        for proc in (self._tikv_proc, self._pd_proc):
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
        for handle_name in ("_tikv_stdout", "_pd_stdout"):
            handle = getattr(self, handle_name, None)
            if handle is not None:
                handle.close()
        lock_handle = getattr(self, "_lock_handle", None)
        if lock_handle is not None:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_handle.close()
            self._lock_handle = None

    def build_store_config(self, *, key_suffix: str) -> FluxonFsTransferStateStoreConfig:
        return FluxonFsTransferStateStoreConfig(
            kind=FluxonFsTransferStateStoreKind.TIKV,
            tikv=FluxonFsTransferStateStoreTiKvConfig(
                pd_endpoints=[self._pd_endpoint],
                key_prefix=f"/fluxon_fs_transfer_pytest/{key_suffix}/",
            ),
        )

    def wait_until_store_ready(self, *, store_config: FluxonFsTransferStateStoreConfig) -> None:
        deadline = time.time() + INTEGRATION_READY_TIMEOUT_SECS
        last_err = "uninitialized"
        while True:
            _require_process_running(self._pd_proc, label="pd-server", log_path=self._pd_log)
            _require_process_running(self._tikv_proc, label="tikv-server", log_path=self._tikv_log)
            try:
                _ = transfer_inspect_local_job_blocking(
                    transfer_state_store=store_config,
                    job_id="__no_such_job__",
                )
            except RuntimeError as err:
                text = str(err)
                if "transfer job snapshot missing" in text:
                    return
                last_err = text
            else:
                return
            if time.time() >= deadline:
                raise RuntimeError(
                    f"timed out waiting for TiKV store ready: err={last_err} "
                    f"pd_log={self._pd_log.read_text(encoding='utf-8', errors='replace')} "
                    f"tikv_log={self._tikv_log.read_text(encoding='utf-8', errors='replace')}"
                )
            time.sleep(0.2)


def has_fluxon_pyo3() -> bool:
    return importlib.util.find_spec("fluxon_pyo3") is not None


INTEGRATION_READY_TIMEOUT_SECS = 180.0
TRANSFER_COMPLETION_TIMEOUT_SECS = 3600.0
TRANSFER_RUNNING_TIMEOUT_SECS = 180.0
READ_RPC_RETRY_OUTAGE_SECS = 35.0
HEARTBEAT_RETRY_PAUSE_SECS = 25.0
STAGING_CLEANUP_TIMEOUT_SECS = 90.0


def _read_text_or_empty(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    for var in (
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
    ):
        env.pop(var, None)
    env["PYTHONUNBUFFERED"] = "1"
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        str(REPO_ROOT)
        if not existing_pythonpath
        else f"{REPO_ROOT}:{existing_pythonpath}"
    )
    env.setdefault("FLUXON_LOG", "info")
    env.setdefault("LOG_LEVEL", "INFO")
    return env


def _spawn_logged(
    *,
    cmd: list[str],
    workdir: Path,
    log_path: Path,
    env: dict[str, str],
) -> subprocess.Popen[str]:
    workdir.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        return subprocess.Popen(
            cmd,
            cwd=str(workdir),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=handle,
            stderr=subprocess.STDOUT,
            text=True,
        )


def _require_process_running(
    proc: subprocess.Popen[str],
    *,
    label: str,
    log_path: Path,
) -> None:
    if proc.poll() is None:
        return
    raise AssertionError(
        f"{label} exited unexpectedly with code {proc.returncode}.\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10.0)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10.0)


def _wait_for_path(
    path: Path,
    *,
    label: str,
    proc: subprocess.Popen[str],
    log_path: Path,
) -> None:
    deadline = time.time() + INTEGRATION_READY_TIMEOUT_SECS
    while time.time() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        if path.exists():
            return
        time.sleep(0.5)
    raise AssertionError(
        f"{label} did not create required path: {path}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _wait_for_log_text(
    log_path: Path,
    pattern: str,
    *,
    label: str,
    proc: subprocess.Popen[str],
) -> None:
    deadline = time.time() + INTEGRATION_READY_TIMEOUT_SECS
    while time.time() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        if pattern in _read_text_or_empty(log_path):
            return
        time.sleep(0.5)
    raise AssertionError(
        f"{label} did not report readiness marker={pattern!r}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


def _http_request(
    *,
    url: str,
    method: str = "GET",
    form: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10.0,
    follow_redirects: bool = True,
) -> tuple[int, bytes, dict[str, str]]:
    data: bytes | None = None
    request_headers = dict(headers or {})
    if form is not None:
        encoded: list[tuple[str, str]] = []
        for key, value in form.items():
            encoded.append((key, str(value)))
        data = urllib.parse.urlencode(encoded).encode("utf-8")
        request_headers.setdefault(
            "Content-Type",
            "application/x-www-form-urlencoded",
        )
    request = urllib.request.Request(
        url,
        data=data,
        headers=request_headers,
        method=method,
    )
    opener = (
        urllib.request.build_opener()
        if follow_redirects
        else urllib.request.build_opener(_NoRedirectHandler)
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            return (
                int(response.status),
                response.read(),
                dict(response.headers.items()),
            )
    except urllib.error.HTTPError as err:
        return int(err.code), err.read(), dict(err.headers.items())


def _http_json_request(
    *,
    url: str,
    method: str = "GET",
    form: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10.0,
    follow_redirects: bool = True,
) -> tuple[int, dict[str, Any]]:
    status, body, _ = _http_request(
        url=url,
        method=method,
        form=form,
        headers=headers,
        timeout=timeout,
        follow_redirects=follow_redirects,
    )
    if not body:
        return status, {}
    return status, json.loads(body.decode("utf-8"))


def _wait_for_http_status(
    *,
    url: str,
    accepted_statuses: tuple[int, ...],
    label: str,
    proc: subprocess.Popen[str],
    log_path: Path,
    headers: dict[str, str] | None = None,
) -> None:
    deadline = time.time() + INTEGRATION_READY_TIMEOUT_SECS
    last_status: int | None = None
    while time.time() < deadline:
        _require_process_running(proc, label=label, log_path=log_path)
        try:
            status, _, _ = _http_request(url=url, headers=headers, timeout=3.0)
            last_status = status
            if status in accepted_statuses:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(
        f"{label} HTTP did not reach accepted_statuses={accepted_statuses} at {url} "
        f"last_status={last_status}\n"
        f"log_path={log_path}\n"
        f"output=\n{_read_text_or_empty(log_path)}"
    )


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(path, yaml.safe_dump(value, sort_keys=False))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True))


def _copy_file_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _basic_auth_headers(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def transfer_skip_entries_json(
    skip_entries: Iterable[FluxonFsTransferSkipEntry],
) -> str:
    return json.dumps(
        [
            {
                "kind": entry.kind.value,
                "relpath": entry.relpath,
            }
            for entry in skip_entries
        ],
        separators=(",", ":"),
    )


def _etcd_runtime_dir() -> Path:
    return REPO_ROOT / "fluxon_release" / "ext_images" / "etcd"


def ensure_etcd_runtime_path() -> Path:
    runtime_dir = _etcd_runtime_dir()
    start_script = runtime_dir / "start.sh"
    if not start_script.is_file():
        raise FileNotFoundError(
            "missing etcd ext runtime. Run `python3 setup_and_pack/pack_release_ext.py --release-dir fluxon_release` first."
        )
    return start_script


class EtcdHarness:
    def __init__(self, *, tag: str):
        _ = tag
        start_script = ensure_etcd_runtime_path()
        self._temp_root = ETCD_HARNESS_WORK_ROOT
        self._lock_path = self._temp_root / ETCD_HARNESS_LOCK_NAME
        self._lock_handle = None
        self._prepare_work_root()
        self._client_port = _pick_free_port()
        self._peer_port = _pick_free_port()
        self._config = self._temp_root / "etcd_config.sh"
        self._log = self._temp_root / "etcd.log"
        self._endpoint = f"127.0.0.1:{self._client_port}"
        self._config.write_text(
            "\n".join(
                [
                    "declare -a ETCD_ARGS=(",
                    '  --data-dir "$WORKDIR/etcd-data"',
                    "  --name etcd0",
                    f'  --advertise-client-urls "http://127.0.0.1:{self._client_port}"',
                    f'  --listen-client-urls "http://127.0.0.1:{self._client_port}"',
                    f'  --listen-peer-urls "http://127.0.0.1:{self._peer_port}"',
                    f'  --initial-advertise-peer-urls "http://127.0.0.1:{self._peer_port}"',
                    f'  --initial-cluster "etcd0=http://127.0.0.1:{self._peer_port}"',
                    '  --initial-cluster-token "fluxon-fs-transfer-test"',
                    '  --initial-cluster-state "new"',
                    "  --auto-compaction-retention=1",
                    ")",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self._stdout = self._log.open("w", encoding="utf-8")
        self._proc = subprocess.Popen(
            [str(start_script), "--config", str(self._config), "--workdir", str(self._temp_root)],
            stdin=subprocess.DEVNULL,
            stdout=self._stdout,
            stderr=subprocess.STDOUT,
            text=True,
        )
        _wait_for_tcp(
            "127.0.0.1",
            self._client_port,
            label="etcd-server",
            proc=self._proc,
            log_path=self._log,
        )

    def _prepare_work_root(self) -> None:
        self._temp_root.mkdir(parents=True, exist_ok=True)
        self._lock_handle = self._lock_path.open("a+", encoding="utf-8")
        try:
            # The fixed etcd work root is a single authoritative harness instance.
            # A second concurrent test must fail immediately instead of racing with
            # directory cleanup and corrupting the local etcd state.
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as err:
            self._lock_handle.close()
            self._lock_handle = None
            raise RuntimeError(
                f"etcd harness work root is already locked: {self._temp_root}"
            ) from err
        _clear_directory_children(
            self._temp_root,
            preserved_names={ETCD_HARNESS_LOCK_NAME},
        )

    @property
    def endpoint(self) -> str:
        return self._endpoint

    def close(self) -> None:
        if hasattr(self, "_proc") and self._proc.poll() is None:
            self._proc.kill()
            self._proc.wait(timeout=10)
        handle = getattr(self, "_stdout", None)
        if handle is not None:
            handle.close()
        lock_handle = getattr(self, "_lock_handle", None)
        if lock_handle is not None:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_handle.close()
            self._lock_handle = None


class _MonitoringRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def do_POST(self) -> None:  # noqa: N802
        _ = self.rfile.read(int(self.headers.get("Content-Length", "0") or "0"))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, format: str, *args: Any) -> None:
        return


class DummyMonitoringHarness:
    def __init__(self) -> None:
        self.port = _pick_free_port()
        self._server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", self.port),
            _MonitoringRequestHandler,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="fluxon_fs_transfer_monitor",
            daemon=True,
        )
        self._thread.start()

    @property
    def prometheus_base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}/v1/prometheus"

    def close(self) -> None:
        if hasattr(self, "_server"):
            self._server.shutdown()
            self._server.server_close()
        if hasattr(self, "_thread"):
            self._thread.join(timeout=10.0)


class FluxonFsRemoteWholeHarness:
    def __init__(
        self,
        *,
        tag: str,
        work_root: Path,
        fixture: PreparedTransferFixture,
        dst_root: Path,
    ) -> None:
        self._tag = tag
        self._work_root = work_root
        self._fixture = fixture
        self._dst_root = dst_root
        self._job_result_by_id: dict[str, dict[str, Any]] = {}
        self._report_root = (
            TRANSFER_REPORT_BASE / f"{self._tag}_{int(time.time() * 1000):x}_{os.getpid():x}"
        )
        self._env = _build_subprocess_env()
        self._processes: dict[str, tuple[subprocess.Popen[str], Path]] = {}
        self._process_order: list[str] = []
        self._admin_username = "admin"
        self._admin_password = "admin-pass-123"
        self._cluster_name = f"fft-{int(time.time() * 1000):x}-{os.getpid():x}"
        self._ui_port = _pick_free_port()
        self._kv_master_port = _pick_free_port()
        self._ui_base_url = f"http://127.0.0.1:{self._ui_port}"
        self._fs_s3_base_url = f"{self._ui_base_url}/fs_s3"
        self._share_mem_root = self._work_root / "sm"
        self._share_mem_root.mkdir(parents=True, exist_ok=True)
        self._etcd: EtcdHarness | None = None
        self._tikv: TiKvHarness | None = None
        self._monitor: DummyMonitoringHarness | None = None
        self._store_config: FluxonFsTransferStateStoreConfig | None = None
        self._owner_shared_json_path: Path | None = None
        try:
            self._etcd = EtcdHarness(tag=f"{tag}_etcd")
            self._tikv = TiKvHarness(tag=f"{tag}_tikv")
            self._store_config = self._tikv.build_store_config(key_suffix=f"{tag}_whole_remote")
            self._tikv.wait_until_store_ready(store_config=self._store_config)
            self._monitor = DummyMonitoringHarness()
            self._prepare_configs()
            self._start_stack()
        except Exception:
            self.close()
            raise

    @property
    def store_config(self) -> FluxonFsTransferStateStoreConfig:
        if self._store_config is None:
            raise RuntimeError("store_config is unavailable before harness init")
        return self._store_config

    def _cluster_scoped_share_mem_dir(self) -> Path:
        return self._share_mem_root / self._cluster_name

    def _monitoring_block(self) -> dict[str, Any]:
        if self._monitor is None:
            raise RuntimeError("monitoring harness is not initialized")
        return {
            "prometheus_base_url": self._monitor.prometheus_base_url,
        }

    def _owner_kvclient_config(self) -> dict[str, Any]:
        if self._etcd is None:
            raise RuntimeError("etcd harness is not initialized")
        return {
            "instance_key": f"{self._tag}_owner",
            "contribute_to_cluster_pool_size": {
                "dram": 1024 * 1024 * 1024,
                "vram": {},
            },
            "fluxonkv_spec": {
                "etcd_addresses": [self._etcd.endpoint],
                "cluster_name": self._cluster_name,
                "share_mem_path": str(self._share_mem_root),
                "sub_cluster": "transfer_owner",
                "large_file_paths": [str(self._work_root / "large" / "owner")],
            },
            "test_spec_config": {
                "disable_observability": True,
            },
        }

    def _external_kvclient_config(self, *, instance_key: str) -> dict[str, Any]:
        return {
            "instance_key": instance_key,
            "fluxonkv_spec": {
                "cluster_name": self._cluster_name,
                "share_mem_path": str(self._share_mem_root),
            },
            "test_spec_config": {
                "disable_observability": True,
            },
        }

    def _prepare_configs(self) -> None:
        store_config = self.store_config
        if store_config.tikv is None:
            raise RuntimeError("transfer state store must be TiKV for remote whole harness")

        self._owner_workdir = self._work_root / "owner"
        self._owner_config_path = self._owner_workdir / "config.yaml"
        self._kv_master_workdir = self._work_root / "kv_master"
        self._kv_master_config_path = self._kv_master_workdir / "config.yaml"
        self._fs_master_workdir = self._work_root / "fs_master"
        self._fs_master_config_path = self._fs_master_workdir / "config.yaml"
        self._src_agent_workdir = self._work_root / "src_agent"
        self._src_agent_config_path = self._src_agent_workdir / "config.yaml"
        self._dst_agent_workdir = self._work_root / "dst_agent"
        self._dst_agent_config_path = self._dst_agent_workdir / "config.yaml"

        _write_yaml(self._owner_config_path, self._owner_kvclient_config())
        _write_yaml(
            self._kv_master_config_path,
            {
                "instance_key": f"{self._tag}_kv_master",
                "cluster_name": self._cluster_name,
                "port": self._kv_master_port,
                "etcd_endpoints": [self._etcd.endpoint],
                "log_dir": str((self._kv_master_workdir / "logs").resolve()),
                "monitoring": self._monitoring_block(),
                "test_spec_config": {
                    "disable_observability": True,
                }
            },
        )
        _write_yaml(
            self._fs_master_config_path,
            {
                "kvclient": self._external_kvclient_config(
                    instance_key=f"{self._tag}_fs_master"
                ),
                "fluxon_fs": {
                    "master": {
                        "instance_key": f"{self._tag}_fs_master",
                        "pull_interval_ms": 1000,
                    },
                    "master_panel": {
                        "listen_addr": f"127.0.0.1:{self._ui_port}",
                        "public_base_url": self._ui_base_url,
                        "auto_refresh_interval_secs": 2,
                        "access_db_path": str(
                            (self._fs_master_workdir / "access.db").resolve()
                        ),
                        "bootstrap_access_model": {
                            "users": [
                                {
                                    "username": self._admin_username,
                                    "password": self._admin_password,
                                    "can_manage_users": True,
                                }
                            ],
                            "scope_access": [],
                        },
                        "transfer_state_store": {
                            "kind": "tikv",
                            "tikv": {
                                "pd_endpoints": list(store_config.tikv.pd_endpoints),
                                "key_prefix": store_config.tikv.key_prefix,
                            },
                        },
                        "s3_gateway": {
                            "get_object_inflight_pieces": 8,
                            "kv_miss_policy": "remote_read",
                        },
                    },
                    "cache": {
                        "stale_window_ms": 1000,
                        "rules": [],
                        "exports": {
                            "src": {
                                "remote_root_dir_abs": str(self._fixture.src_root),
                                "cache_max_bytes": 1024 * 1024,
                            },
                            "dst": {
                                "remote_root_dir_abs": str(self._dst_root),
                                "cache_max_bytes": 1024 * 1024,
                            },
                        },
                    },
                },
            },
        )
        _write_yaml(
            self._src_agent_config_path,
            {
                "kvclient": self._external_kvclient_config(
                    instance_key=f"{self._tag}_src_agent"
                ),
                "fluxon_fs": {
                    "master": {
                        "instance_key": f"{self._tag}_fs_master",
                    },
                    "cache": {
                        "stale_window_ms": 1000,
                        "rules": [],
                        "exports": {
                            "src": {
                                "remote_root_dir_abs": str(self._fixture.src_root),
                                "cache_max_bytes": 1024 * 1024,
                            },
                        },
                    },
                },
            },
        )
        _write_yaml(
            self._dst_agent_config_path,
            {
                "kvclient": self._external_kvclient_config(
                    instance_key=f"{self._tag}_dst_agent"
                ),
                "fluxon_fs": {
                    "master": {
                        "instance_key": f"{self._tag}_fs_master",
                    },
                    "cache": {
                        "stale_window_ms": 1000,
                        "rules": [],
                        "exports": {
                            "dst": {
                                "remote_root_dir_abs": str(self._dst_root),
                                "cache_max_bytes": 1024 * 1024,
                            },
                        },
                    },
                },
            },
        )
        self._owner_shared_json_path = self._cluster_scoped_share_mem_dir() / "shared.json"

    def _start_logged_process(
        self,
        *,
        label: str,
        cmd: list[str],
        workdir: Path,
    ) -> tuple[subprocess.Popen[str], Path]:
        if label in self._processes:
            raise RuntimeError(f"process label already exists: {label}")
        log_path = workdir / f"{label}.log"
        proc = _spawn_logged(cmd=cmd, workdir=workdir, log_path=log_path, env=self._env)
        self._processes[label] = (proc, log_path)
        self._process_order.append(label)
        return proc, log_path

    def _replace_logged_process(
        self,
        *,
        label: str,
        cmd: list[str],
        workdir: Path,
    ) -> tuple[subprocess.Popen[str], Path]:
        if label not in self._processes:
            raise RuntimeError(f"process label is missing: {label}")
        log_path = workdir / f"{label}.log"
        proc = _spawn_logged(cmd=cmd, workdir=workdir, log_path=log_path, env=self._env)
        self._processes[label] = (proc, log_path)
        return proc, log_path

    def _start_stack(self) -> None:
        kv_master_proc, kv_master_log = self._start_logged_process(
            label="kv_master",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.runtime.start_master",
                "--config",
                str(self._kv_master_config_path),
                "--workdir",
                str(self._kv_master_workdir),
            ],
            workdir=self._kv_master_workdir,
        )
        self._kv_master_proc = kv_master_proc
        self._kv_master_log = kv_master_log
        _wait_for_log_text(
            kv_master_log,
            "KV Master started successfully",
            label="kv-master",
            proc=kv_master_proc,
        )

        owner_proc, owner_log = self._start_logged_process(
            label="owner",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.runtime.start_owner_kvclient",
                "--config",
                str(self._owner_config_path),
                "--workdir",
                str(self._owner_workdir),
            ],
            workdir=self._owner_workdir,
        )
        self._owner_proc = owner_proc
        self._owner_log = owner_log
        if self._owner_shared_json_path is None:
            raise RuntimeError("owner shared.json path is unavailable")
        _wait_for_path(
            self._owner_shared_json_path,
            label="owner-shared-json",
            proc=owner_proc,
            log_path=owner_log,
        )

        fs_master_proc, fs_master_log = self._start_logged_process(
            label="fs_master",
            cmd=[
                sys.executable,
                "-m",
                "fluxon_py.fluxon_fs.master_cli",
                "--config",
                str(self._fs_master_config_path),
                "--workdir",
                str(self._fs_master_workdir),
            ],
            workdir=self._fs_master_workdir,
        )
        self._fs_master_proc = fs_master_proc
        self._fs_master_log = fs_master_log
        _wait_for_tcp(
            "127.0.0.1",
            self._ui_port,
            label="fs-master-http",
            proc=fs_master_proc,
            log_path=fs_master_log,
        )
        _wait_for_http_status(
            url=f"{self._fs_s3_base_url}/ui/",
            accepted_statuses=(200,),
            label="fs-master-ui-home",
            proc=fs_master_proc,
            log_path=fs_master_log,
            headers=_basic_auth_headers(self._admin_username, self._admin_password),
        )

        self.start_src_agent()
        self.start_dst_agent()

    def _start_agent_process(
        self,
        *,
        label: str,
        config_path: Path,
        workdir: Path,
        replace_existing: bool,
    ) -> tuple[subprocess.Popen[str], Path]:
        cmd = [
            sys.executable,
            "-m",
            "fluxon_py.fluxon_fs.agent_cli",
            "--config",
            str(config_path),
            "--workdir",
            str(workdir),
        ]
        if replace_existing:
            proc, log_path = self._replace_logged_process(label=label, cmd=cmd, workdir=workdir)
        else:
            proc, log_path = self._start_logged_process(label=label, cmd=cmd, workdir=workdir)
        _wait_for_log_text(
            log_path,
            "fluxon_fs agent ready",
            label=label,
            proc=proc,
        )
        return proc, log_path

    def start_src_agent(self) -> None:
        replace_existing = "src_agent" in self._processes
        proc, log_path = self._start_agent_process(
            label="src_agent",
            config_path=self._src_agent_config_path,
            workdir=self._src_agent_workdir,
            replace_existing=replace_existing,
        )
        self._src_agent_proc = proc
        self._src_agent_log = log_path

    def start_dst_agent(self) -> None:
        replace_existing = "dst_agent" in self._processes
        proc, log_path = self._start_agent_process(
            label="dst_agent",
            config_path=self._dst_agent_config_path,
            workdir=self._dst_agent_workdir,
            replace_existing=replace_existing,
        )
        self._dst_agent_proc = proc
        self._dst_agent_log = log_path

    def _assert_processes_running(self) -> None:
        for label in self._process_order:
            proc, log_path = self._processes[label]
            _require_process_running(proc, label=label, log_path=log_path)

    def stop_src_agent(self) -> None:
        proc, _ = self._processes["src_agent"]
        _terminate_process(proc)

    def pause_fs_master(self) -> None:
        proc, log_path = self._processes["fs_master"]
        _require_process_running(proc, label="fs_master", log_path=log_path)
        os.kill(proc.pid, signal.SIGSTOP)

    def resume_fs_master(self) -> None:
        proc, log_path = self._processes["fs_master"]
        _require_process_running(proc, label="fs_master", log_path=log_path)
        os.kill(proc.pid, signal.SIGCONT)

    def create_transfer_job(
        self,
        *,
        desired_scan_concurrency: int,
        desired_worker_count: int,
        batch_ready_bytes: int,
        skip_entries: list[FluxonFsTransferSkipEntry],
    ) -> dict[str, Any]:
        # This is a test-only stabilization delay for the current share-group metadata
        # convergence race in the real stack. The transfer job must start only after the
        # external peers have had enough time to publish owner-generation binding.
        time.sleep(WHOLE_FLOW_STACK_STABILIZATION_SECS)
        auth_headers = _basic_auth_headers(self._admin_username, self._admin_password)
        status, payload = _http_json_request(
            url=f"{self._fs_s3_base_url}/ui/api/transfer_jobs",
            method="POST",
            form={
                "src_export": "src",
                "src_root_relpath": ".",
                "dst_export": "dst",
                "dst_root_relpath": ".",
                "desired_scan_concurrency": desired_scan_concurrency,
                "desired_worker_count": desired_worker_count,
                "batch_ready_bytes": batch_ready_bytes,
                "skip_entries_json": transfer_skip_entries_json(skip_entries),
            },
            headers=auth_headers,
            timeout=15.0,
            follow_redirects=False,
        )
        if status != 200:
            raise AssertionError(f"unexpected create transfer job status={status} payload={payload}")
        return payload["job"]

    def wait_for_transfer_running(self, *, job_id: str) -> dict[str, Any]:
        deadline = time.time() + TRANSFER_RUNNING_TIMEOUT_SECS
        last_job: dict[str, Any] | None = None
        while time.time() < deadline:
            self._assert_processes_running()
            job = transfer_inspect_local_job_status_blocking(
                transfer_state_store=self.store_config,
                job_id=job_id,
            )
            last_job = job
            if any(batch["state"] == "running" for batch in job["batches"]):
                return job
            time.sleep(0.5)
        raise AssertionError(
            f"timed out waiting for transfer running job_id={job_id} last_job={last_job}"
        )

    def wait_for_transfer_completion(self, *, job_id: str) -> dict[str, Any]:
        deadline = time.time() + TRANSFER_COMPLETION_TIMEOUT_SECS
        last_job: dict[str, Any] | None = None
        while time.time() < deadline:
            self._assert_processes_running()
            job = transfer_inspect_local_job_status_blocking(
                transfer_state_store=self.store_config,
                job_id=job_id,
            )
            last_job = job
            if (
                job["job_state"] == "completed"
                and job["open_batches"] == 0
                and all(batch["state"] == "finished" for batch in job["batches"])
                and all(row["materialized"] for row in job["collect_infos"])
            ):
                return transfer_inspect_local_job_blocking(
                    transfer_state_store=self.store_config,
                    job_id=job_id,
                )
            time.sleep(1.0)
        raise AssertionError(
            f"timed out waiting for transfer completion job_id={job_id} last_job={last_job}"
        )

    def run_transfer_job(
        self,
        *,
        desired_scan_concurrency: int,
        desired_worker_count: int,
        batch_ready_bytes: int,
        skip_entries: list[FluxonFsTransferSkipEntry],
    ) -> dict[str, Any]:
        created_job = self.create_transfer_job(
            desired_scan_concurrency=desired_scan_concurrency,
            desired_worker_count=desired_worker_count,
            batch_ready_bytes=batch_ready_bytes,
            skip_entries=skip_entries,
        )
        job = self.wait_for_transfer_completion(job_id=str(created_job["job_id"]))
        result = {
            "created_job": created_job,
            "job": job,
        }
        self._job_result_by_id[str(created_job["job_id"])] = result
        return result

    def _export_report_artifacts(self) -> None:
        self._report_root.mkdir(parents=True, exist_ok=True)
        _write_json(
            self._report_root / "metadata.json",
            {
                "tag": self._tag,
                "report_root": str(self._report_root),
                "work_root": str(self._work_root),
                "dst_root": str(self._dst_root),
                "cluster_name": self._cluster_name,
                "ui_base_url": self._ui_base_url,
            },
        )
        if FIXTURE_MANIFEST_PATH.exists():
            _write_json(
                self._report_root / "fixture_manifest.json",
                json.loads(FIXTURE_MANIFEST_PATH.read_text(encoding="utf-8")),
            )
        for job_id, result in sorted(self._job_result_by_id.items()):
            _write_json(self._report_root / "jobs" / job_id / "result.json", result)
        for label, (_, log_path) in sorted(self._processes.items()):
            _copy_file_if_exists(log_path, self._report_root / "logs" / f"{label}.log")
        if hasattr(self, "_owner_config_path"):
            _copy_file_if_exists(
                self._owner_config_path,
                self._report_root / "configs" / "owner_config.yaml",
            )
        if hasattr(self, "_kv_master_config_path"):
            _copy_file_if_exists(
                self._kv_master_config_path,
                self._report_root / "configs" / "kv_master_config.yaml",
            )
        if hasattr(self, "_fs_master_config_path"):
            _copy_file_if_exists(
                self._fs_master_config_path,
                self._report_root / "configs" / "fs_master_config.yaml",
            )
        if hasattr(self, "_src_agent_config_path"):
            _copy_file_if_exists(
                self._src_agent_config_path,
                self._report_root / "configs" / "src_agent_config.yaml",
            )
        if hasattr(self, "_dst_agent_config_path"):
            _copy_file_if_exists(
                self._dst_agent_config_path,
                self._report_root / "configs" / "dst_agent_config.yaml",
            )
        print(
            f"[fluxon_fs_transfer_report] exported report_root={self._report_root}",
            flush=True,
        )

    def close(self) -> None:
        for label in reversed(self._process_order):
            proc, _ = self._processes[label]
            _terminate_process(proc)
        self._export_report_artifacts()
        self._processes.clear()
        self._process_order.clear()
        if self._monitor is not None:
            self._monitor.close()
            self._monitor = None
        if self._tikv is not None:
            self._tikv.close()
            self._tikv = None
        if self._etcd is not None:
            self._etcd.close()
            self._etcd = None
