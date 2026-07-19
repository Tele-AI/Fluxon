#!/usr/bin/env python3
"""Compare original decord video loading with FluxonFS VideoReader.

This is an integration benchmark entrypoint for the dataloader boundary. It
replays decode cases from CSV/log/parquet metadata and runs the same frame
selection through:

* original: decord.VideoReader(video_path).get_batch(...).asnumpy()
* fluxon: FluxonFsPatcher.open_video_reader_pool(...).read_frames_numpy_with_stats(...)

The Fluxon path uses export_name + relpath and does not materialize a local
file path for FFmpeg. It requires fluxon_pyo3 built with the
fluxon_fs_video_ffmpeg feature.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import os
import random
import re
import stat
import statistics
import sys
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


DEFAULT_MANIFEST = ""

TIMEOUT_RE = re.compile(
    r"Timeout loading data at index (?P<idx>\d+):.*?"
    r"'(?P<path>/[^']+)'\s*,\s*"
    r"(?P<height>\d+)\s*,\s*"
    r"(?P<width>\d+)\s*,\s*"
    r"(?P<start>\d+)\s*,\s*"
    r"(?P<end>\d+)\s*,\s*"
    r"(?P<num>\d+)",
)


class RawDefaultsHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


@dataclass(frozen=True)
class DecodeCase:
    source: str
    idx: int
    video_path: str
    height: int
    width: int
    start_idx: int
    end_idx: int
    num_frames: int

    @property
    def key(self) -> tuple[Any, ...]:
        return (
            self.video_path,
            self.height,
            self.width,
            self.start_idx,
            self.end_idx,
            self.num_frames,
        )


@dataclass(frozen=True)
class BenchmarkJob:
    sample_id: str
    round_id: int
    case_position: int
    backend: str
    case: DecodeCase


class OriginalDecordBackend:
    name = "original"

    def __init__(self, *, num_threads: int, fingerprint_bytes: int) -> None:
        from decord import VideoReader, cpu  # type: ignore

        self._video_reader_cls = VideoReader
        self._cpu = cpu
        self._num_threads = num_threads
        self._fingerprint_bytes = fingerprint_bytes

    def decode(self, case: DecodeCase) -> dict[str, Any]:
        indices = frame_indices(case)

        t0 = time.perf_counter()
        reader = self._video_reader_cls(
            case.video_path,
            width=case.width,
            height=case.height,
            num_threads=self._num_threads,
            ctx=self._cpu(0),
        )
        t1 = time.perf_counter()
        batch = reader.get_batch(indices)
        t2 = time.perf_counter()
        array = batch.asnumpy()
        t3 = time.perf_counter()

        row = {
            "status": "ok",
            "open_s": t1 - t0,
            "decode_s": t2 - t1,
            "materialize_s": t3 - t2,
            "elapsed_s": t3 - t0,
        }
        row.update(array_result_fields(array, self._fingerprint_bytes))
        return row

    def decode_batch(self, cases: list[DecodeCase]) -> list[dict[str, Any]]:
        return [self.decode(case) for case in cases]


class FluxonVideoBackend:
    name = "fluxon"

    def __init__(
        self,
        *,
        runtime: "FluxonBenchmarkRuntime",
        num_threads: int,
        fingerprint_bytes: int,
    ) -> None:
        self._runtime = runtime
        self._num_threads = num_threads
        self._fingerprint_bytes = fingerprint_bytes

    def decode(self, case: DecodeCase) -> dict[str, Any]:
        return self.decode_batch([case])[0]

    def decode_batch(self, cases: list[DecodeCase]) -> list[dict[str, Any]]:
        from fluxon_py.fluxon_fs.video import FluxonFsVideoReadRequest

        requests: list[FluxonFsVideoReadRequest] = []
        relpaths: list[str] = []
        frame_counts: list[int] = []
        for case in cases:
            indices = tuple(frame_indices(case))
            relpath = self._runtime.relpath_for_case(case)
            requests.append(
                FluxonFsVideoReadRequest(
                    export_name=self._runtime.export_name,
                    relpath=relpath,
                    height=case.height,
                    width=case.width,
                    num_threads=self._num_threads,
                    indices=indices,
                )
            )
            relpaths.append(relpath)
            frame_counts.append(len(indices))

        t0 = time.perf_counter()
        results = self._runtime.video_reader_pool.read_many_numpy_with_stats(requests)
        t1 = time.perf_counter()
        batch_elapsed_s = t1 - t0
        weights = [max(1, int(getattr(result.array, "nbytes", 0))) for result in results]
        elapsed_shares = split_float_by_weights(batch_elapsed_s, weights)

        rows: list[dict[str, Any]] = []
        for case, relpath, frame_count, result, elapsed_s in zip(
            cases,
            relpaths,
            frame_counts,
            results,
            elapsed_shares,
        ):
            row = {
                "status": "ok",
                "fluxon_relpath": relpath,
                "open_s": 0.0,
                "decode_s": elapsed_s,
                "materialize_s": 0.0,
                "elapsed_s": elapsed_s,
                "batch_wall_s": batch_elapsed_s,
                "decode_batch_input_size": len(cases),
                "decode_batch_input_frames": sum(frame_counts),
                "decode_batch_sample_frames": frame_count,
                "fluxon_video_pool_reader_cache_hit": int(result.reader_cache_hit),
            }
            row.update(array_result_fields(result.array, self._fingerprint_bytes))
            row.update({f"fluxon_video_{key}": value for key, value in result.reader_stats.items()})
            row.update({f"fluxon_video_pool_{key}": value for key, value in result.pool_stats.items()})
            row.update({f"fluxon_video_batch_{key}": value for key, value in result.batch_stats.items()})
            rows.append(row)
        return rows


class FluxonBenchmarkRuntime:
    """Owns the FluxonFS client used by the benchmark."""

    def __init__(
        self,
        *,
        patcher: Any,
        stores: list[Any],
        video_reader_pool: Any,
        export_name: str,
        remote_root_abs: Path,
        close_timeout_s: float,
    ) -> None:
        self.patcher = patcher
        self._stores = stores
        self.video_reader_pool = video_reader_pool
        self.export_name = export_name
        self.remote_root_abs = remote_root_abs
        self.close_timeout_s = close_timeout_s

    @classmethod
    def open(cls, args: argparse.Namespace) -> "FluxonBenchmarkRuntime":
        import yaml
        from fluxon_py.config import FluxonKvClientConfig
        from fluxon_py.fluxon_fs.patcher import FluxonFsPatcher
        from fluxon_py.kvclient import new_store

        kv_cfg = load_yaml_mapping(args.fluxon_kv_config, "fluxon kv config")
        request_password = None
        if args.fluxon_request_username:
            request_password = load_secret_file(
                args.fluxon_request_password_file,
                "fluxon request password file",
            )
        suffix = f"{os.getpid()}_{int(time.time() * 1000)}"
        client_key = args.fluxon_client_instance_key or f"dataloader_perf_client_{suffix}"
        agent_node_id = resolve_fluxon_agent_node_id(args)

        client_store = new_fluxon_store(
            new_store_fn=new_store,
            config_cls=FluxonKvClientConfig,
            base_config=kv_cfg,
            instance_key=client_key,
        )

        remote_root_abs = require_abs_existing_dir(
            args.fluxon_remote_root,
            "fluxon remote root",
        )
        cache_yaml = yaml.safe_dump(
            {
                "stale_window_ms": int(args.fluxon_stale_window_ms),
                "rules": [],
                "exports": {
                    args.fluxon_export_name: {
                        "remote_root_dir_abs": str(remote_root_abs),
                        "nodes": [str(agent_node_id)],
                        "cache_max_bytes": int(args.fluxon_cache_max_bytes),
                        "metadata_cache_ttl_ms": int(args.fluxon_metadata_cache_ttl_ms),
                    }
                },
            },
            sort_keys=False,
        )

        patcher = FluxonFsPatcher(client_store)
        patcher.set_cache_config_yaml(cache_yaml)
        patcher.wait_cache_config_loaded()
        if args.fluxon_request_username:
            assert request_password is not None
            patcher.set_request_identity(args.fluxon_request_username, request_password)
        video_reader_pool = patcher.open_video_reader_pool(
            max_readers=int(args.fluxon_reader_cache_size),
        )

        return cls(
            patcher=patcher,
            stores=[client_store],
            video_reader_pool=video_reader_pool,
            export_name=str(args.fluxon_export_name),
            remote_root_abs=remote_root_abs,
            close_timeout_s=float(args.fluxon_close_timeout_s),
        )

    def relpath_for_case(self, case: DecodeCase) -> str:
        video_path = Path(case.video_path)
        if not video_path.is_absolute():
            raise ValueError(f"Fluxon backend requires absolute video_path, got {case.video_path!r}")
        video_abs = video_path.resolve(strict=False)
        try:
            rel = video_abs.relative_to(self.remote_root_abs)
        except ValueError as exc:
            raise ValueError(
                f"video_path is outside fluxon remote root: path={video_abs} root={self.remote_root_abs}"
            ) from exc
        rel_s = rel.as_posix()
        if not rel_s or rel_s == ".":
            raise ValueError(f"invalid empty Fluxon relpath for {video_abs}")
        return rel_s

    def close(self) -> None:
        self.video_reader_pool.close()
        for store in self._stores:
            close_store_with_timeout(store, timeout_s=self.close_timeout_s)

    def __enter__(self) -> "FluxonBenchmarkRuntime":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


def unwrap_result(result: Any, ctx: str) -> Any:
    if result.is_ok():
        return result.unwrap()
    raise RuntimeError(f"{ctx} failed: {result.unwrap_error()}")


def resolve_fluxon_agent_node_id(args: argparse.Namespace) -> str:
    agent_instance_key = str(args.fluxon_agent_instance_key).strip()
    agent_node_id = str(args.fluxon_agent_node_id).strip()
    if agent_instance_key and agent_node_id and agent_instance_key != agent_node_id:
        raise ValueError(
            "--fluxon-agent-instance-key and --fluxon-agent-node-id must match when both are set"
        )
    node_id = agent_instance_key or agent_node_id
    if not node_id:
        raise ValueError(
            "--fluxon-agent-instance-key is required for the Fluxon backend; "
            "start fluxon_py.runtime.start_fs_agent with the same instance_key first"
        )
    return node_id


def close_store_with_timeout(store: Any, *, timeout_s: float) -> None:
    result_holder: dict[str, Any] = {}

    def close_worker() -> None:
        try:
            close_res = store.close()
            if close_res.is_ok():
                result_holder["ok"] = close_res.unwrap()
            else:
                result_holder["error"] = close_res.unwrap_error()
        except BaseException as exc:
            result_holder["exception"] = exc
            result_holder["traceback"] = traceback.format_exc(limit=6)

    worker = threading.Thread(target=close_worker, daemon=True)
    worker.start()
    worker.join(max(0.0, float(timeout_s)))
    if worker.is_alive():
        print(
            f"[dataloader_perf][WARNING] store.close exceeded {timeout_s:.3f}s; continuing",
            flush=True,
        )
        return
    if "error" in result_holder:
        print(f"[dataloader_perf][WARNING] store.close failed: {result_holder['error']}", flush=True)
    if "exception" in result_holder:
        print(
            "[dataloader_perf][WARNING] store.close raised: "
            f"{type(result_holder['exception']).__name__}: {result_holder['exception']}",
            flush=True,
        )


def new_fluxon_store(
    *,
    new_store_fn: Any,
    config_cls: Any,
    base_config: Mapping[str, Any],
    instance_key: str,
) -> Any:
    cfg = copy.deepcopy(dict(base_config))
    cfg["instance_key"] = instance_key
    result = new_store_fn(config_cls(cfg))
    return unwrap_result(result, f"new_store({instance_key})")


def frame_indices(case: DecodeCase) -> list[int]:
    if case.num_frames <= 0:
        raise ValueError(f"num_frames must be positive, got {case.num_frames}")
    if case.num_frames == 1:
        return [int(case.start_idx)]
    span = case.end_idx - case.start_idx
    return [
        int(case.start_idx + (span * i) / (case.num_frames - 1))
        for i in range(case.num_frames)
    ]


def split_float_by_weights(value: float, weights: list[int]) -> list[float]:
    if not weights:
        return []
    positive_weights = [max(0, int(weight)) for weight in weights]
    total_weight = sum(positive_weights)
    if total_weight <= 0:
        return [value / len(weights) for _ in weights]
    shares = [(value * weight) / total_weight for weight in positive_weights]
    if shares:
        shares[-1] += value - sum(shares)
    return shares


def array_result_fields(array: Any, fingerprint_bytes: int) -> dict[str, Any]:
    shape = tuple(int(x) for x in getattr(array, "shape", ()))
    dtype = str(getattr(array, "dtype", "unknown"))
    nbytes = int(getattr(array, "nbytes", 0))
    out = {
        "shape": "x".join(str(x) for x in shape),
        "dtype": dtype,
        "nbytes": nbytes,
    }
    if fingerprint_bytes > 0:
        out["fingerprint"] = array_fingerprint(array, fingerprint_bytes)
    return out


def array_fingerprint(array: Any, fingerprint_bytes: int) -> str:
    if fingerprint_bytes <= 0:
        return ""
    data = array.tobytes(order="C")
    sample_n = min(len(data), fingerprint_bytes)
    h = hashlib.blake2b(digest_size=16)
    h.update(str(getattr(array, "shape", "")).encode("utf-8"))
    h.update(str(getattr(array, "dtype", "")).encode("utf-8"))
    h.update(data[:sample_n])
    if len(data) > sample_n:
        h.update(data[-sample_n:])
    return h.hexdigest()


def parse_case_csv(path: str) -> list[DecodeCase]:
    cases: list[DecodeCase] = []
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(path)
    with csv_path.open(newline="") as f:
        for row in csv.DictReader(f):
            video_path = row.get("video_path") or row.get("path")
            if not video_path:
                continue
            cases.append(
                DecodeCase(
                    source=row.get("source") or f"csv:{path}",
                    idx=int(row.get("idx") or len(cases)),
                    video_path=video_path,
                    height=int(row["height"]),
                    width=int(row["width"]),
                    start_idx=int(row["start_idx"]),
                    end_idx=int(row["end_idx"]),
                    num_frames=int(row["num_frames"]),
                )
            )
    return cases


def parse_timeout_log(path: str) -> list[DecodeCase]:
    cases: list[DecodeCase] = []
    log_path = Path(path)
    if not log_path.exists():
        raise FileNotFoundError(path)
    for line in log_path.read_text(errors="replace").splitlines():
        match = TIMEOUT_RE.search(line)
        if match is None:
            continue
        cases.append(
            DecodeCase(
                source=f"log:{path}",
                idx=int(match.group("idx")),
                video_path=match.group("path"),
                height=int(match.group("height")),
                width=int(match.group("width")),
                start_idx=int(match.group("start")),
                end_idx=int(match.group("end")),
                num_frames=int(match.group("num")),
            )
        )
    return cases


def read_manifest(path: str) -> list[str]:
    manifest = Path(path)
    if not manifest.exists():
        return []
    return [
        line.strip()
        for line in manifest.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def case_from_parquet_row(dataset: Any, idx: int, source: str) -> DecodeCase:
    row = dataset.take([idx]).to_pydict()
    video_path = row["video_path"][0]
    height = int(row["height"][0])
    width = int(row["width"][0])
    caption_start = int(row["caption_start"][0])
    caption_end = int(row["caption_end"][0])
    real_num_frames = int(row["real_num_frames"][0])
    dst_num_frames = int(row["dst_num_frames"][0])

    span = min(real_num_frames, caption_end - caption_start)
    max_start = caption_end - span
    if max_start <= caption_start:
        window_start = caption_start
    else:
        window_start = random.randint(caption_start, max_start)
    window_end = window_start + span - 1

    return DecodeCase(
        source=source,
        idx=idx,
        video_path=video_path,
        height=height,
        width=width,
        start_idx=window_start,
        end_idx=window_end,
        num_frames=dst_num_frames,
    )


def sample_parquet_cases(
    parquet_paths: list[str],
    *,
    count: int,
    seed: int,
) -> list[DecodeCase]:
    if count <= 0:
        return []
    if not parquet_paths:
        raise FileNotFoundError("No parquet paths provided for random sampling.")

    import pyarrow.dataset as ds  # type: ignore

    random.seed(seed)
    dataset = ds.dataset(parquet_paths, format="parquet")
    total_rows = dataset.count_rows()
    indices = [random.randrange(total_rows) for _ in range(count)]
    return [case_from_parquet_row(dataset, idx, "parquet-random") for idx in indices]


def build_cases(args: argparse.Namespace) -> list[DecodeCase]:
    cases: list[DecodeCase] = []
    for csv_path in args.case_csv:
        parsed = parse_case_csv(csv_path)
        print(f"Loaded {len(parsed)} decode cases from {csv_path}", flush=True)
        cases.extend(parsed)
    for log_path in args.case_log:
        parsed = parse_timeout_log(log_path)
        print(f"Loaded {len(parsed)} decode cases from {log_path}", flush=True)
        cases.extend(parsed)

    parquet_paths: list[str] = list(args.parquet)
    if not parquet_paths and args.manifest:
        parquet_paths = read_manifest(args.manifest)
    if args.random_cases > 0:
        parsed = sample_parquet_cases(
            parquet_paths,
            count=args.random_cases,
            seed=args.seed,
        )
        print(f"Sampled {len(parsed)} decode cases from parquet metadata", flush=True)
        cases.extend(parsed)

    if args.dedupe:
        unique: dict[tuple[Any, ...], DecodeCase] = {}
        for case in cases:
            unique.setdefault(case.key, case)
        cases = list(unique.values())

    if args.shuffle:
        random.Random(args.seed).shuffle(cases)
    if args.limit > 0:
        cases = cases[: args.limit]

    validate_cases(cases)
    return cases


def validate_cases(cases: Iterable[DecodeCase]) -> None:
    for case in cases:
        if case.height <= 0 or case.width <= 0:
            raise ValueError(f"case idx={case.idx} has invalid size {case.height}x{case.width}")
        if case.num_frames <= 0:
            raise ValueError(f"case idx={case.idx} has invalid num_frames={case.num_frames}")
        if not case.video_path:
            raise ValueError(f"case idx={case.idx} has empty video_path")


def selected_backends(args: argparse.Namespace) -> list[str]:
    if args.backend == "both":
        return ["original", "fluxon"]
    return [args.backend]


def build_jobs(
    cases: list[DecodeCase],
    *,
    backends: list[str],
    rounds: int,
    alternate_backend_order: bool,
) -> list[BenchmarkJob]:
    jobs: list[BenchmarkJob] = []
    for round_id in range(1, rounds + 1):
        for case_pos, case in enumerate(cases, start=1):
            ordered = list(backends)
            if alternate_backend_order and len(ordered) == 2 and (round_id + case_pos) % 2 == 1:
                ordered.reverse()
            sample_id = f"r{round_id:04d}_c{case_pos:06d}"
            for backend in ordered:
                jobs.append(
                    BenchmarkJob(
                        sample_id=sample_id,
                        round_id=round_id,
                        case_position=case_pos,
                        backend=backend,
                        case=case,
                    )
                )
    return jobs


def build_job_row(job: BenchmarkJob, started: str) -> dict[str, Any]:
    return {
        "sample_id": job.sample_id,
        "round_id": job.round_id,
        "case_position": job.case_position,
        "backend": job.backend,
        "started": started,
        "source": job.case.source,
        "idx": job.case.idx,
        "video_path": job.case.video_path,
        "height": job.case.height,
        "width": job.case.width,
        "start_idx": job.case.start_idx,
        "end_idx": job.case.end_idx,
        "num_frames": job.case.num_frames,
    }


def run_job(job: BenchmarkJob, backend: Any) -> dict[str, Any]:
    started = time.strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    row = build_job_row(job, started)
    try:
        row.update(backend.decode(job.case))
    except BaseException as exc:
        row.update(
            {
                "status": "error",
                "elapsed_s": time.perf_counter() - t0,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(limit=8),
            }
        )
    return row


def run_backend_job_batch(jobs: list[BenchmarkJob], backend: Any) -> list[dict[str, Any]]:
    started = time.strftime("%Y-%m-%d %H:%M:%S")
    rows = [build_job_row(job, started) for job in jobs]
    t0 = time.perf_counter()
    try:
        result_rows = backend.decode_batch([job.case for job in jobs])
        if len(result_rows) != len(rows):
            raise RuntimeError(
                "backend returned unexpected batch result count: "
                f"expected={len(rows)} actual={len(result_rows)}"
            )
        for row, result in zip(rows, result_rows):
            row.update(result)
    except BaseException as exc:
        elapsed_s = time.perf_counter() - t0
        elapsed_shares = split_float_by_weights(elapsed_s, [1 for _ in rows])
        for row, elapsed_share in zip(rows, elapsed_shares):
            row.update(
                {
                    "status": "error",
                    "elapsed_s": elapsed_share,
                    "batch_wall_s": elapsed_s,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(limit=8),
                }
            )
    return rows


def run_job_window(jobs: list[BenchmarkJob], backends: Mapping[str, Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[BenchmarkJob]] = {}
    for job in jobs:
        grouped.setdefault(job.backend, []).append(job)

    rows: list[dict[str, Any]] = []
    for backend_name, backend_jobs in grouped.items():
        rows.extend(run_backend_job_batch(backend_jobs, backends[backend_name]))
    return rows


def build_job_windows(jobs: list[BenchmarkJob], decode_batch_size: int) -> list[list[BenchmarkJob]]:
    if decode_batch_size <= 1:
        return [[job] for job in jobs]
    return [
        jobs[start : start + decode_batch_size]
        for start in range(0, len(jobs), decode_batch_size)
    ]


def run_jobs(
    jobs: list[BenchmarkJob],
    *,
    backends: Mapping[str, Any],
    workers: int,
    prefetch_factor: int,
    decode_batch_size: int,
    label: str,
) -> tuple[list[dict[str, Any]], float]:
    rows: list[dict[str, Any]] = []
    started = time.perf_counter()
    if not jobs:
        return rows, 0.0

    job_windows = build_job_windows(jobs, decode_batch_size)
    print(
        f"{label}: jobs={len(jobs)} windows={len(job_windows)} workers={workers} "
        f"prefetch_factor={prefetch_factor} decode_batch_size={decode_batch_size}",
        flush=True,
    )
    if workers <= 1:
        for window in job_windows:
            for row in run_job_window(window, backends):
                rows.append(row)
                print_progress(label, len(rows), len(jobs), row)
        return rows, time.perf_counter() - started

    max_inflight = max(workers, workers * prefetch_factor)
    window_iter = iter(job_windows)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: dict[Any, list[BenchmarkJob]] = {}

        def submit_next() -> bool:
            try:
                window = next(window_iter)
            except StopIteration:
                return False
            pending[executor.submit(run_job_window, window, backends)] = window
            return True

        for _ in range(min(len(job_windows), max_inflight)):
            submit_next()

        while pending:
            completed, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            for future in completed:
                pending.pop(future)
                for row in future.result():
                    rows.append(row)
                    print_progress(label, len(rows), len(jobs), row)
                submit_next()

    rows.sort(key=lambda r: (int(r["round_id"]), int(r["case_position"]), str(r["backend"])))
    return rows, time.perf_counter() - started


def print_progress(label: str, done: int, total: int, row: Mapping[str, Any]) -> None:
    print(
        f"[{label} {done}/{total}] "
        f"backend={row.get('backend')} status={row.get('status')} "
        f"elapsed={float(row.get('elapsed_s', 0.0)):.3f}s "
        f"idx={row.get('idx')} path={row.get('video_path')}",
        flush=True,
    )


def summarize(rows: list[dict[str, Any]], *, wall_s: float) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "wall_s": wall_s,
        "total_rows": len(rows),
        "backends": {},
    }

    for backend in sorted({str(r.get("backend")) for r in rows}):
        backend_rows = [r for r in rows if r.get("backend") == backend]
        ok_rows = [r for r in backend_rows if r.get("status") == "ok"]
        elapsed = [float(r.get("elapsed_s", 0.0)) for r in ok_rows]
        frames = sum(int(r.get("num_frames", 0)) for r in ok_rows)
        backend_summary = {
            "rows": len(backend_rows),
            "ok": len(ok_rows),
            "error": len(backend_rows) - len(ok_rows),
            "frames": frames,
        }
        if elapsed:
            elapsed_sum = sum(elapsed)
            backend_summary.update(
                {
                    "elapsed_sum_s": elapsed_sum,
                    "decode_rows_per_s": len(ok_rows) / elapsed_sum if elapsed_sum > 0 else 0.0,
                    "elapsed_min_s": min(elapsed),
                    "elapsed_mean_s": statistics.fmean(elapsed),
                    "elapsed_p50_s": percentile(elapsed, 0.50),
                    "elapsed_p95_s": percentile(elapsed, 0.95),
                    "elapsed_p99_s": percentile(elapsed, 0.99),
                    "elapsed_max_s": max(elapsed),
                    "decode_frames_per_s": frames / elapsed_sum if elapsed_sum > 0 else 0.0,
                }
            )
        summary["backends"][backend] = backend_summary

    if {"original", "fluxon"}.issubset(summary["backends"].keys()):
        original_elapsed = [
            float(r["elapsed_s"])
            for r in rows
            if r.get("backend") == "original" and r.get("status") == "ok"
        ]
        fluxon_elapsed = [
            float(r["elapsed_s"])
            for r in rows
            if r.get("backend") == "fluxon" and r.get("status") == "ok"
        ]
        if original_elapsed and fluxon_elapsed:
            original_p50 = percentile(original_elapsed, 0.50)
            fluxon_p50 = percentile(fluxon_elapsed, 0.50)
            summary["fluxon_vs_original"] = {
                "p50_speedup": original_p50 / fluxon_p50 if fluxon_p50 > 0 else 0.0,
                "original_p50_s": original_p50,
                "fluxon_p50_s": fluxon_p50,
            }
    return summary


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def print_summary(summary: Mapping[str, Any]) -> None:
    print("Summary:", flush=True)
    print(f"  wall_s: {float(summary.get('wall_s', 0.0)):.3f}", flush=True)
    backends = summary.get("backends", {})
    if isinstance(backends, Mapping):
        for backend, data in backends.items():
            if not isinstance(data, Mapping):
                continue
            print(
                f"  {backend}: rows={data.get('rows')} ok={data.get('ok')} error={data.get('error')}",
                flush=True,
            )
            if "elapsed_p50_s" in data:
                print(
                    "    elapsed_s: "
                    f"mean={float(data['elapsed_mean_s']):.3f}, "
                    f"p50={float(data['elapsed_p50_s']):.3f}, "
                    f"p95={float(data['elapsed_p95_s']):.3f}, "
                    f"max={float(data['elapsed_max_s']):.3f}, "
                    f"qps={float(data.get('decode_rows_per_s', 0.0)):.2f}, "
                    f"frames/s={float(data['decode_frames_per_s']):.2f}",
                    flush=True,
                )
    speed = summary.get("fluxon_vs_original")
    if isinstance(speed, Mapping):
        print(
            "  fluxon_vs_original: "
            f"p50_speedup={float(speed.get('p50_speedup', 0.0)):.3f}x "
            f"(original={float(speed.get('original_p50_s', 0.0)):.3f}s, "
            f"fluxon={float(speed.get('fluxon_p50_s', 0.0)):.3f}s)",
            flush=True,
        )


def write_rows_csv(path: str, rows: list[dict[str, Any]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: str, payload: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_yaml_mapping(path: str, name: str) -> dict[str, Any]:
    import yaml

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    obj = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"{name} must be a YAML mapping")
    return obj


def require_abs_existing_dir(path: str, name: str) -> Path:
    p = Path(path)
    if not p.is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    resolved = p.resolve()
    if not resolved.is_dir():
        raise ValueError(f"{name} must exist and be a directory: {resolved}")
    return resolved


def load_secret_file(path: str, name: str) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        detail = os.strerror(exc.errno) if exc.errno is not None else type(exc).__name__
        raise ValueError(f"failed to open {name}: {detail}") from None

    with os.fdopen(fd, "r", encoding="utf-8") as secret_file:
        file_stat = os.fstat(secret_file.fileno())
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError(f"{name} must be a regular file")
        if file_stat.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise ValueError(f"{name} must not grant group or world permissions")
        value = secret_file.read()

    if value.endswith("\r\n"):
        value = value[:-2]
    elif value.endswith("\n"):
        value = value[:-1]
    if not value or "\n" in value or "\r" in value:
        raise ValueError(f"{name} must contain exactly one non-empty line")
    return value


def validate_args(args: argparse.Namespace) -> None:
    if args.rounds <= 0:
        raise ValueError("--rounds must be positive")
    if args.warmup_rounds < 0:
        raise ValueError("--warmup-rounds must be non-negative")
    if args.workers <= 0:
        raise ValueError("--workers must be positive")
    if args.prefetch_factor <= 0:
        raise ValueError("--prefetch-factor must be positive")
    if args.decode_batch_size <= 0:
        raise ValueError("--decode-batch-size must be positive")
    if args.num_threads <= 0:
        raise ValueError("--num-threads must be positive")
    if args.fingerprint_bytes < 0:
        raise ValueError("--fingerprint-bytes must be non-negative")
    if args.backend in ("fluxon", "both"):
        if args.fluxon_reader_cache_size <= 0:
            raise ValueError("--fluxon-reader-cache-size must be positive")
        if not args.fluxon_kv_config:
            raise ValueError("--fluxon-kv-config is required for the Fluxon backend")
        if not args.fluxon_remote_root:
            raise ValueError("--fluxon-remote-root is required for the Fluxon backend")
        if bool(args.fluxon_request_username) != bool(args.fluxon_request_password_file):
            raise ValueError(
                "--fluxon-request-username and --fluxon-request-password-file must be provided together"
            )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=RawDefaultsHelpFormatter,
    )

    input_group = parser.add_argument_group("decode cases")
    input_group.add_argument("--case-csv", action="append", default=[])
    input_group.add_argument("--case-log", action="append", default=[])
    input_group.add_argument("--manifest", default=DEFAULT_MANIFEST)
    input_group.add_argument("--parquet", action="append", default=[])
    input_group.add_argument("--random-cases", type=int, default=0)
    input_group.add_argument("--limit", type=int, default=0)
    input_group.add_argument("--shuffle", action="store_true")
    input_group.add_argument("--no-dedupe", dest="dedupe", action="store_false")
    input_group.set_defaults(dedupe=True)

    bench_group = parser.add_argument_group("benchmark")
    bench_group.add_argument("--backend", choices=["original", "fluxon", "both"], default="both")
    bench_group.add_argument("--rounds", type=int, default=1)
    bench_group.add_argument("--warmup-rounds", type=int, default=0)
    bench_group.add_argument("--workers", type=int, default=1)
    bench_group.add_argument("--prefetch-factor", type=int, default=2)
    bench_group.add_argument("--decode-batch-size", type=int, default=1)
    bench_group.add_argument("--num-threads", type=int, default=8)
    bench_group.add_argument("--seed", type=int, default=1234)
    bench_group.add_argument("--fingerprint-bytes", type=int, default=0)
    bench_group.add_argument("--no-alternate-backend-order", dest="alternate_backend_order", action="store_false")
    bench_group.add_argument("--allow-errors", action="store_true")
    bench_group.set_defaults(alternate_backend_order=True)

    fluxon_group = parser.add_argument_group("fluxon backend")
    fluxon_group.add_argument("--fluxon-kv-config", default="")
    fluxon_group.add_argument("--fluxon-remote-root", default="")
    fluxon_group.add_argument("--fluxon-export-name", default="dataloader-videos")
    fluxon_group.add_argument(
        "--fluxon-agent-instance-key",
        default="",
        help="External fluxon_py.runtime.start_fs_agent instance_key used as the static export node.",
    )
    fluxon_group.add_argument("--fluxon-client-instance-key", default="")
    fluxon_group.add_argument(
        "--fluxon-agent-node-id",
        default="",
        help="Optional explicit node id; must match --fluxon-agent-instance-key when both are set.",
    )
    fluxon_group.add_argument("--fluxon-cache-max-bytes", type=int, default=1 << 40)
    fluxon_group.add_argument("--fluxon-reader-cache-size", type=int, default=32)
    fluxon_group.add_argument("--fluxon-metadata-cache-ttl-ms", type=int, default=1000)
    fluxon_group.add_argument("--fluxon-stale-window-ms", type=int, default=1000)
    fluxon_group.add_argument("--fluxon-close-timeout-s", type=float, default=10.0)
    fluxon_group.add_argument(
        "--fluxon-request-username",
        default="",
        help="FluxonFS username used to sign file RPC tokens.",
    )
    fluxon_group.add_argument(
        "--fluxon-request-password-file",
        default="",
        help="Owner-only regular file containing the FluxonFS request password.",
    )

    output_group = parser.add_argument_group("output")
    output_group.add_argument(
        "--output-csv",
        default="runs/dualpointer/dataloader_video_benchmark/results.csv",
    )
    output_group.add_argument(
        "--output-json",
        default="runs/dualpointer/dataloader_video_benchmark/summary.json",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    validate_args(args)

    cases = build_cases(args)
    if not cases:
        raise SystemExit(
            "No decode cases found. Pass --case-csv, --case-log, or --random-cases with parquet metadata."
        )

    backends_to_run = selected_backends(args)
    print(
        "Dataloader decode benchmark: "
        f"cases={len(cases)} backend={args.backend} rounds={args.rounds} "
        f"warmup_rounds={args.warmup_rounds} workers={args.workers} "
        f"prefetch_factor={args.prefetch_factor} decode_batch_size={args.decode_batch_size} "
        f"num_threads={args.num_threads}",
        flush=True,
    )

    fluxon_runtime: Optional[FluxonBenchmarkRuntime] = None
    try:
        if "fluxon" in backends_to_run:
            fluxon_runtime = FluxonBenchmarkRuntime.open(args)

        backend_objs: dict[str, Any] = {}
        if "original" in backends_to_run:
            backend_objs["original"] = OriginalDecordBackend(
                num_threads=args.num_threads,
                fingerprint_bytes=args.fingerprint_bytes,
            )
        if "fluxon" in backends_to_run:
            if fluxon_runtime is None:
                raise RuntimeError("Fluxon runtime was not initialized")
            backend_objs["fluxon"] = FluxonVideoBackend(
                runtime=fluxon_runtime,
                num_threads=args.num_threads,
                fingerprint_bytes=args.fingerprint_bytes,
            )

        warmup_jobs = build_jobs(
            cases,
            backends=backends_to_run,
            rounds=args.warmup_rounds,
            alternate_backend_order=args.alternate_backend_order,
        )
        if warmup_jobs:
            run_jobs(
                warmup_jobs,
                backends=backend_objs,
                workers=args.workers,
                prefetch_factor=args.prefetch_factor,
                decode_batch_size=args.decode_batch_size,
                label="warmup",
            )

        jobs = build_jobs(
            cases,
            backends=backends_to_run,
            rounds=args.rounds,
            alternate_backend_order=args.alternate_backend_order,
        )
        rows, wall_s = run_jobs(
            jobs,
            backends=backend_objs,
            workers=args.workers,
            prefetch_factor=args.prefetch_factor,
            decode_batch_size=args.decode_batch_size,
            label="measure",
        )
    finally:
        if fluxon_runtime is not None:
            fluxon_runtime.close()

    summary = summarize(rows, wall_s=wall_s)
    write_rows_csv(args.output_csv, rows)
    write_json(
        args.output_json,
        {
            "args": sanitized_args(args),
            "summary": summary,
            "rows": rows,
        },
    )
    print_summary(summary)
    print(f"Wrote CSV: {args.output_csv}", flush=True)
    print(f"Wrote JSON: {args.output_json}", flush=True)

    if not args.allow_errors and any(row.get("status") != "ok" for row in rows):
        return 1
    return 0


def sanitized_args(args: argparse.Namespace) -> dict[str, Any]:
    out = dict(vars(args))
    if out.get("fluxon_request_password_file"):
        out["fluxon_request_password_file"] = "<redacted>"
    return out


if __name__ == "__main__":
    raise SystemExit(main())
