#!/usr/bin/env python3
"""Analyze original-vs-Fluxon dataloader benchmark CSV results."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


LATENCY_FIELDS = ("elapsed_s", "open_s", "decode_s", "materialize_s")
FLUXON_VIDEO_COUNTER_FIELDS = (
    "fluxon_video_read_at_calls",
    "fluxon_video_read_at_requested_bytes",
    "fluxon_video_read_at_returned_bytes",
    "fluxon_video_page_cache_hits",
    "fluxon_video_page_cache_misses",
    "fluxon_video_remote_read_calls",
    "fluxon_video_remote_read_bytes",
    "fluxon_video_decode_calls",
    "fluxon_video_decode_frames_requested",
    "fluxon_video_decode_errors",
)


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def fval(row: Mapping[str, str], key: str, default: float = 0.0) -> float:
    raw = row.get(key, "")
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def ival(row: Mapping[str, str], key: str, default: int = 0) -> int:
    raw = row.get(key, "")
    if raw == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def numeric_summary(values: Iterable[float]) -> dict[str, float]:
    data = [float(v) for v in values]
    if not data:
        return {
            "count": 0,
            "min": 0.0,
            "mean": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0,
        }
    return {
        "count": len(data),
        "min": min(data),
        "mean": statistics.fmean(data),
        "p50": percentile(data, 0.50),
        "p95": percentile(data, 0.95),
        "p99": percentile(data, 0.99),
        "max": max(data),
    }


def analyze(rows: list[dict[str, str]]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "total_rows": len(rows),
        "backends": {},
        "pairwise": {},
        "fluxon_video": {},
    }

    backends = sorted({row.get("backend", "") for row in rows if row.get("backend", "")})
    for backend in backends:
        backend_rows = [row for row in rows if row.get("backend") == backend]
        ok_rows = [row for row in backend_rows if row.get("status") == "ok"]
        status_counts = Counter(row.get("status", "unknown") for row in backend_rows)
        frames = sum(ival(row, "num_frames") for row in ok_rows)
        output_bytes = sum(ival(row, "nbytes") for row in ok_rows)
        elapsed_sum = sum(fval(row, "elapsed_s") for row in ok_rows)
        backend_summary: dict[str, Any] = {
            "rows": len(backend_rows),
            "ok": len(ok_rows),
            "error": len(backend_rows) - len(ok_rows),
            "status_counts": dict(sorted(status_counts.items())),
            "frames": frames,
            "output_bytes": output_bytes,
            "elapsed_sum_s": elapsed_sum,
            "qps": len(ok_rows) / elapsed_sum if elapsed_sum > 0 else 0.0,
            "frames_per_s": frames / elapsed_sum if elapsed_sum > 0 else 0.0,
            "output_mib_per_s": (output_bytes / (1024 * 1024)) / elapsed_sum if elapsed_sum > 0 else 0.0,
        }
        for field in LATENCY_FIELDS:
            backend_summary[field] = numeric_summary(fval(row, field) for row in ok_rows)
        backend_summary["batch_wall_s"] = numeric_summary(fval(row, "batch_wall_s") for row in ok_rows)
        backend_summary["decode_batch_input_size"] = numeric_summary(
            ival(row, "decode_batch_input_size") for row in ok_rows
        )
        out["backends"][backend] = backend_summary

    out["pairwise"] = pairwise_analysis(rows)
    out["fluxon_video"] = fluxon_video_analysis(rows)
    return out


def pairwise_analysis(rows: list[dict[str, str]]) -> dict[str, Any]:
    by_sample: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    for row in rows:
        sample_id = row.get("sample_id", "")
        backend = row.get("backend", "")
        if sample_id and backend:
            by_sample[sample_id][backend] = row

    comparable = []
    mismatches = []
    for sample_id, sample_rows in sorted(by_sample.items()):
        original = sample_rows.get("original")
        fluxon = sample_rows.get("fluxon")
        if original is None or fluxon is None:
            continue
        if original.get("status") != "ok" or fluxon.get("status") != "ok":
            continue
        comparable.append((sample_id, original, fluxon))
        for field in ("shape", "dtype", "fingerprint"):
            lhs = original.get(field, "")
            rhs = fluxon.get(field, "")
            if lhs and rhs and lhs != rhs:
                mismatches.append(
                    {
                        "sample_id": sample_id,
                        "field": field,
                        "original": lhs,
                        "fluxon": rhs,
                    }
                )

    speedups = []
    deltas = []
    for _sample_id, original, fluxon in comparable:
        original_elapsed = fval(original, "elapsed_s")
        fluxon_elapsed = fval(fluxon, "elapsed_s")
        if original_elapsed > 0 and fluxon_elapsed > 0:
            speedups.append(original_elapsed / fluxon_elapsed)
            deltas.append(original_elapsed - fluxon_elapsed)

    return {
        "comparable_samples": len(comparable),
        "speedup": numeric_summary(speedups),
        "elapsed_delta_s": numeric_summary(deltas),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:50],
    }


def fluxon_video_analysis(rows: list[dict[str, str]]) -> dict[str, Any]:
    fluxon_ok = [row for row in rows if row.get("backend") == "fluxon" and row.get("status") == "ok"]
    counters = {
        field: sum(ival(row, field) for row in fluxon_ok)
        for field in FLUXON_VIDEO_COUNTER_FIELDS
    }
    page_hits = counters["fluxon_video_page_cache_hits"]
    page_misses = counters["fluxon_video_page_cache_misses"]
    page_lookups = page_hits + page_misses
    remote_read_bytes = counters["fluxon_video_remote_read_bytes"]
    output_bytes = sum(ival(row, "nbytes") for row in fluxon_ok)
    elapsed_sum = sum(fval(row, "elapsed_s") for row in fluxon_ok)

    out: dict[str, Any] = {
        "rows": len(fluxon_ok),
        "counters": counters,
        "reader_pool": fluxon_reader_pool_analysis(fluxon_ok),
        "page_cache_hit_rate": page_hits / page_lookups if page_lookups > 0 else 0.0,
        "remote_read_mib": remote_read_bytes / (1024 * 1024),
        "remote_read_mib_per_s": (remote_read_bytes / (1024 * 1024)) / elapsed_sum if elapsed_sum > 0 else 0.0,
        "compressed_to_output_byte_ratio": remote_read_bytes / output_bytes if output_bytes > 0 else 0.0,
        "remote_read_bytes_per_row": numeric_summary(
            ival(row, "fluxon_video_remote_read_bytes") for row in fluxon_ok
        ),
        "read_at_calls_per_row": numeric_summary(
            ival(row, "fluxon_video_read_at_calls") for row in fluxon_ok
        ),
    }
    return out


def fluxon_reader_pool_analysis(rows: list[dict[str, str]]) -> dict[str, Any]:
    if not rows:
        return {
            "reader_cache_hit_rate": 0.0,
            "reader_cache_hits": 0,
            "reader_cache_misses": 0,
            "open_count": 0,
            "close_count": 0,
            "evict_count": 0,
            "max_current_readers": 0,
            "max_active_readers": 0,
        }
    row_hits = sum(ival(row, "fluxon_video_pool_reader_cache_hit") for row in rows)
    snapshot_hit_max = max(ival(row, "fluxon_video_pool_reader_cache_hits") for row in rows)
    snapshot_miss_max = max(ival(row, "fluxon_video_pool_reader_cache_misses") for row in rows)
    return {
        "reader_cache_hit_rate": row_hits / len(rows),
        "reader_cache_hits": snapshot_hit_max,
        "reader_cache_misses": snapshot_miss_max,
        "open_count": max(ival(row, "fluxon_video_pool_open_count") for row in rows),
        "close_count": max(ival(row, "fluxon_video_pool_close_count") for row in rows),
        "evict_count": max(ival(row, "fluxon_video_pool_evict_count") for row in rows),
        "max_current_readers": max(ival(row, "fluxon_video_pool_current_readers") for row in rows),
        "max_active_readers": max(ival(row, "fluxon_video_pool_active_readers") for row in rows),
    }


def render_markdown(analysis: Mapping[str, Any], *, title: str) -> str:
    lines = [f"# {title}", ""]
    lines.append(f"- total_rows: {analysis.get('total_rows', 0)}")

    lines.extend(["", "## Backend Summary", ""])
    lines.append("| backend | rows | ok | error | p50 elapsed s | p95 elapsed s | qps | frames/s | output MiB/s |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    backends = analysis.get("backends", {})
    if isinstance(backends, Mapping):
        for backend, data in backends.items():
            if not isinstance(data, Mapping):
                continue
            elapsed = data.get("elapsed_s", {})
            if not isinstance(elapsed, Mapping):
                elapsed = {}
            lines.append(
                "| "
                f"{backend} | {data.get('rows', 0)} | {data.get('ok', 0)} | {data.get('error', 0)} | "
                f"{float(elapsed.get('p50', 0.0)):.6f} | {float(elapsed.get('p95', 0.0)):.6f} | "
                f"{float(data.get('qps', 0.0)):.2f} | {float(data.get('frames_per_s', 0.0)):.2f} | "
                f"{float(data.get('output_mib_per_s', 0.0)):.2f} |"
            )

    pairwise = analysis.get("pairwise", {})
    if isinstance(pairwise, Mapping):
        speed = pairwise.get("speedup", {})
        delta = pairwise.get("elapsed_delta_s", {})
        lines.extend(["", "## Pairwise", ""])
        lines.append(f"- comparable_samples: {pairwise.get('comparable_samples', 0)}")
        if isinstance(speed, Mapping):
            lines.append(
                "- speedup original/fluxon: "
                f"p50={float(speed.get('p50', 0.0)):.3f}x, "
                f"p95={float(speed.get('p95', 0.0)):.3f}x, "
                f"mean={float(speed.get('mean', 0.0)):.3f}x"
            )
        if isinstance(delta, Mapping):
            lines.append(
                "- elapsed_delta original-fluxon: "
                f"p50={float(delta.get('p50', 0.0)):.6f}s, "
                f"mean={float(delta.get('mean', 0.0)):.6f}s"
            )
        lines.append(f"- mismatch_count: {pairwise.get('mismatch_count', 0)}")

    fluxon_video = analysis.get("fluxon_video", {})
    if isinstance(fluxon_video, Mapping):
        counters = fluxon_video.get("counters", {})
        if not isinstance(counters, Mapping):
            counters = {}
        lines.extend(["", "## Fluxon VideoReader I/O", ""])
        lines.append(f"- rows: {fluxon_video.get('rows', 0)}")
        lines.append(f"- page_cache_hit_rate: {float(fluxon_video.get('page_cache_hit_rate', 0.0)):.4f}")
        lines.append(f"- remote_read_mib: {float(fluxon_video.get('remote_read_mib', 0.0)):.3f}")
        lines.append(f"- remote_read_mib_per_s: {float(fluxon_video.get('remote_read_mib_per_s', 0.0)):.3f}")
        lines.append(
            "- compressed_to_output_byte_ratio: "
            f"{float(fluxon_video.get('compressed_to_output_byte_ratio', 0.0)):.4f}"
        )
        reader_pool = fluxon_video.get("reader_pool", {})
        if isinstance(reader_pool, Mapping):
            lines.extend(["", "## Fluxon VideoReader Pool", ""])
            lines.append(
                f"- reader_cache_hit_rate: {float(reader_pool.get('reader_cache_hit_rate', 0.0)):.4f}"
            )
            lines.append(f"- reader_cache_hits: {reader_pool.get('reader_cache_hits', 0)}")
            lines.append(f"- reader_cache_misses: {reader_pool.get('reader_cache_misses', 0)}")
            lines.append(f"- open_count: {reader_pool.get('open_count', 0)}")
            lines.append(f"- close_count: {reader_pool.get('close_count', 0)}")
            lines.append(f"- evict_count: {reader_pool.get('evict_count', 0)}")
            lines.append(f"- max_current_readers: {reader_pool.get('max_current_readers', 0)}")
            lines.append(f"- max_active_readers: {reader_pool.get('max_active_readers', 0)}")
        for field in FLUXON_VIDEO_COUNTER_FIELDS:
            lines.append(f"- {field}: {counters.get(field, 0)}")

    lines.append("")
    return "\n".join(lines)


def write_outputs(
    analysis: Mapping[str, Any],
    *,
    output_json: Optional[Path],
    output_md: Optional[Path],
    title: str,
) -> None:
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(analysis, indent=2, sort_keys=True), encoding="utf-8")
    if output_md is not None:
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(render_markdown(analysis, title=title), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-json", default="")
    parser.add_argument("--output-md", default="")
    parser.add_argument("--title", default="Dataloader VideoReader Benchmark Analysis")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = load_rows(Path(args.input_csv))
    analysis = analyze(rows)
    output_json = Path(args.output_json) if args.output_json else None
    output_md = Path(args.output_md) if args.output_md else None
    write_outputs(
        analysis,
        output_json=output_json,
        output_md=output_md,
        title=args.title,
    )
    print(render_markdown(analysis, title=args.title), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
