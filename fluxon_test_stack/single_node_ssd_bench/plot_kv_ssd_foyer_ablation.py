#!/usr/bin/env python3
"""Render the test-stack Fluxon native-vs-Foyer SSD backend ablation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from kv_ssd_pressure_chart import (
    CONCURRENCIES,
    SSD_BACKEND_IMPLEMENTATIONS,
    SsdBackendAblationKey,
    render_ssd_backend_ablation,
)


OUTPUT_MODES = ("holder", "bytes")
PAYLOADS = (1, 4, 8, 16)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary",
        action="append",
        type=Path,
        required=True,
        help="Input Foyer ablation summary.jsonl; later files replace duplicate keys.",
    )
    parser.add_argument("--png-output", type=Path, required=True)
    parser.add_argument("--snapshot-label", required=True)
    return parser.parse_args()


def _key(row: dict) -> SsdBackendAblationKey:
    return SsdBackendAblationKey(
        output_mode=str(row["output_mode"]),
        payload_mib=int(row["payload_mib"]),
        concurrency=int(row["worker_concurrency"]),
        ssd_backend_impl=str(row["ssd_backend_impl"]),
    )


def _validate_row(row: dict, *, context: str) -> None:
    if row.get("workload_kind") != "pressure" or row.get("suite_group") != "cpu":
        raise ValueError(f"unexpected workload in {context}: {row}")
    expected_workload = {
        "workload_id": "kv_ssd_pressure_zipf",
        "request_distribution": "zipfian",
        "read_ratio": 1.0,
        "write_ratio": 0.0,
        "keyspace_capacity_guard": False,
        "kv_bootstrap_concurrency": 1,
    }
    mismatched_workload = {
        field: (row.get(field), expected)
        for field, expected in expected_workload.items()
        if row.get(field) != expected
    }
    if mismatched_workload:
        raise ValueError(
            f"unexpected pressure controls in {context}: {mismatched_workload}"
        )
    payload_mib = int(row["payload_mib"])
    concurrency = int(row["worker_concurrency"])
    mib = 1024 * 1024
    process_layout = {
        2: (1, 2),
        4: (1, 4),
        8: (2, 4),
        16: (4, 4),
    }
    processes_per_target, threads_per_process = process_layout[concurrency]
    payload_bytes = payload_mib * mib - 128
    keyspace_size = 2560 // payload_mib
    expected_controls = {
        "payload_bytes": payload_bytes,
        "keyspace_size": keyspace_size,
        "dataset_payload_bytes": payload_bytes * keyspace_size,
        "owner_dram_bytes": 2048 * mib,
        "ssd_capacity_bytes": 16 * 1024 * mib,
        "processes_per_target": processes_per_target,
        "threads_per_process": threads_per_process,
        "duration_seconds_configured": 35,
        "metric_warmup_seconds": 5,
        "kv_bootstrap_put_gap_ms": max(5, payload_mib * 1000 // 160),
    }
    mismatched_controls = {
        field: (row.get(field), expected)
        for field, expected in expected_controls.items()
        if row.get(field) != expected
    }
    if mismatched_controls:
        raise ValueError(
            f"unexpected capacity or concurrency controls in {context}: "
            f"{mismatched_controls}"
        )
    if row.get("backend") != "fluxon":
        raise ValueError(f"non-Fluxon row in {context}: {row}")
    if row.get("status") != "SUCCESS" or not bool(row.get("valid")):
        raise ValueError(f"invalid row in {context}: {row}")
    if int(row.get("error", 0)) != 0:
        raise ValueError(f"row contains errors in {context}: {row}")
    if int(row.get("ssd_persist_failure_count", -1)) != 0:
        raise ValueError(f"row contains SSD persist failures in {context}: {row}")
    if not bool(row.get("source_complete")):
        raise ValueError(f"incomplete source counts in {context}: {row}")
    if int(row.get("unknown_source_operations", 0)) != 0:
        raise ValueError(f"unknown source operations in {context}: {row}")
    hit = int(row["hit"])
    memory_hits = int(row["memory_hit_operations"])
    ssd_hits = int(row["ssd_hit_operations"])
    if memory_hits + ssd_hits != hit:
        raise ValueError(f"source counts do not match hits in {context}: {row}")
    total = float(row["gib_per_second"])
    memory = float(row["memory_logical_read_gib_per_second"])
    ssd = float(row["ssd_logical_read_gib_per_second"])
    if min(total, memory, ssd) < 0 or abs(memory + ssd - total) > 0.005:
        raise ValueError(f"source bandwidth does not match total in {context}: {row}")


def load_rows(paths: list[Path]) -> dict[SsdBackendAblationKey, dict]:
    selected: dict[SsdBackendAblationKey, dict] = {}
    contexts: dict[SsdBackendAblationKey, str] = {}
    for path in paths:
        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not raw_line.strip():
                continue
            row = json.loads(raw_line)
            if row.get("workload_kind") != "pressure" or row.get("suite_group") != "cpu":
                continue
            key = _key(row)
            selected[key] = row
            contexts[key] = f"{path}:{line_number}"

    expected = {
        SsdBackendAblationKey(
            output_mode=output_mode,
            payload_mib=payload_mib,
            concurrency=concurrency,
            ssd_backend_impl=backend_impl,
        )
        for output_mode in OUTPUT_MODES
        for payload_mib in PAYLOADS
        for concurrency in CONCURRENCIES
        for backend_impl in SSD_BACKEND_IMPLEMENTATIONS
    }
    missing = sorted(expected - selected.keys(), key=repr)
    unexpected = sorted(selected.keys() - expected, key=repr)
    if missing or unexpected:
        raise ValueError(
            f"incomplete Fluxon SSD backend ablation: rows={len(selected)}/{len(expected)} "
            f"missing={missing} unexpected={unexpected}"
        )
    for key, row in selected.items():
        _validate_row(row, context=contexts[key])

    control_fields = (
        "workload_id",
        "request_distribution",
        "read_ratio",
        "write_ratio",
        "keyspace_capacity_guard",
        "kv_bootstrap_concurrency",
        "kv_bootstrap_put_gap_ms",
        "payload_bytes",
        "keyspace_size",
        "dataset_payload_bytes",
        "owner_dram_bytes",
        "ssd_capacity_bytes",
        "worker_concurrency",
        "processes_per_target",
        "threads_per_process",
        "duration_seconds_configured",
        "metric_warmup_seconds",
    )
    for output_mode in OUTPUT_MODES:
        for payload_mib in PAYLOADS:
            for concurrency in CONCURRENCIES:
                native = selected[
                    SsdBackendAblationKey(output_mode, payload_mib, concurrency, "native")
                ]
                foyer = selected[
                    SsdBackendAblationKey(output_mode, payload_mib, concurrency, "foyer")
                ]
                differences = {
                    field: (native.get(field), foyer.get(field))
                    for field in control_fields
                    if native.get(field) != foyer.get(field)
                }
                if differences:
                    raise ValueError(
                        "native/Foyer control mismatch for "
                        f"{output_mode} p{payload_mib} c{concurrency}: {differences}"
                    )
    return selected


def main() -> int:
    args = parse_args()
    rows = load_rows(args.summary)
    render_ssd_backend_ablation(
        path=args.png_output,
        rows=rows,
        output_modes=OUTPUT_MODES,
        payloads=PAYLOADS,
        title="Fluxon SSD backend 消融：原生实现 vs Foyer",
        snapshot_label=args.snapshot_label,
    )
    print(f"rendered_kv_ssd_foyer_ablation matrix_rows={len(rows)}/64")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
