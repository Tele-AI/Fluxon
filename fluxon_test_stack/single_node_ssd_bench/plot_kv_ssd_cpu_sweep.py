#!/usr/bin/env python3
"""Render the test-stack CPU KV SSD-pressure matrix."""

from __future__ import annotations

import argparse
from pathlib import Path

from kv_ssd_pressure_chart import (
    BASE_IMPLEMENTATIONS,
    load_pressure_matrix,
    render_pressure_sweep,
)


PAYLOADS = (1, 4, 8, 16)
OUTPUT_MODES = ("holder", "bytes")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary",
        action="append",
        type=Path,
        required=True,
        help="Input summary.jsonl; later files replace duplicate case keys.",
    )
    parser.add_argument("--cpu-png-output", type=Path, required=True)
    parser.add_argument("--snapshot-label", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, loaded = load_pressure_matrix(
        args.summary,
        suite_group="cpu",
        output_modes=OUTPUT_MODES,
        payloads=PAYLOADS,
        implementations=BASE_IMPLEMENTATIONS,
    )
    render_pressure_sweep(
        path=args.cpu_png_output,
        rows=rows,
        suite_group="cpu",
        output_modes=OUTPUT_MODES,
        payloads=PAYLOADS,
        title="CPU SSD-pressure：吞吐与命中率（原生引用 / handle、bytes）",
        snapshot_label=args.snapshot_label,
        implementations=BASE_IMPLEMENTATIONS,
    )
    print(
        "rendered_kv_ssd_cpu_sweep",
        f"loaded_rows={loaded}",
        f"matrix_rows={len(rows)}/192",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
