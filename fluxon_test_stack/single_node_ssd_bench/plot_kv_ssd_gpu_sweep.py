#!/usr/bin/env python3
"""Render the test-stack GPU KV SSD-pressure matrix."""

from __future__ import annotations

import argparse
from pathlib import Path

from kv_ssd_pressure_chart import load_pressure_matrix, render_pressure_sweep


PAYLOADS = (4, 8, 16)
OUTPUT_MODES = ("cuda",)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--summary",
        action="append",
        type=Path,
        required=True,
        help="Input summary.jsonl; later files replace duplicate case keys.",
    )
    parser.add_argument("--gpu-png-output", type=Path, required=True)
    parser.add_argument("--snapshot-label", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows, loaded = load_pressure_matrix(
        args.summary,
        suite_group="gpu",
        output_modes=OUTPUT_MODES,
        payloads=PAYLOADS,
    )
    render_pressure_sweep(
        path=args.gpu_png_output,
        rows=rows,
        suite_group="gpu",
        output_modes=OUTPUT_MODES,
        payloads=PAYLOADS,
        title="GPU SSD-pressure：吞吐与命中率",
        snapshot_label=args.snapshot_label,
    )
    print(
        "rendered_kv_ssd_gpu_sweep",
        f"loaded_rows={loaded}",
        f"matrix_rows={len(rows)}/72",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
