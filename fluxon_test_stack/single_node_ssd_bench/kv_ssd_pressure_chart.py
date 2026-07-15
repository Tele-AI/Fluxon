#!/usr/bin/env python3
"""Shared CPU/GPU renderer for the test-stack KV SSD-pressure matrix."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from PIL import Image, ImageDraw, ImageFont


CONCURRENCIES = (2, 4, 8, 16)
BASE_IMPLEMENTATIONS = (
    "fluxon",
    "mooncake_dedicated",
    "mooncake_per_process",
)
IMPLEMENTATION_SHORT_LABELS = {
    "fluxon": "F",
    "mooncake_dedicated": "D",
    "mooncake_per_process": "P",
}
OUTPUT_LABELS = {
    "holder": "原生引用 / handle",
    "bytes": "bytes",
    "cuda": "cuda",
}
IMPLEMENTATION_COLORS = {
    "fluxon": ("#A8C7FA", "#2563EB"),
    "mooncake_dedicated": ("#FBC99D", "#E76F2E"),
    "mooncake_per_process": ("#A7E3D5", "#168C7A"),
}
HIT_RATE_COLOR = "#D64A63"
SSD_BACKEND_IMPLEMENTATIONS = ("native", "foyer")
SSD_BACKEND_SHORT_LABELS = {
    "native": "原生",
    "foyer": "Foyer",
}
SSD_BACKEND_COLORS = {
    "native": ("#BFDBFE", "#2563EB"),
    "foyer": ("#DDD6FE", "#7C3AED"),
}


@dataclass(frozen=True)
class RowKey:
    suite_group: str
    output_mode: str
    payload_mib: int
    concurrency: int
    implementation: str
    ssd_enabled: bool


@dataclass(frozen=True)
class SsdBackendAblationKey:
    output_mode: str
    payload_mib: int
    concurrency: int
    ssd_backend_impl: str


def row_implementation(row: Mapping[str, object]) -> str:
    backend = str(row["backend"])
    storage_mode = row.get("mooncake_storage_mode")
    if backend == "fluxon":
        if storage_mode is not None:
            raise ValueError(f"Fluxon row has Mooncake storage mode: {row}")
        return "fluxon"
    if backend != "mooncake":
        raise ValueError(f"unexpected backend: {backend!r}")
    if storage_mode == "DEDICATED_OWNER":
        return "mooncake_dedicated"
    if storage_mode == "PER_BENCHMARK_PROCESS":
        return "mooncake_per_process"
    raise ValueError(f"unexpected Mooncake storage mode: {storage_mode!r}")


def row_key(row: Mapping[str, object]) -> RowKey:
    return RowKey(
        suite_group=str(row["suite_group"]),
        output_mode=str(row["output_mode"]),
        payload_mib=int(row["payload_mib"]),
        concurrency=int(row["worker_concurrency"]),
        implementation=row_implementation(row),
        ssd_enabled=bool(row["ssd_enabled"]),
    )


def _hit_rate_percent(row: Mapping[str, object]) -> float:
    hit = int(row["hit"])
    miss = int(row["miss"])
    completed = hit + miss
    if completed == 0:
        raise ValueError(f"row has no completed hit or miss operation: {row}")
    return hit * 100.0 / completed


def _validate_row(row: Mapping[str, object], *, context: str) -> None:
    if str(row.get("workload_kind")) != "pressure":
        raise ValueError(f"non-pressure row in {context}: {row}")
    if str(row.get("status")) != "SUCCESS" or not bool(row.get("valid")):
        raise ValueError(f"invalid pressure row in {context}: {row}")
    if int(row.get("error", 0)) != 0:
        raise ValueError(f"pressure row contains errors in {context}: {row}")
    if not bool(row.get("source_complete")):
        raise ValueError(f"incomplete source counts in {context}: {row}")
    if int(row.get("unknown_source_operations", 0)) != 0:
        raise ValueError(f"unknown source operations in {context}: {row}")

    hit = int(row["hit"])
    memory_hits = int(row["memory_hit_operations"])
    ssd_hits = int(row["ssd_hit_operations"])
    if memory_hits + ssd_hits != hit:
        raise ValueError(f"source counts do not match hits in {context}: {row}")
    if not bool(row["ssd_enabled"]) and ssd_hits != 0:
        raise ValueError(f"SSD-off row contains SSD hits in {context}: {row}")

    memory_gib_s = float(row["memory_logical_read_gib_per_second"])
    ssd_gib_s = float(row["ssd_logical_read_gib_per_second"])
    total_gib_s = float(row["gib_per_second"])
    if min(memory_gib_s, ssd_gib_s, total_gib_s) < 0:
        raise ValueError(f"negative logical bandwidth in {context}: {row}")
    if abs(memory_gib_s + ssd_gib_s - total_gib_s) > 0.005:
        raise ValueError(f"source bandwidth does not match total in {context}: {row}")
    _hit_rate_percent(row)


def load_pressure_matrix(
    paths: Iterable[Path],
    *,
    suite_group: str,
    output_modes: Sequence[str],
    payloads: Sequence[int],
    implementations: Sequence[str] = BASE_IMPLEMENTATIONS,
) -> tuple[dict[RowKey, dict], int]:
    selected: dict[RowKey, dict] = {}
    contexts: dict[RowKey, str] = {}
    loaded = 0
    output_mode_set = set(output_modes)
    payload_set = set(payloads)
    for path in paths:
        for line_number, raw_line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            if not raw_line.strip():
                continue
            row = json.loads(raw_line)
            loaded += 1
            if row.get("workload_kind") != "pressure":
                continue
            if row.get("suite_group") != suite_group:
                continue
            if row.get("output_mode") not in output_mode_set:
                continue
            if int(row.get("payload_mib", -1)) not in payload_set:
                continue
            if int(row.get("worker_concurrency", -1)) not in CONCURRENCIES:
                continue
            key = row_key(row)
            selected[key] = row
            contexts[key] = f"{path}:{line_number}"

    expected = {
        RowKey(
            suite_group=suite_group,
            output_mode=output_mode,
            payload_mib=payload_mib,
            concurrency=concurrency,
            implementation=implementation,
            ssd_enabled=ssd_enabled,
        )
        for output_mode in output_modes
        for payload_mib in payloads
        for concurrency in CONCURRENCIES
        for implementation in implementations
        for ssd_enabled in (False, True)
    }
    missing = sorted(expected - selected.keys(), key=repr)
    unexpected = sorted(selected.keys() - expected, key=repr)
    if missing or unexpected:
        raise ValueError(
            f"incomplete {suite_group} pressure matrix: "
            f"rows={len(selected)}/{len(expected)} "
            f"missing={missing} unexpected={unexpected}"
        )
    for key, row in selected.items():
        _validate_row(row, context=contexts[key])
    return selected, loaded


class RasterCanvas:
    def __init__(self, *, width: int, height: int) -> None:
        self.width = width
        self.height = height
        self.image = Image.new("RGB", (width, height), "#F8FAFC")
        self.draw = ImageDraw.Draw(self.image)
        self._font_paths = {
            "regular": self._find_font(
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ),
            "bold": self._find_font(
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
                "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            ),
        }
        self._fonts: dict[tuple[str, int], ImageFont.FreeTypeFont] = {}

    @staticmethod
    def _find_font(*candidates: str) -> Path:
        for candidate in candidates:
            path = Path(candidate)
            if path.is_file():
                return path
        raise RuntimeError(f"no usable chart font found in {candidates}")

    def _font(self, *, size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
        weight = "bold" if bold else "regular"
        key = (weight, size)
        font = self._fonts.get(key)
        if font is None:
            font = ImageFont.truetype(str(self._font_paths[weight]), size=size)
            self._fonts[key] = font
        return font

    def rect(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        *,
        fill: str,
        stroke: str,
        stroke_width: float = 1.0,
        rx: float = 0.0,
    ) -> None:
        self.draw.rounded_rectangle(
            (round(x), round(y), round(x + width), round(y + height)),
            radius=max(0, round(rx)),
            fill=fill,
            outline=stroke,
            width=max(1, round(stroke_width)),
        )

    def line(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        *,
        stroke: str,
        stroke_width: float = 1.0,
        dash: tuple[int, int] | None = None,
    ) -> None:
        width = max(1, round(stroke_width))
        if dash is None:
            self.draw.line((x1, y1, x2, y2), fill=stroke, width=width)
            return
        dash_length, gap_length = dash
        dx, dy = x2 - x1, y2 - y1
        length = math.hypot(dx, dy)
        if length == 0:
            return
        ux, uy = dx / length, dy / length
        cursor = 0.0
        while cursor < length:
            end = min(length, cursor + dash_length)
            self.draw.line(
                (
                    x1 + ux * cursor,
                    y1 + uy * cursor,
                    x1 + ux * end,
                    y1 + uy * end,
                ),
                fill=stroke,
                width=width,
            )
            cursor += dash_length + gap_length

    def circle(
        self,
        x: float,
        y: float,
        radius: float,
        *,
        fill: str,
        stroke: str,
        stroke_width: float = 1.0,
    ) -> None:
        self.draw.ellipse(
            (
                round(x - radius),
                round(y - radius),
                round(x + radius),
                round(y + radius),
            ),
            fill=fill,
            outline=stroke,
            width=max(1, round(stroke_width)),
        )

    def text(
        self,
        x: float,
        y: float,
        value: str,
        *,
        style: str,
        anchor: str = "left",
        fill: str | None = None,
    ) -> None:
        styles = {
            "title": (28, True, "#0F172A"),
            "subtitle": (15, False, "#475569"),
            "panel-title": (18, True, "#0F172A"),
            "axis": (12, False, "#334155"),
            "axis-small": (10, False, "#64748B"),
            "legend": (12, False, "#334155"),
            "note": (13, False, "#475569"),
        }
        try:
            size, bold, default_fill = styles[style]
        except KeyError as exc:
            raise ValueError(f"unknown text style: {style}") from exc
        anchors = {"left": "ls", "middle": "ms", "end": "rs"}
        self.draw.text(
            (round(x), round(y)),
            value,
            font=self._font(size=size, bold=bold),
            fill=fill or default_fill,
            anchor=anchors[anchor],
        )

    def write(self, path: Path) -> None:
        if path.suffix.lower() != ".png":
            raise ValueError(f"raster chart output must use .png: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.image.save(path, format="PNG", optimize=True)


def nice_axis_max(values: Iterable[float]) -> float:
    peak = max(values, default=0.0)
    if peak <= 0:
        return 1.0
    raw_step = peak / 4.0
    magnitude = 10 ** math.floor(math.log10(raw_step))
    fraction = raw_step / magnitude
    step_factor = next(
        value for value in (1.0, 2.0, 2.5, 5.0, 10.0) if fraction <= value
    )
    step = step_factor * magnitude
    return step * math.ceil(peak / step)


def format_axis_value(value: float) -> str:
    if value >= 10:
        return f"{value:.0f}"
    if value >= 1:
        return f"{value:.1f}"
    return f"{value:.2f}"


def _draw_axes(
    canvas: RasterCanvas,
    *,
    x: float,
    y: float,
    width: float,
    height: float,
    maximum: float,
) -> None:
    for tick_index in range(5):
        throughput = maximum * tick_index / 4
        tick_y = y + height - height * tick_index / 4
        canvas.line(x, tick_y, x + width, tick_y, stroke="#E2E8F0")
        canvas.text(
            x - 7,
            tick_y + 4,
            format_axis_value(throughput),
            style="axis-small",
            anchor="end",
        )
    for hit_rate in (0, 50, 100):
        tick_y = y + height * (1.0 - hit_rate / 100.0)
        canvas.text(
            x + width + 7,
            tick_y + 4,
            f"{hit_rate}%",
            style="axis-small",
        )
    canvas.line(x, y, x, y + height, stroke="#94A3B8")
    canvas.line(x + width, y, x + width, y + height, stroke="#F2A7B5")
    canvas.line(x, y + height, x + width, y + height, stroke="#94A3B8")


def _draw_legend(
    canvas: RasterCanvas,
    *,
    x: float,
    y: float,
    implementations: Sequence[str],
) -> None:
    labels = {
        "fluxon": ("Fluxon", 100),
        "mooncake_dedicated": ("MC 独立 owner", 160),
        "mooncake_per_process": ("MC 每进程", 130),
    }
    for implementation in implementations:
        label, item_width = labels[implementation]
        memory_color, ssd_color = IMPLEMENTATION_COLORS[implementation]
        canvas.rect(
            x,
            y - 11,
            10,
            12,
            fill=memory_color,
            stroke=ssd_color,
            rx=2,
        )
        canvas.rect(
            x + 10,
            y - 11,
            10,
            12,
            fill=ssd_color,
            stroke=ssd_color,
            rx=2,
        )
        canvas.text(x + 27, y, label, style="legend")
        x += item_width
    canvas.circle(
        x + 8,
        y - 5,
        4,
        fill=HIT_RATE_COLOR,
        stroke="#ffffff",
        stroke_width=1.5,
    )
    canvas.text(x + 24, y, "命中率（右轴）", style="legend")
    x += 135
    canvas.text(
        x,
        y,
        "浅色=DRAM · 深色=SSD",
        style="legend",
    )


def _draw_panel(
    canvas: RasterCanvas,
    *,
    panel_x: float,
    panel_y: float,
    panel_width: float,
    panel_height: float,
    suite_group: str,
    output_mode: str,
    payload_mib: int,
    maximum: float,
    rows: Mapping[RowKey, Mapping[str, object]],
    implementations: Sequence[str],
) -> None:
    plot_x = panel_x + 50
    plot_y = panel_y + 58
    plot_width = panel_width - 88
    plot_height = panel_height - 142
    canvas.rect(
        panel_x,
        panel_y,
        panel_width,
        panel_height,
        fill="#FFFFFF",
        stroke="#E2E8F0",
        rx=12,
    )
    title = f"{payload_mib} MiB"
    if output_mode != "cuda":
        title = f"{OUTPUT_LABELS[output_mode]} · {title}"
    canvas.text(panel_x + 18, panel_y + 29, title, style="panel-title")
    canvas.text(
        panel_x + panel_width - 18,
        panel_y + 29,
        f"0–{format_axis_value(maximum)} GiB/s",
        style="axis",
        anchor="end",
    )
    _draw_axes(
        canvas,
        x=plot_x,
        y=plot_y,
        width=plot_width,
        height=plot_height,
        maximum=maximum,
    )

    group_width = plot_width / len(CONCURRENCIES)
    bar_width = 11.0
    state_gap = 2.0
    implementation_gap = 4.0
    pair_width = 2 * bar_width + state_gap
    series_width = (
        len(implementations) * pair_width
        + (len(implementations) - 1) * implementation_gap
    )
    baseline = plot_y + plot_height
    for concurrency_index, concurrency in enumerate(CONCURRENCIES):
        group_center = plot_x + (concurrency_index + 0.5) * group_width
        cursor_x = group_center - series_width / 2
        for implementation_index, implementation in enumerate(implementations):
            short = IMPLEMENTATION_SHORT_LABELS[implementation]
            memory_color, ssd_color = IMPLEMENTATION_COLORS[implementation]
            for state_index, ssd_enabled in enumerate((False, True)):
                key = RowKey(
                    suite_group=suite_group,
                    output_mode=output_mode,
                    payload_mib=payload_mib,
                    concurrency=concurrency,
                    implementation=implementation,
                    ssd_enabled=ssd_enabled,
                )
                row = rows[key]
                bar_x = cursor_x + state_index * (bar_width + state_gap)
                memory_gib_s = float(row["memory_logical_read_gib_per_second"])
                ssd_gib_s = float(row["ssd_logical_read_gib_per_second"])
                memory_height = memory_gib_s / maximum * plot_height
                ssd_height = ssd_gib_s / maximum * plot_height
                if memory_height > 0:
                    canvas.rect(
                        bar_x,
                        baseline - memory_height,
                        bar_width,
                        memory_height,
                        fill=memory_color,
                        stroke=ssd_color,
                        stroke_width=0.8,
                    )
                if ssd_height > 0:
                    canvas.rect(
                        bar_x,
                        baseline - memory_height - ssd_height,
                        bar_width,
                        ssd_height,
                        fill=ssd_color,
                        stroke=ssd_color,
                        stroke_width=0.8,
                    )
                hit_y = plot_y + plot_height * (
                    1.0 - _hit_rate_percent(row) / 100.0
                )
                canvas.circle(
                    bar_x + bar_width / 2,
                    hit_y,
                    3.5,
                    fill=HIT_RATE_COLOR,
                    stroke="#ffffff",
                    stroke_width=1.2,
                )
                state = "开" if ssd_enabled else "关"
                canvas.text(
                    bar_x + bar_width / 2,
                    baseline + 14,
                    state,
                    style="axis-small",
                    anchor="middle",
                )
            pair_center = cursor_x + pair_width / 2
            canvas.text(
                pair_center,
                baseline + 29,
                short,
                style="axis-small",
                anchor="middle",
                fill=ssd_color,
            )
            cursor_x += pair_width
            if implementation_index + 1 < len(implementations):
                cursor_x += implementation_gap
        canvas.text(
            group_center,
            baseline + 45,
            f"c{concurrency}",
            style="axis",
            anchor="middle",
        )
        if concurrency_index + 1 < len(CONCURRENCIES):
            separator_x = plot_x + (concurrency_index + 1) * group_width
            canvas.line(
                separator_x,
                baseline + 5,
                separator_x,
                baseline + 49,
                stroke="#CBD5E1",
            )


def render_pressure_sweep(
    *,
    path: Path,
    rows: Mapping[RowKey, Mapping[str, object]],
    suite_group: str,
    output_modes: Sequence[str],
    payloads: Sequence[int],
    title: str,
    snapshot_label: str,
    implementations: Sequence[str] = BASE_IMPLEMENTATIONS,
) -> None:
    panel_width = 570
    panel_height = 475
    panel_gap = 18
    left = 48
    top = 132
    bottom = 62
    width = left * 2 + len(payloads) * panel_width + (len(payloads) - 1) * panel_gap
    height = (
        top
        + len(output_modes) * panel_height
        + (len(output_modes) - 1) * panel_gap
        + bottom
    )
    canvas = RasterCanvas(width=width, height=height)
    series_count = len(implementations) * 2
    series_count_label = {6: "六", 8: "八"}.get(series_count, str(series_count))
    canvas.text(left, 45, title, style="title")
    canvas.text(
        left,
        73,
        f"每个并发度固定{series_count_label}柱："
        + "、".join(
            {
                "fluxon": "Fluxon",
                "mooncake_dedicated": "MC 独立 owner",
                "mooncake_per_process": "MC 每进程",
            }[implementation]
            for implementation in implementations
        )
        + " × SSD 关/开；"
        "颜色区分实现，浅/深区分 DRAM / SSD；各 payload 分面独立左轴",
        style="subtitle",
    )
    _draw_legend(canvas, x=left, y=106, implementations=implementations)

    for output_index, output_mode in enumerate(output_modes):
        panel_y = top + output_index * (panel_height + panel_gap)
        for payload_index, payload_mib in enumerate(payloads):
            maximum = nice_axis_max(
                float(row["memory_logical_read_gib_per_second"])
                + float(row["ssd_logical_read_gib_per_second"])
                for key, row in rows.items()
                if key.output_mode == output_mode
                and key.payload_mib == payload_mib
            )
            panel_x = left + payload_index * (panel_width + panel_gap)
            _draw_panel(
                canvas,
                panel_x=panel_x,
                panel_y=panel_y,
                panel_width=panel_width,
                panel_height=panel_height,
                suite_group=suite_group,
                output_mode=output_mode,
                payload_mib=payload_mib,
                maximum=maximum,
                rows=rows,
                implementations=implementations,
            )

    canvas.text(
        left,
        height - 27,
        f"快照：{snapshot_label}。柱高只计 hit payload；红点为 hit / (hit + miss)，"
        "miss 是压力实验结果的一部分。",
        style="note",
    )
    canvas.write(path)


def _draw_ssd_backend_ablation_legend(
    canvas: RasterCanvas,
    *,
    x: float,
    y: float,
) -> None:
    labels = {
        "native": ("Fluxon 原生 SSD", 155),
        "foyer": ("Fluxon Foyer SSD", 165),
    }
    for implementation in SSD_BACKEND_IMPLEMENTATIONS:
        label, item_width = labels[implementation]
        memory_color, ssd_color = SSD_BACKEND_COLORS[implementation]
        canvas.rect(
            x,
            y - 11,
            10,
            12,
            fill=memory_color,
            stroke=ssd_color,
            rx=2,
        )
        canvas.rect(
            x + 10,
            y - 11,
            10,
            12,
            fill=ssd_color,
            stroke=ssd_color,
            rx=2,
        )
        canvas.text(x + 27, y, label, style="legend")
        x += item_width
    canvas.circle(
        x + 8,
        y - 5,
        4,
        fill=HIT_RATE_COLOR,
        stroke="#ffffff",
        stroke_width=1.5,
    )
    canvas.text(x + 24, y, "命中率（右轴）", style="legend")
    x += 135
    canvas.text(x, y, "浅色=DRAM · 深色=SSD", style="legend")


def _draw_ssd_backend_ablation_panel(
    canvas: RasterCanvas,
    *,
    panel_x: float,
    panel_y: float,
    panel_width: float,
    panel_height: float,
    output_mode: str,
    payload_mib: int,
    maximum: float,
    rows: Mapping[SsdBackendAblationKey, Mapping[str, object]],
) -> None:
    plot_x = panel_x + 50
    plot_y = panel_y + 58
    plot_width = panel_width - 88
    plot_height = panel_height - 142
    canvas.rect(
        panel_x,
        panel_y,
        panel_width,
        panel_height,
        fill="#FFFFFF",
        stroke="#E2E8F0",
        rx=12,
    )
    output_label = "MemHolder" if output_mode == "holder" else OUTPUT_LABELS[output_mode]
    canvas.text(
        panel_x + 18,
        panel_y + 29,
        f"{output_label} · {payload_mib} MiB",
        style="panel-title",
    )
    canvas.text(
        panel_x + panel_width - 18,
        panel_y + 29,
        f"0–{format_axis_value(maximum)} GiB/s",
        style="axis",
        anchor="end",
    )
    _draw_axes(
        canvas,
        x=plot_x,
        y=plot_y,
        width=plot_width,
        height=plot_height,
        maximum=maximum,
    )

    group_width = plot_width / len(CONCURRENCIES)
    bar_width = 25.0
    backend_gap = 13.0
    series_width = len(SSD_BACKEND_IMPLEMENTATIONS) * bar_width + backend_gap
    baseline = plot_y + plot_height
    for concurrency_index, concurrency in enumerate(CONCURRENCIES):
        group_center = plot_x + (concurrency_index + 0.5) * group_width
        cursor_x = group_center - series_width / 2
        for backend_index, backend_impl in enumerate(SSD_BACKEND_IMPLEMENTATIONS):
            key = SsdBackendAblationKey(
                output_mode=output_mode,
                payload_mib=payload_mib,
                concurrency=concurrency,
                ssd_backend_impl=backend_impl,
            )
            row = rows[key]
            memory_color, ssd_color = SSD_BACKEND_COLORS[backend_impl]
            memory_gib_s = float(row["memory_logical_read_gib_per_second"])
            ssd_gib_s = float(row["ssd_logical_read_gib_per_second"])
            memory_height = memory_gib_s / maximum * plot_height
            ssd_height = ssd_gib_s / maximum * plot_height
            if memory_height > 0:
                canvas.rect(
                    cursor_x,
                    baseline - memory_height,
                    bar_width,
                    memory_height,
                    fill=memory_color,
                    stroke=ssd_color,
                    stroke_width=0.8,
                )
            if ssd_height > 0:
                canvas.rect(
                    cursor_x,
                    baseline - memory_height - ssd_height,
                    bar_width,
                    ssd_height,
                    fill=ssd_color,
                    stroke=ssd_color,
                    stroke_width=0.8,
                )
            hit_y = plot_y + plot_height * (
                1.0 - _hit_rate_percent(row) / 100.0
            )
            canvas.circle(
                cursor_x + bar_width / 2,
                hit_y,
                4.0,
                fill=HIT_RATE_COLOR,
                stroke="#ffffff",
                stroke_width=1.2,
            )
            canvas.text(
                cursor_x + bar_width / 2,
                baseline + 18,
                SSD_BACKEND_SHORT_LABELS[backend_impl],
                style="axis-small",
                anchor="middle",
                fill=ssd_color,
            )
            cursor_x += bar_width
            if backend_index + 1 < len(SSD_BACKEND_IMPLEMENTATIONS):
                cursor_x += backend_gap
        canvas.text(
            group_center,
            baseline + 40,
            f"c{concurrency}",
            style="axis",
            anchor="middle",
        )
        if concurrency_index + 1 < len(CONCURRENCIES):
            separator_x = plot_x + (concurrency_index + 1) * group_width
            canvas.line(
                separator_x,
                baseline + 5,
                separator_x,
                baseline + 44,
                stroke="#CBD5E1",
            )


def render_ssd_backend_ablation(
    *,
    path: Path,
    rows: Mapping[SsdBackendAblationKey, Mapping[str, object]],
    output_modes: Sequence[str],
    payloads: Sequence[int],
    title: str,
    snapshot_label: str,
) -> None:
    panel_width = 570
    panel_height = 475
    panel_gap = 18
    left = 48
    top = 132
    bottom = 62
    width = left * 2 + len(payloads) * panel_width + (len(payloads) - 1) * panel_gap
    height = (
        top
        + len(output_modes) * panel_height
        + (len(output_modes) - 1) * panel_gap
        + bottom
    )
    canvas = RasterCanvas(width=width, height=height)
    canvas.text(left, 45, title, style="title")
    canvas.text(
        left,
        73,
        "每个并发度固定两柱：Fluxon 原生 SSD、Fluxon Foyer SSD；"
        "颜色区分 SSD 实现，浅/深区分 DRAM / SSD；Fluxon DRAM 均为 2 GiB，"
        "Foyer 内部 memory admission=0；两侧均使用 direct I/O",
        style="subtitle",
    )
    _draw_ssd_backend_ablation_legend(canvas, x=left, y=106)

    for output_index, output_mode in enumerate(output_modes):
        panel_y = top + output_index * (panel_height + panel_gap)
        for payload_index, payload_mib in enumerate(payloads):
            maximum = nice_axis_max(
                float(row["memory_logical_read_gib_per_second"])
                + float(row["ssd_logical_read_gib_per_second"])
                for key, row in rows.items()
                if key.output_mode == output_mode and key.payload_mib == payload_mib
            )
            panel_x = left + payload_index * (panel_width + panel_gap)
            _draw_ssd_backend_ablation_panel(
                canvas,
                panel_x=panel_x,
                panel_y=panel_y,
                panel_width=panel_width,
                panel_height=panel_height,
                output_mode=output_mode,
                payload_mib=payload_mib,
                maximum=maximum,
                rows=rows,
            )

    canvas.text(
        left,
        height - 27,
        f"快照：{snapshot_label}。柱高只计 hit payload；红点为 hit / (hit + miss)，"
        "两组都经过同一 Fluxon KV / FlatDict / route / transport 路径。",
        style="note",
    )
    canvas.write(path)
