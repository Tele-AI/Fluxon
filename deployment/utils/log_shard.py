#!/usr/bin/env python3

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Optional


DEFAULT_DAILY_LOG_RETENTION_DAYS = 31
TEST_LOG_SHARD_WINDOW_SECONDS_ENV = "FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS"
TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV = "FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS"
TEST_LOG_SHARD_BASE_DATE = datetime.date(2026, 1, 1)


def _read_test_log_shard_window_seconds() -> Optional[int]:
    raw_value = os.environ.get(TEST_LOG_SHARD_WINDOW_SECONDS_ENV)
    if raw_value is None:
        return None
    text = raw_value.strip()
    if not text:
        return None
    window_seconds = int(text)
    if window_seconds <= 0:
        raise ValueError(
            f"{TEST_LOG_SHARD_WINDOW_SECONDS_ENV} must be a positive integer, got: {raw_value!r}"
        )
    return window_seconds


def _read_test_log_shard_anchor_unix_seconds() -> int:
    raw_value = os.environ.get(TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV)
    if raw_value is None or not raw_value.strip():
        raise ValueError(
            f"{TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV} is required when "
            f"{TEST_LOG_SHARD_WINDOW_SECONDS_ENV} is set"
        )
    return int(raw_value.strip())


def _resolve_shard_date(ts: datetime.datetime) -> datetime.date:
    window_seconds = _read_test_log_shard_window_seconds()
    if window_seconds is None:
        return ts.date()
    anchor_unix_seconds = _read_test_log_shard_anchor_unix_seconds()
    unix_seconds = int(ts.timestamp())
    bucket_index = (unix_seconds - anchor_unix_seconds) // window_seconds
    if bucket_index < 0:
        raise ValueError(
            "test log shard anchor must not be in the future: "
            f"anchor={anchor_unix_seconds}, ts={unix_seconds}"
        )
    return TEST_LOG_SHARD_BASE_DATE + datetime.timedelta(days=bucket_index)


def daily_sharded_log_path(
    base_path: Path,
    *,
    now: Optional[datetime.datetime] = None,
) -> Path:
    ts = datetime.datetime.now(datetime.timezone.utc) if now is None else now.astimezone(datetime.timezone.utc)
    name = base_path.name
    if not name.endswith(".log"):
        raise ValueError(f"log base path must end with .log: {base_path}")
    stem = name[:-4]
    shard_date = _resolve_shard_date(ts)
    return (base_path.parent / f"{stem}.{shard_date.isoformat()}.log").resolve()


def latest_existing_daily_sharded_log_path(base_path: Path) -> Optional[Path]:
    name = base_path.name
    if not name.endswith(".log"):
        return base_path.resolve() if base_path.exists() else None
    stem = name[:-4]
    prefix = stem + "."
    suffix = ".log"
    latest: Optional[tuple[datetime.date, Path]] = None
    parent = base_path.parent
    if not parent.exists():
        return base_path.resolve() if base_path.exists() else None
    for path in parent.iterdir():
        if not path.is_file():
            continue
        entry_name = path.name
        if not entry_name.startswith(prefix) or not entry_name.endswith(suffix):
            continue
        date_text = entry_name[len(prefix):-len(suffix)]
        try:
            shard_date = datetime.date.fromisoformat(date_text)
        except ValueError:
            continue
        if latest is None or shard_date > latest[0]:
            latest = (shard_date, path.resolve())
    if latest is not None:
        return latest[1]
    if base_path.exists():
        return base_path.resolve()
    return None


def resolve_readable_log_path(base_path: Path) -> Optional[Path]:
    current = daily_sharded_log_path(base_path)
    if current.exists():
        return current
    return latest_existing_daily_sharded_log_path(base_path)


def cleanup_old_daily_sharded_logs(
    base_path: Path,
    *,
    retention_days: int = DEFAULT_DAILY_LOG_RETENTION_DAYS,
) -> None:
    name = base_path.name
    if not name.endswith(".log"):
        return
    current_shard_date = _resolve_shard_date(datetime.datetime.now(datetime.timezone.utc))
    keep_since = current_shard_date - datetime.timedelta(days=max(int(retention_days) - 1, 0))
    stem = name[:-4]
    prefix = stem + "."
    suffix = ".log"
    parent = base_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    for path in parent.iterdir():
        if not path.is_file():
            continue
        entry_name = path.name
        if not entry_name.startswith(prefix) or not entry_name.endswith(suffix):
            continue
        date_text = entry_name[len(prefix):-len(suffix)]
        try:
            shard_date = datetime.date.fromisoformat(date_text)
        except ValueError:
            continue
        if shard_date < keep_since:
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def render_module_source() -> str:
    module_path = Path(__file__).resolve()
    return module_path.read_text(encoding="utf-8")


def import_sibling_log_shard():
    import importlib.util
    import sys

    helper_path = Path(__file__).resolve().with_name("log_shard.py")
    module_name = "_fluxon_log_shard_runtime"
    loaded = sys.modules.get(module_name)
    if loaded is not None:
        return loaded
    spec = importlib.util.spec_from_file_location(module_name, helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load log shard helper: {helper_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def relay_fd_to_daily_sharded_logs(
    *,
    base_log_path: str,
    read_fd: int,
    retention_days: int = DEFAULT_DAILY_LOG_RETENTION_DAYS,
) -> None:
    base_path = Path(os.path.abspath(base_log_path))
    current_path: Optional[Path] = None
    current_fp = None
    try:
        while True:
            try:
                chunk = os.read(read_fd, 65536)
            except OSError:
                break
            if not chunk:
                break
            next_path = daily_sharded_log_path(base_path)
            if current_path != next_path:
                if current_fp is not None:
                    current_fp.flush()
                    current_fp.close()
                cleanup_old_daily_sharded_logs(base_path, retention_days=retention_days)
                next_path.parent.mkdir(parents=True, exist_ok=True)
                current_fp = next_path.open("ab", buffering=0)
                current_path = next_path
            current_fp.write(chunk)
    finally:
        if current_fp is not None:
            current_fp.flush()
            current_fp.close()
        os.close(read_fd)
