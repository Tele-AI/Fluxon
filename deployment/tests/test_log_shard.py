#!/usr/bin/env python3

from __future__ import annotations

import argparse
import datetime
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
DEPLOYMENT_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(DEPLOYMENT_DIR))

from utils import log_shard


def main() -> int:
    parser = argparse.ArgumentParser(description="log_shard util test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    args = parser.parse_args()

    checks = _build_checks(args.test_id)
    failures = 0
    for _, check in checks:
        try:
            check()
            print(f"PASS: {check.__name__}")
        except Exception as exc:
            print(f"FAIL: {check.__name__}: {exc}")
            failures += 1
    return 0 if failures == 0 else 1


def _build_checks(selected_test_id: Optional[str]) -> List[Tuple[str, Callable[[], None]]]:
    checks: List[Tuple[str, Callable[[], None]]] = [
        ("daily_path_uses_utc_date_suffix", test_daily_path_uses_utc_date_suffix),
        ("daily_path_uses_test_window_suffix_when_configured", test_daily_path_uses_test_window_suffix_when_configured),
        ("resolve_readable_prefers_latest_existing_shard", test_resolve_readable_prefers_latest_existing_shard),
        ("cleanup_keeps_only_retention_window", test_cleanup_keeps_only_retention_window),
    ]
    if selected_test_id is None:
        return checks
    for check_id, check in checks:
        if check_id == selected_test_id:
            return [(check_id, check)]
    available = ", ".join(check_id for check_id, _ in checks)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def test_daily_path_uses_utc_date_suffix() -> None:
    base = Path("/tmp/test_runner.log")
    now = datetime.datetime(2026, 6, 21, 4, 0, 0, tzinfo=datetime.timezone.utc)
    resolved = log_shard.daily_sharded_log_path(base, now=now)
    assert resolved.name == "test_runner.2026-06-21.log", resolved


def test_resolve_readable_prefers_latest_existing_shard() -> None:
    with tempfile.TemporaryDirectory(prefix="test_log_shard_resolve_") as td:
        root = Path(td)
        base = root / "service.log"
        (root / "service.2026-06-19.log").write_text("old\n", encoding="utf-8")
        (root / "service.2026-06-20.log").write_text("new\n", encoding="utf-8")
        resolved = log_shard.resolve_readable_log_path(base)
        assert resolved == (root / "service.2026-06-20.log").resolve(), resolved


def test_daily_path_uses_test_window_suffix_when_configured() -> None:
    base = Path("/tmp/test_runner.log")
    saved_window = os.environ.get(log_shard.TEST_LOG_SHARD_WINDOW_SECONDS_ENV)
    saved_anchor = os.environ.get(log_shard.TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV)
    try:
        os.environ[log_shard.TEST_LOG_SHARD_WINDOW_SECONDS_ENV] = "10"
        os.environ[log_shard.TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV] = str(
            int(datetime.datetime(2026, 6, 21, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp())
        )
        now_0 = datetime.datetime(2026, 6, 21, 0, 0, 5, tzinfo=datetime.timezone.utc)
        now_1 = datetime.datetime(2026, 6, 21, 0, 0, 15, tzinfo=datetime.timezone.utc)
        resolved_0 = log_shard.daily_sharded_log_path(base, now=now_0)
        resolved_1 = log_shard.daily_sharded_log_path(base, now=now_1)
        assert resolved_0.name == "test_runner.2026-01-01.log", resolved_0
        assert resolved_1.name == "test_runner.2026-01-02.log", resolved_1
    finally:
        if saved_window is None:
            os.environ.pop(log_shard.TEST_LOG_SHARD_WINDOW_SECONDS_ENV, None)
        else:
            os.environ[log_shard.TEST_LOG_SHARD_WINDOW_SECONDS_ENV] = saved_window
        if saved_anchor is None:
            os.environ.pop(log_shard.TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV, None)
        else:
            os.environ[log_shard.TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV] = saved_anchor


def test_cleanup_keeps_only_retention_window() -> None:
    with tempfile.TemporaryDirectory(prefix="test_log_shard_cleanup_") as td:
        root = Path(td)
        base = root / "service.log"
        keep_date = datetime.datetime.now(datetime.timezone.utc).date()
        old_date = keep_date - datetime.timedelta(days=31)
        recent_date = keep_date - datetime.timedelta(days=30)
        stale_path = root / f"service.{old_date.isoformat()}.log"
        recent_path = root / f"service.{recent_date.isoformat()}.log"
        today_path = root / f"service.{keep_date.isoformat()}.log"
        stale_path.write_text("stale\n", encoding="utf-8")
        recent_path.write_text("recent\n", encoding="utf-8")
        today_path.write_text("today\n", encoding="utf-8")
        log_shard.cleanup_old_daily_sharded_logs(base, retention_days=31)
        assert not stale_path.exists(), stale_path
        assert recent_path.exists(), recent_path
        assert today_path.exists(), today_path


if __name__ == "__main__":
    raise SystemExit(main())
