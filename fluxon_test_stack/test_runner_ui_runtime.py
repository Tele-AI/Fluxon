from __future__ import annotations

import datetime
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gitops import gitops_lib


def _resolve_repo_root_cli_path(*, repo_root: Path, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (repo_root / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _runner_stdio_mirror_enabled() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").strip().lower() == "true"


def _ci_log_timestamp_prefix(now: Optional[float] = None) -> str:
    ts = datetime.datetime.fromtimestamp(
        time.time() if now is None else float(now),
        tz=datetime.timezone.utc,
    )
    return ts.strftime("[%Y-%m-%d %H:%M:%S UTC]")


def _ci_log_prefix_lines(text: str, *, now: Optional[float] = None) -> str:
    if not text:
        return ""
    prefix = _ci_log_timestamp_prefix(now)
    lines = text.splitlines(keepends=True)
    return "".join(f"{prefix} {line}" if line.strip() else line for line in lines)


def _start_runner_stdio_log_mirror(*, log_path: Path, stdout_fd: int) -> threading.Thread:
    def _mirror_loop() -> None:
        offset = 0
        while True:
            try:
                if log_path.exists():
                    size = log_path.stat().st_size
                    if size < offset:
                        offset = 0
                    if size > offset:
                        with log_path.open("r", encoding="utf-8", errors="replace") as fp:
                            fp.seek(offset)
                            chunk = fp.read()
                            offset = fp.tell()
                        if chunk:
                            data = _ci_log_prefix_lines(chunk).encode("utf-8", errors="replace")
                            if stdout_fd >= 0:
                                try:
                                    os.write(stdout_fd, data)
                                except OSError:
                                    pass
                time.sleep(0.2)
            except Exception:
                time.sleep(0.5)

    mirror = threading.Thread(
        target=_mirror_loop,
        name="test-runner-stdio-log-mirror",
        daemon=True,
    )
    mirror.start()
    return mirror


def _redirect_process_stdio_to_log(
    *,
    workdir_root: Path,
    runner_stdio_log_filename: str,
    stdio_log_fp: Optional[Any],
    stdio_keepalive_fds: Optional[Tuple[int, int]],
    start_mirror,
) -> Tuple[Any, Optional[Tuple[int, int]]]:
    """Route runner stdio to a stable workdir log so long suites survive PTY loss."""
    if stdio_log_fp is not None:
        return stdio_log_fp, stdio_keepalive_fds

    log_path = (workdir_root / runner_stdio_log_filename).resolve()
    log_fp = log_path.open("a", encoding="utf-8", buffering=1)
    banner = (
        f"{_ci_log_timestamp_prefix()} [test_runner] redirecting process stdio to stable log: {log_path}\n"
    )
    try:
        sys.stdout.write(banner)
        sys.stdout.flush()
    except OSError:
        pass

    try:
        sys.stdout.flush()
    except OSError:
        pass
    try:
        sys.stderr.flush()
    except OSError:
        pass

    if stdio_keepalive_fds is None:
        try:
            out_fd = os.dup(sys.stdout.fileno())
            err_fd = os.dup(sys.stderr.fileno())
            os.set_inheritable(out_fd, False)
            os.set_inheritable(err_fd, False)
            stdio_keepalive_fds = (out_fd, err_fd)
        except OSError:
            stdio_keepalive_fds = (-1, -1)

    os.dup2(log_fp.fileno(), sys.stdout.fileno())
    os.dup2(log_fp.fileno(), sys.stderr.fileno())
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", encoding="utf-8", buffering=1, closefd=False)
    sys.stderr = os.fdopen(sys.stderr.fileno(), "w", encoding="utf-8", buffering=1, closefd=False)
    if _runner_stdio_mirror_enabled():
        keepalive = stdio_keepalive_fds or (-1, -1)
        start_mirror(
            log_path=log_path,
            stdout_fd=int(keepalive[0]),
        )
    return log_fp, stdio_keepalive_fds


def _resolve_history_roots_cli_paths(*, repo_root: Path, raw_paths: List[str]) -> List[Path]:
    return [
        _resolve_repo_root_cli_path(repo_root=repo_root, raw_path=Path(path), field_name="history_root")
        for path in raw_paths
    ]


def _load_gitops_ctx_for_ui(
    *,
    workdir_root: Path,
    gitops_config_path: Optional[Path],
) -> Optional[gitops_lib.GitOpsContext]:
    if gitops_config_path is None:
        return None
    gitops_workdir = gitops_lib.default_runtime_root(workdir_root)
    gitops_ctx = gitops_lib.load_context(
        config_path=gitops_config_path,
        workdir=gitops_workdir,
    )
    gitops_desc = gitops_lib.describe_context(gitops_ctx)
    print(
        "INFO: test_runner GitOps integrated: "
        f"config={gitops_desc['config_path']} workdir={gitops_desc['workdir']} interval={gitops_desc['interval']}s repos={gitops_desc['repo_count']}",
        flush=True,
    )
    threading.Thread(
        target=gitops_lib.poll_forever,
        args=(gitops_ctx,),
        kwargs={"stop_event": None},
        daemon=True,
    ).start()
    return gitops_ctx


def run_ui_service(
    *,
    workdir_root: Path,
    host: str,
    port: int,
    lookback_days: int,
    extra_history_roots: Optional[List[Path]],
    gitops_config_path: Optional[Path],
    acquire_ui_service_lock,
    serve_test_runner_ui,
) -> None:
    workdir_root = workdir_root.resolve()
    if workdir_root.exists():
        if not workdir_root.is_dir():
            raise ValueError(f"ui workdir is not a directory: {workdir_root}")
    else:
        workdir_root.mkdir(parents=True, exist_ok=True)
    ui_lock = acquire_ui_service_lock(workdir_root=workdir_root)
    _ = ui_lock
    gitops_ctx = _load_gitops_ctx_for_ui(
        workdir_root=workdir_root,
        gitops_config_path=gitops_config_path,
    )
    serve_test_runner_ui(
        workdir_root=workdir_root,
        host=str(host),
        port=int(port),
        lookback_days=int(lookback_days),
        extra_history_roots=extra_history_roots,
        gitops_ctx=gitops_ctx,
    )
