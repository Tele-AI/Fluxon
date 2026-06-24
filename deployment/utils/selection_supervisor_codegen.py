#!/usr/bin/env python3
"""
Code-generation helpers for the shared Python selection supervisor runtime.

This module is the single source of truth for generated selection_supervisor.py.
Bare generators and fluxon_ops both render from here; neither should own another
supervisor implementation.
"""

from __future__ import annotations

import textwrap


PYTHON_SELECTION_SUPERVISOR_FILENAME = "selection_supervisor.py"
LOG_SHARD_HELPER_FILENAME = "log_shard.py"


def render_python_selection_supervisor_module(*, timeouts) -> str:
    """Return a shared Python selection-supervisor runtime module.

    The generated module is the single selection-level lifecycle authority for:
    - `run`: publish one selection supervisor generation
    - `stop`: retire a selection by manual bare stop or directed apply_id stop

    Causal chain:
    - Bare bootstrap and ops-managed desired runtime must share one lifecycle authority.
    - File snapshots drift under overlap and restart, while live supervisor argv + proc state are
      the concrete authority.
    - Therefore ownership and stop both converge to process-command observation.
    """
    term_s = timeouts.term_seconds
    kill_s = timeouts.kill_seconds
    if not hasattr(timeouts, "supersede_seconds"):
        raise ValueError("timeouts.supersede_seconds is required")
    supersede_s = timeouts.supersede_seconds
    template = """\
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ctypes
import enum
import fcntl
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


STOP_TERM_TIMEOUT_SECONDS = __TERM_S__
STOP_KILL_TIMEOUT_SECONDS = __KILL_S__
SUPERVISOR_SUPERSEDE_SECONDS = __SUPERSEDE_S__
POLL_INTERVAL_SECONDS = 0.2
RETIRE_RUNTIME_STABLE_ABSENCE_SECONDS = 1.0
LONG_RUNNING_SELECTION_SUPERVISOR_COMMANDS = ("run", "replace")
SANITIZED_CHILD_ENV_KEYS = ("RDMAV_DRIVERS", "IBV_DRIVERS")

_shutdown_requested = False
_STDIO_ROUTER_THREAD = None
_STDIO_ROUTER_KEEPALIVE_FP = None


def _load_log_shard_helper():
    raw_file = globals().get("__file__")
    if not isinstance(raw_file, str) or not raw_file:
        raise RuntimeError("selection_supervisor.py requires __file__ to resolve log shard helper")
    helper_path = Path(raw_file).resolve().with_name("__LOG_SHARD_HELPER_FILENAME__")
    if not helper_path.is_file():
        raise RuntimeError(f"missing log shard helper next to selection_supervisor.py: {helper_path}")
    spec = importlib.util.spec_from_file_location("_fluxon_selection_log_shard", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load log shard helper: {helper_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_LOG_SHARD = _load_log_shard_helper()


def main() -> int:
    _install_subreaper()
    parser = argparse.ArgumentParser(description="Shared selection lifecycle supervisor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Publish one supervisor generation for a selection",
    )
    run_parser.add_argument("--label", required=True)
    run_parser.add_argument("--scope-key", required=False)
    run_parser.add_argument(
        "--restart-policy",
        required=True,
        choices=[policy.value for policy in RestartPolicy],
    )
    run_parser.add_argument("--restart-delay-seconds", type=int, required=True)
    run_parser.add_argument("--max-backoff-seconds", type=int, required=True)
    run_parser.add_argument("--crashloop-consecutive-restarts", type=int, required=True)
    run_parser.add_argument("--crashloop-interval-lt-seconds", type=int, required=True)
    run_parser.add_argument("--workdir", type=Path, required=False)
    run_parser.add_argument("--state-json", required=False)
    run_parser.add_argument("--owner-ts-ms", type=int, required=True)
    run_parser.add_argument("child_command", nargs=argparse.REMAINDER)

    stop_parser = subparsers.add_parser("stop", help="Stop a selection by label")
    stop_parser.add_argument("--label", required=True)
    stop_parser.add_argument("--scope-key", required=False)
    stop_parser.add_argument("--require-apply-id", required=False)
    stop_parser.add_argument("--missing-ok", action="store_true")

    args = parser.parse_args()
    runtime_state_for_stdio = _runtime_state_for_startup_stdio(args)
    _redirect_process_stdio_to_runtime_log(runtime_state_for_stdio)
    # English note:
    # - The supervisor module is invoked both as a long-running `run` daemon and as a short-lived
    #   `stop` helper from ops-managed reconcile loops.
    # - During self-host rollouts, the parent controller/agent process can restart while a helper
    #   subprocess is in flight, and that parent may send SIGTERM to its children.
    # - If we keep SIGTERM at the default behavior for helper subcommands, a benign controller
    #   restart turns into an opaque non-zero exit and blocks takeover.
    _install_signal_handlers()
    command = str(args.command)
    if command == "run":
        return _run_command(args)
    if command == "stop":
        return _stop_command(args)
    raise RuntimeError(f"unsupported command: {command}")


class RestartPolicy(enum.Enum):
    NEVER = "never"
    ALWAYS = "always"


@dataclass(frozen=True)
class ProcessInfo:
    pid: int
    ppid: int
    pgid: int
    state: str
    start_time_ticks: int

    @property
    def is_zombie(self) -> bool:
        return self.state == "Z"


@dataclass(frozen=True)
class SelectionRuntimeState:
    label: str
    kind: str
    name: str
    service_name: str
    apply_id: Optional[str]
    argv: List[str]
    cwd: Optional[str]
    log_path: str
    owner_ts_ms: int
    started_ts_ms: Optional[int]


@dataclass(frozen=True)
class RunCommandSpec:
    label: str
    scope_key: Optional[str]
    owner_ts_ms: int
    restart_policy: RestartPolicy
    restart_delay_seconds: int
    max_backoff_seconds: int
    crashloop_consecutive_restarts: int
    crashloop_interval_lt_seconds: int
    workdir: Optional[Path]
    runtime_state: Optional[SelectionRuntimeState]
    child_command: List[str]
    supervisor_command: str = "run"


@dataclass(frozen=True)
class LiveSupervisor:
    process_info: ProcessInfo
    owner_ts_ms: int
    label: str
    runtime_state: Optional[SelectionRuntimeState]
    args: List[str]

    @property
    def pid(self) -> int:
        return self.process_info.pid

    @property
    def ppid(self) -> int:
        return self.process_info.ppid

    @property
    def pgid(self) -> int:
        return self.process_info.pgid

    @property
    def start_time_ticks(self) -> int:
        return self.process_info.start_time_ticks


def _run_command(args: argparse.Namespace) -> int:
    spec = _parse_run_command_spec(args)
    selection_lock_fp = _acquire_selection_operation_lock(spec.label, spec.scope_key)
    return _run_supervisor(spec, selection_lock_fp=selection_lock_fp)


def _stop_command(args: argparse.Namespace) -> int:
    label = _require_non_empty_str(args.label, "label")
    scope_key = _require_optional_scope_key(args.scope_key)
    require_apply_id = (
        _require_non_empty_str(args.require_apply_id, "require-apply-id")
        if args.require_apply_id is not None
        else None
    )
    missing_ok = bool(args.missing_ok)
    selection_lock_fp = _acquire_selection_operation_lock(label, scope_key)
    try:
        _retire_selection(
            label=label,
            scope_key=scope_key,
            require_apply_id=require_apply_id,
            missing_ok=missing_ok,
        )
    finally:
        _release_selection_operation_lock(selection_lock_fp)
    return 0


def _require_positive_int(value: int, field_name: str) -> int:
    if value <= 0:
        raise RuntimeError(f"{field_name} must be positive")
    return value


def _require_non_negative_int(value: int, field_name: str) -> int:
    if value < 0:
        raise RuntimeError(f"{field_name} must be non-negative")
    return value


def _install_signal_handlers() -> None:
    def _on_shutdown(_signum: int, _frame) -> None:
        global _shutdown_requested
        _shutdown_requested = True

    signal.signal(signal.SIGTERM, _on_shutdown)
    signal.signal(signal.SIGINT, _on_shutdown)


def _install_subreaper() -> None:
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    pr_set_child_subreaper = 36
    if libc.prctl(pr_set_child_subreaper, 1, 0, 0, 0) != 0:
        err = ctypes.get_errno()
        raise RuntimeError(f"prctl(PR_SET_CHILD_SUBREAPER) failed errno={err}")


def _reap_terminated_children() -> List[tuple[int, int]]:
    out: List[tuple[int, int]] = []
    while True:
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return out
        if pid == 0:
            return out
        out.append((pid, status))


def _log_reaped_children(*, label: str, reason: str, reaped: List[tuple[int, int]]) -> None:
    if not reaped:
        return
    pid_text = ",".join(str(pid) for pid, _ in reaped)
    print(
        f"[selection-supervisor] reaped child processes label={label} reason={reason} pids=[{pid_text}]",
        file=sys.stderr,
    )


def _sleep_with_shutdown(seconds: int) -> None:
    deadline = time.time() + float(seconds)
    while time.time() < deadline:
        _log_reaped_children(
            label="sleep",
            reason="idle",
            reaped=_reap_terminated_children(),
        )
        if _shutdown_requested:
            return
        time.sleep(POLL_INTERVAL_SECONDS)


def _scope_lock_suffix(scope_key: Optional[str]) -> str:
    if scope_key is None:
        return "global"
    return scope_key


def _selection_operation_lock_path(label: str, scope_key: Optional[str]) -> Path:
    lock_root = Path("/tmp/fluxon_selection_supervisor_locks")
    lock_root.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(f"{label}\\n{_scope_lock_suffix(scope_key)}".encode("utf-8")).hexdigest()[:24]
    return lock_root / f"{digest}.op.lock"


def _acquire_selection_operation_lock(label: str, scope_key: Optional[str]):
    lock_path = _selection_operation_lock_path(label, scope_key)
    lock_fp = lock_path.open("a+", encoding="utf-8")
    fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
    return lock_fp


def _release_selection_operation_lock(lock_fp) -> None:
    fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)
    lock_fp.close()


def _parse_run_command_spec(args: argparse.Namespace) -> RunCommandSpec:
    supervisor_command = _require_non_empty_str(args.command, "command")
    if supervisor_command != "run":
        raise RuntimeError(
            f"run-command spec only supports long-running commands: {supervisor_command}"
        )
    label = _require_non_empty_str(args.label, "label")
    scope_key = _require_optional_scope_key(args.scope_key)
    owner_ts_ms = _require_positive_int(int(args.owner_ts_ms), "owner-ts-ms")
    restart_policy = RestartPolicy(str(args.restart_policy))
    restart_delay_seconds = _require_non_negative_int(args.restart_delay_seconds, "restart-delay-seconds")
    max_backoff_seconds = _require_non_negative_int(args.max_backoff_seconds, "max-backoff-seconds")
    crashloop_consecutive_restarts = _require_non_negative_int(
        args.crashloop_consecutive_restarts,
        "crashloop-consecutive-restarts",
    )
    crashloop_interval_lt_seconds = _require_non_negative_int(
        args.crashloop_interval_lt_seconds,
        "crashloop-interval-lt-seconds",
    )
    if restart_policy is RestartPolicy.ALWAYS and restart_delay_seconds <= 0:
        raise RuntimeError("restart-delay-seconds must be positive when restart-policy=always")
    if restart_policy is RestartPolicy.ALWAYS and max_backoff_seconds <= 0:
        raise RuntimeError("max-backoff-seconds must be positive when restart-policy=always")
    child_command = list(args.child_command)
    if child_command and child_command[0] == "--":
        child_command = child_command[1:]
    if not child_command:
        raise RuntimeError("run requires child command after '--'")

    workdir: Optional[Path] = None
    if args.workdir is not None:
        workdir = _resolve_cli_path(args.workdir)
        workdir.mkdir(parents=True, exist_ok=True)

    return RunCommandSpec(
        label=label,
        scope_key=scope_key,
        owner_ts_ms=owner_ts_ms,
        restart_policy=restart_policy,
        restart_delay_seconds=restart_delay_seconds,
        max_backoff_seconds=max_backoff_seconds,
        crashloop_consecutive_restarts=crashloop_consecutive_restarts,
        crashloop_interval_lt_seconds=crashloop_interval_lt_seconds,
        workdir=workdir,
        runtime_state=_build_runtime_state(
            label=label,
            state_json=args.state_json,
        ),
        child_command=child_command,
        supervisor_command=supervisor_command,
    )


def _runtime_state_for_startup_stdio(args: argparse.Namespace) -> Optional[SelectionRuntimeState]:
    if str(args.command) != "run":
        return None
    label = _require_non_empty_str(args.label, "label")
    state_json = args.state_json
    if state_json is None:
        return None
    return _build_runtime_state(label=label, state_json=state_json)


def _requested_phase1_overlap_with_applyless_owner(
    current_owner: Optional[LiveSupervisor],
    requested_runtime_state: Optional[SelectionRuntimeState],
    requested_owner_ts_ms: int,
) -> bool:
    # English note:
    # - Bare-then-apply self-host bootstrap is a two-phase handover.
    # - Phase 1 publishes the newer apply-owned generation and lets it become observable while the
    #   older applyless bare owner keeps the control plane alive.
    # - Phase 2 is an explicit coordinated cutover outside the supervisor; only then should the
    #   old launch-only bare owner be retired immediately.
    # - The selection supervisor therefore treats "new applied generation over older applyless
    #   owner" as an overlap candidate, not as permission to eagerly SIGTERM the old owner.
    if current_owner is None:
        return False
    if requested_runtime_state is None or requested_runtime_state.apply_id is None:
        return False
    if current_owner.runtime_state is None:
        return False
    return (
        current_owner.runtime_state.apply_id is None
        and requested_owner_ts_ms > current_owner.owner_ts_ms
    )


def _run_supervisor(spec: RunCommandSpec, selection_lock_fp=None) -> int:
    _ensure_isolated_process_group(spec.label)
    runtime_state: Optional[SelectionRuntimeState]
    supersede_started_at: Optional[float] = None
    try:
        runtime_state = _bind_runtime_state_owner_ts(
            runtime_state=spec.runtime_state,
            owner_ts_ms=spec.owner_ts_ms,
        )
        current_owner_ts_ms = spec.owner_ts_ms
        current_owner = _selection_owner_supervisor(
            spec.label,
            scope_key=spec.scope_key,
            exclude_pid=os.getpid(),
        )
        if current_owner is not None:
            phase1_overlap_with_applyless_owner = _requested_phase1_overlap_with_applyless_owner(
                current_owner=current_owner,
                requested_runtime_state=runtime_state,
                requested_owner_ts_ms=current_owner_ts_ms,
            )
            if current_owner.owner_ts_ms > current_owner_ts_ms and not phase1_overlap_with_applyless_owner:
                raise RuntimeError(
                    f"requested generation is superseded label={spec.label} "
                    f"requested_owner_ts_ms={current_owner_ts_ms} current_owner_ts_ms={current_owner.owner_ts_ms}"
                )
            if current_owner.owner_ts_ms == current_owner_ts_ms and not phase1_overlap_with_applyless_owner:
                raise RuntimeError(
                    f"replace generation must advance before taking over selection authority "
                    f"label={spec.label} owner_ts_ms={current_owner_ts_ms}"
                )
            if phase1_overlap_with_applyless_owner:
                print(
                    f"[selection-supervisor] allow phase1 overlap with applyless owner "
                    f"label={spec.label} requested_owner_ts_ms={current_owner_ts_ms} "
                    f"current_owner_ts_ms={current_owner.owner_ts_ms}",
                    file=sys.stderr,
                )
                print(
                    f"[selection-supervisor] defer applyless-owner retire until coordinated cutover "
                    f"or supersede fallback label={spec.label} current_pid={current_owner.pid}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"[selection-supervisor] publish newer shared authority generation without eager retire "
                    f"label={spec.label} current_pid={current_owner.pid} "
                    f"current_apply_id={current_owner.runtime_state.apply_id if current_owner.runtime_state is not None else None}",
                    file=sys.stderr,
                )
    finally:
        if selection_lock_fp is not None:
            _release_selection_operation_lock(selection_lock_fp)
            selection_lock_fp = None

    restart_timestamps: List[float] = []
    backoff_seconds = spec.restart_delay_seconds
    _redirect_process_stdio_to_runtime_log(runtime_state)

    while True:
        _log_reaped_children(
            label=spec.label,
            reason="loop",
            reaped=_reap_terminated_children(),
        )
        latest_owner_ts_ms = _latest_owner_ts_ms(spec.label, scope_key=spec.scope_key)
        superseded = latest_owner_ts_ms is not None and latest_owner_ts_ms > current_owner_ts_ms
        if superseded:
            if supersede_started_at is None:
                supersede_started_at = time.time()
                print(
                    f"[selection-supervisor] idle generation superseded label={spec.label} "
                    f"owner_ts_ms={current_owner_ts_ms} latest_owner_ts_ms={latest_owner_ts_ms} "
                    f"retire_in_seconds={SUPERVISOR_SUPERSEDE_SECONDS}",
                    file=sys.stderr,
                )
            if time.time() - supersede_started_at >= float(SUPERVISOR_SUPERSEDE_SECONDS):
                print(
                    f"[selection-supervisor] supersede retire complete label={spec.label} "
                    f"owner_ts_ms={current_owner_ts_ms}",
                    file=sys.stderr,
                )
                return 0
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        supersede_started_at = None
        if _shutdown_requested:
            return 0
        child = _spawn_child(spec.child_command, spec.workdir)
        rc, exited_due_to_supersede = _wait_child(
            child,
            label=spec.label,
            scope_key=spec.scope_key,
            current_owner_ts_ms=current_owner_ts_ms,
        )
        _retire_adopted_children(spec.label)
        if exited_due_to_supersede:
            print(
                f"[selection-supervisor] supersede retire complete label={spec.label} "
                f"owner_ts_ms={current_owner_ts_ms}",
                file=sys.stderr,
            )
            return 0
        if _shutdown_requested:
            return 0
        if spec.restart_policy is RestartPolicy.NEVER:
            return rc

        now = time.time()
        restart_timestamps.append(now)
        if spec.crashloop_consecutive_restarts > 0:
            restart_timestamps = restart_timestamps[-spec.crashloop_consecutive_restarts:]
            if len(restart_timestamps) == spec.crashloop_consecutive_restarts:
                deltas = [
                    restart_timestamps[idx] - restart_timestamps[idx - 1]
                    for idx in range(1, len(restart_timestamps))
                ]
                if deltas and all(delta < float(spec.crashloop_interval_lt_seconds) for delta in deltas):
                    print(
                        f"[selection-supervisor] crashloop detected label={spec.label} "
                        f"restarts={spec.crashloop_consecutive_restarts} interval_lt_seconds="
                        f"{spec.crashloop_interval_lt_seconds} rc={rc}",
                        file=sys.stderr,
                    )
                    return 1

        print(
            f"[selection-supervisor] child exited label={spec.label} rc={rc} "
            f"restart_in_seconds={backoff_seconds}",
            file=sys.stderr,
        )
        _sleep_with_shutdown(backoff_seconds)
        if _shutdown_requested:
            return 0
        if backoff_seconds < spec.max_backoff_seconds:
            backoff_seconds = min(backoff_seconds * 2, spec.max_backoff_seconds)


def _ensure_isolated_process_group(label: str) -> None:
    # English note:
    # - `stop` is allowed to terminate a selection by process tree or PGID.
    # - Therefore the long-lived authority process must own an isolated process group.
    pid = os.getpid()
    pgid = os.getpgid(0)
    if pgid != pid:
        try:
            os.setpgid(0, 0)
        except OSError as exc:
            raise RuntimeError(
                f"failed to create isolated process group for selection supervisor label={label}: {exc}"
            ) from exc
        if os.getpgid(0) != pid:
            raise RuntimeError(
                f"failed to create isolated process group for selection supervisor label={label}: "
                f"pid={pid} pgid_before={pgid} pgid_after={os.getpgid(0)}"
            )


def _retire_selection(
    *,
    label: str,
    scope_key: Optional[str],
    require_apply_id: Optional[str],
    missing_ok: bool,
) -> bool:
    label_live_supervisors = _matching_live_supervisors_for_stop(
        label=label,
        scope_key=scope_key,
        require_apply_id=None,
    )
    matching = _matching_live_supervisors_for_stop(
        label=label,
        scope_key=scope_key,
        require_apply_id=require_apply_id,
    )
    if not matching:
        if require_apply_id is not None:
            if label_live_supervisors:
                live_apply_ids = sorted(
                    {
                        supervisor.runtime_state.apply_id
                        for supervisor in label_live_supervisors
                        if supervisor.runtime_state is not None
                        and supervisor.runtime_state.apply_id is not None
                    }
                )
                raise RuntimeError(
                    f"stop apply_id target is absent label={label} "
                    f"require_apply_id={require_apply_id} live_apply_ids={live_apply_ids}"
                )
        if missing_ok:
            print(
                f"[selection-supervisor] stop: already absent label={label} "
                f"require_apply_id={require_apply_id}",
                file=sys.stderr,
            )
            return False
        if require_apply_id is not None:
            raise RuntimeError(
                f"stop apply_id target is absent label={label} require_apply_id={require_apply_id}"
            )
        raise RuntimeError(f"stop requires a live selection owner: {label}")

    retired_pgids: set[int] = set()
    root_pids: List[int] = []
    for supervisor in matching:
        root_pids.append(supervisor.pid)
        if supervisor.pgid > 0:
            retired_pgids.add(supervisor.pgid)
        print(
            f"[selection-supervisor] stop: retire live supervisor label={label} "
            f"apply_id={supervisor.runtime_state.apply_id if supervisor.runtime_state is not None else None} "
            f"pid={supervisor.pid} pgid={supervisor.pgid} owner_ts_ms={supervisor.owner_ts_ms}",
            file=sys.stderr,
        )
    _stop_pid_tree_batch(sorted(set(root_pids)), label)
    _wait_supervisors_absent(
        label=label,
        scope_key=scope_key,
        require_apply_id=require_apply_id,
        retired_pgids=retired_pgids,
    )
    print(
        f"[selection-supervisor] stop: retired label={label} require_apply_id={require_apply_id}",
        file=sys.stderr,
    )
    return True


def _bind_runtime_state_owner_ts(
    *,
    runtime_state: Optional[SelectionRuntimeState],
    owner_ts_ms: int,
) -> Optional[SelectionRuntimeState]:
    if runtime_state is None:
        return None
    return SelectionRuntimeState(
        label=runtime_state.label,
        kind=runtime_state.kind,
        name=runtime_state.name,
        service_name=runtime_state.service_name,
        apply_id=runtime_state.apply_id,
        argv=runtime_state.argv,
        cwd=runtime_state.cwd,
        log_path=runtime_state.log_path,
        owner_ts_ms=owner_ts_ms,
        started_ts_ms=runtime_state.started_ts_ms,
    )


def _latest_owner_ts_ms(label: str, *, scope_key: Optional[str] = None) -> Optional[int]:
    owners = _iter_live_supervisors(label, scope_key=scope_key)
    if not owners:
        return None
    return max(supervisor.owner_ts_ms for supervisor in owners)


def _path_contains_fluxon_pyo3_libs_dir(path: Path) -> bool:
    return "fluxon_pyo3.libs" in path.parts


def _sanitize_child_ld_library_path(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    sanitized_entries: List[str] = []
    seen_entries: set[str] = set()
    for raw_entry in raw_value.split(":"):
        entry = raw_entry.strip()
        if not entry:
            continue
        if entry in seen_entries:
            continue
        if _path_contains_fluxon_pyo3_libs_dir(Path(entry)):
            continue
        seen_entries.add(entry)
        sanitized_entries.append(entry)
    if not sanitized_entries:
        return None
    return ":".join(sanitized_entries)


def _expand_runtime_state_path(value: str) -> str:
    return os.path.expandvars(value)


def _spawn_child(command: List[str], workdir: Optional[Path]) -> subprocess.Popen[bytes]:
    def _set_pdeathsig_sigterm() -> None:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        pr_set_pdeathsig = 1
        if libc.prctl(pr_set_pdeathsig, int(signal.SIGTERM)) != 0:
            err = ctypes.get_errno()
            raise RuntimeError(f"prctl(PR_SET_PDEATHSIG) failed errno={err}")
        if os.getppid() == 1:
            os.kill(os.getpid(), signal.SIGTERM)

    child_env = os.environ.copy()
    for env_key in SANITIZED_CHILD_ENV_KEYS:
        child_env.pop(env_key, None)
    sanitized_ld_library_path = _sanitize_child_ld_library_path(child_env.get("LD_LIBRARY_PATH"))
    if sanitized_ld_library_path is None:
        child_env.pop("LD_LIBRARY_PATH", None)
    else:
        child_env["LD_LIBRARY_PATH"] = sanitized_ld_library_path
    return subprocess.Popen(
        command,
        cwd=str(workdir) if workdir is not None else None,
        env=child_env,
        preexec_fn=_set_pdeathsig_sigterm,
    )


def _redirect_process_stdio_to_runtime_log(runtime_state: Optional[SelectionRuntimeState]) -> None:
    global _STDIO_ROUTER_THREAD
    global _STDIO_ROUTER_KEEPALIVE_FP
    if runtime_state is None:
        return
    if _STDIO_ROUTER_THREAD is not None:
        return
    base_log_path = _require_non_empty_str(runtime_state.log_path, "state.log_path")
    read_fd, write_fd = os.pipe()
    router_keepalive = os.dup(write_fd)

    def _router_loop() -> None:
        _LOG_SHARD.relay_fd_to_daily_sharded_logs(
            base_log_path=base_log_path,
            read_fd=read_fd,
            retention_days=_LOG_SHARD.DEFAULT_DAILY_LOG_RETENTION_DAYS,
        )

    router = threading.Thread(
        target=_router_loop,
        name="selection-supervisor-stdio-log-router",
        daemon=True,
    )
    router.start()
    os.dup2(write_fd, sys.stdout.fileno())
    os.dup2(write_fd, sys.stderr.fileno())
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", encoding="utf-8", buffering=1, closefd=False)
    sys.stderr = os.fdopen(sys.stderr.fileno(), "w", encoding="utf-8", buffering=1, closefd=False)
    try:
        os.close(write_fd)
    except OSError:
        pass
    _STDIO_ROUTER_KEEPALIVE_FP = os.fdopen(router_keepalive, "w", encoding="utf-8", buffering=1)
    _STDIO_ROUTER_THREAD = router
def _retired_and_preserved_adopted_roots(root_pid: int) -> Tuple[List[int], List[int]]:
    adopted_roots = _direct_live_child_pids(root_pid)
    if not adopted_roots:
        return [], []
    live_supervisor_pids = {
        supervisor.pid
        for supervisor in _iter_live_supervisors()
        if supervisor.pid != root_pid
    }
    retired_roots = [pid for pid in adopted_roots if pid not in live_supervisor_pids]
    preserved_roots = [pid for pid in adopted_roots if pid in live_supervisor_pids]
    return retired_roots, preserved_roots


def _retire_adopted_children(label: str) -> None:
    retired_roots, preserved_roots = _retired_and_preserved_adopted_roots(os.getpid())
    if preserved_roots:
        print(
            f"[selection-supervisor] preserve adopted live supervisor roots label={label} "
            f"root_pids={preserved_roots}",
            file=sys.stderr,
        )
    if retired_roots:
        print(
            f"[selection-supervisor] retire adopted children label={label} root_pids={retired_roots}",
            file=sys.stderr,
        )
        _stop_pid_tree_batch(retired_roots, f"adopted:{label}")
    _log_reaped_children(
        label=label,
        reason="adopted",
        reaped=_reap_terminated_children(),
    )


def _wait_child(
    child: subprocess.Popen[bytes],
    *,
    label: str,
    scope_key: Optional[str],
    current_owner_ts_ms: int,
) -> Tuple[int, bool]:
    supersede_started_at: Optional[float] = None
    while True:
        latest_owner_ts_ms = _latest_owner_ts_ms(label, scope_key=scope_key)
        superseded = latest_owner_ts_ms is not None and latest_owner_ts_ms > current_owner_ts_ms
        rc = child.poll()
        if rc is not None:
            if superseded:
                print(
                    f"[selection-supervisor] superseded child exited without restart label={label} "
                    f"owner_ts_ms={current_owner_ts_ms} latest_owner_ts_ms={latest_owner_ts_ms} rc={rc}",
                    file=sys.stderr,
                )
                return 0, True
            return rc, False
        if superseded:
            if supersede_started_at is None:
                supersede_started_at = time.time()
                print(
                    f"[selection-supervisor] running generation superseded label={label} "
                    f"owner_ts_ms={current_owner_ts_ms} latest_owner_ts_ms={latest_owner_ts_ms} "
                    f"retire_in_seconds={SUPERVISOR_SUPERSEDE_SECONDS}",
                    file=sys.stderr,
                )
            if time.time() - supersede_started_at >= float(SUPERVISOR_SUPERSEDE_SECONDS):
                # English note:
                # - Fast cutover should happen through the explicit phase-2 retire path after the
                #   whole atomic group has confirmed the requested generation.
                # - This timeout path is only the slow fallback when that coordinated cutover does
                #   not arrive, so overlapping generations still converge eventually.
                _stop_pid_tree(child.pid, f"superseded:{label}")
                return 0, True
        else:
            supersede_started_at = None
        if _shutdown_requested:
            _stop_pid_tree(child.pid, f"child:{child.pid}")
            return 0, False
        time.sleep(POLL_INTERVAL_SECONDS)
def _resolve_cli_path(value: Path) -> Path:
    if value.is_absolute():
        return value.resolve()
    raw_file = globals().get("__file__")
    if not isinstance(raw_file, str) or not raw_file:
        raise RuntimeError(f"relative path requires __file__: {value}")
    return (Path(raw_file).resolve().parent / value).resolve()
def _selection_runtime_state_from_raw(
    *,
    raw: Dict[str, object],
    label: str,
    owner_ts_ms: int,
    started_ts_ms: Optional[int],
) -> SelectionRuntimeState:
    return SelectionRuntimeState(
        label=label,
        kind=_require_non_empty_str(raw.get("kind"), "state.kind"),
        name=_require_non_empty_str(raw.get("name"), "state.name"),
        service_name=_require_non_empty_str(raw.get("service_name"), "state.service_name"),
        apply_id=_require_optional_non_empty_str(raw.get("apply_id"), "state.apply_id"),
        argv=_require_non_empty_str_list(raw.get("argv"), "state.argv"),
        cwd=_require_optional_non_empty_str(raw.get("cwd"), "state.cwd"),
        log_path=_expand_runtime_state_path(
            _require_non_empty_str(raw.get("log_path"), "state.log_path")
        ),
        owner_ts_ms=owner_ts_ms,
        started_ts_ms=started_ts_ms,
    )


def _build_runtime_state(
    *,
    label: str,
    state_json: Optional[str],
) -> Optional[SelectionRuntimeState]:
    if state_json is None:
        return None
    try:
        raw = json.loads(state_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid state-json: {exc}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("state-json must decode to an object")
    return _selection_runtime_state_from_raw(
        raw=raw,
        label=label,
        owner_ts_ms=0,
        started_ts_ms=int(time.time() * 1000),
    )


def _parse_positive_int(raw: str, error_message: str) -> int:
    value = int(raw)
    if value <= 0:
        raise RuntimeError(error_message)
    return value


def _require_int(value: object, field_name: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f"{field_name} must be an int")
    return value


def _require_non_empty_str(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{field_name} must be a string")
    trimmed = value.strip()
    if not trimmed:
        raise RuntimeError(f"{field_name} must be non-empty")
    return trimmed


def _require_non_empty_str_preserve(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"{field_name} must be a string")
    if not value.strip():
        raise RuntimeError(f"{field_name} must be non-empty")
    return value


def _require_optional_non_empty_str(value: object, field_name: str) -> Optional[str]:
    if value is None:
        return None
    return _require_non_empty_str(value, field_name)


def _require_optional_scope_key(value: object) -> Optional[str]:
    if value is None:
        return None
    return _require_non_empty_str(value, "scope-key")


def _require_non_empty_str_list(value: object, field_name: str) -> List[str]:
    if not isinstance(value, list):
        raise RuntimeError(f"{field_name} must be a list")
    out: List[str] = []
    for idx, item in enumerate(value):
        out.append(_require_non_empty_str_preserve(item, f"{field_name}[{idx}]"))
    if not out:
        raise RuntimeError(f"{field_name} must be non-empty")
    return out


def _require_list_of_str(value: object, field_name: str) -> List[str]:
    if not isinstance(value, list):
        raise RuntimeError(f"{field_name} must be a list")
    out: List[str] = []
    for idx, item in enumerate(value):
        out.append(_require_non_empty_str(item, f"{field_name}[{idx}]"))
    return out


def _safe_getpgid(pid: int) -> Optional[int]:
    try:
        return os.getpgid(pid)
    except ProcessLookupError:
        return None


def _read_parent_pid(pid: int) -> Optional[int]:
    for info in _iter_process_infos():
        if info.pid != pid:
            continue
        if info.ppid <= 0:
            return None
        return info.ppid
    return None


def _find_process_info(pid: int) -> Optional[ProcessInfo]:
    for info in _iter_process_infos():
        if info.pid == pid:
            return info
    return None


def _iter_process_cmdlines() -> List[tuple[int, List[str]]]:
    out: List[tuple[int, List[str]]] = []
    proc_dir = Path("/proc")
    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        cmdline_path = entry / "cmdline"
        try:
            if not cmdline_path.exists():
                continue
            raw = cmdline_path.read_bytes()
        except (FileNotFoundError, ProcessLookupError, OSError):
            continue
        if not raw:
            continue
        args = [chunk.decode("utf-8", errors="ignore") for chunk in raw.split(b"\\0") if chunk]
        if not args:
            continue
        out.append((pid, args))
    return out


def _iter_process_snapshots() -> List[tuple[ProcessInfo, List[str]]]:
    infos_by_pid = {info.pid: info for info in _iter_process_infos()}
    out: List[tuple[ProcessInfo, List[str]]] = []
    for pid, args in _iter_process_cmdlines():
        process_info = infos_by_pid.get(pid)
        if process_info is None or process_info.is_zombie:
            continue
        out.append((process_info, args))
    return out


def _arg_value(args: List[str], flag: str) -> Optional[str]:
    for idx, arg in enumerate(args[:-1]):
        if arg == flag:
            return args[idx + 1]
    return None


def _command_runtime_state(args: List[str]) -> Optional[Dict[str, object]]:
    raw_state_json = _arg_value(args, "--state-json")
    if raw_state_json is None:
        return None
    try:
        raw = json.loads(raw_state_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid running supervisor state-json: {exc}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("running supervisor state-json must decode to an object")
    return raw


def _find_selection_supervisor_command(args: List[str]) -> Optional[str]:
    # English note:
    # - `/proc/<pid>/cmdline` contains many unrelated commands whose argv can include a
    #   bare `run` token, for example `docker run ...`.
    # - Supervisor-only flags such as `--state-json` must therefore only be parsed after we
    #   have identified the concrete `selection_supervisor.py` entrypoint in argv.
    for idx, arg in enumerate(args[:4]):
        if Path(arg).name != "selection_supervisor.py":
            continue
        if idx + 1 >= len(args):
            return None
        command = args[idx + 1]
        if command in LONG_RUNNING_SELECTION_SUPERVISOR_COMMANDS:
            return command
        return None
    return None


def _live_runtime_state_from_command(
    args: List[str],
    label: str,
    owner_ts_ms: int,
) -> Optional[SelectionRuntimeState]:
    raw_state = _command_runtime_state(args)
    if raw_state is None:
        return None
    return _selection_runtime_state_from_raw(
        raw=raw_state,
        label=label,
        owner_ts_ms=owner_ts_ms,
        started_ts_ms=None,
    )


def _matching_live_supervisors_for_stop(
    *,
    label: str,
    scope_key: Optional[str],
    require_apply_id: Optional[str],
) -> List[LiveSupervisor]:
    if require_apply_id is not None:
        out: List[LiveSupervisor] = []
        for supervisor in _iter_live_supervisors(label, scope_key=scope_key):
            runtime_state = supervisor.runtime_state
            if runtime_state is None or runtime_state.apply_id != require_apply_id:
                continue
            out.append(supervisor)
        out.sort(key=_supervisor_sort_key, reverse=True)
        return out

    out: List[LiveSupervisor] = []
    for supervisor in _iter_live_supervisors(label, scope_key=scope_key):
        out.append(supervisor)
    out.sort(key=_supervisor_sort_key, reverse=True)
    return out


def _wait_supervisors_absent(
    *,
    label: str,
    scope_key: Optional[str],
    require_apply_id: Optional[str],
    retired_pgids: set[int],
) -> None:
    stable_absence_started_at: Optional[float] = None
    while True:
        remaining = _matching_live_supervisors_for_stop(
            label=label,
            scope_key=scope_key,
            require_apply_id=require_apply_id,
        )
        lingering = [
            supervisor
            for supervisor in remaining
            if not retired_pgids or supervisor.pgid in retired_pgids
        ]
        if lingering:
            stable_absence_started_at = None
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        if stable_absence_started_at is None:
            stable_absence_started_at = time.time()
        if time.time() - stable_absence_started_at >= float(RETIRE_RUNTIME_STABLE_ABSENCE_SECONDS):
            return
        time.sleep(POLL_INTERVAL_SECONDS)


def _iter_process_infos() -> List[ProcessInfo]:
    infos: List[ProcessInfo] = []
    proc_dir = Path("/proc")
    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        stat_path = entry / "stat"
        try:
            raw = stat_path.read_text(encoding="utf-8")
        except (FileNotFoundError, ProcessLookupError, OSError):
            continue
        rparen = raw.rfind(")")
        if rparen < 0:
            continue
        head = raw[: rparen + 1]
        tail = raw[rparen + 2 :].split()
        if len(tail) < 20:
            continue
        pid = int(head.split(" ", 1)[0])
        state = str(tail[0])
        ppid = int(tail[1])
        pgid = int(tail[2])
        start_time_ticks = int(tail[19])
        infos.append(
            ProcessInfo(
                pid=pid,
                ppid=ppid,
                pgid=pgid,
                state=state,
                start_time_ticks=start_time_ticks,
            )
        )
    return infos


def _iter_live_supervisors(label: Optional[str] = None, *, scope_key: Optional[str] = None) -> List[LiveSupervisor]:
    out: List[LiveSupervisor] = []
    for process_info, args in _iter_process_snapshots():
        supervisor_command = _find_selection_supervisor_command(args)
        if supervisor_command is None:
            continue
        pid = process_info.pid
        runtime_label = _arg_value(args, "--label")
        if runtime_label is None:
            raise RuntimeError(f"running selection supervisor is missing --label pid={pid}")
        if label is not None and runtime_label != label:
            continue
        runtime_scope_key = _arg_value(args, "--scope-key")
        if scope_key is not None and runtime_scope_key != scope_key:
            continue
        owner_ts_ms_raw = _arg_value(args, "--owner-ts-ms")
        if owner_ts_ms_raw is None:
            if supervisor_command == "replace":
                owner_ts_ms = process_info.start_time_ticks
            else:
                raise RuntimeError(
                    f"running selection supervisor is missing --owner-ts-ms pid={pid} label={runtime_label}"
                )
        else:
            owner_ts_ms = _parse_positive_int(owner_ts_ms_raw, "invalid running supervisor owner_ts_ms")
        runtime_state = _live_runtime_state_from_command(
            args,
            runtime_label,
            owner_ts_ms,
        )
        out.append(
            LiveSupervisor(
                process_info=process_info,
                owner_ts_ms=owner_ts_ms,
                label=runtime_label,
                runtime_state=runtime_state,
                args=args,
            )
        )
    return out


def _supervisor_sort_key(supervisor: LiveSupervisor) -> Tuple[int, int, int, int]:
    return (supervisor.owner_ts_ms, 0, 0, 0)


def _selection_owner_supervisor(
    label: str,
    *,
    scope_key: Optional[str] = None,
    exclude_pid: Optional[int] = None,
) -> Optional[LiveSupervisor]:
    owners = [
        supervisor
        for supervisor in _iter_live_supervisors(label, scope_key=scope_key)
        if exclude_pid is None or supervisor.pid != exclude_pid
    ]
    if not owners:
        return None
    owner_ts_ms = max(_supervisor_sort_key(supervisor)[0] for supervisor in owners)
    matching = [supervisor for supervisor in owners if supervisor.owner_ts_ms == owner_ts_ms]
    if len(matching) > 1:
        details = ", ".join(
            f"pid={supervisor.pid} runtime_state={supervisor.runtime_state is not None}"
            for supervisor in matching
        )
        raise RuntimeError(
            f"selection supervisor owner_ts_ms collision label={label} owner_ts_ms={owner_ts_ms} matches=[{details}]"
        )
    return matching[0]


def _selection_present(label: str, scope_key: Optional[str] = None) -> bool:
    for supervisor in _iter_live_supervisors(label, scope_key=scope_key):
        if _count_pid_tree_members(supervisor.pid) > 1:
            return True
    return False


def _count_process_group_members(pgid: int) -> int:
    return sum(1 for info in _iter_process_infos() if info.pgid == pgid and not info.is_zombie)


def _children_by_ppid() -> Dict[int, List[int]]:
    infos = _iter_process_infos()
    out: Dict[int, List[int]] = {}
    for info in infos:
        if info.is_zombie:
            continue
        out.setdefault(info.ppid, []).append(info.pid)
    return out


def _direct_live_child_pids(root_pid: int) -> List[int]:
    infos = _iter_process_infos()
    out: List[int] = []
    for info in infos:
        if info.is_zombie:
            continue
        if info.ppid != root_pid:
            continue
        out.append(info.pid)
    out.sort()
    return out


def _count_pid_tree_members(root_pid: int) -> int:
    return len(_list_pid_tree(root_pid))


def _list_pid_tree(root_pid: int) -> List[int]:
    infos = _iter_process_infos()
    info_by_pid = {info.pid: info for info in infos}
    if root_pid not in info_by_pid or info_by_pid[root_pid].is_zombie:
        return []
    children_by_ppid: Dict[int, List[int]] = {}
    for info in infos:
        if info.is_zombie:
            continue
        children_by_ppid.setdefault(info.ppid, []).append(info.pid)

    out: List[int] = []
    queue: List[int] = [root_pid]
    seen = {root_pid}
    while queue:
        current = queue.pop(0)
        if current not in info_by_pid or info_by_pid[current].is_zombie:
            continue
        out.append(current)
        for child_pid in children_by_ppid.get(current, []):
            if child_pid in seen:
                continue
            seen.add(child_pid)
            queue.append(child_pid)
    return out


def _stop_process_group(pgid: int, label: str) -> None:
    _signal_process_group(pgid, signal.SIGTERM, label)
    deadline = time.time() + float(STOP_TERM_TIMEOUT_SECONDS)
    while time.time() < deadline:
        if _count_process_group_members(pgid) == 0:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    print(
        f"[selection-supervisor] term timeout exceeded; escalating to SIGKILL "
        f"label={label} pgid={pgid} grace_seconds={STOP_TERM_TIMEOUT_SECONDS}",
        file=sys.stderr,
    )
    _signal_process_group(pgid, signal.SIGKILL, label)
    deadline = time.time() + float(STOP_KILL_TIMEOUT_SECONDS)
    while time.time() < deadline:
        if _count_process_group_members(pgid) == 0:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise RuntimeError(f"failed to stop process group label={label} pgid={pgid}")


def _stop_process_group_batch(pgids: List[int], label: str) -> None:
    if not pgids:
        return
    for pgid in pgids:
        _signal_process_group(pgid, signal.SIGTERM, f"{label}:pgid:{pgid}")
    deadline = time.time() + float(STOP_TERM_TIMEOUT_SECONDS)
    while time.time() < deadline:
        alive_pgids = [pgid for pgid in pgids if _count_process_group_members(pgid) > 0]
        if not alive_pgids:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    alive_pgids = [pgid for pgid in pgids if _count_process_group_members(pgid) > 0]
    print(
        f"[selection-supervisor] term timeout exceeded; escalating batch to SIGKILL "
        f"label={label} pgids={alive_pgids} grace_seconds={STOP_TERM_TIMEOUT_SECONDS}",
        file=sys.stderr,
    )
    for pgid in alive_pgids:
        _signal_process_group(pgid, signal.SIGKILL, f"{label}:pgid:{pgid}")
    deadline = time.time() + float(STOP_KILL_TIMEOUT_SECONDS)
    while time.time() < deadline:
        alive_pgids = [pgid for pgid in pgids if _count_process_group_members(pgid) > 0]
        if not alive_pgids:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    alive_pgids = [pgid for pgid in pgids if _count_process_group_members(pgid) > 0]
    raise RuntimeError(f"failed to stop process groups label={label} pgids={alive_pgids}")


def _signal_process_group(pgid: int, sig: signal.Signals, label: str) -> None:
    try:
        os.killpg(pgid, sig)
        print(
            f"[selection-supervisor] signal group label={label} pgid={pgid} signal={sig.name}",
            file=sys.stderr,
        )
    except ProcessLookupError:
        return


def _stop_pid_tree(root_pid: int, label: str) -> None:
    _signal_pid_tree(root_pid, signal.SIGTERM, label)
    deadline = time.time() + float(STOP_TERM_TIMEOUT_SECONDS)
    while time.time() < deadline:
        if not _list_pid_tree(root_pid):
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    print(
        f"[selection-supervisor] term timeout exceeded; escalating to SIGKILL "
        f"label={label} root_pid={root_pid} grace_seconds={STOP_TERM_TIMEOUT_SECONDS}",
        file=sys.stderr,
    )
    _signal_pid_tree(root_pid, signal.SIGKILL, label)
    deadline = time.time() + float(STOP_KILL_TIMEOUT_SECONDS)
    while time.time() < deadline:
        if not _list_pid_tree(root_pid):
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    alive = _list_pid_tree(root_pid)
    raise RuntimeError(f"failed to stop pid tree label={label} root_pid={root_pid} alive={alive}")


def _stop_pid_tree_batch(root_pids: List[int], label: str) -> None:
    if not root_pids:
        return
    for root_pid in root_pids:
        _signal_pid_tree(root_pid, signal.SIGTERM, f"{label}:pid:{root_pid}")
    deadline = time.time() + float(STOP_TERM_TIMEOUT_SECONDS)
    while time.time() < deadline:
        alive_roots = [root_pid for root_pid in root_pids if _list_pid_tree(root_pid)]
        if not alive_roots:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    alive_roots = [root_pid for root_pid in root_pids if _list_pid_tree(root_pid)]
    print(
        f"[selection-supervisor] term timeout exceeded; escalating tree batch to SIGKILL "
        f"label={label} root_pids={alive_roots} grace_seconds={STOP_TERM_TIMEOUT_SECONDS}",
        file=sys.stderr,
    )
    for root_pid in alive_roots:
        _signal_pid_tree(root_pid, signal.SIGKILL, f"{label}:pid:{root_pid}")
    deadline = time.time() + float(STOP_KILL_TIMEOUT_SECONDS)
    while time.time() < deadline:
        alive_roots = [root_pid for root_pid in root_pids if _list_pid_tree(root_pid)]
        if not alive_roots:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    alive_roots = [root_pid for root_pid in root_pids if _list_pid_tree(root_pid)]
    raise RuntimeError(f"failed to stop pid trees label={label} root_pids={alive_roots}")


def _signal_pid_tree(root_pid: int, sig: signal.Signals, label: str) -> None:
    pids = _list_pid_tree(root_pid)
    if not pids:
        return
    for pid in reversed(pids):
        try:
            os.kill(pid, sig)
            print(
                f"[selection-supervisor] signal tree label={label} pid={pid} signal={sig.name}",
                file=sys.stderr,
            )
        except ProcessLookupError:
            continue


if __name__ == "__main__":
    raise SystemExit(main())
"""
    return (
        textwrap.dedent(template)
        .replace("__LOG_SHARD_HELPER_FILENAME__", LOG_SHARD_HELPER_FILENAME)
        .replace("__TERM_S__", str(term_s))
        .replace("__KILL_S__", str(kill_s))
        .replace("__SUPERSEDE_S__", str(supersede_s))
    )
