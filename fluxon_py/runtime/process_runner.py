from __future__ import annotations

import base64
from collections.abc import Mapping
import atexit
import ctypes
from dataclasses import dataclass
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Sequence

import yaml

from ..config import _to_plain_yaml_obj


RuntimeConfigInput = Path | Mapping[str, Any]
FORCE_KILL_WAIT_SECONDS = 10.0


@dataclass(frozen=True)
class _ProcessInfo:
    pid: int
    ppid: int
    state: str

    @property
    def is_zombie(self) -> bool:
        return self.state == "Z"


@dataclass(frozen=True)
class RuntimeSingletonSpec:
    module_name: str
    entrypoint_path: Path
    workdir_arg: str
    workdir_path: Path


@dataclass(frozen=True)
class ManagedSubprocess:
    label: str
    proc: subprocess.Popen[Any]


def resolve_runtime_config_path(
    *,
    workdir: Path,
    runtime_config_filename: str,
    config: RuntimeConfigInput | None = None,
    config_path: Path | None = None,
) -> Path:
    if (config is None) == (config_path is None):
        raise ValueError("exactly one of config or config_path must be provided")

    if config_path is not None:
        return _resolve_existing_config_path(config_path)

    assert config is not None
    if isinstance(config, Path):
        return _resolve_existing_config_path(config)
    if not isinstance(config, Mapping):
        raise TypeError(f"config must be a pathlib.Path or mapping, got {type(config).__name__}")

    resolved_workdir = workdir.resolve()
    resolved_workdir.mkdir(parents=True, exist_ok=True)
    resolved_config_path = resolved_workdir / runtime_config_filename
    config_yaml = yaml.safe_dump(_to_plain_yaml_obj(config, "config"), sort_keys=False)
    resolved_config_path.write_text(config_yaml, encoding="utf-8")
    return resolved_config_path


def encode_runtime_config_b64(config: Mapping[str, Any]) -> str:
    config_yaml = yaml.safe_dump(_to_plain_yaml_obj(config, "config"), sort_keys=False)
    return base64.b64encode(config_yaml.encode("utf-8")).decode("ascii")


def decode_runtime_config_b64(config_b64: str) -> str:
    if not config_b64.strip():
        raise ValueError("config_b64 must be non-empty")
    try:
        return base64.b64decode(config_b64.encode("ascii"), validate=True).decode("utf-8")
    except Exception as exc:
        raise ValueError(f"decode config_b64 failed: {exc}")


def build_runtime_singleton_spec(
    *,
    module_name: str,
    entrypoint_path: Path,
    workdir: Path,
) -> RuntimeSingletonSpec:
    resolved_module_name = module_name.strip()
    if not resolved_module_name:
        raise ValueError("module_name must be non-empty")
    resolved_entrypoint_path = entrypoint_path.resolve()
    raw_workdir_arg = str(workdir)
    resolved_workdir_path = workdir.resolve()
    return RuntimeSingletonSpec(
        module_name=resolved_module_name,
        entrypoint_path=resolved_entrypoint_path,
        workdir_arg=raw_workdir_arg,
        workdir_path=resolved_workdir_path,
    )


def prepare_singleton_workdir(
    *,
    config_path: Path,
    singleton_spec: RuntimeSingletonSpec,
    stop_timeout_seconds: int,
) -> None:
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")

    resolved_workdir = singleton_spec.workdir_path
    resolved_workdir.mkdir(parents=True, exist_ok=True)
    _stop_existing_processes_if_running(
        singleton_spec=singleton_spec,
        stop_timeout_seconds=stop_timeout_seconds,
    )
    os.chdir(resolved_workdir)


def run_singleton_process(
    *,
    config_path: Path,
    singleton_spec: RuntimeSingletonSpec,
    stop_timeout_seconds: int,
    start_fn: Callable[[], None],
) -> None:
    prepare_singleton_workdir(
        config_path=config_path,
        singleton_spec=singleton_spec,
        stop_timeout_seconds=stop_timeout_seconds,
    )
    start_fn()


def start_python_module_process(
    *,
    module_name: str,
    config_path: Path,
    workdir: Path,
    extra_cli_args: Sequence[str],
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    resolved_config = config_path.resolve()
    resolved_workdir = workdir.resolve()
    resolved_workdir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        module_name,
        "--config",
        str(resolved_config),
        "--workdir",
        str(resolved_workdir),
        *extra_cli_args,
    ]
    return _start_runtime_process(
        cmd=cmd,
        cwd=resolved_workdir,
        log_path=log_path,
    )


def start_python_module_process_with_config_b64(
    *,
    module_name: str,
    config_b64: str,
    extra_cli_args: Sequence[str],
    log_path: Path | None = None,
) -> subprocess.Popen[bytes]:
    if not config_b64.strip():
        raise ValueError("config_b64 must be non-empty")
    cmd = [
        sys.executable,
        "-m",
        module_name,
        "--config-b64",
        config_b64,
        *extra_cli_args,
    ]
    return _start_runtime_process(
        cmd=cmd,
        cwd=None,
        log_path=log_path,
    )


def wait_subproc_or_ctrlc(
    children: Sequence[ManagedSubprocess],
    *,
    on_ctrlc: Callable[[], None] | None = None,
    stop_timeout_seconds: float = 5.0,
) -> None:
    if not children:
        raise ValueError("children must not be empty")

    exited_pid: int | None = None
    restore_wait_signal_handlers = _install_wait_signal_handlers()
    try:
        exited_pid, status = os.wait()
        for child in children:
            if child.proc.pid == exited_pid:
                raise RuntimeError(f"{child.label} exited unexpectedly with status {status}")
        raise RuntimeError(f"unexpected child exited, pid={exited_pid}, status={status}")
    except KeyboardInterrupt:
        if on_ctrlc is not None:
            on_ctrlc()
        raise SystemExit(130)
    finally:
        if restore_wait_signal_handlers is not None:
            restore_wait_signal_handlers()
        _stop_child_processes(children, skip_pid=exited_pid, stop_timeout_seconds=stop_timeout_seconds)


def register_ctrlc_callback(
    on_ctrlc: Callable[[str], None],
    *,
    thread_name: str,
) -> Callable[[], None]:
    signal_set = {signal.SIGINT, signal.SIGTERM}

    if threading.current_thread() is threading.main_thread():
        old_sigint = signal.getsignal(signal.SIGINT)
        old_sigterm = signal.getsignal(signal.SIGTERM)

        def _handler(signum: int, _frame: object) -> None:
            on_ctrlc(_signal_reason(signum))

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

        def _restore() -> None:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

        return _restore

    if hasattr(signal, "pthread_sigmask") and hasattr(signal, "sigwait"):
        old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, signal_set)

        def _listener() -> None:
            on_ctrlc(_signal_reason(signal.sigwait(signal_set)))

        listener = threading.Thread(target=_listener, name=thread_name, daemon=True)
        listener.start()

        def _restore() -> None:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

        return _restore

    raise RuntimeError(
        f"register_ctrlc_callback requires either the Python main thread signal API "
        f"or pthread_sigmask/sigwait support; thread_name={thread_name}"
    )


def bind_current_process_parent_death_sigterm() -> None:
    # Runtime entrypoints also support direct `python -m ...` execution, so the
    # current process must bind parent-death cleanup itself in that shape.
    _set_parent_death_sigterm(expected_parent_pid=os.getppid())


def build_parent_death_sigterm_preexec(*, expected_parent_pid: int) -> Callable[[], None]:
    # Helper-spawned children bind parent-death cleanup in preexec so the spawned
    # runtime stays attached to the launching process without creating a new session.
    def _preexec() -> None:
        _set_parent_death_sigterm(expected_parent_pid=expected_parent_pid)

    return _preexec


def _resolve_existing_config_path(config_path: Path) -> Path:
    resolved_config = config_path.resolve()
    if not resolved_config.exists():
        raise FileNotFoundError(f"config not found: {resolved_config}")
    return resolved_config


def _install_wait_signal_handlers() -> Callable[[], None] | None:
    if threading.current_thread() is not threading.main_thread():
        return None

    old_sigint = signal.getsignal(signal.SIGINT)
    old_sigterm = signal.getsignal(signal.SIGTERM)

    def _raise_keyboard_interrupt(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt()

    signal.signal(signal.SIGINT, _raise_keyboard_interrupt)
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    def _restore() -> None:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    return _restore


def _stop_existing_processes_if_running(
    *,
    singleton_spec: RuntimeSingletonSpec,
    stop_timeout_seconds: int,
) -> None:
    root_pids = _matching_runtime_singleton_root_pids(
        singleton_spec=singleton_spec,
        exclude_pid=os.getpid(),
    )
    if not root_pids:
        return

    member_pids: list[int] = []
    seen_member_pids: set[int] = set()
    for root_pid in root_pids:
        pid_tree = _list_pid_tree(root_pid)
        if not pid_tree:
            if not _is_process_running(root_pid):
                continue
            pid_tree = [root_pid]
        for member_pid in pid_tree:
            if member_pid in seen_member_pids:
                continue
            seen_member_pids.add(member_pid)
            member_pids.append(member_pid)

    if not member_pids:
        return

    _signal_pid_list(member_pids, signal.SIGTERM)
    if _wait_processes_exit(member_pids, timeout_seconds=float(stop_timeout_seconds)):
        return

    _signal_pid_list(member_pids, signal.SIGKILL)
    if _wait_processes_exit(member_pids, timeout_seconds=FORCE_KILL_WAIT_SECONDS):
        return

    alive = [member_pid for member_pid in member_pids if _is_process_running(member_pid)]
    raise RuntimeError(
        "existing singleton runtime did not exit after SIGTERM and SIGKILL, "
        f"module_name={singleton_spec.module_name}, "
        f"entrypoint_path={singleton_spec.entrypoint_path}, "
        f"workdir={singleton_spec.workdir_path}, "
        f"root_pids={root_pids}, alive={alive}, "
        f"term_timeout_seconds={stop_timeout_seconds}, "
        f"kill_timeout_seconds={FORCE_KILL_WAIT_SECONDS}"
    )


def _stop_child_processes(
    children: Sequence[ManagedSubprocess],
    *,
    skip_pid: int | None,
    stop_timeout_seconds: float,
) -> None:
    for child in reversed(children):
        proc = child.proc
        if skip_pid is not None and proc.pid == skip_pid:
            continue
        if proc.poll() is None:
            _signal_managed_child(child, signal.SIGTERM)

    deadline = time.time() + stop_timeout_seconds
    while time.time() < deadline:
        if all(
            skip_pid is not None and child.proc.pid == skip_pid or child.proc.poll() is not None
            for child in children
        ):
            return
        time.sleep(0.2)

    for child in reversed(children):
        proc = child.proc
        if skip_pid is not None and proc.pid == skip_pid:
            continue
        if proc.poll() is None:
            _signal_managed_child(child, signal.SIGKILL)

    for child in reversed(children):
        proc = child.proc
        if skip_pid is not None and proc.pid == skip_pid:
            continue
        if proc.poll() is None:
            proc.wait(timeout=stop_timeout_seconds)


def _signal_managed_child(child: ManagedSubprocess, sig: int) -> None:
    try:
        os.kill(child.proc.pid, sig)
    except ProcessLookupError:
        return
    except PermissionError:
        return


def _matching_runtime_singleton_root_pids(
    *,
    singleton_spec: RuntimeSingletonSpec,
    exclude_pid: int | None,
) -> list[int]:
    root_pids: list[int] = []
    process_info_by_pid = {info.pid: info for info in _iter_process_infos()}
    for pid, args in _iter_process_cmdlines():
        if exclude_pid is not None and pid == exclude_pid:
            continue
        process_info = process_info_by_pid.get(pid)
        if process_info is None or process_info.is_zombie:
            continue
        if not _cmdline_matches_runtime_singleton_spec(args, singleton_spec):
            continue
        root_pids.append(pid)
    root_pids.sort()
    return root_pids


def _cmdline_matches_runtime_singleton_spec(
    args: list[str],
    singleton_spec: RuntimeSingletonSpec,
) -> bool:
    if not _cmdline_matches_runtime_entrypoint(args, singleton_spec):
        return False
    workdir_arg = _arg_value(args, "-w", "--workdir")
    if workdir_arg is None:
        return False
    if workdir_arg == singleton_spec.workdir_arg:
        return True
    return Path(workdir_arg).resolve() == singleton_spec.workdir_path


def _cmdline_matches_runtime_entrypoint(
    args: list[str],
    singleton_spec: RuntimeSingletonSpec,
) -> bool:
    for arg in args:
        if arg == singleton_spec.module_name:
            return True
    for arg in args[:4]:
        candidate = Path(arg)
        if candidate.suffix != ".py":
            continue
        try:
            if candidate.resolve() == singleton_spec.entrypoint_path:
                return True
        except OSError:
            continue
    return False


def _iter_process_cmdlines() -> list[tuple[int, list[str]]]:
    out: list[tuple[int, list[str]]] = []
    proc_dir = Path("/proc")
    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        cmdline_path = entry / "cmdline"
        try:
            raw = cmdline_path.read_bytes()
        except (FileNotFoundError, ProcessLookupError, OSError):
            continue
        if not raw:
            continue
        args = [chunk.decode("utf-8", errors="ignore") for chunk in raw.split(b"\0") if chunk]
        if not args:
            continue
        out.append((int(entry.name), args))
    return out


def _arg_value(args: Sequence[str], *flags: str) -> str | None:
    for idx, arg in enumerate(args[:-1]):
        if arg in flags:
            return args[idx + 1]
    return None


def _start_runtime_process(
    *,
    cmd: Sequence[str],
    cwd: Path | None,
    log_path: Path | None,
) -> subprocess.Popen[bytes]:
    popen_kwargs: dict[str, Any] = {
        "preexec_fn": build_parent_death_sigterm_preexec(expected_parent_pid=os.getpid()),
    }
    if cwd is not None:
        popen_kwargs["cwd"] = str(cwd)

    log_file = None
    if log_path is not None:
        resolved_log_path = log_path.resolve()
        resolved_log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(resolved_log_path, "a", encoding="utf-8")
        popen_kwargs["stdout"] = log_file
        popen_kwargs["stderr"] = subprocess.STDOUT

    proc = subprocess.Popen(cmd, **popen_kwargs)

    if log_file is not None:
        atexit.register(log_file.close)
        log_file.close()

    return proc


def _set_parent_death_sigterm(*, expected_parent_pid: int) -> None:
    # Keep this even in the attached parent/child model:
    # - A plain attached child does not die automatically when the parent is
    #   killed with non-catchable signals such as SIGKILL (`kill -9`).
    # - PR_SET_PDEATHSIG asks the kernel to deliver SIGTERM to this process when
    #   the original parent exits, so parent death still cleans up children in
    #   those abnormal-exit cases.
    # - The parent PID recheck closes the fork->prctl race: if the parent died
    #   before the prctl call completed, the child must terminate immediately.
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    pr_set_pdeathsig = 1
    if libc.prctl(pr_set_pdeathsig, int(signal.SIGTERM)) != 0:
        err = ctypes.get_errno()
        raise RuntimeError(f"prctl(PR_SET_PDEATHSIG) failed errno={err}")

    if os.getppid() != expected_parent_pid:
        os.kill(os.getpid(), signal.SIGTERM)


def _is_process_running(pid: int) -> bool:
    state = _process_state(pid)
    if state is None:
        return False
    if state == "Z":
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _wait_process_exit(*, pid: int, timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not _is_process_running(pid):
            return True
        time.sleep(0.2)
    return not _is_process_running(pid)


def _wait_processes_exit(pids: Sequence[int], *, timeout_seconds: float) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if all(not _is_process_running(pid) for pid in pids):
            return True
        time.sleep(0.2)
    return all(not _is_process_running(pid) for pid in pids)


def _signal_pid_list(pids: Sequence[int], sig: int) -> None:
    seen: set[int] = set()
    for pid in reversed(list(pids)):
        if pid in seen:
            continue
        seen.add(pid)
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            continue
        except PermissionError:
            continue


def _list_pid_tree(root_pid: int) -> list[int]:
    infos = _iter_process_infos()
    root_info = None
    children_by_ppid: dict[int, list[int]] = {}
    for info in infos:
        if info.is_zombie:
            continue
        children_by_ppid.setdefault(info.ppid, []).append(info.pid)
        if info.pid == root_pid:
            root_info = info
    if root_info is None or root_info.is_zombie:
        return []

    ordered: list[int] = []
    stack = [root_pid]
    while stack:
        current = stack.pop()
        ordered.append(current)
        child_pids = children_by_ppid.get(current)
        if child_pids is None:
            continue
        for child_pid in sorted(child_pids, reverse=True):
            stack.append(child_pid)
    return ordered


def _iter_process_infos() -> list[_ProcessInfo]:
    infos: list[_ProcessInfo] = []
    proc_dir = Path("/proc")
    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        status_path = entry / "status"
        try:
            lines = status_path.read_text(encoding="utf-8").splitlines()
        except (FileNotFoundError, ProcessLookupError, OSError):
            continue
        fields: dict[str, str] = {}
        for line in lines:
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            fields[key] = value.strip()
        state = fields.get("State")
        ppid = fields.get("PPid")
        if state is None or ppid is None:
            continue
        state_fields = state.split()
        if len(state_fields) == 0:
            continue
        infos.append(
            _ProcessInfo(
                pid=int(entry.name),
                ppid=int(ppid),
                state=state_fields[0],
            )
        )
    return infos


def _process_state(pid: int) -> str | None:
    status_path = Path(f"/proc/{pid}/status")
    try:
        lines = status_path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    for line in lines:
        if line.startswith("State:"):
            fields = line.split()
            if len(fields) < 2:
                raise RuntimeError(f"invalid process state line: pid={pid}, line={line!r}")
            return fields[1]
    raise RuntimeError(f"missing process state line: pid={pid}, path={status_path}")


def _signal_reason(signum: int) -> str:
    if signum == signal.SIGINT:
        return "Ctrl-C"
    return f"signal {signum}"
