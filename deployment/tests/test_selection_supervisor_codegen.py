#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, List, Optional, Tuple


SCRIPT_DIR = Path(__file__).resolve().parent
UTILS_DIR = SCRIPT_DIR.parent / "utils"
sys.path.insert(0, str(UTILS_DIR))

from log_shard import render_module_source as render_log_shard_module_source  # type: ignore
from selection_supervisor_codegen import render_python_selection_supervisor_module  # type: ignore


def main() -> int:
    parser = argparse.ArgumentParser(description="selection_supervisor codegen test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    args = parser.parse_args()

    checks = _build_checks(args.test_id)
    failures = 0
    for _, check in checks:
        if not _run_check(check):
            failures += 1
    return 0 if failures == 0 else 1


def _build_checks(selected_test_id: Optional[str]) -> List[Tuple[str, Callable[[], None]]]:
    checks: List[Tuple[str, Callable[[], None]]] = [
        ("runtime_only_supports_run_stop", test_runtime_only_supports_run_stop),
        ("runtime_requires_same_directory_log_shard_helper", test_runtime_requires_same_directory_log_shard_helper),
        ("install_subreaper_uses_prctl", test_install_subreaper_uses_prctl),
        ("spawn_child_sanitizes_rdma_driver_env", test_spawn_child_sanitizes_rdma_driver_env),
        ("selection_present_requires_live_child_process", test_selection_present_requires_live_child_process),
        ("runtime_log_path_uses_daily_shard_files", test_runtime_log_path_uses_daily_shard_files),
        ("runtime_log_path_expands_hostworkdir_env", test_runtime_log_path_expands_hostworkdir_env),
        ("runtime_log_shards_roll_and_preserve_content_boundaries", test_runtime_log_shards_roll_and_preserve_content_boundaries),
        ("selection_present_checks_all_live_supervisors", test_selection_present_checks_all_live_supervisors),
        ("zombie_supervisor_is_treated_as_stopped", test_zombie_supervisor_is_treated_as_stopped),
        ("legacy_replace_process_is_observed_as_live_owner", test_legacy_replace_process_is_observed_as_live_owner),
        ("proc_cmdline_race_is_ignored", test_proc_cmdline_race_is_ignored),
        ("bare_stop_retires_live_generation", test_bare_stop_retires_live_generation),
        ("apply_stop_targets_matching_generation", test_apply_stop_targets_matching_generation),
        ("replace_supersedes_old_generation", test_replace_supersedes_old_generation),
        ("replace_supersede_retires_grandchild_process", test_replace_supersede_retires_grandchild_process),
        ("same_label_different_scope_can_coexist", test_same_label_different_scope_can_coexist),
        ("newer_apply_owned_overlap_with_applyless_owner_defers_retire", test_newer_apply_owned_overlap_with_applyless_owner_defers_retire),
        ("stale_apply_owned_takeover_of_applyless_owner_is_rejected", test_stale_apply_owned_takeover_of_applyless_owner_is_rejected),
        ("shutdown_escalates_to_sigkill", test_shutdown_escalates_to_sigkill),
        ("retire_adopted_children_stops_live_roots", test_retire_adopted_children_stops_live_roots),
        ("retire_adopted_children_preserves_live_supervisor_roots", test_retire_adopted_children_preserves_live_supervisor_roots),
        ("state_less_legacy_supervisor_with_newer_owner_ts_becomes_owner", test_state_less_legacy_supervisor_with_newer_owner_ts_becomes_owner),
        ("owner_ts_ms_collision_is_rejected", test_owner_ts_ms_collision_is_rejected),
    ]
    if selected_test_id is None:
        return checks
    for check_id, check in checks:
        if check_id == selected_test_id:
            return [(check_id, check)]
    available = ", ".join(check_id for check_id, _ in checks)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def _run_check(check: Callable[[], None]) -> bool:
    try:
        check()
        print(f"PASS: {check.__name__}")
        return True
    except Exception as exc:
        print(f"FAIL: {check.__name__}: {exc}")
        return False


def _load_runtime_module():
    root = Path(tempfile.mkdtemp(prefix="test_selection_supervisor_runtime_module_"))
    try:
        supervisor_path = _write_runtime_script(root)
        module_name = f"test_selection_supervisor_runtime_{os.getpid()}_{time.time_ns()}"
        spec = importlib.util.spec_from_file_location(module_name, supervisor_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load runtime module spec: {supervisor_path}")
        module = importlib.util.module_from_spec(spec)
        module._test_runtime_root = root
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.modules.pop(module_name, None)


def _write_runtime_script(root: Path, *, term_seconds: int = 5, kill_seconds: int = 5, supersede_seconds: int = 2) -> Path:
    supervisor_path = root / "selection_supervisor.py"
    supervisor_path.write_text(
        render_python_selection_supervisor_module(
            timeouts=SimpleNamespace(
                term_seconds=term_seconds,
                kill_seconds=kill_seconds,
                supersede_seconds=supersede_seconds,
            ),
        ),
        encoding="utf-8",
    )
    (root / "log_shard.py").write_text(
        render_log_shard_module_source(),
        encoding="utf-8",
    )
    return supervisor_path


def _runtime_state_json(
    *,
    name: str,
    service_name: str,
    child_argv: List[str],
    root: Path,
    apply_id: Optional[str],
) -> str:
    payload = {
        "kind": "DaemonSet",
        "name": name,
        "service_name": service_name,
        "argv": child_argv,
        "cwd": str(root),
        "log_path": str(root / f"{service_name}.log"),
    }
    if apply_id is not None:
        payload["apply_id"] = apply_id
    return json.dumps(payload, sort_keys=True)


def _run_supervisor_command(
    *,
    supervisor_path: Path,
    label: str,
    owner_ts_ms: int,
    state_json: str,
    child_argv: List[str],
    cwd: Path,
    scope_key: Optional[str] = None,
) -> subprocess.Popen[str]:
    command = [
        sys.executable,
        str(supervisor_path),
        "run",
        "--label",
        label,
    ]
    if scope_key is not None:
        command.extend(["--scope-key", scope_key])
    command.extend(
        [
            "--state-json",
            state_json,
            "--owner-ts-ms",
            str(owner_ts_ms),
            "--restart-policy",
            "always",
            "--restart-delay-seconds",
            "1",
            "--max-backoff-seconds",
            "1",
            "--crashloop-consecutive-restarts",
            "0",
            "--crashloop-interval-lt-seconds",
            "0",
            "--",
            *child_argv,
        ]
    )
    return subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _terminate_process(proc: Optional[subprocess.Popen[str]]) -> None:
    if proc is None:
        return
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    if proc.stdout is not None:
        proc.stdout.read()
    if proc.stderr is not None:
        proc.stderr.read()


def _observe_status(module, label: str) -> dict:
    owner = module._selection_owner_supervisor(label)
    if owner is None:
        return {
            "running": False,
            "present": False,
            "apply_id": None,
            "owner_ts_ms": None,
            "pid": None,
        }
    process_count = module._count_pid_tree_members(owner.pid)
    child_process_count = max(process_count - 1, 0)
    return {
        "running": True,
        "present": child_process_count > 0,
        "apply_id": owner.runtime_state.apply_id if owner.runtime_state is not None else None,
        "owner_ts_ms": owner.owner_ts_ms,
        "pid": owner.pid,
    }


def _spawn_legacy_stateless_supervisor(
    *,
    supervisor_path: Path,
    label: str,
    owner_ts_ms: int,
    cwd: Path,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            str(supervisor_path),
            "run",
            "--label",
            label,
            "--owner-ts-ms",
            str(owner_ts_ms),
            "--restart-policy",
            "always",
            "--restart-delay-seconds",
            "1",
            "--max-backoff-seconds",
            "1",
            "--crashloop-consecutive-restarts",
            "0",
            "--crashloop-interval-lt-seconds",
            "0",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
        ],
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait_until_present(module, label: str, *, timeout_seconds: int = 15) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = _observe_status(module, label)
        if status["present"]:
            return status
        time.sleep(0.2)
    raise RuntimeError(f"timeout waiting present: label={label} status={_observe_status(module, label)!r}")


def _wait_until_apply_present(
    module,
    label: str,
    apply_id: str,
    *,
    timeout_seconds: int = 15,
) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = _observe_status(module, label)
        if status["present"] and status["apply_id"] == apply_id:
            return status
        time.sleep(0.2)
    raise RuntimeError(
        f"timeout waiting apply present: label={label} apply_id={apply_id} status={_observe_status(module, label)!r}"
    )


def _wait_until_absent(module, label: str, *, require_apply_id: Optional[str] = None, timeout_seconds: int = 15) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        supervisors = module._matching_live_supervisors_for_stop(
            label=label,
            scope_key=None,
            require_apply_id=require_apply_id,
        )
        if not supervisors:
            return
        time.sleep(0.2)
    raise RuntimeError(
        f"timeout waiting absent: label={label} require_apply_id={require_apply_id} "
        f"remaining={module._matching_live_supervisors_for_stop(label=label, scope_key=None, require_apply_id=require_apply_id)!r}"
    )


def _run_stop(supervisor_path: Path, cwd: Path, *, label: str, require_apply_id: Optional[str] = None, missing_ok: bool = False) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(supervisor_path),
        "stop",
        "--label",
        label,
    ]
    if require_apply_id is not None:
        command.extend(["--require-apply-id", require_apply_id])
    if missing_ok:
        command.append("--missing-ok")
    return subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def _run_stop_with_scope(
    supervisor_path: Path,
    cwd: Path,
    *,
    label: str,
    scope_key: str,
    require_apply_id: Optional[str] = None,
    missing_ok: bool = False,
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(supervisor_path),
        "stop",
        "--label",
        label,
        "--scope-key",
        scope_key,
    ]
    if require_apply_id is not None:
        command.extend(["--require-apply-id", require_apply_id])
    if missing_ok:
        command.append("--missing-ok")
    return subprocess.run(
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


def _write_sleep_child(root: Path, filename: str) -> Path:
    child_path = root / filename
    child_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import signal",
                "import time",
                "",
                "def _handle_signal(signum, _frame):",
                "    raise SystemExit(0)",
                "",
                "signal.signal(signal.SIGTERM, _handle_signal)",
                "signal.signal(signal.SIGINT, _handle_signal)",
                "while True:",
                "    time.sleep(0.2)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return child_path


def _write_wrapper_with_grandchild(root: Path, filename: str) -> Path:
    wrapper_path = root / filename
    wrapper_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import subprocess",
                "import sys",
                "import time",
                "from pathlib import Path",
                "",
                "root = Path(sys.argv[1]).resolve()",
                "marker = root / 'grandchild.pid'",
                "child = subprocess.Popen(",
                "    [",
                "        sys.executable,",
                "        '-c',",
                "        (",
                "            'import signal,time; '",
                "            'signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(0))); '",
                "            'signal.signal(signal.SIGINT, lambda *_: (_ for _ in ()).throw(SystemExit(0))); '",
                "            'while True: time.sleep(0.2)'",
                "        ),",
                "    ],",
                "    start_new_session=True,",
                ")",
                "marker.write_text(str(child.pid), encoding='utf-8')",
                "try:",
                "    while True:",
                "        time.sleep(0.2)",
                "finally:",
                "    try:",
                "        child.terminate()",
                "        child.wait(timeout=5)",
                "    except Exception:",
                "        pass",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return wrapper_path


def _read_pid(path: Path) -> int:
    return int(path.read_text(encoding="utf-8").strip())


def _pid_exists(pid: int) -> bool:
    return Path(f"/proc/{pid}").exists()


def _wait_pid_absent(pid: int, *, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not _pid_exists(pid):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timeout waiting pid absent: pid={pid}")


def _read_runtime_log(root: Path, service_name: str) -> str:
    shard_path = root / f"{service_name}.{time.strftime('%Y-%m-%d', time.gmtime())}.log"
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if shard_path.exists():
            return shard_path.read_text(encoding="utf-8", errors="replace")
        time.sleep(0.1)
    raise RuntimeError(f"runtime log shard missing: {shard_path}")


def test_runtime_only_supports_run_stop() -> None:
    code = render_python_selection_supervisor_module(
        timeouts=SimpleNamespace(term_seconds=5, kill_seconds=5, supersede_seconds=2),
    )
    assert "run_parser = subparsers.add_parser(" in code
    assert "stop_parser = subparsers.add_parser(" in code
    assert 'add_parser("wait-present"' not in code
    assert 'add_parser("wait-absent"' not in code
    assert 'add_parser("status"' not in code
    assert 'add_parser("list"' not in code
    assert "--require-supervisor-pid" not in code
    assert "--require-supervisor-start-time-ticks" not in code


def test_runtime_requires_same_directory_log_shard_helper() -> None:
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_missing_helper_") as td:
        root = Path(td)
        supervisor_path = root / "selection_supervisor.py"
        supervisor_path.write_text(
            render_python_selection_supervisor_module(
                timeouts=SimpleNamespace(term_seconds=5, kill_seconds=5, supersede_seconds=2),
            ),
            encoding="utf-8",
        )
        proc = subprocess.run(
            [sys.executable, str(supervisor_path), "stop", "--label", "DaemonSet/test-missing-helper", "--missing-ok"],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert proc.returncode != 0, proc
        assert "missing log shard helper next to selection_supervisor.py" in proc.stderr, proc.stderr


def test_install_subreaper_uses_prctl() -> None:
    module = _load_runtime_module()

    class _FakeLibc:
        def __init__(self) -> None:
            self.calls: List[tuple[int, int, int, int, int]] = []

        def prctl(self, arg0: int, arg1: int, arg2: int, arg3: int, arg4: int) -> int:
            self.calls.append((arg0, arg1, arg2, arg3, arg4))
            return 0

    fake_libc = _FakeLibc()
    original_cdll = module.ctypes.CDLL
    try:
        module.ctypes.CDLL = lambda *_args, **_kwargs: fake_libc
        module._install_subreaper()
        assert fake_libc.calls == [(36, 1, 0, 0, 0)], fake_libc.calls
    finally:
        module.ctypes.CDLL = original_cdll


def test_spawn_child_sanitizes_rdma_driver_env() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_env_") as td:
        root = Path(td)
        output_path = root / "child_env.json"
        child_script = root / "dump_child_env.py"
        child_script.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import json",
                    "import os",
                    "import sys",
                    "from pathlib import Path",
                    "",
                    "Path(sys.argv[1]).write_text(",
                    "    json.dumps(",
                    "        {",
                    '            "IBV_DRIVERS": os.environ.get("IBV_DRIVERS"),',
                    '            "RDMAV_DRIVERS": os.environ.get("RDMAV_DRIVERS"),',
                    '            "SELECTION_SUPERVISOR_TEST_KEEP": os.environ.get("SELECTION_SUPERVISOR_TEST_KEEP"),',
                    "        },",
                    "        sort_keys=True,",
                    "    ),",
                    '    encoding="utf-8",',
                    ")",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        saved_env = {
            "RDMAV_DRIVERS": os.environ.get("RDMAV_DRIVERS"),
            "IBV_DRIVERS": os.environ.get("IBV_DRIVERS"),
            "SELECTION_SUPERVISOR_TEST_KEEP": os.environ.get("SELECTION_SUPERVISOR_TEST_KEEP"),
        }
        os.environ["RDMAV_DRIVERS"] = "mlx5"
        os.environ["IBV_DRIVERS"] = "mlx5"
        os.environ["SELECTION_SUPERVISOR_TEST_KEEP"] = "expected"
        try:
            child = module._spawn_child(
                [sys.executable, str(child_script), str(output_path)],
                root,
            )
            rc = child.wait(timeout=10)
        finally:
            for key, value in saved_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        if rc != 0:
            raise RuntimeError(f"child exited non-zero: rc={rc}")
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if payload["RDMAV_DRIVERS"] is not None:
            raise RuntimeError(f"RDMAV_DRIVERS leaked into child env: {payload}")
        if payload["IBV_DRIVERS"] is not None:
            raise RuntimeError(f"IBV_DRIVERS leaked into child env: {payload}")
        if payload["SELECTION_SUPERVISOR_TEST_KEEP"] != "expected":
            raise RuntimeError(f"unrelated env should be preserved: {payload}")


def test_selection_present_requires_live_child_process() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_present_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-present"
        child_argv = [sys.executable, str(child_path)]
        assert module._selection_present(label) is False
        supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-present",
                service_name="test-present",
                child_argv=child_argv,
                root=root,
                apply_id=None,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        try:
            _wait_until_present(module, label)
            assert module._selection_present(label) is True
            stop_proc = _run_stop(supervisor_path, root, label=label, missing_ok=True)
            assert stop_proc.returncode == 0, stop_proc.stderr
            supervisor.wait(timeout=20)
            _wait_until_absent(module, label)
        finally:
            _terminate_process(supervisor)


def test_runtime_log_path_uses_daily_shard_files() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_log_shard_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = root / "child.py"
        child_path.write_text(
            "import sys, time\n"
            "print('hello-log-shard', flush=True)\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        label = "DaemonSet/test-log-shard"
        child_argv = [sys.executable, str(child_path)]
        base_log_path = root / "test-log-shard.log"
        supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=json.dumps(
                {
                    "kind": "DaemonSet",
                    "name": "test-log-shard",
                    "service_name": "test-log-shard",
                    "argv": child_argv,
                    "cwd": str(root),
                    "log_path": str(base_log_path),
                },
                sort_keys=True,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        try:
            _wait_until_present(module, label)
            deadline = time.time() + 5.0
            shard_path = root / f"test-log-shard.{time.strftime('%Y-%m-%d', time.gmtime())}.log"
            while time.time() < deadline and not shard_path.exists():
                time.sleep(0.1)
            assert shard_path.exists(), shard_path
            assert not base_log_path.exists(), base_log_path
            assert "hello-log-shard" in shard_path.read_text(encoding="utf-8", errors="replace")
        finally:
            _terminate_process(supervisor)


def test_runtime_log_path_expands_hostworkdir_env() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_expand_hostworkdir_") as td:
        root = Path(td)
        hostworkdir = root / "hostworkdir"
        hostworkdir.mkdir(parents=True, exist_ok=True)
        supervisor_path = _write_runtime_script(root)
        child_path = root / "child.py"
        child_path.write_text(
            "import time\n"
            "print('expanded-hostworkdir-log', flush=True)\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        label = "DaemonSet/test-expand-hostworkdir"
        child_argv = [sys.executable, str(child_path)]
        saved_hostworkdir = os.environ.get("HOSTWORKDIR")
        os.environ["HOSTWORKDIR"] = str(hostworkdir)
        supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=json.dumps(
                {
                    "kind": "DaemonSet",
                    "name": "test-expand-hostworkdir",
                    "service_name": "test-expand-hostworkdir",
                    "argv": child_argv,
                    "cwd": str(root),
                    "log_path": "${HOSTWORKDIR}/log/test-expand-hostworkdir.log",
                },
                sort_keys=True,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        try:
            _wait_until_present(module, label)
            deadline = time.time() + 5.0
            shard_path = hostworkdir / "log" / f"test-expand-hostworkdir.{time.strftime('%Y-%m-%d', time.gmtime())}.log"
            while time.time() < deadline and not shard_path.exists():
                time.sleep(0.1)
            assert shard_path.exists(), shard_path
            assert "expanded-hostworkdir-log" in shard_path.read_text(encoding="utf-8", errors="replace")
        finally:
            _terminate_process(supervisor)
            if saved_hostworkdir is None:
                os.environ.pop("HOSTWORKDIR", None)
            else:
                os.environ["HOSTWORKDIR"] = saved_hostworkdir


def test_runtime_log_shards_roll_and_preserve_content_boundaries() -> None:
    module = _load_runtime_module()
    saved_window = os.environ.get("FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS")
    saved_anchor = os.environ.get("FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS")
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_log_roll_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = root / "child.py"
        child_path.write_text(
            "import sys, time\n"
            "print('[ops-log-mgmt][phase=before] ts=' + str(int(time.time())), flush=True)\n"
            "time.sleep(11)\n"
            "print('[ops-log-mgmt][phase=after] ts=' + str(int(time.time())), flush=True)\n"
            "time.sleep(30)\n",
            encoding="utf-8",
        )
        anchor = str(int(time.time()) - 2)
        os.environ["FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS"] = "10"
        os.environ["FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS"] = anchor
        label = "DaemonSet/test-log-roll"
        child_argv = [sys.executable, str(child_path)]
        base_log_path = root / "test-log-roll.log"
        stale_shard = root / "test-log-roll.2025-12-01.log"
        stale_shard.write_text("stale\n", encoding="utf-8")
        supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=json.dumps(
                {
                    "kind": "DaemonSet",
                    "name": "test-log-roll",
                    "service_name": "test-log-roll",
                    "argv": child_argv,
                    "cwd": str(root),
                    "log_path": str(base_log_path),
                },
                sort_keys=True,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        try:
            _wait_until_present(module, label)
            first_shard = root / "test-log-roll.2026-01-01.log"
            second_shard = None
            deadline = time.time() + 20.0
            while time.time() < deadline:
                shard_paths = sorted(root.glob("test-log-roll.*.log"))
                if len(shard_paths) >= 2:
                    second_shard = shard_paths[-1]
                if first_shard.exists() and second_shard is not None and second_shard.exists():
                    first_text = first_shard.read_text(encoding="utf-8", errors="replace")
                    second_text = second_shard.read_text(encoding="utf-8", errors="replace")
                    if "[ops-log-mgmt][phase=before]" in first_text and "[ops-log-mgmt][phase=after]" in second_text:
                        break
                time.sleep(0.2)
            assert first_shard.exists(), first_shard
            assert second_shard is not None, sorted(path.name for path in root.glob("test-log-roll.*.log"))
            assert second_shard.exists(), second_shard
            assert not stale_shard.exists(), stale_shard
            shard_names = sorted(path.name for path in root.glob("test-log-roll.*.log"))
            assert shard_names[0] == "test-log-roll.2026-01-01.log", shard_names
            assert len(shard_names) == 2, shard_names
            first_text = first_shard.read_text(encoding="utf-8", errors="replace")
            second_text = second_shard.read_text(encoding="utf-8", errors="replace")
            assert "[ops-log-mgmt][phase=before]" in first_text, first_text
            assert "[ops-log-mgmt][phase=after]" not in first_text, first_text
            assert "[ops-log-mgmt][phase=after]" in second_text, second_text
            assert "[ops-log-mgmt][phase=before]" not in second_text, second_text
        finally:
            _terminate_process(supervisor)
            if saved_window is None:
                os.environ.pop("FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS", None)
            else:
                os.environ["FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS"] = saved_window
            if saved_anchor is None:
                os.environ.pop("FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS", None)
            else:
                os.environ["FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS"] = saved_anchor


def test_selection_present_checks_all_live_supervisors() -> None:
    module = _load_runtime_module()
    label = "DaemonSet/test-present-any-live-child"
    stale_new = SimpleNamespace(pid=11)
    old_live = SimpleNamespace(pid=22)
    original_iter_live_supervisors = module._iter_live_supervisors
    original_count_pid_tree_members = module._count_pid_tree_members
    try:
        module._iter_live_supervisors = (
            lambda current_label=None, scope_key=None: [stale_new, old_live] if current_label == label else []
        )
        module._count_pid_tree_members = lambda pid: {11: 1, 22: 2}[pid]
        assert module._selection_present(label) is True
    finally:
        module._iter_live_supervisors = original_iter_live_supervisors
        module._count_pid_tree_members = original_count_pid_tree_members


def test_legacy_replace_process_is_observed_as_live_owner() -> None:
    module = _load_runtime_module()
    label = "DaemonSet/test-legacy-replace"
    process_info = module.ProcessInfo(
        pid=123,
        ppid=1,
        pgid=123,
        state="S",
        start_time_ticks=77,
    )
    original_iter_process_cmdlines = module._iter_process_cmdlines
    original_find_process_info = module._find_process_info
    original_count_pid_tree_members = module._count_pid_tree_members
    try:
        module._iter_process_cmdlines = lambda: [
            (
                123,
                [
                    sys.executable,
                    "/tmp/selection_supervisor.py",
                    "replace",
                    "--label",
                    label,
                ],
            )
        ]
        module._find_process_info = lambda pid: process_info if pid == 123 else None
        module._count_pid_tree_members = lambda pid: 2 if pid == 123 else 0
        owner = module._selection_owner_supervisor(label)
        assert owner is not None
        assert owner.owner_ts_ms == 77
        assert module._selection_present(label) is True
    finally:
        module._iter_process_cmdlines = original_iter_process_cmdlines
        module._find_process_info = original_find_process_info
        module._count_pid_tree_members = original_count_pid_tree_members


def test_zombie_supervisor_is_treated_as_stopped() -> None:
    module = _load_runtime_module()
    label = "DaemonSet/test-zombie-stop"
    process_info = module.ProcessInfo(
        pid=321,
        ppid=1,
        pgid=321,
        state="Z",
        start_time_ticks=88,
    )
    original_iter_process_cmdlines = module._iter_process_cmdlines
    original_find_process_info = module._find_process_info
    try:
        module._iter_process_cmdlines = lambda: [
            (
                321,
                [
                    sys.executable,
                    "/tmp/selection_supervisor.py",
                    "run",
                    "--label",
                    label,
                    "--owner-ts-ms",
                    "9",
                ],
            )
        ]
        module._find_process_info = lambda pid: process_info if pid == 321 else None
        assert module._iter_live_supervisors(label) == []
        assert module._selection_owner_supervisor(label) is None
        assert module._selection_present(label) is False
    finally:
        module._iter_process_cmdlines = original_iter_process_cmdlines
        module._find_process_info = original_find_process_info


def test_proc_cmdline_race_is_ignored() -> None:
    module = _load_runtime_module()

    class _FakeCmdlinePath:
        def exists(self) -> bool:
            raise ProcessLookupError(3, "No such process", "/proc/123/cmdline")

        def read_bytes(self) -> bytes:
            raise AssertionError("read_bytes should not run after exists() failure")

    class _FakeEntry:
        name = "123"

        def __truediv__(self, other: str):
            assert other == "cmdline", other
            return _FakeCmdlinePath()

    class _FakeProcDir:
        def iterdir(self):
            return [_FakeEntry()]

    original_path = module.Path
    try:
        module.Path = lambda raw: _FakeProcDir() if raw == "/proc" else original_path(raw)
        result = module._iter_process_cmdlines()
        assert result == [], result
    finally:
        module.Path = original_path


def test_bare_stop_retires_live_generation() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_bare_stop_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-bare-stop"
        child_argv = [sys.executable, str(child_path)]
        supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-bare-stop",
                service_name="test-bare-stop",
                child_argv=child_argv,
                root=root,
                apply_id=None,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        try:
            status = _wait_until_present(module, label)
            assert status["apply_id"] is None
            stop_proc = _run_stop(supervisor_path, root, label=label)
            assert stop_proc.returncode == 0, (
                f"bare stop failed rc={stop_proc.returncode} stdout={stop_proc.stdout!r} stderr={stop_proc.stderr!r}"
            )
            supervisor.wait(timeout=20)
            _wait_until_absent(module, label)
        finally:
            _terminate_process(supervisor)


def test_apply_stop_targets_matching_generation() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_apply_stop_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-apply-stop"
        child_argv = [sys.executable, str(child_path)]
        old_supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-apply-stop",
                service_name="test-apply-stop",
                child_argv=child_argv,
                root=root,
                apply_id="apply-1",
            ),
            child_argv=child_argv,
            cwd=root,
        )
        new_supervisor: Optional[subprocess.Popen[str]] = None
        try:
            _wait_until_present(module, label)
            new_supervisor = _run_supervisor_command(
                supervisor_path=supervisor_path,
                label=label,
                owner_ts_ms=2,
                state_json=_runtime_state_json(
                    name="test-apply-stop",
                    service_name="test-apply-stop",
                    child_argv=child_argv,
                    root=root,
                    apply_id="apply-2",
                ),
                child_argv=child_argv,
                cwd=root,
            )
            status = _wait_until_present(module, label)
            assert status["apply_id"] == "apply-2", f"expected latest owner apply-2, got {status!r}"
            old_supervisor.wait(timeout=10)

            wrong_stop = _run_stop(
                supervisor_path,
                root,
                label=label,
                require_apply_id="apply-1",
                missing_ok=False,
            )
            assert wrong_stop.returncode != 0, "old apply_id stop should fail once target generation is absent"
            status_after_wrong = _observe_status(module, label)
            assert status_after_wrong["apply_id"] == "apply-2", (
                f"wrong apply stop must not affect current owner, got {status_after_wrong!r}"
            )

            right_stop = _run_stop(
                supervisor_path,
                root,
                label=label,
                require_apply_id="apply-2",
                missing_ok=False,
            )
            assert right_stop.returncode == 0, (
                f"current apply stop failed rc={right_stop.returncode} stdout={right_stop.stdout!r} stderr={right_stop.stderr!r}"
            )
            new_supervisor.wait(timeout=20)
            _wait_until_absent(module, label)
        finally:
            _terminate_process(new_supervisor)
            _terminate_process(old_supervisor)


def test_replace_supersedes_old_generation() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_supersede_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-supersede"
        child_argv = [sys.executable, str(child_path)]
        old_supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-supersede",
                service_name="test-supersede",
                child_argv=child_argv,
                root=root,
                apply_id="apply-1",
            ),
            child_argv=child_argv,
            cwd=root,
        )
        new_supervisor: Optional[subprocess.Popen[str]] = None
        try:
            _wait_until_present(module, label)
            new_supervisor = _run_supervisor_command(
                supervisor_path=supervisor_path,
                label=label,
                owner_ts_ms=2,
                state_json=_runtime_state_json(
                    name="test-supersede",
                    service_name="test-supersede",
                    child_argv=child_argv,
                    root=root,
                    apply_id="apply-2",
                ),
                child_argv=child_argv,
                cwd=root,
            )
            status = _wait_until_present(module, label)
            assert status["apply_id"] == "apply-2", f"expected new apply to own selection, got {status!r}"
            assert status["owner_ts_ms"] == 2, f"expected owner_ts_ms=2 after replace, got {status!r}"
            old_supervisor.wait(timeout=10)
            runtime_log = _read_runtime_log(root, "test-supersede")
            assert (
                "running generation superseded" in runtime_log
                or "superseded child exited without restart" in runtime_log
            ), (
                f"expected old supervisor supersede log, runtime_log={runtime_log!r}"
            )
        finally:
            _terminate_process(new_supervisor)
            _terminate_process(old_supervisor)


def test_replace_supersede_retires_grandchild_process() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_supersede_grandchild_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        wrapper_path = _write_wrapper_with_grandchild(root, "wrapper.py")
        marker_path = root / "grandchild.pid"
        label = "DaemonSet/test-supersede-grandchild"
        child_argv = [sys.executable, str(wrapper_path), str(root)]
        old_supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-supersede-grandchild",
                service_name="test-supersede-grandchild",
                child_argv=child_argv,
                root=root,
                apply_id="apply-1",
            ),
            child_argv=child_argv,
            cwd=root,
        )
        new_supervisor: Optional[subprocess.Popen[str]] = None
        try:
            _wait_until_present(module, label)
            deadline = time.time() + 10.0
            while time.time() < deadline and not marker_path.exists():
                time.sleep(0.2)
            if not marker_path.exists():
                raise RuntimeError("grandchild marker was not written by wrapper")
            old_grandchild_pid = _read_pid(marker_path)
            assert _pid_exists(old_grandchild_pid), old_grandchild_pid

            new_supervisor = _run_supervisor_command(
                supervisor_path=supervisor_path,
                label=label,
                owner_ts_ms=2,
                state_json=_runtime_state_json(
                    name="test-supersede-grandchild",
                    service_name="test-supersede-grandchild",
                    child_argv=child_argv,
                    root=root,
                    apply_id="apply-2",
                ),
                child_argv=child_argv,
                cwd=root,
            )
            status = _wait_until_present(module, label)
            assert status["apply_id"] == "apply-2", status
            old_supervisor.wait(timeout=20)
            _wait_pid_absent(old_grandchild_pid, timeout_seconds=10.0)
        finally:
            _terminate_process(new_supervisor)
            _terminate_process(old_supervisor)


def test_same_label_different_scope_can_coexist() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_scope_isolation_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-scope-isolation"
        child_argv = [sys.executable, str(child_path)]
        proc_a = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            scope_key="/tmp/scope-a",
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-scope-isolation",
                service_name="test-scope-isolation",
                child_argv=child_argv,
                root=root,
                apply_id="apply-a",
            ),
            child_argv=child_argv,
            cwd=root,
        )
        proc_b: Optional[subprocess.Popen[str]] = None
        try:
            _wait_until_present(module, label)
            proc_b = _run_supervisor_command(
                supervisor_path=supervisor_path,
                label=label,
                scope_key="/tmp/scope-b",
                owner_ts_ms=2,
                state_json=_runtime_state_json(
                    name="test-scope-isolation",
                    service_name="test-scope-isolation",
                    child_argv=child_argv,
                    root=root,
                    apply_id="apply-b",
                ),
                child_argv=child_argv,
                cwd=root,
            )
            deadline = time.time() + 10
            while time.time() < deadline:
                owners_a = module._iter_live_supervisors(label, scope_key="/tmp/scope-a")
                owners_b = module._iter_live_supervisors(label, scope_key="/tmp/scope-b")
                if owners_a and owners_b:
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("expected both scope-local supervisors to exist")
            assert proc_a.poll() is None, "scope-a supervisor should not be superseded by scope-b"
            assert proc_b.poll() is None, "scope-b supervisor should stay alive"
            stop_a = _run_stop_with_scope(supervisor_path, root, label=label, scope_key="/tmp/scope-a")
            assert stop_a.returncode == 0, stop_a.stderr
            proc_a.wait(timeout=20)
            assert proc_b.poll() is None, "stopping scope-a must not stop scope-b"
        finally:
            _terminate_process(proc_b)
            _terminate_process(proc_a)


def test_newer_apply_owned_overlap_with_applyless_owner_defers_retire() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_phase1_overlap_applyless_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root, supersede_seconds=5)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-phase1-overlap-applyless"
        child_argv = [sys.executable, str(child_path)]
        bare_supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=7,
            state_json=_runtime_state_json(
                name="test-phase1-overlap-applyless",
                service_name="test-phase1-overlap-applyless",
                child_argv=child_argv,
                root=root,
                apply_id=None,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        takeover_supervisor: Optional[subprocess.Popen[str]] = None
        try:
            bare_status = _wait_until_present(module, label)
            assert bare_status["apply_id"] is None, bare_status
            assert bare_status["owner_ts_ms"] == 7, bare_status

            takeover_supervisor = _run_supervisor_command(
                supervisor_path=supervisor_path,
                label=label,
                owner_ts_ms=11,
                state_json=_runtime_state_json(
                    name="test-phase1-overlap-applyless",
                    service_name="test-phase1-overlap-applyless",
                    child_argv=child_argv,
                    root=root,
                    apply_id="apply-1",
                ),
                child_argv=child_argv,
                cwd=root,
            )
            status = _wait_until_apply_present(module, label, "apply-1")
            assert status["owner_ts_ms"] == 11, status
            time.sleep(1.0)
            assert bare_supervisor.poll() is None, "old bare supervisor retired before phase-2 cutover or fallback"

            bare_supervisor.wait(timeout=20)
            runtime_log = _read_runtime_log(root, "test-phase1-overlap-applyless")
            assert (
                "running generation superseded" in runtime_log
                or "superseded child exited without restart" in runtime_log
            ), runtime_log
        finally:
            _terminate_process(takeover_supervisor)
            _terminate_process(bare_supervisor)


def test_stale_apply_owned_takeover_of_applyless_owner_is_rejected() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_applyless_takeover_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-applyless-takeover"
        child_argv = [sys.executable, str(child_path)]
        bare_supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=5,
            state_json=_runtime_state_json(
                name="test-applyless-takeover",
                service_name="test-applyless-takeover",
                child_argv=child_argv,
                root=root,
                apply_id=None,
            ),
            child_argv=child_argv,
            cwd=root,
        )
        takeover_supervisor: Optional[subprocess.Popen[str]] = None
        try:
            bare_status = _wait_until_present(module, label)
            assert bare_status["apply_id"] is None, bare_status
            assert bare_status["owner_ts_ms"] == 5, bare_status

            takeover_supervisor = _run_supervisor_command(
                supervisor_path=supervisor_path,
                label=label,
                owner_ts_ms=2,
                state_json=_runtime_state_json(
                    name="test-applyless-takeover",
                    service_name="test-applyless-takeover",
                    child_argv=child_argv,
                    root=root,
                    apply_id="apply-1",
                ),
                child_argv=child_argv,
                cwd=root,
            )
            takeover_supervisor.wait(timeout=20)
            stderr = takeover_supervisor.stderr.read() if takeover_supervisor.stderr is not None else ""
            assert takeover_supervisor.returncode != 0, takeover_supervisor.returncode
            assert "requested generation is superseded" in stderr, stderr
            status = _wait_until_present(module, label)
            assert status["apply_id"] is None, status
            assert status["owner_ts_ms"] == 5, status
        finally:
            _terminate_process(takeover_supervisor)
            _terminate_process(bare_supervisor)


def test_shutdown_escalates_to_sigkill() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_sigkill_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root, term_seconds=1, kill_seconds=5, supersede_seconds=2)
        child_path = root / "ignore_sigterm_child.py"
        child_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import signal",
                    "import time",
                    "",
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
                    "signal.signal(signal.SIGINT, signal.SIG_IGN)",
                    "while True:",
                    "    time.sleep(1)",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        label = "DaemonSet/test-sigkill"
        child_argv = [sys.executable, str(child_path)]
        supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-sigkill",
                service_name="test-sigkill",
                child_argv=child_argv,
                root=root,
                apply_id="apply-1",
            ),
            child_argv=child_argv,
            cwd=root,
        )
        try:
            _wait_until_present(module, label, timeout_seconds=10)
            supervisor.terminate()
            supervisor.wait(timeout=20)
            stderr = supervisor.stderr.read() if supervisor.stderr is not None else ""
            assert "term timeout exceeded; escalating to SIGKILL" in stderr, (
                f"expected SIGKILL escalation log, stderr={stderr!r}"
            )
            _wait_until_absent(module, label, timeout_seconds=10)
        finally:
            _terminate_process(supervisor)


def test_retire_adopted_children_stops_live_roots() -> None:
    module = _load_runtime_module()
    original_direct_live_child_pids = module._direct_live_child_pids
    original_iter_live_supervisors = module._iter_live_supervisors
    original_stop_pid_tree_batch = module._stop_pid_tree_batch
    original_reap_terminated_children = module._reap_terminated_children
    original_log_reaped_children = module._log_reaped_children
    calls: List[tuple[str, object]] = []
    try:
        module._direct_live_child_pids = lambda pid: [41, 42] if pid == module.os.getpid() else []
        module._iter_live_supervisors = lambda label=None, scope_key=None: []
        module._stop_pid_tree_batch = lambda roots, label: calls.append(("stop", (list(roots), label)))
        module._reap_terminated_children = lambda: [(41, 0), (42, 0)]
        module._log_reaped_children = lambda **kwargs: calls.append(("reap", kwargs))
        module._retire_adopted_children("DaemonSet/test-adopted")
        assert calls[0] == ("stop", ([41, 42], "adopted:DaemonSet/test-adopted")), calls
        assert calls[1][0] == "reap", calls
    finally:
        module._direct_live_child_pids = original_direct_live_child_pids
        module._iter_live_supervisors = original_iter_live_supervisors
        module._stop_pid_tree_batch = original_stop_pid_tree_batch
        module._reap_terminated_children = original_reap_terminated_children
        module._log_reaped_children = original_log_reaped_children


def test_retire_adopted_children_preserves_live_supervisor_roots() -> None:
    module = _load_runtime_module()
    original_direct_live_child_pids = module._direct_live_child_pids
    original_iter_live_supervisors = module._iter_live_supervisors
    original_stop_pid_tree_batch = module._stop_pid_tree_batch
    original_reap_terminated_children = module._reap_terminated_children
    original_log_reaped_children = module._log_reaped_children
    calls: List[tuple[str, object]] = []
    try:
        module._direct_live_child_pids = lambda pid: [41, 42] if pid == module.os.getpid() else []
        module._iter_live_supervisors = lambda label=None, scope_key=None: [
            module.LiveSupervisor(
                process_info=module.ProcessInfo(pid=42, ppid=module.os.getpid(), pgid=42, state="S", start_time_ticks=1),
                owner_ts_ms=7,
                label="DaemonSet/test-replacement",
                runtime_state=None,
                args=[sys.executable, "selection_supervisor.py", "run", "--label", "DaemonSet/test-replacement"],
            )
        ]
        module._stop_pid_tree_batch = lambda roots, label: calls.append(("stop", (list(roots), label)))
        module._reap_terminated_children = lambda: [(41, 0)]
        module._log_reaped_children = lambda **kwargs: calls.append(("reap", kwargs))
        module._retire_adopted_children("DaemonSet/test-adopted")
        assert calls[0] == ("stop", ([41], "adopted:DaemonSet/test-adopted")), calls
        assert calls[1][0] == "reap", calls
    finally:
        module._direct_live_child_pids = original_direct_live_child_pids
        module._iter_live_supervisors = original_iter_live_supervisors
        module._stop_pid_tree_batch = original_stop_pid_tree_batch
        module._reap_terminated_children = original_reap_terminated_children
        module._log_reaped_children = original_log_reaped_children


def test_state_less_legacy_supervisor_with_newer_owner_ts_becomes_owner() -> None:
    module = _load_runtime_module()
    with tempfile.TemporaryDirectory(prefix="test_selection_supervisor_stateless_legacy_") as td:
        root = Path(td)
        supervisor_path = _write_runtime_script(root)
        child_path = _write_sleep_child(root, "child.py")
        label = "DaemonSet/test-stateless-legacy"
        child_argv = [sys.executable, str(child_path)]
        current_supervisor = _run_supervisor_command(
            supervisor_path=supervisor_path,
            label=label,
            owner_ts_ms=1,
            state_json=_runtime_state_json(
                name="test-stateless-legacy",
                service_name="test-stateless-legacy",
                child_argv=child_argv,
                root=root,
                apply_id="apply-1",
            ),
            child_argv=child_argv,
            cwd=root,
        )
        legacy_supervisor: Optional[subprocess.Popen[str]] = None
        try:
            status = _wait_until_present(module, label)
            assert status["apply_id"] == "apply-1", status

            legacy_supervisor = _spawn_legacy_stateless_supervisor(
                supervisor_path=supervisor_path,
                label=label,
                owner_ts_ms=2,
                cwd=root,
            )
            time.sleep(1.0)

            status_after_legacy = _observe_status(module, label)
            assert status_after_legacy["apply_id"] is None, status_after_legacy
            assert status_after_legacy["owner_ts_ms"] == 2, status_after_legacy

            right_stop = _run_stop(
                supervisor_path,
                root,
                label=label,
                require_apply_id="apply-1",
                missing_ok=False,
            )
            if right_stop.returncode == 0:
                current_supervisor.wait(timeout=20)
            else:
                assert "stop apply_id target is absent" in right_stop.stderr, right_stop.stderr
                current_supervisor.wait(timeout=20)
            remaining_status = _observe_status(module, label)
            assert remaining_status["owner_ts_ms"] == 2, remaining_status
            assert remaining_status["apply_id"] is None, remaining_status
        finally:
            _terminate_process(legacy_supervisor)
            _terminate_process(current_supervisor)


def test_owner_ts_ms_collision_is_rejected() -> None:
    module = _load_runtime_module()
    label = "DaemonSet/test-owner-ts-collision"
    current_state = SimpleNamespace(apply_id="apply-1")
    old = SimpleNamespace(
        pid=11,
        owner_ts_ms=2,
        runtime_state=current_state,
        start_time_ticks=100,
    )
    new = SimpleNamespace(
        pid=22,
        owner_ts_ms=2,
        runtime_state=current_state,
        start_time_ticks=200,
    )
    original_iter = module._iter_live_supervisors
    try:
        module._iter_live_supervisors = lambda *_args, **_kwargs: [old, new]
        try:
            module._selection_owner_supervisor(label)
            raise AssertionError("expected owner_ts_ms collision to raise")
        except RuntimeError as err:
            assert "owner_ts_ms collision" in str(err), err
    finally:
        module._iter_live_supervisors = original_iter


if __name__ == "__main__":
    raise SystemExit(main())
