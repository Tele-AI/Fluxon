#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


def main() -> None:
    unittest.main()


from fluxon_py.runtime.process_runner import (  # noqa: E402
    build_runtime_singleton_spec,
    _stop_existing_processes_if_running,
)


def _new_test_dir(tag: str) -> Path:
    base = REPO_ROOT / "fluxon_py" / "tests" / ".tmp_process_runner"
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{tag}_{int(time.time() * 1000)}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def _wait_until(path: Path, *, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timeout waiting for file: {path}")


def _pid_alive(pid: int) -> bool:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return False
    try:
        for line in status_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("State:"):
                fields = line.split()
                return len(fields) >= 2 and fields[1] != "Z"
    except FileNotFoundError:
        return False
    return False


class TestProcessRunner(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = _new_test_dir("process_runner")
        self.addCleanup(lambda: shutil.rmtree(self._tmp, ignore_errors=False))

    def test_stop_existing_processes_if_running_retires_grandchild_tree(self) -> None:
        wrapper_path = self._tmp / "wrapper.py"
        marker_path = self._tmp / "grandchild.pid"
        workdir_path = self._tmp / "singleton_workdir"
        workdir_path.mkdir(parents=True, exist_ok=True)
        singleton_module_name = "test.process_runner.singleton_wrapper"
        wrapper_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import os",
                    "import subprocess",
                    "import sys",
                    "import time",
                    "from pathlib import Path",
                    "",
                    "marker = Path(sys.argv[1]).resolve()",
                    "os.chdir(Path(sys.argv[2]).resolve())",
                    "child = subprocess.Popen(",
                    "    [",
                    "        sys.executable,",
                    "        '-c',",
                    "        '\\n'.join([",
                    "            'import signal',",
                    "            'import time',",
                    "            'signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(0)))',",
                    "            'signal.signal(signal.SIGINT, lambda *_: (_ for _ in ()).throw(SystemExit(0)))',",
                    "            'while True:',",
                    "            '    time.sleep(0.2)',",
                    "        ]),",
                    "    ],",
                    ")",
                    "marker.write_text(str(child.pid), encoding='utf-8')",
                    "while True:",
                    "    time.sleep(0.2)",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        wrapper_proc = subprocess.Popen(
            [
                sys.executable,
                str(wrapper_path),
                str(marker_path),
                str(workdir_path),
                "-m",
                singleton_module_name,
                "--workdir",
                str(workdir_path),
            ],
            cwd=str(self._tmp),
        )
        try:
            _wait_until(marker_path, timeout_seconds=10.0)
            grandchild_pid = int(marker_path.read_text(encoding="utf-8").strip())
            self.assertTrue(_pid_alive(grandchild_pid), grandchild_pid)

            singleton_spec = build_runtime_singleton_spec(
                module_name=singleton_module_name,
                entrypoint_path=wrapper_path,
                workdir=workdir_path,
            )
            _stop_existing_processes_if_running(
                singleton_spec=singleton_spec,
                stop_timeout_seconds=5,
            )

            deadline = time.time() + 10.0
            while time.time() < deadline and _pid_alive(grandchild_pid):
                time.sleep(0.1)
            self.assertFalse(_pid_alive(grandchild_pid), grandchild_pid)
        finally:
            if wrapper_proc.poll() is None:
                wrapper_proc.kill()
                wrapper_proc.wait(timeout=10)

    def test_bind_current_process_parent_death_sigterm_exits_when_parent_exits(self) -> None:
        child_path = self._tmp / "child.py"
        parent_path = self._tmp / "parent.py"
        ready_path = self._tmp / "child.pid"
        child_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import os",
                    "import sys",
                    "import time",
                    "from pathlib import Path",
                    "",
                    f"sys.path.insert(0, {str(REPO_ROOT)!r})",
                    "from fluxon_py.runtime.process_runner import bind_current_process_parent_death_sigterm",
                    "",
                    "ready = Path(sys.argv[1]).resolve()",
                    "bind_current_process_parent_death_sigterm()",
                    "ready.write_text(str(os.getpid()), encoding='utf-8')",
                    "while True:",
                    "    time.sleep(0.2)",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        parent_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import subprocess",
                    "import sys",
                    "import time",
                    "from pathlib import Path",
                    "",
                    "child_path = Path(sys.argv[1]).resolve()",
                    "ready = Path(sys.argv[2]).resolve()",
                    "subprocess.Popen([sys.executable, str(child_path), str(ready)], cwd=str(ready.parent))",
                    "deadline = time.time() + 10.0",
                    "while time.time() < deadline:",
                    "    if ready.exists():",
                    "        raise SystemExit(0)",
                    "    time.sleep(0.1)",
                    "raise RuntimeError(f'timeout waiting for ready file: {ready}')",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        parent_proc = subprocess.Popen(
            [sys.executable, str(parent_path), str(child_path), str(ready_path)],
            cwd=str(self._tmp),
        )
        _wait_until(ready_path, timeout_seconds=10.0)
        child_pid = int(ready_path.read_text(encoding="utf-8").strip())
        parent_proc.wait(timeout=10.0)

        deadline = time.time() + 10.0
        while time.time() < deadline and _pid_alive(child_pid):
            time.sleep(0.1)
        if _pid_alive(child_pid):
            os.kill(child_pid, 9)
        self.assertFalse(_pid_alive(child_pid), child_pid)

    def test_wait_subproc_or_ctrlc_retires_children_on_sigterm(self) -> None:
        launcher_path = self._tmp / "launcher.py"
        parent_path = self._tmp / "parent.py"
        ready_path = self._tmp / "launcher_state.txt"
        launcher_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import os",
                    "import subprocess",
                    "import sys",
                    "import time",
                    "from pathlib import Path",
                    "",
                    f"sys.path.insert(0, {str(REPO_ROOT)!r})",
                    "from fluxon_py.runtime.process_runner import (",
                    "    ManagedSubprocess,",
                    "    bind_current_process_parent_death_sigterm,",
                    "    wait_subproc_or_ctrlc,",
                    ")",
                    "",
                    "ready = Path(sys.argv[1]).resolve()",
                    "bind_current_process_parent_death_sigterm()",
                    "worker = subprocess.Popen(",
                    "    [sys.executable, '-c', 'import time\\nwhile True:\\n    time.sleep(0.2)'],",
                    ")",
                    "ready.write_text(f'{os.getpid()} {worker.pid}', encoding='utf-8')",
                    "wait_subproc_or_ctrlc(",
                    "    [ManagedSubprocess(label='worker', proc=worker)],",
                    "    on_ctrlc=lambda: None,",
                    ")",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        parent_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env python3",
                    "import subprocess",
                    "import sys",
                    "import time",
                    "from pathlib import Path",
                    "",
                    "launcher_path = Path(sys.argv[1]).resolve()",
                    "ready = Path(sys.argv[2]).resolve()",
                    "subprocess.Popen([sys.executable, str(launcher_path), str(ready)], cwd=str(ready.parent))",
                    "deadline = time.time() + 10.0",
                    "while time.time() < deadline:",
                    "    if ready.exists():",
                    "        raise SystemExit(0)",
                    "    time.sleep(0.1)",
                    "raise RuntimeError(f'timeout waiting for ready file: {ready}')",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        parent_proc = subprocess.Popen(
            [sys.executable, str(parent_path), str(launcher_path), str(ready_path)],
            cwd=str(self._tmp),
        )
        _wait_until(ready_path, timeout_seconds=10.0)
        launcher_pid, worker_pid = (
            int(part) for part in ready_path.read_text(encoding="utf-8").strip().split()
        )
        parent_proc.wait(timeout=10.0)

        deadline = time.time() + 10.0
        while time.time() < deadline and (_pid_alive(launcher_pid) or _pid_alive(worker_pid)):
            time.sleep(0.1)
        if _pid_alive(launcher_pid):
            os.kill(launcher_pid, 9)
        if _pid_alive(worker_pid):
            os.kill(worker_pid, 9)
        self.assertFalse(_pid_alive(launcher_pid), launcher_pid)
        self.assertFalse(_pid_alive(worker_pid), worker_pid)


if __name__ == "__main__":
    main()
