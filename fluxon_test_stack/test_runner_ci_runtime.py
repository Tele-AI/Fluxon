from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Callable


_TEST_STACK_DEFAULT_PYTHON_ABI = "cpython3.10"
_CI_RUNTIME_PYTHON_BIN_NAME = "python3.10"


def _ci_runtime_python_executable() -> str:
    python_bin = shutil.which(_CI_RUNTIME_PYTHON_BIN_NAME)
    if python_bin is None:
        raise ValueError(
            "CI runtime requires python3.10 on PATH to create the offline-wheelhouse venv"
        )
    return python_bin


def _ci_runtime_python_abi(
    *,
    venv_python: Path,
    normalize_python_abi: Callable[[str], str],
) -> str:
    try:
        raw = subprocess.check_output(
            [
                str(venv_python),
                "-c",
                (
                    "import sys; "
                    "print(f'{sys.implementation.name}{sys.version_info[0]}.{sys.version_info[1]}')"
                ),
            ],
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ValueError(f"failed to probe CI runtime venv python ABI: python={venv_python}") from exc
    return normalize_python_abi(raw)


def _assert_ci_runtime_python_abi(
    *,
    venv_python: Path,
    normalize_python_abi: Callable[[str], str],
) -> None:
    got_python_abi = _ci_runtime_python_abi(
        venv_python=venv_python,
        normalize_python_abi=normalize_python_abi,
    )
    if got_python_abi != _TEST_STACK_DEFAULT_PYTHON_ABI:
        raise ValueError(
            "CI runtime venv python ABI must match the prepared offline wheelhouse: "
            f"expected={_TEST_STACK_DEFAULT_PYTHON_ABI} got={got_python_abi} python={venv_python}"
        )


def _create_ci_runtime_venv(
    *,
    run_dir: Path,
    run_subprocess: Callable[[list[str]], None],
    assert_python_abi: Callable[[Path], None],
) -> Path:
    venv_dir = (run_dir / "venv").resolve()
    if venv_dir.exists():
        raise ValueError(f"venv dir already exists (no overwrite): {venv_dir}")
    python_bin = _ci_runtime_python_executable()
    run_subprocess([python_bin, "-m", "venv", str(venv_dir)])
    venv_python = venv_dir / "bin" / "python3"
    if not venv_python.exists():
        raise ValueError(f"venv python not found after creation: {venv_python}")
    assert_python_abi(venv_python)
    return venv_python
