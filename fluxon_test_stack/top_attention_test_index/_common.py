from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_REQUIREMENTS: list[str] = ["ops"]


def call(cmd: Sequence[str], *, env: dict[str, str] | None = None) -> int:
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.call(list(cmd), cwd=str(REPO_ROOT), env=env)


def parse_python_passthrough(description: str) -> tuple[str, list[str]]:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON", sys.executable),
        help="Python executable used for the delegated command.",
    )
    args, passthrough = parser.parse_known_args()
    return args.python, passthrough


def run_pytest(description: str, paths: Iterable[str]) -> int:
    python, passthrough = parse_python_passthrough(description)
    return call([python, "-m", "pytest", *paths, *passthrough])


def run_python_file(description: str, path: str, extra_args: Iterable[str] = ()) -> int:
    python, passthrough = parse_python_passthrough(description)
    return call([python, "-u", str(REPO_ROOT / path), *extra_args, *passthrough])


def run_python_files(description: str, paths: Iterable[str]) -> int:
    python, passthrough = parse_python_passthrough(description)
    for path in paths:
        rc = call([python, "-u", str(REPO_ROOT / path), *passthrough])
        if rc != 0:
            return rc
    return 0


def run_cargo(args: Iterable[str], *, env: dict[str, str] | None = None) -> int:
    return call(["cargo", *args], env=env)
