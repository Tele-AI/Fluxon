#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_WORKDIR: Path = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_REL_PATH: str = "setup_and_pack/rather_no_git_submodule.local.yaml"
DEFAULT_FALLBACK_CONFIG_REL_PATH: str = "setup_and_pack/rather_no_git_submodule.yaml"


def _resolve_repo_root_cli_path(*, raw_path: str, field_name: str) -> Path:
    raw = Path(raw_path)
    if raw.is_absolute():
        return raw.resolve()
    resolved = (DEFAULT_WORKDIR / raw).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Clone and checkout a configured module list without using `git submodule`.\n"
            "This is the canonical entrypoint; it delegates to setup_and_pack/rather_no_git_submodule.py."
        )
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=None,
        help=(
            "YAML config path (optional; defaults to "
            f"{DEFAULT_CONFIG_REL_PATH} under workdir when present, otherwise "
            f"{DEFAULT_FALLBACK_CONFIG_REL_PATH})"
        ),
    )
    parser.add_argument(
        "-w",
        "--workdir",
        type=str,
        default=None,
        help=(
            "Base directory for module paths (optional; if relative, resolve against the repo root "
            "inferred from this script path)"
        ),
    )
    args = parser.parse_args()

    workdir = _resolve_repo_root_cli_path(raw_path=args.workdir, field_name="workdir") if args.workdir else DEFAULT_WORKDIR
    if not workdir.exists():
        print(f"workdir does not exist: {workdir}")
        return 2
    if not workdir.is_dir():
        print(f"workdir is not a directory: {workdir}")
        return 2

    delegate = DEFAULT_WORKDIR / "setup_and_pack" / "rather_no_git_submodule.py"
    if not delegate.exists():
        print(f"delegate script does not exist: {delegate}")
        return 2
    if not delegate.is_file():
        print(f"delegate script is not a file: {delegate}")
        return 2

    cmd: list[str] = [sys.executable, str(delegate)]
    if args.config:
        cmd += ["-c", args.config]
    if args.workdir:
        cmd += ["-w", args.workdir]

    print("+ " + " ".join(cmd))
    completed = subprocess.run(cmd, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
