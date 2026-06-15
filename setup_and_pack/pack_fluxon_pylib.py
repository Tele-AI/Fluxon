#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _remove_build_artifact(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink()


def _clean_python_build_artifacts(*, repo_root: Path, release_dir: Path) -> None:
    for artifact in (repo_root / "build", repo_root / "dist"):
        if artifact.resolve() == release_dir.resolve():
            continue
        _remove_build_artifact(artifact)
    for artifact in repo_root.glob("*.egg-info"):
        _remove_build_artifact(artifact)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build pure Python wheel into the given release directory")
    parser.add_argument(
        "--release-dir",
        type=Path,
        default=None,
        help=(
            "Release directory root; if relative, resolve against the repo root inferred from "
            "this script path; defaults to <repo_root>/fluxon_release"
        ),
    )
    args = parser.parse_args()

    setup_py = REPO_ROOT / "setup.py"
    if not setup_py.exists():
        print(f"Missing setup.py at repo root: {setup_py}")
        return 1

    release_dir = (
        _resolve_repo_root_cli_path(raw_path=args.release_dir, field_name="release-dir")
        if args.release_dir is not None
        else (REPO_ROOT / "fluxon_release")
    )
    release_dir.mkdir(parents=True, exist_ok=True)
    _clean_python_build_artifacts(repo_root=REPO_ROOT, release_dir=release_dir)

    subprocess.check_call(
        [sys.executable, "setup.py", "bdist_wheel", "-d", str(release_dir)],
        cwd=str(REPO_ROOT),
    )
    print(f"Built Python library wheel(s) into: {release_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
