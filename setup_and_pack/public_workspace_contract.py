from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
scripts_dir_str = str(SCRIPTS_DIR)
if scripts_dir_str in sys.path:
    sys.path.remove(scripts_dir_str)
sys.path.insert(0, scripts_dir_str)

from source_selection_profiles import (
    SOURCE_SELECTION_PROFILE_BUILD_SEED,
    collect_source_profile_relpaths,
)


def _copy_public_workspace_input_path(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if source_path.is_symlink():
        if target_path.exists() or target_path.is_symlink():
            if target_path.is_dir() and not target_path.is_symlink():
                shutil.rmtree(target_path)
            else:
                target_path.unlink()
        os.symlink(os.readlink(source_path), target_path)
        return
    if source_path.is_dir():
        shutil.copytree(source_path, target_path, symlinks=True, dirs_exist_ok=True)
        return
    shutil.copy2(source_path, target_path)


def collect_public_workspace_input_relative_paths(*, repo_root: Path) -> tuple[str, ...]:
    return collect_source_profile_relpaths(
        repo_root=repo_root,
        profile=SOURCE_SELECTION_PROFILE_BUILD_SEED,
    )


def _sanitize_public_workspace_input(*, workspace_root: Path) -> None:
    for pycache_dir in workspace_root.rglob("__pycache__"):
        shutil.rmtree(pycache_dir, ignore_errors=True)
    for pyc_path in workspace_root.rglob("*.pyc"):
        try:
            pyc_path.unlink()
        except FileNotFoundError:
            pass

__all__ = [
    "collect_public_workspace_input_relative_paths",
    "_copy_public_workspace_input_path",
    "_sanitize_public_workspace_input",
]
