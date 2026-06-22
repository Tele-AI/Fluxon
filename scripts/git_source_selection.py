from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Callable

import yaml


DEFAULT_RATHER_NO_GIT_SUBMODULE_CONFIG_RELPATH = Path(
    "setup_and_pack/rather_no_git_submodule.yaml"
)


def collect_git_listed_source_relpaths(
    *,
    repo_root: Path,
    git_root: Path,
    rel_prefix: str = "",
    is_excluded: Callable[[str], bool],
) -> list[str]:
    argv = [
        "git",
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
        "-z",
    ]
    raw = subprocess.check_output(argv, cwd=str(git_root))
    selected: list[str] = []
    rel_prefix = rel_prefix.strip("/")
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        rel = entry.decode("utf-8").strip()
        if not rel:
            continue
        repo_rel = rel if not rel_prefix else f"{rel_prefix}/{rel}"
        if is_excluded(repo_rel):
            continue
        source_path = (repo_root / repo_rel).resolve()
        if not source_path.exists():
            continue
        selected.append(repo_rel)
    return selected


def load_rather_no_git_submodule_source_roots(
    *,
    repo_root: Path,
    context_name: str,
) -> tuple[tuple[str, Path], ...]:
    config_path = (repo_root / DEFAULT_RATHER_NO_GIT_SUBMODULE_CONFIG_RELPATH).resolve()
    if not config_path.exists():
        return ()
    raw_cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if raw_cfg is None:
        return ()
    if not isinstance(raw_cfg, dict):
        raise RuntimeError(
            "rather_no_git_submodule config must be a YAML mapping: "
            f"{config_path}"
        )
    raw_modules = raw_cfg.get("modules")
    if raw_modules is None:
        return ()
    if not isinstance(raw_modules, list):
        raise RuntimeError(
            "rather_no_git_submodule config `modules` must be a list: "
            f"{config_path}"
        )

    repo_root = repo_root.resolve()
    selected: list[tuple[str, Path]] = []
    seen_relpaths: set[str] = set()
    for index, raw_item in enumerate(raw_modules):
        if not isinstance(raw_item, dict):
            raise RuntimeError(
                "rather_no_git_submodule config entries must be mappings: "
                f"{config_path} modules[{index}]"
            )
        raw_path = raw_item.get("path")
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise RuntimeError(
                "rather_no_git_submodule config path must be a non-empty string: "
                f"{config_path} modules[{index}].path"
            )
        rel_path = Path(raw_path.strip())
        if rel_path.is_absolute() or ".." in rel_path.parts:
            raise RuntimeError(
                "rather_no_git_submodule config path must stay within the repo root: "
                f"{config_path} modules[{index}].path={raw_path!r}"
            )
        relpath = rel_path.as_posix()
        if relpath in seen_relpaths:
            continue
        seen_relpaths.add(relpath)
        module_root = (repo_root / rel_path).resolve()
        if module_root != repo_root and repo_root not in module_root.parents:
            raise RuntimeError(
                "rather_no_git_submodule config path escapes the repo root: "
                f"{config_path} modules[{index}].path={raw_path!r}"
            )
        if not module_root.is_dir():
            raise RuntimeError(
                f"{context_name} requires configured rather_no_git_submodule path "
                f"to exist as a directory: path={relpath} resolved={module_root}"
            )
        selected.append((relpath, module_root))
    return tuple(selected)


def collect_source_relpaths_with_rather_no_git_submodule(
    *,
    repo_root: Path,
    source_roots: tuple[str, ...],
    is_excluded: Callable[[str], bool],
    empty_selection_error: str,
    rather_no_git_submodule_context_name: str,
) -> list[str]:
    repo_root = repo_root.resolve()
    selected: set[str] = set()
    for source_root in source_roots:
        root_path = (repo_root / source_root).resolve()
        if not root_path.exists():
            continue
        if root_path.is_file():
            relpath = Path(source_root).as_posix()
            if not is_excluded(relpath):
                selected.add(relpath)
            continue
        selected.update(
            collect_git_listed_source_relpaths(
                repo_root=repo_root,
                git_root=root_path,
                rel_prefix="" if source_root == "." else source_root,
                is_excluded=is_excluded,
            )
        )
    for relpath, module_root in load_rather_no_git_submodule_source_roots(
        repo_root=repo_root,
        context_name=rather_no_git_submodule_context_name,
    ):
        selected.update(
            collect_git_listed_source_relpaths(
                repo_root=repo_root,
                git_root=module_root,
                rel_prefix=relpath,
                is_excluded=is_excluded,
            )
        )
    if not selected:
        raise RuntimeError(empty_selection_error)
    return sorted(selected)


__all__ = [
    "DEFAULT_RATHER_NO_GIT_SUBMODULE_CONFIG_RELPATH",
    "collect_git_listed_source_relpaths",
    "collect_source_relpaths_with_rather_no_git_submodule",
    "load_rather_no_git_submodule_source_roots",
]
