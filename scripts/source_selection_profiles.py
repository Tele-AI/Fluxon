from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
script_dir_str = str(SCRIPT_DIR)
if script_dir_str in sys.path:
    sys.path.remove(script_dir_str)
sys.path.insert(0, script_dir_str)

import git_source_selection as git_source_selection_utils


SOURCE_SELECTION_PROFILE_BUILD_SEED = "build_seed"
SOURCE_SELECTION_PROFILE_SOURCE_PACK = "source_pack"
SOURCE_SELECTION_PROFILES = (
    SOURCE_SELECTION_PROFILE_BUILD_SEED,
    SOURCE_SELECTION_PROFILE_SOURCE_PACK,
)

BUILD_SEED_SOURCE_ROOTS: tuple[str, ...] = (
    "README.md",
    "setup.py",
    "deployment",
    "fluxon_py",
    "fluxon_release/closed_sdk",
    "fluxon_rs",
    "scripts/git_source_selection.py",
    "scripts/source_selection_profiles.py",
    "setup_and_pack",
)
SOURCE_PACK_SOURCE_ROOTS: tuple[str, ...] = (".",)

BUILD_SEED_INCLUDED_RELPATHS: frozenset[str] = frozenset(
    {
        "fluxon_release/closed_sdk/manifest.json",
        "setup_and_pack/pub_prepare_build.yaml",
    }
)
SOURCE_PACK_EXCLUDED_RELPATH_PREFIXES: tuple[str, ...] = (
    ".dever/",
    "fluxon_release/",
    "skills/",
)
SOURCE_PACK_EXCLUDED_RELPATH_NAMES: frozenset[str] = frozenset(
    {
        ".DS_Store",
    }
)


@dataclass(frozen=True)
class SourceSelectionProfileSpec:
    source_roots: tuple[str, ...]
    empty_selection_error: str
    rather_no_git_submodule_context_name: str
    include_relpaths: frozenset[str] = field(default_factory=frozenset)


BUILD_SEED_PROFILE_SPEC = SourceSelectionProfileSpec(
    source_roots=BUILD_SEED_SOURCE_ROOTS,
    empty_selection_error="public workspace source selection produced no files",
    rather_no_git_submodule_context_name="public workspace source selection",
    include_relpaths=BUILD_SEED_INCLUDED_RELPATHS,
)
SOURCE_PACK_PROFILE_SPEC = SourceSelectionProfileSpec(
    source_roots=SOURCE_PACK_SOURCE_ROOTS,
    empty_selection_error="git-based CI source selection produced no files",
    rather_no_git_submodule_context_name="CI source pack",
)


def get_source_profile_spec(*, profile: str) -> SourceSelectionProfileSpec:
    if profile == SOURCE_SELECTION_PROFILE_BUILD_SEED:
        return BUILD_SEED_PROFILE_SPEC
    if profile == SOURCE_SELECTION_PROFILE_SOURCE_PACK:
        return SOURCE_PACK_PROFILE_SPEC
    raise ValueError(
        f"unsupported source selection profile: {profile!r}; expected one of {SOURCE_SELECTION_PROFILES}"
    )


def get_source_profile_source_roots(*, profile: str) -> tuple[str, ...]:
    return get_source_profile_spec(profile=profile).source_roots


def source_profile_relpath_excluded(*, profile: str, relpath: str) -> bool:
    spec = get_source_profile_spec(profile=profile)
    normalized = relpath.strip("/")
    if not normalized:
        return True
    if normalized in spec.include_relpaths:
        return False
    if profile == SOURCE_SELECTION_PROFILE_SOURCE_PACK:
        if normalized in SOURCE_PACK_EXCLUDED_RELPATH_NAMES:
            return True
        return any(
            normalized == prefix.rstrip("/") or normalized.startswith(prefix)
            for prefix in SOURCE_PACK_EXCLUDED_RELPATH_PREFIXES
        )
    return False


def collect_source_profile_relpaths(*, repo_root: Path, profile: str) -> tuple[str, ...]:
    spec = get_source_profile_spec(profile=profile)
    return tuple(
        git_source_selection_utils.collect_source_relpaths_with_rather_no_git_submodule(
            repo_root=repo_root,
            source_roots=spec.source_roots,
            is_excluded=lambda relpath: source_profile_relpath_excluded(
                profile=profile,
                relpath=relpath,
            ),
            empty_selection_error=spec.empty_selection_error,
            rather_no_git_submodule_context_name=spec.rather_no_git_submodule_context_name,
        )
    )


__all__ = [
    "BUILD_SEED_SOURCE_ROOTS",
    "SOURCE_PACK_SOURCE_ROOTS",
    "SOURCE_PACK_EXCLUDED_RELPATH_NAMES",
    "SOURCE_PACK_EXCLUDED_RELPATH_PREFIXES",
    "SOURCE_SELECTION_PROFILE_BUILD_SEED",
    "SOURCE_SELECTION_PROFILE_SOURCE_PACK",
    "SOURCE_SELECTION_PROFILES",
    "collect_source_profile_relpaths",
    "get_source_profile_source_roots",
    "get_source_profile_spec",
    "source_profile_relpath_excluded",
]
