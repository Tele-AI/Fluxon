from __future__ import annotations

import enum
import fnmatch
import hashlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Collection, Iterator, Sequence

from .shell_command_utils import require_cmd, run_cmd_argv

PIGZ_JOBS = 16

__all__ = [
    "PIGZ_JOBS",
    "PathHashAlgorithm",
    "PathDigestMode",
    "ArtifactState",
    "ArtifactCheck",
    "ArtifactRule",
    "tarball_rule",
    "prune_stage_paths",
    "build_cached_tarball",
    "rsync_stage",
    "tar_gz",
    "_iter_digest_entries",
    "compute_paths_digest",
]



class PathHashAlgorithm(enum.Enum):
    MD5 = "md5"
    SHA256 = "sha256"


class PathDigestMode(enum.Enum):
    CONTENTS_ONLY = "contents_only"
    PACK_INPUTS = "pack_inputs"


class ArtifactState(enum.Enum):
    READY = "ready"
    MISSING_STAMP = "missing_stamp"
    INPUTS_CHANGED = "inputs_changed"
    OUTPUTS_MISSING = "outputs_missing"


@dataclass(frozen=True)
class ArtifactCheck:
    state: ArtifactState
    digest: str
    cached_digest: str | None

    def is_ready(self) -> bool:
        return self.state is ArtifactState.READY


class ArtifactRule:
    def __init__(
        self,
        *,
        name: str,
        stamp_path: Path,
        compute_digest: Callable[[], str],
        outputs_ready: Callable[[], bool],
    ) -> None:
        self.name = name
        self.stamp_path = stamp_path
        self._compute_digest = compute_digest
        self._outputs_ready = outputs_ready

    def check(self) -> ArtifactCheck:
        digest = self._compute_digest()
        if not self.stamp_path.exists():
            return ArtifactCheck(ArtifactState.MISSING_STAMP, digest, None)
        cached_digest = self.stamp_path.read_text(encoding="utf-8").strip()
        if cached_digest != digest:
            return ArtifactCheck(ArtifactState.INPUTS_CHANGED, digest, cached_digest)
        if not self._outputs_ready():
            return ArtifactCheck(ArtifactState.OUTPUTS_MISSING, digest, cached_digest)
        return ArtifactCheck(ArtifactState.READY, digest, cached_digest)

    def write_stamp(self, digest: str) -> None:
        self.stamp_path.parent.mkdir(parents=True, exist_ok=True)
        self.stamp_path.write_text(digest + "\n", encoding="utf-8")


def tarball_rule(*, name: str, out_path: Path, input_paths: list[Path], relative_to: Path) -> ArtifactRule:
    return ArtifactRule(
        name=name,
        stamp_path=out_path.parent / f"{out_path.name}.input.sha256",
        compute_digest=lambda: compute_paths_digest(
            input_paths,
            relative_to=relative_to,
            mode=PathDigestMode.PACK_INPUTS,
            algorithm=PathHashAlgorithm.SHA256,
            ignored_dir_names=(),
            ignored_file_names=(),
            ignored_file_suffixes=(),
        ),
        outputs_ready=out_path.exists,
    )


def build_cached_tarball(*, rule: ArtifactRule, out_path: Path, build_tarball: Callable[[], None]) -> None:
    check = rule.check()
    if check.is_ready():
        print(f"Using cached tarball without rebuild: {out_path}")
        return
    build_tarball()
    if not out_path.exists():
        print(f"Missing tarball after build: {out_path}")
        raise SystemExit(1)
    rule.write_stamp(check.digest)


def rsync_stage(
    *,
    repo_root: Path,
    src: Path,
    dst: Path,
    honor_gitignore: bool,
    exclude_rel_paths: tuple[str, ...] = (),
) -> None:
    if not src.exists():
        print(f"Missing required source path for staging: {src}")
        raise SystemExit(1)
    if dst.exists():
        print(f"Staging destination already exists (no overwrite): {dst}")
        raise SystemExit(1)
    dst.parent.mkdir(parents=True, exist_ok=True)

    require_cmd("rsync")

    argv = ["rsync", "-a"]
    if honor_gitignore:
        argv += [
            "--exclude=.git/",
            "--exclude-from=.gitignore",
            "--filter=:- .gitignore",
        ]
    for pattern in exclude_rel_paths:
        argv.append(f"--exclude={pattern}")
    if src.is_dir():
        argv += [str(src) + "/", str(dst) + "/"]
    else:
        argv += [str(src), str(dst)]
    run_cmd_argv(argv, cwd=repo_root)


def prune_stage_paths(stage_root: Path, exclude_rel_paths: tuple[str, ...]) -> None:
    if not stage_root.exists():
        return
    for path in sorted(stage_root.rglob("*"), reverse=True):
        rel_path = path.relative_to(stage_root).as_posix()
        for pattern in exclude_rel_paths:
            normalized_pattern = pattern.rstrip("/")
            if fnmatch.fnmatch(rel_path, normalized_pattern) or fnmatch.fnmatch(path.name, normalized_pattern):
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink(missing_ok=True)
                break


def tar_gz(
    *,
    cwd: Path,
    out_path: Path,
    inputs: list[str],
    honor_vcs_ignores: bool,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    require_cmd("tar")
    require_cmd("pigz")

    argv: list[str] = [
        "tar",
        "-I",
        f"pigz -p {PIGZ_JOBS}",
        "--exclude=*.pyc",
        "-cf",
        str(out_path),
        "-C",
        str(cwd),
        *inputs,
    ]
    if honor_vcs_ignores:
        pass

    run_cmd_argv(argv)


def _iter_digest_entries(
    roots: Sequence[Path],
    *,
    mode: PathDigestMode,
    ignored_dir_names: Collection[str],
    ignored_file_names: Collection[str],
    ignored_file_suffixes: tuple[str, ...],
) -> Iterator[Path]:
    for root in sorted(Path(root) for root in roots):
        if not root.exists():
            print(f"Missing build input path: {root}")
            raise SystemExit(1)
        if root.is_symlink() or root.is_file():
            if root.name in ignored_file_names or root.name.endswith(ignored_file_suffixes):
                continue
            yield root
            continue
        if not root.is_dir():
            print(f"Unsupported build input path: {root}")
            raise SystemExit(1)
        if mode is PathDigestMode.PACK_INPUTS:
            yield root
        for current_root, dirnames, filenames in os.walk(root, topdown=True):
            dirnames[:] = sorted(
                dir_name for dir_name in dirnames if dir_name not in ignored_dir_names
            )
            current_root_path = Path(current_root)
            if mode is PathDigestMode.PACK_INPUTS:
                for dir_name in dirnames:
                    yield current_root_path / dir_name
            for file_name in sorted(filenames):
                if file_name in ignored_file_names or file_name.endswith(ignored_file_suffixes):
                    continue
                yield current_root_path / file_name


def compute_paths_digest(
    roots: Sequence[Path],
    *,
    relative_to: Path,
    mode: PathDigestMode,
    algorithm: PathHashAlgorithm,
    ignored_dir_names: Collection[str],
    ignored_file_names: Collection[str],
    ignored_file_suffixes: tuple[str, ...],
) -> str:
    hash_obj = hashlib.new(algorithm.value)
    relative_root = relative_to.absolute()
    for path in _iter_digest_entries(
        roots,
        mode=mode,
        ignored_dir_names=ignored_dir_names,
        ignored_file_names=ignored_file_names,
        ignored_file_suffixes=ignored_file_suffixes,
    ):
        relative_name = path.absolute().relative_to(relative_root).as_posix()
        if mode is PathDigestMode.CONTENTS_ONLY:
            hash_obj.update(relative_name.encode("utf-8"))
            hash_obj.update(b"\n")
            if path.is_symlink():
                hash_obj.update(os.readlink(path).encode("utf-8"))
                hash_obj.update(b"\n")
                continue
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    hash_obj.update(chunk)
            continue

        mode_bits = path.lstat().st_mode & 0o777
        if path.is_symlink():
            hash_obj.update(f"symlink {relative_name} {mode_bits}\n".encode("utf-8"))
            hash_obj.update(os.readlink(path).encode("utf-8"))
            hash_obj.update(b"\n")
            continue
        if path.is_dir():
            hash_obj.update(f"dir {relative_name} {mode_bits}\n".encode("utf-8"))
            continue
        if not path.is_file():
            print(f"Unsupported pack input path: {path}")
            raise SystemExit(1)
        hash_obj.update(
            f"file {relative_name} {mode_bits} {path.stat().st_size}\n".encode("utf-8")
        )
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hash_obj.update(chunk)
    return hash_obj.hexdigest()
