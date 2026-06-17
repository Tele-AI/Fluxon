#!/usr/bin/env python3
"""
Clone and checkout a fixed set of "submodules" without using git submodule.

This script always attempts to sync each configured module to the latest state of the
configured ref. If any existing module has local modifications, it fails fast and stops,
so the user can handle their workspace explicitly.

CLI:
  -c/--config: YAML config path (optional; defaults to
                setup_and_pack/rather_no_git_submodule.yaml)
  -w/--workdir: repo root (optional; defaults to the repo root inferred from this script path)

Config schema (YAML):
  modules:
    - path: third_party/some_dep
      repo: https://example.com/some_dep.git
      checkout: v1.2.3
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

import yaml


# Default workdir is inferred from this script path to keep invocation simple and deterministic.
DEFAULT_WORKDIR: Path = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_REL_PATH: str = "setup_and_pack/rather_no_git_submodule.yaml"

# Keep SSH host key handling non-interactive for CI automation.
GIT_SSH_COMMAND_VALUE: str = "ssh -o StrictHostKeyChecking=accept-new"
ORIGIN_REMOTE: str = "origin"
STATUS_IGNORE_SUBMODULES_ARGS: list[str] = ["--ignore-submodules=all"]


def _resolve_repo_root_cli_path(*, raw_path: str, field_name: str) -> Path:
    raw = Path(raw_path)
    if raw.is_absolute():
        return raw.resolve()
    resolved = (DEFAULT_WORKDIR / raw).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _git_cmd(cmd: list[str]) -> list[str]:
    if not cmd:
        return cmd
    if cmd[0] != "git":
        return cmd
    return ["git", "-c", f"core.sshCommand={GIT_SSH_COMMAND_VALUE}", *cmd[1:]]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Clone and checkout a configured module list (no git submodule)."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default=None,
        help=f"YAML config path (optional; defaults to {DEFAULT_CONFIG_REL_PATH} under workdir)",
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

    # Default config path is explicit (a well-known filename) so the script is runnable
    # without CLI flags while still keeping configuration as data.
    config_path = Path(args.config) if args.config else (workdir / DEFAULT_CONFIG_REL_PATH)
    if not config_path.is_absolute():
        config_path = (workdir / config_path).resolve()
    if not config_path.exists():
        print(f"config does not exist: {config_path}")
        return 2
    if not config_path.is_file():
        print(f"config is not a file: {config_path}")
        return 2

    specs = _load_cfg(config_path)

    for spec in specs:
        rel_path = Path(spec.rel_path)
        if rel_path.is_absolute():
            print(f"❌ rel_path must be relative, got: {spec.rel_path}")
            return 2
        if ".." in rel_path.parts:
            print(f"❌ rel_path must not contain '..', got: {spec.rel_path}")
            return 2
        if not spec.repo_url.strip():
            print(f"❌ repo_url must be non-empty for: {spec.rel_path}")
            return 2
        if not spec.checkout.strip():
            print(f"❌ checkout must be non-empty for: {spec.rel_path}")
            return 2

        dest = (workdir / spec.rel_path).resolve()
        if not _is_within_base(workdir, dest):
            print(f"❌ rel_path escapes workdir: workdir={workdir} rel_path={spec.rel_path} dest={dest}")
            return 2

        if dest.exists():
            if dest.is_dir() and not any(dest.iterdir()):
                dest.rmdir()
            else:
                rc = _sync_existing_repo(dest, spec, workdir)
                if rc != 0:
                    return rc
                print(f"✅ {spec.rel_path}: ok")
                continue

        dest.parent.mkdir(parents=True, exist_ok=True)
        rc = _run_checked(["git", "clone", spec.repo_url, str(dest)], cwd=workdir)
        if rc != 0:
            print(f"❌ {spec.rel_path}: git clone failed (rc={rc})")
            return 1

        rc = _sync_existing_repo(dest, spec, workdir)
        if rc != 0:
            return rc
        print(f"✅ {spec.rel_path}: ok")

    print("✅ completed successfully")
    return 0


@dataclass(frozen=True)
class ModuleSpec:
    rel_path: str
    repo_url: str
    checkout: str  # branch/tag/commit


def _is_within_base(base_dir: Path, target: Path) -> bool:
    base_dir = base_dir.resolve()
    target = target.resolve()
    return base_dir == target or base_dir in target.parents


def _run_checked(cmd: list[str], *, cwd: Path) -> int:
    print(f"+ {shlex.join(cmd)}")
    completed = subprocess.run(_git_cmd(cmd), cwd=str(cwd), check=False)
    if completed.returncode != 0:
        print(f"command failed (rc={completed.returncode}): {shlex.join(cmd)}")
    return completed.returncode


def _run_capture(cmd: list[str], *, cwd: Path) -> tuple[int, str]:
    print(f"+ {shlex.join(cmd)}")
    completed = subprocess.run(
        _git_cmd(cmd),
        cwd=str(cwd),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return completed.returncode, completed.stdout or ""


def _sync_existing_repo(dest: Path, spec: ModuleSpec, workdir: Path) -> int:
    if not dest.is_dir():
        print(f"❌ {spec.rel_path}: expected a directory, got: {dest}")
        return 1

    rc = _run_checked(["git", "-C", str(dest), "rev-parse", "--is-inside-work-tree"], cwd=workdir)
    if rc != 0:
        print(f"❌ {spec.rel_path}: not a git repo: {dest}")
        return 1

    rc, remote_url = _run_capture(["git", "-C", str(dest), "remote", "get-url", ORIGIN_REMOTE], cwd=workdir)
    if rc != 0:
        if remote_url:
            print(remote_url, end="")
        print(f"❌ {spec.rel_path}: missing git remote '{ORIGIN_REMOTE}': {dest}")
        return 1
    if remote_url.strip() != spec.repo_url.strip():
        print(
            f"❌ {spec.rel_path}: remote URL mismatch for '{ORIGIN_REMOTE}'\n"
            f"    expected: {spec.repo_url.strip()}\n"
            f"    actual:   {remote_url.strip()}"
        )
        return 1

    # Submodules are handled as independent entries in the module list. We ignore submodule state
    # differences in the superproject, and only fail fast for real local modifications.
    rc, porcelain = _run_capture(
        ["git", "-C", str(dest), "status", "--porcelain", *STATUS_IGNORE_SUBMODULES_ARGS],
        cwd=workdir,
    )
    if rc != 0:
        if porcelain:
            print(porcelain, end="")
        print(f"❌ {spec.rel_path}: failed to read git status: {dest}")
        return 1
    if porcelain.strip():
        print(f"❌ {spec.rel_path}: local modifications detected; please handle them first:\n{porcelain}")
        return 1

    rc = _run_checked(["git", "-C", str(dest), "fetch", "--prune", ORIGIN_REMOTE], cwd=workdir)
    if rc != 0:
        print(f"❌ {spec.rel_path}: git fetch failed")
        return 1

    rc = _run_checked(
        [
            "git",
            "-C",
            str(dest),
            "-c",
            "advice.detachedHead=false",
            "checkout",
            spec.checkout,
        ],
        cwd=workdir,
    )
    # NOTE: Avoid forcing `--detach` here. Some refs in our module list are plain branch names
    # (e.g. `limityummakecache`), and `git checkout --detach <branch>` is not reliable across
    # environments. Plain `git checkout <ref>` keeps behavior stable for branches/tags/commits.
    if rc != 0:
        print(f"❌ {spec.rel_path}: git checkout failed (rc={rc})")
        return 1

    # If checkout resolves to a local branch, fast-forward it to the latest remote branch.
    # This is the only update mode we support; we do not stash/reset/rebase automatically.
    rc, branch_name = _run_capture(
        ["git", "-C", str(dest), "symbolic-ref", "--quiet", "--short", "HEAD"],
        cwd=workdir,
    )
    if rc == 0 and branch_name.strip():
        remote_branch = f"{ORIGIN_REMOTE}/{branch_name.strip()}"
        rc = _run_checked(["git", "-C", str(dest), "rev-parse", "--verify", remote_branch], cwd=workdir)
        if rc != 0:
            print(f"❌ {spec.rel_path}: remote branch does not exist: {remote_branch}")
            return 1

        rc = _run_checked(["git", "-C", str(dest), "merge", "--ff-only", remote_branch], cwd=workdir)
        if rc != 0:
            print(
                f"❌ {spec.rel_path}: fast-forward update failed; local branch '{branch_name.strip()}' is not a strict ancestor of {remote_branch}.\n"
                "    Resolve the divergence manually (e.g., reset/rebase/merge), then rerun this script."
            )
            return 1

    return 0


def _load_cfg(config_path: Path) -> list[ModuleSpec]:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ValueError("Config must be a YAML mapping with key: modules")

    modules = cfg.get("modules")
    if not isinstance(modules, list) or not modules:
        raise ValueError("Config key `modules` must be a non-empty list")

    specs: list[ModuleSpec] = []
    for i, item in enumerate(modules):
        if not isinstance(item, dict):
            raise ValueError(f"modules[{i}] must be a mapping")
        rel_path = item.get("path")
        repo_url = item.get("repo")
        checkout = item.get("checkout")
        if not isinstance(rel_path, str) or not rel_path.strip():
            raise ValueError(f"modules[{i}].path must be a non-empty string")
        if not isinstance(repo_url, str) or not repo_url.strip():
            raise ValueError(f"modules[{i}].repo must be a non-empty string")
        if not isinstance(checkout, str) or not checkout.strip():
            raise ValueError(f"modules[{i}].checkout must be a non-empty string")
        specs.append(ModuleSpec(rel_path=rel_path, repo_url=repo_url, checkout=checkout))

    return specs


if __name__ == "__main__":
    raise SystemExit(main())
