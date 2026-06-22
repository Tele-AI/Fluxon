#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_IMAGE_REF = "fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0"
CONTAINER_CACHE_ROOT = "/opt/fluxon_doc_site_cache"

repo_root_str = str(REPO_ROOT)
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)

from setup_and_pack.utils import sudo_prefix


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Fluxon doc site inside the prewarmed Docker image.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--image-ref", default=os.environ.get("FLUXON_DOC_SITE_DOCKER_IMAGE_REF", DEFAULT_IMAGE_REF))
    parser.add_argument("--image-tar", type=Path, default=None)
    parser.add_argument("--base-url", required=True)
    return parser.parse_args()


def _resolve_path(path: Path, *, base: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (base / path).resolve()


def _docker(*args: str) -> None:
    subprocess.check_call(sudo_prefix() + ["docker", *args])


def _load_image_if_requested(*, image_tar: Path | None, repo_root: Path) -> None:
    if image_tar is None:
        return
    resolved = _resolve_path(image_tar, base=repo_root)
    if not resolved.is_file():
        raise RuntimeError(f"doc-site docker image archive is missing: {resolved}")
    _docker("load", "-i", str(resolved))


def _run_build(*, repo_root: Path, image_ref: str, base_url: str) -> None:
    repo_root = repo_root.resolve()
    if not (repo_root / "scripts" / "build_doc_site.py").is_file():
        raise RuntimeError(f"repo root is missing scripts/build_doc_site.py: {repo_root}")
    command = "\n".join(
        [
            "set -euo pipefail",
            "umask 000",
            "python3 scripts/build_doc_site.py build",
            "chmod -R a+rwX fluxon_release/doc_site",
        ]
    )
    subprocess.check_call(
        sudo_prefix()
        + [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "/bin/bash",
            "-e",
            f"FLUXON_DOC_SITE_BASE_URL={base_url}",
            "-e",
            f"FLUXON_DOC_SITE_CACHE_ROOT={CONTAINER_CACHE_ROOT}",
            "-v",
            f"{repo_root}:/workspace",
            "-w",
            "/workspace",
            image_ref,
            "-lc",
            command,
        ]
    )


def main() -> int:
    args = _parse_args()
    repo_root = _resolve_path(args.repo_root, base=REPO_ROOT)
    _load_image_if_requested(image_tar=args.image_tar, repo_root=repo_root)
    _run_build(repo_root=repo_root, image_ref=args.image_ref, base_url=args.base_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
