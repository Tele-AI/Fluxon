#!/usr/bin/env python3
"""Build fluxon_quick_start Docker image."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
SCRIPTS_DIR = REPO_ROOT / "setup_and_pack"
DOCKERFILE_PATH = SCRIPT_DIR / "Dockerfile"
IMAGE_NAME = "fluxon_quick_start"
IMAGE_TAG = "0.2.1"

# Binaries to copy from ext_images into quick_start bin/.
EXT_BINARIES = ("etcd/etcd", "etcd/etcdctl", "greptime/greptime")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build fluxon_quick_start image")
    parser.add_argument(
        "--mode",
        choices=["existing_release", "url_download"],
        default="existing_release",
        help="existing_release: reuse local fluxon_release; url_download: download wheels from URL",
    )
    parser.add_argument("--fluxon-wheel-url", help="URL for unified fluxon wheel (url_download mode)")
    parser.add_argument("--pylib-src-url", help="URL for pylib_src tarball (url_download mode)")
    parser.add_argument(
        "--release-dir",
        type=Path,
        default=REPO_ROOT / "fluxon_release",
        help="Release directory to consume; relative paths resolve against the repo root",
    )
    args = parser.parse_args()
    release_dir = args.release_dir
    if not release_dir.is_absolute():
        release_dir = (REPO_ROOT / release_dir).resolve()

    if args.mode == "existing_release":
        _validate_existing_release(release_dir=release_dir)
    else:
        if not all([args.fluxon_wheel_url, args.pylib_src_url]):
            parser.error("url_download mode requires --fluxon-wheel-url and --pylib-src-url")
        _url_download(
            release_dir=release_dir,
            fluxon_wheel_url=args.fluxon_wheel_url,
            pylib_src_url=args.pylib_src_url,
        )

    _build_image(release_dir=release_dir)


def _run(cmd: list[str]) -> None:
    print(f"+ {' '.join(str(c) for c in cmd)}")
    subprocess.check_call(cmd, cwd=str(REPO_ROOT))


def _validate_existing_release(*, release_dir: Path) -> None:
    required_globs = ("fluxon-*.whl",)
    required_relpaths = ("ext_images/etcd/etcd", "ext_images/etcd/etcdctl", "ext_images/greptime/greptime")
    if not release_dir.is_dir():
        raise FileNotFoundError(f"missing quick_start release directory: {release_dir}")
    for pattern in required_globs:
        if not any(release_dir.glob(pattern)):
            raise FileNotFoundError(f"missing quick_start release artifact matching {pattern!r} under {release_dir}")
    for relpath in required_relpaths:
        path = release_dir / relpath
        if not path.exists():
            raise FileNotFoundError(f"missing quick_start release runtime artifact: {path}")


def _prepare_release_ext(*, release_dir: Path) -> None:
    _run(
        [
            sys.executable,
            str(SCRIPTS_DIR / "pack_release_ext.py"),
            "--release-dir",
            str(release_dir),
        ]
    )


def _url_download(
    *,
    release_dir: Path,
    fluxon_wheel_url: str,
    pylib_src_url: str,
) -> None:
    import urllib.request

    release_dir.mkdir(parents=True, exist_ok=True)
    for url in (fluxon_wheel_url, pylib_src_url):
        name = url.rsplit("/", 1)[-1]
        dst = release_dir / name
        print(f"  downloading {url} -> {dst}")
        urllib.request.urlretrieve(url, str(dst))

    _prepare_release_ext(release_dir=release_dir)


def _copy_binaries(*, release_dir: Path, bin_dir: Path) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    ext_dir = release_dir / "ext_images"
    for name in EXT_BINARIES:
        src = ext_dir / name
        if not src.exists():
            raise FileNotFoundError(f"missing required quick_start binary: {src}")
        dst = bin_dir / Path(name).name
        shutil.copy2(str(src), str(dst))
        dst.chmod(0o755)
        print(f"  copied {src} -> {dst}")


def _build_image(*, release_dir: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_quick_start_build_") as td:
        context_root = Path(td)
        dockerfile_path = _stage_build_context(release_dir=release_dir, context_root=context_root)
        _run(
            [
                "docker",
                "build",
                "-f",
                str(dockerfile_path),
                "-t",
                f"{IMAGE_NAME}:{IMAGE_TAG}",
                str(context_root),
            ]
        )
    print(f"\nImage built: {IMAGE_NAME}:{IMAGE_TAG}")


def _stage_build_context(*, release_dir: Path, context_root: Path) -> Path:
    examples_dir = context_root / "examples"
    quick_start_dir = examples_dir / "fluxon_quick_start"
    fluxon_release_dir = context_root / "fluxon_release"
    quick_start_bin_dir = quick_start_dir / "bin"

    _copy_tree(SCRIPT_DIR, quick_start_dir)
    fluxon_release_dir.mkdir(parents=True, exist_ok=True)
    _copy_binaries(release_dir=release_dir, bin_dir=quick_start_bin_dir)

    copied_wheels = 0
    for wheel_path in sorted(release_dir.glob("*.whl")):
        shutil.copy2(wheel_path, fluxon_release_dir / wheel_path.name)
        copied_wheels += 1
    if copied_wheels == 0:
        raise FileNotFoundError("missing quick_start release wheels under fluxon_release/*.whl")

    return quick_start_dir / "Dockerfile"


def _copy_tree(src: Path, dst: Path) -> None:
    shutil.copytree(
        src,
        dst,
        ignore=shutil.ignore_patterns(
            "__pycache__",
            "*.pyc",
            ".pytest_cache",
            ".mypy_cache",
            "target",
            "tests",
            "*.btr",
        ),
        dirs_exist_ok=True,
    )


if __name__ == "__main__":
    main()
