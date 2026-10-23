#!/usr/bin/env python3
"""Validate the unified Fluxon wheel selected for PyPI publication."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from email.parser import Parser
import hashlib
import json
from pathlib import Path
import re
import sys
import zipfile


REPO_ROOT = Path(__file__).resolve().parent.parent
repo_root_str = str(REPO_ROOT)
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)

from setup_and_pack.package_contract import (
    PYTHON_DISTRIBUTION_NAME,
    PYTHON_WHEEL_DISTRIBUTION,
    RELEASE_WHEEL_GLOB,
)


DEFAULT_MAX_WHEEL_SIZE_BYTES = 100 * 1024 * 1024
EXPECTED_PYTHON_TAG = "cp38"
EXPECTED_ABI_TAG = "abi3"
EXPECTED_PLATFORM_TAG = "manylinux_2_28_x86_64"


@dataclass(frozen=True)
class ValidatedWheel:
    path: str
    filename: str
    distribution: str
    version: str
    requires_python: str
    python_tag: str
    abi_tag: str
    platform_tag: str
    sha256: str
    size_bytes: int


def _normalize_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _read_metadata(wheel_path: Path) -> tuple[str, str, str]:
    with zipfile.ZipFile(wheel_path) as archive:
        metadata_names = sorted(
            name
            for name in archive.namelist()
            if name.count("/") == 1 and name.endswith(".dist-info/METADATA")
        )
        if len(metadata_names) != 1:
            raise RuntimeError(
                f"expected exactly one wheel METADATA file in {wheel_path.name}, found {metadata_names}"
            )
        metadata = Parser().parsestr(archive.read(metadata_names[0]).decode("utf-8"))

    distribution = metadata.get("Name", "").strip()
    version = metadata.get("Version", "").strip()
    requires_python = metadata.get("Requires-Python", "").strip()
    if not distribution or not version:
        raise RuntimeError(f"wheel METADATA is missing Name or Version: {wheel_path}")
    return distribution, version, requires_python


def _parse_wheel_filename(wheel_path: Path) -> tuple[str, str, str, str]:
    if not wheel_path.name.endswith(".whl"):
        raise RuntimeError(f"not a wheel filename: {wheel_path.name}")
    filename_parts = wheel_path.name[:-4].rsplit("-", 3)
    if len(filename_parts) != 4:
        raise RuntimeError(f"invalid wheel filename: {wheel_path.name}")
    distribution_and_version, python_tag, abi_tag, platform_tag = filename_parts
    distribution_prefix = f"{PYTHON_WHEEL_DISTRIBUTION}-"
    if not distribution_and_version.startswith(distribution_prefix):
        raise RuntimeError(
            f"wheel filename distribution must start with {distribution_prefix!r}: {wheel_path.name}"
        )
    filename_version = distribution_and_version[len(distribution_prefix) :]
    if not filename_version:
        raise RuntimeError(f"wheel filename is missing its version: {wheel_path.name}")
    return filename_version, python_tag, abi_tag, platform_tag


def validate_release_wheel(
    *,
    release_dir: Path,
    release_tag: str,
    max_wheel_size_bytes: int = DEFAULT_MAX_WHEEL_SIZE_BYTES,
) -> ValidatedWheel:
    release_dir = release_dir.resolve()
    matches = sorted(path for path in release_dir.glob(RELEASE_WHEEL_GLOB) if path.is_file())
    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one release wheel matching {RELEASE_WHEEL_GLOB!r} in {release_dir}, "
            f"found {[path.name for path in matches]}"
        )
    wheel_path = matches[0]

    size_bytes = wheel_path.stat().st_size
    if size_bytes > max_wheel_size_bytes:
        raise RuntimeError(
            f"wheel exceeds the PyPI file-size limit: {wheel_path.name} "
            f"size={size_bytes} limit={max_wheel_size_bytes}"
        )

    filename_version, python_tag, abi_tag, platform_tag = _parse_wheel_filename(wheel_path)
    if (python_tag, abi_tag, platform_tag) != (
        EXPECTED_PYTHON_TAG,
        EXPECTED_ABI_TAG,
        EXPECTED_PLATFORM_TAG,
    ):
        raise RuntimeError(
            "unexpected release wheel compatibility tag: "
            f"got={python_tag}-{abi_tag}-{platform_tag} "
            f"expected={EXPECTED_PYTHON_TAG}-{EXPECTED_ABI_TAG}-{EXPECTED_PLATFORM_TAG}"
        )

    distribution, metadata_version, requires_python = _read_metadata(wheel_path)
    if _normalize_distribution_name(distribution) != _normalize_distribution_name(PYTHON_DISTRIBUTION_NAME):
        raise RuntimeError(
            f"wheel METADATA Name mismatch: got={distribution!r} expected={PYTHON_DISTRIBUTION_NAME!r}"
        )
    if metadata_version != filename_version:
        raise RuntimeError(
            f"wheel version mismatch: filename={filename_version!r} metadata={metadata_version!r}"
        )
    expected_tag = f"v{metadata_version}"
    if release_tag != expected_tag:
        raise RuntimeError(f"release tag mismatch: got={release_tag!r} expected={expected_tag!r}")
    if requires_python != ">=3.10":
        raise RuntimeError(
            f"wheel Requires-Python mismatch: got={requires_python!r} expected='>=3.10'"
        )

    digest_builder = hashlib.sha256()
    with wheel_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest_builder.update(chunk)
    digest = digest_builder.hexdigest()
    return ValidatedWheel(
        path=str(wheel_path),
        filename=wheel_path.name,
        distribution=distribution,
        version=metadata_version,
        requires_python=requires_python,
        python_tag=python_tag,
        abi_tag=abi_tag,
        platform_tag=platform_tag,
        sha256=digest,
        size_bytes=size_bytes,
    )


def _write_github_output(path: Path, wheel: ValidatedWheel) -> None:
    outputs = {
        "wheel_path": wheel.path,
        "wheel_filename": wheel.filename,
        "wheel_sha256": wheel.sha256,
        "release_version": wheel.version,
    }
    with path.open("a", encoding="utf-8") as handle:
        for name, value in outputs.items():
            handle.write(f"{name}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument(
        "--max-wheel-size-bytes",
        type=int,
        default=DEFAULT_MAX_WHEEL_SIZE_BYTES,
    )
    args = parser.parse_args()

    wheel = validate_release_wheel(
        release_dir=args.release_dir,
        release_tag=args.release_tag,
        max_wheel_size_bytes=args.max_wheel_size_bytes,
    )
    print(json.dumps(asdict(wheel), indent=2, sort_keys=True))
    if args.github_output is not None:
        _write_github_output(args.github_output, wheel)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
