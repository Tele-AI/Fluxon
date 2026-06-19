#!/usr/bin/env python3
"""Resolve and validate release metadata for GitHub Actions."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
PY_VERSION_FILE = REPO_ROOT / "fluxon_py" / "__init__.py"
QUICKSTART_IMAGE_FILE = REPO_ROOT / "examples" / "fluxon_quick_start" / "build_image.py"
RUST_SETUP_FILE = REPO_ROOT / "fluxon_rs" / "setup.py"
RUST_ROOT = REPO_ROOT / "fluxon_rs"
WORKSPACE_CARGO_FILE = RUST_ROOT / "Cargo.toml"
RELEASE_NOTES_DIR = REPO_ROOT / "fluxon_release" / "release_notes"


def _read_python_string_constant(path: Path, name: str) -> str:
    pattern = re.compile(rf"^{re.escape(name)}\s*=\s*['\"]([^'\"]+)['\"]\s*,?\s*$")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(raw_line.strip())
        if match:
            return match.group(1)
    raise ValueError(f"missing {name} in {path}")


def _read_workspace_members(path: Path) -> list[Path]:
    content = path.read_text(encoding="utf-8")
    match = re.search(r"(?ms)^\[workspace\]\s*(.*?)^\[", content + "\n[", re.MULTILINE)
    if not match:
        raise ValueError(f"missing [workspace] section in {path}")
    section = match.group(1)
    members_match = re.search(r'(?ms)^members\s*=\s*\[(.*?)\]', section)
    if not members_match:
        raise ValueError(f"missing workspace members in {path}")
    members_block = members_match.group(1)
    members = re.findall(r'"([^"]+)"', members_block)
    return [path.parent / member / "Cargo.toml" for member in members]


def _read_workspace_version(path: Path) -> str:
    content = path.read_text(encoding="utf-8")
    match = re.search(r'(?ms)^\[workspace\.package\]\s*(.*?)^\[', content + "\n[", re.MULTILINE)
    if not match:
        raise ValueError(f"missing [workspace.package] section in {path}")
    section = match.group(1)
    version_match = re.search(r'^version\s*=\s*"([^"]+)"\s*$', section, re.MULTILINE)
    if not version_match:
        raise ValueError(f"missing [workspace.package].version in {path}")
    return version_match.group(1)


def _read_cargo_package_version(path: Path, *, workspace_version: str | None = None) -> str:
    current_section: str | None = None
    version_pattern = re.compile(r'^version\s*=\s*"([^"]+)"\s*$')
    workspace_version_pattern = re.compile(r"^version\.workspace\s*=\s*true\s*$")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1].strip()
            continue
        if current_section == "package":
            match = version_pattern.match(line)
            if match:
                return match.group(1)
            if workspace_version is not None and workspace_version_pattern.match(line):
                return workspace_version
    raise ValueError(f"missing [package].version in {path}")


def _iter_release_cargo_manifests() -> tuple[str, list[Path]]:
    workspace_version = _read_workspace_version(WORKSPACE_CARGO_FILE)
    manifests = [WORKSPACE_CARGO_FILE]
    manifests.extend(_read_workspace_members(WORKSPACE_CARGO_FILE))
    return workspace_version, manifests


def _parse_release_tag(git_ref: str) -> str | None:
    if not git_ref:
        return None
    if git_ref.startswith("refs/tags/"):
        return git_ref[len("refs/tags/") :]
    return None


def _is_prerelease(tag: str) -> bool:
    return "-" in tag


def _write_github_output(path: Path, outputs: dict[str, str]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve release metadata from repo version files")
    parser.add_argument(
        "--git-ref",
        default="",
        help="Git ref to validate against, typically GITHUB_REF",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        help="Optional GitHub Actions output file path",
    )
    args = parser.parse_args()

    package_version = _read_python_string_constant(PY_VERSION_FILE, "__version__")
    quickstart_version = _read_python_string_constant(QUICKSTART_IMAGE_FILE, "IMAGE_TAG")
    rust_setup_version = _read_python_string_constant(RUST_SETUP_FILE, "version")
    workspace_version, cargo_manifests = _iter_release_cargo_manifests()
    cargo_versions: dict[Path, str] = {}
    for path in cargo_manifests:
        if path == WORKSPACE_CARGO_FILE:
            cargo_versions[path] = workspace_version
        else:
            cargo_versions[path] = _read_cargo_package_version(path, workspace_version=workspace_version)

    mismatches: list[str] = []
    if quickstart_version != package_version:
        mismatches.append(
            f"{QUICKSTART_IMAGE_FILE.relative_to(REPO_ROOT)} declares {quickstart_version}, expected {package_version}"
        )
    if rust_setup_version != package_version:
        mismatches.append(
            f"{RUST_SETUP_FILE.relative_to(REPO_ROOT)} declares {rust_setup_version}, expected {package_version}"
        )
    for path, version in cargo_versions.items():
        if version != package_version:
            mismatches.append(f"{path.relative_to(REPO_ROOT)} declares {version}, expected {package_version}")

    if mismatches:
        print("release version mismatch detected:", file=sys.stderr)
        for item in mismatches:
            print(f"  - {item}", file=sys.stderr)
        return 1

    expected_tag = f"v{package_version}"
    release_tag = _parse_release_tag(args.git_ref) or expected_tag
    if release_tag != expected_tag:
        print(
            f"release tag mismatch: git ref resolved to {release_tag}, but repo version requires {expected_tag}",
            file=sys.stderr,
        )
        return 1

    outputs = {
        "release_version": package_version,
        "release_tag": release_tag,
        "release_title": release_tag,
        "quickstart_image": f"fluxon_quick_start:{quickstart_version}",
        "quickstart_archive": f"fluxon_quick_start_{quickstart_version}_docker_image.tar.gz",
        "release_notes_file": str(RELEASE_NOTES_DIR / f"{release_tag}.md"),
        "prerelease": "true" if _is_prerelease(release_tag) else "false",
    }

    for key, value in outputs.items():
        print(f"{key}={value}")

    if args.github_output is not None:
        _write_github_output(args.github_output, outputs)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
