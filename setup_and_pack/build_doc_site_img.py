#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "build_doc_site_img" / "doc_site_builder.yaml"
DEFAULT_OUT_PATH = REPO_ROOT / "fluxon_release" / "test_rsc" / "doc_site_builder_image.tar"

repo_root_str = str(REPO_ROOT)
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)

from setup_and_pack.utils import build_docker_image_from_config, host_sudo_prefix, sudo_prefix


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build and export the Docker image used for Fluxon doc-site CI builds."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Docker image YAML config. Relative paths resolve against the repo root.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT_PATH,
        help="Output docker-save tar path. Relative paths resolve against the repo root.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild and re-export even when the input stamp matches.",
    )
    return parser.parse_args()


def _resolve_repo_path(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _input_digest(*, config_path: Path) -> str:
    h = hashlib.sha256()
    h.update(b"fluxon_doc_site_builder_image_v1\n")
    for path in (
        config_path,
        Path(__file__).resolve(),
        REPO_ROOT / "scripts" / "build_doc_site.py",
        REPO_ROOT / "setup_and_pack" / "utils" / "docker_build_runtime_utils.py",
        REPO_ROOT / "setup_and_pack" / "nix" / "build_doc_site_in_container.py",
    ):
        if not path.is_file():
            raise RuntimeError(f"missing doc-site image input: {path}")
        h.update(path.relative_to(REPO_ROOT).as_posix().encode("utf-8"))
        h.update(b"\0")
        h.update(_sha256_file(path).encode("ascii"))
        h.update(b"\n")
    return h.hexdigest()


def _stamp_path(out_path: Path) -> Path:
    return out_path.with_name(out_path.name + ".input.sha256")


def _cache_ready(*, out_path: Path, expected_digest: str) -> bool:
    stamp_path = _stamp_path(out_path)
    if not out_path.is_file() or not stamp_path.is_file():
        return False
    return stamp_path.read_text(encoding="utf-8").strip() == expected_digest


def _ensure_docker_available() -> None:
    cmd = sudo_prefix() + ["docker", "--version"]
    subprocess.check_call(cmd)


def _export_image(*, image_ref: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    subprocess.check_call(sudo_prefix() + ["docker", "save", "-o", str(tmp_path), image_ref])
    subprocess.check_call(host_sudo_prefix() + ["chmod", "666", str(tmp_path)])
    tmp_path.replace(out_path)


def main() -> int:
    args = _parse_args()
    config_path = _resolve_repo_path(args.config)
    out_path = _resolve_repo_path(args.out)
    expected_digest = _input_digest(config_path=config_path)
    if not args.force and _cache_ready(out_path=out_path, expected_digest=expected_digest):
        print(f"Reusing cached doc-site builder image archive: {out_path}")
        return 0

    _ensure_docker_available()
    image_ref = build_docker_image_from_config(REPO_ROOT, config_path)
    _export_image(image_ref=image_ref, out_path=out_path)
    _stamp_path(out_path).write_text(expected_digest + "\n", encoding="utf-8")
    os.chmod(out_path, 0o666)
    os.chmod(_stamp_path(out_path), 0o666)
    print(f"Exported doc-site builder image: image_ref={image_ref} out={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
