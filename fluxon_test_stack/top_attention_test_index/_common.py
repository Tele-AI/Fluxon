from __future__ import annotations

import argparse
import os
import site
import subprocess
import sys
import sysconfig
from pathlib import Path
from typing import Iterable, Sequence

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
TEST_REQUIREMENTS: list[str] = ["ops"]


def call(cmd: Sequence[str], *, env: dict[str, str] | None = None) -> int:
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.call(list(cmd), cwd=str(REPO_ROOT), env=env)


def parse_python_passthrough(description: str) -> tuple[str, list[str]]:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON", sys.executable),
        help="Python executable used for the delegated command.",
    )
    args, passthrough = parser.parse_known_args()
    return args.python, passthrough


def run_pytest(description: str, paths: Iterable[str]) -> int:
    python, passthrough = parse_python_passthrough(description)
    return call([python, "-m", "pytest", *paths, *passthrough])


def run_python_file(description: str, path: str, extra_args: Iterable[str] = ()) -> int:
    python, passthrough = parse_python_passthrough(description)
    return call([python, "-u", str(REPO_ROOT / path), *extra_args, *passthrough])


def run_python_files(description: str, paths: Iterable[str]) -> int:
    python, passthrough = parse_python_passthrough(description)
    for path in paths:
        rc = call([python, "-u", str(REPO_ROOT / path), *passthrough])
        if rc != 0:
            return rc
    return 0


def load_case_config(path: str | Path, *, expected_scene_id: str) -> dict:
    cfg_path = Path(path).resolve()
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"case config must be a YAML mapping: {cfg_path}")
    case = raw.get("case")
    if not isinstance(case, dict):
        raise ValueError(f"case config must define case mapping: {cfg_path}")
    scene_id = str(case.get("scene_id") or "").strip()
    if scene_id != expected_scene_id:
        raise ValueError(f"case config scene_id mismatch: expected {expected_scene_id!r}, got {scene_id!r}")
    scene_config = raw.get("scene_config")
    if not isinstance(scene_config, dict):
        raise ValueError(f"case config must define scene_config mapping: {cfg_path}")
    return scene_config


def load_case_config_payload(path: str | Path, *, expected_scene_id: str) -> dict:
    cfg_path = Path(path).resolve()
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"case config must be a YAML mapping: {cfg_path}")
    case = raw.get("case")
    if not isinstance(case, dict):
        raise ValueError(f"case config must define case mapping: {cfg_path}")
    scene_id = str(case.get("scene_id") or "").strip()
    if scene_id != expected_scene_id:
        raise ValueError(f"case config scene_id mismatch: expected {expected_scene_id!r}, got {scene_id!r}")
    scene_config = raw.get("scene_config")
    if not isinstance(scene_config, dict):
        raise ValueError(f"case config must define scene_config mapping: {cfg_path}")
    return raw


def _path_contains_fluxon_pyo3_libs_dir(path: Path) -> bool:
    return "fluxon_pyo3.libs" in path.parts


def _iter_active_python_site_packages_roots() -> list[Path]:
    raw_roots: list[str] = []
    sysconfig_paths = sysconfig.get_paths()
    for key in ("platlib", "purelib"):
        raw_root = sysconfig_paths.get(key)
        if isinstance(raw_root, str) and raw_root.strip():
            raw_roots.append(raw_root)
    try:
        raw_roots.extend(site.getsitepackages())
    except AttributeError:
        pass
    raw_user_site = site.getusersitepackages()
    if isinstance(raw_user_site, str) and raw_user_site.strip():
        raw_roots.append(raw_user_site)

    resolved_roots: list[Path] = []
    seen_roots: set[Path] = set()
    for raw_root in raw_roots:
        root = Path(raw_root).resolve()
        if root in seen_roots:
            continue
        seen_roots.add(root)
        resolved_roots.append(root)
    return resolved_roots


def _resolve_authoritative_fluxon_pyo3_libs_dir() -> Path | None:
    for site_packages_root in _iter_active_python_site_packages_roots():
        libs_dir = (site_packages_root / "fluxon_pyo3.libs").resolve()
        if libs_dir.is_dir():
            return libs_dir
    return None


def _prepare_cargo_env(env: dict[str, str] | None) -> dict[str, str] | None:
    libs_dir = _resolve_authoritative_fluxon_pyo3_libs_dir()
    if libs_dir is None:
        return None if env is None else dict(env)

    prepared_env = os.environ.copy() if env is None else dict(env)
    authoritative_entry = str(libs_dir)
    prepared_env["FLUXON_PYO3_LIBS_DIR"] = authoritative_entry

    sanitized_entries = [authoritative_entry]
    seen_entries = {authoritative_entry}
    current_ld_library_path = prepared_env.get("LD_LIBRARY_PATH")
    if current_ld_library_path is not None:
        for raw_entry in current_ld_library_path.split(":"):
            entry = raw_entry.strip()
            if not entry:
                continue
            if entry in seen_entries:
                continue
            if _path_contains_fluxon_pyo3_libs_dir(Path(entry)):
                continue
            seen_entries.add(entry)
            sanitized_entries.append(entry)
    prepared_env["LD_LIBRARY_PATH"] = ":".join(sanitized_entries)
    return prepared_env


def run_cargo(args: Iterable[str], *, env: dict[str, str] | None = None) -> int:
    # Rust test binaries launched via cargo run/load depend on the wheel-bundled native
    # runtime under the active venv. Keep one authoritative search root for all wrappers.
    return call(["cargo", *args], env=_prepare_cargo_env(env))
