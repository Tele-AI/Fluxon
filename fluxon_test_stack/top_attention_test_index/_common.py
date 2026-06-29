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


def _prepare_subprocess_env(env: dict[str, str] | None) -> dict[str, str]:
    prepared_env = os.environ.copy() if env is None else dict(env)
    prepared_env["RUST_BACKTRACE"] = "1"
    prepared_env["RUST_LIB_BACKTRACE"] = "1"
    return prepared_env


def call(cmd: Sequence[str], *, env: dict[str, str] | None = None) -> int:
    print("+ " + " ".join(cmd), flush=True)
    return subprocess.call(list(cmd), cwd=str(REPO_ROOT), env=_prepare_subprocess_env(env))


def parse_python_passthrough(description: str) -> tuple[str, list[str]]:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON", sys.executable),
        help="Python executable used for the delegated command.",
    )
    args, passthrough = parser.parse_known_args()
    return args.python, passthrough


def run_pytest(
    description: str,
    paths: Iterable[str],
    *,
    passthrough: Sequence[str] | None = None,
) -> int:
    python, _ = parse_python_passthrough(description)
    effective_passthrough = [] if passthrough is None else list(passthrough)
    return call([python, "-m", "pytest", *paths, *effective_passthrough])


def run_python_file(
    description: str,
    path: str,
    extra_args: Iterable[str] = (),
) -> int:
    python, _ = parse_python_passthrough(description)
    return call([python, "-u", str(REPO_ROOT / path), *extra_args])


def run_python_files(
    description: str,
    paths: Iterable[str],
) -> int:
    python, _ = parse_python_passthrough(description)
    for path in paths:
        rc = call([python, "-u", str(REPO_ROOT / path)])
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


def _require_scene_runtime_endpoint(scene_runtime: object, *, service_id: str) -> tuple[str, int]:
    if not isinstance(scene_runtime, dict):
        raise ValueError("case config scene_runtime must be a mapping")
    raw_service = scene_runtime.get(service_id)
    if not isinstance(raw_service, dict):
        raise ValueError(f"case config scene_runtime.{service_id} must be a mapping")
    ip = str(raw_service.get("ip") or "").strip()
    if not ip:
        raise ValueError(f"case config scene_runtime.{service_id}.ip must be set")
    port = raw_service.get("port")
    if not isinstance(port, int):
        raise ValueError(f"case config scene_runtime.{service_id}.port must be an int")
    return ip, port


def write_build_config_ext(case_cfg_path: str | Path, *, scene_runtime: object) -> Path:
    cfg_path = Path(case_cfg_path).resolve()
    etcd_ip, etcd_port = _require_scene_runtime_endpoint(scene_runtime, service_id="etcd")
    greptime_ip, greptime_port = _require_scene_runtime_endpoint(scene_runtime, service_id="greptime")
    out_path = cfg_path.parents[1] / "src" / "build_config_ext.yml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        yaml.safe_dump(
            {
                "etcd": f"{etcd_ip}:{etcd_port}",
                "prom": f"http://{greptime_ip}:{greptime_port}/v1/prometheus",
                "prom_remote_write_url": f"http://{greptime_ip}:{greptime_port}/v1/prometheus/write",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return out_path


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


def _path_contains_fluxon_pyo3_libs_dir(path: Path) -> bool:
    return "fluxon_pyo3.libs" in path.parts


def _sanitize_cargo_ld_library_path(
    *,
    authoritative_entries: Sequence[str],
    current_value: str | None,
) -> str:
    # Keep the authoritative loader roots first, then retain only non-fluxon entries from the parent env.
    sanitized_entries: list[str] = []
    seen_entries: set[str] = set()
    for raw_entry in authoritative_entries:
        entry = raw_entry.strip()
        if not entry or entry in seen_entries:
            continue
        seen_entries.add(entry)
        sanitized_entries.append(entry)

    if current_value is not None:
        for raw_entry in current_value.split(":"):
            entry = raw_entry.strip()
            if not entry or entry in seen_entries:
                continue
            if _path_contains_fluxon_pyo3_libs_dir(Path(entry)):
                continue
            seen_entries.add(entry)
            sanitized_entries.append(entry)
    return ":".join(sanitized_entries)


def _resolve_repo_closed_sdk_root() -> Path | None:
    closed_sdk_root = (REPO_ROOT / "fluxon_release" / "closed_sdk").resolve()
    if not closed_sdk_root.is_dir():
        return None
    manifest_path = closed_sdk_root / "manifest.json"
    lib_dir = closed_sdk_root / "lib"
    if not manifest_path.is_file() or not lib_dir.is_dir():
        return None
    return closed_sdk_root


def _prepare_cargo_env(env: dict[str, str] | None) -> dict[str, str] | None:
    libs_dir = _resolve_authoritative_fluxon_pyo3_libs_dir()
    closed_sdk_root = _resolve_repo_closed_sdk_root()
    if env is None and libs_dir is None and closed_sdk_root is None:
        return None

    prepared_env = os.environ.copy() if env is None else dict(env)
    authoritative_entries: list[str] = []

    if libs_dir is not None:
        authoritative_entry = str(libs_dir)
        prepared_env["FLUXON_PYO3_LIBS_DIR"] = authoritative_entry
        authoritative_entries.append(authoritative_entry)

    if closed_sdk_root is not None:
        prepared_env["FLUXON_COMMU_CLOSED_SDK_ROOT"] = str(closed_sdk_root)
        authoritative_entries.append(str((closed_sdk_root / "lib").resolve()))

    if authoritative_entries:
        prepared_env["LD_LIBRARY_PATH"] = _sanitize_cargo_ld_library_path(
            authoritative_entries=authoritative_entries,
            current_value=prepared_env.get("LD_LIBRARY_PATH"),
        )

    return prepared_env


def run_cargo(
    args: Iterable[str],
    *,
    env: dict[str, str] | None = None,
    passthrough: Sequence[str] | None = None,
) -> int:
    # Rust test binaries launched via cargo run/load depend on the wheel-bundled native
    # runtime under the active venv. Keep one authoritative search root for all wrappers.
    effective_passthrough = [] if passthrough is None else list(passthrough)
    return call(["cargo", *args, *effective_passthrough], env=_prepare_cargo_env(env))
