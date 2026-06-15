#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
repo_root_str = str(REPO_ROOT)
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)

from setup_and_pack.utils import wheel_runtime_helper as wheel_helper


CLOSED_SDK_LIB_DIR = REPO_ROOT / "fluxon_release" / "closed_sdk" / "lib"
PREPARE_BUILD_SCRIPT = REPO_ROOT / "setup_and_pack" / "pub_prepare_build.py"
PACK_HELPER_PATH = REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.py"
TARGET_ROOT = REPO_ROOT / "fluxon_rs" / "target"
PUBLIC_CLOSED_SDK_LIB_NAMES = (
    "libfluxon_commu_core.so",
    "libfluxon_rdma_probe.so",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a public fluxon_pyo3 wheel and repair bundled closed-sdk libraries."
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "dist",
        help="Directory for the built wheel.",
    )
    parser.add_argument(
        "--skip-smoke-test",
        action="store_true",
        help="Skip install/import verification in a temporary virtualenv.",
    )
    return parser.parse_args()


def _run(argv: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
    subprocess.run(argv, cwd=str(cwd), env=env, check=True)


def _load_pack_helper():
    for import_root in (
        REPO_ROOT,
        PACK_HELPER_PATH.parent,
        PACK_HELPER_PATH.parent.parent,
    ):
        import_root_str = str(import_root)
        if import_root_str in sys.path:
            sys.path.remove(import_root_str)
        sys.path.insert(0, import_root_str)
    spec = importlib.util.spec_from_file_location("fluxon_pack_helper", PACK_HELPER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load pack helper: {PACK_HELPER_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_prepare_build_helper():
    spec = importlib.util.spec_from_file_location("fluxon_prepare_build_helper", PREPARE_BUILD_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load prepare_build helper: {PREPARE_BUILD_SCRIPT}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _require_closed_sdk_libs() -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    for lib_name in PUBLIC_CLOSED_SDK_LIB_NAMES:
        lib_path = CLOSED_SDK_LIB_DIR / lib_name
        if not lib_path.is_file():
            raise RuntimeError(f"missing public closed-sdk library: {lib_path}")
        resolved[lib_name] = lib_path.resolve()
    return resolved


def _find_single_wheel(out_dir: Path) -> Path:
    candidates = sorted(out_dir.glob("fluxon_pyo3-*.whl"))
    if len(candidates) != 1:
        names = ", ".join(path.name for path in candidates) or "<none>"
        raise RuntimeError(f"expected exactly one built wheel under {out_dir}, found: {names}")
    return candidates[0].resolve()


def _prepare_vendor_runtime() -> None:
    raise RuntimeError(
        "build_public_pyo3_wheel.py no longer prepares vendor_runtime on the host. "
        "Use setup_and_pack/pack_release.py so vendor_runtime is self-bootstrapped inside manylinux."
    )


def _replace_bundled_closed_sdk_libs(wheel_path: Path) -> None:
    closed_sdk_libs = _require_closed_sdk_libs()
    extract_root = Path(wheel_helper.extract_wheel(str(wheel_path)))
    try:
        libs_dirs = sorted(
            path for path in extract_root.iterdir() if path.is_dir() and path.name.endswith(".libs")
        )
        if len(libs_dirs) != 1:
            names = ", ".join(path.name for path in libs_dirs) or "<none>"
            raise RuntimeError(f"expected exactly one wheel libs dir, found: {names}")
        libs_dir = libs_dirs[0]

        install_map: dict[str, str] = {
            "libfluxon_commu_core.so": str(closed_sdk_libs["libfluxon_commu_core.so"]),
            "libfluxon_rdma_probe.so": str(closed_sdk_libs["libfluxon_rdma_probe.so"]),
        }
        for pattern, source_name in (
            ("libfluxon_commu_core-*.so*", "libfluxon_commu_core.so"),
            ("libfluxon_rdma_probe-*.so*", "libfluxon_rdma_probe.so"),
        ):
            matches = sorted(libs_dir.glob(pattern))
            if not matches:
                raise RuntimeError(f"wheel is missing expected bundled library pattern: {pattern}")
            for match in matches:
                install_map[match.name] = str(closed_sdk_libs[source_name])

        wheel_helper.install_shared_libraries(str(wheel_path), install_map)
    finally:
        shutil.rmtree(extract_root, ignore_errors=True)


def _bundle_vendor_runtime(wheel_path: Path) -> None:
    pack_helper = _load_pack_helper()
    vendor_runtime_root, vendor_lib_paths = pack_helper._resolve_vendor_runtime_install_root(
        cargo_target_root=TARGET_ROOT.resolve(),
    )
    with pack_helper._extract_wheel_runtime_tree(wheel_path) as (
        wheel_extension_path,
        wheel_bundled_lib_dirs,
    ):
        bundled_name_map: dict[str, str] = {}
        for bundled_lib_dir in wheel_bundled_lib_dirs:
            bundled_name_map.update(wheel_helper.get_repaired_lib_name_map(str(bundled_lib_dir)))
        with tempfile.TemporaryDirectory(prefix="fluxon_vendor_runtime_stage_") as temp_dir_str:
            runtime_stage_dir = Path(temp_dir_str)
            extra_runtime_libs = pack_helper._stage_vendor_runtime_closure(
                wheel_extension_path=wheel_extension_path,
                wheel_bundled_lib_dirs=wheel_bundled_lib_dirs,
                vendor_root=vendor_runtime_root,
                vendor_lib_paths=vendor_lib_paths,
                stage_dir=runtime_stage_dir,
                extra_ld_library_roots=[CLOSED_SDK_LIB_DIR.resolve()],
            )
            packaged_replacements = pack_helper._select_vendor_runtime_packaged_replacements(
                packaged_file_names=set(bundled_name_map),
                vendor_lib_paths=vendor_lib_paths,
            )
            wheel_helper.add_shared_libraries(
                str(wheel_path),
                [str(path) for path in [*vendor_lib_paths, *extra_runtime_libs]],
                packaged_lib_replacements=packaged_replacements,
            )
    driver_config_dir = pack_helper._resolve_ibverbs_driver_config_dir(
        runtime_root=vendor_runtime_root,
        runtime_label="vendor runtime install-root",
    )
    wheel_helper.add_plugins(str(wheel_path), str(driver_config_dir), "libibverbs.d")
    pack_helper._prune_unused_vendor_runtime_aliases(wheel_path)
    pack_helper._validate_vendor_runtime_wheel_layout(wheel_path)


def _build_wheel(out_dir: Path) -> Path:
    _prepare_vendor_runtime()
    out_dir.mkdir(parents=True, exist_ok=True)
    for existing_wheel in out_dir.glob("fluxon_pyo3-*.whl"):
        existing_wheel.unlink()
    _run(
        [
            "maturin",
            "build",
            "--manifest-path",
            str(REPO_ROOT / "fluxon_rs" / "fluxon_pyo3" / "Cargo.toml"),
            "--release",
            "--out",
            str(out_dir),
        ],
        cwd=REPO_ROOT,
    )
    wheel_path = _find_single_wheel(out_dir)
    _replace_bundled_closed_sdk_libs(wheel_path)
    _bundle_vendor_runtime(wheel_path)
    return wheel_path


def _smoke_test_wheel(wheel_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_pub2_wheel_smoke_") as tmpdir:
        tmp_root = Path(tmpdir)
        venv_dir = tmp_root / "venv"
        _run([sys.executable, "-m", "venv", str(venv_dir)], cwd=REPO_ROOT)
        pip_bin = venv_dir / "bin" / "pip"
        python_bin = venv_dir / "bin" / "python"
        _run([str(pip_bin), "install", str(wheel_path)], cwd=REPO_ROOT)
        _run(
            [
                str(python_bin),
                "-c",
                "import fluxon_pyo3; print(fluxon_pyo3.__file__)",
            ],
            cwd=REPO_ROOT,
        )


def main() -> int:
    args = _parse_args()
    wheel_path = _build_wheel(args.out.resolve())
    if not args.skip_smoke_test:
        _smoke_test_wheel(wheel_path)
    print(wheel_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
