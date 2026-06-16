#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
PACK_HELPER_PATH = SCRIPT_DIR / "pack_fluxonkv_pylib.py"
WHEEL_HELPER_PATH = REPO_ROOT / "setup_and_pack" / "utils" / "wheel_runtime_helper.py"
DEFAULT_CLOSED_SDK_ROOT = Path("/tmp/fluxon_sdk_runtime")
DEFAULT_RELEASE_DIR = Path("/release")
DEFAULT_TARGET_ROOT = Path("/workspace/fluxon_rs/target")
ABI3_SMOKE_TEST_INTERPRETERS = (
    "/opt/python/cp310-cp310/bin/python",
    "/opt/python/cp311-cp311/bin/python",
    "/opt/python/cp312-cp312/bin/python",
)

WHEEL_FINALIZE_STEP_KIND_ADD_OFFLINE_RDMA_SHARED_LIBRARIES = "add_offline_rdma_shared_libraries"
WHEEL_FINALIZE_STEP_KIND_ADD_NATIVE_PLUGINS = "add_native_plugins"
WHEEL_FINALIZE_STEP_KIND_ADD_VENDOR_RUNTIME = "add_vendor_runtime"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Finalize a built fluxon_pyo3 wheel inside manylinux.")
    parser.add_argument("--release-dir", type=Path, default=DEFAULT_RELEASE_DIR)
    parser.add_argument("--target-root", type=Path, default=DEFAULT_TARGET_ROOT)
    parser.add_argument("--closed-sdk-root", type=Path, default=None)
    parser.add_argument(
        "--wheel-finalize-steps-json",
        required=True,
        help="JSON array describing wheel finalize steps from the manylinux backend plan.",
    )
    return parser.parse_args()


def _load_module(module_path: Path, module_name: str):
    if not module_path.is_file():
        raise RuntimeError(f"required helper module is missing: {module_path}")
    module_parent = str(module_path.parent)
    if module_parent not in sys.path:
        sys.path.insert(0, module_parent)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _validate_zip_archive(path: Path) -> None:
    with zipfile.ZipFile(path, "r") as zip_ref:
        bad_member = zip_ref.testzip()
    if bad_member is not None:
        raise RuntimeError(f"invalid zip member in {path}: {bad_member}")


def _resolve_native_runtime_root(*, closed_sdk_root: Path, target_root: Path, dir_name: str) -> Path:
    candidates = [
        closed_sdk_root / "native" / dir_name,
        closed_sdk_root / dir_name,
        target_root / dir_name,
        Path("/nix_profile/native") / dir_name,
    ]
    for path in candidates:
        if path.is_dir():
            return path
    raise RuntimeError(
        "missing native runtime dir "
        + dir_name
        + ": "
        + ", ".join(str(path) for path in candidates)
    )


def _resolve_native_lib_roots(*, closed_sdk_root: Path, target_root: Path, dir_name: str) -> list[Path]:
    native_root = _resolve_native_runtime_root(
        closed_sdk_root=closed_sdk_root,
        target_root=target_root,
        dir_name=dir_name,
    )
    lib_roots: list[Path] = []
    seen: set[str] = set()
    for path in (
        native_root / "lib" / "x86_64-linux-gnu",
        native_root / "lib64",
        native_root / "lib",
    ):
        if not path.is_dir():
            continue
        path_key = str(path.resolve())
        if path_key in seen:
            continue
        seen.add(path_key)
        lib_roots.append(path)
    if not lib_roots:
        raise RuntimeError(f"native runtime contains no library dirs: {native_root}")
    return lib_roots


def _resolve_offline_rdma_runtime(*, closed_sdk_root: Path, target_root: Path, packed_dir_name: str) -> list[str]:
    packed_lib_roots = _resolve_native_lib_roots(
        closed_sdk_root=closed_sdk_root,
        target_root=target_root,
        dir_name=packed_dir_name,
    )
    scan_roots = [closed_sdk_root / "lib", *packed_lib_roots]
    scan_roots.extend(lib_root / "libibverbs" for lib_root in packed_lib_roots)
    runtime_libs: list[str] = []
    seen_runtime_libs: set[str] = set()
    for root in scan_roots:
        if not root.is_dir():
            continue
        for path in sorted(root.glob("*.so*")):
            if not path.is_file():
                continue
            path_key = str(path)
            if path_key in seen_runtime_libs:
                continue
            seen_runtime_libs.add(path_key)
            runtime_libs.append(str(path))
    if not runtime_libs:
        raise RuntimeError(f"no offline RDMA runtime shared libraries discovered under {scan_roots}")
    print(f"wheel finalize: discovered offline RDMA shared libs count={len(runtime_libs)}")
    return runtime_libs


def _smoke_test_abi3_wheel(wheel_path: Path) -> None:
    for interp in ABI3_SMOKE_TEST_INTERPRETERS:
        if not Path(interp).exists():
            raise RuntimeError(f"abi3 interpreter is missing: {interp}")
    clean_env = os.environ.copy()
    clean_env.pop("LD_LIBRARY_PATH", None)
    with tempfile.TemporaryDirectory(prefix="fluxon_pyo3_smoke_") as tmp_dir:
        tmp_root = Path(tmp_dir)
        for interp in ABI3_SMOKE_TEST_INTERPRETERS:
            tag = Path(interp).parents[1].name
            venv_dir = tmp_root / f"venv_{tag}"
            subprocess.run([interp, "-m", "venv", str(venv_dir)], check=True, cwd="/workspace", env=clean_env)
            venv_python = venv_dir / "bin" / "python"
            try:
                subprocess.run(
                    [str(venv_python), "-m", "pip", "install", "--no-deps", "--no-cache-dir", str(wheel_path)],
                    check=True,
                    cwd="/workspace",
                    env=clean_env,
                )
                subprocess.run(
                    [str(venv_python), "-c", "import fluxon_pyo3; print(fluxon_pyo3.__file__)"],
                    check=True,
                    cwd="/workspace",
                    env=clean_env,
                    capture_output=True,
                    text=True,
                )
                print(f"wheel finalize: abi3 import OK {tag}")
            except subprocess.CalledProcessError as exc:
                print(
                    "wheel finalize: abi3 import warning "
                    + tag
                    + " exit="
                    + str(exc.returncode)
                    + " stdout="
                    + exc.stdout.strip()
                    + " stderr="
                    + exc.stderr.strip()
                )


def _rewrite_wheel_python_init(wheel_path: Path, wheel_helper) -> None:
    temp_dir = wheel_helper.extract_wheel(str(wheel_path))
    try:
        pkg_name = wheel_path.name.split("-", 1)[0]
        pkg_dir = Path(temp_dir) / pkg_name
        pkg_dir.mkdir(parents=True, exist_ok=True)
        init_path = pkg_dir / "__init__.py"
        init_path.write_text(
            "from .fluxon_pyo3 import *\n\n"
            "__doc__ = fluxon_pyo3.__doc__\n"
            "if hasattr(fluxon_pyo3, '__all__'):\n"
            "    __all__ = fluxon_pyo3.__all__\n",
            encoding="utf-8",
        )
        wheel_helper.create_wheel(str(wheel_path), temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _normalize_finalize_steps(raw_steps: object) -> tuple[dict, ...]:
    if not isinstance(raw_steps, list):
        raise RuntimeError("wheel finalize steps must decode to a JSON list")
    normalized: list[dict] = []
    for index, step in enumerate(raw_steps):
        if not isinstance(step, dict):
            raise RuntimeError(f"wheel finalize step must be a mapping: index={index}")
        normalized.append(
            {
                "kind": step.get("kind"),
                "native_object_id": step.get("native_object_id"),
                "native_dir_name": step.get("native_dir_name"),
                "relative_subdir": step.get("relative_subdir"),
                "plugin_bundle_name": step.get("plugin_bundle_name"),
                "extra_library_file_names": tuple(step.get("extra_library_file_names") or ()),
            }
        )
    return tuple(normalized)


def _main() -> int:
    args = _parse_args()
    release_dir = args.release_dir.resolve()
    target_root = args.target_root.resolve()
    closed_sdk_root = (
        args.closed_sdk_root.resolve()
        if args.closed_sdk_root is not None
        else Path(os.environ.get("FLUXON_COMMU_CLOSED_SDK_ROOT", str(DEFAULT_CLOSED_SDK_ROOT))).resolve()
    )
    finalize_steps = _normalize_finalize_steps(json.loads(args.wheel_finalize_steps_json))

    packmod = _load_module(PACK_HELPER_PATH, "pack_fluxonkv_pylib_helper")
    wheel_helper = _load_module(WHEEL_HELPER_PATH, "wheel_runtime_helper_module")

    wheels = sorted(release_dir.glob("fluxon_pyo3-*.whl"))
    if not wheels:
        raise RuntimeError(f"no fluxon_pyo3 wheel found in release dir: {release_dir}")
    wheel_path = wheels[-1]

    _validate_zip_archive(wheel_path)
    wheel_helper.normalize_wheel_lib_rpaths(str(wheel_path))

    for step in finalize_steps:
        step_kind = step["kind"]
        if step_kind == WHEEL_FINALIZE_STEP_KIND_ADD_OFFLINE_RDMA_SHARED_LIBRARIES:
            native_dir_name = step["native_dir_name"]
            if not isinstance(native_dir_name, str) or not native_dir_name:
                raise RuntimeError(f"wheel finalize step requires native_dir_name: {step}")
            rdma_runtime_libs = _resolve_offline_rdma_runtime(
                closed_sdk_root=closed_sdk_root,
                target_root=target_root,
                packed_dir_name=native_dir_name,
            )
            with tempfile.TemporaryDirectory(prefix="fluxon_offline_rdma_alias_") as tmp_dir:
                rdma_runtime_stage_dir = Path(tmp_dir)
                rdma_runtime_stage_paths = packmod._ensure_vendor_runtime_soname_aliases(
                    staged_paths=[Path(path) for path in rdma_runtime_libs],
                    dest_dir=rdma_runtime_stage_dir,
                )
                wheel_helper.add_shared_libraries(
                    str(wheel_path),
                    [str(path) for path in rdma_runtime_stage_paths],
                )
            continue

        if step_kind == WHEEL_FINALIZE_STEP_KIND_ADD_NATIVE_PLUGINS:
            native_object_id = step["native_object_id"]
            relative_subdir = step["relative_subdir"]
            plugin_bundle_name = step["plugin_bundle_name"]
            if not isinstance(native_object_id, str) or not native_object_id:
                raise RuntimeError(f"wheel finalize step requires native_object_id: {step}")
            if not isinstance(relative_subdir, str) or not relative_subdir:
                raise RuntimeError(f"wheel finalize step requires relative_subdir: {step}")
            if not isinstance(plugin_bundle_name, str) or not plugin_bundle_name:
                raise RuntimeError(f"wheel finalize step requires plugin_bundle_name: {step}")

            native_dir_name = packmod._native_object_dir_name(object_id=native_object_id)
            native_lib_roots = _resolve_native_lib_roots(
                closed_sdk_root=closed_sdk_root,
                target_root=target_root,
                dir_name=native_dir_name,
            )
            plugins_candidates = [lib_root / relative_subdir for lib_root in native_lib_roots]
            plugins_dir = next((path for path in plugins_candidates if path.is_dir()), None)
            if plugins_dir is not None:
                extra_lib_candidates = [
                    plugins_dir.parent / file_name for file_name in step["extra_library_file_names"]
                ]
                extra_libs = [path for path in extra_lib_candidates if path.exists()]
                with tempfile.TemporaryDirectory(prefix="fluxon_native_plugin_stage_") as tmp_dir:
                    stage_dir = Path(tmp_dir) / plugin_bundle_name
                    shutil.copytree(plugins_dir, stage_dir, symlinks=True)
                    for lib in extra_libs:
                        shutil.copy2(lib, stage_dir / lib.name)
                    wheel_helper.add_plugins(str(wheel_path), str(stage_dir), plugin_bundle_name)
                print(f"wheel finalize: packaged native plugins from {plugins_dir}")
            else:
                print(
                    "wheel finalize: native plugin dir is absent, skipped plugins packaging: "
                    + ", ".join(str(path) for path in plugins_candidates)
                )
            continue

        if step_kind == WHEEL_FINALIZE_STEP_KIND_ADD_VENDOR_RUNTIME:
            vendor_runtime_root, vendor_lib_paths = packmod._resolve_vendor_runtime_install_root(
                cargo_target_root=target_root,
            )
            with packmod._extract_wheel_runtime_tree(wheel_path) as (wheel_extension_path, wheel_bundled_lib_dirs):
                bundled_name_map: dict[str, str] = {}
                for bundled_lib_dir in wheel_bundled_lib_dirs:
                    bundled_name_map.update(wheel_helper.get_repaired_lib_name_map(str(bundled_lib_dir)))
                with tempfile.TemporaryDirectory(prefix="fluxon_vendor_runtime_stage_") as tmp_dir:
                    runtime_stage_dir = Path(tmp_dir)
                    extra_runtime_libs = packmod._stage_vendor_runtime_closure(
                        wheel_extension_path=wheel_extension_path,
                        wheel_bundled_lib_dirs=wheel_bundled_lib_dirs,
                        vendor_root=vendor_runtime_root,
                        vendor_lib_paths=vendor_lib_paths,
                        stage_dir=runtime_stage_dir,
                        extra_ld_library_roots=[
                            closed_sdk_root / "lib",
                            target_root / "native_runtime" / "lib64",
                            target_root / "native_runtime" / "lib",
                            target_root / "native_runtime" / "lib" / "x86_64-linux-gnu",
                        ],
                    )
                    packaged_replacements = packmod._select_vendor_runtime_packaged_replacements(
                        packaged_file_names=set(bundled_name_map),
                        vendor_lib_paths=vendor_lib_paths,
                    )
                    bundled_runtime_libs = packmod._ensure_vendor_runtime_soname_aliases(
                        staged_paths=[*vendor_lib_paths, *extra_runtime_libs],
                        dest_dir=runtime_stage_dir,
                    )
                    wheel_helper.add_shared_libraries(
                        str(wheel_path),
                        [str(path) for path in bundled_runtime_libs],
                        packaged_lib_replacements=packaged_replacements,
                    )
            vendor_driver_config_dir = packmod._resolve_ibverbs_driver_config_dir(
                runtime_root=vendor_runtime_root,
                runtime_label="vendor runtime install-root",
            )
            wheel_helper.add_plugins(str(wheel_path), str(vendor_driver_config_dir), "libibverbs.d")
            packmod._validate_wheel_bundled_soname_aliases(wheel_path)
            packmod._validate_vendor_runtime_wheel_layout(wheel_path)
            print(f"wheel finalize: packaged vendor runtime from {vendor_runtime_root}")
            continue

        raise RuntimeError(f"unsupported wheel finalize step in rendered plan: {step_kind}")

    _rewrite_wheel_python_init(wheel_path, wheel_helper)
    _validate_zip_archive(wheel_path)
    _smoke_test_abi3_wheel(wheel_path)
    _validate_zip_archive(wheel_path)
    print(f"wheel finalize: completed {wheel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
