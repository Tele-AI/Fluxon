from __future__ import annotations

import base64
import csv
import hashlib
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path


def extract_wheel(wheel_path: str) -> str:
    temp_dir = tempfile.mkdtemp(prefix="fluxon_wheel_")
    with zipfile.ZipFile(wheel_path, "r") as zip_ref:
        zip_ref.extractall(temp_dir)
    return temp_dir


def _wheel_record_path(source_root: Path) -> Path | None:
    dist_info_dirs = sorted(path for path in source_root.glob("*.dist-info") if path.is_dir())
    if not dist_info_dirs:
        return None
    if len(dist_info_dirs) != 1:
        raise RuntimeError(f"expected exactly one dist-info directory, found {len(dist_info_dirs)} in {source_root}")
    return dist_info_dirs[0] / "RECORD"


def _hash_file_for_record(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256(path.read_bytes()).digest()
    encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"sha256={encoded}", path.stat().st_size


def _refresh_wheel_record(source_root: Path) -> None:
    record_path = _wheel_record_path(source_root)
    if record_path is None:
        return

    record_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[list[str]] = []
    # Rebuild RECORD from the extracted tree so hashes stay in sync after edits.
    for path in sorted(source_root.rglob("*")):
        if path.is_dir() or path == record_path:
            continue
        relpath = path.relative_to(source_root).as_posix()
        digest, size = _hash_file_for_record(path)
        rows.append([relpath, digest, str(size)])
    rows.append([record_path.relative_to(source_root).as_posix(), "", ""])

    with record_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerows(rows)


def create_wheel(wheel_path: str, source_dir: str) -> None:
    source_root = Path(source_dir)
    _refresh_wheel_record(source_root)
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_out:
        for path in sorted(source_root.rglob("*")):
            if path.is_dir():
                continue
            zip_out.write(path, path.relative_to(source_root).as_posix())


def _set_rpath(path: Path, rpath: str) -> None:
    subprocess.run(
        ["patchelf", "--set-rpath", rpath, str(path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _normalize_extracted_wheel_lib_rpaths(*, source_root: Path, pkg_name: str) -> None:
    pkg_dir = source_root / pkg_name
    libs_dir = source_root / f"{pkg_name}.libs"

    for ext_path in sorted(pkg_dir.glob("*.so")):
        # Extension modules should resolve bundled runtime libs from the wheel.
        _set_rpath(ext_path, "$ORIGIN:$ORIGIN/../fluxon_pyo3.libs")

    if libs_dir.is_dir():
        for lib_path in sorted(libs_dir.glob("*.so*")):
            if not lib_path.is_file():
                continue
            # Bundled shared libraries only need to reference siblings in .libs.
            _set_rpath(lib_path, "$ORIGIN")


def normalize_wheel_lib_rpaths(wheel_path: str) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_rpath_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        _normalize_extracted_wheel_lib_rpaths(source_root=temp_dir, pkg_name=pkg_name)

        create_wheel(wheel_path, str(temp_dir))


def get_repaired_lib_name_map(lib_dir: str) -> dict[str, str]:
    root = Path(lib_dir)
    if not root.is_dir():
        return {}
    return {
        path.name: str(path)
        for path in sorted(root.iterdir())
        if path.is_file() and (".so" in path.name or path.suffix in {".dylib", ".dll"})
    }


def add_shared_libraries(
    wheel_path: str,
    library_paths: list[str],
    *,
    packaged_lib_replacements: dict[str, str] | None = None,
) -> None:
    packaged_lib_replacements = packaged_lib_replacements or {}
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_libs_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        libs_dir = temp_dir / f"{pkg_name}.libs"
        libs_dir.mkdir(parents=True, exist_ok=True)

        for raw_path in library_paths:
            source_path = Path(raw_path)
            if not source_path.is_file():
                raise RuntimeError(f"shared library is missing: {source_path}")
            shutil.copy2(source_path, libs_dir / source_path.name)

        for base_name, replacement_path in packaged_lib_replacements.items():
            source_path = Path(replacement_path)
            if not source_path.is_file():
                raise RuntimeError(f"replacement shared library is missing: {source_path}")
            shutil.copy2(source_path, libs_dir / base_name)

        _normalize_extracted_wheel_lib_rpaths(source_root=temp_dir, pkg_name=pkg_name)
        create_wheel(wheel_path, str(temp_dir))


def install_shared_libraries(
    wheel_path: str,
    library_paths_by_name: dict[str, str],
) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_install_libs_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        libs_dir = temp_dir / f"{pkg_name}.libs"
        libs_dir.mkdir(parents=True, exist_ok=True)

        for dest_name, raw_source_path in sorted(library_paths_by_name.items()):
            source_path = Path(raw_source_path)
            if not source_path.is_file():
                raise RuntimeError(f"shared library is missing: {source_path}")
            shutil.copy2(source_path, libs_dir / dest_name)

        _normalize_extracted_wheel_lib_rpaths(source_root=temp_dir, pkg_name=pkg_name)
        create_wheel(wheel_path, str(temp_dir))


def add_plugins(wheel_path: str, plugins_dir: str, bundle_name: str) -> None:
    source_root = Path(plugins_dir)
    if not source_root.is_dir():
        raise RuntimeError(f"plugin directory is missing: {source_root}")
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_plugins_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = Path(wheel_path).name.split("-", 1)[0]
        dst_root = temp_dir / f"{pkg_name}.libs" / bundle_name
        dst_root.mkdir(parents=True, exist_ok=True)
        for path in sorted(source_root.rglob("*")):
            if path.is_dir():
                continue
            # Preserve the plugin tree layout inside the bundle directory.
            relpath = path.relative_to(source_root)
            dst_path = dst_root / relpath
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dst_path)
        create_wheel(wheel_path, str(temp_dir))


def merge_binary_wheel(
    *,
    output_wheel_path: str,
    pure_python_wheel_path: str,
    runtime_wheel_path: str,
    runtime_package_name: str = "fluxon_pyo3",
) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_merge_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(pure_python_wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        runtime_root = Path(extract_wheel(runtime_wheel_path))
        try:
            # Copy the binary runtime payload into the pure wheel before repacking.
            runtime_pkg_dir = runtime_root / runtime_package_name
            if not runtime_pkg_dir.is_dir():
                raise RuntimeError(f"missing runtime package dir in wheel: {runtime_pkg_dir}")
            shutil.copytree(runtime_pkg_dir, temp_dir / runtime_package_name, dirs_exist_ok=True)

            runtime_libs_dir = runtime_root / f"{runtime_package_name}.libs"
            if runtime_libs_dir.is_dir():
                shutil.copytree(runtime_libs_dir, temp_dir / f"{runtime_package_name}.libs", dirs_exist_ok=True)

            pure_dist_info = _wheel_dist_info_dir(temp_dir)
            runtime_dist_info = _wheel_dist_info_dir(runtime_root)
            runtime_wheel_text = _read_wheel_file(runtime_dist_info / "WHEEL")
            pure_wheel_path = pure_dist_info / "WHEEL"
            pure_wheel_path.write_text(
                _merge_wheel_file_text(
                    pure_wheel_path.read_text(encoding="utf-8"),
                    runtime_wheel_text,
                ),
                encoding="utf-8",
            )
            create_wheel(output_wheel_path, str(temp_dir))
        finally:
            shutil.rmtree(runtime_root)


def _wheel_dist_info_dir(source_root: Path) -> Path:
    dist_info_dirs = sorted(path for path in source_root.glob("*.dist-info") if path.is_dir())
    if not dist_info_dirs:
        raise RuntimeError(f"expected exactly one dist-info directory, found none in {source_root}")
    if len(dist_info_dirs) != 1:
        raise RuntimeError(f"expected exactly one dist-info directory, found {len(dist_info_dirs)} in {source_root}")
    return dist_info_dirs[0]


def _read_wheel_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _merge_wheel_file_text(pure_wheel_text: str, runtime_wheel_text: str) -> str:
    pure_lines = pure_wheel_text.splitlines()
    runtime_lines = runtime_wheel_text.splitlines()
    tag_lines = [line for line in runtime_lines if line.startswith("Tag: ")]
    if not tag_lines:
        raise RuntimeError("runtime wheel WHEEL file missing Tag lines")

    out_lines: list[str] = []
    seen_root_line = False
    seen_tag_line = False
    # The merged wheel stops being purelib and must inherit the runtime wheel tags.
    for line in pure_lines:
        if line.startswith("Root-Is-Purelib:"):
            out_lines.append("Root-Is-Purelib: false")
            seen_root_line = True
            continue
        if line.startswith("Tag: "):
            if not seen_tag_line:
                out_lines.extend(tag_lines)
                seen_tag_line = True
            continue
        out_lines.append(line)
    if not seen_root_line:
        out_lines.append("Root-Is-Purelib: false")
    if not seen_tag_line:
        out_lines.extend(tag_lines)
    return "\n".join(out_lines) + "\n"


__all__ = [
    "extract_wheel",
    "create_wheel",
    "normalize_wheel_lib_rpaths",
    "get_repaired_lib_name_map",
    "add_shared_libraries",
    "install_shared_libraries",
    "add_plugins",
    "merge_binary_wheel",
]
