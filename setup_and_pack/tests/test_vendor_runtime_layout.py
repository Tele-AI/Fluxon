from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKER_MODULE_PATHS = (
    REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.py",
)


def _load_module(module_path: Path, module_name: str):
    for import_root in (
        REPO_ROOT,
        module_path.parent,
        module_path.parent.parent,
    ):
        import_root_str = str(import_root)
        if import_root_str in sys.path:
            sys.path.remove(import_root_str)
        sys.path.insert(0, import_root_str)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class VendorRuntimeLayoutTest(unittest.TestCase):
    def test_ensure_vendor_runtime_soname_aliases_materializes_missing_alias_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            staged_dir = root / "stage"
            staged_dir.mkdir(parents=True)
            lib_path = staged_dir / "libfabric.so"
            lib_path.write_text("binary", encoding="utf-8")

            for module_path in PACKER_MODULE_PATHS:
                mod = _load_module(
                    module_path=module_path,
                    module_name=f"vendor_runtime_alias_materialize_{module_path.stem}",
                )
                mod._read_elf_soname = lambda _path: "libfabric.so.1"  # type: ignore[attr-defined]
                aliased_paths = mod._ensure_vendor_runtime_soname_aliases(  # type: ignore[attr-defined]
                    staged_paths=[lib_path],
                    dest_dir=staged_dir,
                )
                self.assertEqual(
                    [path.name for path in aliased_paths],
                    ["libfabric.so", "libfabric.so.1"],
                )
                self.assertTrue((staged_dir / "libfabric.so.1").is_file())

    def test_resolve_install_root_accepts_provider_only_mlx5_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cargo_target_root = Path(tmpdir)
            vendor_root = cargo_target_root / "vendor_runtime"
            (vendor_root / "include" / "rdma").mkdir(parents=True)
            (vendor_root / "include" / "infiniband").mkdir(parents=True)
            (vendor_root / "lib" / "libibverbs").mkdir(parents=True)

            (vendor_root / "include" / "rdma" / "fabric.h").write_text("", encoding="utf-8")
            (vendor_root / "include" / "infiniband" / "verbs.h").write_text("", encoding="utf-8")
            (vendor_root / "lib" / "libfabric.so").write_text("", encoding="utf-8")
            (vendor_root / "lib" / "libibverbs.so").write_text("", encoding="utf-8")
            (vendor_root / "lib" / "libibverbs" / "libmlx5-rdmav34.so").write_text(
                "",
                encoding="utf-8",
            )
            driver_dir = vendor_root / "etc" / "libibverbs.d"
            driver_dir.mkdir(parents=True)
            (driver_dir / "mlx5.driver").write_text("", encoding="utf-8")

            for module_path in PACKER_MODULE_PATHS:
                mod = _load_module(
                    module_path=module_path,
                    module_name=f"vendor_runtime_layout_test_{module_path.stem}",
                )
                mod._read_elf_soname = lambda _path: None  # type: ignore[attr-defined]
                resolved_root, vendor_lib_paths = mod._resolve_vendor_runtime_install_root(
                    cargo_target_root=cargo_target_root,
                )
                self.assertEqual(resolved_root, vendor_root)
                self.assertIn("libmlx5-rdmav34.so", {path.name for path in vendor_lib_paths})

    def test_packaged_replacements_cover_hashed_auditwheel_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            vendor_root = Path(tmpdir)
            vendor_lib_paths = [
                vendor_root / "libibverbs.so.1",
                vendor_root / "libibverbs.so",
                vendor_root / "libfabric.so.1",
            ]
            for path in vendor_lib_paths:
                path.write_text("", encoding="utf-8")

            for module_path in PACKER_MODULE_PATHS:
                mod = _load_module(
                    module_path=module_path,
                    module_name=f"vendor_runtime_replacement_test_{module_path.stem}",
                )
                mod._read_elf_soname = lambda _path: None  # type: ignore[attr-defined]
                replacements = mod._select_vendor_runtime_packaged_replacements(
                    packaged_file_names={
                        "libibverbs-249242c3.so.1",
                        "libibverbs.so",
                        "libfabric-f679d7aa.so.1",
                        "libssl-ignored.so.3",
                    },
                    vendor_lib_paths=vendor_lib_paths,
                )
                self.assertEqual(
                    replacements["libibverbs-249242c3.so.1"],
                    str(vendor_root / "libibverbs.so.1"),
                )
                self.assertEqual(
                    replacements["libibverbs.so"],
                    str(vendor_root / "libibverbs.so.1"),
                )
                self.assertEqual(
                    replacements["libfabric-f679d7aa.so.1"],
                    str(vendor_root / "libfabric.so.1"),
                )
                self.assertNotIn("libssl-ignored.so.3", replacements)

    def test_pub_prepare_vendor_runtime_readiness_rejects_missing_transitive_dependency(self) -> None:
        module_path = REPO_ROOT / "setup_and_pack" / "pub_prepare_build.py"
        mod = _load_module(
            module_path=module_path,
            module_name="pub_prepare_vendor_runtime_readiness_test",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            vendor_root = Path(tmpdir) / "vendor_runtime"
            (vendor_root / "etc" / "libibverbs.d").mkdir(parents=True)
            (vendor_root / "lib").mkdir(parents=True)
            (vendor_root / "etc" / "libibverbs.d" / "mlx5.driver").write_text("", encoding="utf-8")
            for lib_name in (
                "libfabric.so.1",
                "libibverbs.so.1",
                "libmlx5.so.1",
                "libmlx5-rdmav34.so",
            ):
                (vendor_root / "lib" / lib_name).write_text("", encoding="utf-8")

            def fake_read_runtime_dependency_entries(path: Path, *, ld_library_paths: list[Path]):
                if path.name == "libfabric.so.1":
                    raise RuntimeError(
                        f"pub_prepare_build.py: runtime dependency not found for {path}: libpsm_infinipath.so.1"
                    )
                return []

            mod.read_runtime_dependency_entries = fake_read_runtime_dependency_entries  # type: ignore[attr-defined]
            self.assertFalse(mod.vendor_runtime_install_root_ready(vendor_root))

    def test_pub_prepare_vendor_runtime_readiness_does_not_require_protoc(self) -> None:
        module_path = REPO_ROOT / "setup_and_pack" / "pub_prepare_build.py"
        mod = _load_module(
            module_path=module_path,
            module_name="pub_prepare_vendor_runtime_no_protoc_test",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            vendor_root = Path(tmpdir) / "vendor_runtime"
            (vendor_root / "etc" / "libibverbs.d").mkdir(parents=True)
            (vendor_root / "lib").mkdir(parents=True)
            (vendor_root / "etc" / "libibverbs.d" / "mlx5.driver").write_text("", encoding="utf-8")
            for lib_name in (
                "libfabric.so.1",
                "libibverbs.so.1",
                "libmlx5.so.1",
                "libmlx5-rdmav34.so",
                "libpsm_infinipath.so.1",
                "libpsm2.so.2",
                "libinfinipath.so.4",
            ):
                (vendor_root / "lib" / lib_name).write_text("", encoding="utf-8")

            mod.read_runtime_dependency_entries = lambda _path, *, ld_library_paths: []  # type: ignore[attr-defined]
            self.assertTrue(mod.vendor_runtime_install_root_ready(vendor_root))


if __name__ == "__main__":
    unittest.main()
