from __future__ import annotations

import base64
import csv
import hashlib
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path

from setup_and_pack.utils import wheel_runtime_helper as _WHEEL_HELPER


class WheelRuntimeHelperTest(unittest.TestCase):
    def test_create_wheel_refreshes_record_after_binary_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            dist_info_dir = source_dir / "fluxon_pyo3-0.0.0.dist-info"
            pkg_dir = source_dir / "fluxon_pyo3"
            dist_info_dir.mkdir(parents=True)
            pkg_dir.mkdir(parents=True)
            payload_path = pkg_dir / "payload.bin"
            payload_path.write_bytes(b"old-payload")
            (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
            (dist_info_dir / "WHEEL").write_text("Wheel-Version: 1.0\n", encoding="utf-8")
            (dist_info_dir / "METADATA").write_text("Metadata-Version: 2.1\nName: fluxon-pyo3\nVersion: 0.0.0\n", encoding="utf-8")
            (dist_info_dir / "RECORD").write_text("stale,sha256=deadbeef,1\n", encoding="utf-8")

            payload_path.write_bytes(b"new-payload")
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            with zipfile.ZipFile(wheel_path, "r") as zip_ref:
                record_rows = list(csv.reader(zip_ref.read("fluxon_pyo3-0.0.0.dist-info/RECORD").decode("utf-8").splitlines()))
                record_map = {row[0]: row[1:] for row in record_rows}
                actual_payload = zip_ref.read("fluxon_pyo3/payload.bin")
                actual_hash = "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(actual_payload).digest()).decode("ascii").rstrip("=")
                self.assertEqual(record_map["fluxon_pyo3/payload.bin"], [actual_hash, str(len(actual_payload))])
                self.assertEqual(record_map["fluxon_pyo3-0.0.0.dist-info/RECORD"], ["", ""])

    def test_install_shared_libraries_overwrites_and_adds_members(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            libs_dir = source_dir / "fluxon_pyo3.libs"
            libs_dir.mkdir(parents=True)
            (source_dir / "fluxon_pyo3").mkdir()
            (source_dir / "fluxon_pyo3" / "__init__.py").write_text("", encoding="utf-8")
            old_core = root / "libfluxon_commu_core_old.so"
            subprocess.run(
                ["cc", "-shared", "-fPIC", "-Wl,-soname,libfluxon_commu_core-abc.so", "-o", str(old_core), "-xc", "-"],
                input="int old_core(void) { return 1; }\n",
                text=True,
                check=True,
            )
            (libs_dir / "libfluxon_commu_core-abc.so").write_bytes(old_core.read_bytes())
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            core_source = root / "libfluxon_commu_core.so"
            probe_source = root / "libfluxon_rdma_probe.so"
            subprocess.run(
                ["cc", "-shared", "-fPIC", "-Wl,-soname,libfluxon_commu_core.so", "-o", str(core_source), "-xc", "-"],
                input="int new_core(void) { return 2; }\n",
                text=True,
                check=True,
            )
            subprocess.run(
                ["cc", "-shared", "-fPIC", "-Wl,-soname,libfluxon_rdma_probe.so", "-o", str(probe_source), "-xc", "-"],
                input="int new_probe(void) { return 3; }\n",
                text=True,
                check=True,
            )

            _WHEEL_HELPER.install_shared_libraries(
                str(wheel_path),
                {
                    "libfluxon_commu_core-abc.so": str(core_source),
                    "libfluxon_rdma_probe.so": str(probe_source),
                },
            )

            extract_root = Path(_WHEEL_HELPER.extract_wheel(str(wheel_path)))
            try:
                core_installed = extract_root / "fluxon_pyo3.libs" / "libfluxon_commu_core-abc.so"
                probe_installed = extract_root / "fluxon_pyo3.libs" / "libfluxon_rdma_probe.so"
                self.assertTrue(core_installed.is_file())
                self.assertTrue(probe_installed.is_file())
                for installed_path, expected_soname in (
                    (core_installed, "libfluxon_commu_core.so"),
                    (probe_installed, "libfluxon_rdma_probe.so"),
                ):
                    readelf_out = subprocess.run(
                        ["readelf", "-d", str(installed_path)],
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout
                    self.assertIn(expected_soname, readelf_out)
                    self.assertIn("$ORIGIN", readelf_out)
            finally:
                subprocess.run(["rm", "-rf", str(extract_root)], check=True)

    def test_normalize_wheel_lib_rpaths_sets_extension_runpath_to_wheel_libs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            pkg_dir = source_dir / "fluxon_pyo3"
            libs_dir = source_dir / "fluxon_pyo3.libs"
            pkg_dir.mkdir(parents=True)
            libs_dir.mkdir(parents=True)
            (pkg_dir / "__init__.py").write_text("", encoding="utf-8")

            ext_source = root / "libexample.so"
            subprocess.run(
                ["cc", "-shared", "-fPIC", "-Wl,-soname,libexample.so", "-o", str(ext_source), "-xc", "-"],
                input="int fluxon_dummy(void) { return 0; }\n",
                text=True,
                check=True,
            )
            (pkg_dir / "fluxon_pyo3.abi3.so").write_bytes(ext_source.read_bytes())
            (libs_dir / "libexample.so").write_bytes(ext_source.read_bytes())
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            _WHEEL_HELPER.normalize_wheel_lib_rpaths(str(wheel_path))

            extract_root = Path(_WHEEL_HELPER.extract_wheel(str(wheel_path)))
            try:
                ext_path = extract_root / "fluxon_pyo3" / "fluxon_pyo3.abi3.so"
                readelf_out = subprocess.run(
                    ["readelf", "-d", str(ext_path)],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                self.assertIn("$ORIGIN/../fluxon_pyo3.libs", readelf_out)
            finally:
                subprocess.run(["rm", "-rf", str(extract_root)], check=True)

    def test_add_shared_libraries_sets_runpath_for_added_shared_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            wheel_path = root / "fluxon_pyo3-0.0.0-py3-none-any.whl"
            source_dir = root / "wheel_src"
            pkg_dir = source_dir / "fluxon_pyo3"
            dist_dir = source_dir / "fluxon_pyo3-0.0.0.dist-info"
            pkg_dir.mkdir(parents=True)
            dist_dir.mkdir(parents=True)
            (pkg_dir / "__init__.py").write_text("", encoding="utf-8")
            (dist_dir / "WHEEL").write_text("Wheel-Version: 1.0\nTag: py3-none-any\n", encoding="utf-8")
            (dist_dir / "METADATA").write_text(
                "Metadata-Version: 2.1\nName: fluxon-pyo3\nVersion: 0.0.0\n",
                encoding="utf-8",
            )
            _WHEEL_HELPER.create_wheel(str(wheel_path), str(source_dir))

            lib_source = root / "libruntime_dep.so.1"
            subprocess.run(
                ["cc", "-shared", "-fPIC", "-Wl,-soname,libruntime_dep.so.1", "-o", str(lib_source), "-xc", "-"],
                input="int runtime_dep_dummy(void) { return 0; }\n",
                text=True,
                check=True,
            )

            _WHEEL_HELPER.add_shared_libraries(
                str(wheel_path),
                [str(lib_source)],
            )

            extract_root = Path(_WHEEL_HELPER.extract_wheel(str(wheel_path)))
            try:
                lib_path = extract_root / "fluxon_pyo3.libs" / "libruntime_dep.so.1"
                readelf_out = subprocess.run(
                    ["readelf", "-d", str(lib_path)],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                self.assertIn("$ORIGIN", readelf_out)
            finally:
                subprocess.run(["rm", "-rf", str(extract_root)], check=True)

    def test_merge_binary_wheel_copies_runtime_payload_and_runtime_tag(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            pure_wheel = root / "fluxon-0.2.1-py3-none-any.whl"
            runtime_wheel = root / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"
            output_wheel = root / "fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl"

            pure_src = root / "pure_src"
            pure_pkg = pure_src / "fluxon_py"
            pure_dist = pure_src / "fluxon-0.2.1.dist-info"
            pure_pkg.mkdir(parents=True)
            pure_dist.mkdir(parents=True)
            (pure_pkg / "__init__.py").write_text("__version__ = '0.2.1'\n", encoding="utf-8")
            (pure_dist / "WHEEL").write_text(
                "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
                encoding="utf-8",
            )
            (pure_dist / "METADATA").write_text(
                "Metadata-Version: 2.1\nName: fluxon\nVersion: 0.2.1\n",
                encoding="utf-8",
            )
            _WHEEL_HELPER.create_wheel(str(pure_wheel), str(pure_src))

            runtime_src = root / "runtime_src"
            runtime_pkg = runtime_src / "fluxon_pyo3"
            runtime_libs = runtime_src / "fluxon_pyo3.libs"
            runtime_dist = runtime_src / "fluxon_pyo3-0.2.1.dist-info"
            runtime_pkg.mkdir(parents=True)
            runtime_libs.mkdir(parents=True)
            runtime_dist.mkdir(parents=True)
            (runtime_pkg / "__init__.py").write_text("", encoding="utf-8")
            (runtime_pkg / "fluxon_pyo3.abi3.so").write_bytes(b"runtime-so")
            (runtime_libs / "libruntime.so").write_bytes(b"runtime-lib")
            (runtime_dist / "WHEEL").write_text(
                "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: false\nTag: cp38-abi3-manylinux_2_28_x86_64\n",
                encoding="utf-8",
            )
            (runtime_dist / "METADATA").write_text(
                "Metadata-Version: 2.1\nName: fluxon-pyo3\nVersion: 0.2.1\n",
                encoding="utf-8",
            )
            _WHEEL_HELPER.create_wheel(str(runtime_wheel), str(runtime_src))

            _WHEEL_HELPER.merge_binary_wheel(
                output_wheel_path=str(output_wheel),
                pure_python_wheel_path=str(pure_wheel),
                runtime_wheel_path=str(runtime_wheel),
            )

            with zipfile.ZipFile(output_wheel, "r") as zip_ref:
                self.assertEqual(zip_ref.read("fluxon_py/__init__.py").decode("utf-8"), "__version__ = '0.2.1'\n")
                self.assertEqual(zip_ref.read("fluxon_pyo3/fluxon_pyo3.abi3.so"), b"runtime-so")
                self.assertEqual(zip_ref.read("fluxon_pyo3.libs/libruntime.so"), b"runtime-lib")
                wheel_text = zip_ref.read("fluxon-0.2.1.dist-info/WHEEL").decode("utf-8")
                self.assertIn("Root-Is-Purelib: false", wheel_text)
                self.assertIn("Tag: cp38-abi3-manylinux_2_28_x86_64", wheel_text)


if __name__ == "__main__":
    unittest.main()
