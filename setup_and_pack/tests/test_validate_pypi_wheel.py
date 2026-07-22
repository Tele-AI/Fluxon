from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import zipfile

from setup_and_pack.package_contract import (
    PYTHON_DISTRIBUTION_NAME,
    PYTHON_WHEEL_DISTRIBUTION,
    RELEASE_WHEEL_GLOB,
)
from setup_and_pack.validate_pypi_wheel import validate_release_wheel


class ValidatePyPIWheelTest(unittest.TestCase):
    def test_distribution_and_wheel_names_are_canonical(self) -> None:
        self.assertEqual(PYTHON_DISTRIBUTION_NAME, "fluxon-ai")
        self.assertEqual(PYTHON_WHEEL_DISTRIBUTION, "fluxon_ai")
        self.assertEqual(RELEASE_WHEEL_GLOB, "fluxon_ai-*.whl")

    def _write_wheel(
        self,
        release_dir: Path,
        *,
        filename: str = "fluxon_ai-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
        metadata_name: str = "fluxon-ai",
        metadata_version: str = "0.2.1",
        requires_python: str = ">=3.10",
    ) -> Path:
        wheel_path = release_dir / filename
        dist_info = f"fluxon_ai-{metadata_version}.dist-info"
        with zipfile.ZipFile(wheel_path, "w") as archive:
            archive.writestr(
                f"{dist_info}/METADATA",
                "\n".join(
                    (
                        "Metadata-Version: 2.1",
                        f"Name: {metadata_name}",
                        f"Version: {metadata_version}",
                        f"Requires-Python: {requires_python}",
                        "",
                    )
                ),
            )
            archive.writestr(
                f"{dist_info}/WHEEL",
                "Wheel-Version: 1.0\nTag: cp38-abi3-manylinux_2_28_x86_64\n",
            )
        return wheel_path

    def test_accepts_expected_unified_wheel(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            wheel_path = self._write_wheel(release_dir)

            validated = validate_release_wheel(release_dir=release_dir, release_tag="v0.2.1")

            self.assertEqual(validated.path, str(wheel_path.resolve()))
            self.assertEqual(validated.distribution, "fluxon-ai")
            self.assertEqual(validated.version, "0.2.1")
            self.assertEqual(len(validated.sha256), 64)

    def test_rejects_wrong_metadata_distribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            self._write_wheel(release_dir, metadata_name="fluxon")

            with self.assertRaisesRegex(RuntimeError, "METADATA Name mismatch"):
                validate_release_wheel(release_dir=release_dir, release_tag="v0.2.1")

    def test_rejects_tag_version_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            self._write_wheel(release_dir)

            with self.assertRaisesRegex(RuntimeError, "release tag mismatch"):
                validate_release_wheel(release_dir=release_dir, release_tag="v0.2.2")

    def test_rejects_multiple_release_wheels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            self._write_wheel(release_dir)
            self._write_wheel(
                release_dir,
                filename="fluxon_ai-0.2.2-cp38-abi3-manylinux_2_28_x86_64.whl",
                metadata_version="0.2.2",
            )

            with self.assertRaisesRegex(RuntimeError, "expected exactly one release wheel"):
                validate_release_wheel(release_dir=release_dir, release_tag="v0.2.1")

    def test_rejects_unexpected_platform_tag(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            self._write_wheel(
                release_dir,
                filename="fluxon_ai-0.2.1-py3-none-any.whl",
            )

            with self.assertRaisesRegex(RuntimeError, "unexpected release wheel compatibility tag"):
                validate_release_wheel(release_dir=release_dir, release_tag="v0.2.1")

    def test_rejects_wheel_over_configured_size_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_dir = Path(tmpdir)
            wheel_path = self._write_wheel(release_dir)

            with self.assertRaisesRegex(RuntimeError, "file-size limit"):
                validate_release_wheel(
                    release_dir=release_dir,
                    release_tag="v0.2.1",
                    max_wheel_size_bytes=wheel_path.stat().st_size - 1,
                )


if __name__ == "__main__":
    unittest.main()
