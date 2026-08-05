from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RESOLVER_PATH = REPO_ROOT / "fluxon_release" / "resolve_release_meta.py"


def _load_resolver():
    spec = importlib.util.spec_from_file_location("fluxon_resolve_release_meta", RESOLVER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {RESOLVER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_RESOLVER = _load_resolver()


class ResolveReleaseMetaTest(unittest.TestCase):
    def test_release_notes_output_is_portable_between_jobs(self) -> None:
        version = _RESOLVER._read_python_string_constant(_RESOLVER.PY_VERSION_FILE, "__version__")
        completed = subprocess.run(
            [sys.executable, str(RESOLVER_PATH), "--git-ref", f"refs/tags/v{version}"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        outputs = dict(line.split("=", 1) for line in completed.stdout.splitlines())
        release_notes_file = Path(outputs["release_notes_file"])
        self.assertFalse(release_notes_file.is_absolute())
        self.assertEqual(
            release_notes_file,
            Path("fluxon_release") / "release_notes" / f"v{version}.md",
        )

    def test_reads_closed_sdk_required_open_surface_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(
                json.dumps({"required_open_surface_version": "1.2.3"}),
                encoding="utf-8",
            )
            self.assertEqual(
                _RESOLVER._read_json_string(path, "required_open_surface_version"),
                "1.2.3",
            )

    def test_reads_open_surface_version_independently_from_release_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "lib.rs"
            path.write_text(
                'pub const FLUXON_COMMU_OPEN_SURFACE_VERSION: &str = "1.2.3";\n',
                encoding="utf-8",
            )
            self.assertEqual(
                _RESOLVER._read_rust_string_constant(
                    path,
                    "FLUXON_COMMU_OPEN_SURFACE_VERSION",
                ),
                "1.2.3",
            )

    def test_repo_closed_sdk_matches_open_surface_contract(self) -> None:
        self.assertEqual(
            _RESOLVER._read_json_string(
                _RESOLVER.CLOSED_SDK_MANIFEST_FILE,
                "required_open_surface_version",
            ),
            _RESOLVER._read_rust_string_constant(
                _RESOLVER.COMMU_CONTRACT_FILE,
                "FLUXON_COMMU_OPEN_SURFACE_VERSION",
            ),
        )

    def test_rejects_non_string_closed_sdk_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(
                json.dumps({"required_open_surface_version": 123}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "missing string required_open_surface_version"):
                _RESOLVER._read_json_string(path, "required_open_surface_version")


if __name__ == "__main__":
    unittest.main()
