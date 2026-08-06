from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from setup_and_pack.release_ci_provenance import (
    WORKFLOW_PATH,
    build_provenance,
    validate_branch_provenance,
    write_provenance,
)


class ReleaseCIProvenanceTest(unittest.TestCase):
    def test_exact_default_branch_provenance_round_trip(self) -> None:
        payload = build_provenance(
            repository="example/fluxon",
            ref="refs/heads/main",
            ref_name="main",
            ref_type="branch",
            sha="a" * 40,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "release-ci-provenance.json"
            write_provenance(path, payload)
            validated = validate_branch_provenance(
                path,
                expected_repository="example/fluxon",
                expected_sha="a" * 40,
                expected_branch="main",
            )
        self.assertEqual(validated, payload)
        self.assertEqual(validated["workflow_path"], WORKFLOW_PATH)

    def test_tag_cannot_satisfy_default_branch_validation(self) -> None:
        payload = build_provenance(
            repository="example/fluxon",
            ref="refs/tags/v1.2.3",
            ref_name="v1.2.3",
            ref_type="tag",
            sha="b" * 40,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "release-ci-provenance.json"
            write_provenance(path, payload)
            with self.assertRaisesRegex(ValueError, "release CI provenance mismatch"):
                validate_branch_provenance(
                    path,
                    expected_repository="example/fluxon",
                    expected_sha="b" * 40,
                    expected_branch="main",
                )

    def test_unknown_fields_are_rejected(self) -> None:
        payload = build_provenance(
            repository="example/fluxon",
            ref="refs/heads/main",
            ref_name="main",
            ref_type="branch",
            sha="c" * 40,
        )
        payload["unexpected"] = True
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "release-ci-provenance.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "fields mismatch"):
                validate_branch_provenance(
                    path,
                    expected_repository="example/fluxon",
                    expected_sha="c" * 40,
                    expected_branch="main",
                )


if __name__ == "__main__":
    unittest.main()
