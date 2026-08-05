from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from setup_and_pack.release_ci_provenance import (
    WORKFLOW_PATH,
    build_provenance,
    validate_tag_provenance,
    write_provenance,
)


class ReleaseCIProvenanceTest(unittest.TestCase):
    def test_exact_tag_provenance_round_trip(self) -> None:
        payload = build_provenance(
            repository="example/fluxon",
            ref="refs/tags/v1.2.3",
            ref_name="v1.2.3",
            ref_type="tag",
            sha="a" * 40,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "release-ci-provenance.json"
            write_provenance(path, payload)
            validated = validate_tag_provenance(
                path,
                expected_repository="example/fluxon",
                expected_sha="a" * 40,
                expected_tag="v1.2.3",
            )
        self.assertEqual(validated, payload)
        self.assertEqual(validated["workflow_path"], WORKFLOW_PATH)

    def test_branch_cannot_satisfy_tag_validation(self) -> None:
        payload = build_provenance(
            repository="example/fluxon",
            ref="refs/heads/v1.2.3",
            ref_name="v1.2.3",
            ref_type="branch",
            sha="b" * 40,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "release-ci-provenance.json"
            write_provenance(path, payload)
            with self.assertRaisesRegex(ValueError, "release CI provenance mismatch"):
                validate_tag_provenance(
                    path,
                    expected_repository="example/fluxon",
                    expected_sha="b" * 40,
                    expected_tag="v1.2.3",
                )

    def test_unknown_fields_are_rejected(self) -> None:
        payload = build_provenance(
            repository="example/fluxon",
            ref="refs/tags/v1.2.3",
            ref_name="v1.2.3",
            ref_type="tag",
            sha="c" * 40,
        )
        payload["unexpected"] = True
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "release-ci-provenance.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "fields mismatch"):
                validate_tag_provenance(
                    path,
                    expected_repository="example/fluxon",
                    expected_sha="c" * 40,
                    expected_tag="v1.2.3",
                )


if __name__ == "__main__":
    unittest.main()
