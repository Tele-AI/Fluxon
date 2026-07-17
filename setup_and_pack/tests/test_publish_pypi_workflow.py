from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "publish-pypi.yml"


class PublishPyPIWorkflowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.workflow = yaml.load(self.source, Loader=yaml.BaseLoader)

    def test_waits_for_successful_tag_ci(self) -> None:
        trigger = self.workflow["on"]["workflow_run"]
        self.assertEqual(trigger["workflows"], ["ci_2_virt_node"])
        self.assertEqual(trigger["types"], ["completed"])

        prepare = self.workflow["jobs"]["prepare-wheel"]
        condition = prepare["if"]
        self.assertIn("workflow_run.event == 'push'", condition)
        self.assertIn("workflow_run.conclusion == 'success'", condition)
        self.assertIn("workflow_run.path == '.github/workflows/all_test.yml'", condition)
        self.assertIn("startsWith(github.event.workflow_run.head_branch, 'v')", condition)
        self.assertIn("release-source/fluxon_release/resolve_release_meta.py", self.source)

    def test_oidc_permission_is_scoped_to_publish_job(self) -> None:
        self.assertNotIn("id-token", self.workflow["permissions"])
        prepare = self.workflow["jobs"]["prepare-wheel"]
        self.assertNotIn("permissions", prepare)

        publish = self.workflow["jobs"]["publish-wheel"]
        self.assertEqual(publish["permissions"]["id-token"], "write")
        self.assertEqual(publish["environment"]["name"], "pypi")

    def test_publishes_only_the_validated_short_lived_artifact(self) -> None:
        self.assertNotIn("PYPI_TOKEN", self.source)
        self.assertIn("pypa/gh-action-pypi-publish@", self.source)

        prepare_steps = self.workflow["jobs"]["prepare-wheel"]["steps"]
        upload_step = next(step for step in prepare_steps if step.get("name") == "Upload validated PyPI wheel")
        self.assertEqual(upload_step["with"]["retention-days"], "3")
        self.assertEqual(upload_step["with"]["path"], "${{ runner.temp }}/pypi-dist/*.whl")


if __name__ == "__main__":
    unittest.main()
