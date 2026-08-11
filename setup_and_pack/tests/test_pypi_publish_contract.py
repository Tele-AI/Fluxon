from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "workflow.yml"


class PyPIPublishContractTest(unittest.TestCase):
    def test_distribution_name_is_fluxon_py(self) -> None:
        completed = subprocess.run(
            [sys.executable, "setup.py", "--name"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(completed.stdout.strip(), "fluxon-py")

    def test_release_workflow_uses_trusted_publishing(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("name: Publish fluxon-py to PyPI", workflow)
        self.assertIn("name: pypi", workflow)
        self.assertIn("id-token: write", workflow)
        self.assertIn("pypa/gh-action-pypi-publish@release/v1", workflow)
        self.assertIn("cp fluxon_release/fluxon_py-*.whl dist/", workflow)
        self.assertNotIn("PYPI_API_TOKEN", workflow)


if __name__ == "__main__":
    unittest.main()
