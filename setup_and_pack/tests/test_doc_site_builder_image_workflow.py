from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "doc-site-builder-image.yml"
ALL_TEST_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "all_test.yml"
ALL_TEST_SUITE_HELPER_PATH = REPO_ROOT / "scripts" / "ci_2_virt_node_workflow.py"
DOCS_PAGES_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "docs-pages.yml"
STANDALONE_LARGESCALE_MQ_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "largescale-mq.yml"


class DocSiteBuilderImageWorkflowTest(unittest.TestCase):
    def test_workflows_do_not_use_path_filters(self) -> None:
        for workflow_path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml")):
            workflow_text = workflow_path.read_text(encoding="utf-8")
            yaml.load(workflow_text, Loader=yaml.BaseLoader)
            self.assertNotIn("paths:", workflow_text, workflow_path.as_posix())

    def test_workflow_builds_exports_and_smokes_image_without_testbed(self) -> None:
        workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")
        yaml.load(workflow_text, Loader=yaml.BaseLoader)

        self.assertIn("setup_and_pack/build_doc_site_img.py", workflow_text)
        self.assertNotIn("packages: write", workflow_text)
        self.assertIn("--force", workflow_text)
        self.assertIn("--out \"$DOC_SITE_IMAGE_TAR\"", workflow_text)
        self.assertNotIn("DOC_SITE_REGISTRY_IMAGE_REF", workflow_text)
        self.assertNotIn("docker/login-action", workflow_text)
        self.assertNotIn("DOCKERHUB", workflow_text)
        self.assertNotIn("docker push", workflow_text)
        self.assertIn("scripts/build_doc_site_in_container.py", workflow_text)
        self.assertIn("--image-tar \"$DOC_SITE_IMAGE_TAR\"", workflow_text)
        self.assertIn("actions/upload-artifact@v4", workflow_text)
        self.assertNotIn("ci_2_virt_node.py", workflow_text)
        self.assertNotIn("fluxon_test_stack/", workflow_text)

    def test_main_testbed_workflow_builds_release_before_parallel_test_jobs(self) -> None:
        workflow_text = ALL_TEST_WORKFLOW_PATH.read_text(encoding="utf-8")
        suite_helper_text = ALL_TEST_SUITE_HELPER_PATH.read_text(encoding="utf-8")
        workflow = yaml.load(workflow_text, Loader=yaml.BaseLoader)
        jobs = workflow["jobs"]

        self.assertIn("fluxon_test_stack/ci_2_virt_node.py", workflow_text)
        self.assertEqual(
            jobs["ci-2-virt-node"]["needs"],
            "package-wheel",
        )
        self.assertEqual(
            jobs["ci-large-scale-mq"]["needs"],
            "package-wheel",
        )
        self.assertNotIn("needs", jobs["package-wheel"])
        self.assertEqual(
            jobs["codex_failure_analysis"]["needs"],
            ["package-wheel", "ci-2-virt-node", "ci-large-scale-mq"],
        )
        self.assertIn("Write test-all suite", workflow_text)
        self.assertNotIn("Write standalone large-scale MQ suite", workflow_text)
        self.assertNotIn("--suite-kind", workflow_text)
        self.assertEqual(workflow_text.count("--skip-pack"), 1)
        self.assertIn("fluxon-ci-release-${{ github.sha }}", workflow_text)
        self.assertIn("timeout --preserve-status --signal=INT 17000s", workflow_text)
        self.assertIn("test-all failed or timed out before GitHub job cancellation", workflow_text)
        self.assertNotIn("large-scale MQ failed or timed out before GitHub job cancellation", workflow_text)
        self.assertIn("rather_no_git_submodule.py", workflow_text)
        self.assertNotIn("Reclaim runner disk before CI", workflow_text)
        self.assertNotIn("/usr/local/lib/android", workflow_text)
        self.assertNotIn("/usr/share/dotnet", workflow_text)
        self.assertIn("--cleanup-pack-runtime-after-success", workflow_text)
        self.assertIn("--cleanup-successful-case-artifacts", workflow_text)
        self.assertIn("if: ${{ failure() }}", workflow_text)
        self.assertIn("ci_top_attention_cargo_kv_unit", suite_helper_text)
        self.assertIn('"kv_test_rounds": "p2p_only,p2p_only_ssd"', suite_helper_text)

    def test_docs_pages_uses_container_entrypoint(self) -> None:
        workflow_text = DOCS_PAGES_WORKFLOW_PATH.read_text(encoding="utf-8")
        yaml.load(workflow_text, Loader=yaml.BaseLoader)

        self.assertIn("DOC_SITE_IMAGE_REF: hanbaoaaa/fluxon-doc-site-builder", workflow_text)
        self.assertIn("scripts/build_doc_site_in_container.py", workflow_text)
        self.assertIn("--image-ref \"$DOC_SITE_IMAGE_REF\"", workflow_text)
        self.assertNotIn("actions/setup-node", workflow_text)
        self.assertNotIn("doc-site-npm", workflow_text)
        self.assertNotIn("doc-site-plugins", workflow_text)

    def test_largescale_mq_is_a_dedicated_job_in_main_dag(self) -> None:
        self.assertFalse(STANDALONE_LARGESCALE_MQ_WORKFLOW_PATH.exists())
        workflow_text = ALL_TEST_WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn("ci-large-scale-mq:", workflow_text)
        large_job = workflow_text.split("  ci-large-scale-mq:", 1)[1].split(
            "  codex_failure_analysis:",
            1,
        )[0]
        self.assertIn(
            "fluxon_test_stack/top_attention_test_index/_largescale_mq.py",
            large_job,
        )
        self.assertIn("Install packaged Fluxon wheel", large_job)
        self.assertNotIn("ci_2_virt_node.py", large_job)
        self.assertNotIn("test_runner.py", large_job)
        self.assertNotIn("start_test_bed", large_job)


if __name__ == "__main__":
    unittest.main()
