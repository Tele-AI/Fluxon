from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "doc-site-builder-image.yml"
ALL_TEST_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "all_test.yml"
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

    def test_main_testbed_workflow_keeps_suite_generation_in_workflow(self) -> None:
        workflow_text = ALL_TEST_WORKFLOW_PATH.read_text(encoding="utf-8")
        yaml.load(workflow_text, Loader=yaml.BaseLoader)

        self.assertIn("fluxon_test_stack/ci_2_virt_node.py", workflow_text)
        self.assertIn("Write ci_2_virt_node suite", workflow_text)
        self.assertIn("timeout --preserve-status --signal=INT 17000s", workflow_text)
        self.assertIn("ci_2_virt_node failed or timed out before GitHub job cancellation", workflow_text)
        self.assertIn("ci_top_attention_bin_kvtest", workflow_text)
        self.assertIn("ci_top_attention_doc_page_build", workflow_text)
        self.assertIn("ci_top_attention_mq_core", workflow_text)
        self.assertIn("ci_top_attention_largescale_mq", workflow_text)
        self.assertIn("_{suffix}.py", workflow_text)
        self.assertIn("--single-host-logical-targets", workflow_text)
        self.assertIn("--testbed-bundle-source", workflow_text)
        self.assertIn("__TEST_BED_BUNDLE_ROOT__", workflow_text)
        self.assertIn("largescale_mq_ci_single_host", workflow_text)
        self.assertIn("--owner-count", workflow_text)
        self.assertIn('"4"', workflow_text)
        self.assertIn("--threads-per-process", workflow_text)
        self.assertNotIn('"timeout_seconds": 3600', workflow_text)
        self.assertIn("--value-size", workflow_text)
        self.assertIn('"256"', workflow_text)
        self.assertIn("for producer_count, consumer_count in ((8, 8), (32, 32), (160, 8))", workflow_text)
        self.assertIn('"--duration-seconds",\n                              "90"', workflow_text)
        self.assertIn('"--metric-warmup-seconds",\n                              "60"', workflow_text)
        self.assertIn("nested largescale MQ diagnostics", workflow_text)
        self.assertIn("nested largescale MQ failed run diagnostics", workflow_text)
        self.assertIn("logs/ci_runner/restart_count.txt", workflow_text)
        self.assertIn("logs/ci_runner/inflight_attempt.txt", workflow_text)
        self.assertIn("benchmark_result.json", workflow_text)
        self.assertIn("deploy_result.yaml", workflow_text)
        self.assertNotIn("Print ci_2_virt_node failure summary", workflow_text)
        self.assertIn("doc_site_base_url", workflow_text)
        self.assertIn("rather_no_git_submodule.py", workflow_text)

    def test_docs_pages_uses_container_entrypoint(self) -> None:
        workflow_text = DOCS_PAGES_WORKFLOW_PATH.read_text(encoding="utf-8")
        yaml.load(workflow_text, Loader=yaml.BaseLoader)

        self.assertIn("DOC_SITE_IMAGE_REF: hanbaoaaa/fluxon-doc-site-builder", workflow_text)
        self.assertIn("scripts/build_doc_site_in_container.py", workflow_text)
        self.assertIn("--image-ref \"$DOC_SITE_IMAGE_REF\"", workflow_text)
        self.assertNotIn("actions/setup-node", workflow_text)
        self.assertNotIn("doc-site-npm", workflow_text)
        self.assertNotIn("doc-site-plugins", workflow_text)

    def test_largescale_mq_only_uses_main_testbed_workflow(self) -> None:
        self.assertFalse(STANDALONE_LARGESCALE_MQ_WORKFLOW_PATH.exists())
        for workflow_path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml")):
            if workflow_path == ALL_TEST_WORKFLOW_PATH:
                continue
            workflow_text = workflow_path.read_text(encoding="utf-8")
            self.assertNotIn("ci_top_attention_largescale_mq", workflow_text, workflow_path.as_posix())
            self.assertNotIn("top_attention_test_index/_largescale_mq.py", workflow_text, workflow_path.as_posix())


if __name__ == "__main__":
    unittest.main()
