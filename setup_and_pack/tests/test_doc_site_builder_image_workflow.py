from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "doc-site-builder-image.yml"
ALL_TEST_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "all_test.yml"
DOCS_PAGES_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "docs-pages.yml"


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
        self.assertIn("ci_top_attention_bin_kvtest", workflow_text)
        self.assertIn("ci_top_attention_doc_page_build", workflow_text)
        self.assertIn("ci_top_attention_mq_core", workflow_text)
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


if __name__ == "__main__":
    unittest.main()
