from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
ALL_TEST_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "all_test.yml"
CREATE_TAG_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "create-release-tag.yml"
RELEASE_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "manual-release.yml"
RELEASE_PROMPT_PATH = REPO_ROOT / ".github" / "codex" / "release-readiness-prompt.md"


class ReleaseWorkflowTest(unittest.TestCase):
    def setUp(self) -> None:
        self.create_tag_source = CREATE_TAG_WORKFLOW_PATH.read_text(encoding="utf-8")
        self.create_tag = yaml.load(self.create_tag_source, Loader=yaml.BaseLoader)
        self.release_source = RELEASE_WORKFLOW_PATH.read_text(encoding="utf-8")
        self.release = yaml.load(self.release_source, Loader=yaml.BaseLoader)
        self.all_test_source = ALL_TEST_WORKFLOW_PATH.read_text(encoding="utf-8")

    def test_tag_workflow_is_the_only_manual_release_entrypoint(self) -> None:
        trigger = self.create_tag["on"]
        self.assertEqual(list(trigger), ["workflow_dispatch"])
        self.assertEqual(trigger["workflow_dispatch"], "")
        self.assertEqual(self.create_tag["permissions"], {"actions": "write", "contents": "write"})

        self.assertEqual(list(self.release["on"]), ["workflow_run"])
        self.assertFalse((REPO_ROOT / ".github" / "workflows" / "release.yml").exists())
        self.assertFalse((REPO_ROOT / ".github" / "workflows" / "publish-pypi.yml").exists())
        self.assertNotIn("git tag", self.create_tag_source)
        self.assertNotIn("git push", self.create_tag_source)
        self.assertNotIn("${{ inputs.", self.create_tag_source)

    def test_tag_workflow_validates_exact_default_branch_commit(self) -> None:
        self.assertIn('"$GITHUB_REF" != "refs/heads/$DEFAULT_BRANCH"', self.create_tag_source)
        self.assertIn(
            'fluxon_release/resolve_release_meta.py --github-output "$GITHUB_OUTPUT"',
            self.create_tag_source,
        )
        self.assertIn("steps.release_meta.outputs.release_tag", self.create_tag_source)
        self.assertIn('test -f "$RELEASE_NOTES_FILE"', self.create_tag_source)
        self.assertIn("actions/workflows/all_test.yml/runs", self.create_tag_source)
        self.assertIn(".head_branch == $branch and .head_sha == $sha", self.create_tag_source)

    def test_tag_is_created_and_ci_is_dispatched_with_github_token(self) -> None:
        self.assertNotIn("secrets.", self.create_tag_source)
        self.assertNotIn("vars.", self.create_tag_source)
        self.assertEqual(self.create_tag_source.count("GH_TOKEN: ${{ github.token }}"), 3)
        self.assertIn('"repos/$GITHUB_REPOSITORY/git/refs"', self.create_tag_source)
        self.assertIn('gh workflow run all_test.yml --ref "$RELEASE_TAG"', self.create_tag_source)

    def test_unified_release_waits_for_successful_tag_ci(self) -> None:
        trigger = self.release["on"]["workflow_run"]
        self.assertEqual(trigger["workflows"], ["ci_2_virt_node"])
        self.assertEqual(trigger["types"], ["completed"])

        verify = self.release["jobs"]["verify-release"]
        condition = verify["if"]
        self.assertIn("workflow_run.event == 'workflow_dispatch'", condition)
        self.assertIn("workflow_run.conclusion == 'success'", condition)
        self.assertIn("workflow_run.actor.type == 'Bot'", condition)
        self.assertIn("workflow_run.head_repository.full_name == github.repository", condition)
        self.assertIn("workflow_run.path == '.github/workflows/all_test.yml'", condition)
        self.assertIn("startsWith(github.event.workflow_run.head_branch, 'v')", condition)
        self.assertIn("Tag/CI commit mismatch", self.release_source)
        self.assertIn("Tagged commit is not in the default-branch history", self.release_source)

    def test_release_paths_require_exact_tag_ci_provenance(self) -> None:
        self.assertIn("release_ci_provenance.py write", self.all_test_source)
        all_test = yaml.load(self.all_test_source, Loader=yaml.BaseLoader)
        self.assertEqual(all_test["on"]["push"]["branches"], ["**"])
        self.assertIn("workflow_dispatch", all_test["on"])
        self.assertIn(
            "github.event_name == 'push' || github.event_name == 'workflow_dispatch'",
            self.all_test_source,
        )
        self.assertIn("release-ci-provenance-${{ github.sha }}", self.all_test_source)
        self.assertGreaterEqual(self.release_source.count("release_ci_provenance.py validate-tag"), 2)
        self.assertIn('--expected-sha "$SOURCE_SHA"', self.release_source)
        self.assertIn('--expected-tag "$RELEASE_TAG"', self.release_source)

    def test_three_publications_are_parallel_after_one_review(self) -> None:
        jobs = self.release["jobs"]
        publish_jobs = {
            "publish-github-release",
            "publish-pypi",
            "publish-docker-image",
        }
        self.assertTrue(publish_jobs.issubset(jobs))
        for job_name in publish_jobs:
            needs = set(jobs[job_name]["needs"])
            self.assertIn("verify-release", needs)
            self.assertIn("release-readiness-review", needs)
            self.assertTrue(needs.isdisjoint(publish_jobs))

        self.assertEqual(jobs["publish-github-release"]["environment"]["name"], "github-release")
        self.assertEqual(jobs["publish-pypi"]["environment"]["name"], "pypi")
        self.assertEqual(jobs["publish-docker-image"]["environment"]["name"], "docker-image")

    def test_destination_credentials_and_permissions_are_scoped(self) -> None:
        jobs = self.release["jobs"]
        self.assertEqual(self.release["permissions"], {"actions": "read", "contents": "read"})
        self.assertEqual(jobs["publish-github-release"]["permissions"], {"contents": "write"})
        self.assertEqual(jobs["publish-pypi"]["permissions"]["id-token"], "write")
        self.assertNotIn("id-token", self.release["permissions"])
        self.assertNotIn("PYPI_TOKEN", self.release_source)

        self.assertIn("DOCKERHUB_USERNAME", self.release_source)
        self.assertIn("DOCKERHUB_TOKEN", self.release_source)
        self.assertIn("hanbaoaaa/fluxon_quick_start", self.release_source)
        self.assertIn("docker load --input", self.release_source)
        self.assertIn('docker push "$target_image"', self.release_source)

    def test_pypi_publishes_only_the_validated_short_lived_artifact(self) -> None:
        jobs = self.release["jobs"]
        prepare_steps = jobs["prepare-pypi-wheel"]["steps"]
        upload_step = next(step for step in prepare_steps if step.get("name") == "Upload validated PyPI wheel")
        self.assertEqual(upload_step["with"]["retention-days"], "3")
        self.assertEqual(upload_step["with"]["path"], "${{ runner.temp }}/pypi-dist/*.whl")
        self.assertIn("pypa/gh-action-pypi-publish@", self.release_source)

    def test_release_review_combines_all_artifacts_and_read_only_codex(self) -> None:
        review = self.release["jobs"]["release-readiness-review"]
        self.assertEqual(
            review["needs"],
            ["verify-release", "pack-release", "prepare-pypi-wheel"],
        )
        self.assertEqual(review["environment"], "OPENAI_API_KEY")
        self.assertIn("sha256sum --check fluxon_release.sha256", self.release_source)
        self.assertIn("pypi-wheel.sha256", self.release_source)
        self.assertIn("tag-ci-run.json", self.release_source)
        self.assertIn("tag-ci-jobs.json", self.release_source)
        self.assertIn("openai/codex-action@", self.release_source)
        self.assertIn("prompt-file: .github/codex/release-readiness-prompt.md", self.release_source)
        self.assertIn('permission-profile: ":read-only"', self.release_source)

        prompt = RELEASE_PROMPT_PATH.read_text(encoding="utf-8")
        self.assertIn("结论：可进入人工审核", prompt)
        self.assertIn("结论：阻止发布", prompt)
        self.assertIn("不替代确定性门禁或 required reviewers", prompt)


if __name__ == "__main__":
    unittest.main()
