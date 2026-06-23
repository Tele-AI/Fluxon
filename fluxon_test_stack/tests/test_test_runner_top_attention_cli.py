#!/usr/bin/env python3

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "fluxon_test_stack" / "test_runner.py"


class TestTestRunnerTopAttentionCli(unittest.TestCase):
    def run_runner(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(RUNNER), *args],
            cwd=str(REPO_ROOT),
            text=True,
            capture_output=True,
        )

    def test_top_attention_list_requires_selector(self) -> None:
        completed = self.run_runner("--action", "top_attention_list")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("--top-attention-prefix is required unless --top-attention-all is set", completed.stdout)

    def test_top_attention_list_json_all(self) -> None:
        completed = self.run_runner("--action", "top_attention_list", "--top-attention-json", "--top-attention-all")
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertGreater(payload["entry_count"], 0)
        names = {entry["name"] for entry in payload["entries"]}
        self.assertIn("_mq_core.py", names)

    def test_top_attention_list_text_prefix(self) -> None:
        completed = self.run_runner(
            "--action",
            "top_attention_list",
            "--top-attention-prefix",
            "mq",
            "--top-attention-requirements-only",
        )
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        requirements = {line.strip() for line in completed.stdout.splitlines() if line.strip()}
        self.assertIn("ci_runner", requirements)
        self.assertIn("master", requirements)

    def test_top_attention_run_requires_selector(self) -> None:
        completed = self.run_runner("--action", "top_attention_run")
        self.assertEqual(completed.returncode, 2)
        self.assertIn("--top-attention-prefix is required unless --top-attention-all is set", completed.stdout)

    def test_top_attention_run_prefix_executes_selected_entry(self) -> None:
        completed = self.run_runner(
            "--action",
            "top_attention_run",
            "--top-attention-prefix",
            "_test_requirements",
        )
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        self.assertIn("fluxon_test_stack/top_attention_test_index/_test_requirements.py", completed.stdout)
        self.assertIn("fluxon_test_stack/top_attention_test_index/test_test_requirements.py", completed.stdout)

    def test_top_attention_list_json_contains_doc_page_entry(self) -> None:
        completed = self.run_runner("--action", "top_attention_list", "--top-attention-json", "--top-attention-prefix", "doc_page")
        self.assertEqual(completed.returncode, 0, msg=completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["name"], "_doc_page_build.py")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
