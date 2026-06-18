#!/usr/bin/env python3

from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from fluxon_test_stack.top_attention_index_helper import (
    QUICK_ENTRY_NAMES,
    collect_top_attention_payload,
    iter_quick_entry_paths,
    match_top_attention_prefix,
    select_top_attention_entries,
)


class TestTopAttentionIndexHelper(unittest.TestCase):
    def test_match_prefix_accepts_token_matches(self) -> None:
        self.assertTrue(match_top_attention_prefix(Path("_relay_mq.py"), "mq"))
        self.assertTrue(match_top_attention_prefix(Path("_config_mq.py"), "_config"))
        self.assertFalse(match_top_attention_prefix(Path("_relay_mq.py"), "kv"))

    def test_select_entries_returns_known_subset(self) -> None:
        matched = select_top_attention_entries(["mq"])
        names = {path.name for path in matched}
        self.assertIn("_mq_core.py", names)
        self.assertIn("_config_mq.py", names)
        self.assertIn("_relay_mq.py", names)
        self.assertNotIn("_config_kv.py", names)

    def test_select_entries_matches_doc_page_prefix(self) -> None:
        matched = select_top_attention_entries(["doc_page"])
        names = {path.name for path in matched}
        self.assertEqual(names, {"_doc_page_build.py"})

    def test_collect_payload_reports_requirements(self) -> None:
        payload = collect_top_attention_payload(["_config_kv"])
        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["name"], "_config_kv.py")
        self.assertEqual(payload["entries"][0]["requirements"], ["ops"])
        self.assertEqual(payload["requirements"], ["ops"])

    def test_quick_entries_exist_and_match_declared_order(self) -> None:
        names = [path.name for path in iter_quick_entry_paths()]
        self.assertEqual(names, list(QUICK_ENTRY_NAMES))


if __name__ == "__main__":
    unittest.main()
