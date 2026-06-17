#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_pack_test_stack_rsc_top_attention", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PACK = _load_module()


class TestPackTestStackRscTopAttentionContract(unittest.TestCase):
    def test_ci_source_tarball_is_repo_root_scoped(self) -> None:
        self.assertEqual(_PACK.CI_SOURCE_ROOT_NAMES, (".",))


if __name__ == "__main__":
    raise SystemExit(unittest.main())
