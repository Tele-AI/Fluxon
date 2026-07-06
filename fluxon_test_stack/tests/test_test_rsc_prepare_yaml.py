#!/usr/bin/env python3

from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
PREPARE_YAML_PATH = REPO_ROOT / "fluxon_release" / "test_rsc" / "source" / "prepare.yaml"


class TestTestRscPrepareYaml(unittest.TestCase):
    def test_base_dependency_set_includes_pytest(self) -> None:
        payload = yaml.safe_load(PREPARE_YAML_PATH.read_text(encoding="utf-8"))
        self.assertIsInstance(payload, dict)
        python_runtime = payload.get("python_runtime")
        self.assertIsInstance(python_runtime, dict)
        dependency_sets = python_runtime.get("dependency_sets")
        self.assertIsInstance(dependency_sets, dict)
        base = dependency_sets.get("base")
        self.assertIsInstance(base, dict)
        requirements = base.get("requirements")
        self.assertIsInstance(requirements, list)
        pinned = {item.get("pinned") for item in requirements if isinstance(item, dict)}
        self.assertIn("pytest==8.3.5", pinned)
        self.assertIn("tomli==2.2.1", pinned)
        self.assertIn("exceptiongroup==1.3.0", pinned)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
