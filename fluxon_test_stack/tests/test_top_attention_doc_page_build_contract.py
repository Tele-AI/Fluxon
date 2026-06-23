#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index" / "_doc_page_build.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_top_attention_doc_page_build", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_DOC_BUILD = _load_module()


class TopAttentionDocPageBuildContractTest(unittest.TestCase):
    def test_doc_page_build_requires_docker_image_ref(self) -> None:
        with (
            mock.patch.object(
                _DOC_BUILD,
                "load_case_config",
                return_value={"doc_site_base_url": "tele-ai.github.io/Fluxon"},
            ),
            mock.patch.object(
                sys,
                "argv",
                ["_doc_page_build.py", "--case-config", "/tmp/case.yaml"],
            ),
            mock.patch.dict(os.environ, {"FLUXON_DOC_SITE_DOCKER_IMAGE_REF": ""}, clear=False),
            self.assertRaisesRegex(ValueError, "FLUXON_DOC_SITE_DOCKER_IMAGE_REF must be set"),
        ):
            _DOC_BUILD.main()

    def test_doc_page_build_always_uses_container_builder(self) -> None:
        image_ref = "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0"
        with (
            mock.patch.object(
                _DOC_BUILD,
                "load_case_config",
                return_value={"doc_site_base_url": "tele-ai.github.io/Fluxon"},
            ),
            mock.patch.object(_DOC_BUILD, "call", return_value=0) as call_mock,
            mock.patch.object(
                sys,
                "argv",
                ["_doc_page_build.py", "--python", "/usr/bin/python3", "--case-config", "/tmp/case.yaml"],
            ),
            mock.patch.dict(os.environ, {"FLUXON_DOC_SITE_DOCKER_IMAGE_REF": image_ref}, clear=False),
        ):
            self.assertEqual(_DOC_BUILD.main(), 0)

        argv = call_mock.call_args.args[0]
        env = call_mock.call_args.kwargs["env"]
        self.assertEqual(argv[0], "/usr/bin/python3")
        self.assertIn(str(REPO_ROOT / "scripts" / "build_doc_site_in_container.py"), argv)
        self.assertIn("--image-ref", argv)
        self.assertIn(image_ref, argv)
        self.assertEqual(env["FLUXON_DOC_SITE_BASE_URL"], "tele-ai.github.io/Fluxon")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
