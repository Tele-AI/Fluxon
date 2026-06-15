#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_doc_site.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_build_doc_site_contract", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_DOC_SITE = _load_module()


class TestBuildDocSiteContract(unittest.TestCase):
    def test_counterpart_routes_cover_home_and_doc_pairs(self) -> None:
        routes = _DOC_SITE.LANGUAGE_COUNTERPART_ROUTES
        self.assertEqual(routes["/"], "/cn")
        self.assertEqual(routes["/cn"], "/")
        self.assertEqual(
            routes["/user_doc/User---0---Installation"],
            "/cn/user_doc/用户---0---安装",
        )
        self.assertEqual(
            routes["/cn/dev_doc/开发者---2---打包中间件和镜像"],
            "/dev_doc/Developer---2---Package-Middleware-and-Images",
        )

    def test_rewrite_homepage_target_path_for_english_home(self) -> None:
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./README_CN.md", language="en"),
            "./cn/",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_doc_en/user_doc/", language="en"),
            "./user_doc/",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_doc_cn/user_doc/", language="en"),
            "./cn/user_doc/",
        )

    def test_rewrite_homepage_target_path_for_chinese_home(self) -> None:
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./README.md", language="cn"),
            "../",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_doc_cn/user_doc/", language="cn"),
            "./user_doc/",
        )
        self.assertEqual(
            _DOC_SITE.rewrite_homepage_target_path("./fluxon_rs/rust-toolchain.toml", language="cn"),
            "../fluxon_rs/rust-toolchain.toml",
        )


if __name__ == "__main__":
    unittest.main()
