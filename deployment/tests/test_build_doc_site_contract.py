#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


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
    def test_default_track_poll_seconds(self) -> None:
        self.assertEqual(_DOC_SITE.DEFAULT_TRACK_POLL_SECONDS, 30.0)

    def test_main_defaults_to_track(self) -> None:
        with mock.patch.object(sys, "argv", [str(SCRIPT_PATH)]):
            with mock.patch.object(_DOC_SITE, "track_site", return_value=123) as track_site:
                rc = _DOC_SITE.main()
        self.assertEqual(rc, 123)
        track_site.assert_called_once_with(_DOC_SITE.DEFAULT_SERVE_ADDR, 30.0)

    def test_explorer_priority_root_routes(self) -> None:
        self.assertEqual(
            _DOC_SITE.EXPLORER_PRIORITY_ROOT_ROUTES["en"],
            ("/dev_doc", "/user_doc"),
        )
        self.assertEqual(
            _DOC_SITE.EXPLORER_PRIORITY_ROOT_ROUTES["cn"],
            ("/cn/dev_doc", "/cn/user_doc", "/cn/design"),
        )

    def test_explorer_hidden_route_prefixes(self) -> None:
        self.assertEqual(_DOC_SITE.EXPLORER_HIDDEN_ROUTE_PREFIXES["en"], ("/design",))
        self.assertEqual(_DOC_SITE.EXPLORER_HIDDEN_ROUTE_PREFIXES["cn"], ())

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
        self.assertEqual(routes["/design"], "/cn/design")
        self.assertEqual(routes["/cn/design"], "/design")
        self.assertEqual(
            routes["/design/python_rust_零拷贝参数传递链路设计实现"],
            "/cn/design/python_rust_零拷贝参数传递链路设计实现",
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

    def test_stage_cn_only_design_into_en_root_when_english_design_missing(self) -> None:
        tmpdir = Path(tempfile.mkdtemp(prefix="fluxon_doc_site_contract_"))
        old_stage_root = _DOC_SITE.STAGE_DOCS_ROOT
        try:
            _DOC_SITE.STAGE_DOCS_ROOT = tmpdir / "content"
            _DOC_SITE.ensure_dir(_DOC_SITE.STAGE_DOCS_ROOT)

            _DOC_SITE.stage_cn_only_design_into_en_root()

            staged_design_root = _DOC_SITE.STAGE_DOCS_ROOT / "design"
            self.assertTrue(staged_design_root.is_dir())
            self.assertTrue(
                (staged_design_root / "python_rust_零拷贝参数传递链路设计实现.md").is_file()
            )
            self.assertTrue((staged_design_root / "kv_1_概览与分层.md").is_file())
        finally:
            _DOC_SITE.STAGE_DOCS_ROOT = old_stage_root
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_build_site_replaces_symlink_output_root(self) -> None:
        tmpdir = Path(tempfile.mkdtemp(prefix="fluxon_doc_site_symlink_"))
        old_output_root = _DOC_SITE.OUTPUT_ROOT
        try:
            real_output = tmpdir / "real_output"
            real_output.mkdir(parents=True, exist_ok=True)
            (real_output / "stale.txt").write_text("stale", encoding="utf-8")

            symlink_output = tmpdir / "doc_site"
            symlink_output.symlink_to(real_output, target_is_directory=True)
            self.assertTrue(symlink_output.is_symlink())

            _DOC_SITE.OUTPUT_ROOT = symlink_output

            with mock.patch.object(_DOC_SITE, "bootstrap_toolchain") as bootstrap_toolchain:
                with mock.patch.object(_DOC_SITE, "reset_staged_docs") as reset_staged_docs:
                    with mock.patch.object(_DOC_SITE, "stage_source_docs") as stage_source_docs:
                        with mock.patch.object(_DOC_SITE, "run_quartz_build") as run_quartz_build:
                            rc = _DOC_SITE.build_site()

            self.assertEqual(rc, 0)
            bootstrap_toolchain.assert_called_once_with()
            reset_staged_docs.assert_called_once_with()
            stage_source_docs.assert_called_once_with()
            run_quartz_build.assert_called_once_with()
            self.assertTrue(_DOC_SITE.OUTPUT_ROOT.is_dir())
            self.assertFalse(_DOC_SITE.OUTPUT_ROOT.is_symlink())
            self.assertTrue(real_output.exists())
            self.assertTrue((real_output / "stale.txt").is_file())
        finally:
            _DOC_SITE.OUTPUT_ROOT = old_output_root
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_ensure_quartz_plugins_rebuilds_stale_plugin_tree(self) -> None:
        tmpdir = Path(tempfile.mkdtemp(prefix="fluxon_doc_site_plugins_"))
        old_toolchain_root = _DOC_SITE.TOOLCHAIN_ROOT
        old_runtime_config_path = _DOC_SITE.RUNTIME_CONFIG_PATH
        old_runtime_lockfile_path = _DOC_SITE.RUNTIME_LOCKFILE_PATH
        old_plugin_stamp_path = _DOC_SITE.PLUGIN_STAMP_PATH
        try:
            _DOC_SITE.TOOLCHAIN_ROOT = tmpdir / "toolchain"
            _DOC_SITE.RUNTIME_CONFIG_PATH = _DOC_SITE.TOOLCHAIN_ROOT / "quartz.config.yaml"
            _DOC_SITE.RUNTIME_LOCKFILE_PATH = _DOC_SITE.TOOLCHAIN_ROOT / "quartz.lock.json"
            _DOC_SITE.PLUGIN_STAMP_PATH = _DOC_SITE.TOOLCHAIN_ROOT / ".fluxon-plugin-stamp"

            plugins_root = _DOC_SITE.TOOLCHAIN_ROOT / ".quartz" / "plugins"
            stale_plugin_dir = plugins_root / "search"
            stale_plugin_dir.mkdir(parents=True, exist_ok=True)
            (stale_plugin_dir / "package.json").write_text("{}", encoding="utf-8")
            _DOC_SITE.RUNTIME_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            _DOC_SITE.RUNTIME_CONFIG_PATH.write_text("config", encoding="utf-8")
            _DOC_SITE.RUNTIME_LOCKFILE_PATH.write_text("lock", encoding="utf-8")
            _DOC_SITE.PLUGIN_STAMP_PATH.write_text("stale", encoding="utf-8")

            with mock.patch.object(_DOC_SITE, "require_binary", side_effect=lambda name: name):
                with mock.patch.object(_DOC_SITE, "run_cmd") as run_cmd:
                    _DOC_SITE.ensure_quartz_plugins()

            self.assertFalse(stale_plugin_dir.exists())
            self.assertTrue(_DOC_SITE.PLUGIN_STAMP_PATH.is_file())
            self.assertEqual(run_cmd.call_count, 2)
            self.assertEqual(
                run_cmd.call_args_list[0].kwargs["cwd"],
                _DOC_SITE.TOOLCHAIN_ROOT,
            )
            self.assertEqual(
                run_cmd.call_args_list[1].args[0][-2:],
                ["plugin", "install"],
            )
        finally:
            _DOC_SITE.TOOLCHAIN_ROOT = old_toolchain_root
            _DOC_SITE.RUNTIME_CONFIG_PATH = old_runtime_config_path
            _DOC_SITE.RUNTIME_LOCKFILE_PATH = old_runtime_lockfile_path
            _DOC_SITE.PLUGIN_STAMP_PATH = old_plugin_stamp_path
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
