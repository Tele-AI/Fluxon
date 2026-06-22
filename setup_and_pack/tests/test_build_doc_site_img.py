from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "setup_and_pack" / "build_doc_site_img.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_build_doc_site_img", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_IMG = _load_module()


class BuildDocSiteImgTest(unittest.TestCase):
    def test_doc_site_builder_config_uses_manylinux_and_bootstraps_quartz(self) -> None:
        config_text = _IMG.DEFAULT_CONFIG_PATH.read_text(encoding="utf-8")

        self.assertIn("base_image: quay.io/pypa/manylinux_2_28_x86_64", config_text)
        self.assertIn("image_name: fluxon-doc-site-builder", config_text)
        self.assertIn("image_tag: quartz-v5.0.0-node-v24.16.0", config_text)
        self.assertIn("FLUXON_DOC_SITE_CACHE_ROOT=/opt/fluxon_doc_site_cache", config_text)
        self.assertIn("build_doc_site.py bootstrap", config_text)
        self.assertIn("-name node_modules", config_text)
        self.assertNotIn("-name .git", config_text)

    def test_cache_ready_requires_archive_and_matching_stamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "doc_site_builder_image.tar"
            digest = "abc123"

            self.assertFalse(_IMG._cache_ready(out_path=out_path, expected_digest=digest))

            out_path.write_text("image\n", encoding="utf-8")
            self.assertFalse(_IMG._cache_ready(out_path=out_path, expected_digest=digest))

            _IMG._stamp_path(out_path).write_text(digest + "\n", encoding="utf-8")
            self.assertTrue(_IMG._cache_ready(out_path=out_path, expected_digest=digest))

            _IMG._stamp_path(out_path).write_text("stale\n", encoding="utf-8")
            self.assertFalse(_IMG._cache_ready(out_path=out_path, expected_digest=digest))

    def test_main_reuses_cached_archive_without_docker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "doc_site_builder_image.tar"
            out_path.write_text("image\n", encoding="utf-8")
            digest = "digest"
            _IMG._stamp_path(out_path).write_text(digest + "\n", encoding="utf-8")

            with (
                mock.patch.object(_IMG, "_parse_args") as parse_args,
                mock.patch.object(_IMG, "_input_digest", return_value=digest),
                mock.patch.object(_IMG, "_ensure_docker_available") as ensure_docker_available,
                mock.patch.object(_IMG, "build_docker_image_from_config") as build_image,
                mock.patch.object(_IMG, "_export_image") as export_image,
            ):
                parse_args.return_value = mock.Mock(
                    config=_IMG.DEFAULT_CONFIG_PATH,
                    out=out_path,
                    force=False,
                )
                rc = _IMG.main()

            self.assertEqual(rc, 0)
            ensure_docker_available.assert_not_called()
            build_image.assert_not_called()
            export_image.assert_not_called()

    def test_main_builds_and_exports_image_when_cache_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "doc_site_builder_image.tar"
            digest = "digest"

            with (
                mock.patch.object(_IMG, "_parse_args") as parse_args,
                mock.patch.object(_IMG, "_input_digest", return_value=digest),
                mock.patch.object(_IMG, "_ensure_docker_available") as ensure_docker_available,
                mock.patch.object(_IMG, "build_docker_image_from_config", return_value="doc:latest") as build_image,
                mock.patch.object(_IMG, "_export_image") as export_image,
                mock.patch.object(_IMG.os, "chmod") as chmod,
            ):
                parse_args.return_value = mock.Mock(
                    config=_IMG.DEFAULT_CONFIG_PATH,
                    out=out_path,
                    force=False,
                )
                rc = _IMG.main()

            self.assertEqual(rc, 0)
            ensure_docker_available.assert_called_once_with()
            build_image.assert_called_once_with(_IMG.REPO_ROOT, _IMG.DEFAULT_CONFIG_PATH.resolve())
            export_image.assert_called_once_with(image_ref="doc:latest", out_path=out_path.resolve())
            self.assertEqual(_IMG._stamp_path(out_path).read_text(encoding="utf-8"), digest + "\n")
            chmod.assert_any_call(out_path.resolve(), 0o666)
            chmod.assert_any_call(_IMG._stamp_path(out_path.resolve()), 0o666)

    def test_export_image_uses_docker_save_and_host_chmod(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "out" / "doc_site_builder_image.tar"
            calls: list[list[str]] = []

            def fake_check_call(argv):
                calls.append(list(argv))
                if "save" in argv:
                    tmp_path = Path(argv[argv.index("-o") + 1])
                    tmp_path.write_text("image\n", encoding="utf-8")

            with (
                mock.patch.object(_IMG, "sudo_prefix", return_value=["sudo"]),
                mock.patch.object(_IMG, "host_sudo_prefix", return_value=[]),
                mock.patch.object(_IMG.subprocess, "check_call", side_effect=fake_check_call),
            ):
                _IMG._export_image(image_ref="doc:latest", out_path=out_path)

            self.assertEqual(
                calls,
                [
                    ["sudo", "docker", "save", "-o", str(out_path.with_name(out_path.name + ".tmp")), "doc:latest"],
                    ["chmod", "666", str(out_path.with_name(out_path.name + ".tmp"))],
                ],
            )
            self.assertEqual(out_path.read_text(encoding="utf-8"), "image\n")


if __name__ == "__main__":
    unittest.main()
