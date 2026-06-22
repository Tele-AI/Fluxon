from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "setup_and_pack" / "nix" / "build_doc_site_in_container.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_build_doc_site_in_container", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_CONTAINER = _load_module()


class BuildDocSiteInContainerTest(unittest.TestCase):
    def test_load_image_if_requested_uses_docker_load(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            image_tar = repo_root / "doc_site_builder_image.tar"
            image_tar.write_text("image\n", encoding="utf-8")

            with mock.patch.object(_CONTAINER, "_docker") as docker:
                _CONTAINER._load_image_if_requested(
                    image_tar=Path("doc_site_builder_image.tar"),
                    repo_root=repo_root,
                )

            docker.assert_called_once_with("load", "-i", str(image_tar.resolve()))

    def test_load_image_if_requested_rejects_missing_archive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(RuntimeError, "image archive is missing"):
                _CONTAINER._load_image_if_requested(
                    image_tar=Path("missing.tar"),
                    repo_root=Path(tmpdir),
                )

    def test_run_build_invokes_docker_run_with_repo_mount_and_cache_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            script_path = repo_root / "scripts" / "build_doc_site.py"
            script_path.parent.mkdir(parents=True)
            script_path.write_text("#!/usr/bin/env python3\n", encoding="utf-8")

            with (
                mock.patch.object(_CONTAINER, "sudo_prefix", return_value=["sudo"]),
                mock.patch.object(_CONTAINER.subprocess, "check_call") as check_call,
            ):
                _CONTAINER._run_build(
                    repo_root=repo_root,
                    image_ref="doc:latest",
                    base_url="tele-ai.github.io/Fluxon",
                )

            argv = check_call.call_args.args[0]
            self.assertEqual(argv[:3], ["sudo", "docker", "run"])
            self.assertIn("--rm", argv)
            self.assertIn("FLUXON_DOC_SITE_BASE_URL=tele-ai.github.io/Fluxon", argv)
            self.assertIn(
                f"FLUXON_DOC_SITE_CACHE_ROOT={_CONTAINER.CONTAINER_CACHE_ROOT}",
                argv,
            )
            self.assertIn(f"{repo_root.resolve()}:/workspace", argv)
            self.assertIn("/workspace", argv)
            self.assertIn("doc:latest", argv)
            command = argv[-1]
            self.assertIn("python3 scripts/build_doc_site.py build", command)
            self.assertIn("chmod -R a+rwX fluxon_release/doc_site", command)

    def test_run_build_rejects_repo_without_doc_site_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(RuntimeError, "missing scripts/build_doc_site.py"):
                _CONTAINER._run_build(
                    repo_root=Path(tmpdir),
                    image_ref="doc:latest",
                    base_url="tele-ai.github.io/Fluxon",
                )


if __name__ == "__main__":
    unittest.main()
