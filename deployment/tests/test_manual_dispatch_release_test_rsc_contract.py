#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import io
import tarfile
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "deployment" / "manual_dispatch_release.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("deployment_manual_dispatch_release_test_rsc_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_DISPATCH = _load_module()


class TestManualDispatchReleaseTestRscContract(unittest.TestCase):
    def test_finalize_remote_staged_dir_keeps_backup_when_cleanup_fails(self) -> None:
        captured: list[tuple[str | None, str]] = []

        def _fake_check_call_bash_with_optional_password(*, password: str | None, cmd: str) -> None:
            captured.append((password, cmd))

        with mock.patch.object(
            _DISPATCH,
            "_check_call_bash_with_optional_password",
            side_effect=_fake_check_call_bash_with_optional_password,
        ):
            _DISPATCH._finalize_remote_staged_dir(
                stage_dir_s="/remote/.fluxon_release.stage.abc123",
                dst_dir_s="/remote/fluxon_release",
                ssh_user="root",
                ip="203.0.113.7",
                ssh_port=30245,
                ssh_password=None,
            )

        self.assertEqual(len(captured), 1)
        password, cmd = captured[0]
        self.assertIsNone(password)
        self.assertIn('mv \'"\'"\'/remote/.fluxon_release.stage.abc123\'"\'"\' \'"\'"\'/remote/fluxon_release\'"\'"\'', cmd)
        self.assertIn('rm -rf "$backup" || {', cmd)
        self.assertIn("[manual_dispatch_release] warning: failed to remove old staged backup; keep it for later cleanup:", cmd)

    def test_deploy_and_profiles_dispatches_test_rsc_tree(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            release_dir = Path(td)
            (release_dir / "profiles" / "fluxon_tcp_thread").mkdir(parents=True)
            (release_dir / "test_rsc" / "fluxon_tcp_thread").mkdir(parents=True)
            (release_dir / "install.py").write_text("print('ok')\n", encoding="utf-8")
            (release_dir / "ext_images.tar.gz").write_bytes(b"tar")
            (release_dir / "ext_images" ).mkdir()
            (release_dir / "ext_images" / "ext_images.sha256").write_text("", encoding="utf-8")
            (release_dir / "wheel.whl").write_bytes(b"wheel")
            (release_dir / "profiles" / "fluxon_tcp_thread" / "fluxon_release.sha256").write_text("", encoding="utf-8")
            (release_dir / "test_rsc" / "fluxon_tcp_thread" / "fluxon_test_rsc.sha256").write_text("", encoding="utf-8")
            (release_dir / "fluxon_release.sha256").write_text(
                "a" * 64 + " ext_images.tar.gz\n" + "b" * 64 + " wheel.whl\n",
                encoding="utf-8",
            )

            relpaths = _DISPATCH._release_dispatch_relpaths(
                src_release_dir=release_dir,
                dispatch_release_scope=_DISPATCH.DISPATCH_RELEASE_SCOPE_DEPLOY_AND_PROFILES,
            )

        self.assertIn("profiles", relpaths)
        self.assertIn("test_rsc", relpaths)

    def test_test_rsc_manifest_relpaths_lists_profile_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            release_dir = Path(td)
            (release_dir / "test_rsc" / "fluxon_tcp_thread").mkdir(parents=True)
            (release_dir / "test_rsc" / "fluxon_tcp_thread" / "fluxon_test_rsc.sha256").write_text(
                "c" * 64 + " src_ci.tar.gz\n",
                encoding="utf-8",
            )

            relpaths = _DISPATCH._test_rsc_manifest_relpaths(
                src_release_dir=release_dir,
                dispatch_release_scope=_DISPATCH.DISPATCH_RELEASE_SCOPE_DEPLOY_AND_PROFILES,
            )

        self.assertEqual(relpaths, ["test_rsc/fluxon_tcp_thread/fluxon_test_rsc.sha256"])

    def test_execution_mode_local_is_treated_as_local_dispatch_node(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg_path = root / "deployconf.yaml"
            release_dir = root / "release"
            release_dir.mkdir()
            (release_dir / "install.py").write_text("print('ok')\n", encoding="utf-8")
            (release_dir / "ext_images.tar.gz").write_bytes(b"tar")
            (release_dir / "ext_images").mkdir()
            (release_dir / "ext_images" / "ext_images.sha256").write_text("", encoding="utf-8")
            (release_dir / "wheel.whl").write_bytes(b"wheel")
            (release_dir / "fluxon_release.sha256").write_text(
                "a" * 64 + " ext_images.tar.gz\n" + "b" * 64 + " wheel.whl\n",
                encoding="utf-8",
            )
            cfg_path.write_text(
                "\n".join(
                    [
                        "cluster_nodes:",
                        "  - hostname: logic-a",
                        "    ip: 127.0.0.1",
                        f"    hostworkdir: {root / 'logic-a'}",
                        "    execution_mode: local",
                        "    ssh_user: tester",
                        "    ssh_port: 22",
                        "global_envs:",
                        "  FLUXON_RELEASE_SHA256_FILE: fluxon_release.sha256",
                        "service: {}",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            stdout = io.StringIO()
            with (
                mock.patch.object(_DISPATCH, "_find_repo_root_from_script_path", return_value=REPO_ROOT),
                mock.patch.object(_DISPATCH, "_validate_release_manifest_integrity"),
                mock.patch.object(_DISPATCH, "_copy_local_release_artifact") as copy_local_release_mock,
                mock.patch.object(_DISPATCH, "_copy_local_artifact") as copy_local_mock,
                mock.patch.object(_DISPATCH, "_copy_remote_release_artifact") as copy_remote_release_mock,
                mock.patch.object(_DISPATCH, "_copy_remote_artifact") as copy_remote_mock,
                mock.patch.object(_DISPATCH.subprocess, "check_output", return_value=b"actual-host\n"),
                mock.patch.object(_DISPATCH.subprocess, "check_call") as check_call_mock,
                mock.patch.object(_DISPATCH.tempfile, "TemporaryDirectory") as tempdir_mock,
                mock.patch.object(sys, "argv", [
                    str(MODULE_PATH),
                    "-c",
                    str(cfg_path),
                    "--release-dir",
                    str(release_dir),
                ]),
                redirect_stdout(stdout),
            ):
                bare_dir = root / "bare"
                bare_tmp = root / "bare_tmp"
                bare_dir.mkdir()
                bare_tmp.mkdir()
                bare_tmpdir = mock.Mock()
                bare_tmpdir.name = str(bare_tmp)
                bare_tmpdir.cleanup = lambda: None
                tempdir_mock.return_value = bare_tmpdir
                _DISPATCH.main()

            self.assertTrue(copy_local_release_mock.called)
            self.assertTrue(copy_local_mock.called)
            self.assertFalse(copy_remote_release_mock.called)
            self.assertFalse(copy_remote_mock.called)
            mkdir_calls = [args.args for args in check_call_mock.call_args_list if args.args and args.args[0][:2] == ["bash", "-lc"]]
            self.assertTrue(mkdir_calls)

    def test_materialize_local_ext_images_reextracts_when_only_manifest_exists(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            release_dir = Path(td)
            ext_dir = release_dir / "ext_images"
            ext_dir.mkdir(parents=True)
            (ext_dir / "ext_images.sha256").write_text("a" * 64 + "  etcd/etcdctl\n", encoding="utf-8")

            tarball = release_dir / "ext_images.tar.gz"
            payload_root = release_dir / "payload"
            (payload_root / "ext_images" / "etcd").mkdir(parents=True)
            (payload_root / "ext_images" / "etcd" / "etcdctl").write_text("etcdctl\n", encoding="utf-8")
            with tarfile.open(tarball, "w:gz") as tf:
                tf.add(payload_root / "ext_images", arcname="ext_images")

            _DISPATCH._materialize_local_ext_images_from_tarball(
                dst_release_dir_s=str(release_dir),
                dst_owner="tester:tester",
            )

            self.assertTrue((release_dir / "ext_images" / "etcd" / "etcdctl").is_file())


if __name__ == "__main__":
    raise SystemExit(unittest.main())
