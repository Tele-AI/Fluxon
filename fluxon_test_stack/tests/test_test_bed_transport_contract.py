from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "test_bed_transport.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_bed_transport_contract", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_ENTRY = _load_module()


class TestTestBedTransportContract(unittest.TestCase):
    def test_load_manifest_transport_ctx_reads_relative_remote_auth(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bootstrap_config_path = root / "start_test_bed.yaml"
            manifest_path = root / "manifest.json"
            remote_auth_path = root / "remote_auth.yaml"

            bootstrap_config_path.write_text("deployconf_path: deployconf.yaml\n", encoding="utf-8")
            manifest_path.write_text(
                (
                    "{\n"
                    '  "controller_request_mode": "ssh_exec_per_request",\n'
                    '  "controller_url": "http://192.168.151.44:19080/r/ops/fluxon_testbed",\n'
                    '  "controller_public_url": "http://192.168.151.44:19080/r/ops/fluxon_testbed",\n'
                    '  "controller_bastion_local_url": "http://127.0.0.1:19080/r/ops/fluxon_testbed",\n'
                    '  "remote_auth_config_path": "remote_auth.yaml",\n'
                    '  "bastion": {\n'
                    '    "name": "testbed_44_46-bastion",\n'
                    '    "host": "192.168.151.44",\n'
                    '    "ssh_port": 2233\n'
                    "  }\n"
                    "}\n"
                ),
                encoding="utf-8",
            )
            remote_auth_path.write_text(
                yaml.safe_dump(
                    {
                        "bastion_user": "tester",
                        "bastion_password": "secret",
                        "controller_exec_host": "192.168.151.44",
                        "controller_exec_user": "runner",
                        "controller_exec_port": 2244,
                        "controller_exec_password": "runner-secret",
                    },
                    sort_keys=False,
                    allow_unicode=False,
                ),
                encoding="utf-8",
            )

            transport_ctx = _ENTRY.load_test_bed_manifest_transport_ctx_opt(
                bootstrap_config_path=bootstrap_config_path,
            )

            self.assertIsNotNone(transport_ctx)
            assert transport_ctx is not None
            self.assertEqual(transport_ctx["bastion_name"], "testbed_44_46-bastion")
            self.assertEqual(transport_ctx["bastion_host"], "192.168.151.44")
            self.assertEqual(transport_ctx["bastion_port"], 2233)
            self.assertEqual(transport_ctx["bastion_user"], "tester")
            self.assertEqual(transport_ctx["bastion_password"], "secret")

    def test_controller_request_via_manifest_rewrites_to_bastion_local_url(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bootstrap_config_path = root / "start_test_bed.yaml"
            manifest_path = root / "manifest.json"
            remote_auth_path = root / "remote_auth.yaml"

            bootstrap_config_path.write_text("deployconf_path: deployconf.yaml\n", encoding="utf-8")
            manifest_path.write_text(
                (
                    "{\n"
                    '  "controller_request_mode": "ssh_exec_per_request",\n'
                    '  "controller_url": "http://192.168.151.44:19080/r/ops/fluxon_testbed",\n'
                    '  "controller_public_url": "http://192.168.151.44:19080/r/ops/fluxon_testbed",\n'
                    '  "controller_bastion_local_url": "http://127.0.0.1:19080/r/ops/fluxon_testbed",\n'
                    '  "remote_auth_config_path": "remote_auth.yaml",\n'
                    '  "bastion": {\n'
                    '    "name": "testbed_44_46-bastion",\n'
                    '    "host": "192.168.151.44",\n'
                    '    "ssh_port": 2233\n'
                    "  }\n"
                    "}\n"
                ),
                encoding="utf-8",
            )
            remote_auth_path.write_text(
                yaml.safe_dump(
                    {
                        "bastion_user": "tester",
                        "bastion_password": "secret",
                        "controller_exec_host": "192.168.151.44",
                        "controller_exec_user": "runner",
                        "controller_exec_port": 2244,
                        "controller_exec_password": "runner-secret",
                    },
                    sort_keys=False,
                    allow_unicode=False,
                ),
                encoding="utf-8",
            )

            req = _ENTRY.urllib.request.Request(
                "http://192.168.151.44:19080/r/ops/fluxon_testbed/api/status?x=1",
                method="GET",
            )
            captured_argv: list[str] = []

            def fake_run(*args, **kwargs):
                del kwargs
                captured_argv.extend(args[0])
                return subprocess.CompletedProcess(
                    args=args[0],
                    returncode=0,
                    stdout=b'{"ok": true}',
                    stderr=b'{"status": 200}\n',
                )

            with mock.patch.object(_ENTRY.subprocess, "run", side_effect=fake_run):
                status_code, body = _ENTRY.controller_request_via_manifest(
                    req,
                    timeout_seconds=30.0,
                    bootstrap_config_path=bootstrap_config_path,
                )

            self.assertEqual(status_code, 200)
            self.assertEqual(body, b'{"ok": true}')
            self.assertIn("runner@192.168.151.44", captured_argv)
            self.assertIn("http://127.0.0.1:19080/r/ops/fluxon_testbed/api/status?x=1", captured_argv[-1])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
