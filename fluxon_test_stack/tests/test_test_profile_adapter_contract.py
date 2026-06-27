#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "test_profile_adapter.py"


def _load_module():
    module_dir = MODULE_PATH.parent
    sys.path.insert(0, str(module_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_profile_adapter_contract", MODULE_PATH)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(module_dir):
            sys.path.pop(0)


_ADAPTER = _load_module()


class TestTestProfileAdapterContract(unittest.TestCase):
    def test_action_collect_writes_per_instance_status_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            instances = [
                _ADAPTER._InstanceReq(
                    id="coordinator",
                    k8s_ref="deployment/coord",
                    workload_kind="Deployment",
                    workload_name="coord",
                    authority="coord",
                    target="local-node-a",
                    controller_target="controller-a",
                    node_ip="127.0.0.1",
                    lifecycle="service",
                    endpoint_scheme=None,
                    host_port=None,
                    payload_file_rel=None,
                    payload_file_abs=None,
                    payload_dest_path=None,
                ),
                _ADAPTER._InstanceReq(
                    id="node_0",
                    k8s_ref="deployment/node",
                    workload_kind="Deployment",
                    workload_name="node",
                    authority="node",
                    target="local-node-b",
                    controller_target="controller-b",
                    node_ip="127.0.0.2",
                    lifecycle="job",
                    endpoint_scheme=None,
                    host_port=None,
                    payload_file_rel=None,
                    payload_file_abs=None,
                    payload_dest_path=None,
                ),
            ]
            statuses = [
                (200, {"ok": True, "instance_id": "coordinator"}),
                (503, {"ok": False, "instance_id": "node_0"}),
            ]

            with mock.patch.object(_ADAPTER, "_http_status_allow_error", side_effect=statuses) as status_mock:
                _ADAPTER._action_collect(run_dir, "http://controller", instances)

            self.assertEqual(status_mock.call_count, 2)
            coordinator_payload = yaml.safe_load((run_dir / "logs" / "coordinator" / "status.yaml").read_text(encoding="utf-8"))
            node_payload = yaml.safe_load((run_dir / "logs" / "node_0" / "status.yaml").read_text(encoding="utf-8"))
            self.assertEqual(coordinator_payload, {"status_code": 200, "status": {"ok": True, "instance_id": "coordinator"}})
            self.assertEqual(node_payload, {"status_code": 503, "status": {"ok": False, "instance_id": "node_0"}})


if __name__ == "__main__":
    raise SystemExit(unittest.main())
