#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
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
    def test_action_deploy_waits_for_service_instances_before_ready(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            run_dir = Path(td)
            (run_dir / "deployer_deploy.yaml").write_text("apiVersion: v1\nkind: List\n", encoding="utf-8")
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
                    id="producer_0_proc_0",
                    k8s_ref="deployment/producer",
                    workload_kind="Deployment",
                    workload_name="producer",
                    authority="producer",
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

            with mock.patch.object(_ADAPTER, "_preflight_ops_agents"):
                with mock.patch.object(_ADAPTER, "_http_deploy", return_value={"history_id": "hist-1"}):
                    with mock.patch.object(_ADAPTER, "_wait_running") as wait_running:
                        _ADAPTER._action_deploy(
                            run_dir,
                            run_dir,
                            {},
                            "http://controller",
                            instances,
                            None,
                            30,
                        )

            self.assertEqual(
                wait_running.call_args_list,
                [
                    mock.call("http://controller", "controller-a", "Deployment", "coord", "coord"),
                ],
            )
            deploy_result = yaml.safe_load((run_dir / "deploy_result.yaml").read_text(encoding="utf-8"))
            self.assertTrue(deploy_result["ready"])
            self.assertEqual(deploy_result["service_wait_count"], 1)
            self.assertEqual(deploy_result["job_status_wait_skipped_count"], 1)

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

    def test_wait_running_treats_transport_errors_as_wait_observations(self) -> None:
        with mock.patch.object(
            _ADAPTER,
            "_http_status_allow_error",
            side_effect=[
                (0, {"ok": False, "err": "TimeoutError: timed out"}),
                (200, {"ok": True, "running": True}),
            ],
        ) as status_mock:
            with mock.patch.object(_ADAPTER.time, "sleep") as sleep_mock:
                _ADAPTER._wait_running("http://controller", "node-a", "Deployment", "worker", "worker")

        self.assertEqual(status_mock.call_count, 2)
        sleep_mock.assert_called_once_with(1.0)

    def test_action_collect_records_status_transport_error_without_failing(self) -> None:
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
            ]

            with mock.patch.object(
                _ADAPTER,
                "_http_status_allow_error",
                return_value=(0, {"ok": False, "err": "TimeoutError: timed out"}),
            ):
                with mock.patch.object(_ADAPTER, "_http_workload_log_allow_error", return_value=(0, {"ok": False})):
                    _ADAPTER._action_collect(run_dir, "http://controller", instances)

            status_payload = yaml.safe_load((run_dir / "logs" / "coordinator" / "status.yaml").read_text(encoding="utf-8"))
            self.assertEqual(status_payload["status_code"], 0)
            self.assertEqual(status_payload["status"]["err"], "TimeoutError: timed out")

    def test_action_collect_writes_workload_log_tails(self) -> None:
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

            with mock.patch.object(
                _ADAPTER,
                "_http_status_allow_error",
                side_effect=[
                    (200, {"ok": True, "instance_id": "coordinator"}),
                    (503, {"ok": False, "instance_id": "node_0"}),
                ],
            ):
                with mock.patch.object(
                    _ADAPTER,
                    "_http_workload_log_allow_error",
                    side_effect=[
                        (200, {"ok": True, "text": "coord tail\n"}),
                        (400, {"ok": False, "err": "log file is not available yet"}),
                    ],
                ) as log_mock:
                    _ADAPTER._action_collect(run_dir, "http://controller", instances)

            self.assertEqual(
                log_mock.call_args_list,
                [
                    mock.call(
                        "http://controller",
                        instance_key="fluxon_ops_controller-a",
                        kind="Deployment",
                        name="coord",
                        max_bytes=_ADAPTER._COLLECT_WORKLOAD_LOG_TAIL_BYTES,
                    ),
                    mock.call(
                        "http://controller",
                        instance_key="fluxon_ops_controller-b",
                        kind="Deployment",
                        name="node",
                        max_bytes=_ADAPTER._COLLECT_WORKLOAD_LOG_TAIL_BYTES,
                    ),
                ],
            )
            coord_tail = (run_dir / "logs" / "coordinator" / "workload_log_tail.txt").read_text(encoding="utf-8")
            self.assertEqual(coord_tail, "coord tail\n")
            coord_diag = json.loads(
                (run_dir / "logs" / "coordinator" / "workload_log_tail.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(coord_diag["request"]["direction"], "Forward")
            node_diag = json.loads((run_dir / "logs" / "node_0" / "workload_log_tail.json").read_text(encoding="utf-8"))
            self.assertEqual(node_diag["status_code"], 400)
            self.assertEqual(node_diag["response"]["err"], "log file is not available yet")

    def test_action_collect_records_workload_log_exception_without_failing(self) -> None:
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
            ]

            with mock.patch.object(_ADAPTER, "_http_status_allow_error", return_value=(200, {"ok": True})):
                with mock.patch.object(
                    _ADAPTER,
                    "_http_workload_log_allow_error",
                    side_effect=TimeoutError("controller timeout"),
                ):
                    _ADAPTER._action_collect(run_dir, "http://controller", instances)

            diag = json.loads((run_dir / "logs" / "coordinator" / "workload_log_tail.json").read_text(encoding="utf-8"))
            self.assertIsNone(diag["status_code"])
            self.assertIn("TimeoutError: controller timeout", diag["error"])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
