#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
START_TEST_BED_PATH = REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py"


def _load_start_test_bed_module():
    spec = importlib.util.spec_from_file_location("test_start_test_bed_deploy_payload", START_TEST_BED_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load start_test_bed module from {START_TEST_BED_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _manifest(
    *,
    name: str,
    logical_selection: str,
    service_name: str,
    atomic_group: str | None,
) -> dict[str, Any]:
    annotations: dict[str, str] = {
        "fluxon.io/namespace": "fluxon_testbed",
        "fluxon.io/logical_selection": logical_selection,
        "fluxon.io/service_name": service_name,
    }
    if atomic_group is not None:
        annotations["fluxon.io/atomic_group"] = atomic_group
    return {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {
            "name": name,
            "annotations": annotations,
        },
        "spec": {
            "template": {
                "spec": {
                    "affinity": {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {
                                                "key": "kubernetes.io/hostname",
                                                "operator": "In",
                                                "values": ["node-2"],
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "containers": [
                        {
                            "name": service_name,
                            "image": "fluxon_quick_start:0.2.2",
                            "command": ["/bin/bash", "-lc"],
                            "args": ["echo ok\n"],
                        }
                    ],
                }
            }
        },
    }


def test_preserves_shared_identity_from_deployconf_name_prefix() -> None:
    module = _load_start_test_bed_module()
    deployconf = {"name_prefix": "fluxon-bench-n3-runtime-20260428-bastion-bootstrap"}
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_payload_rewrite_") as td:
        daemonset_dir = Path(td)
        (daemonset_dir / "fluxon_core_controller.daemonset.yaml").write_text(
            yaml.safe_dump_all(
                [
                    _manifest(
                        name="fluxon-bench-n3-runtime-20260428-bastion-bootstrap-fluxon_core_controller__master",
                        logical_selection="fluxon_core_controller",
                        service_name="master",
                        atomic_group="fluxon_core_controller",
                    ),
                    _manifest(
                        name="fluxon-bench-n3-runtime-20260428-bastion-bootstrap-fluxon_core_controller__owner",
                        logical_selection="fluxon_core_controller",
                        service_name="owner",
                        atomic_group="fluxon_core_controller",
                    ),
                ],
                sort_keys=False,
                explicit_start=True,
            ),
            encoding="utf-8",
        )
        (daemonset_dir / "fluxon_fs_agent.daemonset.yaml").write_text(
            yaml.safe_dump(
                _manifest(
                    name="fluxon-bench-n3-runtime-20260428-bastion-bootstrap-fluxon_fs_agent",
                    logical_selection="fluxon_fs_agent",
                    service_name="fluxon_fs_agent",
                    atomic_group=None,
                ),
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        payload = module._load_deploy_payload(
            deployconf=deployconf,
            daemonset_dir=daemonset_dir,
            deploy_workloads=["fluxon_core_controller", "fluxon_fs_agent"],
        )
        docs = list(yaml.safe_load_all(payload))
        assert [doc["metadata"]["name"] for doc in docs] == [
            "fluxon-bench-n3-runtime-20260428-bastion-bootstrap-fluxon_core_controller__master",
            "fluxon-bench-n3-runtime-20260428-bastion-bootstrap-fluxon_core_controller__owner",
            "fluxon-bench-n3-runtime-20260428-bastion-bootstrap-fluxon_fs_agent",
        ], payload
        print("PASS: test_preserves_shared_identity_from_deployconf_name_prefix")


def test_keeps_already_desired_identity_unchanged() -> None:
    module = _load_start_test_bed_module()
    deployconf = {"name_prefix": "fluxon-bench-n3-runtime-20260428"}
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_payload_passthrough_") as td:
        daemonset_dir = Path(td)
        (daemonset_dir / "fluxon_fs_master.daemonset.yaml").write_text(
            yaml.safe_dump(
                _manifest(
                    name="fluxon-bench-n3-runtime-20260428-fluxon_fs_master",
                    logical_selection="fluxon_fs_master",
                    service_name="fluxon_fs_master",
                    atomic_group=None,
                ),
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        payload = module._load_deploy_payload(
            deployconf=deployconf,
            daemonset_dir=daemonset_dir,
            deploy_workloads=["fluxon_fs_master"],
        )
        docs = list(yaml.safe_load_all(payload))
        assert docs[0]["metadata"]["name"] == "fluxon-bench-n3-runtime-20260428-fluxon_fs_master", payload
        print("PASS: test_keeps_already_desired_identity_unchanged")


def test_rejects_unexpected_workload_name_drift() -> None:
    module = _load_start_test_bed_module()
    deployconf = {"name_prefix": "fluxon-bench-n3-runtime-20260428-bastion-bootstrap"}
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_payload_drift_") as td:
        daemonset_dir = Path(td)
        (daemonset_dir / "fluxon_fs_agent.daemonset.yaml").write_text(
            yaml.safe_dump(
                _manifest(
                    name="fluxon-bench-n3-runtime-20260428-weird-fluxon_fs_agent",
                    logical_selection="fluxon_fs_agent",
                    service_name="fluxon_fs_agent",
                    atomic_group=None,
                ),
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        try:
            module._load_deploy_payload(
                deployconf=deployconf,
                daemonset_dir=daemonset_dir,
                deploy_workloads=["fluxon_fs_agent"],
            )
        except ValueError as exc:
            message = str(exc)
            assert "workload identity drifted" in message, message
        else:
            raise AssertionError("expected workload identity drift to raise")
        print("PASS: test_rejects_unexpected_workload_name_drift")


def test_rejects_split_identity_generated_manifest() -> None:
    module = _load_start_test_bed_module()
    deployconf = {"name_prefix": "fluxon-bench-n3-runtime-20260428-bastion-bootstrap"}
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_payload_split_identity_") as td:
        daemonset_dir = Path(td)
        (daemonset_dir / "fluxon_fs_agent.daemonset.yaml").write_text(
            yaml.safe_dump(
                _manifest(
                    name="fluxon-bench-n3-runtime-20260428-fluxon_fs_agent",
                    logical_selection="fluxon_fs_agent",
                    service_name="fluxon_fs_agent",
                    atomic_group=None,
                ),
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        try:
            module._load_deploy_payload(
                deployconf=deployconf,
                daemonset_dir=daemonset_dir,
                deploy_workloads=["fluxon_fs_agent"],
            )
        except ValueError as exc:
            message = str(exc)
            assert "shared naming contract" in message, message
        else:
            raise AssertionError("expected split-identity generated manifest to raise")
        print("PASS: test_rejects_split_identity_generated_manifest")


def main() -> int:
    parser = argparse.ArgumentParser(description="start_test_bed deploy payload test runner")
    parser.add_argument("--test-id", help="Run only one named test")
    args = parser.parse_args()

    checks: list[tuple[str, Callable[[], None]]] = [
        ("shared_identity_passthrough", test_preserves_shared_identity_from_deployconf_name_prefix),
        ("desired_identity_passthrough", test_keeps_already_desired_identity_unchanged),
        ("reject_workload_name_drift", test_rejects_unexpected_workload_name_drift),
        ("reject_split_identity_manifest", test_rejects_split_identity_generated_manifest),
    ]
    if args.test_id is not None:
        checks = [item for item in checks if item[0] == args.test_id]
        if not checks:
            raise SystemExit(f"unknown --test-id: {args.test_id}")

    failures = 0
    for _, check in checks:
        try:
            check()
        except Exception as exc:
            failures += 1
            print(f"FAIL: {check.__name__}: {exc}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
