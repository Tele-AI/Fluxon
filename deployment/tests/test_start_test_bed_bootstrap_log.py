#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import importlib.util
import io
import sys
import tempfile
from pathlib import Path
from typing import Callable

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
START_TEST_BED_PATH = REPO_ROOT / "fluxon_test_stack" / "start_test_bed.py"
MANUAL_DISPATCH_RELEASE_PATH = REPO_ROOT / "deployment" / "manual_dispatch_release.py"


def _load_start_test_bed_module():
    spec = importlib.util.spec_from_file_location("test_start_test_bed_runtime", START_TEST_BED_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load start_test_bed module from {START_TEST_BED_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _load_manual_dispatch_release_module():
    spec = importlib.util.spec_from_file_location(
        "test_manual_dispatch_release_runtime",
        MANUAL_DISPATCH_RELEASE_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load manual_dispatch_release module from {MANUAL_DISPATCH_RELEASE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _build_result(
    *,
    bootstrap_log_path: Path,
    launcher_rc: int,
    selection_name: str,
    bare_script_name: str,
    node_name: str,
    expected_service_names: list[str],
    launch_error: str | None = None,
) -> dict[str, object]:
    return {
        "node_name": node_name,
        "selection_name": selection_name,
        "bare_script_name": bare_script_name,
        "bootstrap_log_path": bootstrap_log_path,
        "launch_error": launch_error,
        "launcher_rc": launcher_rc,
        "expected_service_names": expected_service_names,
    }


def test_zero_rc_is_success() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_zero_rc_") as td:
        log_path = Path(td) / "plain.log"
        log_path.write_text("launcher completed cleanly\n", encoding="utf-8")
        result = _build_result(
            bootstrap_log_path=log_path,
            launcher_rc=0,
            selection_name="fluxon_fs_agent",
            bare_script_name="fluxon_fs_agent",
            node_name="node-6",
            expected_service_names=["fluxon_fs_agent"],
        )
        ready, source, err = module._bare_launch_ready_summary(result=result)
        assert ready is True, f"expected rc=0 to be ready, got err={err}"
        assert source == "launcher_rc", f"expected launcher_rc source, got {source!r}"
        statuses = module._collect_bare_runtime_statuses(
            deployconf={},
            cluster_nodes={},
            local_node_cfg={},
            result=result,
        )
        assert statuses == [
            {
                "service_name": "fluxon_fs_agent",
                "present": True,
                "running": True,
                "log_path": str(log_path),
                "status_source": "launcher_rc",
                "status_error": None,
            }
        ], f"unexpected statuses: {statuses!r}"
        print("PASS: test_zero_rc_is_success")


def test_rc255_recovers_from_plain_ready_marker() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_plain_marker_") as td:
        log_path = Path(td) / "plain.log"
        log_path.write_text(
            "Starting fluxon_fs_agent on node-6\nStarted fluxon_fs_agent (label: DaemonSet/fluxon_fs_agent)\n",
            encoding="utf-8",
        )
        result = _build_result(
            bootstrap_log_path=log_path,
            launcher_rc=255,
            selection_name="fluxon_fs_agent",
            bare_script_name="fluxon_fs_agent",
            node_name="node-6",
            expected_service_names=["fluxon_fs_agent"],
        )
        ready, source, err = module._bare_launch_ready_summary(result=result)
        assert ready is True, f"expected ready-marker recovery, got err={err}"
        assert source == "bootstrap_log_ready", f"expected bootstrap_log_ready source, got {source!r}"
        print("PASS: test_rc255_recovers_from_plain_ready_marker")


def test_rc255_recovers_from_atomic_ready_marker() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_atomic_marker_") as td:
        log_path = Path(td) / "atomic.log"
        log_path.write_text(
            "[atomic-group] group=fluxon_core_controller node=node-6 hostworkdir=/opt/fluxon_testbed\n"
            "[atomic-group] ready group=fluxon_core_controller node=node-6\n",
            encoding="utf-8",
        )
        result = _build_result(
            bootstrap_log_path=log_path,
            launcher_rc=255,
            selection_name="fluxon_core_controller",
            bare_script_name="fluxon_core_controller",
            node_name="node-6",
            expected_service_names=["owner", "ops_agent"],
        )
        statuses = module._collect_bare_runtime_statuses(
            deployconf={},
            cluster_nodes={},
            local_node_cfg={},
            result=result,
        )
        assert len(statuses) == 2, f"expected one status per service, got {statuses!r}"
        assert all(status["present"] is True for status in statuses), f"expected recovered statuses, got {statuses!r}"
        assert all(status["status_source"] == "bootstrap_log_ready" for status in statuses), (
            f"expected bootstrap_log_ready source, got {statuses!r}"
        )
        print("PASS: test_rc255_recovers_from_atomic_ready_marker")


def test_failed_status_includes_bootstrap_and_service_log_tails() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_failure_tails_") as td:
        root = Path(td)
        bootstrap_log = root / "tikv.bootstrap.log"
        bootstrap_log.write_text("[bare] probable-ready failed svc=tikv\n", encoding="utf-8")
        service_log = root / "monitor" / "tikv" / "store" / "tikv.log"
        service_log.parent.mkdir(parents=True, exist_ok=True)
        service_log.write_text("FATAL: connect to PD failed\n", encoding="utf-8")
        local_node_cfg = {
            "hostname": "node-a",
            "hostworkdir": str(root),
        }
        result = _build_result(
            bootstrap_log_path=bootstrap_log,
            launcher_rc=1,
            selection_name="tikv",
            bare_script_name="tikv",
            node_name="node-a",
            expected_service_names=["tikv"],
        )
        statuses = module._collect_bare_runtime_statuses(
            deployconf={},
            cluster_nodes={},
            local_node_cfg=local_node_cfg,
            result=result,
        )
        assert len(statuses) == 1, statuses
        status = statuses[0]
        assert status["present"] is False, status
        assert status["running"] is False, status
        assert status["log_path"] == str(service_log), status
        err = status["status_error"]
        assert isinstance(err, str) and "bootstrap_log_tail=" in err, err
        assert "service_log_tail=" in err, err
        assert "connect to PD failed" in err, err
        print("PASS: test_failed_status_includes_bootstrap_and_service_log_tails")


def test_failed_status_resolves_daily_sharded_service_log_tail() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_sharded_failure_tails_") as td:
        root = Path(td)
        bootstrap_log = root / "fluxon_core_controller.bootstrap.log"
        bootstrap_log.write_text("[rollout] probable-ready failed svc=owner\n", encoding="utf-8")
        base_service_log = root / "log" / "master.log"
        base_service_log.parent.mkdir(parents=True, exist_ok=True)
        sharded_service_log = root / "log" / "master.2026-06-23.log"
        sharded_service_log.write_text("FATAL: owner bootstrap dependency failed\n", encoding="utf-8")
        local_node_cfg = {
            "hostname": "node-a",
            "hostworkdir": str(root),
        }
        result = _build_result(
            bootstrap_log_path=bootstrap_log,
            launcher_rc=1,
            selection_name="fluxon_core_controller",
            bare_script_name="fluxon_core_controller",
            node_name="node-a",
            expected_service_names=["master"],
        )
        statuses = module._collect_bare_runtime_statuses(
            deployconf={},
            cluster_nodes={},
            local_node_cfg=local_node_cfg,
            result=result,
        )
        assert len(statuses) == 1, statuses
        status = statuses[0]
        assert status["present"] is False, status
        assert status["running"] is False, status
        assert status["log_path"] == str(sharded_service_log.resolve()), status
        err = status["status_error"]
        assert isinstance(err, str) and "bootstrap_log_tail=" in err, err
        assert "service_log_tail=" in err, err
        assert "owner bootstrap dependency failed" in err, err
        print("PASS: test_failed_status_resolves_daily_sharded_service_log_tail")


def test_testbed_template_tikv_uses_low_fd_limits_for_ci_runner() -> None:
    deployconf = yaml.safe_load((REPO_ROOT / "fluxon_test_stack" / "deployconf_testbed.yml").read_text(encoding="utf-8"))
    tikv_cfg = deployconf["service"]["tikv"]["entrypoint"]
    assert "max-open-files = 4096" in tikv_cfg, tikv_cfg
    assert "max-open-files = 2048" in tikv_cfg, tikv_cfg
    print("PASS: test_testbed_template_tikv_uses_low_fd_limits_for_ci_runner")


def test_direct_supervisor_status_path_is_rejected() -> None:
    module = _load_start_test_bed_module()
    try:
        module._local_bare_service_present(
            deployconf={},
            local_node_cfg={},
            service_name="etcd",
        )
    except RuntimeError as exc:
        message = str(exc)
        assert "no longer supported" in message, f"unexpected error: {message!r}"
    else:
        raise AssertionError("expected direct supervisor status path to raise")
    print("PASS: test_direct_supervisor_status_path_is_rejected")


def test_ops_agent_snapshot_payload_rejects_empty_file() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_ops_agent_snapshot_empty_") as td:
        snapshot_path = Path(td) / "agent_desired_snapshot.json"
        try:
            module._validate_ops_agent_snapshot_payload(
                snapshot_path=snapshot_path,
                node_name="node-a",
                raw_bytes=b"",
            )
        except ValueError as exc:
            message = str(exc)
            assert "remove the file instead of truncating it" in message, message
        else:
            raise AssertionError("expected empty snapshot payload to raise")
    print("PASS: test_ops_agent_snapshot_payload_rejects_empty_file")


def test_ops_agent_snapshot_payload_accepts_valid_json() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_ops_agent_snapshot_valid_") as td:
        snapshot_path = Path(td) / "agent_desired_snapshot.json"
        payload = (
            '{'
            '"instance_key":"fluxon_ops_node-a",'
            '"desired_keys":[],'
            '"workloads":[],'
            '"delete_workloads":[]'
            '}'
        ).encode("utf-8")
        module._validate_ops_agent_snapshot_payload(
            snapshot_path=snapshot_path,
            node_name="node-a",
            raw_bytes=payload,
        )
    print("PASS: test_ops_agent_snapshot_payload_accepts_valid_json")


def test_ops_agent_snapshot_prereq_allows_missing_file() -> None:
    module = _load_start_test_bed_module()
    cluster_nodes = {
        "node-a": {
            "hostname": "node-a",
            "ip": "127.0.0.1",
            "hostworkdir": "/tmp/hostworkdir",
            "execution_mode": "local",
            "ssh_user": "tester",
            "ssh_port": 22,
        }
    }
    deployconf = {
        "service": {
            "ops_agent": {
                "node_bind": {
                    "node": ["node-a"],
                }
            }
        },
        "atomic_groups": {},
    }
    module._validate_ops_agent_snapshot_prerequisites(
        deployconf=deployconf,
        cluster_nodes=cluster_nodes,
        local_node_cfg=cluster_nodes["node-a"],
        fixed_bootstrap_batches=[{"node": "node-a", "services": ["ops_agent"]}],
        coverage_bootstrap_services=[],
    )
    print("PASS: test_ops_agent_snapshot_prereq_allows_missing_file")


def test_parse_cluster_nodes_accepts_local_execution_mode() -> None:
    module = _load_start_test_bed_module()
    cluster_nodes = module._parse_cluster_nodes(
        {
            "cluster_nodes": [
                {
                    "hostname": "logic-a",
                    "ip": "127.0.0.1",
                    "hostworkdir": "/tmp/logic-a",
                    "execution_mode": "local",
                    "ssh_user": "tester",
                    "ssh_port": 22,
                },
                {
                    "hostname": "logic-b",
                    "ip": "127.0.0.1",
                    "hostworkdir": "/tmp/logic-b",
                    "ssh_user": "tester",
                    "ssh_port": 22,
                },
            ]
        }
    )
    assert module._cluster_node_is_local(cluster_nodes["logic-a"]) is True
    assert module._cluster_node_is_local(cluster_nodes["logic-b"]) is False
    print("PASS: test_parse_cluster_nodes_accepts_local_execution_mode")


def test_run_bare_waves_treats_local_execution_mode_node_as_local() -> None:
    module = _load_start_test_bed_module()
    cluster_nodes = {
        "logic-a": {
            "hostname": "logic-a",
            "ip": "127.0.0.1",
            "hostworkdir": "/tmp/logic-a",
            "execution_mode": "local",
            "ssh_user": "tester",
            "ssh_port": 22,
        },
        "logic-b": {
            "hostname": "logic-b",
            "ip": "127.0.0.1",
            "hostworkdir": "/tmp/logic-b",
            "execution_mode": "local",
            "ssh_user": "tester",
            "ssh_port": 22,
        },
    }
    deployconf = {
        "service": {
            "ops_agent": {"node_bind": {"node": ["logic-a", "logic-b"]}},
        },
        "atomic_groups": {},
    }
    calls: list[tuple[str, str]] = []
    original_spawn_local = module._spawn_local_start
    original_spawn_remote = module._spawn_remote_start
    original_join = module._join_bare_launch
    original_collect = module._collect_bare_runtime_statuses
    original_bare_script_name = module._selection_bare_script_name
    original_service_names = module._selection_service_names_for_target_node
    original_log_path = module._bare_wave_bootstrap_log_path
    try:
        module._spawn_local_start = lambda **kwargs: calls.append(("local", kwargs["local_node_cfg"]["hostname"])) or {
            "mode": "local",
            "node_name": kwargs["local_node_cfg"]["hostname"],
            "selection_name": kwargs["selection_name"],
            "bare_script_name": kwargs["bare_script_name"],
            "bootstrap_log_path": kwargs["bootstrap_log_path"],
            "expected_service_names": kwargs["expected_service_names"],
            "launch_error": None,
            "launcher_rc": 0,
            "runtime_statuses": [],
        }
        module._spawn_remote_start = lambda **kwargs: calls.append(("remote", kwargs["node_name"])) or {
            "mode": "remote",
            "node_name": kwargs["node_name"],
            "selection_name": kwargs["selection_name"],
            "bare_script_name": kwargs["bare_script_name"],
            "bootstrap_log_path": kwargs["bootstrap_log_path"],
            "expected_service_names": kwargs["expected_service_names"],
            "launch_error": None,
            "launcher_rc": 0,
            "runtime_statuses": [],
        }
        module._join_bare_launch = lambda result: None
        module._collect_bare_runtime_statuses = lambda **kwargs: []
        module._selection_bare_script_name = lambda **kwargs: "ops_agent"
        module._selection_service_names_for_target_node = lambda **kwargs: ["ops_agent"]
        module._bare_wave_bootstrap_log_path = (
            lambda **kwargs: Path("/tmp") / f"{kwargs['node_name']}_{kwargs['selection_name']}.log"
        )
        module._run_bare_waves(
            workdir=Path("/tmp"),
            deployconf=deployconf,
            cluster_nodes=cluster_nodes,
            local_node_cfg=cluster_nodes["logic-a"],
            waves=[
                {
                    "launches": [
                        {"node": "logic-a", "selection_name": "ops_agent"},
                        {"node": "logic-b", "selection_name": "ops_agent"},
                    ]
                }
            ],
            bootstrap_bare_services=set(),
        )
    finally:
        module._spawn_local_start = original_spawn_local
        module._spawn_remote_start = original_spawn_remote
        module._join_bare_launch = original_join
        module._collect_bare_runtime_statuses = original_collect
        module._selection_bare_script_name = original_bare_script_name
        module._selection_service_names_for_target_node = original_service_names
        module._bare_wave_bootstrap_log_path = original_log_path
    assert calls == [("local", "logic-a"), ("local", "logic-b")], calls
    print("PASS: test_run_bare_waves_treats_local_execution_mode_node_as_local")


def test_local_coverage_bootstrap_excludes_duplicate_local_control_plane_selection() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "service": {
            "master": {"node_bind": {"node": ["logic-a"]}},
            "owner": {"node_bind": {"node": ["logic-a", "logic-b"]}},
            "ops_controller": {"node_bind": {"node": ["logic-a"]}},
            "ops_agent": {"node_bind": {"node": ["logic-a", "logic-b"]}},
            "fluxon_fs_master": {"node_bind": {"node": ["logic-a"]}},
        },
        "atomic_groups": {
            "fluxon_core_controller": {
                "phase": 1,
                "nodes": ["logic-a", "logic-b"],
                "services": ["master", "owner", "ops_controller", "ops_agent"],
            }
        },
    }
    excluded_targets = module._local_control_plane_coverage_excluded_targets(
        deployconf=deployconf,
        fixed_bootstrap_batches=[{"node": "logic-a", "services": ["fluxon_core_controller"]}],
        local_node_name="logic-a",
        coverage_bootstrap_services=["owner", "ops_controller", "fluxon_fs_master"],
    )
    assert excluded_targets == [
        {
            "node": "logic-a",
            "selection_name": "fluxon_core_controller",
            "service_names": ["master", "owner", "ops_controller", "ops_agent"],
            "reason": "local_fixed_bare_already_started_same_control_plane_service",
        }
    ], excluded_targets

    coverage_batches = module._build_coverage_bootstrap_batches(
        deployconf=deployconf,
        coverage_bootstrap_services=["owner", "ops_controller", "fluxon_fs_master"],
        excluded_targets={
            (
                item["node"],
                item["selection_name"],
            )
            for item in excluded_targets
        },
    )
    assert coverage_batches == [
        {"node": "logic-b", "services": ["fluxon_core_controller"]},
        {"node": "logic-a", "services": ["fluxon_fs_master"]},
    ], coverage_batches
    print("PASS: test_local_coverage_bootstrap_excludes_duplicate_local_control_plane_selection")


def test_start_test_bed_release_scope_rejects_missing_ext_images_manifest_reference() -> None:
    module = _load_manual_dispatch_release_module()
    manifest_text = "\n".join(
        [
            "0" * 64 + "  ext_images.tar.gz",
            "1" * 64 + "  fluxon-0.2.1-py3-none-any.whl",
            "2" * 64 + "  fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            "3" * 64 + "  pylib_src.tar.gz",
        ]
    )
    try:
        module._start_test_bed_ext_manifest_relpaths_from_release_manifest_text(manifest_text=manifest_text)
    except RuntimeError as exc:
        assert "ext_images/ext_images.sha256" in str(exc), exc
    else:
        raise AssertionError("expected missing ext_images manifest reference to raise")
    print("PASS: test_start_test_bed_release_scope_rejects_missing_ext_images_manifest_reference")


def test_start_test_bed_release_manifest_scope_contract_accepts_ext_manifest() -> None:
    manual_dispatch_module = _load_manual_dispatch_release_module()
    relpaths = [
        "ext_images.tar.gz",
        "ext_images/ext_images.sha256",
        "fluxon-0.2.1-py3-none-any.whl",
        "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
        "pylib_src.tar.gz",
    ]
    assert "ext_images/ext_images.sha256" in relpaths, relpaths
    manifest_text = "\n".join(
        f"{idx:064x}  {relpath}"
        for idx, relpath in enumerate(relpaths, start=1)
    )
    required_relpaths = manual_dispatch_module._release_scope_required_relpaths(
        manifest_text=manifest_text,
        dispatch_release_scope=manual_dispatch_module.DISPATCH_RELEASE_SCOPE_START_TEST_BED,
    )
    assert "ext_images/ext_images.sha256" in required_relpaths, required_relpaths
    assert "ext_images.tar.gz" not in required_relpaths, required_relpaths
    print("PASS: test_start_test_bed_release_manifest_scope_contract_accepts_ext_manifest")


def test_start_test_bed_release_scope_dispatches_ext_runtime_files_from_manifest() -> None:
    manual_dispatch_module = _load_manual_dispatch_release_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_ext_runtime_") as td:
        release_dir = Path(td) / "fluxon_release"
        ext_dir = release_dir / "ext_images"
        tikv_dir = ext_dir / "tikv"
        etcd_dir = ext_dir / "etcd"
        release_dir.mkdir(parents=True, exist_ok=True)
        tikv_dir.mkdir(parents=True, exist_ok=True)
        etcd_dir.mkdir(parents=True, exist_ok=True)
        (release_dir / "install.py").write_text("print('ok')\n", encoding="utf-8")
        (release_dir / "fluxon-0.2.1-py3-none-any.whl").write_text("wheel\n", encoding="utf-8")
        (release_dir / "fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl").write_text("wheel\n", encoding="utf-8")
        (release_dir / "pylib_src.tar.gz").write_text("tar\n", encoding="utf-8")
        (release_dir / "ext_images.tar.gz").write_text("tarball\n", encoding="utf-8")
        (tikv_dir / "start_pd.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
        (tikv_dir / "tikv-server").write_text("tikv\n", encoding="utf-8")
        (etcd_dir / "etcd").write_text("etcd\n", encoding="utf-8")
        ext_manifest_lines = [
            f"{'a'*64}  tikv/start_pd.sh",
            f"{'b'*64}  tikv/tikv-server",
            f"{'c'*64}  etcd/etcd",
        ]
        (ext_dir / "ext_images.sha256").write_text("\n".join(ext_manifest_lines) + "\n", encoding="utf-8")
        release_manifest_lines = [
            f"{'1'*64}  fluxon-0.2.1-py3-none-any.whl",
            f"{'2'*64}  fluxon_pyo3-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl",
            f"{'3'*64}  pylib_src.tar.gz",
            f"{'4'*64}  ext_images.tar.gz",
            f"{'5'*64}  ext_images/ext_images.sha256",
        ]
        (release_dir / "fluxon_release.sha256").write_text(
            "\n".join(release_manifest_lines) + "\n",
            encoding="utf-8",
        )
        relpaths = manual_dispatch_module._release_dispatch_relpaths(
            src_release_dir=release_dir,
            dispatch_release_scope=manual_dispatch_module.DISPATCH_RELEASE_SCOPE_START_TEST_BED,
        )
        assert "ext_images/tikv/start_pd.sh" in relpaths, relpaths
        assert "ext_images/tikv/tikv-server" in relpaths, relpaths
        assert "ext_images/etcd/etcd" in relpaths, relpaths
    print("PASS: test_start_test_bed_release_scope_dispatches_ext_runtime_files_from_manifest")


def test_parse_test_runner_ui_config_resolves_paths() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_ui_cfg_") as td:
        root = Path(td)
        gitops_cfg = root / "gitops.yaml"
        gitops_cfg.write_text("repos: []\n", encoding="utf-8")
        cfg = module._parse_test_runner_ui_config(
            {
                "test_runner_ui": {
                    "enabled": True,
                    "host": "0.0.0.0",
                    "port": 18080,
                    "workdir": "./ui_runtime",
                    "history_lookback_days": 30,
                    "history_roots": ["./suite_history", "./bench_history"],
                    "gitops_config_path": "./gitops.yaml",
                }
            },
            config_root=root,
        )
        assert cfg["enabled"] is True, cfg
        assert cfg["host"] == "0.0.0.0", cfg
        assert cfg["port"] == 18080, cfg
        assert cfg["workdir"] == (root / "ui_runtime").resolve(), cfg
        assert cfg["log_path"] == (root / "ui_runtime" / module.TEST_RUNNER_UI_LOG_FILENAME).resolve(), cfg
        assert cfg["history_roots"] == [
            (root / "suite_history").resolve(),
            (root / "bench_history").resolve(),
        ], cfg
        assert cfg["gitops_config_path"] == gitops_cfg.resolve(), cfg
        assert cfg["entrypoint"] == (REPO_ROOT / "fluxon_test_stack" / "test_runner_ui.py").resolve(), cfg
    print("PASS: test_parse_test_runner_ui_config_resolves_paths")


def test_normalize_bootstrap_deployconf_strips_legacy_master_p2p_listen_port() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "service": {
            "master": {
                "entrypoint": (
                    'cat > "${CONFIG_PATH}" <<YAML\n'
                    'instance_key: "unified_master"\n'
                    "p2p_listen_port: 31100\n"
                    "port: 51051\n"
                    "YAML\n"
                )
            },
            "ops_agent": {
                "entrypoint": (
                    'cat > "${WORKDIR}/ops_agent.yaml" <<YAML\n'
                    "kv_client:\n"
                    "  fluxonkv_spec:\n"
                    "    p2p_listen_port: 12102\n"
                    "YAML\n"
                )
            },
        }
    }
    normalized, notes = module._normalize_bootstrap_deployconf(deployconf=deployconf)
    master_entrypoint = normalized["service"]["master"]["entrypoint"]
    ops_agent_entrypoint = normalized["service"]["ops_agent"]["entrypoint"]
    assert "p2p_listen_port: 31100" not in master_entrypoint, master_entrypoint
    assert "p2p_listen_port: 12102" in ops_agent_entrypoint, ops_agent_entrypoint
    assert normalized["service"]["master"]["port"] == 51051, normalized["service"]["master"]
    assert notes == ["service.master.entrypoint: removed legacy master field p2p_listen_port"], notes
    assert "p2p_listen_port: 31100" in deployconf["service"]["master"]["entrypoint"], deployconf
    print("PASS: test_normalize_bootstrap_deployconf_strips_legacy_master_p2p_listen_port")


def test_normalize_bootstrap_deployconf_rejects_missing_fluxon_fs_master_prometheus_base_url() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "service": {
            "fluxon_fs_master": {
                "entrypoint": (
                    'cat > "${WORKDIR}/all_config.yaml" <<YAML\n'
                    "fluxon_fs:\n"
                    "  master_panel:\n"
                    '    listen_addr: "${FLUXON_FS_MASTER_PANEL_LISTEN_ADDR}"\n'
                    '    public_base_url: "${FLUXON_FS_MASTER_PANEL_BASE_URL}"\n'
                    "    auto_refresh_interval_secs: 10\n"
                    "YAML\n"
                )
            },
        }
    }
    try:
        module._normalize_bootstrap_deployconf(deployconf=deployconf)
    except ValueError as exc:
        assert (
            str(exc)
            == "deployconf.service.fluxon_fs_master.entrypoint is missing fluxon_fs.master_panel.prometheus_base_url"
        ), exc
    else:
        raise AssertionError("expected ValueError for missing fluxon_fs.master_panel.prometheus_base_url")
    print("PASS: test_normalize_bootstrap_deployconf_rejects_missing_fluxon_fs_master_prometheus_base_url")


def test_normalize_bootstrap_deployconf_rejects_missing_greptime_loopback_bind_addrs() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "service": {
            "greptime": {
                "entrypoint": (
                    "set -euo pipefail\n"
                    'exec greptime standalone start \\\n'
                    '  --data-home "${DATA_DIR}" \\\n'
                    '  --http-addr 0.0.0.0:41555\n'
                )
            }
        }
    }
    try:
        module._normalize_bootstrap_deployconf(deployconf=deployconf)
    except ValueError as exc:
        assert (
            str(exc)
            == "deployconf.service.greptime.entrypoint is missing required loopback bind flags: "
            "--rpc-bind-addr 127.0.0.1:$((GREPTIME__PORT + 1)), "
            "--mysql-addr 127.0.0.1:$((GREPTIME__PORT + 2)), "
            "--postgres-addr 127.0.0.1:$((GREPTIME__PORT + 3))"
        ), exc
    else:
        raise AssertionError("expected ValueError for missing greptime loopback bind flags")
    print("PASS: test_normalize_bootstrap_deployconf_rejects_missing_greptime_loopback_bind_addrs")


def test_normalize_bootstrap_deployconf_rewrites_same_host_local_multi_node_fixed_ports() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "cluster_nodes": [
            {
                "hostname": "logic-a",
                "ip": "127.0.0.1",
                "hostworkdir": "/tmp/logic-a",
                "execution_mode": "local",
                "ssh_user": "tester",
                "ssh_port": 22,
            },
            {
                "hostname": "logic-b",
                "ip": "127.0.0.1",
                "hostworkdir": "/tmp/logic-b",
                "execution_mode": "local",
                "ssh_user": "tester",
                "ssh_port": 22,
            },
        ],
        "global_envs": {
            "MASTER__PORT": "19280",
            "TIKV_PD_PEER_PORT": "33680",
            "TIKV_STATUS_FULL_ADDRESS": "${${TIKV__NODE_ID}__IP}:34180",
            "FLUXON_FS_MASTER_PANEL_BASE_URL": "http://${FLUXON_FS_MASTER__NODE_ID__IP}:25080",
            "FLUXON_FS_MASTER_PANEL_LISTEN_ADDR": "0.0.0.0:25080",
        },
        "service": {
            "etcd": {
                "port": 33579,
                "in_container_port": 33579,
                "entrypoint": (
                    "${HOSTWORKDIR}/fluxon_release/ext_images/etcd/etcd \\\n"
                    '  --advertise-client-urls "http://0.0.0.0:33579" \\\n'
                    '  --listen-client-urls "http://0.0.0.0:33579" \\\n'
                    '  --listen-peer-urls "http://0.0.0.0:2480" \\\n'
                    '  --initial-advertise-peer-urls "http://0.0.0.0:2480" \\\n'
                    '  --initial-cluster "etcd0=http://0.0.0.0:2480"\n'
                ),
            },
            "greptime": {
                "port": 35030,
                "in_container_port": 35030,
                "entrypoint": (
                    "set -euo pipefail\n"
                    'exec greptime standalone start \\\n'
                    '  --data-home "${DATA_DIR}" \\\n'
                    '  --http-addr 0.0.0.0:35030 \\\n'
                    '  --rpc-bind-addr 127.0.0.1:$((GREPTIME__PORT + 1)) \\\n'
                    '  --mysql-addr 127.0.0.1:$((GREPTIME__PORT + 2)) \\\n'
                    '  --postgres-addr 127.0.0.1:$((GREPTIME__PORT + 3))\n'
                ),
            },
            "tikv_pd": {
                "port": 33679,
                "entrypoint": "exec pd\n",
            },
            "tikv": {
                "port": 34160,
                "entrypoint": "exec tikv\n",
            },
            "master": {
                "entrypoint": (
                    'cat > "${CONFIG_PATH}" <<YAML\n'
                    'instance_key: "unified_master"\n'
                    "port: 51051\n"
                    "p2p_listen_port: 31100\n"
                    "YAML\n"
                )
            },
            "ops_agent": {
                "entrypoint": (
                    'case "${NODE_ID}" in\n'
                    "  logic-a)\n"
                    "    OPS_AGENT_P2P_LISTEN_PORT=12112\n"
                    "    ;;\n"
                    "  logic-b)\n"
                    "    OPS_AGENT_P2P_LISTEN_PORT=12113\n"
                    "    ;;\n"
                    "esac\n"
                )
            },
            "ops_controller": {
                "entrypoint": (
                    'cat > "${WORKDIR}/ops_controller.yaml" <<YAML\n'
                    "ops_controller:\n"
                    "  kv_client:\n"
                    "    fluxonkv_spec:\n"
                    "      p2p_listen_port: 12102\n"
                    "YAML\n"
                )
            },
            "fluxon_fs_master": {
                "entrypoint": (
                    'cat > "${WORKDIR}/all_config.yaml" <<YAML\n'
                    "fluxon_fs:\n"
                    "  master_panel:\n"
                    '    listen_addr: "${FLUXON_FS_MASTER_PANEL_LISTEN_ADDR}"\n'
                    '    prometheus_base_url: "${FLUXON_PROMETHEUS_BASE_URL}"\n'
                    "YAML\n"
                )
            },
        },
    }
    normalized, notes = module._normalize_bootstrap_deployconf(deployconf=deployconf)
    assert normalized["global_envs"]["TIKV_PD_PEER_PORT"] == "19401", normalized["global_envs"]
    assert normalized["global_envs"]["TIKV_STATUS_FULL_ADDRESS"] == "${${TIKV__NODE_ID}__IP}:19411", normalized["global_envs"]
    assert (
        normalized["global_envs"]["FLUXON_FS_MASTER_PANEL_BASE_URL"]
        == "http://${FLUXON_FS_MASTER__NODE_ID__IP}:19300"
    ), normalized["global_envs"]
    assert normalized["global_envs"]["FLUXON_FS_MASTER_PANEL_LISTEN_ADDR"] == "0.0.0.0:19300", normalized["global_envs"]
    assert normalized["service"]["etcd"]["port"] == 19380, normalized["service"]["etcd"]
    assert normalized["service"]["etcd"]["in_container_port"] == 19380, normalized["service"]["etcd"]
    assert 'http://0.0.0.0:19380' in normalized["service"]["etcd"]["entrypoint"], normalized["service"]["etcd"]["entrypoint"]
    assert 'http://0.0.0.0:19381' in normalized["service"]["etcd"]["entrypoint"], normalized["service"]["etcd"]["entrypoint"]
    assert normalized["service"]["greptime"]["port"] == 19390, normalized["service"]["greptime"]
    assert normalized["service"]["greptime"]["in_container_port"] == 19390, normalized["service"]["greptime"]
    assert "--http-addr 0.0.0.0:19390" in normalized["service"]["greptime"]["entrypoint"], normalized["service"]["greptime"]["entrypoint"]
    assert normalized["service"]["tikv_pd"]["port"] == 19400, normalized["service"]["tikv_pd"]
    assert normalized["service"]["tikv"]["port"] == 19410, normalized["service"]["tikv"]
    assert normalized["service"]["master"]["port"] == 19290, normalized["service"]["master"]
    assert "port: 19290" in normalized["service"]["master"]["entrypoint"], normalized["service"]["master"]["entrypoint"]
    assert "OPS_AGENT_P2P_LISTEN_PORT=19320" in normalized["service"]["ops_agent"]["entrypoint"], normalized["service"]["ops_agent"]["entrypoint"]
    assert "OPS_AGENT_P2P_LISTEN_PORT=19321" in normalized["service"]["ops_agent"]["entrypoint"], normalized["service"]["ops_agent"]["entrypoint"]
    assert "p2p_listen_port: 19310" in normalized["service"]["ops_controller"]["entrypoint"], normalized["service"]["ops_controller"]["entrypoint"]
    assert notes[0] == "same_host_local_multi_node: rewrote fixed host-listen ports from controller anchor 19280", notes
    assert deployconf["service"]["etcd"]["port"] == 33579, deployconf
    assert deployconf["service"]["master"]["entrypoint"].count("51051") == 1, deployconf
    print("PASS: test_normalize_bootstrap_deployconf_rewrites_same_host_local_multi_node_fixed_ports")


def test_normalize_bootstrap_deployconf_keeps_non_local_or_single_node_ports_unchanged() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "cluster_nodes": [
            {
                "hostname": "logic-a",
                "ip": "127.0.0.1",
                "hostworkdir": "/tmp/logic-a",
                "execution_mode": "local",
                "ssh_user": "tester",
                "ssh_port": 22,
            },
            {
                "hostname": "logic-b",
                "ip": "198.51.100.10",
                "hostworkdir": "/opt/logic-b",
                "execution_mode": "ssh",
                "ssh_user": "tester",
                "ssh_port": 22,
            },
        ],
        "global_envs": {
            "MASTER__PORT": "19080",
            "TIKV_PD_PEER_PORT": "33680",
            "TIKV_STATUS_FULL_ADDRESS": "${${TIKV__NODE_ID}__IP}:34180",
            "FLUXON_FS_MASTER_PANEL_BASE_URL": "http://${FLUXON_FS_MASTER__NODE_ID__IP}:25080",
            "FLUXON_FS_MASTER_PANEL_LISTEN_ADDR": "0.0.0.0:25080",
        },
        "service": {
            "etcd": {
                "port": 33579,
                "entrypoint": 'exec etcd --listen-client-urls "http://0.0.0.0:33579"\n',
            },
            "greptime": {
                "port": 35030,
                "entrypoint": 'exec greptime --http-addr 0.0.0.0:35030\n',
            },
            "tikv_pd": {"port": 33679, "entrypoint": "exec pd\n"},
            "tikv": {"port": 34160, "entrypoint": "exec tikv\n"},
            "master": {"entrypoint": "port: 51051\n"},
            "ops_agent": {"entrypoint": "OPS_AGENT_P2P_LISTEN_PORT=12112\n"},
            "ops_controller": {"entrypoint": "p2p_listen_port: 12102\n"},
            "fluxon_fs_master": {"entrypoint": 'listen_addr: "${FLUXON_FS_MASTER_PANEL_LISTEN_ADDR}"\n'},
        },
    }
    normalized, notes = module._normalize_bootstrap_deployconf(deployconf=deployconf)
    assert normalized["service"]["master"]["port"] == 51051, normalized["service"]["master"]
    expected = copy.deepcopy(deployconf)
    expected["service"]["master"]["port"] = 51051
    assert normalized == expected, normalized
    assert notes == [], notes
    print("PASS: test_normalize_bootstrap_deployconf_keeps_non_local_or_single_node_ports_unchanged")


def test_normalize_bootstrap_deployconf_promotes_master_port_from_entrypoint() -> None:
    module = _load_start_test_bed_module()
    deployconf = {
        "service": {
            "master": {
                "entrypoint": (
                    'cat > "${CONFIG_PATH}" <<YAML\n'
                    'instance_key: "unified_master"\n'
                    "port: 51051\n"
                    "YAML\n"
                )
            }
        }
    }
    normalized, notes = module._normalize_bootstrap_deployconf(deployconf=deployconf)
    assert normalized["service"]["master"]["port"] == 51051, normalized["service"]["master"]
    assert notes == [], notes
    assert "port" not in deployconf["service"]["master"], deployconf
    print("PASS: test_normalize_bootstrap_deployconf_promotes_master_port_from_entrypoint")


def test_refresh_cluster_bare_deploy_scripts_copies_local_and_remote_nodes() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_refresh_bare_") as td:
        root = Path(td)
        deployconf_path = root / "deployconf.yaml"
        deployconf_path.write_text("service: {}\n", encoding="utf-8")
        bare_scripts_dir = root / "gen_bare_deploy_bash"
        bare_scripts_dir.mkdir(parents=True, exist_ok=True)
        cluster_nodes = {
            "logic-a": {
                "hostname": "logic-a",
                "ip": "127.0.0.1",
                "hostworkdir": "/tmp/logic-a",
                "execution_mode": "local",
                "ssh_user": "tester",
                "ssh_port": 22,
                "ssh_password": None,
            },
            "logic-b": {
                "hostname": "logic-b",
                "ip": "198.51.100.10",
                "ssh_host": "198.51.100.11",
                "hostworkdir": "/opt/logic-b",
                "execution_mode": "ssh",
                "ssh_user": "tester",
                "ssh_port": 2202,
                "ssh_password": "secret",
            },
        }
        original_generate_bare_deploy_scripts = module._generate_bare_deploy_scripts
        original_copy_local_artifact = module.manual_dispatch_release._copy_local_artifact
        original_copy_remote_artifact = module.manual_dispatch_release._copy_remote_artifact
        calls: list[tuple[str, dict[str, object]]] = []
        try:
            module._generate_bare_deploy_scripts = (
                lambda **kwargs: calls.append(("generate", kwargs))
            )
            module.manual_dispatch_release._copy_local_artifact = (
                lambda **kwargs: calls.append(("local_copy", kwargs))
            )
            module.manual_dispatch_release._copy_remote_artifact = (
                lambda **kwargs: calls.append(("remote_copy", kwargs))
            )
            module._refresh_cluster_bare_deploy_scripts(
                deployconf_path=deployconf_path,
                cluster_nodes=cluster_nodes,
                bare_scripts_dir=bare_scripts_dir,
            )
        finally:
            module._generate_bare_deploy_scripts = original_generate_bare_deploy_scripts
            module.manual_dispatch_release._copy_local_artifact = original_copy_local_artifact
            module.manual_dispatch_release._copy_remote_artifact = original_copy_remote_artifact
        assert calls[0][0] == "generate", calls
        assert calls[0][1]["deployconf_path"] == deployconf_path, calls
        assert calls[0][1]["bare_scripts_dir"] == bare_scripts_dir, calls
        assert calls[1] == (
            "local_copy",
            {
                "src_dir": bare_scripts_dir,
                "dst_dir_s": "/tmp/logic-a/gen_bare_deploy_bash",
                "dst_owner": "tester:tester",
            },
        ), calls
        assert calls[2] == (
            "remote_copy",
            {
                "src_dir": bare_scripts_dir,
                "dst_dir_s": "/opt/logic-b/gen_bare_deploy_bash",
                "ssh_user": "tester",
                "ip": "198.51.100.11",
                "ssh_port": 2202,
                "ssh_password": "secret",
                "dst_owner": "tester:tester",
            },
        ), calls
    print("PASS: test_refresh_cluster_bare_deploy_scripts_copies_local_and_remote_nodes")


def test_bare_then_apply_success_path_does_not_run_post_apply_stop() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_no_post_apply_stop_") as td:
        workdir = Path(td)
        config_path = workdir / "start_test_bed.yaml"
        config_path.write_text(
            """
schema_version: 6
deployconf_path: ./deployconf_testbed.yml
controller_url: http://127.0.0.1:19080/r/ops/fluxon_testbed
controller_basic_auth:
  username: ops_admin
  password: ops_password
controller_ready_timeout_seconds: 30
bootstrap_stability_window_seconds: 1
required_inotify_max_user_watches: 1
required_inotify_max_user_instances: 1
test_runner_ui:
  enabled: true
  host: 0.0.0.0
  port: 18080
  workdir: ./ui_runtime
  history_lookback_days: 30
  gitops_config_path: ./gitops.yaml
bootstrap_phases:
  - mode: fixed_bare
    node: infra44-ThinkStation-PX
    services:
      - etcd
  - mode: fixed_bare
    node: infra44-ThinkStation-PX
    services:
      - fluxon_core_controller
deploy_workloads:
  - fluxon_core_controller
  - fluxon_fs_agent
""".strip()
            + "\n",
            encoding="utf-8",
        )
        (workdir / "gitops.yaml").write_text("repos: []\n", encoding="utf-8")
        deployconf_path = workdir / "deployconf_testbed.yml"
        deployconf_path.write_text(
            """
namespace: fluxon_testbed
name_prefix: fluxon_testbed
global_envs:
  FLUXON_CLUSTER_NAME: fluxon_testbed
  FLUXON_RELEASE_SHA256_FILE: fluxon_release.sha256
  FLUXON_RELEASE_PYLIB_SRC_TAR: pylib_src.tar.gz
  FLUXON_RELEASE_WHEEL: fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl
cluster_nodes:
  - hostname: infra44-ThinkStation-PX
    ip: 127.0.0.1
    hostworkdir: /tmp/fluxon_testbed
    ssh_user: tester
    ssh_port: 22
    ssh_password: test-password
atomic_groups:
  fluxon_core_controller:
    phase: 1
    nodes: ["infra44-ThinkStation-PX"]
    services: ["master", "owner", "ops_controller", "ops_agent"]
bootstrap_bare_services: ["etcd"]
service:
  etcd:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  master:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  owner:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  ops_controller:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  ops_agent:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  fluxon_fs_agent:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
""".strip()
            + "\n",
            encoding="utf-8",
        )

        daemonset_dir = workdir / "gen_k8s_daemonset"
        daemonset_dir.mkdir(parents=True, exist_ok=True)
        (daemonset_dir / "fluxon_core_controller.daemonset.yaml").write_text(
            "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: fluxon_testbed-fluxon_core_controller__master\n",
            encoding="utf-8",
        )
        (daemonset_dir / "fluxon_fs_agent.daemonset.yaml").write_text(
            "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: fluxon_testbed-fluxon_fs_agent\n",
            encoding="utf-8",
        )

        original_read_local_release_manifest_sha256 = module._read_local_release_manifest_sha256
        original_with_release_manifest_sha256_env = module._with_release_manifest_sha256_env
        original_generate_daemonset_artifacts = module._generate_daemonset_artifacts
        original_refresh_cluster_bare_deploy_scripts = module._refresh_cluster_bare_deploy_scripts
        original_is_controller_initially_reachable = module._is_controller_initially_reachable
        original_run_bare_waves = module._run_bare_waves
        original_wait_controller_ready_stable = module._wait_controller_ready_stable
        original_wait_controller_agents_ready = module._wait_controller_agents_ready
        original_load_deploy_payload = module._load_deploy_payload
        original_acquire_bootstrap_target_lock = module._acquire_bootstrap_target_lock
        original_validate_release_generation_prerequisites = module._validate_release_generation_prerequisites
        original_validate_bare_bootstrap_prerequisites = module._validate_bare_bootstrap_prerequisites
        original_validate_ops_agent_snapshot_prerequisites = module._validate_ops_agent_snapshot_prerequisites
        original_deploy_controller_payload = module._deploy_controller_payload
        original_wait_apply_id = module._wait_apply_id
        original_run_local_stop = module._run_local_stop
        original_run_remote_stop = module._run_remote_stop
        original_ensure_test_runner_ui_started = module._ensure_test_runner_ui_started

        deploy_calls: list[list[str]] = []
        wait_calls: list[str] = []
        stop_calls: list[tuple[str, str]] = []
        ops_agent_snapshot_validation_calls: list[dict[str, object]] = []
        call_sequence: list[str] = []

        try:
            module._read_local_release_manifest_sha256 = lambda **_: "sha256"
            module._with_release_manifest_sha256_env = lambda **kwargs: kwargs["deployconf"]
            module._generate_daemonset_artifacts = lambda **_: None
            module._refresh_cluster_bare_deploy_scripts = lambda **_: None
            module._is_controller_initially_reachable = lambda **_: False
            module._run_bare_waves = lambda **_: None
            module._wait_controller_ready_stable = lambda **_: call_sequence.append("wait")
            module._wait_controller_agents_ready = lambda **_: call_sequence.append("agents_ready")
            module._load_deploy_payload = (
                lambda **kwargs: "\n".join(kwargs["deploy_workloads"])
            )
            module._acquire_bootstrap_target_lock = lambda **_: io.StringIO()
            module._validate_release_generation_prerequisites = lambda **_: None
            module._validate_bare_bootstrap_prerequisites = lambda **_: None
            module._validate_ops_agent_snapshot_prerequisites = (
                lambda **kwargs: ops_agent_snapshot_validation_calls.append(kwargs)
            )
            module._ensure_test_runner_ui_started = lambda **kwargs: (
                call_sequence.append("ui") or {
                    "enabled": True,
                    "status": "started",
                    "host": kwargs["ui_cfg"]["host"],
                    "port": kwargs["ui_cfg"]["port"],
                    "url": kwargs["ui_cfg"]["url"],
                    "probe_url": kwargs["ui_cfg"]["probe_url"],
                    "workdir": str(kwargs["ui_cfg"]["workdir"]),
                    "log_path": str(kwargs["ui_cfg"]["log_path"]),
                    "history_lookback_days": kwargs["ui_cfg"]["history_lookback_days"],
                    "history_roots": [str(path) for path in kwargs["ui_cfg"]["history_roots"]],
                    "gitops_config_path": str(kwargs["ui_cfg"]["gitops_config_path"]),
                    "reused_existing": False,
                    "pid": 12345,
                }
            )

            def _fake_deploy_controller_payload(*, yaml_text: str, **_: object) -> dict[str, str]:
                workloads = [line for line in yaml_text.splitlines() if line.strip()]
                call_sequence.append("deploy")
                deploy_calls.append(workloads)
                return {"apply_id": "apply-" + "-".join(workloads)}

            def _fake_wait_apply_id(*, apply_id: str, **_: object) -> dict[str, str]:
                wait_calls.append(apply_id)
                return {"apply_id": apply_id, "status": "ready"}

            def _fail_local_stop(*, service_name: str, **_: object) -> None:
                stop_calls.append(("local", service_name))
                raise AssertionError(f"unexpected local stop: {service_name}")

            def _fail_remote_stop(*, node_name: str, service_name: str, **_: object) -> None:
                stop_calls.append((node_name, service_name))
                raise AssertionError(f"unexpected remote stop: node={node_name} service={service_name}")

            module._deploy_controller_payload = _fake_deploy_controller_payload
            module._wait_apply_id = _fake_wait_apply_id
            module._run_local_stop = _fail_local_stop
            module._run_remote_stop = _fail_remote_stop

            argv = [
                "start_test_bed.py",
                "-c",
                str(config_path),
                "-w",
                str(workdir),
                "--bootstrap-mode",
                module.BOOTSTRAP_MODE_BARE_THEN_APPLY,
            ]
            original_argv = sys.argv[:]
            try:
                sys.argv = argv
                module.main()
            finally:
                sys.argv = original_argv
        finally:
            module._read_local_release_manifest_sha256 = original_read_local_release_manifest_sha256
            module._with_release_manifest_sha256_env = original_with_release_manifest_sha256_env
            module._generate_daemonset_artifacts = original_generate_daemonset_artifacts
            module._refresh_cluster_bare_deploy_scripts = original_refresh_cluster_bare_deploy_scripts
            module._is_controller_initially_reachable = original_is_controller_initially_reachable
            module._run_bare_waves = original_run_bare_waves
            module._wait_controller_ready_stable = original_wait_controller_ready_stable
            module._wait_controller_agents_ready = original_wait_controller_agents_ready
            module._load_deploy_payload = original_load_deploy_payload
            module._acquire_bootstrap_target_lock = original_acquire_bootstrap_target_lock
            module._validate_release_generation_prerequisites = original_validate_release_generation_prerequisites
            module._validate_bare_bootstrap_prerequisites = original_validate_bare_bootstrap_prerequisites
            module._validate_ops_agent_snapshot_prerequisites = original_validate_ops_agent_snapshot_prerequisites
            module._deploy_controller_payload = original_deploy_controller_payload
            module._wait_apply_id = original_wait_apply_id
            module._run_local_stop = original_run_local_stop
            module._run_remote_stop = original_run_remote_stop
            module._ensure_test_runner_ui_started = original_ensure_test_runner_ui_started

        assert deploy_calls == [["fluxon_core_controller"], ["fluxon_fs_agent"]], (
            f"unexpected deploy calls: {deploy_calls!r}"
        )
        assert wait_calls == [
            "apply-fluxon_core_controller",
            "apply-fluxon_fs_agent",
        ], f"unexpected wait calls: {wait_calls!r}"
        assert call_sequence[:4] == ["wait", "ui", "agents_ready", "deploy"], call_sequence
        assert stop_calls == [], f"success path must not invoke post-apply stop: {stop_calls!r}"
        assert len(ops_agent_snapshot_validation_calls) == 1, ops_agent_snapshot_validation_calls
        print("PASS: test_bare_then_apply_success_path_does_not_run_post_apply_stop")


def test_bare_only_stops_after_controller_ready() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_bare_only_") as td:
        workdir = Path(td)
        config_path = workdir / "start_test_bed.yaml"
        config_path.write_text(
            """
schema_version: 6
deployconf_path: ./deployconf_testbed.yml
controller_url: http://127.0.0.1:19080/r/ops/fluxon_testbed
controller_basic_auth:
  username: ops_admin
  password: ops_password
controller_ready_timeout_seconds: 30
bootstrap_stability_window_seconds: 1
required_inotify_max_user_watches: 1
required_inotify_max_user_instances: 1
test_runner_ui:
  enabled: true
  host: 0.0.0.0
  port: 18080
  workdir: ./ui_runtime
  history_lookback_days: 30
  gitops_config_path: ./gitops.yaml
bootstrap_phases:
  - mode: fixed_bare
    node: infra44-ThinkStation-PX
    services:
      - etcd
  - mode: fixed_bare
    node: infra44-ThinkStation-PX
    services:
      - fluxon_core_controller
  - mode: coverage_bare
    services:
      - owner
      - ops_controller
deploy_workloads:
  - fluxon_core_controller
  - fluxon_fs_agent
""".strip()
            + "\n",
            encoding="utf-8",
        )
        (workdir / "gitops.yaml").write_text("repos: []\n", encoding="utf-8")
        deployconf_path = workdir / "deployconf_testbed.yml"
        deployconf_path.write_text(
            """
namespace: fluxon_testbed
name_prefix: fluxon_testbed
global_envs:
  FLUXON_CLUSTER_NAME: fluxon_testbed
  FLUXON_RELEASE_SHA256_FILE: fluxon_release.sha256
  FLUXON_RELEASE_PYLIB_SRC_TAR: pylib_src.tar.gz
  FLUXON_RELEASE_WHEEL: fluxon-0.2.1-cp38-abi3-manylinux_2_28_x86_64.whl
cluster_nodes:
  - hostname: infra44-ThinkStation-PX
    ip: 127.0.0.1
    hostworkdir: /tmp/fluxon_testbed
    ssh_user: tester
    ssh_port: 22
    ssh_password: test-password
atomic_groups:
  fluxon_core_controller:
    phase: 1
    nodes: ["infra44-ThinkStation-PX"]
    services: ["master", "owner", "ops_controller", "ops_agent"]
bootstrap_bare_services: ["etcd"]
service:
  etcd:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  greptime:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  master:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  owner:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  ops_controller:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  ops_agent:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
  fluxon_fs_agent:
    node_bind:
      node: ["infra44-ThinkStation-PX"]
""".strip()
            + "\n",
            encoding="utf-8",
        )

        original_read_local_release_manifest_sha256 = module._read_local_release_manifest_sha256
        original_with_release_manifest_sha256_env = module._with_release_manifest_sha256_env
        original_generate_daemonset_artifacts = module._generate_daemonset_artifacts
        original_refresh_cluster_bare_deploy_scripts = module._refresh_cluster_bare_deploy_scripts
        original_is_controller_initially_reachable = module._is_controller_initially_reachable
        original_run_bare_waves = module._run_bare_waves
        original_wait_controller_ready_stable = module._wait_controller_ready_stable
        original_acquire_bootstrap_target_lock = module._acquire_bootstrap_target_lock
        original_validate_release_generation_prerequisites = module._validate_release_generation_prerequisites
        original_validate_bare_bootstrap_prerequisites = module._validate_bare_bootstrap_prerequisites
        original_validate_ops_agent_snapshot_prerequisites = module._validate_ops_agent_snapshot_prerequisites
        original_deploy_controller_payload = module._deploy_controller_payload
        original_wait_apply_id = module._wait_apply_id
        original_ensure_test_runner_ui_started = module._ensure_test_runner_ui_started

        run_calls: list[tuple[str, object]] = []

        try:
            module._read_local_release_manifest_sha256 = lambda **_: "sha256"
            module._with_release_manifest_sha256_env = lambda **kwargs: kwargs["deployconf"]
            module._generate_daemonset_artifacts = lambda **_: run_calls.append(("generate", None))
            module._refresh_cluster_bare_deploy_scripts = lambda **_: run_calls.append(("refresh_bare", None))
            module._is_controller_initially_reachable = lambda **_: False
            module._run_bare_waves = lambda **kwargs: run_calls.append(("bare", kwargs["waves"]))
            module._wait_controller_ready_stable = lambda **kwargs: run_calls.append(("wait", kwargs["controller_url"]))
            module._acquire_bootstrap_target_lock = lambda **_: io.StringIO()
            module._validate_release_generation_prerequisites = lambda **_: None
            module._validate_bare_bootstrap_prerequisites = lambda **_: None
            module._validate_ops_agent_snapshot_prerequisites = lambda **_: None
            module._ensure_test_runner_ui_started = lambda **kwargs: (
                run_calls.append(("ui", kwargs["ui_cfg"]["url"])) or {
                    "enabled": True,
                    "status": "started",
                    "host": kwargs["ui_cfg"]["host"],
                    "port": kwargs["ui_cfg"]["port"],
                    "url": kwargs["ui_cfg"]["url"],
                    "probe_url": kwargs["ui_cfg"]["probe_url"],
                    "workdir": str(kwargs["ui_cfg"]["workdir"]),
                    "log_path": str(kwargs["ui_cfg"]["log_path"]),
                    "history_lookback_days": kwargs["ui_cfg"]["history_lookback_days"],
                    "history_roots": [str(path) for path in kwargs["ui_cfg"]["history_roots"]],
                    "gitops_config_path": str(kwargs["ui_cfg"]["gitops_config_path"]),
                    "reused_existing": False,
                    "pid": 23456,
                }
            )

            def _fail_deploy_or_wait(**_: object) -> dict[str, str]:
                raise AssertionError("bare_only must not call deploy/apply")

            module._deploy_controller_payload = _fail_deploy_or_wait
            module._wait_apply_id = _fail_deploy_or_wait

            argv = [
                "start_test_bed.py",
                "-c",
                str(config_path),
                "-w",
                str(workdir),
                "--bootstrap-mode",
                module.BOOTSTRAP_MODE_BARE_ONLY,
            ]
            original_argv = sys.argv[:]
            try:
                sys.argv = argv
                module.main()
            finally:
                sys.argv = original_argv
        finally:
            module._read_local_release_manifest_sha256 = original_read_local_release_manifest_sha256
            module._with_release_manifest_sha256_env = original_with_release_manifest_sha256_env
            module._generate_daemonset_artifacts = original_generate_daemonset_artifacts
            module._refresh_cluster_bare_deploy_scripts = original_refresh_cluster_bare_deploy_scripts
            module._is_controller_initially_reachable = original_is_controller_initially_reachable
            module._run_bare_waves = original_run_bare_waves
            module._wait_controller_ready_stable = original_wait_controller_ready_stable
            module._acquire_bootstrap_target_lock = original_acquire_bootstrap_target_lock
            module._validate_release_generation_prerequisites = original_validate_release_generation_prerequisites
            module._validate_bare_bootstrap_prerequisites = original_validate_bare_bootstrap_prerequisites
            module._validate_ops_agent_snapshot_prerequisites = original_validate_ops_agent_snapshot_prerequisites
            module._deploy_controller_payload = original_deploy_controller_payload
            module._wait_apply_id = original_wait_apply_id
            module._ensure_test_runner_ui_started = original_ensure_test_runner_ui_started

        summary_text = (workdir / "start_test_bed_summary.yaml").read_text(encoding="utf-8")
        summary = yaml.safe_load(summary_text)
        assert "bootstrap_mode: bare_only" in summary_text, summary_text
        assert "deploy_response_atomic: null" in summary_text, summary_text
        assert [item[0] for item in run_calls] == ["generate", "refresh_bare", "bare", "wait", "ui"], run_calls
        assert run_calls[2][1] == [
            {
                "launches": [
                    {
                        "node": "infra44-ThinkStation-PX",
                        "selection_name": "etcd",
                    }
                ]
            },
            {
                "launches": [
                    {
                        "node": "infra44-ThinkStation-PX",
                        "selection_name": "fluxon_core_controller",
                    }
                ]
            },
        ], run_calls
        assert summary["test_runner_ui"]["status"] == "started", summary
        assert summary["test_runner_ui_status"] == "started", summary
        assert summary["test_runner_ui_url"] == "http://0.0.0.0:18080", summary
        assert summary["test_runner_ui_pid"] == 23456, summary
        print("PASS: test_bare_only_stops_after_controller_ready")


def test_initial_controller_handover_uses_local_bare_stop() -> None:
    module = _load_start_test_bed_module()
    with tempfile.TemporaryDirectory(prefix="test_start_test_bed_controller_handover_stop_") as td:
        workdir = Path(td)
        local_node_cfg = {
            "hostname": "infra44-ThinkStation-PX-a",
            "ip": "127.0.0.1",
            "hostworkdir": str(workdir / "hostworkdir"),
            "execution_mode": "local",
            "ssh_user": "tester",
            "ssh_port": 22,
        }
        deployconf = {
            "name_prefix": "fluxon_testbed",
            "atomic_groups": {
                "fluxon_core_controller": {
                    "phase": 1,
                    "nodes": ["infra44-ThinkStation-PX-a"],
                    "services": ["master", "owner", "ops_controller", "ops_agent"],
                }
            },
            "service": {
                "master": {"node_bind": {"node": ["infra44-ThinkStation-PX-a"]}},
                "owner": {"node_bind": {"node": ["infra44-ThinkStation-PX-a"]}},
                "ops_controller": {"node_bind": {"node": ["infra44-ThinkStation-PX-a"]}},
                "ops_agent": {"node_bind": {"node": ["infra44-ThinkStation-PX-a"]}},
            },
        }
        stop_calls: list[str] = []
        original_run_local_stop = module._run_local_stop
        try:
            module._run_local_stop = (
                lambda *, local_node_cfg, service_name: stop_calls.append(
                    f"{local_node_cfg['hostname']}:{service_name}"
                )
            )
            stopped = module._stop_local_controller_handover_selections(
                deployconf=deployconf,
                local_node_cfg=local_node_cfg,
                selection_names=["fluxon_core_controller", "fluxon_core_controller"],
            )
        finally:
            module._run_local_stop = original_run_local_stop
        assert stopped == ["master", "owner", "ops_controller", "ops_agent"], stopped
        assert stop_calls == [
            "infra44-ThinkStation-PX-a:master",
            "infra44-ThinkStation-PX-a:owner",
            "infra44-ThinkStation-PX-a:ops_controller",
            "infra44-ThinkStation-PX-a:ops_agent",
        ], stop_calls
    print("PASS: test_initial_controller_handover_uses_local_bare_stop")


def main() -> int:
    parser = argparse.ArgumentParser(description="start_test_bed bootstrap log test runner")
    parser.add_argument("--test-id", help="Run only one named test")
    args = parser.parse_args()

    checks: list[tuple[str, Callable[[], None]]] = [
        ("zero_rc_is_success", test_zero_rc_is_success),
        ("rc255_plain_marker", test_rc255_recovers_from_plain_ready_marker),
        ("rc255_atomic_marker", test_rc255_recovers_from_atomic_ready_marker),
        ("direct_supervisor_status_path_is_rejected", test_direct_supervisor_status_path_is_rejected),
        ("ops_agent_snapshot_payload_rejects_empty_file", test_ops_agent_snapshot_payload_rejects_empty_file),
        ("ops_agent_snapshot_payload_accepts_valid_json", test_ops_agent_snapshot_payload_accepts_valid_json),
        ("ops_agent_snapshot_prereq_allows_missing_file", test_ops_agent_snapshot_prereq_allows_missing_file),
        (
            "start_test_bed_release_scope_rejects_missing_ext_images_manifest_reference",
            test_start_test_bed_release_scope_rejects_missing_ext_images_manifest_reference,
        ),
        (
            "start_test_bed_release_manifest_scope_contract_accepts_ext_manifest",
            test_start_test_bed_release_manifest_scope_contract_accepts_ext_manifest,
        ),
        (
            "start_test_bed_release_scope_dispatches_ext_runtime_files_from_manifest",
            test_start_test_bed_release_scope_dispatches_ext_runtime_files_from_manifest,
        ),
        ("parse_cluster_nodes_accepts_local_execution_mode", test_parse_cluster_nodes_accepts_local_execution_mode),
        (
            "run_bare_waves_treats_local_execution_mode_node_as_local",
            test_run_bare_waves_treats_local_execution_mode_node_as_local,
        ),
        (
            "local_coverage_bootstrap_excludes_duplicate_local_control_plane_selection",
            test_local_coverage_bootstrap_excludes_duplicate_local_control_plane_selection,
        ),
        ("parse_test_runner_ui_config_resolves_paths", test_parse_test_runner_ui_config_resolves_paths),
        (
            "normalize_bootstrap_deployconf_strips_legacy_master_p2p_listen_port",
            test_normalize_bootstrap_deployconf_strips_legacy_master_p2p_listen_port,
        ),
        (
            "normalize_bootstrap_deployconf_rejects_missing_fluxon_fs_master_prometheus_base_url",
            test_normalize_bootstrap_deployconf_rejects_missing_fluxon_fs_master_prometheus_base_url,
        ),
        (
            "normalize_bootstrap_deployconf_rejects_missing_greptime_loopback_bind_addrs",
            test_normalize_bootstrap_deployconf_rejects_missing_greptime_loopback_bind_addrs,
        ),
        (
            "normalize_bootstrap_deployconf_rewrites_same_host_local_multi_node_fixed_ports",
            test_normalize_bootstrap_deployconf_rewrites_same_host_local_multi_node_fixed_ports,
        ),
        (
            "normalize_bootstrap_deployconf_keeps_non_local_or_single_node_ports_unchanged",
            test_normalize_bootstrap_deployconf_keeps_non_local_or_single_node_ports_unchanged,
        ),
        (
            "normalize_bootstrap_deployconf_promotes_master_port_from_entrypoint",
            test_normalize_bootstrap_deployconf_promotes_master_port_from_entrypoint,
        ),
        (
            "refresh_cluster_bare_deploy_scripts_copies_local_and_remote_nodes",
            test_refresh_cluster_bare_deploy_scripts_copies_local_and_remote_nodes,
        ),
        (
            "initial_controller_handover_uses_local_bare_stop",
            test_initial_controller_handover_uses_local_bare_stop,
        ),
        ("no_post_apply_stop", test_bare_then_apply_success_path_does_not_run_post_apply_stop),
        ("bare_only_stops_after_controller_ready", test_bare_only_stops_after_controller_ready),
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
