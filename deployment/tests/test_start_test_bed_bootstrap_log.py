#!/usr/bin/env python3

from __future__ import annotations

import argparse
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
        original_is_controller_initially_reachable = module._is_controller_initially_reachable
        original_run_bare_waves = module._run_bare_waves
        original_wait_controller_ready_stable = module._wait_controller_ready_stable
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
            module._is_controller_initially_reachable = lambda **_: False
            module._run_bare_waves = lambda **_: None
            module._wait_controller_ready_stable = lambda **_: call_sequence.append("wait")
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
            module._is_controller_initially_reachable = original_is_controller_initially_reachable
            module._run_bare_waves = original_run_bare_waves
            module._wait_controller_ready_stable = original_wait_controller_ready_stable
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
        assert call_sequence[:3] == ["wait", "ui", "deploy"], call_sequence
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
        assert [item[0] for item in run_calls] == ["generate", "bare", "wait", "ui"], run_calls
        assert run_calls[1][1] == [
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
        ("parse_test_runner_ui_config_resolves_paths", test_parse_test_runner_ui_config_resolves_paths),
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
