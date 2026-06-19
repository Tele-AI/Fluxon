#!/usr/bin/env python3

from __future__ import annotations

import argparse
import contextlib
import copy
import importlib.util
import io
import tempfile
import sys
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import yaml


def main() -> int:
    parser = argparse.ArgumentParser(description="Fluxon test_runner contract checks")
    parser.add_argument("--test-id", help="Run only the named test id")
    args = parser.parse_args()

    print("=" * 60)
    print("Testing fluxon_test_stack/test_runner.py contracts")
    print("=" * 60)

    try:
        checks = _build_checks(args.test_id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    failures = 0
    for _, check in checks:
        if not _run_check(check):
            failures += 1

    print("=" * 60)
    print("All tests completed!" if failures == 0 else f"Completed with {failures} failing check group(s)")
    print("=" * 60)
    return 0 if failures == 0 else 1


def _build_checks(selected_test_id: Optional[str]) -> List[Tuple[str, Callable[[], None]]]:
    checks: List[Tuple[str, Callable[[], None]]] = [
        (
            "tcp_thread_keeps_protocol_implicit",
            test_tcp_thread_keeps_protocol_implicit,
        ),
        (
            "explicit_protocol_is_preserved",
            test_explicit_protocol_is_preserved,
        ),
        (
            "suite_requires_benchmark_bundle_only_for_bench_cases",
            test_suite_requires_benchmark_bundle_only_for_bench_cases,
        ),
        (
            "ci_cluster_kv_owner_allows_single_node_topology",
            test_ci_cluster_kv_owner_allows_single_node_topology,
        ),
        (
            "ci_top_attention_doc_page_build_declares_setup_dev_env_prepare",
            test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare,
        ),
    ]
    if selected_test_id is None:
        return checks
    for check_id, check in checks:
        if check_id == selected_test_id:
            return [(check_id, check)]
    available = ", ".join(check_id for check_id, _ in checks)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def _run_check(check: Callable[[], None]) -> bool:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        check()
    output = buf.getvalue()
    if output:
        print(output, end="")
    return "FAIL" not in output


def _import_test_runner_module():
    repo_root = Path(__file__).resolve().parents[2]
    runner_dir = repo_root / "fluxon_test_stack"
    runner_path = runner_dir / "test_runner.py"
    sys.path.insert(0, str(runner_dir))
    try:
        spec = importlib.util.spec_from_file_location("fluxon_test_stack_test_runner", runner_path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)
        return mod
    finally:
        if sys.path and sys.path[0] == str(runner_dir):
            sys.path.pop(0)


_TEST_RUNNER = _import_test_runner_module()


def test_tcp_thread_keeps_protocol_implicit() -> None:
    kv_base = {
        "instance_key": "bench_base",
        "fluxonkv_spec": {"cluster_name": "bench"},
    }
    merged_test_spec_config = {
        "p2p_transport_impl": "tcp_thread",
        "transport_mode": "transfer_with_rpc",
    }
    actual = _TEST_RUNNER._resolve_test_stack_fluxon_protocol_cfg(
        kv_base=copy.deepcopy(kv_base),
        merged_test_spec_config=copy.deepcopy(merged_test_spec_config),
        ctx="profile.test_stack.runtime_config.kv_base",
    )
    if actual is not None:
        print(
            "FAIL: test_tcp_thread_keeps_protocol_implicit - "
            f"expected None, got {actual!r}"
        )
        return
    print("PASS: test_tcp_thread_keeps_protocol_implicit")


def test_explicit_protocol_is_preserved() -> None:
    kv_base = {
        "protocol": {"protocol_type": "rdma"},
    }
    actual = _TEST_RUNNER._resolve_test_stack_fluxon_protocol_cfg(
        kv_base=copy.deepcopy(kv_base),
        merged_test_spec_config={"p2p_transport_impl": "tcp_thread"},
        ctx="profile.test_stack.runtime_config.kv_base",
    )
    expected = {"protocol_type": "rdma"}
    if actual != expected:
        print(
            "FAIL: test_explicit_protocol_is_preserved - "
            f"expected {expected!r}, got {actual!r}"
        )
        return
    print("PASS: test_explicit_protocol_is_preserved")


def test_suite_requires_benchmark_bundle_only_for_bench_cases() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    suite_cfg_path = repo_root / "fluxon_test_stack" / "ci_test_list.yaml"
    suite_cfg = yaml.safe_load(suite_cfg_path.read_text(encoding="utf-8"))
    if not isinstance(suite_cfg, dict):
        print("FAIL: test_suite_requires_benchmark_bundle_only_for_bench_cases - suite config is not a mapping")
        return

    suite_for_contract = copy.deepcopy(suite_cfg)
    artifact_sets = suite_for_contract.get("artifact_sets")
    if not isinstance(artifact_sets, dict):
        print("FAIL: test_suite_requires_benchmark_bundle_only_for_bench_cases - artifact_sets is not a mapping")
        return
    for artifact_set in artifact_sets.values():
        if not isinstance(artifact_set, dict):
            continue
        release_artifacts = artifact_set.get("release_artifacts")
        if isinstance(release_artifacts, dict):
            python_wheel = release_artifacts.get("python_wheel")
            if isinstance(python_wheel, str) and python_wheel.strip():
                artifact_set["release_artifacts"] = {"wheel": python_wheel}

    suite_with_bench = _TEST_RUNNER._parse_suite_config(copy.deepcopy(suite_for_contract))
    resolved_with_bench = _TEST_RUNNER._expand_cases(suite_with_bench)
    if not _TEST_RUNNER._suite_requires_benchmark_bundle(
        suite=suite_with_bench,
        resolved_cases=resolved_with_bench,
    ):
        print(
            "FAIL: test_suite_requires_benchmark_bundle_only_for_bench_cases - "
            "expected bench-containing suite to require benchmark bundle"
        )
        return

    ci_only_cfg = copy.deepcopy(suite_for_contract)
    scenes = ci_only_cfg.get("scenes")
    if not isinstance(scenes, dict):
        print("FAIL: test_suite_requires_benchmark_bundle_only_for_bench_cases - scenes is not a mapping")
        return
    ci_only_cfg["scenes"] = {
        scene_id: scene
        for scene_id, scene in scenes.items()
        if isinstance(scene, dict) and scene.get("ci") is not None
    }
    suite_ci_only = _TEST_RUNNER._parse_suite_config(ci_only_cfg)
    resolved_ci_only = _TEST_RUNNER._expand_cases(suite_ci_only)
    if _TEST_RUNNER._suite_requires_benchmark_bundle(
        suite=suite_ci_only,
        resolved_cases=resolved_ci_only,
    ):
        print(
            "FAIL: test_suite_requires_benchmark_bundle_only_for_bench_cases - "
            "expected CI-only suite to skip benchmark bundle requirement"
        )
        return
    print("PASS: test_suite_requires_benchmark_bundle_only_for_bench_cases")


def test_ci_cluster_kv_owner_allows_single_node_topology() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    suite_cfg_path = repo_root / "fluxon_test_stack" / "ci_test_list.yaml"
    suite_cfg = yaml.safe_load(suite_cfg_path.read_text(encoding="utf-8"))
    if not isinstance(suite_cfg, dict):
        print("FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - suite config is not a mapping")
        return

    artifact_sets = suite_cfg.get("artifact_sets")
    if not isinstance(artifact_sets, dict):
        print("FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - artifact_sets is not a mapping")
        return
    for artifact_set in artifact_sets.values():
        if not isinstance(artifact_set, dict):
            continue
        release_artifacts = artifact_set.get("release_artifacts")
        if isinstance(release_artifacts, dict):
            python_wheel = release_artifacts.get("python_wheel")
            if isinstance(python_wheel, str) and python_wheel.strip():
                artifact_set["release_artifacts"] = {"wheel": python_wheel}

    suite_cfg["scenes"] = {
        "ci_kv": copy.deepcopy(_TEST_RUNNER._require_dict(suite_cfg.get("scenes"), "suite.scenes").get("ci_kv"))
    }
    if not isinstance(suite_cfg["scenes"]["ci_kv"], dict):
        print("FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - ci_kv scene missing")
        return
    suite_cfg["scenes"]["ci_kv"]["select"]["profiles"] = ["fluxon_tcp"]
    scales = suite_cfg.get("scales")
    if not isinstance(scales, dict):
        print("FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - scales is not a mapping")
        return
    n1_scale = scales.get("n1_kvowner_dram_3gib")
    if not isinstance(n1_scale, dict):
        print("FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - n1 scale missing")
        return
    targets = n1_scale.get("targets")
    if not isinstance(targets, dict):
        print("FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - n1 scale targets missing")
        return
    targets["primary"] = "infra44-ThinkStation-PX"

    suite = _TEST_RUNNER._parse_suite_config(copy.deepcopy(suite_cfg))
    cases = _TEST_RUNNER._expand_cases(suite)
    if len(cases) != 1:
        print(
            "FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - "
            f"expected exactly 1 case, got {len(cases)}"
        )
        return

    ci_scene = _TEST_RUNNER._require_dict(
        _TEST_RUNNER._require_dict(suite.scenes["ci_kv"], "suite.scenes[ci_kv]").get("ci"),
        "suite.scenes[ci_kv].ci",
    )
    planned_commands = copy.deepcopy(
        _TEST_RUNNER._parse_ci_commands(ci_scene.get("commands"), "suite.scenes[ci_kv].ci.commands")
    )
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        stack_identity = {
            "ops_cluster_name": "fluxon_testbed",
            "cluster_name": "fluxon_benchmark",
            "controller_url": "http://127.0.0.1:19080/r/ops/fluxon_testbed",
            "shared_memory_path": "/tmp/fluxon_test_stack_shm",
            "shared_file_path": "/tmp/fluxon_test_stack_shm_files",
        }
        resolved_case = _TEST_RUNNER._build_resolved_case_yaml(
            cases[0],
            suite,
            config_root=str(root / "config_root"),
            workdir_root=str(root / "workdir_root"),
            run_dir=str(root / "run_dir"),
            ci_commands=planned_commands,
            ci_prepare_steps=None,
            execution_label=cases[0].case_id,
            command_id=None,
            test_id=None,
            stack_identity=stack_identity,
        )
        _TEST_RUNNER._compile_ci_case(resolved_case)

        instances = _TEST_RUNNER._require_list(
            _TEST_RUNNER._require_dict(resolved_case.get("deploy"), "resolved_case.deploy").get("instances"),
            "resolved_case.deploy.instances",
        )
        placements = {
            _TEST_RUNNER._require_str(inst.get("id"), "deploy.instances[].id"): _TEST_RUNNER._require_str(
                _TEST_RUNNER._require_dict(inst.get("deployer"), "deploy.instances[].deployer").get("target"),
                "deploy.instances[].deployer.target",
            )
            for inst in instances
        }
        expected_target = "infra44-ThinkStation-PX"
        if placements.get("master") != expected_target:
            print(
                "FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - "
                f"master target mismatch: {placements.get('master')!r}"
            )
            return
        if placements.get("owner_0") != expected_target:
            print(
                "FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - "
                f"owner_0 target mismatch: {placements.get('owner_0')!r}"
            )
            return
        if placements.get("ci_runner") != expected_target:
            print(
                "FAIL: test_ci_cluster_kv_owner_allows_single_node_topology - "
                f"ci_runner target mismatch: {placements.get('ci_runner')!r}"
            )
            return
    print("PASS: test_ci_cluster_kv_owner_allows_single_node_topology")


def test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    suite_cfg_path = repo_root / "fluxon_test_stack" / "ci_test_list.yaml"
    suite_cfg = yaml.safe_load(suite_cfg_path.read_text(encoding="utf-8"))
    if not isinstance(suite_cfg, dict):
        print("FAIL: test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare - suite config is not a mapping")
        return

    suite_for_contract = copy.deepcopy(suite_cfg)
    artifact_sets = suite_for_contract.get("artifact_sets")
    if not isinstance(artifact_sets, dict):
        print("FAIL: test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare - artifact_sets is not a mapping")
        return
    for artifact_set in artifact_sets.values():
        if not isinstance(artifact_set, dict):
            continue
        release_artifacts = artifact_set.get("release_artifacts")
        if isinstance(release_artifacts, dict):
            python_wheel = release_artifacts.get("python_wheel")
            if isinstance(python_wheel, str) and python_wheel.strip():
                artifact_set["release_artifacts"] = {"wheel": python_wheel}

    suite = _TEST_RUNNER._parse_suite_config(suite_for_contract)
    scene = suite.scenes.get("ci_top_attention_doc_page_build")
    if not isinstance(scene, dict):
        print("FAIL: test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare - missing scene")
        return
    ci = scene.get("ci")
    if not isinstance(ci, dict):
        print("FAIL: test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare - scene.ci missing")
        return
    prepare = ci.get("prepare")
    expected = [
        {
            "kind": "setup_dev_env",
            "config": "setup_and_pack/setup_dev_env/doc_page_ci.yaml",
            "cache_relpath": ".cached/fluxon_doc_site/toolchain",
        }
    ]
    if prepare != expected:
        print(
            "FAIL: test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare - "
            f"expected {expected!r}, got {prepare!r}"
        )
        return
    print("PASS: test_ci_top_attention_doc_page_build_declares_setup_dev_env_prepare")


if __name__ == "__main__":
    raise SystemExit(main())
