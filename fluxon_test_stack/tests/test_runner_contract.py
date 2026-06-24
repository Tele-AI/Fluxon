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
            "ci_top_attention_doc_page_build_uses_online_docker_image",
            test_ci_top_attention_doc_page_build_uses_online_docker_image,
        ),
        (
            "ci_top_attention_mq_core_uses_cluster_kv_owner_runtime",
            test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime,
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


def test_ci_top_attention_doc_page_build_uses_online_docker_image() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    suite_cfg_path = repo_root / "fluxon_test_stack" / "ci_test_list.yaml"
    suite_cfg = yaml.safe_load(suite_cfg_path.read_text(encoding="utf-8"))
    if not isinstance(suite_cfg, dict):
        print("FAIL: test_ci_top_attention_doc_page_build_uses_online_docker_image - suite config is not a mapping")
        return

    suite_for_contract = copy.deepcopy(suite_cfg)
    suite = _TEST_RUNNER._parse_suite_config(suite_for_contract)
    cases = _TEST_RUNNER._expand_cases(suite)
    case = next(
        (
            item
            for item in cases
            if item.scene_id == "ci_top_attention_doc_page_build"
            and item.profile_id == "fluxon_tcp"
        ),
        None,
    )
    if case is None:
        print("FAIL: test_ci_top_attention_doc_page_build_uses_online_docker_image - missing doc page case")
        return
    planned = _TEST_RUNNER._build_ci_execution_plan(case, suite)
    if len(planned) != 1:
        print(
            "FAIL: test_ci_top_attention_doc_page_build_uses_online_docker_image - "
            f"expected one planned case, got {len(planned)}"
        )
        return
    prepare = planned[0].ci_prepare_steps
    expected = [
        {
            "kind": "online_docker_image",
            "image_ref": "hanbaoaaa/fluxon-doc-site-builder:quartz-v5.0.0-node-v24.16.0",
            "env": "FLUXON_DOC_SITE_DOCKER_IMAGE_REF",
        }
    ]
    if prepare != expected:
        print(
            "FAIL: test_ci_top_attention_doc_page_build_uses_online_docker_image - "
            f"expected {expected!r}, got {prepare!r}"
        )
        return
    print("PASS: test_ci_top_attention_doc_page_build_uses_online_docker_image")


def test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    suite_cfg_path = repo_root / "fluxon_test_stack" / "ci_test_list.yaml"
    suite_cfg = yaml.safe_load(suite_cfg_path.read_text(encoding="utf-8"))
    if not isinstance(suite_cfg, dict):
        print("FAIL: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime - suite config is not a mapping")
        return

    suite_for_contract = copy.deepcopy(suite_cfg)
    suite = _TEST_RUNNER._parse_suite_config(suite_for_contract)
    cases = _TEST_RUNNER._expand_cases(suite)
    case = next(
        (
            item
            for item in cases
            if item.scene_id == "ci_top_attention_mq_core"
            and item.profile_id == "fluxon_tcp"
        ),
        None,
    )
    if case is None:
        print("FAIL: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime - missing mq core case")
        return
    planned = _TEST_RUNNER._build_ci_execution_plan(case, suite)
    if len(planned) != 1:
        print(
            "FAIL: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime - "
            f"expected one planned case, got {len(planned)}"
        )
        return
    commands = planned[0].ci_commands
    if len(commands) != 1:
        print(
            "FAIL: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime - "
            f"expected one command, got {len(commands)}"
        )
        return
    command = commands[0]
    if command.get("id") != "top_attention_mq_core":
        print(
            "FAIL: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime - "
            f"unexpected command id: {command.get('id')!r}"
        )
        return
    command_text = command.get("command")
    if not isinstance(command_text, str) or "_mq_core.py --case-config __RUN_DIR__/configs/ci_scene_config.yaml" not in command_text:
        print(
            "FAIL: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime - "
            f"unexpected command: {command_text!r}"
        )
        return
    print("PASS: test_ci_top_attention_mq_core_uses_cluster_kv_owner_runtime")


if __name__ == "__main__":
    raise SystemExit(main())
