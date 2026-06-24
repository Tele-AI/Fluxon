from __future__ import annotations

import json
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional


def _prepare_ci_case(
    *,
    ctx: Any,
    planned_case: Any,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    case_plan: Any,
    runtime_tracking: Any,
) -> Any:
    _ = ctx._require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    ci_checkout_root = ctx._runner_repo_root()
    runtime_tracking.ci_lock_fp = ctx._acquire_ci_lock()
    ctx._ensure_deployer_online(resolved_case)
    out_cluster_name = ctx._ci_cluster_name(resolved_case)

    scale = ctx._require_dict(resolved_case.get("scale"), "resolved_case.scale")
    owner_scale = ctx._require_dict(scale.get("owner"), "resolved_case.scale.owner")
    owner_count = ctx._require_int(owner_scale.get("owner_count"), "scale.owner.owner_count", min_v=1)
    if owner_count != 1:
        raise ValueError("CI currently supports only owner_count=1")
    owner_dram_bytes = ctx._require_int(
        owner_scale.get("owner_dram_bytes"), "scale.owner.owner_dram_bytes", min_v=16777216
    )
    if owner_dram_bytes % 16777216 != 0:
        raise ValueError("scale.owner.owner_dram_bytes must be 16MiB aligned")

    release_root = ctx._resolved_case_release_root(resolved_case)
    if not release_root.exists():
        raise ValueError(f"materialized case release_root is missing: {release_root}")

    if ctx._ci_has_instance(resolved_case, instance_id="owner_0"):
        owner0 = ctx._find_deploy_instance(resolved_case, instance_id="owner_0")
        ci_runner = ctx._find_deploy_instance(resolved_case, instance_id="ci_runner")
        owner0_target = ctx._require_str(
            ctx._require_dict(owner0.get("deployer"), "owner_0.deployer").get("target"),
            "owner_0.target",
        )
        ci_target = ctx._require_str(
            ctx._require_dict(ci_runner.get("deployer"), "ci_runner.deployer").get("target"),
            "ci_runner.target",
        )
        if owner0_target != ci_target:
            raise ValueError("ci_runner must run on the same target as owner_0")

    ctx._ci_cleanup_runtime(resolved_case, timeout_s=120)
    ctx._cleanup_previous_failed_ci_runtime(
        resolved_case,
        run_dir=run_dir,
        run_index=run_index,
    )
    ctx._ci_assert_ports_free(resolved_case)
    ctx._wait_ci_base_runtime_ready(resolved_case)

    services_root = (run_dir / "services").resolve()
    services_root.mkdir(parents=True, exist_ok=True)
    (services_root / "share_mem").mkdir(parents=True, exist_ok=True)
    share_mem_path = ctx._ci_share_mem_path(resolved_case, run_dir=run_dir)
    Path(share_mem_path).mkdir(parents=True, exist_ok=True)

    venv_python = ctx._create_ci_runtime_venv(run_dir=run_dir)

    src_root = (run_dir / "src").resolve()
    ctx._ci_prepare_run_inputs(
        resolved_case=resolved_case,
        source_root=ci_checkout_root,
        release_root=release_root,
        test_rsc_root=ctx._resolved_case_test_rsc_root(resolved_case),
        src_root=src_root,
        venv_python=venv_python,
        ci_commands=planned_case.ci_commands,
        overlay_live_checkout=True,
        etcd_address=f"{ctx._ci_base_runtime_service_target_ip(resolved_case, service_id='etcd')}:{ctx._ci_base_runtime_service_port(resolved_case, service_id='etcd')}",
        cluster_name=out_cluster_name,
        share_mem_path=share_mem_path,
    )

    prepare_env_exports = ctx._run_ci_prepare_steps(
        resolved_case=resolved_case,
        run_dir=run_dir,
        src_root=src_root,
    )
    if prepare_env_exports:
        ctx._write_ci_prepare_env_script(run_dir=run_dir, exports=prepare_env_exports)

    profile = ctx._require_dict(resolved_case.get("profile"), "resolved_case.profile")
    profile_ci = ctx._require_dict(profile.get("ci"), "resolved_case.profile.ci")
    if profile_ci.get("scene_config") is not None:
        ctx._write_ci_scene_config_yaml(
            resolved_case,
            run_dir=run_dir,
        )
    if ctx._ci_cluster_runtime_instance_ids(resolved_case):
        ctx._write_ci_master_owner_configs(
            resolved_case,
            run_dir=run_dir,
            cluster_name=out_cluster_name,
            share_mem_path=share_mem_path,
            owner_dram_bytes=owner_dram_bytes,
        )
    _ = ctx._write_ci_runner_script(
        resolved_case,
        run_dir=run_dir,
        src_root=src_root,
        share_mem_path=share_mem_path,
    )
    ci_runner_exit_code_path = (run_dir / "logs" / "ci_runner" / "exit_code.txt").resolve()
    ci_runner_exit_code_baseline = ctx._observe_file_state(ci_runner_exit_code_path)

    for cluster_runtime_phase in case_plan.prepare_phases:
        cluster_runtime_deploy_result = ctx._deploy_runtime_phase(
            resolved_case,
            run_dir=run_dir,
            phase=cluster_runtime_phase,
        )
        for instance_id in cluster_runtime_phase.instance_ids:
            ctx._record_ci_apply_id(
                runtime_tracking.ci_attempted_instance_ids,
                runtime_tracking.ci_apply_ids,
                instance_id=instance_id,
                deploy_result=cluster_runtime_deploy_result,
                ctx=f"CI cluster_runtime deploy_result[{instance_id}]",
            )
        for instance_id in cluster_runtime_phase.instance_ids:
            ctx._wait_ci_instance_ready(resolved_case, instance_id=instance_id)
    return ctx._PreparedCase(
        plan=case_plan,
        ci_runner_exit_code_baseline=ci_runner_exit_code_baseline,
    )


def _prepare_test_stack_case(
    *,
    ctx: Any,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    case_plan: Any,
    test_stack_meta: Dict[str, Any],
    runtime_tracking: Any,
) -> Any:
    ctx._ensure_deployer_online(resolved_case)
    deploy = ctx._require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = ctx._require_str(deploy.get("controller_url"), "resolved_case.deploy.controller_url").rstrip("/")
    case_obj = ctx._require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = ctx._require_str(case_obj.get("case_id"), "resolved_case.case.case_id")
    ctx._cleanup_skipped_case_desired_applies(controller_url=controller_url, case_id=case_id)
    ctx._write_deployer_manifests(resolved_case, run_dir, allow_overwrite=False)

    scale = ctx._require_dict(resolved_case.get("scale"), "resolved_case.scale")
    max_secs = ctx._require_int(scale.get("duration_seconds"), "scale.duration_seconds", min_v=1)
    benchmark_scale = ctx._require_dict(
        scale.get("benchmark"),
        "resolved_case.scale.benchmark",
    )
    metric_warmup_seconds = ctx._require_number(
        benchmark_scale.get("metric_warmup_seconds"),
        "resolved_case.scale.benchmark.metric_warmup_seconds",
    )
    profile = ctx._require_dict(resolved_case.get("profile"), "resolved_case.profile")
    profile_test_stack = ctx._require_dict(profile.get("test_stack"), "resolved_case.profile.test_stack")
    coordinator_ready_timeout_seconds = ctx._require_int(
        profile_test_stack.get("coordinator_ready_timeout_seconds"),
        "resolved_case.profile.test_stack.coordinator_ready_timeout_seconds",
        min_v=1,
    )

    coordinator_addr = ctx._require_str(test_stack_meta.get("coordinator_addr"), "test_stack_meta.coordinator_addr")
    if ":" not in coordinator_addr:
        raise ValueError(f"invalid coordinator_addr: {coordinator_addr!r}")
    coord_host, coord_port_s = coordinator_addr.rsplit(":", 1)
    coord_port = int(coord_port_s)
    coordinator_phase = ctx._require_runtime_phase_by_id(
        case_plan.prepare_phases,
        phase_id="coordinator",
        ctx="TEST_STACK prepare",
    )
    scene = ctx._require_dict(resolved_case.get("scene"), "resolved_case.scene")
    ts_scene = ctx._require_dict(scene.get("test_stack"), "resolved_case.scene.test_stack")
    mode = ctx._require_str(ts_scene.get("mode"), "scene.test_stack.mode")
    backend_kind = ctx._require_test_stack_backend_kind(
        profile_test_stack.get("kind"),
        "resolved_case.profile.test_stack.kind",
    )
    owner_instance_ids: List[str] = []
    share_mem_path: Optional[str] = None
    stack_cluster_name: Optional[str] = None
    if ctx._test_stack_backend_uses_dedicated_kv_owners(backend_kind=backend_kind, mode=mode):
        runtime = ctx._require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
        owner_instance_ids = [
            iid
            for iid in coordinator_phase.instance_ids
            if isinstance(iid, str) and iid.startswith(ctx.TEST_STACK_KV_OWNER_INSTANCE_ID_PREFIX)
        ]
        if not owner_instance_ids:
            raise ValueError(
                f"{mode} requires dedicated KV owner instances (missing kv_owner_* in prepare phase)"
            )
        if ctx._test_stack_backend_uses_external_fluxon_kv(backend_kind=backend_kind, mode=mode):
            stack_identity = ctx._require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
            stack_cluster_name = ctx._require_str(
                stack_identity.get("cluster_name"),
                "runtime.stack_identity.cluster_name",
            )
            share_mem_path = ctx._require_str(
                stack_identity.get("share_mem_path"),
                "runtime.stack_identity.share_mem_path",
            )
            ctx._converge_test_stack_external_owner_shared_bundle_cleanup(
                resolved_case,
                controller_url=controller_url,
                owner_instance_ids=owner_instance_ids,
            )
    ctx._stage_runtime_phase_run_dir(resolved_case, run_dir=run_dir, phase=coordinator_phase)
    ctx._ensure_test_stack_runtime_env_ready_for_instance_ids(
        resolved_case,
        run_dir=run_dir,
        instance_ids=coordinator_phase.instance_ids,
    )
    runtime_tracking.ts_coord_deploy_attempted = True
    coord_deploy_result = ctx._deploy_runtime_phase_after_stage(
        resolved_case,
        run_dir=run_dir,
        phase=coordinator_phase,
    )
    runtime_tracking.ts_coord_apply_id = ctx._deploy_result_history_id(
        coord_deploy_result,
        ctx="TEST_STACK coordinator deploy_result",
    )
    ctx._wait_instance_running(resolved_case, instance_id="coordinator", timeout_s=30)
    ctx._wait_instance_tcp_ready(
        resolved_case,
        instance_id="coordinator",
        host=coord_host,
        port=coord_port,
        timeout_s=coordinator_ready_timeout_seconds,
    )
    if backend_kind == ctx.TEST_STACK_BACKEND_MOONCAKE:
        bench_cfg = ctx._load_test_stack_benchmark_config(run_dir)
        run_kv_base = ctx._require_dict(bench_cfg.get("kv_base"), "benchmark_config.CONFIG.kv_base")
        run_mooncake_spec = ctx._require_dict(
            run_kv_base.get("mooncake_spec"),
            "benchmark_config.CONFIG.kv_base.mooncake_spec",
        )
        metadata_server = ctx._require_str(
            run_mooncake_spec.get("metadata_server"),
            "benchmark_config.CONFIG.kv_base.mooncake_spec.metadata_server",
        )
        master_server_address = ctx._require_str(
            run_mooncake_spec.get("master_server_address"),
            "benchmark_config.CONFIG.kv_base.mooncake_spec.master_server_address",
        )
        metadata_host_port = urllib.parse.urlparse(metadata_server)
        metadata_port = int(metadata_host_port.port or 0)
        if metadata_port <= 0:
            raise ValueError(f"invalid TEST_STACK Mooncake metadata_server port: {metadata_server!r}")
        if ":" not in master_server_address:
            raise ValueError(f"invalid TEST_STACK Mooncake master_server_address: {master_server_address!r}")
        _, rpc_port_s = master_server_address.rsplit(":", 1)
        rpc_port = int(rpc_port_s)
        ctx._wait_instance_running(
            resolved_case,
            instance_id=ctx.TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID,
            timeout_s=60,
        )
        ctx._wait_instance_tcp_ready(
            resolved_case,
            instance_id=ctx.TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID,
            host=coord_host,
            port=rpc_port,
            timeout_s=coordinator_ready_timeout_seconds,
        )
        ctx._wait_instance_tcp_ready(
            resolved_case,
            instance_id=ctx.TEST_STACK_MOONCAKE_MASTER_INSTANCE_ID,
            host=coord_host,
            port=metadata_port,
            timeout_s=coordinator_ready_timeout_seconds,
        )

    node_runtime_phase = ctx._require_runtime_phase_by_id(
        case_plan.prepare_phases,
        phase_id="node_runtime",
        ctx="TEST_STACK prepare",
    )
    if ctx._test_stack_backend_uses_external_fluxon_kv(backend_kind=backend_kind, mode=mode):
        if share_mem_path is None or stack_cluster_name is None:
            raise ValueError(
                "internal error: TEST_STACK shared bundle identity is missing after pre-deploy cleanup"
            )
        if "master" in set(coordinator_phase.instance_ids):
            ctx._wait_instance_running(resolved_case, instance_id="master", timeout_s=60)
        bench = ctx._require_dict(
            ctx._require_dict(resolved_case.get("scale"), "resolved_case.scale").get("benchmark"),
            "scale.benchmark",
        )
        cluster_ready_timeout_seconds = ctx._require_int(
            bench.get("cluster_ready_timeout_seconds"),
            "scale.benchmark.cluster_ready_timeout_seconds",
            min_v=1,
        )
        for owner_id in owner_instance_ids:
            owner_target = ctx._instance_target_name(resolved_case, instance_id=owner_id)
            shared_bundle_paths = ctx._test_stack_external_owner_shared_bundle_paths(
                resolved_case,
                owner_target=owner_target,
            )
            ctx._wait_instance_running(resolved_case, instance_id=owner_id, timeout_s=60)
            ctx._wait_instance_files_present(
                resolved_case,
                instance_id=owner_id,
                paths=shared_bundle_paths,
                timeout_s=int(cluster_ready_timeout_seconds),
                ctx="TEST_STACK owner shared bundle",
            )
    elif ctx._test_stack_backend_uses_dedicated_kv_owners(backend_kind=backend_kind, mode=mode):
        for owner_id in owner_instance_ids:
            ctx._wait_instance_running(resolved_case, instance_id=owner_id, timeout_s=60)
    ctx._stage_runtime_phase_run_dir(resolved_case, run_dir=run_dir, phase=node_runtime_phase)
    ctx._ensure_test_stack_runtime_env_ready_for_instance_ids(
        resolved_case,
        run_dir=run_dir,
        instance_ids=node_runtime_phase.instance_ids,
    )
    return ctx._PreparedCase(
        plan=case_plan,
        test_stack_result_path=Path(
            ctx._require_str(test_stack_meta.get("result_path"), "test_stack_meta.result_path")
        ),
        test_stack_coordinator_addr=coordinator_addr,
        test_stack_result_timeout_s=_test_stack_result_timeout_seconds(
            max_benchmark_seconds=int(max_secs),
            metric_warmup_seconds=float(metric_warmup_seconds),
        ),
    )


def _execute_ci_case(
    *,
    ctx: Any,
    planned_case: Any,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    started_at: int,
    prepared_case: Any,
    runtime_tracking: Any,
) -> Any:
    ci_runner_exit_timeout_s = ctx._ci_runner_exit_code_timeout_seconds(resolved_case)
    ci_runner_phase = prepared_case.plan.execute_phases[0]
    ci_runner_deploy_result = ctx._deploy_runtime_phase(
        resolved_case,
        run_dir=run_dir,
        phase=ci_runner_phase,
    )
    ctx._record_ci_apply_id(
        runtime_tracking.ci_attempted_instance_ids,
        runtime_tracking.ci_apply_ids,
        instance_id="ci_runner",
        deploy_result=ci_runner_deploy_result,
        ctx="CI ci_runner deploy_result",
    )
    ctx._wait_ci_instance_ready(resolved_case, instance_id="ci_runner")
    rc = ctx._wait_ci_runner_exit_code(
        resolved_case=resolved_case,
        run_dir=run_dir,
        timeout_s=ci_runner_exit_timeout_s,
        baseline_state=_require_ci_runner_exit_code_baseline(
            prepared_case.ci_runner_exit_code_baseline,
        ),
    )
    outcome = ctx.RUN_OUTCOME_SUCCESS if rc == 0 else ctx.RUN_OUTCOME_FAILED
    if outcome == ctx.RUN_OUTCOME_SUCCESS and runtime_tracking.ci_apply_ids.get("ci_runner") is not None:
        ctx._delete_apply_id(
            resolved_case,
            apply_id=ctx._require_str(runtime_tracking.ci_apply_ids.get("ci_runner"), "CI ci_runner apply_id"),
            ctx="CI ci_runner apply",
        )
        del runtime_tracking.ci_apply_ids["ci_runner"]
    summary = ctx._build_ci_summary_yaml(
        resolved_case,
        run_index=run_index,
        started_at_unix_s=started_at,
        finished_at_unix_s=int(time.time()),
        outcome=outcome,
        counted=False,
        ci_out={"rc": rc},
    )
    for phase in prepared_case.plan.collect_phases:
        ctx._collect_runtime_phase(resolved_case, run_dir=run_dir, phase=phase)
    return ctx._ExecutedCase(outcome=outcome, summary=summary)


def _execute_test_stack_case(
    *,
    ctx: Any,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    run_index: int,
    started_at: int,
    prepared_case: Any,
    runtime_tracking: Any,
) -> Any:
    case_obj = ctx._require_dict(resolved_case.get("case"), "resolved_case.case")
    case_id = ctx._require_str(case_obj.get("case_id"), "case.case_id")

    outcome = ctx.RUN_OUTCOME_FAILED
    error_detail: Optional[str] = None
    collect_error_detail: Optional[str] = None
    result_obj: Optional[Dict[str, Any]] = None

    try:
        node_phase = prepared_case.plan.execute_phases[0]
        runtime_tracking.ts_nodes_deploy_attempted = True
        node_deploy_result = ctx._deploy_runtime_phase(
            resolved_case,
            run_dir=run_dir,
            phase=node_phase,
        )
        runtime_tracking.ts_nodes_apply_id = ctx._deploy_result_history_id(
            node_deploy_result,
            ctx="TEST_STACK node deploy_result",
        )

        result_path = _require_test_stack_result_path(prepared_case.test_stack_result_path)
        timeout_s = _require_test_stack_result_timeout(prepared_case.test_stack_result_timeout_s)
        result_obj = _wait_and_load_test_stack_benchmark_result_json(
            ctx=ctx,
            resolved_case=resolved_case,
            result_path=result_path,
            timeout_s=timeout_s,
            case_id=case_id,
            writer_instance_id="coordinator",
        )

        ctx._validate_test_stack_benchmark_result(result_obj, case_id=case_id)
        outcome = ctx.RUN_OUTCOME_SUCCESS
    except Exception as exc:  # noqa: BLE001
        error_detail = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            for phase in prepared_case.plan.collect_phases:
                ctx._collect_runtime_phase(resolved_case, run_dir=run_dir, phase=phase)
        except Exception as exc:  # noqa: BLE001
            collect_error_detail = f"{type(exc).__name__}: {exc}"

    summary = {
        "schema_version": ctx.SCHEMA_VERSION,
        "case_id": case_id,
        "case_key": ctx._require_str(case_obj.get("case_key"), "case.case_key"),
        "run_index": int(run_index),
        "outcome": outcome,
        "counted": False,
        "timing": {
            "started_at_unix_s": int(started_at),
            "finished_at_unix_s": int(time.time()),
        },
        "test_stack": {
            "coordinator_addr": ctx._require_str(
                prepared_case.test_stack_coordinator_addr,
                "prepared_case.test_stack_coordinator_addr",
            ),
            "completion_signal": "benchmark_result_json",
            "result_path": str(_require_test_stack_result_path(prepared_case.test_stack_result_path)),
            "result": result_obj,
            "error": error_detail,
            "collect_error": collect_error_detail,
        },
    }
    return ctx._ExecutedCase(outcome=outcome, summary=summary)


def _wait_and_load_test_stack_benchmark_result_json(
    *,
    ctx: Any,
    resolved_case: Dict[str, Any],
    result_path: Path,
    timeout_s: int,
    case_id: str,
    writer_instance_id: str,
) -> Dict[str, Any]:
    deadline = time.time() + float(timeout_s)
    last_err: Optional[str] = None
    while True:
        try:
            raw = ctx._instance_read_text_if_present(
                resolved_case,
                instance_id=writer_instance_id,
                path=result_path,
            )
            if raw is None:
                raise FileNotFoundError(
                    f"result file is not present yet: instance_id={writer_instance_id} path={result_path}"
                )
            parsed = json.loads(raw)
            result_obj = ctx._require_dict(parsed, "test_stack.benchmark_result")
            runs = ctx._require_list(result_obj.get("runs"), "benchmark_result.runs")
            if not runs:
                raise ValueError("benchmark_result.runs is empty")
            run0 = ctx._require_dict(runs[0], "benchmark_result.runs[0]")
            completion = ctx._require_dict(run0.get("completion"), "benchmark_result.runs[0].completion")
            _ = ctx._require_str(completion.get("status"), "benchmark_result.runs[0].completion.status")
            if not result_path.exists() or result_path.read_text(encoding="utf-8") != raw:
                result_path.write_text(raw, encoding="utf-8")
            return result_obj
        except Exception as exc:  # noqa: BLE001
            last_err = f"{type(exc).__name__}: {exc}"
            if time.time() >= deadline:
                raise ValueError(
                    f"benchmark result json did not become readable/valid within timeout: "
                    f"case_id={case_id} path={result_path} timeout_s={timeout_s} last_err={last_err}"
                ) from exc
            time.sleep(0.5)


def _finalize_case_runtime(
    *,
    ctx: Any,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    case_plan: Any,
    runtime_tracking: Any,
    outcome: str,
) -> None:
    if case_plan.case_family == ctx.CASE_FAMILY_CI:
        _finalize_ci_case_runtime(
            ctx=ctx,
            resolved_case=resolved_case,
            run_dir=run_dir,
            runtime_tracking=runtime_tracking,
            outcome=outcome,
        )
        return
    if case_plan.case_family == ctx.CASE_FAMILY_BENCH:
        _finalize_test_stack_case_runtime(
            ctx=ctx,
            resolved_case=resolved_case,
            runtime_tracking=runtime_tracking,
            outcome=outcome,
        )
        return
    raise ValueError(f"unsupported case family for finalize_case_runtime: {case_plan.case_family}")


def _finalize_ci_case_runtime(
    *,
    ctx: Any,
    resolved_case: Dict[str, Any],
    run_dir: Path,
    runtime_tracking: Any,
    outcome: str,
) -> None:
    case = ctx._require_dict(resolved_case.get("case"), "resolved_case.case")
    run_mode = ctx._require_str(case.get("run_mode"), "resolved_case.case.run_mode")
    ci_preserved_apply_ids: list[dict[str, str]] = []
    tracked_apply_entries = ctx._ci_runtime_tracked_apply_entries(runtime_tracking)
    for entry in tracked_apply_entries:
        apply_id = ctx._require_str(entry.get("apply_id"), "ci tracked apply entry.apply_id")
        instance_ids = ctx._require_list(entry.get("instance_ids"), "ci tracked apply entry.instance_ids")
        if not instance_ids:
            continue
        ci_preserved_apply_ids.append(
            {
                "instance_ids": [
                    ctx._require_str(raw_instance_id, "ci tracked apply entry.instance_ids[]")
                    for raw_instance_id in instance_ids
                ],
                "apply_id": apply_id,
            }
        )
    should_teardown = outcome == ctx.RUN_OUTCOME_SUCCESS or run_mode == ctx.RUN_MODE_FULL_ONCE
    if should_teardown:
        (run_dir / ctx.CI_PRESERVED_APPLY_IDS_FILENAME).unlink(missing_ok=True)
        for entry in reversed(tracked_apply_entries):
            apply_id = ctx._require_str(entry.get("apply_id"), "ci tracked apply entry.apply_id")
            instance_ids = ctx._require_list(entry.get("instance_ids"), "ci tracked apply entry.instance_ids")
            instance_id_text = ",".join(
                ctx._require_str(raw_instance_id, "ci tracked apply entry.instance_ids[]")
                for raw_instance_id in instance_ids
            )
            ctx._delete_apply_id(
                resolved_case,
                apply_id=apply_id,
                ctx=f"CI {instance_id_text} apply",
            )
        ctx._ci_cleanup_runtime(resolved_case, timeout_s=120)
        return
    if not ci_preserved_apply_ids:
        return
    ctx._write_yaml_file(
        run_dir / ctx.CI_PRESERVED_APPLY_IDS_FILENAME,
        {
            "schema_version": ctx.CI_PRESERVED_APPLY_IDS_SCHEMA_VERSION,
            "apply_ids": ci_preserved_apply_ids,
        },
    )
    print(
        "[CI preserve_runtime] "
        "case_id="
        f"{ctx._resolved_case_case_id(resolved_case)} outcome={outcome} apply_ids="
        + ", ".join(
            f"{','.join(ctx._require_list(entry.get('instance_ids'), 'ci preserved apply entry.instance_ids'))}={entry['apply_id']}"
            for entry in ci_preserved_apply_ids
        )
    )


def _finalize_test_stack_case_runtime(
    *,
    ctx: Any,
    resolved_case: Dict[str, Any],
    runtime_tracking: Any,
    outcome: str,
) -> None:
    case = ctx._require_dict(resolved_case.get("case"), "resolved_case.case")
    run_mode = ctx._require_str(case.get("run_mode"), "resolved_case.case.run_mode")
    ts_preserved_apply_ids: list[str] = []
    if runtime_tracking.ts_nodes_deploy_attempted and runtime_tracking.ts_nodes_apply_id is not None:
        ts_preserved_apply_ids.append(
            f"nodes={ctx._require_str(runtime_tracking.ts_nodes_apply_id, 'TEST_STACK node apply_id')}"
        )
    if runtime_tracking.ts_coord_deploy_attempted and runtime_tracking.ts_coord_apply_id is not None:
        ts_preserved_apply_ids.append(
            f"coordinator={ctx._require_str(runtime_tracking.ts_coord_apply_id, 'TEST_STACK coordinator apply_id')}"
        )
    should_teardown = outcome == ctx.RUN_OUTCOME_SUCCESS or run_mode == ctx.RUN_MODE_FULL_ONCE
    if should_teardown:
        if runtime_tracking.ts_nodes_deploy_attempted and runtime_tracking.ts_nodes_apply_id is not None:
            ctx._delete_apply_id(
                resolved_case,
                apply_id=ctx._require_str(runtime_tracking.ts_nodes_apply_id, "TEST_STACK node apply_id"),
                ctx="TEST_STACK node apply",
            )
        if runtime_tracking.ts_coord_deploy_attempted and runtime_tracking.ts_coord_apply_id is not None:
            ctx._delete_apply_id(
                resolved_case,
                apply_id=ctx._require_str(runtime_tracking.ts_coord_apply_id, "TEST_STACK coordinator apply_id"),
                ctx="TEST_STACK coordinator apply",
            )
        return
    if not ts_preserved_apply_ids:
        return
    print(
        "[TEST_STACK preserve_runtime] "
        f"case_id={ctx._resolved_case_case_id(resolved_case)} outcome={outcome} apply_ids={', '.join(ts_preserved_apply_ids)}"
    )


def _require_ci_runner_exit_code_baseline(
    baseline_state: Any,
) -> Any:
    return baseline_state


def _require_test_stack_result_path(result_path: Optional[Path]) -> Path:
    if result_path is None:
        raise ValueError("prepared_case.test_stack_result_path is missing")
    return result_path


def _require_test_stack_result_timeout(timeout_s: Optional[int]) -> int:
    if timeout_s is None or timeout_s < 1:
        raise ValueError("prepared_case.test_stack_result_timeout_s must be positive")
    return int(timeout_s)


def _test_stack_result_timeout_seconds(
    *,
    max_benchmark_seconds: int,
    metric_warmup_seconds: float,
) -> int:
    if max_benchmark_seconds <= 0:
        raise ValueError(
            f"max_benchmark_seconds must be > 0, got: {max_benchmark_seconds}"
        )
    if metric_warmup_seconds < 0.0:
        raise ValueError(
            f"metric_warmup_seconds must be >= 0, got: {metric_warmup_seconds}"
        )
    return int(
        float(max_benchmark_seconds)
        + float(metric_warmup_seconds)
        + 600.0
    )
