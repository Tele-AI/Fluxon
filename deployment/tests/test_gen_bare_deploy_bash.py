#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import os
import shlex
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
DEPLOYMENT_DIR = SCRIPT_DIR.parent
GENERATOR_PATH = DEPLOYMENT_DIR / "gen_bare_deploy_bash.py"


def main() -> int:
    parser = argparse.ArgumentParser(description="gen_bare_deploy_bash test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    args = parser.parse_args()

    print("=" * 60)
    print("Testing gen_bare_deploy_bash")
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
        ("preserves_hostworkdir_runtime_token", test_preserves_hostworkdir_runtime_token),
        ("generated_scripts_do_not_embed_pidfile_authority", test_generated_scripts_do_not_embed_pidfile_authority),
        ("ops_entrypoints_use_direct_scripts", test_ops_entrypoints_use_direct_scripts),
        ("bare_start_uses_no_exit_startup_gate", test_bare_start_uses_no_exit_startup_gate),
        (
            "normalized_testbed_master_exports_service_port_for_atomic_group",
            test_normalized_testbed_master_exports_service_port_for_atomic_group,
        ),
        ("normalized_testbed_owner_emits_large_file_paths", test_normalized_testbed_owner_emits_large_file_paths),
        ("bare_child_command_preserves_runtime_hostworkdir_expansion", test_bare_child_command_preserves_runtime_hostworkdir_expansion),
        ("supervisor_label_uses_stable_selection_suffix", test_supervisor_label_uses_stable_selection_suffix),
        ("bootstrap_start_reuses_already_present_selection", test_bootstrap_start_reuses_already_present_selection),
        ("bare_start_fails_when_child_exits_within_startup_window", test_bare_start_fails_when_child_exits_within_startup_window),
        ("pid_ready_check_requires_full_stable_window_after_first_child_observation", test_pid_ready_check_requires_full_stable_window_after_first_child_observation),
        ("atomic_group_start_does_not_auto_stop_on_failure", test_atomic_group_start_does_not_auto_stop_on_failure),
        ("atomic_group_preserves_nested_heredoc_terminator", test_atomic_group_preserves_nested_heredoc_terminator),
        ("atomic_group_stop_script_is_shell_valid", test_atomic_group_stop_script_is_shell_valid),
    ]
    if selected_test_id is None:
        return checks

    for check_id, check in checks:
        if check_id == selected_test_id:
            return [(check_id, check)]
    available = ", ".join(check_id for check_id, _ in checks)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def _run_check(check: Callable[[], None]) -> bool:
    try:
        check()
        return True
    except Exception as exc:
        print(f"FAIL: {check.__name__} - {exc}")
        return False


def test_preserves_hostworkdir_runtime_token() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                global_envs:
                  FLUXON_SHARED_MEM: "${HOSTWORKDIR}/shm1"
                service:
                  svc_plain:
                    port: 12345
                    entrypoint: |
                      WORKDIR="${HOSTWORKDIR}/svc_${NODE_ID}"
                      EXPORT_TABLE=$(cat <<EOF
                      demo|${HOSTWORKDIR}
                      EOF
                      )
                      mkdir -p "${WORKDIR}"
                      echo "${EXPORT_TABLE}" > "${WORKDIR}/exports.txt"
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        script = (outdir / "start_svc_plain.sh").read_text(encoding="utf-8")
        stop_script = (outdir / "stop_svc_plain.sh").read_text(encoding="utf-8")
        entrypoint_script = (outdir / "entrypoint__fluxon-testbed-svc_plain.sh").read_text(encoding="utf-8")
        assert 'export FLUXON_SHARED_MEM="${HOSTWORKDIR}/shm1"' in script, script
        assert '${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-svc_plain.sh' in script, script
        assert 'run --label "$SUPERVISOR_LABEL" --scope-key "$HOSTWORKDIR"' in script, script
        assert ' -- /usr/bin/env bash "${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-svc_plain.sh"' in script, script
        assert "selection_present()" in script, script
        assert 'if [ "${FLUXON_BARE_ALLOW_ALREADY_PRESENT:-false}" = "true" ]; then' in script, script
        assert 'WORKDIR="${HOSTWORKDIR}/svc_${NODE_ID}"' in entrypoint_script, entrypoint_script
        assert "demo|${HOSTWORKDIR}" in entrypoint_script, entrypoint_script
        assert "/hostworkdir/svc_" not in script, script
        assert "wait-present" not in script, script
        assert "launch_only_start_gate" not in script, script
        _assert_standalone_deadline_after_launch(script)
        assert 'wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID" 10 "$STARTUP_DEADLINE_SECONDS" "[bare]"' in script, script
        assert "export SERVICE_PORT=12345" in script, script
        assert "wait_service_tcp_ready" not in script, script
        assert "wait_service_etcd_endpoint_healthy" not in script, script
        assert 'SUPERVISOR_PID=$( setsid ' not in script, script
        assert '>>"$LOGFILE" 2>&1' not in script, script
        assert 'touch "$LOGFILE"' not in script, script
        assert 'python3 "$SELECTION_SUPERVISOR" stop --label "$SUPERVISOR_LABEL" --scope-key "$HOSTWORKDIR" --missing-ok' in stop_script, stop_script
        assert "retire-runtime" not in stop_script, stop_script
        print("PASS: test_preserves_hostworkdir_runtime_token")


def test_atomic_group_start_does_not_auto_stop_on_failure() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_atomic_group_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  svc_a:
                    port: 23456
                    entrypoint: |
                      echo svc_a
                    node_bind:
                      node: ["node-a"]
                  svc_b:
                    entrypoint: |
                      echo svc_b
                    node_bind:
                      node: ["node-a"]
                atomic_groups:
                  grp_ab:
                    phase: 1
                    nodes: ["node-a"]
                    services: ["svc_a", "svc_b"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        script = (outdir / "start_grp_ab.sh").read_text(encoding="utf-8")
        assert "stop_group()" not in script, script
        assert "stopping group" not in script, script
        assert "wait-present" not in script, script
        assert 'SUPERVISOR_PID=$( setsid ' not in script, script
        assert 'echo "[rollout] probable-ready failed svc=$SERVICE label=$SUPERVISOR_LABEL supervisor_pid=$SUPERVISOR_PID"' in script, script
        assert 'wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID"' in script, script
        assert 'GROUP_STARTUP_DEADLINE_SECONDS=' not in script, script
        assert script.count('STARTUP_DEADLINE_SECONDS=20') == 2, script
        _assert_deadline_after_launch(
            script=script,
            wait_call='wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID" 10 "$STARTUP_DEADLINE_SECONDS" "[rollout]"',
        )
        assert "export SERVICE_PORT=23456" in script, script
        assert "unset SERVICE_PORT" in script, script
        assert "wait_service_tcp_ready" not in script, script
        assert "wait_service_etcd_endpoint_healthy" not in script, script
        print("PASS: test_atomic_group_start_does_not_auto_stop_on_failure")


def test_generated_scripts_do_not_embed_pidfile_authority() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_no_pidfile_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  svc_plain:
                    entrypoint: |
                      echo plain
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        generated_scripts = [
            outdir / "start_svc_plain.sh",
            outdir / "stop_svc_plain.sh",
        ]
        forbidden_literals = [
            "pidfile",
            "stop_service_by_pidfile",
            "_pidfile_read_pid",
            "_pidfile_read_pgid_optional",
            "_stop_pgid_strict",
        ]
        for script_path in generated_scripts:
            script = script_path.read_text(encoding="utf-8")
            for forbidden_literal in forbidden_literals:
                assert forbidden_literal not in script, (
                    f"unexpected pidfile authority literal in {script_path.name}: {forbidden_literal}\n{script}"
                )
        print("PASS: test_generated_scripts_do_not_embed_pidfile_authority")


def test_ops_entrypoints_use_direct_scripts() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_ops_entrypoints_") as td:
        tmpdir = Path(td)
        outdir = tmpdir / "out"

        result = _run_generator(
            config_path=DEPLOYMENT_DIR.parent / "fluxon_test_stack" / "deployconf_testbed.yml",
            outdir=outdir,
        )
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        controller_entrypoint = (
            outdir / "entrypoint__fluxon-self-host2-fluxon_core_controller__ops_controller.sh"
        ).read_text(encoding="utf-8")
        agent_entrypoint = (
            outdir / "entrypoint__fluxon-self-host2-fluxon_core_controller__ops_agent.sh"
        ).read_text(encoding="utf-8")

        assert "-m fluxon_py.runtime.start_ops_controller" in controller_entrypoint, controller_entrypoint
        assert "examples/fluxon_ops/start_controller.py" not in controller_entrypoint, controller_entrypoint
        assert 'http_listen_addr: "0.0.0.0:19080"' in controller_entrypoint, controller_entrypoint
        assert 'http_listen_addr: "0.0.0.0:${MASTER__PORT}"' not in controller_entrypoint, controller_entrypoint
        assert "-m fluxon_py.runtime.start_ops_agent" in agent_entrypoint, agent_entrypoint
        assert "examples/fluxon_ops/start_agent.py" not in agent_entrypoint, agent_entrypoint
        print("PASS: test_ops_entrypoints_use_direct_scripts")


def test_bare_start_uses_no_exit_startup_gate() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_no_exit_gate_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  etcd:
                    port: 2379
                    entrypoint: |
                      echo etcd
                    node_bind:
                      node: ["node-a"]
                  tikv:
                    port: 20160
                    entrypoint: |
                      echo tikv
                    node_bind:
                      node: ["node-a"]
                  svc_plain:
                    port: 12345
                    entrypoint: |
                      echo plain
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        etcd_script = (outdir / "start_etcd.sh").read_text(encoding="utf-8")
        tikv_script = (outdir / "start_tikv.sh").read_text(encoding="utf-8")
        plain_script = (outdir / "start_svc_plain.sh").read_text(encoding="utf-8")

        for script in (etcd_script, tikv_script, plain_script):
            _assert_standalone_deadline_after_launch(script)
            assert 'wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID" 10 "$STARTUP_DEADLINE_SECONDS" "[bare]"' in script, script
            assert "wait_service_tcp_ready" not in script, script
            assert "wait_service_etcd_endpoint_healthy" not in script, script
        print("PASS: test_bare_start_uses_no_exit_startup_gate")


def test_normalized_testbed_master_exports_service_port_for_atomic_group() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_normalized_testbed_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.normalized.yaml"
        outdir = tmpdir / "out"

        start_test_bed = _load_python_module(
            module_name="start_test_bed_for_gen_bare_tests",
            path=DEPLOYMENT_DIR.parent / "fluxon_test_stack" / "start_test_bed.py",
        )
        base_cfg = yaml.safe_load(
            (DEPLOYMENT_DIR.parent / "fluxon_test_stack" / "deployconf_testbed.yml").read_text(encoding="utf-8")
        )
        normalized, _ = start_test_bed._normalize_bootstrap_deployconf(deployconf=base_cfg)
        config_path.write_text(
            yaml.safe_dump(normalized, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        script = (outdir / "start_fluxon_core_controller.sh").read_text(encoding="utf-8")
        master_block_start = script.index("export SERVICE=master")
        owner_block_start = script.index("export SERVICE=owner")
        master_block = script[master_block_start:owner_block_start]
        assert "export MASTER__PORT=51051" in master_block, master_block
        assert "export SERVICE_PORT=51051" in master_block, master_block
        assert "unset SERVICE_PORT" not in master_block, master_block
        assert 'GROUP_STARTUP_DEADLINE_SECONDS=' not in master_block, master_block
        _assert_deadline_after_launch(
            script=master_block,
            wait_call='wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID" 10 "$STARTUP_DEADLINE_SECONDS" "[rollout]"',
        )
        assert "wait_service_tcp_ready" not in master_block, master_block
        print("PASS: test_normalized_testbed_master_exports_service_port_for_atomic_group")


def test_normalized_testbed_owner_emits_large_file_paths() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_testbed_owner_large_paths_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.normalized.yaml"
        outdir = tmpdir / "out"

        start_test_bed = _load_python_module(
            module_name="start_test_bed_for_owner_large_paths_tests",
            path=DEPLOYMENT_DIR.parent / "fluxon_test_stack" / "start_test_bed.py",
        )
        base_cfg = yaml.safe_load(
            (DEPLOYMENT_DIR.parent / "fluxon_test_stack" / "deployconf_testbed.yml").read_text(encoding="utf-8")
        )
        normalized, _ = start_test_bed._normalize_bootstrap_deployconf(deployconf=base_cfg)
        config_path.write_text(
            yaml.safe_dump(normalized, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        script = (outdir / "entrypoint__fluxon-self-host2-fluxon_core_controller__owner.sh").read_text(encoding="utf-8")
        assert 'large_file_paths:' in script, script
        assert '- "${HOSTWORKDIR}/large/owner_${NODE_ID}"' in script, script
        print("PASS: test_normalized_testbed_owner_emits_large_file_paths")


def test_bare_child_command_preserves_runtime_hostworkdir_expansion() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_runtime_expand_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  svc_plain:
                    entrypoint: |
                      echo plain
                    node_bind:
                      node: ["node-a"]
                  svc_a:
                    entrypoint: |
                      echo svc_a
                    node_bind:
                      node: ["node-a"]
                  svc_b:
                    entrypoint: |
                      echo svc_b
                    node_bind:
                      node: ["node-a"]
                atomic_groups:
                  grp_ab:
                    phase: 1
                    nodes: ["node-a"]
                    services: ["svc_a", "svc_b"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        plain_script = (outdir / "start_svc_plain.sh").read_text(encoding="utf-8")
        group_script = (outdir / "start_grp_ab.sh").read_text(encoding="utf-8")
        assert ' -- /usr/bin/env bash "${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-svc_plain.sh"' in plain_script, plain_script
        assert " -- /usr/bin/env bash '${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-svc_plain.sh'" not in plain_script, plain_script
        assert ' -- /usr/bin/env bash "${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-grp_ab__svc_a.sh"' in group_script, group_script
        assert " -- /usr/bin/env bash '${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-grp_ab__svc_a.sh'" not in group_script, group_script
        print("PASS: test_bare_child_command_preserves_runtime_hostworkdir_expansion")


def test_supervisor_label_uses_stable_selection_suffix() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_supervisor_label_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-bench-n3-runtime-20260428-bastion-bootstrap
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  svc_plain:
                    entrypoint: |
                      echo plain
                    node_bind:
                      node: ["node-a"]
                  svc_a:
                    entrypoint: |
                      echo svc_a
                    node_bind:
                      node: ["node-a"]
                  svc_b:
                    entrypoint: |
                      echo svc_b
                    node_bind:
                      node: ["node-a"]
                atomic_groups:
                  grp_ab:
                    phase: 1
                    nodes: ["node-a"]
                    services: ["svc_a", "svc_b"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        plain_start = (outdir / "start_svc_plain.sh").read_text(encoding="utf-8")
        plain_stop = (outdir / "stop_svc_plain.sh").read_text(encoding="utf-8")
        group_start = (outdir / "start_grp_ab.sh").read_text(encoding="utf-8")
        group_stop = (outdir / "stop_grp_ab.sh").read_text(encoding="utf-8")

        assert 'SUPERVISOR_LABEL=DaemonSet/fluxon-bench-n3-runtime-20260428-bastion-bootstrap-svc_plain' in plain_start, plain_start
        assert 'SUPERVISOR_LABEL=DaemonSet/fluxon-bench-n3-runtime-20260428-bastion-bootstrap-svc_plain' in plain_stop, plain_stop
        assert 'SUPERVISOR_LABEL=DaemonSet/fluxon-bench-n3-runtime-20260428-bastion-bootstrap-grp_ab__svc_a' in group_start, group_start
        assert 'SUPERVISOR_LABEL=DaemonSet/fluxon-bench-n3-runtime-20260428-bastion-bootstrap-grp_ab__svc_b' in group_start, group_start
        assert 'SUPERVISOR_LABEL=DaemonSet/fluxon-bench-n3-runtime-20260428-bastion-bootstrap-grp_ab__svc_b' in group_stop, group_stop
        assert 'SUPERVISOR_LABEL=DaemonSet/fluxon-bench-n3-runtime-20260428-bastion-bootstrap-grp_ab__svc_a' in group_stop, group_stop
        assert "DaemonSet/svc_plain" not in plain_start, plain_start
        assert "DaemonSet/grp_ab__svc_a" not in group_start, group_start
        print("PASS: test_supervisor_label_uses_workload_identity")


def test_bootstrap_start_reuses_already_present_selection() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_reuse_present_") as td:
        tmpdir = Path(td)
        hostworkdir = tmpdir / "hostworkdir"
        outdir = hostworkdir / "gen_bare_deploy_bash"
        config_path = tmpdir / "deployconf.yaml"
        hostworkdir.mkdir(parents=True, exist_ok=True)
        (hostworkdir / "sleep_child.py").write_text(
            textwrap.dedent(
                """
                #!/usr/bin/env python3
                import signal
                import time

                def _handle_signal(_signum, _frame):
                    raise SystemExit(0)

                signal.signal(signal.SIGTERM, _handle_signal)
                signal.signal(signal.SIGINT, _handle_signal)

                while True:
                    time.sleep(0.2)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        config_path.write_text(
            textwrap.dedent(
                f"""
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: {hostworkdir}
                service:
                  svc_plain:
                    entrypoint: |
                      exec python3 "${{HOSTWORKDIR}}/sleep_child.py"
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        start_script = outdir / "start_svc_plain.sh"
        stop_script = outdir / "stop_svc_plain.sh"
        supervisor_module = _load_generated_supervisor_module(outdir / "selection_supervisor.py")
        label = "DaemonSet/fluxon-testbed-svc_plain"
        repo_root = DEPLOYMENT_DIR.parent
        base_env = os.environ.copy()
        base_env["NODE_ID"] = "node-a"

        try:
            first = subprocess.run(
                [str(start_script)],
                check=False,
                capture_output=True,
                text=True,
                cwd=str(repo_root),
                env=base_env,
                timeout=20,
            )
            assert first.returncode == 0, (
                f"first start failed rc={first.returncode} stdout={first.stdout!r} stderr={first.stderr!r}"
            )
            _wait_until_selection_present(supervisor_module, label=label, scope_key=str(hostworkdir))

            second_env = base_env.copy()
            second_env["FLUXON_BARE_ALLOW_ALREADY_PRESENT"] = "true"
            second = subprocess.run(
                [str(start_script)],
                check=False,
                capture_output=True,
                text=True,
                cwd=str(repo_root),
                env=second_env,
                timeout=20,
            )
            assert second.returncode == 0, (
                f"reuse start failed rc={second.returncode} stdout={second.stdout!r} stderr={second.stderr!r}"
            )
            assert "[bare] already present svc=svc_plain" in second.stdout, second.stdout
            live_supervisors = supervisor_module._iter_live_supervisors(label, scope_key=str(hostworkdir))
            assert len(live_supervisors) == 1, live_supervisors
        finally:
            subprocess.run(
                [str(stop_script)],
                check=False,
                capture_output=True,
                text=True,
                cwd=str(repo_root),
                env=base_env,
                timeout=20,
            )
            _wait_until_selection_absent(supervisor_module, label=label, scope_key=str(hostworkdir))
        print("PASS: test_bootstrap_start_reuses_already_present_selection")


def test_bare_start_fails_when_child_exits_within_startup_window() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_child_exit_") as td:
        tmpdir = Path(td)
        hostworkdir = tmpdir / "hostworkdir"
        outdir = hostworkdir / "gen_bare_deploy_bash"
        config_path = tmpdir / "deployconf.yaml"
        hostworkdir.mkdir(parents=True, exist_ok=True)
        (hostworkdir / "exit_after_delay.py").write_text(
            textwrap.dedent(
                """
                #!/usr/bin/env python3
                import time

                time.sleep(1.5)
                raise SystemExit(17)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        config_path.write_text(
            textwrap.dedent(
                f"""
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: {hostworkdir}
                service:
                  svc_plain:
                    entrypoint: |
                      exec python3 "${{HOSTWORKDIR}}/exit_after_delay.py"
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        start_script = outdir / "start_svc_plain.sh"
        stop_script = outdir / "stop_svc_plain.sh"
        supervisor_module = _load_generated_supervisor_module(outdir / "selection_supervisor.py")
        label = "DaemonSet/fluxon-testbed-svc_plain"
        repo_root = DEPLOYMENT_DIR.parent
        env = os.environ.copy()
        env["NODE_ID"] = "node-a"

        try:
            start = subprocess.run(
                [str(start_script)],
                check=False,
                capture_output=True,
                text=True,
                cwd=str(repo_root),
                env=env,
                timeout=20,
            )
            assert start.returncode != 0, (
                f"expected startup gate failure rc={start.returncode} stdout={start.stdout!r} stderr={start.stderr!r}"
            )
            assert "[bare] probable-ready failed svc=svc_plain" in start.stdout, start.stdout
            assert "child pid exited" in start.stdout or "child pid changed" in start.stdout, start.stdout
        finally:
            subprocess.run(
                [str(stop_script)],
                check=False,
                capture_output=True,
                text=True,
                cwd=str(repo_root),
                env=env,
                timeout=20,
            )
            _wait_until_selection_absent(supervisor_module, label=label, scope_key=str(hostworkdir))
        print("PASS: test_bare_start_fails_when_child_exits_within_startup_window")


def test_pid_ready_check_requires_full_stable_window_after_first_child_observation() -> None:
    proc_lifecycle = _load_python_module(
        module_name="test_proc_lifecycle_codegen_runtime",
        path=DEPLOYMENT_DIR / "utils" / "proc_lifecycle_codegen.py",
    )
    helpers = proc_lifecycle.render_bash_proc_lifecycle_funcs_pid_tree(
        timeouts=proc_lifecycle.StopTimeouts(term_seconds=60, kill_seconds=10, supersede_seconds=30)
    )
    with tempfile.TemporaryDirectory(prefix="test_proc_lifecycle_late_child_") as td:
        tmpdir = Path(td)
        shell_script = tmpdir / "probe.sh"
        supervisor_script = tmpdir / "delayed_child_supervisor.py"
        child_script = tmpdir / "sleep_child.py"

        child_script.write_text(
            textwrap.dedent(
                """
                #!/usr/bin/env python3
                import signal
                import time

                def _handle_signal(_signum, _frame):
                    raise SystemExit(0)

                signal.signal(signal.SIGTERM, _handle_signal)
                signal.signal(signal.SIGINT, _handle_signal)

                while True:
                    time.sleep(0.2)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        supervisor_script.write_text(
            textwrap.dedent(
                f"""
                #!/usr/bin/env python3
                import signal
                import subprocess
                import sys
                import time
                from pathlib import Path

                child = None

                def _shutdown(_signum, _frame):
                    global child
                    if child is not None and child.poll() is None:
                        child.terminate()
                        try:
                            child.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            child.kill()
                    raise SystemExit(0)

                signal.signal(signal.SIGTERM, _shutdown)
                signal.signal(signal.SIGINT, _shutdown)

                time.sleep(4)
                child = subprocess.Popen([sys.executable, str(Path({str(child_script)!r}))])
                while True:
                    if child.poll() is not None:
                        raise SystemExit(child.returncode or 0)
                    time.sleep(0.2)
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        shell_script.write_text(
            textwrap.dedent(
                f"""\
                #!/usr/bin/env bash
                set -euo pipefail
                {helpers}
                python3 {shlex.quote(str(supervisor_script))} &
                root_pid="$!"
                startup_deadline_seconds=6
                if wait_service_probably_ready_pid_tree "svc_plain" "$root_pid" 4 "$startup_deadline_seconds" "[test]"; then
                  echo "unexpected success"
                  kill "$root_pid" >/dev/null 2>&1 || true
                  wait "$root_pid" >/dev/null 2>&1 || true
                  exit 99
                else
                  wait_rc="$?"
                fi
                kill "$root_pid" >/dev/null 2>&1 || true
                wait "$root_pid" >/dev/null 2>&1 || true
                exit "$wait_rc"
                """
            ),
            encoding="utf-8",
        )
        shell_script.chmod(0o755)

        result = subprocess.run(
            ["bash", str(shell_script)],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(DEPLOYMENT_DIR.parent),
            timeout=20,
        )
        assert result.returncode != 0, (
            f"expected startup gate failure rc={result.returncode} stdout={result.stdout!r} stderr={result.stderr!r}"
        )
        assert "unexpected success" not in result.stdout, result.stdout
        assert "child pid not stable long enough" in result.stdout, result.stdout
        print("PASS: test_pid_ready_check_requires_full_stable_window_after_first_child_observation")


def test_atomic_group_preserves_nested_heredoc_terminator() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_atomic_heredoc_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  svc_a:
                    entrypoint: |
                      cd /
                      cat > "all_config.yaml" <<YAML
                      demo:
                        hostworkdir: "${HOSTWORKDIR}"
                      YAML
                      python3 -c "print('svc_a ok')"
                    node_bind:
                      node: ["node-a"]
                  svc_b:
                    entrypoint: |
                      echo svc_b
                    node_bind:
                      node: ["node-a"]
                atomic_groups:
                  grp_ab:
                    phase: 1
                    nodes: ["node-a"]
                    services: ["svc_a", "svc_b"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        script = (outdir / "start_grp_ab.sh").read_text(encoding="utf-8")
        entrypoint_script = (outdir / "entrypoint__fluxon-testbed-grp_ab__svc_a.sh").read_text(encoding="utf-8")
        assert '\nYAML\n' in entrypoint_script, entrypoint_script
        assert '\n  YAML\n' not in entrypoint_script, entrypoint_script
        assert '\ncat > "all_config.yaml" <<YAML\n' in entrypoint_script, entrypoint_script
        assert ' -- /usr/bin/env bash "${HOSTWORKDIR}/gen_bare_deploy_bash/entrypoint__fluxon-testbed-grp_ab__svc_a.sh"' in script, script
        print("PASS: test_atomic_group_preserves_nested_heredoc_terminator")


def test_atomic_group_stop_script_is_shell_valid() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_bare_deploy_bash_atomic_stop_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                service:
                  svc_a:
                    entrypoint: |
                      echo svc_a
                    node_bind:
                      node: ["node-a"]
                  svc_b:
                    entrypoint: |
                      echo svc_b
                    node_bind:
                      node: ["node-a"]
                atomic_groups:
                  grp_ab:
                    phase: 1
                    nodes: ["node-a"]
                    services: ["svc_a", "svc_b"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        stop_script = outdir / "stop_grp_ab.sh"
        syntax_check = subprocess.run(
            ["bash", "-n", str(stop_script)],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(DEPLOYMENT_DIR.parent),
        )
        assert syntax_check.returncode == 0, (
            f"bash -n failed rc={syntax_check.returncode} stdout={syntax_check.stdout!r} "
            f"stderr={syntax_check.stderr!r}"
        )

        script = stop_script.read_text(encoding="utf-8")
        assert 'if [ "$STOP_FAILED" -ne 0 ]; then\n    return 1\n  fi\n  return 0\n}' in script, script
        print("PASS: test_atomic_group_stop_script_is_shell_valid")


def _run_generator(*, config_path: Path, outdir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(GENERATOR_PATH), "-c", str(config_path), "-w", str(outdir)],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(DEPLOYMENT_DIR.parent),
    )


def _load_generated_supervisor_module(supervisor_path: Path):
    module_name = f"test_gen_bare_deploy_bash_supervisor_{abs(hash(str(supervisor_path.resolve())))}"
    spec = importlib.util.spec_from_file_location(module_name, supervisor_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load generated selection supervisor: {supervisor_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_python_module(*, module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _wait_until_selection_present(
    module,
    *,
    label: str,
    scope_key: Optional[str] = None,
    timeout_seconds: int = 15,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if module._selection_present(label, scope_key=scope_key):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timeout waiting selection present: label={label} scope_key={scope_key}")


def _wait_until_selection_absent(
    module,
    *,
    label: str,
    scope_key: Optional[str] = None,
    timeout_seconds: int = 15,
) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not module._iter_live_supervisors(label, scope_key=scope_key):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timeout waiting selection absent: label={label} scope_key={scope_key}")


def _assert_deadline_after_launch(*, script: str, wait_call: str) -> None:
    launch_check = 'if [[ ! "$SUPERVISOR_PID" =~ ^[0-9]+$ ]]; then'
    deadline_assign = 'STARTUP_DEADLINE_SECONDS=20'
    assert launch_check in script, script
    assert deadline_assign in script, script
    assert wait_call in script, script

    launch_check_idx = script.index(launch_check)
    deadline_idx = script.index(deadline_assign)
    wait_idx = script.index(wait_call)
    assert launch_check_idx < deadline_idx < wait_idx, script


def _assert_standalone_deadline_after_launch(script: str) -> None:
    _assert_deadline_after_launch(
        script=script,
        wait_call='wait_service_probably_ready_pid_tree "$SERVICE" "$SUPERVISOR_PID" 10 "$STARTUP_DEADLINE_SECONDS" "[bare]"',
    )


if __name__ == "__main__":
    raise SystemExit(main())
