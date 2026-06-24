#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
DEPLOYMENT_DIR = SCRIPT_DIR.parent
GENERATOR_PATH = DEPLOYMENT_DIR / "gen_k8s_daemonset.py"


def main() -> int:
    parser = argparse.ArgumentParser(description="gen_k8s_daemonset test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    args = parser.parse_args()

    print("=" * 60)
    print("Testing gen_k8s_daemonset")
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
        ("writes_namespace_annotation", test_writes_namespace_annotation),
        ("preserves_hostworkdir_runtime_token", test_preserves_hostworkdir_runtime_token),
        ("ops_entrypoints_use_direct_scripts", test_ops_entrypoints_use_direct_scripts),
        ("rejects_missing_namespace", test_rejects_missing_namespace),
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


def test_writes_namespace_annotation() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_k8s_daemonset_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                namespace: fluxon_testbed
                name_prefix: fluxon-testbed
                image: example.com/fluxon:test
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                global_envs:
                  FLUXON_CLUSTER_NODE_IDS: "node-a"
                atomic_groups:
                  control_group:
                    phase: 1
                    nodes: ["node-a"]
                    services: ["svc_grouped"]
                service:
                  svc_plain:
                    entrypoint: echo plain
                    node_bind:
                      node: ["node-a"]
                  svc_grouped:
                    entrypoint: echo grouped
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        manifests = []
        for path in sorted(outdir.glob("*.daemonset.yaml")):
            with path.open("r", encoding="utf-8") as f:
                manifests.extend(list(yaml.safe_load_all(f)))
        assert manifests, "expected generator to write daemonset manifests"
        for manifest in manifests:
            annotations = manifest["metadata"]["annotations"]
            assert annotations["fluxon.io/namespace"] == "fluxon_testbed", (
                f"expected namespace annotation on manifest {manifest['metadata']['name']}, got {annotations!r}"
            )
        print("PASS: test_writes_namespace_annotation")


def test_preserves_hostworkdir_runtime_token() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_k8s_daemonset_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                namespace: fluxon_testbed
                name_prefix: fluxon-testbed
                image: example.com/fluxon:test
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                global_envs:
                  FLUXON_CLUSTER_NODE_IDS: "node-a"
                  FLUXON_SHARED_MEM: "${HOSTWORKDIR}/shm1"
                service:
                  svc_plain:
                    entrypoint: |
                      mkdir -p "${HOSTWORKDIR}/svc_${NODE_ID}"
                      echo "${FLUXON_SHARED_MEM}" > "${HOSTWORKDIR}/svc_${NODE_ID}/shared_mem.txt"
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        manifest_path = outdir / "svc_plain.daemonset.yaml"
        with manifest_path.open("r", encoding="utf-8") as f:
            manifest = yaml.safe_load(f)
        script = manifest["spec"]["template"]["spec"]["containers"][0]["args"][0]
        assert 'export FLUXON_SHARED_MEM="${HOSTWORKDIR}/shm1"' in script, script
        assert 'mkdir -p "${HOSTWORKDIR}/svc_${NODE_ID}"' in script, script
        assert "/hostworkdir/svc_" not in script, script
        print("PASS: test_preserves_hostworkdir_runtime_token")


def test_ops_entrypoints_use_direct_scripts() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_k8s_daemonset_ops_entrypoints_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                namespace: fluxon_testbed
                name_prefix: fluxon-testbed
                image: example.com/fluxon:test
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                  - hostname: node-b
                    ip: 127.0.0.2
                    hostworkdir: /tmp/hostworkdir
                global_envs:
                  FLUXON_CLUSTER_NODE_IDS: "node-a node-b"
                  FLUXON_PIP_CONF_CMD: "true"
                  FLUXON_RELEASE_WHEEL_FETCH_CMD: "true"
                  FLUXON_SHARED_MEM: "${HOSTWORKDIR}/shm1"
                  ETCD_FULL_ADDRESS: "127.0.0.1:33579"
                  FLUXON_CLUSTER_NAME: "fluxon_testbed"
                  FLUXON_OPS_CONTROLLER_INSTANCE_KEY: "ops_controller_node-a"
                  FLUXON_PROMETHEUS_BASE_URL: "http://127.0.0.1:35030/v1/prometheus"
                  MONITOR_GREPTIMEDB_BASE_URL: "http://127.0.0.1:35030"
                  MASTER__PORT: "19080"
                service:
                  ops_agent:
                    entrypoint: |
                      WORKDIR="${HOSTWORKDIR}/ops_agent/${NODE_ID}"
                      mkdir -p "${WORKDIR}" "${FLUXON_SHARED_MEM}"
                      cat > "${WORKDIR}/ops_agent.yaml" <<YAML
                      kv_client:
                        instance_key: "fluxon_ops_${NODE_ID}"
                        pprof_duration_seconds: 60
                        fluxonkv_spec:
                          cluster_name: "${FLUXON_CLUSTER_NAME}"
                          share_mem_path: "${FLUXON_SHARED_MEM}"
                      controller_instance_key: "${FLUXON_OPS_CONTROLLER_INSTANCE_KEY}"
                      hostworkdir: "${HOSTWORKDIR}"
                      YAML
                      ${HOSTWORKDIR}/venv/bin/python -m fluxon_py.runtime.start_ops_agent -c "${WORKDIR}/ops_agent.yaml" -w "${WORKDIR}"
                    node_bind:
                      node: ["node-a", "node-b"]
                  ops_controller:
                    entrypoint: |
                      WORKDIR="${HOSTWORKDIR}/ops_controller"
                      mkdir -p "${WORKDIR}" "${FLUXON_SHARED_MEM}"
                      cat > "${WORKDIR}/ops_controller.yaml" <<YAML
                      ops_controller:
                        kv_client:
                          instance_key: "ops_controller_${NODE_ID}"
                          pprof_duration_seconds: 60
                          fluxonkv_spec:
                            cluster_name: "${FLUXON_CLUSTER_NAME}"
                            share_mem_path: "${FLUXON_SHARED_MEM}"
                            p2p_listen_port: 12102
                        panel:
                          max_body_bytes: 1073741824
                          auth:
                            username: "ops_admin"
                            password: "ops_password"
                        reconcile:
                          interval_ms: 5000
                      fluxon_cli:
                        etcd_endpoints:
                          - "http://${ETCD_FULL_ADDRESS}"
                        prometheus_base_url: "${FLUXON_PROMETHEUS_BASE_URL}"
                        greptime_sql:
                          base_url: "${MONITOR_GREPTIMEDB_BASE_URL}"
                          db: "public"
                          log_table: "fluxon_logs"
                        cluster_name: "${FLUXON_CLUSTER_NAME}"
                        member_kind: kv
                        output: web
                        http_listen_addr: "0.0.0.0:${OPS_CONTROLLER__PORT}"
                      YAML
                      ${HOSTWORKDIR}/venv/bin/python -m fluxon_py.runtime.start_ops_controller -c "${WORKDIR}/ops_controller.yaml" -w "${WORKDIR}"
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 0, f"generator failed: stdout={result.stdout} stderr={result.stderr}"

        controller_manifest = (outdir / "ops_controller.daemonset.yaml").read_text(encoding="utf-8")
        agent_manifest = (outdir / "ops_agent.daemonset.yaml").read_text(encoding="utf-8")
        assert "-m fluxon_py.runtime.start_ops_controller" in controller_manifest, controller_manifest
        assert "examples/fluxon_ops/start_controller.py" not in controller_manifest, controller_manifest
        assert "-m fluxon_py.runtime.start_ops_agent" in agent_manifest, agent_manifest
        assert "examples/fluxon_ops/start_agent.py" not in agent_manifest, agent_manifest
        print("PASS: test_ops_entrypoints_use_direct_scripts")


def test_rejects_missing_namespace() -> None:
    with tempfile.TemporaryDirectory(prefix="test_gen_k8s_daemonset_") as td:
        tmpdir = Path(td)
        config_path = tmpdir / "deployconf.yaml"
        outdir = tmpdir / "out"
        config_path.write_text(
            textwrap.dedent(
                """
                name_prefix: fluxon-testbed
                image: example.com/fluxon:test
                cluster_nodes:
                  - hostname: node-a
                    ip: 127.0.0.1
                    hostworkdir: /tmp/hostworkdir
                global_envs:
                  FLUXON_CLUSTER_NODE_IDS: "node-a"
                service:
                  svc_plain:
                    entrypoint: echo plain
                    node_bind:
                      node: ["node-a"]
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_generator(config_path=config_path, outdir=outdir)
        assert result.returncode == 1, f"expected generator failure, got {result.returncode}: {result.stdout}"
        assert "Invalid namespace at top-level 'namespace'" in result.stdout, (
            f"expected namespace validation error, got stdout={result.stdout!r}"
        )
        print("PASS: test_rejects_missing_namespace")


def _run_generator(*, config_path: Path, outdir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(GENERATOR_PATH), "-c", str(config_path), "-w", str(outdir)],
        check=False,
        capture_output=True,
        text=True,
        cwd=str(DEPLOYMENT_DIR.parent),
    )


if __name__ == "__main__":
    raise SystemExit(main())
