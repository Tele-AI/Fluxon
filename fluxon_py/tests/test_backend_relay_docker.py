"""Relay docker integration test for Fluxon KV."""

import base64
import os
from pathlib import Path
import re
import subprocess
import sys
import textwrap
import time
from typing import Dict, List, Optional, Sequence, Tuple, Union

import yaml

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from fluxon_py.logging import init_logger
from fluxon_py.tests.test_lib import KV_SVC_TYPE, setup_test_environment


logging = init_logger("test_backend_relay_docker")

RELAY_DOCKER_SERVICE_NAMES: Tuple[str, ...] = ("etcd", "master", "owner1", "owner2", "owner3", "owner4")
RELAY_DOCKER_WAIT_TIMEOUT_SECONDS = 180
RELAY_DOCKER_GET_TIMEOUT_SECONDS = 120
RELAY_DOCKER_RUN_MOUNT = "/relay_docker_mount"
RELAY_DOCKER_HELPER_NAME = "relay_client_helper.py"
RELAY_DOCKER_HELPER_SOURCE = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import base64
    import os
    import sys
    import time

    from fluxon_py import FluxonKvClientConfig, new_store
    from fluxon_py.api_error import KeyNotFoundError


    def main() -> None:
        if len(sys.argv) < 2:
            raise RuntimeError("mode is required")
        mode = sys.argv[1]
        if mode == "wait-store":
            if len(sys.argv) != 5:
                raise RuntimeError("wait-store requires: cluster_name share_mem_path timeout_seconds")
            _wait_store(sys.argv[2], sys.argv[3], float(sys.argv[4]))
            print("wait-store ok")
            return
        if mode == "put":
            if len(sys.argv) != 6:
                raise RuntimeError("put requires: cluster_name share_mem_path key payload_base64")
            _put(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
            print("put ok")
            return
        if mode == "get":
            if len(sys.argv) != 7:
                raise RuntimeError("get requires: cluster_name share_mem_path key expected_base64 timeout_seconds")
            _get(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], float(sys.argv[6]))
            print("get ok")
            return
        raise RuntimeError(f"unknown mode: {mode}")


    def _wait_store(
        cluster_name: str,
        share_mem_path: str,
        timeout_seconds: float,
    ) -> None:
        deadline = time.time() + timeout_seconds
        last_error = ""
        while time.time() < deadline:
            result = new_store(_new_config(cluster_name, share_mem_path))
            if result.is_ok():
                store = result.unwrap()
                _close_store(store)
                return
            last_error = str(result.unwrap_error())
            time.sleep(1.0)
        raise RuntimeError(f"wait-store timed out: {last_error}")


    def _put(
        cluster_name: str,
        share_mem_path: str,
        key: str,
        payload_base64: str,
    ) -> None:
        payload = base64.b64decode(payload_base64.encode("ascii"))
        store = _open_store(cluster_name, share_mem_path)
        try:
            put_result = store.put(key, {"payload": payload})
            if not put_result.is_ok():
                raise RuntimeError(f"put failed: {put_result.unwrap_error()}")
            wait_result = put_result.unwrap().wait()
            if not wait_result.is_ok():
                raise RuntimeError(f"put future failed: {wait_result.unwrap_error()}")
            _ = wait_result.unwrap()
        finally:
            _close_store(store)


    def _get(
        cluster_name: str,
        share_mem_path: str,
        key: str,
        expected_base64: str,
        timeout_seconds: float,
    ) -> None:
        expected = base64.b64decode(expected_base64.encode("ascii"))
        deadline = time.time() + timeout_seconds
        store = _open_store(cluster_name, share_mem_path)
        try:
            last_error = ""
            while time.time() < deadline:
                get_result = store.get(key)
                if not get_result.is_ok():
                    last_error = str(get_result.unwrap_error())
                    time.sleep(1.0)
                    continue
                wait_result = get_result.unwrap().wait()
                if wait_result.is_ok():
                    holder = wait_result.unwrap()
                    payload = holder.access().unwrap().get("payload")
                    if payload != expected:
                        raise RuntimeError(f"unexpected payload: expected={expected!r} got={payload!r}")
                    return
                err = wait_result.unwrap_error()
                last_error = str(err)
                time.sleep(1.0)
                continue
            raise RuntimeError(f"get timed out: {last_error}")
        finally:
            _close_store(store)


    def _new_config(
        cluster_name: str,
        share_mem_path: str,
    ) -> FluxonKvClientConfig:
        return FluxonKvClientConfig(
            {
                "instance_key": f"relay_helper_{os.getpid()}_{int(time.time() * 1000)}",
                "contribute_to_cluster_pool_size": {"dram": 0, "vram": {}},
                "fluxonkv_spec": {
                    "cluster_name": cluster_name,
                    "share_mem_path": share_mem_path,
                },
            }
        )


    def _open_store(cluster_name: str, share_mem_path: str):
        result = new_store(_new_config(cluster_name, share_mem_path))
        if not result.is_ok():
            raise RuntimeError(f"new_store failed: {result.unwrap_error()}")
        return result.unwrap()


    def _close_store(store) -> None:
        result = store.close()
        if not result.is_ok():
            raise RuntimeError(f"store.close failed: {result.unwrap_error()}")
        _ = result.unwrap()


    if __name__ == "__main__":
        main()
    """
)


def main() -> int:
    setup_test_environment(logging, True)
    logging.info("Test: relay_docker_connectivity started")
    rc = test_relay_docker_connectivity()
    print("")
    print("=" * 30 + " Final Report " + "=" * 30)
    if rc == 0:
        print("\x1b[32;20m SUCCESS!  relay_docker_connectivity \x1b[0m")
        logging.info("Test: relay_docker_connectivity passed.")
    else:
        print("\x1b[31;20m FAILED!  relay_docker_connectivity \x1b[0m")
        logging.error("Test: relay_docker_connectivity failed with err_code: %s", rc)
    print("=" * 30 + "==============" + "=" * 30)
    return rc


def _relay_runtime_layout() -> Tuple[Path, Path, str]:
    source_root = Path(__file__).resolve().parents[2]
    if source_root.name == "src":
        return source_root, source_root.parent, f"{RELAY_DOCKER_RUN_MOUNT}/src"
    return source_root, source_root, RELAY_DOCKER_RUN_MOUNT


def _relay_run(
    args: Sequence[Union[str, Path]],
    *,
    cwd: Optional[Path] = None,
    timeout_seconds: Optional[int] = None,
) -> subprocess.CompletedProcess[str]:
    argv = [str(arg) for arg in args]
    result = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    if result.returncode == 0:
        return result
    raise RuntimeError(
        f"command failed: {' '.join(argv)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def _relay_container_path(run_root: Path, host_path: Path) -> str:
    rel = host_path.relative_to(run_root).as_posix()
    if rel:
        return f"{RELAY_DOCKER_RUN_MOUNT}/{rel}"
    return RELAY_DOCKER_RUN_MOUNT


def _relay_load_images(deployconf_path: Path) -> Dict[str, str]:
    deployconf = yaml.safe_load(deployconf_path.read_text(encoding="utf-8"))
    if not isinstance(deployconf, dict):
        raise RuntimeError(f"deployconf must be a mapping: {deployconf_path}")
    services = deployconf.get("service")
    if not isinstance(services, dict):
        raise RuntimeError(f"deployconf.service must be a mapping: {deployconf_path}")
    images: Dict[str, str] = {}
    for alias, service_name in (
        ("etcd", "etcd"),
        ("master", "master"),
        ("owner", "kvclient_relay"),
    ):
        service = services.get(service_name)
        if not isinstance(service, dict):
            raise RuntimeError(f"missing service.{service_name} in {deployconf_path}")
        image = service.get("image")
        if not isinstance(image, str) or not image.strip():
            raise RuntimeError(f"service.{service_name}.image must be a non-empty string")
        images[alias] = image.strip()
    return images


def _relay_load_release_wheels(deployconf_path: Path, fluxon_release_dir: Path) -> Dict[str, str]:
    deployconf = yaml.safe_load(deployconf_path.read_text(encoding="utf-8"))
    if not isinstance(deployconf, dict):
        raise RuntimeError(f"deployconf must be a mapping: {deployconf_path}")
    global_envs = deployconf.get("global_envs")
    if not isinstance(global_envs, dict):
        raise RuntimeError(f"deployconf.global_envs must be a mapping: {deployconf_path}")
    wheels: Dict[str, str] = {}
    for alias, env_key in (
        ("python", "FLUXON_RELEASE_WHEEL_PY"),
        ("pyo3", "FLUXON_RELEASE_WHEEL_PYO3"),
    ):
        wheel_name = global_envs.get(env_key)
        if not isinstance(wheel_name, str) or not wheel_name.strip():
            raise RuntimeError(f"{env_key} must be a non-empty string in {deployconf_path}")
        wheel_path = fluxon_release_dir / wheel_name.strip()
        if not wheel_path.is_file():
            raise RuntimeError(f"missing release wheel: {wheel_path}")
        wheels[alias] = wheel_name.strip()
    return wheels


def _relay_render_template(template_path: Path, output_path: Path, replacements: Dict[str, str]) -> None:
    rendered = template_path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        rendered = rendered.replace(key, value)
    unresolved = sorted(set(re.findall(r"__[A-Z0-9_]+__", rendered)))
    if unresolved:
        raise RuntimeError(f"unresolved relay deployconf tokens: {unresolved}")
    output_path.write_text(rendered, encoding="utf-8")


def _relay_wait_for_etcd(container_name: str) -> None:
    deadline = time.time() + RELAY_DOCKER_WAIT_TIMEOUT_SECONDS
    last_output = ""
    command = [
        "docker",
        "exec",
        container_name,
        "/bin/sh",
        "-lc",
        "ETCDCTL_API=3 etcdctl --endpoints=http://127.0.0.1:2379 endpoint health",
    ]
    while time.time() < deadline:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode == 0:
            return
        last_output = result.stdout + result.stderr
        time.sleep(1.0)
    raise RuntimeError(f"etcd did not become healthy: {last_output}")


def _relay_wait_for_store(
    *,
    container_name: str,
    helper_path: str,
    cluster_name: str,
    share_mem_path: str,
) -> None:
    _relay_run(
        [
            "docker",
            "exec",
            container_name,
            "python3",
            helper_path,
            "wait-store",
            cluster_name,
            share_mem_path,
            str(RELAY_DOCKER_WAIT_TIMEOUT_SECONDS),
        ],
        timeout_seconds=RELAY_DOCKER_WAIT_TIMEOUT_SECONDS + 30,
    )


def _relay_wait_for_fluxon_py(container_name: str) -> None:
    deadline = time.time() + RELAY_DOCKER_WAIT_TIMEOUT_SECONDS
    last_output = ""
    command = ["docker", "exec", container_name, "python3", "-c", "import fluxon_py"]
    while time.time() < deadline:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode == 0:
            return
        last_output = result.stdout + result.stderr
        time.sleep(1.0)
    raise RuntimeError(f"fluxon_py did not become importable in {container_name}: {last_output}")


def _relay_wait_for_master_segment_registration(runtime_dir: Path, owner_names: Sequence[str]) -> None:
    deadline = time.time() + RELAY_DOCKER_WAIT_TIMEOUT_SECONDS
    master_log_dir = runtime_dir / "logs" / "master"
    last_log_snapshot = ""
    expected = {
        owner_name: f"Successfully requested segment registration from client {owner_name}"
        for owner_name in owner_names
    }
    while time.time() < deadline:
        log_files = sorted(master_log_dir.glob("**/*.log"))
        if log_files:
            latest_log = log_files[-1]
            log_text = latest_log.read_text(encoding="utf-8")
            last_log_snapshot = log_text[-4000:]
            if all(needle in log_text for needle in expected.values()):
                return
        time.sleep(1.0)
    raise RuntimeError(
        "master did not finish owner segment registration\n"
        f"recent master log tail:\n{last_log_snapshot}"
    )


def _relay_collect_container_logs(container_names: Sequence[str]) -> None:
    for container_name in container_names:
        result = subprocess.run(
            ["docker", "logs", "--tail", "200", container_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logging.error("failed to collect logs for %s: %s", container_name, result.stderr.strip())
            continue
        logging.error("docker logs for %s:\n%s%s", container_name, result.stdout, result.stderr)


def _relay_cleanup(generated_dir: Path, network_names: Sequence[str]) -> List[str]:
    errors: List[str] = []
    stop_all = generated_dir / "stop_and_rm_all.sh"
    if stop_all.exists():
        result = subprocess.run([str(stop_all)], capture_output=True, text=True)
        if result.returncode != 0:
            errors.append(
                f"cleanup script failed: {stop_all}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
    elif generated_dir.exists():
        errors.append(f"cleanup script missing: {stop_all}")

    for network_name in network_names:
        result = subprocess.run(
            ["docker", "network", "rm", network_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            errors.append(
                f"docker network rm failed: {network_name}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
    return errors


def test_relay_docker_connectivity() -> int:
    if KV_SVC_TYPE.lower() != "fluxon":
        logging.info("relay_docker_connectivity skipped for backend %s", KV_SVC_TYPE)
        return 0

    source_root, run_root, source_root_in_mount = _relay_runtime_layout()
    run_suffix = f"{os.getpid()}-{int(time.time())}"
    name_prefix = f"relay-docker-{run_suffix}"
    cluster_name = f"relay-docker-{run_suffix}"
    node_id = os.uname().nodename.split(".")[0]
    runtime_dir = run_root / "relay_docker_runtime" / name_prefix
    generated_dir = runtime_dir / "generated"
    helper_host_path = runtime_dir / RELAY_DOCKER_HELPER_NAME
    rendered_deployconf_path = runtime_dir / "deployconf.yaml"
    deployconf_template_path = source_root / "fluxon_py" / "tests" / "test_backend_relay_deployconf.template.yaml"
    authority_deployconf_path = source_root / "deployment" / "deployconf.yaml"
    fluxon_release_dir = run_root / "fluxon_release"
    network_names = [
        f"{name_prefix}-a",
        f"{name_prefix}-b",
        f"{name_prefix}-c",
    ]
    container_names = {
        service_name: f"{name_prefix}-{service_name}-{node_id}"
        for service_name in RELAY_DOCKER_SERVICE_NAMES
    }
    cleanup_networks: List[str] = []
    get_proc: Optional[subprocess.Popen[str]] = None
    rc = 0

    try:
        runtime_dir.mkdir(parents=True, exist_ok=True)

        images = _relay_load_images(authority_deployconf_path)
        wheels = _relay_load_release_wheels(authority_deployconf_path, fluxon_release_dir)
        helper_host_path.write_text(RELAY_DOCKER_HELPER_SOURCE, encoding="utf-8")

        container_runtime_root = _relay_container_path(run_root, runtime_dir)
        owner_shm_paths = {
            owner_name: f"{container_runtime_root}/shm/{owner_name}"
            for owner_name in ("owner1", "owner2", "owner3", "owner4")
        }
        owner_large_root_paths = {
            owner_name: f"{container_runtime_root}/large/{owner_name}"
            for owner_name in ("owner1", "owner2", "owner3", "owner4")
        }
        _relay_render_template(
            deployconf_template_path,
            rendered_deployconf_path,
            {
                "__NAME_PREFIX__": name_prefix,
                "__NODE_ID__": node_id,
                "__RUN_DIR__": str(run_root),
                "__RUN_MOUNT__": RELAY_DOCKER_RUN_MOUNT,
                "__SOURCE_ROOT__": source_root_in_mount,
                "__RUNTIME_ROOT__": container_runtime_root,
                "__ETCD_IMAGE__": images["etcd"],
                "__MASTER_IMAGE__": images["master"],
                "__OWNER_IMAGE__": images["owner"],
                "__PYTHON_WHEEL__": wheels["python"],
                "__PYO3_WHEEL__": wheels["pyo3"],
                "__CLUSTER_NAME__": cluster_name,
                "__ETCD_CONTAINER_NAME__": container_names["etcd"],
                "__MASTER_PORT__": "18080",
                "__NET_A__": network_names[0],
                "__NET_B__": network_names[1],
                "__NET_C__": network_names[2],
                "__OWNER1_SHM__": owner_shm_paths["owner1"],
                "__OWNER2_SHM__": owner_shm_paths["owner2"],
                "__OWNER3_SHM__": owner_shm_paths["owner3"],
                "__OWNER4_SHM__": owner_shm_paths["owner4"],
                "__OWNER1_LARGE_ROOT__": owner_large_root_paths["owner1"],
                "__OWNER2_LARGE_ROOT__": owner_large_root_paths["owner2"],
                "__OWNER3_LARGE_ROOT__": owner_large_root_paths["owner3"],
                "__OWNER4_LARGE_ROOT__": owner_large_root_paths["owner4"],
            },
        )

        _relay_run(["docker", "info"], timeout_seconds=30)
        _relay_run(
            [
                sys.executable,
                source_root / "deployment" / "gen_docker_deploy_bash.py",
                "-c",
                rendered_deployconf_path,
                "-o",
                generated_dir,
            ],
            timeout_seconds=60,
        )

        for network_name in network_names:
            _relay_run(["docker", "network", "create", network_name], timeout_seconds=30)
            cleanup_networks.append(network_name)

        _relay_run([generated_dir / "etcd.sh", "--no-follow"], cwd=generated_dir, timeout_seconds=60)
        _relay_wait_for_etcd(container_names["etcd"])
        _relay_run([generated_dir / "master.sh", "--no-follow"], cwd=generated_dir, timeout_seconds=180)
        for owner_name in ("owner1", "owner2", "owner3", "owner4"):
            _relay_run([generated_dir / f"{owner_name}.sh", "--no-follow"], cwd=generated_dir, timeout_seconds=180)

        _relay_wait_for_fluxon_py(container_names["master"])
        for owner_name in ("owner1", "owner2", "owner3", "owner4"):
            _relay_wait_for_fluxon_py(container_names[owner_name])
        _relay_wait_for_master_segment_registration(
            runtime_dir,
            ("owner1", "owner2", "owner3", "owner4"),
        )

        helper_container_path = _relay_container_path(run_root, helper_host_path)
        _relay_wait_for_store(
            container_name=container_names["owner1"],
            helper_path=helper_container_path,
            cluster_name=cluster_name,
            share_mem_path=owner_shm_paths["owner1"],
        )
        _relay_wait_for_store(
            container_name=container_names["owner4"],
            helper_path=helper_container_path,
            cluster_name=cluster_name,
            share_mem_path=owner_shm_paths["owner4"],
        )

        key = f"/relay_docker/{run_suffix}/payload"
        payload = f"relay-docker-payload-{run_suffix}".encode("utf-8")
        payload_base64 = base64.b64encode(payload).decode("ascii")
        get_proc = subprocess.Popen(
            [
                "docker",
                "exec",
                container_names["owner4"],
                "python3",
                helper_container_path,
                "get",
                cluster_name,
                owner_shm_paths["owner4"],
                key,
                payload_base64,
                str(RELAY_DOCKER_GET_TIMEOUT_SECONDS),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        time.sleep(2.0)
        _relay_run(
            [
                "docker",
                "exec",
                container_names["owner1"],
                "python3",
                helper_container_path,
                "put",
                cluster_name,
                owner_shm_paths["owner1"],
                key,
                payload_base64,
            ],
            timeout_seconds=RELAY_DOCKER_GET_TIMEOUT_SECONDS + 30,
        )
        get_stdout, get_stderr = get_proc.communicate(timeout=RELAY_DOCKER_GET_TIMEOUT_SECONDS + 30)
        if get_proc.returncode != 0:
            raise RuntimeError(
                "relay get command failed\n"
                f"stdout:\n{get_stdout}\n"
                f"stderr:\n{get_stderr}"
            )
        logging.info("relay_docker_connectivity verified: owner1 put reached owner4 through relay path")
    except Exception as exc:
        rc = 1
        logging.error("relay_docker_connectivity failed: %s", exc)
        _relay_collect_container_logs(list(container_names.values()))
    finally:
        if get_proc is not None and get_proc.poll() is None:
            get_proc.kill()
            get_stdout, get_stderr = get_proc.communicate(timeout=5)
            logging.error("relay get command terminated during cleanup\nstdout:\n%s\nstderr:\n%s", get_stdout, get_stderr)
        cleanup_errors = _relay_cleanup(generated_dir, cleanup_networks)
        for cleanup_error in cleanup_errors:
            logging.error(cleanup_error)
        if rc == 0 and cleanup_errors:
            rc = 2
    return rc


if __name__ == "__main__":
    sys.exit(main())
