#!/usr/bin/env python3

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
RUNNER_STACK_DIR = REPO_ROOT / "fluxon_test_stack"
if str(RUNNER_STACK_DIR) not in sys.path:
    sys.path.insert(0, str(RUNNER_STACK_DIR))
DEPLOYMENT_DIR = REPO_ROOT / "deployment"
if str(DEPLOYMENT_DIR) not in sys.path:
    sys.path.insert(0, str(DEPLOYMENT_DIR))

from ci_scene_catalog import canonical_ci_scene_ids
import manual_dispatch_release


DEFAULT_CI_SUITE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml"
DEFAULT_BENCHMARK_SUITE_PATH = REPO_ROOT / "fluxon_test_stack" / "benchmark_full_matrix.yaml"
DEFAULT_SUITE_PATH = DEFAULT_BENCHMARK_SUITE_PATH
DEFAULT_DEPLOYCONF_TEMPLATE = REPO_ROOT / "fluxon_test_stack" / "deployconf_testbed.yml"
DEFAULT_START_TEST_BED_TEMPLATE = REPO_ROOT / "fluxon_test_stack" / "start_test_bed.yaml"
DEFAULT_WORKDIR = REPO_ROOT / "ci_remote_testbed_workdir"
DEFAULT_LOCAL_CONFIG_PATH = REPO_ROOT / "ci_remote_testbed.local.yaml"
DEFAULT_RELEASE_DIR = REPO_ROOT / "fluxon_release"
DEFAULT_REMOTE_WORKDIR_ROOT_NAME = "ci_remote_testbed_remote"
TEST_STACK_START_TEST_BED_CONFIG_ENV = "FLUXON_TEST_STACK_START_TEST_BED_CONFIG"
DEFAULT_REMOTE_CONTROLLER_REQUEST_MODE = "ssh_exec_per_request"
PLACEHOLDER_WHEEL_NAME = "fluxon-0.0.0-ci-placeholder-cp38-abi3-manylinux_2_28_x86_64.whl"
TESTBED_BUNDLE_DIRNAME = "testbed_bundle"
TESTBED_GENERATED_DIRNAME = "generated"
TESTBED_START_WORKDIR_DIRNAME = "start_test_bed"
TESTBED_RUNNER_WORKDIR_DIRNAME = "runner_run"
REMOTE_RUNNER_SCRIPT_FILENAME = "remote_runner.py"
REMOTE_RUNNER_EXIT_CODE_FILENAME = ".remote_runner_exit_code"
REMOTE_RUNNER_LAUNCH_LOG_FILENAME = "remote_runner.launch.log"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Canonical GitHub-triggered remote shared-testbed entrypoint. It packages release/test resources, "
            "dispatches them to a bounded remote testbed cluster, and starts test_runner on the remote host."
        )
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=DEFAULT_WORKDIR,
        help="State root for generated configs, testbed bundle, and runner outputs.",
    )
    parser.add_argument(
        "--release-dir",
        type=Path,
        default=DEFAULT_RELEASE_DIR,
        help="Release artifact root used for dispatch and runner reuse.",
    )
    parser.add_argument(
        "--skip-pack",
        action="store_true",
        help="Skip release/test_rsc packaging and assume artifacts already exist.",
    )
    parser.add_argument(
        "--skip-dispatch",
        action="store_true",
        help="Skip deployment/manual_dispatch_release.py.",
    )
    parser.add_argument(
        "--runner-workdir",
        type=Path,
        default=None,
        help="Optional explicit test_runner workdir. Defaults to <workdir>/runner_run.",
    )
    parser.add_argument(
        "--bootstrap-mode",
        choices=("bare_then_apply", "apply_only", "bare_only"),
        default="bare_then_apply",
        help="Bootstrap mode recorded in the generated manifest.",
    )
    parser.add_argument(
        "--print-generated",
        action="store_true",
        help="Print generated config and bundle paths before executing commands.",
    )
    return parser.parse_args()


def _resolve_repo_root_cli_path(raw_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    return (REPO_ROOT / raw_path).resolve()


def _load_yaml_mapping(path: Path, *, ctx: str) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{ctx} must be a YAML mapping: {path}")
    return raw


def _load_remote_testbed_local_config() -> dict[str, Any]:
    config_path = DEFAULT_LOCAL_CONFIG_PATH
    if not config_path.exists():
        raise ValueError(f"remote testbed local config not found: {config_path}")
    if not config_path.is_file():
        raise ValueError(f"remote testbed local config must be a YAML file: {config_path}")
    return _load_yaml_mapping(config_path, ctx="remote testbed local config")


# Expected local YAML shape:
# testbed_cluster_id: <label used for manifest/debug naming>
# remote_repo_root: /absolute/path/to the repo checkout on controller_exec_host
# remote_testbed_hostworkdir: /absolute/path/to the remote shared testbed root
# controller_exec_host: <SSH host used to trigger the remote runner>
# controller_exec_user: <SSH user for controller_exec_host>
# controller_exec_port: <SSH port for controller_exec_host>
# controller_exec_password: <SSH password for controller_exec_host>
# testbed_cluster:
#   bootstrap_primary_hostname: <cluster node hostname>
#   supported_topologies: [1, 2]
#   default_profile_ids: [fluxon_fastws, fluxon_tcp]
#   cluster_nodes:
#     - hostname: <cluster node hostname>
#       ip: <cluster node ip>
def _load_remote_testbed_cluster_spec(local_config: dict[str, Any]) -> dict[str, Any]:
    spec = copy.deepcopy(_require_mapping(local_config.get("testbed_cluster"), "remote testbed local config.testbed_cluster"))
    raw_supported_topologies = spec.get("supported_topologies")
    if not isinstance(raw_supported_topologies, list) or not raw_supported_topologies:
        raise ValueError("remote testbed local config.testbed_cluster.supported_topologies must be a non-empty list")
    supported_topologies: set[int] = set()
    for idx, raw_topology in enumerate(raw_supported_topologies):
        topology = _optional_int_value(
            raw_topology,
            field_name=f"remote testbed local config.testbed_cluster.supported_topologies[{idx}]",
            min_v=1,
        )
        if topology is None:
            raise ValueError(
                f"remote testbed local config.testbed_cluster.supported_topologies[{idx}] must be set"
            )
        supported_topologies.add(int(topology))
    spec["supported_topologies"] = supported_topologies
    return spec


def _require_local_config_str(local_config: dict[str, Any], field_name: str) -> str:
    return _require_nonempty_str(local_config.get(field_name), f"remote testbed local config.{field_name}")


def _require_local_config_int(local_config: dict[str, Any], field_name: str, *, min_v: int = 1) -> int:
    value = _optional_int_value(
        local_config.get(field_name),
        field_name=f"remote testbed local config.{field_name}",
        min_v=min_v,
    )
    if value is None:
        raise ValueError(f"remote testbed local config.{field_name} must be set")
    return value


def _require_local_config_abs_path(local_config: dict[str, Any], field_name: str) -> Path:
    raw_value = _require_local_config_str(local_config, field_name)
    path = Path(raw_value).expanduser()
    if not path.is_absolute():
        raise ValueError(f"remote testbed local config.{field_name} must be an absolute path")
    return path


def _controller_public_url_for_local_config(local_config: dict[str, Any]) -> str:
    controller_public_host = _require_local_config_str(local_config, "controller_public_host")
    controller_port = _require_local_config_int(local_config, "controller_port")
    return f"http://{controller_public_host}:{controller_port}/r/ops/fluxon_testbed"


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding="utf-8")


def _shell_quote(text: str) -> str:
    if not text:
        return "''"
    safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./:=@+-"
    if all(ch in safe for ch in text):
        return text
    return "'" + text.replace("'", "'\\''") + "'"


def _run(argv: list[str], *, env: dict[str, str] | None = None) -> None:
    print("RUN: " + " ".join(_shell_quote(part) for part in argv), flush=True)
    subprocess.check_call(argv, cwd=str(REPO_ROOT), env=env)


def _optional_str_value(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    return text


def _optional_int_value(raw: Any, *, field_name: str, min_v: int = 1) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError(f"{field_name} must be an integer")
    if isinstance(raw, int):
        value = int(raw)
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        try:
            value = int(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an integer") from exc
    else:
        raise ValueError(f"{field_name} must be an integer")
    if value < min_v:
        raise ValueError(f"{field_name} must be >= {min_v}")
    return value


def _require_nonempty_str(value: str | None, field_name: str) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        raise ValueError(f"{field_name} must be non-empty")
    return text


def _find_single_wheel(release_dir: Path, *, pattern: str, ctx: str) -> str:
    matches = sorted(path.name for path in release_dir.glob(pattern) if path.is_file())
    if len(matches) == 1:
        return matches[0]
    non_placeholder_matches = [name for name in matches if name != PLACEHOLDER_WHEEL_NAME]
    if len(non_placeholder_matches) == 1:
        return non_placeholder_matches[0]
    raise ValueError(f"{ctx} expected exactly one match for {pattern!r}, got {matches}")


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _require_path_within_root(*, path: Path, root: Path, ctx: str) -> Path:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    if resolved_path != resolved_root and resolved_root not in resolved_path.parents:
        raise ValueError(f"{ctx} escaped root: path={resolved_path} root={resolved_root}")
    return resolved_path


def _bundle_relative_path(*, bundle_root: Path, path: Path, ctx: str) -> Path:
    resolved_bundle_root = bundle_root.resolve()
    resolved_path = _require_path_within_root(path=path, root=resolved_bundle_root, ctx=ctx)
    return resolved_path.relative_to(resolved_bundle_root)


def _remote_runner_workdir_root(*, remote_testbed_hostworkdir: Path, testbed_cluster_id: str) -> Path:
    return (remote_testbed_hostworkdir.resolve() / testbed_cluster_id / DEFAULT_REMOTE_WORKDIR_ROOT_NAME).resolve()


def _remote_testbed_bundle_root(*, remote_workdir_root: Path) -> Path:
    return (remote_workdir_root.resolve() / TESTBED_BUNDLE_DIRNAME).resolve()


def _remote_testbed_runner_workdir_root(*, remote_workdir_root: Path) -> Path:
    return (remote_workdir_root.resolve() / TESTBED_RUNNER_WORKDIR_DIRNAME).resolve()


def _remote_testbed_release_root(*, remote_testbed_hostworkdir: Path) -> Path:
    return (remote_testbed_hostworkdir.resolve() / "fluxon_release").resolve()


def _build_remote_ssh_cmd(
    *,
    ssh_user: str,
    ssh_host: str,
    ssh_port: int,
    remote_cmd: str,
) -> str:
    return (
        "ssh "
        + manual_dispatch_release.SSH_COMMON_OPTS
        + " -p "
        + manual_dispatch_release.sh_quote(str(int(ssh_port)))
        + " "
        + manual_dispatch_release.sh_quote(f"{ssh_user}@{ssh_host}")
        + " "
        + manual_dispatch_release.sh_quote(remote_cmd)
    )


def _run_remote_bash(
    *,
    ssh_user: str,
    ssh_host: str,
    ssh_port: int,
    ssh_password: str | None,
    remote_cmd: str,
) -> None:
    ssh_cmd = _build_remote_ssh_cmd(
        ssh_user=ssh_user,
        ssh_host=ssh_host,
        ssh_port=ssh_port,
        remote_cmd=remote_cmd,
    )
    manual_dispatch_release._check_call_bash_with_optional_password(password=ssh_password, cmd=ssh_cmd)


def _run_remote_bash_output(
    *,
    ssh_user: str,
    ssh_host: str,
    ssh_port: int,
    ssh_password: str | None,
    remote_cmd: str,
) -> str:
    ssh_cmd = _build_remote_ssh_cmd(
        ssh_user=ssh_user,
        ssh_host=ssh_host,
        ssh_port=ssh_port,
        remote_cmd=remote_cmd,
    )
    return manual_dispatch_release._check_output_bash_with_optional_password(password=ssh_password, cmd=ssh_cmd)


def _copy_remote_dir_to_local(
    *,
    ssh_user: str,
    ssh_host: str,
    ssh_port: int,
    ssh_password: str | None,
    remote_dir: Path,
    local_dir: Path,
) -> None:
    tempdir = Path(tempfile.mkdtemp(prefix="fluxon_ci_remote_testbed_pull_"))
    stage_dir = tempdir / local_dir.name
    try:
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        stage_dir.parent.mkdir(parents=True, exist_ok=True)
        remote_src = str(remote_dir.resolve()) + "/."
        scp_cmd = (
            "scp "
            + manual_dispatch_release.SCP_COMMON_OPTS
            + " -r -p -P "
            + manual_dispatch_release.sh_quote(str(int(ssh_port)))
            + " "
            + manual_dispatch_release.sh_quote(f"{ssh_user}@{ssh_host}:{remote_src}")
            + " "
            + manual_dispatch_release.sh_quote(str(stage_dir))
        )
        manual_dispatch_release._check_call_bash_with_optional_password(password=ssh_password, cmd=scp_cmd)
        if local_dir.exists():
            shutil.rmtree(local_dir)
        local_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(stage_dir), str(local_dir))
    finally:
        shutil.rmtree(tempdir, ignore_errors=True)


def _copy_local_dir_to_remote(
    *,
    src_dir: Path,
    ssh_user: str,
    ssh_host: str,
    ssh_port: int,
    ssh_password: str | None,
    dst_dir: Path,
    dst_owner: str,
) -> None:
    manual_dispatch_release._copy_remote_artifact(
        src_dir=src_dir,
        dst_dir_s=str(dst_dir.resolve()),
        ssh_user=ssh_user,
        ip=ssh_host,
        ssh_port=ssh_port,
        ssh_password=ssh_password,
        dst_owner=dst_owner,
    )


def _write_remote_runner_script(
    *,
    path: Path,
    remote_repo_root: Path,
    remote_workdir_root: Path,
    remote_release_root: Path,
    phase_names: list[str],
) -> None:
    payload = {
        "repo_root": str(remote_repo_root.resolve()),
        "workdir_root": str(remote_workdir_root.resolve()),
        "release_root": str(remote_release_root.resolve()),
        "phase_names": list(phase_names),
        "test_runner_path": str((remote_repo_root / "fluxon_test_stack" / "test_runner.py").resolve()),
        "exit_code_filename": REMOTE_RUNNER_EXIT_CODE_FILENAME,
        "bundle_dirname": TESTBED_BUNDLE_DIRNAME,
        "generated_dirname": TESTBED_GENERATED_DIRNAME,
        "runner_workdir_dirname": TESTBED_RUNNER_WORKDIR_DIRNAME,
        "start_test_bed_filename": "start_test_bed.remote.yaml",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    script = """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import traceback
from pathlib import Path


PAYLOAD = json.loads(Path(__file__).with_name("remote_runner.payload.json").read_text(encoding="utf-8"))
RUNNER_REPO_ROOT = Path(PAYLOAD["repo_root"])
RUNNER_DEPLOYMENT_DIR = RUNNER_REPO_ROOT / "deployment"
if str(RUNNER_DEPLOYMENT_DIR) not in sys.path:
    sys.path.insert(0, str(RUNNER_DEPLOYMENT_DIR))

import manual_dispatch_release  # noqa: E402


def _write_exit_code(exit_code: int) -> None:
    Path(__file__).with_name(PAYLOAD["exit_code_filename"]).write_text(f"{int(exit_code)}\\n", encoding="utf-8")


def _materialize_release_ext_images(*, release_root: Path) -> None:
    manual_dispatch_release._materialize_local_ext_images_from_tarball(  # noqa: SLF001
        dst_release_dir_s=str(release_root.resolve()),
        dst_owner=PAYLOAD["repo_root"],
    )


def _run_command(*, cmd: list[str], cwd: Path, env: dict[str, str]) -> int:
    print("RUN: " + " ".join(json.dumps(part) for part in cmd), flush=True)
    completed = subprocess.run(cmd, cwd=str(cwd), env=env, check=False)
    print(f"RC: {completed.returncode}", flush=True)
    return int(completed.returncode)


def main() -> int:
    repo_root = Path(PAYLOAD["repo_root"])
    workdir_root = Path(PAYLOAD["workdir_root"])
    release_root = Path(PAYLOAD["release_root"])
    runner_path = Path(PAYLOAD["test_runner_path"])
    phase_names = list(PAYLOAD["phase_names"])
    bundle_root = (workdir_root / PAYLOAD["bundle_dirname"]).resolve()
    generated_dirname = PAYLOAD["generated_dirname"]
    start_test_bed_filename = PAYLOAD["start_test_bed_filename"]
    runner_workdir_root = (bundle_root / PAYLOAD["runner_workdir_dirname"]).resolve()
    try:
        print("remote runner started", flush=True)
        _materialize_release_ext_images(release_root=release_root)
        if not phase_names:
            raise RuntimeError("remote runner received no phases")
        for phase_name in phase_names:
            suite_path = (bundle_root / generated_dirname / f"{phase_name}.yaml").resolve()
            start_cfg_path = (bundle_root / start_test_bed_filename).resolve()
            phase_runner_workdir = (runner_workdir_root / phase_name).resolve()
            phase_runner_workdir.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env["FLUXON_TEST_STACK_LOCAL_RELEASE_ROOT"] = str(release_root.resolve())
            env["FLUXON_TEST_STACK_START_TEST_BED_CONFIG"] = str(start_cfg_path)
            cmd = [
                sys.executable,
                str(runner_path),
                "-c",
                str(suite_path),
                "-w",
                str(phase_runner_workdir),
            ]
            completed_rc = _run_command(cmd=cmd, cwd=repo_root, env=env)
            print(f"PHASE {phase_name} rc={completed_rc}", flush=True)
            if completed_rc != 0:
                _write_exit_code(completed_rc)
                return completed_rc
        _write_exit_code(0)
        print("remote runner complete", flush=True)
        return 0
    except Exception:
        traceback.print_exc()
        if not Path(__file__).with_name(PAYLOAD["exit_code_filename"]).exists():
            _write_exit_code(1)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
"""
    path.write_text(script, encoding="utf-8")
    payload_path = path.with_name("remote_runner.payload.json")
    payload_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    path.chmod(0o755)


def _remote_runner_status_path(remote_workdir_root: Path, *, filename: str) -> Path:
    return (remote_workdir_root / filename).resolve()


def _remote_runner_launch_script_path(remote_workdir_root: Path) -> Path:
    return (remote_workdir_root / REMOTE_RUNNER_SCRIPT_FILENAME).resolve()


def _remote_runner_exit_code_path(remote_workdir_root: Path) -> Path:
    return _remote_runner_status_path(remote_workdir_root, filename=REMOTE_RUNNER_EXIT_CODE_FILENAME)


def _remote_runner_launch_log_path(remote_workdir_root: Path) -> Path:
    return _remote_runner_status_path(remote_workdir_root, filename=REMOTE_RUNNER_LAUNCH_LOG_FILENAME)


def _remote_runner_phase_root(remote_workdir_root: Path, *, phase_name: str) -> Path:
    return (remote_workdir_root / phase_name).resolve()


def _remote_runner_phase_bundle_root(remote_workdir_root: Path, *, phase_name: str) -> Path:
    return (_remote_runner_phase_root(remote_workdir_root, phase_name=phase_name) / TESTBED_BUNDLE_DIRNAME).resolve()


def _remote_runner_phase_suite_path(remote_workdir_root: Path, *, phase_name: str) -> Path:
    return (
        _remote_runner_phase_bundle_root(remote_workdir_root, phase_name=phase_name)
        / TESTBED_GENERATED_DIRNAME
        / f"{phase_name}.yaml"
    ).resolve()


def _remote_runner_phase_start_cfg_path(remote_workdir_root: Path, *, phase_name: str) -> Path:
    return (
        _remote_runner_phase_bundle_root(remote_workdir_root, phase_name=phase_name)
        / "start_test_bed.remote.yaml"
    ).resolve()


def _build_remote_runner_launch_cmd(
    *,
    remote_repo_root: Path,
    remote_workdir_root: Path,
    remote_release_root: Path,
    phase_names: list[str],
) -> str:
    exit_code_path = _remote_runner_exit_code_path(remote_workdir_root)
    launch_log_path = _remote_runner_launch_log_path(remote_workdir_root)
    runner_path = _remote_runner_launch_script_path(remote_workdir_root)
    phase_clause = " ".join(_shell_quote(phase_name) for phase_name in phase_names)
    inner_script = "\n".join(
        [
            "set -euo pipefail",
            f"mkdir -p {_shell_quote(str(remote_workdir_root.resolve()))}",
            f"exit_code_path={_shell_quote(str(exit_code_path))}",
            f"launch_log_path={_shell_quote(str(launch_log_path))}",
            f"runner_path={_shell_quote(str(runner_path.resolve()))}",
            f"repo_root={_shell_quote(str(remote_repo_root.resolve()))}",
            f"release_root={_shell_quote(str(remote_release_root.resolve()))}",
            "rm -f \"$exit_code_path\"",
            "rm -f \"$launch_log_path\"",
            "exec >>\"$launch_log_path\" 2>&1",
            "echo \"remote runner launch requested\"",
            f"echo \"remote runner phases: {phase_clause}\"",
            "cd \"$repo_root\"",
            "nohup python3 \"$runner_path\" </dev/null &",
            "echo \"remote runner background pid=$!\"",
        ]
    )
    return "bash -lc " + manual_dispatch_release.sh_quote(inner_script)


def _build_remote_runner_poll_cmd(*, remote_workdir_root: Path) -> str:
    exit_code_path = _remote_runner_exit_code_path(remote_workdir_root)
    launch_log_path = _remote_runner_launch_log_path(remote_workdir_root)
    exit_code_probe = "if [ -f {path} ]; then printf 'REMOTE_EXIT_CODE:%s\\n' \"$(cat {path})\"; fi".format(
        path=_shell_quote(str(exit_code_path)),
    )
    log_probe = "if [ -f {path} ]; then tail -n 80 {path}; fi".format(
        path=_shell_quote(str(launch_log_path)),
    )
    return exit_code_probe + "; " + log_probe


def _parse_remote_runner_exit_code(poll_output: str) -> int | None:
    for raw_line in poll_output.splitlines():
        if not raw_line.startswith("REMOTE_EXIT_CODE:"):
            continue
        raw_code = raw_line.split(":", 1)[1].strip()
        if not raw_code:
            raise ValueError("remote runner exit code marker was empty")
        try:
            return int(raw_code)
        except ValueError as exc:
            raise ValueError(f"remote runner exit code must be an int, got: {raw_code!r}") from exc
    return None


def _selected_ci_scene_ids(suite_cfg: dict[str, Any]) -> list[str]:
    scenes = suite_cfg.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError("suite.scenes must be a mapping")
    canonical_scene_ids = canonical_ci_scene_ids()
    missing_scene_ids = [scene_id for scene_id in canonical_scene_ids if scene_id not in scenes]
    if missing_scene_ids:
        raise ValueError(
            "remote CI suite template is missing canonical CI scene ids: "
            f"{missing_scene_ids}"
        )
    return list(canonical_scene_ids)


def _selected_scene_profile_ids(suite_cfg: dict[str, Any], *, scene_ids: list[str]) -> list[str]:
    scenes = suite_cfg.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError("suite.scenes must be a mapping")
    out: list[str] = []
    for scene_id in scene_ids:
        scene_obj = scenes.get(scene_id)
        if not isinstance(scene_obj, dict):
            raise ValueError(f"scene[{scene_id}] must be a mapping")
        select_cfg = scene_obj.get("select")
        if not isinstance(select_cfg, dict):
            raise ValueError(f"scene[{scene_id}].select must be a mapping")
        raw_profile_ids = select_cfg.get("profiles")
        if not isinstance(raw_profile_ids, list) or not raw_profile_ids:
            raise ValueError(f"scene[{scene_id}].select.profiles must be a non-empty list")
        for raw_profile_id in raw_profile_ids:
            profile_id = _require_nonempty_str(str(raw_profile_id), f"scene[{scene_id}].select.profiles[]")
            if profile_id not in out:
                out.append(profile_id)
    if not out:
        raise ValueError("selected CI scenes have no profiles")
    return out


def _remote_cluster_multi_machine_topologies(remote_spec: dict[str, Any]) -> set[int]:
    supported_topologies = remote_spec.get("supported_topologies")
    if not isinstance(supported_topologies, set):
        raise ValueError("remote testbed spec.supported_topologies must be a set")
    multi_machine_topologies = {topology for topology in supported_topologies if topology > 1}
    if not multi_machine_topologies:
        raise ValueError("remote testbed spec.supported_topologies must include at least one topology > 1")
    return multi_machine_topologies


def _selected_benchmark_scene_ids(
    suite_cfg: dict[str, Any],
    *,
    remote_spec: dict[str, Any],
) -> list[str]:
    scenes = suite_cfg.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError("suite.scenes must be a mapping")
    scales = suite_cfg.get("scales")
    if not isinstance(scales, dict):
        raise ValueError("suite.scales must be a mapping")
    multi_machine_topologies = _remote_cluster_multi_machine_topologies(remote_spec)
    out: list[str] = []
    for scene_id, scene_obj in scenes.items():
        if not isinstance(scene_obj, dict):
            raise ValueError(f"scene[{scene_id}] must be a mapping")
        scene_ts = scene_obj.get("test_stack")
        if not isinstance(scene_ts, dict):
            continue
        select_cfg = scene_obj.get("select")
        if not isinstance(select_cfg, dict):
            raise ValueError(f"scene[{scene_id}].select must be a mapping")
        raw_scale_ids = select_cfg.get("scales")
        if not isinstance(raw_scale_ids, list) or not raw_scale_ids:
            raise ValueError(f"scene[{scene_id}].select.scales must be a non-empty list")
        for raw_scale_id in raw_scale_ids:
            scale_id = _require_nonempty_str(str(raw_scale_id), f"scene[{scene_id}].select.scales[]")
            scale_obj = scales.get(scale_id)
            if not isinstance(scale_obj, dict):
                raise ValueError(f"selected scale is missing or not a mapping: {scale_id}")
            topology = _optional_int_value(
                scale_obj.get("topology"),
                field_name=f"scale[{scale_id}].topology",
                min_v=1,
            )
            if topology is None:
                raise ValueError(f"scale[{scale_id}].topology must be set")
            if topology in multi_machine_topologies:
                out.append(scene_id)
                break
    if not out:
        raise ValueError("remote benchmark suite has no scenes with multi-machine scales")
    return out


def _selected_profile_ids(suite_cfg: dict[str, Any], *, remote_spec: dict[str, Any]) -> list[str]:
    profiles = suite_cfg.get("profiles")
    if not isinstance(profiles, dict):
        raise ValueError("suite.profiles must be a mapping")
    default_profile_ids = remote_spec.get("default_profile_ids")
    if not isinstance(default_profile_ids, list) or not default_profile_ids:
        raise ValueError("remote testbed spec.default_profile_ids must be a non-empty list")
    out: list[str] = []
    for raw_profile_id in default_profile_ids:
        profile_id = _require_nonempty_str(str(raw_profile_id), "remote_spec.default_profile_ids[]")
        if profile_id not in profiles:
            continue
        out.append(profile_id)
    if not out:
        raise ValueError("generated suite has no selected profiles after applying remote testbed defaults")
    return out


def _remote_cluster_target_ip_map(remote_spec: dict[str, Any]) -> dict[str, str]:
    raw_cluster_nodes = remote_spec.get("cluster_nodes")
    if not isinstance(raw_cluster_nodes, list) or not raw_cluster_nodes:
        raise ValueError("remote testbed spec.cluster_nodes must be a non-empty list")
    out: dict[str, str] = {}
    for idx, raw_node in enumerate(raw_cluster_nodes):
        if not isinstance(raw_node, dict):
            raise ValueError(f"remote testbed spec.cluster_nodes[{idx}] must be a mapping")
        hostname = _require_nonempty_str(raw_node.get("hostname"), f"remote_spec.cluster_nodes[{idx}].hostname")
        ip = _require_nonempty_str(raw_node.get("ip"), f"remote_spec.cluster_nodes[{idx}].ip")
        out[hostname] = ip
    return out


def _topology_targets_for_remote_cluster(*, topology: int, remote_target_ip_map: dict[str, str]) -> list[str]:
    ordered_targets = list(remote_target_ip_map.keys())
    if topology <= 0:
        raise ValueError(f"topology must be positive, got: {topology}")
    if len(ordered_targets) < topology:
        raise ValueError(
            f"remote cluster does not provide enough targets for topology={topology}: "
            f"available={ordered_targets}"
        )
    return ordered_targets[:topology]


def _rewrite_scale_targets_for_remote_cluster(
    *,
    scale_id: str,
    scale_obj: dict[str, Any],
    remote_spec: dict[str, Any],
    remote_target_ip_map: dict[str, str],
) -> dict[str, Any]:
    out = copy.deepcopy(scale_obj)
    topology = out.get("topology")
    if not isinstance(topology, int):
        raise ValueError(f"scale[{scale_id}].topology must be an int")
    supported_topologies = remote_spec.get("supported_topologies")
    if not isinstance(supported_topologies, set):
        raise ValueError("remote testbed spec.supported_topologies must be a set")
    if topology not in supported_topologies:
        raise ValueError(
            f"remote cluster does not support topology={topology} for scale[{scale_id}]; "
            f"supported={sorted(supported_topologies)}"
        )
    ordered_hosts = _topology_targets_for_remote_cluster(topology=topology, remote_target_ip_map=remote_target_ip_map)
    targets: dict[str, Any] = {"hosts": ordered_hosts}
    if topology == 1:
        targets["primary"] = ordered_hosts[0]
    elif topology == 2:
        targets["primary"] = ordered_hosts[0]
        targets["secondary"] = ordered_hosts[1]
    out["targets"] = targets
    return out


def _scale_topology_supported_by_remote_cluster(
    *,
    scale_id: str,
    scale_obj: dict[str, Any],
    remote_spec: dict[str, Any],
) -> bool:
    topology = scale_obj.get("topology")
    if not isinstance(topology, int):
        raise ValueError(f"scale[{scale_id}].topology must be an int")
    supported_topologies = remote_spec.get("supported_topologies")
    if not isinstance(supported_topologies, set):
        raise ValueError("remote testbed spec.supported_topologies must be a set")
    return topology in supported_topologies


def _filter_suite_for_remote_cluster(
    *,
    suite_cfg: dict[str, Any],
    scene_ids: list[str],
    profile_ids: list[str],
    remote_spec: dict[str, Any],
    remote_target_ip_map: dict[str, str],
    allowed_scale_topologies: set[int] | None = None,
) -> dict[str, Any]:
    suite = copy.deepcopy(suite_cfg)
    scenes = suite.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError("suite.scenes must be a mapping")
    selected_scenes: dict[str, Any] = {}
    selected_scale_ids: list[str] = []
    for scene_id in scene_ids:
        scene_obj = scenes.get(scene_id)
        if not isinstance(scene_obj, dict):
            raise ValueError(f"scene[{scene_id}] must be a mapping")
        selected_scene = copy.deepcopy(scene_obj)
        select_cfg = selected_scene.get("select")
        if not isinstance(select_cfg, dict):
            raise ValueError(f"scene[{scene_id}].select must be a mapping")
        raw_scale_ids = select_cfg.get("scales")
        if not isinstance(raw_scale_ids, list) or not raw_scale_ids:
            raise ValueError(f"scene[{scene_id}].select.scales must be a non-empty list")
        rewritten_scale_ids: list[str] = []
        for raw_scale_id in raw_scale_ids:
            scale_id = _require_nonempty_str(str(raw_scale_id), f"scene[{scene_id}].select.scales[]")
            scales = suite.get("scales")
            if not isinstance(scales, dict):
                raise ValueError("suite.scales must be a mapping")
            scale_obj = scales.get(scale_id)
            if not isinstance(scale_obj, dict):
                raise ValueError(f"selected scale is missing or not a mapping: {scale_id}")
            if not _scale_topology_supported_by_remote_cluster(
                scale_id=scale_id,
                scale_obj=scale_obj,
                remote_spec=remote_spec,
            ):
                continue
            scale_topology = _optional_int_value(
                scale_obj.get("topology"),
                field_name=f"scale[{scale_id}].topology",
                min_v=1,
            )
            if scale_topology is None:
                raise ValueError(f"scale[{scale_id}].topology must be set")
            if allowed_scale_topologies is not None and scale_topology not in allowed_scale_topologies:
                continue
            if scale_id not in selected_scale_ids:
                selected_scale_ids.append(scale_id)
            rewritten_scale_ids.append(scale_id)
        if not rewritten_scale_ids:
            raise ValueError(
                f"scene[{scene_id}] has no scales supported by remote cluster after topology filtering"
            )
        select_cfg["scales"] = rewritten_scale_ids
        select_cfg["profiles"] = list(profile_ids)
        selected_scenes[scene_id] = selected_scene
    suite["scenes"] = selected_scenes

    run_cfg = suite.get("run")
    if not isinstance(run_cfg, dict):
        raise ValueError("suite.run must be a mapping")
    selectors = run_cfg.get("selectors")
    if not isinstance(selectors, dict):
        raise ValueError("suite.run.selectors must be a mapping")
    selectors["profile_ids"] = list(profile_ids)

    scales = suite.get("scales")
    if not isinstance(scales, dict):
        raise ValueError("suite.scales must be a mapping")
    rewritten_scales: dict[str, Any] = {}
    for scale_id in selected_scale_ids:
        scale_obj = scales.get(scale_id)
        if not isinstance(scale_obj, dict):
            raise ValueError(f"selected scale is missing or not a mapping: {scale_id}")
        rewritten_scales[scale_id] = _rewrite_scale_targets_for_remote_cluster(
            scale_id=scale_id,
            scale_obj=scale_obj,
            remote_spec=remote_spec,
            remote_target_ip_map=remote_target_ip_map,
        )
    suite["scales"] = rewritten_scales

    profiles = suite.get("profiles")
    if not isinstance(profiles, dict):
        raise ValueError("suite.profiles must be a mapping")
    rewritten_profiles: dict[str, Any] = {}
    for profile_id in profile_ids:
        profile_obj = profiles.get(profile_id)
        if not isinstance(profile_obj, dict):
            raise ValueError(f"profile[{profile_id}] must be a mapping")
        rewritten_profile = copy.deepcopy(profile_obj)
        runtime = rewritten_profile.get("runtime")
        if not isinstance(runtime, dict):
            raise ValueError(f"profile[{profile_id}].runtime must be a mapping")
        for runtime_key in ("ci", "test_stack"):
            runtime_block = runtime.get(runtime_key)
            if not isinstance(runtime_block, dict):
                continue
            deploy_cfg = runtime_block.get("deploy")
            if not isinstance(deploy_cfg, dict):
                raise ValueError(f"profile[{profile_id}].runtime.{runtime_key}.deploy must be a mapping")
            deploy_cfg["target_ip_map"] = copy.deepcopy(remote_target_ip_map)
        rewritten_profiles[profile_id] = rewritten_profile
    suite["profiles"] = rewritten_profiles

    artifact_sets = suite.get("artifact_sets")
    if not isinstance(artifact_sets, dict):
        raise ValueError("suite.artifact_sets must be a mapping")
    needed_artifact_set_ids: list[str] = []
    for profile_id in profile_ids:
        artifact_set_id = _require_nonempty_str(
            rewritten_profiles[profile_id].get("artifact_set"),
            f"profile[{profile_id}].artifact_set",
        )
        if artifact_set_id not in needed_artifact_set_ids:
            needed_artifact_set_ids.append(artifact_set_id)
    suite["artifact_sets"] = {
        artifact_set_id: copy.deepcopy(_require_mapping(artifact_sets.get(artifact_set_id), f"artifact_sets[{artifact_set_id}]"))
        for artifact_set_id in needed_artifact_set_ids
    }
    return suite


def _require_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _rewrite_remote_deployconf(
    *,
    deployconf_cfg: dict[str, Any],
    remote_spec: dict[str, Any],
    remote_target_ip_map: dict[str, str],
    remote_hostworkdir_root: Path,
    remote_ssh_user: str,
    remote_ssh_port: int,
    remote_ssh_password: str | None,
    wheel_name: str,
    controller_port: int,
) -> dict[str, Any]:
    cfg = copy.deepcopy(deployconf_cfg)
    raw_cluster_nodes = remote_spec.get("cluster_nodes")
    if not isinstance(raw_cluster_nodes, list) or not raw_cluster_nodes:
        raise ValueError("remote testbed spec.cluster_nodes must be a non-empty list")
    if len(raw_cluster_nodes) != 2:
        raise ValueError("remote deployconf rewrite currently requires exactly two bounded cluster nodes")
    primary_hostname = _require_nonempty_str(
        raw_cluster_nodes[0].get("hostname") if isinstance(raw_cluster_nodes[0], dict) else None,
        "remote_spec.cluster_nodes[0].hostname",
    )
    secondary_hostname = _require_nonempty_str(
        raw_cluster_nodes[1].get("hostname") if isinstance(raw_cluster_nodes[1], dict) else None,
        "remote_spec.cluster_nodes[1].hostname",
    )
    cfg = _replace_remote_template_node_names(
        cfg,
        primary_hostname=primary_hostname,
        secondary_hostname=secondary_hostname,
    )
    rendered_cluster_nodes: list[dict[str, Any]] = []
    for idx, raw_node in enumerate(raw_cluster_nodes):
        node_cfg = _require_mapping(raw_node, f"remote_spec.cluster_nodes[{idx}]")
        hostname = _require_nonempty_str(node_cfg.get("hostname"), f"remote_spec.cluster_nodes[{idx}].hostname")
        ip = _require_nonempty_str(node_cfg.get("ip"), f"remote_spec.cluster_nodes[{idx}].ip")
        rendered_cluster_nodes.append(
            {
                "hostname": hostname,
                "ip": ip,
                "hostworkdir": str(remote_hostworkdir_root.resolve()),
                "ssh_host": ip,
                "ssh_user": remote_ssh_user,
                "ssh_port": int(remote_ssh_port),
                "ssh_password": remote_ssh_password,
            }
        )
    cfg["cluster_nodes"] = rendered_cluster_nodes
    cfg["gen_k8s_daemonset_mirror_outdir"] = str((remote_hostworkdir_root / "gen_k8s_daemonset").resolve())
    atomic_groups = cfg.get("atomic_groups")
    if isinstance(atomic_groups, dict):
        controller_group = atomic_groups.get("fluxon_core_controller")
        if isinstance(controller_group, dict):
            controller_group["nodes"] = list(remote_target_ip_map.keys())
    global_envs = _require_mapping(cfg.get("global_envs"), "deployconf.global_envs")
    global_envs["FLUXON_RELEASE_WHEEL"] = wheel_name
    global_envs["FLUXON_RELEASE_WHEEL_PY"] = wheel_name
    global_envs["FLUXON_CLUSTER_NODE_IDS"] = " ".join(remote_target_ip_map.keys())
    global_envs["MASTER__PORT"] = str(int(controller_port))
    global_envs["FLUXON_OPS_UI_BASE_URL"] = f"http://${{OPS_CONTROLLER__NODE_ID__IP}}:{int(controller_port)}"
    fetch_cmd = global_envs.get("FLUXON_RELEASE_WHEEL_FETCH_CMD")
    if isinstance(fetch_cmd, str):
        global_envs["FLUXON_RELEASE_WHEEL_FETCH_CMD"] = fetch_cmd.replace(
            '--wheel-py "$FLUXON_RELEASE_WHEEL_PY" --wheel-pyo3 "$FLUXON_RELEASE_WHEEL_PYO3"',
            '--wheel "$FLUXON_RELEASE_WHEEL"',
        )
    service_cfg = _require_mapping(cfg.get("service"), "deployconf.service")
    ops_controller_cfg = _require_mapping(service_cfg.get("ops_controller"), "deployconf.service.ops_controller")
    ops_controller_cfg["port"] = int(controller_port)
    return cfg


def _replace_remote_template_node_names(
    obj: Any,
    *,
    primary_hostname: str,
    secondary_hostname: str,
) -> Any:
    if isinstance(obj, str):
        # Keep Fluxon FS export names host-agnostic. They are stable runtime ids, not hostnames.
        return obj.replace("example-node-a", primary_hostname).replace("example-node-b", secondary_hostname)
    if isinstance(obj, list):
        return [
            _replace_remote_template_node_names(
                item,
                primary_hostname=primary_hostname,
                secondary_hostname=secondary_hostname,
            )
            for item in obj
        ]
    if isinstance(obj, dict):
        return {
            key: _replace_remote_template_node_names(
                value,
                primary_hostname=primary_hostname,
                secondary_hostname=secondary_hostname,
            )
            for key, value in obj.items()
        }
    return obj


def _rewrite_remote_start_test_bed(
    *,
    start_cfg: dict[str, Any],
    generated_deployconf_path: Path,
    remote_spec: dict[str, Any],
    controller_public_url: str,
    ui_port: int,
    ui_workdir: Path,
) -> dict[str, Any]:
    cfg = copy.deepcopy(start_cfg)
    cfg["deployconf_path"] = str(generated_deployconf_path)
    cfg["controller_url"] = controller_public_url
    cfg["controller_basic_auth"] = {"username": "ops_admin", "password": "ops_password"}
    ui_cfg = _require_mapping(cfg.get("test_runner_ui"), "start_test_bed.test_runner_ui")
    ui_cfg["enabled"] = True
    ui_cfg["host"] = "0.0.0.0"
    ui_cfg["port"] = int(ui_port)
    ui_cfg["workdir"] = str(ui_workdir)
    bootstrap_phases = cfg.get("bootstrap_phases")
    if isinstance(bootstrap_phases, list):
        primary_hostname = _require_nonempty_str(
            remote_spec.get("bootstrap_primary_hostname"),
            "remote_spec.bootstrap_primary_hostname",
        )
        for phase in bootstrap_phases:
            if isinstance(phase, dict) and "node" in phase:
                phase["node"] = primary_hostname
    return cfg


def _rewrite_start_test_bed_for_apply_check(*, start_cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = copy.deepcopy(start_cfg)
    deploy_workloads = cfg.get("deploy_workloads")
    if not isinstance(deploy_workloads, list):
        raise ValueError("start_test_bed.deploy_workloads must be a list")
    cfg["deploy_workloads"] = [
        item
        for item in deploy_workloads
        if str(item) != "fluxon_core_controller"
    ]
    return cfg


def _copy_bundle_artifacts(
    *,
    release_dir: Path,
    bundle_artifacts_root: Path,
    needed_artifact_set_ids: list[str],
    artifact_sets: dict[str, Any],
) -> None:
    _copy_tree(release_dir.resolve(), bundle_artifacts_root)
    for artifact_set_id in needed_artifact_set_ids:
        artifact_set_cfg = _require_mapping(artifact_sets.get(artifact_set_id), f"artifact_sets[{artifact_set_id}]")
        for source_field in ("release_source", "test_rsc_source"):
            source_cfg = _require_mapping(artifact_set_cfg.get(source_field), f"artifact_sets[{artifact_set_id}].{source_field}")
            key_prefix = _require_nonempty_str(source_cfg.get("key_prefix"), f"artifact_sets[{artifact_set_id}].{source_field}.key_prefix")
            src_path = release_dir.resolve() / key_prefix
            if not src_path.exists():
                continue
            dst_path = bundle_artifacts_root / key_prefix
            if src_path.is_dir():
                _copy_tree(src_path, dst_path)
            else:
                _copy_file(src_path, dst_path)


def _write_generated_ssh_config(
    *,
    path: Path,
    testbed_cluster_id: str,
    remote_spec: dict[str, Any],
    bastion_host: str,
    bastion_user: str,
    bastion_port: int,
    remote_ssh_user: str,
    remote_ssh_port: int,
) -> None:
    lines: list[str] = []
    lines.extend(
        [
            f"Host {testbed_cluster_id}-bastion",
            f"  HostName {bastion_host}",
            f"  User {bastion_user}",
            f"  Port {int(bastion_port)}",
            "  StrictHostKeyChecking accept-new",
            "  ConnectTimeout 10",
            "",
        ]
    )
    raw_cluster_nodes = remote_spec.get("cluster_nodes")
    if not isinstance(raw_cluster_nodes, list):
        raise ValueError("remote testbed spec.cluster_nodes must be a list")
    for raw_node in raw_cluster_nodes:
        node_cfg = _require_mapping(raw_node, "remote_spec.cluster_nodes[]")
        hostname = _require_nonempty_str(node_cfg.get("hostname"), "remote_spec.cluster_nodes[].hostname")
        host_ip = _require_nonempty_str(node_cfg.get("ip"), f"remote_spec.cluster_nodes[{hostname}].ip")
        lines.extend(
            [
                f"Host {hostname}",
                f"  HostName {host_ip}",
                f"  User {remote_ssh_user}",
                f"  Port {int(remote_ssh_port)}",
                "  StrictHostKeyChecking accept-new",
                "  ConnectTimeout 10",
                "",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_generated_bundle(
    *,
    args: argparse.Namespace,
    testbed_cluster_id: str,
    local_config: dict[str, Any],
    workdir: Path,
    generated_dir: Path,
    bundle_root: Path,
    phase_specs: list[dict[str, Any]],
    deployconf_template: dict[str, Any],
    start_test_bed_template: dict[str, Any],
    remote_spec: dict[str, Any],
    release_dir: Path,
    wheel_name: str,
) -> dict[str, Any]:
    remote_target_ip_map = _remote_cluster_target_ip_map(remote_spec)
    remote_repo_root = _require_local_config_abs_path(local_config, "remote_repo_root")
    remote_testbed_hostworkdir = Path(_require_local_config_str(local_config, "remote_testbed_hostworkdir"))
    remote_ssh_user = _require_local_config_str(local_config, "remote_ssh_user")
    remote_ssh_password = _optional_str_value(local_config.get("remote_ssh_password"))
    remote_ssh_port = _require_local_config_int(local_config, "remote_ssh_port")
    bastion_host = _require_local_config_str(local_config, "bastion_host")
    bastion_user = _require_local_config_str(local_config, "bastion_user")
    bastion_port = _require_local_config_int(local_config, "bastion_port")
    bastion_password = _optional_str_value(local_config.get("bastion_password"))
    controller_exec_host = _require_local_config_str(local_config, "controller_exec_host")
    controller_exec_user = _require_local_config_str(local_config, "controller_exec_user")
    controller_exec_port = _require_local_config_int(local_config, "controller_exec_port")
    controller_exec_password = _optional_str_value(local_config.get("controller_exec_password"))
    controller_public_url = _controller_public_url_for_local_config(local_config)
    controller_port = _require_local_config_int(local_config, "controller_port")
    controller_bastion_local_url = f"http://127.0.0.1:{controller_port}/r/ops/fluxon_testbed"
    ui_port = _require_local_config_int(local_config, "ui_port")
    remote_workdir_root = _remote_runner_workdir_root(
        remote_testbed_hostworkdir=remote_testbed_hostworkdir,
        testbed_cluster_id=testbed_cluster_id,
    )

    generated_dir = (workdir / TESTBED_GENERATED_DIRNAME).resolve()
    runner_workdir_root = (
        args.runner_workdir.resolve()
        if args.runner_workdir
        else (workdir / TESTBED_RUNNER_WORKDIR_DIRNAME).resolve()
    )
    remote_release_root = _remote_testbed_release_root(remote_testbed_hostworkdir=remote_testbed_hostworkdir)
    generated_dir.mkdir(parents=True, exist_ok=True)
    runner_workdir_root.mkdir(parents=True, exist_ok=True)
    phase_runs: list[dict[str, Any]] = []
    needed_artifact_set_ids: list[str] = []
    merged_artifact_sets: dict[str, Any] = {}
    for phase_spec in phase_specs:
        phase_name = _require_nonempty_str(phase_spec.get("phase_name"), "phase_spec.phase_name")
        phase_suite_cfg = _require_mapping(phase_spec.get("suite_cfg"), f"phase_spec[{phase_name}].suite_cfg")
        phase_artifact_sets = _require_mapping(
            phase_suite_cfg.get("artifact_sets"),
            f"phase_spec[{phase_name}].suite_cfg.artifact_sets",
        )
        for artifact_set_id, artifact_set_cfg in phase_artifact_sets.items():
            existing_artifact_set_cfg = merged_artifact_sets.get(artifact_set_id)
            if existing_artifact_set_cfg is None:
                merged_artifact_sets[artifact_set_id] = copy.deepcopy(
                    _require_mapping(
                        artifact_set_cfg,
                        f"phase_spec[{phase_name}].suite_cfg.artifact_sets[{artifact_set_id}]",
                    )
                )
            elif existing_artifact_set_cfg != artifact_set_cfg:
                raise ValueError(f"conflicting artifact_set definition for {artifact_set_id}")
        phase_scene_ids = phase_spec.get("scene_ids")
        if not isinstance(phase_scene_ids, list) or not phase_scene_ids:
            raise ValueError(f"phase_spec[{phase_name}].scene_ids must be a non-empty list")
        phase_profile_ids = phase_spec.get("profile_ids")
        if not isinstance(phase_profile_ids, list) or not phase_profile_ids:
            raise ValueError(f"phase_spec[{phase_name}].profile_ids must be a non-empty list")
        allowed_scale_topologies_raw = phase_spec.get("allowed_scale_topologies")
        allowed_scale_topologies: set[int] | None
        if allowed_scale_topologies_raw is None:
            allowed_scale_topologies = None
        else:
            if not isinstance(allowed_scale_topologies_raw, set) or not allowed_scale_topologies_raw:
                raise ValueError(
                    f"phase_spec[{phase_name}].allowed_scale_topologies must be a non-empty set when set"
                )
            allowed_scale_topologies = set(int(value) for value in allowed_scale_topologies_raw)

        generated_suite = _filter_suite_for_remote_cluster(
            suite_cfg=phase_suite_cfg,
            scene_ids=phase_scene_ids,
            profile_ids=phase_profile_ids,
            remote_spec=remote_spec,
            remote_target_ip_map=remote_target_ip_map,
            allowed_scale_topologies=allowed_scale_topologies,
        )
        phase_suite_path = (generated_dir / f"{phase_name}.yaml").resolve()
        _write_yaml(phase_suite_path, generated_suite)
        _write_yaml((bundle_root / TESTBED_GENERATED_DIRNAME / f"{phase_name}.yaml").resolve(), generated_suite)
        phase_runner_workdir = (runner_workdir_root / phase_name).resolve()
        phase_runner_workdir.mkdir(parents=True, exist_ok=True)

        generated_profiles = _require_mapping(generated_suite.get("profiles"), f"generated_suite[{phase_name}].profiles")
        phase_needed_artifact_set_ids: list[str] = []
        for profile_id in phase_profile_ids:
            generated_profile = _require_mapping(
                generated_profiles.get(profile_id),
                f"generated_suite[{phase_name}].profiles[{profile_id}]",
            )
            artifact_set_id = _require_nonempty_str(
                generated_profile.get("artifact_set"),
                f"generated_suite[{phase_name}].profiles[{profile_id}].artifact_set",
            )
            if artifact_set_id not in phase_needed_artifact_set_ids:
                phase_needed_artifact_set_ids.append(artifact_set_id)
            if artifact_set_id not in needed_artifact_set_ids:
                needed_artifact_set_ids.append(artifact_set_id)

        phase_runs.append(
            {
                "phase_name": phase_name,
                "suite_path": phase_suite_path,
                "runner_workdir": phase_runner_workdir,
                "scene_ids": list(phase_scene_ids),
                "profile_ids": list(phase_profile_ids),
                "allowed_scale_topologies": None
                if allowed_scale_topologies is None
                else sorted(allowed_scale_topologies),
                "needed_artifact_set_ids": phase_needed_artifact_set_ids,
            }
        )

    generated_deployconf = _rewrite_remote_deployconf(
        deployconf_cfg=deployconf_template,
        remote_spec=remote_spec,
        remote_target_ip_map=remote_target_ip_map,
        remote_hostworkdir_root=remote_testbed_hostworkdir,
        remote_ssh_user=remote_ssh_user,
        remote_ssh_port=remote_ssh_port,
        remote_ssh_password=remote_ssh_password,
        wheel_name=wheel_name,
        controller_port=controller_port,
    )
    generated_start_cfg = _rewrite_remote_start_test_bed(
        start_cfg=start_test_bed_template,
        generated_deployconf_path=Path("deployconf_testbed.remote.yaml"),
        remote_spec=remote_spec,
        controller_public_url=controller_public_url,
        ui_port=ui_port,
        ui_workdir=Path("test_runner_ui_runtime"),
    )
    generated_apply_check_cfg = _rewrite_start_test_bed_for_apply_check(start_cfg=generated_start_cfg)

    deployconf_path = bundle_root / "deployconf_testbed.remote.yaml"
    _write_yaml(deployconf_path, generated_deployconf)
    _write_yaml(bundle_root / "start_test_bed.remote.yaml", generated_start_cfg)

    bundle_artifacts_root = bundle_root / "artifacts"
    _copy_bundle_artifacts(
        release_dir=release_dir,
        bundle_artifacts_root=bundle_artifacts_root,
        needed_artifact_set_ids=needed_artifact_set_ids,
        artifact_sets=merged_artifact_sets,
    )

    ssh_config_path = bundle_root / "ssh_config"
    _write_generated_ssh_config(
        path=ssh_config_path,
        testbed_cluster_id=testbed_cluster_id,
        remote_spec=remote_spec,
        bastion_host=bastion_host,
        bastion_user=bastion_user,
        bastion_port=bastion_port,
        remote_ssh_user=remote_ssh_user,
        remote_ssh_port=remote_ssh_port,
    )

    remote_auth_path = bundle_root / "remote_auth.yaml"
    remote_auth_payload: dict[str, Any] = {
        "remote_ssh_user": remote_ssh_user,
        "remote_ssh_port": remote_ssh_port,
        "bastion_user": bastion_user,
        "controller_exec_host": controller_exec_host,
    }
    if remote_ssh_password is not None:
        remote_auth_payload["remote_ssh_password"] = remote_ssh_password
    if bastion_password is not None:
        remote_auth_payload["bastion_password"] = bastion_password
    if controller_exec_user:
        remote_auth_payload["controller_exec_user"] = controller_exec_user
    if controller_exec_port is not None:
        remote_auth_payload["controller_exec_port"] = int(controller_exec_port)
    if controller_exec_password is not None:
        remote_auth_payload["controller_exec_password"] = controller_exec_password
    bastion_private_key = _optional_str_value(local_config.get("bastion_private_key"))
    if bastion_private_key is not None:
        remote_auth_payload["bastion_private_key"] = str(Path(bastion_private_key).expanduser().resolve())
    _write_yaml(remote_auth_path, remote_auth_payload)

    manifest_path = bundle_root / "manifest.json"
    manifest_payload: dict[str, Any] = {
        "schema_version": 1,
        "testbed_cluster_id": testbed_cluster_id,
        "deployconf_path": str(Path("deployconf_testbed.remote.yaml")),
        "start_config_path": str(Path("start_test_bed.remote.yaml")),
        "ssh_config_path": str(Path("ssh_config")),
        "remote_auth_config_path": str(remote_auth_path.name),
        "workdir": str(Path(TESTBED_START_WORKDIR_DIRNAME)),
        "runner_workdir_root": str(Path(TESTBED_RUNNER_WORKDIR_DIRNAME)),
        "bootstrap_mode": str(args.bootstrap_mode),
        "controller_request_mode": DEFAULT_REMOTE_CONTROLLER_REQUEST_MODE,
        "controller_url": controller_public_url,
        "controller_public_url": controller_public_url,
        "controller_bastion_local_url": controller_bastion_local_url,
        "phase_runs": [
            {
                "phase_name": phase_run["phase_name"],
                "suite_path": str(Path(phase_run["suite_path"]).relative_to(workdir.resolve())),
                "runner_workdir": str(Path(phase_run["runner_workdir"]).relative_to(workdir.resolve())),
                "scene_ids": list(phase_run["scene_ids"]),
                "profile_ids": list(phase_run["profile_ids"]),
                "allowed_scale_topologies": phase_run["allowed_scale_topologies"],
            }
            for phase_run in phase_runs
        ],
        "bastion": {
            "name": f"{testbed_cluster_id}-bastion",
            "host": bastion_host,
            "ssh_port": int(bastion_port),
        },
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return {
        "phase_runs": phase_runs,
        "deployconf_path": deployconf_path,
        "manifest_path": manifest_path,
        "ssh_config_path": ssh_config_path,
        "bundle_root": bundle_root,
        "bundle_artifacts_root": bundle_artifacts_root,
        "start_test_bed_path": bundle_root / "start_test_bed.remote.yaml",
        "remote_repo_root": remote_repo_root,
        "remote_workdir_root": remote_workdir_root,
        "remote_release_root": remote_release_root,
        "controller_exec_host": controller_exec_host,
        "controller_exec_user": controller_exec_user,
        "controller_exec_port": int(controller_exec_port),
        "controller_exec_password": controller_exec_password,
        "remote_auth_path": remote_auth_path,
        "target_ip_map": remote_target_ip_map,
        "controller_public_url": controller_public_url,
        "controller_bastion_local_url": controller_bastion_local_url,
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def _print_generated(metadata: dict[str, Any]) -> None:
    serializable = _jsonable(metadata)
    print(json.dumps(serializable, ensure_ascii=False, indent=2, sort_keys=True))


def _bundle_remote_workdir(
    *,
    metadata: dict[str, Any],
    workdir: Path,
    bundle_root: Path,
    remote_workdir_root: Path,
) -> None:
    local_remote_workdir_root = (workdir / DEFAULT_REMOTE_WORKDIR_ROOT_NAME).resolve()
    if local_remote_workdir_root.exists():
        shutil.rmtree(local_remote_workdir_root)
    local_remote_workdir_root.mkdir(parents=True, exist_ok=True)
    _copy_tree(bundle_root, local_remote_workdir_root / TESTBED_BUNDLE_DIRNAME)
    # The remote runner resolves phase suite inputs from testbed_bundle/generated/*.yaml.
    _copy_tree((workdir / TESTBED_GENERATED_DIRNAME).resolve(), local_remote_workdir_root / TESTBED_BUNDLE_DIRNAME / TESTBED_GENERATED_DIRNAME)
    _write_remote_runner_script(
        path=local_remote_workdir_root / REMOTE_RUNNER_SCRIPT_FILENAME,
        remote_repo_root=Path(metadata["remote_repo_root"]),
        remote_workdir_root=remote_workdir_root,
        remote_release_root=Path(metadata["remote_release_root"]),
        phase_names=[phase_run["phase_name"] for phase_run in metadata["phase_runs"]],
    )
    _copy_local_dir_to_remote(
        src_dir=local_remote_workdir_root,
        ssh_user=metadata["controller_exec_user"],
        ssh_host=metadata["controller_exec_host"],
        ssh_port=int(metadata["controller_exec_port"]),
        ssh_password=metadata["controller_exec_password"],
        dst_dir=remote_workdir_root,
        dst_owner=metadata["controller_exec_user"],
    )


def _remote_runner_poll_until_complete(
    *,
    remote_workdir_root: Path,
    ssh_user: str,
    ssh_host: str,
    ssh_port: int,
    ssh_password: str | None,
    poll_interval_s: float = 5.0,
) -> int:
    while True:
        poll_output = _run_remote_bash_output(
            ssh_user=ssh_user,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_password=ssh_password,
            remote_cmd=_build_remote_runner_poll_cmd(remote_workdir_root=remote_workdir_root),
        )
        exit_code = _parse_remote_runner_exit_code(poll_output)
        print(poll_output, end="" if poll_output.endswith("\n") else "\n", flush=True)
        if exit_code is not None:
            return exit_code
        time.sleep(poll_interval_s)


def main() -> int:
    args = _parse_args()
    local_config = _load_remote_testbed_local_config()
    testbed_cluster_id = _require_local_config_str(local_config, "testbed_cluster_id")
    remote_spec = _load_remote_testbed_cluster_spec(local_config)
    workdir = _resolve_repo_root_cli_path(args.workdir)
    generated_dir = (workdir / "generated").resolve()
    bundle_root = (workdir / "testbed_bundle").resolve()
    generated_dir.mkdir(parents=True, exist_ok=True)
    bundle_root.mkdir(parents=True, exist_ok=True)

    ci_suite_cfg = _load_yaml_mapping(DEFAULT_CI_SUITE_PATH, ctx="remote CI suite template")
    benchmark_suite_cfg = _load_yaml_mapping(DEFAULT_BENCHMARK_SUITE_PATH, ctx="remote benchmark suite template")
    ci_scene_ids = _selected_ci_scene_ids(ci_suite_cfg)
    ci_profile_ids = _selected_scene_profile_ids(ci_suite_cfg, scene_ids=ci_scene_ids)
    benchmark_scene_ids = _selected_benchmark_scene_ids(benchmark_suite_cfg, remote_spec=remote_spec)
    benchmark_profile_ids = _selected_profile_ids(benchmark_suite_cfg, remote_spec=remote_spec)
    multi_machine_topologies = _remote_cluster_multi_machine_topologies(remote_spec)
    phase_specs = [
        {
            "phase_name": "ci",
            "suite_cfg": ci_suite_cfg,
            "scene_ids": ci_scene_ids,
            "profile_ids": ci_profile_ids,
            "allowed_scale_topologies": None,
        },
        {
            "phase_name": "benchmark",
            "suite_cfg": benchmark_suite_cfg,
            "scene_ids": benchmark_scene_ids,
            "profile_ids": benchmark_profile_ids,
            "allowed_scale_topologies": multi_machine_topologies,
        },
    ]
    deployconf_template = _load_yaml_mapping(DEFAULT_DEPLOYCONF_TEMPLATE, ctx="remote deployconf template")
    start_test_bed_template = _load_yaml_mapping(DEFAULT_START_TEST_BED_TEMPLATE, ctx="remote start_test_bed template")
    release_dir = _resolve_repo_root_cli_path(args.release_dir)

    wheel_name = PLACEHOLDER_WHEEL_NAME
    if not args.skip_pack:
        pack_cmd = [
            sys.executable,
            str((REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py").resolve()),
            "--all-profiles",
            "--release-dir",
            str(release_dir),
            "-c",
            str(DEFAULT_BENCHMARK_SUITE_PATH),
        ]
        _run(pack_cmd)

    wheel_name = _find_single_wheel(release_dir, pattern="fluxon-*.whl", ctx="top-level release wheel")
    metadata = _build_generated_bundle(
        args=args,
        testbed_cluster_id=testbed_cluster_id,
        local_config=local_config,
        workdir=workdir,
        generated_dir=generated_dir,
        bundle_root=bundle_root,
        phase_specs=phase_specs,
        deployconf_template=deployconf_template,
        start_test_bed_template=start_test_bed_template,
        remote_spec=remote_spec,
        release_dir=release_dir,
        wheel_name=wheel_name,
    )

    if args.print_generated:
        _print_generated(metadata)

    if not args.skip_dispatch:
        dispatch_cmd = [
            sys.executable,
            str((REPO_ROOT / "deployment" / "manual_dispatch_release.py").resolve()),
            "-c",
            str(metadata["deployconf_path"]),
            "--release-dir",
            str(release_dir),
            "--release-scope",
            "deploy_and_profiles",
        ]
        _run(dispatch_cmd)

    remote_workdir_root = Path(metadata["remote_workdir_root"])
    _bundle_remote_workdir(metadata=metadata, remote_workdir_root=remote_workdir_root)
    remote_runner_launch_cmd = _build_remote_runner_launch_cmd(
        remote_repo_root=Path(metadata["remote_repo_root"]),
        remote_workdir_root=remote_workdir_root,
        remote_release_root=Path(metadata["remote_release_root"]),
        phase_names=[phase_run["phase_name"] for phase_run in metadata["phase_runs"]],
    )
    _run_remote_bash(
        ssh_user=metadata["controller_exec_user"],
        ssh_host=metadata["controller_exec_host"],
        ssh_port=int(metadata["controller_exec_port"]),
        ssh_password=metadata["controller_exec_password"],
        remote_cmd=remote_runner_launch_cmd,
    )

    remote_exit_code = _remote_runner_poll_until_complete(
        remote_workdir_root=remote_workdir_root,
        ssh_user=metadata["controller_exec_user"],
        ssh_host=metadata["controller_exec_host"],
        ssh_port=int(metadata["controller_exec_port"]),
        ssh_password=metadata["controller_exec_password"],
    )
    if remote_exit_code != 0:
        raise SystemExit(remote_exit_code)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
