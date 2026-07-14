#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time

TOP_ATTENTION_SCENE_PREFIX = "ci_top_attention_"
CI_SUITE_KIND_TEST_ALL = "test-all"
CI_SUITE_KIND_LARGE_SCALE = "large-scale"
CI_LARGE_SCALE_SCENE_ID = "ci_top_attention_largescale_mq"
SYSTEM_TEMP_ROOT = Path("/tmp")
RUNNER_TEMP_PROTECTED_PREFIXES = ("_github_", "_runner_")
SYSTEM_TEMP_GARBAGE_PREFIXES = (
    "fluxon-",
    "fluxon_",
    "pip-build-",
    "pip-ephem-wheel-cache-",
    "pip-install-",
    "pip-unpack-",
)
RUNNER_TEMP_MIN_AGE_SECONDS = 15 * 60
SYSTEM_TEMP_MIN_AGE_SECONDS = 30 * 60


def _entry_allocated_bytes(path: Path) -> int:
    try:
        if path.is_symlink() or not path.is_dir():
            return int(path.lstat().st_blocks) * 512
    except FileNotFoundError:
        return 0
    total = 0
    for root, dirs, files in os.walk(path, followlinks=False):
        for name in [*dirs, *files]:
            child = Path(root) / name
            try:
                total += int(child.lstat().st_blocks) * 512
            except FileNotFoundError:
                continue
    return total


def _remove_temp_entry(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        path.unlink(missing_ok=True)
        return
    shutil.rmtree(path)


def _scan_and_clean_temp(_: argparse.Namespace) -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("scan-and-clean-temp is restricted to GitHub-hosted workflow runs")
    runner_temp_raw = os.environ.get("RUNNER_TEMP")
    if not runner_temp_raw:
        raise RuntimeError("RUNNER_TEMP must be set for scan-and-clean-temp")
    runner_temp = Path(runner_temp_raw).resolve()
    if runner_temp == Path("/") or SYSTEM_TEMP_ROOT.resolve() == Path("/"):
        raise RuntimeError("temporary cleanup roots must not resolve to /")

    now = time.time()
    free_before = shutil.disk_usage("/").free
    subprocess.run(["systemd-tmpfiles", "--clean"], check=True)

    candidates: list[Path] = []
    if runner_temp.is_dir():
        for path in runner_temp.iterdir():
            if path.name.startswith(RUNNER_TEMP_PROTECTED_PREFIXES):
                continue
            try:
                age_seconds = now - path.lstat().st_mtime
            except FileNotFoundError:
                continue
            if age_seconds >= RUNNER_TEMP_MIN_AGE_SECONDS:
                candidates.append(path)

    if SYSTEM_TEMP_ROOT.is_dir():
        for path in SYSTEM_TEMP_ROOT.iterdir():
            try:
                age_seconds = now - path.lstat().st_mtime
            except FileNotFoundError:
                continue
            if path.name.startswith(SYSTEM_TEMP_GARBAGE_PREFIXES):
                if age_seconds >= SYSTEM_TEMP_MIN_AGE_SECONDS:
                    candidates.append(path)

    removed: list[dict[str, int | str]] = []
    seen: set[Path] = set()
    for path in candidates:
        normalized = path.absolute()
        if normalized in seen:
            continue
        seen.add(normalized)
        allocated_bytes = _entry_allocated_bytes(path)
        _remove_temp_entry(path)
        removed.append({"path": str(path), "allocated_bytes": allocated_bytes})

    free_after = shutil.disk_usage("/").free
    print(
        "temporary data cleanup complete: "
        f"removed={removed} reclaimed_bytes={max(0, free_after - free_before)} free_bytes={free_after}",
        flush=True,
    )


def _top_attention_command(
    scene_id: str,
    *,
    case_config: bool,
    extra_args: list[str] | None = None,
    timeout_seconds: int = 21600,
) -> dict[str, object]:
    if not scene_id.startswith(TOP_ATTENTION_SCENE_PREFIX):
        raise ValueError(f"not a top-attention CI scene id: {scene_id}")
    suffix = scene_id[len(TOP_ATTENTION_SCENE_PREFIX) :]
    command = (
        "__RUN_DIR__/venv/bin/python3 -u "
        f"__RUN_DIR__/src/fluxon_test_stack/top_attention_test_index/_{suffix}.py"
    )
    if case_config:
        command += " --case-config __RUN_DIR__/configs/ci_scene_config.yaml"
    for arg in extra_args or []:
        command += " " + shlex.quote(str(arg))
    return {
        "id": f"top_attention_{suffix}",
        "command": command,
        "timeout_seconds": timeout_seconds,
    }


def _top_attention_ci_scenes(doc_site_base_url: str) -> dict[str, dict[str, object]]:
    return {
        "ci_top_attention_doc_page_build": {
            "subject": "doc_page",
            "runtime_contract": "rust_self_managed",
            "scale": "n1_kvowner_dram_3gib",
            "case_config": True,
            "timeout_seconds": 10800,
            "scene_config": {"doc_site_base_url": doc_site_base_url},
        },
        "ci_top_attention_bin_kvtest": {
            "subject": "rust",
            "runtime_contract": "rust_self_managed",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": True,
            "scene_config": {"kv_test_rounds": "p2p_only"},
        },
        "ci_top_attention_log_mgmt": {
            "subject": "rust",
            "runtime_contract": "rust_self_managed",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": True,
            "scene_config": {"enabled": True},
        },
        "ci_top_attention_ctrl_c_kv": {
            "subject": "rust",
            "runtime_contract": "rust_self_managed",
            "scale": "n1_kvowner_dram_3gib",
            "case_config": False,
            "scene_config": {},
        },
        "ci_top_attention_ctrl_c_mq": {
            "subject": "mq",
            "runtime_contract": "rust_self_managed",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": False,
            "scene_config": {},
        },
        "ci_top_attention_mq_core": {
            "subject": "mq",
            "runtime_contract": "cluster_kv_owner",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": True,
            "scene_config": {},
        },
        "ci_top_attention_mq_mpsc": {
            "subject": "mq",
            "runtime_contract": "cluster_kv_owner",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": True,
            "scene_config": {},
        },
        "ci_top_attention_mq_mpmc": {
            "subject": "mq",
            "runtime_contract": "cluster_kv_owner",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": True,
            "scene_config": {},
        },
        "ci_top_attention_mq_mpmc_bench": {
            "subject": "mq",
            "runtime_contract": "cluster_kv_owner",
            "scale": "n1_kvowner_dram_20gib",
            "case_config": True,
            "scene_config": {},
        },
        "ci_top_attention_largescale_mq": {
            "subject": "mq",
            "runtime_contract": "rust_self_managed",
            "scale": "n1_kvowner_dram_3gib",
            "case_config": False,
            "extra_args": [
                "--single-host-logical-targets",
                "--testbed-bundle-source",
                "__TEST_BED_BUNDLE_ROOT__",
                "--workdir",
                "__WORKDIR_ROOT__/largescale_mq_ci_single_host/p160_c8",
                "--owner-count",
                "4",
                "--owner-dram-gib",
                "1",
                "--producer-count",
                "160",
                "--consumer-count",
                "8",
                "--threads-per-process",
                "1",
                "--duration-seconds",
                "90",
                "--metric-warmup-seconds",
                "60",
                "--value-size",
                "256",
                "--op-timeout-seconds",
                "5",
                "--cluster-ready-timeout-seconds",
                "1800",
                "--consumer-sim-min-ms",
                "1",
                "--consumer-sim-max-ms",
                "1",
            ],
            "scene_config": {},
        },
    }


def _select_ci_scenes(
    scenes: dict[str, dict[str, object]],
    *,
    suite_kind: str,
) -> dict[str, dict[str, object]]:
    if suite_kind == CI_SUITE_KIND_TEST_ALL:
        return {
            scene_id: scene
            for scene_id, scene in scenes.items()
            if scene_id != CI_LARGE_SCALE_SCENE_ID
        }
    if suite_kind == CI_SUITE_KIND_LARGE_SCALE:
        return {CI_LARGE_SCALE_SCENE_ID: scenes[CI_LARGE_SCALE_SCENE_ID]}
    raise ValueError(f"unsupported CI suite kind: {suite_kind!r}")


def _write_suite(args: argparse.Namespace) -> None:
    import yaml

    owner, separator, repository_name = args.repository.partition("/")
    if not separator or not owner or not repository_name or "/" in repository_name:
        raise ValueError("--repository must use the OWNER/REPOSITORY form")
    suite = yaml.safe_load(args.source.read_text(encoding="utf-8"))
    if not isinstance(suite, dict):
        raise ValueError(f"suite must be a YAML mapping: {args.source}")
    top_attention_ci_scenes = _select_ci_scenes(
        _top_attention_ci_scenes(f"{owner}.github.io/{repository_name}"),
        suite_kind=args.suite_kind,
    )

    for scene_id, scene_def in top_attention_ci_scenes.items():
        commands = [
            _top_attention_command(
                scene_id,
                case_config=bool(scene_def["case_config"]),
                extra_args=scene_def.get("extra_args"),
                timeout_seconds=int(scene_def.get("timeout_seconds", 21600)),
            )
        ]
        existing_scene = suite["scenes"].get(scene_id)
        if existing_scene is None:
            suite["scenes"][scene_id] = {
                "ci": {
                    "subject": scene_def["subject"],
                    "runtime_contract": scene_def["runtime_contract"],
                    "commands": commands,
                },
                "select": {
                    "scales": [scene_def["scale"]],
                    "profiles": ["fluxon_tcp"],
                },
            }
            continue
        existing_scene["ci"]["subject"] = scene_def["subject"]
        existing_scene["ci"]["runtime_contract"] = scene_def["runtime_contract"]
        existing_scene["ci"]["commands"] = commands

    # Keep one bounded top-attention scene set and one transport profile in CI.
    suite["scenes"] = {
        key: value
        for key, value in suite["scenes"].items()
        if key in top_attention_ci_scenes
    }
    suite["profiles"] = {"fluxon_tcp": copy.deepcopy(suite["profiles"]["fluxon_tcp"])}
    suite["run"]["selectors"]["profile_ids"] = ["fluxon_tcp"]

    scene_configs = suite["profiles"]["fluxon_tcp"]["runtime"]["ci"].setdefault(
        "scene_configs", {}
    )
    for scene_id, scene_def in top_attention_ci_scenes.items():
        scene_configs[scene_id] = copy.deepcopy(scene_def["scene_config"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        yaml.safe_dump(suite, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    print(args.output)


def _print_file(path: Path) -> None:
    print(f"=== {path} ===")
    if path.exists():
        print(path.read_text(encoding="utf-8", errors="replace"))
    else:
        print(f"missing {path}")


def _print_tail(path: Path, *, line_count: int = 240) -> None:
    print(f"=== {path} tail ===")
    if path.exists():
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        print("\n".join(lines[-line_count:]))
    else:
        print(f"missing {path}")


def _print_failure_diagnostics(args: argparse.Namespace) -> None:
    import yaml

    workdir = args.workdir
    case_runs_path = workdir / "case_runs.yaml"
    if not case_runs_path.exists():
        print(f"missing {case_runs_path}")
        return

    case_runs = yaml.safe_load(case_runs_path.read_text(encoding="utf-8")) or {}
    print("=== case_runs.yaml ===")
    print(yaml.safe_dump(case_runs, sort_keys=False, allow_unicode=False))
    for case in case_runs.get("cases", []):
        last_run = case.get("last_run", {})
        if last_run.get("outcome") == "SUCCESS":
            continue
        case_id = case.get("case_id")
        run_index = last_run.get("run_index")
        print(f"=== failed case: {case_id} run_{run_index} ===")
        run_dir = workdir / "results" / str(case_id) / f"run_{run_index}"
        for relative in (
            "summary.yaml",
            "exception.txt",
            "logs/ci_runner/exit_code.txt",
            "logs/ci_runner/restart_count.txt",
            "logs/ci_runner/inflight_attempt.txt",
        ):
            _print_file(run_dir / relative)
        _print_tail(run_dir / "logs" / "ci_runner" / "stdout.log")

    nested_root = workdir / "largescale_mq_ci_single_host"
    print(f"=== nested largescale MQ diagnostics: {nested_root} ===")
    if not nested_root.exists():
        print(f"missing {nested_root}")
        return

    nested_failed_run_dirs: list[Path] = []
    for path in sorted(nested_root.glob("*/case_runs.yaml")):
        _print_file(path)
        try:
            nested_case_runs = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            print(f"failed to parse {path}: {type(exc).__name__}: {exc}")
            continue
        for case in nested_case_runs.get("cases", []):
            last_run = case.get("last_run", {})
            if last_run.get("outcome") == "SUCCESS":
                continue
            case_id = case.get("case_id")
            run_index = last_run.get("run_index")
            nested_failed_run_dirs.append(
                path.parent / "results" / str(case_id) / f"run_{run_index}"
            )

    print("=== nested largescale MQ failed run diagnostics ===")
    if not nested_failed_run_dirs:
        print("no failed nested run dirs found from nested case_runs.yaml")
    for run_dir in nested_failed_run_dirs:
        print(f"--- nested failed run: {run_dir} ---")
        for relative in (
            "summary.yaml",
            "exception.txt",
            "benchmark_result.json",
            "deploy_result.yaml",
            "logs/ci_runner/exit_code.txt",
            "logs/ci_runner/restart_count.txt",
            "logs/ci_runner/inflight_attempt.txt",
        ):
            _print_file(run_dir / relative)
        _print_tail(run_dir / "logs" / "ci_runner" / "stdout.log", line_count=320)
        for path in sorted(run_dir.glob("logs/*/status.yaml")):
            _print_file(path)
        for path in sorted(run_dir.glob("logs/*/workload_log_tail.txt")):
            _print_tail(path, line_count=160)
        for path in sorted(run_dir.glob("logs/*/workload_log_tail.json")):
            if path.with_suffix(".txt").exists():
                continue
            _print_tail(path, line_count=160)

    for path in sorted(nested_root.glob("*/test_runner.log")):
        _print_tail(path, line_count=320)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Support the ci_2_virt_node GitHub workflow.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    clean_temp = subparsers.add_parser(
        "scan-and-clean-temp",
        help="Clean stale runner and system temporary data before the CI flow starts.",
    )
    clean_temp.set_defaults(handler=_scan_and_clean_temp)

    write_suite = subparsers.add_parser("write-suite", help="Write the bounded CI suite.")
    write_suite.add_argument("--source", type=Path, required=True)
    write_suite.add_argument("--output", type=Path, required=True)
    write_suite.add_argument("--repository", required=True)
    write_suite.add_argument(
        "--suite-kind",
        choices=(CI_SUITE_KIND_TEST_ALL, CI_SUITE_KIND_LARGE_SCALE),
        required=True,
    )
    write_suite.set_defaults(handler=_write_suite)

    diagnostics = subparsers.add_parser(
        "print-failure-diagnostics",
        help="Print complete diagnostics for failed test-stack cases.",
    )
    diagnostics.add_argument("--workdir", type=Path, required=True)
    diagnostics.set_defaults(handler=_print_failure_diagnostics)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    args.handler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
