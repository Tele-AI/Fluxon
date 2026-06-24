#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Any, Optional
import urllib.parse
import urllib.request

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SETUP_AND_PACK_DIR = REPO_ROOT / "setup_and_pack"
setup_and_pack_dir_str = str(SETUP_AND_PACK_DIR)
if setup_and_pack_dir_str in sys.path:
    sys.path.remove(setup_and_pack_dir_str)
sys.path.insert(0, setup_and_pack_dir_str)
SCRIPTS_DIR = REPO_ROOT / "scripts"
scripts_dir_str = str(SCRIPTS_DIR)
if scripts_dir_str in sys.path:
    sys.path.remove(scripts_dir_str)
sys.path.insert(0, scripts_dir_str)

import utils as script_utils
from source_selection_profiles import (
    SOURCE_SELECTION_PROFILE_SOURCE_PACK,
    collect_source_profile_relpaths,
    get_source_profile_source_roots,
    source_profile_relpath_excluded,
)


CI_SOURCE_ROOT_NAMES: tuple[str, ...] = get_source_profile_source_roots(
    profile=SOURCE_SELECTION_PROFILE_SOURCE_PACK
)
PACKED_RUNTIME_ROOT_NAMES: tuple[str, ...] = (
    "bin",
    "etc",
    "include",
    "lib",
    "share",
)
PACKED_RUNTIME_DIR_NAME = "cxxpacked"
PACKED_RUNTIME_EXCLUDE_REL_PATHS: tuple[str, ...] = (
    "__pycache__/",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    "*.pyc",
    "*.swp",
    "*.lock",
    ".rustc_info.json",
    "CACHEDIR.TAG",
)
TEST_RSC_REPO_TREE_REL_DIR = "fluxon_release/test_rsc/source"
TEST_RSC_PREPARE_CONFIG_NAME = "prepare.yaml"
TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS: tuple[str, ...] = (
    "__pycache__/",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    "*.pyc",
    "*.swp",
    "baselines/",
)
TEST_RSC_MANIFEST_FILENAME = "fluxon_test_rsc.sha256"
TEST_RSC_MOONCAKE_WHEEL_REL_PARENT = Path("mooncake")
TEST_RSC_PYTHON_RUNTIME_REL_PARENT = Path("python_runtime")
TEST_RSC_PYTHON_RUNTIME_WHEELHOUSE_DIRNAME = "wheels"
TEST_RSC_RELEASE_SHARED_ROOT_DIRNAME = "test_rsc"
TEST_RSC_RELEASE_SHARED_BASELINES_DIRNAME = "baselines"
TEST_RSC_CANONICAL_PROFILE_ROOT_DIRNAME = "fluxon_release"
TEST_RSC_PROFILE_PREPARED_RESOURCE_ROOT_NAMES: tuple[str, ...] = (
    "python_runtime",
    "mooncake",
)
RELEASE_MANIFEST_FILENAME = "fluxon_release.sha256"
DEFAULT_REDIS_BUILD_IMAGE = "quay.io/pypa/manylinux_2_28_x86_64"
DEFAULT_REDIS_DOWNLOAD_URL_TEMPLATE = "https://download.redis.io/releases/redis-{version}.tar.gz"
DEFAULT_REDIS_VERSION = "7.2.5"
DEFAULT_TEST_STACK_PYTHON_ABI = "cpython3.10"
DEFAULT_TEST_STACK_WHEEL_PLATFORM = "manylinux2014_x86_64"
DEFAULT_TEST_STACK_CONFIG_RELPATH = Path("fluxon_test_stack/ci_test_list.yaml")
DEFAULT_TOP_LEVEL_TRANSPORT_BACKEND = script_utils.PUBLIC_TRANSPORT_BACKEND
BASELINE_BUNDLE_SPECS: tuple[dict[str, str], ...] = (
    {
        "id": "redis",
        "rel_parent": "baselines/redis",
        "bundle_name": "redis_bundle",
    },
    {
        "id": "alluxio",
        "rel_parent": "baselines/alluxio",
        "bundle_name": "alluxio_bundle",
    },
)
PROFILE_ID_TO_TRANSPORT_BACKEND = {
    profile_id: transport_backend
    for transport_backend, profile_id in script_utils.TRANSPORT_PROFILE_IDS.items()
}
def main() -> int:
    script_utils.reset_stage_summary()
    try:
        parser = argparse.ArgumentParser(
            description=(
                "Prepare Fluxon test stack runtime resources. "
                "Supports both the legacy single-profile pack path and the canonical "
                "suite-level release/test_rsc preparation path."
            )
        )
        parser.add_argument(
            "-c",
            "--config",
            type=Path,
            default=DEFAULT_TEST_STACK_CONFIG_RELPATH,
            help=(
                "Suite config used to derive transport profiles when running the suite-level "
                "preparation path. If relative, resolve against the repo root."
            ),
        )
        parser.add_argument(
            "--profile-id",
            dest="profile_ids",
            action="append",
            default=[],
            help=(
                "Suite profile id to prepare. May be passed multiple times. "
                "If omitted, derive selected profiles from --config."
            ),
        )
        parser.add_argument(
            "--transport-backend",
            choices=script_utils.TRANSPORT_BACKENDS,
            default=None,
            help=(
                "Rust PyO3 transport backend variant to build for the paired release. "
                "When --all-profiles is used, this becomes the top-level release transport backend. "
                f"Default there is {DEFAULT_TOP_LEVEL_TRANSPORT_BACKEND}."
            ),
        )
        parser.add_argument(
            "--all-profiles",
            action="store_true",
            help=(
                "Prepare the top-level release, every selected profile release, and "
                "every selected profile test_rsc subtree in one invocation."
            ),
        )
        parser.add_argument(
            "--reuse-existing-release",
            action="store_true",
            help=(
                "Do not rebuild the top-level release or profile releases. "
                "Validate the existing directories in-place and only run test_rsc preparation."
            ),
        )
        parser.add_argument(
            "--skip-top-level-release",
            action="store_true",
            help="Skip the top-level release pack/validation step when --all-profiles is used.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print the computed plan and exit without running commands.",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="When used with --dry-run, print the computed plan as JSON.",
        )
        parser.add_argument(
            "--rdma-backend",
            choices=script_utils.RDMA_BACKENDS,
            default="closed_sdk",
            help="Rust PyO3 RDMA transfer backend variant to build for the paired release",
        )
        parser.add_argument(
            "--release-dir",
            type=Path,
            default=None,
            help=(
                "Paired release directory root; if relative, resolve against the repo root inferred from "
                "this script path; defaults to <repo_root>/fluxon_release. "
                "When --all-profiles is used, profile releases are prepared under "
                "<release_dir>/profiles/<profile_id> and test_rsc is prepared under "
                "<release_dir>/test_rsc/<profile_id>."
            ),
        )
        parser.add_argument(
            "--with-tikv-runtime",
            choices=("true", "false"),
            default="true",
            help=(
                "Forwarded to setup_and_pack/pack_release.py when release packing is needed. "
                "Relevant only when --all-profiles is used without --reuse-existing-release."
            ),
        )
        parser.add_argument(
            "--skip-release-validate",
            action="store_true",
            help=(
                "Skip validation of the existing release_dir. "
                "Use only when a caller has already validated a complete release payload."
            ),
        )
        parser.add_argument(
            "--out-dir",
            type=Path,
            default=None,
            help=(
                "Output directory that will contain "
                f"{TEST_RSC_MANIFEST_FILENAME} and test runtime archives; "
                "if relative, resolve against the repo root inferred from this script path; "
                "defaults to <release_dir>/test_rsc/<transport profile id>"
            ),
        )
        parser.add_argument(
            "--repo-test-rsc-root",
            type=Path,
            default=Path(TEST_RSC_REPO_TREE_REL_DIR),
            help=(
                "Source-only test_rsc tree used for declarative inputs such as `prepare.yaml`; "
                "generated baseline bundles are owned by `<release_dir>/test_rsc/baselines`. "
                "If relative, resolve against the repo root."
            ),
        )
        parser.add_argument(
            "--prepare-config",
            type=Path,
            help=(
                "Optional test_rsc prepare config YAML. If omitted, "
                f"`<repo_test_rsc_root>/{TEST_RSC_PREPARE_CONFIG_NAME}` is used when present."
            ),
        )
        parser.add_argument(
            "--baseline-source-root",
            type=Path,
            help=(
                "Optional source root for prepared baseline bundles. The script looks for "
                "`redis/redis_bundle` or `redis/redis_bundle.tar.gz`, and likewise under "
                "`alluxio/`, with an optional leading `baselines/` segment. When omitted, "
                "the existing release-side baseline authority under "
                "`<release_dir>/test_rsc/baselines` is reused when present."
            ),
        )
        parser.add_argument(
            "--redis-bundle-src",
            type=Path,
            help="Optional explicit Redis bundle source path. Accepts either a directory or a `.tar.gz` file.",
        )
        parser.add_argument(
            "--alluxio-bundle-src",
            type=Path,
            help="Optional explicit Alluxio bundle source path. Accepts either a directory or a `.tar.gz` file.",
        )
        parser.add_argument(
            "--build-redis-bundle-docker",
            action="store_true",
            help=(
                "Build `baselines/redis/redis_bundle(.tar.gz)` from the official Redis source tarball "
                f"inside `{DEFAULT_REDIS_BUILD_IMAGE}` and update "
                "`<release_dir>/test_rsc/baselines/redis`."
            ),
        )
        parser.add_argument(
            "--redis-version",
            default=DEFAULT_REDIS_VERSION,
            help=(
                "Redis version used by --build-redis-bundle-docker. "
                f"Default: {DEFAULT_REDIS_VERSION}"
            ),
        )
        parser.add_argument(
            "--redis-source-url",
            help=(
                "Optional Redis source tarball URL for --build-redis-bundle-docker. "
                "Defaults to the official `download.redis.io/releases/redis-<version>.tar.gz`."
            ),
        )
        parser.add_argument(
            "--redis-source-sha256",
            help=(
                "Optional expected sha256 for the Redis source tarball used by "
                "--build-redis-bundle-docker."
            ),
        )
        parser.add_argument(
            "--redis-docker-image",
            default=DEFAULT_REDIS_BUILD_IMAGE,
            help=(
                "Docker image used by --build-redis-bundle-docker. "
                f"Default: {DEFAULT_REDIS_BUILD_IMAGE}"
            ),
        )
        args = parser.parse_args()

        if args.json and not args.dry_run:
            parser.error("--json currently requires --dry-run")
        if not args.all_profiles and args.reuse_existing_release:
            parser.error("--reuse-existing-release requires --all-profiles")
        if not args.all_profiles and args.skip_top_level_release:
            parser.error("--skip-top-level-release requires --all-profiles")

        if args.all_profiles:
            return _run_all_profiles_mode(args)

        if args.transport_backend is None:
            args.transport_backend = "tcp_thread"

        release_dir = (
            _resolve_repo_root_cli_path(raw_path=args.release_dir, field_name="release-dir")
            if args.release_dir is not None
            else (REPO_ROOT / "fluxon_release")
        )
        profile_id = script_utils.TRANSPORT_PROFILE_IDS.get(args.transport_backend)
        if profile_id is None:
            raise RuntimeError(f"unsupported transport backend for test_rsc output: {args.transport_backend}")
        out_dir = (
            _resolve_repo_root_cli_path(raw_path=args.out_dir, field_name="out-dir")
            if args.out_dir is not None
            else (release_dir / "test_rsc" / profile_id)
        )
        repo_test_rsc_root = _resolve_repo_root_cli_path(
            raw_path=args.repo_test_rsc_root,
            field_name="repo-test-rsc-root",
        )
        prepare_config_path = _resolve_prepare_config_path(
            raw_path=args.prepare_config,
            repo_test_rsc_root=repo_test_rsc_root,
        )
        prepare_config = (
            _load_prepare_config(path=prepare_config_path)
            if prepare_config_path is not None
            else {}
        )
        baseline_source_root = _resolve_optional_repo_root_cli_path(
            raw_path=args.baseline_source_root,
            field_name="baseline-source-root",
        )
        redis_bundle_src = _resolve_optional_repo_root_cli_path(
            raw_path=args.redis_bundle_src,
            field_name="redis-bundle-src",
        )
        alluxio_bundle_src = _resolve_optional_repo_root_cli_path(
            raw_path=args.alluxio_bundle_src,
            field_name="alluxio-bundle-src",
        )
        if args.build_redis_bundle_docker and redis_bundle_src is not None:
            parser.error("--build-redis-bundle-docker cannot be combined with --redis-bundle-src")

        if args.skip_release_validate:
            pass
        else:
            with script_utils.stage("Validating existing release"):
                _validate_existing_release_dir(release_dir=release_dir)

        baseline_inputs_provided = any(
            src is not None for src in (baseline_source_root, redis_bundle_src, alluxio_bundle_src)
        ) or args.build_redis_bundle_docker

        with tempfile.TemporaryDirectory(prefix="fluxon_pack_test_stack_rsc_") as td:
            prepared_test_rsc_root = Path(td) / "test_rsc"
            built_redis_bundle_src: Optional[Path] = None
            with script_utils.stage("Staging repository test_rsc tree"):
                _stage_repo_test_rsc_tree(
                    repo_test_rsc_root=repo_test_rsc_root,
                    out_dir=prepared_test_rsc_root,
                )

            with script_utils.stage("Staging shared release baseline authority"):
                _stage_release_shared_baselines_into_root(
                    release_dir=release_dir,
                    prepared_root=prepared_test_rsc_root,
                )

            with script_utils.stage("Staging canonical profile prepared resources"):
                _stage_canonical_profile_prepared_resources_into_root(
                    profile_id=profile_id,
                    prepared_root=prepared_test_rsc_root,
                )

            if prepare_config:
                with script_utils.stage("Preparing configured test_rsc resources"):
                    _prepare_configured_test_rsc_resources_into_root(
                        prepared_root=prepared_test_rsc_root,
                        prepare_config=prepare_config,
                        scratch_root=Path(td) / "prepared_downloads",
                    )

            if args.build_redis_bundle_docker:
                with script_utils.stage("Building Redis bundle in Docker"):
                    built_redis_bundle_src = _build_redis_bundle_with_docker(
                        scratch_root=Path(td) / "redis_docker_build",
                        redis_version=args.redis_version,
                        redis_source_url=args.redis_source_url,
                        redis_source_sha256=args.redis_source_sha256,
                        docker_image=args.redis_docker_image,
                    )

            if baseline_inputs_provided:
                with script_utils.stage("Preparing baseline bundles"):
                    _prepare_baselines_into_root(
                        prepared_root=prepared_test_rsc_root,
                        baseline_source_root=baseline_source_root,
                        redis_bundle_src=redis_bundle_src or built_redis_bundle_src,
                        alluxio_bundle_src=alluxio_bundle_src,
                    )

                with script_utils.stage("Syncing baselines into release test_rsc authority"):
                    _sync_prepared_baselines_into_release_tree(
                        prepared_root=prepared_test_rsc_root,
                        release_dir=release_dir,
                    )

            if out_dir is not None:
                out_dir.mkdir(parents=True, exist_ok=True)

                ci_src_tar = out_dir / "src_ci.tar.gz"
                with script_utils.stage("Packing CI source tarball"):
                    _pack_ci_src(repo_root=REPO_ROOT, out_path=ci_src_tar)

                ci_ext_rsc_tar = out_dir / "fluxon_ci_ext_rsc.tar.gz"
                with script_utils.stage("Packing CI external runtime tarball"):
                    _pack_ci_ext_rsc(repo_root=REPO_ROOT, out_path=ci_ext_rsc_tar)

                with script_utils.stage("Staging prepared test_rsc tree"):
                    _stage_prepared_test_rsc(
                        prepared_root=prepared_test_rsc_root,
                        out_dir=out_dir,
                    )

                manifest_path = out_dir / TEST_RSC_MANIFEST_FILENAME
                with script_utils.stage("Writing test_rsc sha256 manifest"):
                    _write_sha256_manifest(
                        out_path=manifest_path,
                        root_dir=out_dir,
                        files=_test_rsc_manifest_file_list(
                            out_dir=out_dir,
                            prepared_root=prepared_test_rsc_root,
                        ),
                    )

                with script_utils.stage("Chmod test_rsc artifacts"):
                    subprocess.check_call(["sudo", "chmod", "-R", "777", str(out_dir)])

                print(f"Packed test runtime resources into: {out_dir}")
                print(f"- {ci_src_tar.name}")
                print(f"- {ci_ext_rsc_tar.name}")
                print(f"- {manifest_path.name}")
        return 0
    finally:
        script_utils.print_stage_summary()


def _run_all_profiles_mode(args: argparse.Namespace) -> int:
    release_dir = (
        _resolve_repo_root_cli_path(raw_path=args.release_dir, field_name="release-dir")
        if args.release_dir is not None
        else (REPO_ROOT / "fluxon_release")
    )
    config_path = _resolve_repo_root_cli_path(raw_path=args.config, field_name="config")
    transport_backends = _resolve_transport_backends(
        config_path=config_path,
        explicit_profile_ids=list(args.profile_ids),
    )
    plan = _build_all_profiles_plan(
        release_dir=release_dir,
        config_path=config_path,
        top_level_transport_backend=args.transport_backend or DEFAULT_TOP_LEVEL_TRANSPORT_BACKEND,
        rdma_backend=args.rdma_backend,
        with_tikv_runtime=args.with_tikv_runtime == "true",
        transport_backends=transport_backends,
        reuse_existing_release=args.reuse_existing_release,
        skip_top_level_release=args.skip_top_level_release,
        repo_test_rsc_root=args.repo_test_rsc_root,
        prepare_config=args.prepare_config,
        baseline_source_root=args.baseline_source_root,
        redis_bundle_src=args.redis_bundle_src,
        alluxio_bundle_src=args.alluxio_bundle_src,
        build_redis_bundle_docker=args.build_redis_bundle_docker,
        redis_version=args.redis_version,
        redis_source_url=args.redis_source_url,
        redis_source_sha256=args.redis_source_sha256,
        redis_docker_image=args.redis_docker_image,
    )

    if args.dry_run:
        if args.json:
            print(json.dumps({"steps": plan}, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            _print_preparation_plan(plan)
        return 0

    for step in plan:
        if step["action"] == "validate_release":
            _validate_existing_release_dir(release_dir=Path(step["release_dir"]))
            print(f"Validated existing release dir: {step['release_dir']}", flush=True)
            continue
        subprocess.check_call(step["command"], cwd=str(REPO_ROOT))
    return 0


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _load_yaml_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _normalize_nonempty_str_list(raw_values: list[Any], *, field_name: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        if not isinstance(raw_value, str):
            raise ValueError(f"{field_name} entries must be strings")
        value = raw_value.strip()
        if not value:
            raise ValueError(f"{field_name} entries must be non-empty strings")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _resolve_transport_backends(*, config_path: Path, explicit_profile_ids: list[str]) -> list[str]:
    explicit_profile_ids = _normalize_nonempty_str_list(explicit_profile_ids, field_name="--profile-id")
    suite_cfg = _load_yaml_file(config_path)
    if not isinstance(suite_cfg, dict):
        raise ValueError(f"suite config must be a YAML mapping: {config_path}")
    profiles_cfg = suite_cfg.get("profiles")
    artifact_sets_cfg = suite_cfg.get("artifact_sets")
    if not isinstance(profiles_cfg, dict):
        raise ValueError(f"config.profiles must be a mapping: {config_path}")
    if not isinstance(artifact_sets_cfg, dict):
        raise ValueError(f"config.artifact_sets must be a mapping: {config_path}")

    if explicit_profile_ids:
        selected_profile_ids = explicit_profile_ids
    else:
        run_cfg = suite_cfg.get("run")
        if not isinstance(run_cfg, dict):
            raise ValueError(f"config.run must be a mapping: {config_path}")
        selectors_cfg = run_cfg.get("selectors")
        if not isinstance(selectors_cfg, dict):
            raise ValueError(f"config.run.selectors must be a mapping: {config_path}")
        profile_ids_cfg = selectors_cfg.get("profile_ids")
        if not isinstance(profile_ids_cfg, list):
            raise ValueError(f"config.run.selectors.profile_ids must be a list: {config_path}")
        selected_profile_ids = _normalize_nonempty_str_list(
            profile_ids_cfg,
            field_name="config.run.selectors.profile_ids",
        )

    out: list[str] = []
    seen: set[str] = set()

    def add_backend(transport_backend: str) -> None:
        if transport_backend in seen:
            return
        seen.add(transport_backend)
        out.append(transport_backend)

    for profile_id in selected_profile_ids:
        profile_cfg = profiles_cfg.get(profile_id)
        if not isinstance(profile_cfg, dict):
            raise ValueError(f"unknown profile id in suite selection: {profile_id}")
        artifact_set_id = profile_cfg.get("artifact_set")
        if not isinstance(artifact_set_id, str) or not artifact_set_id.strip():
            raise ValueError(f"profiles[{profile_id!r}].artifact_set must be a non-empty string")
        artifact_set_cfg = artifact_sets_cfg.get(artifact_set_id)
        if not isinstance(artifact_set_cfg, dict):
            raise ValueError(
                f"profiles[{profile_id!r}].artifact_set references unknown artifact set: {artifact_set_id}"
            )
        for field_name in ("release_source", "test_rsc_source"):
            source_cfg = artifact_set_cfg.get(field_name)
            if not isinstance(source_cfg, dict):
                raise ValueError(f"artifact_sets[{artifact_set_id!r}].{field_name} must be a mapping")
            key_prefix = source_cfg.get("key_prefix")
            if not isinstance(key_prefix, str) or not key_prefix.strip():
                raise ValueError(f"artifact_sets[{artifact_set_id!r}].{field_name}.key_prefix must be a string")
            candidate_profile_id = Path(key_prefix).name.strip()
            transport_backend = PROFILE_ID_TO_TRANSPORT_BACKEND.get(candidate_profile_id)
            if transport_backend is None:
                continue
            add_backend(transport_backend)

    if not out:
        raise ValueError("no transport backends were selected; pass --profile-id or fix the suite config")
    return out


def _append_optional_path_arg(command: list[str], flag: str, raw_path: Optional[Path]) -> None:
    if raw_path is None:
        return
    command.extend([flag, str(_resolve_repo_root_cli_path(raw_path=raw_path, field_name=flag.lstrip("-")))])


def _append_optional_str_arg(command: list[str], flag: str, raw_value: Optional[str]) -> None:
    if raw_value is None:
        return
    command.extend([flag, raw_value])


def _build_release_step(
    *,
    scope: str,
    release_dir: Path,
    transport_backend: str,
    rdma_backend: str,
    with_tikv_runtime_value: str,
    reuse_existing_release: bool,
    profile_id: Optional[str] = None,
) -> dict[str, Any]:
    if transport_backend != script_utils.PUBLIC_TRANSPORT_BACKEND:
        raise ValueError(
            "public pack_release.py only supports the fixed closed_sdk transport backend "
            f"{script_utils.PUBLIC_TRANSPORT_BACKEND!r}, got {transport_backend!r}"
        )
    if reuse_existing_release:
        step: dict[str, Any] = {
            "action": "validate_release",
            "scope": scope,
            "release_dir": str(release_dir),
            "transport_backend": transport_backend,
        }
        if profile_id is not None:
            step["profile_id"] = profile_id
        return step
    command = [
        sys.executable,
        str((REPO_ROOT / "setup_and_pack" / "pack_release.py").resolve()),
        "--rdma-backend",
        rdma_backend,
        "--release-dir",
        str(release_dir),
        "--with-tikv-runtime",
        with_tikv_runtime_value,
    ]
    step = {
        "action": "pack_release",
        "scope": scope,
        "release_dir": str(release_dir),
        "transport_backend": transport_backend,
        "command": command,
    }
    if profile_id is not None:
        step["profile_id"] = profile_id
    return step


def _profile_release_authority_dir(*, release_dir: Path, profile_id: str) -> Path:
    # The public tcp_thread profile is a compatibility alias for the top-level release.
    if profile_id == script_utils.PUBLIC_TRANSPORT_PROFILE_ID:
        return release_dir.resolve()
    return (release_dir / "profiles" / profile_id).resolve()


def _build_all_profiles_plan(
    *,
    release_dir: Path,
    config_path: Path,
    top_level_transport_backend: str,
    rdma_backend: str,
    with_tikv_runtime: bool,
    transport_backends: list[str],
    reuse_existing_release: bool,
    skip_top_level_release: bool,
    repo_test_rsc_root: Optional[Path],
    prepare_config: Optional[Path],
    baseline_source_root: Optional[Path],
    redis_bundle_src: Optional[Path],
    alluxio_bundle_src: Optional[Path],
    build_redis_bundle_docker: bool,
    redis_version: Optional[str],
    redis_source_url: Optional[str],
    redis_source_sha256: Optional[str],
    redis_docker_image: Optional[str],
) -> list[dict[str, Any]]:
    with_tikv_runtime_value = "true" if with_tikv_runtime else "false"
    plan: list[dict[str, Any]] = []
    planned_release_dirs: set[Path] = set()

    if not skip_top_level_release:
        planned_release_dirs.add(release_dir.resolve())
        plan.append(
            _build_release_step(
                scope="top_level_release",
                release_dir=release_dir,
                transport_backend=top_level_transport_backend,
                rdma_backend=rdma_backend,
                with_tikv_runtime_value=with_tikv_runtime_value,
                reuse_existing_release=reuse_existing_release,
            )
        )

    for transport_backend in transport_backends:
        profile_id = script_utils.TRANSPORT_PROFILE_IDS.get(transport_backend)
        if profile_id is None:
            raise ValueError(f"unsupported transport backend: {transport_backend}")
        profile_release_dir = _profile_release_authority_dir(
            release_dir=release_dir,
            profile_id=profile_id,
        )
        if profile_release_dir not in planned_release_dirs:
            planned_release_dirs.add(profile_release_dir)
            plan.append(
                _build_release_step(
                    scope="profile_release",
                    release_dir=profile_release_dir,
                    transport_backend=transport_backend,
                    rdma_backend=rdma_backend,
                    with_tikv_runtime_value=with_tikv_runtime_value,
                    reuse_existing_release=reuse_existing_release,
                    profile_id=profile_id,
                )
            )
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--transport-backend",
            transport_backend,
            "--rdma-backend",
            rdma_backend,
            "--release-dir",
            str(release_dir),
            "--out-dir",
            str((release_dir / "test_rsc" / profile_id).resolve()),
        ]
        _append_optional_path_arg(command, "--repo-test-rsc-root", repo_test_rsc_root)
        _append_optional_path_arg(command, "--prepare-config", prepare_config)
        _append_optional_path_arg(command, "--baseline-source-root", baseline_source_root)
        _append_optional_path_arg(command, "--redis-bundle-src", redis_bundle_src)
        _append_optional_path_arg(command, "--alluxio-bundle-src", alluxio_bundle_src)
        if build_redis_bundle_docker:
            command.append("--build-redis-bundle-docker")
        _append_optional_str_arg(command, "--redis-version", redis_version)
        _append_optional_str_arg(command, "--redis-source-url", redis_source_url)
        _append_optional_str_arg(command, "--redis-source-sha256", redis_source_sha256)
        _append_optional_str_arg(command, "--redis-docker-image", redis_docker_image)
        plan.append(
            {
                "action": "prepare_test_rsc",
                "config_path": str(config_path),
                "profile_id": profile_id,
                "transport_backend": transport_backend,
                "release_dir": str(release_dir),
                "out_dir": str((release_dir / "test_rsc" / profile_id).resolve()),
                "command": command,
            }
        )
    return plan


def _print_preparation_plan(plan: list[dict[str, Any]]) -> None:
    for index, step in enumerate(plan, start=1):
        action = step["action"]
        scope = step.get("scope")
        profile_id = step.get("profile_id")
        prefix = f"[{index}] {action}"
        if scope is not None:
            prefix += f" scope={scope}"
        if profile_id is not None:
            prefix += f" profile_id={profile_id}"
        print(prefix, flush=True)
        if action == "validate_release":
            print(f"    release_dir={step['release_dir']}", flush=True)
            continue
        print("    " + " ".join(step["command"]), flush=True)


def _resolve_optional_repo_root_cli_path(*, raw_path: Optional[Path], field_name: str) -> Optional[Path]:
    if raw_path is None:
        return None
    return _resolve_repo_root_cli_path(raw_path=raw_path, field_name=field_name)


def _resolve_prepare_config_path(*, raw_path: Optional[Path], repo_test_rsc_root: Path) -> Optional[Path]:
    if raw_path is not None:
        return _resolve_repo_root_cli_path(raw_path=raw_path, field_name="prepare-config")
    candidate = (repo_test_rsc_root / TEST_RSC_PREPARE_CONFIG_NAME).resolve()
    if candidate.exists():
        return candidate
    return None


def _load_prepare_config(*, path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise RuntimeError(f"test_rsc prepare config is missing or not a file: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise RuntimeError(f"test_rsc prepare config must be a YAML mapping: {path}")
    return raw


def _validate_existing_release_dir(*, release_dir: Path) -> None:
    release_dir = release_dir.resolve()
    if not release_dir.exists() or not release_dir.is_dir():
        raise RuntimeError(f"pack_test_stack_rsc requires an existing release dir: {release_dir}")

    required_file_globs = ("fluxon-*.whl",)
    required_relpaths = (
        "install.py",
        RELEASE_MANIFEST_FILENAME,
    )
    required_dir_relpaths = ("ext_images",)

    for pattern in required_file_globs:
        matches = sorted(path for path in release_dir.glob(pattern) if path.is_file())
        if not matches:
            raise RuntimeError(
                f"pack_test_stack_rsc requires existing release artifact matching {pattern!r} under {release_dir}"
            )
    for relpath in required_relpaths:
        path = release_dir / relpath
        if not path.exists() or not path.is_file():
            raise RuntimeError(
                f"pack_test_stack_rsc requires existing release file {relpath!r} under {release_dir}"
            )
    for relpath in required_dir_relpaths:
        path = release_dir / relpath
        if not path.exists() or not path.is_dir():
            raise RuntimeError(
                f"pack_test_stack_rsc requires existing release directory {relpath!r} under {release_dir}"
            )


def _pack_ci_src(*, repo_root: Path, out_path: Path) -> None:
    required_repo_paths = (
        repo_root / "setup_and_pack",
        repo_root / "fluxon_py",
        repo_root / "fluxon_rs",
        repo_root / "deployment",
        repo_root / "examples",
        repo_root / "fluxon_test_stack",
        repo_root / "setup.py",
        repo_root / "scripts" / "_build_doc_site_in_container_inner.py",
        repo_root / "README.md",
    )
    for path in required_repo_paths:
        if not path.exists():
            print(f"Missing required CI source input: {path}")
            raise SystemExit(1)

    def build_tarball() -> None:
        with script_utils.stage("Staging CI sources (git ls-files)"):
            with tempfile.TemporaryDirectory(prefix="fluxon_pack_test_src_", dir=str(out_path.parent)) as td:
                stage_root = Path(td)
                staged_relpaths = _git_stage_ci_source_tree(repo_root=repo_root, stage_root=stage_root)
                script_utils.tar_gz(
                    cwd=stage_root,
                    out_path=out_path,
                    inputs=staged_relpaths,
                    honor_vcs_ignores=False,
                )

    script_utils.build_cached_tarball(
        rule=script_utils.ArtifactRule(
            name="ci source tarball",
            stamp_path=out_path.parent / f"{out_path.name}.input.sha256",
            compute_digest=lambda: _compute_ci_source_digest(repo_root=repo_root),
            outputs_ready=out_path.exists,
        ),
        out_path=out_path,
        build_tarball=build_tarball,
    )


def _git_stage_ci_source_tree(*, repo_root: Path, stage_root: Path) -> list[str]:
    selected: list[str] = []
    seen_dirs: set[str] = set()
    for rel in _collect_ci_source_relpaths(repo_root=repo_root):
        src_path = (repo_root / rel).resolve()
        dst_path = (stage_root / rel).resolve()
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if src_path.is_symlink():
            os.symlink(os.readlink(src_path), dst_path)
        elif src_path.is_file():
            shutil.copy2(src_path, dst_path)
        elif src_path.is_dir():
            dst_path.mkdir(parents=True, exist_ok=True)
        else:
            continue
        selected.append(rel)
        parent = Path(rel).parent
        while str(parent) not in (".", ""):
            parent_str = parent.as_posix()
            if parent_str not in seen_dirs:
                seen_dirs.add(parent_str)
                (stage_root / parent_str).mkdir(parents=True, exist_ok=True)
            parent = parent.parent
    if not selected:
        raise RuntimeError("git-based CI source staging produced no files")
    return selected


def _collect_ci_source_relpaths(*, repo_root: Path) -> list[str]:
    return list(
        collect_source_profile_relpaths(
            repo_root=repo_root.resolve(),
            profile=SOURCE_SELECTION_PROFILE_SOURCE_PACK,
        )
    )


def _compute_ci_source_digest(*, repo_root: Path) -> str:
    return script_utils.compute_paths_digest(
        [(repo_root / rel).resolve() for rel in _collect_ci_source_relpaths(repo_root=repo_root)],
        relative_to=repo_root,
        mode=script_utils.PathDigestMode.PACK_INPUTS,
        algorithm=script_utils.PathHashAlgorithm.SHA256,
        ignored_dir_names=(),
        ignored_file_names=(),
        ignored_file_suffixes=(),
    )


def _ci_source_relpath_excluded(relpath: str) -> bool:
    return source_profile_relpath_excluded(
        profile=SOURCE_SELECTION_PROFILE_SOURCE_PACK,
        relpath=relpath,
    )


def _relpath_matches_exclude_patterns(relpath: str, patterns: tuple[str, ...]) -> bool:
    relpath = relpath.strip("/")
    rel_parts = Path(relpath).parts
    rel_name = rel_parts[-1] if rel_parts else relpath
    for pattern in patterns:
        if pattern.endswith("/"):
            dir_name = pattern.rstrip("/")
            if dir_name in rel_parts:
                return True
            continue
        if fnmatch.fnmatch(relpath, pattern) or fnmatch.fnmatch(rel_name, pattern):
            return True
    return False


def _pack_ci_ext_rsc(*, repo_root: Path, out_path: Path) -> None:
    canonical_release_tar = (repo_root / "fluxon_release" / out_path.name).resolve()
    if canonical_release_tar.exists():
        if canonical_release_tar == out_path.resolve():
            print(f"Using canonical CI external runtime tarball in-place: {canonical_release_tar}")
            return
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(canonical_release_tar, out_path)
        print(f"Using canonical CI external runtime tarball without rebuild: {canonical_release_tar}")
        return

    # The offline RDMA runtime authority already converged to target/cxxpacked in the
    # build and release flows. test_rsc packing must consume the same authority object
    # directly instead of drifting back to the old target/packed name.
    packed_dir = repo_root / "fluxon_rs" / "target" / PACKED_RUNTIME_DIR_NAME
    if not packed_dir.exists():
        print(f"Missing required packed runtime directory: {packed_dir}")
        raise SystemExit(1)

    if out_path.exists():
        print(f"Using existing CI external runtime tarball without rebuild: {out_path}")
        return

    def build_tarball() -> None:
        with script_utils.stage("Staging CI external runtime resources (packed)"):
            with tempfile.TemporaryDirectory(prefix="fluxon_pack_test_ext_", dir=str(out_path.parent)) as td:
                stage_root = Path(td)
                packed_stage_root = stage_root / "fluxon_rs" / "target" / PACKED_RUNTIME_DIR_NAME
                packed_stage_root.mkdir(parents=True, exist_ok=True)
                for rel_name in PACKED_RUNTIME_ROOT_NAMES:
                    src = packed_dir / rel_name
                    if not src.exists():
                        continue
                    script_utils.rsync_stage(
                        repo_root=repo_root,
                        src=src,
                        dst=packed_stage_root / rel_name,
                        honor_gitignore=False,
                        exclude_rel_paths=PACKED_RUNTIME_EXCLUDE_REL_PATHS,
                    )
                script_utils.tar_gz(
                    cwd=stage_root,
                    out_path=out_path,
                    inputs=["fluxon_rs"],
                    honor_vcs_ignores=False,
                )

    script_utils.build_cached_tarball(
        rule=script_utils.tarball_rule(
            name="ci ext runtime tarball",
            out_path=out_path,
            input_paths=[packed_dir],
            relative_to=repo_root,
        ),
        out_path=out_path,
        build_tarball=build_tarball,
    )


def _stage_repo_test_rsc_tree(*, repo_test_rsc_root: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not repo_test_rsc_root.exists():
        return
    if not repo_test_rsc_root.is_dir():
        print(f"Repository test_rsc root must be a directory: {repo_test_rsc_root}")
        raise SystemExit(1)
    for src in sorted(repo_test_rsc_root.iterdir()):
        dst = out_dir / src.name
        if dst.exists():
            print(f"Refusing to overwrite staged test resource: {dst}")
            raise SystemExit(1)
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=False)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
    script_utils.prune_stage_paths(out_dir, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _release_shared_baselines_root(*, release_dir: Path) -> Path:
    return (
        release_dir.resolve()
        / TEST_RSC_RELEASE_SHARED_ROOT_DIRNAME
        / TEST_RSC_RELEASE_SHARED_BASELINES_DIRNAME
    ).resolve()


def _canonical_profile_test_rsc_root(*, profile_id: str) -> Path:
    return (
        REPO_ROOT.resolve()
        / TEST_RSC_CANONICAL_PROFILE_ROOT_DIRNAME
        / TEST_RSC_RELEASE_SHARED_ROOT_DIRNAME
        / profile_id
    ).resolve()


def _stage_release_shared_baselines_into_root(*, release_dir: Path, prepared_root: Path) -> None:
    shared_baselines_root = _release_shared_baselines_root(release_dir=release_dir)
    if not shared_baselines_root.exists():
        return
    if not shared_baselines_root.is_dir():
        raise RuntimeError(f"release test_rsc baselines authority must be a directory: {shared_baselines_root}")
    prepared_root.mkdir(parents=True, exist_ok=True)
    baselines_dst = prepared_root / "baselines"
    if baselines_dst.exists():
        raise RuntimeError(f"prepared test_rsc baselines path already exists before release authority stage: {baselines_dst}")
    shutil.copytree(shared_baselines_root, baselines_dst, dirs_exist_ok=False)
    script_utils.prune_stage_paths(baselines_dst, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _stage_canonical_profile_prepared_resources_into_root(*, profile_id: str, prepared_root: Path) -> None:
    canonical_profile_root = _canonical_profile_test_rsc_root(profile_id=profile_id)
    if not canonical_profile_root.exists():
        return
    if not canonical_profile_root.is_dir():
        raise RuntimeError(
            f"canonical profile test_rsc authority must be a directory: {canonical_profile_root}"
        )
    prepared_root.mkdir(parents=True, exist_ok=True)
    for root_name in TEST_RSC_PROFILE_PREPARED_RESOURCE_ROOT_NAMES:
        src = canonical_profile_root / root_name
        if not src.exists():
            continue
        dst = prepared_root / root_name
        if dst.exists():
            raise RuntimeError(
                "prepared test_rsc resource path already exists before canonical profile stage: "
                f"{dst}"
            )
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=False)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        script_utils.prune_stage_paths(dst, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _stage_prepared_test_rsc(*, prepared_root: Path, out_dir: Path) -> None:
    if not prepared_root.exists():
        return
    for src in sorted(prepared_root.iterdir()):
        dst = out_dir / src.name
        _remove_path(dst)
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=False)
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
    script_utils.prune_stage_paths(out_dir, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _prepare_baselines_into_root(
    *,
    prepared_root: Path,
    baseline_source_root: Optional[Path],
    redis_bundle_src: Optional[Path],
    alluxio_bundle_src: Optional[Path],
) -> None:
    explicit_sources = {
        "redis": redis_bundle_src,
        "alluxio": alluxio_bundle_src,
    }
    for spec in BASELINE_BUNDLE_SPECS:
        explicit_src = explicit_sources[spec["id"]]
        dir_source, archive_source = _resolve_baseline_sources(
            spec=spec,
            baseline_source_root=baseline_source_root,
            explicit_source=explicit_src,
        )
        if dir_source is None and archive_source is None:
            continue
        _materialize_baseline_bundle(
            prepared_root=prepared_root,
            spec=spec,
            dir_source=dir_source,
            archive_source=archive_source,
        )
    script_utils.prune_stage_paths(prepared_root, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _prepare_configured_test_rsc_resources_into_root(
    *,
    prepared_root: Path,
    prepare_config: dict[str, Any],
    scratch_root: Path,
) -> None:
    python_runtime_cfg_raw = prepare_config.get("python_runtime")
    if python_runtime_cfg_raw is not None:
        if not isinstance(python_runtime_cfg_raw, dict):
            raise RuntimeError("test_rsc prepare config `python_runtime` must be a mapping")
        _prepare_python_runtime_wheelhouse_into_root(
            prepared_root=prepared_root,
            scratch_root=scratch_root,
            python_runtime_cfg=python_runtime_cfg_raw,
        )
    mooncake_cfg_raw = prepare_config.get("mooncake")
    if mooncake_cfg_raw is not None:
        if not isinstance(mooncake_cfg_raw, dict):
            raise RuntimeError("test_rsc prepare config `mooncake` must be a mapping")
        _prepare_mooncake_wheel_into_root(
            prepared_root=prepared_root,
            scratch_root=scratch_root,
            mooncake_cfg=mooncake_cfg_raw,
        )
    script_utils.prune_stage_paths(prepared_root, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _prepare_python_runtime_wheelhouse_into_root(
    *,
    prepared_root: Path,
    scratch_root: Path,
    python_runtime_cfg: dict[str, Any],
) -> None:
    python_abi = _prepare_config_optional_str(
        python_runtime_cfg.get("python_abi"),
        "python_runtime.python_abi",
    )
    if python_abi is None:
        python_abi = DEFAULT_TEST_STACK_PYTHON_ABI
    if python_abi != DEFAULT_TEST_STACK_PYTHON_ABI:
        raise RuntimeError(
            "test_rsc prepare config `python_runtime.python_abi` must match the current TEST_STACK runtime: "
            f"{DEFAULT_TEST_STACK_PYTHON_ABI}"
        )

    platform_tag = _prepare_config_optional_str(
        python_runtime_cfg.get("platform"),
        "python_runtime.platform",
    )
    if platform_tag is None:
        platform_tag = DEFAULT_TEST_STACK_WHEEL_PLATFORM

    dependency_set_ids, dependency_sets = _prepare_python_runtime_dependency_sets(
        python_runtime_cfg.get("dependency_sets")
    )
    wheelhouse_root = (
        prepared_root
        / TEST_RSC_PYTHON_RUNTIME_REL_PARENT
        / python_abi
        / TEST_RSC_PYTHON_RUNTIME_WHEELHOUSE_DIRNAME
    ).resolve()
    wheelhouse_root.mkdir(parents=True, exist_ok=True)
    expected_specs = _python_runtime_expected_wheel_specs(
        dependency_set_ids=dependency_set_ids,
        dependency_sets=dependency_sets,
    )
    existing_names = sorted(path.name for path in wheelhouse_root.glob("*.whl"))
    if _wheelhouse_satisfies_specs(existing_names=existing_names, expected_specs=expected_specs):
        print(f"Using existing prepared TEST_STACK runtime wheelhouse: {wheelhouse_root}")
        return

    scratch_download_root = (
        scratch_root
        / "python_runtime"
        / python_abi
        / TEST_RSC_PYTHON_RUNTIME_WHEELHOUSE_DIRNAME
    ).resolve()
    _remove_path(scratch_download_root)
    scratch_download_root.mkdir(parents=True, exist_ok=True)
    _download_python_runtime_wheels(
        out_dir=scratch_download_root,
        python_abi=python_abi,
        platform_tag=platform_tag,
        expected_specs=expected_specs,
    )
    _remove_path(wheelhouse_root)
    shutil.copytree(scratch_download_root, wheelhouse_root, dirs_exist_ok=False)
    print(f"Prepared TEST_STACK runtime wheelhouse: {wheelhouse_root}")


def _prepare_python_runtime_dependency_sets(
    raw_value: Any,
) -> tuple[tuple[str, ...], dict[str, tuple[dict[str, str], ...]]]:
    if raw_value is None:
        raise RuntimeError("test_rsc prepare config `python_runtime.dependency_sets` must be a mapping")
    if not isinstance(raw_value, dict):
        raise RuntimeError("test_rsc prepare config `python_runtime.dependency_sets` must be a mapping")
    out: dict[str, tuple[dict[str, str], ...]] = {}
    for raw_set_id, raw_set_cfg in raw_value.items():
        if not isinstance(raw_set_id, str):
            raise RuntimeError("test_rsc prepare config `python_runtime.dependency_sets` keys must be strings")
        set_id = raw_set_id.strip()
        if not set_id:
            raise RuntimeError("test_rsc prepare config `python_runtime.dependency_sets` keys must be non-empty")
        if not isinstance(raw_set_cfg, dict):
            raise RuntimeError(
                f"test_rsc prepare config `python_runtime.dependency_sets.{set_id}` must be a mapping"
            )
        requirements_raw = raw_set_cfg.get("requirements")
        out[set_id] = _prepare_python_runtime_requirement_specs(
            requirements_raw,
            field_name=f"python_runtime.dependency_sets.{set_id}.requirements",
        )
    if "base" not in out:
        raise RuntimeError("test_rsc prepare config `python_runtime.dependency_sets.base` is required")
    return (tuple(out.keys()), out)


def _prepare_python_runtime_requirement_specs(
    raw_value: Any,
    *,
    field_name: str,
) -> tuple[dict[str, str], ...]:
    if not isinstance(raw_value, list):
        raise RuntimeError(f"test_rsc prepare config `{field_name}` must be a list")
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw_item in enumerate(raw_value):
        if not isinstance(raw_item, dict):
            raise RuntimeError(f"test_rsc prepare config `{field_name}[{index}]` must be a mapping")
        pinned_raw = raw_item.get("pinned")
        if not isinstance(pinned_raw, str):
            raise RuntimeError(f"test_rsc prepare config `{field_name}[{index}].pinned` must be a string")
        pinned = pinned_raw.strip()
        if not pinned:
            raise RuntimeError(f"test_rsc prepare config `{field_name}[{index}].pinned` must be non-empty")
        if pinned.count("==") != 1:
            raise RuntimeError(
                f"test_rsc prepare config `{field_name}[{index}].pinned` must use exact `name==version` syntax"
            )
        name, version = pinned.split("==", 1)
        if not name or not version:
            raise RuntimeError(
                f"test_rsc prepare config `{field_name}[{index}].pinned` must use exact `name==version` syntax"
            )
        source_raw = raw_item.get("source")
        if not isinstance(source_raw, str):
            raise RuntimeError(f"test_rsc prepare config `{field_name}[{index}].source` must be a string")
        source = source_raw.strip().lower()
        if source not in ("wheel", "sdist"):
            raise RuntimeError(
                f"test_rsc prepare config `{field_name}[{index}].source` must be `wheel` or `sdist`"
            )
        key = (_normalize_python_distribution_name(name), version)
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": name, "version": version, "source": source})
    return tuple(out)


def _python_runtime_expected_wheel_specs(
    *,
    dependency_set_ids: tuple[str, ...],
    dependency_sets: dict[str, tuple[dict[str, str], ...]],
) -> tuple[dict[str, str], ...]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    if "base" not in dependency_set_ids:
        raise RuntimeError("python_runtime dependency_sets must contain `base`")
    for set_id in dependency_set_ids:
        for spec in dependency_sets[set_id]:
            key = (_normalize_python_distribution_name(spec["name"]), spec["version"])
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(spec))
    return tuple(out)


def _wheelhouse_satisfies_specs(
    *,
    existing_names: list[str],
    expected_specs: tuple[dict[str, str], ...],
) -> bool:
    normalized_existing = tuple(_normalize_wheel_distribution_name_from_filename(name) for name in existing_names)
    for spec in expected_specs:
        prefix = f"{_normalize_python_distribution_name(spec['name'])}-{spec['version']}-"
        if not any(name.startswith(prefix) and name.endswith(".whl") for name in normalized_existing):
            return False
    return True


def _download_python_runtime_wheels(
    *,
    out_dir: Path,
    python_abi: str,
    platform_tag: str,
    expected_specs: tuple[dict[str, str], ...],
) -> None:
    abi_suffix = python_abi.removeprefix("cpython")
    cp_tag = "cp" + abi_suffix.replace(".", "")
    wheel_specs: list[str] = []
    sdist_specs: list[str] = []
    for spec in expected_specs:
        pinned = f"{spec['name']}=={spec['version']}"
        if spec["source"] == "sdist":
            sdist_specs.append(pinned)
        else:
            wheel_specs.append(pinned)

    if wheel_specs:
        argv = [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--only-binary=:all:",
            "--dest",
            str(out_dir),
            "--implementation",
            "cp",
            "--python-version",
            abi_suffix,
            "--abi",
            cp_tag,
            "--platform",
            platform_tag,
        ]
        argv.extend(wheel_specs)
        subprocess.check_call(argv, cwd=str(REPO_ROOT))

    for pinned in sdist_specs:
        argv = [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(out_dir),
            pinned,
        ]
        subprocess.check_call(argv, cwd=str(REPO_ROOT))
    downloaded_names = sorted(path.name for path in out_dir.glob("*.whl"))
    if not _wheelhouse_satisfies_specs(existing_names=downloaded_names, expected_specs=expected_specs):
        raise RuntimeError(
            "downloaded TEST_STACK runtime wheelhouse is incomplete: "
            f"out_dir={out_dir} expected={[spec['name'] + '==' + spec['version'] for spec in expected_specs]}"
        )


def _normalize_python_distribution_name(name: str) -> str:
    normalized = name.strip().lower().replace("-", "_").replace(".", "_")
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    if not normalized:
        raise RuntimeError("python distribution name normalization produced an empty value")
    return normalized


def _normalize_wheel_distribution_name_from_filename(filename: str) -> str:
    if not filename.endswith(".whl"):
        return filename
    parts = filename[:-4].split("-")
    if len(parts) < 5:
        return filename[:-4]
    dist = parts[0]
    version = parts[1]
    remainder = "-".join(parts[2:])
    return f"{_normalize_python_distribution_name(dist)}-{version}-{remainder}.whl"


def _prepare_mooncake_wheel_into_root(
    *,
    prepared_root: Path,
    scratch_root: Path,
    mooncake_cfg: dict[str, Any],
) -> None:
    wheel_url_raw = mooncake_cfg.get("wheel_url")
    if wheel_url_raw is None:
        return
    if not isinstance(wheel_url_raw, str) or not wheel_url_raw.strip():
        raise RuntimeError("test_rsc prepare config `mooncake.wheel_url` must be a non-empty string")
    wheel_url = wheel_url_raw.strip()
    wheel_name = _prepare_config_optional_str(mooncake_cfg.get("wheel_name"), "mooncake.wheel_name")
    if wheel_name is None:
        parsed = urllib.parse.urlparse(wheel_url)
        wheel_name = Path(parsed.path).name.strip()
    if not wheel_name:
        raise RuntimeError(f"cannot derive Mooncake wheel filename from URL: {wheel_url}")
    wheel_sha256 = _prepare_config_optional_sha256(
        mooncake_cfg.get("wheel_sha256"),
        "mooncake.wheel_sha256",
    )
    wheel_dest = (prepared_root / TEST_RSC_MOONCAKE_WHEEL_REL_PARENT / wheel_name).resolve()
    if wheel_dest.exists():
        if not wheel_dest.is_file():
            raise RuntimeError(f"prepared Mooncake wheel path must be a file: {wheel_dest}")
        if wheel_sha256 is not None:
            existing_sha256 = _sha256_file(wheel_dest)
            if existing_sha256 != wheel_sha256:
                raise RuntimeError(
                    "prepared Mooncake wheel sha256 mismatch: "
                    f"expected={wheel_sha256} actual={existing_sha256} path={wheel_dest}"
                )
        print(f"Using existing prepared Mooncake wheel: {wheel_dest}")
        return

    scratch_root.mkdir(parents=True, exist_ok=True)
    download_path = (scratch_root / wheel_name).resolve()
    _download_file(url=wheel_url, dest_path=download_path)
    actual_sha256 = _sha256_file(download_path)
    if wheel_sha256 is not None and actual_sha256 != wheel_sha256:
        raise RuntimeError(
            "Mooncake wheel sha256 mismatch: "
            f"expected={wheel_sha256} actual={actual_sha256} path={download_path}"
        )
    wheel_dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(download_path, wheel_dest)
    print(f"Mooncake wheel URL: {wheel_url}")
    print(f"Mooncake wheel sha256: {actual_sha256}")
    print(f"Prepared Mooncake wheel: {wheel_dest}")


def _prepare_config_optional_str(raw_value: Any, field_name: str) -> Optional[str]:
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise RuntimeError(f"test_rsc prepare config `{field_name}` must be a string")
    value = raw_value.strip()
    if not value:
        raise RuntimeError(f"test_rsc prepare config `{field_name}` must be non-empty when set")
    return value


def _prepare_config_optional_sha256(raw_value: Any, field_name: str) -> Optional[str]:
    value = _prepare_config_optional_str(raw_value, field_name)
    if value is None:
        return None
    normalized = value.lower()
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise RuntimeError(f"test_rsc prepare config `{field_name}` must be a 64-char hex sha256")
    return normalized


def _build_redis_bundle_with_docker(
    *,
    scratch_root: Path,
    redis_version: str,
    redis_source_url: Optional[str],
    redis_source_sha256: Optional[str],
    docker_image: str,
) -> Path:
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        raise RuntimeError("docker is required for --build-redis-bundle-docker, but was not found in PATH")
    if not redis_version:
        raise RuntimeError("redis version must be non-empty")

    scratch_root.mkdir(parents=True, exist_ok=True)
    download_root = scratch_root / "downloads"
    workspace_root = scratch_root / "workspace"
    output_root = scratch_root / "out"
    download_root.mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)

    source_url = redis_source_url or DEFAULT_REDIS_DOWNLOAD_URL_TEMPLATE.format(version=redis_version)
    source_tarball = download_root / f"redis-{redis_version}.tar.gz"
    _download_file(url=source_url, dest_path=source_tarball)
    actual_sha256 = _sha256_file(source_tarball)
    if redis_source_sha256 is not None and actual_sha256 != redis_source_sha256.lower():
        raise RuntimeError(
            "redis source sha256 mismatch: "
            f"expected={redis_source_sha256.lower()} actual={actual_sha256} path={source_tarball}"
        )

    subprocess.check_call(
        [
            docker_bin,
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-v",
            f"{scratch_root}:/io",
            "-w",
            "/io/workspace",
            docker_image,
            "bash",
            "-lc",
            (
                "set -euo pipefail\n"
                "tar xf /io/downloads/redis-" + redis_version + ".tar.gz\n"
                "cd redis-" + redis_version + "\n"
                "make distclean >/dev/null 2>&1 || true\n"
                "make -j\"$(nproc)\" BUILD_TLS=no MALLOC=libc redis-server\n"
                "rm -rf /io/out/redis_bundle\n"
                "mkdir -p /io/out/redis_bundle/bin\n"
                "install -Dm755 src/redis-server /io/out/redis_bundle/bin/redis-server\n"
                "cd /io/out\n"
                "rm -f redis_bundle.tar.gz\n"
                "tar czf redis_bundle.tar.gz redis_bundle\n"
            ),
        ]
    )
    bundle_dir = output_root / "redis_bundle"
    bundle_tar = output_root / "redis_bundle.tar.gz"
    if not bundle_dir.is_dir():
        raise RuntimeError(f"Docker-built redis bundle dir is missing: {bundle_dir}")
    if not bundle_tar.is_file():
        raise RuntimeError(f"Docker-built redis bundle archive is missing: {bundle_tar}")
    print(f"Redis source: {source_url}")
    print(f"Redis source sha256: {actual_sha256}")
    print(f"Redis bundle dir: {bundle_dir}")
    print(f"Redis bundle tarball: {bundle_tar}")
    return bundle_dir


def _download_file(*, url: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    curl_bin = shutil.which("curl")
    if curl_bin is not None:
        subprocess.check_call([curl_bin, "-fL", url, "-o", str(dest_path)])
        return
    req = urllib.request.Request(url, headers={"User-Agent": "fluxon-test-stack/1.0"})
    with urllib.request.urlopen(req) as response, dest_path.open("wb") as fh:
        shutil.copyfileobj(response, fh)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_baseline_sources(
    *,
    spec: dict[str, str],
    baseline_source_root: Optional[Path],
    explicit_source: Optional[Path],
) -> tuple[Optional[Path], Optional[Path]]:
    bundle_name = spec["bundle_name"]
    archive_name = f"{bundle_name}.tar.gz"
    if explicit_source is not None:
        if not explicit_source.exists():
            raise RuntimeError(f"baseline source does not exist for {spec['id']}: {explicit_source}")
        if explicit_source.is_dir():
            return explicit_source, None
        if explicit_source.is_file():
            if explicit_source.name != archive_name:
                raise RuntimeError(
                    f"baseline archive name mismatch for {spec['id']}: expected {archive_name}, got {explicit_source.name}"
                )
            return None, explicit_source
        raise RuntimeError(f"unsupported baseline source path type for {spec['id']}: {explicit_source}")

    if baseline_source_root is None:
        return None, None
    dir_candidates, archive_candidates = _baseline_source_candidates(
        baseline_source_root=baseline_source_root,
        spec=spec,
    )
    dir_source = next((candidate for candidate in dir_candidates if candidate.is_dir()), None)
    archive_source = next((candidate for candidate in archive_candidates if candidate.is_file()), None)
    return dir_source, archive_source


def _baseline_source_candidates(
    *,
    baseline_source_root: Path,
    spec: dict[str, str],
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    bundle_name = spec["bundle_name"]
    archive_name = f"{bundle_name}.tar.gz"
    logical_id = spec["id"]
    prefixes = (
        baseline_source_root / logical_id,
        baseline_source_root / "baselines" / logical_id,
    )
    dir_candidates = tuple(prefix / bundle_name for prefix in prefixes)
    archive_candidates = tuple(prefix / archive_name for prefix in prefixes)
    return dir_candidates, archive_candidates


def _materialize_baseline_bundle(
    *,
    prepared_root: Path,
    spec: dict[str, str],
    dir_source: Optional[Path],
    archive_source: Optional[Path],
) -> None:
    bundle_name = spec["bundle_name"]
    rel_parent = Path(spec["rel_parent"])
    bundle_parent = prepared_root / rel_parent
    bundle_dir = bundle_parent / bundle_name
    bundle_archive = bundle_parent / f"{bundle_name}.tar.gz"
    bundle_parent.mkdir(parents=True, exist_ok=True)
    _remove_path(bundle_dir)
    _remove_path(bundle_archive)
    if dir_source is not None:
        shutil.copytree(dir_source, bundle_dir, dirs_exist_ok=False)
        script_utils.tar_gz(
            cwd=bundle_parent,
            out_path=bundle_archive,
            inputs=[bundle_name],
            honor_vcs_ignores=False,
        )
    elif archive_source is not None:
        shutil.copy2(archive_source, bundle_archive)
        _extract_bundle_archive(
            archive_path=bundle_archive,
            out_dir=bundle_parent,
            expected_root_name=bundle_name,
        )
    else:
        return
    if not bundle_dir.is_dir():
        raise RuntimeError(f"prepared baseline directory is missing for {spec['id']}: {bundle_dir}")
    if not bundle_archive.is_file():
        raise RuntimeError(f"prepared baseline archive is missing for {spec['id']}: {bundle_archive}")


def _sync_prepared_baselines_into_release_tree(*, prepared_root: Path, release_dir: Path) -> None:
    prepared_baselines_root = prepared_root / "baselines"
    if not prepared_baselines_root.exists():
        return
    release_shared_baselines_root = _release_shared_baselines_root(release_dir=release_dir)
    release_shared_baselines_root.parent.mkdir(parents=True, exist_ok=True)
    _remove_path(release_shared_baselines_root)
    shutil.copytree(prepared_baselines_root, release_shared_baselines_root, dirs_exist_ok=False)
    script_utils.prune_stage_paths(release_shared_baselines_root, TEST_RSC_REPO_TREE_EXCLUDE_REL_PATHS)


def _extract_bundle_archive(*, archive_path: Path, out_dir: Path, expected_root_name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir_resolved = out_dir.resolve()
    with tarfile.open(archive_path, mode="r:gz") as tar:
        for member in tar.getmembers():
            member_path = (out_dir / member.name).resolve()
            if member_path != out_dir_resolved and out_dir_resolved not in member_path.parents:
                raise RuntimeError(f"archive member escapes target directory: {archive_path} member={member.name!r}")
        tar.extractall(out_dir)
    expected_root = out_dir / expected_root_name
    if not expected_root.exists():
        raise RuntimeError(
            f"baseline archive extracted without expected root {expected_root_name!r}: {archive_path}"
        )


def _remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink()


def _test_rsc_manifest_file_list(*, out_dir: Path, prepared_root: Path) -> list[Path]:
    files: list[Path] = []
    for fixed_name in ("src_ci.tar.gz", "fluxon_ci_ext_rsc.tar.gz"):
        fixed_path = out_dir / fixed_name
        if not fixed_path.exists():
            print(f"Cannot write test_rsc manifest: missing file: {fixed_path}")
            raise SystemExit(1)
        files.append(fixed_path)
    if prepared_root.exists():
        for prepared_child in sorted(prepared_root.iterdir()):
            staged_path = out_dir / prepared_child.name
            if not staged_path.exists():
                print(f"Cannot write test_rsc manifest: missing staged test_rsc path: {staged_path}")
                raise SystemExit(1)
            if staged_path.is_file():
                files.append(staged_path)
                continue
            files.extend(sorted(path for path in staged_path.rglob("*") if path.is_file()))
    return files


def _write_sha256_manifest(*, out_path: Path, root_dir: Path, files: list[Path]) -> None:
    lines: list[str] = []
    for path in files:
        if not path.exists():
            print(f"Cannot write test_rsc manifest: missing file: {path}")
            raise SystemExit(1)
        digest = subprocess.check_output(["sha256sum", str(path)], text=True).split()[0]
        lines.append(f"{digest}  {path.relative_to(root_dir).as_posix()}\n")
    out_path.write_text("".join(lines), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
