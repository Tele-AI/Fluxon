#!/usr/bin/env python3
from __future__ import annotations

import atexit
import argparse
import hashlib
import json
import os
import re
import shutil
import shlex
import subprocess
import sys
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
PARENT_SCRIPTS_DIR = SCRIPT_DIR.parent
repo_root_str = str(REPO_ROOT)
if repo_root_str in sys.path:
    sys.path.remove(repo_root_str)
sys.path.insert(0, repo_root_str)
parent_scripts_dir_str = str(PARENT_SCRIPTS_DIR)
if parent_scripts_dir_str in sys.path:
    sys.path.remove(parent_scripts_dir_str)
sys.path.insert(0, parent_scripts_dir_str)

import yaml

from lib_layout import (
    apply_layout,
    build_layout,
    build_runtime_targets,
    load_experiment_config_root,
    load_experiment_spec,
    render_layout_summary,
)
from setup_and_pack.closed_sdk_contract import (
    CLOSED_SDK_CONSUMER_BOUNDARY_MODE,
    rewrite_fluxon_native_export_bundle,
)
from setup_and_pack.public_workspace_contract import (
    collect_public_workspace_input_relative_paths,
)
from utils.sudo_prefix_utils import host_sudo_prefix
import utils as script_utils
ABI3_SMOKE_TEST_INTERPRETERS = (
    "/opt/python/cp310-cp310/bin/python",
    "/opt/python/cp311-cp311/bin/python",
    "/opt/python/cp312-cp312/bin/python",
)
CONTAINER_PROFILE_PATH = "/nix_profile"
CONTAINER_WORKSPACE_SEED_PATH = f"{CONTAINER_PROFILE_PATH}/workspace_seed"
CONTAINER_NATIVE_RUNTIME_PATH = f"{CONTAINER_PROFILE_PATH}/native"
CONTAINER_TARGET_SUPPORT_PATH = f"{CONTAINER_PROFILE_PATH}/target_support"
CONTAINER_CLOSED_SDK_RUNTIME_ROOT_PATH = "/tmp/fluxon_sdk_runtime"
CONTAINER_CLOSED_SDK_LIB_PATH = f"{CONTAINER_CLOSED_SDK_RUNTIME_ROOT_PATH}/lib"
CONTAINER_VENDOR_RUNTIME_PATH = f"{CONTAINER_NATIVE_RUNTIME_PATH}/vendor_runtime"
CONTAINER_TARGET_CACHE_MOUNT_PATH = "/cargo_target"
CONTAINER_INSTANCE_TARGET_PATH = "/workspace/fluxon_rs/target"
CONTAINER_RELEASE_PATH = "/release"
DOCKER_NOFILE_LIMIT = 65535
CARGO_BUILD_JOBS = 8
SKIP_PREPARE_BUILD_ENV = "FLUXON_SKIP_PREPARE_BUILD"
PYO3_CHECKSUM_FILE_NAME = ".fluxon_pyo3_inputs.sha256"
NATIVE_CACHE_STAMP_FILE_PREFIX = ".fluxon_native_inputs"
STANDARD_SYSTEM_PATH_ENTRIES = (
    "/usr/local/sbin",
    "/usr/local/bin",
    "/usr/sbin",
    "/usr/bin",
    "/sbin",
    "/bin",
)
PUBLISHED_PROFILE_NIX_FILE_NAME = "profile.nix"
TARGET_CACHE_MANIFEST_FILE_NAME = "target_cache_manifest.json"
TARGET_CACHE_SCHEMA_VERSION = 4
AUTHORITY_MERMAID_FILE_NAME = "dag.mmd"
SOURCE_KIND_LOCAL_PATH = "local_path"
FLUXON_COMMU_AUTHORITY_RELATIVE_PATH = "fluxon_rs/fluxon_commu"
FLUXON_NATIVE_EXPORT_RELATIVE_DIR = Path("lib") / "cmake" / "FluxonNative"
FLUXON_NATIVE_EXPORT_FILE_NAMES = (
    "FluxonNativeConfig.cmake",
    "FluxonNativeTargets.cmake",
    "FluxonNativeLinkArgs.txt",
)
AUTHORITY_SCHEMA_VERSION = 1
AUTHORITY_EXPORT_PUBLISHED_PROFILE_NIX = "published_profile_nix"
AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN = "manylinux_toolchain"
AUTHORITY_OBJECT_KIND_WORKSPACE_SEED = "workspace_seed"
AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE = "fluxon_commu_runtime_source"
AUTHORITY_OBJECT_KIND_TARGET_SUPPORT = "target_support"
AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME = "vendor_runtime"
AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME = "native_runtime"
AUTHORITY_OBJECT_KIND_CXXPACKED = "cxxpacked"
AUTHORITY_OBJECT_KIND_PYO3_WHEEL = "pyo3_wheel"
AUTHORITY_OBJECT_KIND_MANYLINUX_PROFILE = "manylinux_profile"
SUPPORTED_AUTHORITY_OBJECT_KINDS = frozenset(
    {
        AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN,
        AUTHORITY_OBJECT_KIND_WORKSPACE_SEED,
        AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE,
        AUTHORITY_OBJECT_KIND_TARGET_SUPPORT,
        AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME,
        AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME,
        AUTHORITY_OBJECT_KIND_CXXPACKED,
        AUTHORITY_OBJECT_KIND_PYO3_WHEEL,
        AUTHORITY_OBJECT_KIND_MANYLINUX_PROFILE,
    }
)
NATIVE_RUNTIME_OBJECT_KINDS = frozenset(
    (
        AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME,
    )
)
NATIVE_RUNTIME_PREPARE_SCENARIOS = {
    AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME: "cargo_closed_sdk_runtime",
}
NATIVE_RUNTIME_OBJECT_KIND_BY_ID = {
    "nativeRuntime": AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME,
}
PREPARE_BUILD_BINARY_PATH_PREFIX = "PREPARE_BUILD_BINARY_PATH="
VENDOR_RUNTIME_DIR_NAME = "vendor_runtime"
NATIVE_RUNTIME_DIR_NAME = "native_runtime"
CXXPACKED_DIR_NAME = "cxxpacked"
GENERATED_RELEASE_RELATIVE_ROOT = Path("fluxon_release") / "generated"
GENERATED_TOOLCHAIN_DIR_NAME = "toolchain"
PREPARE_BUILD_GENERATED_DIR_NAME = "prepare_build"
PREPARE_BUILD_RESOURCE_STORE_DIR_NAME = "resource_store"
PUBLIC_CLOSED_SDK_REPO_RELATIVE_ROOT = Path("fluxon_release") / "closed_sdk"
PUBLIC_WHEEL_RUNTIME_HELPER_REPO_RELATIVE_PATH = Path("setup_and_pack") / "utils" / "wheel_runtime_helper.py"
PACK_RELEASE_IN_CONTAINER_REPO_RELATIVE_PATH = Path("setup_and_pack") / "nix" / "pack_release_in_container.py"
WHEEL_FINALIZE_STEP_KIND_ADD_OFFLINE_RDMA_SHARED_LIBRARIES = "add_offline_rdma_shared_libraries"
WHEEL_FINALIZE_STEP_KIND_ADD_NATIVE_PLUGINS = "add_native_plugins"
WHEEL_FINALIZE_STEP_KIND_ADD_VENDOR_RUNTIME = "add_vendor_runtime"
SUPPORTED_WHEEL_FINALIZE_STEP_KINDS = frozenset(
    (
        WHEEL_FINALIZE_STEP_KIND_ADD_OFFLINE_RDMA_SHARED_LIBRARIES,
        WHEEL_FINALIZE_STEP_KIND_ADD_NATIVE_PLUGINS,
        WHEEL_FINALIZE_STEP_KIND_ADD_VENDOR_RUNTIME,
    )
)
SUPPORTED_TARGET_CACHE_GENERATOR_KINDS = frozenset()
TEMP_WORKSPACE_MOUNT_DIRS: list[Path] = []


def _cleanup_temp_workspace_mount_dirs() -> None:
    while TEMP_WORKSPACE_MOUNT_DIRS:
        path = TEMP_WORKSPACE_MOUNT_DIRS.pop()
        try:
            shutil.rmtree(path)
        except Exception:
            pass


atexit.register(_cleanup_temp_workspace_mount_dirs)
PYO3_INPUT_RELATIVE_PATHS_BY_TRANSPORT_BACKEND = {
    "fastws": (),
    "tquic": (),
    "sockudo_ws": (),
    "tcp": (),
    "tcp_thread": (),
}
PYO3_INPUT_RELATIVE_PATHS_BY_RDMA_BACKEND = {
    "closed_sdk": ("fluxon_release/closed_sdk",),
}
TRANSPORT_BACKEND_FEATURES = {
    "fastws": ["fastws_transport"],
    "tquic": ["tquic_transport"],
    "sockudo_ws": ["sockudo_ws_transport"],
    "tcp": ["tcp_transport"],
    "tcp_thread": ["tcp_thread_transport"],
}
# Public fluxon_pyo3 only accepts its own transport toggles. Closed SDK linkage is
# provided through fluxon_commu_closed_sdk_consumer via FLUXON_COMMU_CLOSED_SDK_ROOT,
# so the wheel build must not inject closed-only feature names into the open crate.
PYO3_BASE_FEATURES = ("p2p_transfer",)
RDMA_BACKEND_FEATURES = {
    "closed_sdk": [],
}
CORE_SYSTEM_RUNTIME_LIB_PREFIXES = (
    "libc.so",
    "libm.so",
    "libpthread.so",
    "libdl.so",
    "librt.so",
    "libutil.so",
    "libresolv.so",
    "ld-linux",
    "ld64-",
    "libgcc_s.so",
)
MANYLINUX_CXX_RUNTIME_LIBRARY_NAMES = (
    "libstdc++.so.6",
    "libgomp.so.1",
)

def _dedupe_relative_paths(relative_paths: tuple[str, ...]) -> tuple[str, ...]:
    ordered_relative_paths: list[str] = []
    seen_relative_paths: set[str] = set()
    for relative_path in relative_paths:
        if relative_path in seen_relative_paths:
            continue
        seen_relative_paths.add(relative_path)
        ordered_relative_paths.append(relative_path)
    return tuple(ordered_relative_paths)


def pyo3_workspace_copy_relative_paths(transport_backend: str, rdma_backend: str) -> tuple[str, ...]:
    del transport_backend
    del rdma_backend
    return collect_public_workspace_input_relative_paths(repo_root=REPO_ROOT)


def _pyo3_input_relative_paths(transport_backend: str, rdma_backend: str) -> tuple[str, ...]:
    return pyo3_workspace_copy_relative_paths(transport_backend, rdma_backend)


def _wheel_variant_key(transport_backend: str, rdma_backend: str) -> str:
    if rdma_backend != "closed_sdk":
        raise RuntimeError(f"unsupported rdma_backend for public wheel variant key: {rdma_backend!r}")
    return f"te_{rdma_backend}.{transport_backend}"


def _transport_backend_feature_csv(transport_backend: str, rdma_backend: str) -> str:
    features = TRANSPORT_BACKEND_FEATURES[transport_backend] + RDMA_BACKEND_FEATURES[rdma_backend]
    return ",".join(features)


def _get_arch_name() -> str:
    mach = os.uname().machine.lower()
    if mach in ("x86_64", "amd64"):
        return "x86_64"
    if mach in ("aarch64", "arm64"):
        return "aarch64"
    raise RuntimeError(f"Unsupported architecture: {mach}")


def _read_fluxon_rust_toolchain_channel(*, project_root: Path) -> str:
    toolchain_path = project_root / "fluxon_rs" / "rust-toolchain.toml"
    if not toolchain_path.exists():
        raise RuntimeError(f"missing Rust toolchain authority file: {toolchain_path}")
    text = toolchain_path.read_text(encoding="utf-8")
    in_toolchain_section = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_toolchain_section = line == "[toolchain]"
            continue
        if not in_toolchain_section:
            continue
        match = re.match(r'^channel\s*=\s*"([^"]+)"', line)
        if match is None:
            continue
        channel = match.group(1).strip()
        if channel:
            return channel
    raise RuntimeError(f"failed to parse Rust toolchain channel from: {toolchain_path}")


def _container_rust_toolchain_bin_path(*, project_root: Path) -> str:
    channel = _read_fluxon_rust_toolchain_channel(project_root=project_root)
    arch = _get_arch_name()
    return f"/root/.rustup/toolchains/{channel}-{arch}-unknown-linux-gnu/bin"


def _unique_existing_paths(paths: list[Path]) -> list[Path]:
    unique_paths: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        resolved_key = str(path.resolve())
        if resolved_key in seen:
            continue
        seen.add(resolved_key)
        unique_paths.append(path)
    return unique_paths


def _input_roots(repo_root: Path, relative_paths: tuple[str, ...]) -> tuple[Path, ...]:
    return tuple(repo_root / relative_path for relative_path in relative_paths)


def _compute_inputs_digest(repo_root: Path, relative_paths: tuple[str, ...]) -> str:
    return script_utils.compute_paths_digest(
        _input_roots(repo_root, relative_paths),
        relative_to=repo_root,
        mode=script_utils.PathDigestMode.CONTENTS_ONLY,
        algorithm=script_utils.PathHashAlgorithm.MD5,
        ignored_dir_names=(),
        ignored_file_names=(),
        ignored_file_suffixes=(),
    )


@dataclass(frozen=True)
class CacheStep:
    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]


def _load_prepare_cache_steps(repo_root: Path, scenario: str) -> tuple[CacheStep, ...]:
    prepare_build_path = _resolve_prepare_build_authority_path(repo_root=repo_root)
    if prepare_build_path is None:
        return (
            CacheStep(
                name=f"public_{scenario}_prebuilt_closed_sdk",
                inputs=(str(PUBLIC_CLOSED_SDK_REPO_RELATIVE_ROOT),),
                outputs=(str(PUBLIC_CLOSED_SDK_REPO_RELATIVE_ROOT),),
            ),
        )
    cmd = [
        "python3",
        str(prepare_build_path),
        "--scenario",
        scenario,
        "--print-cache-steps-json",
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise Exception(
            "failed to load prepare-build cache steps\n"
            + f"cmd={' '.join(shlex.quote(part) for part in cmd)}\n"
            + f"stdout={proc.stdout}\n"
            + f"stderr={proc.stderr}"
        )
    try:
        raw_steps = json.loads(proc.stdout)
    except json.JSONDecodeError as err:
        raise Exception(
            "prepare_build.py emitted invalid cache-step JSON\n"
            + f"stdout={proc.stdout}\n"
            + f"stderr={proc.stderr}"
        ) from err
    steps = [
        CacheStep(
            name=raw_step["name"],
            inputs=tuple(raw_step["inputs"]),
            outputs=tuple(raw_step["outputs"]),
        )
        for raw_step in raw_steps
    ]
    if len(steps) == 0:
        raise Exception(f"no CACHE_STEPS declared for scenario={scenario}")
    return tuple(steps)


def _load_prepare_target_dir_names(repo_root: Path, scenario: str) -> tuple[str, ...]:
    prepare_build_path = _resolve_prepare_build_authority_path(repo_root=repo_root)
    if prepare_build_path is None:
        return ()
    cmd = [
        "python3",
        str(prepare_build_path),
        "--scenario",
        scenario,
        "--print-target-dir-names-json",
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise Exception(
            "failed to load prepare-build target dir names\n"
            + f"cmd={' '.join(shlex.quote(part) for part in cmd)}\n"
            + f"stdout={proc.stdout}\n"
            + f"stderr={proc.stderr}"
        )
    try:
        raw_names = json.loads(proc.stdout)
    except json.JSONDecodeError as err:
        raise Exception(
            "prepare_build.py emitted invalid target-dir JSON\n"
            + f"stdout={proc.stdout}\n"
            + f"stderr={proc.stderr}"
        ) from err
    if not isinstance(raw_names, list):
        raise Exception(f"prepare_build.py target-dir payload must be a list: {raw_names!r}")
    dir_names: list[str] = []
    for raw_name in raw_names:
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise Exception(f"prepare_build.py target-dir entry must be a non-empty string: {raw_name!r}")
        dir_names.append(raw_name.strip())
    return tuple(dir_names)


def _resolve_prepare_build_authority_path(*, repo_root: Path) -> Path | None:
    candidate = repo_root / "setup_and_pack" / "pub_prepare_build.py"
    if candidate.is_file():
        return candidate.resolve()
    return None


def _require_prepare_build_authority_path(*, repo_root: Path) -> Path:
    prepare_build_path = _resolve_prepare_build_authority_path(repo_root=repo_root)
    if prepare_build_path is not None:
        return prepare_build_path
    raise RuntimeError(
        "public workspace pack requires setup_and_pack/pub_prepare_build.py when external mount "
        "authorities must be materialized automatically"
    )


class PyO3PackState:
    def __init__(
        self,
        repo_root: Path,
        manylinux_version: str,
        *,
        transport_backend: str,
        rdma_backend: str,
        release_dir: Path,
    ):
        self.repo_root = repo_root
        self.rs_root = repo_root / "fluxon_rs"
        self.transport_backend = transport_backend
        self.rdma_backend = rdma_backend
        self.variant_key = _wheel_variant_key(transport_backend, rdma_backend)
        self.target_wheels_dir = self.rs_root / "wheels" / self.variant_key
        self.release_dir = release_dir.resolve()
        self.checksum_path = self.rs_root / f".fluxon_pyo3_inputs.{self.variant_key}.sha256"
        self.legacy_checksum_path = self.rs_root / "checksum.pkl"
        self.manylinux_version = manylinux_version
        self.wheel_rule = script_utils.ArtifactRule(
            name="Rust PyO3 wheel",
            stamp_path=self.checksum_path,
            compute_digest=self.current_checksum,
            outputs_ready=lambda: self.find_cached_wheel() is not None,
        )

    def current_checksum(self) -> str:
        return _compute_inputs_digest(
            self.repo_root,
            _pyo3_input_relative_paths(self.transport_backend, self.rdma_backend),
        ) + f"|transport_backend={self.transport_backend}|rdma_backend={self.rdma_backend}"

    def find_cached_wheel(self) -> Path | None:
        if not self.target_wheels_dir.exists():
            return None
        arch = _get_arch_name()
        token = f"manylinux_{self.manylinux_version}_{arch}"
        candidates = sorted(
            wheel_path
            for wheel_path in self.target_wheels_dir.glob("fluxon_pyo3-*.whl")
            if token in wheel_path.name
        )
        if len(candidates) == 0:
            return None
        if len(candidates) != 1:
            names = ", ".join(path.name for path in candidates)
            raise RuntimeError(f"ambiguous cached wheel for {token} in {self.target_wheels_dir}: {names}")
        return candidates[0]

    def wheel_check(self) -> script_utils.ArtifactCheck:
        return self.wheel_rule.check()

    def reuse_existing_wheel(self) -> bool:
        wheel_check = self.wheel_check()
        if not wheel_check.is_ready():
            return False
        wheel_path = self.find_cached_wheel()
        if wheel_path is None:
            return False
        self.release_dir.mkdir(parents=True, exist_ok=True)
        dst = self.release_dir / wheel_path.name
        shutil.copyfile(wheel_path, dst)
        (self.release_dir / PYO3_CHECKSUM_FILE_NAME).write_text(
            self.current_checksum() + "\n",
            encoding="utf-8",
        )
        return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the isolated setup_and_pack/nix manylinux Fluxon pack experiment"
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Experiment pack config path",
    )
    parser.add_argument(
        "--apply-layout",
        action="store_true",
        help="Create or refresh the isolated project layout before printing or running docker",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the cold-start manylinux container build after printing the docker command",
    )
    parser.add_argument(
        "--publish-profile",
        action="store_true",
        help="Publish the bridge profile into the configured external profile root before preview or run",
    )
    args = parser.parse_args()

    config_path = args.config.resolve()
    cfg = load_experiment_config_root(config_path=config_path)
    spec = load_experiment_spec(config_path=config_path)
    runtime_targets = build_runtime_targets(spec=spec)
    manylinux_cfg = _require_mapping(cfg, "manylinux")
    backend_plan = _load_manylinux_backend_plan(config_root=cfg)
    derived_manylinux_version = _derive_manylinux_version(base_system=spec.base_system)

    runtime_image_ref = _require_explicit_image_ref(manylinux_cfg, "runtime_image_ref")
    transport_backend = _require_non_empty_string(manylinux_cfg, "transport_backend")
    rdma_backend = _require_non_empty_string(manylinux_cfg, "rdma_backend")
    cargo_registry_dir = _require_absolute_path(manylinux_cfg, "cargo_registry_dir")
    cargo_git_dir = _require_absolute_path(manylinux_cfg, "cargo_git_dir")
    selected_backend_plan = _select_backend_plan(
        backend_plan=backend_plan,
        rdma_backend=rdma_backend,
    )
    run_pack_flow = True

    for runtime_target in runtime_targets:
        layout = build_layout(spec=spec, runtime_target=runtime_target)
        release_dir = layout.instance_release_dir
        run_log_path = layout.instance_logs_dir / "pack_fluxonkv_pylib.run.log"

        print(render_layout_summary(spec=spec, runtime_target=runtime_target, layout=layout))

        if args.apply_layout or args.run:
            _ensure_bridge_prebuilt_writable_native_authority(
                spec=spec,
                runtime_target=runtime_target,
                transport_backend=transport_backend,
                selected_backend_plan=selected_backend_plan,
            )
            apply_layout(spec=spec, runtime_target=runtime_target, layout=layout)
        profile_dir, published_profile_dir = _resolve_runtime_profile_dir(
            spec=spec,
            runtime_target=runtime_target,
            layout=layout,
            transport_backend=transport_backend,
            selected_backend_plan=selected_backend_plan,
            manylinux_version=derived_manylinux_version,
            runtime_image_ref=runtime_image_ref,
            force_republish=args.publish_profile,
            config_path=config_path,
            config_root=cfg,
        )
        external_mounts = _resolve_external_mounts(
            spec=spec,
            profile_dir=profile_dir,
            manylinux_cfg=manylinux_cfg,
            selected_backend_plan=selected_backend_plan,
        )
        target_cache_descriptor = _build_target_cache_descriptor(
            spec=spec,
            runtime_target=runtime_target,
            layout=layout,
            profile_dir=profile_dir,
            transport_backend=transport_backend,
            runtime_image_ref=runtime_image_ref,
            selected_backend_plan=selected_backend_plan,
            external_mounts=external_mounts,
        )
        target_cache_key_descriptor = _build_target_cache_key_descriptor(
            spec=spec,
            runtime_target=runtime_target,
            layout=layout,
            transport_backend=transport_backend,
        )
        target_cache_key = _sha256_json_bytes(raw=target_cache_key_descriptor)
        target_cache_dir = layout.target_caches_root_dir / target_cache_key
        target_cache_manifest_path = target_cache_dir / TARGET_CACHE_MANIFEST_FILE_NAME
        _maybe_promote_compatible_target_cache_dir(
            target_caches_root=layout.target_caches_root_dir,
            target_cache_dir=target_cache_dir,
            target_cache_key_descriptor=target_cache_key_descriptor,
        )
        if args.apply_layout or args.run:
            _prepare_manylinux_target_cache_view(
                spec=spec,
                manylinux_cfg=manylinux_cfg,
                profile_dir=profile_dir,
                target_cache_dir=target_cache_dir,
                target_cache_descriptor=target_cache_descriptor,
                selected_backend_plan=selected_backend_plan,
            )
        workspace_mount_dir = _resolve_workspace_mount_dir(profile_dir=profile_dir)
        if args.run:
            _clear_directory(release_dir)
            cargo_registry_dir.mkdir(parents=True, exist_ok=True)
            cargo_git_dir.mkdir(parents=True, exist_ok=True)

        docker_argv = _build_docker_argv(
            spec=spec,
            runtime_image_ref=runtime_image_ref,
            manylinux_version=derived_manylinux_version,
            transport_backend=transport_backend,
            selected_backend_plan=selected_backend_plan,
            workspace_mount_dir=workspace_mount_dir,
            target_cache_dir=target_cache_dir,
            release_dir=release_dir,
            profile_dir=profile_dir,
            external_mounts=external_mounts,
            extra_host_mount_paths=_bridge_prebuilt_direct_mount_paths(
                spec=spec,
                profile_dir=profile_dir,
                published_profile_dir=published_profile_dir,
                selected_backend_plan=selected_backend_plan,
            ),
            cargo_registry_dir=cargo_registry_dir,
            cargo_git_dir=cargo_git_dir,
        )

        print(f"config_path={config_path}")
        print(f"architecture={runtime_target.architecture}")
        print(f"profile_dir={profile_dir}")
        if published_profile_dir is not None:
            print(f"published_profile_dir={published_profile_dir}")
            print(f"published_profile_nix_path={published_profile_dir / PUBLISHED_PROFILE_NIX_FILE_NAME}")
        print(f"workspace_mount_dir={workspace_mount_dir}")
        print(f"target_cache_key={target_cache_key}")
        print(f"target_cache_dir={target_cache_dir}")
        print(f"target_cache_manifest_path={target_cache_manifest_path}")
        print(f"release_dir={release_dir}")
        print(f"run_log_path={run_log_path}")
        print(f"manylinux_version={derived_manylinux_version}")
        print(f"rdma_backend={rdma_backend}")
        print(f"runtime_image_ref={runtime_image_ref}")
        print(f"docker_command={_shell_join(docker_argv)}")

        if args.run:
            _run_with_tee_log(argv=docker_argv, log_path=run_log_path)

    if not args.run:
        print("mode=preview")
        return 0

    print("mode=run")
    return 0


def _ensure_bridge_prebuilt_writable_native_authority(
    *,
    spec,
    runtime_target,
    transport_backend: str,
    selected_backend_plan: dict,
) -> None:
    if spec.profile_source.source_kind != "bridge_prebuilt":
        return
    writable_native_dir_name = _backend_writable_native_dir_name(selected_backend_plan=selected_backend_plan)
    if writable_native_dir_name is None:
        return
    build_root_path = spec.profile_source.build_root_path
    if build_root_path is None:
        raise RuntimeError("bridge_prebuilt profile.build_root_path is required for writable native authority")
    fluxon_rs_root = Path(build_root_path).resolve() / "fluxon_rs"
    if not fluxon_rs_root.is_dir():
        raise RuntimeError(
            "bridge_prebuilt profile.build_root_path must contain fluxon_rs for writable native authority: "
            f"{fluxon_rs_root}"
        )
    # target/ is a writable build output root and may be absent in a clean checkout.
    native_runtime_store_path = fluxon_rs_root / "target"
    native_runtime_store_path.mkdir(parents=True, exist_ok=True)
    _initialize_writable_native_runtime_dir(
        staged_dir=native_runtime_store_path / writable_native_dir_name,
        dir_name=writable_native_dir_name,
        authoritative_export_dir=None,
    )
    _materialize_bridge_prebuilt_external_mount_authorities(
        spec=spec,
        runtime_target=runtime_target,
        transport_backend=transport_backend,
        selected_backend_plan=selected_backend_plan,
    )
    _seed_bridge_prebuilt_native_runtime_store_from_published_profile(
        spec=spec,
        runtime_target=runtime_target,
        transport_backend=transport_backend,
        selected_backend_plan=selected_backend_plan,
        build_root=Path(build_root_path).resolve(),
        generated_system_dir=_ensure_generated_system_dir(
            spec=spec,
            runtime_target=runtime_target,
        ),
        writable_native_dir_name=writable_native_dir_name,
    )


def _backend_prepare_build_scenario(*, selected_backend_plan: dict) -> str | None:
    native_object_id = _backend_native_object_id(selected_backend_plan)
    if native_object_id is None:
        return None
    native_object_kind = NATIVE_RUNTIME_OBJECT_KIND_BY_ID.get(native_object_id)
    if native_object_kind is None:
        raise RuntimeError(f"unknown backend native object id for prepare_build scenario: {native_object_id}")
    return NATIVE_RUNTIME_PREPARE_SCENARIOS[native_object_kind]


def _bridge_prebuilt_dynamic_target_support_dir_names(*, spec, selected_backend_plan: dict) -> tuple[str, ...]:
    if spec.profile_source.source_kind != "bridge_prebuilt":
        return ()
    if _backend_prepare_build_scenario(selected_backend_plan=selected_backend_plan) is None:
        return ()
    return tuple(spec.profile_layout.target_support_dir_names)


def _initialize_prepare_target_placeholder_dir(*, target_root: Path, dir_name: str) -> None:
    target_dir = target_root / dir_name
    if target_dir.is_symlink() or target_dir.is_file():
        target_dir.unlink()
    elif target_dir.exists():
        _sudo_remove_tree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_dir.chmod(0o777)
    if dir_name == VENDOR_RUNTIME_DIR_NAME:
        for subdir in (
            target_dir / "bin",
            target_dir / "etc" / "libibverbs.d",
            target_dir / "include",
            target_dir / "lib",
        ):
            subdir.mkdir(parents=True, exist_ok=True)
            subdir.chmod(0o777)
        lib64_dir = target_dir / "lib64"
        if lib64_dir.exists() or lib64_dir.is_symlink():
            if lib64_dir.is_symlink():
                lib64_dir.unlink()
        if not lib64_dir.exists():
            os.symlink("lib", lib64_dir)
        return
    if dir_name == CXXPACKED_DIR_NAME:
        for subdir in (
            target_dir / "bin",
            target_dir / "etc" / "libibverbs.d",
            target_dir / "include" / "infiniband",
            target_dir / "lib64" / "libibverbs",
        ):
            subdir.mkdir(parents=True, exist_ok=True)
            subdir.chmod(0o777)
        return


def _initialize_target_support_placeholder_dir(*, target_root: Path, dir_name: str) -> None:
    target_dir = target_root / dir_name
    if target_dir.is_symlink() or target_dir.is_file():
        target_dir.unlink()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_dir.chmod(0o777)


def _materialize_bridge_prebuilt_external_mount_authorities(
    *,
    spec,
    runtime_target,
    transport_backend: str,
    selected_backend_plan: dict,
) -> None:
    if spec.profile_source.source_kind != "bridge_prebuilt":
        return
    if not selected_backend_plan["external_mounts"]:
        return
    build_root_path = spec.profile_source.build_root_path
    if build_root_path is None:
        raise RuntimeError("bridge_prebuilt profile.build_root_path is required for external mount authority")
    prepare_build_scenario = _backend_prepare_build_scenario(selected_backend_plan=selected_backend_plan)
    if prepare_build_scenario is None:
        return
    build_root = Path(build_root_path).resolve()
    prepare_target_dir_names = _load_prepare_target_dir_names(build_root, prepare_build_scenario)
    dynamic_target_support_dir_names = set(
        _bridge_prebuilt_dynamic_target_support_dir_names(
            spec=spec,
            selected_backend_plan=selected_backend_plan,
        )
    )
    target_root = build_root / "fluxon_rs" / "target"
    missing_mount_names: list[str] = []
    for mount_spec in selected_backend_plan["external_mounts"]:
        candidate_root = (build_root / mount_spec["project_relative_path"]).resolve()
        missing_entries = _validate_external_mount_candidate(
            mount_name=mount_spec["name"],
            candidate_root=candidate_root,
        )
        if not missing_entries:
            continue
        missing_mount_names.append(mount_spec["name"])
        if mount_spec["name"] in prepare_target_dir_names:
            _initialize_prepare_target_placeholder_dir(target_root=target_root, dir_name=mount_spec["name"])
    missing_prepare_dir_names: list[str] = []
    writable_native_dir_name = _backend_writable_native_dir_name(
        selected_backend_plan=selected_backend_plan,
    )
    for dir_name in _required_native_dir_names(selected_backend_plan=selected_backend_plan):
        if dir_name == writable_native_dir_name:
            continue
        candidate_root = build_root / "fluxon_rs" / "target" / dir_name
        missing_entries = _bridge_prebuilt_seed_native_dir_missing_entries(
            dir_name=dir_name,
            candidate_dir=candidate_root,
        )
        if not missing_entries:
            continue
        missing_prepare_dir_names.append(dir_name)
        if dir_name in prepare_target_dir_names:
            _initialize_prepare_target_placeholder_dir(target_root=target_root, dir_name=dir_name)
    missing_target_support_dir_names: list[str] = []
    target_root = build_root / "fluxon_rs" / "target"
    for dir_name in spec.profile_layout.target_support_dir_names:
        candidate_root = target_root / dir_name
        if candidate_root.is_dir():
            continue
        missing_target_support_dir_names.append(dir_name)
        if dir_name in dynamic_target_support_dir_names:
            _initialize_target_support_placeholder_dir(target_root=target_root, dir_name=dir_name)
    if not missing_mount_names and not missing_prepare_dir_names and not missing_target_support_dir_names:
        return
    unresolved_mount_errors: list[str] = []
    for mount_spec in selected_backend_plan["external_mounts"]:
        candidate_root = (build_root / mount_spec["project_relative_path"]).resolve()
        missing_entries = _validate_external_mount_candidate(
            mount_name=mount_spec["name"],
            candidate_root=candidate_root,
        )
        if (
            missing_entries
            and mount_spec["name"] in prepare_target_dir_names
            and candidate_root.is_dir()
        ):
            continue
        if not missing_entries:
            continue
        unresolved_mount_errors.append(
            f"mount={mount_spec['name']} path={candidate_root} missing={', '.join(missing_entries)}"
        )
    unresolved_prepare_dir_errors: list[str] = []
    for dir_name in _required_native_dir_names(selected_backend_plan=selected_backend_plan):
        if dir_name == writable_native_dir_name:
            continue
        candidate_root = build_root / "fluxon_rs" / "target" / dir_name
        missing_entries = _bridge_prebuilt_seed_native_dir_missing_entries(
            dir_name=dir_name,
            candidate_dir=candidate_root,
        )
        if (
            missing_entries
            and dir_name in prepare_target_dir_names
            and candidate_root.is_dir()
        ):
            continue
        if not missing_entries:
            continue
        unresolved_prepare_dir_errors.append(
            f"dir={dir_name} path={candidate_root} missing={', '.join(missing_entries)}"
        )
    unresolved_target_support_errors = [
        f"dir={dir_name} path={build_root / 'fluxon_rs' / 'target' / dir_name}"
        for dir_name in spec.profile_layout.target_support_dir_names
        if not (build_root / "fluxon_rs" / "target" / dir_name).is_dir()
    ]
    if unresolved_mount_errors or unresolved_prepare_dir_errors or unresolved_target_support_errors:
        raise RuntimeError(
            "bridge_prebuilt authority placeholders remained incomplete: "
            + "; ".join(
                [
                    *unresolved_mount_errors,
                    *unresolved_prepare_dir_errors,
                    *unresolved_target_support_errors,
                ]
            )
        )


def _seed_bridge_prebuilt_native_runtime_store_from_published_profile(
    *,
    spec,
    runtime_target,
    transport_backend: str,
    selected_backend_plan: dict,
    build_root: Path,
    generated_system_dir: Path,
    writable_native_dir_name: str | None,
) -> None:
    native_target_dir = build_root / "fluxon_rs" / "target"
    # This optional host-side seed is only a reuse optimization for bridge_prebuilt.
    # The required cxxpacked payload is still materialized inside the manylinux container
    # by pub_prepare_build.py before the Rust build runs.
    reusable_cxxpacked_seed_dir = _resolve_bridge_prebuilt_seed_native_dir(
        spec=spec,
        runtime_target=runtime_target,
        transport_backend=transport_backend,
        dir_name=CXXPACKED_DIR_NAME,
        build_root=build_root,
        allow_missing=True,
    )
    for dir_name in _required_native_dir_names(selected_backend_plan=selected_backend_plan):
        if writable_native_dir_name is not None and dir_name == writable_native_dir_name:
            continue
        link_path = native_target_dir / dir_name
        if dir_name == VENDOR_RUNTIME_DIR_NAME:
            seed_vendor_runtime_dir = _resolve_bridge_prebuilt_seed_native_dir(
                spec=spec,
                runtime_target=runtime_target,
                transport_backend=transport_backend,
                dir_name=VENDOR_RUNTIME_DIR_NAME,
                build_root=build_root,
                allow_missing=True,
            )
            if seed_vendor_runtime_dir is None:
                continue
            materialized_vendor_runtime_dir = _materialize_bridge_prebuilt_vendor_runtime_seed(
                generated_system_dir=generated_system_dir,
                runtime_target=runtime_target,
                seed_vendor_runtime_dir=seed_vendor_runtime_dir,
                reusable_cxxpacked_seed_dir=reusable_cxxpacked_seed_dir,
            )
            _replace_workspace_entry(
                link_path=link_path,
                target_path=str(materialized_vendor_runtime_dir),
            )
            continue
        seed_dir = _resolve_bridge_prebuilt_seed_native_dir(
            spec=spec,
            runtime_target=runtime_target,
            transport_backend=transport_backend,
            dir_name=dir_name,
            build_root=build_root,
            allow_missing=True,
        )
        if seed_dir is None:
            continue
        _replace_workspace_entry(
            link_path=link_path,
            target_path=str(seed_dir),
        )


def _resolve_bridge_prebuilt_seed_native_dir(
    *,
    spec,
    runtime_target,
    transport_backend: str,
    dir_name: str,
    build_root: Path | None = None,
    allow_missing: bool = False,
) -> Path | None:
    inspected: list[str] = []

    local_candidate_dirs: list[Path] = []
    if build_root is not None:
        local_candidate_dirs.extend(
            [
                build_root / "fluxon_rs" / "target" / dir_name,
                build_root
                / GENERATED_RELEASE_RELATIVE_ROOT
                / spec.base_system
                / "bridge_prebuilt_external_mounts"
                / runtime_target.runtime_abi_key
                / dir_name,
            ]
        )
        if dir_name == VENDOR_RUNTIME_DIR_NAME:
            local_candidate_dirs.append(build_root / "fluxon_rs" / "target" / "vendor_runtime")
            authoritative_closed_sdk_root = _discover_authoritative_vendor_runtime_sdk_root(
                build_root=build_root,
                base_system=spec.base_system,
                runtime_abi_key=runtime_target.runtime_abi_key,
                closed_sdk_search_roots=spec.profile_source.closed_sdk_search_roots,
            )
            if authoritative_closed_sdk_root is not None:
                local_candidate_dirs.extend(
                    [
                        authoritative_closed_sdk_root / "native" / VENDOR_RUNTIME_DIR_NAME,
                        authoritative_closed_sdk_root / VENDOR_RUNTIME_DIR_NAME,
                    ]
                )

    for candidate_dir in local_candidate_dirs:
        inspected.append(str(candidate_dir))
        seed_missing_entries = _bridge_prebuilt_seed_native_dir_missing_entries(
            dir_name=dir_name,
            candidate_dir=candidate_dir,
        )
        if not seed_missing_entries:
            return candidate_dir.resolve()

    project_scope_id = hashlib.sha256(
        os.path.realpath(spec.project_root).encode("utf-8")
    ).hexdigest()
    profiles_root = (
        spec.project_data_root
        / "profile-store"
        / "projects"
        / project_scope_id
        / "substrates"
        / runtime_target.execution_substrate
        / "profiles"
        / runtime_target.base_system_key
        / runtime_target.runtime_abi_key
    )
    candidates: list[tuple[float, Path]] = []
    if profiles_root.is_dir():
        if dir_name == VENDOR_RUNTIME_DIR_NAME:
            profile_name_prefix = f"pack_release_{transport_backend}_closed_sdk_"
        else:
            profile_name_prefix = f"pack_release_{transport_backend}_"
        for profile_outer_dir in sorted(profiles_root.iterdir()):
            if not profile_outer_dir.is_dir() or not profile_outer_dir.name.startswith(profile_name_prefix):
                continue
            profile_dir = profile_outer_dir / profile_outer_dir.name
            if not profile_dir.is_dir():
                continue
            candidate_dir = profile_dir / "native" / dir_name
            inspected.append(str(candidate_dir))
            seed_missing_entries = _bridge_prebuilt_seed_native_dir_missing_entries(
                dir_name=dir_name,
                candidate_dir=candidate_dir,
            )
            if seed_missing_entries:
                continue
            manifest_path = profile_dir / "manifest.json"
            manifest_mtime = manifest_path.stat().st_mtime if manifest_path.is_file() else profile_dir.stat().st_mtime
            candidates.append((manifest_mtime, candidate_dir.resolve()))
    if not candidates:
        if allow_missing:
            return None
        raise RuntimeError(
            f"unable to locate a published manylinux profile with complete native seed authority for {dir_name}; searched="
            + ", ".join(inspected[:32])
        )
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def _bridge_prebuilt_seed_native_dir_missing_entries(*, dir_name: str, candidate_dir: Path) -> list[str]:
    if dir_name == VENDOR_RUNTIME_DIR_NAME:
        return _vendor_runtime_seed_missing_entries(candidate_dir)
    if dir_name == CXXPACKED_DIR_NAME:
        missing_entries = _cxxpacked_missing_entries(candidate_dir)
        driver_dir = candidate_dir / "etc" / "libibverbs.d"
        if not driver_dir.is_dir():
            missing_entries.append(str(driver_dir))
        elif not any(path.is_file() and path.name.endswith(".driver") for path in driver_dir.iterdir()):
            missing_entries.append(f"{driver_dir}/*.driver")
        return missing_entries
    if not candidate_dir.is_dir():
        return [str(candidate_dir)]
    return []


def _vendor_runtime_seed_missing_entries(candidate_root: Path) -> list[str]:
    missing_entries: list[str] = []
    lib_roots = [candidate_root / "lib", candidate_root / "lib64"]
    required_lib_prefixes = ("libfabric.so", "libibverbs.so", "libmlx5.so", "libmlx5-rdmav")
    for prefix in required_lib_prefixes:
        found = False
        for lib_root in lib_roots:
            if not lib_root.is_dir():
                continue
            if any(path.is_file() and path.name.startswith(prefix) for path in lib_root.iterdir()):
                found = True
                break
            provider_root = lib_root / "libibverbs"
            if any(path.is_file() and path.name.startswith(prefix) for path in provider_root.glob("*.so*")):
                found = True
                break
        if not found:
            missing_entries.append(f"{candidate_root}/lib{{,64}}/{prefix}*")
    required_soname_aliases = (
        "libibverbs.so.1",
        "libpsm_infinipath.so.1",
        "libpsm2.so.2",
        "libinfinipath.so.4",
    )
    for alias_name in required_soname_aliases:
        found = False
        for lib_root in lib_roots:
            if (lib_root / alias_name).exists():
                found = True
                break
        if not found:
            missing_entries.append(f"{candidate_root}/lib{{,64}}/{alias_name}")
    return missing_entries


def _discover_authoritative_vendor_runtime_sdk_root(
    *,
    build_root: Path,
    base_system: str,
    runtime_abi_key: str,
    closed_sdk_search_roots: tuple[str, ...],
) -> Path | None:
    candidate_sdk_roots: list[Path] = [
        (
            build_root
            / GENERATED_RELEASE_RELATIVE_ROOT
            / base_system
            / "bridge_prebuilt_external_mounts"
            / runtime_abi_key
        )
    ]
    for raw_search_root in closed_sdk_search_roots:
        search_root = Path(raw_search_root)
        if not search_root.is_dir():
            continue
        candidate_sdk_roots.append(search_root)
        candidate_sdk_roots.extend(
            sorted(
                search_root.glob(
                    f"fluxon*/fluxon_release/generated/{base_system}/bridge_prebuilt_external_mounts/{runtime_abi_key}"
                )
            )
        )
        candidate_sdk_roots.extend(sorted(search_root.glob("fluxon*/build/closed-sdk-check")))

    seen: set[str] = set()
    for sdk_root in candidate_sdk_roots:
        resolved_sdk_root = sdk_root.resolve()
        sdk_root_key = str(resolved_sdk_root)
        if sdk_root_key in seen:
            continue
        seen.add(sdk_root_key)
        for candidate_root in (
            resolved_sdk_root / "native" / VENDOR_RUNTIME_DIR_NAME,
            resolved_sdk_root / VENDOR_RUNTIME_DIR_NAME,
        ):
            if not _vendor_runtime_seed_missing_entries(candidate_root):
                return resolved_sdk_root
    return None


def _materialize_bridge_prebuilt_vendor_runtime_seed(
    *,
    generated_system_dir: Path,
    runtime_target,
    seed_vendor_runtime_dir: Path,
    reusable_cxxpacked_seed_dir: Path | None,
) -> Path:
    stage_parent = _ensure_generated_dir(
        generated_system_dir / "bridge_prebuilt_external_mounts" / runtime_target.runtime_abi_key
    )
    staged_root = stage_parent / VENDOR_RUNTIME_DIR_NAME
    same_root = staged_root.exists() and staged_root.resolve() == seed_vendor_runtime_dir.resolve()
    if not same_root:
        if staged_root.is_symlink():
            staged_root.unlink()
        elif staged_root.exists():
            _sudo_remove_tree(staged_root)
        _sudo_copy_path(
            source_path=seed_vendor_runtime_dir,
            target_path=staged_root,
        )
    driver_config_source_dir = None
    candidate_driver_config_dirs = [
        seed_vendor_runtime_dir / "etc" / "libibverbs.d",
    ]
    if reusable_cxxpacked_seed_dir is not None:
        candidate_driver_config_dirs.append(reusable_cxxpacked_seed_dir / "etc" / "libibverbs.d")
    for candidate_dir in candidate_driver_config_dirs:
        if candidate_dir.is_dir() and any(
            path.is_file() and path.name.endswith(".driver") for path in candidate_dir.iterdir()
        ):
            driver_config_source_dir = candidate_dir
            break
    if driver_config_source_dir is None:
        cxxpacked_driver_hint = (
            str(reusable_cxxpacked_seed_dir / "etc" / "libibverbs.d")
            if reusable_cxxpacked_seed_dir is not None
            else "<missing cxxpacked seed>"
        )
        raise RuntimeError(
            "vendor runtime driver config dir is missing from both vendor_runtime and cxxpacked seeds: "
            f"{seed_vendor_runtime_dir / 'etc' / 'libibverbs.d'}, "
            f"{cxxpacked_driver_hint}"
        )
    staged_driver_config_dir = staged_root / "etc" / "libibverbs.d"
    if staged_driver_config_dir.exists() and staged_driver_config_dir.resolve() == driver_config_source_dir.resolve():
        host_sudo = host_sudo_prefix()
        subprocess.run(
            host_sudo + ["chmod", "-R", "777", str(staged_root)],
            check=True,
        )
        return staged_root.resolve()
    if staged_driver_config_dir.is_symlink():
        staged_driver_config_dir.unlink()
    elif staged_driver_config_dir.exists():
        _sudo_remove_tree(staged_driver_config_dir)
    _sudo_copy_path(
        source_path=driver_config_source_dir,
        target_path=staged_driver_config_dir,
    )
    host_sudo = host_sudo_prefix()
    subprocess.run(
        host_sudo + ["chmod", "-R", "777", str(staged_root)],
        check=True,
    )
    return staged_root.resolve()


def _resolve_generated_authority_repo_root(*, spec) -> Path:
    if spec.profile_source.source_kind == "bridge_prebuilt":
        build_root_path = spec.profile_source.build_root_path
        if build_root_path is None:
            raise RuntimeError("bridge_prebuilt profile.build_root_path is required for generated authority roots")
        return Path(build_root_path).resolve()
    return spec.project_root.resolve()


def _ensure_generated_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o777)
    return path.resolve()


def _ensure_generated_system_dir(*, spec, runtime_target) -> Path:
    repo_root = _resolve_generated_authority_repo_root(spec=spec)
    _ = runtime_target
    return _ensure_generated_dir(repo_root / GENERATED_RELEASE_RELATIVE_ROOT / spec.base_system)


def _ensure_generated_prepare_build_resource_store(*, generated_system_dir: Path) -> Path:
    prepare_build_dir = _ensure_generated_dir(generated_system_dir / PREPARE_BUILD_GENERATED_DIR_NAME)
    resource_store_dir = _ensure_generated_dir(prepare_build_dir / PREPARE_BUILD_RESOURCE_STORE_DIR_NAME)
    _ensure_generated_dir(resource_store_dir / "objects")
    _ensure_generated_dir(resource_store_dir / "specs")
    return resource_store_dir.resolve()


def _build_docker_argv(
    *,
    spec,
    runtime_image_ref: str,
    manylinux_version: str,
    transport_backend: str,
    selected_backend_plan: dict,
    workspace_mount_dir: Path,
    target_cache_dir: Path,
    release_dir: Path,
    profile_dir: Path,
    external_mounts: tuple[dict, ...],
    extra_host_mount_paths: tuple[Path, ...],
    cargo_registry_dir: Path,
    cargo_git_dir: Path,
) -> list[str]:
    rdma_backend = selected_backend_plan["rdma_backend"]
    native_sync_dir_names = _required_native_dir_names(selected_backend_plan=selected_backend_plan)
    protoc_object_id = selected_backend_plan["protoc_object_id"]
    protoc_root = f"{CONTAINER_INSTANCE_TARGET_PATH}/{_native_object_dir_name(object_id=protoc_object_id)}"
    protoc_path = f"{protoc_root}/bin/protoc"
    prepare_build_scenario = _backend_prepare_build_scenario(selected_backend_plan=selected_backend_plan)
    finalize_script_path = Path("/workspace") / PACK_RELEASE_IN_CONTAINER_REPO_RELATIVE_PATH
    container_lines = [
        "set -euo pipefail",
        "cleanup_nix_profile() { chmod -R 777 /nix_profile >/dev/null 2>&1 || true; }",
        "trap cleanup_nix_profile EXIT",
        # Permission contract:
        # - The manylinux container materializes target-cache and release-side objects through bind mounts.
        # - Host-side umask policy is not the authority inside this shell payload.
        # - Set umask 000 here so container-created files and directories converge at creation time instead of
        #   relying on recursive chmod over mounted trees after the fact.
        "umask 000",
        f"ulimit -n {DOCKER_NOFILE_LIMIT}",
        f"mkdir -p {shlex.quote(CONTAINER_INSTANCE_TARGET_PATH)}/release/deps",
        f"export CARGO_TARGET_DIR={shlex.quote(CONTAINER_INSTANCE_TARGET_PATH)}",
    ]
    docker_env_args = [
        "-e",
        f"MANYLINUX_VERSION={manylinux_version}",
        "-e",
        f"CARGO_TARGET_DIR={CONTAINER_INSTANCE_TARGET_PATH}",
    ]
    path_entries = [
        _container_rust_toolchain_bin_path(project_root=workspace_mount_dir),
        *[
            f"{CONTAINER_INSTANCE_TARGET_PATH}/{_native_object_dir_name(object_id=object_id)}/bin"
            for object_id in selected_backend_plan["path_object_ids"]
        ],
    ]
    ld_library_entries: list[str] = []
    for object_id in selected_backend_plan["ld_library_object_ids"]:
        object_container_root = f"{CONTAINER_INSTANCE_TARGET_PATH}/{_native_object_dir_name(object_id=object_id)}"
        ld_library_entries.extend((f"{object_container_root}/lib64", f"{object_container_root}/lib"))
    protoc_container_root = f"{CONTAINER_INSTANCE_TARGET_PATH}/{_native_object_dir_name(object_id=protoc_object_id)}"
    for tool_runtime_dir in (f"{protoc_container_root}/lib64", f"{protoc_container_root}/lib"):
        if tool_runtime_dir not in ld_library_entries:
            ld_library_entries.append(tool_runtime_dir)
    for dir_name in selected_backend_plan["extra_path_dir_names"]:
        path_entries.append(f"{CONTAINER_NATIVE_RUNTIME_PATH}/{dir_name}/bin")
    for dir_name in selected_backend_plan["extra_ld_library_dir_names"]:
        object_container_root = f"{CONTAINER_NATIVE_RUNTIME_PATH}/{dir_name}"
        ld_library_entries.extend((f"{object_container_root}/lib64", f"{object_container_root}/lib"))
    docker_env_args.extend(
        [
            "-e",
            f"PROTOC={protoc_path}",
            "-e",
            f"{SKIP_PREPARE_BUILD_ENV}=1",
            "-e",
            f"PATH={':'.join([*path_entries, *STANDARD_SYSTEM_PATH_ENTRIES])}",
            "-e",
            f"LD_LIBRARY_PATH={':'.join(ld_library_entries)}",
        ]
    )
    container_lines.extend(
        [
            f"export PROTOC={shlex.quote(protoc_path)}",
            f"export PATH={':'.join(shlex.quote(entry) for entry in path_entries)}:$PATH",
            "export LD_LIBRARY_PATH="
            + ":".join(shlex.quote(entry) for entry in ld_library_entries)
            + "${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}",
            f"if [ ! -d {shlex.quote(str(Path('/workspace') / PUBLIC_CLOSED_SDK_REPO_RELATIVE_ROOT))} ]; then",
            "  echo 'missing public closed_sdk runtime under /workspace/fluxon_release/closed_sdk' >&2",
            "  exit 1",
            "fi",
            f"rm -rf {shlex.quote(CONTAINER_CLOSED_SDK_RUNTIME_ROOT_PATH)}",
            f"cp -a {shlex.quote(str(Path('/workspace') / PUBLIC_CLOSED_SDK_REPO_RELATIVE_ROOT))} {shlex.quote(CONTAINER_CLOSED_SDK_RUNTIME_ROOT_PATH)}",
            f"export FLUXON_COMMU_CLOSED_SDK_ROOT={shlex.quote(CONTAINER_CLOSED_SDK_RUNTIME_ROOT_PATH)}",
            f"export {SKIP_PREPARE_BUILD_ENV}=1",
        ]
    )
    for dir_name in native_sync_dir_names:
        container_lines.extend(
            [
                f"if [ -d \"$FLUXON_COMMU_CLOSED_SDK_ROOT/native/{dir_name}\" ]; then",
                f"  rm -rf \"$CARGO_TARGET_DIR/{dir_name}\"",
                f"  cp -a \"$FLUXON_COMMU_CLOSED_SDK_ROOT/native/{dir_name}\" \"$CARGO_TARGET_DIR/{dir_name}\"",
                "fi",
            ]
        )
    if prepare_build_scenario is not None:
        container_lines.extend(
            [
                "export FLUXON_PREPARE_BUILD_SKIP_EXISTING_VENDOR_RUNTIME=1",
                f"python3 /workspace/setup_and_pack/pub_prepare_build.py --scenario {shlex.quote(prepare_build_scenario)}",
                "if [ -x \"$CARGO_TARGET_DIR/cxxpacked/bin/protoc\" ]; then",
                "  export PROTOC=\"$CARGO_TARGET_DIR/cxxpacked/bin/protoc\"",
                "fi",
                "export PATH=\"$CARGO_TARGET_DIR/cxxpacked/bin:$CARGO_TARGET_DIR/vendor_runtime/bin:$PATH\"",
                "export LD_LIBRARY_PATH="
                "\"$CARGO_TARGET_DIR/cxxpacked/lib64:"
                "$CARGO_TARGET_DIR/cxxpacked/lib:"
                "$CARGO_TARGET_DIR/vendor_runtime/lib64:"
                "$CARGO_TARGET_DIR/vendor_runtime/lib"
                "${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}\"",
            ]
        )
    else:
        container_lines.extend(
            [
                "if [ -x \"$CARGO_TARGET_DIR/cxxpacked/bin/protoc\" ]; then",
                "  export PROTOC=\"$CARGO_TARGET_DIR/cxxpacked/bin/protoc\"",
                "fi",
            ]
        )
    container_lines.append("cd /workspace/fluxon_rs/fluxon_pyo3")

    container_lines.extend(
        [
            "env "
            f"CARGO_BUILD_JOBS={CARGO_BUILD_JOBS} "
            f"MANYLINUX_VERSION={shlex.quote(manylinux_version)} "
            "maturin build --release "
            f"--compatibility {shlex.quote(f'manylinux_{manylinux_version}')} "
            "--auditwheel skip "
            f"--out {shlex.quote(CONTAINER_RELEASE_PATH)} "
            "--no-default-features "
            f"--features {shlex.quote(_transport_backend_feature_csv(transport_backend, rdma_backend))}",
            "python3 "
            + shlex.quote(str(finalize_script_path))
            + " --release-dir "
            + shlex.quote(CONTAINER_RELEASE_PATH)
            + " --target-root "
            + shlex.quote(CONTAINER_INSTANCE_TARGET_PATH)
            + " --closed-sdk-root "
            + shlex.quote(CONTAINER_CLOSED_SDK_RUNTIME_ROOT_PATH)
            + " --wheel-finalize-steps-json "
            + shlex.quote(json.dumps(list(selected_backend_plan["wheel_finalize_steps"]))),
        ]
    )
    container_cmd = "\n".join(container_lines)
    docker_argv = [
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "/bin/bash",
        "--ulimit",
        f"nofile={DOCKER_NOFILE_LIMIT}:{DOCKER_NOFILE_LIMIT}",
        *docker_env_args,
        "-v",
        f"{workspace_mount_dir}:/workspace",
        "-v",
        f"{target_cache_dir}:{CONTAINER_TARGET_CACHE_MOUNT_PATH}",
        "-v",
        f"{target_cache_dir}:{CONTAINER_INSTANCE_TARGET_PATH}",
        "-v",
        f"{release_dir}:/release",
        "-v",
        f"{profile_dir}:{CONTAINER_PROFILE_PATH}",
        "-v",
        f"{cargo_registry_dir}:/root/.cargo/registry",
        "-v",
        f"{cargo_git_dir}:/root/.cargo/git",
        "-w",
        "/workspace",
        runtime_image_ref,
        "-lc",
        container_cmd,
    ]
    for external_mount in external_mounts:
        host_path = external_mount["host_path"]
        if host_path is None:
            continue
        docker_argv[docker_argv.index(runtime_image_ref) : docker_argv.index(runtime_image_ref)] = [
            "-v",
            f"{host_path}:{external_mount['container_path']}:ro",
        ]
    image_arg_index = docker_argv.index(runtime_image_ref)
    for host_path in extra_host_mount_paths:
        docker_argv[image_arg_index:image_arg_index] = ["-v", f"{host_path}:{host_path}:ro"]
        image_arg_index += 2
    return docker_argv


def _container_native_object_root(*, object_id: str) -> str:
    return f"{CONTAINER_NATIVE_RUNTIME_PATH}/{_native_object_dir_name(object_id=object_id)}"


def _native_object_dir_name(*, object_id: str) -> str:
    return _camel_to_snake(object_id)


def _backend_native_object_id(selected_backend_plan: dict) -> str | None:
    return selected_backend_plan.get("native_object_id")


def _backend_writable_native_dir_name(*, selected_backend_plan: dict) -> str | None:
    native_object_id = _backend_native_object_id(selected_backend_plan)
    if native_object_id is None:
        return None
    return _native_object_dir_name(object_id=native_object_id)


def _backend_profile_object_id(selected_backend_plan: dict) -> str:
    profile_object_id = selected_backend_plan.get("profile_object_id")
    if not isinstance(profile_object_id, str) or not profile_object_id:
        raise RuntimeError("manylinux backend plan must declare profile_object_id")
    return profile_object_id


def _required_profile_native_dir_names(*, selected_backend_plan: dict) -> tuple[str, ...]:
    dir_names: list[str] = []
    seen_dir_names: set[str] = set()
    for object_id in selected_backend_plan["shared_native_input_object_ids"]:
        dir_name = _native_object_dir_name(object_id=object_id)
        if dir_name in seen_dir_names:
            continue
        seen_dir_names.add(dir_name)
        dir_names.append(dir_name)
    return tuple(dir_names)


def _required_native_dir_names(*, selected_backend_plan: dict) -> tuple[str, ...]:
    dir_names = list(_required_profile_native_dir_names(selected_backend_plan=selected_backend_plan))
    writable_native_dir_name = _backend_writable_native_dir_name(selected_backend_plan=selected_backend_plan)
    if writable_native_dir_name is not None and writable_native_dir_name not in dir_names:
        dir_names.append(writable_native_dir_name)
    return tuple(dir_names)


def _object_kind_from_object_id(*, object_id: str) -> str:
    try:
        return NATIVE_RUNTIME_OBJECT_KIND_BY_ID[object_id]
    except KeyError as exc:
        raise RuntimeError(f"unsupported native runtime object id: {object_id}") from exc


def _camel_to_snake(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def _bridge_prebuilt_direct_mount_paths(
    *,
    spec,
    profile_dir: Path,
    published_profile_dir: Path | None,
    selected_backend_plan: dict,
) -> tuple[Path, ...]:
    # Direct bridge mode mounts the host authority trees at their original absolute paths so
    # the assembly profile symlinks remain valid inside the container. Once a published profile
    # is selected, it is self-contained under `profile_dir` and needs no extra host mounts.
    if spec.profile_source.source_kind != "bridge_prebuilt":
        return ()
    if published_profile_dir is not None:
        return ()
    baseline_dir = _require_profile_baseline_dir(profile_dir=profile_dir)
    workspace_seed_dir = _require_profile_workspace_seed_dir(profile_dir=profile_dir)
    native_runtime_dir = _require_profile_native_dir(
        spec=spec,
        profile_dir=profile_dir,
        required_dir_names=_required_profile_native_dir_names(
            selected_backend_plan=selected_backend_plan,
        ),
    )
    target_support_dir = _require_profile_target_support_dir(spec=spec, profile_dir=profile_dir)
    prepare_build_scenario = _backend_prepare_build_scenario(selected_backend_plan=selected_backend_plan)
    prepare_target_dir_names = (
        set(_load_prepare_target_dir_names(spec.project_root, prepare_build_scenario))
        if prepare_build_scenario is not None
        else set()
    )

    mount_paths: list[Path] = [
        baseline_dir,
        workspace_seed_dir,
    ]
    for dir_name in _required_profile_native_dir_names(
        selected_backend_plan=selected_backend_plan,
    ):
        if dir_name in prepare_target_dir_names:
            continue
        mount_paths.append((native_runtime_dir / dir_name).resolve())
    dynamic_target_support_dir_names = set(
        _bridge_prebuilt_dynamic_target_support_dir_names(
            spec=spec,
            selected_backend_plan=selected_backend_plan,
        )
    )
    for dir_name in spec.profile_layout.target_support_dir_names:
        if dir_name in dynamic_target_support_dir_names:
            continue
        mount_paths.append((target_support_dir / dir_name).resolve())

    seen: set[Path] = set()
    deduped_mount_paths: list[Path] = []
    for path in mount_paths:
        if path not in seen:
            seen.add(path)
            deduped_mount_paths.append(path)
    return tuple(deduped_mount_paths)

def _build_target_cache_descriptor(
    *,
    spec,
    runtime_target,
    layout,
    profile_dir: Path,
    transport_backend: str,
    runtime_image_ref: str,
    selected_backend_plan: dict,
    external_mounts: tuple[dict, ...],
) -> dict:
    native_dir = _require_profile_native_dir(
        spec=spec,
        profile_dir=profile_dir,
        required_dir_names=_required_profile_native_dir_names(selected_backend_plan=selected_backend_plan),
    )
    target_support_dir = _require_profile_target_support_dir(spec=spec, profile_dir=profile_dir)
    descriptor = {
        "object_kind": "FluxonManylinuxRustTargetCache",
        "schema_version": TARGET_CACHE_SCHEMA_VERSION,
        "project_scope_id": layout.project_scope_id,
        "execution_substrate": runtime_target.execution_substrate,
        "base_system_key": runtime_target.base_system_key,
        "runtime_abi_key": runtime_target.runtime_abi_key,
        "assembly_name": runtime_target.assembly_name,
        "instance_id": runtime_target.instance_id,
        "transport_backend": transport_backend,
        "rdma_backend": selected_backend_plan["rdma_backend"],
        "runtime_image_ref": runtime_image_ref,
        "profile_source_kind": spec.profile_source.source_kind,
        "profile_dir": str(profile_dir.resolve()),
        "profile_manifest_sha256": _optional_file_sha256(profile_dir / "manifest.json"),
        "native_runtime_refs": {
            dir_name: str((native_dir / dir_name).resolve())
            for dir_name in _required_profile_native_dir_names(selected_backend_plan=selected_backend_plan)
        },
        "target_support_refs": {
            dir_name: str((target_support_dir / dir_name).resolve())
            for dir_name in spec.profile_layout.target_support_dir_names
        },
    }
    native_object_id = _backend_native_object_id(selected_backend_plan)
    if native_object_id is not None:
        native_export_dir = _require_authoritative_fluxon_native_export_dir(
            spec=spec,
            profile_dir=profile_dir,
            native_object_id=native_object_id,
        )
        if native_export_dir is not None:
            descriptor["native_export_sha256"] = {
                file_name: _optional_file_sha256(native_export_dir / file_name)
                for file_name in FLUXON_NATIVE_EXPORT_FILE_NAMES
            }
    if external_mounts:
        descriptor["external_mount_refs"] = {
            mount["name"]: str(mount["resolved_path"])
            for mount in external_mounts
        }
    return descriptor


def _build_target_cache_key_descriptor(
    *,
    spec,
    runtime_target,
    layout,
    transport_backend: str,
) -> dict:
    # Keep one Rust target cache per project + runtime ABI. Cargo already tracks source, feature,
    # rustc, and build-script changes inside the target dir, so transport/backend/profile churn
    # should not force a brand new manylinux target cache directory.
    return {
        "object_kind": "FluxonManylinuxRustTargetCacheKey",
        "schema_version": TARGET_CACHE_SCHEMA_VERSION,
        "project_scope_id": layout.project_scope_id,
        "execution_substrate": runtime_target.execution_substrate,
        "base_system_key": runtime_target.base_system_key,
        "runtime_abi_key": runtime_target.runtime_abi_key,
    }


def _maybe_promote_compatible_target_cache_dir(
    *,
    target_caches_root: Path,
    target_cache_dir: Path,
    target_cache_key_descriptor: dict,
) -> None:
    if target_cache_dir.exists() or not target_caches_root.is_dir():
        return

    compatible_dirs: list[Path] = []
    for candidate_dir in target_caches_root.iterdir():
        if not candidate_dir.is_dir() or candidate_dir == target_cache_dir:
            continue
        manifest_path = candidate_dir / TARGET_CACHE_MANIFEST_FILE_NAME
        if not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if all(manifest.get(key) == value for key, value in target_cache_key_descriptor.items()):
            compatible_dirs.append(candidate_dir)

    if not compatible_dirs:
        return

    compatible_dirs.sort(key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)
    chosen_dir = compatible_dirs[0]
    print(
        "target cache migrate: "
        f"reusing compatible cache {chosen_dir} -> {target_cache_dir}"
    )
    try:
        chosen_dir.rename(target_cache_dir)
    except OSError as exc:
        print(
            "target cache migrate: "
            f"rename skipped for {chosen_dir} -> {target_cache_dir}: {exc}"
        )


def _write_target_cache_manifest(*, target_cache_dir: Path, target_cache_descriptor: dict) -> None:
    manifest_path = target_cache_dir / TARGET_CACHE_MANIFEST_FILE_NAME
    manifest_path.write_text(
        json.dumps(target_cache_descriptor, indent=2) + "\n",
        encoding="utf-8",
    )


def _prepare_manylinux_target_cache_view(
    *,
    spec,
    manylinux_cfg: dict,
    profile_dir: Path,
    target_cache_dir: Path,
    target_cache_descriptor: dict,
    selected_backend_plan: dict,
) -> None:
    native_runtime_dir = _require_profile_native_dir(
        spec=spec,
        profile_dir=profile_dir,
        required_dir_names=_required_profile_native_dir_names(selected_backend_plan=selected_backend_plan),
    )
    target_support_dir = _require_profile_target_support_dir(spec=spec, profile_dir=profile_dir)
    _prepare_target_cache_view(
        spec=spec,
        manylinux_cfg=manylinux_cfg,
        profile_dir=profile_dir,
        target_cache_dir=target_cache_dir,
        target_cache_descriptor=target_cache_descriptor,
        native_runtime_dir=native_runtime_dir,
        target_support_dir=target_support_dir,
        selected_backend_plan=selected_backend_plan,
    )


def _resolve_workspace_mount_dir(*, profile_dir: Path) -> Path:
    source_dir = _require_profile_workspace_seed_dir(profile_dir=profile_dir)
    temp_root = Path(tempfile.mkdtemp(prefix="fluxon_manylinux_workspace_")).resolve()
    workspace_mount_dir = temp_root / "workspace"
    shutil.copytree(source_dir, workspace_mount_dir, symlinks=True)
    TEMP_WORKSPACE_MOUNT_DIRS.append(temp_root)
    return workspace_mount_dir


def _clear_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child_path in path.iterdir():
        if child_path.is_symlink() or child_path.is_file():
            child_path.unlink()
            continue
        _sudo_remove_tree(child_path)


def _prepare_target_cache_view(
    *,
    spec,
    manylinux_cfg: dict,
    profile_dir: Path,
    target_cache_dir: Path,
    target_cache_descriptor: dict,
    native_runtime_dir: Path,
    target_support_dir: Path,
    selected_backend_plan: dict,
) -> None:
    target_cache_dir.mkdir(parents=True, exist_ok=True)
    existing_manifest = _read_existing_target_cache_manifest(target_cache_dir=target_cache_dir)
    writable_native_dir_name = _backend_writable_native_dir_name(selected_backend_plan=selected_backend_plan)
    authoritative_native_export_dir: Path | None = None
    native_object_id = _backend_native_object_id(selected_backend_plan)
    prepare_build_scenario = _backend_prepare_build_scenario(selected_backend_plan=selected_backend_plan)
    prepare_target_dir_names = (
        set(_load_prepare_target_dir_names(spec.project_root, prepare_build_scenario))
        if prepare_build_scenario is not None
        else set()
    )
    dynamic_target_support_dir_names = set(
        _bridge_prebuilt_dynamic_target_support_dir_names(
            spec=spec,
            selected_backend_plan=selected_backend_plan,
        )
    )
    if native_object_id is not None:
        authoritative_native_export_dir = _require_authoritative_fluxon_native_export_dir(
            spec=spec,
            profile_dir=profile_dir,
            native_object_id=native_object_id,
        )

    # Keep Cargo writable outputs in the persistent target cache while linking immutable
    # native runtime roots from the selected mounted profile authority.
    for dir_name in _required_native_dir_names(selected_backend_plan=selected_backend_plan):
        if dir_name == writable_native_dir_name or dir_name in prepare_target_dir_names:
            _prepare_writable_target_cache_native_dir(
                target_cache_dir=target_cache_dir,
                dir_name=dir_name,
                existing_manifest=existing_manifest,
                target_cache_descriptor=target_cache_descriptor,
                authoritative_export_dir=(
                    authoritative_native_export_dir if dir_name == writable_native_dir_name else None
                ),
            )
            continue
        source_dir = native_runtime_dir / dir_name
        if not source_dir.is_dir():
            raise RuntimeError(f"native runtime store is missing required target dir: {source_dir}")
        _replace_workspace_entry(
            link_path=target_cache_dir / dir_name,
            target_path=f"{CONTAINER_NATIVE_RUNTIME_PATH}/{dir_name}",
        )
    for dir_name in spec.profile_layout.target_support_dir_names:
        if dir_name in dynamic_target_support_dir_names:
            _prepare_writable_target_cache_support_dir(
                target_cache_dir=target_cache_dir,
                dir_name=dir_name,
            )
            continue
        source_dir = target_support_dir / dir_name
        if not source_dir.is_dir():
            raise RuntimeError(f"profile target support store is missing required dir: {source_dir}")
        _replace_workspace_entry(
            link_path=target_cache_dir / dir_name,
            target_path=f"{CONTAINER_TARGET_SUPPORT_PATH}/{dir_name}",
        )
    for external_mount in selected_backend_plan["external_mounts"]:
        if external_mount["name"] in prepare_target_dir_names:
            continue
        _replace_workspace_entry(
            link_path=target_cache_dir / external_mount["name"],
            target_path=external_mount["container_path"],
        )
    _write_target_cache_manifest(
        target_cache_dir=target_cache_dir,
        target_cache_descriptor=target_cache_descriptor,
    )


def _read_existing_target_cache_manifest(*, target_cache_dir: Path) -> dict | None:
    manifest_path = target_cache_dir / TARGET_CACHE_MANIFEST_FILE_NAME
    if not manifest_path.is_file():
        return None
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise RuntimeError(f"target cache manifest must decode to a mapping: {manifest_path}")
    return manifest


def _prepare_writable_target_cache_native_dir(
    *,
    target_cache_dir: Path,
    dir_name: str,
    existing_manifest: dict | None,
    target_cache_descriptor: dict,
    authoritative_export_dir: Path | None,
) -> None:
    staged_dir = target_cache_dir / dir_name
    if _writable_target_cache_native_dir_is_reusable(
        staged_dir=staged_dir,
        dir_name=dir_name,
        existing_manifest=existing_manifest,
        target_cache_descriptor=target_cache_descriptor,
    ):
        return
    if staged_dir.is_symlink() or staged_dir.is_file():
        staged_dir.unlink()
    elif staged_dir.exists():
        _sudo_remove_tree(staged_dir)
    _initialize_writable_native_runtime_dir(
        staged_dir=staged_dir,
        dir_name=dir_name,
        authoritative_export_dir=authoritative_export_dir,
    )


def _prepare_writable_target_cache_support_dir(*, target_cache_dir: Path, dir_name: str) -> None:
    staged_dir = target_cache_dir / dir_name
    if staged_dir.is_dir() and not staged_dir.is_symlink():
        return
    if staged_dir.is_symlink() or staged_dir.is_file():
        staged_dir.unlink()
    elif staged_dir.exists():
        _sudo_remove_tree(staged_dir)
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_dir.chmod(0o777)


def _writable_target_cache_native_dir_is_reusable(
    *,
    staged_dir: Path,
    dir_name: str,
    existing_manifest: dict | None,
    target_cache_descriptor: dict,
) -> bool:
    if existing_manifest is None or staged_dir.is_symlink() or not staged_dir.is_dir():
        return False
    previous_native_refs = existing_manifest.get("native_runtime_refs")
    current_native_refs = target_cache_descriptor.get("native_runtime_refs")
    if not isinstance(previous_native_refs, dict) or not isinstance(current_native_refs, dict):
        raise RuntimeError("target cache native_runtime_refs must be mappings")
    previous_native_ref = previous_native_refs.get(dir_name)
    current_native_ref = current_native_refs.get(dir_name)
    if previous_native_ref != current_native_ref:
        return False
    previous_export_sha256 = existing_manifest.get("native_export_sha256")
    current_export_sha256 = target_cache_descriptor.get("native_export_sha256")
    if previous_export_sha256 != current_export_sha256:
        return False
    return True


def _initialize_writable_native_runtime_dir(
    *,
    staged_dir: Path,
    dir_name: str,
    authoritative_export_dir: Path | None,
) -> None:
    supported_dir_names = {
        _native_object_dir_name(object_id="nativeRuntime"),
        VENDOR_RUNTIME_DIR_NAME,
        CXXPACKED_DIR_NAME,
    }
    if dir_name not in supported_dir_names:
        raise RuntimeError(f"unsupported writable native runtime dir: {dir_name}")

    if dir_name == VENDOR_RUNTIME_DIR_NAME:
        _initialize_prepare_target_placeholder_dir(target_root=staged_dir.parent, dir_name=dir_name)
        return
    if dir_name == CXXPACKED_DIR_NAME:
        _initialize_prepare_target_placeholder_dir(target_root=staged_dir.parent, dir_name=dir_name)
        return

    # Keep the manylinux-owned native build prefix empty except for the deterministic
    # directory skeleton expected by build.rs. This prevents host-built ELF artifacts,
    # stamps, and exports from leaking into the container build.
    for subdir in (
        staged_dir / "lib",
        staged_dir / "lib64",
        staged_dir / "lib" / "x86_64-linux-gnu",
    ):
        subdir.mkdir(parents=True, exist_ok=True)

    # The writable native target root must expose FluxonNative export files before dependent
    # Rust build scripts run. In the closed SDK pack path fluxon_commu/build.rs can execute before
    # the native export files are regenerated inside the container.
    if authoritative_export_dir is not None:
        export_dir = staged_dir / FLUXON_NATIVE_EXPORT_RELATIVE_DIR
        export_dir.mkdir(parents=True, exist_ok=True)
        for file_name in FLUXON_NATIVE_EXPORT_FILE_NAMES:
            shutil.copy2(authoritative_export_dir / file_name, export_dir / file_name)
        system_library_rewrites = {
            "libdl.so": "-ldl",
            "libm.so": "-lm",
            "libpthread.so": "-lpthread",
            "librt.so": "-lrt",
            "libz.so": "-lz",
            "libnuma.so": "-lnuma",
        }
        rewrite_fluxon_native_export_bundle(
            export_root=export_dir,
            native_root_rewrites={
                _native_object_dir_name(object_id="nativeRuntime"): (
                    f"{CONTAINER_INSTANCE_TARGET_PATH}/{_native_object_dir_name(object_id='nativeRuntime')}"
                ),
                VENDOR_RUNTIME_DIR_NAME: f"{CONTAINER_NATIVE_RUNTIME_PATH}/{VENDOR_RUNTIME_DIR_NAME}",
                CXXPACKED_DIR_NAME: f"{CONTAINER_NATIVE_RUNTIME_PATH}/{CXXPACKED_DIR_NAME}",
            },
            system_library_rewrites=system_library_rewrites,
        )


def _vendor_runtime_missing_entries(candidate_root: Path) -> list[str]:
    missing_entries: list[str] = []
    driver_dir = candidate_root / "etc" / "libibverbs.d"
    if not driver_dir.is_dir():
        missing_entries.append(str(driver_dir))
    elif not any(path.is_file() and path.name.endswith(".driver") for path in driver_dir.iterdir()):
        missing_entries.append(f"{driver_dir}/*.driver")
    lib_dir = candidate_root / "lib"
    required_lib_prefixes = ("libfabric.so", "libibverbs.so", "libmlx5.so", "libmlx5-rdmav")
    if not lib_dir.is_dir():
        missing_entries.append(str(lib_dir))
        return missing_entries
    lib_names = {path.name for path in lib_dir.iterdir() if path.is_file()}
    for prefix in required_lib_prefixes:
        if not any(name.startswith(prefix) for name in lib_names):
            missing_entries.append(f"{lib_dir}/{prefix}*")
    required_soname_aliases = (
        "libibverbs.so.1",
        "libpsm_infinipath.so.1",
        "libpsm2.so.2",
        "libinfinipath.so.4",
    )
    for alias_name in required_soname_aliases:
        if not (lib_dir / alias_name).exists():
            missing_entries.append(str(lib_dir / alias_name))
    return missing_entries


def _cxxpacked_missing_entries(candidate_root: Path) -> list[str]:
    required_paths = (
        candidate_root / "include" / "infiniband" / "verbs.h",
        candidate_root / "lib64" / "libibverbs.so",
        candidate_root / "lib64" / "libmlx5.so",
        candidate_root / "bin" / "protoc",
    )
    missing_entries = [str(path) for path in required_paths if not path.exists()]
    provider_matches = sorted((candidate_root / "lib64" / "libibverbs").glob("libmlx5-rdmav*.so*"))
    if not provider_matches:
        missing_entries.append(str(candidate_root / "lib64" / "libibverbs" / "libmlx5-rdmav*.so*"))
    return missing_entries


def _resolve_external_mounts(
    *,
    spec,
    profile_dir: Path,
    manylinux_cfg: dict,
    selected_backend_plan: dict,
) -> tuple[dict, ...]:
    resolved_mounts: list[dict] = []
    for mount_spec in selected_backend_plan["external_mounts"]:
        resolved_mounts.append(
            _resolve_external_mount(
                spec=spec,
                profile_dir=profile_dir,
                manylinux_cfg=manylinux_cfg,
                selected_backend_plan=selected_backend_plan,
                rdma_backend=selected_backend_plan["rdma_backend"],
                mount_spec=mount_spec,
            )
        )
    return tuple(resolved_mounts)


def _resolve_external_mount(
    *,
    spec,
    profile_dir: Path,
    manylinux_cfg: dict,
    selected_backend_plan: dict,
    rdma_backend: str,
    mount_spec: dict,
) -> dict:
    profile_native_candidate = (profile_dir / "native" / mount_spec["name"]).resolve()
    if profile_native_candidate.is_dir():
        missing_entries = _validate_external_mount_candidate(
            mount_name=mount_spec["name"],
            candidate_root=profile_native_candidate,
        )
        if not missing_entries:
            return {
                "name": mount_spec["name"],
                "container_path": mount_spec["container_path"],
                "host_path": None,
                "resolved_path": profile_native_candidate,
            }
    candidate_root = spec.project_root / mount_spec["project_relative_path"]
    resolved_candidate = candidate_root.resolve()
    if not resolved_candidate.is_dir():
        raise RuntimeError(
            "external mount authority dir is missing; "
            f"rdma_backend={rdma_backend} mount={mount_spec['name']} path={resolved_candidate}"
        )
    missing_entries = _validate_external_mount_candidate(
        mount_name=mount_spec["name"],
        candidate_root=resolved_candidate,
    )
    prepare_build_scenario = _backend_prepare_build_scenario(selected_backend_plan=selected_backend_plan)
    prepare_target_dir_names = (
        _load_prepare_target_dir_names(spec.project_root, prepare_build_scenario)
        if prepare_build_scenario is not None
        else ()
    )
    if (
        missing_entries
        and spec.profile_source.source_kind == "bridge_prebuilt"
        and mount_spec["name"] in prepare_target_dir_names
        and resolved_candidate.is_dir()
    ):
        missing_entries = []
    if missing_entries:
        raise RuntimeError(
            "external mount authority dir is incomplete; "
            f"rdma_backend={rdma_backend} mount={mount_spec['name']} "
            f"path={resolved_candidate} missing={', '.join(missing_entries)}"
        )
    return {
        "name": mount_spec["name"],
        "container_path": mount_spec["container_path"],
        "host_path": None if profile_native_candidate == resolved_candidate else resolved_candidate,
        "resolved_path": resolved_candidate,
    }


def _validate_external_mount_candidate(*, mount_name: str, candidate_root: Path) -> list[str]:
    if mount_name == VENDOR_RUNTIME_DIR_NAME:
        return _vendor_runtime_missing_entries(candidate_root)
    if mount_name == CXXPACKED_DIR_NAME:
        return _cxxpacked_missing_entries(candidate_root)
    raise RuntimeError(f"unsupported external mount validator: {mount_name}")


def _collect_vendor_runtime_shared_libraries(target_root: Path) -> list[Path]:
    lib_roots = [target_root / "lib64", target_root / "lib"]
    libs: list[Path] = []
    for lib_root in lib_roots:
        if not lib_root.is_dir():
            continue
        for path in sorted(lib_root.rglob("*.so*")):
            if not (path.is_file() or path.is_symlink()):
                continue
            libs.append(path)
    return _unique_existing_paths(libs)


VENDOR_RUNTIME_REQUIRED_LIB_PREFIXES = (
    "libfabric.so",
    "libibverbs.so",
    "libmlx5-rdmav",
)


def _missing_vendor_runtime_lib_prefixes(vendor_lib_paths: list[Path]) -> list[str]:
    vendor_names = [path.name for path in vendor_lib_paths]
    return [
        prefix
        for prefix in VENDOR_RUNTIME_REQUIRED_LIB_PREFIXES
        if not any(name.startswith(prefix) for name in vendor_names)
    ]


def _resolve_vendor_runtime_root_candidates(
    *,
    cargo_target_root: Path,
) -> list[Path]:
    return _unique_existing_paths([cargo_target_root / VENDOR_RUNTIME_DIR_NAME])


def _resolve_vendor_runtime_install_root(
    *,
    cargo_target_root: Path,
) -> tuple[Path, list[Path]]:
    inspected_candidates: list[str] = []
    for candidate_root in _resolve_vendor_runtime_root_candidates(
        cargo_target_root=cargo_target_root,
    ):
        vendor_lib_paths = _collect_vendor_runtime_shared_libraries(candidate_root)
        inspected_candidates.append(f"{candidate_root} (libs={len(vendor_lib_paths)})")
        if not vendor_lib_paths:
            continue
        missing_prefixes = _missing_vendor_runtime_lib_prefixes(vendor_lib_paths)
        if missing_prefixes:
            raise RuntimeError(
                "incomplete vendor runtime install-root "
                + f"{candidate_root}; missing={missing_prefixes}"
            )
        preferred_paths = list(
            _select_preferred_shared_library_paths(vendor_lib_paths).values()
        )
        preferred_paths.sort(key=lambda path: path.name)
        return candidate_root, preferred_paths
    raise RuntimeError(
        "unable to locate a populated vendor runtime install-root; searched="
        + ", ".join(inspected_candidates)
    )


def _shared_library_base_name(lib_name: str) -> str | None:
    if ".so" not in lib_name:
        return None
    return lib_name.split(".so", 1)[0]


def _normalized_packaged_shared_library_key(lib_name: str) -> str | None:
    base_name = _shared_library_base_name(lib_name)
    if base_name is None:
        return None
    return re.sub(r"-[0-9a-f]{8,}$", "", base_name)


def _shared_library_preference_key(path: Path) -> tuple[int, int, int, str]:
    soname = _read_elf_soname(path)
    if soname is not None and path.name == soname:
        soname_rank = 0
    elif soname is not None:
        soname_rank = 1
    else:
        soname_rank = 2
    if path.name.endswith(".so"):
        alias_rank = 2
    elif soname is not None and path.name == soname:
        alias_rank = 0
    else:
        alias_rank = 1
    return (soname_rank, alias_rank, len(path.name), path.name)


def _select_preferred_shared_library_paths(
    paths: list[Path],
) -> dict[str, Path]:
    preferred: dict[str, Path] = {}
    for path in paths:
        base_name = _shared_library_base_name(path.name)
        if base_name is None:
            continue
        current = preferred.get(base_name)
        if current is None:
            preferred[base_name] = path
            continue
        if _shared_library_preference_key(path) < _shared_library_preference_key(current):
            preferred[base_name] = path
    return preferred


def _select_vendor_runtime_packaged_replacements(
    *,
    packaged_file_names: set[str],
    vendor_lib_paths: list[Path],
) -> dict[str, str]:
    preferred_paths = _select_preferred_shared_library_paths(vendor_lib_paths)
    replacements: dict[str, str] = {}
    for packaged_file_name in packaged_file_names:
        packaged_key = _normalized_packaged_shared_library_key(packaged_file_name)
        if packaged_key is None:
            continue
        vendor_path = preferred_paths.get(packaged_key)
        if vendor_path is None:
            continue
        replacements[packaged_file_name] = str(vendor_path)
    return replacements


def _resolve_ibverbs_driver_config_dir(*, runtime_root: Path, runtime_label: str) -> Path:
    driver_dir = runtime_root / "etc" / "libibverbs.d"
    if not driver_dir.is_dir():
        raise RuntimeError(
            f"missing libibverbs driver config dir under {runtime_label}: {driver_dir}"
        )
    driver_files = sorted(driver_dir.glob("*.driver"))
    if not driver_files:
        raise RuntimeError(
            f"empty libibverbs driver config dir under {runtime_label}: {driver_dir}"
        )
    return driver_dir


def _validate_vendor_runtime_wheel_layout(wheel_path: Path) -> None:
    _validate_zip_archive(wheel_path)
    with zipfile.ZipFile(wheel_path, "r") as zip_ref:
        names = set(zip_ref.namelist())
    driver_members = sorted(
        name
        for name in names
        if name.startswith("fluxon_pyo3.libs/libibverbs.d/") and name.endswith(".driver")
    )
    if not driver_members:
        raise RuntimeError(
            f"wheel is missing bundled libibverbs driver configs: {wheel_path}"
        )
    provider_members = sorted(
        name
        for name in names
        if name.startswith("fluxon_pyo3.libs/lib")
        and "-rdmav" in name
        and ".so" in name
    )
    if not provider_members:
        raise RuntimeError(
            f"wheel is missing bundled libibverbs providers: {wheel_path}"
        )


def _validate_zip_archive(path: Path) -> None:
    with zipfile.ZipFile(path, "r") as zip_ref:
        bad_member = zip_ref.testzip()
    if bad_member is not None:
        raise RuntimeError(f"invalid zip member in {path}: {bad_member}")


def _is_path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _is_core_system_runtime_lib(lib_name: str) -> bool:
    if lib_name == "linux-vdso.so.1":
        return True
    if lib_name in MANYLINUX_CXX_RUNTIME_LIBRARY_NAMES:
        return True
    return any(lib_name.startswith(prefix) for prefix in CORE_SYSTEM_RUNTIME_LIB_PREFIXES)


def _read_runtime_dependency_entries(
    path: Path,
    *,
    ld_library_paths: list[Path],
) -> list[tuple[str, Path]]:
    env = os.environ.copy()
    resolved_ld_library_paths = [str(candidate.resolve()) for candidate in _unique_existing_paths(ld_library_paths)]
    inherited_ld_library_path = env.get("LD_LIBRARY_PATH", "")
    if resolved_ld_library_paths:
        if inherited_ld_library_path:
            env["LD_LIBRARY_PATH"] = ":".join([*resolved_ld_library_paths, inherited_ld_library_path])
        else:
            env["LD_LIBRARY_PATH"] = ":".join(resolved_ld_library_paths)
    completed = subprocess.run(
        ["ldd", str(path)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "ldd failed while reading runtime dependencies\n"
            + f"path={path}\n"
            + f"stdout={completed.stdout}\n"
            + f"stderr={completed.stderr}"
        )
    entries: list[tuple[str, Path]] = []
    seen: set[tuple[str, str]] = set()
    for raw_line in completed.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.endswith(":"):
            # Some ldd variants prefix output with "<binary>:"; this is not a dependency entry.
            possible_header = line[:-1].strip()
            if possible_header.startswith("/"):
                continue
        if "=>" in line:
            needed_name, raw_target = (part.strip() for part in line.split("=>", 1))
            if raw_target == "not found":
                raise RuntimeError(f"runtime dependency not found for {path}: {needed_name}")
            dep_path_str = raw_target.split(" ", 1)[0].strip()
        else:
            dep_path_str = line.split(" ", 1)[0].strip()
            if dep_path_str.endswith(":") and dep_path_str[:-1].startswith("/"):
                continue
            if not dep_path_str.startswith("/"):
                continue
            needed_name = Path(dep_path_str).name
        if not dep_path_str.startswith("/"):
            continue
        dep_path = Path(dep_path_str)
        if not dep_path.exists():
            raise RuntimeError(f"runtime dependency path does not exist for {path}: {needed_name} -> {dep_path}")
        resolved_path = dep_path.resolve()
        entry_key = (needed_name, str(resolved_path))
        if entry_key in seen:
            continue
        seen.add(entry_key)
        entries.append((needed_name, resolved_path))
    return entries


def _stage_vendor_runtime_closure(
    *,
    wheel_extension_path: Path,
    wheel_bundled_lib_dirs: list[Path],
    vendor_root: Path,
    vendor_lib_paths: list[Path],
    stage_dir: Path,
    extra_ld_library_roots: list[Path] | None = None,
) -> list[Path]:
    ld_roots = _unique_existing_paths(
        [path.parent for path in vendor_lib_paths]
        + wheel_bundled_lib_dirs
        + ([] if extra_ld_library_roots is None else extra_ld_library_roots)
    )
    bundled_lib_roots = _unique_existing_paths(wheel_bundled_lib_dirs)
    queue: list[Path] = [wheel_extension_path, *vendor_lib_paths]
    seen_targets: set[str] = set()
    staged_by_name: dict[str, Path] = {}

    while queue:
        target_path = queue.pop(0)
        target_key = str(target_path.resolve())
        if target_key in seen_targets:
            continue
        seen_targets.add(target_key)
        for needed_name, resolved_path in _read_runtime_dependency_entries(
            target_path,
            ld_library_paths=[*ld_roots, stage_dir],
        ):
            if _is_path_within(resolved_path, vendor_root):
                continue
            if any(_is_path_within(resolved_path, root) for root in bundled_lib_roots):
                queue.append(resolved_path)
                continue
            if _is_core_system_runtime_lib(needed_name) or _is_core_system_runtime_lib(resolved_path.name):
                continue
            if needed_name in staged_by_name:
                continue
            staged_path = stage_dir / needed_name
            shutil.copy2(resolved_path, staged_path)
            staged_by_name[needed_name] = staged_path
            queue.append(staged_path)
    return [staged_by_name[name] for name in sorted(staged_by_name)]


@contextmanager
def _extract_wheel_runtime_tree(wheel_path: Path):
    with tempfile.TemporaryDirectory(prefix="fluxon_wheel_extract_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        candidates = sorted(temp_dir.glob("fluxon_pyo3/*.so"))
        if len(candidates) != 1:
            raise RuntimeError(
                "expected exactly one fluxon_pyo3 extension in wheel, got "
                + ", ".join(str(path) for path in candidates)
            )
        wheel_bundled_lib_dirs = sorted(
            path for path in temp_dir.iterdir() if path.is_dir() and path.name.endswith(".libs")
        )
        yield candidates[0], wheel_bundled_lib_dirs


def _validate_wheel_bundled_soname_aliases(wheel_path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="fluxon_vendor_runtime_alias_validate_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        with zipfile.ZipFile(wheel_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        pkg_name = wheel_path.name.split("-", 1)[0]
        libs_dir = temp_dir / f"{pkg_name}.libs"
        if not libs_dir.is_dir():
            return
        missing_aliases: list[str] = []
        for lib_path in sorted(path for path in libs_dir.iterdir() if path.is_file() and ".so" in path.name):
            try:
                soname = _read_elf_soname(lib_path)
            except subprocess.CalledProcessError:
                continue
            if soname is None or soname == lib_path.name:
                continue
            if not (libs_dir / soname).is_file():
                missing_aliases.append(soname)
        if missing_aliases:
            raise RuntimeError(
                f"wheel is missing bundled SONAME aliases: {wheel_path}: {sorted(set(missing_aliases))}"
            )


def _read_elf_needed_names(path: Path) -> list[str]:
    completed = subprocess.run(
        ["readelf", "-d", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    needed_names: list[str] = []
    for line in completed.stdout.splitlines():
        marker = "Shared library: ["
        if marker not in line:
            continue
        start = line.index(marker) + len(marker)
        end = line.find("]", start)
        if end == -1:
            continue
        needed_name = line[start:end]
        if needed_name and needed_name not in needed_names:
            needed_names.append(needed_name)
    return needed_names


def _read_elf_soname(path: Path) -> str | None:
    completed = subprocess.run(
        ["readelf", "-d", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    marker = "Library soname: ["
    for line in completed.stdout.splitlines():
        if marker not in line:
            continue
        start = line.index(marker) + len(marker)
        end = line.find("]", start)
        if end == -1:
            continue
        soname = line[start:end]
        if soname:
            return soname
    return None


def _ensure_vendor_runtime_soname_aliases(
    *,
    staged_paths: list[Path],
    dest_dir: Path,
) -> list[Path]:
    aliased_paths = list(staged_paths)
    existing_names = {path.name for path in aliased_paths}
    for staged_path in list(staged_paths):
        try:
            soname = _read_elf_soname(staged_path)
        except subprocess.CalledProcessError:
            continue
        if soname is None or soname == staged_path.name or soname in existing_names:
            continue
        alias_path = dest_dir / soname
        shutil.copy2(staged_path, alias_path)
        aliased_paths.append(alias_path)
        existing_names.add(soname)
    aliased_paths.sort(key=lambda path: path.name)
    return aliased_paths


def _require_profile_workspace_seed_dir(*, profile_dir: Path) -> Path:
    if not profile_dir.is_dir():
        raise RuntimeError(f"profile authority dir must be a directory: {profile_dir}")
    workspace_seed_dir = profile_dir / "workspace_seed"
    if not workspace_seed_dir.is_dir():
        raise RuntimeError(f"profile workspace_seed authority is missing: {workspace_seed_dir}")
    return workspace_seed_dir.resolve()


def _require_profile_native_dir(
    *,
    spec,
    profile_dir: Path,
    required_dir_names: tuple[str, ...] | None = None,
) -> Path:
    if not profile_dir.is_dir():
        raise RuntimeError(f"profile authority dir must be a directory: {profile_dir}")
    native_dir = profile_dir / "native"
    if not native_dir.is_dir():
        raise RuntimeError(f"profile native authority dir is missing: {native_dir}")
    if required_dir_names is None:
        required_dir_names = spec.profile_layout.native_runtime_dir_names
    for dir_name in required_dir_names:
        required_dir = native_dir / dir_name
        if not required_dir.is_dir():
            raise RuntimeError(f"profile native authority is missing required dir: {required_dir}")
    return native_dir.resolve()


def _missing_fluxon_native_export_paths(*, export_dir: Path) -> list[Path]:
    return [
        export_dir / file_name
        for file_name in FLUXON_NATIVE_EXPORT_FILE_NAMES
        if not (export_dir / file_name).is_file()
    ]


def _is_closed_sdk_profile_dir(*, profile_dir: Path) -> bool:
    manifest_path = profile_dir / "manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(manifest, dict):
        return False
    if manifest.get("object_kind") != "FluxonCommuClosedSdk":
        return False
    feature_contract = manifest.get("feature_contract")
    if not isinstance(feature_contract, dict):
        return False
    return feature_contract.get("boundary_mode") == CLOSED_SDK_CONSUMER_BOUNDARY_MODE


def _require_authoritative_fluxon_native_export_dir(
    *,
    spec,
    profile_dir: Path,
    native_object_id: str,
) -> Path | None:
    profile_export_dir = (
        profile_dir
        / "native"
        / _native_object_dir_name(object_id=native_object_id)
        / FLUXON_NATIVE_EXPORT_RELATIVE_DIR
    )
    missing_profile_paths = _missing_fluxon_native_export_paths(export_dir=profile_export_dir)
    if not missing_profile_paths:
        return profile_export_dir.resolve()

    if _is_closed_sdk_profile_dir(profile_dir=profile_dir):
        return None

    # Bridge-prebuilt closed SDK keeps native_runtime writable under the Cargo target cache
    # instead of publishing it as an immutable profile-native root. The closed SDK bundle under
    # fluxon_release/closed_sdk is the authority that seeds that writable pack-time root.
    if native_object_id == "nativeRuntime":
        closed_sdk_export_dir = (
            spec.project_root
            / PUBLIC_CLOSED_SDK_REPO_RELATIVE_ROOT
            / "native"
            / "native_runtime"
            / FLUXON_NATIVE_EXPORT_RELATIVE_DIR
        )
        missing_closed_sdk_paths = _missing_fluxon_native_export_paths(export_dir=closed_sdk_export_dir)
        if not missing_closed_sdk_paths:
            return closed_sdk_export_dir.resolve()
        missing_text = ", ".join(str(path) for path in missing_closed_sdk_paths)
        raise RuntimeError(
            "closed SDK manylinux pack requires a complete FluxonNative export under "
            f"{closed_sdk_export_dir}; missing: {missing_text}"
        )

    missing_text = ", ".join(str(path) for path in missing_profile_paths)
    raise RuntimeError(
        "profile native authority is missing the required FluxonNative export: "
        f"{missing_text}"
    )


def _require_profile_target_support_dir(*, spec, profile_dir: Path) -> Path:
    if not profile_dir.is_dir():
        raise RuntimeError(f"profile authority dir must be a directory: {profile_dir}")
    target_support_dir = profile_dir / "target_support"
    if not target_support_dir.is_dir():
        raise RuntimeError(f"profile target_support authority dir is missing: {target_support_dir}")
    for dir_name in spec.profile_layout.target_support_dir_names:
        required_dir = target_support_dir / dir_name
        if not required_dir.is_dir():
            raise RuntimeError(f"profile target_support authority is missing required dir: {required_dir}")
    return target_support_dir.resolve()


def _require_effective_profile_dir(*, layout) -> Path:
    if not layout.profile_link.is_dir():
        raise RuntimeError(
            "layout profile authority is missing; run with --apply-layout first or provide an existing profile layout: "
            f"{layout.profile_link}"
        )
    return layout.profile_link.resolve()


def _resolve_runtime_profile_dir(
    *,
    spec,
    runtime_target,
    layout,
    transport_backend: str,
    selected_backend_plan: dict,
    manylinux_version: str,
    runtime_image_ref: str,
    force_republish: bool,
    config_path: Path,
    config_root: dict,
) -> tuple[Path, Path | None]:
    profile_dir = _require_effective_profile_dir(layout=layout)
    if spec.profile_source.source_kind != "bridge_prebuilt":
        if force_republish:
            raise RuntimeError(
                "--publish-profile requires profile.source_kind=bridge_prebuilt"
            )
        return profile_dir, None
    if not force_republish:
        # Default to direct bridge execution: mount the host authority trees into the manylinux
        # container so the assembly profile's absolute symlink targets remain valid without
        # republishing a self-contained profile copy on every latest-code run.
        return profile_dir, None
    published_profile_dir = _publish_bridge_profile(
        spec=spec,
        runtime_target=runtime_target,
        layout=layout,
        source_profile_dir=profile_dir,
        transport_backend=transport_backend,
        selected_backend_plan=selected_backend_plan,
        manylinux_version=manylinux_version,
        runtime_image_ref=runtime_image_ref,
        force_republish=force_republish,
        config_path=config_path,
        config_root=config_root,
    )
    return published_profile_dir, published_profile_dir


def _publish_bridge_profile(
    *,
    spec,
    runtime_target,
    layout,
    source_profile_dir: Path,
    transport_backend: str,
    selected_backend_plan: dict,
    manylinux_version: str,
    runtime_image_ref: str,
    force_republish: bool,
    config_path: Path,
    config_root: dict,
) -> Path:
    # English note:
    # - bridge_prebuilt accepts exactly one external path authority: profile.build_root_path.
    # - published profiles remain a derived cache under store.project_data_root, so callers do not
    #   provide a second output path knob for this branch.
    publish_root = _require_absolute_existing_or_creatable_dir(
        raw_value=str((spec.project_data_root / "profile-store").resolve()),
        field_name="store.project_data_root/profile-store",
    )
    published_profile_dir = (
        publish_root
        / "projects"
        / layout.project_scope_id
        / "substrates"
        / runtime_target.execution_substrate
        / "profiles"
        / runtime_target.base_system_key
        / runtime_target.runtime_abi_key
        / runtime_target.assembly_name
        / runtime_target.profile_name
    )
    workspace_seed_dir = _require_profile_workspace_seed_dir(profile_dir=source_profile_dir)
    native_runtime_dir = _require_profile_native_dir(
        spec=spec,
        profile_dir=source_profile_dir,
        required_dir_names=_required_profile_native_dir_names(selected_backend_plan=selected_backend_plan),
    )
    target_support_dir = _require_profile_target_support_dir(spec=spec, profile_dir=source_profile_dir)
    baseline_dir = _require_profile_baseline_dir(profile_dir=source_profile_dir)
    native_build_authority = _build_native_build_authority(
        spec=spec,
        runtime_target=runtime_target,
        source_profile_dir=source_profile_dir,
        native_runtime_dir=native_runtime_dir,
        workspace_seed_dir=workspace_seed_dir,
        selected_backend_plan=selected_backend_plan,
    )
    # The first cold-start run is allowed to publish a bridge profile before FluxonNative
    # export files exist. Those exports are materialized by the later native build itself,
    # so requiring them up front would make the initial publish/build cycle impossible.

    published_manifest = _build_published_profile_manifest(
        spec=spec,
        runtime_target=runtime_target,
        layout=layout,
        source_profile_dir=source_profile_dir,
        published_profile_dir=published_profile_dir,
        transport_backend=transport_backend,
        config_path=config_path,
        config_root=config_root,
        workspace_seed_dir=workspace_seed_dir,
        native_runtime_dir=native_runtime_dir,
        target_support_dir=target_support_dir,
        baseline_dir=baseline_dir,
        selected_backend_plan=selected_backend_plan,
        native_build_authority=native_build_authority,
    )
    if not force_republish and _published_profile_is_reusable(
        spec=spec,
        published_profile_dir=published_profile_dir,
        expected_manifest=published_manifest,
    ):
        return published_profile_dir.resolve()

    _recreate_directory_tree(published_profile_dir)
    (published_profile_dir / "native").mkdir(parents=True, exist_ok=True)
    (published_profile_dir / "target_support").mkdir(parents=True, exist_ok=True)
    (published_profile_dir / "manifest.json").write_text(
        json.dumps(published_manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    _copy_workspace_seed_subset(
        source_workspace_seed_dir=workspace_seed_dir,
        target_workspace_seed_dir=published_profile_dir / "workspace_seed",
        transport_backend=transport_backend,
        rdma_backend=selected_backend_plan["rdma_backend"],
    )
    _sudo_copy_path(source_path=baseline_dir, target_path=published_profile_dir / "baseline")
    native_source_path_by_dir_name = _resolve_native_source_path_by_dir_name(
        spec=spec,
        source_profile_dir=source_profile_dir,
        native_runtime_dir=native_runtime_dir,
        selected_backend_plan=selected_backend_plan,
    )
    for dir_name in _required_profile_native_dir_names(
        selected_backend_plan=selected_backend_plan,
    ):
        _sudo_copy_path(
            source_path=native_source_path_by_dir_name[dir_name],
            target_path=published_profile_dir / "native" / dir_name,
        )
    writable_native_dir_name = _backend_writable_native_dir_name(selected_backend_plan=selected_backend_plan)
    if writable_native_dir_name is not None:
        _initialize_writable_native_runtime_dir(
            staged_dir=published_profile_dir / "native" / writable_native_dir_name,
            dir_name=writable_native_dir_name,
        )
    for dir_name in spec.profile_layout.target_support_dir_names:
        _sudo_copy_path(
            source_path=(target_support_dir / dir_name).resolve(),
            target_path=published_profile_dir / "target_support" / dir_name,
        )
    authority_context = _build_authority_context(
        spec=spec,
        runtime_target=runtime_target,
        published_profile_dir=published_profile_dir,
        transport_backend=transport_backend,
        manylinux_version=manylinux_version,
        runtime_image_ref=runtime_image_ref,
        selected_backend_plan=selected_backend_plan,
        native_build_authority=native_build_authority,
    )
    authority_graph = _load_authority_graph(
        config_path=config_path,
        config_root=config_root,
        context=authority_context,
    )
    _write_authority_exports(
        published_profile_dir=published_profile_dir,
        authority_graph=authority_graph,
    )
    return published_profile_dir.resolve()


def _build_published_profile_manifest(
    *,
    spec,
    runtime_target,
    layout,
    source_profile_dir: Path,
    published_profile_dir: Path,
    transport_backend: str,
    config_path: Path,
    config_root: dict,
    workspace_seed_dir: Path,
    native_runtime_dir: Path,
    target_support_dir: Path,
    baseline_dir: Path,
    selected_backend_plan: dict,
    native_build_authority: dict | None,
) -> dict:
    del transport_backend
    workspace_seed_digest = script_utils.compute_paths_digest(
        [workspace_seed_dir],
        relative_to=workspace_seed_dir,
        mode=script_utils.PathDigestMode.CONTENTS_ONLY,
        algorithm=script_utils.PathHashAlgorithm.MD5,
        ignored_dir_names=(),
        ignored_file_names=(),
        ignored_file_suffixes=(),
    )
    manifest = {
        "object_kind": "FluxonManylinuxPublishedProfile",
        "schema_version": 1,
        "source_kind": "bridge_prebuilt",
        "project_scope_id": layout.project_scope_id,
        "execution_substrate": runtime_target.execution_substrate,
        "base_system_key": runtime_target.base_system_key,
        "runtime_abi_key": runtime_target.runtime_abi_key,
        "assembly_name": runtime_target.assembly_name,
        "profile_name": runtime_target.profile_name,
        "transport_backend": transport_backend,
        "rdma_backend": selected_backend_plan["rdma_backend"],
        "config_path": str(config_path.resolve()),
        "config_sha256": hashlib.sha256(
            config_path.read_bytes()
        ).hexdigest(),
        "authority_graph_sha256": _sha256_json_bytes(
            raw=_extract_authority_graph_root(config_root=config_root)
        ),
        "source_profile_dir": str(source_profile_dir.resolve()),
        "source_profile_manifest_sha256": _optional_file_sha256(source_profile_dir / "manifest.json"),
        "workspace_seed": str(workspace_seed_dir),
        "workspace_seed_digest": workspace_seed_digest,
        "native": {
            dir_name: str(path)
            for dir_name, path in _resolve_native_source_path_by_dir_name(
                spec=spec,
                source_profile_dir=source_profile_dir,
                native_runtime_dir=native_runtime_dir,
                selected_backend_plan=selected_backend_plan,
            ).items()
        },
        "target_support": {
            dir_name: str((target_support_dir / dir_name).resolve())
            for dir_name in spec.profile_layout.target_support_dir_names
        },
        "baseline": str(baseline_dir),
        "generated_nix_profile_expr": str(
            (published_profile_dir / PUBLISHED_PROFILE_NIX_FILE_NAME).resolve()
        ),
        "generated_authority_mermaid": str(
            (published_profile_dir / AUTHORITY_MERMAID_FILE_NAME).resolve()
        ),
    }
    native_object_id = _backend_native_object_id(selected_backend_plan)
    if native_object_id is not None:
        assert native_build_authority is not None
        native_export_dir = (
            native_runtime_dir
            / _native_object_dir_name(object_id=native_object_id)
            / FLUXON_NATIVE_EXPORT_RELATIVE_DIR
        )
        manifest["native_export_sha256"] = {
            "file_name": None,
        }
        manifest["native_export_sha256"] = {
            file_name: _optional_file_sha256(native_export_dir / file_name)
            for file_name in FLUXON_NATIVE_EXPORT_FILE_NAMES
        }
        manifest["native_build_authority_sha256"] = _sha256_json_bytes(raw=native_build_authority)
    return manifest


def _published_profile_is_reusable(*, spec, published_profile_dir: Path, expected_manifest: dict) -> bool:
    manifest_path = published_profile_dir / "manifest.json"
    profile_expr_path = published_profile_dir / PUBLISHED_PROFILE_NIX_FILE_NAME
    authority_mermaid_path = published_profile_dir / AUTHORITY_MERMAID_FILE_NAME
    workspace_seed_dir = published_profile_dir / "workspace_seed"
    baseline_dir = published_profile_dir / "baseline"
    if not manifest_path.is_file():
        return False
    if not profile_expr_path.is_file():
        return False
    if not authority_mermaid_path.is_file():
        return False
    if not workspace_seed_dir.is_dir():
        return False
    if not baseline_dir.exists():
        return False
    actual_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if actual_manifest != expected_manifest:
        return False
    for dir_name in spec.profile_layout.target_support_dir_names:
        if not (published_profile_dir / "target_support" / dir_name).exists():
            return False
    for dir_name in expected_manifest["native"]:
        if not (published_profile_dir / "native" / dir_name).exists():
            return False
    return True


def _resolve_native_source_path_by_dir_name(
    *,
    spec,
    source_profile_dir: Path,
    native_runtime_dir: Path,
    selected_backend_plan: dict,
) -> dict[str, Path]:
    resolved: dict[str, Path] = {}
    external_mounts_by_name = {
        mount["name"]: mount
        for mount in selected_backend_plan["external_mounts"]
    }
    for dir_name in _required_profile_native_dir_names(selected_backend_plan=selected_backend_plan):
        external_mount = external_mounts_by_name.get(dir_name)
        if external_mount is not None:
            source_path = (spec.project_root / external_mount["project_relative_path"]).resolve()
            if not source_path.is_dir():
                raise RuntimeError(f"external native source dir is missing: {source_path}")
            resolved[dir_name] = source_path
            continue
        source_path = (native_runtime_dir / dir_name).resolve()
        if not source_path.is_dir():
            raise RuntimeError(f"profile native source dir is missing: {source_path}")
        resolved[dir_name] = source_path
    return resolved


def _require_profile_baseline_dir(*, profile_dir: Path) -> Path:
    if not profile_dir.is_dir():
        raise RuntimeError(f"profile authority dir must be a directory: {profile_dir}")
    baseline_dir = profile_dir / "baseline"
    if not baseline_dir.is_dir():
        raise RuntimeError(f"profile baseline authority dir is missing: {baseline_dir}")
    return baseline_dir.resolve()


def _require_absolute_existing_or_creatable_dir(*, raw_value: str, field_name: str) -> Path:
    path = Path(raw_value)
    if not path.is_absolute():
        raise RuntimeError(f"{field_name} must be an absolute path: {path}")
    resolved_path = path.resolve()
    if resolved_path.exists() and not resolved_path.is_dir():
        raise RuntimeError(f"{field_name} must be a directory when it exists: {resolved_path}")
    resolved_path.mkdir(parents=True, exist_ok=True)
    resolved_path.chmod(0o777)
    return resolved_path


def _recreate_directory_tree(path: Path) -> None:
    if path.exists():
        _clear_directory(path)
        return
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o777)


def _copy_workspace_seed_subset(
    *,
    source_workspace_seed_dir: Path,
    target_workspace_seed_dir: Path,
    transport_backend: str,
    rdma_backend: str,
) -> None:
    del transport_backend
    del rdma_backend
    target_workspace_seed_dir.mkdir(parents=True, exist_ok=True)
    target_workspace_seed_dir.chmod(0o777)
    for source_path in sorted(source_workspace_seed_dir.rglob("*")):
        if source_path == source_workspace_seed_dir:
            continue
        relative_path = source_path.relative_to(source_workspace_seed_dir)
        target_path = target_workspace_seed_dir / relative_path
        if source_path.is_dir() and not source_path.is_symlink():
            target_path.mkdir(parents=True, exist_ok=True)
            target_path.chmod(0o777)
            continue
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.parent.chmod(0o777)
        _sudo_copy_path(source_path=source_path, target_path=target_path)


def _require_publishable_fluxon_native_runtime_export(*, native_runtime_dir: Path, native_object_id: str) -> None:
    export_dir = native_runtime_dir / _native_object_dir_name(object_id=native_object_id) / FLUXON_NATIVE_EXPORT_RELATIVE_DIR
    missing_paths = [
        export_dir / file_name
        for file_name in FLUXON_NATIVE_EXPORT_FILE_NAMES
        if not (export_dir / file_name).is_file()
    ]
    if not missing_paths:
        return
    missing_text = ", ".join(str(path) for path in missing_paths)
    raise RuntimeError(
        "published profile requires a complete native runtime FluxonNative export before "
        f"copying authority objects, missing: {missing_text}. "
        "Run the setup_and_pack/nix manylinux build once against the assembly profile to materialize "
        "the FluxonNative export files, then publish again."
    )


def _copy_path(*, source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.parent.chmod(0o777)
    subprocess.run(
        ["cp", "-a", str(source_path), str(target_path)],
        check=True,
    )
    if target_path.is_dir():
        target_path.chmod(0o777)


def _sudo_copy_path(*, source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.parent.chmod(0o777)
    host_sudo = host_sudo_prefix()
    subprocess.run(
        host_sudo + ["cp", "-a", str(source_path), str(target_path)],
        check=True,
    )


def _sudo_remove_tree(path: Path) -> None:
    # The manylinux container writes root-owned build outputs into the bind-mounted
    # instance workspace. Host-side experiment cleanup must remove that authority tree
    # before the next run can recreate the isolated target view.
    host_sudo = host_sudo_prefix()
    subprocess.run(
        host_sudo + ["chmod", "-R", "777", str(path)],
        check=True,
    )
    subprocess.run(
        host_sudo + ["rm", "-rf", str(path)],
        check=True,
    )


def _replace_workspace_entry(*, link_path: Path, target_path: str) -> None:
    normalized_target_path = Path(os.path.abspath(target_path))
    normalized_link_path = Path(os.path.abspath(link_path))
    if normalized_target_path == normalized_link_path:
        if link_path.is_symlink():
            link_path.unlink()
        return
    link_path.parent.mkdir(parents=True, exist_ok=True)
    if link_path.is_symlink():
        link_path.unlink()
    elif link_path.exists():
        if link_path.is_dir():
            _sudo_remove_tree(link_path)
        else:
            link_path.unlink()
    os.symlink(target_path, link_path)


def _run_with_tee_log(*, argv: list[str], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"command={_shell_join(argv)}\n")
        log_file.write("---\n")
        log_file.flush()

        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
            log_file.write(line)
        return_code = process.wait()

    if return_code != 0:
        raise RuntimeError(f"docker run failed with exit code {return_code}, log={log_path}")
def _require_workspace_seed_fluxon_commu_source_dir(*, workspace_seed_dir: Path, field_name: str) -> Path:
    source_dir = workspace_seed_dir / FLUXON_COMMU_AUTHORITY_RELATIVE_PATH
    cargo_toml_path = source_dir / "Cargo.toml"
    if not cargo_toml_path.is_file():
        raise RuntimeError(
            f"{field_name} must contain a complete fluxon_commu authority at {cargo_toml_path}"
        )
    return source_dir.resolve()


def _build_native_build_authority(
    *,
    spec,
    runtime_target,
    source_profile_dir: Path,
    native_runtime_dir: Path,
    workspace_seed_dir: Path,
    selected_backend_plan: dict,
) -> dict | None:
    native_object_id = _backend_native_object_id(selected_backend_plan)
    if native_object_id is None:
        return None
    native_object_kind = _object_kind_from_object_id(object_id=native_object_id)
    prepare_build_scenario = NATIVE_RUNTIME_PREPARE_SCENARIOS.get(native_object_kind)
    if prepare_build_scenario is None:
        raise RuntimeError(f"unsupported native runtime object kind for build authority: {native_object_kind}")
    native_export_dir = (
        native_runtime_dir
        / _native_object_dir_name(object_id=native_object_id)
        / FLUXON_NATIVE_EXPORT_RELATIVE_DIR
    )
    fluxon_commu_source_dir = _require_workspace_seed_fluxon_commu_source_dir(
        workspace_seed_dir=workspace_seed_dir,
        field_name="profile.workspace_seed",
    )
    return {
        "object_kind": "FluxonNativeBuildAuthority",
        "schema_version": 1,
        "authority_kind": "prebuilt_tree_migration",
        "base_system_key": runtime_target.base_system_key,
        "runtime_abi_key": runtime_target.runtime_abi_key,
        "rdma_backend": selected_backend_plan["rdma_backend"],
        "native_object_id": native_object_id,
        "prepare_build_scenario": prepare_build_scenario,
        "source_profile_dir": str(source_profile_dir.resolve()),
        "fluxon_commu_runtime_source_dir": str(fluxon_commu_source_dir),
        "prepare_cache_steps": [
            {
                "name": step.name,
                "inputs": list(step.inputs),
                "outputs": list(step.outputs),
            }
            for step in _load_prepare_cache_steps(spec.project_root, prepare_build_scenario)
        ],
        "native_export_sha256": {
            file_name: _optional_file_sha256(native_export_dir / file_name)
            for file_name in FLUXON_NATIVE_EXPORT_FILE_NAMES
        },
        "authoritative_inputs": [
            "workspaceSeed",
            "fluxonCommuAuthority",
            *list(selected_backend_plan["shared_native_input_object_ids"]),
            native_object_id,
            "targetSupport",
            "baseSystemKey",
            "runtimeAbiKey",
            "prepareBuildScenario",
        ],
    }


def _read_cargo_package_version(cargo_toml_path: Path) -> str:
    in_package_section = False
    for raw_line in cargo_toml_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_package_section = stripped == "[package]"
            continue
        if not in_package_section:
            continue
        if not stripped.startswith("version"):
            continue
        _, raw_value = stripped.split("=", 1)
        version = raw_value.strip().strip('"').strip("'")
        if not version:
            raise RuntimeError(f"Cargo package version must be non-empty: {cargo_toml_path}")
        return version
    raise RuntimeError(f"Cargo package version is missing from [package]: {cargo_toml_path}")


def _build_authority_context(
    *,
    spec,
    runtime_target,
    published_profile_dir: Path,
    transport_backend: str,
    manylinux_version: str,
    runtime_image_ref: str,
    selected_backend_plan: dict,
    native_build_authority: dict | None,
) -> dict:
    published_workspace_seed_dir = (published_profile_dir / "workspace_seed").resolve()
    fluxon_commu_source_dir = _require_workspace_seed_fluxon_commu_source_dir(
        workspace_seed_dir=published_workspace_seed_dir,
        field_name="published_profile.workspace_seed",
    )
    fluxon_commu_cargo_toml_path = fluxon_commu_source_dir / "Cargo.toml"
    fluxon_commu_crate_version = _read_cargo_package_version(fluxon_commu_cargo_toml_path)
    return {
        "spec": spec,
        "runtime_target": runtime_target,
        "published_profile_dir": published_profile_dir.resolve(),
        "transport_backend": transport_backend,
        "manylinux_version": manylinux_version,
        "runtime_image_ref": runtime_image_ref,
        "system_name": _require_nix_system_name(base_system_key=runtime_target.base_system_key),
        "profile_constructor_name": _require_profile_constructor_name(
            base_system_key=runtime_target.base_system_key,
            runtime_abi_key=runtime_target.runtime_abi_key,
        ),
        "fluxon_commu_runtime_source_dir": fluxon_commu_source_dir,
        "fluxon_commu_crate_version": fluxon_commu_crate_version,
        "selected_backend_plan": selected_backend_plan,
        "native_build_authority": native_build_authority,
        "authority_bindings": {
            "manylinux": {
                "toolchain": {
                    "base_system_key": runtime_target.base_system_key,
                    "runtime_abi_key": runtime_target.runtime_abi_key,
                    "manylinux_version": manylinux_version,
                    "runtime_image_ref": runtime_image_ref,
                }
            },
            "runtime_sources": {
                "fluxon_commu": {
                    "source_kind": SOURCE_KIND_LOCAL_PATH,
                    "source_path": str(fluxon_commu_source_dir),
                    "crate_version": fluxon_commu_crate_version,
                }
            },
            "published_profile": {
                "root": {
                    "profile_name": runtime_target.profile_name,
                    "assembly_name": runtime_target.assembly_name,
                    "base_system_key": runtime_target.base_system_key,
                    "runtime_abi_key": runtime_target.runtime_abi_key,
                    "native_object_ids": list(selected_backend_plan["shared_native_input_object_ids"]),
                    "profile_object_id": _backend_profile_object_id(selected_backend_plan),
                },
                "workspace_seed": {
                    "workspace_seed_path": str((published_profile_dir / "workspace_seed").resolve()),
                    "project_root": str(spec.project_root.resolve()),
                    "transport_backend": transport_backend,
                    "runtime_abi_key": runtime_target.runtime_abi_key,
                },
                "target_support": {
                    "source_path": str((published_profile_dir / "target_support").resolve()),
                    "runtime_abi_key": runtime_target.runtime_abi_key,
                    "target_dir_names": list(spec.profile_layout.target_support_dir_names),
                },
                "native": _build_published_profile_native_bindings(
                    published_profile_dir=published_profile_dir,
                    runtime_target=runtime_target,
                    selected_backend_plan=selected_backend_plan,
                    native_build_authority=native_build_authority,
                ),
                "wheel_argument": {
                    "runtime_abi_key": runtime_target.runtime_abi_key,
                    "transport_backend": transport_backend,
                },
            },
        },
    }


def _build_published_profile_native_bindings(
    *,
    published_profile_dir: Path,
    runtime_target,
    selected_backend_plan: dict,
    native_build_authority: dict | None,
) -> dict:
    bindings: dict[str, dict] = {}
    native_object_id = _backend_native_object_id(selected_backend_plan)
    for object_id in selected_backend_plan["shared_native_input_object_ids"]:
        native_binding_key = _native_object_dir_name(object_id=object_id)
        source_path = str((published_profile_dir / "native" / native_binding_key).resolve())
        bindings[native_binding_key] = {
            "source_path": source_path,
            "runtime_abi_key": runtime_target.runtime_abi_key,
        }
    if native_object_id is not None:
        assert native_build_authority is not None
        native_binding_key = _native_object_dir_name(object_id=native_object_id)
        bindings[native_binding_key] = {
            "source_path": str((published_profile_dir / "native" / native_binding_key).resolve()),
            "base_system_key": runtime_target.base_system_key,
            "runtime_abi_key": runtime_target.runtime_abi_key,
            "build_authority": native_build_authority,
        }
    return bindings


def _load_authority_graph(*, config_path: Path, config_root: dict, context: dict) -> dict:
    raw = _extract_authority_graph_root(config_root=config_root)
    selected_backend_plan = context["selected_backend_plan"]
    schema_version = raw.get("schema_version")
    if schema_version != AUTHORITY_SCHEMA_VERSION:
        raise RuntimeError(
            "authority schema_version must be "
            f"{AUTHORITY_SCHEMA_VERSION}: {config_path}"
        )
    assembly_cfg = _require_mapping(raw, "assembly")
    default_root_object_id = _require_non_empty_string(assembly_cfg, "root_object")
    export_roots_cfg = _require_mapping(assembly_cfg, "export_roots")
    objects_cfg = _require_mapping(assembly_cfg, "objects")
    root_object_id = _backend_profile_object_id(selected_backend_plan)
    if root_object_id not in objects_cfg:
        raise RuntimeError(f"authority root_object is missing from objects: {root_object_id}")

    objects: dict[str, dict] = {}
    for object_id, object_raw in objects_cfg.items():
        if not isinstance(object_id, str) or not object_id.strip():
            raise RuntimeError(f"authority object id must be a non-empty string: {object_id!r}")
        if not isinstance(object_raw, dict):
            raise RuntimeError(f"authority object must be a mapping: {object_id}")
        kind = _require_non_empty_string(object_raw, "kind")
        if kind not in SUPPORTED_AUTHORITY_OBJECT_KINDS:
            raise RuntimeError(f"unsupported authority object kind: {object_id} kind={kind}")
        deps = _require_string_list_field(object_raw, "deps")
        authority_ref = _require_non_empty_string(object_raw, "authority_ref")
        params = object_raw.get("params")
        if not isinstance(params, dict):
            raise RuntimeError(f"authority object params must be a mapping: {object_id}")
        objects[object_id] = {
            "id": object_id,
            "kind": kind,
            "deps": deps,
            "authority_ref": authority_ref,
            "params": params,
        }

    for object_id, node in objects.items():
        for dep_id in node["deps"]:
            if dep_id not in objects:
                raise RuntimeError(f"authority object dependency is missing: {object_id} -> {dep_id}")

    export_roots = {}
    for export_name in (AUTHORITY_EXPORT_PUBLISHED_PROFILE_NIX,):
        object_id = _optional_non_empty_string(export_roots_cfg, export_name)
        if object_id is None:
            continue
        export_roots[export_name] = object_id
    export_roots[AUTHORITY_EXPORT_PUBLISHED_PROFILE_NIX] = root_object_id
    for export_name, object_id in export_roots.items():
        if object_id not in objects:
            raise RuntimeError(
                f"authority export root object is missing: {export_name} -> {object_id}"
            )
    _validate_authority_object_shapes(objects=objects)
    topo_order = _toposort_authority_objects(objects=objects)
    return {
        "config_path": config_path.resolve(),
        "root_object_id": root_object_id,
        "default_root_object_id": default_root_object_id,
        "export_roots": export_roots,
        "objects": objects,
        "topo_order": topo_order,
        "context": context,
    }


def _extract_authority_graph_root(*, config_root: dict) -> dict:
    if not isinstance(config_root, dict):
        raise RuntimeError("config root must be a mapping")
    assembly_cfg = _require_mapping(config_root, "assembly")
    return {
        "schema_version": config_root.get("schema_version"),
        "assembly": {
            "root_object": assembly_cfg.get("root_object"),
            "export_roots": assembly_cfg.get("export_roots"),
            "objects": assembly_cfg.get("objects"),
        },
    }


def _validate_authority_object_shapes(*, objects: dict[str, dict]) -> None:
    object_ids_by_kind = _group_object_ids_by_kind(objects=objects)
    _require_single_object_of_kind(object_ids_by_kind=object_ids_by_kind, kind=AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN)
    _require_single_object_of_kind(object_ids_by_kind=object_ids_by_kind, kind=AUTHORITY_OBJECT_KIND_WORKSPACE_SEED)
    _require_single_object_of_kind(
        object_ids_by_kind=object_ids_by_kind,
        kind=AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE,
    )
    _require_single_object_of_kind(object_ids_by_kind=object_ids_by_kind, kind=AUTHORITY_OBJECT_KIND_TARGET_SUPPORT)
    _require_single_object_of_kind(object_ids_by_kind=object_ids_by_kind, kind=AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME)
    _require_single_object_of_kind(object_ids_by_kind=object_ids_by_kind, kind=AUTHORITY_OBJECT_KIND_CXXPACKED)
    if not object_ids_by_kind.get(AUTHORITY_OBJECT_KIND_PYO3_WHEEL):
        raise RuntimeError("authority graph must contain at least one pyo3_wheel object")
    if not object_ids_by_kind.get(AUTHORITY_OBJECT_KIND_MANYLINUX_PROFILE):
        raise RuntimeError("authority graph must contain at least one manylinux_profile object")

    for object_id, node in objects.items():
        if node["kind"] in {
            AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN,
            AUTHORITY_OBJECT_KIND_WORKSPACE_SEED,
            AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE,
            AUTHORITY_OBJECT_KIND_TARGET_SUPPORT,
            AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME,
            AUTHORITY_OBJECT_KIND_CXXPACKED,
        }:
            _require_exact_dep_set(objects=objects, object_id=object_id, expected_deps=())
    for native_object_id in object_ids_by_kind.get(AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME, []):
        native_params = objects[native_object_id]["params"]
        prepare_build_scenario = native_params.get("prepare_build_scenario")
        if prepare_build_scenario != "cargo_closed_sdk_runtime":
            raise RuntimeError(
                "authority native runtime params.prepare_build_scenario must be cargo_closed_sdk_runtime"
            )
        expected_native_deps = (
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_WORKSPACE_SEED,
            ),
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE,
            ),
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_TARGET_SUPPORT,
            ),
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME,
            ),
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_CXXPACKED,
            ),
        )
        _require_exact_dep_set(objects=objects, object_id=native_object_id, expected_deps=expected_native_deps)
    for wheel_object_id in object_ids_by_kind.get(AUTHORITY_OBJECT_KIND_PYO3_WHEEL, []):
        expected_deps = (
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN,
            ),
            _require_single_object_of_kind(
                object_ids_by_kind=object_ids_by_kind,
                kind=AUTHORITY_OBJECT_KIND_WORKSPACE_SEED,
            ),
            *tuple(objects[wheel_object_id]["deps"][2:]),
        )
        _require_exact_dep_set(objects=objects, object_id=wheel_object_id, expected_deps=expected_deps)


def _group_object_ids_by_kind(*, objects: dict[str, dict]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for object_id, node in objects.items():
        grouped.setdefault(node["kind"], []).append(object_id)
    return grouped


def _require_single_object_of_kind(*, object_ids_by_kind: dict[str, list[str]], kind: str) -> str:
    object_ids = object_ids_by_kind.get(kind, [])
    if len(object_ids) != 1:
        raise RuntimeError(f"authority graph must contain exactly one object for kind={kind}: {object_ids}")
    return object_ids[0]


def _require_single_kind_in_object_ids(*, authority_graph: dict, object_ids: set[str], kind: str) -> str:
    matching_object_ids = [
        object_id
        for object_id in authority_graph["topo_order"]
        if object_id in object_ids and authority_graph["objects"][object_id]["kind"] == kind
    ]
    if len(matching_object_ids) != 1:
        raise RuntimeError(
            f"authority dependency closure must contain exactly one object for kind={kind}: {matching_object_ids}"
        )
    return matching_object_ids[0]


def _require_authority_kind(*, objects: dict[str, dict], object_id: str, expected_kind: str) -> None:
    node = objects.get(object_id)
    if node is None:
        raise RuntimeError(f"required authority object is missing: {object_id}")
    actual_kind = node["kind"]
    if actual_kind != expected_kind:
        raise RuntimeError(
            f"authority object kind mismatch: {object_id} expected={expected_kind} actual={actual_kind}"
        )


def _require_exact_dep_set(*, objects: dict[str, dict], object_id: str, expected_deps: tuple[str, ...]) -> None:
    actual_deps = tuple(objects[object_id]["deps"])
    if actual_deps != expected_deps:
        raise RuntimeError(
            f"authority deps mismatch: {object_id} expected={expected_deps} actual={actual_deps}"
        )


def _load_manylinux_backend_plan(*, config_root: dict) -> dict[str, dict]:
    manylinux_cfg = _require_mapping(config_root, "manylinux")
    backend_plans_raw = _require_mapping(manylinux_cfg, "backend_plans")
    backend_plans: dict[str, dict] = {}
    for rdma_backend, plan_raw in backend_plans_raw.items():
        if not isinstance(rdma_backend, str) or not rdma_backend.strip():
            raise RuntimeError(f"manylinux.backend_plans key must be a non-empty string: {rdma_backend!r}")
        if rdma_backend != "closed_sdk":
            raise RuntimeError(
                "public manylinux backend_plans must contain only closed_sdk: "
                f"{rdma_backend!r}"
            )
        if not isinstance(plan_raw, dict):
            raise RuntimeError(f"manylinux.backend_plans.{rdma_backend} must be a mapping")
        profile_object_id = _require_non_empty_string(plan_raw, "profile_object_id")
        native_object_id = _optional_non_empty_string(plan_raw, "native_object_id")
        shared_native_input_object_ids = tuple(
            _require_non_empty_string_list(plan_raw, "shared_native_input_object_ids")
        )
        if native_object_id is not None and native_object_id in shared_native_input_object_ids:
            raise RuntimeError(
                "manylinux backend shared_native_input_object_ids must exclude native_object_id: "
                f"{rdma_backend}.{native_object_id}"
            )
        protoc_object_id = _require_non_empty_string(plan_raw, "protoc_object_id")
        path_object_ids = tuple(_require_non_empty_string_list(plan_raw, "path_object_ids"))
        ld_library_object_ids = tuple(_require_non_empty_string_list(plan_raw, "ld_library_object_ids"))
        finalize_steps_raw = _require_list(plan_raw, "wheel_finalize_steps")
        target_cache_generators_raw = _require_optional_list(plan_raw, "target_cache_generators")
        external_mounts_cfg = _require_optional_mapping(plan_raw, "external_mounts")
        extra_path_dir_names = tuple(_require_optional_string_list(plan_raw, "extra_path_dir_names"))
        extra_ld_library_dir_names = tuple(
            _require_optional_string_list(plan_raw, "extra_ld_library_dir_names")
        )
        finalize_steps: list[dict] = []
        for index, step_raw in enumerate(finalize_steps_raw):
            if not isinstance(step_raw, dict):
                raise RuntimeError(
                    f"manylinux.backend_plans.{rdma_backend}.wheel_finalize_steps[{index}] must be a mapping"
                )
            step_kind = _require_non_empty_string(step_raw, "kind")
            if step_kind not in SUPPORTED_WHEEL_FINALIZE_STEP_KINDS:
                raise RuntimeError(
                    "manylinux backend wheel finalize step kind must be one of "
                    f"{sorted(SUPPORTED_WHEEL_FINALIZE_STEP_KINDS)}: {rdma_backend}.{step_kind}"
                )
            finalize_steps.append(
                {
                    "kind": step_kind,
                    "native_object_id": _optional_non_empty_string(step_raw, "native_object_id"),
                    "native_dir_name": _optional_non_empty_string(step_raw, "native_dir_name"),
                    "relative_subdir": _optional_non_empty_string(step_raw, "relative_subdir"),
                    "plugin_bundle_name": _optional_non_empty_string(step_raw, "plugin_bundle_name"),
                    "extra_library_file_names": tuple(
                        _require_optional_string_list(step_raw, "extra_library_file_names")
                    ),
                }
            )
        target_cache_generators: list[dict] = []
        for index, generator_raw in enumerate(target_cache_generators_raw):
            if not isinstance(generator_raw, dict):
                raise RuntimeError(
                    f"manylinux.backend_plans.{rdma_backend}.target_cache_generators[{index}] must be a mapping"
                )
            generator_kind = _require_non_empty_string(generator_raw, "kind")
            if generator_kind not in SUPPORTED_TARGET_CACHE_GENERATOR_KINDS:
                raise RuntimeError(
                    "manylinux backend target cache generator kind must be one of "
                    f"{sorted(SUPPORTED_TARGET_CACHE_GENERATOR_KINDS)}: {rdma_backend}.{generator_kind}"
                )
            target_cache_generators.append({"kind": generator_kind})
        external_mount_specs: list[dict] = []
        for mount_name, mount_raw in external_mounts_cfg.items():
            if not isinstance(mount_name, str) or not mount_name.strip():
                raise RuntimeError(
                    f"manylinux.backend_plans.{rdma_backend}.external_mounts key must be a non-empty string: {mount_name!r}"
                )
            if not isinstance(mount_raw, dict):
                raise RuntimeError(
                    f"manylinux.backend_plans.{rdma_backend}.external_mounts.{mount_name} must be a mapping"
                )
            container_path = _require_non_empty_string(mount_raw, "container_path")
            if not container_path.startswith("/"):
                raise RuntimeError(
                    "manylinux backend external_mounts.container_path must be an absolute container path: "
                    f"{rdma_backend}.{mount_name}={container_path}"
                )
            external_mount_specs.append(
                {
                    "name": mount_name,
                    "container_path": container_path,
                    "project_relative_path": _require_non_empty_string(mount_raw, "project_relative_path"),
                }
            )
        backend_plans[rdma_backend] = {
            "rdma_backend": rdma_backend,
            "profile_object_id": profile_object_id,
            "native_object_id": native_object_id,
            "shared_native_input_object_ids": shared_native_input_object_ids,
            "protoc_object_id": protoc_object_id,
            "path_object_ids": path_object_ids,
            "ld_library_object_ids": ld_library_object_ids,
            "extra_path_dir_names": extra_path_dir_names,
            "extra_ld_library_dir_names": extra_ld_library_dir_names,
            "wheel_finalize_steps": tuple(finalize_steps),
            "target_cache_generators": tuple(target_cache_generators),
            "external_mounts": tuple(external_mount_specs),
        }
    if not backend_plans:
        raise RuntimeError("manylinux.backend_plans must not be empty")
    return backend_plans


def _select_backend_plan(*, backend_plan: dict[str, dict], rdma_backend: str) -> dict:
    selected_plan = backend_plan.get(rdma_backend)
    if selected_plan is None:
        raise RuntimeError(
            f"manylinux.rdma_backend must be one of {sorted(backend_plan)}: {rdma_backend}"
        )
    return selected_plan


def _toposort_authority_objects(*, objects: dict[str, dict]) -> list[str]:
    ordered: list[str] = []
    visit_state: dict[str, str] = {}
    path_stack: list[str] = []

    def dfs(object_id: str) -> None:
        state = visit_state.get(object_id)
        if state == "done":
            return
        if state == "visiting":
            cycle_path = " -> ".join(path_stack + [object_id])
            raise RuntimeError(f"authority object graph contains a cycle: {cycle_path}")
        visit_state[object_id] = "visiting"
        path_stack.append(object_id)
        for dep_id in objects[object_id]["deps"]:
            dfs(dep_id)
        path_stack.pop()
        visit_state[object_id] = "done"
        ordered.append(object_id)

    for object_id in objects:
        dfs(object_id)
    return ordered


def _write_authority_exports(*, published_profile_dir: Path, authority_graph: dict) -> None:
    profile_expr_path = published_profile_dir / PUBLISHED_PROFILE_NIX_FILE_NAME
    profile_expr_path.write_text(
        _render_authority_profile_nix_expr(authority_graph=authority_graph),
        encoding="utf-8",
    )
    authority_mermaid_path = published_profile_dir / AUTHORITY_MERMAID_FILE_NAME
    authority_mermaid_path.write_text(
        _render_authority_mermaid(authority_graph=authority_graph),
        encoding="utf-8",
    )


def _render_authority_profile_nix_expr(*, authority_graph: dict) -> str:
    context = authority_graph["context"]
    export_root_id = authority_graph["export_roots"][AUTHORITY_EXPORT_PUBLISHED_PROFILE_NIX]
    root_node = authority_graph["objects"][export_root_id]
    if root_node["kind"] != AUTHORITY_OBJECT_KIND_MANYLINUX_PROFILE:
        raise RuntimeError(
            "authority export root for published_profile_nix must be manylinux_profile"
        )
    header = f"""# Generated by setup_and_pack/nix/pack_fluxonkv_pylib.py --publish-profile
# Authority graph source: {authority_graph["config_path"]}
# This expression is derived from the flat authority object graph YAML.
#
# Example:
# nix build -f ./profile.nix \\
#   --argstr containerImageDigest sha256:<image-digest> \\
#   --argstr pythonAbiTag cp38-abi3 \\
#   --argstr wheelPathText /abs/path/to/fluxon_pyo3.whl
{{ containerImageDigest, pythonAbiTag, wheelPathText }}:
let
  flakeRoot = {_nix_path_expr(SCRIPT_DIR)};
  fluxon = (builtins.getFlake (toString flakeRoot)).lib.forSystem {_nix_string_literal(context["system_name"])};
"""
    lines = [header.rstrip("\n")]
    lines.extend(
        _render_authority_object_bindings(
            authority_graph=authority_graph,
            root_object_id=export_root_id,
        )
    )
    root_binding_expr = _render_profile_root_invocation(
        authority_graph=authority_graph,
        root_object_id=export_root_id,
    )
    lines.extend(
        [
            "in",
            root_binding_expr,
            "",
        ]
    )
    return "\n".join(lines)


def _render_authority_object_bindings(*, authority_graph: dict, root_object_id: str) -> list[str]:
    included_object_ids = _collect_authority_dependency_closure(
        authority_graph=authority_graph,
        root_object_id=root_object_id,
    )
    lines: list[str] = []
    for object_id in authority_graph["topo_order"]:
        if object_id not in included_object_ids:
            continue
        node = authority_graph["objects"][object_id]
        lines.extend(_render_authority_object_binding(authority_graph=authority_graph, node=node))
    return lines


def _collect_authority_dependency_closure(*, authority_graph: dict, root_object_id: str) -> set[str]:
    collected: set[str] = set()

    def visit(object_id: str) -> None:
        for dep_id in authority_graph["objects"][object_id]["deps"]:
            if dep_id in collected:
                continue
            collected.add(dep_id)
            visit(dep_id)

    visit(root_object_id)
    return collected


def _render_authority_object_binding(*, authority_graph: dict, node: dict) -> list[str]:
    object_id = node["id"]
    kind = node["kind"]
    object_ids_by_kind = _group_object_ids_by_kind(objects=authority_graph["objects"])
    resolved = _resolve_authority_ref_value(
        bindings=authority_graph["context"]["authority_bindings"],
        authority_ref=node["authority_ref"],
    )
    if kind == AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN:
        lines = [
            f"  {object_id} = fluxon.mkFluxonManylinuxToolchain {{",
            f"    baseSystemKey = {_nix_string_literal(resolved['base_system_key'])};",
            f"    runtimeAbiKey = {_nix_string_literal(resolved['runtime_abi_key'])};",
            f"    manylinuxVersion = {_nix_string_literal(resolved['manylinux_version'])};",
            "    pythonAbiTag = pythonAbiTag;",
            f"    runtimeImageRef = {_nix_string_literal(resolved['runtime_image_ref'])};",
            "    containerImageDigest = containerImageDigest;",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_WORKSPACE_SEED:
        lines = [
            f"  {object_id} = fluxon.mkFluxonWorkspaceSeed {{",
            f"    workspaceSeedPath = {_nix_path_expr(Path(resolved['workspace_seed_path']))};",
            f"    projectRoot = {_nix_path_expr(Path(resolved['project_root']))};",
            f"    transportBackend = {_nix_string_literal(resolved['transport_backend'])};",
            f"    runtimeAbiKey = {_nix_string_literal(resolved['runtime_abi_key'])};",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE:
        lines = [
            f"  {object_id} = fluxon.mkFluxonCommuRuntimeSource {{",
            f"    fluxonCommuSource = {_nix_path_expr(Path(resolved['source_path']))};",
            f"    crateVersion = {_nix_string_literal(resolved['crate_version'])};",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_TARGET_SUPPORT:
        lines = [
            f"  {object_id}TargetDirNames = {_nix_list_literal(resolved['target_dir_names'])};",
            f"  {object_id} = fluxon.mkFluxonTargetSupport {{",
            f"    sourcePath = {_nix_path_expr(Path(resolved['source_path']))};",
            f"    runtimeAbiKey = {_nix_string_literal(resolved['runtime_abi_key'])};",
            f"    targetDirNames = {object_id}TargetDirNames;",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME:
        lines = [
            f"  {object_id} = fluxon.mkFluxonVendorRuntime {{",
            f"    sourcePath = {_nix_path_expr(Path(resolved['source_path']))};",
            f"    runtimeAbiKey = {_nix_string_literal(resolved['runtime_abi_key'])};",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_NATIVE_RUNTIME:
        fluxon_commu_object_id = _require_single_object_of_kind(
            object_ids_by_kind=object_ids_by_kind,
            kind=AUTHORITY_OBJECT_KIND_FLUXON_COMMU_RUNTIME_SOURCE,
        )
        workspace_seed_object_id = _require_single_object_of_kind(
            object_ids_by_kind=object_ids_by_kind,
            kind=AUTHORITY_OBJECT_KIND_WORKSPACE_SEED,
        )
        vendor_runtime_object_id = _require_single_object_of_kind(
            object_ids_by_kind=object_ids_by_kind,
            kind=AUTHORITY_OBJECT_KIND_VENDOR_RUNTIME,
        )
        cxxpacked_object_id = _require_single_object_of_kind(
            object_ids_by_kind=object_ids_by_kind,
            kind=AUTHORITY_OBJECT_KIND_CXXPACKED,
        )
        lines = [
            f"  {object_id} = fluxon.mkFluxonNativeRuntime {{",
            f"    sourcePath = {_nix_path_expr(Path(resolved['source_path']))};",
            f"    baseSystemKey = {_nix_string_literal(resolved['base_system_key'])};",
            f"    runtimeAbiKey = {_nix_string_literal(resolved['runtime_abi_key'])};",
            f"    buildAuthority = {_nix_json_expr(resolved['build_authority'])};",
            f"    fluxonCommuAuthority = {fluxon_commu_object_id};",
            f"    workspaceSeed = {workspace_seed_object_id};",
            f"    vendorRuntime = {vendor_runtime_object_id};",
            f"    cxxpacked = {cxxpacked_object_id};",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_CXXPACKED:
        lines = [
            f"  {object_id} = fluxon.mkFluxonCxxpacked {{",
            f"    sourcePath = {_nix_path_expr(Path(resolved['source_path']))};",
            f"    runtimeAbiKey = {_nix_string_literal(resolved['runtime_abi_key'])};",
            "  };",
        ]
    elif kind == AUTHORITY_OBJECT_KIND_PYO3_WHEEL:
        wheel_runtime_abi_key = resolved["runtime_abi_key"]
        transport_backend = resolved["transport_backend"]
        lines = [
            f"  {object_id} = fluxon.mkFluxonPyo3Wheel {{",
            "    wheelPath = /. + wheelPathText;",
            f"    runtimeAbiKey = {_nix_string_literal(wheel_runtime_abi_key)};",
            "    pythonAbiTag = pythonAbiTag;",
            f"    transportBackend = {_nix_string_literal(transport_backend)};",
            "  };",
        ]
    else:
        raise RuntimeError(f"unsupported authority object kind for Nix binding: {kind}")
    return lines


def _render_profile_root_invocation(*, authority_graph: dict, root_object_id: str) -> str:
    context = authority_graph["context"]
    included_object_ids = _collect_authority_dependency_closure(
        authority_graph=authority_graph,
        root_object_id=root_object_id,
    )
    root_params = _resolve_authority_ref_value(
        bindings=context["authority_bindings"],
        authority_ref=authority_graph["objects"][root_object_id]["authority_ref"],
    )
    inherited_object_ids = [
        _require_single_kind_in_object_ids(
            authority_graph=authority_graph,
            object_ids=included_object_ids,
            kind=AUTHORITY_OBJECT_KIND_MANYLINUX_TOOLCHAIN,
        ),
        _require_single_kind_in_object_ids(
            authority_graph=authority_graph,
            object_ids=included_object_ids,
            kind=AUTHORITY_OBJECT_KIND_WORKSPACE_SEED,
        ),
        _require_single_kind_in_object_ids(
            authority_graph=authority_graph,
            object_ids=included_object_ids,
            kind=AUTHORITY_OBJECT_KIND_TARGET_SUPPORT,
        ),
        *_require_object_ids_for_profile_native_inputs(
            authority_graph=authority_graph,
            root_params=root_params,
        ),
        _require_single_kind_in_object_ids(
            authority_graph=authority_graph,
            object_ids=included_object_ids,
            kind=AUTHORITY_OBJECT_KIND_PYO3_WHEEL,
        ),
    ]
    return "\n".join(
        [
            f"fluxon.{context['profile_constructor_name']} {{",
            f"  profileName = {_nix_string_literal(root_params['profile_name'])};",
            f"  assemblyName = {_nix_string_literal(root_params['assembly_name'])};",
            f"  baseSystemKey = {_nix_string_literal(root_params['base_system_key'])};",
            f"  runtimeAbiKey = {_nix_string_literal(root_params['runtime_abi_key'])};",
            f"  nativeInputObjectIds = {_nix_list_literal(root_params['native_object_ids'])};",
            f"  inherit {' '.join(inherited_object_ids)};",
            "}",
        ]
    )


def _require_object_ids_for_profile_native_inputs(*, authority_graph: dict, root_params: dict) -> list[str]:
    objects = authority_graph["objects"]
    object_ids_by_kind = _group_object_ids_by_kind(objects=objects)
    native_object_ids = root_params.get("native_object_ids")
    if not isinstance(native_object_ids, list) or not native_object_ids:
        raise RuntimeError("published_profile.root.native_object_ids must be a non-empty list")
    resolved_object_ids: list[str] = []
    for object_id in native_object_ids:
        if not isinstance(object_id, str) or not object_id:
            raise RuntimeError(f"published_profile.root.native_object_ids item must be a non-empty string: {object_id!r}")
        if object_id not in objects:
            raise RuntimeError(f"published_profile.root.native_object_ids references missing object: {object_id}")
        resolved_object_ids.append(object_id)
    return resolved_object_ids


def _render_authority_mermaid(*, authority_graph: dict) -> str:
    lines = [
        """---
title: Fluxon Manylinux Authority DAG
---
flowchart TD""".rstrip("\n")
    ]
    for object_id in authority_graph["topo_order"]:
        node = authority_graph["objects"][object_id]
        label = f"{object_id}\\n{node['kind']}"
        lines.append(f"  {object_id}[\"{label}\"]")
    for object_id in authority_graph["topo_order"]:
        node = authority_graph["objects"][object_id]
        for dep_id in node["deps"]:
            lines.append(f"  {dep_id} --> {object_id}")
    lines.append(
        f"  classDef root fill:#d5f5e3,stroke:#1e8449,stroke-width:2px"
    )
    lines.append(f"  class {authority_graph['root_object_id']} root")
    lines.append("")
    return "\n".join(lines)


def _resolve_authority_ref_value(*, bindings: dict, authority_ref: str):
    current = bindings
    for part in authority_ref.split("."):
        if not isinstance(current, dict) or part not in current:
            raise RuntimeError(f"authority_ref is invalid: {authority_ref}")
        current = current[part]
    return current


def _require_string_list_field(raw: dict, key: str) -> list[str]:
    value = raw.get(key)
    if not isinstance(value, list):
        raise RuntimeError(f"config field must be a list: {key}")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise RuntimeError(f"config field list item must be a non-empty string: {key}")
        normalized.append(item)
    return normalized


def _require_profile_constructor_name(*, base_system_key: str, runtime_abi_key: str) -> str:
    if base_system_key.startswith("manylinux_2_28_") and runtime_abi_key.endswith("_cpython3.10"):
        return "mkManylinux228Cpython310Profile"
    raise RuntimeError(
        "setup_and_pack/nix currently has no concrete profile constructor for "
        f"base_system_key={base_system_key} runtime_abi_key={runtime_abi_key}"
    )


def _require_nix_system_name(*, base_system_key: str) -> str:
    if base_system_key.endswith("_x86_64"):
        return "x86_64-linux"
    if base_system_key.endswith("_aarch64"):
        return "aarch64-linux"
    raise RuntimeError(f"unsupported Nix system mapping for base_system_key={base_system_key}")


def _derive_manylinux_version(*, base_system: str) -> str:
    if base_system == "manylinux_2_28":
        return "2_28"
    raise RuntimeError(f"unsupported manylinux version mapping for base_system={base_system}")


def _nix_string_literal(value: str) -> str:
    return json.dumps(value)


def _nix_path_expr(path: Path) -> str:
    return f"/. + {_nix_string_literal(str(path.resolve()))}"


def _nix_list_literal(values: tuple[str, ...] | list[str]) -> str:
    return "[ " + " ".join(_nix_string_literal(value) for value in values) + " ]"


def _nix_json_expr(value: dict) -> str:
    return f"builtins.fromJSON {_nix_string_literal(json.dumps(value, sort_keys=True))}"


def _optional_file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _sha256_json_bytes(*, raw: dict) -> str:
    encoded = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_yaml_mapping(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"yaml root must be a mapping: {path}")
    return raw


def _require_mapping(raw: dict, key: str) -> dict:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"config.{key} must be a mapping")
    return value


def _require_optional_mapping(raw: dict, key: str) -> dict:
    value = raw.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise RuntimeError(f"config.{key} must be a mapping when present")
    return value


def _require_list(raw: dict, key: str) -> list:
    value = raw.get(key)
    if not isinstance(value, list):
        raise RuntimeError(f"config.{key} must be a list")
    return value


def _require_optional_list(raw: dict, key: str) -> list:
    value = raw.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise RuntimeError(f"config.{key} must be a list when present")
    return value


def _require_non_empty_string(raw: dict, key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f"config field must be a string: {key}")
    value = value.strip()
    if not value:
        raise RuntimeError(f"config field must be non-empty: {key}")
    return value


def _optional_non_empty_string(raw: dict, key: str) -> str | None:
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"config field must be a string when present: {key}")
    value = value.strip()
    if not value:
        raise RuntimeError(f"config field must be non-empty when present: {key}")
    return value


def _require_optional_string_list(raw: dict, key: str) -> list[str]:
    values = _require_optional_list(raw, key)
    normalized: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise RuntimeError(f"config field list item must be a string: {key}")
        item = item.strip()
        if not item:
            raise RuntimeError(f"config field list item must be non-empty: {key}")
        normalized.append(item)
    return normalized


def _require_non_empty_string_list(raw: dict, key: str) -> list[str]:
    values = _require_list(raw, key)
    normalized: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise RuntimeError(f"config field list item must be a string: {key}")
        item = item.strip()
        if not item:
            raise RuntimeError(f"config field list item must be non-empty: {key}")
        if item in normalized:
            raise RuntimeError(f"config field list item must be unique: {key}={item}")
        normalized.append(item)
    if not normalized:
        raise RuntimeError(f"config field list must be non-empty: {key}")
    return normalized


def _require_absolute_path(raw: dict, key: str) -> Path:
    path = Path(_require_non_empty_string(raw, key))
    if not path.is_absolute():
        raise RuntimeError(f"config path must be absolute: {key}={path}")
    return path.resolve()


def _require_absolute_existing_dir(raw: dict, key: str) -> Path:
    path = _require_absolute_path(raw, key)
    if not path.exists():
        raise RuntimeError(f"required directory does not exist: {path}")
    if not path.is_dir():
        raise RuntimeError(f"required path must be a directory: {path}")
    return path.resolve()


def _require_absolute_existing_file(raw: dict, key: str) -> Path:
    path = _require_absolute_path(raw, key)
    if not path.exists():
        raise RuntimeError(f"required file does not exist: {path}")
    if not path.is_file():
        raise RuntimeError(f"required path must be a file: {path}")
    return path.resolve()


def _require_explicit_image_ref(raw: dict, key: str) -> str:
    image_ref = _require_non_empty_string(raw, key)
    if "@" in image_ref:
        raise RuntimeError(
            f"config docker image ref must use name:tag, digest refs are unsupported: {key}={image_ref}"
        )
    if ":" not in image_ref:
        raise RuntimeError(f"config docker image ref must include an explicit tag: {key}={image_ref}")
    image_name, image_tag = image_ref.rsplit(":", 1)
    if not image_name or not image_tag:
        raise RuntimeError(
            f"config docker image ref must be non-empty on both sides of ':': {key}={image_ref}"
        )
    return image_ref


def _shell_join(argv: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in argv)


if __name__ == "__main__":
    main()
