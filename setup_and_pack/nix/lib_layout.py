from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml

from setup_and_pack.public_workspace_contract import (
    PUBLIC_WORKSPACE_INPUT_RELATIVE_PATHS,
    _copy_public_workspace_input_path,
    _sanitize_public_workspace_input,
)


INSTANCE_DIR_NAMES = ("work", "logs", "shm", "tmp")
PROFILE_SOURCE_KIND_BRIDGE_PREBUILT = "bridge_prebuilt"
PROFILE_SOURCE_KIND_NIX_PROFILE = "nix_profile"
PROFILE_SOURCE_KIND_EXTERNAL_PATH = "external_path"
PROFILE_SOURCE_KINDS = (
    PROFILE_SOURCE_KIND_BRIDGE_PREBUILT,
    PROFILE_SOURCE_KIND_NIX_PROFILE,
    PROFILE_SOURCE_KIND_EXTERNAL_PATH,
)
PROFILE_SOURCE_KIND_ALIASES = {
    PROFILE_SOURCE_KIND_EXTERNAL_PATH: PROFILE_SOURCE_KIND_NIX_PROFILE,
}
PROJECT_ROOT_MARKER_FILE_NAMES = ("setup.py", "pyproject.toml")
PROJECT_ROOT_MARKER_DIR_NAMES = (".git",)
PROJECT_ROOT_REQUIRED_CHILD_DIR_NAMES = ("setup_and_pack",)
MANYLINUX_EXECUTION_SUBSTRATE = "manylinux_container"
SUPPORTED_BASE_SYSTEMS = ("manylinux_2_28",)
SUPPORTED_ARCHITECTURES = ("x86_64", "aarch64")
SUPPORTED_PYTHON_ABIS = ("cpython3.10",)
DEFAULT_PROFILE_NAME = "current"
DEFAULT_ASSEMBLY_NAME = "cold_start"
DEFAULT_INSTANCE_ID = "cold_start"
PACK_CONFIG_STATIC_STEM_SUFFIX = "_static"
PACK_CONFIG_ENV_STEM_SUFFIX = "_env"
DEFAULT_PACK_CONFIG_ENV_DIR_NAME = "setup_and_pack"
HOST_PATHS_ROOT_KEY = "root_path"
DEFAULT_HOST_PATH_SUFFIXES = {
    ("store", "project_data_root"): "",
    ("assembly", "baseline_path"): "manylinux-release",
    ("manylinux", "cargo_registry_dir"): "manylinux-cache/cargo-registry",
    ("manylinux", "cargo_git_dir"): "manylinux-cache/cargo-git",
}
BRIDGE_PREBUILT_WORKSPACE_SEED_EXTRA_RELATIVE_PATHS = (
    "setup_and_pack/nix",
    "setup_and_pack/lib_tool.py",
    "setup_and_pack/pyscript_util.py",
    "setup_and_pack/closed_sdk_contract.py",
    "setup_and_pack/public_workspace_contract.py",
    "setup_and_pack/pub_prepare_build.py",
    "setup_and_pack/pub_prepare_build.yaml",
    "setup_and_pack/utils/wheel_runtime_helper.py",
    "setup_and_pack/utils",
    "deployment/utils/placeholder_utils.py",
    "deployment/utils/proc_lifecycle_codegen.py",
    "deployment/utils/selection_supervisor_codegen.py",
    "fluxon_release/closed_sdk",
    "fluxon_rs/fluxon_commu_contract",
    "fluxon_rs/fluxon_commu",
    "fluxon_rs/fluxon_commu_closed_sdk_consumer",
    "fluxon_rs/Cargo.lock",
)
BRIDGE_PREBUILT_WORKSPACE_SEED_RELATIVE_PATHS = tuple(
    dict.fromkeys(
        (
            *PUBLIC_WORKSPACE_INPUT_RELATIVE_PATHS,
            *BRIDGE_PREBUILT_WORKSPACE_SEED_EXTRA_RELATIVE_PATHS,
        )
    )
)


@dataclass(frozen=True)
class AssemblyRefs:
    baseline_path: str


@dataclass(frozen=True)
class ProfileSource:
    source_kind: str
    profile_path: str | None
    build_root_path: str | None
    closed_sdk_search_roots: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProfileLayoutSpec:
    native_runtime_dir_names: tuple[str, ...]
    target_support_dir_names: tuple[str, ...]
    ext_bundle_dir_name: str


@dataclass(frozen=True)
class ResolvedProfileSource:
    source_kind: str
    profile_path: Path
    build_root_path: Path
    native_runtime_store_path: Path
    target_support_store_path: Path
    ext_bundle_path: Path
    native_runtime_path_by_name: dict[str, Path]
    target_support_path_by_name: dict[str, Path]


@dataclass(frozen=True)
class ExperimentSpec:
    config_path: Path
    project_root: Path
    project_data_root: Path
    base_system: str
    architectures: tuple[str, ...]
    python_abi: str
    profile_name: str
    assembly_name: str
    instance_id: str
    target_cache_namespace: str
    profile_source: ProfileSource
    profile_layout: ProfileLayoutSpec
    assembly_refs: AssemblyRefs


@dataclass(frozen=True)
class RuntimeTarget:
    execution_substrate: str
    base_system_key: str
    runtime_abi_key: str
    architecture: str
    python_abi: str
    profile_name: str
    assembly_name: str
    instance_id: str


@dataclass(frozen=True)
class LayoutPaths:
    project_scope_id: str
    project_root_dir: Path
    project_meta_path: Path
    substrate_root: Path
    assemblies_dir: Path
    assembly_dir: Path
    manifest_path: Path
    profiles_dir: Path
    profile_link: Path
    assembly_profile_dir: Path
    assembly_profile_manifest_path: Path
    assembly_profile_native_dir: Path
    assembly_profile_target_support_dir: Path
    assembly_profile_baseline_link: Path
    instances_dir: Path
    instance_dir: Path
    instance_work_dir: Path
    instance_logs_dir: Path
    instance_shm_dir: Path
    instance_tmp_dir: Path
    instance_release_dir: Path
    instance_target_caches_dir: Path
    target_caches_root_dir: Path
    app_link: Path
    native_link: Path
    target_support_link: Path
    ext_link: Path
    baseline_link: Path


def load_experiment_config_root(*, config_path: Path) -> dict:
    config_path = config_path.resolve()
    raw_config = _load_yaml_mapping_file(config_path)
    if config_path.stem.endswith(PACK_CONFIG_STATIC_STEM_SUFFIX):
        env_path = _resolve_split_experiment_env_path(
            config_path=config_path,
        )
        template_path = env_path.with_name(env_path.name + ".template")
        if not env_path.is_file():
            raise RuntimeError(
                "split experiment config is missing required env companion file: "
                f"static={config_path} env={env_path} template={template_path}. "
                f"Create {env_path.name} from {template_path.name} first."
            )
        env_config = _load_yaml_mapping_file(env_path)
        static_schema = raw_config.get("schema_version")
        env_schema = env_config.get("schema_version")
        if env_schema is not None and static_schema != env_schema:
            raise RuntimeError(
                "split experiment config schema_version mismatch: "
                f"static={config_path} env={env_path} "
                f"static_schema={static_schema!r} env_schema={env_schema!r}"
            )
        raw_config = _deep_merge_mappings(base=raw_config, overlay=env_config)
    _expand_host_path_root_aliases(raw_config)
    return raw_config


def _resolve_split_experiment_env_path(*, config_path: Path) -> Path:
    project_root = _detect_project_root(config_path=config_path)
    return (
        project_root
        / DEFAULT_PACK_CONFIG_ENV_DIR_NAME
        / (
            config_path.stem[: -len(PACK_CONFIG_STATIC_STEM_SUFFIX)]
            + PACK_CONFIG_ENV_STEM_SUFFIX
            + config_path.suffix
        )
    )


def _expand_host_path_root_aliases(raw_config: dict) -> None:
    host_paths = raw_config.get("host_paths")
    if host_paths is None:
        return
    if not isinstance(host_paths, dict):
        raise RuntimeError("config.host_paths must be a mapping when present")
    root_value = host_paths.get(HOST_PATHS_ROOT_KEY)
    if root_value is None:
        return
    if not isinstance(root_value, str) or not root_value.strip():
        raise RuntimeError("config.host_paths.root_path must be a non-empty string when present")
    root_path = Path(root_value.strip())
    if not root_path.is_absolute():
        raise RuntimeError(f"config.host_paths.root_path must be an absolute path: {root_value}")
    resolved_root = root_path.resolve()

    for key_path, suffix in DEFAULT_HOST_PATH_SUFFIXES.items():
        section_name, field_name = key_path
        section = raw_config.get(section_name)
        if section is None:
            section = {}
            raw_config[section_name] = section
        if not isinstance(section, dict):
            raise RuntimeError(f"config.{section_name} must be a mapping when present")
        if field_name in section:
            continue
        derived_path = resolved_root if not suffix else (resolved_root / suffix)
        section[field_name] = str(derived_path)


def load_experiment_spec(*, config_path: Path) -> ExperimentSpec:
    raw_config = load_experiment_config_root(config_path=config_path)
    return load_experiment_spec_from_root(config_path=config_path, config_root=raw_config)


def load_experiment_spec_from_root(*, config_path: Path, config_root: dict) -> ExperimentSpec:
    raw_config = config_root
    store_config = _require_mapping(raw_config, "store")
    runtime_config = _require_mapping(raw_config, "runtime")
    profile_config = _require_mapping(raw_config, "profile")
    assembly_config = _require_mapping(raw_config, "assembly")

    project_root = _detect_project_root(config_path=config_path)
    project_data_root = _require_absolute_path(store_config, "project_data_root")
    base_system = _require_enum_string(runtime_config, "base_system", SUPPORTED_BASE_SYSTEMS)
    architectures = tuple(
        _require_string_enum_list(
            runtime_config,
            "architectures",
            SUPPORTED_ARCHITECTURES,
        )
    )
    python_abi = _require_enum_string(runtime_config, "python_abi", SUPPORTED_PYTHON_ABIS)
    profile_name = _optional_non_empty_string(runtime_config, "profile_name") or DEFAULT_PROFILE_NAME
    assembly_name = _optional_non_empty_string(runtime_config, "assembly_name") or DEFAULT_ASSEMBLY_NAME
    instance_id = _optional_non_empty_string(runtime_config, "instance_id") or DEFAULT_INSTANCE_ID
    target_cache_namespace = (
        _optional_non_empty_string(runtime_config, "target_cache_namespace") or assembly_name
    )

    profile_source_kind = _require_enum_string(
        profile_config,
        "source_kind",
        PROFILE_SOURCE_KINDS,
    )
    profile_source_kind = PROFILE_SOURCE_KIND_ALIASES.get(profile_source_kind, profile_source_kind)
    native_runtime_dir_names = tuple(
        _require_non_empty_string_list(profile_config, "native_runtime_dir_names")
    )
    target_support_dir_names = tuple(
        _require_string_list(profile_config, "target_support_dir_names")
    )
    ext_bundle_dir_name = _require_non_empty_string(profile_config, "ext_bundle_dir_name")
    if ext_bundle_dir_name not in native_runtime_dir_names:
        raise RuntimeError(
            "profile.ext_bundle_dir_name must be one of profile.native_runtime_dir_names: "
            f"{ext_bundle_dir_name}"
        )

    return ExperimentSpec(
        config_path=config_path.resolve(),
        project_root=project_root,
        project_data_root=project_data_root,
        base_system=base_system,
        architectures=architectures,
        python_abi=python_abi,
        profile_name=profile_name,
        assembly_name=assembly_name,
        instance_id=instance_id,
        target_cache_namespace=target_cache_namespace,
        profile_source=ProfileSource(
            source_kind=profile_source_kind,
            profile_path=_optional_non_empty_string(profile_config, "profile_path"),
            build_root_path=_optional_non_empty_string(profile_config, "build_root_path"),
            closed_sdk_search_roots=_require_optional_absolute_path_list(
                profile_config,
                "closed_sdk_search_roots",
            ),
        ),
        profile_layout=ProfileLayoutSpec(
            native_runtime_dir_names=native_runtime_dir_names,
            target_support_dir_names=target_support_dir_names,
            ext_bundle_dir_name=ext_bundle_dir_name,
        ),
        assembly_refs=AssemblyRefs(
            baseline_path=_require_non_empty_string(assembly_config, "baseline_path"),
        ),
    )


def build_runtime_targets(*, spec: ExperimentSpec) -> tuple[RuntimeTarget, ...]:
    runtime_targets: list[RuntimeTarget] = []
    for architecture in spec.architectures:
        runtime_targets.append(
            RuntimeTarget(
                execution_substrate=_derive_execution_substrate(base_system=spec.base_system),
                base_system_key=_derive_base_system_key(
                    base_system=spec.base_system,
                    architecture=architecture,
                ),
                runtime_abi_key=_derive_runtime_abi_key(
                    base_system=spec.base_system,
                    architecture=architecture,
                    python_abi=spec.python_abi,
                ),
                architecture=architecture,
                python_abi=spec.python_abi,
                profile_name=spec.profile_name,
                assembly_name=spec.assembly_name,
                instance_id=spec.instance_id,
            )
        )
    return tuple(runtime_targets)


def build_layout(*, spec: ExperimentSpec, runtime_target: RuntimeTarget) -> LayoutPaths:
    project_scope_id = compute_project_scope_id(project_root=spec.project_root)
    project_root_dir = spec.project_data_root / "projects" / project_scope_id
    substrate_root = project_root_dir / "substrates" / runtime_target.execution_substrate
    assemblies_dir = substrate_root / "assemblies"
    assembly_dir = assemblies_dir / runtime_target.assembly_name
    profiles_dir = substrate_root / "profiles"
    profile_link = profiles_dir / runtime_target.profile_name
    assembly_profile_dir = assembly_dir / "profile"
    target_caches_root_dir = substrate_root / "target-caches" / spec.target_cache_namespace
    instances_dir = substrate_root / "instances"
    instance_dir = instances_dir / runtime_target.instance_id
    return LayoutPaths(
        project_scope_id=project_scope_id,
        project_root_dir=project_root_dir,
        project_meta_path=project_root_dir / "project_meta.yaml",
        substrate_root=substrate_root,
        assemblies_dir=assemblies_dir,
        assembly_dir=assembly_dir,
        manifest_path=assembly_dir / "manifest.lock.yaml",
        profiles_dir=profiles_dir,
        profile_link=profile_link,
        assembly_profile_dir=assembly_profile_dir,
        assembly_profile_manifest_path=assembly_profile_dir / "manifest.json",
        assembly_profile_native_dir=assembly_profile_dir / "native",
        assembly_profile_target_support_dir=assembly_profile_dir / "target_support",
        assembly_profile_baseline_link=assembly_profile_dir / "baseline",
        instances_dir=instances_dir,
        instance_dir=instance_dir,
        instance_work_dir=instance_dir / "work",
        instance_logs_dir=instance_dir / "logs",
        instance_shm_dir=instance_dir / "shm",
        instance_tmp_dir=instance_dir / "tmp",
        instance_release_dir=instance_dir / "release",
        instance_target_caches_dir=target_caches_root_dir,
        target_caches_root_dir=target_caches_root_dir,
        app_link=assembly_dir / "app",
        native_link=assembly_dir / "native",
        target_support_link=assembly_dir / "target_support",
        ext_link=assembly_dir / "ext",
        baseline_link=assembly_dir / "baseline",
    )


def apply_layout(*, spec: ExperimentSpec, runtime_target: RuntimeTarget, layout: LayoutPaths) -> None:
    resolved_profile_source = _resolve_profile_source(spec=spec, layout=layout)
    baseline_path = _resolve_existing_dir_path(spec.assembly_refs.baseline_path, "assembly.baseline_path")

    layout.project_root_dir.mkdir(parents=True, exist_ok=True)
    layout.assemblies_dir.mkdir(parents=True, exist_ok=True)
    layout.assembly_dir.mkdir(parents=True, exist_ok=True)
    layout.profiles_dir.mkdir(parents=True, exist_ok=True)
    layout.assembly_profile_dir.mkdir(parents=True, exist_ok=True)
    layout.assembly_profile_native_dir.mkdir(parents=True, exist_ok=True)
    layout.assembly_profile_target_support_dir.mkdir(parents=True, exist_ok=True)
    layout.instances_dir.mkdir(parents=True, exist_ok=True)
    layout.instance_dir.mkdir(parents=True, exist_ok=True)

    for path in (
        layout.instance_work_dir,
        layout.instance_logs_dir,
        layout.instance_shm_dir,
        layout.instance_tmp_dir,
        layout.instance_release_dir,
        layout.instance_target_caches_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)

    project_meta = {
        "project": {
            "config_path": str(spec.config_path),
            "project_root": str(spec.project_root),
            "project_scope_id": layout.project_scope_id,
            "project_data_root": str(spec.project_data_root),
        }
    }
    layout.project_meta_path.write_text(
        yaml.safe_dump(project_meta, sort_keys=False),
        encoding="utf-8",
    )

    manifest = {
        "assembly": {
            "project_scope_id": layout.project_scope_id,
            "execution_substrate": runtime_target.execution_substrate,
            "base_system_key": runtime_target.base_system_key,
            "runtime_abi_key": runtime_target.runtime_abi_key,
            "assembly_name": runtime_target.assembly_name,
            "profile_name": runtime_target.profile_name,
            "instance_id": runtime_target.instance_id,
            "profile_source_kind": resolved_profile_source.source_kind,
            "app_ref": str(spec.project_root),
            "build_root_ref": str(resolved_profile_source.build_root_path),
            "native_runtime_ref": str(resolved_profile_source.native_runtime_store_path),
            "target_support_ref": str(resolved_profile_source.target_support_store_path),
            "ext_bundle_ref": str(resolved_profile_source.ext_bundle_path),
            "baseline_ref": str(baseline_path),
            "profile_ref": str(resolved_profile_source.profile_path),
        }
    }
    layout.manifest_path.write_text(
        yaml.safe_dump(manifest, sort_keys=False),
        encoding="utf-8",
    )

    profile_manifest = {
        "object_kind": "FluxonManylinuxProfileLayout",
        "source_kind": resolved_profile_source.source_kind,
        "project_scope_id": layout.project_scope_id,
        "execution_substrate": runtime_target.execution_substrate,
        "base_system_key": runtime_target.base_system_key,
        "runtime_abi_key": runtime_target.runtime_abi_key,
        "profile_name": runtime_target.profile_name,
        "assembly_name": runtime_target.assembly_name,
        "native_runtime_dir_names": list(spec.profile_layout.native_runtime_dir_names),
        "target_support_dir_names": list(spec.profile_layout.target_support_dir_names),
        "source_refs": {
            "build_root": str(resolved_profile_source.build_root_path),
            "native_runtime": str(resolved_profile_source.native_runtime_store_path),
            "target_support": str(resolved_profile_source.target_support_store_path),
            "baseline": str(baseline_path),
        },
    }
    if resolved_profile_source.source_kind == PROFILE_SOURCE_KIND_BRIDGE_PREBUILT:
        # English note:
        # - assembly_profile_dir is a derived cache owned by apply_layout rather than user data.
        # - older schema revisions may have materialized real directories here instead of symlinks.
        # - remove those stale derived entries first so the current authority graph can be reapplied
        #   idempotently without manual cleanup between schema changes.
        _materialize_bridge_prebuilt_workspace_seed(
            source_root=spec.project_root,
            target_root=layout.assembly_profile_dir / "workspace_seed",
        )
        for dir_name in spec.profile_layout.native_runtime_dir_names:
            _remove_stale_derived_entry(path=layout.assembly_profile_native_dir / dir_name)
        for dir_name in spec.profile_layout.target_support_dir_names:
            _remove_stale_derived_entry(path=layout.assembly_profile_target_support_dir / dir_name)
        _remove_stale_derived_entry(path=layout.assembly_profile_baseline_link)
        layout.assembly_profile_manifest_path.write_text(
            json.dumps(profile_manifest, indent=2) + "\n",
            encoding="utf-8",
        )
        for dir_name in spec.profile_layout.native_runtime_dir_names:
            _replace_symlink(
                link_path=layout.assembly_profile_native_dir / dir_name,
                target_path=str(resolved_profile_source.native_runtime_path_by_name[dir_name]),
            )
        for dir_name in spec.profile_layout.target_support_dir_names:
            _replace_symlink(
                link_path=layout.assembly_profile_target_support_dir / dir_name,
                target_path=str(resolved_profile_source.target_support_path_by_name[dir_name]),
            )
        _replace_symlink(
            link_path=layout.assembly_profile_baseline_link,
            target_path=str(baseline_path),
        )

    _replace_symlink(link_path=layout.app_link, target_path=str(spec.project_root))
    _replace_symlink(link_path=layout.native_link, target_path="profile/native")
    _replace_symlink(link_path=layout.target_support_link, target_path="profile/target_support")
    _replace_symlink(
        link_path=layout.ext_link,
        target_path=f"profile/native/{spec.profile_layout.ext_bundle_dir_name}",
    )
    _replace_symlink(link_path=layout.baseline_link, target_path=str(baseline_path))
    if resolved_profile_source.source_kind == PROFILE_SOURCE_KIND_BRIDGE_PREBUILT:
        _replace_symlink(
            link_path=layout.profile_link,
            target_path=os.path.join("..", "assemblies", runtime_target.assembly_name, "profile"),
        )
        return
    _replace_symlink(link_path=layout.profile_link, target_path=str(resolved_profile_source.profile_path))


def render_layout_summary(*, spec: ExperimentSpec, runtime_target: RuntimeTarget, layout: LayoutPaths) -> str:
    lines = [
        f"config_path={spec.config_path}",
        f"project_root={spec.project_root}",
        f"project_scope_id={layout.project_scope_id}",
        f"execution_substrate={runtime_target.execution_substrate}",
        f"base_system={spec.base_system}",
        f"architecture={runtime_target.architecture}",
        f"python_abi={runtime_target.python_abi}",
        f"base_system_key={runtime_target.base_system_key}",
        f"runtime_abi_key={runtime_target.runtime_abi_key}",
        f"project_data_root={spec.project_data_root}",
        f"profile_source_kind={spec.profile_source.source_kind}",
        f"assembly_dir={layout.assembly_dir}",
        f"assembly_profile_dir={layout.assembly_profile_dir}",
        f"profile_link={layout.profile_link}",
        f"instance_dir={layout.instance_dir}",
        f"instance_release_dir={layout.instance_release_dir}",
        f"instance_target_caches_dir={layout.instance_target_caches_dir}",
    ]
    return "\n".join(lines)


def compute_project_scope_id(*, project_root: Path) -> str:
    real_project_root = os.path.realpath(project_root)
    return hashlib.sha256(real_project_root.encode("utf-8")).hexdigest()


def _derive_execution_substrate(*, base_system: str) -> str:
    if base_system.startswith("manylinux_"):
        return MANYLINUX_EXECUTION_SUBSTRATE
    raise RuntimeError(f"unsupported execution substrate mapping for base_system={base_system}")


def _derive_base_system_key(*, base_system: str, architecture: str) -> str:
    return f"{base_system}_{architecture}"


def _derive_runtime_abi_key(*, base_system: str, architecture: str, python_abi: str) -> str:
    return f"{base_system}_{architecture}_{python_abi}"


def _detect_project_root(*, config_path: Path) -> Path:
    config_dir = config_path.resolve().parent
    for candidate_root in (config_dir, *config_dir.parents):
        if not _is_project_root_candidate(candidate_root):
            continue
        return candidate_root.resolve()
    raise RuntimeError(
        "failed to detect project root from config path; expected a parent directory "
        "containing one of "
        f"{PROJECT_ROOT_MARKER_FILE_NAMES + PROJECT_ROOT_MARKER_DIR_NAMES} "
        f"and child dirs {PROJECT_ROOT_REQUIRED_CHILD_DIR_NAMES}: {config_path}"
    )


def _is_project_root_candidate(candidate_root: Path) -> bool:
    has_marker = any(
        (candidate_root / marker_name).exists()
        for marker_name in PROJECT_ROOT_MARKER_FILE_NAMES + PROJECT_ROOT_MARKER_DIR_NAMES
    )
    if not has_marker:
        return False
    return all(
        (candidate_root / child_dir_name).is_dir()
        for child_dir_name in PROJECT_ROOT_REQUIRED_CHILD_DIR_NAMES
    )


def resolve_bridge_prebuilt_build_root(*, spec: ExperimentSpec) -> Path:
    if spec.profile_source.source_kind != PROFILE_SOURCE_KIND_BRIDGE_PREBUILT:
        raise RuntimeError(
            "bridge_prebuilt build root is only defined when "
            "profile.source_kind=bridge_prebuilt"
        )
    if spec.profile_source.build_root_path is None:
        return spec.project_root.resolve()
    return _resolve_existing_dir_path(
        spec.profile_source.build_root_path,
        "profile.build_root_path",
    )


def _resolve_profile_source(*, spec: ExperimentSpec, layout: LayoutPaths) -> ResolvedProfileSource:
    if spec.profile_source.source_kind == PROFILE_SOURCE_KIND_BRIDGE_PREBUILT:
        if spec.profile_source.profile_path is not None:
            raise RuntimeError(
                "profile.profile_path must be omitted when profile.source_kind=bridge_prebuilt"
            )
        build_root_path = resolve_bridge_prebuilt_build_root(spec=spec)
        fluxon_rs_root = build_root_path / "fluxon_rs"
        if not fluxon_rs_root.is_dir():
            raise RuntimeError(
                "bridge_prebuilt build root must contain fluxon_rs: "
                f"{fluxon_rs_root}"
            )
        # target/ is a writable build output root and may be absent in a clean checkout.
        native_runtime_store_path = fluxon_rs_root / "target"
        native_runtime_store_path.mkdir(parents=True, exist_ok=True)
        native_store_root = native_runtime_store_path
        target_support_store_root = native_runtime_store_path
        profile_native_dir = native_runtime_store_path / "native"
        profile_target_support_dir = native_runtime_store_path / "target_support"
        if profile_native_dir.is_dir() and profile_target_support_dir.is_dir():
            native_store_root = profile_native_dir
            target_support_store_root = profile_target_support_dir
        native_runtime_path_by_name = _validate_native_runtime_store(
            native_runtime_store_path=native_store_root,
            field_name="profile.native_runtime_store_path",
            required_dir_names=spec.profile_layout.native_runtime_dir_names,
        )
        target_support_path_by_name: dict[str, Path] = {}
        for dir_name in spec.profile_layout.target_support_dir_names:
            support_dir_path = target_support_store_root / dir_name
            if not support_dir_path.is_dir():
                continue
            target_support_path_by_name[dir_name] = support_dir_path.resolve()
        return ResolvedProfileSource(
            source_kind=spec.profile_source.source_kind,
            profile_path=layout.assembly_profile_dir,
            build_root_path=build_root_path,
            native_runtime_store_path=native_store_root,
            target_support_store_path=target_support_store_root,
            ext_bundle_path=native_runtime_path_by_name[spec.profile_layout.ext_bundle_dir_name],
            native_runtime_path_by_name=native_runtime_path_by_name,
            target_support_path_by_name=target_support_path_by_name,
        )

    if spec.profile_source.build_root_path is not None:
        raise RuntimeError(
            "profile.build_root_path must be omitted when profile.source_kind=nix_profile"
        )
    if spec.profile_source.profile_path is None:
        raise RuntimeError(
            "profile.profile_path is required when profile.source_kind=nix_profile"
        )

    profile_root_path = _resolve_existing_dir_path(
        spec.profile_source.profile_path,
        "profile.profile_path",
    )
    profile_path = _resolve_effective_profile_layout_dir(
        profile_root_path=profile_root_path,
        field_name="profile.profile_path",
    )
    native_runtime_store_path = _resolve_existing_dir_path(
        str(profile_path / "native"),
        "profile.profile_path/native",
    )
    native_runtime_path_by_name = _validate_native_runtime_store(
        native_runtime_store_path=native_runtime_store_path,
        field_name="profile.profile_path/native",
        required_dir_names=spec.profile_layout.native_runtime_dir_names,
    )
    if spec.profile_layout.target_support_dir_names:
        target_support_store_path = _resolve_existing_dir_path(
            str(profile_path / "target_support"),
            "profile.profile_path/target_support",
        )
    else:
        target_support_candidate = profile_path / "target_support"
        target_support_store_path = (
            target_support_candidate.resolve()
            if target_support_candidate.is_dir()
            else profile_path.resolve()
        )
    target_support_path_by_name = _validate_target_support_store(
        target_store_path=target_support_store_path,
        field_name="profile.profile_path/target_support",
        required_dir_names=spec.profile_layout.target_support_dir_names,
    )
    return ResolvedProfileSource(
        source_kind=spec.profile_source.source_kind,
        profile_path=profile_path,
        build_root_path=profile_path,
        native_runtime_store_path=native_runtime_store_path,
        target_support_store_path=target_support_store_path,
        ext_bundle_path=native_runtime_path_by_name[spec.profile_layout.ext_bundle_dir_name],
        native_runtime_path_by_name=native_runtime_path_by_name,
        target_support_path_by_name=target_support_path_by_name,
    )


def _resolve_effective_profile_layout_dir(*, profile_root_path: Path, field_name: str) -> Path:
    direct_layout_markers = (profile_root_path / "native",)
    if all(marker.is_dir() for marker in direct_layout_markers):
        return profile_root_path.resolve()

    nested_profile_dir = profile_root_path / "profile"
    nested_layout_markers = (nested_profile_dir / "native",)
    if all(marker.is_dir() for marker in nested_layout_markers):
        return nested_profile_dir.resolve()

    raise RuntimeError(
        f"{field_name} must point to a profile layout root or a Nix output root with profile/: "
        f"{profile_root_path}"
    )


def _validate_native_runtime_store(
    *,
    native_runtime_store_path: Path,
    field_name: str,
    required_dir_names: tuple[str, ...],
) -> dict[str, Path]:
    native_runtime_path_by_name: dict[str, Path] = {}
    for dir_name in required_dir_names:
        native_dir_path = native_runtime_store_path / dir_name
        if not native_dir_path.is_dir():
            raise RuntimeError(
                f"{field_name} must contain the required profile authority dir: {native_dir_path}"
            )
        native_runtime_path_by_name[dir_name] = native_dir_path.resolve()
    return native_runtime_path_by_name


def _validate_target_support_store(
    *,
    target_store_path: Path,
    field_name: str,
    required_dir_names: tuple[str, ...],
) -> dict[str, Path]:
    target_support_path_by_name: dict[str, Path] = {}
    for dir_name in required_dir_names:
        support_dir_path = target_store_path / dir_name
        if not support_dir_path.is_dir():
            raise RuntimeError(
                f"{field_name} must contain the required profile target support dir: {support_dir_path}"
        )
        target_support_path_by_name[dir_name] = support_dir_path.resolve()
    return target_support_path_by_name


def _materialize_bridge_prebuilt_workspace_seed(*, source_root: Path, target_root: Path) -> None:
    _remove_stale_derived_entry(path=target_root)
    target_root.mkdir(parents=True, exist_ok=True)
    target_root.chmod(0o777)
    for relative_path in BRIDGE_PREBUILT_WORKSPACE_SEED_RELATIVE_PATHS:
        source_path = source_root / relative_path
        if not source_path.exists():
            raise RuntimeError(
                "bridge_prebuilt workspace seed source path is missing: "
                f"{source_path}"
            )
        _copy_public_workspace_input_path(source_path, target_root / relative_path)
    _sanitize_public_workspace_input(workspace_root=target_root)


def _replace_symlink(*, link_path: Path, target_path: str) -> None:
    if link_path.is_symlink():
        link_path.unlink()
    elif link_path.exists():
        raise RuntimeError(f"refusing to overwrite non-symlink path: {link_path}")
    os.symlink(target_path, link_path)


def _remove_stale_derived_entry(*, path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        shutil.rmtree(path)


def _require_mapping(raw_config: dict, key: str) -> dict:
    value = raw_config.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(f"config.{key} must be a mapping")
    return value


def _load_yaml_mapping_file(path: Path) -> dict:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"experiment config must be a mapping: {path}")
    return raw


def _deep_merge_mappings(*, base: dict, overlay: dict) -> dict:
    merged = dict(base)
    for key, overlay_value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            merged[key] = _deep_merge_mappings(base=base_value, overlay=overlay_value)
            continue
        merged[key] = overlay_value
    return merged


def _require_non_empty_string(raw_config: dict, key: str) -> str:
    value = raw_config.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f"config field must be a string: {key}")
    if not value.strip():
        raise RuntimeError(f"config field must be non-empty: {key}")
    return value.strip()


def _optional_non_empty_string(raw_config: dict, key: str) -> str | None:
    value = raw_config.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"config field must be a string when present: {key}")
    if not value.strip():
        raise RuntimeError(f"config field must be non-empty when present: {key}")
    return value.strip()


def _require_enum_string(raw_config: dict, key: str, allowed_values: tuple[str, ...]) -> str:
    value = _require_non_empty_string(raw_config, key)
    if value not in allowed_values:
        raise RuntimeError(
            f"config field must be one of {', '.join(allowed_values)}: {key}={value}"
        )
    return value


def _require_string_enum_list(raw_config: dict, key: str, allowed_values: tuple[str, ...]) -> list[str]:
    value = raw_config.get(key)
    if not isinstance(value, list):
        raise RuntimeError(f"config field must be a list: {key}")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RuntimeError(f"config field list item must be a string: {key}")
        stripped = item.strip()
        if not stripped:
            raise RuntimeError(f"config field list item must be non-empty: {key}")
        if stripped not in allowed_values:
            raise RuntimeError(
                f"config field list item must be one of {', '.join(allowed_values)}: {key}={stripped}"
            )
        if stripped in normalized:
            raise RuntimeError(f"config field list item must be unique: {key}={stripped}")
        normalized.append(stripped)
    if not normalized:
        raise RuntimeError(f"config field list must be non-empty: {key}")
    return normalized


def _require_non_empty_string_list(raw_config: dict, key: str) -> list[str]:
    value = raw_config.get(key)
    if not isinstance(value, list):
        raise RuntimeError(f"config field must be a list: {key}")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RuntimeError(f"config field list item must be a string: {key}")
        stripped = item.strip()
        if not stripped:
            raise RuntimeError(f"config field list item must be non-empty: {key}")
        if stripped in normalized:
            raise RuntimeError(f"config field list item must be unique: {key}={stripped}")
        normalized.append(stripped)
    if not normalized:
        raise RuntimeError(f"config field list must be non-empty: {key}")
    return normalized


def _require_string_list(raw_config: dict, key: str) -> list[str]:
    value = raw_config.get(key)
    if not isinstance(value, list):
        raise RuntimeError(f"config field must be a list: {key}")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RuntimeError(f"config field list item must be a string: {key}")
        stripped = item.strip()
        if not stripped:
            raise RuntimeError(f"config field list item must be non-empty: {key}")
        if stripped in normalized:
            raise RuntimeError(f"config field list item must be unique: {key}={stripped}")
        normalized.append(stripped)
    return normalized


def _require_optional_absolute_path_list(raw_config: dict, key: str) -> tuple[str, ...]:
    value = raw_config.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        raise RuntimeError(f"config field must be a list when present: {key}")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise RuntimeError(f"config field list item must be a string: {key}")
        stripped = item.strip()
        if not stripped:
            raise RuntimeError(f"config field list item must be non-empty: {key}")
        path = Path(stripped)
        if not path.is_absolute():
            raise RuntimeError(f"config path must be absolute: {key}={stripped}")
        resolved = str(path.resolve())
        if resolved in normalized:
            raise RuntimeError(f"config field list item must be unique: {key}={resolved}")
        normalized.append(resolved)
    return tuple(normalized)


def _require_absolute_existing_dir(raw_config: dict, key: str) -> Path:
    path = _require_absolute_path(raw_config, key)
    if not path.exists():
        raise RuntimeError(f"required directory does not exist: {path}")
    if not path.is_dir():
        raise RuntimeError(f"required path must be a directory: {path}")
    return path.resolve()


def _require_absolute_path(raw_config: dict, key: str) -> Path:
    raw_value = _require_non_empty_string(raw_config, key)
    path = Path(raw_value)
    if not path.is_absolute():
        raise RuntimeError(f"config path must be absolute: {key}={raw_value}")
    return path.resolve()


def _resolve_existing_dir_path(raw_value: str, field_name: str) -> Path:
    path = Path(raw_value)
    if not path.is_absolute():
        raise RuntimeError(f"{field_name} must be an absolute path: {path}")
    resolved_path = path.resolve()
    if not resolved_path.exists():
        raise RuntimeError(f"{field_name} directory does not exist: {resolved_path}")
    if not resolved_path.is_dir():
        raise RuntimeError(f"{field_name} must be a directory: {resolved_path}")
    return resolved_path
