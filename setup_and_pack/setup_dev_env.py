#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve()
if str(SCRIPT_PATH.parent) not in sys.path:
    sys.path.insert(0, str(SCRIPT_PATH.parent))

import utils as script_utils


REPO_ROOT = SCRIPT_PATH.parent.parent
CONFIG_DIR = SCRIPT_PATH.parent / "setup_dev_env"
SUPPORTED_DEFAULT_CONFIGS_BY_HOST = {
    ("ubuntu", "22.04"): CONFIG_DIR / "ubuntu22.yaml",
    ("ubuntu", "24.04"): CONFIG_DIR / "ubuntu24.yaml",
}
DEFAULT_NIX_PACK_CONFIG_PATH = REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_static.yaml"
NIX_DIR = REPO_ROOT / "setup_and_pack" / "nix"
NIX_LAYOUT_MODULE_PATH = NIX_DIR / "lib_layout.py"
RUST_TOOLCHAIN_TOML = REPO_ROOT / "fluxon_rs" / "rust-toolchain.toml"
RUST_TOOLCHAIN_PLACEHOLDER = "__FLUXON_RUST_TOOLCHAIN__"


def main() -> int:
    args = _parse_args()
    workdir = _resolve_workdir(args.workdir)
    host_info = _detect_host_os()
    _ensure_yaml_available(host_info=host_info, dry_run=args.dry_run)

    import yaml

    config_path = _resolve_config_path(
        workdir=workdir,
        raw_config=args.config,
        host_info=host_info,
    )
    with script_utils.stage("Assembling setup config"):
        config = _load_effective_config(config_path)
        _replace_rust_toolchain_placeholder(config)
        _validate_config(config=config, config_path=config_path, host_info=host_info)
        generated_config_path = config_path.with_name(f"{config_path.stem}.generated.yaml")
        generated_config_path.write_text(
            yaml.safe_dump(config, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        print(f"Generated config: {generated_config_path}")

    command_env = _build_command_env()
    with script_utils.stage("Installing apt packages"):
        _run_apt_install(config=config, dry_run=args.dry_run)
    with script_utils.stage("Installing pip packages"):
        _run_pip_install(config=config, dry_run=args.dry_run, env=command_env)
    with script_utils.stage("Running setup commands"):
        _run_command_steps(config=config, workdir=workdir, dry_run=args.dry_run, env=command_env)
    with script_utils.stage("Preparing nix pack host environment"):
        _prepare_nix_pack_host_environment(config=config, dry_run=args.dry_run)
    with script_utils.stage("Verifying nix pack host commands"):
        _verify_nix_pack_host_commands(config=config, dry_run=args.dry_run)

    print('Setup finished. Open a new shell or run: source "$HOME/.cargo/env"')
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap the Fluxon host development environment")
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help=(
            "Repo-relative or absolute setup config path. "
            "Default: infer from /etc/os-release."
        ),
    )
    parser.add_argument(
        "-w",
        "--workdir",
        default=None,
        help="Repo root path. Default: infer from this script location.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without executing them.",
    )
    return parser.parse_args()


def _resolve_workdir(raw_workdir: str | None) -> Path:
    if raw_workdir is None:
        return REPO_ROOT
    return Path(raw_workdir).expanduser().resolve()


def _resolve_config_path(
    *,
    workdir: Path,
    raw_config: str | None,
    host_info: dict[str, str],
) -> Path:
    if raw_config is None:
        host_key = (host_info.get("ID", ""), host_info.get("VERSION_ID", ""))
        config_path = SUPPORTED_DEFAULT_CONFIGS_BY_HOST.get(host_key)
        if config_path is None:
            print(
                "No default setup_dev_env config for host: "
                f"id={host_key[0]!r} version_id={host_key[1]!r}. "
                "Pass --config explicitly."
            )
            raise SystemExit(1)
        return config_path
    candidate = Path(raw_config).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    return (workdir / candidate).resolve()


def _detect_host_os() -> dict[str, str]:
    os_release_path = Path("/etc/os-release")
    if not os_release_path.exists():
        print(f"Missing host authority file: {os_release_path}")
        raise SystemExit(1)

    values: dict[str, str] = {}
    for raw_line in os_release_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or "=" not in line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"')
    return values


def _ensure_yaml_available(*, host_info: dict[str, str], dry_run: bool) -> None:
    if importlib.util.find_spec("yaml") is not None:
        return

    os_id = host_info.get("ID")
    version_id = host_info.get("VERSION_ID")
    if (os_id, version_id) not in SUPPORTED_DEFAULT_CONFIGS_BY_HOST:
        print(
            "Missing Python yaml module and automatic bootstrap only supports "
            "Ubuntu 22.04 / 24.04. "
            f"Detected: id={os_id!r} version_id={version_id!r}"
        )
        raise SystemExit(1)

    bootstrap_cmds = [
        _format_argv(_sudo_prefix() + ["apt-get", "update"]),
        _format_argv(
            _sudo_prefix()
            + ["apt-get", "install", "-y", "--no-install-recommends", "python3-pip", "python3-yaml"]
        ),
    ]
    if dry_run:
        print("Missing Python yaml module. Dry-run stops before bootstrap prerequisites:")
        for cmd in bootstrap_cmds:
            print(f"$ {cmd}")
        raise SystemExit(1)

    print("Bootstrapping python3-yaml so setup config can be parsed.")
    _run_argv(_sudo_prefix() + ["apt-get", "update"], dry_run=False)
    _run_argv(
        _sudo_prefix()
        + ["apt-get", "install", "-y", "--no-install-recommends", "python3-pip", "python3-yaml"],
        dry_run=False,
    )
    if importlib.util.find_spec("yaml") is None:
        print("python3-yaml bootstrap finished but the yaml module is still unavailable.")
        raise SystemExit(1)


def _load_effective_config(config_path: Path) -> dict[str, Any]:
    config = _load_yaml_file(config_path)
    base_config_name = config.pop("base_config", None)
    if base_config_name is None:
        return config
    if not isinstance(base_config_name, str) or not base_config_name:
        print(f"base_config must be a non-empty string: {config_path}")
        raise SystemExit(1)
    base_config_path = (config_path.parent / base_config_name).resolve()
    if not base_config_path.exists():
        print(f"Missing base_config for setup_dev_env: {base_config_path}")
        raise SystemExit(1)
    base_config = _load_effective_config(base_config_path)
    return _merge_config(base_config, config)


def _load_yaml_file(path: Path) -> dict[str, Any]:
    import yaml

    if not path.exists():
        print(f"Missing setup config: {path}")
        raise SystemExit(1)
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        print(f"Top-level setup config must be a mapping: {path}")
        raise SystemExit(1)
    return data


def _merge_config(base: Any, overlay: Any) -> Any:
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = dict(base)
        for key, overlay_value in overlay.items():
            if key in merged:
                merged[key] = _merge_config(merged[key], overlay_value)
            else:
                merged[key] = overlay_value
        return merged
    if isinstance(base, list) and isinstance(overlay, list):
        return [*base, *overlay]
    return overlay


def _replace_rust_toolchain_placeholder(config: dict[str, Any]) -> None:
    channel = _read_rust_toolchain_channel()
    replaced = _replace_placeholder_recursive(config, placeholder=RUST_TOOLCHAIN_PLACEHOLDER, value=channel)
    if not replaced:
        print(f"Missing Rust toolchain placeholder in setup config: {RUST_TOOLCHAIN_PLACEHOLDER}")
        raise SystemExit(1)


def _read_rust_toolchain_channel() -> str:
    if not RUST_TOOLCHAIN_TOML.exists():
        print(f"Missing Rust toolchain authority file: {RUST_TOOLCHAIN_TOML}")
        raise SystemExit(1)

    in_toolchain_section = False
    for raw_line in RUST_TOOLCHAIN_TOML.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_toolchain_section = line == "[toolchain]"
            continue
        if not in_toolchain_section or not line.startswith("channel"):
            continue
        prefix, sep, suffix = line.partition("=")
        if prefix.strip() != "channel" or not sep:
            continue
        value = suffix.split("#", 1)[0].strip().strip('"')
        if value:
            return value

    print(f"Failed to parse Rust toolchain channel from: {RUST_TOOLCHAIN_TOML}")
    raise SystemExit(1)


def _replace_placeholder_recursive(node: Any, *, placeholder: str, value: str) -> bool:
    replaced = False
    if isinstance(node, dict):
        for key, child in node.items():
            if isinstance(child, str) and placeholder in child:
                node[key] = child.replace(placeholder, value)
                replaced = True
                continue
            replaced = _replace_placeholder_recursive(child, placeholder=placeholder, value=value) or replaced
        return replaced
    if isinstance(node, list):
        for index, child in enumerate(node):
            if isinstance(child, str) and placeholder in child:
                node[index] = child.replace(placeholder, value)
                replaced = True
                continue
            replaced = _replace_placeholder_recursive(child, placeholder=placeholder, value=value) or replaced
    return replaced


def _validate_config(*, config: dict[str, Any], config_path: Path, host_info: dict[str, str]) -> None:
    schema_version = config.get("schema_version")
    if schema_version != 1:
        print(f"setup_dev_env schema_version must be 1: {config_path}")
        raise SystemExit(1)

    host_cfg = config.get("host")
    if host_cfg is not None:
        if not isinstance(host_cfg, dict):
            print(f"host must be a mapping: {config_path}")
            raise SystemExit(1)
        expected_id = host_cfg.get("os_id")
        expected_version = host_cfg.get("version_id")
        actual_id = host_info.get("ID")
        actual_version = host_info.get("VERSION_ID")
        if expected_id != actual_id or expected_version != actual_version:
            print(
                "Host mismatch for setup_dev_env: "
                f"expected id={expected_id!r} version_id={expected_version!r}, "
                f"actual id={actual_id!r} version_id={actual_version!r}"
            )
            raise SystemExit(1)

    _validate_package_section(section_name="apt", section=config.get("apt"))
    _validate_package_section(section_name="pip", section=config.get("pip"))
    _validate_nix_pack_section(section=config.get("nix_pack"), config_path=config_path)

    commands = config.get("commands", [])
    if not isinstance(commands, list):
        print(f"commands must be a list: {config_path}")
        raise SystemExit(1)
    for index, step in enumerate(commands):
        if not isinstance(step, dict):
            print(f"commands[{index}] must be a mapping: {config_path}")
            raise SystemExit(1)
        if not isinstance(step.get("name"), str) or not step["name"]:
            print(f"commands[{index}].name must be a non-empty string: {config_path}")
            raise SystemExit(1)
        if not isinstance(step.get("shell"), str) or not step["shell"].strip():
            print(f"commands[{index}].shell must be a non-empty string: {config_path}")
            raise SystemExit(1)
        cwd = step.get("cwd")
        if cwd is not None and (not isinstance(cwd, str) or not cwd):
            print(f"commands[{index}].cwd must be a non-empty string when present: {config_path}")
            raise SystemExit(1)


def _validate_nix_pack_section(*, section: Any, config_path: Path) -> None:
    if section is None:
        return
    if not isinstance(section, dict):
        print(f"nix_pack must be a mapping: {config_path}")
        raise SystemExit(1)

    string_fields = ("config_path", "store_root")
    for field_name in string_fields:
        value = section.get(field_name)
        if value is not None and (not isinstance(value, str) or not value):
            print(f"nix_pack.{field_name} must be a non-empty string when present: {config_path}")
            raise SystemExit(1)

    bool_fields = (
        "require_store_root_mountpoint",
        "prepare_project_data_root",
        "prepare_assembly_baseline_parent",
        "prepare_manylinux_cache_dirs",
    )
    for field_name in bool_fields:
        value = section.get(field_name)
        if value is not None and not isinstance(value, bool):
            print(f"nix_pack.{field_name} must be a boolean when present: {config_path}")
            raise SystemExit(1)

    required_commands = section.get("required_commands", [])
    if not isinstance(required_commands, list):
        print(f"nix_pack.required_commands must be a list: {config_path}")
        raise SystemExit(1)
    for index, command_name in enumerate(required_commands):
        if not isinstance(command_name, str) or not command_name:
            print(
                "nix_pack.required_commands"
                f"[{index}] must be a non-empty string: {config_path}"
            )
            raise SystemExit(1)


def _validate_package_section(*, section_name: str, section: Any) -> None:
    if section is None:
        return
    if not isinstance(section, dict):
        print(f"{section_name} must be a mapping")
        raise SystemExit(1)
    packages = section.get("packages", [])
    if not isinstance(packages, list):
        print(f"{section_name}.packages must be a list")
        raise SystemExit(1)
    for index, package_name in enumerate(packages):
        if not isinstance(package_name, str) or not package_name:
            print(f"{section_name}.packages[{index}] must be a non-empty string")
            raise SystemExit(1)


def _prepare_nix_pack_host_environment(*, config: dict[str, Any], dry_run: bool) -> None:
    nix_pack_cfg = config.get("nix_pack")
    if nix_pack_cfg is None:
        print("No nix_pack section declared.")
        return

    config_path = _resolve_repo_path(
        nix_pack_cfg.get("config_path", str(DEFAULT_NIX_PACK_CONFIG_PATH))
    )
    store_root = _resolve_absolute_path(nix_pack_cfg.get("store_root", "/nix"))
    _check_nix_store_root(
        store_root=store_root,
        require_mountpoint=bool(nix_pack_cfg.get("require_store_root_mountpoint", True)),
    )

    try:
        lib_layout = _load_nix_layout_module()
        spec = lib_layout.load_experiment_spec(config_path=config_path)
        runtime_targets = lib_layout.build_runtime_targets(spec=spec)
        if not runtime_targets:
            raise RuntimeError(f"nix pack config produced no runtime targets: {config_path}")
        print(f"nix_pack_config={config_path}")
        print(f"nix_project_data_root={spec.project_data_root}")
        print(
            "nix_runtime_targets="
            + ",".join(runtime_target.runtime_abi_key for runtime_target in runtime_targets)
        )
        for runtime_target in runtime_targets:
            layout = lib_layout.build_layout(spec=spec, runtime_target=runtime_target)
            print(
                "nix_layout="
                f"{runtime_target.runtime_abi_key}:"
                f"instance={layout.instance_dir}:release={layout.instance_release_dir}"
            )
    except Exception as exc:
        print(f"Failed to load nix pack authority from {config_path}: {exc}")
        raise SystemExit(1) from exc

    paths_to_prepare: list[Path] = []
    if nix_pack_cfg.get("prepare_project_data_root", True):
        paths_to_prepare.append(spec.project_data_root)
    if nix_pack_cfg.get("prepare_assembly_baseline_parent", True):
        paths_to_prepare.append(Path(spec.assembly_refs.baseline_path).expanduser().parent)
    if nix_pack_cfg.get("prepare_manylinux_cache_dirs", True):
        pack_root = lib_layout.load_experiment_config_root(config_path=config_path)
        manylinux_cfg = pack_root.get("manylinux", {})
        if not isinstance(manylinux_cfg, dict):
            print(f"manylinux must be a mapping in nix pack config: {config_path}")
            raise SystemExit(1)
        for field_name in ("cargo_registry_dir", "cargo_git_dir"):
            raw_path = manylinux_cfg.get(field_name)
            if raw_path is None:
                continue
            if not isinstance(raw_path, str) or not raw_path:
                print(f"manylinux.{field_name} must be a non-empty string: {config_path}")
                raise SystemExit(1)
            paths_to_prepare.append(Path(raw_path).expanduser())

    _ensure_dirs(paths_to_prepare, dry_run=dry_run)


def _verify_nix_pack_host_commands(*, config: dict[str, Any], dry_run: bool) -> None:
    nix_pack_cfg = config.get("nix_pack")
    if nix_pack_cfg is None:
        print("No nix_pack section declared.")
        return
    _check_required_commands(nix_pack_cfg.get("required_commands", []), dry_run=dry_run)


def _resolve_repo_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _resolve_absolute_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        print(f"Expected absolute path: {raw_path}")
        raise SystemExit(1)
    return path.resolve()


def _check_nix_store_root(*, store_root: Path, require_mountpoint: bool) -> None:
    if not store_root.is_dir():
        print(f"Missing nix store root: {store_root}")
        raise SystemExit(1)
    if require_mountpoint and not _is_mountpoint(store_root):
        print(f"nix store root must already be a mountpoint: {store_root}")
        raise SystemExit(1)
    print(f"nix_store_root={store_root}")


def _is_mountpoint(path: Path) -> bool:
    completed = subprocess.run(
        ["mountpoint", "-q", str(path)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return completed.returncode == 0


def _check_required_commands(command_names: list[str], *, dry_run: bool) -> None:
    for command_name in command_names:
        command_path = shutil.which(command_name)
        if command_path is None:
            if dry_run:
                print(f"would_require_command={command_name}")
                continue
            print(f"Missing required command for nix pack host environment: {command_name}")
            raise SystemExit(1)
        print(f"required_command.{command_name}={command_path}")


def _load_nix_layout_module() -> Any:
    if not NIX_LAYOUT_MODULE_PATH.exists():
        print(f"Missing nix layout authority module: {NIX_LAYOUT_MODULE_PATH}")
        raise SystemExit(1)
    module_spec = importlib.util.spec_from_file_location(
        "_fluxon_setup_dev_env_nix_lib_layout",
        NIX_LAYOUT_MODULE_PATH,
    )
    if module_spec is None or module_spec.loader is None:
        print(f"Failed to load nix layout authority module: {NIX_LAYOUT_MODULE_PATH}")
        raise SystemExit(1)
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = module
    module_spec.loader.exec_module(module)
    return module


def _ensure_dirs(paths: list[Path], *, dry_run: bool) -> None:
    seen: set[Path] = set()
    for path in paths:
        resolved_path = path.resolve() if path.exists() else path
        if resolved_path in seen:
            continue
        seen.add(resolved_path)
        print(f"ensure_dir={resolved_path}")
        if dry_run:
            continue
        resolved_path.mkdir(parents=True, exist_ok=True)


def _build_command_env() -> dict[str, str]:
    env = os.environ.copy()
    cargo_bin = str(Path.home() / ".cargo" / "bin")
    env["PATH"] = f"{cargo_bin}:{env.get('PATH', '')}"
    return env


def _run_apt_install(*, config: dict[str, Any], dry_run: bool) -> None:
    apt_cfg = config.get("apt")
    if apt_cfg is None:
        print("No apt section declared.")
        return

    packages = apt_cfg.get("packages", [])
    if not packages:
        print("No apt packages declared.")
        return

    _run_argv(_sudo_prefix() + ["apt-get", "update"], dry_run=dry_run)
    _run_argv(
        _sudo_prefix() + ["apt-get", "install", "-y", "--no-install-recommends", *packages],
        dry_run=dry_run,
    )


def _run_pip_install(*, config: dict[str, Any], dry_run: bool, env: dict[str, str]) -> None:
    pip_cfg = config.get("pip")
    if pip_cfg is None:
        print("No pip section declared.")
        return

    packages = pip_cfg.get("packages", [])
    if not packages:
        print("No pip packages declared.")
        return

    _run_argv(
        [sys.executable, "-m", "pip", "install", "--upgrade", *packages],
        dry_run=dry_run,
        env=env,
    )


def _run_command_steps(
    *,
    config: dict[str, Any],
    workdir: Path,
    dry_run: bool,
    env: dict[str, str],
) -> None:
    commands = config.get("commands", [])
    if not commands:
        print("No setup commands declared.")
        return

    for step in commands:
        step_name = step["name"]
        step_cwd = workdir if "cwd" not in step else (workdir / step["cwd"]).resolve()
        with script_utils.stage(f"Command step: {step_name}"):
            _run_shell(step["shell"], cwd=step_cwd, dry_run=dry_run, env=env)


def _run_argv(
    argv: list[str],
    *,
    dry_run: bool,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> None:
    printable = _format_argv(argv)
    if cwd is not None:
        print(f"(cwd: {cwd})")
    print(f"$ {printable}")
    if dry_run:
        return
    subprocess.check_call(argv, cwd=str(cwd) if cwd is not None else None, env=env)


def _run_shell(
    command: str,
    *,
    dry_run: bool,
    cwd: Path,
    env: dict[str, str],
) -> None:
    print(f"(cwd: {cwd})")
    print("$ bash -lc <<'EOF'")
    print(command.rstrip())
    print("EOF")
    if dry_run:
        return
    subprocess.check_call(["bash", "-lc", command], cwd=str(cwd), env=env)


def _sudo_prefix() -> list[str]:
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        return []
    return ["sudo", "-E"]


def _format_argv(argv: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in argv)


if __name__ == "__main__":
    raise SystemExit(main())
