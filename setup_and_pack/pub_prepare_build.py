#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
YAML_PATH = SCRIPT_DIR / "pub_prepare_build.yaml"
BINARY_PATH_OUTPUT_PREFIX = "PREPARE_BUILD_BINARY_PATH="
PREPARE_BUILD_VENDOR_RUNTIME_BRIDGE_RELATIVE_PATH = (
    "fluxon_release/generated/manylinux_2_28/bridge_prebuilt_external_mounts"
)
REQUIRED_VENDOR_RUNTIME_LIB_PREFIXES = (
    "libfabric.so",
    "libibverbs.so",
    "libmlx5.so",
    "libmlx5-rdmav",
)
VENDOR_RUNTIME_OPTIONAL_LIB_PREFIXES = (
    "librdmacm.so",
    "libefa.so",
    "libnl-3.so",
    "libnl-route-3.so",
)
PROTOC_RUNTIME_LIB_PREFIXES = (
    "libprotoc.so",
    "libprotobuf.so",
    "libz.so",
    "libstdc++.so",
    "libgcc_s.so",
)
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare public manylinux build resources for a named scenario"
    )
    parser.add_argument("--scenario", required=True, help="Scenario name declared in PREPARE_SCENARIOS")
    parser.add_argument(
        "--print-binary-path",
        default=None,
        help="Print the absolute path of a binary exposed by the scenario binary target",
    )
    parser.add_argument(
        "--print-cache-steps-json",
        action="store_true",
        help="Print the cache-step records for the selected scenario as JSON and exit",
    )
    parser.add_argument(
        "--print-target-dir-names-json",
        action="store_true",
        help="Print the prepared target dir names for the selected scenario as JSON and exit",
    )
    return parser.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def strip_inline_comment(raw_line: str) -> str:
    in_single_quote = False
    in_double_quote = False
    chars: list[str] = []
    for ch in raw_line:
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
        elif ch == "#" and not in_single_quote and not in_double_quote:
            break
        chars.append(ch)
    return "".join(chars).rstrip()


def parse_scalar(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def split_mapping_line(stripped: str) -> tuple[str, str]:
    key, value = stripped.split(":", 1)
    return key.strip(), parse_scalar(value.strip())


def parse_prepare_targets_line(
    targets: dict[str, dict],
    current_name: str | None,
    current_list_name: str | None,
    indent: int,
    stripped: str,
) -> tuple[str | None, str | None]:
    if indent == 2 and stripped.endswith(":"):
        target_name = stripped[:-1].strip()
        targets[target_name] = {
            "kind": "",
            "dir_name": "",
            "url": "",
            "verify": "",
            "verify_format": "",
            "binary_install": [],
        }
        return target_name, None
    if current_name is None:
        raise RuntimeError("pub_prepare_build.py: PREPARE_TARGETS entry is missing its target name header")
    if indent == 4 and stripped == "binary_install:":
        return current_name, "binary_install"
    if indent == 4 and ":" in stripped:
        key, value = split_mapping_line(stripped)
        targets[current_name][key] = value
        return current_name, None
    if indent == 6 and current_list_name == "binary_install" and stripped.startswith("- "):
        targets[current_name]["binary_install"].append(parse_scalar(stripped[2:].strip()))
        return current_name, current_list_name
    raise RuntimeError(f"pub_prepare_build.py: unsupported PREPARE_TARGETS line: {stripped}")


def parse_prepare_scenarios_line(
    scenarios: dict[str, dict],
    current_name: str | None,
    current_list_name: str | None,
    indent: int,
    stripped: str,
) -> tuple[str | None, str | None]:
    if indent == 2 and stripped.endswith(":"):
        scenario_name = stripped[:-1].strip()
        scenarios[scenario_name] = {
            "targets": [],
            "binary_target": None,
        }
        return scenario_name, None
    if current_name is None:
        raise RuntimeError("pub_prepare_build.py: PREPARE_SCENARIOS entry is missing its scenario name header")
    if indent == 4 and stripped == "targets:":
        return current_name, "targets"
    if indent == 4 and ":" in stripped:
        key, value = split_mapping_line(stripped)
        scenarios[current_name][key] = value
        return current_name, None
    if indent == 6 and current_list_name == "targets" and stripped.startswith("- "):
        scenarios[current_name]["targets"].append(parse_scalar(stripped[2:].strip()))
        return current_name, current_list_name
    raise RuntimeError(f"pub_prepare_build.py: unsupported PREPARE_SCENARIOS line: {stripped}")


def parse_cache_steps_line(
    cache_steps: dict[str, dict],
    current_name: str | None,
    current_list_name: str | None,
    indent: int,
    stripped: str,
) -> tuple[str | None, str | None]:
    if indent == 2 and stripped.endswith(":"):
        step_name = stripped[:-1].strip()
        cache_steps[step_name] = {
            "scenarios": [],
            "inputs": [],
            "outputs": [],
        }
        return step_name, None
    if current_name is None:
        raise RuntimeError("pub_prepare_build.py: CACHE_STEPS entry is missing its step name header")
    if indent == 4 and stripped in ("scenarios:", "inputs:", "outputs:"):
        return current_name, stripped[:-1]
    if indent == 6 and current_list_name in ("scenarios", "inputs", "outputs") and stripped.startswith("- "):
        cache_steps[current_name][current_list_name].append(parse_scalar(stripped[2:].strip()))
        return current_name, current_list_name
    raise RuntimeError(f"pub_prepare_build.py: unsupported CACHE_STEPS line: {stripped}")


def parse_yaml_subset(yaml_text: str) -> dict:
    cfg = {
        "prepare_targets": {},
        "prepare_scenarios": {},
        "cache_steps": {},
    }
    section = None
    current_name = None
    current_list_name = None
    for raw_line in yaml_text.splitlines():
        line = strip_inline_comment(raw_line)
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()
        if indent == 0:
            section = None
            current_name = None
            current_list_name = None
            if stripped == "PREPARE_TARGETS:":
                section = "prepare_targets"
            elif stripped == "PREPARE_SCENARIOS:":
                section = "prepare_scenarios"
            elif stripped == "CACHE_STEPS:":
                section = "cache_steps"
            continue
        if section == "prepare_targets":
            current_name, current_list_name = parse_prepare_targets_line(
                cfg["prepare_targets"], current_name, current_list_name, indent, stripped
            )
            continue
        if section == "prepare_scenarios":
            current_name, current_list_name = parse_prepare_scenarios_line(
                cfg["prepare_scenarios"], current_name, current_list_name, indent, stripped
            )
            continue
        if section == "cache_steps":
            current_name, current_list_name = parse_cache_steps_line(
                cfg["cache_steps"], current_name, current_list_name, indent, stripped
            )
    validate_prepare_config(cfg)
    return cfg


def validate_prepare_config(cfg: dict) -> None:
    targets = cfg["prepare_targets"]
    scenarios = cfg["prepare_scenarios"]
    cache_steps = cfg["cache_steps"]
    if not targets:
        raise RuntimeError("pub_prepare_build.py: PREPARE_TARGETS is empty")
    if not scenarios:
        raise RuntimeError("pub_prepare_build.py: PREPARE_SCENARIOS is empty")
    for target_name, target_cfg in targets.items():
        if not str(target_cfg.get("dir_name", "")).strip():
            raise RuntimeError(
                f"pub_prepare_build.py: PREPARE_TARGETS.{target_name}.dir_name must be set"
            )
        target_kind = str(target_cfg.get("kind", "")).strip()
        if target_kind not in ("system_resource", "remote_archive"):
            raise RuntimeError(f"pub_prepare_build.py: unsupported target kind for {target_name}: {target_kind}")
        if target_kind == "remote_archive":
            if not str(target_cfg.get("url", "")).strip():
                raise RuntimeError(f"pub_prepare_build.py: PREPARE_TARGETS.{target_name}.url must be set")
            if not str(target_cfg.get("verify", "")).strip():
                raise RuntimeError(f"pub_prepare_build.py: PREPARE_TARGETS.{target_name}.verify must be set")
            if str(target_cfg.get("verify_format", "")).strip() != "first_token_sha256":
                raise RuntimeError(
                    f"pub_prepare_build.py: PREPARE_TARGETS.{target_name}.verify_format must be first_token_sha256"
                )
    for scenario_name, scenario_cfg in scenarios.items():
        targets_list = scenario_cfg.get("targets")
        if not isinstance(targets_list, list) or not targets_list:
            raise RuntimeError(
                f"pub_prepare_build.py: PREPARE_SCENARIOS.{scenario_name}.targets must not be empty"
            )
        binary_target = scenario_cfg.get("binary_target")
        if binary_target not in targets:
            raise RuntimeError(
                f"pub_prepare_build.py: PREPARE_SCENARIOS.{scenario_name}.binary_target must reference a target"
            )
        for target_name in targets_list:
            if target_name not in targets:
                raise RuntimeError(
                    f"pub_prepare_build.py: PREPARE_SCENARIOS.{scenario_name} references unknown target {target_name}"
                )
    for step_name, step_cfg in cache_steps.items():
        for key in ("scenarios", "inputs", "outputs"):
            values = step_cfg.get(key)
            if not isinstance(values, list) or not values:
                raise RuntimeError(
                    f"pub_prepare_build.py: CACHE_STEPS.{step_name}.{key} must be a non-empty list"
                )


def resolve_target(cfg: dict, target_name: str) -> dict:
    try:
        return dict(cfg["prepare_targets"][target_name])
    except KeyError as err:
        raise RuntimeError(f"pub_prepare_build.py: unknown prepare target {target_name}") from err


def resolve_scenario(cfg: dict, scenario_name: str) -> dict:
    try:
        return dict(cfg["prepare_scenarios"][scenario_name])
    except KeyError as err:
        raise RuntimeError(f"pub_prepare_build.py: unknown prepare scenario {scenario_name}") from err


def resolve_cache_steps(cfg: dict, scenario_name: str) -> list[dict]:
    resolve_scenario(cfg, scenario_name)
    steps: list[dict] = []
    for step_name, step_cfg in cfg["cache_steps"].items():
        if scenario_name not in step_cfg["scenarios"]:
            continue
        steps.append(
            {
                "name": step_name,
                "inputs": list(step_cfg["inputs"]),
                "outputs": list(step_cfg["outputs"]),
            }
        )
    return steps


def get_arch_name() -> str:
    mach = os.uname().machine.lower()
    if mach in ("x86_64", "amd64"):
        return "x86_64"
    if mach in ("aarch64", "arm64"):
        return "aarch64"
    raise RuntimeError(f"Unsupported architecture: {mach}")


def calc_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def download_file(url: str, out_path: Path) -> None:
    subprocess.run(
        [
            "curl",
            "-L",
            "-f",
            "--connect-timeout",
            "30",
            "--max-time",
            "300",
            "--retry",
            "3",
            "--retry-delay",
            "1",
            "-o",
            str(out_path),
            url,
        ],
        check=True,
    )


def resolve_archive_urls(target_cfg: dict) -> tuple[str, str]:
    arch = get_arch_name()
    return (
        str(target_cfg["url"]).strip().replace("${arch}", arch),
        str(target_cfg["verify"]).strip().replace("${arch}", arch),
    )


def parse_verify_payload(verify_payload_text: str) -> str:
    first_token = verify_payload_text.strip().split()
    if not first_token:
        raise RuntimeError("pub_prepare_build.py: empty verify payload")
    return first_token[0]


def extract_archive(*, archive_path: Path, install_root: Path) -> None:
    install_root.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["tar", "-xzf", str(archive_path), "-C", str(install_root), "--strip-components=1"],
        check=True,
    )
    symlink_lib64_if_needed(install_root)


def populate_remote_archive_resource(target_cfg: dict, install_root: Path) -> None:
    download_url, verify_url = resolve_archive_urls(target_cfg)
    target_dir = get_target_dir()
    downloads_dir = target_dir / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    archive_path = downloads_dir / Path(download_url).name
    verify_path = downloads_dir / (Path(download_url).name + ".sha256")
    if not archive_path.is_file():
        print(f"[pub_prepare_build] downloading packed resource from {download_url}")
        download_file(download_url, archive_path)
    if not verify_path.is_file():
        print(f"[pub_prepare_build] downloading packed resource verify file from {verify_url}")
        download_file(verify_url, verify_path)
    expected_sha256 = parse_verify_payload(verify_path.read_text(encoding="utf-8"))
    actual_sha256 = calc_sha256(archive_path)
    if expected_sha256.lower() != actual_sha256.lower():
        raise RuntimeError(
            "pub_prepare_build.py: archive integrity verification failed for "
            + f"{download_url}: expected={expected_sha256} actual={actual_sha256}"
        )
    if install_root.is_symlink() or install_root.is_file():
        install_root.unlink()
    elif install_root.exists():
        shutil.rmtree(install_root)
    with tempfile.TemporaryDirectory(prefix=f"{install_root.name}_extract_") as temp_dir_str:
        temp_root = Path(temp_dir_str)
        extracted_root = temp_root / "install_root"
        print(f"[pub_prepare_build] extracting {archive_path} -> {install_root}")
        extract_archive(archive_path=archive_path, install_root=extracted_root)
        shutil.copytree(extracted_root, install_root, symlinks=True)


def resolve_prepare_project_root() -> Path:
    override = os.environ.get("FLUXON_PREPARE_BUILD_PROJECT_ROOT", "").strip()
    if override:
        return Path(override).resolve()
    return PROJECT_ROOT.resolve()


def get_target_dir() -> Path:
    raw_target_dir = os.environ.get("CARGO_TARGET_DIR", "target").strip()
    target_dir = Path(raw_target_dir)
    if target_dir.is_absolute():
        return target_dir.resolve()
    return (PROJECT_ROOT / "fluxon_rs" / target_dir).resolve()


def ensure_dirs(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def copytree_replace(src: Path, dst: Path) -> None:
    if dst.is_symlink() or dst.is_file():
        dst.unlink()
    elif dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, symlinks=True)


def symlink_lib64_if_needed(install_root: Path) -> None:
    lib_dir = install_root / "lib"
    lib64_dir = install_root / "lib64"
    if lib64_dir.exists():
        return
    if lib_dir.exists():
        lib64_dir.symlink_to("lib")


def copy_binary_from_path(binary_name: str, dst_dir: Path) -> None:
    source_path = shutil.which(binary_name)
    if source_path is None:
        raise RuntimeError(f"pub_prepare_build.py: required system binary is missing from PATH: {binary_name}")
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_path = dst_dir / binary_name
    shutil.copy2(source_path, dst_path)
    dst_path.chmod(dst_path.stat().st_mode | 0o111)


def copy_binary_runtime_dependencies(binary_path: Path, install_root: Path) -> None:
    proc = subprocess.run(
        ["ldd", str(binary_path)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    lib_dir = install_root / "lib"
    lib_dir.mkdir(parents=True, exist_ok=True)
    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if "=>" not in line:
            continue
        rhs = line.split("=>", 1)[1].strip()
        lib_path_text = rhs.split("(", 1)[0].strip()
        if not lib_path_text.startswith("/"):
            continue
        lib_path = Path(lib_path_text)
        if not lib_path.is_file():
            continue
        if not any(lib_path.name.startswith(prefix) for prefix in PROTOC_RUNTIME_LIB_PREFIXES):
            continue
        shutil.copy2(lib_path, lib_dir / lib_path.name)


def is_core_system_runtime_lib(lib_name: str) -> bool:
    if lib_name == "linux-vdso.so.1":
        return True
    if lib_name in MANYLINUX_CXX_RUNTIME_LIBRARY_NAMES:
        return True
    return any(lib_name.startswith(prefix) for prefix in CORE_SYSTEM_RUNTIME_LIB_PREFIXES)


def read_runtime_dependency_entries(
    path: Path,
    *,
    ld_library_paths: list[Path],
) -> list[tuple[str, Path]]:
    env = os.environ.copy()
    resolved_ld_library_paths = [
        str(candidate.resolve())
        for candidate in ld_library_paths
        if candidate.exists()
    ]
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
            "pub_prepare_build.py: ldd failed while reading runtime dependencies\n"
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
            possible_header = line[:-1].strip()
            if possible_header.startswith("/"):
                continue
        if "=>" in line:
            needed_name, raw_target = (part.strip() for part in line.split("=>", 1))
            if raw_target == "not found":
                raise RuntimeError(f"pub_prepare_build.py: runtime dependency not found for {path}: {needed_name}")
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
            raise RuntimeError(
                f"pub_prepare_build.py: runtime dependency path does not exist for {path}: "
                + f"{needed_name} -> {dep_path}"
            )
        resolved_path = dep_path.resolve()
        entry_key = (needed_name, str(resolved_path))
        if entry_key in seen:
            continue
        seen.add(entry_key)
        entries.append((needed_name, resolved_path))
    return entries


def read_elf_soname(path: Path) -> str | None:
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


def find_existing_directory(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.is_dir():
            return path
    return None


def find_library_file(root: Path, prefix: str) -> Path | None:
    if not root.is_dir():
        return None
    for path in sorted(root.glob(f"{prefix}*")):
        if path.is_file() or path.is_symlink():
            return path
    return None


def vendor_runtime_lib_roots(install_root: Path) -> list[Path]:
    roots: list[Path] = []
    for root in (install_root / "lib", install_root / "lib64"):
        if not root.is_dir():
            continue
        roots.append(root)
        provider_root = root / "libibverbs"
        if provider_root.is_dir():
            roots.append(provider_root)
    return roots


def vendor_runtime_dependency_probe_paths(install_root: Path) -> list[Path]:
    return [*vendor_runtime_lib_roots(install_root), *system_provider_dir_candidates()]


def collect_vendor_runtime_shared_libraries(install_root: Path) -> list[Path]:
    libs: list[Path] = []
    seen: set[str] = set()
    for lib_root in vendor_runtime_lib_roots(install_root):
        for path in sorted(lib_root.rglob("*.so*")):
            if not (path.is_file() or path.is_symlink()):
                continue
            resolved = path.resolve()
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            libs.append(resolved)
    return libs


def ensure_vendor_runtime_soname_aliases(install_root: Path) -> None:
    for lib_root in vendor_runtime_lib_roots(install_root):
        for path in sorted(lib_root.rglob("*.so*")):
            if not path.is_file():
                continue
            try:
                soname = read_elf_soname(path)
            except subprocess.CalledProcessError:
                continue
            if soname is None or soname == path.name:
                continue
            alias_path = path.parent / soname
            if alias_path.exists():
                continue
            alias_path.symlink_to(path.name)


def copy_vendor_runtime_transitive_dependencies(install_root: Path) -> None:
    lib_dst = install_root / "lib"
    lib_dst.mkdir(parents=True, exist_ok=True)
    queue = collect_vendor_runtime_shared_libraries(install_root)
    if not queue:
        return
    seen_targets: set[str] = set()
    staged_names = {path.name for path in queue}
    ld_library_paths = vendor_runtime_dependency_probe_paths(install_root)
    while queue:
        target_path = queue.pop(0)
        target_key = str(target_path.resolve())
        if target_key in seen_targets:
            continue
        seen_targets.add(target_key)
        for needed_name, resolved_path in read_runtime_dependency_entries(
            target_path,
            ld_library_paths=ld_library_paths,
        ):
            if is_core_system_runtime_lib(needed_name) or is_core_system_runtime_lib(resolved_path.name):
                continue
            if resolved_path.parent in vendor_runtime_lib_roots(install_root):
                if needed_name not in staged_names:
                    staged_names.add(needed_name)
                    queue.append(resolved_path)
                continue
            staged_path = lib_dst / resolved_path.name
            if not staged_path.exists():
                shutil.copy2(resolved_path, staged_path)
            if resolved_path.name not in staged_names:
                staged_names.add(resolved_path.name)
                queue.append(staged_path.resolve())
    ensure_vendor_runtime_soname_aliases(install_root)


def vendor_runtime_has_transitive_runtime_closure(install_root: Path) -> bool:
    probe_paths = vendor_runtime_dependency_probe_paths(install_root)
    ensure_vendor_runtime_soname_aliases(install_root)
    for lib_path in collect_vendor_runtime_shared_libraries(install_root):
        try:
            read_runtime_dependency_entries(lib_path, ld_library_paths=probe_paths)
        except RuntimeError:
            return False
    return True


def authoritative_vendor_runtime_root_candidates() -> list[Path]:
    project_root = resolve_prepare_project_root()
    bridge_root = project_root / PREPARE_BUILD_VENDOR_RUNTIME_BRIDGE_RELATIVE_PATH
    candidates: list[Path] = []
    if bridge_root.is_dir():
        for runtime_abi_dir in sorted(bridge_root.iterdir()):
            if not runtime_abi_dir.is_dir():
                continue
            candidates.append((runtime_abi_dir / "vendor_runtime").resolve())
    return candidates


def find_authoritative_vendor_runtime_root() -> Path | None:
    if os.environ.get("FLUXON_PREPARE_BUILD_SKIP_EXISTING_VENDOR_RUNTIME", "").strip() == "1":
        return None
    ready_candidates = [
        candidate
        for candidate in authoritative_vendor_runtime_root_candidates()
        if vendor_runtime_install_root_ready(candidate)
    ]
    if not ready_candidates:
        return None
    ready_candidates.sort(key=lambda path: path.stat().st_mtime_ns, reverse=True)
    return ready_candidates[0]


def system_library_roots() -> list[Path]:
    return [
        Path("/usr/lib64"),
        Path("/usr/lib"),
        Path("/lib64"),
        Path("/lib"),
        Path("/usr/lib/x86_64-linux-gnu"),
        Path("/lib/x86_64-linux-gnu"),
    ]


def system_include_root_candidates() -> list[Path]:
    return [
        Path("/usr/include"),
        Path("/usr/local/include"),
    ]


def system_driver_config_candidates() -> list[Path]:
    return [
        Path("/etc/libibverbs.d"),
        Path("/usr/etc/libibverbs.d"),
        Path("/usr/local/etc/libibverbs.d"),
    ]


def system_provider_dir_candidates() -> list[Path]:
    candidates = []
    for lib_root in system_library_roots():
        candidates.append(lib_root / "libibverbs")
        candidates.append(lib_root)
    return candidates


def copy_vendor_runtime_headers(install_root: Path) -> None:
    include_root = install_root / "include"
    rdma_dst = include_root / "rdma"
    infiniband_dst = include_root / "infiniband"
    rdma_src = find_existing_directory([path / "rdma" for path in system_include_root_candidates()])
    infiniband_src = find_existing_directory([path / "infiniband" for path in system_include_root_candidates()])
    if rdma_src is None:
        raise RuntimeError("pub_prepare_build.py: missing system RDMA headers under /usr/include/rdma")
    if infiniband_src is None:
        raise RuntimeError(
            "pub_prepare_build.py: missing system ibverbs headers under /usr/include/infiniband"
        )
    copytree_replace(rdma_src, rdma_dst)
    copytree_replace(infiniband_src, infiniband_dst)


def copy_vendor_runtime_driver_configs(install_root: Path) -> None:
    driver_src = find_existing_directory(system_driver_config_candidates())
    if driver_src is None:
        raise RuntimeError("pub_prepare_build.py: missing system libibverbs driver configs under /etc/libibverbs.d")
    driver_dst = install_root / "etc" / "libibverbs.d"
    copytree_replace(driver_src, driver_dst)


def copy_vendor_runtime_libraries(install_root: Path) -> None:
    lib_dst = install_root / "lib"
    lib_dst.mkdir(parents=True, exist_ok=True)
    for prefix in REQUIRED_VENDOR_RUNTIME_LIB_PREFIXES:
        copied = False
        for root in system_provider_dir_candidates():
            source_path = find_library_file(root, prefix)
            if source_path is None:
                continue
            shutil.copy2(source_path, lib_dst / source_path.name)
            copied = True
            break
        if not copied:
            raise RuntimeError(
                f"pub_prepare_build.py: missing required system vendor runtime library with prefix {prefix}"
            )
    for prefix in VENDOR_RUNTIME_OPTIONAL_LIB_PREFIXES:
        for root in system_library_roots():
            source_path = find_library_file(root, prefix)
            if source_path is None:
                continue
            shutil.copy2(source_path, lib_dst / source_path.name)
            break
    copy_vendor_runtime_transitive_dependencies(install_root)


def copy_cxxpacked_projection(install_root: Path) -> None:
    if install_root.is_symlink() or install_root.is_file():
        install_root.unlink()
    elif install_root.exists():
        shutil.rmtree(install_root)
    install_root.mkdir(parents=True, exist_ok=True)

    include_root = install_root / "include" / "infiniband"
    infiniband_src = find_existing_directory([path / "infiniband" for path in system_include_root_candidates()])
    if infiniband_src is None:
        raise RuntimeError(
            "pub_prepare_build.py: missing system ibverbs headers under /usr/include/infiniband"
        )
    copytree_replace(infiniband_src, include_root)

    lib64_root = install_root / "lib64"
    lib64_root.mkdir(parents=True, exist_ok=True)
    provider_root = lib64_root / "libibverbs"
    provider_root.mkdir(parents=True, exist_ok=True)

    for prefix in ("libibverbs.so", "libmlx5.so"):
        copied = False
        for root in system_library_roots():
            source_path = find_library_file(root, prefix)
            if source_path is None:
                continue
            shutil.copy2(source_path, lib64_root / source_path.name)
            copied = True
            break
        if not copied:
            raise RuntimeError(
                f"pub_prepare_build.py: missing required system cxxpacked projection library with prefix {prefix}"
            )

    copied_provider = False
    for root in system_provider_dir_candidates():
        source_path = find_library_file(root, "libmlx5-rdmav")
        if source_path is None:
            continue
        shutil.copy2(source_path, provider_root / source_path.name)
        copied_provider = True
        break
    if not copied_provider:
        raise RuntimeError(
            "pub_prepare_build.py: missing required system cxxpacked provider library with prefix libmlx5-rdmav"
        )

    copy_vendor_runtime_driver_configs(install_root)
    copy_binary_from_path("protoc", install_root / "bin")
    copy_binary_runtime_dependencies(install_root / "bin" / "protoc", install_root)
    symlink_lib64_if_needed(install_root)


def populate_authoritative_vendor_runtime_install_root(packed_dir: Path) -> None:
    authoritative_root = find_authoritative_vendor_runtime_root()
    if authoritative_root is not None:
        copytree_replace(authoritative_root, packed_dir)
        return
    if packed_dir.is_symlink() or packed_dir.is_file():
        packed_dir.unlink()
    elif packed_dir.exists():
        shutil.rmtree(packed_dir)
    packed_dir.mkdir(parents=True, exist_ok=True)
    copy_vendor_runtime_headers(packed_dir)
    copy_vendor_runtime_libraries(packed_dir)
    copy_vendor_runtime_driver_configs(packed_dir)
    symlink_lib64_if_needed(packed_dir)


def system_resource_ready(resource_url: str, packed_dir: Path) -> bool:
    if resource_url != "system://vendor_runtime":
        raise RuntimeError(f"pub_prepare_build.py: unsupported system resource {resource_url}")
    return vendor_runtime_install_root_ready(packed_dir)


def populate_system_resource(resource_url: str, packed_dir: Path) -> None:
    if resource_url != "system://vendor_runtime":
        raise RuntimeError(f"pub_prepare_build.py: unsupported system resource {resource_url}")
    populate_authoritative_vendor_runtime_install_root(packed_dir)


def vendor_runtime_install_root_ready(packed_dir: Path) -> bool:
    driver_config_dir = packed_dir / "etc" / "libibverbs.d"
    if not driver_config_dir.is_dir():
        return False
    if not any(path.is_file() and path.name.endswith(".driver") for path in driver_config_dir.iterdir()):
        return False
    lib_roots = (packed_dir / "lib", packed_dir / "lib64")
    for prefix in REQUIRED_VENDOR_RUNTIME_LIB_PREFIXES:
        found = False
        for lib_root in lib_roots:
            if not lib_root.is_dir():
                continue
            if any(
                path.is_file() or path.is_symlink()
                for path in lib_root.glob(f"{prefix}*")
            ):
                found = True
                break
            provider_root = lib_root / "libibverbs"
            if provider_root.is_dir() and any(
                path.is_file() or path.is_symlink()
                for path in provider_root.glob(f"{prefix}*")
            ):
                found = True
                break
        if not found:
            return False
    return vendor_runtime_has_transitive_runtime_closure(packed_dir)


def cxxpacked_projection_ready(packed_dir: Path) -> bool:
    required_paths = (
        packed_dir / "include" / "infiniband" / "verbs.h",
        packed_dir / "lib64" / "libibverbs.so",
        packed_dir / "lib64" / "libmlx5.so",
        packed_dir / "bin" / "protoc",
    )
    if not all(path.exists() for path in required_paths):
        return False
    provider_root = packed_dir / "lib64" / "libibverbs"
    if not provider_root.is_dir():
        return False
    if not any(
        (path.is_file() or path.is_symlink()) and path.name.startswith("libmlx5-rdmav")
        for path in provider_root.iterdir()
    ):
        return False
    driver_dir = packed_dir / "etc" / "libibverbs.d"
    if not driver_dir.is_dir():
        return False
    return any(path.is_file() and path.name.endswith(".driver") for path in driver_dir.iterdir())


def install_binaries(install_root: Path, binaries: list[str]) -> None:
    if not binaries:
        return
    if not hasattr(os, "geteuid") or os.geteuid() != 0:
        print("[pub_prepare_build] skip installing binaries to /usr/bin (not running as root)")
        return
    system_bin = Path("/usr/bin")
    for name in binaries:
        src = install_root / "bin" / name
        if not src.is_file():
            continue
        dst = system_bin / name
        shutil.copy2(src, dst)
        dst.chmod(dst.stat().st_mode | 0o111)


def ensure_meson(target_dir: Path) -> None:
    meson_root = target_dir / "meson-0.64.0"
    meson_entry = meson_root / "meson.py"
    if meson_entry.exists():
        return
    downloads_dir = target_dir / "downloads"
    downloads_dir.mkdir(parents=True, exist_ok=True)
    meson_root.mkdir(parents=True, exist_ok=True)

    tar_name = "meson-0.64.0.tar.gz"
    tar_path = downloads_dir / tar_name
    url = "https://github.com/mesonbuild/meson/releases/download/0.64.0/meson-0.64.0.tar.gz"
    print(f"[pub_prepare_build] downloading Meson from {url}")
    subprocess.run(
        [
            "curl",
            "-L",
            "-f",
            "--connect-timeout",
            "30",
            "--max-time",
            "300",
            "--retry",
            "3",
            "--retry-delay",
            "1",
            "-o",
            str(tar_path),
            url,
        ],
        check=True,
    )
    print(f"[pub_prepare_build] extracting {tar_path} -> {meson_root}")
    subprocess.run(
        ["tar", "-xzf", str(tar_path), "-C", str(meson_root), "--strip-components=1"],
        check=True,
    )


def ensure_build_tools(target_dir: Path) -> None:
    ensure_meson(target_dir)


def materialize_prepare_target(target_cfg: dict, target_dir: Path) -> Path:
    target_kind = str(target_cfg.get("kind", "")).strip()
    install_root = target_dir / str(target_cfg["dir_name"]).strip()
    dir_name = str(target_cfg["dir_name"]).strip()
    if dir_name == "vendor_runtime":
        if target_kind != "system_resource":
            raise RuntimeError("pub_prepare_build.py: vendor_runtime must be a system_resource target")
        if not system_resource_ready(str(target_cfg["url"]).strip(), install_root):
            populate_system_resource(str(target_cfg["url"]).strip(), install_root)
        if not system_resource_ready(str(target_cfg["url"]).strip(), install_root):
            raise RuntimeError(
                f"pub_prepare_build.py: prepared system resource remained incomplete: {install_root}"
            )
    elif dir_name == "cxxpacked":
        if target_kind == "remote_archive":
            if not cxxpacked_projection_ready(install_root):
                populate_remote_archive_resource(target_cfg, install_root)
        elif target_kind == "system_resource":
            if not cxxpacked_projection_ready(install_root):
                copy_cxxpacked_projection(install_root)
        else:
            raise RuntimeError(f"pub_prepare_build.py: unsupported target kind for cxxpacked: {target_kind}")
        if not cxxpacked_projection_ready(install_root):
            raise RuntimeError(
                f"pub_prepare_build.py: prepared cxxpacked projection remained incomplete: {install_root}"
            )
    else:
        raise RuntimeError(f"pub_prepare_build.py: unsupported prepare target dir_name={dir_name}")
    install_binaries(install_root, list(target_cfg.get("binary_install", [])))
    return install_root


def resolve_binary_path(install_root: Path, binary_name: str) -> Path:
    binary_path = install_root / "bin" / binary_name
    if not binary_path.is_file():
        raise RuntimeError(
            f"pub_prepare_build.py: requested binary is missing from prepared target: {binary_path}"
        )
    return binary_path.resolve()


def main() -> int:
    args = parse_args()
    cfg = parse_yaml_subset(read_text(YAML_PATH))
    scenario_cfg = resolve_scenario(cfg, args.scenario)
    if args.print_cache_steps_json:
        print(json.dumps(resolve_cache_steps(cfg, args.scenario), ensure_ascii=True))
        return 0
    if args.print_target_dir_names_json:
        print(
            json.dumps(
                [
                    str(resolve_target(cfg, target_name)["dir_name"]).strip()
                    for target_name in scenario_cfg["targets"]
                ],
                ensure_ascii=True,
            )
        )
        return 0
    target_dir = get_target_dir()
    ensure_dirs(target_dir)
    prepared_targets: dict[str, Path] = {}
    for target_name in scenario_cfg["targets"]:
        target_cfg = resolve_target(cfg, target_name)
        prepared_targets[target_name] = materialize_prepare_target(target_cfg, target_dir)
    ensure_build_tools(target_dir)
    binary_target_name = str(scenario_cfg["binary_target"])
    if args.print_binary_path is not None:
        binary_path = resolve_binary_path(prepared_targets[binary_target_name], args.print_binary_path)
        print(f"{BINARY_PATH_OUTPUT_PREFIX}{binary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
