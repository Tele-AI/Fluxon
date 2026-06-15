#!/usr/bin/env python3
"""
Build a dedicated Docker image for building the PyO3 package.

This script generates an extended build config at runtime
and then builds the image directly with `docker build`.
"""

import platform
import sys
from pathlib import Path
from typing import List
import utils as script_utils
from utils import (
    build_docker_image_from_config,
    sudo_prefix,
)
import yaml


SCRIPT_DIR = Path(__file__).absolute().parent
BASE_CONFIG = SCRIPT_DIR / "build_pack_fluxonkv_pylib_img" / "pypack_builder_manylinux_2_28.yaml"
GENERATED_CONFIG = SCRIPT_DIR / "build_pack_fluxonkv_pylib_img" / "pypack_builder_manylinux_2_28.generated.yaml"
PREPARE_ARTIFACT_COPY_STEP_NAME = "copy_prepare_build_artifacts"
LEGACY_COPY_STEP_NAME = "copy_sources_and_pyo3_build"
RUST_TOOLCHAIN_PLACEHOLDER = "__FLUXON_RUST_TOOLCHAIN__"
FLUXON_RS_TOOLCHAIN_TOML = Path("fluxon_rs") / "rust-toolchain.toml"
PREPARE_BUILD_YAML = Path("setup_and_pack") / "pub_prepare_build.yaml"


def _host_arch_name() -> str:
    mach = platform.machine().lower()
    if mach in ("x86_64", "amd64"):
        return "x86_64"
    if mach in ("aarch64", "arm64"):
        return "aarch64"
    print(f"❌ Unsupported host architecture for prepare_build local artifact copy: {mach}")
    sys.exit(1)


def _local_prepare_build_artifacts(*, project_root: Path) -> list[tuple[Path, str]]:
    prepare_build_path = project_root / PREPARE_BUILD_YAML
    if not prepare_build_path.exists():
        return []

    try:
        config = yaml.safe_load(prepare_build_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        print(f"❌ Failed to parse prepare_build.yaml: {exc}")
        sys.exit(1)

    prepare_targets = config.get("PREPARE_TARGETS", {})
    if not isinstance(prepare_targets, dict):
        print(f"❌ PREPARE_TARGETS must be a mapping in: {prepare_build_path}")
        sys.exit(1)

    artifacts: list[tuple[Path, str]] = []
    arch = _host_arch_name()
    for target_name, target_cfg in prepare_targets.items():
        if not isinstance(target_cfg, dict):
            print(f"❌ PREPARE_TARGETS.{target_name} must be a mapping")
            sys.exit(1)
        for field_name in ("url", "verify"):
            raw_value = target_cfg.get(field_name)
            if not raw_value:
                continue
            if not isinstance(raw_value, str):
                print(f"❌ PREPARE_TARGETS.{target_name}.{field_name} must be a string")
                sys.exit(1)
            if not raw_value.startswith("file://"):
                continue
            resolved_path = raw_value[len("file://") :].replace("${arch}", arch)
            artifact_path = Path(resolved_path)
            if not artifact_path.exists():
                print(
                    f"❌ Local prepare_build artifact declared in YAML does not exist: "
                    f"target={target_name} field={field_name} path={artifact_path}"
                )
                sys.exit(1)
            artifacts.append((artifact_path, resolved_path))
    return artifacts


def _read_fluxon_rust_toolchain_channel(*, project_root: Path) -> str:
    toolchain_path = project_root / FLUXON_RS_TOOLCHAIN_TOML
    if not toolchain_path.exists():
        print(f"❌ Missing Rust toolchain authority file: {toolchain_path}")
        sys.exit(1)

    # English note:
    # rust-toolchain.toml often carries inline comments, e.g.
    #   channel = "1.93.0"       # Or "stable", "beta", "nightly"
    # A naive quote-based slice can accidentally capture the trailing comment.
    # We only want the first quoted string literal after `channel =`.
    import re

    text = toolchain_path.read_text(encoding="utf-8")
    in_toolchain_section = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            in_toolchain_section = (line == "[toolchain]")
            continue
        if not in_toolchain_section:
            continue

        m = re.match(r'^channel\s*=\s*"([^"]+)"', line)
        if not m:
            continue
        channel = m.group(1).strip()
        if not channel:
            continue
        return channel

    print(f"❌ Failed to parse Rust toolchain channel from: {toolchain_path}")
    print("   Expected: [toolchain] section with: channel = \"<version>\"")
    sys.exit(1)


def _replace_rust_toolchain_placeholder(config: dict, *, project_root: Path) -> None:
    channel = _read_fluxon_rust_toolchain_channel(project_root=project_root)
    found_placeholder = False

    heavy_setup = config.get("heavy_setup", {})
    script_installs = heavy_setup.get("script_installs", [])
    for step in script_installs:
        commands = step.get("commands")
        if not isinstance(commands, list):
            continue
        for i, cmd in enumerate(commands):
            if not isinstance(cmd, str):
                continue
            if RUST_TOOLCHAIN_PLACEHOLDER not in cmd:
                continue
            found_placeholder = True
            commands[i] = cmd.replace(RUST_TOOLCHAIN_PLACEHOLDER, channel)

    if not found_placeholder:
        print("❌ Missing Rust toolchain placeholder in base builder config.")
        print(f"   Expected placeholder: {RUST_TOOLCHAIN_PLACEHOLDER}")
        print(f"   Base config: {BASE_CONFIG}")
        sys.exit(1)

    if RUST_TOOLCHAIN_PLACEHOLDER in yaml.safe_dump(config, allow_unicode=True, sort_keys=False):
        print("❌ Rust toolchain placeholder still present after replacement.")
        print(f"   Placeholder: {RUST_TOOLCHAIN_PLACEHOLDER}")
        sys.exit(1)


def ensure_docker_available() -> None:
    """Verify Docker is reachable before attempting the build."""
    with script_utils.stage("检查 Docker 环境"):
        try:
            prefix = " ".join(sudo_prefix())
            cmd = f"{prefix} docker --version" if prefix else "docker --version"
            script_utils.run_cmd_sure(cmd)
            print("✓ Docker 可用")
        except Exception:
            print("❌ Docker 不可用，请确保 Docker 已安装并正在运行")
            sys.exit(1)


def load_builder_config() -> dict:
    """Load the base YAML configuration and extend it dynamically."""
    if not BASE_CONFIG.exists():
        print(f"❌ 未找到构建配置文件: {BASE_CONFIG}")
        sys.exit(1)

    try:
        with open(BASE_CONFIG, "r", encoding="utf-8") as config_file:
            data = yaml.safe_load(config_file) or {}
    except yaml.YAMLError as exc:
        print(f"❌ 解析构建配置文件失败: {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"❌ 读取构建配置文件失败: {exc}")
        sys.exit(1)

    extend_builder_config(data)
    _replace_rust_toolchain_placeholder(data, project_root=SCRIPT_DIR.parent)

    try:
        with open(GENERATED_CONFIG, "w", encoding="utf-8") as output_file:
            yaml.safe_dump(data, output_file, allow_unicode=True, sort_keys=False)
    except Exception as exc:
        print(f"❌ 写入生成的构建配置失败: {exc}")
        sys.exit(1)

    return data


def extend_builder_config(config: dict) -> None:
    """Materialize only non-mounted local authority artifacts into the builder image.

    The runtime build container bind-mounts a persistent host workspace at `/workspace`,
    so any source tree or cargo cache baked into the image under `/workspace` is fully
    shadowed at runtime and cannot be reused. Keeping repo source copies in the image
    therefore causes expensive cache churn without any runtime benefit.

    The only dynamic inputs that still need to be copied into the image are local
    `file://` prepare_build artifacts, because those absolute paths are referenced by
    authority config and are not covered by the runtime bind mounts.
    """
    heavy_setup = config.setdefault("heavy_setup", {})
    script_installs = heavy_setup.setdefault("script_installs", [])

    project_root = SCRIPT_DIR.parent
    copies_field: List[dict] = []

    def add_path_copy(src_path: Path, dst_path: str) -> None:
        if src_path.exists():
            copies_field.append(
                {
                    "kind": "path",
                    "src": str(src_path),
                    "dst": dst_path,
                }
            )
        else:
            print(f"⚠️  警告: 复制源不存在，跳过 {src_path}")

    for src_path, dst_path in _local_prepare_build_artifacts(project_root=project_root):
        add_path_copy(src_path, dst_path)

    script_installs[:] = [
        step
        for step in script_installs
        if step.get("name") not in {PREPARE_ARTIFACT_COPY_STEP_NAME, LEGACY_COPY_STEP_NAME}
    ]

    if not copies_field:
        return

    script_installs.append(
        {
            "name": PREPARE_ARTIFACT_COPY_STEP_NAME,
            "copies": copies_field,
            "commands": [":"],
        }
    )


def build_image(config: dict) -> None:
    """Build the image from the generated YAML config."""
    with script_utils.stage("Build PyO3 builder image"):
        print("🔨 Running image build...")
        try:
            image_ref = build_docker_image_from_config(SCRIPT_DIR.parent, GENERATED_CONFIG)
        except Exception as exc:
            print(f"❌ Build failed: {exc}")
            raise SystemExit(1)

    image_name = config.get("image_name", "pypack-builder")
    image_tag = config.get("image_tag", "latest")
    print(f"\n🎉 Successfully built PyO3 package builder image '{image_name}:{image_tag}'!")
    print(f"   image_ref: {image_ref}")

    print("\nUsage examples:")
    print("  # Interactive usage:")
    demo_ref = image_ref
    print(f"  docker run -it --rm -v $(pwd):/workspace {demo_ref}")
    print("  ")
    print("  # Run the build script:")
    print(
        f"  docker run --rm -v $(pwd):/workspace {demo_ref} "
        "python3 setup_and_pack/nix/pack_fluxonkv_pylib.py --config setup_and_pack/nix/pack_fluxonkv_pylib_static.yaml --apply-layout --run"
    )
    print("  ")
    print("  # Manual build:")
    print(
        f"  docker run --rm -v $(pwd):/workspace {demo_ref} maturin build --release"
    )


def main() -> None:
    script_utils.chdir_to_cur_file()
    ensure_docker_available()
    config = load_builder_config()
    build_image(config)


if __name__ == "__main__":
    main()
