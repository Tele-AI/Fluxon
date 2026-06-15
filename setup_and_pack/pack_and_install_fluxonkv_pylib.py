#!/usr/bin/env python3
"""
Pack and install the unified PyO3 Python library.

Steps:
1) Invoke setup_and_pack/nix/pack_fluxonkv_pylib.py to build wheels into the release cache
2) Pick a wheel matching current Python (cpXY) and install via pip

Notes:
- No rollback; on error, print and exit.
- Avoid env vars; installs into the current Python environment.
"""

import sys
import subprocess
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _release_dir(root: Path) -> Path:
    nix_dir = root / "setup_and_pack" / "nix"
    if str(nix_dir) not in sys.path:
        sys.path.insert(0, str(nix_dir))
    import lib_layout

    config_path = root / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_static.yaml"
    spec = lib_layout.load_experiment_spec(config_path=config_path)
    runtime_targets = lib_layout.build_runtime_targets(spec=spec)
    if len(runtime_targets) != 1:
        raise RuntimeError(
            "pack_and_install_fluxonkv_pylib.py expects exactly one runtime target in setup_and_pack/nix/pack_fluxonkv_pylib_static.yaml"
        )
    layout = lib_layout.build_layout(spec=spec, runtime_target=runtime_targets[0])
    return layout.instance_release_dir.resolve()


def _select_wheel(dirpath: Path) -> Path:
    # Prefer fluxon_pyo3 (runtime module name), then fluxon_kv; pick newest by mtime
    patterns = ["fluxon_pyo3-*.whl", "fluxon_kv-*.whl", "*.whl"]
    for pat in patterns:
        candidates = sorted(dirpath.rglob(pat), key=lambda p: p.stat().st_mtime)
        if candidates:
            return candidates[-1]
    raise SystemExit(f"❌ 未在 {dirpath} 找到可安装的轮子 (*.whl)")


def main() -> int:
    root = _repo_root()
    pack_script = root / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.py"
    config_path = root / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_static.yaml"
    wheels_dir = _release_dir(root)

    if not pack_script.exists() or not config_path.exists():
        print(f"❌ 未找到打包脚本: {pack_script}")
        return 1

    print("📦 打包统一 Python 库 (PyO3)...")
    subprocess.check_call(
        [
            sys.executable,
            str(pack_script),
            "--config",
            str(config_path),
            "--apply-layout",
            "--run",
        ]
    )

    if not wheels_dir.exists():
        print(f"❌ 轮子目录不存在: {wheels_dir}")
        return 1

    wheel = _select_wheel(wheels_dir)
    print(f"✅ 打包完成，准备安装: {wheel.name}")

    pip_cmd = [sys.executable, "-m", "pip", "install", str(wheel), "--force-reinstall"]
    # Debian/Ubuntu system Python enables PEP 668 externally-managed protection.
    # This script installs into the interpreter that launches it, so system Python
    # must opt in explicitly when it is not running inside a virtual environment.
    if sys.prefix == getattr(sys, "base_prefix", sys.prefix):
        pip_cmd.append("--break-system-packages")
    subprocess.check_call(pip_cmd)
    print("🎉 安装完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
