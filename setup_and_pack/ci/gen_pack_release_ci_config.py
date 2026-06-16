#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DEFAULT_ENV_TEMPLATE = REPO_ROOT / "setup_and_pack" / "pack_fluxonkv_pylib_env.yaml.template"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a GitHub Actions-friendly pack_release config")
    parser.add_argument(
        "--env-template",
        type=Path,
        default=DEFAULT_ENV_TEMPLATE,
        help="Base nix pack env template; relative paths resolve against repo root",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory for the generated pack_fluxonkv_pylib_env.yaml",
    )
    parser.add_argument(
        "--project-data-root",
        type=Path,
        required=True,
        help="Absolute project_data_root for CI runtime state",
    )
    return parser.parse_args()


def _resolve_repo_path(path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def main() -> int:
    args = _parse_args()
    env_template_path = _resolve_repo_path(args.env_template)
    out_dir = args.out_dir.resolve()
    env_out_path = out_dir / "pack_fluxonkv_pylib_env.yaml"

    cfg = yaml.safe_load(env_template_path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise RuntimeError(f"template config must be a mapping: {env_template_path}")

    host_paths_cfg = cfg.get("host_paths")
    if not isinstance(host_paths_cfg, dict):
        raise RuntimeError(f"template config missing host_paths mapping: {env_template_path}")

    host_root = args.project_data_root.resolve()
    host_paths_cfg["root_path"] = str(host_root)

    out_dir.mkdir(parents=True, exist_ok=True)
    env_out_path.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    print(env_out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
