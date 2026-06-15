#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import platform
import re
import sys
import tempfile
import shutil
import textwrap
import uuid
from pathlib import Path
import subprocess
import importlib.util

import utils as script_utils
import yaml

from utils.manylinux_version_utils import load_manylinux_version_static


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_NIX_PACK_CONFIG_PATH = REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib_static.yaml"
repo_root_str = str(REPO_ROOT)
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)

from setup_and_pack.utils import wheel_runtime_helper


PYO3_CHECKSUM_FILE_NAME = ".fluxon_pyo3_inputs.sha256"
_FIXED_TRANSPORT_BACKEND = script_utils.PUBLIC_TRANSPORT_BACKEND
_FIXED_TRANSPORT_PROFILE_ID = script_utils.PUBLIC_TRANSPORT_PROFILE_ID


def _top_level_release_manifest_relpaths(*, wheel_name: str) -> list[str]:
    relpaths = [
        "pylib_src.tar.gz",
        wheel_name,
        "ext_images.tar.gz",
        "ext_images/ext_images.sha256",
        "closed_sdk/manifest.json",
        "closed_sdk/lib/libfluxon_commu_core.so",
        "closed_sdk/lib/libfluxon_rdma_probe.so",
        "closed_sdk/native/native_runtime/include/rdma_probe_c.h",
        "closed_sdk/native/native_runtime/lib/libfluxon_rdma_probe.so",
    ]
    return sorted(dict.fromkeys(relpaths))


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pack release artifacts into a release directory")
    parser.add_argument(
        "--rdma-backend",
        choices=script_utils.RDMA_BACKENDS,
        default="closed_sdk",
        help="Rust PyO3 RDMA transfer backend variant to build",
    )
    parser.add_argument(
        "--release-dir",
        type=Path,
        default=None,
        help=(
            "Release directory root; if relative, resolve against the repo root inferred from "
            "this script path; defaults to <repo_root>/fluxon_release"
        ),
    )
    parser.add_argument(
        "--with-tikv-runtime",
        choices=("true", "false"),
        default="true",
        help=(
            "Whether ext_images in this release includes TiKV runtime binaries. "
            "Set false for KV benchmark-only releases that do not run transfer/TiKV flows."
        ),
    )
    return parser


def main() -> int:
    script_utils.reset_stage_summary()
    try:
        # Permission contract:
        # - The release tree authority object starts empty or absent for each pack run.
        # - All host-side artifacts materialized by this entrypoint must stay cross-step writable
        #   without post-hoc recursive chmod on the release tree.
        # - Set umask(0) here so newly created release files and directories converge at creation time;
        #   executable outputs still use explicit per-file chmod at their authority write sites.
        os.umask(0)
        repo_root = Path(__file__).resolve().parent.parent
        parser = _build_arg_parser()
        args = parser.parse_args()
        release_dir = (
            _resolve_repo_root_cli_path(repo_root=repo_root, raw_path=args.release_dir, field_name="release-dir")
            if args.release_dir is not None
            else (repo_root / "fluxon_release")
        )
        _run_pack_steps(
            repo_root,
            release_dir=release_dir,
            rdma_backend=args.rdma_backend,
            with_tikv_runtime=args.with_tikv_runtime == "true",
        )
        if not release_dir.exists():
            print(f"Missing release dir after pack steps: {release_dir}")
            return 1

        with script_utils.stage("Seeding invariant release runtime"):
            _seed_invariant_release_runtime(
                repo_root=repo_root,
                release_dir=release_dir,
                with_tikv_runtime=args.with_tikv_runtime == "true",
            )

        with script_utils.stage("Seeding profile cache compatibility entries"):
            _seed_profile_cache_compat_entries(release_dir=release_dir)

        wheel = _find_single(release_dir, "fluxon-*.whl", "release wheel")
        try:
            manylinux_version = load_manylinux_version_static(repo_root=repo_root)
        except Exception as e:
            print(f"ERROR: {e}", flush=True)
            raise SystemExit(1)
        _assert_no_top_level_pyo3_wheel(release_dir=release_dir, manylinux_version=manylinux_version)

        pylib_tar = release_dir / "pylib_src.tar.gz"
        with script_utils.stage("Packing pylib source tarball"):
            _pack_pylib_src(repo_root=repo_root, out_path=pylib_tar)

        # CI runs from an isolated workdir, so it cannot rely on the original checkout layout.
        # Keep runtime inputs as a small set of explicit release artifacts materialized from
        # artifact_set.source into a run-scoped fluxon_release:
        # - ext_images.tar.gz: service binaries (etcd/greptime/...)
        ext_images_tar = release_dir / "ext_images.tar.gz"
        with script_utils.stage("Packing ext_images tarball"):
            _pack_ext_images(release_dir=release_dir, out_path=ext_images_tar)

        with script_utils.stage("Removing test stack runtime residues from release"):
            _remove_release_test_stack_runtime_residues(release_dir=release_dir)

        sha_manifest = release_dir / "fluxon_release.sha256"
        release_manifest_relpaths = _top_level_release_manifest_relpaths(
            wheel_name=wheel.name,
        )
        with script_utils.stage("Writing sha256 manifest"):
            _write_sha256_manifest(
                out_path=sha_manifest,
                root_dir=release_dir,
                relpaths=release_manifest_relpaths,
            )

        with script_utils.stage("Verifying release manifest"):
            _verify_sha256_manifest(manifest_path=sha_manifest)

        print(f"Packed release wheels into: {release_dir}")
        print(f"- {wheel.name}")
        print(f"Packed pylib source tarball into: {pylib_tar}")
        print(f"Packed ext_images tarball into: {ext_images_tar}")
        print(f"Wrote release sha256 manifest: {sha_manifest}")
        return 0
    finally:
        script_utils.print_stage_summary()


def _resolve_repo_root_cli_path(*, repo_root: Path, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (repo_root / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _run_pack_steps(
    repo_root: Path,
    *,
    release_dir: Path,
    rdma_backend: str,
    with_tikv_runtime: bool,
) -> None:
    release_dir.mkdir(parents=True, exist_ok=True)

    with script_utils.stage("Packing Rust PyO3 wheel"):
        selected_pyo3_wheel = _pack_rust_pyo3_wheel_via_nix(
            repo_root=repo_root,
            release_dir=release_dir,
            rdma_backend=rdma_backend,
        )
    with script_utils.stage("Verifying Rust PyO3 wheel"):
        _require_pyo3_wheel_import_probe(selected_pyo3_wheel)

    _remove_release_wheels(
        release_dir=release_dir,
        pattern="fluxon-*.whl",
        ignore_predicate=lambda path: "pyo3" in path.name,
    )

    pack_py = repo_root / "setup_and_pack" / "pack_fluxon_pylib.py"
    if not pack_py.exists():
        print(f"Missing pack script: {pack_py}")
        raise SystemExit(1)
    with script_utils.stage("Packing pure Python wheel"):
        subprocess.check_call(
            [sys.executable, str(pack_py), "--release-dir", str(release_dir)],
            cwd=str(repo_root),
        )

    wheel_py = _find_single(release_dir, "fluxon-*.whl", "pure python wheel")
    with script_utils.stage("Assembling unified release wheel"):
        merged_wheel = _assemble_unified_release_wheel(
            release_dir=release_dir,
            pure_python_wheel=wheel_py,
            pyo3_wheel=selected_pyo3_wheel,
        )
    with script_utils.stage("Verifying unified release wheel"):
        _require_release_wheel_import_probe(merged_wheel)


def _remove_release_test_stack_runtime_residues(*, release_dir: Path) -> None:
    residue_names = (
        "fluxon_test_rsc.sha256",
        "src_ci.tar.gz",
        "fluxon_ci_ext_rsc.tar.gz",
        "baselines",
    )
    for name in residue_names:
        path = release_dir / name
        if not path.exists():
            continue
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
            continue
        path.unlink()


def _seed_invariant_release_runtime(
    *,
    repo_root: Path,
    release_dir: Path,
    with_tikv_runtime: bool,
) -> None:
    release_dir = release_dir.resolve()
    release_dir.mkdir(parents=True, exist_ok=True)

    install_template = (repo_root / "fluxon_release" / "install.py").resolve()
    if not install_template.exists() or not install_template.is_file():
        print(f"Missing tracked install.py template for release runtime seed: {install_template}")
        raise SystemExit(1)
    dst_install = release_dir / "install.py"
    if dst_install.resolve() != install_template:
        dst_install.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(install_template, dst_install)

    pack_release_ext = repo_root / "setup_and_pack" / "pack_release_ext.py"
    if not pack_release_ext.exists():
        print(f"Missing ext_images pack script: {pack_release_ext}")
        raise SystemExit(1)
    subprocess.check_call(
        [
            sys.executable,
            str(pack_release_ext),
            "--release-dir",
            str(release_dir),
            "--with-tikv-runtime",
            "true" if with_tikv_runtime else "false",
        ],
        cwd=str(repo_root),
    )

    closed_sdk_src = (repo_root / "fluxon_release" / "closed_sdk").resolve()
    if not closed_sdk_src.exists() or not closed_sdk_src.is_dir():
        print(f"Missing tracked closed_sdk runtime seed for release: {closed_sdk_src}")
        raise SystemExit(1)
    closed_sdk_dst = release_dir / "closed_sdk"
    if closed_sdk_dst.resolve() == closed_sdk_src:
        return
    if closed_sdk_dst.exists():
        if closed_sdk_dst.is_dir() and not closed_sdk_dst.is_symlink():
            shutil.rmtree(closed_sdk_dst)
        else:
            closed_sdk_dst.unlink()
    shutil.copytree(closed_sdk_src, closed_sdk_dst, dirs_exist_ok=False)


def _seed_profile_cache_compat_entries(*, release_dir: Path) -> None:
    profiles_dir = release_dir / "profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)

    profile_link = profiles_dir / _FIXED_TRANSPORT_PROFILE_ID
    expected_target = Path("..")
    if profile_link.is_symlink():
        if Path(os.readlink(profile_link)) == expected_target:
            return
        profile_link.unlink()
    elif profile_link.exists():
        raise RuntimeError(f"profile cache compatibility path already exists and is not a symlink: {profile_link}")

    profile_link.symlink_to(expected_target)


def _remove_release_wheels(
    *,
    release_dir: Path,
    pattern: str,
    ignore_predicate=None,
) -> None:
    for path in sorted(release_dir.glob(pattern)):
        if ignore_predicate is not None and ignore_predicate(path):
            continue
        if path.is_file() or path.is_symlink():
            path.unlink()


def _assemble_unified_release_wheel(
    *,
    release_dir: Path,
    pure_python_wheel: Path,
    pyo3_wheel: Path,
) -> Path:
    release_dir = release_dir.resolve()
    pure_python_wheel = pure_python_wheel.resolve()
    pyo3_wheel = pyo3_wheel.resolve()
    merged_wheel_name = _merged_fluxon_wheel_name(
        pure_python_wheel_name=pure_python_wheel.name,
        pyo3_wheel_name=pyo3_wheel.name,
    )
    merged_wheel_path = release_dir / merged_wheel_name
    if merged_wheel_path.exists():
        merged_wheel_path.unlink()
    wheel_runtime_helper.merge_binary_wheel(
        output_wheel_path=str(merged_wheel_path),
        pure_python_wheel_path=str(pure_python_wheel),
        runtime_wheel_path=str(pyo3_wheel),
    )
    pure_python_wheel.unlink(missing_ok=True)
    pyo3_wheel.unlink(missing_ok=True)
    return merged_wheel_path


def _merged_fluxon_wheel_name(*, pure_python_wheel_name: str, pyo3_wheel_name: str) -> str:
    pure_parts = pure_python_wheel_name.split("-")
    pyo3_parts = pyo3_wheel_name.split("-")
    if len(pure_parts) < 2:
        raise RuntimeError(f"invalid pure-python wheel filename: {pure_python_wheel_name}")
    if len(pyo3_parts) < 5:
        raise RuntimeError(f"invalid pyo3 wheel filename: {pyo3_wheel_name}")
    version = pure_parts[1]
    tag_parts = pyo3_parts[-3:]
    return "-".join(["fluxon", version, *tag_parts])


def _assert_no_top_level_pyo3_wheel(*, release_dir: Path, manylinux_version: str) -> None:
    lingering = _maybe_find_pyo3_wheel(release_dir, manylinux_version=manylinux_version)
    if lingering is not None:
        raise RuntimeError(f"release dir still contains top-level fluxon_pyo3 wheel after merge: {lingering}")


def _release_wheel_import_probe_ok(wheel_path: Path) -> bool:
    wheel_path = wheel_path.resolve()
    if not wheel_path.is_file():
        print(f"Release wheel import probe skipped; file is missing: {wheel_path}")
        return False
    with tempfile.TemporaryDirectory(prefix="fluxon_pack_release_unified_probe_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        venv_dir = temp_dir / "venv"
        subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            check=True,
            cwd=str(temp_dir),
        )
        probe_python = venv_dir / "bin" / "python"
        probe_pip = venv_dir / "bin" / "pip"
        install_completed = subprocess.run(
            [str(probe_pip), "install", "--no-deps", "--no-cache-dir", str(wheel_path)],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(temp_dir),
        )
        if install_completed.returncode != 0:
            print(f"Release wheel import probe install failed for {wheel_path}: rc={install_completed.returncode}")
            if install_completed.stdout.strip():
                print(install_completed.stdout.rstrip())
            if install_completed.stderr.strip():
                print(install_completed.stderr.rstrip())
            return False
        probe_completed = subprocess.run(
            [
                str(probe_python),
                "-c",
                _release_wheel_import_probe_code(),
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(temp_dir),
        )
        if probe_completed.returncode == 0:
            return True
        print(f"Release wheel import probe failed for {wheel_path}: rc={probe_completed.returncode}")
        if probe_completed.stdout.strip():
            print(probe_completed.stdout.rstrip())
        if probe_completed.stderr.strip():
            print(probe_completed.stderr.rstrip())
        return False


def _require_release_wheel_import_probe(wheel_path: Path) -> None:
    if not _release_wheel_import_probe_ok(wheel_path):
        raise RuntimeError(f"merged release wheel failed import probe: {wheel_path}")


def _release_wheel_import_probe_code() -> str:
    return textwrap.dedent(
        """
        import importlib.util
        import sys
        from pathlib import Path

        probe_path = None
        for raw_path in sys.path:
            if not raw_path:
                continue
            candidate = Path(raw_path) / "fluxon_py" / "tool" / "pyo3.py"
            if candidate.is_file():
                probe_path = candidate
                break
        if probe_path is None:
            raise RuntimeError("missing fluxon_py/tool/pyo3.py in installed wheel")

        spec = importlib.util.spec_from_file_location("_fluxon_release_probe_tool_pyo3", probe_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load release probe helper from {probe_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        imported_module = module.import_fluxon_pyo3_local()
        if getattr(imported_module, "__file__", None) is None:
            raise RuntimeError("fluxon_pyo3 loaded but __file__ is missing")
        print("IMPORT_OK")
        """
    )


def _load_nix_module(repo_root: Path):
    module_path = repo_root / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.py"
    if not module_path.exists():
        print(f"Missing NIX pack script: {module_path}")
        raise SystemExit(1)
    repo_root_str = str(repo_root.resolve())
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)
    module_parent = str(module_path.parent)
    if module_parent not in sys.path:
        sys.path.insert(0, module_parent)
    module_name = "_fluxon_nix_pack_fluxonkv_pylib"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load NIX pack module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_nix_layout_module(repo_root: Path):
    module_path = repo_root / "setup_and_pack" / "nix" / "lib_layout.py"
    if not module_path.exists():
        print(f"Missing NIX layout module: {module_path}")
        raise SystemExit(1)
    repo_root_str = str(repo_root.resolve())
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)
    module_parent = str(module_path.parent)
    if module_parent not in sys.path:
        sys.path.insert(0, module_parent)
    module_name = "_fluxon_nix_lib_layout"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load NIX layout module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_nix_config_root(*, repo_root: Path, config_path: Path) -> dict:
    nix_layout_module = _load_nix_layout_module(repo_root)
    return nix_layout_module.load_experiment_config_root(config_path=config_path)


def _nix_release_layout_paths(
    repo_root: Path,
    *,
    profile_name: str,
    assembly_name: str,
    instance_id: str,
    target_cache_namespace: str,
) -> tuple[Path, Path]:
    config_path = DEFAULT_NIX_PACK_CONFIG_PATH.resolve()
    if not config_path.exists():
        print(f"Missing NIX pack config: {config_path}")
        raise SystemExit(1)
    nix_layout_module = _load_nix_layout_module(repo_root)
    spec = nix_layout_module.load_experiment_spec(config_path=config_path)
    spec = nix_layout_module.ExperimentSpec(
        config_path=spec.config_path,
        project_root=spec.project_root,
        project_data_root=spec.project_data_root,
        base_system=spec.base_system,
        architectures=spec.architectures,
        python_abi=spec.python_abi,
        profile_name=profile_name,
        assembly_name=assembly_name,
        instance_id=instance_id,
        target_cache_namespace=target_cache_namespace,
        profile_source=spec.profile_source,
        profile_layout=spec.profile_layout,
        assembly_refs=spec.assembly_refs,
    )
    runtime_targets = nix_layout_module.build_runtime_targets(spec=spec)
    if len(runtime_targets) != 1:
        raise RuntimeError(
            "pack_release.py expects exactly one runtime target in nix pack config: "
            + str(config_path)
        )
    layout = nix_layout_module.build_layout(spec=spec, runtime_target=runtime_targets[0])
    return config_path, layout.instance_release_dir.resolve()


def _pack_rust_pyo3_wheel_via_nix(
    *,
    repo_root: Path,
    release_dir: Path,
    rdma_backend: str,
) -> Path:
    manylinux_version = load_manylinux_version_static(repo_root=repo_root)
    nix_module = _load_nix_module(repo_root)
    pack_state = nix_module.PyO3PackState(
        repo_root=repo_root,
        manylinux_version=manylinux_version,
        transport_backend=_FIXED_TRANSPORT_BACKEND,
        rdma_backend=rdma_backend,
        release_dir=release_dir,
    )
    if pack_state.reuse_existing_wheel():
        reused_release_wheel = _find_pyo3_wheel(
            release_dir,
            manylinux_version=manylinux_version,
            what="pyo3 wheel",
        )
        if _wheel_import_probe_ok(reused_release_wheel):
            return reused_release_wheel
        print(
            "Cached release-dir PyO3 wheel failed local import probe; forcing rebuild: "
            + str(reused_release_wheel)
        )
        reused_release_wheel.unlink(missing_ok=True)

    run_id = uuid.uuid4().hex
    profile_name = f"pack_release_{_FIXED_TRANSPORT_BACKEND}_{rdma_backend}_{run_id}"
    assembly_name = profile_name
    instance_id = profile_name
    target_cache_namespace = f"pack_release_{_FIXED_TRANSPORT_BACKEND}_{rdma_backend}"
    config_template_path, instance_release_dir = _nix_release_layout_paths(
        repo_root,
        profile_name=profile_name,
        assembly_name=assembly_name,
        instance_id=instance_id,
        target_cache_namespace=target_cache_namespace,
    )
    cached_instance_wheel = _maybe_find_pyo3_wheel(
        instance_release_dir,
        manylinux_version=manylinux_version,
    )
    if cached_instance_wheel is not None:
        cached_instance_checksum = _read_pyo3_checksum_or_none(instance_release_dir)
        current_checksum = pack_state.current_checksum()
        if (
            cached_instance_checksum == current_checksum
            and _wheel_import_probe_ok(cached_instance_wheel)
        ):
            print(f"Reusing NIX layout PyO3 wheel without rebuild: {cached_instance_wheel}")
            copied_wheel = _copy_release_artifacts(
                src_release_dir=instance_release_dir,
                dst_release_dir=release_dir,
                pattern="fluxon_pyo3-*.whl",
                what="pyo3 wheel",
                selected_src_path=cached_instance_wheel,
            )
            checksum_src = instance_release_dir / PYO3_CHECKSUM_FILE_NAME
            if checksum_src.exists():
                shutil.copy2(checksum_src, release_dir / PYO3_CHECKSUM_FILE_NAME)
            return copied_wheel
        if cached_instance_checksum != current_checksum:
            print(
                "Cached NIX layout PyO3 wheel checksum mismatch; rebuilding: "
                + f"cached={cached_instance_checksum!r} current={current_checksum!r}"
            )
        else:
            print(
                "Cached NIX layout PyO3 wheel failed local import probe; rebuilding: "
                + str(cached_instance_wheel)
            )
        cached_instance_wheel.unlink(missing_ok=True)
        cached_instance_checksum_path = instance_release_dir / PYO3_CHECKSUM_FILE_NAME
        cached_instance_checksum_path.unlink(missing_ok=True)

    pack_rust = repo_root / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.py"
    if not pack_rust.exists():
        print(f"Missing NIX pack script: {pack_rust}")
        raise SystemExit(1)

    nix_runs_dir = repo_root / "setup_and_pack" / "nix" / "runs"
    nix_runs_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        prefix=f"fluxon_pack_release_nix_{_FIXED_TRANSPORT_BACKEND}_",
        suffix=".yaml",
        delete=False,
        dir=str(nix_runs_dir),
    ) as tmp:
        tmp_config_path = Path(tmp.name)

    try:
        tmp_config_path.write_text(
            _render_nix_pack_config(
                template_path=config_template_path,
                repo_root=repo_root,
                rdma_backend=rdma_backend,
                profile_name=profile_name,
                assembly_name=assembly_name,
                instance_id=instance_id,
                target_cache_namespace=target_cache_namespace,
            ),
            encoding="utf-8",
        )
        subprocess.check_call(
            [
                sys.executable,
                str(pack_rust),
                "--config",
                str(tmp_config_path),
                "--apply-layout",
                "--run",
            ],
            cwd=str(repo_root),
        )
    finally:
        if tmp_config_path.exists():
            tmp_config_path.unlink()

    if not instance_release_dir.exists() or not instance_release_dir.is_dir():
        raise RuntimeError(f"NIX release dir was not produced: {instance_release_dir}")
    built_wheel = _copy_release_artifacts(
        src_release_dir=instance_release_dir,
        dst_release_dir=release_dir,
        pattern="fluxon_pyo3-*.whl",
        what="pyo3 wheel",
    )
    return built_wheel


def _wheel_import_probe_ok(wheel_path: Path) -> bool:
    wheel_path = wheel_path.resolve()
    if not wheel_path.is_file():
        print(f"PyO3 wheel import probe skipped; file is missing: {wheel_path}")
        return False
    with tempfile.TemporaryDirectory(prefix="fluxon_pack_release_probe_") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        venv_dir = temp_dir / "venv"
        python_bin = Path(sys.executable).resolve()
        # English note:
        # - The import probe validates a built wheel as an isolated artifact.
        # - Running probe subprocesses under the checkout root can let unrelated host-side import/build
        #   hooks observe the repository and materialize debug artifacts into the checkout target tree.
        # - Keep the probe cwd inside its own temp authority so success depends only on the wheel and
        #   the fresh venv, not on ambient repo-side state.
        probe_cwd = temp_dir
        subprocess.run(
            [str(python_bin), "-m", "venv", str(venv_dir)],
            check=True,
            cwd=str(probe_cwd),
        )
        probe_python = venv_dir / "bin" / "python"
        probe_pip = venv_dir / "bin" / "pip"
        install_completed = subprocess.run(
            [
                str(probe_pip),
                "install",
                "--no-deps",
                "--no-cache-dir",
                str(wheel_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(probe_cwd),
        )
        if install_completed.returncode != 0:
            print(
                "PyO3 wheel import probe install failed for "
                + f"{wheel_path}: rc={install_completed.returncode}"
            )
            if install_completed.stdout.strip():
                print(install_completed.stdout.rstrip())
            if install_completed.stderr.strip():
                print(install_completed.stderr.rstrip())
            return False
        probe_completed = subprocess.run(
            [
                str(probe_python),
                "-c",
                "import fluxon_pyo3; print('IMPORT_OK')",
            ],
            check=False,
            capture_output=True,
            text=True,
            cwd=str(probe_cwd),
        )
        if probe_completed.returncode == 0:
            return True
        print(
            "PyO3 wheel import probe failed for "
            + f"{wheel_path}: rc={probe_completed.returncode}"
        )
        if probe_completed.stdout.strip():
            print(probe_completed.stdout.rstrip())
        if probe_completed.stderr.strip():
            print(probe_completed.stderr.rstrip())
        return False


def _require_pyo3_wheel_import_probe(wheel_path: Path) -> None:
    if not _wheel_import_probe_ok(wheel_path):
        raise RuntimeError(f"built PyO3 wheel failed local import probe: {wheel_path}")


def _render_nix_pack_config(
    *,
    template_path: Path,
    repo_root: Path,
    rdma_backend: str,
    profile_name: str,
    assembly_name: str,
    instance_id: str,
    target_cache_namespace: str,
) -> str:
    cfg = _load_nix_config_root(repo_root=repo_root, config_path=template_path)
    if not isinstance(cfg, dict):
        raise RuntimeError(f"NIX pack config must be a mapping: {template_path}")
    manylinux_cfg = cfg.get("manylinux")
    if not isinstance(manylinux_cfg, dict):
        raise RuntimeError(f"NIX pack config is missing mapping field manylinux: {template_path}")
    manylinux_cfg["transport_backend"] = _FIXED_TRANSPORT_BACKEND
    manylinux_cfg["rdma_backend"] = rdma_backend

    runtime_cfg = cfg.get("runtime")
    if not isinstance(runtime_cfg, dict):
        raise RuntimeError(f"NIX pack config is missing mapping field runtime: {template_path}")
    runtime_cfg["profile_name"] = profile_name
    runtime_cfg["assembly_name"] = assembly_name
    runtime_cfg["instance_id"] = instance_id
    runtime_cfg["target_cache_namespace"] = target_cache_namespace

    fluxon_commu_source_path = (repo_root / "fluxon_rs" / "fluxon_commu").resolve()
    if not fluxon_commu_source_path.is_dir():
        raise RuntimeError(
            "bridge_prebuilt fluxon_commu source dir is missing: "
            + str(fluxon_commu_source_path)
        )

    profile_cfg = cfg.get("profile")
    if not isinstance(profile_cfg, dict):
        raise RuntimeError(f"NIX pack config is missing mapping field profile: {template_path}")
    native_runtime_dir_names = profile_cfg.get("native_runtime_dir_names")
    if not isinstance(native_runtime_dir_names, list):
        raise RuntimeError(
            f"NIX pack config profile.native_runtime_dir_names must be a list: {template_path}"
        )
    if rdma_backend != "closed_sdk":
        raise RuntimeError(f"public pack_release.py only supports rdma_backend=closed_sdk, got {rdma_backend!r}")
    profile_cfg["native_runtime_dir_names"] = [
        dir_name
        for dir_name in native_runtime_dir_names
        if dir_name in ("native_runtime", "cxxpacked", "vendor_runtime")
    ]
    profile_cfg["ext_bundle_dir_name"] = "vendor_runtime"
    profile_cfg.update(
        {
            "source_kind": "bridge_prebuilt",
            "build_root_path": str(repo_root.resolve()),
        }
    )

    cfg["runtime_sources"] = {
        "fluxon_commu": {
            "source_kind": "local_path",
            "source_path": str(fluxon_commu_source_path),
        }
    }
    return yaml.safe_dump(cfg, sort_keys=False)
def _copy_release_artifacts(
    *,
    src_release_dir: Path,
    dst_release_dir: Path,
    pattern: str,
    what: str,
    selected_src_path: Path | None = None,
) -> Path:
    matches = sorted(src_release_dir.glob(pattern))
    if selected_src_path is not None:
        selected_src_path = selected_src_path.resolve()
        matches = [path for path in matches if path.resolve() == selected_src_path]
    if len(matches) != 1:
        names = [path.name for path in matches]
        raise RuntimeError(
            f"expected exactly one {what} in {src_release_dir} matching {pattern}, got {names}"
        )
    src_path = matches[0]
    dst_path = dst_release_dir / src_path.name
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_path, dst_path)
    return dst_path


def _read_pyo3_checksum_or_none(release_dir: Path) -> str | None:
    checksum_path = release_dir / PYO3_CHECKSUM_FILE_NAME
    if not checksum_path.exists():
        return None
    text = checksum_path.read_text(encoding="utf-8").strip()
    return text or None


def _maybe_find_pyo3_wheel(release_dir: Path, *, manylinux_version: str) -> Path | None:
    arch = _get_arch_name()
    matches = sorted(release_dir.glob("fluxon_pyo3-*.whl"))
    if not matches:
        return None
    wanted_token = f"manylinux_{manylinux_version}_{arch}"
    selected = [path for path in matches if wanted_token in path.name]
    if not selected:
        return None
    if len(selected) != 1:
        names = "\n".join(f"- {path.name}" for path in selected)
        raise RuntimeError(f"Ambiguous cached pyo3 wheel for {wanted_token} in {release_dir}\n{names}")
    return selected[0]


def _prune_release_wheels_except(*, release_dir: Path, pattern: str, keep_path: Path) -> None:
    keep_resolved = keep_path.resolve()
    for path in sorted(release_dir.glob(pattern)):
        if path.resolve() == keep_resolved:
            continue
        if path.is_file() or path.is_symlink():
            path.unlink()


def _get_arch_name() -> str:
    mach = platform.machine().lower()
    if mach in ("x86_64", "amd64"):
        return "x86_64"
    if mach in ("aarch64", "arm64"):
        return "aarch64"
    raise SystemExit(f"Unsupported architecture: {mach}")


def _find_pyo3_wheel(release_dir: Path, *, manylinux_version: str, what: str) -> Path:
    arch = _get_arch_name()
    matches = sorted(release_dir.glob("fluxon_pyo3-*.whl"))
    if not matches:
        print(f"Missing {what} in release dir: {release_dir} (pattern: fluxon_pyo3-*.whl)")
        raise SystemExit(1)

    wanted_token = f"manylinux_{manylinux_version}_{arch}"
    selected = [p for p in matches if wanted_token in p.name]
    if not selected:
        names = "\n".join(f"- {p.name}" for p in matches)
        print(f"Missing {what} for {wanted_token} in release dir: {release_dir}")
        print("Available wheels:")
        print(names)
        raise SystemExit(1)
    if len(selected) != 1:
        names = "\n".join(f"- {p.name}" for p in selected)
        print(f"Ambiguous {what} for {wanted_token} in release dir: {release_dir}")
        print(names)
        raise SystemExit(1)
    return selected[0]


def _find_single(release_dir: Path, pattern: str, what: str) -> Path:
    matches = sorted(release_dir.glob(pattern))
    if not matches:
        print(f"Missing {what} in release dir: {release_dir} (pattern: {pattern})")
        raise SystemExit(1)
    if len(matches) != 1:
        names = "\n".join(f"- {p.name}" for p in matches)
        print(f"Ambiguous {what} in release dir: {release_dir} (pattern: {pattern})")
        print(names)
        raise SystemExit(1)
    return matches[0]


def _resolve_examples_dir(*, repo_root: Path) -> Path:
    for candidate in (repo_root / "app" / "examples", repo_root / "examples"):
        if candidate.exists():
            return candidate
    print(
        "Missing examples directory at authority paths: "
        + f"{repo_root / 'app' / 'examples'} or {repo_root / 'examples'}"
    )
    raise SystemExit(1)


def _pack_pylib_src(*, repo_root: Path, out_path: Path) -> None:
    setup_py = repo_root / "setup.py"
    fluxon_py = repo_root / "fluxon_py"
    examples_dir = _resolve_examples_dir(repo_root=repo_root)
    if not setup_py.exists():
        print(f"Missing setup.py at repo root: {setup_py}")
        raise SystemExit(1)
    if not fluxon_py.exists():
        print(f"Missing fluxon_py directory at repo root: {fluxon_py}")
        raise SystemExit(1)

    def build_tarball() -> None:
        with script_utils.stage("Staging pylib sources (rsync + gitignore)"):
            with tempfile.TemporaryDirectory(prefix="fluxon_pack_pylib_") as td:
                stage_root = Path(td)
                script_utils.rsync_stage(
                    repo_root=repo_root,
                    src=repo_root / "fluxon_py",
                    dst=stage_root / "fluxon_py",
                    honor_gitignore=True,
                )
                script_utils.rsync_stage(
                    repo_root=repo_root,
                    src=repo_root / "setup.py",
                    dst=stage_root / "setup.py",
                    honor_gitignore=True,
                )
                script_utils.rsync_stage(
                    repo_root=repo_root,
                    src=examples_dir,
                    dst=stage_root / "examples",
                    honor_gitignore=True,
                )
                with script_utils.stage("Compressing pylib_src.tar.gz (tar + pigz)"):
                    script_utils.tar_gz(
                        cwd=stage_root,
                        out_path=out_path,
                        inputs=["setup.py", "fluxon_py", "examples"],
                        honor_vcs_ignores=False,
                    )

    script_utils.build_cached_tarball(
        rule=script_utils.tarball_rule(
            name="pylib source tarball",
            out_path=out_path,
            input_paths=[setup_py, fluxon_py, examples_dir],
            relative_to=repo_root,
        ),
        out_path=out_path,
        build_tarball=build_tarball,
    )


def _pack_ext_images(*, release_dir: Path, out_path: Path) -> None:
    ext_images = release_dir / "ext_images"
    if not ext_images.exists():
        print(f"Missing ext_images directory in release dir: {ext_images}")
        raise SystemExit(1)

    script_utils.build_cached_tarball(
        rule=script_utils.tarball_rule(
            name="ext_images tarball",
            out_path=out_path,
            input_paths=[ext_images],
            relative_to=release_dir,
        ),
        out_path=out_path,
        build_tarball=lambda: script_utils.tar_gz(
            cwd=release_dir,
            out_path=out_path,
            inputs=["ext_images"],
            honor_vcs_ignores=False,
        ),
    )


def _parse_relpaths_from_sha256_manifest(manifest_path: Path) -> list[str]:
    relpaths: list[str] = []
    for raw in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        _, relpath = _parse_sha256_manifest_line(raw)
        relpaths.append(relpath)
    return relpaths


def _write_sha256_manifest(*, out_path: Path, root_dir: Path, relpaths: list[str]) -> None:
    lines: list[str] = []
    for relpath in relpaths:
        p = root_dir / relpath
        if not p.exists():
            print(f"Cannot write sha256 manifest: missing file: {p}")
            raise SystemExit(1)
        digest = subprocess.check_output(["sha256sum", str(p)], text=True).split()[0]
        lines.append(f"{digest}  {relpath}\n")
    out_path.write_text("".join(lines), encoding="utf-8")


def _verify_sha256_manifest(*, manifest_path: Path) -> None:
    if not manifest_path.exists():
        print(f"Missing sha256 manifest after write: {manifest_path}")
        raise SystemExit(1)
    release_dir = manifest_path.parent
    for raw in manifest_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        expected_sha, rel_name = _parse_sha256_manifest_line(raw)
        file_path = release_dir / rel_name
        if not file_path.exists():
            print(f"Manifest references missing artifact: {file_path}")
            raise SystemExit(1)
        actual_sha = subprocess.check_output(["sha256sum", str(file_path)], text=True).split()[0]
        if actual_sha != expected_sha:
            print(
                "Release manifest drift detected immediately after pack.\n"
                f"manifest={manifest_path}\n"
                f"file={rel_name}\n"
                f"expected_sha256={expected_sha}\n"
                f"actual_sha256={actual_sha}"
            )
            raise SystemExit(1)


def _parse_sha256_manifest_line(raw: str) -> tuple[str, str]:
    line = raw.rstrip("\n")
    if len(line) < 67 or line[64:66] != "  ":
        print(f"Invalid manifest line: {raw}")
        raise SystemExit(1)
    digest = line[:64]
    relpath = line[66:]
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        print(f"Invalid manifest digest: {raw}")
        raise SystemExit(1)
    if not _is_clean_manifest_relpath(relpath):
        print(f"Invalid manifest relpath: {raw}")
        raise SystemExit(1)
    return digest, relpath


def _is_clean_manifest_relpath(relpath: str) -> bool:
    if not relpath or relpath.startswith("/") or relpath.startswith("\\"):
        return False
    if "\\" in relpath or "\x00" in relpath:
        return False
    return all(part not in ("", ".", "..") for part in relpath.split("/"))


if __name__ == "__main__":
    sys.exit(main())
