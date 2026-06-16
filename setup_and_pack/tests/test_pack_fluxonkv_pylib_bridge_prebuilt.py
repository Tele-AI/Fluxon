from __future__ import annotations

import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.py"


def _load_module():
    for import_root in (
        REPO_ROOT,
        SCRIPT_PATH.parent,
        SCRIPT_PATH.parent.parent,
    ):
        import_root_str = str(import_root)
        if import_root_str in sys.path:
            sys.path.remove(import_root_str)
        sys.path.insert(0, import_root_str)
    spec = importlib.util.spec_from_file_location(
        "setup_and_pack_nix_pack_fluxonkv_pylib_bridge_prebuilt_test",
        SCRIPT_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PACKMOD = _load_module()


class BridgePrebuiltAuthorityMaterializationTest(unittest.TestCase):
    def test_host_side_materialization_only_creates_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            build_root = Path(tmpdir)
            target_root = build_root / "fluxon_rs" / "target"
            target_root.mkdir(parents=True)

            spec = types.SimpleNamespace(
                profile_source=types.SimpleNamespace(
                    source_kind="bridge_prebuilt",
                    build_root_path=str(build_root),
                    closed_sdk_search_roots=(),
                ),
                profile_layout=types.SimpleNamespace(
                    target_support_dir_names=("meson-0.64.0",),
                ),
                base_system="manylinux_2_28",
            )
            runtime_target = types.SimpleNamespace(runtime_abi_key="manylinux_2_28_x86_64_cpython3.10")
            selected_backend_plan = {
                "external_mounts": (
                    {
                        "name": "vendor_runtime",
                        "project_relative_path": "fluxon_rs/target/vendor_runtime",
                    },
                ),
                "rdma_backend": "closed_sdk",
                "shared_native_input_object_ids": ("cxxpacked", "vendorRuntime"),
                "native_object_id": "nativeRuntime",
            }

            with mock.patch.object(
                _PACKMOD,
                "_load_prepare_target_dir_names",
                return_value=("vendor_runtime", "cxxpacked"),
            ), mock.patch.object(
                _PACKMOD,
                "_required_native_dir_names",
                return_value=("cxxpacked", "vendor_runtime", "native_runtime"),
            ), mock.patch.object(
                _PACKMOD.subprocess,
                "run",
            ) as run_mock:
                _PACKMOD._materialize_bridge_prebuilt_external_mount_authorities(
                    spec=spec,
                    runtime_target=runtime_target,
                    transport_backend="tcp_thread",
                    selected_backend_plan=selected_backend_plan,
                )

            for argv in (call.args[0] for call in run_mock.call_args_list if call.args):
                if not isinstance(argv, list):
                    continue
                self.assertNotIn("pub_prepare_build.py", " ".join(str(part) for part in argv))
            self.assertTrue((target_root / "vendor_runtime" / "lib").is_dir())
            self.assertTrue((target_root / "vendor_runtime" / "etc" / "libibverbs.d").is_dir())
            self.assertTrue((target_root / "cxxpacked" / "lib64" / "libibverbs").is_dir())
            self.assertTrue((target_root / "meson-0.64.0").is_dir())

    def test_prepare_target_support_dirs_use_writable_target_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target_cache_dir = root / "cache"
            profile_dir = root / "profile"
            native_dir = profile_dir / "native"
            target_support_dir = profile_dir / "target_support"
            (native_dir / "native_runtime" / "lib" / "cmake" / "FluxonNative").mkdir(parents=True)
            (target_support_dir / "meson-0.64.0").mkdir(parents=True)
            for file_name in _PACKMOD.FLUXON_NATIVE_EXPORT_FILE_NAMES:
                (native_dir / "native_runtime" / "lib" / "cmake" / "FluxonNative" / file_name).write_text(
                    "stub\n",
                    encoding="utf-8",
                )

            spec = types.SimpleNamespace(
                profile_source=types.SimpleNamespace(source_kind="bridge_prebuilt"),
                profile_layout=types.SimpleNamespace(target_support_dir_names=("meson-0.64.0",)),
                project_root=REPO_ROOT,
            )
            selected_backend_plan = {
                "external_mounts": (),
                "shared_native_input_object_ids": (),
                "native_object_id": "nativeRuntime",
            }

            with mock.patch.object(
                _PACKMOD,
                "_load_prepare_target_dir_names",
                return_value=("vendor_runtime", "cxxpacked"),
            ), mock.patch.object(
                _PACKMOD,
                "_required_native_dir_names",
                return_value=("native_runtime",),
            ), mock.patch.object(
                _PACKMOD,
                "_backend_writable_native_dir_name",
                return_value="native_runtime",
            ), mock.patch.object(
                _PACKMOD,
                "_build_target_cache_descriptor",
                return_value={},
            ):
                _PACKMOD._prepare_target_cache_view(
                    spec=spec,
                    manylinux_cfg={},
                    profile_dir=profile_dir,
                    target_cache_dir=target_cache_dir,
                    target_cache_descriptor={"native_runtime_refs": {}, "native_export_sha256": {}},
                    native_runtime_dir=native_dir,
                    target_support_dir=target_support_dir,
                    selected_backend_plan=selected_backend_plan,
                )

            self.assertTrue((target_cache_dir / "meson-0.64.0").is_dir())
            self.assertFalse((target_cache_dir / "meson-0.64.0").is_symlink())

    def test_direct_bridge_mounts_skip_dynamic_target_support_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            profile_dir = root / "profile"
            workspace_seed_dir = profile_dir / "workspace_seed"
            native_dir = profile_dir / "native"
            target_support_dir = profile_dir / "target_support"
            baseline_dir = profile_dir / "baseline"
            workspace_seed_dir.mkdir(parents=True)
            (native_dir / "native_runtime").mkdir(parents=True)
            (target_support_dir / "meson-0.64.0").mkdir(parents=True)
            baseline_dir.mkdir(parents=True)

            spec = types.SimpleNamespace(
                profile_source=types.SimpleNamespace(source_kind="bridge_prebuilt"),
                profile_layout=types.SimpleNamespace(target_support_dir_names=("meson-0.64.0",)),
                project_root=REPO_ROOT,
            )
            selected_backend_plan = {
                "shared_native_input_object_ids": ("nativeRuntime",),
                "native_object_id": "nativeRuntime",
            }

            with mock.patch.object(
                _PACKMOD,
                "_load_prepare_target_dir_names",
                return_value=("vendor_runtime", "cxxpacked"),
            ):
                mount_paths = _PACKMOD._bridge_prebuilt_direct_mount_paths(
                    spec=spec,
                    profile_dir=profile_dir,
                    published_profile_dir=None,
                    selected_backend_plan=selected_backend_plan,
                )

            mount_path_strings = {str(path) for path in mount_paths}
            self.assertNotIn(str((target_support_dir / "meson-0.64.0").resolve()), mount_path_strings)
            self.assertIn(str(workspace_seed_dir.resolve()), mount_path_strings)
            self.assertIn(str(baseline_dir.resolve()), mount_path_strings)

    def test_build_docker_argv_invokes_container_finalize_script_file(self) -> None:
        selected_backend_plan = {
            "rdma_backend": "closed_sdk",
            "shared_native_input_object_ids": ("cxxpacked",),
            "native_object_id": "nativeRuntime",
            "protoc_object_id": "cxxpacked",
            "path_object_ids": ("cxxpacked",),
            "ld_library_object_ids": ("cxxpacked",),
            "extra_path_dir_names": (),
            "extra_ld_library_dir_names": (),
            "wheel_finalize_steps": (
                {
                    "kind": "add_vendor_runtime",
                    "native_object_id": None,
                    "native_dir_name": None,
                    "relative_subdir": None,
                    "plugin_bundle_name": None,
                    "extra_library_file_names": (),
                },
            ),
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            workspace_mount_dir = root / "workspace"
            target_cache_dir = root / "target-cache"
            release_dir = root / "release"
            profile_dir = root / "profile"
            cargo_registry_dir = root / "cargo-registry"
            cargo_git_dir = root / "cargo-git"
            for path in (
                workspace_mount_dir,
                target_cache_dir,
                release_dir,
                profile_dir,
                cargo_registry_dir,
                cargo_git_dir,
            ):
                path.mkdir(parents=True, exist_ok=True)

            with mock.patch.object(
                _PACKMOD,
                "_container_rust_toolchain_bin_path",
                return_value="/root/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/bin",
            ):
                docker_argv = _PACKMOD._build_docker_argv(
                    spec=types.SimpleNamespace(),
                    runtime_image_ref="builder:latest",
                    manylinux_version="2_28",
                    transport_backend="tcp_thread",
                    selected_backend_plan=selected_backend_plan,
                    workspace_mount_dir=workspace_mount_dir,
                    target_cache_dir=target_cache_dir,
                    release_dir=release_dir,
                    profile_dir=profile_dir,
                    external_mounts=(),
                    extra_host_mount_paths=(),
                    cargo_registry_dir=cargo_registry_dir,
                    cargo_git_dir=cargo_git_dir,
                )

        container_cmd = docker_argv[-1]
        self.assertIn("pack_release_in_container.py", container_cmd)
        self.assertIn("--wheel-finalize-steps-json", container_cmd)
        self.assertNotIn("python3 - <<'PY'", container_cmd)


if __name__ == "__main__":
    unittest.main()
