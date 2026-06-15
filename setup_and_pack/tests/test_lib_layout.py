from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
LIB_LAYOUT_PATH = REPO_ROOT / "setup_and_pack" / "nix" / "lib_layout.py"


def _load_lib_layout():
    spec = importlib.util.spec_from_file_location("setup_and_pack_nix_lib_layout_test", LIB_LAYOUT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_LIB_LAYOUT = _load_lib_layout()


class ApplyLayoutTest(unittest.TestCase):
    def test_bridge_prebuilt_materializes_workspace_seed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            project_root = REPO_ROOT

            build_root = root / "build_root"
            native_root = build_root / "fluxon_rs" / "target"
            (native_root / "cxxpacked").mkdir(parents=True)
            (native_root / "meson-0.64.0").mkdir(parents=True)

            baseline_dir = root / "baseline"
            baseline_dir.mkdir()
            project_data_root = root / "project_data"
            config_path = root / "pack_config.yaml"
            config_path.write_text("schema_version: 1\n", encoding="utf-8")

            spec = _LIB_LAYOUT.ExperimentSpec(
                config_path=config_path,
                project_root=project_root,
                project_data_root=project_data_root,
                base_system="manylinux_2_28",
                architectures=("x86_64",),
                python_abi="cpython3.10",
                profile_name="current",
                assembly_name="cold_start",
                instance_id="cold_start",
                target_cache_namespace="cold_start",
                profile_source=_LIB_LAYOUT.ProfileSource(
                    source_kind=_LIB_LAYOUT.PROFILE_SOURCE_KIND_BRIDGE_PREBUILT,
                    profile_path=None,
                    build_root_path=str(build_root),
                ),
                profile_layout=_LIB_LAYOUT.ProfileLayoutSpec(
                    native_runtime_dir_names=("cxxpacked",),
                    target_support_dir_names=("meson-0.64.0",),
                    ext_bundle_dir_name="cxxpacked",
                ),
                assembly_refs=_LIB_LAYOUT.AssemblyRefs(baseline_path=str(baseline_dir)),
            )
            runtime_target = _LIB_LAYOUT.RuntimeTarget(
                execution_substrate="manylinux_container",
                base_system_key="manylinux_2_28_x86_64",
                runtime_abi_key="manylinux_2_28_x86_64_cpython3.10",
                architecture="x86_64",
                python_abi="cpython3.10",
                profile_name="current",
                assembly_name="cold_start",
                instance_id="cold_start",
            )

            layout = _LIB_LAYOUT.build_layout(spec=spec, runtime_target=runtime_target)
            _LIB_LAYOUT.apply_layout(spec=spec, runtime_target=runtime_target, layout=layout)

            workspace_seed_dir = layout.assembly_profile_dir / "workspace_seed"
            self.assertTrue(workspace_seed_dir.is_dir())
            self.assertTrue((workspace_seed_dir / "setup_and_pack/closed_sdk_contract.py").is_file())
            self.assertTrue((workspace_seed_dir / "setup_and_pack/public_workspace_contract.py").is_file())
            self.assertTrue((workspace_seed_dir / "fluxon_rs/fluxon_commu_contract/Cargo.toml").is_file())
            self.assertTrue((workspace_seed_dir / "fluxon_rs/fluxon_commu/Cargo.toml").is_file())
            self.assertTrue((workspace_seed_dir / "fluxon_release/closed_sdk/manifest.json").is_file())
            self.assertTrue((workspace_seed_dir / "setup_and_pack/nix/pack_fluxonkv_pylib.py").is_file())
            self.assertTrue((workspace_seed_dir / "setup_and_pack/utils/__init__.py").is_file())
            self.assertTrue((workspace_seed_dir / "setup_and_pack/utils/sudo_prefix_utils.py").is_file())
            self.assertTrue((workspace_seed_dir / "setup_and_pack/utils/wheel_runtime_helper.py").is_file())
            self.assertTrue((workspace_seed_dir / "fluxon_rs/fluxon_kv/Cargo.toml").is_file())
            self.assertTrue((workspace_seed_dir / "fluxon_rs/Cargo.lock").is_file())
            self.assertTrue((workspace_seed_dir / "fluxon_rs/moka/Cargo.toml").is_file())
            self.assertTrue(layout.profile_link.is_symlink())
            self.assertEqual(layout.profile_link.resolve(), layout.assembly_profile_dir.resolve())

    def test_load_experiment_spec_from_root_parses_closed_sdk_search_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            closed_sdk_root = root / "closed_sdk_roots"
            closed_sdk_root.mkdir()

            spec = _LIB_LAYOUT.load_experiment_spec_from_root(
                config_path=(REPO_ROOT / "setup_and_pack" / "nix" / "pack_fluxonkv_pylib.yaml").resolve(),
                config_root={
                    "store": {
                        "project_data_root": str((root / "project_data").resolve()),
                    },
                    "runtime": {
                        "base_system": "manylinux_2_28",
                        "architectures": ["x86_64"],
                        "python_abi": "cpython3.10",
                    },
                    "profile": {
                        "source_kind": "bridge_prebuilt",
                        "native_runtime_dir_names": ["cxxpacked"],
                        "target_support_dir_names": ["meson-0.64.0"],
                        "ext_bundle_dir_name": "cxxpacked",
                        "closed_sdk_search_roots": [str(closed_sdk_root)],
                    },
                    "assembly": {
                        "baseline_path": str((root / "baseline").resolve()),
                    },
                },
            )

            self.assertEqual(
                spec.profile_source.closed_sdk_search_roots,
                (str(closed_sdk_root.resolve()),),
            )


if __name__ == "__main__":
    unittest.main()
