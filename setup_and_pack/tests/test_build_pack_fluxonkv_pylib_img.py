from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
BASE_CONFIG_PATH = REPO_ROOT / "setup_and_pack" / "build_pack_fluxonkv_pylib_img" / "pypack_builder_manylinux_2_28.yaml"


class BuildPackFluxonkvPylibImgConfigTest(unittest.TestCase):
    def test_manylinux_builder_only_bootstraps_existing_abi3_interpreters(self) -> None:
        cfg = yaml.safe_load(BASE_CONFIG_PATH.read_text(encoding="utf-8"))
        script_installs = cfg["heavy_setup"]["script_installs"]

        step_names = [step["name"] for step in script_installs]
        self.assertNotIn("install_maturin_cp38", step_names)
        self.assertNotIn("install_maturin_cp39", step_names)
        self.assertIn("install_maturin_cp310", step_names)
        self.assertIn("install_maturin_cp311", step_names)
        self.assertIn("install_maturin_cp312", step_names)

        command_text = "\n".join(
            cmd
            for step in script_installs
            for cmd in step.get("commands", [])
        )
        self.assertNotIn("/opt/python/cp38-cp38/bin/pip", command_text)
        self.assertNotIn("/opt/python/cp39-cp39/bin/pip", command_text)
        self.assertIn("/opt/python/cp310-cp310/bin/pip", command_text)
        self.assertIn("/opt/python/cp311-cp311/bin/pip", command_text)
        self.assertIn("/opt/python/cp312-cp312/bin/pip", command_text)


if __name__ == "__main__":
    unittest.main()
