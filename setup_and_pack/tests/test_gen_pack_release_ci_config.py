from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "setup_and_pack" / "ci" / "gen_pack_release_ci_config.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("setup_and_pack_ci_gen_pack_release_ci_config_test", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_SCRIPT = _load_module()


class GenPackReleaseCiConfigTest(unittest.TestCase):
    def test_generated_config_overrides_ci_host_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            env_template = root / "pack_fluxonkv_pylib_env.yaml.template"
            output_dir = root / "generated"
            project_data_root = root / "project-data"

            env_template.write_text(
                yaml.safe_dump(
                    {
                        "schema_version": 1,
                        "host_paths": {"root_path": "/tmp/original-store"},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            argv = [
                "gen_pack_release_ci_config.py",
                "--env-template",
                str(env_template),
                "--out-dir",
                str(output_dir),
                "--project-data-root",
                str(project_data_root),
            ]
            old_argv = sys.argv
            try:
                sys.argv = argv
                rc = _SCRIPT.main()
            finally:
                sys.argv = old_argv

            self.assertEqual(rc, 0)
            cfg = yaml.safe_load((output_dir / "pack_fluxonkv_pylib_env.yaml").read_text(encoding="utf-8"))
            self.assertEqual(cfg["host_paths"]["root_path"], str(project_data_root.resolve()))
            self.assertNotIn("manylinux", cfg)


if __name__ == "__main__":
    unittest.main()
