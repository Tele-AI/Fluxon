#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


TEST_REQUIREMENTS = ["ops"]
SCENE_ID = "ci_top_attention_config_fs"


def main() -> int:
    return run_python_file(
        "Flat index entry for FS Python config/schema tests.",
        "fluxon_py/tests/test_fluxon_fs_config_types.py",
        expected_scene_id=SCENE_ID,
    )


if __name__ == "__main__":
    raise SystemExit(main())
