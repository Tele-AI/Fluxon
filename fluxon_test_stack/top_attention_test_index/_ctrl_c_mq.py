#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


SCENE_ID = "ci_top_attention_ctrl_c_mq"


def main() -> int:
    return run_python_file(
        "Flat index entry for existing MQ Ctrl-C integration coverage.",
        "fluxon_py/tests/test_mq/test_example_ctrl_c_exit.py",
        expected_scene_id=SCENE_ID,
    )


if __name__ == "__main__":
    raise SystemExit(main())
