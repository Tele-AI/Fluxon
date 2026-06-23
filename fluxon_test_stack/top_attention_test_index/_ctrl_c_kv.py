#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


TEST_REQUIREMENTS = ["ops"]
SCENE_ID = "ci_top_attention_ctrl_c_kv"


def main() -> int:
    return run_python_file(
        "Flat index entry for existing KV/runtime Ctrl-C shutdown coverage.",
        "fluxon_py/tests/test_process_runner.py",
        ["TestProcessRunner.test_wait_subproc_or_ctrlc_retires_children_on_sigterm"],
        expected_scene_id=SCENE_ID,
    )


if __name__ == "__main__":
    raise SystemExit(main())
