#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from _common import load_case_config_payload, run_python_file


TEST_REQUIREMENTS = ["etcd", "fluxon-pyo3", "fluxon-release", "greptime", "ops", "submodules"]
SCENE_ID = "ci_top_attention_ctrl_c_mq"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for existing MQ Ctrl-C integration coverage."
    )
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON", sys.executable),
        help="Python executable used for the delegated command.",
    )
    parser.add_argument(
        "--case-config",
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args, passthrough = parser.parse_known_args()
    if args.case_config:
        load_case_config_payload(Path(args.case_config).resolve(), expected_scene_id=SCENE_ID)
    return run_python_file(
        "Flat index entry for existing MQ Ctrl-C integration coverage.",
        "fluxon_py/tests/test_mq/test_example_ctrl_c_exit.py",
        passthrough=passthrough,
        python=args.python,
    )


if __name__ == "__main__":
    raise SystemExit(main())
