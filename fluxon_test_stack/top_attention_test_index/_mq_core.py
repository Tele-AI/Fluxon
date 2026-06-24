#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from _common import call, load_case_config_payload


TEST_REQUIREMENTS = ["etcd", "fluxon-pyo3", "kv-cluster", "ops", "submodules"]
SCENE_ID = "ci_top_attention_mq_core"
TEST_PATHS = [
    "fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py",
    "fluxon_py/tests/test_mq/test_payload_lease_error.py",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for non-Ctrl-C MQ script tests."
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
    args = parser.parse_args()
    if args.case_config:
        load_case_config_payload(Path(args.case_config).resolve(), expected_scene_id=SCENE_ID)
    for test_path in TEST_PATHS:
        rc = call([args.python, "-u", str((Path(__file__).resolve().parents[2] / test_path))])
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
