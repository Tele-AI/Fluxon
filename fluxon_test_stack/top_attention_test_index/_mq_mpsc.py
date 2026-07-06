#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from _common import call, load_case_config_payload


TEST_REQUIREMENTS = ["etcd", "kv-cluster", "ops"]
SCENE_ID = "ci_top_attention_mq_mpsc"
SCRIPT_PATHS = [
    "fluxon_py/tests/test_api_chan_mpsc/test_api_chan_mpsc_base.py",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for MPSC API channel tests."
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
    for script_path in SCRIPT_PATHS:
        rc = call([args.python, "-u", script_path], env=None)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
