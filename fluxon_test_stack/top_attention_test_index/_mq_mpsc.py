#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from _common import load_case_config_payload, run_pytest


TEST_REQUIREMENTS = ["etcd", "kv-cluster", "ops"]
SCENE_ID = "ci_top_attention_mq_mpsc"


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
    args, passthrough = parser.parse_known_args()
    if args.case_config:
        load_case_config_payload(Path(args.case_config).resolve(), expected_scene_id=SCENE_ID)
    return run_pytest(
        "Flat index entry for MPSC API channel tests.",
        ["fluxon_py/tests/test_api_chan_mpsc/test_api_chan_mpsc_base.py"],
        passthrough=passthrough,
        python=args.python,
    )


if __name__ == "__main__":
    raise SystemExit(main())
