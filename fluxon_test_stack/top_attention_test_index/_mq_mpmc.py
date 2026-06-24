#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from _common import load_case_config_payload, run_pytest


TEST_REQUIREMENTS = ["etcd", "kv-cluster", "ops"]
SCENE_ID = "ci_top_attention_mq_mpmc"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for MPMC API channel tests."
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
        "Flat index entry for MPMC API channel tests.",
        [
            "fluxon_py/tests/test_api_chan_mpmc/test_api_chan_mpmc_base.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_api_chan_mpmc_quick_and_weighted_consume.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_ready_channels_access.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_rebind_client.py",
        ],
        passthrough=passthrough,
        python=args.python,
    )


if __name__ == "__main__":
    raise SystemExit(main())
