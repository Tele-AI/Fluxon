#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from _common import call, load_case_config_payload


TEST_REQUIREMENTS = ["etcd", "kv-cluster", "ops"]
SCENE_ID = "ci_top_attention_mq_mpmc_bench"
SCRIPT_COMMANDS = [
    (
        "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench.py",
        (
            "--producer-count",
            "4",
            "--consumer-counts",
            "2",
            "--duration-seconds",
            "60",
            "--sample-start-seconds",
            "10",
            "--sample-duration-seconds",
            "10",
        ),
    ),
    (
        "fluxon_py/tests/test_api_chan_mpmc/test_mpmc_simple_bench2.py",
        (
            "--producer-count",
            "4",
            "--video-messages-per-producer",
            "15",
            "--batch-size",
            "64",
            "--prefetch-num",
            "0",
            "--channel-capacity",
            "128",
        ),
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for heavier MPMC benchmark-style tests."
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
    for script_path, script_args in SCRIPT_COMMANDS:
        rc = call([args.python, "-u", script_path, *script_args], env=None)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
