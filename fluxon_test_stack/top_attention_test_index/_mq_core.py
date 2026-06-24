#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _common import call, load_case_config_payload, parse_python_passthrough


TEST_REQUIREMENTS = ["etcd", "fluxon-pyo3", "kv-cluster", "ops", "submodules"]
SCENE_ID = "ci_top_attention_mq_core"
TEST_PATHS = [
    "fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py",
    "fluxon_py/tests/test_mq/test_payload_lease_error.py",
]


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--case-config",
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args, _ = parser.parse_known_args()
    if args.case_config:
        load_case_config_payload(Path(args.case_config).resolve(), expected_scene_id=SCENE_ID)
    python, passthrough = parse_python_passthrough("Flat index entry for non-Ctrl-C MQ tests.")
    filtered_passthrough: list[str] = []
    idx = 0
    while idx < len(passthrough):
        token = passthrough[idx]
        if token == "--case-config":
            idx += 2
            continue
        if token.startswith("--case-config="):
            idx += 1
            continue
        filtered_passthrough.append(token)
        idx += 1
    for test_path in TEST_PATHS:
        rc = call([python, "-u", str((Path(__file__).resolve().parents[2] / test_path)), *filtered_passthrough])
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
