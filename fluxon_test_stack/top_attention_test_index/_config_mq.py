#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest


TEST_REQUIREMENTS = ["etcd", "fluxon-pyo3", "kv-cluster", "ops", "submodules"]
SCENE_ID = "ci_top_attention_config_mq"


def main() -> int:
    return run_pytest(
        "Flat index entry for existing MQ config/capacity semantic tests.",
        [
            "fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py",
            "fluxon_py/tests/test_mq/test_payload_lease_error.py",
        ],
        expected_scene_id=SCENE_ID,
    )


if __name__ == "__main__":
    raise SystemExit(main())
