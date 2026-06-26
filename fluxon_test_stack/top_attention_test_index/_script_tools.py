#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


TEST_REQUIREMENTS = ["ops"]
TEST_PATHS = [
    "setup_and_pack/tests/test_rclone_dist.py",
    "setup_and_pack/tests/test_rclone_sequential.py",
    "setup_and_pack/tests/test_roundrobin_buckets.py",
    "setup_and_pack/tests/test_scan_dir_size_progress.py",
]
DESCRIPTION = "Flat index entry for script utility tests."


def main() -> int:
    for path in TEST_PATHS:
        rc = run_python_file(DESCRIPTION, path)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
