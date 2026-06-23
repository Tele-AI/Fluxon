#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_files



def main() -> int:
    return run_python_files(
        "Flat index entry for script utility tests.",
        [
            "setup_and_pack/tests/test_rclone_dist.py",
            "setup_and_pack/tests/test_rclone_sequential.py",
            "setup_and_pack/tests/test_roundrobin_buckets.py",
            "setup_and_pack/tests/test_scan_dir_size_progress.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
