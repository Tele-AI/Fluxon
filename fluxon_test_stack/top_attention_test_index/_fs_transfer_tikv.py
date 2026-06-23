#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest



def main() -> int:
    return run_pytest(
        "Flat index entry for heavier Fluxon FS transfer integration tests.",
        [
            "fluxon_py/tests/test_fluxon_fs_transfer_scan_only_tikv.py",
            "fluxon_py/tests/test_fluxon_fs_transfer_whole_tikv.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
