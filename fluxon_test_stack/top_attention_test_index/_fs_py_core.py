#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest



def main() -> int:
    return run_pytest(
        "Flat index entry for Fluxon FS Python core tests.",
        [
            "fluxon_py/tests/test_fluxon_fs_config_types.py",
            "fluxon_py/tests/test_fluxon_fs_patcher.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
