#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest



def main() -> int:
    return run_pytest(
        "Flat index entry for MPSC API channel tests.",
        ["fluxon_py/tests/test_api_chan_mpsc/test_api_chan_mpsc_base.py"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
