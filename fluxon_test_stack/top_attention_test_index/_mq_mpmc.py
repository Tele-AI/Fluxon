#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest



def main() -> int:
    return run_pytest(
        "Flat index entry for MPMC API channel tests.",
        [
            "fluxon_py/tests/test_api_chan_mpmc/test_api_chan_mpmc_base.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_api_chan_mpmc_quick_and_weighted_consume.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_ready_channels_access.py",
            "fluxon_py/tests/test_api_chan_mpmc/test_rebind_client.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
