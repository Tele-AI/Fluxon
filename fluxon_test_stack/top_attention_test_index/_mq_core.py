#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest



def main() -> int:
    return run_pytest(
        "Flat index entry for non-Ctrl-C MQ tests.",
        [
            "fluxon_py/tests/test_mq/test_capacity_and_auto_clean.py",
            "fluxon_py/tests/test_mq/test_payload_lease_error.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
