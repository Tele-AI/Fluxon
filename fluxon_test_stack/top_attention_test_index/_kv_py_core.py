#!/usr/bin/env python3
from __future__ import annotations

from _common import run_pytest



def main() -> int:
    return run_pytest(
        "Flat index entry for Python KV backend core smoke tests.",
        [
            "fluxon_py/tests/test_backend.py",
            "fluxon_py/tests/test_backend_fallback_close.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
