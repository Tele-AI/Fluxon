#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_files



def main() -> int:
    return run_python_files(
        "Flat index entry for Python runtime/process tests.",
        [
            "fluxon_py/tests/test_process_runner.py",
            "fluxon_py/tests/test_backend_fallback_close.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
