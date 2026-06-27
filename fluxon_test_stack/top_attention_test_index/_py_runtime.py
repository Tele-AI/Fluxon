#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file


TEST_REQUIREMENTS = ["ops"]
TEST_PATHS = [
    "fluxon_py/tests/test_process_runner.py",
    "fluxon_py/tests/test_backend_fallback_close.py",
]
DESCRIPTION = "Flat index entry for Python runtime/process tests."


def main() -> int:
    for path in TEST_PATHS:
        rc = run_python_file(DESCRIPTION, path)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
