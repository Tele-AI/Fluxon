#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file



def main() -> int:
    return run_python_file(
        "Flat index entry for test-stack runner contract tests.",
        "fluxon_test_stack/tests/test_runner_contract.py",
    )


if __name__ == "__main__":
    raise SystemExit(main())
