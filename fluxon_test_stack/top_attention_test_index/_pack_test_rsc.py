#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_file



def main() -> int:
    return run_python_file(
        "Flat index entry for preparing the test-stack release/test_rsc authority.",
        "fluxon_test_stack/pack_test_stack_rsc.py",
    )


if __name__ == "__main__":
    raise SystemExit(main())
