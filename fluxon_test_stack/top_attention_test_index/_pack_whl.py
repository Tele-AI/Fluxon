#!/usr/bin/env python3
from __future__ import annotations

from _common import REPO_ROOT, call, parse_python_passthrough



def main() -> int:
    python, passthrough = parse_python_passthrough(
        description="Flat index entry for the existing pure Python wheel pack smoke."
    )
    return call([
        python,
        str(REPO_ROOT / "setup_and_pack" / "pack_fluxon_pylib.py"),
        *passthrough,
    ])


if __name__ == "__main__":
    raise SystemExit(main())
