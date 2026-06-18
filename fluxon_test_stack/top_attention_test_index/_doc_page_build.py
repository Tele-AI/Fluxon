#!/usr/bin/env python3
from __future__ import annotations

from _common import REPO_ROOT, call, parse_python_passthrough


TEST_REQUIREMENTS = ["ops"]


def main() -> int:
    python, passthrough = parse_python_passthrough(
        description="Flat index entry for the documentation page build."
    )
    return call(
        [
            python,
            str(REPO_ROOT / "scripts" / "build_doc_site.py"),
            "build",
            *passthrough,
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
