#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from _common import REPO_ROOT, call


TEST_REQUIREMENTS = ["ops"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for the documentation page build."
    )
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON", sys.executable),
        help="Python executable used for the delegated command.",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("FLUXON_DOC_SITE_BASE_URL"),
        help="Doc site base URL forwarded through FLUXON_DOC_SITE_BASE_URL.",
    )
    args, passthrough = parser.parse_known_args()
    env = None
    if args.base_url is not None:
        env = os.environ.copy()
        env["FLUXON_DOC_SITE_BASE_URL"] = args.base_url
    return call(
        [
            args.python,
            str(REPO_ROOT / "scripts" / "build_doc_site.py"),
            "build",
            *passthrough,
        ],
        env=env,
    )


if __name__ == "__main__":
    raise SystemExit(main())
