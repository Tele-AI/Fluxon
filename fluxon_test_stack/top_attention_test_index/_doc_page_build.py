#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from _common import REPO_ROOT, call, load_case_config


TEST_REQUIREMENTS = ["ops"]
SCENE_ID = "ci_top_attention_doc_page_build"


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
        "--case-config",
        required=True,
        help="Canonical CI case config YAML emitted by test_runner.",
    )
    args, passthrough = parser.parse_known_args()
    scene_config = load_case_config(args.case_config, expected_scene_id=SCENE_ID)
    base_url = str(scene_config.get("doc_site_base_url") or "").strip()
    if not base_url:
        raise ValueError("scene_config.doc_site_base_url must be set")
    env = os.environ.copy()
    env["FLUXON_DOC_SITE_BASE_URL"] = base_url
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
