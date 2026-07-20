#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

from _common import REPO_ROOT, call


TEST_REQUIREMENTS = [
    "docker",
    "etcd",
    "fluxon-pyo3",
    "fluxon-release",
    "ops",
    "submodules",
    "tikv",
]
RCLONE_IMAGE_ENV = "FLUXON_RCLONE_DOCKER_IMAGE_REF"
RCLONE_IMAGE_REF = "rclone/rclone:1.60.1"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Flat index entry for the FluxonFS S3 rclone v1.60.1 integration test."
    )
    parser.add_argument(
        "--python",
        default=os.environ.get("PYTHON", sys.executable),
        help="Python executable used for the integration test.",
    )
    args = parser.parse_args()
    image_ref = os.environ.get(RCLONE_IMAGE_ENV, "").strip()
    if image_ref != RCLONE_IMAGE_REF:
        raise ValueError(
            f"{RCLONE_IMAGE_ENV} must be exactly {RCLONE_IMAGE_REF!r}, got {image_ref!r}"
        )
    return call(
        [
            args.python,
            "-u",
            str(REPO_ROOT / "fluxon_py" / "tests" / "fluxon_fs_rclone_e2e.py"),
            "--rclone-image-ref",
            image_ref,
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
