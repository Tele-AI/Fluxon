"""Canonical Python distribution and release-wheel names."""

from __future__ import annotations

import re


PYTHON_DISTRIBUTION_NAME = "fluxon-ai"
PYTHON_WHEEL_DISTRIBUTION = re.sub(r"[-_.]+", "_", PYTHON_DISTRIBUTION_NAME)
RELEASE_WHEEL_GLOB = f"{PYTHON_WHEEL_DISTRIBUTION}-*.whl"

