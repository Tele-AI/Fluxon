#!/usr/bin/env python3
from __future__ import annotations

from _common import run_python_files



def main() -> int:
    return run_python_files(
        "Flat index entry for deployment codegen tests.",
        [
            "deployment/tests/test_gen_bare_deploy_bash.py",
            "deployment/tests/test_gen_k8s_daemonset.py",
            "deployment/tests/test_selection_supervisor_codegen.py",
            "deployment/tests/test_start_test_bed_bootstrap_log.py",
            "deployment/tests/test_start_test_bed_deploy_payload.py",
        ],
    )


if __name__ == "__main__":
    raise SystemExit(main())
