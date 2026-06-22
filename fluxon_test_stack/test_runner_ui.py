#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import test_runner


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Long-running UI service for test_runner suite history, logs, and GitOps state."
    )
    parser.add_argument(
        "--workdir",
        "-w",
        required=True,
        help="UI service workdir; if relative, resolve against the repo root inferred from this script path",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind host for the UI service.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=18080,
        help="Bind port for the UI service.",
    )
    parser.add_argument(
        "--history-lookback-days",
        type=int,
        default=test_runner._TEST_RUNNER_UI_DEFAULT_LOOKBACK_DAYS,
        help="How many recent days of suite history to show.",
    )
    parser.add_argument(
        "--history-root",
        dest="history_roots",
        action="append",
        default=[],
        help="Additional suite history root to scan; may be passed multiple times.",
    )
    parser.add_argument(
        "--gitops-config",
        required=False,
        help="Optional GitOps config YAML owned by this UI service.",
    )
    args = parser.parse_args()

    workdir_root = test_runner._resolve_repo_root_cli_path(
        raw_path=Path(args.workdir),
        field_name="workdir",
    )
    test_runner._redirect_process_stdio_to_log(
        workdir_root,
        filename="test_runner_ui.log",
    )
    gitops_cfg_path = None
    if args.gitops_config:
        gitops_cfg_path = test_runner._resolve_repo_root_cli_path(
            raw_path=Path(args.gitops_config),
            field_name="gitops_config",
        )
    test_runner.run_ui_service(
        workdir_root=workdir_root,
        host=str(args.host),
        port=int(args.port),
        lookback_days=int(args.history_lookback_days),
        extra_history_roots=test_runner._resolve_history_roots_cli_paths(args.history_roots),
        gitops_config_path=gitops_cfg_path,
    )


if __name__ == "__main__":
    main()
