#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from urllib.parse import urlsplit


LOG_SUFFIXES = frozenset({".err", ".log", ".out", ".stderr", ".stdout"})
DIAGNOSTIC_NAMES = frozenset(
    {
        "benchmark_result.json",
        "case_runs.yaml",
        "ci_scene_config.yaml",
        "deploy_result.yaml",
        "exception.txt",
        "exit_code.txt",
        "inflight_attempt.txt",
        "restart_count.txt",
        "result.json",
        "status.yaml",
        "stderr.txt",
        "stdout.txt",
        "summary.yaml",
    }
)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required environment value is missing: {name}")
    return value


def _append_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(value)


def _file_digest(path: Path) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    byte_count = 0
    line_count = 0
    last_byte = b""
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            byte_count += len(chunk)
            line_count += chunk.count(b"\n")
            last_byte = chunk[-1:]
    if byte_count and last_byte != b"\n":
        line_count += 1
    return digest.hexdigest(), byte_count, line_count


def _should_collect(path: Path) -> bool:
    lowered_parts = {part.lower() for part in path.parts}
    return (
        path.name in DIAGNOSTIC_NAMES
        or path.name.endswith("_log_tail.json")
        or path.name.endswith("_log_tail.txt")
        or path.suffix.lower() in LOG_SUFFIXES
        or "logs" in lowered_parts
    )


def _collect_context(args: argparse.Namespace) -> None:
    repo_root = args.repo_root.resolve()
    runner_temp = args.runner_temp.resolve()
    context_root = args.context_root.resolve()
    if (
        context_root == Path(context_root.anchor)
        or context_root == repo_root
        or not context_root.is_relative_to(runner_temp)
    ):
        raise RuntimeError(f"refusing unsafe context root: {context_root}")
    if context_root.exists():
        shutil.rmtree(context_root)
    repository_root = context_root / "repository"
    repository_root.mkdir(parents=True, exist_ok=True)

    source_roots = [
        repo_root / ".dever" / "ci_2_virt_node" / "runner_run",
        repo_root / "setup_and_pack" / "nix" / "runs",
        repo_root / "fluxon_release",
    ]
    copied: list[dict[str, object]] = []
    skipped: list[dict[str, str]] = []

    def copy_text_diagnostic(source: Path, relative: Path) -> None:
        try:
            with source.open("rb") as stream:
                sample = stream.read(65536)
            if b"\0" in sample:
                skipped.append({"source": str(source), "reason": "binary content detected"})
                return

            sha256, byte_count, line_count = _file_digest(source)
            destination = repository_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            copied.append(
                {
                    "source": str(source),
                    "artifact_path": str(destination.relative_to(context_root)),
                    "bytes": byte_count,
                    "lines": line_count,
                    "sha256": sha256,
                }
            )
        except Exception as exc:
            skipped.append(
                {
                    "source": str(source),
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )

    for source_root in source_roots:
        if not source_root.exists():
            skipped.append({"source": str(source_root), "reason": "source root is missing"})
            continue
        for source in sorted(source_root.rglob("*")):
            if not source.is_file() or source.is_symlink() or not _should_collect(source):
                continue
            copy_text_diagnostic(source, source.relative_to(repo_root))

    explicit_files = [
        repo_root / ".github" / "workflows" / "all_test.yml",
        repo_root / ".github" / "codex" / "ci-failure-analysis-prompt.md",
        repo_root / "scripts" / "ci_codex_failure_analysis.py",
        runner_temp / "ci_test_list.ci.yaml",
    ]
    for source in explicit_files:
        if not source.is_file():
            skipped.append({"source": str(source), "reason": "file is missing"})
            continue
        relative = (
            source.relative_to(repo_root)
            if source.is_relative_to(repo_root)
            else Path("runner-temp") / source.name
        )
        copy_text_diagnostic(source, relative)

    manifest = {
        "contract": "Files are copied in full without tail truncation.",
        "source_roots": [str(path) for path in source_roots],
        "copied_file_count": len(copied),
        "copied_total_bytes": sum(int(item["bytes"]) for item in copied),
        "copied": copied,
        "skipped": skipped,
    }
    (context_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(
        f"Collected {manifest['copied_file_count']} complete text diagnostics "
        f"({manifest['copied_total_bytes']} bytes) in {context_root}"
    )
    if skipped:
        print(f"Recorded {len(skipped)} missing, binary, or unreadable paths in manifest.json")


def _validate_openai_base_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    if base_url != base_url.strip() or "\n" in base_url or "\r" in base_url:
        raise ValueError("OPENAI_BASE_URL must not contain surrounding whitespace or newlines")
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("OPENAI_BASE_URL must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("OPENAI_BASE_URL must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("OPENAI_BASE_URL must not contain a query string or fragment")
    if base_url.endswith("/"):
        raise ValueError("OPENAI_BASE_URL must not end with a slash")
    if parsed.path.endswith("/responses"):
        raise ValueError("OPENAI_BASE_URL must not include the /responses endpoint suffix")
    return f"{base_url}/responses"


def _escape_workflow_command(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def _check_api_config(_args: argparse.Namespace) -> None:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    base_url = os.environ.get("OPENAI_BASE_URL", "")
    output_path = Path(_required_env("GITHUB_OUTPUT"))
    summary_path = Path(_required_env("GITHUB_STEP_SUMMARY"))
    if not api_key or not base_url:
        _append_text(output_path, "available=false\n")
        _append_text(
            summary_path,
            "## Codex failure analysis skipped\n"
            "Environment secrets OPENAI_API_KEY and OPENAI_BASE_URL must both be "
            "available for this event.\n",
        )
        return

    responses_endpoint = _validate_openai_base_url(base_url)
    print(f"::add-mask::{_escape_workflow_command(responses_endpoint)}")
    _append_text(output_path, "available=true\n")


def _run_to_file(
    command: list[str],
    *,
    destination: Path,
    error_path: Path,
) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)
    error_path.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as stdout, error_path.open("wb") as stderr:
        result = subprocess.run(command, stdout=stdout, stderr=stderr, check=False)
    if result.returncode == 0:
        error_path.unlink(missing_ok=True)
        return True
    destination.unlink(missing_ok=True)
    if not error_path.exists() or error_path.stat().st_size == 0:
        error_path.write_text(
            f"Command failed with exit code {result.returncode}: {command[0]}\n",
            encoding="utf-8",
        )
    return False


def _fetch_api_json(repository: str, api_path: str, destination: Path, error_path: Path) -> bool:
    return _run_to_file(
        ["gh", "api", f"repos/{repository}/{api_path}"],
        destination=destination,
        error_path=error_path,
    )


def _write_fetch_error(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{message}\n", encoding="utf-8")


def _fetch_test_job_log(
    *,
    repository: str,
    run_id: str,
    destination: Path,
    jobs_json: Path,
) -> None:
    error_path = Path(f"{destination}.fetch-error.txt")
    if not _fetch_api_json(
        repository,
        f"actions/runs/{run_id}/jobs?per_page=100",
        jobs_json,
        error_path,
    ):
        return
    try:
        payload = json.loads(jobs_json.read_text(encoding="utf-8"))
        jobs = payload.get("jobs", [])
        job_id = next(
            (str(job["id"]) for job in jobs if job.get("name") == "ci-2-virt-node"),
            "",
        )
    except (KeyError, TypeError, ValueError) as exc:
        _write_fetch_error(error_path, f"Failed to parse jobs for run {run_id}: {exc}")
        return
    if not job_id:
        _write_fetch_error(error_path, f"Run {run_id} has no ci-2-virt-node job")
        return
    _run_to_file(
        ["gh", "run", "view", run_id, "--repo", repository, "--job", job_id, "--log"],
        destination=destination,
        error_path=error_path,
    )


def _safe_tsv_field(value: object) -> str:
    return str(value if value is not None else "").replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _run_metadata_fields(run: dict[str, object]) -> list[str]:
    return [
        _safe_tsv_field(run.get("id")),
        _safe_tsv_field(run.get("run_number")),
        _safe_tsv_field(run.get("head_sha")),
        _safe_tsv_field(run.get("head_branch")),
        _safe_tsv_field(run.get("event")),
        _safe_tsv_field(run.get("created_at")),
        _safe_tsv_field(run.get("html_url")),
    ]


def _fetch_logs(args: argparse.Namespace) -> None:
    context_root = args.context_root.resolve()
    github_root = context_root / "github"
    history_root = github_root / "history"
    history_root.mkdir(parents=True, exist_ok=True)
    if not (context_root / "current" / "manifest.json").is_file():
        _write_fetch_error(
            github_root / "current-artifact.fetch-error.txt",
            "Current failure-context artifact is missing or could not be downloaded",
        )

    _fetch_api_json(
        args.repository,
        f"actions/runs/{args.current_run_id}",
        github_root / "current-run.json",
        github_root / "current-run.fetch-error.txt",
    )
    _fetch_test_job_log(
        repository=args.repository,
        run_id=args.current_run_id,
        destination=github_root / "current-job.log",
        jobs_json=github_root / "current-jobs.json",
    )

    history_runs_json = github_root / "history-runs.json"
    if not _fetch_api_json(
        args.repository,
        f"actions/workflows/{args.workflow_file}/runs?status=completed&per_page=100",
        history_runs_json,
        github_root / "history-runs.fetch-error.txt",
    ):
        return
    try:
        payload = json.loads(history_runs_json.read_text(encoding="utf-8"))
        raw_runs = payload.get("workflow_runs", [])
        if not isinstance(raw_runs, list):
            raise TypeError("workflow_runs is not a list")
        selected_runs = [
            run
            for run in raw_runs
            if isinstance(run, dict)
            and run.get("conclusion") == "failure"
            and run.get("id") is not None
            and str(run.get("id")) != args.current_run_id
        ][: args.history_failure_limit]
    except (TypeError, ValueError) as exc:
        _write_fetch_error(
            github_root / "history-runs.fetch-error.txt",
            f"Failed to parse workflow history: {exc}",
        )
        return

    selected_history_path = github_root / "selected-history.tsv"
    selected_history_path.write_text(
        "".join("\t".join(_run_metadata_fields(run)) + "\n" for run in selected_runs),
        encoding="utf-8",
    )
    for run in selected_runs:
        run_id = str(run["id"])
        run_root = history_root / run_id
        run_root.mkdir(parents=True, exist_ok=True)
        (run_root / "metadata.tsv").write_text(
            "\t".join(_run_metadata_fields(run)) + "\n",
            encoding="utf-8",
        )
        _fetch_api_json(
            args.repository,
            f"actions/runs/{run_id}",
            run_root / "run.json",
            run_root / "run.fetch-error.txt",
        )
        _fetch_test_job_log(
            repository=args.repository,
            run_id=run_id,
            destination=run_root / "ci-2-virt-node.log",
            jobs_json=run_root / "jobs.json",
        )


def _build_inventory(args: argparse.Namespace) -> None:
    root = args.context_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    inventory_path = root / "inventory.json"
    inventory: list[dict[str, object]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path == inventory_path:
            continue
        sha256, byte_count, line_count = _file_digest(path)
        inventory.append(
            {
                "path": str(path.relative_to(root)),
                "bytes": byte_count,
                "lines": line_count,
                "sha256": sha256,
            }
        )

    payload = {
        "file_count": len(inventory),
        "total_bytes": sum(int(item["bytes"]) for item in inventory),
        "files": inventory,
    }
    inventory_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"Evidence inventory: files={payload['file_count']} bytes={payload['total_bytes']}")


def _render_report(args: argparse.Namespace) -> None:
    report = _required_env("CODEX_REPORT")
    failed_run_url = _required_env("FAILED_RUN_URL")
    api_key = _required_env("OPENAI_API_KEY")
    base_url = _required_env("OPENAI_BASE_URL")
    responses_endpoint = _validate_openai_base_url(base_url)
    for secret_value in sorted({api_key, base_url, responses_endpoint}, key=len, reverse=True):
        report = report.replace(secret_value, "[REDACTED]")

    rendered = (
        "# Codex CI failure analysis\n\n"
        f"Failed run: {failed_run_url}\n\n"
        f"{report}\n"
    )
    output_path = args.output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    _append_text(args.summary.resolve(), rendered)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage Codex CI failure-analysis evidence and reports.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect-context", help="Collect complete text diagnostics.")
    collect.add_argument("--repo-root", type=Path, required=True)
    collect.add_argument("--runner-temp", type=Path, required=True)
    collect.add_argument("--context-root", type=Path, required=True)
    collect.set_defaults(handler=_collect_context)

    check_config = subparsers.add_parser(
        "check-api-config",
        help="Validate required OpenAI environment secrets and mask the derived endpoint.",
    )
    check_config.set_defaults(handler=_check_api_config)

    fetch_logs = subparsers.add_parser(
        "fetch-logs",
        help="Fetch current and historical GitHub Actions failure logs.",
    )
    fetch_logs.add_argument("--repository", required=True)
    fetch_logs.add_argument("--current-run-id", required=True)
    fetch_logs.add_argument("--workflow-file", required=True)
    fetch_logs.add_argument("--history-failure-limit", type=int, default=5)
    fetch_logs.add_argument("--context-root", type=Path, required=True)
    fetch_logs.set_defaults(handler=_fetch_logs)

    build_inventory = subparsers.add_parser(
        "build-inventory",
        help="Build a complete evidence inventory.",
    )
    build_inventory.add_argument("--context-root", type=Path, required=True)
    build_inventory.set_defaults(handler=_build_inventory)

    render_report = subparsers.add_parser(
        "render-report",
        help="Redact configured secrets and write the final report.",
    )
    render_report.add_argument("--output", type=Path, required=True)
    render_report.add_argument("--summary", type=Path, required=True)
    render_report.set_defaults(handler=_render_report)

    args = parser.parse_args()
    if getattr(args, "history_failure_limit", 0) < 0:
        parser.error("--history-failure-limit must be non-negative")
    return args


def main() -> int:
    args = _parse_args()
    try:
        args.handler(args)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
