#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit
import zipfile


LOG_SUFFIXES = frozenset({".err", ".log", ".out", ".stderr", ".stdout"})
TESTBED_HOSTWORKDIR_DIRNAME = "fluxon_deploy"
TESTBED_SERVICE_LOG_NAMES = ("etcd", "greptime", "ops_controller")
DIAGNOSTIC_NAMES = frozenset(
    {
        "benchmark_config.py",
        "benchmark_result.json",
        "case_runs.yaml",
        "ci_scene_config.yaml",
        "deploy_result.yaml",
        "exception.txt",
        "exit_code.txt",
        "failure.json",
        "inflight_attempt.txt",
        "processes.json",
        "resource_samples.jsonl",
        "restart_count.txt",
        "result.json",
        "run_plan.json",
        "status.yaml",
        "stderr.txt",
        "stdout.txt",
        "summary.json",
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


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True),
        encoding="utf-8",
    )


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
        or (
            "configs" in lowered_parts
            and path.suffix.lower() in {".yaml", ".yml"}
        )
    )


def _testbed_service_log_name(path: Path) -> str | None:
    """Return the canonical service name for a collected testbed log."""
    if path.parent.name.lower() != "log" or path.suffix.lower() != ".log":
        return None
    service_name = path.name.split(".", 1)[0].lower()
    if service_name not in TESTBED_SERVICE_LOG_NAMES:
        return None
    return service_name


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
        repo_root / ".dever" / "ci_2_virt_node" / "start_test_bed",
        repo_root / ".dever" / "ci_large_scale_mq",
        repo_root / "setup_and_pack" / "nix" / "runs",
        repo_root / "fluxon_release",
    ]
    testbed_service_log_root = runner_temp / TESTBED_HOSTWORKDIR_DIRNAME
    testbed_service_log_counts = {
        service_name: 0 for service_name in TESTBED_SERVICE_LOG_NAMES
    }
    copied: list[dict[str, object]] = []
    skipped: list[dict[str, str]] = []

    def copy_text_diagnostic(source: Path, relative: Path) -> bool:
        try:
            with source.open("rb") as stream:
                sample = stream.read(65536)
            if b"\0" in sample:
                skipped.append({"source": str(source), "reason": "binary content detected"})
                return False

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
            return True
        except Exception as exc:
            skipped.append(
                {
                    "source": str(source),
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )
            return False

    for source_root in source_roots:
        if not source_root.exists():
            skipped.append({"source": str(source_root), "reason": "source root is missing"})
            continue
        for source in sorted(source_root.rglob("*")):
            if not source.is_file() or source.is_symlink() or not _should_collect(source):
                continue
            copy_text_diagnostic(source, source.relative_to(repo_root))

    if not testbed_service_log_root.exists():
        skipped.append(
            {
                "source": str(testbed_service_log_root),
                "reason": "testbed hostworkdir root is missing",
            }
        )
    else:
        testbed_service_log_candidates = {
            source
            for pattern in ("log/*.log", "*/log/*.log")
            for source in testbed_service_log_root.glob(pattern)
        }
        for source in sorted(testbed_service_log_candidates):
            if not source.is_file() or source.is_symlink():
                continue
            service_name = _testbed_service_log_name(source)
            if service_name is None:
                continue
            relative = Path("runner-temp") / source.relative_to(runner_temp)
            if copy_text_diagnostic(source, relative):
                testbed_service_log_counts[service_name] += 1

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
        "testbed_service_logs": {
            "contract": (
                "Complete etcd, GreptimeDB, and Ops controller service stdout/stderr "
                "logs are copied; service data directories are excluded."
            ),
            "source_root": str(testbed_service_log_root),
            "services": {
                service_name: {
                    "copied_file_count": testbed_service_log_counts[service_name]
                }
                for service_name in TESTBED_SERVICE_LOG_NAMES
            },
        },
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
    print(
        "Collected testbed service logs: "
        + " ".join(
            f"{service_name}={testbed_service_log_counts[service_name]}"
            for service_name in TESTBED_SERVICE_LOG_NAMES
        )
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


def _fetch_api_url(api_url: str, destination: Path, error_path: Path) -> bool:
    return _run_to_file(
        ["gh", "api", api_url],
        destination=destination,
        error_path=error_path,
    )


def _write_fetch_error(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{message}\n", encoding="utf-8")


def _safe_extract_zip(archive_path: Path, destination: Path) -> None:
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    destination_root = destination.resolve()
    with zipfile.ZipFile(archive_path) as archive:
        for info in archive.infolist():
            relative = PurePosixPath(info.filename)
            if relative.is_absolute() or not relative.parts or ".." in relative.parts:
                raise ValueError(f"unsafe artifact archive path: {info.filename!r}")
            if stat.S_ISLNK(info.external_attr >> 16):
                raise ValueError(f"artifact archive contains a symlink: {info.filename!r}")
            target = destination.joinpath(*relative.parts)
            resolved_target = target.resolve()
            if not resolved_target.is_relative_to(destination_root):
                raise ValueError(f"artifact archive path escapes destination: {info.filename!r}")
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)


def _artifact_api_result(repository: str, run_id: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            "gh",
            "api",
            f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def _download_current_context_artifact(
    *,
    repository: str,
    run_id: str,
    artifact_name: str,
    context_root: Path,
    github_root: Path,
    wait_seconds: float,
    poll_seconds: float,
) -> bool:
    status_path = github_root / "current-artifact-status.json"
    error_path = github_root / "current-artifact.fetch-error.txt"
    deadline = time.monotonic() + wait_seconds
    attempts = 0
    last_error = "artifact was not listed"
    while True:
        attempts += 1
        result = _artifact_api_result(repository, run_id)
        artifact: dict[str, object] | None = None
        if result.returncode == 0:
            try:
                payload = json.loads(result.stdout.decode("utf-8"))
                artifacts = payload.get("artifacts", [])
                if not isinstance(artifacts, list):
                    raise TypeError("artifacts is not a list")
                matches = [
                    item
                    for item in artifacts
                    if isinstance(item, dict)
                    and item.get("name") == artifact_name
                    and not item.get("expired", False)
                    and item.get("id") is not None
                ]
                if matches:
                    artifact = max(matches, key=lambda item: int(item["id"]))
                else:
                    last_error = "artifact was not listed"
            except (TypeError, ValueError) as exc:
                last_error = f"failed to parse artifact listing: {exc}"
        else:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            last_error = detail or f"artifact listing exited with {result.returncode}"

        if artifact is not None:
            artifact_id = str(artifact["id"])
            with tempfile.TemporaryDirectory(prefix="codex-artifact-", dir=github_root) as raw_tmp:
                archive_path = Path(raw_tmp) / "artifact.zip"
                download_error = Path(raw_tmp) / "download-error.txt"
                downloaded = _run_to_file(
                    [
                        "gh",
                        "api",
                        f"repos/{repository}/actions/artifacts/{artifact_id}/zip",
                    ],
                    destination=archive_path,
                    error_path=download_error,
                )
                if downloaded:
                    try:
                        _safe_extract_zip(archive_path, context_root / "current")
                    except (OSError, ValueError, zipfile.BadZipFile) as exc:
                        last_error = f"failed to extract artifact: {type(exc).__name__}: {exc}"
                    else:
                        error_path.unlink(missing_ok=True)
                        _write_json(
                            status_path,
                            {
                                "artifact_id": artifact["id"],
                                "artifact_name": artifact_name,
                                "attempts": attempts,
                                "size_in_bytes": artifact.get("size_in_bytes"),
                                "status": "downloaded",
                            },
                        )
                        print(
                            f"Downloaded current failure context artifact after {attempts} attempt(s)"
                        )
                        return True
                else:
                    last_error = download_error.read_text(
                        encoding="utf-8", errors="replace"
                    ).strip()

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(poll_seconds, remaining))

    _write_fetch_error(
        error_path,
        f"Failed to download artifact {artifact_name!r} after {attempts} attempt(s): {last_error}",
    )
    _write_json(
        status_path,
        {
            "artifact_name": artifact_name,
            "attempts": attempts,
            "reason": last_error,
            "status": "unavailable",
        },
    )
    print(f"Current failure context artifact is unavailable after {attempts} attempt(s)")
    return False


def _extract_job_log_from_run_archive(
    *,
    repository: str,
    run_id: str,
    job_name: str,
    destination: Path,
) -> bool:
    error_path = destination.with_name(f"{destination.stem}.run-archive.fetch-error.txt")
    with tempfile.TemporaryDirectory(prefix="codex-run-logs-") as raw_tmp:
        archive_path = Path(raw_tmp) / "run-logs.zip"
        if not _run_to_file(
            ["gh", "api", f"repos/{repository}/actions/runs/{run_id}/logs"],
            destination=archive_path,
            error_path=error_path,
        ):
            return False
        try:
            with zipfile.ZipFile(archive_path) as archive:
                expected_suffix = f"_{job_name}.txt"
                candidates = [
                    info
                    for info in archive.infolist()
                    if "/" not in info.filename and info.filename.endswith(expected_suffix)
                ]
                if len(candidates) != 1:
                    raise ValueError(
                        f"expected one combined log for job {job_name!r}, found {len(candidates)}"
                    )
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(candidates[0]) as source, destination.open("wb") as output:
                    shutil.copyfileobj(source, output)
        except (OSError, ValueError, zipfile.BadZipFile) as exc:
            destination.unlink(missing_ok=True)
            _write_fetch_error(
                error_path,
                f"Failed to extract job log from run archive: {type(exc).__name__}: {exc}",
            )
            return False
    error_path.unlink(missing_ok=True)
    return True


def _fetch_test_job_log(
    *,
    repository: str,
    run_id: str,
    job_name: str,
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
        if not isinstance(jobs, list):
            raise TypeError("jobs is not a list")
        job = next(
            (job for job in jobs if isinstance(job, dict) and job.get("name") == job_name),
            None,
        )
    except (KeyError, TypeError, ValueError) as exc:
        _write_fetch_error(error_path, f"Failed to parse jobs for run {run_id}: {exc}")
        return
    if job is None or job.get("id") is None:
        _write_fetch_error(error_path, f"Run {run_id} has no {job_name!r} job")
        return

    job_id = str(job["id"])
    evidence_prefix = destination.with_suffix("")
    check_run_url = str(
        job.get("check_run_url")
        or f"https://api.github.com/repos/{repository}/check-runs/{job_id}"
    )
    _fetch_api_url(
        check_run_url,
        evidence_prefix.with_name(f"{evidence_prefix.name}.check-run.json"),
        evidence_prefix.with_name(f"{evidence_prefix.name}.check-run.fetch-error.txt"),
    )
    _fetch_api_url(
        f"{check_run_url}/annotations?per_page=100",
        evidence_prefix.with_name(f"{evidence_prefix.name}.annotations.json"),
        evidence_prefix.with_name(f"{evidence_prefix.name}.annotations.fetch-error.txt"),
    )

    log_status_path = evidence_prefix.with_name(f"{evidence_prefix.name}.log-status.json")
    if _run_to_file(
        ["gh", "api", f"repos/{repository}/actions/jobs/{job_id}/logs"],
        destination=destination,
        error_path=error_path,
    ):
        _write_json(
            log_status_path,
            {"job_id": int(job_id), "source": "job_logs_api", "status": "downloaded"},
        )
        return

    if _extract_job_log_from_run_archive(
        repository=repository,
        run_id=run_id,
        job_name=job_name,
        destination=destination,
    ):
        error_path.unlink(missing_ok=True)
        _write_json(
            log_status_path,
            {"job_id": int(job_id), "source": "run_logs_archive", "status": "downloaded"},
        )
        return

    _write_json(
        log_status_path,
        {"job_id": int(job_id), "source": None, "status": "unavailable"},
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


def _fetch_evidence(args: argparse.Namespace) -> None:
    context_root = args.context_root.resolve()
    github_root = context_root / "github"
    history_root = github_root / "history"
    history_root.mkdir(parents=True, exist_ok=True)
    _download_current_context_artifact(
        repository=args.repository,
        run_id=args.current_run_id,
        artifact_name=args.current_context_artifact_name,
        context_root=context_root,
        github_root=github_root,
        wait_seconds=args.artifact_wait_seconds,
        poll_seconds=args.artifact_poll_seconds,
    )
    if not (context_root / "current" / "manifest.json").is_file():
        error_path = github_root / "current-artifact.fetch-error.txt"
        if not error_path.exists():
            _write_fetch_error(
                error_path,
                "Current failure-context artifact is missing or did not contain manifest.json",
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
        job_name=args.current_job_name,
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
            job_name=args.current_job_name,
            destination=run_root / "selected-job.log",
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


def _print_report(args: argparse.Namespace) -> None:
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
    print(rendered, end="", flush=True)
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

    fetch_evidence = subparsers.add_parser(
        "fetch-evidence",
        help="Fetch current and historical GitHub Actions failure evidence.",
    )
    fetch_evidence.add_argument("--repository", required=True)
    fetch_evidence.add_argument("--current-run-id", required=True)
    fetch_evidence.add_argument("--current-job-name", required=True)
    fetch_evidence.add_argument("--current-context-artifact-name", required=True)
    fetch_evidence.add_argument("--workflow-file", required=True)
    fetch_evidence.add_argument("--history-failure-limit", type=int, default=5)
    fetch_evidence.add_argument("--artifact-wait-seconds", type=float, default=60.0)
    fetch_evidence.add_argument("--artifact-poll-seconds", type=float, default=5.0)
    fetch_evidence.add_argument("--context-root", type=Path, required=True)
    fetch_evidence.set_defaults(handler=_fetch_evidence)

    build_inventory = subparsers.add_parser(
        "build-inventory",
        help="Build a complete evidence inventory.",
    )
    build_inventory.add_argument("--context-root", type=Path, required=True)
    build_inventory.set_defaults(handler=_build_inventory)

    print_report = subparsers.add_parser(
        "print-report",
        help="Redact configured secrets and print the final report.",
    )
    print_report.add_argument("--summary", type=Path, required=True)
    print_report.set_defaults(handler=_print_report)

    args = parser.parse_args()
    if getattr(args, "history_failure_limit", 0) < 0:
        parser.error("--history-failure-limit must be non-negative")
    if getattr(args, "artifact_wait_seconds", 0.0) < 0:
        parser.error("--artifact-wait-seconds must be non-negative")
    if getattr(args, "artifact_poll_seconds", 1.0) <= 0:
        parser.error("--artifact-poll-seconds must be positive")
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
