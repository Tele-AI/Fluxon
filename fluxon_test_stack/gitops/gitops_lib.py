#!/usr/bin/env python3
"""GitOps library for test_runner-owned CI automation."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import subprocess
import yaml
import shutil

# -----------------------
# Constants (single source)
# -----------------------
LOG_DIR_NAME = "ci_logs"
HISTORY_FILE_NAME = "gitops_history.yaml"
RUN_INDEX_FILE_NAME = "runs_index.yaml"
RUN_META_DIR_NAME = "run_meta"
DEFAULT_INTERVAL = 60
REPOS_DIR_NAME = "repos"
RUNS_DIR_NAME = "runs"

# Runs index file is shared by poller loop and HTTP handler.
RUN_INDEX_LOCK = threading.Lock()

# Retention policy for meta/log files under ci_logs/.
DEFAULT_RETENTION_MAX_AGE_DAYS = 7

# SSH behavior for all git network operations issued by this tool.
# We explicitly make host key handling non-interactive to avoid CI getting stuck
# on first-contact prompts like:
#   "The authenticity of host 'github.com (...)' can't be established. ... Are you sure you want to continue connecting?"
# Using "accept-new" will automatically trust and persist a host key the first
# time we see it, and still protect against key changes afterwards. This keeps
# the flow non-interactive without fully disabling verification. If your OpenSSH
# is older and does not support "accept-new", switch the option below to "no"
# (i.e. StrictHostKeyChecking=no) to suppress prompts entirely, at the cost of
# skipping verification. We avoid environment-variable toggles here on purpose;
# behavior is single-sourced and predictable for the CI runner.
GIT_SSH_COMMAND_VALUE = "ssh -o StrictHostKeyChecking=accept-new"


@dataclass(frozen=True)
class GitOpsContext:
    workdir: Path
    config_path: Path
    config: dict[str, Any]
    log_dir: Path
    history_file: Path
    run_index_file: Path
    run_meta_dir: Path
    repos_dir: Path
    runs_dir: Path
    interval: int
    max_age_days: int
    repos_cfg: list[dict]
    history_lock: threading.Lock


def main() -> None:
    parser = argparse.ArgumentParser(description="Deprecated standalone GitOps entry")
    default_config = (Path(__file__).parent / "gitops.yaml").resolve()
    parser.add_argument("-c", "--config", type=str, default=str(default_config))
    parser.add_argument("-w", "--workdir", type=str, default=".")
    parser.parse_args()
    raise SystemExit(
        "gitops standalone service is removed; use fluxon_test_stack/test_runner_ui.py "
        "--gitops-config <gitops.yaml>"
    )


def default_runtime_root(service_workdir: Path) -> Path:
    return (Path(service_workdir).resolve() / "gitops").resolve()


def load_context(*, config_path: Path, workdir: Path) -> GitOpsContext:
    config_path = Path(config_path).resolve()
    workdir = Path(workdir).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"gitops config not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    if not isinstance(config, dict):
        raise ValueError(f"gitops config must be a mapping: {config_path}")

    retention_cfg = config.get("retention", {})
    if retention_cfg is None:
        retention_cfg = {}
    if not isinstance(retention_cfg, dict):
        raise ValueError("gitops retention must be a mapping")
    max_age_days = retention_cfg.get("max_age_days", DEFAULT_RETENTION_MAX_AGE_DAYS)
    if not isinstance(max_age_days, int) or max_age_days <= 0:
        raise ValueError("gitops retention.max_age_days must be an integer > 0")

    interval = int(config.get("interval", DEFAULT_INTERVAL))
    if interval <= 0:
        raise ValueError("gitops interval must be > 0")

    repos_cfg = _parse_repos_config(config.get("repos", []))

    log_dir = (workdir / LOG_DIR_NAME).resolve()
    history_file = (log_dir / HISTORY_FILE_NAME).resolve()
    run_index_file = (log_dir / RUN_INDEX_FILE_NAME).resolve()
    run_meta_dir = (log_dir / RUN_META_DIR_NAME).resolve()
    repos_dir = (workdir / REPOS_DIR_NAME).resolve()
    runs_dir = (workdir / RUNS_DIR_NAME).resolve()
    for path in (log_dir, run_meta_dir, repos_dir, runs_dir):
        path.mkdir(parents=True, exist_ok=True)

    return GitOpsContext(
        workdir=workdir,
        config_path=config_path,
        config=config,
        log_dir=log_dir,
        history_file=history_file,
        run_index_file=run_index_file,
        run_meta_dir=run_meta_dir,
        repos_dir=repos_dir,
        runs_dir=runs_dir,
        interval=interval,
        max_age_days=int(max_age_days),
        repos_cfg=repos_cfg,
        history_lock=threading.Lock(),
    )


def describe_context(ctx: GitOpsContext) -> dict[str, Any]:
    return {
        "workdir": str(ctx.workdir),
        "config_path": str(ctx.config_path),
        "repos_dir": str(ctx.repos_dir),
        "runs_dir": str(ctx.runs_dir),
        "log_dir": str(ctx.log_dir),
        "history_file": str(ctx.history_file),
        "interval": int(ctx.interval),
        "max_age_days": int(ctx.max_age_days),
        "repo_count": len(ctx.repos_cfg),
    }


def _get_follow_cfg_for_repo_branch(repos_cfg: list[dict], repo: str, branch: str) -> Optional[dict]:
    for item in repos_cfg:
        if item.get("addr") != repo:
            continue
        branches = item.get("branches")
        if not isinstance(branches, dict):
            continue
        follow_cfg = branches.get(branch)
        if isinstance(follow_cfg, dict):
            return follow_cfg
    return None


def _record_commit_run(
    ctx: GitOpsContext,
    *,
    repo: str,
    branch: str,
    name_prefix: str,
    commands: list[str],
    clone_path: Path,
    commit: str,
    run_id_prefix: str = "",
) -> dict[str, Any]:
    run_dir = _run_dir_for(ctx.runs_dir, repo, branch, commit)
    short = commit[:7]
    run_id = f"{run_id_prefix}{_safe_name(repo)}__{branch}__{datetime.now().strftime('%Y%m%d_%H%M%S')}__{short}"
    meta_dir = (ctx.run_meta_dir / run_id).resolve()
    meta_dir.mkdir(parents=True, exist_ok=True)

    progress_file = meta_dir / "progress.jsonl"
    result_file = meta_dir / "result.yaml"
    log_file = meta_dir / "run.log"

    _record_run_index(
        ctx.run_index_file,
        run_id,
        {
            "repo": repo,
            "branch": branch,
            "commit": commit,
            "name_prefix": name_prefix,
            "status": "running",
            "started_ts": datetime.now().isoformat(),
            "log_file": str(log_file),
            "progress_file": str(progress_file),
            "result_file": str(result_file),
            "run_dir": str(run_dir),
            "meta_dir": str(meta_dir),
        },
    )

    try:
        run_dir.parent.mkdir(parents=True, exist_ok=True)
        _clone_repo_at_commit(clone_path, run_dir, commit)
        rc, failed_step = _run_commands(
            commands=commands,
            run_dir=run_dir,
            log_file=log_file,
            progress_file=progress_file,
        )
        _record_run_index(
            ctx.run_index_file,
            run_id,
            {
                "status": "ok" if rc == 0 else "error",
                "rc": rc,
                "failed_step": failed_step,
                "finished_ts": datetime.now().isoformat(),
            },
        )
        with ctx.history_lock:
            _record_history(ctx.history_file, repo, branch, commit)
        _dump_yaml_atomic(
            result_file,
            {
                "ok": rc == 0,
                "rc": rc,
                "failed_step": failed_step,
            },
        )
        if rc == 0:
            if run_dir.exists():
                shutil.rmtree(run_dir)
        else:
            print(f"[gitops] preserved failed run workspace: {run_dir}", flush=True)
        return {
            "status": "ok",
            "run_id": run_id,
            "rc": int(rc),
            "failed_step": failed_step,
            "log_file": str(log_file),
            "run_dir": str(run_dir),
        }
    except Exception as exc:
        _record_run_index(
            ctx.run_index_file,
            run_id,
            {"status": "error", "error": str(exc), "finished_ts": datetime.now().isoformat()},
        )
        _dump_yaml_atomic(
            result_file,
            {
                "ok": False,
                "rc": 1,
                "error": str(exc),
            },
        )
        return {
            "status": "error",
            "run_id": run_id,
            "rc": 1,
            "error": str(exc),
            "log_file": str(log_file),
            "run_dir": str(run_dir),
        }


def poll_once(ctx: GitOpsContext) -> None:
    _apply_retention(
        run_index_file=ctx.run_index_file,
        run_meta_dir=ctx.run_meta_dir,
        runs_dir=ctx.runs_dir,
        max_age_days=ctx.max_age_days,
    )
    for repo in ctx.repos_cfg:
        addr = repo["addr"]
        branches = repo["branches"]
        clone_path = _ensure_local_clone(addr, ctx.repos_dir)
        try:
            _run(["git", "fetch", "--all"], cwd=clone_path)
        except Exception as exc:
            print(f"[gitops] [{addr}] fetch failed: {exc}", flush=True)
            continue

        with ctx.history_lock:
            history = _load_history(ctx.history_file)

        for branch, follow_cfg in branches.items():
            run_cfg = follow_cfg["run"]
            name_prefix = str(run_cfg["name_prefix"])
            commands = list(run_cfg["commands"])
            try:
                new_commits = _compute_new_commits(
                    clone_path,
                    branch,
                    history.get(addr, {}).get(branch, []),
                )
            except Exception as exc:
                print(f"[gitops] [{addr} {branch}] compute commits failed: {exc}", flush=True)
                continue
            for commit in new_commits:
                result = _record_commit_run(
                    ctx,
                    repo=addr,
                    branch=branch,
                    name_prefix=name_prefix,
                    commands=commands,
                    clone_path=clone_path,
                    commit=commit,
                )
                if result.get("status") != "ok":
                    print(
                        f"[gitops] [{addr} {branch} {commit[:7]}] run failed: {result.get('error') or result.get('rc')}",
                        flush=True,
                    )


def poll_forever(ctx: GitOpsContext, *, stop_event: Optional[threading.Event] = None) -> None:
    desc = describe_context(ctx)
    print(
        "[gitops] poller started: "
        f"workdir={desc['workdir']} config={desc['config_path']} interval={desc['interval']}s repos={desc['repo_count']}",
        flush=True,
    )
    while True:
        if stop_event is not None and stop_event.is_set():
            print("[gitops] poller stopping", flush=True)
            return
        try:
            poll_once(ctx)
        except Exception as exc:
            print(f"[gitops] unexpected poll error: {type(exc).__name__}: {exc}", flush=True)
        if stop_event is not None:
            if stop_event.wait(ctx.interval):
                print("[gitops] poller stopping", flush=True)
                return
        else:
            time.sleep(ctx.interval)


def list_runs(ctx: GitOpsContext) -> list[tuple[str, dict]]:
    data = _load_runs_index(ctx.run_index_file)
    runs = data.get("runs", {}) if isinstance(data, dict) else {}
    out: list[tuple[str, dict]] = []
    if isinstance(runs, dict):
        for run_id, info in runs.items():
            if isinstance(run_id, str) and isinstance(info, dict):
                out.append((run_id, info))
    out.sort(key=lambda item: str(item[1].get("started_ts", "")), reverse=True)
    return out


def get_run(ctx: GitOpsContext, run_id: str) -> dict[str, Any]:
    data = _load_runs_index(ctx.run_index_file)
    info = (data.get("runs") or {}).get(run_id) if isinstance(data, dict) else None
    if not isinstance(info, dict):
        raise FileNotFoundError(f"gitops run not found: {run_id}")
    return info


def get_run_progress_tail(ctx: GitOpsContext, run_id: str, *, max_lines: int = 400) -> list[dict]:
    info = get_run(ctx, run_id)
    progress_file = Path(str(info.get("progress_file", "")))
    return _read_jsonl_tail(progress_file, max_lines=max_lines)


def get_run_last_event(ctx: GitOpsContext, run_id: str) -> Optional[dict]:
    tail = get_run_progress_tail(ctx, run_id, max_lines=1)
    return tail[-1] if tail else None


def get_run_step_count(ctx: GitOpsContext, run_id: str) -> int:
    info = get_run(ctx, run_id)
    follow_cfg = _get_follow_cfg_for_repo_branch(
        ctx.repos_cfg,
        str(info.get("repo", "")),
        str(info.get("branch", "")),
    )
    if not isinstance(follow_cfg, dict):
        return 0
    run_cfg = follow_cfg.get("run")
    if not isinstance(run_cfg, dict):
        return 0
    commands = run_cfg.get("commands")
    if not isinstance(commands, list):
        return 0
    return len(commands)


def read_log_chunk(
    ctx: GitOpsContext,
    *,
    run_id: str,
    kind: str,
    step: Optional[int],
    from_offset: Optional[int],
    before_offset: Optional[int],
    max_bytes: int,
) -> dict[str, Any]:
    if kind not in ("run", "step"):
        raise ValueError("gitops log kind must be 'run' or 'step'")
    if max_bytes <= 0:
        raise ValueError("gitops max_bytes must be > 0")
    if max_bytes > 1024 * 1024:
        raise ValueError("gitops max_bytes too large")
    if from_offset is not None and before_offset is not None:
        raise ValueError("gitops from and before are mutually exclusive")

    info = get_run(ctx, run_id)
    meta_dir = Path(str(info.get("meta_dir", ""))).resolve()
    if not meta_dir.exists():
        raise FileNotFoundError(f"gitops meta_dir not found: {meta_dir}")
    if kind == "run":
        log_path = (meta_dir / "run.log").resolve()
    else:
        if step is None or int(step) <= 0:
            raise ValueError("gitops step must be >= 1 for kind=step")
        log_path = (meta_dir / "steps" / f"step_{int(step)}.log").resolve()
    if not _is_within_base(meta_dir, log_path):
        raise ValueError("gitops invalid log path")
    if not log_path.exists():
        raise FileNotFoundError(f"gitops log not found: {log_path}")
    size = int(log_path.stat().st_size)
    if from_offset is not None:
        start = max(0, int(from_offset))
        end = min(size, start + int(max_bytes))
    elif before_offset is not None:
        end = min(size, max(0, int(before_offset)))
        start = max(0, end - int(max_bytes))
    else:
        end = size
        start = max(0, size - int(max_bytes))
    with open(log_path, "rb") as f:
        f.seek(start)
        data = f.read(max(0, end - start))
    return {
        "id": run_id,
        "kind": kind,
        "path": str(log_path),
        "start": int(start),
        "end": int(end),
        "size": int(size),
        "eof": end == size,
        "text": data.decode("utf-8", errors="replace"),
    }


def rerun_target(ctx: GitOpsContext, *, target: str) -> dict[str, Any]:
    try:
        repo, branch, commit = str(target).rsplit(":", 2)
    except ValueError as exc:
        raise ValueError("bad target format; expect repo:branch:commit") from exc
    follow_cfg = _get_follow_cfg_for_repo_branch(ctx.repos_cfg, repo, branch)
    if not isinstance(follow_cfg, dict):
        raise ValueError(f"unknown repo/branch in gitops config: {repo} {branch}")
    run_cfg = follow_cfg.get("run")
    if not isinstance(run_cfg, dict):
        raise ValueError(f"invalid follow.run config for: {repo} {branch}")
    name_prefix = str(run_cfg.get("name_prefix") or "").strip()
    commands = run_cfg.get("commands")
    if not name_prefix:
        raise ValueError(f"invalid follow.run.name_prefix for: {repo} {branch}")
    if not isinstance(commands, list) or not commands:
        raise ValueError(f"invalid follow.run.commands for: {repo} {branch}")
    clone_path = _ensure_local_clone(repo, ctx.repos_dir)
    _run(["git", "fetch", "--all"], cwd=clone_path)
    return _record_commit_run(
        ctx,
        repo=repo,
        branch=branch,
        name_prefix=name_prefix,
        commands=list(commands),
        clone_path=clone_path,
        commit=commit,
        run_id_prefix="rerun__",
    )


# -----------------------
# Helpers
# -----------------------
def _resolve_path(p: str | os.PathLike[str], base: Path) -> Path:
    pp = Path(p)
    return (base / pp).resolve() if not Path(p).is_absolute() else pp.resolve()


def _read_jsonl_tail(path: Path, *, max_lines: int) -> list[dict]:
    if max_lines <= 0 or not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-max_lines:]
    except Exception:
        return []
    out: list[dict] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except Exception:
            continue
        if isinstance(record, dict):
            out.append(record)
    return out


class ProgressWriter:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._f = self.path.open("a", encoding="utf-8")

    def emit(self, event: str, payload: dict) -> None:
        rec = {"event": event, "payload": payload, "ts": time.time()}
        self._f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()


def _run_commands(*, commands: list[str], run_dir: Path, log_file: Path, progress_file: Path) -> tuple[int, int | None]:
    if not commands:
        raise ValueError("commands must be non-empty")

    pw = ProgressWriter(progress_file)
    pw.emit("run_started", {"cwd": str(run_dir)})

    failed_step: int | None = None
    rc: int = 0

    steps_dir = log_file.parent / "steps"
    steps_dir.mkdir(parents=True, exist_ok=True)

    # Stream stdout/stderr to both:
    # - a combined run.log (keeps the run readable)
    # - per-step log files (so UI can link to an individual step)
    with open(log_file, "wb") as lf:
        for idx, cmd in enumerate(commands, start=1):
            step_log = steps_dir / f"step_{idx}.log"
            pw.emit("step_started", {"idx": idx, "cmd": cmd, "step_log": str(step_log)})

            header = ("\n" + ("=" * 80) + "\n" + f"STEP {idx}: {cmd}\n" + ("=" * 80) + "\n").encode("utf-8")
            lf.write(header)
            lf.flush()

            with open(step_log, "wb") as sf:
                sf.write(header)
                sf.flush()

                proc = subprocess.Popen(
                    ["bash", "-lc", cmd],
                    cwd=str(run_dir),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                if proc.stdout is None:
                    raise RuntimeError("Popen stdout is None")

                while True:
                    chunk = proc.stdout.read(4096)
                    if not chunk:
                        break
                    lf.write(chunk)
                    lf.flush()
                    sf.write(chunk)
                    sf.flush()

                rc = int(proc.wait())

            if rc == 0:
                pw.emit("step_ok", {"idx": idx, "rc": rc})
                continue

            failed_step = idx
            pw.emit("step_err", {"idx": idx, "rc": rc})
            break

    pw.emit("run_finished", {"rc": rc, "failed_step": failed_step})
    pw.close()
    return rc, failed_step



def _git_cmd(cmd: list[str]) -> list[str]:
    if not cmd:
        return cmd
    if cmd[0] != "git":
        return cmd
    # Keep SSH behavior deterministic without relying on environment variables.
    return ["git", "-c", f"core.sshCommand={GIT_SSH_COMMAND_VALUE}", *cmd[1:]]


def _run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(
        _git_cmd(cmd),
        cwd=str(cwd) if cwd else None,
        check=True,
    )


def _run_capture(cmd: list[str], cwd: Path | None = None) -> str:
    out = subprocess.check_output(
        _git_cmd(cmd),
        cwd=str(cwd) if cwd else None,
    )
    return out.decode().strip()


def _git_default_branch(repo_path: Path) -> str:
    ref = _run_capture(["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"], cwd=repo_path)
    return ref.split("/", 1)[1] if "/" in ref else ref



def _load_history(history_file: Path) -> dict:
    if not history_file.exists():
        return {}
    with open(history_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"history file must be a YAML mapping: {history_file}")
    return data


def _record_history(history_file: Path, repo_url: str, branch: str, commit: str) -> None:
    data = _load_history(history_file)
    repo_map = data.setdefault(repo_url, {})
    lst = repo_map.setdefault(branch, [])
    if commit not in lst:
        lst.append(commit)

    history_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = history_file.with_name(history_file.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=True)
    os.replace(tmp, history_file)



def _forbid_unknown_keys(d: dict, allowed: set[str], ctx: str) -> None:
    unknown = set(d.keys()) - allowed
    if unknown:
        raise ValueError(f"unknown keys in {ctx}: {sorted(unknown)}")


def _parse_repos_config(repos_node: list) -> list[dict]:
    """Parse repos config.

    Schema:
      repos:
        - addr: <repo_url>
          follow:
            - branch: <branch>
                  run:
                name_prefix: <string>
                commands: [<string>, ...]

    GitOps is intentionally a simple orchestrator: the per-commit pipeline is a
    command list executed in the checked-out workspace.
    """
    parsed: list[dict] = []
    for item in repos_node or []:
        if not isinstance(item, dict):
            continue
        _forbid_unknown_keys(item, {"addr", "follow"}, "repos[]")
        addr = str(item.get("addr", "")).strip()
        if not addr:
            raise ValueError("repos[*].addr must be a non-empty string")

        follow = item.get("follow")
        if not isinstance(follow, list) or not follow:
            raise ValueError(f"repo.follow must be a non-empty list (repo={addr})")

        branches: dict[str, dict] = {}
        for f in follow:
            if not isinstance(f, dict):
                raise ValueError(f"repo.follow items must be mappings (repo={addr})")
            _forbid_unknown_keys(f, {"branch", "run"}, f"repo.follow[] (repo={addr})")
            br = str(f.get("branch", "")).strip()
            if not br:
                raise ValueError(f"repo.follow.branch must be non-empty (repo={addr})")

            run_cfg = f.get("run")
            if not isinstance(run_cfg, dict):
                raise ValueError(f"repo.follow.run must be a mapping (repo={addr} branch={br})")
            _forbid_unknown_keys(run_cfg, {"name_prefix", "commands"}, f"repo.follow.run (repo={addr} branch={br})")

            name_prefix = run_cfg.get("name_prefix")
            if not isinstance(name_prefix, str) or not name_prefix.strip():
                raise ValueError(f"repo.follow.run.name_prefix must be a non-empty string (repo={addr} branch={br})")

            commands = run_cfg.get("commands")
            if not isinstance(commands, list) or not commands:
                raise ValueError(f"repo.follow.run.commands must be a non-empty list (repo={addr} branch={br})")
            parsed_cmds: list[str] = []
            for i, c in enumerate(commands):
                if not isinstance(c, str) or not c.strip():
                    raise ValueError(f"repo.follow.run.commands[{i}] must be a non-empty string (repo={addr} branch={br})")
                parsed_cmds.append(c)

            branches[br] = {
                "run": {
                    "name_prefix": name_prefix.strip(),
                    "commands": parsed_cmds,
                },
            }

        parsed.append({"addr": addr, "branches": branches})
    return parsed


def _ensure_local_clone(addr: str, repos_dir: Path) -> Path:
    safe = _safe_name(addr)
    clone_path = repos_dir / safe
    if not clone_path.exists():
        clone_path.parent.mkdir(parents=True, exist_ok=True)
        # Use _run so our non-interactive SSH policy applies to the initial clone.
        _run(["git", "clone", addr, str(clone_path)])
    return clone_path


def _compute_new_commits(clone_path: Path, branch: str, history_list: list[str]) -> list[str]:
    head = _run_capture(["git", "rev-parse", f"origin/{branch}"], cwd=clone_path)
    if not history_list:
        return [head]
    last = history_list[-1]
    if head == last:
        return []
    return [head]


def _run_dir_for(runs_dir: Path, addr: str, branch: str, commit: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    name = f"{_safe_name(addr)}__{branch}__{ts}__{commit}"
    return (runs_dir / name).resolve()


def _safe_name(addr: str) -> str:
    # Sanitize repo address to filesystem-friendly name
    safe = addr
    for ch in [":", "/", "\\", "@", " ", "."]:
        safe = safe.replace(ch, "-")
    while "--" in safe:
        safe = safe.replace("--", "-")
    return safe.strip("-")


def _dump_yaml(path: Path, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def _dump_yaml_atomic(path: Path, data: dict) -> None:
    """Write YAML atomically (tmp + rename) to avoid partial files on crash."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
    os.replace(tmp, path)


def _load_runs_index_nolock(path: Path) -> dict:
    if not path.exists():
        return {"runs": {}}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"runs index must be a YAML mapping: {path}")
    runs = data.get("runs")
    if not isinstance(runs, dict):
        raise ValueError(f"runs index missing mapping key 'runs': {path}")
    return {"runs": runs}


def _load_runs_index(path: Path) -> dict:
    with RUN_INDEX_LOCK:
        return _load_runs_index_nolock(path)


def _record_run_index(path: Path, run_id: str, patch: dict) -> None:
    with RUN_INDEX_LOCK:
        data = _load_runs_index_nolock(path)
        runs = data.setdefault("runs", {})
        cur = runs.get(run_id) if isinstance(runs.get(run_id), dict) else {}
        merged = dict(cur)
        merged.update(patch)
        runs[run_id] = merged
        _dump_yaml_atomic(path, data)



def _is_within_base(base_dir: Path, target: Path) -> bool:
    base_dir = base_dir.resolve()
    target = target.resolve()
    return base_dir == target or base_dir in target.parents


def _apply_retention(*, run_index_file: Path, run_meta_dir: Path, runs_dir: Path, max_age_days: int) -> None:
    """Delete old per-run meta/log files and preserved workspaces."""
    if max_age_days <= 0:
        raise ValueError("retention.max_age_days must be > 0")

    cutoff = datetime.now() - timedelta(days=max_age_days)

    with RUN_INDEX_LOCK:
        data = _load_runs_index_nolock(run_index_file)
        runs = data.get("runs")
        if not isinstance(runs, dict) or not runs:
            return

        stale_ids: list[str] = []
        for rid, info in runs.items():
            if not isinstance(rid, str) or not rid:
                continue
            if not isinstance(info, dict):
                continue
            started_ts = info.get("started_ts")
            if not isinstance(started_ts, str) or not started_ts.strip():
                continue
            try:
                started_dt = datetime.fromisoformat(started_ts)
            except Exception:
                continue
            if started_dt <= cutoff:
                stale_ids.append(rid)

        if not stale_ids:
            return

        for rid in stale_ids:
            info = runs.get(rid)
            if not isinstance(info, dict):
                info = {}

            meta_dir_raw = info.get("meta_dir")
            if isinstance(meta_dir_raw, str) and meta_dir_raw.strip():
                meta_dir = Path(meta_dir_raw)
            else:
                meta_dir = run_meta_dir / rid

            if meta_dir.exists():
                if not _is_within_base(run_meta_dir, meta_dir):
                    raise ValueError(f"refuse to delete meta_dir outside run_meta_dir: meta_dir={meta_dir}")
                shutil.rmtree(meta_dir)

            run_dir_raw = info.get("run_dir")
            if isinstance(run_dir_raw, str) and run_dir_raw.strip():
                run_dir = Path(run_dir_raw)
                if run_dir.exists():
                    if not _is_within_base(runs_dir, run_dir):
                        raise ValueError(f"refuse to delete run_dir outside runs_dir: run_dir={run_dir}")
                    shutil.rmtree(run_dir)

            runs.pop(rid, None)

        _dump_yaml_atomic(run_index_file, data)

    print(
        f"[retention] deleted {len(stale_ids)} runs older than {max_age_days}d (cutoff={cutoff.isoformat()})",
        flush=True,
    )


def _clone_repo_at_commit(src_repo: Path, dst_repo_root: Path, commit: str) -> None:
    """Clone from local mirror repo and checkout a specific commit."""
    if dst_repo_root.exists():
        shutil.rmtree(dst_repo_root)
    _run(["git", "clone", str(src_repo), str(dst_repo_root)])
    _run(["git", "-c", "advice.detachedHead=false", "checkout", commit], cwd=dst_repo_root)



def _serve_http(
    repos_dir: Path,
    runs_dir: Path,
    log_dir: Path,
    run_index_file: Path,
    run_meta_dir: Path,
    history_file: Path,
    history_lock: threading.Lock,
    repos_cfg: list[dict],
    *,
    host: str,
    port: int,
) -> None:
    def _html_escape(v: str) -> str:
        return (
            v.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    def _read_jsonl_tail(path: Path, *, max_lines: int) -> list[dict]:
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()[-max_lines:]
        out: list[dict] = []
        for ln in lines:
            if not ln.strip():
                continue
            try:
                rec = json.loads(ln)
            except Exception:
                continue
            if isinstance(rec, dict):
                out.append(rec)
        return out

    def _read_last_event(progress_file: Path) -> str:
        tail = _read_jsonl_tail(progress_file, max_lines=1)
        if not tail:
            return ""
        ev = tail[-1].get("event")
        return str(ev) if isinstance(ev, str) else ""


    def _get_follow_cfg_for_repo_branch(repo: str, branch: str) -> dict | None:
        for r in repos_cfg:
            if r.get("addr") != repo:
                continue
            branches = r.get("branches")
            if not isinstance(branches, dict):
                continue
            b = branches.get(branch)
            if isinstance(b, dict):
                return b
        return None

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args) -> None:  # noqa: A003
            pass

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send_json(200, {"status": "ok"})
                return
            if parsed.path == "/":
                self._handle_index()
                return
            if parsed.path == "/run":
                self._handle_run(parsed)
                return
            if parsed.path == "/log":
                self._handle_log(parsed)
                return
            if parsed.path == "/step_log":
                self._handle_step_log(parsed)
                return
            if parsed.path == "/api/run_state":
                self._handle_run_state(parsed)
                return
            if parsed.path == "/api/log_chunk":
                self._handle_api_log_chunk(parsed)
                return
            if parsed.path == "/rerun":
                self._handle_rerun(parsed)
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/rerun":
                self._send_json(404, {"error": "not found"})
                return

            length = int(self.headers.get("Content-Length", "0") or 0)
            body = self.rfile.read(length).decode("utf-8") if length > 0 else ""
            target = None
            if body:
                try:
                    payload = json.loads(body)
                    target = payload.get("target")
                except Exception:
                    target = parse_qs(body).get("target", [None])[0]
            if not target:
                qs = parse_qs(parsed.query)
                target = (qs.get("target") or [None])[0]
            if not target:
                self._send_json(400, {"error": "missing target"})
                return
            self._process_rerun(str(target))

        def _handle_index(self) -> None:
            try:
                data = _load_runs_index(run_index_file)
            except Exception as e:
                self._send_html(500, f"load runs index failed: {e}")
                return
            runs = data.get("runs", {}) if isinstance(data, dict) else {}

            items: list[tuple[str, dict]] = []
            if isinstance(runs, dict):
                for rid, info in runs.items():
                    if isinstance(info, dict):
                        items.append((str(rid), info))
            items.sort(key=lambda x: str(x[1].get("started_ts", "")), reverse=True)

            rows: list[str] = []
            for rid, info in items:
                status = str(info.get("status", ""))
                repo = str(info.get("repo", ""))
                branch = str(info.get("branch", ""))
                commit = str(info.get("commit", ""))
                name_prefix = str(info.get("name_prefix", ""))
                rc = info.get("rc")
                started = str(info.get("started_ts", ""))

                progress_file = Path(str(info.get("progress_file", "")))
                last_event = _read_last_event(progress_file)

                rows.append(
                    "<tr>"
                    f"<td><a href='/run?id={_html_escape(rid)}'>{_html_escape(rid)}</a></td>"
                    f"<td>{_html_escape(status)}</td>"
                    f"<td>{_html_escape(str(rc)) if rc is not None else ''}</td>"
                    f"<td>{_html_escape(name_prefix)}</td>"
                    f"<td>{_html_escape(repo)}</td>"
                    f"<td>{_html_escape(branch)}</td>"
                    f"<td>{_html_escape(commit[:12])}</td>"
                    f"<td>{_html_escape(started)}</td>"
                    f"<td>{_html_escape(last_event)}</td>"
                    "</tr>"
                )

            idx = _html_escape(str(run_index_file))
            rows_html = "\n".join(rows)
            html = """<!doctype html>
            <html><head><meta charset='utf-8'/>
            <meta name='viewport' content='width=device-width, initial-scale=1'/>
            <title>GitOps</title>
            <style>
            body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
            code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}
            .table{width:100%;border-collapse:collapse;}
            .table th,.table td{border:1px solid #e5e7eb;padding:6px 8px;text-align:left;vertical-align:top;}
            .table th{background:#f9fafb;}
            .small{color:#6b7280;font-size:12px;}
            </style>
            </head><body>
            <h1>GitOps</h1>
            <div class='small'>Runs index: @@IDX@@</div>
            <form method='post' action='/rerun' style='margin:12px 0'>
              <label>rerun target (repo:branch:commit)</label>
              <input name='target' style='width:70%' />
              <button type='submit'>rerun</button>
            </form>
            <table class='table'>
            <tr><th>run_id</th><th>status</th><th>rc</th><th>name_prefix</th><th>repo</th><th>branch</th><th>commit</th><th>started</th><th>last_event</th></tr>
            @@ROWS@@
            </table>
            </body></html>"""
            html = html.replace('@@IDX@@', idx).replace('@@ROWS@@', rows_html)
            self._send_html(200, html)

        def _handle_run(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            rid = (qs.get("id") or [""])[0]
            if not rid:
                self._send_html(400, "missing id")
                return

            try:
                data = _load_runs_index(run_index_file)
            except Exception as e:
                self._send_html(500, f"load runs index failed: {e}")
                return
            info = (data.get("runs") or {}).get(rid) if isinstance(data, dict) else None
            if not isinstance(info, dict):
                self._send_html(404, "run not found")
                return

            progress_file = Path(str(info.get("progress_file", "")))
            prog = _read_jsonl_tail(progress_file, max_lines=400)
            prog_html = "\n".join(
                f"<pre>{_html_escape(json.dumps(e, ensure_ascii=False))}</pre>" for e in prog
            )

            log_link = f"/log?id={_html_escape(rid)}"

            follow_cfg = _get_follow_cfg_for_repo_branch(str(info.get('repo','')), str(info.get('branch','')))
            step_links = ''
            if isinstance(follow_cfg, dict):
                run_cfg = follow_cfg.get('run')
                if isinstance(run_cfg, dict):
                    cmds = run_cfg.get('commands')
                    if isinstance(cmds, list) and cmds:
                        parts = []
                        for idx2 in range(1, len(cmds) + 1):
                            parts.append(f"<a href='/step_log?id={_html_escape(rid)}&step={idx2}' target='_blank'>step_{idx2}</a>")
                        step_links = ' '.join(parts)
            rid_html = _html_escape(rid)
            status_html = _html_escape(str(info.get('status','')))
            repo_html = _html_escape(str(info.get('repo','')))
            branch_html = _html_escape(str(info.get('branch','')))
            commit_html = _html_escape(str(info.get('commit','')))
            name_prefix_html = _html_escape(str(info.get('name_prefix','')))
            
            html = """<!doctype html>
            <html><head><meta charset='utf-8'/>
            <meta name='viewport' content='width=device-width, initial-scale=1'/>
            <meta http-equiv='refresh' content='2'/>
            <title>Run @@RID@@</title>
            <style>
            body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1200px;margin:16px auto;padding:0 12px;}
            pre{background:#0b1020;color:#e5e7eb;padding:10px;border-radius:8px;overflow:auto;}
            </style>
            </head><body>
            <a href='/'>back</a>
            <h2>Run @@RID@@</h2>
            <div>status: <code>@@STATUS@@</code></div>
            <div>repo: <code>@@REPO@@</code></div>
            <div>branch: <code>@@BRANCH@@</code></div>
            <div>commit: <code>@@COMMIT@@</code></div>
            <div>name_prefix: <code>@@NAME_PREFIX@@</code></div>
            <div>log: <a href='@@LOG_LINK@@'>@@LOG_LINK@@</a></div>
            <div>step_logs: @@STEP_LINKS@@</div>
            <h3>Logs</h3>
            <iframe src='@@LOG_LINK@@' style='width:100%;height:82vh;border:1px solid #e5e7eb;border-radius:8px;'></iframe>
            <details style='margin-top:12px'>
              <summary>progress.jsonl (tail)</summary>
              @@PROG_HTML@@
            </details>
            </body></html>"""
            
            html = (
                html.replace('@@RID@@', rid_html)
                .replace('@@STATUS@@', status_html)
                .replace('@@REPO@@', repo_html)
                .replace('@@BRANCH@@', branch_html)
                .replace('@@COMMIT@@', commit_html)
                .replace('@@NAME_PREFIX@@', name_prefix_html)
                .replace('@@LOG_LINK@@', log_link)
                .replace('@@STEP_LINKS@@', step_links)
                .replace('@@PROG_HTML@@', prog_html)
            )
            self._send_html(200, html)

        def _handle_step_log(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            rid = (qs.get("id") or [""])[0]
            step_raw = (qs.get("step") or [""])[0]
            if not rid or not step_raw:
                self._send_html(400, "missing id or step")
                return
            try:
                step = int(step_raw)
            except Exception:
                self._send_html(400, "invalid step")
                return
            if step <= 0:
                self._send_html(400, "step must be >= 1")
                return

            rid_html = _html_escape(rid)
            step_html = _html_escape(str(step))

            html = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Step Log @@RID@@ step @@STEP@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}
#log{white-space:pre;background:#0b1020;color:#e5e7eb;padding:12px;border-radius:8px;height:78vh;overflow:auto;font-size:12px;line-height:1.35;tab-size:4;}
.small{color:#6b7280;font-size:12px;}
.btn{padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;background:#fff;cursor:pointer;}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
</style>
</head><body>
<a href='/run?id=@@RID@@'>back</a>
<h2>step_@@STEP@@.log tail</h2>
<div class='row'>
  <div class='small'>run_id: <code>@@RID@@</code></div>
  <div class='small'>step: <code>@@STEP@@</code></div>
  <button class='btn' id='btnFollow'>follow: on</button>
  <button class='btn' id='btnReload'>reload tail</button>
  <div class='small' id='status'></div>
</div>
<div id='log'></div>
<script>
(function(){
  const runId = "@@RID@@";
  const step = "@@STEP@@";
  const logEl = document.getElementById('log');
  const statusEl = document.getElementById('status');
  const btnFollow = document.getElementById('btnFollow');
  const btnReload = document.getElementById('btnReload');

  let follow = true;
  let loading = false;
  let loadedStart = null;
  let loadedEnd = null;

  const MAX_BYTES = 65536;
  const POLL_MS = 800;

  function setStatus(s){ statusEl.textContent = s; }
  function nearBottom(){ return (logEl.scrollHeight - (logEl.scrollTop + logEl.clientHeight)) < 40; }
  function scrollToBottom(){ logEl.scrollTop = logEl.scrollHeight; }

  async function fetchChunk(params){
    const url = new URL('/api/log_chunk', window.location.origin);
    url.searchParams.set('id', runId);
    url.searchParams.set('kind', 'step');
    url.searchParams.set('step', step);
    url.searchParams.set('max_bytes', String(MAX_BYTES));
    for (const [k,v] of Object.entries(params || {})) {
      url.searchParams.set(k, String(v));
    }
    const resp = await fetch(url.toString());
    if (!resp.ok) {
      const t = await resp.text();
      throw new Error('http ' + resp.status + ' ' + t);
    }
    return await resp.json();
  }

  async function loadTail(){
    if (loading) return;
    loading = true;
    try {
      setStatus('loading tail...');
      const data = await fetchChunk({});
      logEl.textContent = data.text || '';
      loadedStart = data.start;
      loadedEnd = data.end;
      setStatus('size=' + data.size + ' loaded=[' + loadedStart + ',' + loadedEnd + ']');
      scrollToBottom();
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }

  async function pollAppend(){
    if (!follow) return;
    if (loading) return;
    if (loadedEnd === null) return;

    loading = true;
    try {
      const shouldScroll = nearBottom();
      const data = await fetchChunk({from: loadedEnd});
      if (data.text && data.text.length > 0) {
        logEl.textContent += data.text;
      }
      loadedEnd = data.end;
      if (shouldScroll) scrollToBottom();
      setStatus('size=' + data.size + ' loaded=[' + loadedStart + ',' + loadedEnd + ']');
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }

  async function loadMoreBefore(){
    if (loading) return;
    if (loadedStart === null) return;
    if (loadedStart <= 0) return;

    loading = true;
    try {
      const prevScrollHeight = logEl.scrollHeight;
      setStatus('loading older...');
      const data = await fetchChunk({before: loadedStart});
      if (data.text && data.text.length > 0) {
        logEl.textContent = data.text + logEl.textContent;
        loadedStart = data.start;
        const newScrollHeight = logEl.scrollHeight;
        logEl.scrollTop = newScrollHeight - prevScrollHeight;
      } else {
        loadedStart = data.start;
      }
      setStatus('size=' + data.size + ' loaded=[' + loadedStart + ',' + loadedEnd + ']');
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }

  btnFollow.addEventListener('click', function(){
    follow = !follow;
    btnFollow.textContent = 'follow: ' + (follow ? 'on' : 'off');
    if (follow) scrollToBottom();
  });
  btnReload.addEventListener('click', function(){
    loadTail();
  });

  logEl.addEventListener('scroll', function(){
    if (logEl.scrollTop < 20) {
      loadMoreBefore();
    }
  });

  loadTail().then(function(){
    setInterval(pollAppend, POLL_MS);
  });
})();
</script>
</body></html>"""

            html = html.replace('@@RID@@', rid_html).replace('@@STEP@@', step_html)
            self._send_html(200, html)

        def _handle_api_log_chunk(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            rid = (qs.get("id") or [""])[0]
            kind = (qs.get("kind") or ["run"])[0]
            step_raw = (qs.get("step") or [""])[0]
            from_raw = (qs.get("from") or [""])[0]
            before_raw = (qs.get("before") or [""])[0]
            max_bytes_raw = (qs.get("max_bytes") or [""])[0]

            if not rid:
                self._send_json(400, {"error": "missing id"})
                return
            if kind not in ("run", "step"):
                self._send_json(400, {"error": "invalid kind"})
                return

            try:
                data = _load_runs_index(run_index_file)
            except Exception as e:
                self._send_json(500, {"error": f"load runs index failed: {e}"})
                return
            info = (data.get("runs") or {}).get(rid) if isinstance(data, dict) else None
            if not isinstance(info, dict):
                self._send_json(404, {"error": "run not found"})
                return

            meta_dir = Path(str(info.get("meta_dir", "")))
            if not meta_dir.exists():
                self._send_json(404, {"error": f"meta_dir not found: {meta_dir}"})
                return

            if kind == "run":
                log_path = (meta_dir / "run.log").resolve()
            else:
                if not step_raw:
                    self._send_json(400, {"error": "missing step for kind=step"})
                    return
                try:
                    step = int(step_raw)
                except Exception:
                    self._send_json(400, {"error": "invalid step"})
                    return
                if step <= 0:
                    self._send_json(400, {"error": "step must be >= 1"})
                    return
                log_path = (meta_dir / "steps" / f"step_{step}.log").resolve()

            if not _is_within_base(meta_dir, log_path):
                self._send_json(400, {"error": "invalid log path"})
                return
            if not log_path.exists():
                self._send_json(404, {"error": f"log not found: {log_path}"})
                return

            try:
                st = log_path.stat()
            except Exception as e:
                self._send_json(500, {"error": f"stat log failed: {e}"})
                return
            size = int(st.st_size)

            max_bytes = 65536
            if max_bytes_raw:
                try:
                    max_bytes = int(max_bytes_raw)
                except Exception:
                    self._send_json(400, {"error": "invalid max_bytes"})
                    return
            if max_bytes <= 0:
                self._send_json(400, {"error": "max_bytes must be > 0"})
                return
            if max_bytes > 1024 * 1024:
                # Keep response bounded (log viewer is incremental).
                self._send_json(400, {"error": "max_bytes too large"})
                return

            if from_raw and before_raw:
                self._send_json(400, {"error": "from and before are mutually exclusive"})
                return

            if from_raw:
                try:
                    start = int(from_raw)
                except Exception:
                    self._send_json(400, {"error": "invalid from"})
                    return
                if start < 0:
                    start = 0
                end = min(size, start + max_bytes)
            elif before_raw:
                try:
                    end = int(before_raw)
                except Exception:
                    self._send_json(400, {"error": "invalid before"})
                    return
                if end < 0:
                    end = 0
                if end > size:
                    end = size
                start = max(0, end - max_bytes)
            else:
                end = size
                start = max(0, size - max_bytes)

            try:
                with open(log_path, "rb") as f:
                    f.seek(start)
                    data_bytes = f.read(max(0, end - start))
            except Exception as e:
                self._send_json(500, {"error": f"read log failed: {e}"})
                return

            txt = data_bytes.decode("utf-8", errors="replace")
            self._send_json(
                200,
                {
                    "ok": True,
                    "id": rid,
                    "kind": kind,
                    "path": str(log_path),
                    "start": start,
                    "end": end,
                    "size": size,
                    "eof": end == size,
                    "text": txt,
                },
            )

        def _handle_log(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            rid = (qs.get("id") or [""])[0]
            if not rid:
                self._send_html(400, "missing id")
                return

            rid_html = _html_escape(rid)

            # Log viewer: tail-first, auto-refresh, lazy-load older chunks on scroll.
            # We keep HTML as a static template so it can be copied into a standalone file.
            html = """<!doctype html>
<html><head><meta charset='utf-8'/>
<meta name='viewport' content='width=device-width, initial-scale=1'/>
<title>Log @@RID@@</title>
<style>
body{font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial;max-width:1400px;margin:16px auto;padding:0 12px;}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;}
#log{white-space:pre;background:#0b1020;color:#e5e7eb;padding:12px;border-radius:8px;height:78vh;overflow:auto;font-size:12px;line-height:1.35;tab-size:4;}
.small{color:#6b7280;font-size:12px;}
.btn{padding:4px 8px;border:1px solid #e5e7eb;border-radius:6px;background:#fff;cursor:pointer;}
.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
</style>
</head><body>
<a href='/run?id=@@RID@@'>back</a>
<h2>run.log tail</h2>
<div class='row'>
  <div class='small'>run_id: <code>@@RID@@</code></div>
  <button class='btn' id='btnFollow'>follow: on</button>
  <button class='btn' id='btnReload'>reload tail</button>
  <div class='small' id='status'></div>
</div>
<div id='log'></div>
<script>
(function(){
  const runId = "@@RID@@";
  const logEl = document.getElementById('log');
  const statusEl = document.getElementById('status');
  const btnFollow = document.getElementById('btnFollow');
  const btnReload = document.getElementById('btnReload');

  let follow = true;
  let loading = false;
  let loadedStart = null;
  let loadedEnd = null;

  const MAX_BYTES = 65536;
  const POLL_MS = 800;

  function setStatus(s){ statusEl.textContent = s; }
  function nearBottom(){ return (logEl.scrollHeight - (logEl.scrollTop + logEl.clientHeight)) < 40; }
  function scrollToBottom(){ logEl.scrollTop = logEl.scrollHeight; }

  async function fetchChunk(params){
    const url = new URL('/api/log_chunk', window.location.origin);
    url.searchParams.set('id', runId);
    url.searchParams.set('kind', 'run');
    url.searchParams.set('max_bytes', String(MAX_BYTES));
    for (const [k,v] of Object.entries(params || {})) {
      url.searchParams.set(k, String(v));
    }
    const resp = await fetch(url.toString());
    if (!resp.ok) {
      const t = await resp.text();
      throw new Error('http ' + resp.status + ' ' + t);
    }
    return await resp.json();
  }

  async function loadTail(){
    if (loading) return;
    loading = true;
    try {
      setStatus('loading tail...');
      const data = await fetchChunk({});
      logEl.textContent = data.text || '';
      loadedStart = data.start;
      loadedEnd = data.end;
      setStatus('size=' + data.size + ' loaded=[' + loadedStart + ',' + loadedEnd + ']');
      scrollToBottom();
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }

  async function pollAppend(){
    if (!follow) return;
    if (loading) return;
    if (loadedEnd === null) return;

    loading = true;
    try {
      const shouldScroll = nearBottom();
      const data = await fetchChunk({from: loadedEnd});
      if (data.text && data.text.length > 0) {
        logEl.textContent += data.text;
      }
      loadedEnd = data.end;
      if (shouldScroll) scrollToBottom();
      setStatus('size=' + data.size + ' loaded=[' + loadedStart + ',' + loadedEnd + ']');
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }

  async function loadMoreBefore(){
    if (loading) return;
    if (loadedStart === null) return;
    if (loadedStart <= 0) return;

    loading = true;
    try {
      const prevScrollHeight = logEl.scrollHeight;
      setStatus('loading older...');
      const data = await fetchChunk({before: loadedStart});
      if (data.text && data.text.length > 0) {
        logEl.textContent = data.text + logEl.textContent;
        loadedStart = data.start;
        // Keep the viewport anchored around the previous top.
        const newScrollHeight = logEl.scrollHeight;
        logEl.scrollTop = newScrollHeight - prevScrollHeight;
      } else {
        loadedStart = data.start;
      }
      setStatus('size=' + data.size + ' loaded=[' + loadedStart + ',' + loadedEnd + ']');
    } catch (e) {
      setStatus('error: ' + (e && e.message ? e.message : String(e)));
    } finally {
      loading = false;
    }
  }

  btnFollow.addEventListener('click', function(){
    follow = !follow;
    btnFollow.textContent = 'follow: ' + (follow ? 'on' : 'off');
    if (follow) scrollToBottom();
  });
  btnReload.addEventListener('click', function(){
    loadTail();
  });

  logEl.addEventListener('scroll', function(){
    if (logEl.scrollTop < 20) {
      loadMoreBefore();
    }
    if (!nearBottom()) {
      // User is reading history; don't force scroll.
      // follow flag is explicit (button), but we stop auto-scroll behavior when not near bottom.
    }
  });

  // Boot.
  loadTail().then(function(){
    setInterval(pollAppend, POLL_MS);
  });
})();
</script>
</body></html>"""

            html = html.replace('@@RID@@', rid_html)
            self._send_html(200, html)

        def _handle_run_state(self, parsed) -> None:
            qs = parse_qs(parsed.query)
            rid = (qs.get("id") or [""])[0]
            if not rid:
                self._send_json(400, {"error": "missing id"})
                return

            try:
                data = _load_runs_index(run_index_file)
            except Exception as e:
                self._send_json(500, {"error": f"load runs index failed: {e}"})
                return
            info = (data.get("runs") or {}).get(rid) if isinstance(data, dict) else None
            if not isinstance(info, dict):
                self._send_json(404, {"error": "run not found"})
                return

            progress_file = Path(str(info.get("progress_file", "")))
            tail = _read_jsonl_tail(progress_file, max_lines=1)
            last = tail[-1] if tail else None
            self._send_json(200, {"run": info, "last_event": last})

        def _handle_rerun(self, parsed):
            qs = parse_qs(parsed.query)
            target = (qs.get("target") or [None])[0]
            if not target:
                self._send_json(400, {"error": "missing target"})
                return
            self._process_rerun(str(target))

        def _process_rerun(self, target: str) -> None:
            try:
                repo, branch, commit = target.split(":", 2)
            except ValueError:
                self._send_json(400, {"error": "bad target format; expect repo:branch:commit"})
                return

            follow_cfg = _get_follow_cfg_for_repo_branch(repo, branch)
            if not isinstance(follow_cfg, dict):
                self._send_json(400, {"error": f"unknown repo/branch in config: {repo} {branch}"})
                return

            run_cfg = follow_cfg.get("run")
            if not isinstance(run_cfg, dict):
                self._send_json(500, {"error": f"invalid follow.run config for: {repo} {branch}"})
                return

            name_prefix = str(run_cfg.get("name_prefix") or "").strip()
            commands = run_cfg.get("commands")
            if not name_prefix:
                self._send_json(500, {"error": f"invalid follow.run.name_prefix for: {repo} {branch}"})
                return
            if not isinstance(commands, list) or not commands:
                self._send_json(500, {"error": f"invalid follow.run.commands for: {repo} {branch}"})
                return

            clone_path = _ensure_local_clone(repo, repos_dir)
            try:
                _run(["git", "fetch", "--all"], cwd=clone_path)
            except Exception as e:
                self._send_json(500, {"error": f"fetch failed: {e}"})
                return

            run_dir = _run_dir_for(runs_dir, repo, branch, commit)
            short = commit[:7]
            run_id = f"rerun__{_safe_name(repo)}__{branch}__{datetime.now().strftime('%Y%m%d_%H%M%S')}__{short}"
            meta_dir = (run_meta_dir / run_id).resolve()
            meta_dir.mkdir(parents=True, exist_ok=True)

            progress_file = meta_dir / "progress.jsonl"
            result_file = meta_dir / "result.yaml"
            log_file = meta_dir / "run.log"

            _record_run_index(
                run_index_file,
                run_id,
                {
                    "repo": repo,
                    "branch": branch,
                    "commit": commit,
                    "name_prefix": name_prefix,
                    "status": "running",
                    "started_ts": datetime.now().isoformat(),
                    "log_file": str(log_file),
                    "progress_file": str(progress_file),
                    "result_file": str(result_file),
                    "run_dir": str(run_dir),
                    "meta_dir": str(meta_dir),
                },
            )

            try:
                run_dir.parent.mkdir(parents=True, exist_ok=True)
                _clone_repo_at_commit(clone_path, run_dir, commit)

                rc, failed_step = _run_commands(
                    commands=list(commands),
                    run_dir=run_dir,
                    log_file=log_file,
                    progress_file=progress_file,
                )

                _record_run_index(
                    run_index_file,
                    run_id,
                    {
                        "status": "ok" if rc == 0 else "error",
                        "rc": rc,
                        "failed_step": failed_step,
                        "finished_ts": datetime.now().isoformat(),
                    },
                )

                with history_lock:
                    _record_history(history_file, repo, branch, commit)

                _dump_yaml_atomic(
                    result_file,
                    {
                        "ok": rc == 0,
                        "rc": rc,
                        "failed_step": failed_step,
                    },
                )

                self._send_json(200, {"status": "ok", "run_id": run_id, "rc": rc, "log": str(log_file)})

                if rc == 0:
                    if run_dir.exists():
                        shutil.rmtree(run_dir)
                else:
                    print(f"Preserved failed rerun workspace: {run_dir}")

            except Exception as e:
                _record_run_index(
                    run_index_file,
                    run_id,
                    {"status": "error", "error": str(e), "finished_ts": datetime.now().isoformat()},
                )
                _dump_yaml_atomic(
                    result_file,
                    {
                        "ok": False,
                        "rc": 1,
                        "error": str(e),
                    },
                )
                self._send_json(500, {"error": str(e), "run_id": run_id})


        def _send_json(self, code: int, payload: dict) -> None:
            data = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_html(self, code: int, html: str) -> None:
            data = html.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_text(self, code: int, text: str) -> None:
            data = text.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"HTTP server listening on {host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
