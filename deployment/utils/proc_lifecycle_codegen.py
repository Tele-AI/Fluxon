#!/usr/bin/env python3
"""
Code-generation helpers for bash lifecycle management in generated scripts.

Scope:
- Keep stop semantics consistent across generators (k8s daemonset, bare scripts).
- Make live process trees the authority for lifecycle observation.

Notes:
- This module generates bash *text* only. It does not execute processes.
- Generated helpers operate on live PIDs / PGIDs and on command-argument markers.
"""

from __future__ import annotations

from dataclasses import dataclass




@dataclass(frozen=True)
class StopTimeouts:
    term_seconds: int
    kill_seconds: int
    supersede_seconds: int

    def __post_init__(self) -> None:
        if self.term_seconds <= 0:
            raise ValueError("term_seconds must be positive")
        if self.kill_seconds <= 0:
            raise ValueError("kill_seconds must be positive")
        if self.supersede_seconds <= 0:
            raise ValueError("supersede_seconds must be positive")


def render_bash_proc_lifecycle_funcs(*, timeouts: StopTimeouts) -> str:
    """Compatibility wrapper that now returns pid-tree based helpers only."""
    return render_bash_proc_lifecycle_funcs_pid_tree(timeouts=timeouts)


def render_bash_proc_lifecycle_funcs_pid_tree(*, timeouts: StopTimeouts) -> str:
    """Return bash helper functions that stop a service by killing its PID subtree.

    This is used by generated runner scripts where the live supervisor PID subtree is the
    lifecycle authority.

    Notes:
    - Callers are expected to observe the live supervisor process, not persisted lifecycle metadata.
    - Stop semantics are intentionally single-sourced on the supervisor PID subtree.
    """
    term_s = timeouts.term_seconds
    kill_s = timeouts.kill_seconds

    return f"""# === Proc lifecycle helpers (generated) ===
STOP_TERM_TIMEOUT_SECONDS={term_s}
STOP_KILL_TIMEOUT_SECONDS={kill_s}

_list_pids_with_cmd_arg_exact() {{
  # English note:
  # - `ps` substring matching is unsafe because the runner `bash -lc "<script>"` contains many
  #   literals inside the `-c` argument; a literal path may appear in the script text without
  #   being an executed command.
  # - Parse `/proc/<pid>/cmdline` (NUL-separated) and match one argument exactly.
  needle="$1"
  if [ -z "$needle" ]; then
    return 0
  fi

  out=""
  for f in /proc/[0-9]*/cmdline; do
    pid="${{f#/proc/}}"
    pid="${{pid%/cmdline}}"
    if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
      continue
    fi
    if [ "$pid" -eq "$$" ]; then
      continue
    fi
    if [ ! -r "$f" ]; then
      continue
    fi
    if tr '\\0' '\\n' < "$f" 2>/dev/null | grep -Fqx "$needle"; then
      out="$out $pid"
    fi
  done

  echo "$out"
  return 0
}}

_pid_exists() {{
  pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}}

_pid_tree_list() {{
  root_pid="$1"
  if [[ ! "$root_pid" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if ! _pid_exists "$root_pid"; then
    return 1
  fi

  # English note: avoid `pgrep -P` because it is not guaranteed to exist in minimal images.
  # Use `ps -eo pid=,ppid=` which is widely supported.
  ps -eo pid=,ppid= 2>/dev/null | awk -v root="$root_pid" '
    {{
      pid=$1;
      ppid=$2;
      children[ppid]=children[ppid] " " pid;
    }}
    END {{
      qh=1;
      qt=1;
      queue[1]=root;
      seen[root]=1;
      out=root;
      while (qh<=qt) {{
        p=queue[qh++];
        n=split(children[p], arr, " ");
        for (i=1; i<=n; i++) {{
          c=arr[i];
          if (c=="" ) continue;
          if (!seen[c]) {{
            seen[c]=1;
            qt++;
            queue[qt]=c;
            out=out " " c;
          }}
        }}
      }}
      print out;
    }}
  '
}}

_pid_tree_has_child_process() {{
  root_pid="$1"
  pids="$(_pid_tree_list "$root_pid" 2>/dev/null || true)"
  if [ -z "$pids" ]; then
    return 1
  fi
  # More than one PID means the supervisor has a live child process.
  set -- $pids
  if [ "$#" -ge 2 ]; then
    return 0
  fi
  return 1
}}

wait_service_probably_ready_pid_tree() {{
  # Startup gate contract:
  # - Success means the supervisor PID stays alive across the fixed startup window.
  # - During this startup window we do not probe service ports or readiness endpoints.
  # - We intentionally do not require the child to expose ports, endpoints, or even finish
  #   spawning before the window ends.
  svc="$1"
  root_pid="$2"
  startup_window_seconds="$3"
  deadline_ts="$4"
  context="$5"

  if [[ ! "$startup_window_seconds" =~ ^[0-9]+$ ]] || [ "$startup_window_seconds" -le 0 ]; then
    echo "$context probable-ready: invalid startup_window_seconds=$startup_window_seconds svc=$svc"
    return 1
  fi
  if [[ ! "$deadline_ts" =~ ^[0-9]+$ ]] || [ "$deadline_ts" -le 0 ]; then
    echo "$context probable-ready: invalid deadline_ts=$deadline_ts svc=$svc"
    return 1
  fi

  while true; do
    if ! _pid_exists "$root_pid"; then
      echo "$context probable-ready: supervisor pid exited svc=$svc pid=$root_pid"
      return 1
    fi

    now=$(date +%s)
    if [ "$now" -ge "$deadline_ts" ]; then
      echo "$context probable-ready: ok svc=$svc startup_window_seconds=$startup_window_seconds pid=$root_pid"
      return 0
    fi

    sleep 1
  done
}}

_stop_pid_tree_strict() {{
  svc="$1"
  root_pid="$2"
  context="$3"

  if ! _pid_exists "$root_pid"; then
    return 0
  fi

  echo "$context stop: kill -TERM svc=$svc root_pid=$root_pid"
  pids="$(_pid_tree_list "$root_pid" 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    kill -TERM $pids 2>/dev/null || true
  fi

  deadline=$(( $(date +%s) + STOP_TERM_TIMEOUT_SECONDS ))
  while _pid_exists "$root_pid"; do
    if [ $(date +%s) -ge "$deadline" ]; then
      break
    fi
    sleep 0.2
  done

  if _pid_exists "$root_pid"; then
    echo "$context stop: escalating to KILL svc=$svc root_pid=$root_pid"
    pids="$(_pid_tree_list "$root_pid" 2>/dev/null || true)"
    if [ -n "$pids" ]; then
      kill -KILL $pids 2>/dev/null || true
    else
      kill -KILL "$root_pid" 2>/dev/null || true
    fi

    deadline=$(( $(date +%s) + STOP_KILL_TIMEOUT_SECONDS ))
    while _pid_exists "$root_pid"; do
      if [ $(date +%s) -ge "$deadline" ]; then
        break
      fi
      sleep 0.2
    done
  fi

  if _pid_exists "$root_pid"; then
    echo "$context stop: pid subtree still alive svc=$svc root_pid=$root_pid"
    return 1
  fi

  return 0
}}

"""
