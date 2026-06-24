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

_pid_tree_direct_child_pids() {{
  root_pid="$1"
  if [[ ! "$root_pid" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if ! _pid_exists "$root_pid"; then
    return 1
  fi

  ps -eo pid=,ppid=,stat= 2>/dev/null | awk -v root="$root_pid" '
    {{
      pid=$1;
      ppid=$2;
      state=$3;
      if (ppid != root) {{
        next;
      }}
      if (state ~ /^Z/) {{
        next;
      }}
      out=out " " pid;
    }}
    END {{
      sub(/^ /, "", out);
      print out;
    }}
  '
}}

_now_monotonic_ms() {{
  python3 - <<'__FLUXON_MONOTONIC_MS__'
import time

print(time.monotonic_ns() // 1_000_000)
__FLUXON_MONOTONIC_MS__
}}

wait_service_probably_ready_pid_tree() {{
  # Startup gate contract:
  # - Success means one supervised direct child PID becomes visible, then stays unchanged for the
  #   full startup_window_seconds before the overall startup deadline expires.
  # - During this startup window we do not probe service ports or readiness endpoints.
  # - A child exit or restart inside the window is treated as startup failure even if the
  #   supervisor process itself stays alive and restarts again later.
  svc="$1"
  root_pid="$2"
  startup_window_seconds="$3"
  startup_deadline_seconds="$4"
  context="$5"

  if [[ ! "$startup_window_seconds" =~ ^[0-9]+$ ]] || [ "$startup_window_seconds" -le 0 ]; then
    echo "$context probable-ready: invalid startup_window_seconds=$startup_window_seconds svc=$svc"
    return 1
  fi
  if [[ ! "$startup_deadline_seconds" =~ ^[0-9]+$ ]] || [ "$startup_deadline_seconds" -le 0 ]; then
    echo "$context probable-ready: invalid startup_deadline_seconds=$startup_deadline_seconds svc=$svc"
    return 1
  fi

  startup_window_ms=$(( startup_window_seconds * 1000 ))
  startup_deadline_ms=$(( startup_deadline_seconds * 1000 ))
  started_at_monotonic_ms="$(_now_monotonic_ms)"
  deadline_monotonic_ms=$(( started_at_monotonic_ms + startup_deadline_ms ))
  observed_child_pid=""
  observed_child_since_monotonic_ms=""
  while true; do
    if ! _pid_exists "$root_pid"; then
      echo "$context probable-ready: supervisor pid exited svc=$svc pid=$root_pid"
      return 1
    fi

    current_child_pids="$(_pid_tree_direct_child_pids "$root_pid" 2>/dev/null || true)"
    current_child_pid=""
    if [ -n "$current_child_pids" ]; then
      set -- $current_child_pids
      if [ "$#" -ne 1 ]; then
        echo "$context probable-ready: multiple direct child pids svc=$svc supervisor_pid=$root_pid child_pids=$current_child_pids"
        return 1
      fi
      current_child_pid="$1"
    fi

    now_monotonic_ms="$(_now_monotonic_ms)"
    if [ -z "$current_child_pid" ]; then
      if [ -n "$observed_child_pid" ]; then
        echo "$context probable-ready: child pid exited svc=$svc supervisor_pid=$root_pid child_pid=$observed_child_pid"
        return 1
      fi
    elif [ -z "$observed_child_pid" ]; then
      observed_child_pid="$current_child_pid"
      observed_child_since_monotonic_ms="$now_monotonic_ms"
    elif [ "$current_child_pid" != "$observed_child_pid" ]; then
      echo "$context probable-ready: child pid changed svc=$svc supervisor_pid=$root_pid child_pid=$observed_child_pid replacement_child_pid=$current_child_pid"
      return 1
    fi

    if [ -n "$observed_child_since_monotonic_ms" ] && [ $(( now_monotonic_ms - observed_child_since_monotonic_ms )) -ge "$startup_window_ms" ]; then
      echo "$context probable-ready: ok svc=$svc startup_window_seconds=$startup_window_seconds supervisor_pid=$root_pid child_pid=$observed_child_pid"
      return 0
    fi

    if [ "$now_monotonic_ms" -ge "$deadline_monotonic_ms" ]; then
      if [ -z "$observed_child_pid" ]; then
        echo "$context probable-ready: no child pid observed svc=$svc supervisor_pid=$root_pid startup_window_seconds=$startup_window_seconds startup_deadline_seconds=$startup_deadline_seconds"
        return 1
      fi
      echo "$context probable-ready: child pid not stable long enough svc=$svc supervisor_pid=$root_pid child_pid=$observed_child_pid observed_for_ms=$(( now_monotonic_ms - observed_child_since_monotonic_ms )) startup_window_seconds=$startup_window_seconds startup_deadline_seconds=$startup_deadline_seconds"
      return 1
    fi

    sleep 0.2
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
