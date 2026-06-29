#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import yaml


CONTROLLER_REQUEST_MODE_SSH_EXEC_PER_REQUEST = "ssh_exec_per_request"
TEST_STACK_START_TEST_BED_CONFIG_ENV = "FLUXON_TEST_STACK_START_TEST_BED_CONFIG"
_SSH_STDERR_NOISE_PREFIXES = ("/etc/zsh/zshenv:", "zsh:")
_CONTROLLER_SSH_CONNECT_TIMEOUT_SECONDS = 10
_CONTROLLER_SSH_SUBPROCESS_GRACE_SECONDS = 10.0
_CONTROLLER_SSH_SUBPROCESS_MIN_TIMEOUT_SECONDS = 20.0


def load_test_bed_bootstrap_config_path_from_env_or_repo_root(*, repo_root: Path) -> Path:
    raw_override = os.environ.get(TEST_STACK_START_TEST_BED_CONFIG_ENV)
    if raw_override:
        override_path = Path(raw_override).expanduser()
        if not override_path.is_absolute():
            override_path = override_path.resolve()
        if not override_path.exists():
            raise ValueError(
                f"{TEST_STACK_START_TEST_BED_CONFIG_ENV} points to a missing file: {override_path}"
            )
        return override_path
    return (repo_root / "fluxon_test_stack" / "start_test_bed.yaml").resolve()


def load_test_bed_deployconf_path(*, bootstrap_config_path: Path) -> Path:
    cfg_path = bootstrap_config_path.resolve()
    cfg = _require_mapping(_load_yaml_file(cfg_path), f"test bed bootstrap config {cfg_path}")
    raw_path = _require_nonempty_str(cfg.get("deployconf_path"), "start_test_bed.deployconf_path")
    deployconf_path = Path(raw_path)
    if not deployconf_path.is_absolute():
        deployconf_path = (cfg_path.parent / deployconf_path).resolve()
    else:
        deployconf_path = deployconf_path.resolve()
    if not deployconf_path.exists():
        raise ValueError(f"test bed deployconf_path not found: {deployconf_path}")
    return deployconf_path


def load_test_bed_manifest_opt(*, bootstrap_config_path: Path) -> tuple[Path, dict[str, Any]] | None:
    manifest_path = bootstrap_config_path.resolve().with_name("manifest.json")
    if not manifest_path.exists():
        return None
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"failed to load test bed manifest {manifest_path}: {exc}") from exc
    manifest = _require_mapping(raw, f"test bed manifest {manifest_path}")
    return manifest_path, manifest


def load_test_bed_remote_auth(manifest_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    raw_path = manifest.get("remote_auth_config_path")
    if raw_path is None:
        raise ValueError(f"test bed manifest {manifest_path}.remote_auth_config_path is required")
    auth_path = Path(_require_nonempty_str(raw_path, f"test bed manifest {manifest_path}.remote_auth_config_path"))
    if not auth_path.is_absolute():
        auth_path = (manifest_path.parent / auth_path).resolve()
    else:
        auth_path = auth_path.resolve()
    if not auth_path.exists():
        raise ValueError(f"test bed remote auth config not found: {auth_path}")
    return _require_mapping(_load_yaml_file(auth_path), f"test bed remote auth config {auth_path}")


def load_test_bed_cluster_hostnames_by_ip_opt(
    *,
    bootstrap_config_path: Path,
) -> dict[str, list[str]] | None:
    deployconf_path = load_test_bed_deployconf_path(bootstrap_config_path=bootstrap_config_path)
    deployconf = _require_mapping(_load_yaml_file(deployconf_path), f"test bed deployconf {deployconf_path}")
    raw_nodes = deployconf.get("cluster_nodes")
    if not isinstance(raw_nodes, list):
        return None
    out: dict[str, list[str]] = {}
    for idx, raw_node in enumerate(raw_nodes):
        node = _require_mapping(raw_node, f"deployconf.cluster_nodes[{idx}]")
        hostname = _require_nonempty_str(node.get("hostname"), f"deployconf.cluster_nodes[{idx}].hostname")
        ip = _require_nonempty_str(node.get("ip"), f"deployconf.cluster_nodes[{idx}].ip")
        out.setdefault(ip, []).append(hostname)
    for ip, names in out.items():
        out[ip] = sorted(names)
    return out


def canonical_targets_for_ip_from_test_bed(*, bootstrap_config_path: Path, node_ip: str) -> list[str]:
    by_ip = load_test_bed_cluster_hostnames_by_ip_opt(bootstrap_config_path=bootstrap_config_path)
    if by_ip is None:
        return []
    return list(by_ip.get(node_ip, []))


def load_test_bed_manifest_transport_ctx_opt(
    *,
    bootstrap_config_path: Path,
) -> dict[str, Any] | None:
    manifest_info = load_test_bed_manifest_opt(bootstrap_config_path=bootstrap_config_path)
    if manifest_info is None:
        return None
    manifest_path, manifest = manifest_info
    auth_cfg = load_test_bed_remote_auth(manifest_path, manifest)
    bastion = _require_mapping(manifest.get("bastion"), f"test bed manifest {manifest_path}.bastion")
    bastion_user_raw = auth_cfg.get("bastion_user")
    bastion_private_key_raw = auth_cfg.get("bastion_private_key")
    bastion_password_raw = auth_cfg.get("bastion_password")
    bastion_name_raw = bastion.get("name")
    return {
        "manifest_path": manifest_path,
        "manifest": manifest,
        "bastion_name": (
            ""
            if bastion_name_raw is None or not str(bastion_name_raw).strip()
            else _require_nonempty_str(bastion_name_raw, f"test bed manifest {manifest_path}.bastion.name")
        ),
        "bastion_host": _require_nonempty_str(bastion.get("host"), f"test bed manifest {manifest_path}.bastion.host"),
        "bastion_port": _require_int(
            bastion.get("ssh_port"),
            f"test bed manifest {manifest_path}.bastion.ssh_port",
            min_v=1,
        ),
        "bastion_user": (
            "root"
            if bastion_user_raw is None or not str(bastion_user_raw).strip()
            else _require_nonempty_str(bastion_user_raw, f"test bed manifest {manifest_path}.bastion_user")
        ),
        "bastion_private_key": (
            None
            if bastion_private_key_raw is None or not str(bastion_private_key_raw).strip()
            else str(Path(str(bastion_private_key_raw)).expanduser().resolve())
        ),
        "bastion_password": (
            None
            if bastion_password_raw is None
            else _require_nonempty_str(bastion_password_raw, f"test bed manifest {manifest_path}.bastion_password")
        ),
    }


def controller_request_exec_host(
    manifest_path: Path,
    manifest: dict[str, Any],
) -> tuple[str, str | None, int | None, str | None]:
    auth_cfg = load_test_bed_remote_auth(manifest_path, manifest)
    exec_host = _require_nonempty_str(auth_cfg.get("controller_exec_host"), "test bed remote auth controller_exec_host")
    exec_user_raw = auth_cfg.get("controller_exec_user")
    exec_port_raw = auth_cfg.get("controller_exec_port")
    exec_password_raw = auth_cfg.get("controller_exec_password")
    exec_user = None if exec_user_raw is None else _require_nonempty_str(exec_user_raw, "test bed remote auth controller_exec_user")
    exec_port = None if exec_port_raw is None else _require_int(exec_port_raw, "test bed remote auth controller_exec_port", min_v=1)
    exec_password = (
        None
        if exec_password_raw is None
        else _require_nonempty_str(exec_password_raw, "test bed remote auth controller_exec_password")
    )
    return exec_host, exec_user, exec_port, exec_password


def controller_request_url_via_manifest(
    manifest_path: Path,
    manifest: dict[str, Any],
    *,
    url: str,
) -> str:
    request_parts = urllib.parse.urlsplit(url)
    exec_host, _, _, _ = controller_request_exec_host(manifest_path, manifest)
    bastion = _require_mapping(manifest.get("bastion"), "testbed manifest.bastion")
    bastion_host = _require_nonempty_str(bastion.get("host"), "testbed manifest.bastion.host")
    if exec_host != bastion_host:
        raise ValueError(
            "testbed controller transport requires controller_exec_host to match bastion.host "
            f"for ssh_exec_per_request; exec_host={exec_host!r} bastion_host={bastion_host!r}"
        )
    local_base = _require_nonempty_str(
        manifest.get("controller_bastion_local_url"),
        "testbed manifest.controller_bastion_local_url",
    )
    local_parts = urllib.parse.urlsplit(local_base)
    return urllib.parse.urlunsplit(
        (local_parts.scheme, local_parts.netloc, request_parts.path, request_parts.query, "")
    )


def controller_request_via_manifest(
    req: urllib.request.Request,
    *,
    timeout_seconds: float,
    bootstrap_config_path: Path,
) -> tuple[int, bytes] | None:
    manifest_info = load_test_bed_manifest_opt(bootstrap_config_path=bootstrap_config_path)
    if manifest_info is None:
        return None
    manifest_path, manifest = manifest_info
    if not _url_uses_test_bed_manifest_transport(manifest_path=manifest_path, manifest=manifest, url=str(req.full_url)):
        return None
    transport_ctx = load_test_bed_manifest_transport_ctx_opt(bootstrap_config_path=bootstrap_config_path)
    if transport_ctx is None:
        raise ValueError("testbed transport manifest not found")
    exec_host, exec_user, exec_port, exec_password = controller_request_exec_host(
        transport_ctx["manifest_path"],
        manifest,
    )
    effective_url = controller_request_url_via_manifest(
        transport_ctx["manifest_path"],
        manifest,
        url=str(req.full_url),
    )
    headers_json = json.dumps(dict(req.header_items()), separators=(",", ":"))
    remote_script = (
        "import json, sys, urllib.error, urllib.request\n"
        "url, method, timeout_seconds, headers_json = sys.argv[1:5]\n"
        "headers = json.loads(headers_json)\n"
        "payload = sys.stdin.buffer.read()\n"
        "if payload == b'':\n"
        "    payload = None\n"
        "request = urllib.request.Request(url, data=payload, method=method)\n"
        "for key, value in headers.items():\n"
        "    request.add_header(key, value)\n"
        "try:\n"
        "    with urllib.request.urlopen(request, timeout=float(timeout_seconds)) as resp:\n"
        "        body = resp.read()\n"
        "        status = int(resp.status)\n"
        "except urllib.error.HTTPError as err:\n"
        "    body = err.read()\n"
        "    status = int(err.code)\n"
        "except Exception as exc:\n"
        "    print(json.dumps({'transport_error': f'{type(exc).__name__}: {exc}'}), file=sys.stderr)\n"
        "    sys.exit(0)\n"
        "sys.stdout.buffer.write(body)\n"
        "sys.stdout.buffer.flush()\n"
        "print(json.dumps({'status': status}), file=sys.stderr)\n"
    )
    remote_cmd = (
        "python3 -c "
        + _shell_quote(remote_script)
        + " "
        + _shell_quote(effective_url)
        + " "
        + _shell_quote(req.get_method())
        + " "
        + _shell_quote(str(float(timeout_seconds)))
        + " "
        + _shell_quote(headers_json)
    )
    argv: list[str] = []
    effective_password = exec_password
    direct_bastion = exec_host == str(transport_ctx["bastion_host"])
    if direct_bastion and effective_password is None and transport_ctx.get("bastion_password") is not None:
        effective_password = str(transport_ctx["bastion_password"])
    if effective_password is not None:
        argv.extend(["sshpass", "-p", effective_password])
    argv.extend(
        [
            "ssh",
            "-o",
            "BatchMode=yes" if effective_password is None else "BatchMode=no",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            f"ConnectTimeout={_CONTROLLER_SSH_CONNECT_TIMEOUT_SECONDS}",
        ]
    )
    if direct_bastion:
        argv.extend(
            [
                "-o",
                "HostKeyAlgorithms=+ssh-rsa",
                "-o",
                "PubkeyAcceptedAlgorithms=+ssh-rsa",
            ]
        )
        if transport_ctx["bastion_private_key"]:
            argv.extend(["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"])
    else:
        proxy_parts: list[str] = []
        if transport_ctx.get("bastion_password"):
            proxy_parts.extend(["sshpass", "-p", str(transport_ctx["bastion_password"])])
        proxy_parts.extend(
            [
                "ssh",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "ConnectTimeout=10",
                "-o",
                "HostKeyAlgorithms=+ssh-rsa",
                "-o",
                "PubkeyAcceptedAlgorithms=+ssh-rsa",
            ]
        )
        if transport_ctx["bastion_private_key"]:
            proxy_parts.extend(["-i", str(transport_ctx["bastion_private_key"]), "-o", "IdentitiesOnly=yes"])
        proxy_parts.extend(
            [
                "-p",
                str(transport_ctx["bastion_port"]),
                f"{transport_ctx['bastion_user']}@{transport_ctx['bastion_host']}",
                "-W",
                "%h:%p",
            ]
        )
        argv.extend(["-o", "ProxyCommand=" + " ".join(shlex.quote(str(part)) for part in proxy_parts)])
    if exec_port is not None:
        argv.extend(["-p", str(int(exec_port))])
    target = exec_host if exec_user is None else f"{exec_user}@{exec_host}"
    argv.extend([target, remote_cmd])
    try:
        completed = subprocess.run(
            argv,
            input=req.data if isinstance(req.data, bytes) else b"",
            capture_output=True,
            timeout=max(
                float(timeout_seconds) + _CONTROLLER_SSH_SUBPROCESS_GRACE_SECONDS,
                _CONTROLLER_SSH_SUBPROCESS_MIN_TIMEOUT_SECONDS,
            ),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise urllib.error.URLError(
            f"ssh controller request timed out: url={effective_url} timeout={timeout_seconds}"
        ) from exc
    stdout_bytes = completed.stdout
    stderr_text = _clean_ssh_stderr_text(completed.stderr.decode("utf-8", errors="replace"))
    if completed.returncode != 0:
        detail = stderr_text or stdout_bytes.decode("utf-8", errors="replace") or f"ssh exited with rc={completed.returncode}"
        raise urllib.error.URLError(f"ssh controller request failed: url={effective_url} detail={detail}")
    lines = [line for line in stderr_text.splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"empty ssh controller response envelope: url={effective_url}")
    envelope = _require_mapping(json.loads(lines[-1]), f"ssh controller response {effective_url}")
    transport_error = envelope.get("transport_error")
    if transport_error is not None:
        raise urllib.error.URLError(f"ssh controller transport error: url={effective_url} err={transport_error}")
    status_code = _require_int(envelope.get("status"), f"ssh controller response {effective_url}.status", min_v=100)
    return int(status_code), stdout_bytes


def _url_uses_test_bed_manifest_transport(
    *,
    manifest_path: Path,
    manifest: dict[str, Any],
    url: str,
) -> bool:
    mode = _require_nonempty_str(
        manifest.get("controller_request_mode"),
        f"testbed manifest {manifest_path}.controller_request_mode",
    )
    if mode != CONTROLLER_REQUEST_MODE_SSH_EXEC_PER_REQUEST:
        return False
    controller_url = _require_nonempty_str(
        manifest.get("controller_url"),
        f"testbed manifest {manifest_path}.controller_url",
    ).rstrip("/")
    controller_public_url = _require_nonempty_str(
        manifest.get("controller_public_url"),
        f"testbed manifest {manifest_path}.controller_public_url",
    ).rstrip("/")
    normalized_url = _require_nonempty_str(url, "controller transport url").rstrip("/")
    return normalized_url.startswith(controller_url) or normalized_url.startswith(controller_public_url)


def _load_yaml_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def _require_mapping(value: Any, ctx: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{ctx} must be a mapping")
    return value


def _require_nonempty_str(value: Any, ctx: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{ctx} must be a non-empty string")
    return value


def _require_int(value: Any, ctx: str, *, min_v: int) -> int:
    if not isinstance(value, int):
        raise ValueError(f"{ctx} must be an integer")
    if value < min_v:
        raise ValueError(f"{ctx} must be >= {min_v}")
    return value


def _clean_ssh_stderr_text(text: str) -> str:
    if not text:
        return ""
    lines: list[str] = []
    for raw_line in text.splitlines():
        if any(raw_line.startswith(prefix) for prefix in _SSH_STDERR_NOISE_PREFIXES):
            continue
        lines.append(raw_line)
    return "\n".join(lines).strip()


def _shell_quote(text: str) -> str:
    if text == "":
        return "''"
    if re.fullmatch(r"[A-Za-z0-9_./:=@+-]+", text):
        return text
    return "'" + text.replace("'", "'\\''") + "'"
