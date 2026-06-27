#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import datetime
import hashlib
import hmac
import http.client
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


SCHEMA_VERSION = 1

_ENDPOINT_SCHEME_HTTP = "HTTP"
_ENDPOINT_SCHEME_HTTPS = "HTTPS"

_LIFECYCLE_SERVICE = "service"
_LIFECYCLE_JOB = "job"

PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3 = "FLUXON_FS_S3"

OPS_AGENT_INSTANCE_KEY_PREFIX = "fluxon_ops_"
_DEPLOY_GUARD_ERR = "another deploy operation is in-flight; try again later"
_DEPLOY_GUARD_WAIT_SECONDS = 120.0
_DEPLOY_GUARD_POLL_SECONDS = 2.0
# Controller API calls can legitimately stall under load (large workload sets, slow backing store, etc.).
# Keep per-attempt timeout bounded, but align the adapter with the runner's more tolerant retry window so
# deploy preflight does not fail while the control plane is still converging.
_CONTROLLER_HTTP_TIMEOUT_SECONDS = 30.0
_CONTROLLER_HTTP_RETRY_DEADLINE_SECONDS = 300.0
_CONTROLLER_HTTP_RETRY_SLEEP_SECONDS = 1.0
_CONTROLLER_TRANSIENT_HTTP_STATUS_CODES = (500, 502, 503, 504)
_CONTROLLER_SSH_CONNECT_TIMEOUT_SECONDS = 10
_CONTROLLER_SSH_SUBPROCESS_GRACE_SECONDS = 10.0
CONTROLLER_REQUEST_MODE_SSH_EXEC_PER_REQUEST = "ssh_exec_per_request"
TEST_STACK_START_TEST_BED_CONFIG_ENV = "FLUXON_TEST_STACK_START_TEST_BED_CONFIG"
RUNNER_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONTROLLER_BASIC_AUTH_HEADER_NAME = "x-fluxon-ops-authorization"
_CONTROLLER_BASIC_AUTH_HEADER: str | None = None
_SSH_STDERR_NOISE_PREFIXES = ("/etc/zsh/zshenv:", "zsh:")


def _resolve_repo_root_cli_path(*, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (RUNNER_REPO_ROOT / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _load_test_bed_bootstrap_config_path() -> Path:
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
    return (RUNNER_REPO_ROOT / "fluxon_test_stack" / "start_test_bed.yaml").resolve()


def _load_test_bed_manifest_opt() -> Optional[tuple[Path, Dict[str, Any]]]:
    manifest_path = _load_test_bed_bootstrap_config_path().with_name("manifest.json")
    if not manifest_path.exists():
        return None
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest = _require_dict(raw, f"test bed manifest {manifest_path}")
    return manifest_path, manifest


def _load_test_bed_cluster_hostnames_by_ip_opt() -> Optional[Dict[str, List[str]]]:
    cfg_path = _load_test_bed_bootstrap_config_path()
    if not cfg_path.exists():
        return None
    cfg = _require_dict(_load_yaml_file(cfg_path), f"test bed bootstrap config {cfg_path}")
    raw_deployconf = _require_str(cfg.get("deployconf_path"), "start_test_bed.deployconf_path")
    deployconf_path = Path(raw_deployconf)
    if not deployconf_path.is_absolute():
        deployconf_path = (cfg_path.parent / deployconf_path).resolve()
    if not deployconf_path.exists():
        return None
    deployconf = _require_dict(_load_yaml_file(deployconf_path), f"test bed deployconf {deployconf_path}")
    raw_nodes = deployconf.get("cluster_nodes")
    if not isinstance(raw_nodes, list):
        return None
    out: Dict[str, List[str]] = {}
    for idx, raw_node in enumerate(raw_nodes):
        node = _require_dict(raw_node, f"deployconf.cluster_nodes[{idx}]")
        hostname = _require_str(node.get("hostname"), f"deployconf.cluster_nodes[{idx}].hostname")
        ip = _require_str(node.get("ip"), f"deployconf.cluster_nodes[{idx}].ip")
        out.setdefault(ip, []).append(hostname)
    for ip, names in out.items():
        out[ip] = sorted(names)
    return out


def _canonical_targets_for_ip_from_test_bed(node_ip: str) -> List[str]:
    by_ip = _load_test_bed_cluster_hostnames_by_ip_opt()
    if by_ip is None:
        return []
    return list(by_ip.get(node_ip, []))


def _test_bed_manifest_transport_ctx_opt() -> Optional[Dict[str, Any]]:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return None
    manifest_path, manifest = manifest_info
    bastion = _require_dict(manifest.get("bastion"), f"test bed manifest {manifest_path}.bastion")
    bastion_user_raw = manifest.get("bastion_user")
    bastion_private_key_raw = manifest.get("bastion_private_key")
    bastion_password_raw = manifest.get("bastion_password")
    return {
        "manifest": manifest,
        "bastion_host": _require_str(bastion.get("host"), f"test bed manifest {manifest_path}.bastion.host"),
        "bastion_port": _require_int(
            bastion.get("ssh_port"),
            f"test bed manifest {manifest_path}.bastion.ssh_port",
            min_v=1,
        ),
        "bastion_user": (
            "root"
            if bastion_user_raw is None or not str(bastion_user_raw).strip()
            else _require_str(bastion_user_raw, f"test bed manifest {manifest_path}.bastion_user")
        ),
        "bastion_private_key": (
            None
            if bastion_private_key_raw is None or not str(bastion_private_key_raw).strip()
            else str(Path(str(bastion_private_key_raw)).expanduser().resolve())
        ),
        "bastion_password": (
            None
            if bastion_password_raw is None
            else _require_str(bastion_password_raw, f"test bed manifest {manifest_path}.bastion_password")
        ),
    }


def _clean_ssh_stderr_text(text: str) -> str:
    if not text:
        return ""
    lines: List[str] = []
    for raw in text.splitlines():
        if any(raw.startswith(prefix) for prefix in _SSH_STDERR_NOISE_PREFIXES):
            continue
        lines.append(raw)
    return "\n".join(lines).strip()


def _shell_quote(s: str) -> str:
    if s == "":
        return "''"
    if re.fullmatch(r"[A-Za-z0-9_./:=@+-]+", s):
        return s
    return "'" + s.replace("'", "'\\''") + "'"


def _controller_transport_manifest_opt(*, url: str) -> Optional[Dict[str, Any]]:
    manifest_info = _load_test_bed_manifest_opt()
    if manifest_info is None:
        return None
    manifest_path, manifest = manifest_info
    mode = _require_str(
        manifest.get("controller_request_mode"),
        f"testbed manifest {manifest_path}.controller_request_mode",
    )
    if mode != CONTROLLER_REQUEST_MODE_SSH_EXEC_PER_REQUEST:
        return None
    controller_url = _require_str(
        manifest.get("controller_url"),
        f"testbed manifest {manifest_path}.controller_url",
    ).rstrip("/")
    controller_public_url = _require_str(
        manifest.get("controller_public_url"),
        f"testbed manifest {manifest_path}.controller_public_url",
    ).rstrip("/")
    controller_cluster_url = str(manifest.get("controller_cluster_url") or "").rstrip("/")
    normalized_url = _require_str(url, "controller transport url").rstrip("/")
    allowed_prefixes = [controller_url, controller_public_url]
    if controller_cluster_url:
        allowed_prefixes.append(controller_cluster_url)
    if not any(normalized_url.startswith(prefix) for prefix in allowed_prefixes):
        return None
    return manifest


def _controller_request_exec_host(manifest: Dict[str, Any]) -> tuple[str, Optional[str], Optional[int], Optional[str]]:
    raw_exec_host = manifest.get("controller_exec_host")
    if raw_exec_host is not None and str(raw_exec_host).strip():
        exec_host = _require_str(raw_exec_host, "testbed manifest.controller_exec_host")
        exec_user_raw = manifest.get("controller_exec_user")
        exec_port_raw = manifest.get("controller_exec_port")
        exec_password_raw = manifest.get("controller_exec_password")
        exec_user = None if exec_user_raw is None else _require_str(exec_user_raw, "testbed manifest.controller_exec_user")
        exec_port = None if exec_port_raw is None else _require_int(exec_port_raw, "testbed manifest.controller_exec_port", min_v=1)
        exec_password = (
            None
            if exec_password_raw is None
            else _require_str(exec_password_raw, "testbed manifest.controller_exec_password")
        )
        return exec_host, exec_user, exec_port, exec_password
    bastion = _require_dict(manifest.get("bastion"), "testbed manifest.bastion")
    return _require_str(bastion.get("host"), "testbed manifest.bastion.host"), None, None, None


def _controller_request_url_via_manifest(manifest: Dict[str, Any], *, url: str) -> str:
    request_parts = urllib.parse.urlsplit(url)
    exec_host, _, _, _ = _controller_request_exec_host(manifest)
    bastion = _require_dict(manifest.get("bastion"), "testbed manifest.bastion")
    bastion_host = _require_str(bastion.get("host"), "testbed manifest.bastion.host")
    local_base = ""
    if exec_host == bastion_host:
        local_base = str(manifest.get("controller_bastion_local_url") or "").strip()
    if not local_base:
        local_base = str(manifest.get("controller_cluster_url") or "").strip()
    if not local_base:
        local_base = _require_str(
            manifest.get("controller_bastion_local_url"),
            "testbed manifest.controller_bastion_local_url",
        )
    local_parts = urllib.parse.urlsplit(local_base)
    return urllib.parse.urlunsplit(
        (local_parts.scheme, local_parts.netloc, request_parts.path, request_parts.query, "")
    )


def _controller_request_via_manifest(
    req: urllib.request.Request,
    *,
    timeout_seconds: float,
) -> Optional[tuple[int, bytes]]:
    manifest = _controller_transport_manifest_opt(url=str(req.full_url))
    if manifest is None:
        return None
    transport_ctx = _test_bed_manifest_transport_ctx_opt()
    if transport_ctx is None:
        raise ValueError("testbed transport manifest not found")
    exec_host, exec_user, exec_port, exec_password = _controller_request_exec_host(manifest)
    effective_url = _controller_request_url_via_manifest(manifest, url=str(req.full_url))
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
    argv = []
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
        proxy_parts = []
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
        argv.extend(["-o", "ProxyCommand=" + " ".join(_shell_quote(str(part)) for part in proxy_parts)])
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
                20.0,
            ),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise urllib.error.URLError(
            f"ssh controller request timed out: url={effective_url} timeout={timeout_seconds}"
        ) from exc
    stderr_text = _clean_ssh_stderr_text(completed.stderr.decode("utf-8", errors="replace"))
    if completed.returncode != 0:
        detail = stderr_text or completed.stdout.decode("utf-8", errors="replace") or f"ssh exited with rc={completed.returncode}"
        raise urllib.error.URLError(f"ssh controller request failed: url={effective_url} detail={detail}")
    lines = [line for line in stderr_text.splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"empty ssh controller response envelope: url={effective_url}")
    envelope = _require_dict(json.loads(lines[-1]), f"ssh controller response {effective_url}")
    transport_error = envelope.get("transport_error")
    if transport_error is not None:
        raise urllib.error.URLError(f"ssh controller transport error: url={effective_url} err={transport_error}")
    status_code = _require_int(envelope.get("status"), f"ssh controller response {effective_url}.status", min_v=100)
    return int(status_code), completed.stdout


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fluxon deployer adapter (Deployment YAML subset; produces deploy_result.yaml)."
    )
    parser.add_argument("--action", required=True, choices=["deploy", "collect", "teardown"])
    parser.add_argument(
        "--workdir",
        required=True,
        help="Run directory containing resolved_case.yaml; if relative, resolve against the repo root inferred from this script path",
    )
    args = parser.parse_args()

    run_dir = _resolve_repo_root_cli_path(raw_path=Path(args.workdir), field_name="workdir")
    if not run_dir.exists() or not run_dir.is_dir():
        print(f"ERROR: --workdir must be an existing directory: {run_dir}")
        raise SystemExit(2)

    resolved_path = run_dir / "resolved_case.yaml"
    if not resolved_path.exists():
        print(f"ERROR: missing resolved_case.yaml in run_dir: {resolved_path}")
        raise SystemExit(2)

    resolved_case = _load_yaml_file(resolved_path)
    resolved_case = _require_dict(resolved_case, "resolved_case")
    _validate_resolved_case_header(resolved_case)

    runtime = _require_dict(resolved_case.get("runtime"), "resolved_case.runtime")
    config_root = Path(_require_str(runtime.get("config_root"), "runtime.config_root")).resolve()
    run_dir_in_file = Path(_require_str(runtime.get("run_dir"), "runtime.run_dir")).resolve()
    stack_identity = _require_dict(runtime.get("stack_identity"), "resolved_case.runtime.stack_identity")
    _install_controller_basic_auth(
        stack_identity.get("controller_basic_auth"),
        field_name="resolved_case.runtime.stack_identity.controller_basic_auth",
    )
    if run_dir_in_file != run_dir:
        print(
            "ERROR: runtime.run_dir mismatch. "
            f"arg={run_dir} file={run_dir_in_file}"
        )
        raise SystemExit(2)

    deploy = _require_dict(resolved_case.get("deploy"), "resolved_case.deploy")
    controller_url = _require_str(deploy.get("controller_url"), "deploy.controller_url").rstrip("/")
    if not controller_url.startswith("http://") and not controller_url.startswith("https://"):
        raise ValueError("deploy.controller_url must start with http:// or https://")

    target_ip_map = _require_dict(deploy.get("target_ip_map"), "deploy.target_ip_map")

    instances_raw = _require_list(deploy.get("instances"), "deploy.instances")
    instances = [_parse_instance_req(x, config_root, target_ip_map) for x in instances_raw]

    if args.action == "deploy":
        ops_ready_timeout_seconds = _require_int(
            deploy.get("bootstrap_ready_timeout_seconds"),
            "deploy.bootstrap_ready_timeout_seconds",
            min_v=1,
        )
        payload_delivery = _parse_payload_delivery(deploy, instances)
        _action_deploy(
            run_dir,
            config_root,
            deploy,
            controller_url,
            instances,
            payload_delivery,
            ops_ready_timeout_seconds,
        )
        return

    if args.action == "collect":
        _action_collect(run_dir, controller_url, instances)
        return

    if args.action == "teardown":
        _action_teardown(controller_url, instances)
        return


@dataclass(frozen=True)
class _InstanceReq:
    id: str
    k8s_ref: str
    workload_kind: str
    workload_name: str
    authority: str
    target: str
    controller_target: str
    node_ip: str
    lifecycle: str
    endpoint_scheme: Optional[str]
    host_port: Optional[int]
    payload_file_rel: Optional[str]
    payload_file_abs: Optional[Path]
    payload_dest_path: Optional[str]

@dataclass(frozen=True)
class _PayloadDeliveryFluxonFsS3:
    s3_base_url: str
    bucket: str
    access_key: str
    secret_key: str
    region: str
    key_prefix: str


_SIGV4_ALG = "AWS4-HMAC-SHA256"
_SIGV4_SERVICE = "s3"
_SIGV4_TERM = "aws4_request"
_UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
_HTTP_TIMEOUT_SECONDS = 60
_HTTP_CHUNK_BYTES = 1024 * 1024


def _parse_payload_delivery(
    deploy: Dict[str, Any],
    instances: List[_InstanceReq],
) -> Optional[_PayloadDeliveryFluxonFsS3]:
    any_payload = any(inst.payload_file_abs is not None for inst in instances)
    if not any_payload:
        return None

    pd = _require_dict(deploy.get("payload_delivery"), "deploy.payload_delivery")
    _forbid_unknown_keys(
        pd,
        {"kind", "s3_base_url", "bucket", "access_key", "secret_key", "region", "key_prefix"},
        "deploy.payload_delivery",
    )
    kind = _require_str(pd.get("kind"), "deploy.payload_delivery.kind")
    if kind != PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3:
        raise ValueError(
            f"deploy.payload_delivery.kind must be {PAYLOAD_DELIVERY_KIND_FLUXON_FS_S3!r}, got: {kind!r}"
        )

    base_url = _require_str(pd.get("s3_base_url"), "deploy.payload_delivery.s3_base_url").rstrip("/")
    u = urllib.parse.urlparse(base_url)
    if u.scheme not in ("http", "https"):
        raise ValueError("deploy.payload_delivery.s3_base_url must start with http:// or https://")
    if not u.netloc:
        raise ValueError("deploy.payload_delivery.s3_base_url must include host:port")
    if not u.path or u.path == "/":
        raise ValueError("deploy.payload_delivery.s3_base_url must include a non-root path prefix (e.g. /fs_s3)")

    key_prefix = _require_str(pd.get("key_prefix"), "deploy.payload_delivery.key_prefix")
    if key_prefix.startswith("/") or key_prefix.endswith("/"):
        raise ValueError("deploy.payload_delivery.key_prefix must not start or end with '/'")
    if "\\" in key_prefix:
        raise ValueError("deploy.payload_delivery.key_prefix must not contain backslashes")
    if any(p in (".", "..", "") for p in key_prefix.split("/")):
        raise ValueError("deploy.payload_delivery.key_prefix must not contain empty / '.' / '..' segments")

    return _PayloadDeliveryFluxonFsS3(
        s3_base_url=base_url,
        bucket=_require_str(pd.get("bucket"), "deploy.payload_delivery.bucket"),
        access_key=_require_str(pd.get("access_key"), "deploy.payload_delivery.access_key"),
        secret_key=_require_str(pd.get("secret_key"), "deploy.payload_delivery.secret_key"),
        region=_require_str(pd.get("region"), "deploy.payload_delivery.region"),
        key_prefix=key_prefix,
    )


def _build_payload_object_key(key_prefix: str, payload_file_rel: str) -> str:
    if payload_file_rel.startswith("/") or payload_file_rel.endswith("/"):
        raise ValueError("deployer.payload_file must not start or end with '/'")
    if payload_file_rel.startswith("./") or payload_file_rel.startswith(".\\"):
        raise ValueError("deployer.payload_file must not start with './' (use a clean workdir-relative path)")
    if "\\" in payload_file_rel:
        raise ValueError("deployer.payload_file must not contain backslashes")
    if any(p in (".", "..", "") for p in payload_file_rel.split("/")):
        raise ValueError("deployer.payload_file must not contain empty / '.' / '..' segments")

    out = f"{key_prefix}/{payload_file_rel}"
    if out == ".fluxon_fs_s3_multipart" or out.startswith(".fluxon_fs_s3_multipart/"):
        raise ValueError("computed s3 object key uses reserved prefix: .fluxon_fs_s3_multipart")
    return out


def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _hmac_sha256(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def _derive_sigv4_signing_key(*, secret_key: str, scope_date: str, region: str) -> bytes:
    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), scope_date.encode("utf-8"))
    k_region = _hmac_sha256(k_date, region.encode("utf-8"))
    k_service = _hmac_sha256(k_region, _SIGV4_SERVICE.encode("utf-8"))
    return _hmac_sha256(k_service, _SIGV4_TERM.encode("utf-8"))


def _sigv4_authorization_header(
    *,
    access_key: str,
    secret_key: str,
    region: str,
    method: str,
    signing_path: str,
    query: str,
    host: str,
    scope_date: str,
    amz_date: str,
    payload_hash: str,
) -> str:
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    canonical_request = "\n".join(
        [
            method,
            signing_path,
            query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    cr_hash = _sha256_hex(canonical_request.encode("utf-8"))
    scope = f"{scope_date}/{region}/{_SIGV4_SERVICE}/{_SIGV4_TERM}"
    string_to_sign = "\n".join([_SIGV4_ALG, amz_date, scope, cr_hash])
    signing_key = _derive_sigv4_signing_key(secret_key=secret_key, scope_date=scope_date, region=region)
    sig = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return (
        f"{_SIGV4_ALG} "
        f"Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={sig}"
    )


def _fluxon_fs_s3_put_object(cfg: _PayloadDeliveryFluxonFsS3, *, object_key: str, local_path: Path) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"payload file not found: {local_path}")
    file_size = local_path.stat().st_size

    base = urllib.parse.urlparse(cfg.s3_base_url)
    if base.scheme not in ("http", "https"):
        raise ValueError("payload_delivery.s3_base_url must be http(s)")
    if not base.hostname:
        raise ValueError("payload_delivery.s3_base_url missing hostname")

    base_path = (base.path or "").rstrip("/")
    if base_path == "":
        raise ValueError("payload_delivery.s3_base_url must include a non-root path prefix (e.g. /fs_s3)")

    bucket_enc = urllib.parse.quote(cfg.bucket, safe="-_.~")
    key_enc = urllib.parse.quote(object_key, safe="/-_.~")
    req_path = f"{base_path}/{bucket_enc}/{key_enc}"
    # Sign the *actual* client-visible request path (including s3_base_url path prefix, e.g. "/fs_s3").
    signing_path = req_path

    now = datetime.datetime.utcnow()
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    scope_date = now.strftime("%Y%m%d")

    host_hdr = base.netloc
    auth = _sigv4_authorization_header(
        access_key=cfg.access_key,
        secret_key=cfg.secret_key,
        region=cfg.region,
        method="PUT",
        signing_path=signing_path,
        query="",
        host=host_hdr,
        scope_date=scope_date,
        amz_date=amz_date,
        payload_hash=_UNSIGNED_PAYLOAD,
    )

    conn_cls = http.client.HTTPSConnection if base.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(base.hostname, base.port, timeout=_HTTP_TIMEOUT_SECONDS)
    try:
        conn.putrequest("PUT", req_path)
        conn.putheader("Host", host_hdr)
        conn.putheader("x-amz-date", amz_date)
        conn.putheader("x-amz-content-sha256", _UNSIGNED_PAYLOAD)
        conn.putheader("Authorization", auth)
        conn.putheader("Content-Type", "application/octet-stream")
        conn.putheader("Content-Length", str(file_size))
        conn.endheaders()

        with local_path.open("rb") as f:
            while True:
                b = f.read(_HTTP_CHUNK_BYTES)
                if not b:
                    break
                conn.send(b)

        resp = conn.getresponse()
        body = resp.read(4096)
        if resp.status != 200:
            raise ValueError(
                "s3 put failed: "
                f"status={resp.status} reason={resp.reason!r} body={body!r} "
                f"bucket={cfg.bucket!r} object_key={object_key!r}"
            )
    finally:
        conn.close()


def _action_deploy(
    run_dir: Path,
    config_root: Path,
    deploy: Dict[str, Any],
    controller_url: str,
    instances: List[_InstanceReq],
    payload_delivery: Optional[_PayloadDeliveryFluxonFsS3],
    ops_ready_timeout_seconds: int,
) -> None:
    _preflight_ops_agents(
        controller_url,
        instances,
        ready_timeout_seconds=ops_ready_timeout_seconds,
    )

    deploy_yaml_path = run_dir / "deployer_deploy.yaml"
    if not deploy_yaml_path.exists():
        raise ValueError(f"missing deployer_deploy.yaml in run_dir: {deploy_yaml_path}")
    deploy_text = deploy_yaml_path.read_text(encoding="utf-8")
    target_alias_map = {
        inst.target: inst.controller_target
        for inst in instances
        if inst.target != inst.controller_target
    }
    deploy_text = _rewrite_deploy_yaml_node_targets(
        deploy_text=deploy_text,
        target_alias_map=target_alias_map,
    )

    upload_results: Dict[str, Dict[str, Any]] = {}

    if payload_delivery is not None:
        uploads: Dict[str, Path] = {}
        for inst in instances:
            if inst.payload_file_abs is None:
                continue
            if inst.payload_file_rel is None:
                raise ValueError("internal error: payload_file_abs is set but payload_file_rel is missing")
            object_key = _build_payload_object_key(payload_delivery.key_prefix, inst.payload_file_rel)
            prev = uploads.get(object_key)
            if prev is not None and prev != inst.payload_file_abs:
                raise ValueError(
                    "s3 object_key collision with different local files: "
                    f"object_key={object_key!r} prev={prev} now={inst.payload_file_abs}"
                )
            uploads[object_key] = inst.payload_file_abs

        for object_key, local_path in uploads.items():
            _fluxon_fs_s3_put_object(payload_delivery, object_key=object_key, local_path=local_path)

        for inst in instances:
            if inst.payload_file_abs is None:
                continue
            assert inst.payload_file_rel is not None
            object_key = _build_payload_object_key(payload_delivery.key_prefix, inst.payload_file_rel)
            upload_results[inst.id] = {
                "bucket": payload_delivery.bucket,
                "object_key": object_key,
                "s3_base_url": payload_delivery.s3_base_url,
                "payload_file": str(inst.payload_file_abs),
            }

    deploy_resp = _http_deploy(controller_url, deploy_text)

    history_id = deploy_resp.get("history_id")
    if isinstance(history_id, str) and history_id.strip():
        ui_url = controller_url.rstrip('/') + '/?history_id=' + history_id.strip()
        print('deployer_history_url=' + ui_url, flush=True)

    for inst in instances:
        if inst.lifecycle == _LIFECYCLE_SERVICE:
            _wait_running(
                controller_url,
                inst.controller_target,
                inst.workload_kind,
                inst.workload_name,
                inst.authority,
            )

    out_instances: List[Dict[str, Any]] = []
    for inst in instances:
        row: Dict[str, Any] = {
            "id": inst.id,
            "k8s_ref": inst.k8s_ref,
            # Deployer is not Kubernetes; keep a stable non-empty placeholder.
            "pod_name": f"deployer:{inst.controller_target}:{inst.k8s_ref}",
            "node_name": inst.controller_target,
            "node_ip": inst.node_ip,
        }
        if inst.host_port is not None:
            scheme = (inst.endpoint_scheme or _ENDPOINT_SCHEME_HTTP).lower()
            row["endpoint_url"] = f"{scheme}://{inst.node_ip}:{inst.host_port}"
        out_instances.append(row)

    deploy_result = {
        "schema_version": SCHEMA_VERSION,
        "instances": out_instances,
        "ready": True,
        "history_id": history_id,
    }
    _write_yaml_file(run_dir / "deploy_result.yaml", deploy_result)

    logs_dir = run_dir / "logs" / "deployer"
    logs_dir.mkdir(parents=True, exist_ok=True)
    _write_yaml_file(logs_dir / "upload_results.yaml", upload_results)
    _write_yaml_file(logs_dir / "deploy_response.yaml", deploy_resp)


def _action_collect(run_dir: Path, controller_url: str, instances: List[_InstanceReq]) -> None:
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    for inst in instances:
        inst_dir = logs_dir / inst.id
        inst_dir.mkdir(parents=True, exist_ok=True)

        # English note:
        # - /api/status is an observability endpoint. During transient runtime failures (e.g. P2P timeouts)
        #   the controller may return a non-2xx HTTP status. Treat that as captured status, not as a
        #   hard failure of the collect phase, so the runner can still finalize deterministically using
        #   terminal artifacts (summary.yaml / benchmark_result.json).
        status_code, status = _http_status_allow_error(
            controller_url,
            inst.controller_target,
            inst.workload_kind,
            inst.workload_name,
            inst.authority,
        )
        _write_yaml_file(inst_dir / "status.yaml", {"status_code": int(status_code), "status": status})



def _action_teardown(controller_url: str, instances: List[_InstanceReq]) -> None:
    for inst in instances:
        resp = _http_delete_generation(
            controller_url,
            inst.controller_target,
            inst.workload_kind,
            inst.workload_name,
            inst.authority,
        )
        ok = resp.get("ok")
        if ok is not True:
            raise ValueError(
                f"delete_generation failed: target={inst.target} controller_target={inst.controller_target} "
                f"kind={inst.workload_kind} name={inst.workload_name} resp={resp}"
            )


def _controller_target_for_target(target: str, target_ip_map: Dict[str, Any]) -> Tuple[str, str]:
    node_ip_raw = target_ip_map.get(target)
    if not isinstance(node_ip_raw, str) or not node_ip_raw.strip():
        raise ValueError(f"target_ip_map missing node ip for target: {target}")
    node_ip = node_ip_raw.strip()

    same_ip_targets: List[str] = []
    for raw_target, raw_ip in target_ip_map.items():
        candidate = _require_str(raw_target, "target_ip_map key")
        ip_value = _require_str(raw_ip, f"target_ip_map[{candidate!r}]")
        if ip_value == node_ip:
            same_ip_targets.append(candidate)
    if not same_ip_targets:
        raise ValueError(f"target_ip_map has no targets for node ip: target={target} node_ip={node_ip}")

    test_bed_targets = [candidate for candidate in _canonical_targets_for_ip_from_test_bed(node_ip) if candidate in same_ip_targets]
    if test_bed_targets:
        return test_bed_targets[0], node_ip

    bastion_targets = sorted(
        (candidate for candidate in same_ip_targets if candidate == "primary-bastion" or candidate.endswith("bastion")),
        key=lambda candidate: (0 if candidate == "primary-bastion" else 1, len(candidate), candidate),
    )
    if bastion_targets:
        return bastion_targets[0], node_ip

    canonical_targets = sorted(candidate for candidate in same_ip_targets if re.fullmatch(r"node-\d+", candidate))
    if canonical_targets:
        return canonical_targets[0], node_ip

    ordered = sorted(same_ip_targets, key=lambda candidate: (0 if candidate == target else 1, len(candidate), candidate))
    return ordered[0], node_ip


def _rewrite_deploy_yaml_node_targets(*, deploy_text: str, target_alias_map: Dict[str, str]) -> str:
    if not target_alias_map:
        return deploy_text
    docs = list(yaml.safe_load_all(deploy_text))
    changed = False
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        spec = doc.get("spec")
        if not isinstance(spec, dict):
            continue
        template = spec.get("template")
        if not isinstance(template, dict):
            continue
        template_spec = template.get("spec")
        if not isinstance(template_spec, dict):
            continue
        affinity = template_spec.get("affinity")
        if not isinstance(affinity, dict):
            continue
        node_affinity = affinity.get("nodeAffinity")
        if not isinstance(node_affinity, dict):
            continue
        required = node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution")
        if not isinstance(required, dict):
            continue
        terms = required.get("nodeSelectorTerms")
        if not isinstance(terms, list):
            continue
        for term in terms:
            if not isinstance(term, dict):
                continue
            expressions = term.get("matchExpressions")
            if not isinstance(expressions, list):
                continue
            for expr in expressions:
                if not isinstance(expr, dict):
                    continue
                if expr.get("key") != "kubernetes.io/hostname":
                    continue
                values = expr.get("values")
                if not isinstance(values, list):
                    continue
                new_values = [
                    target_alias_map.get(value, value) if isinstance(value, str) else value
                    for value in values
                ]
                if new_values != values:
                    expr["values"] = new_values
                    changed = True
    if not changed:
        return deploy_text
    return "\n---\n".join(yaml.safe_dump(doc, sort_keys=False).rstrip() for doc in docs) + "\n"


def _parse_instance_req(raw: Any, config_root: Path, target_ip_map: Dict[str, Any]) -> _InstanceReq:
    d = _require_dict(raw, "deploy.instances[]")
    _forbid_unknown_keys(d, {"id", "k8s_ref", "lifecycle", "endpoint", "deployer"}, "deploy.instances[]")

    iid = _require_str(d.get("id"), "deploy.instances[].id")
    k8s_ref = _require_str(d.get("k8s_ref"), "deploy.instances[].k8s_ref")
    if "/" not in k8s_ref:
        raise ValueError(f"deploy.instances[].k8s_ref must be <deployment|daemonset>/<name>, got: {k8s_ref!r}")
    k8s_ref_kind, workload_name = k8s_ref.split("/", 1)
    if k8s_ref_kind not in ("deployment", "daemonset"):
        raise ValueError(f"deploy.instances[].k8s_ref kind must be deployment or daemonset, got: {k8s_ref!r}")
    if not workload_name.strip():
        raise ValueError(f"deploy.instances[].k8s_ref name must be non-empty, got: {k8s_ref!r}")
    workload_kind = "Deployment" if k8s_ref_kind == "deployment" else "DaemonSet"

    lifecycle = _require_str(d.get("lifecycle"), "deploy.instances[].lifecycle").strip().lower()
    if lifecycle not in (_LIFECYCLE_SERVICE, _LIFECYCLE_JOB):
        raise ValueError("deploy.instances[].lifecycle must be 'service' or 'job'")

    endpoint_scheme = None
    host_port = None
    ep = d.get("endpoint")
    if ep is not None:
        ep_d = _require_dict(ep, "deploy.instances[].endpoint")
        _forbid_unknown_keys(ep_d, {"scheme", "host_port"}, "deploy.instances[].endpoint")
        endpoint_scheme = _require_str(ep_d.get("scheme"), "deploy.instances[].endpoint.scheme")
        if endpoint_scheme not in (_ENDPOINT_SCHEME_HTTP, _ENDPOINT_SCHEME_HTTPS):
            raise ValueError("invalid endpoint.scheme")
        hp = ep_d.get("host_port")
        if not isinstance(hp, int) or hp < 1 or hp > 65535:
            raise ValueError("invalid endpoint.host_port")
        host_port = int(hp)

    deployer = _require_dict(d.get("deployer"), "deploy.instances[].deployer")
    _forbid_unknown_keys(
        deployer,
        {"target", "payload_file", "payload_dest_path", "command", "args", "working_dir"},
        "deploy.instances[].deployer",
    )

    target = _require_str(deployer.get("target"), "deployer.target")
    controller_target, node_ip = _controller_target_for_target(target, target_ip_map)

    payload_file_abs: Optional[Path] = None
    payload_dest_path: Optional[str] = None
    payload_file_rel: Optional[str] = None
    raw_payload_file = deployer.get("payload_file")
    raw_payload_dest_path = deployer.get("payload_dest_path")
    if raw_payload_file is not None or raw_payload_dest_path is not None:
        payload_file = _require_str(raw_payload_file, "deployer.payload_file")
        if os.path.isabs(payload_file):
            raise ValueError("deployer.payload_file must be workdir-relative")
        if payload_file.startswith("./") or payload_file.startswith(".\\"):
            raise ValueError("deployer.payload_file must not start with './' (use a clean workdir-relative path)")
        if "\\" in payload_file:
            raise ValueError("deployer.payload_file must not contain backslashes")
        if any(p in (".", "..", "") for p in payload_file.split("/")):
            raise ValueError("deployer.payload_file must not contain empty / '.' / '..' segments")
        payload_file_rel = payload_file
        payload_file_abs = (config_root / payload_file).resolve()
        if not payload_file_abs.exists():
            raise ValueError(f"payload_file not found: {payload_file_abs}")

        payload_dest_path = _require_str(raw_payload_dest_path, "deployer.payload_dest_path")

    return _InstanceReq(
        id=iid,
        k8s_ref=k8s_ref,
        workload_kind=workload_kind,
        workload_name=workload_name,
        authority=workload_name,
        target=target,
        controller_target=controller_target,
        node_ip=node_ip,
        lifecycle=lifecycle,
        endpoint_scheme=endpoint_scheme,
        host_port=host_port,
        payload_file_rel=payload_file_rel,
        payload_file_abs=payload_file_abs,
        payload_dest_path=payload_dest_path,
    )


def _wait_running(
    controller_url: str,
    target: str,
    kind: str,
    name: str,
    authority: str,
) -> None:
    deadline = time.time() + 600.0
    last_detail = ""
    while True:
        status_code, st = _http_status_allow_error(controller_url, target, kind, name, authority)
        ok = st.get("ok")
        running = st.get("running")
        if status_code == 200 and ok is True and running is True:
            return
        last_detail = f"status_code={status_code} status={st}"
        if time.time() >= deadline:
            raise ValueError(f"status wait timeout: target={target} kind={kind} name={name} {last_detail}")
        time.sleep(1.0)


def _http_status_allow_error(
    controller_url: str,
    target: str,
    kind: str,
    name: str,
    authority: str,
) -> tuple[int, Dict[str, Any]]:
    qs = urllib.parse.urlencode(
        {"target": target, "kind": kind, "name": name, "authority": authority}
    )
    url = controller_url + "/api/status?" + qs
    req = _new_controller_request(url, method="GET")
    return _http_json_allow_error_status(req)


def _http_deploy(controller_url: str, yaml_text: str) -> Dict[str, Any]:
    url = controller_url + "/api/deploy"
    data = yaml_text.encode("utf-8")
    deadline = time.time() + _DEPLOY_GUARD_WAIT_SECONDS
    while True:
        req = _new_controller_request(
            url,
            method="POST",
            data=data,
            content_type="text/yaml; charset=utf-8",
        )
        try:
            resp = _http_json(req)
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise
            body = err.read()
            body_text = body.decode("utf-8", errors="replace").strip()
            payload: Optional[Dict[str, Any]] = None
            if body_text:
                try:
                    payload = _require_dict(json.loads(body_text), "deploy_conflict")
                except Exception:
                    payload = None
            if payload is not None:
                if payload.get("err") != _DEPLOY_GUARD_ERR:
                    raise ValueError(f"deploy failed: status=409 resp={payload}")
            elif body_text and _DEPLOY_GUARD_ERR not in body_text:
                raise ValueError(
                    "deploy conflict response is not valid json and does not look like a deploy guard conflict: "
                    f"status=409 body={body_text!r}"
                )
            if payload is None:
                print(
                    "[_http_deploy] treating non-json 409 response as transient deploy guard conflict: "
                    f"body={body_text!r}",
                    flush=True,
                )
            if time.time() >= deadline:
                detail: Any = payload if payload is not None else {"status": 409, "body": body_text}
                raise ValueError(f"deploy timed out waiting for deploy guard: {detail}")
            time.sleep(_DEPLOY_GUARD_POLL_SECONDS)
            continue
        if resp.get("ok") is True:
            return resp
        if resp.get("err") == _DEPLOY_GUARD_ERR and time.time() < deadline:
            time.sleep(_DEPLOY_GUARD_POLL_SECONDS)
            continue
        if resp.get("err") == _DEPLOY_GUARD_ERR:
            raise ValueError(f"deploy timed out waiting for deploy guard: {resp}")
        raise ValueError(f"deploy failed: {resp}")

def _http_workloads(controller_url: str) -> Dict[str, Any]:
    url = controller_url + "/api/workloads"
    req = _new_controller_request(url, method="GET")
    return _http_json(req)

def _http_agents(controller_url: str, instance_keys: List[str]) -> Dict[str, Any]:
    url = controller_url + "/api/agents"
    if instance_keys:
        url += "?" + urllib.parse.urlencode([("instance_key", key) for key in instance_keys])
    req = _new_controller_request(url, method="GET")
    return _http_json(req)


def _ops_agent_readiness_problems_by_target(
    controller_url: str,
    required_targets: List[str],
) -> Dict[str, str]:
    required_instance_keys = [OPS_AGENT_INSTANCE_KEY_PREFIX + str(target) for target in required_targets]
    payload = _http_agents(controller_url, required_instance_keys)
    agents = payload.get("agents")
    if not isinstance(agents, list):
        raise ValueError(f"invalid /api/agents response: missing agents list: {payload}")

    by_key: Dict[str, Dict[str, Any]] = {}
    for i, raw in enumerate(agents):
        if not isinstance(raw, dict):
            continue
        k = raw.get("instance_key")
        if not isinstance(k, str) or not k.strip():
            raise ValueError(f"invalid /api/workloads agents[{i}].instance_key: {raw!r}")
        by_key[k.strip()] = raw

    problems: Dict[str, str] = {}
    for target in required_targets:
        expected_key = OPS_AGENT_INSTANCE_KEY_PREFIX + str(target)
        agent = by_key.get(expected_key)
        if agent is None:
            problems[target] = f"missing agent={expected_key}"
            continue
        if agent.get("ok") is not True:
            err = agent.get("err")
            problems[target] = f"agent={expected_key} err={err!r}"
    return problems


def _describe_ops_agent_readiness_problem(
    controller_url: str,
    required_targets: List[str],
) -> Optional[str]:
    problems = _ops_agent_readiness_problems_by_target(controller_url, required_targets)
    if not problems:
        return None
    parts = [f"{target}: {problems[target]}" for target in sorted(problems)]
    return "ops agents not ready for requested targets: " + "; ".join(parts)


def _preflight_ops_agents(
    controller_url: str,
    instances: List[_InstanceReq],
    *,
    ready_timeout_seconds: int,
) -> None:
    """Wait until requested Ops agents become deploy-ready.

    English note:
    - This adapter is Ops-specific and may check Ops-specific readiness endpoints.
    - This keeps Ops coupling out of test_runner, so the runner can stay launcher-agnostic
      (swap adapter_cmd to switch to a k8s adapter later).
    """
    required_targets = sorted({inst.controller_target for inst in instances})
    if not required_targets:
        raise ValueError("internal error: empty instances list")

    deadline = time.time() + float(ready_timeout_seconds)
    last_problem: Optional[str] = None
    while True:
        try:
            last_problem = _describe_ops_agent_readiness_problem(controller_url, required_targets)
        except Exception as exc:
            last_problem = f"ops agent readiness check failed: {exc}"
        if last_problem is None:
            return
        if time.time() >= deadline:
            raise ValueError(last_problem)
        time.sleep(_DEPLOY_GUARD_POLL_SECONDS)

def _http_status(controller_url: str, target: str, kind: str, name: str) -> Dict[str, Any]:
    qs = urllib.parse.urlencode({"target": target, "kind": kind, "name": name})
    url = controller_url + "/api/status?" + qs
    req = _new_controller_request(url, method="GET")
    return _http_json(req)


def _http_delete_generation(
    controller_url: str,
    target: str,
    kind: str,
    name: str,
    authority: str,
) -> Dict[str, Any]:
    qs = urllib.parse.urlencode(
        {"target": target, "kind": kind, "name": name, "authority": authority}
    )
    url = controller_url + "/api/delete_generation?" + qs
    req = _new_controller_request(
        url,
        method="POST",
        data=b"",
        content_type="application/octet-stream",
    )
    return _http_json(req)


def _new_controller_request(
    url: str,
    *,
    method: str,
    data: bytes | None = None,
    content_type: str | None = None,
) -> urllib.request.Request:
    if _CONTROLLER_BASIC_AUTH_HEADER is None:
        raise RuntimeError("controller_basic_auth is not initialized")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header(_CONTROLLER_BASIC_AUTH_HEADER_NAME, _CONTROLLER_BASIC_AUTH_HEADER)
    if data is not None and content_type is not None:
        req.add_header("Content-Type", content_type)
    return req


def _retry_controller_request_or_raise(*, deadline: float, ctx: str, req: urllib.request.Request, exc: Exception) -> None:
    if time.time() >= deadline:
        raise ValueError(
            f"{ctx} controller request timed out after retry deadline: "
            f"url={req.full_url} err={type(exc).__name__}: {exc}"
        ) from exc
    print(
        f"[{ctx}] controller request transient error; retrying: "
        f"url={req.full_url} err={type(exc).__name__}: {exc}",
        flush=True,
    )
    time.sleep(_CONTROLLER_HTTP_RETRY_SLEEP_SECONDS)


def _http_json(req: urllib.request.Request) -> Dict[str, Any]:
    deadline = time.time() + _CONTROLLER_HTTP_RETRY_DEADLINE_SECONDS
    while True:
        try:
            transported = _controller_request_via_manifest(req, timeout_seconds=_CONTROLLER_HTTP_TIMEOUT_SECONDS)
            if transported is not None:
                status_code, data = transported
                if status_code in _CONTROLLER_TRANSIENT_HTTP_STATUS_CODES:
                    raise urllib.error.HTTPError(req.full_url, status_code, "transient", hdrs=None, fp=None)
                if status_code < 200 or status_code >= 300:
                    raise urllib.error.HTTPError(req.full_url, status_code, f"status={status_code}", hdrs=None, fp=None)
                obj = json.loads(data.decode("utf-8"))
                return _require_dict(obj, "http_json")
            with urllib.request.urlopen(req, timeout=_CONTROLLER_HTTP_TIMEOUT_SECONDS) as resp:
                data = resp.read()
            try:
                obj = json.loads(data.decode("utf-8"))
            except Exception as exc:
                raise ValueError(f"http response is not valid json: {exc}") from exc
            return _require_dict(obj, "http_json")
        except urllib.error.HTTPError as exc:
            if int(exc.code) not in _CONTROLLER_TRANSIENT_HTTP_STATUS_CODES:
                raise
            _retry_controller_request_or_raise(
                deadline=deadline,
                ctx="_http_json",
                req=req,
                exc=exc,
            )
        except (urllib.error.URLError, TimeoutError, OSError, ConnectionError) as exc:
            _retry_controller_request_or_raise(
                deadline=deadline,
                ctx="_http_json",
                req=req,
                exc=exc,
            )


def _http_json_allow_error_status(req: urllib.request.Request) -> tuple[int, Dict[str, Any]]:
    deadline = time.time() + _CONTROLLER_HTTP_RETRY_DEADLINE_SECONDS
    while True:
        try:
            transported = _controller_request_via_manifest(req, timeout_seconds=_CONTROLLER_HTTP_TIMEOUT_SECONDS)
            if transported is not None:
                status_code, data = transported
                return int(status_code), _require_dict(json.loads(data.decode("utf-8")), "http_json")
            return 200, _http_json(req)
        except urllib.error.HTTPError as err:
            if int(err.code) in _CONTROLLER_TRANSIENT_HTTP_STATUS_CODES:
                _retry_controller_request_or_raise(
                    deadline=deadline,
                    ctx="_http_json_allow_error_status",
                    req=req,
                    exc=err,
                )
                continue
            data = err.read()
            if not data:
                return err.code, {"ok": False, "err": f"http status {err.code}"}
            try:
                obj = json.loads(data.decode("utf-8"))
            except Exception as exc:
                raise ValueError(f"http error response is not valid json: status={err.code} err={exc}") from exc
            return err.code, _require_dict(obj, "http_error_json")
        except (urllib.error.URLError, TimeoutError, OSError, ConnectionError) as exc:
            _retry_controller_request_or_raise(
                deadline=deadline,
                ctx="_http_json_allow_error_status",
                req=req,
                exc=exc,
            )


def _load_yaml_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _write_yaml_file(path: Path, obj: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        yaml.safe_dump(
            obj,
            f,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=False,
        )
    tmp.replace(path)


def _require_dict(d: Any, ctx: str) -> Dict[str, Any]:
    if not isinstance(d, dict):
        raise ValueError(f"{ctx} must be a mapping")
    return d


def _require_list(d: Any, ctx: str) -> List[Any]:
    if not isinstance(d, list):
        raise ValueError(f"{ctx} must be a list")
    return d


def _require_str(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{ctx} must be a non-empty string")
    return v


def _require_basic_auth_username(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v:
        raise ValueError(f"{ctx} must be a non-empty string")
    if v.strip() != v:
        raise ValueError(f"{ctx} must not have leading/trailing whitespace")
    if ":" in v:
        raise ValueError(f"{ctx} must not contain ':'")
    return v


def _require_basic_auth_password(v: Any, ctx: str) -> str:
    if not isinstance(v, str) or not v:
        raise ValueError(f"{ctx} must be a non-empty string")
    if v.strip() != v:
        raise ValueError(f"{ctx} must not have leading/trailing whitespace")
    return v


def _parse_controller_basic_auth(value: Any, *, field_name: str) -> Dict[str, str]:
    auth = _require_dict(value, field_name)
    return {
        "username": _require_basic_auth_username(auth.get("username"), f"{field_name}.username"),
        "password": _require_basic_auth_password(auth.get("password"), f"{field_name}.password"),
    }


def _install_controller_basic_auth(value: Any, *, field_name: str) -> None:
    auth = _parse_controller_basic_auth(value, field_name=field_name)
    raw = f"{auth['username']}:{auth['password']}".encode("utf-8")
    global _CONTROLLER_BASIC_AUTH_HEADER
    _CONTROLLER_BASIC_AUTH_HEADER = "Basic " + base64.b64encode(raw).decode("ascii")


def _require_int(v: Any, ctx: str, *, min_v: int) -> int:
    if not isinstance(v, int):
        raise ValueError(f"{ctx} must be an integer")
    if v < min_v:
        raise ValueError(f"{ctx} must be >= {min_v}")
    return v


def _forbid_unknown_keys(d: Dict[str, Any], allowed: set[str], ctx: str) -> None:
    unknown = set(d.keys()) - allowed
    if unknown:
        raise ValueError(f"{ctx} contains unknown keys: {sorted(unknown)}")


def _validate_resolved_case_header(resolved_case: Dict[str, Any]) -> None:
    _forbid_unknown_keys(
        resolved_case,
        {"schema_version", "runtime", "runtime_model", "artifact_set", "case", "scene", "scale", "profile", "deploy"},
        "resolved_case",
    )
    if resolved_case.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"resolved_case.schema_version must be {SCHEMA_VERSION}")


if __name__ == "__main__":
    main()
