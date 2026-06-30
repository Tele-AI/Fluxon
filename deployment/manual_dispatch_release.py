#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import IO

import yaml


STRICT_RELEASE_FILE_REL_PATHS = ("install.py",)
DISPATCH_LOCK_FILENAME = "manual_dispatch_release.lock"
RELEASE_MANIFEST_SHA256_ENV_KEY = "FLUXON_RELEASE_MANIFEST_SHA256"
DISPATCH_RELEASE_SCOPE_DEPLOY_ONLY = "deploy_only"
DISPATCH_RELEASE_SCOPE_DEPLOY_AND_PROFILES = "deploy_and_profiles"
DISPATCH_RELEASE_SCOPE_START_TEST_BED = "start_test_bed"
START_TEST_BED_RELEASE_EXTRA_REL_PATHS = ("ext_images/ext_images.sha256",)
DISPATCH_RELEASE_SCOPES = (
    DISPATCH_RELEASE_SCOPE_DEPLOY_ONLY,
    DISPATCH_RELEASE_SCOPE_DEPLOY_AND_PROFILES,
    DISPATCH_RELEASE_SCOPE_START_TEST_BED,
)


@dataclass(frozen=True)
class DispatchReleaseScopeSpec:
    # Keep scope-level dispatch rules centralized here.
    # The final relpath list is still expanded dynamically from manifests and optional subtrees.
    required_base_relpaths: tuple[str, ...]
    strict_file_relpaths: tuple[str, ...]
    extra_manifest_relpaths: tuple[str, ...]
    include_manifest_ext_runtime_payloads: bool
    needs_ext_images_materialization: bool
    dispatch_profiles: bool
    dispatch_test_rsc: bool


DISPATCH_RELEASE_SCOPE_SPECS = {
    DISPATCH_RELEASE_SCOPE_DEPLOY_ONLY: DispatchReleaseScopeSpec(
        required_base_relpaths=("fluxon_release.sha256", "install.py"),
        strict_file_relpaths=STRICT_RELEASE_FILE_REL_PATHS,
        extra_manifest_relpaths=(),
        include_manifest_ext_runtime_payloads=False,
        needs_ext_images_materialization=True,
        dispatch_profiles=False,
        dispatch_test_rsc=False,
    ),
    DISPATCH_RELEASE_SCOPE_DEPLOY_AND_PROFILES: DispatchReleaseScopeSpec(
        required_base_relpaths=("fluxon_release.sha256", "install.py"),
        strict_file_relpaths=STRICT_RELEASE_FILE_REL_PATHS,
        extra_manifest_relpaths=(),
        include_manifest_ext_runtime_payloads=False,
        needs_ext_images_materialization=True,
        dispatch_profiles=True,
        dispatch_test_rsc=True,
    ),
    DISPATCH_RELEASE_SCOPE_START_TEST_BED: DispatchReleaseScopeSpec(
        required_base_relpaths=("fluxon_release.sha256", "install.py"),
        strict_file_relpaths=STRICT_RELEASE_FILE_REL_PATHS + ("ext_images/ext_images.sha256",),
        extra_manifest_relpaths=START_TEST_BED_RELEASE_EXTRA_REL_PATHS,
        include_manifest_ext_runtime_payloads=True,
        needs_ext_images_materialization=False,
        dispatch_profiles=False,
        dispatch_test_rsc=False,
    ),
}

# English note:
# - This script must not block on interactive SSH prompts (host key / password), because it is
#   frequently triggered by automation.
# - Password auth is handled via SSH_ASKPASS (see _check_call_bash_with_optional_password).
# - Host key prompts must be avoided explicitly; accept-new is non-interactive and fails fast on
#   changed keys.
SSH_COMMON_OPTS = "-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"
SCP_COMMON_OPTS = SSH_COMMON_OPTS


def _acquire_dispatch_lock(*, lock_path: Path, cfg_path: Path, src_release_dir: Path) -> IO[str]:
    # English note:
    # - This script is frequently triggered by automation and may be started multiple times concurrently.
    # - Concurrent dispatch can produce an inconsistent release view for Fluxon FS clients:
    #   one process may update the manifest earlier/later than the referenced files.
    # - Enforce single-writer behavior per deployconf to keep releases atomic at the operator level.
    #
    # Implementation:
    # - Use an OS-level file lock (fcntl.flock) on a deterministic path next to the deployconf.
    # - Open in a mode that does not truncate if we fail to acquire the lock.
    import fcntl

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    f = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        # Best-effort: show existing lock content if present.
        try:
            f.seek(0)
            existing = f.read().strip()
        except Exception:  # noqa: BLE001
            existing = ""
        msg = (
            "Another manual_dispatch_release process is active; refusing to run concurrently.\n"
            f"- lock_file={lock_path}\n"
            f"- existing_lock={existing!r}\n"
            f"- deployconf={cfg_path}\n"
            f"- release_dir={src_release_dir}\n"
            "If the lock is stale, terminate the running process and retry."
        )
        print(msg, file=sys.stderr, flush=True)
        raise SystemExit(2)

    f.seek(0)
    f.truncate()
    f.write(f"pid={os.getpid()}\n")
    f.write(f"deployconf={cfg_path.resolve()}\n")
    f.write(f"release_dir={src_release_dir.resolve()}\n")
    f.flush()
    return f


def _find_repo_root_from_script_path(script_path: Path) -> Path:
    script_abs = script_path.resolve()
    for d in [script_abs.parent] + list(script_abs.parents):
        if (d / "deployment").is_dir() and (d / "deployment" / "manual_dispatch_release.py").is_file():
            return d
    raise RuntimeError(
        "Cannot locate repo root from manual_dispatch_release.py path.\n"
        f"script={script_abs}\n"
        "Expected to find a parent directory containing deployment/manual_dispatch_release.py."
    )


def _resolve_repo_root_cli_path(*, repo_root: Path, raw_path: Path, field_name: str) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    resolved = (repo_root / raw_path).resolve()
    if not resolved:
        raise RuntimeError(f"failed to resolve {field_name} against repo root: raw={raw_path}")
    return resolved


def _parse_sha256_manifest(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for idx, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) != 2:
            raise RuntimeError(f"Invalid fluxon_release.sha256 line {idx}: {raw}")
        digest, relpath = parts
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise RuntimeError(f"Invalid sha256 digest in fluxon_release.sha256 line {idx}: {digest}")
        out[relpath] = digest
    if not out:
        raise RuntimeError("fluxon_release.sha256 is empty")
    return out


def _dispatch_release_scope_spec(dispatch_release_scope: str) -> DispatchReleaseScopeSpec:
    spec = DISPATCH_RELEASE_SCOPE_SPECS.get(dispatch_release_scope)
    if spec is None:
        raise RuntimeError(f"unsupported dispatch_release_scope: {dispatch_release_scope}")
    return spec



def _write_askpass_script(*, password: str) -> tuple[tempfile.TemporaryDirectory, Path]:
    td = tempfile.TemporaryDirectory(prefix="fluxon_ssh_askpass_")
    path = Path(td.name) / "askpass.sh"
    # English-only comments (project rule).
    #
    # We use SSH_ASKPASS to avoid external deps like sshpass. ssh/scp will call SSH_ASKPASS
    # only when there is no controlling TTY; we ensure that by running under setsid with
    # stdin redirected to /dev/null.
    path.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"printf '%s\\n' {sh_quote(password)}\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o700)
    return td, path


def _check_call_bash_with_optional_password(*, password: str | None, cmd: str) -> None:
    if password is None:
        subprocess.check_call(["bash", "-lc", cmd])
        return

    td, askpass_path = _write_askpass_script(password=password)
    try:
        env = os.environ.copy()
        env["SSH_ASKPASS"] = str(askpass_path)
        env["SSH_ASKPASS_REQUIRE"] = "force"
        env["DISPLAY"] = "fluxon:0"
        subprocess.check_call(
            ["bash", "-lc", cmd],
            env=env,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    finally:
        td.cleanup()


def _check_output_bash_with_optional_password(*, password: str | None, cmd: str) -> str:
    if password is None:
        return subprocess.check_output(["bash", "-lc", cmd], text=True)

    td, askpass_path = _write_askpass_script(password=password)
    try:
        env = os.environ.copy()
        env["SSH_ASKPASS"] = str(askpass_path)
        env["SSH_ASKPASS_REQUIRE"] = "force"
        env["DISPLAY"] = "fluxon:0"
        return subprocess.check_output(
            ["bash", "-lc", cmd],
            env=env,
            text=True,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    finally:
        td.cleanup()


def _release_manifest_required_relpaths(manifest_text: str) -> list[str]:
    out = ["fluxon_release.sha256", "install.py"]
    manifest_relpaths: list[str] = []
    for line in manifest_text.splitlines():
        parts = line.strip().split()
        if len(parts) != 2:
            raise RuntimeError(f"Invalid fluxon_release.sha256 line: {line}")
        manifest_relpaths.append(parts[1])

    # English note:
    # - ext_images/ is the release-owned runtime binary tree for deploy flows.
    # - ext_images.tar.gz is the canonical shipping unit. After dispatch we materialize ext_images/ by extracting
    #   ext_images.tar.gz on each target node so runtime paths like /hostworkdir/fluxon_release/ext_images/... exist.
    if "ext_images.tar.gz" not in set(manifest_relpaths):
        raise RuntimeError("fluxon_release.sha256 is missing required ext_images.tar.gz entry")
    out.extend(manifest_relpaths)
    return out


def _release_scope_required_relpaths(*, manifest_text: str, dispatch_release_scope: str) -> list[str]:
    spec = _dispatch_release_scope_spec(dispatch_release_scope)
    if not spec.include_manifest_ext_runtime_payloads:
        return _release_manifest_required_relpaths(manifest_text)

    # start_test_bed dispatches the manifest-defined release surface except ext_images.tar.gz,
    # then materializes the ext runtime tree explicitly from ext_images/ext_images.sha256.
    manifest_relpaths = list(_parse_sha256_manifest(manifest_text).keys())
    ext_manifest_relpaths = _start_test_bed_ext_manifest_relpaths_from_release_manifest_text(
        manifest_text=manifest_text
    )
    out = list(spec.required_base_relpaths)
    for relpath in manifest_relpaths:
        if relpath == "ext_images.tar.gz":
            continue
        out.append(relpath)
    out.extend(spec.extra_manifest_relpaths)
    out.extend(ext_manifest_relpaths)
    seen: set[str] = set()
    deduped: list[str] = []
    for relpath in out:
        if relpath in seen:
            continue
        seen.add(relpath)
        deduped.append(relpath)
    return deduped


def _release_scope_strict_file_relpaths(dispatch_release_scope: str) -> tuple[str, ...]:
    return _dispatch_release_scope_spec(dispatch_release_scope).strict_file_relpaths


def _release_scope_needs_ext_images_materialization(dispatch_release_scope: str) -> bool:
    return _dispatch_release_scope_spec(dispatch_release_scope).needs_ext_images_materialization


def _start_test_bed_ext_manifest_relpaths_from_release_manifest_text(*, manifest_text: str) -> list[str]:
    manifest_relpaths = set(_parse_sha256_manifest(manifest_text).keys())
    ext_manifest_relpath = "ext_images/ext_images.sha256"
    if ext_manifest_relpath not in manifest_relpaths:
        raise RuntimeError(
            "start_test_bed release scope requires ext_images/ext_images.sha256 to enumerate runtime files"
        )
    return [ext_manifest_relpath]


def _cluster_node_execution_mode(raw_node: dict[str, object], *, hostname: str) -> str:
    execution_mode = raw_node.get("execution_mode", "ssh")
    if not isinstance(execution_mode, str) or execution_mode.strip() not in {"ssh", "local"}:
        raise RuntimeError(f"cluster_nodes[{hostname}].execution_mode must be 'ssh' or 'local'")
    return execution_mode.strip()


def _start_test_bed_ext_runtime_relpaths_from_src_release_dir(*, src_release_dir: Path) -> list[str]:
    ext_manifest_path = src_release_dir / "ext_images" / "ext_images.sha256"
    if not ext_manifest_path.exists():
        raise RuntimeError(f"Missing ext_images manifest required by start_test_bed scope: {ext_manifest_path}")
    ext_manifest = _parse_sha256_manifest(ext_manifest_path.read_text(encoding="utf-8"))
    out = ["ext_images/ext_images.sha256"]
    out.extend(f"ext_images/{relpath}" for relpath in ext_manifest.keys())
    return out


def _validate_start_test_bed_ext_images_integrity(*, src_release_dir: Path) -> None:
    ext_manifest_path = src_release_dir / "ext_images" / "ext_images.sha256"
    if not ext_manifest_path.exists():
        raise RuntimeError(f"Missing ext_images manifest required by start_test_bed scope: {ext_manifest_path}")
    ext_manifest = _parse_sha256_manifest(ext_manifest_path.read_text(encoding="utf-8"))
    for relpath, expected_sha in ext_manifest.items():
        file_path = src_release_dir / "ext_images" / relpath
        if not file_path.exists():
            raise RuntimeError(
                "start_test_bed scope references a missing ext_images payload.\n"
                f"manifest={ext_manifest_path}\n"
                f"missing={file_path}"
            )
        got_sha = _sha256_file(file_path)
        if got_sha != expected_sha:
            raise RuntimeError(
                "start_test_bed ext_images payload drift detected.\n"
                f"manifest={ext_manifest_path}\n"
                f"file={file_path}\n"
                f"expected_sha256={expected_sha}\n"
                f"actual_sha256={got_sha}\n"
                "Re-run setup_and_pack/pack_release_ext.py before dispatch."
            )


def _materialize_local_ext_images_from_tarball(*, dst_release_dir_s: str, dst_owner: str) -> None:
    ext_dir = Path(dst_release_dir_s) / "ext_images"
    tarball = Path(dst_release_dir_s) / "ext_images.tar.gz"
    if ext_dir.exists():
        if not ext_dir.is_dir():
            raise RuntimeError(f"release invariant path must be a directory: {ext_dir}")
        ext_manifest = ext_dir / "ext_images.sha256"
        if ext_manifest.exists():
            try:
                manifest = _parse_sha256_manifest(ext_manifest.read_text(encoding="utf-8"))
            except RuntimeError:
                manifest = {}
            if manifest and all((ext_dir / relpath).exists() for relpath in manifest.keys()):
                return
        subprocess.check_call(["bash", "-lc", "rm -rf " + sh_quote(str(ext_dir))])
    if not tarball.exists():
        raise RuntimeError(f"missing ext_images.tar.gz in dispatched release dir: {tarball}")
    subprocess.check_call(
        [
            "bash",
            "-lc",
            "tar -xzf " + sh_quote(str(tarball)) + " -C " + sh_quote(dst_release_dir_s),
        ]
    )


def _materialize_remote_ext_images_from_tarball(
    *,
    dst_release_dir_s: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
    dst_owner: str,
) -> None:
    # English note:
    # - The dispatched release directory must contain ext_images.tar.gz (validated earlier).
    # - We extract it on the target node to create ext_images/ so deployconf entrypoints can use it directly.
    _check_call_bash_with_optional_password(
        password=ssh_password,
        cmd=(
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote(
                "set -euo pipefail; "
                + "cd "
                + sh_quote(dst_release_dir_s)
                + "; "
                + "need_extract=1; "
                + "if [ -d ext_images ] && [ -f ext_images/ext_images.sha256 ]; then "
                + "python3 - <<'PY'\n"
                + "from pathlib import Path\n"
                + "root = Path('ext_images')\n"
                + "manifest = root / 'ext_images.sha256'\n"
                + "ok = True\n"
                + "for raw in manifest.read_text(encoding='utf-8').splitlines():\n"
                + "    line = raw.strip()\n"
                + "    if not line:\n"
                + "        continue\n"
                + "    parts = line.split()\n"
                + "    if len(parts) != 2:\n"
                + "        ok = False\n"
                + "        break\n"
                + "    relpath = parts[1]\n"
                + "    if not (root / relpath).exists():\n"
                + "        ok = False\n"
                + "        break\n"
                + "raise SystemExit(0 if ok else 1)\n"
                + "PY\n"
                + " && need_extract=0; "
                + "fi; "
                + "if [ \"$need_extract\" = 1 ]; then rm -rf ext_images && tar -xzf ext_images.tar.gz; fi; "
                + "true"
            )
        ),
    )


def _should_dispatch_profiles(*, dispatch_release_scope: str) -> bool:
    return _dispatch_release_scope_spec(dispatch_release_scope).dispatch_profiles


def _should_dispatch_test_rsc(*, dispatch_release_scope: str) -> bool:
    return _dispatch_release_scope_spec(dispatch_release_scope).dispatch_test_rsc


def _release_dispatch_relpaths(*, src_release_dir: Path, dispatch_release_scope: str) -> list[str]:
    manifest_path = src_release_dir / "fluxon_release.sha256"
    if not manifest_path.exists():
        raise RuntimeError(f"Missing fluxon_release.sha256 in {src_release_dir}")
    # Build the dispatch surface from:
    # - scope-level static rules
    # - the release manifest
    # - optional runtime/profile/test_rsc subtrees present in this release dir
    relpaths = _release_scope_required_relpaths(
        manifest_text=manifest_path.read_text(encoding="utf-8"),
        dispatch_release_scope=dispatch_release_scope,
    )
    if dispatch_release_scope == DISPATCH_RELEASE_SCOPE_START_TEST_BED:
        relpaths.extend(
            _start_test_bed_ext_runtime_relpaths_from_src_release_dir(src_release_dir=src_release_dir)
        )
    if _should_dispatch_profiles(dispatch_release_scope=dispatch_release_scope):
        # English note:
        # - test_stack variants live under fluxon_release/profiles/<profile_id>/.
        # - suite artifact_sets reference them via key_prefix "profiles/<profile_id>".
        # - profiles are not part of the top-level fluxon_release.sha256, so we must dispatch them
        #   explicitly when the caller requests the full release export surface.
        profiles_dir = src_release_dir / "profiles"
        if profiles_dir.exists() and profiles_dir.is_dir():
            relpaths.append("profiles")
    if _should_dispatch_test_rsc(dispatch_release_scope=dispatch_release_scope):
        # English note:
        # - CI / TEST_STACK runtime resources are exported under fluxon_release/test_rsc/<profile_id>/.
        # - test_runner materializes them through artifact_sets[*].test_rsc_source.key_prefix.
        # - They are not part of the top-level fluxon_release.sha256, so dispatch must carry them
        #   explicitly together with profiles/*.
        test_rsc_dir = src_release_dir / "test_rsc"
        if test_rsc_dir.exists() and test_rsc_dir.is_dir():
            relpaths.append("test_rsc")
    seen: set[str] = set()
    out: list[str] = []
    for relpath in relpaths:
        if relpath in seen:
            continue
        seen.add(relpath)
        out.append(relpath)
    return out


def _profile_manifest_relpaths(*, src_release_dir: Path, dispatch_release_scope: str) -> list[str]:
    if not _should_dispatch_profiles(dispatch_release_scope=dispatch_release_scope):
        return []
    profiles_dir = src_release_dir / "profiles"
    if not profiles_dir.exists() or not profiles_dir.is_dir():
        return []
    out: list[str] = []
    for child in sorted(profiles_dir.iterdir()):
        if not child.is_dir():
            continue
        manifest_path = child / "fluxon_release.sha256"
        if not manifest_path.exists():
            continue
        out.append(f"profiles/{child.name}/fluxon_release.sha256")
    return out


def _test_rsc_manifest_relpaths(*, src_release_dir: Path, dispatch_release_scope: str) -> list[str]:
    if not _should_dispatch_test_rsc(dispatch_release_scope=dispatch_release_scope):
        return []
    test_rsc_dir = src_release_dir / "test_rsc"
    if not test_rsc_dir.exists() or not test_rsc_dir.is_dir():
        return []
    out: list[str] = []
    for child in sorted(test_rsc_dir.iterdir()):
        if not child.is_dir():
            continue
        manifest_path = child / "fluxon_test_rsc.sha256"
        if not manifest_path.exists():
            continue
        out.append(f"test_rsc/{child.name}/fluxon_test_rsc.sha256")
    return out


def _dispatch_tmp_root(*, deployconf_path: Path) -> Path:
    # English note:
    # - Do not inherit TMPDIR from outer automation (it may point into a tool-managed .dever namespace).
    # - Keep temp artifacts next to the deployconf so the path is deterministic and discoverable.
    p = deployconf_path.resolve().parent / ".manual_dispatch_release_tmp"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _release_manifest_payload_sha256(*, cfg: dict, src_release_dir: Path) -> str:
    global_envs = cfg.get("global_envs")
    if not isinstance(global_envs, dict):
        raise RuntimeError("deployconf.global_envs must be a mapping")
    release_manifest_name = global_envs.get("FLUXON_RELEASE_SHA256_FILE")
    if not isinstance(release_manifest_name, str) or not release_manifest_name.strip():
        raise RuntimeError("deployconf.global_envs.FLUXON_RELEASE_SHA256_FILE must be a non-empty string")
    manifest_path = src_release_dir / release_manifest_name
    if not manifest_path.exists():
        raise RuntimeError(f"Missing release manifest for bare-script fingerprint: {manifest_path}")
    return _sha256_file(manifest_path)


def _with_release_manifest_sha256_env(*, cfg: dict, release_manifest_sha256: str) -> dict:
    global_envs = cfg.get("global_envs")
    if not isinstance(global_envs, dict):
        raise RuntimeError("deployconf.global_envs must be a mapping")
    if RELEASE_MANIFEST_SHA256_ENV_KEY in global_envs:
        raise RuntimeError(
            f"deployconf.global_envs must not predefine {RELEASE_MANIFEST_SHA256_ENV_KEY}; "
            "manual_dispatch_release injects the current release fingerprint explicitly"
        )
    cfg_with_release_fingerprint = dict(cfg)
    cfg_with_release_fingerprint["global_envs"] = dict(global_envs)
    cfg_with_release_fingerprint["global_envs"][RELEASE_MANIFEST_SHA256_ENV_KEY] = release_manifest_sha256
    return cfg_with_release_fingerprint


def _validate_release_manifest_integrity(*, src_release_dir: Path, dispatch_release_scope: str) -> None:
    manifest_path = src_release_dir / "fluxon_release.sha256"
    if not manifest_path.exists():
        raise RuntimeError(f"Missing fluxon_release.sha256 in {src_release_dir}")
    manifest = _parse_sha256_manifest(manifest_path.read_text(encoding="utf-8"))
    for relpath, expected_sha in manifest.items():
        file_path = src_release_dir / relpath
        if not file_path.exists():
            raise RuntimeError(f"Release manifest references missing file: {file_path}")
        got_sha = _sha256_file(file_path)
        if got_sha != expected_sha:
            raise RuntimeError(
                "Release manifest drift detected.\n"
                f"release_dir={src_release_dir}\n"
                f"file={relpath}\n"
                f"expected_sha256={expected_sha}\n"
                f"actual_sha256={got_sha}\n"
                "Regenerate the release before dispatch so manifest and artifacts are generated together."
            )
    for relpath in _release_scope_strict_file_relpaths(dispatch_release_scope):
        file_path = src_release_dir / relpath
        if not file_path.exists():
            raise RuntimeError(f"Release dispatch scope references a missing strict file: {file_path}")

    if dispatch_release_scope == DISPATCH_RELEASE_SCOPE_START_TEST_BED:
        _validate_start_test_bed_ext_images_integrity(src_release_dir=src_release_dir)

    # English note:
    # Profiles are dispatched explicitly but are not referenced by the top-level fluxon_release.sha256.
    # Validate each profile's own fluxon_release.sha256 so dispatch can fail fast if a profile is stale
    # or internally inconsistent.
    for rel_manifest_path in _profile_manifest_relpaths(
        src_release_dir=src_release_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        profile_manifest_path = src_release_dir / rel_manifest_path
        profile_root = profile_manifest_path.parent
        profile_manifest = _parse_sha256_manifest(profile_manifest_path.read_text(encoding="utf-8"))
        for relpath, expected_sha in profile_manifest.items():
            file_path = profile_root / relpath
            if not file_path.exists():
                raise RuntimeError(
                    "Profile release manifest references missing file.\n"
                    f"release_dir={src_release_dir}\n"
                    f"profile_manifest={rel_manifest_path}\n"
                    f"file={relpath}\n"
                    f"missing={file_path}"
                )
            got_sha = _sha256_file(file_path)
            if got_sha != expected_sha:
                raise RuntimeError(
                    "Profile release manifest drift detected.\n"
                    f"release_dir={src_release_dir}\n"
                    f"profile_manifest={rel_manifest_path}\n"
                    f"file={relpath}\n"
                    f"expected_sha256={expected_sha}\n"
                    f"actual_sha256={got_sha}\n"
                    "Regenerate the corresponding release profile before dispatch."
                )

    for rel_manifest_path in _test_rsc_manifest_relpaths(
        src_release_dir=src_release_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        test_rsc_manifest_path = src_release_dir / rel_manifest_path
        test_rsc_root = test_rsc_manifest_path.parent
        test_rsc_manifest = _parse_sha256_manifest(test_rsc_manifest_path.read_text(encoding="utf-8"))
        for relpath, expected_sha in test_rsc_manifest.items():
            file_path = test_rsc_root / relpath
            if not file_path.exists():
                raise RuntimeError(
                    "Profile test_rsc manifest references missing file.\n"
                    f"release_dir={src_release_dir}\n"
                    f"test_rsc_manifest={rel_manifest_path}\n"
                    f"file={relpath}\n"
                    f"missing={file_path}"
                )
            got_sha = _sha256_file(file_path)
            if got_sha != expected_sha:
                raise RuntimeError(
                    "Profile test_rsc manifest drift detected.\n"
                    f"release_dir={src_release_dir}\n"
                    f"test_rsc_manifest={rel_manifest_path}\n"
                    f"file={relpath}\n"
                    f"expected_sha256={expected_sha}\n"
                    f"actual_sha256={got_sha}\n"
                    "Regenerate the corresponding test_rsc subtree before dispatch."
                )


def _dst_aliases_src(*, src_dir: Path, dst_dir: Path) -> bool:
    try:
        return dst_dir.exists() and dst_dir.resolve() == src_dir.resolve()
    except OSError:
        return False


def _local_release_is_current(
    *,
    src_release_dir: Path,
    dst_release_dir: Path,
    dispatch_release_scope: str,
) -> bool:
    if _dst_aliases_src(src_dir=src_release_dir, dst_dir=dst_release_dir):
        return True
    manifest_path = src_release_dir / "fluxon_release.sha256"
    if not manifest_path.exists():
        return False
    dst_manifest_path = dst_release_dir / "fluxon_release.sha256"
    if not dst_manifest_path.exists():
        return False
    manifest_text = manifest_path.read_text(encoding="utf-8")
    if dst_manifest_path.read_text(encoding="utf-8") != manifest_text:
        return False
    for relpath in _release_scope_required_relpaths(
        manifest_text=manifest_text,
        dispatch_release_scope=dispatch_release_scope,
    ):
        if not (dst_release_dir / relpath).exists():
            return False
    for relpath in _release_scope_strict_file_relpaths(dispatch_release_scope):
        if _sha256_file(src_release_dir / relpath) != _sha256_file(dst_release_dir / relpath):
            return False
    if dispatch_release_scope == DISPATCH_RELEASE_SCOPE_START_TEST_BED:
        for relpath in _start_test_bed_ext_runtime_relpaths_from_src_release_dir(src_release_dir=src_release_dir):
            src_path = src_release_dir / relpath
            dst_path = dst_release_dir / relpath
            if not dst_path.exists() or _sha256_file(src_path) != _sha256_file(dst_path):
                return False

    # English note:
    # Profiles are not covered by the top-level fluxon_release.sha256, so also compare each
    # profile's own fluxon_release.sha256 to avoid treating a partially-updated release as current.
    for rel_manifest_path in _profile_manifest_relpaths(
        src_release_dir=src_release_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        src_profile_manifest = src_release_dir / rel_manifest_path
        dst_profile_manifest = dst_release_dir / rel_manifest_path
        if not dst_profile_manifest.exists():
            return False
        src_text = src_profile_manifest.read_text(encoding="utf-8")
        if dst_profile_manifest.read_text(encoding="utf-8") != src_text:
            return False
        profile_prefix = rel_manifest_path.rsplit("/", 1)[0]
        for required in _release_manifest_required_relpaths(src_text):
            if not (dst_release_dir / profile_prefix / required).exists():
                return False
        for relpath in STRICT_RELEASE_FILE_REL_PATHS:
            src_path = src_release_dir / profile_prefix / relpath
            dst_path = dst_release_dir / profile_prefix / relpath
            if src_path.exists() and (not dst_path.exists() or _sha256_file(src_path) != _sha256_file(dst_path)):
                return False
    return True


def _remote_release_is_current(
    *,
    src_release_dir: Path,
    dst_release_dir: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
    dispatch_release_scope: str,
) -> bool:
    manifest_path = src_release_dir / "fluxon_release.sha256"
    if not manifest_path.exists():
        return False
    manifest_text = manifest_path.read_text(encoding="utf-8")
    remote_manifest_cmd = (
        "ssh "
        + SSH_COMMON_OPTS
        + " -p "
        + sh_quote(str(ssh_port))
        + " "
        + sh_quote(f"{ssh_user}@{ip}")
        + " "
        + sh_quote(
            "if [ -f "
            + sh_quote(dst_release_dir + "/fluxon_release.sha256")
            + " ]; then cat "
            + sh_quote(dst_release_dir + "/fluxon_release.sha256")
            + "; fi"
        )
    )
    remote_manifest_text = _check_output_bash_with_optional_password(
        password=ssh_password,
        cmd=remote_manifest_cmd,
    )
    if remote_manifest_text != manifest_text:
        return False
    required_relpaths = _release_scope_required_relpaths(
        manifest_text=manifest_text,
        dispatch_release_scope=dispatch_release_scope,
    )
    required_checks = " && ".join(
        "test -e " + sh_quote(dst_release_dir + "/" + relpath) for relpath in required_relpaths
    )
    remote_validate_cmd = (
        "ssh "
        + SSH_COMMON_OPTS
        + " -p "
        + sh_quote(str(ssh_port))
        + " "
        + sh_quote(f"{ssh_user}@{ip}")
        + " "
        + sh_quote(required_checks)
    )
    try:
        _check_call_bash_with_optional_password(password=ssh_password, cmd=remote_validate_cmd)
    except subprocess.CalledProcessError:
        return False
    for relpath in _release_scope_strict_file_relpaths(dispatch_release_scope):
        local_sha = _sha256_file(src_release_dir / relpath)
        remote_sha_cmd = (
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote("sha256sum " + sh_quote(dst_release_dir + "/" + relpath) + " | awk '{print $1}'")
        )
        remote_sha = _check_output_bash_with_optional_password(
            password=ssh_password,
            cmd=remote_sha_cmd,
        ).strip()
        if remote_sha != local_sha:
            return False
    if dispatch_release_scope == DISPATCH_RELEASE_SCOPE_START_TEST_BED:
        for relpath in _start_test_bed_ext_runtime_relpaths_from_src_release_dir(src_release_dir=src_release_dir):
            local_sha = _sha256_file(src_release_dir / relpath)
            remote_sha_cmd = (
                "ssh "
                + SSH_COMMON_OPTS
                + " -p "
                + sh_quote(str(ssh_port))
                + " "
                + sh_quote(f"{ssh_user}@{ip}")
                + " "
                + sh_quote("sha256sum " + sh_quote(dst_release_dir + "/" + relpath) + " | awk '{print $1}'")
            )
            remote_sha = _check_output_bash_with_optional_password(
                password=ssh_password,
                cmd=remote_sha_cmd,
            ).strip()
            if remote_sha != local_sha:
                return False

    # English note:
    # Profiles are dispatched explicitly but are not part of the top-level fluxon_release.sha256.
    # If a profile wheel changes (e.g. transport backend hotfix), the top-level manifest may remain
    # unchanged, so we must compare profile-level manifests too; otherwise dispatch may incorrectly
    # skip updating profiles and consumers will see sha256 mismatches.
    for rel_manifest_path in _profile_manifest_relpaths(
        src_release_dir=src_release_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        local_manifest_text = (src_release_dir / rel_manifest_path).read_text(encoding="utf-8")
        remote_manifest_cmd = (
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote(
                "if [ -f "
                + sh_quote(dst_release_dir + "/" + rel_manifest_path)
                + " ]; then cat "
                + sh_quote(dst_release_dir + "/" + rel_manifest_path)
                + "; fi"
            )
        )
        remote_text = _check_output_bash_with_optional_password(
            password=ssh_password,
            cmd=remote_manifest_cmd,
        )
        if remote_text != local_manifest_text:
            return False

        profile_prefix = rel_manifest_path.rsplit("/", 1)[0]
        required_relpaths = _release_manifest_required_relpaths(local_manifest_text)
        required_checks = " && ".join(
            "test -e " + sh_quote(dst_release_dir + "/" + profile_prefix + "/" + relpath) for relpath in required_relpaths
        )
        profile_validate_cmd = (
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote(required_checks)
        )
        try:
            _check_call_bash_with_optional_password(password=ssh_password, cmd=profile_validate_cmd)
        except subprocess.CalledProcessError:
            return False

        for relpath in STRICT_RELEASE_FILE_REL_PATHS:
            local_path = src_release_dir / profile_prefix / relpath
            if not local_path.exists():
                continue
            local_sha = _sha256_file(local_path)
            remote_sha_cmd = (
                "ssh "
                + SSH_COMMON_OPTS
                + " -p "
                + sh_quote(str(ssh_port))
                + " "
                + sh_quote(f"{ssh_user}@{ip}")
                + " "
                + sh_quote(
                    "sha256sum "
                    + sh_quote(dst_release_dir + "/" + profile_prefix + "/" + relpath)
                    + " | awk '{print $1}'"
                )
            )
            remote_sha = _check_output_bash_with_optional_password(
                password=ssh_password,
                cmd=remote_sha_cmd,
            ).strip()
            if remote_sha != local_sha:
                return False
    for rel_manifest_path in _test_rsc_manifest_relpaths(
        src_release_dir=src_release_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        local_manifest_text = (src_release_dir / rel_manifest_path).read_text(encoding="utf-8")
        remote_manifest_cmd = (
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote(
                "if [ -f "
                + sh_quote(dst_release_dir + "/" + rel_manifest_path)
                + " ]; then cat "
                + sh_quote(dst_release_dir + "/" + rel_manifest_path)
                + "; fi"
            )
        )
        remote_text = _check_output_bash_with_optional_password(
            password=ssh_password,
            cmd=remote_manifest_cmd,
        )
        if remote_text != local_manifest_text:
            return False
        test_rsc_prefix = rel_manifest_path.rsplit("/", 1)[0]
        test_rsc_manifest = _parse_sha256_manifest(local_manifest_text)
        required_checks = " && ".join(
            "test -e " + sh_quote(dst_release_dir + "/" + test_rsc_prefix + "/" + relpath)
            for relpath in test_rsc_manifest.keys()
        )
        if required_checks:
            test_rsc_validate_cmd = (
                "ssh "
                + SSH_COMMON_OPTS
                + " -p "
                + sh_quote(str(ssh_port))
                + " "
                + sh_quote(f"{ssh_user}@{ip}")
                + " "
                + sh_quote(required_checks)
            )
            try:
                _check_call_bash_with_optional_password(password=ssh_password, cmd=test_rsc_validate_cmd)
            except subprocess.CalledProcessError:
                return False
        for relpath, expected_sha in test_rsc_manifest.items():
            remote_sha_cmd = (
                "ssh "
                + SSH_COMMON_OPTS
                + " -p "
                + sh_quote(str(ssh_port))
                + " "
                + sh_quote(f"{ssh_user}@{ip}")
                + " "
                + sh_quote(
                    "sha256sum "
                    + sh_quote(dst_release_dir + "/" + test_rsc_prefix + "/" + relpath)
                    + " | awk '{print $1}'"
                )
            )
            remote_sha = _check_output_bash_with_optional_password(
                password=ssh_password,
                cmd=remote_sha_cmd,
            ).strip()
            if remote_sha != expected_sha:
                return False
    return True


def _create_local_stage_dir(*, dst_dir_s: str, dst_owner: str) -> str:
    dst_dir = Path(dst_dir_s)
    parent_dir = dst_dir.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    stage_dir = Path(tempfile.mkdtemp(prefix=f".{dst_dir.name}.stage.", dir=str(parent_dir)))
    return str(stage_dir)


def _finalize_local_staged_dir(*, stage_dir_s: str, dst_dir_s: str) -> None:
    dst_dir = Path(dst_dir_s)
    backup_dir = Path(tempfile.mkdtemp(prefix=f".{dst_dir.name}.old.", dir=str(dst_dir.parent)))
    backup_dir.rmdir()
    subprocess.check_call(
        [
            "bash",
            "-lc",
            "if [ -e "
            + sh_quote(dst_dir_s)
            + " ] || [ -L "
            + sh_quote(dst_dir_s)
            + " ]; then mv "
            + sh_quote(dst_dir_s)
            + " "
            + sh_quote(str(backup_dir))
            + "; fi",
        ]
    )
    subprocess.check_call(["bash", "-lc", "mv " + sh_quote(stage_dir_s) + " " + sh_quote(dst_dir_s)])
    subprocess.check_call(["bash", "-lc", "rm -rf " + sh_quote(str(backup_dir))])


def _create_remote_stage_dir(
    *,
    dst_dir_s: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
    dst_owner: str,
) -> str:
    dst_dir = Path(dst_dir_s)
    parent_dir_s = dst_dir.parent.as_posix()
    _check_call_bash_with_optional_password(
        password=ssh_password,
        cmd=(
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote("mkdir -p " + parent_dir_s)
        ),
    )
    stage_dir_s = _check_output_bash_with_optional_password(
        password=ssh_password,
        cmd=(
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote("mktemp -d " + sh_quote(parent_dir_s + f"/.{dst_dir.name}.stage.XXXXXX"))
        ),
    ).strip()
    return stage_dir_s


def _finalize_remote_staged_dir(
    *,
    stage_dir_s: str,
    dst_dir_s: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
) -> None:
    dst_dir = Path(dst_dir_s)
    backup_template = dst_dir.parent.as_posix() + f"/.{dst_dir.name}.old.XXXXXX"
    _check_call_bash_with_optional_password(
        password=ssh_password,
        cmd=(
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote(
                "backup=$(mktemp -d "
                + sh_quote(backup_template)
                + ") && rmdir \"$backup\" && "
                + "if [ -e "
                + sh_quote(dst_dir_s)
                + " ] || [ -L "
                + sh_quote(dst_dir_s)
                + " ]; then mv "
                + sh_quote(dst_dir_s)
                + " \"$backup\"; fi && "
                + "mv "
                + sh_quote(stage_dir_s)
                + " "
                + sh_quote(dst_dir_s)
                + " && "
                + "if [ -n \"${backup:-}\" ] && [ -e \"$backup\" -o -L \"$backup\" ]; then "
                + "rm -rf \"$backup\" || "
                + "{ "
                + "echo "
                + sh_quote(
                    "[manual_dispatch_release] warning: failed to remove old staged backup; keep it for later cleanup: "
                )
                + " \"$backup\" 1>&2; "
                + "}; "
                + "fi"
            )
        ),
    )


def _ensure_remote_dir_owned_by_user(
    *,
    dst_dir_s: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
    dst_owner: str,
) -> None:
    _check_call_bash_with_optional_password(
        password=ssh_password,
        cmd=(
            "ssh "
            + SSH_COMMON_OPTS
            + " -p "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(f"{ssh_user}@{ip}")
            + " "
            + sh_quote("mkdir -p " + dst_dir_s)
        ),
    )


def _replace_local_artifact_with_symlink(*, src_dir: Path, dst_dir_s: str, dst_owner: str) -> None:
    dst_dir = Path(dst_dir_s)
    parent_dir = dst_dir.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    if dst_dir.is_symlink() and dst_dir.resolve() == src_dir.resolve():
        return
    subprocess.check_call(["bash", "-lc", "rm -rf " + sh_quote(dst_dir_s)])
    subprocess.check_call(["bash", "-lc", "ln -s " + sh_quote(str(src_dir)) + " " + sh_quote(dst_dir_s)])


def _copy_local_artifact(*, src_dir: Path, dst_dir_s: str, dst_owner: str) -> None:
    stage_dir_s = _create_local_stage_dir(dst_dir_s=dst_dir_s, dst_owner=dst_owner)
    subprocess.check_call(["bash", "-lc", "cp -a " + sh_quote(str(src_dir) + "/.") + " " + sh_quote(stage_dir_s + "/")])
    _finalize_local_staged_dir(stage_dir_s=stage_dir_s, dst_dir_s=dst_dir_s)


def _copy_local_release_artifact(
    *,
    src_dir: Path,
    dst_dir_s: str,
    dst_owner: str,
    dispatch_release_scope: str,
) -> None:
    stage_dir_s = _create_local_stage_dir(dst_dir_s=dst_dir_s, dst_owner=dst_owner)
    for relpath in _release_dispatch_relpaths(
        src_release_dir=src_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        src_path = (src_dir / relpath).resolve()
        relpath_obj = Path(relpath)
        dst_path = Path(stage_dir_s) / relpath_obj
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if src_path.is_dir():
            subprocess.check_call(
                ["bash", "-lc", "cp -a " + sh_quote(str(src_path)) + " " + sh_quote(str(dst_path.parent))]
            )
            continue
        subprocess.check_call(
            ["bash", "-lc", "cp -a " + sh_quote(str(src_path)) + " " + sh_quote(str(dst_path))]
        )
    _finalize_local_staged_dir(stage_dir_s=stage_dir_s, dst_dir_s=dst_dir_s)
    if _release_scope_needs_ext_images_materialization(dispatch_release_scope):
        _materialize_local_ext_images_from_tarball(dst_release_dir_s=dst_dir_s, dst_owner=dst_owner)


def _copy_remote_artifact(
    *,
    src_dir: Path,
    dst_dir_s: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
    dst_owner: str,
) -> None:
    stage_dir_s = _create_remote_stage_dir(
        dst_dir_s=dst_dir_s,
        ssh_user=ssh_user,
        ip=ip,
        ssh_port=ssh_port,
        ssh_password=ssh_password,
        dst_owner=dst_owner,
    )
    _check_call_bash_with_optional_password(
        password=ssh_password,
        cmd=(
            "scp "
            + SCP_COMMON_OPTS
            + " -r -p -P "
            + sh_quote(str(ssh_port))
            + " "
            + sh_quote(str(src_dir) + "/.")
            + " "
            + sh_quote(f"{ssh_user}@{ip}:{stage_dir_s}/")
        ),
    )
    _finalize_remote_staged_dir(
        stage_dir_s=stage_dir_s,
        dst_dir_s=dst_dir_s,
        ssh_user=ssh_user,
        ip=ip,
        ssh_port=ssh_port,
        ssh_password=ssh_password,
    )


def _copy_remote_release_artifact(
    *,
    src_dir: Path,
    dst_dir_s: str,
    ssh_user: str,
    ip: str,
    ssh_port: int,
    ssh_password: str | None,
    dst_owner: str,
    dispatch_release_scope: str,
) -> None:
    stage_dir_s = _create_remote_stage_dir(
        dst_dir_s=dst_dir_s,
        ssh_user=ssh_user,
        ip=ip,
        ssh_port=ssh_port,
        ssh_password=ssh_password,
        dst_owner=dst_owner,
    )
    for relpath in _release_dispatch_relpaths(
        src_release_dir=src_dir,
        dispatch_release_scope=dispatch_release_scope,
    ):
        src_path = (src_dir / relpath).resolve()
        relpath_obj = Path(relpath)
        remote_parent = relpath_obj.parent.as_posix()
        remote_parent_dir = stage_dir_s if remote_parent == "." else stage_dir_s + "/" + remote_parent
        _ensure_remote_dir_owned_by_user(
            dst_dir_s=remote_parent_dir,
            ssh_user=ssh_user,
            ip=ip,
            ssh_port=ssh_port,
            ssh_password=ssh_password,
            dst_owner=dst_owner,
        )
        if src_path.is_dir():
            _check_call_bash_with_optional_password(
                password=ssh_password,
                cmd=(
                    "scp "
                    + SCP_COMMON_OPTS
                    + " -r -p -P "
                    + sh_quote(str(ssh_port))
                    + " "
                    + sh_quote(str(src_path))
                    + " "
                    + sh_quote(f"{ssh_user}@{ip}:{remote_parent_dir}/")
                ),
            )
            _ensure_remote_dir_owned_by_user(
                dst_dir_s=stage_dir_s + "/" + relpath_obj.as_posix(),
                ssh_user=ssh_user,
                ip=ip,
                ssh_port=ssh_port,
                ssh_password=ssh_password,
                dst_owner=dst_owner,
            )
            continue
        _check_call_bash_with_optional_password(
            password=ssh_password,
            cmd=(
                "scp "
                + SCP_COMMON_OPTS
                + " -r -p -P "
                + sh_quote(str(ssh_port))
                + " "
                + sh_quote(str(src_path))
                + " "
                + sh_quote(f"{ssh_user}@{ip}:{stage_dir_s}/{relpath_obj.as_posix()}")
            ),
        )
    _finalize_remote_staged_dir(
        stage_dir_s=stage_dir_s,
        dst_dir_s=dst_dir_s,
        ssh_user=ssh_user,
        ip=ip,
        ssh_port=ssh_port,
        ssh_password=ssh_password,
    )
    if _release_scope_needs_ext_images_materialization(dispatch_release_scope):
        _materialize_remote_ext_images_from_tarball(
            dst_release_dir_s=dst_dir_s,
            ssh_user=ssh_user,
            ip=ip,
            ssh_port=ssh_port,
            ssh_password=ssh_password,
            dst_owner=dst_owner,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Manually dispatch fluxon_release + generated bare scripts to all nodes defined in a deployconf YAML.\n"
            "\n"
            "This script is intentionally strict:\n"
            "- It can dispatch an explicit release dir when the caller selects a variant-specific release.\n"
            "- It generates bare scripts directly from the provided deployconf before dispatch.\n"
            "- SSH settings are read from cluster_nodes[].ssh_user/ssh_port.\n"
            "- It runs commands sequentially and fails fast on the first error.\n"
            "- It uses ssh/scp; if cluster_nodes[].ssh_password is set, it uses SSH_ASKPASS (no sshpass).\n"
        )
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        required=True,
        help=(
            "Path to deployconf YAML; if relative, resolve against the repo root inferred from "
            "this script path"
        ),
    )
    parser.add_argument(
        "--release-dir",
        type=Path,
        help=(
            "Explicit fluxon_release source dir; if relative, resolve against the repo root "
            "inferred from this script path; defaults to <repo_root>/fluxon_release"
        ),
    )
    parser.add_argument(
        "--release-scope",
        choices=DISPATCH_RELEASE_SCOPES,
        default=DISPATCH_RELEASE_SCOPE_DEPLOY_AND_PROFILES,
        help=(
            "Dispatch release surface. "
            "'deploy_only' sends the full top-level deploy release without profiles; "
            "'deploy_and_profiles' also sends profiles/* suite artifacts; "
            "'start_test_bed' sends only install.py, wheels, pylib_src.tar.gz, and the etcd/greptime/tikv ext_images "
            "payload required by fluxon_test_stack/start_test_bed.py."
        ),
    )
    args = parser.parse_args()

    try:
        repo_root = _find_repo_root_from_script_path(Path(__file__))
    except Exception as e:
        print(str(e), file=sys.stderr)
        raise SystemExit(1)

    cfg_path = _resolve_repo_root_cli_path(repo_root=repo_root, raw_path=args.config, field_name="config")
    if not cfg_path.exists():
        print(f"deployconf not found: {cfg_path}", file=sys.stderr)
        raise SystemExit(1)

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        print("deployconf root must be a mapping", file=sys.stderr)
        raise SystemExit(1)

    cluster_nodes = cfg.get("cluster_nodes")
    if not isinstance(cluster_nodes, list) or not cluster_nodes:
        print("deployconf.cluster_nodes must be a non-empty list", file=sys.stderr)
        raise SystemExit(1)

    # Source locations are split by responsibility:
    # - repo_root is where generator sources live (deployment/manual_dispatch_release.py, gen_bare_deploy_bash.py)
    # - release_dir may be an explicit run-scoped materialized release and therefore must not be
    #   inferred from the deployconf parent chain
    src_release_dir = (
        _resolve_repo_root_cli_path(repo_root=repo_root, raw_path=args.release_dir, field_name="release-dir")
        if args.release_dir is not None
        else (repo_root / "fluxon_release").resolve()
    )
    dispatch_release_scope = args.release_scope
    if not src_release_dir.exists() or not src_release_dir.is_dir():
        print(f"fluxon_release dir not found: expected={src_release_dir}", file=sys.stderr)
        print(f"deployconf={cfg_path}", file=sys.stderr)
        raise SystemExit(1)

    tmp_root = _dispatch_tmp_root(deployconf_path=cfg_path)
    _dispatch_lock_f = _acquire_dispatch_lock(
        lock_path=tmp_root / DISPATCH_LOCK_FILENAME,
        cfg_path=cfg_path,
        src_release_dir=src_release_dir,
    )
    try:
        _validate_release_manifest_integrity(
            src_release_dir=src_release_dir,
            dispatch_release_scope=dispatch_release_scope,
        )
    except Exception as e:
        print(str(e), file=sys.stderr)
        raise SystemExit(1)

    try:
        cfg_for_bare_generation = _with_release_manifest_sha256_env(
            cfg=cfg,
            release_manifest_sha256=_release_manifest_payload_sha256(
                cfg=cfg,
                src_release_dir=src_release_dir,
            ),
        )
    except Exception as e:
        print(str(e), file=sys.stderr)
        raise SystemExit(1)
    generated_bare_cfg_path = tmp_root / "deployconf.with_release_manifest_sha256.yaml"
    generated_bare_cfg_path.write_text(
        yaml.safe_dump(cfg_for_bare_generation, sort_keys=False),
        encoding="utf-8",
    )

    gen_bare_script = repo_root / "deployment" / "gen_bare_deploy_bash.py"
    prebuilt_bare_scripts_dir = repo_root / "deployment" / "gen_bare_deploy_bash"
    try:
        has_gen_bare_script = gen_bare_script.is_file()
    except OSError:
        has_gen_bare_script = False
    has_prebuilt_bare_scripts = prebuilt_bare_scripts_dir.is_dir()

    bare_scripts_tmpdir: tempfile.TemporaryDirectory[str] | None = None
    if has_gen_bare_script:
        bare_scripts_tmpdir = tempfile.TemporaryDirectory(
            dir=str(tmp_root),
            prefix="fluxon_gen_bare_deploy_bash_",
        )
        try:
            src_bare_scripts_dir = (Path(bare_scripts_tmpdir.name) / "gen_bare_deploy_bash").resolve()
            subprocess.check_call(
                [
                    sys.executable,
                    str(gen_bare_script.resolve()),
                    "-c",
                    str(generated_bare_cfg_path.resolve()),
                    "-w",
                    str(src_bare_scripts_dir),
                ]
            )
        except Exception as e:
            bare_scripts_tmpdir.cleanup()
            print(f"failed to generate bare deploy scripts from deployconf: {e}", file=sys.stderr)
            raise SystemExit(1)
    elif has_prebuilt_bare_scripts:
        src_bare_scripts_dir = prebuilt_bare_scripts_dir.resolve()
    else:
        print(
            "gen_bare_deploy_bash source is missing: expected either "
            f"{gen_bare_script} or {prebuilt_bare_scripts_dir}",
            file=sys.stderr,
        )
        raise SystemExit(1)

    # Destination paths are fixed for self-host:
    # - ${HOSTWORKDIR}/fluxon_release
    # - ${HOSTWORKDIR}/gen_bare_deploy_bash
    artifacts = [
        ("fluxon_release", src_release_dir, "fluxon_release"),
        ("gen_bare_deploy_bash", src_bare_scripts_dir, "gen_bare_deploy_bash"),
    ]

    # We treat the node whose hostname matches local hostname as "local". This avoids
    # unnecessary ssh for the master node where you run this script.
    local_host = (
        subprocess.check_output(["bash", "-lc", "hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown"])
        .decode("utf-8")
        .strip()
    )

    print(f"dispatch src_release_dir={src_release_dir}")
    print(f"dispatch src_bare_scripts_dir={src_bare_scripts_dir}")
    print(f"local hostname={local_host}")

    try:
        for raw in cluster_nodes:
            if not isinstance(raw, dict):
                print("cluster_nodes entries must be mappings", file=sys.stderr)
                raise SystemExit(1)
            hostname = raw.get("hostname")
            ip = raw.get("ip")
            hostworkdir = raw.get("hostworkdir")
            ssh_user = raw.get("ssh_user")
            ssh_port = raw.get("ssh_port")
            ssh_password = raw.get("ssh_password")
            if not isinstance(hostname, str) or not hostname.strip():
                print("cluster_nodes[].hostname must be a non-empty string", file=sys.stderr)
                raise SystemExit(1)
            if not isinstance(ip, str) or not ip.strip():
                print(f"cluster_nodes[{hostname}].ip must be a non-empty string", file=sys.stderr)
                raise SystemExit(1)
            if not isinstance(hostworkdir, str) or not hostworkdir.strip():
                print(f"cluster_nodes[{hostname}].hostworkdir must be a non-empty string", file=sys.stderr)
                raise SystemExit(1)
            if not isinstance(ssh_user, str) or not ssh_user.strip():
                print(f"cluster_nodes[{hostname}].ssh_user must be a non-empty string", file=sys.stderr)
                raise SystemExit(1)
            if not isinstance(ssh_port, int) or ssh_port <= 0:
                print(f"cluster_nodes[{hostname}].ssh_port must be a positive int", file=sys.stderr)
                raise SystemExit(1)
            if ssh_password is not None and (not isinstance(ssh_password, str) or not ssh_password):
                print(f"cluster_nodes[{hostname}].ssh_password must be a non-empty string when present", file=sys.stderr)
                raise SystemExit(1)
            execution_mode = _cluster_node_execution_mode(raw, hostname=hostname)

            dst_owner = f"{ssh_user}:{ssh_user}"

            print("")
            print(f"[dispatch] node={hostname} ip={ip} hostworkdir={hostworkdir} execution_mode={execution_mode}")

            if execution_mode == "local" or hostname == local_host:
                # Ensure hostworkdir itself is writable so bare scripts can create run/log dirs.
                subprocess.check_call(["bash", "-lc", f"mkdir -p {sh_quote(hostworkdir)}"])
                for artifact_name, src_dir, dst_subdir in artifacts:
                    dst_dir = Path(hostworkdir) / dst_subdir
                    dst_dir_s = str(dst_dir)
                    print(f"[dispatch] local copy {artifact_name}: {src_dir} -> {dst_dir_s}")
                    if artifact_name == "fluxon_release":
                        _copy_local_release_artifact(
                            src_dir=src_dir,
                            dst_dir_s=dst_dir_s,
                            dst_owner=dst_owner,
                            dispatch_release_scope=dispatch_release_scope,
                        )
                    else:
                        _copy_local_artifact(src_dir=src_dir, dst_dir_s=dst_dir_s, dst_owner=dst_owner)
                continue

            # Ensure hostworkdir itself is writable so bare scripts can create run/log dirs.
            _check_call_bash_with_optional_password(
                password=ssh_password,
                cmd=(
                    "ssh "
                    + SSH_COMMON_OPTS
                    + " -p "
                    + sh_quote(str(ssh_port))
                    + " "
                    + sh_quote(f"{ssh_user}@{ip}")
                    + " "
                    + sh_quote("mkdir -p " + hostworkdir)
                ),
            )

            for artifact_name, src_dir, dst_subdir in artifacts:
                dst_dir = Path(hostworkdir) / dst_subdir
                dst_dir_s = str(dst_dir)
                print(f"[dispatch] remote copy {artifact_name}: {src_dir} -> {dst_dir_s}")
                if artifact_name == "fluxon_release":
                    _copy_remote_release_artifact(
                        src_dir=src_dir,
                        dst_dir_s=dst_dir_s,
                        ssh_user=ssh_user,
                        ip=ip,
                        ssh_port=ssh_port,
                        ssh_password=ssh_password,
                        dst_owner=dst_owner,
                        dispatch_release_scope=dispatch_release_scope,
                    )
                else:
                    _copy_remote_artifact(
                        src_dir=src_dir,
                        dst_dir_s=dst_dir_s,
                        ssh_user=ssh_user,
                        ip=ip,
                        ssh_port=ssh_port,
                        ssh_password=ssh_password,
                        dst_owner=dst_owner,
                    )
    finally:
        if bare_scripts_tmpdir is not None:
            bare_scripts_tmpdir.cleanup()


def sh_quote(s: str) -> str:
    # Minimal shell single-quote quoting for bash -lc.
    return "'" + s.replace("'", "'\"'\"'") + "'"


if __name__ == "__main__":
    main()
