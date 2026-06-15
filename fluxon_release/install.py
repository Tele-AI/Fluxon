#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import re
import subprocess
import sys
import tarfile
import time
import zipfile
from contextlib import contextmanager
from pathlib import Path


READ_CHUNK_BYTES = 1024 * 1024
FINGERPRINT_FILENAME = ".fluxon_release_fingerprint"
FLUXON_PYO3_BUILD_INFO_PATTERN = re.compile(rb"([0-9a-f]{40})([0-9a-f]{64})")
LOCK_SLEEP_SECONDS = 1


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(description="Install Fluxon runtime from local release artifacts")
    parser.add_argument(
        "--release-dir",
        type=Path,
        required=True,
        help="Local directory containing release artifacts (fluxon_release.sha256, wheels, tar.gz, etc.)",
    )
    parser.add_argument("--src-root", type=Path, required=True, help="Extracted pylib source root directory")
    parser.add_argument(
        "--venv-dir",
        type=Path,
        default=None,
        help=(
            "Optional venv directory used for pip installs. "
            "Recommended for bare/self-host setups to avoid system-managed Python (PEP 668)."
        ),
    )
    parser.add_argument("--sha256-file", required=True, help="Release sha256 manifest filename")
    parser.add_argument("--tar-name", required=True, help="Python source tarball filename")
    parser.add_argument("--wheel", required=True, help="Unified Fluxon wheel filename")
    args = parser.parse_args()

    release_dir = args.release_dir
    src_root = args.src_root

    release_dir.mkdir(parents=True, exist_ok=True)

    fp = _compute_fingerprint_from_manifest(
        release_dir=release_dir,
        sha256_file=args.sha256_file,
        tar_name=args.tar_name,
        wheel=args.wheel,
    )
    if _is_src_ready(src_root=src_root, fingerprint=fp) and _is_pip_ready(
        release_dir=release_dir,
        venv_dir=args.venv_dir,
        fingerprint=fp,
        wheel=args.wheel,
    ):
        print(f"[fluxon_release] fast-path ok: release_dir={release_dir} src_root={src_root}")
        return

    lock_path = release_dir / ".fluxon_release_lock"
    with _install_singleflight_lock(lock_path):
        if _is_src_ready(src_root=src_root, fingerprint=fp) and _is_pip_ready(
            release_dir=release_dir,
            venv_dir=args.venv_dir,
            fingerprint=fp,
            wheel=args.wheel,
        ):
            print(f"[fluxon_release] fast-path ok (under lock): release_dir={release_dir} src_root={src_root}")
            return
        _bootstrap_under_lock(
            release_dir=release_dir,
            src_root=src_root,
            venv_dir=args.venv_dir,
            sha256_file=args.sha256_file,
            tar_name=args.tar_name,
            wheel=args.wheel,
        )


@contextmanager
def _install_singleflight_lock(lock_path: Path):
    if lock_path.exists() and lock_path.is_dir():
        _rm_tree(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as fp:
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"[fluxon_release] waiting for install lock: lock={lock_path}", flush=True)
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)


def _bootstrap_under_lock(
    *,
    release_dir: Path,
    src_root: Path,
    venv_dir: Path | None,
    sha256_file: str,
    tar_name: str,
    wheel: str,
) -> None:
    sha_path = release_dir / sha256_file
    if not sha_path.exists():
        raise RuntimeError(f"Missing sha256 manifest in release-dir: path={sha_path}")
    sha_bytes = sha_path.read_bytes()

    need = [tar_name, wheel]
    expected = _parse_sha256_manifest(sha_bytes, need=need)
    missing = [n for n in need if n not in expected]
    if missing:
        raise RuntimeError(f"Missing checksum entries in sha256 manifest: missing={missing} need={need}")

    tar_path = release_dir / tar_name
    expected_tar_hash = expected[tar_name]
    wheel_path = release_dir / wheel
    expected_wheel_hash = expected[wheel]

    fingerprint = _fingerprint_text(
        tar_name=tar_name,
        tar_hash=expected_tar_hash,
        wheel=wheel,
        wheel_hash=expected_wheel_hash,
    )

    def _verify_tar() -> None:
        if not tar_path.exists():
            raise RuntimeError(f"Missing tar in release-dir: path={tar_path}")
        if _sha256_file(tar_path) != expected_tar_hash:
            raise RuntimeError(f"Checksum mismatch for tar: {tar_name}")

    _verify_tar()
    print(f"[fluxon_release] tar verified: {tar_name} sha256={expected_tar_hash}")

    def _verify_wheel(name: str, path: Path, expected_hash: str) -> None:
        if not path.exists():
            raise RuntimeError(f"Missing wheel in release-dir: name={name} path={path}")
        got = _sha256_file(path)
        if got != expected_hash:
            raise RuntimeError(f"Checksum mismatch for {name}: got={got} expected={expected_hash}")

    _verify_wheel(wheel, wheel_path, expected_wheel_hash)
    print(f"[fluxon_release] wheel verified: {wheel} sha256={expected_wheel_hash}")

    if not _is_src_ready(src_root=src_root, fingerprint=fingerprint):
        if src_root.exists():
            _rm_tree(src_root)
        src_root.mkdir(parents=True, exist_ok=True)
        print(f"[fluxon_release] extracting source tar into: {src_root}")
        with tarfile.open(tar_path, "r:gz") as tf:
            tf.extractall(path=src_root)

        _assert_src_complete(src_root)
        (src_root / FINGERPRINT_FILENAME).write_text(fingerprint, encoding="utf-8")
    else:
        print(f"[fluxon_release] src up to date: src_root={src_root}")

    python_for_pip = sys.executable
    if venv_dir is not None:
        _ensure_venv(venv_dir)
        python_for_pip = str(venv_dir / "bin" / "python")

    pip_fp_path = _pip_fingerprint_path(release_dir=release_dir, venv_dir=venv_dir)
    if _is_pip_ready(
        release_dir=release_dir,
        venv_dir=venv_dir,
        fingerprint=fingerprint,
        wheel=wheel,
    ):
        print(f"[fluxon_release] pip up to date: marker={pip_fp_path}", flush=True)
        return

    pip_install_argv = [python_for_pip, "-m", "pip", "install", "--force-reinstall", "--no-deps"]
    print("[fluxon_release] Installing unified wheel via pip", flush=True)
    subprocess.check_call([*pip_install_argv, str(wheel_path)])
    pip_fp_path.parent.mkdir(parents=True, exist_ok=True)
    pip_fp_path.write_text(fingerprint, encoding="utf-8")


def _ensure_venv(venv_dir: Path) -> None:
    python_bin = venv_dir / "bin" / "python"
    if not python_bin.exists():
        venv_dir.parent.mkdir(parents=True, exist_ok=True)
        subprocess.check_call([sys.executable, "-m", "venv", "--system-site-packages", str(venv_dir)])

    _ensure_pip_in_venv(python_bin)


def _ensure_pip_in_venv(python_bin: Path) -> None:
    check = subprocess.run(
        [str(python_bin), "-m", "pip", "--version"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if check.returncode == 0:
        return

    print(f"[fluxon_release] pip is missing in venv: python={python_bin}", flush=True)
    print("[fluxon_release] Attempting to bootstrap pip via ensurepip (offline)", flush=True)
    boot = subprocess.run([str(python_bin), "-m", "ensurepip", "--upgrade"])
    if boot.returncode != 0:
        raise RuntimeError(
            "pip is missing in venv and ensurepip bootstrap failed. "
            "This usually means the system Python venv/ensurepip components are not installed. "
            "Fix by installing python3-venv (or python3.12-venv), then delete and recreate the venv dir, "
            "or recreate it via: python3 -m venv <venv_dir>."
        )

    check2 = subprocess.run(
        [str(python_bin), "-m", "pip", "--version"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if check2.returncode != 0:
        raise RuntimeError(
            "pip bootstrap via ensurepip completed, but pip is still not importable from the venv. "
            "Delete and recreate the venv dir, or install python3-venv/python3.12-venv on the host."
        )


def _rm_tree(p: Path) -> None:
    if p.is_symlink() or p.is_file():
        try:
            p.unlink()
        except FileNotFoundError:
            return
        return

    last_err: OSError | None = None
    for _ in range(10):
        try:
            children = list(p.iterdir())
        except FileNotFoundError:
            return
        for child in children:
            _rm_tree(child)
        try:
            p.rmdir()
            return
        except FileNotFoundError:
            return
        except OSError as e:
            last_err = e
            if e.errno == 39:
                time.sleep(LOCK_SLEEP_SECONDS)
                continue
            raise
    if last_err is not None:
        raise last_err


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(READ_CHUNK_BYTES)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _fingerprint_text(
    *,
    tar_name: str,
    tar_hash: str,
    wheel: str,
    wheel_hash: str,
) -> str:
    return f"{tar_name} {tar_hash}\n{wheel} {wheel_hash}\n"


def _compute_fingerprint_from_manifest(
    *,
    release_dir: Path,
    sha256_file: str,
    tar_name: str,
    wheel: str,
) -> str:
    sha_path = release_dir / sha256_file
    if not sha_path.exists():
        raise RuntimeError(f"Missing sha256 manifest in release-dir: path={sha_path}")
    sha_bytes = sha_path.read_bytes()

    need = [tar_name, wheel]
    expected = _parse_sha256_manifest(sha_bytes, need=need)
    missing = [n for n in need if n not in expected]
    if missing:
        raise RuntimeError(f"Missing checksum entries in sha256 manifest: missing={missing} need={need}")

    return _fingerprint_text(
        tar_name=tar_name,
        tar_hash=expected[tar_name],
        wheel=wheel,
        wheel_hash=expected[wheel],
    )


def _required_src_files(src_root: Path) -> list[Path]:
    return [
        src_root / "fluxon_py" / "runtime" / "start_master.py",
        src_root / "fluxon_py" / "runtime" / "start_owner_kvclient.py",
        src_root / "fluxon_py" / "runtime" / "start_ops_agent.py",
        src_root / "fluxon_py" / "runtime" / "start_ops_controller.py",
        src_root / "fluxon_py" / "runtime" / "start_fs_master.py",
        src_root / "fluxon_py" / "runtime" / "start_fs_agent.py",
    ]


def _assert_src_complete(src_root: Path) -> None:
    missing = [p for p in _required_src_files(src_root) if not p.exists()]
    if missing:
        missing_s = ", ".join(str(p) for p in missing)
        raise RuntimeError(
            "Source extraction is incomplete: missing required files after extracting pylib_src.tar.gz: " + missing_s
        )


def _is_src_ready(*, src_root: Path, fingerprint: str) -> bool:
    marker = src_root / FINGERPRINT_FILENAME
    if not marker.exists():
        return False
    if marker.read_text(encoding="utf-8") != fingerprint:
        return False
    for p in _required_src_files(src_root):
        if not p.exists():
            return False
    return True


def _pip_fingerprint_path(*, release_dir: Path, venv_dir: Path | None) -> Path:
    if venv_dir is not None:
        return venv_dir / FINGERPRINT_FILENAME
    return release_dir / (FINGERPRINT_FILENAME + ".system")


def _extract_fluxon_pyo3_build_info_from_bytes(*, payload: bytes, ctx: str) -> dict[str, str]:
    for match in FLUXON_PYO3_BUILD_INFO_PATTERN.finditer(payload):
        commit_id = match.group(1).decode("ascii")
        source_sha256 = match.group(2).decode("ascii")
        if re.search(r"[a-f]", commit_id) is None:
            continue
        if re.search(r"[a-f]", source_sha256) is None:
            continue
        return {
            "commit_id": commit_id,
            "source_sha256": source_sha256,
        }
    raise RuntimeError(f"{ctx} missing embedded fluxon_pyo3 build info")


def _read_release_fluxon_pyo3_build_info(*, release_dir: Path, wheel: str) -> dict[str, str]:
    wheel_path = release_dir / wheel
    if not wheel_path.exists():
        raise RuntimeError(f"Missing release wheel in release-dir: path={wheel_path}")
    with zipfile.ZipFile(wheel_path) as zf:
        shared_object_names = [
            name
            for name in zf.namelist()
            if name.startswith("fluxon_pyo3/") and name.endswith(".so") and ".bak_" not in name
        ]
        if len(shared_object_names) != 1:
            raise RuntimeError(
                f"Expected exactly one fluxon_pyo3 shared object in {wheel_path}, got {shared_object_names}"
            )
        payload = zf.read(shared_object_names[0])
    return _extract_fluxon_pyo3_build_info_from_bytes(
        payload=payload,
        ctx=f"release wheel {wheel_path}",
    )


def _read_installed_fluxon_pyo3_build_info(*, venv_dir: Path) -> dict[str, str] | None:
    shared_object_paths = [
        path
        for path in sorted(venv_dir.glob("lib/python*/site-packages/fluxon_pyo3/fluxon_pyo3*.so"))
        if ".bak_" not in path.name
    ]
    if len(shared_object_paths) != 1:
        return None
    return _extract_fluxon_pyo3_build_info_from_bytes(
        payload=shared_object_paths[0].read_bytes(),
        ctx=f"installed shared object {shared_object_paths[0]}",
    )


def _is_pip_ready(*, release_dir: Path, venv_dir: Path | None, fingerprint: str, wheel: str) -> bool:
    marker = _pip_fingerprint_path(release_dir=release_dir, venv_dir=venv_dir)
    if not marker.exists():
        return False
    if marker.read_text(encoding="utf-8") != fingerprint:
        return False
    if venv_dir is not None:
        if not (venv_dir / "bin" / "python").exists():
            return False
        release_build_info = _read_release_fluxon_pyo3_build_info(
            release_dir=release_dir,
            wheel=wheel,
        )
        installed_build_info = _read_installed_fluxon_pyo3_build_info(venv_dir=venv_dir)
        if installed_build_info is None:
            return False
        if installed_build_info != release_build_info:
            return False
    return True


def _bad_files(*, release_dir: Path, expected: dict[str, str]) -> list[str]:
    bad: list[str] = []
    for name, hexsum in expected.items():
        p = release_dir / name
        if not p.exists():
            bad.append(name)
            continue
        if _sha256_file(p) != hexsum:
            bad.append(name)
    return bad


def _parse_sha256_manifest(sha_bytes: bytes, *, need: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw in sha_bytes.decode("utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        hexsum, name = parts[0], parts[1]
        if name in need:
            out[name] = hexsum
    return out


if __name__ == "__main__":
    main()
