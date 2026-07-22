#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fluxon_fs_s3_test_support import FluxonFsS3Harness

RCLONE_IMAGE_REF = "rclone/rclone:1.60.1"
RCLONE_VERSION_LINE = "rclone v1.60.1"
RCLONE_REMOTE_NAME = "fluxon"
RCLONE_CONTAINER_WORKDIR = "/work"
RCLONE_CONFIG_CONTAINER_PATH = f"{RCLONE_CONTAINER_WORKDIR}/rclone.conf"
RCLONE_COMMAND_TIMEOUT_SECS = 120
RCLONE_COMPLEX_COPY_TIMEOUT_SECS = 600
RCLONE_LIST_READY_TIMEOUT_SECS = 180.0
COMPLEX_GROUP_COUNT = 8
COMPLEX_FILES_PER_GROUP = 50
COMPLEX_EXPECTED_FILE_COUNT = 405


FileSignature = tuple[int, str]


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _file_signatures(root: Path) -> dict[str, FileSignature]:
    return {
        path.relative_to(root).as_posix(): (path.stat().st_size, _sha256_file(path))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _write_files(root: Path, files: dict[str, bytes]) -> dict[str, FileSignature]:
    for relpath, content in files.items():
        if " " in relpath:
            raise ValueError(f"rclone CI fixture paths must not contain spaces: {relpath!r}")
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return _file_signatures(root)


def _print_completed_output(completed: subprocess.CompletedProcess[str]) -> None:
    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n", flush=True)
    if completed.stderr:
        print(
            completed.stderr,
            end="" if completed.stderr.endswith("\n") else "\n",
            file=sys.stderr,
            flush=True,
        )


def _run_process(cmd: list[str], *, timeout_secs: int = RCLONE_COMMAND_TIMEOUT_SECS) -> str:
    print("+ " + shlex.join(cmd), flush=True)
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_secs,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"command timed out after {timeout_secs}s: {shlex.join(cmd)}"
        ) from exc
    _print_completed_output(completed)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}: {shlex.join(cmd)}"
        )
    return completed.stdout


def _docker_rclone_command(
    *,
    image_ref: str,
    work_root: Path,
    args: Iterable[str],
    use_config: bool,
) -> list[str]:
    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        "host",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--volume",
        f"{work_root}:{RCLONE_CONTAINER_WORKDIR}",
        image_ref,
    ]
    if use_config:
        cmd.extend(["--config", RCLONE_CONFIG_CONTAINER_PATH])
    cmd.extend(args)
    return cmd


def _run_rclone(
    *,
    image_ref: str,
    work_root: Path,
    args: Iterable[str],
    use_config: bool = True,
    timeout_secs: int = RCLONE_COMMAND_TIMEOUT_SECS,
) -> str:
    return _run_process(
        _docker_rclone_command(
            image_ref=image_ref,
            work_root=work_root,
            args=args,
            use_config=use_config,
        ),
        timeout_secs=timeout_secs,
    )


def _assert_rclone_version(*, image_ref: str, work_root: Path) -> None:
    output = _run_rclone(
        image_ref=image_ref,
        work_root=work_root,
        args=["version"],
        use_config=False,
    )
    first_line = next((line.strip() for line in output.splitlines() if line.strip()), "")
    if first_line != RCLONE_VERSION_LINE:
        raise AssertionError(
            f"unexpected rclone version: expected {RCLONE_VERSION_LINE!r}, got {first_line!r}"
        )


def _write_rclone_config(path: Path, harness: FluxonFsS3Harness) -> None:
    path.write_text(
        "\n".join(
            [
                f"[{RCLONE_REMOTE_NAME}]",
                "type = s3",
                "provider = Other",
                "env_auth = false",
                f"access_key_id = {harness.s3_access_key}",
                f"secret_access_key = {harness.s3_secret_key}",
                "region = us-east-1",
                f"endpoint = {harness.s3_endpoint}",
                "force_path_style = true",
                "disable_checksum = true",
                "use_multipart_etag = false",
                "",
            ]
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)


def _recursive_listed_files(output: str) -> list[str]:
    return sorted(
        line.strip()
        for line in output.splitlines()
        if line.strip() and not line.strip().endswith("/")
    )


def _wait_for_recursive_listing(
    *,
    image_ref: str,
    work_root: Path,
    remote: str,
    expected_files: list[str],
) -> None:
    deadline = time.monotonic() + RCLONE_LIST_READY_TIMEOUT_SECS
    last_detail = "rclone lsf -R did not run"
    while True:
        try:
            output = _run_rclone(
                image_ref=image_ref,
                work_root=work_root,
                args=["lsf", "-R", remote],
            )
            actual_files = _recursive_listed_files(output)
            if actual_files == expected_files:
                return
            last_detail = f"expected files={expected_files!r}, got={actual_files!r}"
        except RuntimeError as exc:
            last_detail = str(exc)
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"timed out waiting for recursive rclone listing: {last_detail}"
            )
        time.sleep(1.0)


def _assert_signatures(
    *,
    label: str,
    expected: dict[str, FileSignature],
    actual_root: Path,
) -> None:
    actual = _file_signatures(actual_root)
    if actual != expected:
        raise AssertionError(f"{label} signatures differ: expected={expected!r}, got={actual!r}")


def _build_complex_fixture_files() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for group_index in range(COMPLEX_GROUP_COUNT):
        for file_index in range(COMPLEX_FILES_PER_GROUP):
            relpath = f"fanout/group_{group_index:02d}/file_{file_index:03d}.bin"
            files[relpath] = (
                f"group={group_index:02d} file={file_index:03d}\n".encode("utf-8")
            )
    files.update(
        {
            "deep/l1/l2/l3/l4/l5/l6/l7/l8/final.bin": b"deep payload\n",
            "configs/dev/app.yaml": b"environment: dev\n",
            "configs/prod/app.yaml": b"environment: prod\n",
            "blobs/small.bin": bytes(range(64)),
            "blobs/medium-8m.bin": bytes(range(256)) * (8 * 1024 * 1024 // 256),
        }
    )
    assert len(files) == COMPLEX_EXPECTED_FILE_COUNT
    return files


def run_e2e(*, image_ref: str) -> None:
    work_root = Path(tempfile.mkdtemp(prefix="fluxon_fs_rclone_e2e_"))
    harness: FluxonFsS3Harness | None = None
    try:
        _assert_rclone_version(image_ref=image_ref, work_root=work_root)

        export_root = work_root / "export"
        export_root.mkdir()
        source_signatures = _write_files(
            export_root,
            {
                "root.txt": b"root object\n",
                "nested/child.txt": b"nested child object\n",
                "nested/deeper/grandchild.txt": b"deep object\n",
            },
        )
        harness = FluxonFsS3Harness(
            tag="fluxon_fs_rclone_e2e",
            work_root=work_root / "stack",
            export_root=export_root,
        )
        _write_rclone_config(work_root / "rclone.conf", harness)

        remote_root = f"{RCLONE_REMOTE_NAME}:{harness.source_export_name}"
        _wait_for_recursive_listing(
            image_ref=image_ref,
            work_root=work_root,
            remote=remote_root,
            expected_files=sorted(source_signatures),
        )

        download_root = work_root / "download"
        _run_rclone(
            image_ref=image_ref,
            work_root=work_root,
            args=["copy", remote_root, f"{RCLONE_CONTAINER_WORKDIR}/download"],
        )
        _assert_signatures(
            label="bucket-to-local copy",
            expected=source_signatures,
            actual_root=download_root,
        )

        upload_root = work_root / "upload"
        upload_signatures = _write_files(
            upload_root,
            {
                "from-client.txt": b"uploaded through rclone\n",
                "branch/payload.bin": bytes(range(256)) * 16,
            },
        )
        _run_rclone(
            image_ref=image_ref,
            work_root=work_root,
            args=[
                "copy",
                f"{RCLONE_CONTAINER_WORKDIR}/upload",
                f"{remote_root}/uploaded",
            ],
        )
        _assert_signatures(
            label="local-to-bucket copy",
            expected=upload_signatures,
            actual_root=export_root / "uploaded",
        )

        deleted_relpath = "from-client.txt"
        _run_rclone(
            image_ref=image_ref,
            work_root=work_root,
            args=["deletefile", f"{remote_root}/uploaded/{deleted_relpath}"],
        )
        remaining_upload_signatures = dict(upload_signatures)
        del remaining_upload_signatures[deleted_relpath]
        _assert_signatures(
            label="remote deletefile",
            expected=remaining_upload_signatures,
            actual_root=export_root / "uploaded",
        )

        complex_source_root = work_root / "complex-source"
        complex_signatures = _write_files(
            complex_source_root,
            _build_complex_fixture_files(),
        )
        assert len(complex_signatures) == COMPLEX_EXPECTED_FILE_COUNT
        complex_remote_root = f"{remote_root}/complex-copy"
        _run_rclone(
            image_ref=image_ref,
            work_root=work_root,
            args=[
                "copy",
                f"{RCLONE_CONTAINER_WORKDIR}/complex-source",
                complex_remote_root,
            ],
            timeout_secs=RCLONE_COMPLEX_COPY_TIMEOUT_SECS,
        )
        _assert_signatures(
            label="complex local-to-bucket copy",
            expected=complex_signatures,
            actual_root=export_root / "complex-copy",
        )
        _wait_for_recursive_listing(
            image_ref=image_ref,
            work_root=work_root,
            remote=complex_remote_root,
            expected_files=sorted(complex_signatures),
        )

        complex_download_root = work_root / "complex-download"
        _run_rclone(
            image_ref=image_ref,
            work_root=work_root,
            args=[
                "copy",
                complex_remote_root,
                f"{RCLONE_CONTAINER_WORKDIR}/complex-download",
            ],
            timeout_secs=RCLONE_COMPLEX_COPY_TIMEOUT_SECS,
        )
        _assert_signatures(
            label="complex bucket-to-local copy",
            expected=complex_signatures,
            actual_root=complex_download_root,
        )
        print("FluxonFS rclone v1.60.1 E2E passed", flush=True)
    finally:
        try:
            if harness is not None:
                harness.close()
        finally:
            shutil.rmtree(work_root, ignore_errors=False)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the pinned rclone v1.60.1 client against a self-managed FluxonFS S3 stack."
    )
    parser.add_argument("--rclone-image-ref", required=True)
    args = parser.parse_args()
    if args.rclone_image_ref != RCLONE_IMAGE_REF:
        parser.error(
            f"--rclone-image-ref must be exactly {RCLONE_IMAGE_REF!r}, got {args.rclone_image_ref!r}"
        )
    run_e2e(image_ref=args.rclone_image_ref)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
