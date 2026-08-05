#!/usr/bin/env python3
"""Write and validate the GitHub ref identity for a release CI run."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


SCHEMA_VERSION = 1
WORKFLOW_PATH = ".github/workflows/all_test.yml"
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_FIELDS = {
    "ref",
    "ref_name",
    "ref_type",
    "repository",
    "schema_version",
    "sha",
    "workflow_path",
}


def build_provenance(
    *,
    repository: str,
    ref: str,
    ref_name: str,
    ref_type: str,
    sha: str,
    workflow_path: str = WORKFLOW_PATH,
) -> dict[str, object]:
    if not _REPOSITORY_RE.fullmatch(repository):
        raise ValueError(f"invalid GitHub repository: {repository!r}")
    if ref_type not in {"branch", "tag"}:
        raise ValueError(f"invalid GitHub ref type: {ref_type!r}")
    expected_ref = f"refs/{'tags' if ref_type == 'tag' else 'heads'}/{ref_name}"
    if ref != expected_ref:
        raise ValueError(f"ref identity mismatch: ref={ref!r}, expected={expected_ref!r}")
    if not _SHA_RE.fullmatch(sha):
        raise ValueError(f"invalid GitHub commit SHA: {sha!r}")
    if workflow_path != WORKFLOW_PATH:
        raise ValueError(f"invalid release CI workflow path: {workflow_path!r}")
    return {
        "schema_version": SCHEMA_VERSION,
        "repository": repository,
        "workflow_path": workflow_path,
        "ref": ref,
        "ref_name": ref_name,
        "ref_type": ref_type,
        "sha": sha,
    }


def write_provenance(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_provenance(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("release CI provenance must be a JSON object")
    if set(payload) != _FIELDS:
        raise ValueError(
            f"release CI provenance fields mismatch: got={sorted(payload)}, expected={sorted(_FIELDS)}"
        )
    return payload


def validate_tag_provenance(
    path: Path,
    *,
    expected_repository: str,
    expected_sha: str,
    expected_tag: str,
) -> dict[str, object]:
    payload = load_provenance(path)
    expected = build_provenance(
        repository=expected_repository,
        ref=f"refs/tags/{expected_tag}",
        ref_name=expected_tag,
        ref_type="tag",
        sha=expected_sha,
    )
    if payload != expected:
        raise ValueError(f"release CI provenance mismatch: got={payload!r}, expected={expected!r}")
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    write = subparsers.add_parser("write", help="Write provenance for the current push run")
    write.add_argument("--output", type=Path, required=True)
    write.add_argument("--repository", required=True)
    write.add_argument("--ref", required=True)
    write.add_argument("--ref-name", required=True)
    write.add_argument("--ref-type", choices=("branch", "tag"), required=True)
    write.add_argument("--sha", required=True)

    validate = subparsers.add_parser("validate-tag", help="Require provenance for an exact tag and commit")
    validate.add_argument("--path", type=Path, required=True)
    validate.add_argument("--expected-repository", required=True)
    validate.add_argument("--expected-sha", required=True)
    validate.add_argument("--expected-tag", required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.command == "write":
        payload = build_provenance(
            repository=args.repository,
            ref=args.ref,
            ref_name=args.ref_name,
            ref_type=args.ref_type,
            sha=args.sha,
        )
        write_provenance(args.output, payload)
        print(f"Wrote release CI provenance: {args.output}")
        return 0

    payload = validate_tag_provenance(
        args.path,
        expected_repository=args.expected_repository,
        expected_sha=args.expected_sha,
        expected_tag=args.expected_tag,
    )
    print(
        "Validated release CI provenance: "
        f"ref={payload['ref']} sha={payload['sha']} workflow={payload['workflow_path']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
