from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    from fluxon_test_stack.top_attention_test_index.requirements_all import (
        TEST_REQUIREMENT_DESCRIPTIONS,
        extract_test_requirements,
        iter_index_entry_paths,
    )
except ModuleNotFoundError:
    from top_attention_test_index.requirements_all import (  # type: ignore[no-redef]
        TEST_REQUIREMENT_DESCRIPTIONS,
        extract_test_requirements,
        iter_index_entry_paths,
    )


REPO_ROOT = Path(__file__).resolve().parent.parent
TOP_ATTENTION_INDEX_DIR = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"
TOP_ATTENTION_SCENE_ID_PREFIX = "ci_top_attention_"
QUICK_ENTRY_NAMES: tuple[str, ...] = (
    "_doc_page_build.py",
    "_config_kv.py",
    "_config_fs.py",
    "_py_runtime.py",
    "_test_requirements.py",
    "_test_stack_contract.py",
    "_deployment_codegen.py",
    "_script_tools.py",
    "_cargo_fs_core.py",
)


def display_top_attention_relpath(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def top_attention_scene_id(path: Path) -> str:
    return TOP_ATTENTION_SCENE_ID_PREFIX + path.stem.lstrip("_")


def match_top_attention_prefix(path: Path, raw_prefix: str) -> bool:
    prefix = raw_prefix.strip()
    if not prefix:
        return False
    if prefix.endswith(".py"):
        prefix = prefix[:-3]
    prefix_token = prefix.lstrip("_")
    candidates = {prefix}
    if prefix and not prefix.startswith("_"):
        candidates.add("_" + prefix)
    if any(path.stem.startswith(candidate) for candidate in candidates):
        return True
    if not prefix_token:
        return False
    stem_tokens = [token for token in path.stem.split("_") if token]
    return any(token.startswith(prefix_token) for token in stem_tokens)


def select_top_attention_entries(prefixes: Sequence[str]) -> list[Path]:
    selected: list[Path] = []
    seen: set[Path] = set()
    for path in iter_index_entry_paths():
        if not any(match_top_attention_prefix(path, prefix) for prefix in prefixes):
            continue
        if path in seen:
            continue
        seen.add(path)
        selected.append(path)
    if not selected:
        raise SystemExit(f"no top-attention test index entries matched prefixes: {list(prefixes)}")
    return selected


def collect_top_attention_requirements(paths: Iterable[Path]) -> list[str]:
    requirements: set[str] = set()
    for path in paths:
        requirements.update(extract_test_requirements(path))
    return sorted(requirements)


def collect_top_attention_payload(prefixes: Sequence[str] | None = None) -> dict[str, Any]:
    paths = list(iter_index_entry_paths()) if prefixes is None else select_top_attention_entries(prefixes)
    entries = []
    for path in paths:
        entries.append(
            {
                "id": path.stem,
                "name": path.name,
                "path": display_top_attention_relpath(path),
                "requirements": sorted(extract_test_requirements(path)),
            }
        )
    return {
        "index_dir": display_top_attention_relpath(TOP_ATTENTION_INDEX_DIR),
        "entry_count": len(entries),
        "entries": entries,
        "requirements": collect_top_attention_requirements(paths),
    }


def iter_quick_entry_paths() -> list[Path]:
    by_name = {path.name: path for path in iter_index_entry_paths()}
    selected: list[Path] = []
    for entry_name in QUICK_ENTRY_NAMES:
        path = by_name.get(entry_name)
        if path is None:
            raise AssertionError(f"missing quick top-attention entry: {entry_name}")
        selected.append(path)
    return selected


def run_top_attention_entries(paths: Sequence[Path], *, python_executable: str = sys.executable) -> int:
    for path in paths:
        cmd = [python_executable, str(path)]
        print("+ " + " ".join(cmd), flush=True)
        rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
        if rc != 0:
            return rc
    return 0


def print_top_attention_payload(payload: dict[str, Any], *, requirements_only: bool) -> None:
    if requirements_only:
        for requirement in payload["requirements"]:
            print(requirement, flush=True)
        return
    print(f"index_dir: {payload['index_dir']}", flush=True)
    print("entries:", flush=True)
    for entry in payload["entries"]:
        req_text = ", ".join(entry["requirements"]) if entry["requirements"] else "(none)"
        print(f"- {entry['name']} [{req_text}]", flush=True)
    print("requirements:", flush=True)
    for requirement in payload["requirements"]:
        description = TEST_REQUIREMENT_DESCRIPTIONS.get(requirement, "")
        suffix = f": {description}" if description else ""
        print(f"- {requirement}{suffix}", flush=True)


__all__ = [
    "QUICK_ENTRY_NAMES",
    "TOP_ATTENTION_SCENE_ID_PREFIX",
    "collect_top_attention_payload",
    "collect_top_attention_requirements",
    "display_top_attention_relpath",
    "iter_quick_entry_paths",
    "match_top_attention_prefix",
    "print_top_attention_payload",
    "run_top_attention_entries",
    "select_top_attention_entries",
    "top_attention_scene_id",
]
