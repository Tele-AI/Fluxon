from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
TOP_ATTENTION_INDEX_DIR = REPO_ROOT / "fluxon_test_stack" / "top_attention_test_index"
TOP_ATTENTION_SUITE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml"
TOP_ATTENTION_SCENE_ID_PREFIX = "ci_top_attention_"
IGNORED_INDEX_ENTRY_NAMES: frozenset[str] = frozenset({"_common.py"})
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
TEST_REQUIREMENT_DESCRIPTIONS: dict[str, str] = {
    "cargo": "Rust cargo toolchain is required.",
    "docker": "A working Docker daemon is required.",
    "fluxon-pyo3": "The compiled fluxon_pyo3 Python extension must be available.",
    "fluxon-release": "The local fluxon_release runtime/artifact tree must be populated.",
    "kv-cluster": "A configured KV backend runtime from the repo test config must be reachable.",
    "ops": "A reachable Fluxon Ops control plane is required by the test-stack execution flow.",
    "python-wheel-build": "Python wheel build dependencies must be available.",
    "submodules": "Required git submodules must be initialized for build-using tests.",
    "test-stack-targets": "A TEST_STACK config with reachable target hosts is required.",
    "tikv": "A TiKV/PD runtime is required, either external or started by the test.",
    "testbed_etcd": "The shared testbed etcd service must already be running.",
    "testbed_greptime": "The shared testbed Greptime service must already be running.",
    "master": "The runner must start a Fluxon master instance for the case.",
    "owner_0": "The runner must start owner_0 for the case.",
    "ci_runner": "The runner must start the ci_runner workload for the case.",
    "owner_shared_bundle": "The runner must wait for owner shared bundle files before executing the case.",
    "fluxon_kv_readiness_probe": "The runner must pass the configured Fluxon KV readiness probe before executing the case.",
}


def iter_index_entry_paths() -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(TOP_ATTENTION_INDEX_DIR.glob("*.py"))
        if path.name.startswith("_") and path.name not in IGNORED_INDEX_ENTRY_NAMES
    )


def display_top_attention_relpath(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def top_attention_scene_id(path: Path) -> str:
    return TOP_ATTENTION_SCENE_ID_PREFIX + path.stem.lstrip("_")


def _load_top_attention_suite_scenes() -> dict[str, Any]:
    raw = yaml.safe_load(TOP_ATTENTION_SUITE_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"top-attention suite must be a YAML mapping: {TOP_ATTENTION_SUITE_PATH}")
    scenes = raw.get("scenes")
    if not isinstance(scenes, dict):
        raise ValueError(f"top-attention suite scenes must be a mapping: {TOP_ATTENTION_SUITE_PATH}")
    return scenes


def _scene_requirements_for_path(path: Path) -> list[str]:
    scene_id = top_attention_scene_id(path)
    scenes = _load_top_attention_suite_scenes()
    scene = scenes.get(scene_id)
    if not isinstance(scene, dict):
        return []
    ci = scene.get("ci")
    if not isinstance(ci, dict):
        return []
    requirements = ci.get("requirements")
    if not isinstance(requirements, list):
        return []
    out: list[str] = []
    for index, raw_requirement in enumerate(requirements):
        if not isinstance(raw_requirement, str) or not raw_requirement.strip():
            raise ValueError(f"scene[{scene_id}].ci.requirements[{index}] must be a non-empty string")
        out.append(raw_requirement.strip())
    return sorted(set(out))


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
        requirements.update(_scene_requirements_for_path(path))
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
                "requirements": _scene_requirements_for_path(path),
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
    "IGNORED_INDEX_ENTRY_NAMES",
    "QUICK_ENTRY_NAMES",
    "TOP_ATTENTION_SCENE_ID_PREFIX",
    "collect_top_attention_payload",
    "collect_top_attention_requirements",
    "display_top_attention_relpath",
    "iter_index_entry_paths",
    "iter_quick_entry_paths",
    "match_top_attention_prefix",
    "print_top_attention_payload",
    "run_top_attention_entries",
    "select_top_attention_entries",
    "top_attention_scene_id",
]
