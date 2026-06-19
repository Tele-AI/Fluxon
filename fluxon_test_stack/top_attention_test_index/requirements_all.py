from __future__ import annotations

import ast
from pathlib import Path


TEST_REQUIREMENTS: list[str] = ["ops"]
TEST_DIR = Path(__file__).resolve().parent
IGNORED_INDEX_ENTRY_NAMES = frozenset({"_common.py"})


ALL_TEST_REQUIREMENTS: tuple[str, ...] = (
    "cargo",
    "docker",
    "etcd",
    "fluxon-pyo3",
    "fluxon-release",
    "greptime",
    "kv-cluster",
    "ops",
    "python-wheel-build",
    "submodules",
    "test-stack-targets",
    "tikv",
)

ALL_TEST_REQUIREMENTS_SET = frozenset(ALL_TEST_REQUIREMENTS)

TEST_REQUIREMENT_DESCRIPTIONS: dict[str, str] = {
    "cargo": "Rust cargo toolchain is required.",
    "docker": "A working Docker daemon is required.",
    "etcd": "An etcd runtime is required, either external or started by the test.",
    "fluxon-pyo3": "The compiled fluxon_pyo3 Python extension must be available.",
    "fluxon-release": "The local fluxon_release runtime/artifact tree must be populated.",
    "greptime": "A GreptimeDB runtime is required, either external or started by the test.",
    "kv-cluster": "A configured KV backend runtime from the repo test config must be reachable.",
    "ops": "A reachable Fluxon Ops control plane is required by the test-stack execution flow.",
    "python-wheel-build": "Python wheel build dependencies must be available.",
    "submodules": "Required git submodules must be initialized for build-using tests.",
    "test-stack-targets": "A TEST_STACK config with reachable target hosts is required.",
    "tikv": "A TiKV/PD runtime is required, either external or started by the test.",
}


def iter_test_python_paths() -> tuple[Path, ...]:
    return tuple(sorted(TEST_DIR.glob("*.py")))


def iter_index_entry_paths() -> tuple[Path, ...]:
    return tuple(
        path
        for path in iter_test_python_paths()
        if path.name.startswith("_") and path.name not in IGNORED_INDEX_ENTRY_NAMES
    )


def extract_test_requirements(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found = None
    for node in tree.body:
        target_name = None
        value_node = None
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "TEST_REQUIREMENTS":
                    target_name = target.id
                    value_node = node.value
                    break
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "TEST_REQUIREMENTS":
                target_name = node.target.id
                value_node = node.value

        if target_name != "TEST_REQUIREMENTS":
            continue
        if found is not None:
            raise AssertionError(f"{path.name} defines TEST_REQUIREMENTS more than once")
        if value_node is None:
            raise AssertionError(f"{path.name} TEST_REQUIREMENTS must assign a list literal")
        try:
            value = ast.literal_eval(value_node)
        except (ValueError, SyntaxError) as exc:
            raise AssertionError(
                f"{path.name} TEST_REQUIREMENTS must be a static list literal"
            ) from exc
        if not isinstance(value, list):
            raise AssertionError(f"{path.name} TEST_REQUIREMENTS must be a list literal")
        if not all(isinstance(item, str) for item in value):
            raise AssertionError(f"{path.name} TEST_REQUIREMENTS must contain only strings")
        found = value
    if found is None:
        raise AssertionError(f"{path.name} is missing TEST_REQUIREMENTS")
    return found
