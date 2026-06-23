from __future__ import annotations

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
INDEX_DIR = Path(__file__).resolve().parent
SUITE_PATH = REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml"
TOP_ATTENTION_SCENE_ID_PREFIX = "ci_top_attention_"
IGNORED_INDEX_ENTRY_NAMES = frozenset({"_common.py"})


def iter_index_entry_paths() -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(INDEX_DIR.glob("*.py"))
        if path.name.startswith("_") and path.name not in IGNORED_INDEX_ENTRY_NAMES
    )


def top_attention_scene_id(path: Path) -> str:
    return TOP_ATTENTION_SCENE_ID_PREFIX + path.stem.lstrip("_")


def _load_suite_scenes() -> dict[str, object]:
    raw = yaml.safe_load(SUITE_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise AssertionError(f"suite must be a mapping: {SUITE_PATH}")
    scenes = raw.get("scenes")
    if not isinstance(scenes, dict):
        raise AssertionError(f"suite scenes must be a mapping: {SUITE_PATH}")
    return scenes


def _ci_scene_ids() -> set[str]:
    return {
        scene_id
        for scene_id, raw_scene in _load_suite_scenes().items()
        if isinstance(scene_id, str)
        and isinstance(raw_scene, dict)
        and isinstance(raw_scene.get("ci"), dict)
    }


def _scene_requirements(scene_id: str) -> list[str]:
    scenes = _load_suite_scenes()
    scene = scenes.get(scene_id)
    if not isinstance(scene, dict):
        raise AssertionError(f"missing top-attention scene in suite: {scene_id}")
    ci = scene.get("ci")
    if not isinstance(ci, dict):
        raise AssertionError(f"top-attention scene must be CI-backed: {scene_id}")
    requirements = ci.get("requirements")
    if not isinstance(requirements, list):
        raise AssertionError(f"scene[{scene_id}].ci.requirements must be a list")
    out: list[str] = []
    for index, raw_item in enumerate(requirements):
        if not isinstance(raw_item, str) or not raw_item.strip():
            raise AssertionError(f"scene[{scene_id}].ci.requirements[{index}] must be a non-empty string")
        out.append(raw_item.strip())
    return out


class TestTopAttentionYamlRequirements(unittest.TestCase):
    def test_every_index_entry_has_suite_scene(self) -> None:
        for path in iter_index_entry_paths():
            scene_id = top_attention_scene_id(path)
            if scene_id not in _ci_scene_ids():
                continue
            with self.subTest(path=path.name, scene_id=scene_id):
                _ = _scene_requirements(scene_id)

    def test_scene_requirements_are_sorted_and_unique(self) -> None:
        for path in iter_index_entry_paths():
            scene_id = top_attention_scene_id(path)
            if scene_id not in _ci_scene_ids():
                continue
            requirements = _scene_requirements(scene_id)
            with self.subTest(path=path.name, scene_id=scene_id):
                self.assertEqual(requirements, sorted(set(requirements)))

    def test_removed_direct_entrypoints_stay_removed(self) -> None:
        self.assertFalse((INDEX_DIR / "_all_quick.py").exists())
        self.assertFalse((INDEX_DIR / "run_match_prefix.py").exists())


if __name__ == "__main__":
    raise SystemExit(unittest.main())
