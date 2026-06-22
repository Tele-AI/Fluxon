from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "scripts" / "git_source_selection.py"
PROFILE_MODULE_PATH = REPO_ROOT / "scripts" / "source_selection_profiles.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "scripts_git_source_selection_test",
        MODULE_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_MOD = _load_module()


def _load_profile_module():
    scripts_root_str = str(REPO_ROOT / "scripts")
    if scripts_root_str in sys.path:
        sys.path.remove(scripts_root_str)
    sys.path.insert(0, scripts_root_str)
    spec = importlib.util.spec_from_file_location(
        "scripts_source_selection_profiles_test",
        PROFILE_MODULE_PATH,
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PROFILE_MOD = _load_profile_module()


class GitSourceSelectionUtilsTest(unittest.TestCase):
    def test_collect_source_relpaths_with_rather_no_git_submodule_merges_module_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "README.md").write_text("repo\n", encoding="utf-8")
            module_root = repo_root / "fluxon_rs" / "moka"
            (module_root / "src").mkdir(parents=True, exist_ok=True)
            (module_root / "Cargo.toml").write_text("module\n", encoding="utf-8")
            (module_root / "src" / "lib.rs").write_text("pub fn x() {}\n", encoding="utf-8")
            cfg_path = repo_root / "setup_and_pack" / "rather_no_git_submodule.yaml"
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(
                "modules:\n"
                "  - path: fluxon_rs/moka\n"
                "    repo: https://example.com/moka.git\n"
                "    checkout: main\n",
                encoding="utf-8",
            )

            def fake_check_output(argv, cwd=None):
                del argv
                cwd_path = Path(cwd).resolve()
                if cwd_path == repo_root.resolve():
                    return b"README.md\0"
                if cwd_path == module_root.resolve():
                    return b"Cargo.toml\0src/lib.rs\0"
                raise AssertionError(f"unexpected git ls-files cwd: {cwd_path}")

            with mock.patch.object(_MOD.subprocess, "check_output", side_effect=fake_check_output):
                relpaths = _MOD.collect_source_relpaths_with_rather_no_git_submodule(
                    repo_root=repo_root,
                    source_roots=("README.md",),
                    is_excluded=lambda _relpath: False,
                    empty_selection_error="no files",
                    rather_no_git_submodule_context_name="test source selection",
                )

            self.assertEqual(
                relpaths,
                [
                    "README.md",
                    "fluxon_rs/moka/Cargo.toml",
                    "fluxon_rs/moka/src/lib.rs",
                ],
            )

    def test_load_rather_no_git_submodule_source_roots_uses_context_name_in_missing_dir_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            cfg_path = repo_root / "setup_and_pack" / "rather_no_git_submodule.yaml"
            cfg_path.parent.mkdir(parents=True, exist_ok=True)
            cfg_path.write_text(
                "modules:\n"
                "  - path: fluxon_rs/moka\n"
                "    repo: https://example.com/moka.git\n"
                "    checkout: main\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                RuntimeError,
                "test source selection requires configured rather_no_git_submodule path to exist",
            ):
                _MOD.load_rather_no_git_submodule_source_roots(
                    repo_root=repo_root,
                    context_name="test source selection",
                )

    def test_source_profiles_only_add_inclusions_beyond_gitignore(self) -> None:
        self.assertTrue(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_SOURCE_PACK,
                relpath=".dever/run.log",
            )
        )
        self.assertTrue(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_SOURCE_PACK,
                relpath="fluxon_release/install.py",
            )
        )
        self.assertTrue(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_SOURCE_PACK,
                relpath="skills/demo/SKILL.md",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="fluxon_release/closed_sdk/manifest.json",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="fluxon_doc_cn/roadmap.md",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="deployment/utils/log_shard.py",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="scripts/source_selection_profiles.py",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="fluxon_rs/moka/examples/append_value_async.rs",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="fluxon_rs/moka/tests/entry_api_sync.rs",
            )
        )
        self.assertFalse(
            _PROFILE_MOD.source_profile_relpath_excluded(
                profile=_PROFILE_MOD.SOURCE_SELECTION_PROFILE_BUILD_SEED,
                relpath="fluxon_rs/fluxon_cli/templates/landing.html",
            )
        )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
