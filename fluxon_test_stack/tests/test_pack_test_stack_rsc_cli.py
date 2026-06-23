#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = REPO_ROOT / "fluxon_test_stack" / "pack_test_stack_rsc.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fluxon_test_stack_pack_test_stack_rsc", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PACK = _load_module()


class TestPackTestStackRscCli(unittest.TestCase):
    def test_resolve_transport_backends_from_ci_suite(self) -> None:
        backends = _PACK._resolve_transport_backends(
            config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
            explicit_profile_ids=[],
        )
        self.assertEqual(backends, ["fastws", "tquic", "sockudo_ws", "tcp"])

    def test_resolve_transport_backends_from_nontransport_profile(self) -> None:
        backends = _PACK._resolve_transport_backends(
            config_path=(REPO_ROOT / "fluxon_test_stack" / "benchmark_full_matrix.yaml").resolve(),
            explicit_profile_ids=["redis_sharded", "alluxio_posix"],
        )
        self.assertEqual(backends, ["fastws"])

    def test_build_plan_reuses_existing_releases(self) -> None:
        release_dir = (REPO_ROOT / ".tmp_test_pack_test_stack_rsc_release").resolve()
        plan = _PACK._build_all_profiles_plan(
            release_dir=release_dir,
            config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
            top_level_transport_backend="tcp_thread",
            rdma_backend="closed_sdk",
            with_tikv_runtime=True,
            transport_backends=["tcp_thread"],
            reuse_existing_release=True,
            skip_top_level_release=False,
            repo_test_rsc_root=None,
            prepare_config=None,
            baseline_source_root=None,
            redis_bundle_src=None,
            alluxio_bundle_src=None,
            build_redis_bundle_docker=False,
            redis_version=None,
            redis_source_url=None,
            redis_source_sha256=None,
            redis_docker_image=None,
        )
        self.assertEqual(plan[0]["action"], "validate_release")
        self.assertEqual(plan[0]["scope"], "top_level_release")
        self.assertEqual(plan[0]["transport_backend"], "tcp_thread")
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[1]["action"], "prepare_test_rsc")
        self.assertEqual(plan[1]["profile_id"], "fluxon_tcp_thread")
        self.assertIn("--out-dir", plan[1]["command"])
        self.assertIn(str((release_dir / "test_rsc" / "fluxon_tcp_thread").resolve()), plan[1]["command"])
        self.assertNotIn("--transport-backend", plan[0]["command"] if "command" in plan[0] else [])

    def test_build_plan_deduplicates_public_profile_release(self) -> None:
        release_dir = (REPO_ROOT / ".tmp_test_pack_test_stack_rsc_release").resolve()
        plan = _PACK._build_all_profiles_plan(
            release_dir=release_dir,
            config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
            top_level_transport_backend="tcp_thread",
            rdma_backend="closed_sdk",
            with_tikv_runtime=True,
            transport_backends=["tcp_thread"],
            reuse_existing_release=False,
            skip_top_level_release=False,
            repo_test_rsc_root=None,
            prepare_config=None,
            baseline_source_root=None,
            redis_bundle_src=None,
            alluxio_bundle_src=None,
            build_redis_bundle_docker=False,
            redis_version=None,
            redis_source_url=None,
            redis_source_sha256=None,
            redis_docker_image=None,
        )
        self.assertEqual(plan[0]["action"], "pack_release")
        self.assertEqual(plan[0]["release_dir"], str(release_dir))
        self.assertEqual(plan[0]["transport_backend"], "tcp_thread")
        self.assertNotIn("--transport-backend", plan[0]["command"])
        self.assertEqual(len(plan), 2)
        self.assertEqual(plan[1]["action"], "prepare_test_rsc")
        self.assertEqual(plan[1]["profile_id"], "fluxon_tcp_thread")
        self.assertEqual(plan[1]["out_dir"], str((release_dir / "test_rsc" / "fluxon_tcp_thread").resolve()))

    def test_profile_release_authority_dir_uses_profiles_subdir_for_nonpublic_profile(self) -> None:
        release_dir = (REPO_ROOT / ".tmp_test_pack_test_stack_rsc_release").resolve()
        authority_dir = _PACK._profile_release_authority_dir(
            release_dir=release_dir,
            profile_id="fluxon_fastws",
        )
        self.assertEqual(authority_dir, (release_dir / "profiles" / "fluxon_fastws").resolve())

    def test_build_plan_rejects_nonpublic_transport_release(self) -> None:
        release_dir = (REPO_ROOT / ".tmp_test_pack_test_stack_rsc_release").resolve()

        with self.assertRaisesRegex(ValueError, "only supports the fixed closed_sdk transport backend"):
            _PACK._build_all_profiles_plan(
                release_dir=release_dir,
                config_path=(REPO_ROOT / "fluxon_test_stack" / "ci_test_list.yaml").resolve(),
                top_level_transport_backend="tcp_thread",
                rdma_backend="closed_sdk",
                with_tikv_runtime=True,
                transport_backends=["fastws"],
                reuse_existing_release=False,
                skip_top_level_release=False,
                repo_test_rsc_root=None,
                prepare_config=None,
                baseline_source_root=None,
                redis_bundle_src=None,
                alluxio_bundle_src=None,
                build_redis_bundle_docker=False,
                redis_version=None,
                redis_source_url=None,
                redis_source_sha256=None,
                redis_docker_image=None,
            )

    def test_default_top_level_transport_backend_is_tcp_thread(self) -> None:
        self.assertEqual(_PACK.DEFAULT_TOP_LEVEL_TRANSPORT_BACKEND, "tcp_thread")

    def test_pack_ci_src_stages_all_declared_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            for relpath in (
                "setup_and_pack/tool.py",
                "fluxon_py/__init__.py",
                "fluxon_rs/Cargo.toml",
                "deployment/dispatch.py",
                "examples/demo.py",
                "fluxon_test_stack/case.yaml",
                "scripts/_build_doc_site_in_container_inner.py",
                "fluxon_doc_cn/roadmap.md",
                "fluxon_doc_en/roadmap.md",
                "README.md",
                ".github/workflows/all_test.yml",
                "fluxon_release/install.py",
                "fluxon_release/closed_sdk/manifest.json",
                "fluxon_release/test_rsc/source/prepare.yaml",
                ".dever/tmp.log",
                "skills/demo/SKILL.md",
            ):
                path = repo_root / relpath
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("x\n", encoding="utf-8")
            (repo_root / "setup.py").write_text("x\n", encoding="utf-8")
            (repo_root / ".gitignore").write_text(
                "\n".join(
                    [
                        "fluxon_release/*",
                        "!fluxon_release/install.py",
                        "!fluxon_release/closed_sdk/",
                        "!fluxon_release/closed_sdk/**",
                        "!fluxon_release/test_rsc/",
                        "fluxon_release/test_rsc/*",
                        "!fluxon_release/test_rsc/source/",
                        "!fluxon_release/test_rsc/source/**",
                        ".dever",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            out_path = repo_root / "out" / "src_ci.tar.gz"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            tar_invocations: list[tuple[Path, list[str]]] = []
            git_relpaths = [
                "setup_and_pack/tool.py",
                "fluxon_py/__init__.py",
                "fluxon_rs/Cargo.toml",
                "deployment/dispatch.py",
                "examples/demo.py",
                "fluxon_test_stack/case.yaml",
                "scripts/_build_doc_site_in_container_inner.py",
                "fluxon_doc_cn/roadmap.md",
                "fluxon_doc_en/roadmap.md",
                "README.md",
                ".github/workflows/all_test.yml",
            ]

            def fake_git_stage_ci_source_tree(*, repo_root, stage_root):
                for relpath in git_relpaths:
                    src = repo_root / relpath
                    dst = stage_root / relpath
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
                return list(git_relpaths)

            def fake_tar_gz(*, cwd, out_path, inputs, honor_vcs_ignores):
                del honor_vcs_ignores
                tar_invocations.append((cwd, list(inputs)))
                for name in inputs:
                    assert (cwd / name).exists(), f"missing staged input {name}"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text("stub\n", encoding="utf-8")

            with (
                mock.patch.object(
                    _PACK,
                    "_collect_ci_source_relpaths",
                    return_value=list(git_relpaths),
                ),
                mock.patch.object(_PACK, "_git_stage_ci_source_tree", side_effect=fake_git_stage_ci_source_tree),
                mock.patch.object(_PACK.script_utils, "tar_gz", side_effect=fake_tar_gz),
            ):
                _PACK._pack_ci_src(repo_root=repo_root, out_path=out_path)

            self.assertTrue(out_path.is_file())
            self.assertEqual(len(tar_invocations), 1)
            staged_inputs = set(tar_invocations[0][1])
            for relpath in (
                "setup_and_pack/tool.py",
                "fluxon_py/__init__.py",
                "fluxon_rs/Cargo.toml",
                "deployment/dispatch.py",
                "examples/demo.py",
                "fluxon_test_stack/case.yaml",
                "scripts/_build_doc_site_in_container_inner.py",
                "fluxon_doc_cn/roadmap.md",
                "fluxon_doc_en/roadmap.md",
                "README.md",
                ".github/workflows/all_test.yml",
            ):
                self.assertIn(relpath, staged_inputs)
            for relpath in (
                "fluxon_release/install.py",
                "fluxon_release/closed_sdk/manifest.json",
                "fluxon_release/test_rsc/source/prepare.yaml",
                ".dever/tmp.log",
                "skills/demo/SKILL.md",
            ):
                self.assertNotIn(relpath, staged_inputs)

    def test_git_stage_ci_source_tree_excludes_runtime_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            stage_root = repo_root / "stage"
            for relpath in (
                "scripts/_build_doc_site_in_container_inner.py",
                "fluxon_doc_cn/roadmap.md",
                "README.md",
            ):
                path = repo_root / relpath
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("x\n", encoding="utf-8")
            raw = b"\0".join(
                [
                    b"scripts/_build_doc_site_in_container_inner.py",
                    b"fluxon_doc_cn/roadmap.md",
                    b"README.md",
                    b"fluxon_release/install.py",
                    b".dever/run.log",
                    b"skills/demo/SKILL.md",
                ]
            ) + b"\0"

            with mock.patch.object(_PACK.subprocess, "check_output", return_value=raw):
                relpaths = _PACK._git_stage_ci_source_tree(repo_root=repo_root, stage_root=stage_root)

            self.assertEqual(
                relpaths,
                ["README.md", "fluxon_doc_cn/roadmap.md", "scripts/_build_doc_site_in_container_inner.py"],
            )
            self.assertTrue((stage_root / "README.md").is_file())
            self.assertTrue((stage_root / "fluxon_doc_cn" / "roadmap.md").is_file())
            self.assertTrue((stage_root / "scripts" / "_build_doc_site_in_container_inner.py").is_file())
            self.assertFalse((stage_root / "fluxon_release").exists())
            self.assertFalse((stage_root / ".dever").exists())
            self.assertFalse((stage_root / "skills").exists())

    def test_collect_ci_source_relpaths_excludes_runtime_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            for relpath in (
                "scripts/_build_doc_site_in_container_inner.py",
                "fluxon_doc_cn/roadmap.md",
                "README.md",
                "fluxon_release/install.py",
                ".dever/run.log",
                "skills/demo/SKILL.md",
            ):
                path = repo_root / relpath
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("x\n", encoding="utf-8")
            (repo_root / ".gitignore").write_text(
                "\n".join(
                    [
                        "fluxon_release/*",
                        "!fluxon_release/install.py",
                        ".dever",
                        "skills/",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            raw = b"\0".join(
                [
                    b"scripts/_build_doc_site_in_container_inner.py",
                    b"fluxon_doc_cn/roadmap.md",
                    b"README.md",
                    b"fluxon_release/install.py",
                    b".dever/run.log",
                    b"skills/demo/SKILL.md",
                ]
            ) + b"\0"

            with mock.patch.object(
                _PACK.collect_source_profile_relpaths.__globals__["git_source_selection_utils"].subprocess,
                "check_output",
                return_value=raw,
            ):
                relpaths = _PACK._collect_ci_source_relpaths(repo_root=repo_root)

            self.assertEqual(
                relpaths,
                ["README.md", "fluxon_doc_cn/roadmap.md", "scripts/_build_doc_site_in_container_inner.py"],
            )
            self.assertNotIn("fluxon_release/install.py", relpaths)
            self.assertNotIn(".dever/run.log", relpaths)
            self.assertNotIn("skills/demo/SKILL.md", relpaths)

    def test_collect_ci_source_relpaths_includes_rather_no_git_submodule_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            tracked_root = repo_root / "scripts"
            tracked_root.mkdir(parents=True, exist_ok=True)
            (tracked_root / "_build_doc_site_in_container_inner.py").write_text("tracked\n", encoding="utf-8")
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
                    return b"scripts/_build_doc_site_in_container_inner.py\0"
                if cwd_path == module_root.resolve():
                    return b"Cargo.toml\0src/lib.rs\0"
                raise AssertionError(f"unexpected git ls-files cwd: {cwd_path}")

            with mock.patch.object(
                _PACK.collect_source_profile_relpaths.__globals__["git_source_selection_utils"].subprocess,
                "check_output",
                side_effect=fake_check_output,
            ):
                relpaths = _PACK._collect_ci_source_relpaths(repo_root=repo_root)

            self.assertEqual(
                relpaths,
                [
                    "fluxon_rs/moka/Cargo.toml",
                    "fluxon_rs/moka/src/lib.rs",
                    "scripts/_build_doc_site_in_container_inner.py",
                ],
            )

    def test_test_rsc_manifest_file_list_ignores_unstaged_extra_archive_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir) / "out"
            prepared_root = Path(tmpdir) / "prepared"
            out_dir.mkdir()
            prepared_root.mkdir()
            (out_dir / "src_ci.tar.gz").write_text("src\n", encoding="utf-8")
            (out_dir / "fluxon_ci_ext_rsc.tar.gz").write_text("ext\n", encoding="utf-8")
            (out_dir / "extra_image.tar").write_text("image\n", encoding="utf-8")

            files = _PACK._test_rsc_manifest_file_list(
                out_dir=out_dir,
                prepared_root=prepared_root,
            )

            self.assertEqual(
                [path.name for path in files],
                ["src_ci.tar.gz", "fluxon_ci_ext_rsc.tar.gz"],
            )

    def test_collect_ci_source_relpaths_requires_rather_no_git_submodule_root_to_exist(self) -> None:
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

            with (
                mock.patch.object(
                    _PACK.collect_source_profile_relpaths.__globals__["git_source_selection_utils"],
                    "collect_git_listed_source_relpaths",
                    return_value=["scripts/_build_doc_site_in_container_inner.py"],
                ),
                self.assertRaisesRegex(
                    RuntimeError,
                    "CI source pack requires configured rather_no_git_submodule path to exist",
                ),
            ):
                _PACK._collect_ci_source_relpaths(repo_root=repo_root)

    def test_compute_ci_source_digest_uses_selected_git_paths_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            tracked = repo_root / "README.md"
            tracked.write_text("tracked\n", encoding="utf-8")
            blocked = repo_root / ".dever" / "blocked.txt"
            blocked.parent.mkdir(parents=True, exist_ok=True)
            blocked.write_text("blocked\n", encoding="utf-8")

            with (
                mock.patch.object(
                    _PACK,
                    "_collect_ci_source_relpaths",
                    return_value=["README.md"],
                ),
                mock.patch.object(
                    _PACK.script_utils,
                    "compute_paths_digest",
                    wraps=_PACK.script_utils.compute_paths_digest,
                ) as digest_mock,
            ):
                digest = _PACK._compute_ci_source_digest(repo_root=repo_root)

            self.assertTrue(digest)
            digest_roots = digest_mock.call_args.args[0]
            self.assertEqual(digest_roots, [tracked.resolve()])

    def test_prune_stage_paths_applies_glob_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            stage_root = Path(tmpdir)
            keep_path = stage_root / "keep.txt"
            pyc_path = stage_root / "pkg" / "drop.pyc"
            baseline_file = stage_root / "baselines" / "manifest.txt"
            pyc_path.parent.mkdir(parents=True, exist_ok=True)
            baseline_file.parent.mkdir(parents=True, exist_ok=True)
            keep_path.write_text("keep\n", encoding="utf-8")
            pyc_path.write_text("drop\n", encoding="utf-8")
            baseline_file.write_text("drop\n", encoding="utf-8")

            _PACK.script_utils.prune_stage_paths(
                stage_root,
                ("*.pyc", "baselines/"),
            )

            self.assertTrue(keep_path.exists())
            self.assertFalse(pyc_path.exists())
            self.assertFalse(baseline_file.exists())

    def test_shared_rsync_stage_accepts_exclude_patterns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            src = repo_root / "src"
            dst = repo_root / "dst"
            (src / "keep").mkdir(parents=True, exist_ok=True)
            (src / "drop").mkdir(parents=True, exist_ok=True)
            (src / "keep" / "a.txt").write_text("keep\n", encoding="utf-8")
            (src / "drop" / "b.txt").write_text("drop\n", encoding="utf-8")

            run_mock = mock.Mock()
            with mock.patch.dict(
                _PACK.script_utils.rsync_stage.__globals__,
                {"run_cmd_argv": run_mock},
            ):
                _PACK.script_utils.rsync_stage(
                    repo_root=repo_root,
                    src=src,
                    dst=dst,
                    honor_gitignore=False,
                    exclude_rel_paths=("drop/", "*.tmp"),
                )

            argv = run_mock.call_args.args[0]
            self.assertIn("--exclude=drop/", argv)
            self.assertIn("--exclude=*.tmp", argv)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
