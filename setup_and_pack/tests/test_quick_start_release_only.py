from __future__ import annotations

import importlib.util
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fluxon_py.api_error import OK_NONE, Result
from fluxon_py import quick_start as _QUICK_START_API


REPO_ROOT = Path(__file__).resolve().parents[2]
QUICK_START_BUILD_IMAGE_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "build_image.py"
QUICK_START_WRAPPER_PATH = REPO_ROOT / "examples" / "fluxon_quick_start" / "start.py"
QUICK_START_START_PATH = REPO_ROOT / "fluxon_py" / "quick_start" / "start.py"
PACK_FLUXON_PYLIB_PATH = REPO_ROOT / "setup_and_pack" / "pack_fluxon_pylib.py"


def _new_kv_configs(
    *,
    master_root: Path,
    owner_root: Path,
    share_mem_path: Path,
    etcd_endpoint: str = "127.0.0.1:22379",
    cluster_name: str = "qs_fs_cluster",
    master_port: int = 25100,
):
    monitoring_base_url = "http://127.0.0.1:24000"
    kv_master_config = {
        "etcd_endpoints": [etcd_endpoint],
        "cluster_name": cluster_name,
        "instance_key": "qs_master",
        "port": master_port,
        "log_dir": str(master_root / "log"),
        "monitoring": {
            "prometheus_base_url": f"{monitoring_base_url}/v1/prometheus",
            "prom_remote_write_url": [
                f"{monitoring_base_url}/v1/prometheus/write"
            ],
            "otlp_log_api": {
                "otlp_endpoint": f"{monitoring_base_url}/v1/otlp/v1/logs",
                "db_name": "public",
                "table_name": "fluxon_logs",
            },
        },
    }
    kv_owner_config = {
        "instance_key": "qs_kvclient",
        "contribute_to_cluster_pool_size": {"dram": 1073741824, "vram": {}},
        "fluxonkv_spec": {
            "etcd_addresses": [etcd_endpoint],
            "cluster_name": cluster_name,
            "share_mem_path": str(share_mem_path),
            "sub_cluster": "default",
            "large_file_paths": [str(owner_root / "large")],
        },
    }
    return kv_master_config, kv_owner_config


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


_BUILD_IMAGE = _load_module("fluxon_quick_start_build_image_test", QUICK_START_BUILD_IMAGE_PATH)
_START = _load_module("fluxon_quick_start_start_test", QUICK_START_START_PATH)
_PACK_FLUXON_PYLIB = _load_module("pack_fluxon_pylib_test", PACK_FLUXON_PYLIB_PATH)


class QuickStartReleaseOnlyTest(unittest.TestCase):
    def test_example_wrapper_only_calls_the_installed_module(self) -> None:
        source = QUICK_START_WRAPPER_PATH.read_text(encoding="utf-8")

        self.assertIn("from fluxon_py.quick_start.start import main", source)
        self.assertNotIn("sys.path", source)

    def test_installed_serve_s3_single_node_api_builds_the_complete_service_command(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/srv/state/kv-master"),
            owner_root=Path("/srv/state/kv-owner"),
            share_mem_path=Path("/dev/shm/fluxon-s3"),
        )
        with mock.patch.object(_QUICK_START_API, "main") as quick_start_main:
            _QUICK_START_API.serve_s3_single_node(
                "/srv/data",
                "/srv/state",
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
            )

        quick_start_main.assert_called_once()
        call_args, call_kwargs = quick_start_main.call_args
        self.assertEqual(
            call_args[0],
            [
                "--mode",
                "fs",
                "--serve",
                "--fs-root",
                "/srv/data",
                "--export-name",
                "quick-start-export",
                "--workdir",
                "/srv/state",
                "--panel-port",
                "26180",
                "--etcd-client-port",
                "22379",
                "--kv-master-port",
                "25100",
                "--greptime-http-port",
                "24000",
            ],
        )
        self.assertEqual(call_kwargs["fs_kv_master_config"], kv_master_config)
        self.assertEqual(call_kwargs["fs_kv_owner_config"], kv_owner_config)

    def test_installed_serve_s3_single_node_forwards_custom_export_name(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/srv/state/kv-master"),
            owner_root=Path("/srv/state/kv-owner"),
            share_mem_path=Path("/dev/shm/fluxon-s3"),
        )
        with mock.patch.object(_QUICK_START_API, "main") as quick_start_main:
            _QUICK_START_API.serve_s3_single_node(
                "/srv/data",
                "/srv/state",
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
                export_name="model-artifacts",
            )

        command = quick_start_main.call_args.args[0]
        export_name_index = command.index("--export-name")
        self.assertEqual(command[export_name_index + 1], "model-artifacts")

    def test_serve_s3_single_node_rejects_invalid_export_name(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/srv/state/kv-master"),
            owner_root=Path("/srv/state/kv-owner"),
            share_mem_path=Path("/dev/shm/fluxon-s3"),
        )
        with self.assertRaisesRegex(ValueError, "valid S3 bucket name"):
            _QUICK_START_API.serve_s3_single_node(
                "/srv/data",
                "/srv/state",
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
                export_name="Invalid_Bucket",
            )

    def test_serve_s3_single_node_requires_explicit_kv_dependencies(self) -> None:
        with self.assertRaises(TypeError):
            _QUICK_START_API.serve_s3_single_node("/srv/data", "/srv/state")

    def test_serve_s3_single_node_requires_complete_master_monitoring(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/srv/state/kv-master"),
            owner_root=Path("/srv/state/kv-owner"),
            share_mem_path=Path("/dev/shm/fluxon-s3"),
        )
        del kv_master_config["monitoring"]

        with self.assertRaisesRegex(
            ValueError,
            r"kv_master_config\.monitoring must be a dict",
        ):
            _QUICK_START_API.serve_s3_single_node(
                "/srv/data",
                "/srv/state",
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
            )

    def test_serve_s3_single_node_can_reuse_external_middleware(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/srv/state/kv-master"),
            owner_root=Path("/srv/state/kv-owner"),
            share_mem_path=Path("/dev/shm/fluxon-s3"),
            etcd_endpoint="host.docker.internal:22379",
        )
        with mock.patch.object(_QUICK_START_API, "main") as quick_start_main:
            _QUICK_START_API.serve_s3_single_node(
                "/srv/data",
                "/srv/state",
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
                start_middleware=False,
                greptime_base_url="http://host.docker.internal:24000",
            )

        command = quick_start_main.call_args.args[0]
        self.assertEqual(
            command[-3:],
            [
                "--external-middleware",
                "--greptime-base-url",
                "http://host.docker.internal:24000",
            ],
        )
        self.assertEqual(
            quick_start_main.call_args.kwargs["fs_kv_master_config"],
            kv_master_config,
        )

    def test_quick_start_does_not_export_kv_process_wrappers(self) -> None:
        self.assertFalse(hasattr(_QUICK_START_API, "KvMaster"))
        self.assertFalse(hasattr(_QUICK_START_API, "KvOwner"))

    def test_fs_cli_requires_an_explicit_share_mem_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(ValueError, "--share-mem-path is required"):
                _START.main(
                    [
                        "--mode",
                        "fs",
                        "--workdir",
                        tmpdir,
                        "--greptime-http-port",
                        "24000",
                        "--panel-port",
                        "26180",
                    ]
                )

    def test_fs_config_uses_injected_kv_runtime_configs(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/custom/master"),
            owner_root=Path("/custom/owner"),
            share_mem_path=Path("/custom/sharemem"),
            etcd_endpoint="127.0.0.1:12379",
            master_port=34100,
        )

        cfg = _START._gen_fs_config(
            34180,
            14000,
            Path("/srv/state"),
            fs_root=Path("/srv/data"),
            kv_master_config=kv_master_config,
            kv_owner_config=kv_owner_config,
        )

        self.assertEqual(cfg["master"], kv_master_config)
        self.assertEqual(cfg["kvclient"], kv_owner_config)
        owner_spec = cfg["kvclient"]["fluxonkv_spec"]
        self.assertEqual(owner_spec["share_mem_path"], "/custom/sharemem")
        self.assertEqual(owner_spec["large_file_paths"], ["/custom/owner/large"])

    def test_fs_config_uses_external_greptime_base_url(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/custom/master"),
            owner_root=Path("/custom/owner"),
            share_mem_path=Path("/custom/sharemem"),
        )
        cfg = _START._gen_fs_config(
            26180,
            24000,
            Path("/srv/state"),
            fs_root=Path("/srv/data"),
            kv_master_config=kv_master_config,
            kv_owner_config=kv_owner_config,
            greptime_base_url="http://fluxon-greptime:4000",
        )

        panel = cfg["fs_master"]["fluxon_fs"]["master_panel"]
        self.assertEqual(
            panel["prometheus_base_url"],
            "http://fluxon-greptime:4000/v1/prometheus",
        )

    def test_fs_config_uses_export_name_for_master_and_agent(self) -> None:
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=Path("/custom/master"),
            owner_root=Path("/custom/owner"),
            share_mem_path=Path("/custom/sharemem"),
        )
        cfg = _START._gen_fs_config(
            26180,
            24000,
            Path("/srv/state"),
            fs_root=Path("/srv/data"),
            kv_master_config=kv_master_config,
            kv_owner_config=kv_owner_config,
            export_name="model-artifacts",
        )

        for component in ("fs_master", "fs_agent"):
            exports = cfg[component]["fluxon_fs"]["cache"]["exports"]
            self.assertEqual(list(exports), ["model-artifacts"])
            self.assertEqual(
                exports["model-artifacts"]["remote_root_dir_abs"],
                "/srv/data",
            )

    def test_fs_infrastructure_uses_canonical_kv_runtime_starters(self) -> None:
        state_root = Path("/srv/state")
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=state_root / "kv-master",
            owner_root=state_root / "kv-owner",
            share_mem_path=Path("/dev/shm/fluxon-s3"),
        )
        cfg = _START._gen_fs_config(
            26180,
            24000,
            state_root,
            fs_root=Path("/srv/data"),
            kv_master_config=kv_master_config,
            kv_owner_config=kv_owner_config,
        )
        master_proc = mock.Mock()
        owner_proc = mock.Mock()

        with (
            mock.patch.object(_START, "_start_greptime", return_value=mock.Mock()),
            mock.patch.object(_START, "_start_etcd", return_value=mock.Mock()),
            mock.patch.object(_START, "_wait_for_process_tcp_ready"),
            mock.patch.object(_START, "_wait_for_process_http_ready"),
            mock.patch.object(_START, "_wait_for_process_alive"),
            mock.patch.object(_START, "_wait_for_shared_json"),
            mock.patch.object(_START, "_clear_stale_shared_json"),
            mock.patch.object(_START, "_track_child", side_effect=lambda proc: proc),
            mock.patch.object(
                _START,
                "start_kv_master_process",
                return_value=master_proc,
            ) as start_master,
            mock.patch.object(
                _START,
                "start_owner_kvclient_process",
                return_value=owner_proc,
            ) as start_owner,
        ):
            _START._start_cluster_infra(
                cfg=cfg,
                workdir=state_root,
                etcd_client_port=22379,
                greptime_port=24000,
            )

        start_master.assert_called_once_with(
            config=kv_master_config,
            log_path=state_root / "log" / "master.log",
        )
        start_owner.assert_called_once_with(
            config=kv_owner_config,
            log_path=state_root / "log" / "kvclient.log",
        )

    def test_fs_infrastructure_reuses_external_middleware(self) -> None:
        state_root = Path("/srv/state")
        kv_master_config, kv_owner_config = _new_kv_configs(
            master_root=state_root / "kv-master",
            owner_root=state_root / "kv-owner",
            share_mem_path=Path("/dev/shm/fluxon-s3"),
            etcd_endpoint="host.docker.internal:22379",
        )
        cfg = _START._gen_fs_config(
            26180,
            24000,
            state_root,
            fs_root=Path("/srv/data"),
            kv_master_config=kv_master_config,
            kv_owner_config=kv_owner_config,
            greptime_base_url="http://host.docker.internal:24000",
        )

        with (
            mock.patch.object(_START, "_start_greptime") as start_greptime,
            mock.patch.object(_START, "_start_etcd") as start_etcd,
            mock.patch.object(_START, "_wait_for_tcp") as wait_for_tcp,
            mock.patch.object(_START, "_wait_for_http_ready") as wait_for_http,
            mock.patch.object(_START, "_wait_for_process_alive"),
            mock.patch.object(_START, "_wait_for_shared_json"),
            mock.patch.object(_START, "_clear_stale_shared_json"),
            mock.patch.object(_START, "_track_child", side_effect=lambda proc: proc),
            mock.patch.object(_START, "start_kv_master_process", return_value=mock.Mock()),
            mock.patch.object(_START, "start_owner_kvclient_process", return_value=mock.Mock()),
        ):
            _START._start_cluster_infra(
                cfg=cfg,
                workdir=state_root,
                etcd_client_port=22379,
                greptime_port=24000,
                external_middleware=True,
                greptime_base_url="http://host.docker.internal:24000",
            )

        start_greptime.assert_not_called()
        start_etcd.assert_not_called()
        wait_for_tcp.assert_called_once_with(
            "host.docker.internal",
            24000,
            "greptime",
            timeout=30,
        )
        wait_for_http.assert_called_once_with(
            "http://host.docker.internal:22379/health",
            "etcd",
            timeout=30,
        )

    def test_stage_build_context_copies_release_wheels_without_source_tree(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            release_dir = root / "release"
            context_root = root / "context"
            (release_dir / "ext_images" / "etcd").mkdir(parents=True)
            (release_dir / "ext_images" / "greptime").mkdir(parents=True)
            (release_dir / "ext_images" / "etcd" / "etcd").write_text("etcd", encoding="utf-8")
            (release_dir / "ext_images" / "etcd" / "etcdctl").write_text("etcdctl", encoding="utf-8")
            (release_dir / "ext_images" / "greptime" / "greptime").write_text("greptime", encoding="utf-8")
            (release_dir / "fluxon_py-0.2.2-cp38-abi3-manylinux_2_28_x86_64.whl").write_text("wheel", encoding="utf-8")

            dockerfile_path = _BUILD_IMAGE._stage_build_context(release_dir=release_dir, context_root=context_root)

            self.assertEqual(dockerfile_path, context_root / "examples" / "fluxon_quick_start" / "Dockerfile")
            self.assertTrue(
                (context_root / "fluxon_release" / "fluxon_py-0.2.2-cp38-abi3-manylinux_2_28_x86_64.whl").is_file()
            )
            self.assertFalse((context_root / "fluxon_py").exists())
            self.assertFalse((context_root / "setup.py").exists())

    def test_kv_http_delete_route_uses_store_remove_contract(self) -> None:
        class _FakeStore:
            def __init__(self) -> None:
                self.remove_calls: list[str] = []

            def remove(self, key: str):
                self.remove_calls.append(key)
                return Result.new_ok(OK_NONE)

        fake_store = _FakeStore()
        previous_store = _START._kv_http_store
        _START._kv_http_store = fake_store
        try:
            with _START._KV_HTTP_APP.test_client() as client:
                resp = client.delete("/api/kv/demo")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.get_json()["key"], "demo")
            self.assertEqual(fake_store.remove_calls, ["demo"])
        finally:
            _START._kv_http_store = previous_store

    def test_handle_mq_shell_line_treats_status_as_command_not_message(self) -> None:
        source = QUICK_START_START_PATH.read_text(encoding="utf-8")

        self.assertIn('if cmd == "status":', source)
        self.assertIn('print("Commands:  put <message>  |  status  |  exit")', source)

        namespace: dict[str, object] = {}
        helper_source = """
def _handle_mq_shell_line(line, shutdown_requested, status_lines):
    parts = line.split(None, 1)
    cmd = parts[0].lower()
    if cmd in ("exit", "quit", "q"):
        shutdown_requested.set()
        return True, None
    if cmd == "help":
        print("Commands:  put <message>  |  status  |  exit")
        return True, None
    if cmd == "status":
        for status_line in status_lines():
            print(status_line)
        return True, None

    msg = parts[1] if cmd == "put" and len(parts) >= 2 else line
    return False, msg
"""
        exec(helper_source, namespace)
        helper = namespace["_handle_mq_shell_line"]

        shutdown_requested = mock.Mock()
        stdout = io.StringIO()
        with mock.patch("sys.stdout", stdout):
            handled, msg = helper("status", shutdown_requested, lambda: ["MQ shell status:", "  ok"])
        self.assertEqual((handled, msg), (True, None))
        self.assertIn("MQ shell status:", stdout.getvalue())
        shutdown_requested.set.assert_not_called()

    def test_quick_start_owner_configs_include_large_file_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            workdir = Path(tmpdir)

            kv_cfg = _START._gen_kv_config(
                "127.0.0.1:12379",
                "qs_kv_cluster",
                31000,
                8083,
                0,
                14000,
                workdir,
            )
            mq_cfg = _START._gen_mq_config(
                "127.0.0.1:12379",
                "qs_mq_cluster",
                34200,
                14000,
                workdir,
                panel_port=18080,
            )
            kv_master_config, kv_owner_config = _new_kv_configs(
                master_root=workdir / "kv-master",
                owner_root=workdir / "kv-owner",
                share_mem_path=workdir / "sharemem-fs",
                etcd_endpoint="127.0.0.1:12379",
                master_port=34100,
            )
            fs_cfg = _START._gen_fs_config(
                34180,
                14000,
                workdir,
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
            )

            expected = [str(workdir / "large" / "owner")]
            self.assertEqual(kv_cfg["kvclient"]["fluxonkv_spec"]["large_file_paths"], expected)
            self.assertEqual(mq_cfg["kvclient"]["fluxonkv_spec"]["large_file_paths"], expected)
            self.assertEqual(
                fs_cfg["kvclient"]["fluxonkv_spec"]["large_file_paths"],
                [str(workdir / "kv-owner" / "large")],
            )

    def test_fs_quick_start_bootstraps_fixed_credentials_and_requires_initial_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fs_root = Path(tmpdir) / "existing-data"
            kv_master_config, kv_owner_config = _new_kv_configs(
                master_root=Path(tmpdir) / "kv-master",
                owner_root=Path(tmpdir) / "kv-owner",
                share_mem_path=Path(tmpdir) / "sharemem",
                etcd_endpoint="127.0.0.1:12379",
                master_port=34100,
            )
            cfg = _START._gen_fs_config(
                34180,
                14000,
                Path(tmpdir),
                fs_root=fs_root,
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
            )

        panel = cfg["fs_master"]["fluxon_fs"]["master_panel"]
        self.assertEqual(
            panel["bootstrap_access_model"]["users"],
            [
                {
                    "username": "admin",
                    "password": "admin",
                    "can_manage_users": True,
                }
            ],
        )
        self.assertTrue(panel["require_bootstrap_credentials_change"])
        self.assertEqual(
            cfg["fs_master"]["fluxon_fs"]["cache"]["exports"]["quick-start-export"][
                "remote_root_dir_abs"
            ],
            str(fs_root),
        )

    def test_fs_service_mode_waits_until_shutdown_signal(self) -> None:
        callbacks: list[object] = []

        def _register(callback, *, thread_name: str):
            self.assertEqual(thread_name, "qs-fs-service-signal")
            callbacks.append(callback)

            def _restore() -> None:
                return None

            callback("test shutdown")
            return _restore

        with (
            mock.patch.object(_START, "register_ctrlc_callback", side_effect=_register),
            mock.patch.object(_START, "_children", []),
        ):
            _START._run_service_until_stopped()

        self.assertEqual(len(callbacks), 1)

    def test_explicit_fs_root_is_not_seeded_with_sample_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fs_root = Path(tmpdir) / "existing-data"
            _START._prepare_fs_root(fs_root, create_sample=False)

            self.assertTrue(fs_root.is_dir())
            self.assertFalse((fs_root / "hello.txt").exists())

    def test_quick_start_etcd_listens_on_loopback_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.object(_START, "_find_binary", return_value="/usr/bin/etcd"),
                mock.patch.object(_START, "_spawn", return_value=object()) as spawn,
            ):
                _START._start_etcd(22379, Path(tmpdir))

        command = spawn.call_args.args[0]
        listen_index = command.index("--listen-client-urls")
        self.assertEqual(command[listen_index + 1], "http://127.0.0.1:22379")

    def test_pack_fluxon_pylib_cleans_stale_build_artifacts_before_bdist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            release_dir = repo_root / "fluxon_release"
            build_file = repo_root / "build" / "lib" / "fluxon_py" / "runtime" / "start_monitor_web.py"
            dist_dir = repo_root / "dist"
            egg_info = repo_root / "fluxon.egg-info"
            build_file.parent.mkdir(parents=True)
            build_file.write_text("stale", encoding="utf-8")
            dist_dir.mkdir()
            egg_info.mkdir()
            release_dir.mkdir()

            _PACK_FLUXON_PYLIB._clean_python_build_artifacts(repo_root=repo_root, release_dir=release_dir)

            self.assertFalse((repo_root / "build").exists())
            self.assertFalse(dist_dir.exists())
            self.assertFalse(egg_info.exists())
            self.assertTrue(release_dir.exists())

if __name__ == "__main__":
    unittest.main()
