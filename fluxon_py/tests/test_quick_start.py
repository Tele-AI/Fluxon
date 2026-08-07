from __future__ import annotations

import importlib
import tempfile
from pathlib import Path
import unittest


class QuickStartCompatTest(unittest.TestCase):
    def test_import_quick_start_module(self) -> None:
        module = importlib.import_module("fluxon_py.quick_start")
        self.assertTrue(hasattr(module, "serve_s3_single_node"))

    def test_build_s3_single_node_bundle_uses_expected_paths(self) -> None:
        module = importlib.import_module("fluxon_py.quick_start")

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_dir = root / "data"
            state_dir = root / "state"
            data_dir.mkdir()
            state_dir.mkdir()

            kv_master_config = {
                "etcd_endpoints": ["127.0.0.1:22379"],
                "cluster_name": "fluxon_s3",
                "instance_key": "fluxon_s3_master",
                "port": 25100,
                "log_dir": "/tmp/unused",
                "monitoring": {
                    "prometheus_base_url": "http://127.0.0.1:24000/v1/prometheus",
                    "prom_remote_write_url": ["http://127.0.0.1:24000/v1/prometheus/write"],
                    "otlp_log_api": {
                        "otlp_endpoint": "http://127.0.0.1:24000/v1/otlp/v1/logs",
                        "db_name": "public",
                        "table_name": "fluxon_logs",
                    },
                },
            }
            kv_owner_config = {
                "instance_key": "fluxon_s3_owner",
                "contribute_to_cluster_pool_size": {"dram": 1024 * 1024 * 1024, "vram": {}},
                "fluxonkv_spec": {
                    "etcd_addresses": ["127.0.0.1:22379"],
                    "cluster_name": "fluxon_s3",
                    "share_mem_path": str(state_dir / "sharemem"),
                    "sub_cluster": "default",
                    "large_file_paths": [str(state_dir / "large" / "owner")],
                },
            }

            bundle = module._build_s3_single_node_bundle(
                data_dir=data_dir,
                state_dir=state_dir,
                kv_master_config=kv_master_config,
                kv_owner_config=kv_owner_config,
                export_name="quick-start-export",
                greptime_base_url="http://127.0.0.1:24000",
                panel_port=26180,
                panel_listen_host="0.0.0.0",
                bootstrap_username="admin",
                bootstrap_password="admin",
                export_cache_max_bytes=1024 * 1024 * 1024,
            )

            self.assertEqual(bundle.s3_endpoint, "http://127.0.0.1:26180/fs_s3")
            self.assertEqual(bundle.s3_ui_url, "http://127.0.0.1:26180/fs_s3/ui/")
            self.assertEqual(
                bundle.fs_master_config["fluxon_fs"]["cache"]["exports"]["quick-start-export"]["remote_root_dir_abs"],
                str(data_dir.resolve()),
            )
            self.assertEqual(
                bundle.fs_master_config["fluxon_fs"]["master_panel"]["access_db_path"],
                str((state_dir / "fs_master" / "access.db").resolve()),
            )
            self.assertEqual(
                bundle.fs_master_config["fluxon_fs"]["master_panel"]["bootstrap_access_model"]["users"][0]["username"],
                "admin",
            )
            self.assertEqual(
                bundle.fs_agent_config["fluxon_fs"]["cache"]["exports"]["quick-start-export"]["remote_root_dir_abs"],
                str(data_dir.resolve()),
            )
            self.assertEqual(
                bundle.kv_owner_config["fluxonkv_spec"]["share_mem_path"],
                str((state_dir / "sharemem").resolve()),
            )

    def test_start_middleware_true_is_rejected(self) -> None:
        module = importlib.import_module("fluxon_py.quick_start")
        with self.assertRaises(NotImplementedError):
            module.serve_s3_single_node(
                "/tmp/data",
                "/tmp/state",
                kv_master_config={},
                kv_owner_config={},
                start_middleware=True,
            )

