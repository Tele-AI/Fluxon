import argparse
import contextlib
import io
import os
import site
import yaml
import sys
import copy
import types
import importlib.util
from pathlib import Path
from typing import Callable, List, Optional, Tuple
from unittest import mock


def main() -> int:
    parser = argparse.ArgumentParser(description="Fluxon config test runner")
    parser.add_argument("--test-id", help="Run only the named test id")
    args = parser.parse_args()

    print("=" * 60)
    print("Testing FluxonKvClientConfig with upgraded verification")
    print("=" * 60)

    try:
        checks = _build_checks(args.test_id)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 2

    failures = 0
    for _, check in checks:
        if not _run_check(check):
            failures += 1

    print("=" * 60)
    print("All tests completed!" if failures == 0 else f"Completed with {failures} failing check group(s)")
    print("=" * 60)
    return 0 if failures == 0 else 1


def _build_checks(selected_test_id: Optional[str]) -> List[Tuple[str, Callable[[], None]]]:
    checks: List[Tuple[str, Callable[[], None]]] = [
        ("verification", _run_test_verification),
        ("load_from_dict", _run_test_load_from_dict),
        ("load_from_file", _run_test_load_from_file),
        ("to_yaml_str_roundtrip", _run_test_to_yaml_str_roundtrip),
        ("fluxonkv_sub_cluster_config", test_fluxonkv_sub_cluster_config),
        ("fluxonkv_owner_requires_sub_cluster", test_fluxonkv_owner_requires_sub_cluster),
        ("fluxonkv_owner_requires_large_file_paths", test_fluxonkv_owner_requires_large_file_paths),
        ("fluxonkv_external_forbids_large_file_paths", test_fluxonkv_external_forbids_large_file_paths),
        ("fluxonkv_p2p_relay_removed", test_fluxonkv_p2p_relay_removed),
        ("fluxon_client_config_yaml_shape", test_fluxon_client_config_yaml_shape),
        ("fluxonkv_protocol_field", test_fluxonkv_protocol_field),
        ("fluxonkv_runtime_defaults_are_internal", test_fluxonkv_runtime_defaults_are_internal),
        ("fluxonkv_removed_rdma_config_keys", test_fluxonkv_removed_rdma_config_keys),
        ("fluxonkv_test_spec_config", test_fluxonkv_test_spec_config),
        ("fluxon_pyo3_import_authority", test_fluxon_pyo3_import_authority),
    ]
    if selected_test_id is None:
        return checks

    for check_id, check in checks:
        if check_id == selected_test_id:
            return [(check_id, check)]
    available = ", ".join(check_id for check_id, _ in checks)
    raise ValueError(f"unknown --test-id: {selected_test_id}. Available: {available}")


def _run_test_verification() -> None:
    test_verification(config_dict())


def _run_test_load_from_dict() -> None:
    test_load_from_dict(config_dict())


def _run_test_load_from_file() -> None:
    test_load_from_file(temp_config_file())


def _run_test_to_yaml_str_roundtrip() -> None:
    test_to_yaml_str_roundtrip(config_dict())


def _run_check(check) -> bool:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        check()
    output = buf.getvalue()
    if output:
        print(output, end="")
    return "❌ FAIL:" not in output


def _import_fluxon_config_without_package_init():
    repo_root = Path(__file__).resolve().parents[2]
    pkg_dir = repo_root / "fluxon_py"
    pkg_name = "fluxon_py"

    pkg = sys.modules.get(pkg_name)
    if pkg is None:
        pkg = types.ModuleType(pkg_name)
        pkg.__path__ = [str(pkg_dir)]
        sys.modules[pkg_name] = pkg

    spec = importlib.util.spec_from_file_location(f"{pkg_name}.config", pkg_dir / "config.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


FluxonKvClientConfig = _import_fluxon_config_without_package_init().FluxonKvClientConfig


def _import_fluxon_pyo3_tool_without_package_init():
    repo_root = Path(__file__).resolve().parents[2]
    pkg_dir = repo_root / "fluxon_py"
    pkg_name = "fluxon_py"

    pkg = sys.modules.get(pkg_name)
    if pkg is None:
        pkg = types.ModuleType(pkg_name)
        pkg.__path__ = [str(pkg_dir)]
        sys.modules[pkg_name] = pkg

    tool_pkg_name = f"{pkg_name}.tool"
    tool_pkg = sys.modules.get(tool_pkg_name)
    if tool_pkg is None:
        tool_pkg = types.ModuleType(tool_pkg_name)
        tool_pkg.__path__ = [str(pkg_dir / "tool")]
        sys.modules[tool_pkg_name] = tool_pkg

    spec = importlib.util.spec_from_file_location(f"{tool_pkg_name}.pyo3", pkg_dir / "tool" / "pyo3.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


_PYO3_TOOL = _import_fluxon_pyo3_tool_without_package_init()


def _owner_large_file_paths(tag: str) -> list[str]:
    return [f"/tmp/kvcache_large/{tag}"]


def _owner_fluxonkv_base_config(
    *,
    instance_key: str = "test_instance",
    cluster_name: str = "test_cluster",
    share_mem_path: str = "/tmp/kvcache_shared_memory/test",
    sub_cluster: str = "rack-a",
    tag: str = "test",
) -> dict:
    return {
        "instance_key": instance_key,
        "contribute_to_cluster_pool_size": {"dram": 16777216, "vram": {}},
        "fluxonkv_spec": {
            "etcd_addresses": ["localhost:2379"],
            "cluster_name": cluster_name,
            "share_mem_path": share_mem_path,
            "sub_cluster": sub_cluster,
            "large_file_paths": _owner_large_file_paths(tag),
        },
    }


def test_fluxonkv_sub_cluster_config():
    """Test fluxonkv_spec.sub_cluster is accepted and exposed."""
    try:
        config = FluxonKvClientConfig(
            _owner_fluxonkv_base_config(sub_cluster="producer_side", tag="sub_cluster")
        )
        assert config.fluxonkv_spec_sub_cluster == "producer_side"
        print("✅ PASS: test_fluxonkv_sub_cluster_config")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_sub_cluster_config - {e}")


def test_fluxon_pyo3_import_authority():
    """Ensure the PyO3 binding is imported only from the active venv authority."""
    try:
        active_venv = Path("/tmp/test_fluxon_active_venv").resolve()
        user_site = Path("/tmp/test_fluxon_user_site").resolve()
        foreign_module = user_site / "fluxon_pyo3" / "__init__.py"
        ok_module = active_venv / "lib" / "python3.12" / "site-packages" / "fluxon_pyo3" / "__init__.py"

        _PYO3_TOOL._verify_fluxon_pyo3_authority(
            module_origin=foreign_module,
            sys_prefix=Path("/usr").resolve(),
            sys_base_prefix=Path("/usr").resolve(),
            sys_executable="/usr/bin/python3",
            user_site=user_site,
        )

        try:
            _PYO3_TOOL._verify_fluxon_pyo3_authority(
                module_origin=foreign_module,
                sys_prefix=active_venv,
                sys_base_prefix=Path("/usr").resolve(),
                sys_executable=str(active_venv / "bin" / "python"),
                user_site=user_site,
            )
            print(
                "❌ FAIL: test_fluxon_pyo3_import_authority - user site-packages outside active venv should be rejected"
            )
            return
        except RuntimeError as exc:
            assert "authority mismatch" in str(exc)
            assert "user site-packages outside active venv" in str(exc)

        _PYO3_TOOL._verify_fluxon_pyo3_authority(
            module_origin=ok_module,
            sys_prefix=active_venv,
            sys_base_prefix=Path("/usr").resolve(),
            sys_executable=str(active_venv / "bin" / "python"),
            user_site=user_site,
        )

        with mock.patch.object(_PYO3_TOOL, "_resolve_fluxon_pyo3_module_origin", return_value=foreign_module):
            with mock.patch.object(site, "getusersitepackages", return_value=str(user_site)):
                with mock.patch.object(sys, "prefix", str(active_venv)):
                    with mock.patch.object(sys, "base_prefix", "/usr"):
                        try:
                            _PYO3_TOOL.import_fluxon_pyo3_local()
                            print(
                                "❌ FAIL: test_fluxon_pyo3_import_authority - import path authority mismatch should fail fast"
                            )
                            return
                        except RuntimeError as exc:
                            assert "authority mismatch" in str(exc)

        print("✅ PASS: test_fluxon_pyo3_import_authority")
    except Exception as e:
        print(f"❌ FAIL: test_fluxon_pyo3_import_authority - {e}")


def test_fluxonkv_owner_requires_sub_cluster():
    """Ensure owner mode requires a clean non-empty fluxonkv_spec.sub_cluster."""
    try:
        base = _owner_fluxonkv_base_config(tag="owner_requires_sub_cluster")
        del base["fluxonkv_spec"]["sub_cluster"]

        try:
            FluxonKvClientConfig(copy.deepcopy(base))
            print("❌ FAIL: test_fluxonkv_owner_requires_sub_cluster - missing sub_cluster should be rejected")
            return
        except ValueError:
            pass

        invalid_blank = copy.deepcopy(base)
        invalid_blank["fluxonkv_spec"]["sub_cluster"] = "   "
        try:
            FluxonKvClientConfig(invalid_blank)
            print("❌ FAIL: test_fluxonkv_owner_requires_sub_cluster - blank sub_cluster should be rejected")
            return
        except ValueError:
            pass

        invalid_spaced = copy.deepcopy(base)
        invalid_spaced["fluxonkv_spec"]["sub_cluster"] = " rack-a "
        try:
            FluxonKvClientConfig(invalid_spaced)
            print("❌ FAIL: test_fluxonkv_owner_requires_sub_cluster - spaced sub_cluster should be rejected")
            return
        except ValueError:
            pass

        valid = copy.deepcopy(base)
        valid["fluxonkv_spec"]["sub_cluster"] = "rack-a"
        FluxonKvClientConfig(valid)

        print("✅ PASS: test_fluxonkv_owner_requires_sub_cluster")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_owner_requires_sub_cluster - {e}")


def test_fluxonkv_owner_requires_large_file_paths():
    """Ensure owner mode requires explicit large_file_paths roots."""
    try:
        base = _owner_fluxonkv_base_config(tag="owner_requires_large_file_paths")
        del base["fluxonkv_spec"]["large_file_paths"]

        try:
            FluxonKvClientConfig(copy.deepcopy(base))
            print("❌ FAIL: test_fluxonkv_owner_requires_large_file_paths - missing large_file_paths should be rejected")
            return
        except ValueError:
            pass

        invalid_blank = copy.deepcopy(base)
        invalid_blank["fluxonkv_spec"]["large_file_paths"] = ["   "]
        try:
            FluxonKvClientConfig(invalid_blank)
            print("❌ FAIL: test_fluxonkv_owner_requires_large_file_paths - blank large_file_paths entry should be rejected")
            return
        except ValueError:
            pass

        valid = _owner_fluxonkv_base_config(tag="owner_requires_large_file_paths_valid")
        rendered = FluxonKvClientConfig(valid).to_fluxon_kv_client_config_yaml_str()
        assert "large_file_paths:" in rendered
        assert "- /tmp/kvcache_large/owner_requires_large_file_paths_valid" in rendered
        print("✅ PASS: test_fluxonkv_owner_requires_large_file_paths")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_owner_requires_large_file_paths - {e}")


def test_fluxonkv_external_forbids_large_file_paths():
    """Ensure zero-contribution external config cannot declare owner-only large_file_paths."""
    try:
        external = {
            "instance_key": "test_external",
            "contribute_to_cluster_pool_size": {"dram": 0, "vram": {}},
            "fluxonkv_spec": {
                "cluster_name": "test_cluster",
                "share_mem_path": "/tmp/kvcache_shared_memory/test",
                "large_file_paths": _owner_large_file_paths("external_forbidden"),
            },
        }
        try:
            FluxonKvClientConfig(external)
            print("❌ FAIL: test_fluxonkv_external_forbids_large_file_paths - external large_file_paths should be rejected")
            return
        except ValueError:
            pass
        print("✅ PASS: test_fluxonkv_external_forbids_large_file_paths")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_external_forbids_large_file_paths - {e}")


def test_fluxonkv_p2p_relay_removed():
    """Ensure removed fluxonkv_spec.p2p_relay is rejected as an unknown key."""
    try:
        base = _owner_fluxonkv_base_config(tag="p2p_relay_removed")

        _ = FluxonKvClientConfig(copy.deepcopy(base))

        invalid = copy.deepcopy(base)
        invalid["fluxonkv_spec"]["p2p_relay"] = "not_a_bool"
        try:
            FluxonKvClientConfig(invalid)
            print("❌ FAIL: test_fluxonkv_p2p_relay_removed - removed key should be rejected")
            return
        except ValueError:
            pass

        print("✅ PASS: test_fluxonkv_p2p_relay_removed")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_p2p_relay_removed - {e}")


def test_fluxon_client_config_yaml_shape():
    """Test YAML shape required by Rust ClientConfigYaml."""
    try:
        base = _owner_fluxonkv_base_config(tag="yaml_shape")
        config = FluxonKvClientConfig(copy.deepcopy(base))
        yaml_text = config.to_fluxon_kv_client_config_yaml_str()
        loaded = yaml.safe_load(yaml_text)
        assert loaded["fluxonkv_spec"]["share_mem_path"] == base["fluxonkv_spec"]["share_mem_path"]
        assert loaded["fluxonkv_spec"]["sub_cluster"] == base["fluxonkv_spec"]["sub_cluster"]
        assert "share_mem_path" not in loaded
        assert "rdma_device_names" not in loaded
        assert "transfer_engine" not in loaded["fluxonkv_spec"]
        print("✅ PASS: test_fluxon_client_config_yaml_shape")
    except Exception as e:
        print(f"❌ FAIL: test_fluxon_client_config_yaml_shape - {e}")


def test_fluxonkv_protocol_field():
    """Ensure Rust-generated side-worker YAML protocol blocks survive Python validation."""
    try:
        cfg = {
            "instance_key": "test_instance__side_0",
            "protocol": {
                "protocol_type": "tcp",
            },
            "fluxonkv_spec": {
                "cluster_name": "test_cluster",
                "share_mem_path": "/tmp/kvcache_shared_memory/test_side_worker",
            },
            "test_spec_config": {
                "enable_side_transfer": True,
                "side_transfer_role": "worker",
                "side_transfer_worker_count": 0,
            },
        }
        config = FluxonKvClientConfig(copy.deepcopy(cfg))
        loaded = yaml.safe_load(config.to_fluxon_kv_client_config_yaml_str())
        assert loaded["protocol"]["protocol_type"] == "tcp"
        assert config.protocol_type == "tcp"
        assert config.protocol_rdma_device_names is None
        assert config.fluxonkv_spec_transfer_engine == "p2p"
        print("✅ PASS: test_fluxonkv_protocol_field")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_protocol_field - {e}")


def test_fluxonkv_runtime_defaults_are_internal():
    """Ensure Fluxon KV runtime defaults stay internal and are not serialized into YAML."""
    try:
        base = _owner_fluxonkv_base_config(tag="runtime_defaults")
        config = FluxonKvClientConfig(copy.deepcopy(base))
        assert config.fluxonkv_spec_transfer_engine == "closed"
        assert config.protocol_rdma_device_names is None
        loaded = yaml.safe_load(config.to_fluxon_kv_client_config_yaml_str())
        assert "transfer_engine" not in loaded["fluxonkv_spec"]
        assert "rdma_device_names" not in loaded
        assert "test_spec_config" not in loaded
        print("✅ PASS: test_fluxonkv_runtime_defaults_are_internal")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_runtime_defaults_are_internal - {e}")


def test_fluxonkv_removed_rdma_config_keys():
    """Ensure removed Fluxon KV RDMA config keys are rejected."""
    try:
        base = _owner_fluxonkv_base_config(tag="removed_rdma_keys")

        invalid_rdma = copy.deepcopy(base)
        invalid_rdma["rdma_device_names"] = "mlx5_0:1"
        try:
            FluxonKvClientConfig(invalid_rdma)
            print("❌ FAIL: test_fluxonkv_removed_rdma_config_keys - rdma_device_names should be rejected")
            return
        except ValueError:
            pass

        invalid_engine = copy.deepcopy(base)
        invalid_engine["fluxonkv_spec"]["transfer_engine"] = "closed"
        try:
            FluxonKvClientConfig(invalid_engine)
            print("❌ FAIL: test_fluxonkv_removed_rdma_config_keys - transfer_engine should be rejected")
            return
        except ValueError:
            pass

        print("✅ PASS: test_fluxonkv_removed_rdma_config_keys")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_removed_rdma_config_keys - {e}")


def test_fluxonkv_test_spec_config():
    """Ensure test_spec_config is accepted, normalized, and serialized."""
    try:
        base = _owner_fluxonkv_base_config(tag="test_spec_config")
        base["test_spec_config"] = {
            "disable_observability": True,
            "enable_iceoryx_logs": True,
            "transport_mode": "transfer_only",
        }

        try:
            FluxonKvClientConfig(copy.deepcopy(base))
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - transport_mode without rdma_device_names should be rejected"
            )
            return
        except ValueError:
            pass

        rdma_devices = copy.deepcopy(base)
        rdma_devices["test_spec_config"]["transport_mode"] = "transfer_with_rpc"
        rdma_devices["test_spec_config"]["rdma_device_names"] = [" mlx5_4 ", "mlx5_0", "mlx5_4"]
        rdma_devices["test_spec_config"]["disable_local_ipc"] = True
        rdma_devices["test_spec_config"]["disable_crossowner_ipc"] = True
        rdma_devices["test_spec_config"]["iceoryx_external_busy_poll"] = True
        rdma_devices["test_spec_config"]["iceoryx_owner_client_busy_poll"] = True
        rdma_devices["test_spec_config"]["tcp_thread_reactor_shard_count"] = 2
        rdma_devices["test_spec_config"]["tcp_thread_bulk_lane_count"] = 4
        rdma_devices["test_spec_config"]["tcp_thread_control_lane_count"] = 3
        rdma_devices["test_spec_config"][
            "require_transfer_rpc_fast_path_ready_timeout_seconds"
        ] = 45
        config = FluxonKvClientConfig(rdma_devices)
        loaded = yaml.safe_load(config.to_fluxon_kv_client_config_yaml_str())
        assert loaded["test_spec_config"]["disable_local_ipc"] is True
        assert loaded["test_spec_config"]["disable_crossowner_ipc"] is True
        assert loaded["test_spec_config"]["enable_iceoryx_logs"] is True
        assert loaded["test_spec_config"]["iceoryx_external_busy_poll"] is True
        assert loaded["test_spec_config"]["iceoryx_owner_client_busy_poll"] is True
        assert loaded["test_spec_config"]["rdma_device_names"] == ["mlx5_0", "mlx5_4"]
        assert loaded["test_spec_config"]["tcp_thread_reactor_shard_count"] == 2
        assert loaded["test_spec_config"]["tcp_thread_bulk_lane_count"] == 4
        assert loaded["test_spec_config"]["tcp_thread_control_lane_count"] == 3
        assert (
            loaded["test_spec_config"]["require_transfer_rpc_fast_path_ready_timeout_seconds"]
            == 45
        )
        assert config.protocol_rdma_device_names == "mlx5_0,mlx5_4"

        implicit_transport = copy.deepcopy(base)
        implicit_transport["test_spec_config"] = {
            "disable_observability": True,
            "enable_iceoryx_logs": True,
        }
        config = FluxonKvClientConfig(implicit_transport)
        loaded = yaml.safe_load(config.to_fluxon_kv_client_config_yaml_str())
        assert loaded["test_spec_config"]["disable_observability"] is True
        assert loaded["test_spec_config"]["enable_iceoryx_logs"] is True
        assert "transport_mode" not in loaded["test_spec_config"]

        closed_backend = copy.deepcopy(base)
        closed_backend["test_spec_config"]["transport_mode"] = "transfer_only"
        closed_backend["test_spec_config"]["rdma_device_names"] = ["mlx5_0"]
        config = FluxonKvClientConfig(closed_backend)
        loaded = yaml.safe_load(config.to_fluxon_kv_client_config_yaml_str())
        assert "legacy_transfer_backend" not in loaded["test_spec_config"]
        assert config.fluxonkv_spec_transfer_engine == "closed"

        invalid = copy.deepcopy(base)
        invalid["test_spec_config"]["transport_mode"] = "rdma_control"
        try:
            FluxonKvClientConfig(invalid)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - invalid transport_mode should be rejected"
            )
            return
        except ValueError:
            pass

        implicit_rdma = copy.deepcopy(base)
        del implicit_rdma["test_spec_config"]["transport_mode"]
        implicit_rdma["test_spec_config"]["rdma_device_names"] = ["mlx5_0"]
        config = FluxonKvClientConfig(implicit_rdma)
        loaded = yaml.safe_load(config.to_fluxon_kv_client_config_yaml_str())
        assert loaded["test_spec_config"]["rdma_device_names"] == ["mlx5_0"]
        assert "transport_mode" not in loaded["test_spec_config"]

        invalid_backend = copy.deepcopy(base)
        invalid_backend["test_spec_config"]["legacy_transfer_backend"] = "closed"
        try:
            FluxonKvClientConfig(invalid_backend)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - unknown legacy transfer field should be rejected"
            )
            return
        except ValueError:
            pass

        invalid_fast_path_timeout = copy.deepcopy(base)
        invalid_fast_path_timeout["test_spec_config"]["transport_mode"] = "transfer_with_rpc"
        invalid_fast_path_timeout["test_spec_config"][
            "require_transfer_rpc_fast_path_ready_timeout_seconds"
        ] = 30
        try:
            FluxonKvClientConfig(invalid_fast_path_timeout)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - transfer-rpc fast-path ready timeout should require explicit rdma_device_names"
            )
            return
        except ValueError:
            pass

        invalid_fast_path_timeout_transport = copy.deepcopy(base)
        invalid_fast_path_timeout_transport["test_spec_config"]["transport_mode"] = "transfer_only"
        invalid_fast_path_timeout_transport["test_spec_config"]["rdma_device_names"] = ["mlx5_0"]
        invalid_fast_path_timeout_transport["test_spec_config"][
            "require_transfer_rpc_fast_path_ready_timeout_seconds"
        ] = 30
        try:
            FluxonKvClientConfig(invalid_fast_path_timeout_transport)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - transfer-rpc fast-path ready timeout should require transfer_with_rpc"
            )
            return
        except ValueError:
            pass

        invalid_tcp_thread = copy.deepcopy(base)
        invalid_tcp_thread["test_spec_config"]["tcp_thread_control_lane_count"] = 0
        try:
            FluxonKvClientConfig(invalid_tcp_thread)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - invalid tcp_thread_control_lane_count should be rejected"
            )
            return
        except ValueError:
            pass

        invalid_backend_with_legacy_runtime = copy.deepcopy(base)
        invalid_backend_with_legacy_runtime["test_spec_config"]["transport_mode"] = "transfer_with_rpc"
        invalid_backend_with_legacy_runtime["test_spec_config"]["rdma_device_names"] = ["mlx5_0"]
        invalid_backend_with_legacy_runtime["test_spec_config"]["legacy_transfer_backend"] = "closed"
        invalid_backend_with_legacy_runtime["test_spec_config"]["legacy_transfer_runtime"] = "threaded"
        try:
            FluxonKvClientConfig(invalid_backend_with_legacy_runtime)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - unknown legacy transfer fields should be rejected"
            )
            return
        except ValueError:
            pass

        invalid_legacy_runtime = copy.deepcopy(base)
        invalid_legacy_runtime["test_spec_config"]["legacy_transfer_runtime"] = "polling"
        try:
            FluxonKvClientConfig(invalid_legacy_runtime)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - unknown legacy runtime field should be rejected"
            )
            return
        except ValueError:
            pass

        legacy_field = copy.deepcopy(base)
        del legacy_field["test_spec_config"]
        legacy_field["benchmark_fast_path"] = {"disable_observability": True}
        try:
            FluxonKvClientConfig(legacy_field)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - legacy benchmark_fast_path should be rejected"
            )
            return
        except ValueError:
            pass

        legacy_runtime = copy.deepcopy(base)
        del legacy_runtime["test_spec_config"]
        legacy_runtime["test_config"] = {"rdma": {"transfer_backend_activation_mode": "tcp_test_bypass_rdma_control"}}
        try:
            FluxonKvClientConfig(legacy_runtime)
            print(
                "❌ FAIL: test_fluxonkv_test_spec_config - legacy test_config should be rejected"
            )
            return
        except ValueError:
            pass

        print("✅ PASS: test_fluxonkv_test_spec_config")
    except Exception as e:
        print(f"❌ FAIL: test_fluxonkv_test_spec_config - {e}")


def config_dict():
    return {
        "instance_key": "test_instance",
        "contribute_to_cluster_pool_size": {
            "dram": 16777216,
            "vram": {},
        },
        "mooncake_spec": {
            "local_buffer_size": 16777216,
            "metadata_server": "http://localhost:8080/metadata",
            "master_server_address": "localhost:8081",
            "etcd_addresses": ["localhost:2379"],
        },
    }


def temp_config_file():
    """Create a temporary config file for testing."""
    test_dir = os.path.dirname(__file__)
    config_file_path = os.path.join(test_dir, "temp_config_for_test.yaml")
    config_data = {
        "instance_key": "test_instance",
        "contribute_to_cluster_pool_size": {
            "dram": 16777216,
            "vram": {},
        },
        "mooncake_spec": {
            "local_buffer_size": 16777216,
            "metadata_server": "http://localhost:8080/metadata",
            "master_server_address": "localhost:8081",
            "etcd_addresses": ["localhost:2379"],
        },
    }
    with open(config_file_path, "w") as f:
        yaml.dump(config_data, f)
    return config_file_path


def test_load_from_dict(cfg):
    """Test loading configuration from a dictionary."""
    try:
        config = FluxonKvClientConfig(cfg)
        assert config.instance_key == "test_instance"
        assert config.contribute_to_cluster_pool_size["dram"] == 16777216
        assert config.contribute_to_cluster_pool_size["vram"] == {}
        assert config.protocol_type == "rdma"
        assert config.mooncake_spec_local_buffer_size == 16777216
        assert config.mooncake_spec_metadata_server == "http://localhost:8080/metadata"
        assert config.mooncake_spec_master_server_address == "localhost:8081"
        assert config.mooncake_spec_etcd_addresses == ["localhost:2379"]
        print("✅ PASS: test_load_from_dict")
    except Exception as e:
        print(f"❌ FAIL: test_load_from_dict - {e}")


def test_load_from_file(config_file_path):
    """Test loading from a YAML file."""
    try:
        config = FluxonKvClientConfig.from_file(config_file_path)
        assert config.instance_key == "test_instance"
        assert config.protocol_type == "rdma"
        assert config.mooncake_spec_local_buffer_size == 16777216
        print("✅ PASS: test_load_from_file")
    except Exception as e:
        print(f"❌ FAIL: test_load_from_file - {e}")


def test_to_yaml_str_roundtrip(cfg: dict):
    """Test converting config dict -> YAML string -> dict."""
    try:
        config = FluxonKvClientConfig(cfg)
        yaml_text = config.to_yaml_str()
        loaded = yaml.safe_load(yaml_text)
        assert loaded == config.to_dict()
        print("✅ PASS: test_to_yaml_str_roundtrip")
    except Exception as e:
        print(f"❌ FAIL: test_to_yaml_str_roundtrip - {e}")


def test_verification(cfg):
    """Test configuration verification for invalid values."""
    try:
        invalid_config = copy.deepcopy(cfg)
        invalid_config["contribute_to_cluster_pool_size"]["dram"] = 16777217
        FluxonKvClientConfig(invalid_config)
        print("❌ FAIL: Invalid dram size should be rejected")
    except ValueError:
        print("✅ PASS: Invalid dram size correctly rejected")

    try:
        invalid_config = copy.deepcopy(cfg)
        invalid_config["mooncake_spec"]["master_server_address"] = "http://localhost:8081"
        FluxonKvClientConfig(invalid_config)
        print("❌ FAIL: Invalid address format should be rejected")
    except ValueError:
        print("✅ PASS: Invalid address format correctly rejected")

    try:
        invalid_config = copy.deepcopy(cfg)
        invalid_config["mooncake_spec"]["metadata_server"] = "localhost:8080/metadata"
        FluxonKvClientConfig(invalid_config)
        print("❌ FAIL: Invalid metadata server format should be rejected")
    except ValueError:
        print("✅ PASS: Invalid metadata server format correctly rejected")

    try:
        incomplete_config = {"instance_key": "test"}
        FluxonKvClientConfig(incomplete_config)
        print("❌ FAIL: Incomplete config should be rejected")
    except ValueError:
        print("✅ PASS: Incomplete config correctly rejected")

    try:
        unknown_field_config = copy.deepcopy(cfg)
        unknown_field_config["unknown_field"] = "should_be_rejected"
        FluxonKvClientConfig(unknown_field_config)
        print("❌ FAIL: Unknown field should be rejected")
    except ValueError:
        print("✅ PASS: Unknown field correctly rejected")

    try:
        empty_vram_config = copy.deepcopy(cfg)
        empty_vram_config["contribute_to_cluster_pool_size"]["vram"] = {}
        FluxonKvClientConfig(empty_vram_config)
        print("✅ PASS: Empty vram dict is allowed (any_not_none constraint)")
    except ValueError as e:
        print(f"❌ FAIL: Empty vram dict should be allowed - {e}")

    try:
        missing_vram_config = copy.deepcopy(cfg)
        del missing_vram_config["contribute_to_cluster_pool_size"]["vram"]
        FluxonKvClientConfig(missing_vram_config)
        print("✅ PASS: Missing vram key is allowed (any_not_none constraint)")
    except ValueError as e:
        print(f"❌ FAIL: Missing vram key should be allowed - {e}")

    try:
        all_empty_config = copy.deepcopy(cfg)
        all_empty_config["contribute_to_cluster_pool_size"]["dram"] = None
        all_empty_config["contribute_to_cluster_pool_size"]["vram"] = {}
        FluxonKvClientConfig(all_empty_config)
        print("❌ FAIL: All empty values should be rejected")
    except ValueError:
        print("✅ PASS: All empty values correctly rejected")

    try:
        valid_vram_config = copy.deepcopy(cfg)
        valid_vram_config["contribute_to_cluster_pool_size"]["vram"] = {"0": 16777216}
        FluxonKvClientConfig(valid_vram_config)
        print("✅ PASS: Valid vram values accepted")
    except ValueError as e:
        print(f"❌ FAIL: Valid vram values should be accepted - {e}")

    try:
        valid_metadata_config = copy.deepcopy(cfg)
        valid_metadata_config["mooncake_spec"]["metadata_server"] = "http://localhost:8080/metadata"
        FluxonKvClientConfig(valid_metadata_config)
        print("✅ PASS: Valid metadata server format accepted")
    except ValueError as e:
        print(f"❌ FAIL: Valid metadata server format should be accepted - {e}")

    try:
        invalid_metadata_config = copy.deepcopy(cfg)
        invalid_metadata_config["mooncake_spec"]["metadata_server"] = "localhost:8080/metadata"
        FluxonKvClientConfig(invalid_metadata_config)
        print("❌ FAIL: Invalid metadata server format should be rejected")
    except ValueError:
        print("✅ PASS: Invalid metadata server format correctly rejected")

    try:
        valid_gpu_config = copy.deepcopy(cfg)
        valid_gpu_config["contribute_to_cluster_pool_size"]["vram"] = {"0": 16777216, "1": 33554432}
        FluxonKvClientConfig(valid_gpu_config)
        print("✅ PASS: Valid GPU configuration accepted")
    except ValueError as e:
        print(f"❌ FAIL: Valid GPU configuration should be accepted - {e}")

    try:
        invalid_gpu_config = copy.deepcopy(cfg)
        invalid_gpu_config["contribute_to_cluster_pool_size"]["vram"] = {"0": 16777217}
        FluxonKvClientConfig(invalid_gpu_config)
        print("❌ FAIL: Invalid GPU configuration should be rejected")
    except ValueError:
        print("✅ PASS: Invalid GPU configuration correctly rejected")


if __name__ == "__main__":
    sys.exit(main())
