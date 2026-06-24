"""
Configuration management for the KV Cache API layer.

This module handles reading configuration from YAML files only.
"""

from typing import Dict, Any, List, Union, Tuple
from abc import abstractmethod
from collections.abc import Mapping
from enum import Enum
import sys
import yaml
import re
from typing import Optional, Callable, Literal
import copy
from .logging import init_logger

logging = init_logger(__name__)

DEBUG_MODE = False
def debug_print(*args):
    if DEBUG_MODE:
        print(*args)

_YAML_KEY_TYPES = (str, int, float, bool)
_YAML_SCALAR_TYPES = (str, int, float, bool, type(None))


def _to_plain_yaml_obj(value: Any, path: str) -> Any:
    """
    Normalize config containers into plain Python types accepted by PyYAML.

    This avoids serialization failures when callers pass Mapping/Sequence implementations
    (e.g. OmegaConf DictConfig) or Enum values (including str-based Enums).
    """
    if isinstance(value, Enum):
        return _to_plain_yaml_obj(value.value, path)

    if isinstance(value, _YAML_SCALAR_TYPES):
        return value

    if isinstance(value, Mapping):
        out: Dict[Any, Any] = {}
        for k, v in value.items():
            key = _to_plain_yaml_obj(k, f"{path}.<key>")
            if not isinstance(key, _YAML_KEY_TYPES):
                raise TypeError(
                    f"{path}: YAML mapping keys must be scalar (str|int|float|bool), got {type(key).__name__}"
                )
            out[key] = _to_plain_yaml_obj(v, f"{path}.{key}")
        return out

    if isinstance(value, (list, tuple)):
        return [_to_plain_yaml_obj(v, f"{path}[{i}]") for i, v in enumerate(value)]

    if isinstance(value, (set, frozenset)):
        raise TypeError(f"{path}: sets are not supported in YAML config; use a list instead")

    raise TypeError(f"{path}: unsupported value type for YAML config: {type(value).__name__}")

# YAML config template
def _yaml_template():
    return """
instance_key: xxx                      # Unique distributed instance id (str)
protocol:                              # Transport protocol override (dict(optional))
  protocol_type:                       # Protocol type (('tcp'|'rdma'))
  rdma_device_names:                   # Explicit RDMA devices for protocol config (['{str}'](optional))
pprof_duration_seconds:                # Dump pprof flamegraph after N seconds (int(optional))
contribute_to_cluster_pool_size:       # Capacity contributed to cluster pool (dict(optional))
  dram: 1677721600                     # - DRAM contribution (int(multiple of 16777216))
  vram:                                # - VRAM contribution per GPU (dict(dynamic_key))
    '{gpu_id}': 1677721600             # - Capacity for a given GPU id (int(multiple of 16777216))
test_spec_config:                      # Test-only config overrides (dict(optional))
  disable_observability: false         # Disable observe / OTLP background tasks (bool(optional))
  disable_master_replica_cache: false  # Disable master replica cache maintenance (bool(optional))
  disable_prefix_index: false          # Disable master prefix index maintenance (bool(optional))
  disable_local_ipc: false             # Disable all same-machine local IPC for test runs so peers use direct transport instead (bool(optional))
  disable_crossowner_ipc: false        # Keep same-owner local IPC but force same-host cross-owner peers onto direct transport (bool(optional))
  enable_iceoryx_logs: false           # Lift Fluxon's default third-party log suppression for iceoryx2 crates (bool(optional))
  iceoryx_external_busy_poll: false    # Force external local IPC receiver onto busy-poll instead of WaitSet for test diagnosis (bool(optional))
  iceoryx_owner_client_busy_poll: true # Keep owner/client local IPC receiver on busy-poll unless explicitly turned to WaitSet for test diagnosis (bool(optional))
  prefer_local_placement: false        # Prefer placing new KV writes on the requester-local owner when possible (bool(optional))
  short_circuit_put_payload_path: false # Keep large put_start allocation but skip payload memcpy + transfer (bool(optional))
  skip_put_end_commit: false           # Return success after payload transfer without put_done commit; inflight_put TTL cleanup only (bool(optional))
  transport_mode:                      # transfer_only|transfer_with_rpc (str(optional))
  tcp_thread_reactor_shard_count:      # tcp_thread reactor shard count, 1..16 (int(optional))
  tcp_thread_bulk_lane_count:          # tcp_thread bulk lane count, 1..8 (int(optional))
  tcp_thread_control_lane_count:       # tcp_thread control lane count, 1..8 (int(optional))
  user_rpc_sync_handler_thread_count:  # Owner-dedicated sync user-RPC worker thread count, >0 (int(optional))
  require_transfer_rpc_fast_path_ready_timeout_seconds: # Require owner-owner transfer-rpc fast path before owner ready/shared.json publication (int(optional))
  rdma_device_names:                   # Explicit RDMA devices for benchmark/test fast-path fanout (['{str}'](optional))
  enable_side_transfer: false          # Enable TCP side-transfer fast-path (bool(optional))
  side_transfer_worker_count: 0        # Owner-side worker count for side-transfer fanout (int(optional))
  side_transfer_worker_p2p_port_base:  # Optional owner-side worker port base (int(optional))
  side_transfer_role:                  # worker (str(optional))
# Notes:
# - Zero-contribution mode is selected when contribute_to_cluster_pool_size is missing,
#   or when dram is 0 and all VRAM entries are 0.
# - In zero-contribution mode, the backend bootstrap derives routing info from owner shared.json.

# specific part, only one of [mooncake_spec,fluxonkv_spec] is required
mooncake_spec:                         # mooncake 特定配置 (dict(optional))
  local_buffer_size:                   # 本地缓冲区大小 (int(multiple of 16777216))
  metadata_server:                     # 元数据服务器地址 ('{str}://{str}:{int}/metadata')
  master_server_address:               # 主服务器地址 ('{str}:{int}')
  etcd_addresses:                      # etcd地址列表, 注意是列表!!! (['{str}:{int}'])
  
fluxonkv_spec:                        # fluxon kv specific config (dict(optional))
  etcd_addresses:                     # Etcd address list ((None|['{str}:{int}']))
  cluster_name:                       # Cluster name (str)
  share_mem_path:                     # Shared bundle path for mmap.file/shared.json/peer metadata (str)
  large_file_paths:                   # Owner-mode ordered large-file roots (['{str}'](optional))
  p2p_listen_port:                    # P2P QUIC listen port override (int(optional))
  redis_compat:                       # Enable Redis protocol shim (dict(optional))
    listen_addr:                      # TCP listen addr, e.g. "127.0.0.1:16379" (str)
  sub_cluster:                        # KV node sub-cluster label (None|str)
"""


def _normalize_test_spec_config(raw: Any, ctx: str) -> Dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{ctx} must be a mapping")

    allowed_keys = {
        "disable_observability",
        "disable_master_replica_cache",
        "disable_prefix_index",
        "disable_local_ipc",
        "disable_crossowner_ipc",
        "enable_iceoryx_logs",
        "iceoryx_external_busy_poll",
        "iceoryx_owner_client_busy_poll",
        "prefer_local_placement",
        "short_circuit_put_payload_path",
        "skip_put_end_commit",
        "transport_mode",
        "tcp_thread_reactor_shard_count",
        "tcp_thread_bulk_lane_count",
        "tcp_thread_control_lane_count",
        "user_rpc_sync_handler_thread_count",
        "require_transfer_rpc_fast_path_ready_timeout_seconds",
        "rdma_device_names",
        "enable_side_transfer",
        "side_transfer_worker_count",
        "side_transfer_worker_p2p_port_base",
        "side_transfer_role",
    }
    unknown = sorted(set(raw.keys()) - allowed_keys)
    if unknown:
        raise ValueError(f"{ctx} contains unknown keys: {unknown}")

    out: Dict[str, Any] = {}
    for key in (
        "disable_observability",
        "disable_master_replica_cache",
        "disable_prefix_index",
        "disable_local_ipc",
        "disable_crossowner_ipc",
        "enable_iceoryx_logs",
        "iceoryx_external_busy_poll",
        "iceoryx_owner_client_busy_poll",
        "prefer_local_placement",
        "short_circuit_put_payload_path",
        "skip_put_end_commit",
        "enable_side_transfer",
    ):
        value = raw.get(key)
        if value is not None:
            if not isinstance(value, bool):
                raise ValueError(f"{ctx}.{key} must be a bool")
            out[key] = value

    transport_mode = raw.get("transport_mode")
    transport_mode_was_explicit = transport_mode is not None
    side_transfer_role_raw = raw.get("side_transfer_role")
    default_transport_mode = None if side_transfer_role_raw == "worker" else "transfer_with_rpc"
    if transport_mode is not None:
        if not isinstance(transport_mode, str):
            raise ValueError(f"{ctx}.transport_mode must be a string")
        allowed_transport_modes = {"transfer_only", "transfer_with_rpc"}
        if transport_mode not in allowed_transport_modes:
            raise ValueError(
                f"{ctx}.transport_mode must be one of {sorted(allowed_transport_modes)}, got {transport_mode!r}"
            )
        out["transport_mode"] = transport_mode

    rdma_device_names = raw.get("rdma_device_names")
    if rdma_device_names is not None:
        if not isinstance(rdma_device_names, list):
            raise ValueError(f"{ctx}.rdma_device_names must be a list of strings")
        normalized = []
        seen = set()
        for idx, value in enumerate(rdma_device_names):
            if not isinstance(value, str):
                raise ValueError(f"{ctx}.rdma_device_names[{idx}] must be a string")
            trimmed = value.strip()
            if not trimmed:
                raise ValueError(f"{ctx}.rdma_device_names[{idx}] must be a non-empty string")
            if trimmed in seen:
                continue
            seen.add(trimmed)
            normalized.append(trimmed)
        out["rdma_device_names"] = sorted(normalized)
    require_transfer_rpc_fast_path_ready_timeout_seconds = raw.get(
        "require_transfer_rpc_fast_path_ready_timeout_seconds"
    )
    if require_transfer_rpc_fast_path_ready_timeout_seconds is not None:
        if isinstance(require_transfer_rpc_fast_path_ready_timeout_seconds, bool) or not isinstance(
            require_transfer_rpc_fast_path_ready_timeout_seconds, int
        ):
            raise ValueError(
                f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds must be an int"
            )
        if require_transfer_rpc_fast_path_ready_timeout_seconds <= 0:
            raise ValueError(
                f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds must be > 0"
            )
        effective_transport_mode = out.get("transport_mode", default_transport_mode)
        if effective_transport_mode != "transfer_with_rpc":
            raise ValueError(
                f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds requires {ctx}.transport_mode=transfer_with_rpc"
            )
        if "rdma_device_names" not in out:
            raise ValueError(
                f"{ctx}.require_transfer_rpc_fast_path_ready_timeout_seconds requires explicit {ctx}.rdma_device_names"
            )
        out["require_transfer_rpc_fast_path_ready_timeout_seconds"] = (
            require_transfer_rpc_fast_path_ready_timeout_seconds
        )

    for key, min_v, max_v in (
        ("tcp_thread_reactor_shard_count", 1, 16),
        ("tcp_thread_bulk_lane_count", 1, 8),
        ("tcp_thread_control_lane_count", 1, 8),
    ):
        value = raw.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{ctx}.{key} must be an int")
        if value < min_v or value > max_v:
            raise ValueError(f"{ctx}.{key} must be in [{min_v}, {max_v}]")
        out[key] = value

    user_rpc_sync_handler_thread_count = raw.get("user_rpc_sync_handler_thread_count")
    if user_rpc_sync_handler_thread_count is not None:
        if isinstance(user_rpc_sync_handler_thread_count, bool) or not isinstance(
            user_rpc_sync_handler_thread_count, int
        ):
            raise ValueError(f"{ctx}.user_rpc_sync_handler_thread_count must be an int")
        if user_rpc_sync_handler_thread_count <= 0:
            raise ValueError(f"{ctx}.user_rpc_sync_handler_thread_count must be > 0")
        out["user_rpc_sync_handler_thread_count"] = user_rpc_sync_handler_thread_count

    side_transfer_worker_count = raw.get("side_transfer_worker_count")
    if side_transfer_worker_count is not None:
        if isinstance(side_transfer_worker_count, bool) or not isinstance(side_transfer_worker_count, int):
            raise ValueError(f"{ctx}.side_transfer_worker_count must be an int")
        if side_transfer_worker_count < 0:
            raise ValueError(f"{ctx}.side_transfer_worker_count must be >= 0")
        out["side_transfer_worker_count"] = side_transfer_worker_count

    side_transfer_worker_p2p_port_base = raw.get("side_transfer_worker_p2p_port_base")
    if side_transfer_worker_p2p_port_base is not None:
        if isinstance(side_transfer_worker_p2p_port_base, bool) or not isinstance(
            side_transfer_worker_p2p_port_base, int
        ):
            raise ValueError(f"{ctx}.side_transfer_worker_p2p_port_base must be an int")
        if side_transfer_worker_p2p_port_base < 0:
            raise ValueError(f"{ctx}.side_transfer_worker_p2p_port_base must be >= 0")
        out["side_transfer_worker_p2p_port_base"] = side_transfer_worker_p2p_port_base

    side_transfer_role = raw.get("side_transfer_role")
    if side_transfer_role is not None:
        if not isinstance(side_transfer_role, str):
            raise ValueError(f"{ctx}.side_transfer_role must be a string")
        allowed_side_transfer_roles = {"worker"}
        if side_transfer_role not in allowed_side_transfer_roles:
            raise ValueError(
                f"{ctx}.side_transfer_role must be one of {sorted(allowed_side_transfer_roles)}, got {side_transfer_role!r}"
            )
        out["side_transfer_role"] = side_transfer_role

    if out.get("side_transfer_role") == "worker":
        if "rdma_device_names" in out and not transport_mode_was_explicit:
            raise ValueError(f"{ctx}.rdma_device_names requires {ctx}.transport_mode")

    if transport_mode_was_explicit and "rdma_device_names" not in out:
        raise ValueError(
            f"explicit {ctx}.transport_mode now requires {ctx}.rdma_device_names to avoid implicit RDMA device selection"
        )

    return out


def _is_zero_contribution_fluxonkv_config(cfg: Dict[str, Any]) -> bool:
    """
    Determine whether one Fluxon KV config uses zero-contribution mode.

    Contract:
    - Missing contribute_to_cluster_pool_size means zero-contribution.
    - Explicit dram=0 with all vram entries=0 also means zero-contribution.
    - Partial-zero configurations are rejected to keep the role contract explicit.
    """
    contrib_present = "contribute_to_cluster_pool_size" in cfg
    contrib = cfg.get("contribute_to_cluster_pool_size")
    if not contrib_present or contrib is None:
        return True
    if not isinstance(contrib, dict):
        raise ValueError("contribute_to_cluster_pool_size must be a mapping when provided")

    dram = int(contrib["dram"])
    vram_raw = contrib.get("vram")
    # Missing vram is normalized to "no GPU contribution".
    if vram_raw is None:
        vram: Dict[str, Any] = {}
    elif not isinstance(vram_raw, dict):
        raise ValueError("contribute_to_cluster_pool_size.vram must be a mapping")
    else:
        vram = vram_raw

    vram_is_zero = True
    for _, value in vram.items():
        if int(value) != 0:
            vram_is_zero = False
            break
    if dram == 0 and not vram_is_zero:
        raise ValueError(
            "contribute_to_cluster_pool_size is partially zero: dram=0 but vram has non-zero values"
        )
    return dram == 0 and vram_is_zero


def _validate_fluxonkv_contract(cfg: Dict[str, Any]) -> None:
    """
    Validate the shared Fluxon KV contract and then apply role-specific checks.

    The contract must stay canonical across Python construction, YAML export, and
    the Rust bridge, so owner/external differences live inside this one path.
    """
    if "fluxonkv_spec" not in cfg:
        return

    spec = cfg.get("fluxonkv_spec")
    if not isinstance(spec, dict):
        raise ValueError("fluxonkv_spec must be a mapping")

    is_zero_contribution = _is_zero_contribution_fluxonkv_config(cfg)

    share_mem_path = spec.get("share_mem_path")
    if not isinstance(share_mem_path, str) or not share_mem_path.strip():
        raise ValueError("fluxonkv_spec.share_mem_path must be a non-empty string")

    if "rdma_device_names" in cfg:
        raise ValueError("rdma_device_names has been removed from Fluxon KV config")

    if "transfer_engine" in spec:
        raise ValueError("fluxonkv_spec.transfer_engine has been removed from Fluxon KV config")

    if is_zero_contribution:
        forbidden_spec_keys = [
            "etcd_addresses",
            "redis_compat",
            "sub_cluster",
            "large_file_paths",
        ]
        for key in forbidden_spec_keys:
            if key in spec:
                raise ValueError(f"fluxonkv_spec.{key} is forbidden in zero-contribution mode")
        return

    contrib = cfg.get("contribute_to_cluster_pool_size")
    if not isinstance(contrib, dict):
        raise ValueError(
            "contribute_to_cluster_pool_size is required for owner mode (non-zero contribution)"
        )
    if int(contrib["dram"]) == 0:
        raise ValueError("owner mode requires non-zero contribute_to_cluster_pool_size.dram")

    if "etcd_addresses" not in spec:
        raise ValueError("fluxonkv_spec.etcd_addresses is required for owner mode")
    etcd_addresses = spec.get("etcd_addresses")
    if not isinstance(etcd_addresses, list) or len(etcd_addresses) == 0:
        raise ValueError("fluxonkv_spec.etcd_addresses must be a non-empty list")

    if "sub_cluster" not in spec:
        raise ValueError("fluxonkv_spec.sub_cluster is required for owner mode")
    sub_cluster = spec.get("sub_cluster")
    if not isinstance(sub_cluster, str) or not sub_cluster.strip():
        raise ValueError("fluxonkv_spec.sub_cluster must be a non-empty string in owner mode")
    if sub_cluster != sub_cluster.strip():
        raise ValueError("fluxonkv_spec.sub_cluster must not have leading/trailing whitespace")

    if "large_file_paths" not in spec:
        raise ValueError("fluxonkv_spec.large_file_paths is required for owner mode")
    large_file_paths = spec.get("large_file_paths")
    if not isinstance(large_file_paths, list) or len(large_file_paths) == 0:
        raise ValueError("fluxonkv_spec.large_file_paths must be a non-empty list in owner mode")
    for idx, field_value in enumerate(large_file_paths):
        if not isinstance(field_value, str) or not field_value.strip():
            raise ValueError(
                f"fluxonkv_spec.large_file_paths[{idx}] must be a non-empty string in owner mode"
            )


class FluxonKvClientConfig():
    """Configuration class for KV Cache stores that reads from YAML config files."""

    def __init__(self, config_dict: Dict[str, Any]):
        """
        Initialize configuration from dictionary.

        Args:
            config_dict: Configuration dictionary (required)
        """
        plain = _to_plain_yaml_obj(config_dict, "config_dict")
        if not isinstance(plain, dict):
            raise TypeError(f"config_dict must be a mapping, got {type(config_dict).__name__}")

        _verify_config_by_template(plain)

        plain["test_spec_config"] = _normalize_test_spec_config(
            plain.get("test_spec_config"), "test_spec_config"
        )

        # Backend selection contract:
        # - Exactly one backend spec must be provided (no fallback inference).
        # - This keeps config shape explicit and prevents silently running with "no backend".
        has_mooncake = "mooncake_spec" in plain and plain.get("mooncake_spec") is not None
        has_fluxon = "fluxonkv_spec" in plain and plain.get("fluxonkv_spec") is not None
        if has_mooncake == has_fluxon:
            raise ValueError(
                "exactly one of [mooncake_spec, fluxonkv_spec] is required (and the chosen spec must not be null)"
            )

        pprof_duration_seconds = plain.get("pprof_duration_seconds")
        if pprof_duration_seconds is None:
            pass
        else:
            pprof_duration_seconds = int(pprof_duration_seconds)
            if pprof_duration_seconds == 0:
                raise ValueError("pprof_duration_seconds must be > 0")
            plain["pprof_duration_seconds"] = pprof_duration_seconds

        if "fluxonkv_spec" in plain:
            _validate_fluxonkv_contract(plain)

        self.config_dict = plain

    @property
    def instance_key(self):
        return self.config_dict["instance_key"]

    @property
    def pprof_duration_seconds(self) -> Optional[int]:
        v = self.config_dict.get("pprof_duration_seconds")
        if v is None:
            return None
        return int(v)
    
    @property
    def contribute_to_cluster_pool_size(self):
        return self.config_dict["contribute_to_cluster_pool_size"]
    
    @property
    def protocol_type(self):
        protocol = self.config_dict.get("protocol")
        if isinstance(protocol, dict):
            protocol_type = protocol.get("protocol_type")
            if isinstance(protocol_type, str) and protocol_type:
                return protocol_type
        return "rdma"
    
    @property
    def protocol_rdma_device_names(self):
        protocol = self.config_dict.get("protocol")
        if isinstance(protocol, dict):
            raw = protocol.get("rdma_device_names")
            if isinstance(raw, list) and raw:
                return ",".join(str(device) for device in raw)
        test_spec_config = self.config_dict.get("test_spec_config") or {}
        devices = test_spec_config.get("rdma_device_names")
        if not devices:
            return None
        return ",".join(str(device) for device in devices)

    @property
    def mooncake_spec_local_buffer_size(self):
        if "mooncake_spec" not in self.config_dict:
            return None
        return self.config_dict["mooncake_spec"]["local_buffer_size"]
    
    @property
    def mooncake_spec_metadata_server(self):
        if "mooncake_spec" not in self.config_dict:
            return None
        return self.config_dict["mooncake_spec"]["metadata_server"]
    
    @property
    def mooncake_spec_master_server_address(self):
        if "mooncake_spec" not in self.config_dict:
            return None
        return self.config_dict["mooncake_spec"]["master_server_address"]

    @property
    def mooncake_spec_etcd_addresses(self):
        if "mooncake_spec" not in self.config_dict:
            return None
        return self.config_dict["mooncake_spec"]["etcd_addresses"]
    
    @property
    def fluxonkv_spec_etcd_addresses(self):
        if "fluxonkv_spec" not in self.config_dict:
            return None
        spec = self.config_dict["fluxonkv_spec"]
        if not isinstance(spec, dict):
            return None
        return spec.get("etcd_addresses")

    @property
    def fluxonkv_spec_cluster_name(self):
        if "fluxonkv_spec" not in self.config_dict:
            return None
        return self.config_dict["fluxonkv_spec"]["cluster_name"]
    
    @property
    def fluxonkv_spec_share_mem_path(self):
        if "fluxonkv_spec" not in self.config_dict:
            return None
        return self.config_dict["fluxonkv_spec"]["share_mem_path"]
    
    @property
    def fluxonkv_spec_transfer_engine(self):
        if "fluxonkv_spec" not in self.config_dict:
            return None
        test_spec_config = self.config_dict.get("test_spec_config")
        if (
            isinstance(test_spec_config, dict)
            and test_spec_config.get("side_transfer_role") == "worker"
        ):
            return "p2p"
        return "closed"

    @property
    def fluxonkv_spec_sub_cluster(self) -> Optional[str]:
        if "fluxonkv_spec" not in self.config_dict:
            return None
        return self.config_dict["fluxonkv_spec"].get("sub_cluster")
        

    def __str__(self):
        """Return YAML-formatted configuration string."""
        return self.to_yaml_str()

    def to_yaml_str(self) -> str:
        """Serialize the config dict into a YAML document string."""
        cfg = self.to_dict()
        _validate_fluxonkv_contract(cfg)
        return yaml.safe_dump(cfg, sort_keys=False)

    def to_fluxon_kv_client_config_yaml_str(self) -> str:
        """Build the YAML string expected by the Rust `ClientConfigYaml` schema."""
        cfg = self.to_dict()
        # Keep runtime defaults implicit in the YAML handed to Rust so omitted
        # fields are not upgraded into explicit test overrides.
        test_spec_config = cfg.get("test_spec_config")
        if isinstance(test_spec_config, dict) and not test_spec_config:
            del cfg["test_spec_config"]
        spec = cfg.get("fluxonkv_spec")
        if not isinstance(spec, dict):
            raise ValueError("fluxonkv_spec is required for Fluxon KV client")
        _validate_fluxonkv_contract(cfg)
        return yaml.safe_dump(cfg, sort_keys=False)
    

    @classmethod
    def from_file(cls, config_path: str = "./config.yaml") -> "FluxonKvClientConfig":
        import yaml
        import os

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}, abs config path: {os.path.abspath(config_path)}")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                raw = f.read()
            try:
                config_dict = yaml.safe_load(raw)
            except yaml.YAMLError as e:
                # English note: YAML parse errors often miss context; print the full document for debugging.
                print(
                    f"YAML parse failed: source={config_path}\n--- YAML BEGIN ---\n{raw}\n--- YAML END ---",
                    file=sys.stderr,
                )
                raise
            return cls(config_dict)
        except Exception as e:
            raise ValueError(f"Failed to load or parse config file {config_path}: {e}")

    def get_backend_type(self):
        """
        Determine backend type from configuration.

        Returns:
            KvClientType: Corresponding client backend type
        """
        from .kvclient import KvClientType

        # 2) autodetect
        if self.mooncake_spec_master_server_address is not None:
            return KvClientType.MOONCAKE
        if "fluxonkv_spec" in self.config_dict:
            return KvClientType.FLUXON

        raise ValueError("Unable to determine backend type. Please provide the corresponding backend spec in the configuration.")

    def get_etcd_config(self) -> List[str]:
        """
        Returns:
            List[str]: etcd endpoint list
        """
        if self.mooncake_spec_etcd_addresses is not None:
            return self.mooncake_spec_etcd_addresses
        elif self.fluxonkv_spec_etcd_addresses is not None:
            return self.fluxonkv_spec_etcd_addresses
        else:
            raise ValueError("Unable to determine etcd configuration. Please check your configuration.")

    def to_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self.config_dict)

    # Channel-related validations

    def ensure_zero_contribution_for_channel(self) -> None:
        """
        Ensure the underlying KV store contributes 0 capacity when used for channels.

        Semantics: MQ (MPSC/MPMC) supports dynamic producer/consumer join/leave. If the
        underlying store contributes non-zero capacity to the cluster pool, capacity
        fluctuates and can destabilize the system.

        Contract:
        - "Zero-contribution" mode is selected when contribute_to_cluster_pool_size is missing,
          or when dram is 0 and all VRAM entries are 0.
        - Channel usage requires zero-contribution mode.
        """
        cfg = self.config_dict.get("contribute_to_cluster_pool_size")
        if cfg is None:
            return

        if not isinstance(cfg, dict):
            raise ValueError(
                "contribute_to_cluster_pool_size must be a mapping when provided; "
                f"got {type(cfg).__name__}"
            )

        dram = int(cfg["dram"])
        vram_raw = cfg.get("vram")
        if vram_raw is None:
            vram = {}
        elif not isinstance(vram_raw, dict):
            raise ValueError("contribute_to_cluster_pool_size.vram must be a mapping")
        else:
            vram = vram_raw

        if dram != 0:
            raise ValueError(
                f"For channel storage, contribute_to_cluster_pool_size must be zero-contribution. Current value: {cfg}. "
                "Message-queue semantics require dynamic join/leave; non-zero contribution causes instability."
            )
        for gpu_id, size in vram.items():
            if int(size) != 0:
                raise ValueError(
                    f"For channel storage, contribute_to_cluster_pool_size must be zero-contribution. Current value: {cfg}. "
                    f"Non-zero vram entry detected: gpu_id={gpu_id}. "
                    "Message-queue semantics require dynamic join/leave; non-zero contribution causes instability."
                )


def _dict_template():
    return yaml.safe_load(_yaml_template())

def _parse_type_annotation(comment: str) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Parse type annotation embedded in a comment (recursive parser).
    
    Args:
        comment: Comment string
        
    Returns:
        (type_name, validation_params) or None
    """
    debug_print("parsing type annotation", comment)
    def parse_type_recursive(type_str: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Recursively parse a type string."""
        type_str = type_str.strip()


        if type_str.startswith("(") and type_str.endswith(")"):
            inner_content = type_str[1:-1].strip()
            return parse_type_recursive(inner_content)
        
        # 1) List format: ['{str}:{int}']
        if type_str.startswith('[') and type_str.endswith(']'):
            inner_content = type_str[1:-1]  # strip [ ]
            if inner_content.startswith("'") and inner_content.endswith("'"):
                inner_content = inner_content[1:-1]
            return "list_format", {"format": inner_content}
        
        # 2) Union/enum: ('tcp'|'rdma') - choose one among multiple options
        if '|' in type_str:
            # Extract inner content
            inner_content = type_str.strip()
            if inner_content.startswith("(") and inner_content.endswith(")"):
                inner_content = inner_content[1:-1]  # strip surrounding ()
            values = inner_content.split('|')
            values = [parse_type_recursive(v) for v in values]
            return "enum", {"values": values}
        
        # 3) String matcher: ('{str}:{int}')
        if type_str.startswith("'") and type_str.endswith("'"):
            return "str_matcher", {"matcher": type_str[1:-1]}
        
                
        

        # 5) Constrained types, e.g. int(multiple of 16777216)
        if '(' in type_str and type_str.endswith(')'):
            # Find the last '(' to split type and constraint
            last_open = type_str.rfind('(')
            if last_open > 0:
                type_name = type_str[:last_open].strip()
                constraint = type_str[last_open+1:-1].strip()
                
                if type_name == "int":
                    return type_name, {"constraint": constraint}
                elif type_name == "dict":
                    if constraint == "any_not_none":
                        return "dict", {"constraint": "any_not_none"}
                    else:
                        return "dict", {"constraint": constraint}
                else:
                    parsed_base = parse_type_recursive(type_name)
                    if parsed_base is not None:
                        parsed_type_name, parsed_params = parsed_base
                        merged_params = dict(parsed_params)
                        merged_params["constraint"] = constraint
                        return parsed_type_name, merged_params
                    return type_name, {"constraint": constraint}
        
        # 6) Primitive types: str, int, bool, None
        if type_str in ["str", "int", "bool", "None"]:
            return type_str, {}
        
        debug_print("type_str ", type_str, "not matched to any type")
        return None
    
    # Extract content inside the trailing (...) annotation
    def comment_endswith_bracket(comment: str) -> Tuple[bool, int, int]:
        stack = []
        poped = False
        bracket_start = 0
        bracket_end = 0
        for i, char in enumerate(comment):
            if char == '(':
                if len(stack) == 0:
                    bracket_start = i
                stack.append(char)
            elif char == ')':
                if not stack:
                    debug_print("comment type annotation not complete with bracket")
                    return False, 0, 0
                stack.pop()
                poped = True
                if len(stack) == 0:
                    bracket_end = i
        return len(stack) == 0 and poped, bracket_start, bracket_end
    matched, bracket_start, bracket_end=comment_endswith_bracket(comment)
    if matched:
        inner_content = comment[bracket_start+1:bracket_end].strip()
        return parse_type_recursive(inner_content)
    
    debug_print("comment ", comment, "not matched to any type")
    return None

def _convert_format_to_regex_pattern(format_str: str) -> str:
    """
    Convert a format string into a regex pattern.
    
    Args:
        format_str: Format string, e.g. "{str}:{int}"
        
    Returns:
        Regex pattern
    """
    # Map placeholders to regex fragments
    placeholder_patterns = {
        "{str}": r"[^:]+",  # any non-colon chars
        "{int}": r"\d+",    # digits
        "{gpu_id}": r"\d+", # GPU id is typically numeric
    }
    
    # Replace placeholders directly without extra escaping
    regex_pattern = format_str
    for placeholder, pattern in placeholder_patterns.items():
        regex_pattern = regex_pattern.replace(placeholder, f"({pattern})")
    
    # Only escape ':' as it is a literal separator in the format string
    regex_pattern = regex_pattern.replace(":", r"\:")
    
    return f"^{regex_pattern}$"

def _validate_value_by_type(value: Any, type_info: Tuple[str, Dict[str, Any]], path: str, raise_err: bool = True):
    """
    Validate value based on parsed type info.
    
    Args:
        value: Value to validate
        type_info: (type_name, validation_params)
        path: Config path for error messages
    """
    type_name, params = type_info
    debug_print("validating value", value, "with type", type_name, "and params", params)

    if params.get("constraint") == "optional" and value is None:
        return None
    
    def raise_validation_error(msg: str):
        raise ValueError(f"Validation error at {path}: {msg}")
    
    if type_name == "str_matcher":
        matcher = params["matcher"]
        regex_pattern=_convert_format_to_regex_pattern(matcher)
        debug_print("regex_pattern", regex_pattern, ", matcher", matcher)
        matched = re.match(regex_pattern, value)
        if matched is None:
            if raise_err:
                raise_validation_error(f"Value does not match matcher '{matcher}', got {value}")
            else:
                return None
        return matched

    elif type_name == "str":
        if not isinstance(value, str):
            debug_print("value", value, "is not str, type", type(value).__name__)
            if raise_err:
                raise_validation_error(f"Expected str, got {type(value).__name__}")
            else:
                return None
        else:
            debug_print("value", value, "is str, type", type(value).__name__)
            return value
    
    elif type_name == "int":
        if not isinstance(value, int):
            if raise_err:
                raise_validation_error(f"Expected int, got {type(value).__name__}")
            else:
                return None
        if "constraint" in params:
            constraint = params["constraint"]
            if "multiple of" in constraint:
                # Parse "multiple of 16777216"
                match = re.search(r'multiple of (\d+)', constraint)
                if match:
                    multiple = int(match.group(1))
                    if value % multiple != 0:
                        if raise_err:
                            raise_validation_error(f"Value must be multiple of {multiple}, got {value}")
                        else:
                            return None
    
    elif type_name == "bool":
        if not isinstance(value, bool):
            if raise_err:
                raise_validation_error(f"Expected bool, got {type(value).__name__}")
            else:
                return None
    
    elif type_name == "dict":
        if not isinstance(value, dict):
            if raise_err:
                raise_validation_error(f"Expected dict, got {type(value).__name__}")
            else:
                return None
        if "constraint" in params:
            constraint = params["constraint"]
            if constraint == "any_not_none":
                # Require at least one key with a valid value (not None and not empty)
                has_valid_key = False
                for key, val in value.items():
                    if val is not None and val != {}:
                        has_valid_key = True
                        break
                if not has_valid_key:
                    if raise_err:
                        raise_validation_error(f"Dict must have at least one key with valid value (not None and not empty), got {value}")
                    else:
                        return None
    
    elif type_name == "enum":
        debug_print("enum type info", params["values"])
        for v in params["values"]:
            valid = _validate_value_by_type(value, v, path, False)
            debug_print("validated:", valid, "with type", type(valid))
            if valid is not None:
                debug_print("valid success", valid)
                return valid
        if raise_err:
            raise_validation_error(f"Expected one of {params['values']}, got {value} with type {type(value)}")
        else:
            return None
    
    elif type_name == "optional":
        if value is not None:
            if raise_err:
                _validate_value_by_type(value, (params["base_type"], {}), path)
            else:
                return _validate_value_by_type(value, (params["base_type"], {}), path)
    
    elif type_name == "format":
        format_str = params["format"]
        if not isinstance(value, str):
            if raise_err:
                raise_validation_error(f"Expected str for format validation, got {type(value).__name__}")
            else:
                return None
        
        # Validate string format via regex
        regex_pattern = _convert_format_to_regex_pattern(format_str)
        if not re.match(regex_pattern, value):
            if raise_err:
                raise_validation_error(f"Value does not match format '{format_str}', got {value}, regex_pattern: {regex_pattern}")
            else:
                return None
    
    elif type_name == "list_format":
        if not isinstance(value, list):
            if raise_err:
                raise_validation_error(f"Expected list, got {type(value).__name__}")
            else:
                return None
        
        format_str = params["format"]
        regex_pattern = _convert_format_to_regex_pattern(format_str)
        for i, item in enumerate(value):
            if not isinstance(item, str):
                if raise_err:
                    raise_validation_error(f"Expected str in list at index {i}, got {type(item).__name__}")
                else:
                    return None
            
            # Validate each element in the list via regex
            if not re.match(regex_pattern, item):
                if raise_err:
                    raise_validation_error(f"Item at index {i} does not match format '{format_str}', got {item}")
                else:
                    return None
        return value
    

    elif type_name == "None":
        debug_print("type_name == None, value:", value, type(value))
        if value is not None:
            if raise_err:
                raise_validation_error(f"Expected None, got {type(value).__name__}")
            else:
                return None
        else:
            debug_print("value is None, return \"None\"")
            return "None"
    
    else:
        raise ValueError(f"Unknown type: {type_name}")

def _extract_comments_from_yaml(yaml_str: str) -> Dict[str, str]:
    """
    Extract inline comments from a YAML template string.
    
    Args:
        yaml_str: YAML string
        
    Returns:
        Mapping from dotted-path to comment string
    """
    comments = {}
    lines = yaml_str.split('\n')
    current_path = []
    
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        
        # Compute indent level
        indent = len(line) - len(line.lstrip())
        level = indent // 2  # assume 2 spaces per level
        
        # Adjust current path
        while len(current_path) > level:
            current_path.pop()
        
        # Extract key name
        if ':' in stripped:
            key = stripped.split(':')[0].strip()
            if key.startswith("'") and key.endswith("'"):
                key = key[1:-1]
            current_path.append(key)
            path = '.'.join(current_path)
            
            # Extract trailing comment
            comment_match = re.search(r'#\s*(.+)$', line)
            if comment_match:
                comments[path] = comment_match.group(1).strip()
    
    return comments

def _type_info_support_optional(type_info: Optional[Tuple[str, Dict[str, Any]]]) -> bool:
    if type_info is None:
        return False
    type_name, params = type_info
    if type_name == "optional":
        return True
    if type_name == "enum":
        for v in params["values"]:
            if _type_info_support_optional(v):
                return True
    if type_name == "None":
        return True
    return False

def _verify_config_by_template(config_dict: Dict[str, Any]):
    """
    Verify config using the YAML template and parse type annotations from comments.
    """
    yaml_str = _yaml_template()
    comments = _extract_comments_from_yaml(yaml_str)
    template_dict = _dict_template()
    
    def recursive_verify(path: str, config_dict: Dict[str, Any], template_dict: Dict[str, Any], parent_constraint: Optional[str] = None):
        def new_err(msg: str):
            return ValueError(msg +
                f"\n\n========= config template: =========\n" +
                yaml_str +
                "\n=======================================\n"
            )
        
        for temp_key, temp_value in template_dict.items():
            current_path = f"{path}.{temp_key}" if path else temp_key
            
            # Check whether a type annotation exists
            current_type_info = None
            current_constraint = None
            if current_path in comments:
                comment = comments[current_path]
                current_type_info = _parse_type_annotation(comment)
                if current_type_info is not None:
                    if "constraint" in current_type_info[1]:
                        current_constraint = current_type_info[1]["constraint"]
            else:
                debug_print("warning: no comment found for", current_path, ", all comments:", comments)

            debug_print("1current_path",current_path, ", current_type_info", current_type_info)
            
            # Check whether the key exists
            if temp_key not in config_dict and parent_constraint != "dynamic_key":
                debug_print("2current_path",current_path, ", parent_constraint", parent_constraint)
                # If parent has any_not_none constraint, allow missing keys.
                #
                # English note:
                # - dict(dynamic_key) nodes describe maps with user-defined keys (e.g. VRAM per GPU id).
                # - Requiring the dict key itself to exist forces noisy `vram: {}` boilerplate everywhere.
                # - Therefore a dict(dynamic_key) field is treated as "present => validate entries; missing => empty".
                if parent_constraint == "any_not_none" or current_constraint == "optional" or current_constraint == "dynamic_key":
                    continue
                elif current_type_info is not None and _type_info_support_optional(current_type_info):
                    continue
                else:
                    raise new_err(f"missing key: config_dict.{current_path}, cur sub config dict: {config_dict}")
            
            if isinstance(temp_value, dict):
                # Allow optional dict nodes to be explicitly set to None.
                # This keeps the config schema strict while supporting "missing or null disables feature" toggles.
                if parent_constraint != "dynamic_key":
                    if temp_key in config_dict and config_dict[temp_key] is None:
                        if current_constraint == "optional" or _type_info_support_optional(current_type_info):
                            continue
                # Pass current constraint down to children
                child_constraint = None
                if current_type_info is not None and "constraint" in current_type_info[1]:
                    child_constraint = current_type_info[1]["constraint"]
                debug_print("3current_path",current_path, ", current_type_info", current_type_info)
                recursive_verify(current_path, config_dict[temp_key], temp_value, child_constraint)
            else:
                # Validate leaf value
                debug_print("4current_path",current_path, ", current_type_info", current_type_info)
                def validate_value(value: Any):
                    if current_type_info:
                        try:
                            _validate_value_by_type(value, current_type_info, current_path)
                        except ValueError as e:
                            raise new_err(str(e))
                    elif parent_constraint != "dynamic_key":
                        # Basic type check
                        temp_value_type = type(temp_value)
                        if temp_value_type is not type(value):
                            raise new_err(f"config_dict.{current_path} must be {temp_value_type}, but got {type(value)}")
                if parent_constraint == "dynamic_key":
                    for key, value in config_dict.items():
                        validate_value(value)
                else:
                    validate_value(config_dict[temp_key])
                
        
        # Check unknown keys
        if parent_constraint != "dynamic_key":
            for key in config_dict:
                if key not in template_dict:
                    if key == "pprof":
                        raise new_err(
                            "unknown key: config_dict.{path}.pprof (pprof is removed; use pprof_duration_seconds instead)"
                        )
                    raise new_err(f"unknown key: config_dict.{path}.{key}")
        
        # For any_not_none constraint, require at least one valid value
        if parent_constraint == "any_not_none":
            has_valid_value = False
            for key, value in config_dict.items():
                if value is not None and value != {}:
                    has_valid_value = True
                    break
            if not has_valid_value:
                raise new_err(f"Dict at {path} must have at least one key with valid value (not None and not empty), got {config_dict}")
    
    recursive_verify("", config_dict, template_dict)
        
