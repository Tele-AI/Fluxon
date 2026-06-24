from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from deployment.utils.deployconf_config_utils import (
    load_deployconf_etcd_address,
    load_deployconf_fluxon_cluster_name,
    load_deployconf_fluxon_share_mem_path,
    load_deployconf_mapping,
    load_deployconf_prom_remote_write_url,
    load_deployconf_prometheus_base_url,
    load_deployconf_resolved_global_envs,
    load_deployconf_service_ip_port,
)

__all__ = [
    "_load_build_config",
    "_verify_host_port",
    "_verify_url",
    "_verify_query_url",
    "_host_port_from_url",
    "_get_nested_config_value",
    "_load_config_field",
    "load_etcd_config",
    "load_tsdb_host_port",
    "load_tsdb_base_url",
    "load_tsdb_remote_write_url",
    "_load_yaml_mapping",
    "load_test_config_mapping",
    "load_test_kv_svc_type_from_test_config",
    "load_test_etcd_address_from_test_config",
    "load_test_fluxon_cluster_name_from_test_config",
    "load_test_fluxon_share_mem_path_from_test_config",
    "load_deployconf_mapping",
    "load_deployconf_resolved_global_envs",
    "load_deployconf_etcd_address",
    "load_deployconf_prometheus_base_url",
    "load_deployconf_prom_remote_write_url",
    "load_deployconf_fluxon_cluster_name",
    "load_deployconf_fluxon_share_mem_path",
    "load_deployconf_service_ip_port",
]



_WARNED_KEYS: set[str] = set()


def _warn_once(key: str, message: str) -> None:
    if key in _WARNED_KEYS:
        return
    _WARNED_KEYS.add(key)
    print(message)


def _load_build_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load config from build_config_ext.yml (searching upwards).

    Behavior:
    - If `config_path` is provided, load it directly.
    - Otherwise search for the nearest `build_config_ext.yml` upwards from `scripts/`.
    - If not found, raise FileNotFoundError.

    Returns:
        Config dict (returns {} if the YAML file is empty).

    Raises:
        FileNotFoundError: config file not found
        yaml.YAMLError: invalid YAML
    """

    def _search_upwards(start: Path, filename: str) -> Optional[Path]:
        cur = start.resolve()
        while True:
            candidate = cur / filename
            if candidate.exists():
                return candidate
            if cur.parent == cur:
                return None
            cur = cur.parent

    cfg_path: Optional[Path] = None
    if config_path is not None:
        p = Path(config_path)
        if p.exists():
            cfg_path = p
        else:
            raise FileNotFoundError(f"Config file does not exist: {p}")
    else:
        cfg_path = _search_upwards(Path(__file__).resolve().parents[1], "build_config_ext.yml")

    if cfg_path is None:
        raise FileNotFoundError(
            "Config file not found: build_config_ext.yml (searched upwards from script directory)"
        )

    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config or {}
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Invalid YAML in config file: {e}")


def _verify_host_port(value: Any, *, field: str) -> Tuple[str, int]:
    """Strictly validate host:port string and return (host, port)."""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, e.g. '127.0.0.1:9090'")
    raw = value.strip()
    if not raw:
        raise ValueError(f"{field} must not be empty, e.g. '127.0.0.1:9090'")
    if "://" in raw:
        raise ValueError(f"{field} must not include a scheme like 'http://'; expected 'host:port'")

    host: str
    port_str: str

    if raw.startswith("["):
        end = raw.find("]:")
        if end == -1:
            raise ValueError(f"{field} IPv6 format should look like '[::1]:9090'")
        host = raw[1:end]
        port_str = raw[end + 2 :]
    else:
        if raw.count(":") != 1:
            raise ValueError(f"{field} expected 'host:port', e.g. 127.0.0.1:9090")
        host, port_str = raw.split(":", 1)

    if not host:
        raise ValueError(f"{field} hostname must not be empty")
    try:
        port = int(port_str)
    except Exception:
        raise ValueError(f"{field} port must be an integer, e.g. 127.0.0.1:9090")
    if not (1 <= port <= 65535):
        raise ValueError(f"{field} port out of range (1-65535)")
    return host, port


def _verify_url(value: Any, *, field: str) -> str:
    """Strictly validate URL: must be http/https, include host+port, and include a path."""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string, e.g. http://127.0.0.1:9090/api/v1/write")
    raw = value.strip()
    if not raw:
        raise ValueError(f"{field} must not be empty, e.g. http://127.0.0.1:9090/api/v1/write")
    from urllib.parse import urlparse as _urlparse
    parsed = _urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(
            f"{field} must be a full URL starting with http/https, e.g. http://127.0.0.1:9090/api/v1/write"
        )
    if parsed.port is None:
        raise ValueError(f"{field} must explicitly include a port, e.g. http://127.0.0.1:9090/api/v1/write")
    if not parsed.path or parsed.path == "/":
        raise ValueError(f"{field} must include a path, e.g. http://127.0.0.1:9090/api/v1/write")
    return raw


def _verify_query_url(value: Any, *, field: str) -> str:
    """Strictly validate query URL: http/https, explicit port, and path starts with /api/v1 or /v1."""
    raw = _verify_url(value, field=field)
    from urllib.parse import urlparse as _urlparse
    path = _urlparse(raw).path
    if not (path.startswith("/api/v1") or path.startswith("/v1")):
        raise ValueError(
            f"{field} path should point to a query endpoint, e.g. http://127.0.0.1:9090/api/v1 or http://127.0.0.1:4000/v1"
        )
    return raw


def _host_port_from_url(url: str, *, field: str) -> Tuple[str, int]:
    from urllib.parse import urlparse as _urlparse
    parsed = _urlparse(url)
    if parsed.port is None or not parsed.hostname:
        raise ValueError(f"{field} missing host or port, e.g. http://127.0.0.1:9090/api/v1")
    return parsed.hostname, int(parsed.port)


def _get_nested_config_value(config: Dict[str, Any], key: str) -> Optional[Any]:
    """Retrieve a value from the configuration dictionary using dot notation."""
    current: Any = config
    for part in key.split('.'):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _load_config_field(
    key: str,
    default: Optional[Any] = None,
    *,
    config_path: Optional[Path] = None,
    required: bool = False,
) -> Any:
    """Load a specific configuration field from build_config_ext.yml.

    Args:
        key: Configuration key, supports dot-separated notation for nested fields.
        default: Value to return when the key is missing (ignored if required=True).
        config_path: Optional explicit path to the configuration file.
        required: When True, raise ValueError if the field is missing or empty string.

    Returns:
        The value found in the configuration, or the provided default.

    Raises:
        ValueError: If required is True but the key is missing or empty.
    """

    config = _load_build_config(config_path)
    value = _get_nested_config_value(config, key)

    if isinstance(value, str):
        value = value.strip()

    if value in (None, ""):
        if required:
            raise ValueError(f"Missing required config field: {key}")
        return default

    return value


def load_etcd_config(*, config_path: Optional[Path] = None) -> str:
    """Load required ETCD address (ip:port) from build_config_ext.yml.

    Contract:
    - Only accept the `etcd` field in the config file as the single data source;
    - If missing / empty / not a string, raise ValueError;
    - Use `_verify_host_port` to strictly validate format and port range, with no defaults/fallbacks.
    """
    cfg = _load_build_config(config_path)
    raw = cfg.get("etcd")
    if raw is None:
        raise ValueError("build_config_ext.yml missing required field 'etcd'; e.g. 127.0.0.1:2379")
    addr = str(raw).strip()
    if not addr:
        raise ValueError("build_config_ext.yml field 'etcd' must not be empty; e.g. 127.0.0.1:2379")
    # Reuse the common validator to ensure host and port are valid.
    _verify_host_port(addr, field="etcd")
    return addr


def load_tsdb_host_port(*, config_path: Optional[Path] = None) -> Tuple[str, int]:
    """Read TSDB base listen address host:port (used by Grafana, etc.).

    The config field `prom` must be a query URL (including port), like:
    - Prometheus: http://127.0.0.1:9090/api/v1
    - Greptime:   http://127.0.0.1:4000/v1
    Missing or invalid values raise.
    """
    cfg = _load_build_config(config_path)
    base_value = cfg.get("prom")
    if base_value is None:
        raise ValueError(
            "build_config_ext.yml missing required field 'prom'; e.g. http://127.0.0.1:9090/api/v1 or http://127.0.0.1:4000/v1"
        )
    url = _verify_query_url(base_value, field="prom")
    host, port = _host_port_from_url(url, field="prom")
    return host, port


def load_tsdb_base_url(*, config_path: Optional[Path] = None) -> str:
    """Read TSDB base URL (for Grafana data source).

    Only depends on `prom`; does not fall back to `promql`; missing values raise.
    """
    cfg = _load_build_config(config_path)
    base_value = cfg.get("prom")
    if base_value is None:
        raise ValueError(
            "build_config_ext.yml missing required field 'prom'; e.g. http://127.0.0.1:9090/api/v1 or http://127.0.0.1:4000/v1"
        )
    return _verify_query_url(base_value, field="prom")


def load_tsdb_remote_write_url(config_path: Optional[Path] = None) -> str:
    """Read TSDB Remote Write URL.

    - Prefer `prom_remote_write_url`
    - Accept legacy `prometheus_remote_write_url` and warn to migrate
    - If missing: raise (no default value)
    """
    cfg = _load_build_config(config_path)
    value = cfg.get("prom_remote_write_url")
    legacy = cfg.get("prometheus_remote_write_url") if not value else None

    if value is None and legacy is not None:
        _warn_once(
            "legacy_prom_remote_write_url",
            "ℹ️ Detected legacy field 'prometheus_remote_write_url'; please migrate to 'prom_remote_write_url'",
        )
        value = legacy

    if value is None:
        raise ValueError(
            "build_config_ext.yml missing required field 'prom_remote_write_url'; e.g. "
            "http://127.0.0.1:9090/api/v1/write or http://127.0.0.1:4000/v1/prometheus/write"
        )

    return _verify_url(value, field="prom_remote_write_url")


def _load_yaml_mapping(config_path: Path, *, label: str) -> Dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(f"Missing {label}: {config_path}")
    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if loaded is None:
        raise ValueError(f"{label} is empty: {config_path}")
    if not isinstance(loaded, dict):
        raise ValueError(f"{label} must be a YAML mapping: {config_path}")
    return loaded


def load_test_config_mapping(*, config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load fluxon_py test_config.yaml.

    Contract:
    - The test-layer config is separate from build_config_ext.yml.
    - If config_path is omitted, prefer FLUXON_TEST_CONFIG_PATH when set; otherwise load
      fluxon_py/tests/test_config.yaml from the repo.
    - No upward search or fallback is used so the authority stays explicit.
    """
    if config_path is None:
        env_config_path = os.environ.get("FLUXON_TEST_CONFIG_PATH", "").strip()
        if env_config_path:
            config_path = Path(env_config_path)
        else:
            repo_root = Path(__file__).resolve().parents[2]
            config_path = repo_root / "fluxon_py" / "tests" / "test_config.yaml"
    return _load_yaml_mapping(Path(config_path), label="test_config.yaml")


def load_test_kv_svc_type_from_test_config(*, config_path: Optional[Path] = None) -> str:
    """Load kv_svc_type from test_config.yaml and validate it against KvClientType."""
    test_cfg = load_test_config_mapping(config_path=config_path)
    raw = test_cfg.get("kv_svc_type")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("test_config.yaml must define non-empty kv_svc_type")
    value = raw.strip().lower()
    from fluxon_py.kvclient import KvClientType as _KvClientType

    allowed = {e.value for e in _KvClientType}
    if value not in allowed:
        raise ValueError(f"test_config.yaml kv_svc_type must be one of {sorted(allowed)}")
    return value


def load_test_etcd_address_from_test_config(*, config_path: Optional[Path] = None) -> str:
    """Load etcd address from test_config.yaml as the single test authority."""
    test_cfg = load_test_config_mapping(config_path=config_path)
    raw = test_cfg.get("etcd_address")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("test_config.yaml must define non-empty etcd_address")
    host, port = _verify_host_port(raw.strip(), field="test_config.yaml.etcd_address")
    return f"{host}:{port}"


def load_test_fluxon_cluster_name_from_test_config(*, config_path: Optional[Path] = None) -> str:
    """Load Fluxon cluster name from test_config.yaml as the single test authority."""
    test_cfg = load_test_config_mapping(config_path=config_path)
    raw = test_cfg.get("cluster_name")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("test_config.yaml must define non-empty cluster_name")
    return raw.strip()


def load_test_fluxon_share_mem_path_from_test_config(*, config_path: Optional[Path] = None) -> str:
    """Load Fluxon shared bundle root from test_config.yaml as the single test authority."""
    test_cfg = load_test_config_mapping(config_path=config_path)
    raw = test_cfg.get("share_mem_path")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("test_config.yaml must define non-empty share_mem_path")
    return raw.strip()
