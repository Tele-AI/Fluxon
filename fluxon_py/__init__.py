"""
Python KV Cache API Layer

A unified interface for key-value caching with support for multiple backends
including Mooncake and Rust implementations.
"""

from .kvclient.kvclient_interface import KvClient, KvFuture, MemHolder
from .api_error import (
    # Core Result type
    Result,
    # Channel-specific errors
    ChanKeyNotFoundError,
    ChanConfigEmptyError,
    ChanCreateError,
    ChanDeleteError,
    ChanBindError,
    ChanUnBindError,
    ChanMessageConsumptionError,
    ChanMessageProduceError,
    ChanIdxDuplicateError,
    ConsumerRegistrationError,
    # File cache errors
    ApiFileNotFoundError,
    FileAccessDeniedError,
    FileReadError,
    InvalidRangeError,
    CacheCorruptedError,
    CacheInvalidationError,
    # MPSC Channel errors
    ChannelNotFoundError,
    ChannelClosedError,
    ProducerRegistrationError,
    ProducerClosedError,
    ConsumerInitError,
    MessageBufferFullError,
    VersionConflictError,
    ProducerDiscoveryError,
    MessageProductionError,
    MessageConsumptionError,
    EtcdError,
    TransportName,
    TransportUser,
    JoinError,
    InternalError,
    InvalidConfigurationError,
    ResourceCleanupError,
    # Utility functions
    exception_to_ext_error,
    validate_file_range,
    validate_channel_config,
)
from .kvclient import KvClientType, new_store
from .config import FluxonKvClientConfig
from .monitor import render_monitor_cli, render_monitor_web

import importlib
from typing import Any


__version__ = "0.2.1"
__all__ = [
    # Core API
    "KvClient",
    # Helper Classes
    "KvFuture",
    "MemHolder",
    "FluxonMemHolder",
    "GpuBufferRegistration",
    "GpuDestination",
    "GpuGetStartHandle",
    # Backend management
    "KvClientType",
    "new_store",
    # Configuration
    "FluxonKvClientConfig",
    "render_monitor_cli",
    "render_monitor_web",
    # MPSC Channel - api_ext_chan.py (etcd-based implementation)
    "MPSCChanProducer",
    "MPSCChanConsumer",
    "MPMCChanProducer",
    "MPMCChanConsumer",
    # Channel types and roles
    "ChanType",
    "ChanRole",
    # Channel C-style API functions
    "chan_new",
    "chan_bind",
    "chan_unbind",
    "new_etcd_client",
    "new_or_bind_with_unique_key",
    # Result type
    "Result",
    # Channel-specific errors
    "ChanKeyNotFoundError",
    "ChanConfigEmptyError",
    "ChanCreateError",
    "ChanDeleteError",
    "ChanBindError",
    "ChanUnBindError",
    "ChanMessageConsumptionError",
    "ChanMessageProduceError",
    "ChanIdxDuplicateError",
    "ConsumerRegistrationError",
    # File cache errors
    "ApiFileNotFoundError",
    "FileAccessDeniedError",
    "FileReadError",
    "InvalidRangeError",
    "CacheCorruptedError",
    "CacheInvalidationError",
    # MPSC Channel errors
    "ChannelNotFoundError",
    "ChannelClosedError",
    "ProducerRegistrationError",
    "ProducerClosedError",
    "ConsumerInitError",
    "MessageBufferFullError",
    "VersionConflictError",
    "ProducerDiscoveryError",
    "MessageProductionError",
    "MessageConsumptionError",
    "EtcdError",
    "TransportName",
    "TransportUser",
    "JoinError",
    "InternalError",
    "InvalidConfigurationError",
    "ResourceCleanupError",
    # Utility functions
    "exception_to_ext_error",
    "validate_file_range",
    "validate_channel_config",
]


_LAZY_API_EXT_CHAN = {
    "MPSCChanProducer": ("api_ext_chan", "MPSCChanProducer"),
    "MPSCChanConsumer": ("api_ext_chan", "MPSCChanConsumer"),
    "MPMCChanProducer": ("api_ext_chan", "MPMCChanProducer"),
    "MPMCChanConsumer": ("api_ext_chan", "MPMCChanConsumer"),
    "ChanType": ("api_ext_chan", "ChanType"),
    "ChanRole": ("api_ext_chan", "ChanRole"),
    "chan_new": ("api_ext_chan", "chan_new"),
    "chan_bind": ("api_ext_chan", "chan_bind"),
    "chan_unbind": ("api_ext_chan", "chan_unbind"),
    "new_etcd_client": ("api_ext_chan", "new_etcd_client"),
    "new_or_bind_with_unique_key": ("api_ext_chan", "new_or_bind_with_unique_key"),
}

_LAZY_PYO3 = {
    "FluxonMemHolder": ("kvclient.fluxon", "FluxonMemHolder"),
    "GpuBufferRegistration": ("kvclient.fluxon", "GpuBufferRegistration"),
    "GpuDestination": ("kvclient.fluxon", "GpuDestination"),
    "GpuGetStartHandle": ("kvclient.fluxon", "GpuGetStartHandle"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_API_EXT_CHAN:
        mod_name, attr = _LAZY_API_EXT_CHAN[name]
        m = importlib.import_module(f"{__name__}.{mod_name}")
        v = getattr(m, attr)
        globals()[name] = v
        return v
    if name in _LAZY_PYO3:
        mod_name, attr = _LAZY_PYO3[name]
        m = importlib.import_module(f"{__name__}.{mod_name}")
        v = getattr(m, attr)
        globals()[name] = v
        return v
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(
        set(
            list(globals().keys())
            + list(_LAZY_API_EXT_CHAN.keys())
            + list(_LAZY_PYO3.keys())
        )
    )
