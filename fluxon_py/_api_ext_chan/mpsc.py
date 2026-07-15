"""MPSC channel shim backed by Rust implementation.

This module preserves the original public API surface
(`MPSCChanProducer`, `MPSCChanConsumer`, `ChanType`, `ChanRole`, etc.)
but delegates the underlying channel
management to the Rust library exposed via `fluxon_pyo3`.

Legacy Python watcher and prefetch implementations have been removed.

Currently this shim focuses on wiring up leases and identities. Data
path operations (`put_data`/`get_data`) are intentionally left as
placeholders and should be implemented in Rust and exposed via
`fluxon_pyo3` in follow-up work.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import ctypes
import etcd3

from ..kvclient.kvclient_interface import KvClient
from ..kvclient.kvclient_interface import DLPacked
from ..kvclient import fluxon as _fluxon_kv
from ..api_error import (
    InvalidConfigurationError,
    ChanKeyNotFoundError,
    ChanBindError,
    ChanCreateError,
    MessageConsumptionNoNewMessageError,
)
from ..api_error import (
    Result,
    ApiError,
    StorageFullError,
    NetworkError,
    OkNone,
    ChannelClosedError,
    ProducerClosedError,
    KeyNotFoundError,
    TransferBlockFailedError,
    PutDoneFailedError,
    MqGetDataUnknownError,
    PayloadLeaseNotFoundError,
    InvalidArgumentError,
    InternalError,
    ResourceCleanupError,
)
from fluxon_py.logging import init_logger
from .mq_config_check import validate_mpsc_config
from .mq_lifecycle import MqShutdownCtl
from . import ChannelProducer, ChannelConsumer


logging = init_logger(__name__)

# ---------------------------------------------------------------------------
# Test-only GC close markers
# ---------------------------------------------------------------------------
#
# Minimal, opt-in-free instrumentation to help tests verify that __del__
# actually invoked close(). We always record into a module-local dict to
# avoid introducing environment flags or fallback paths. The overhead is
# negligible and helps converge behavior across tests.
_TEST_CLOSE_MARKERS: Dict[str, bool] = {}
_CLOSE_DURING_PUT_DETAIL = "producer closed during put_with_payload"
_DISTRIBUTED_CLEANUP_ATTEMPTS = 3
_DISTRIBUTED_CLEANUP_RPC_TIMEOUT_SECONDS = 5

def _record_test_close_marker(tag: str, by_gc: bool) -> None:
    _TEST_CLOSE_MARKERS[tag] = by_gc

def test_get_close_marker(tag: str) -> Optional[bool]:
    """Return recorded close marker for `tag` if present.

    - True: close() was invoked from __del__ (GC path)
    - False: close() was invoked explicitly by user code
    - None: no record for this tag
    """
    return _TEST_CLOSE_MARKERS.get(tag)

def test_clear_close_marker(tag: str) -> None:
    """Remove a recorded marker; tests can clear between rounds."""
    _TEST_CLOSE_MARKERS.pop(tag, None)


def _is_close_during_put_error(err: Exception) -> bool:
    return (
        isinstance(err, InternalError)
        and err.component == "mpsc_rust"
        and _CLOSE_DURING_PUT_DETAIL in err.message
    )


# Small helper to satisfy lifecycle requirement: ensure close() can be called
# even on partially-constructed objects without getattr/None checks.
class _NoopCloseable:
    def close(self) -> None:  # pragma: no cover - trivial utility
        return


def _close_owned_mpsc_context(
    context: Any, *, channel_id: Optional[str]
) -> Result[OkNone, ApiError]:
    """Close one endpoint-owned MQ context before its KV client closes."""
    try:
        context.close()
    except Exception as e:
        return Result.new_error(
            ResourceCleanupError(
                message=f"failed to close MPSC context: {e}",
                resource_type="mpsc_context",
                resource_id=channel_id or "unknown",
            )
        )
    return Result.new_ok(OkNone())

# ---------------------------------------------------------------------------
# fluxon_pyo3 bridging
# ---------------------------------------------------------------------------

try:
    from ..tool import import_fluxon_pyo3_local

    _fluxon_pyo3 = import_fluxon_pyo3_local()
except ImportError as e:
    # The MPSC/MPMC Python layer is now tightly coupled with fluxon_pyo3: all
    # channel management, etcd/lease handling, and capacity control are
    # implemented on the Rust side. Without this module, continuing would
    # only fail later at arbitrary call sites (RuntimeError), which is hard
    # to debug and unfriendly for operations.
    #
    # Fail fast at import time with a clear ImportError and a neutral recovery
    # hint that matches both local builds and packaged wheel installs.
    raise ImportError(
        "fluxon_pyo3 is required for MPSC/MQ features but is not importable in this "
        "environment. Please ensure the unified PyO3 backend wheel has been built and "
        "installed for this runtime, and that "
        "`python -c \"import fluxon_pyo3\"` succeeds in the current runtime."
    ) from e

fluxon_pyo3 = _fluxon_pyo3

# PyO3 bindings aliases for clarity
_RustMpscContext = fluxon_pyo3.MpscContext  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Key helpers and the MPMC subchannel payload contract
# ---------------------------------------------------------------------------


@dataclass
class ConsumedMessage:
    """Payload plus producer/channel identity for the MPMC subchannel boundary."""

    data: Dict[str, Union[int, float, bool, str, bytes, DLPacked]]
    producer_id: str
    channel_id: str


def _new_etcd_meta_key_prefix() -> str:
    return "/channels/meta/"


def _new_etcd_meta_key(chan_id: str) -> str:
    return f"/channels/meta/{chan_id}"


def _new_etcd_producer_key(chan_id: str, producer_idx: str) -> str:
    return f"/channels/{chan_id}/producer/producer_{producer_idx}"


def _new_etcd_consumer_key(chan_id: str, consumer_idx: str) -> str:
    return f"/channels/{chan_id}/consumer/consumer_{consumer_idx}"


def _new_etcd_producer_weight_key(chan_id: str, producer_idx: str) -> str:
    return f"/channels/{chan_id}/producer_weight/{producer_idx}"


def _new_consume_offset_of_one_producer_key(chan_id: str, producer_idx: str) -> str:
    return f"/channels/{chan_id}/consumer_offset_of_all_producer/{producer_idx}"


def _new_produce_offset_of_all_producer_key(chan_id: str) -> str:
    return f"/channels/{chan_id}/producer_offset_of_all_producer/"


def _delete_owned_etcd_state(
    api: KvClient,
    *,
    keys: List[str],
    prefixes: List[str],
    dbg: str,
) -> Result[OkNone, ApiError]:
    """Delete and verify etcd state owned by one graceful lifecycle action."""

    if not keys and not prefixes:
        return Result.new_ok(OkNone())
    endpoints = api.get_etcd_config()
    if not endpoints:
        return Result.new_error(
            ResourceCleanupError(
                message="cannot delete owned etcd state: empty etcd endpoint list",
                resource_type="mq_etcd_state",
                resource_id=dbg,
            )
        )
    first_address = endpoints[0]
    errors: List[str] = []
    for attempt in range(1, _DISTRIBUTED_CLEANUP_ATTEMPTS + 1):
        client: Optional[etcd3.Etcd3Client] = None
        try:
            host, port_str = first_address.split(":")
            client = etcd3.client(
                host=host,
                port=int(port_str),
                timeout=_DISTRIBUTED_CLEANUP_RPC_TIMEOUT_SECONDS,
            )
            for key in keys:
                client.delete(key)
            for prefix in prefixes:
                client.delete_prefix(prefix)

            remaining_keys = [key for key in keys if client.get(key)[0] is not None]
            remaining_prefixes = [
                prefix
                for prefix in prefixes
                if next(iter(client.get_prefix(prefix)), None) is not None
            ]
            if not remaining_keys and not remaining_prefixes:
                return Result.new_ok(OkNone())
            errors.append(
                f"attempt={attempt} remaining_keys={remaining_keys!r} "
                f"remaining_prefixes={remaining_prefixes!r}"
            )
        except Exception as e:  # noqa: BLE001
            errors.append(f"attempt={attempt} {type(e).__name__}: {e}")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception as e:  # noqa: BLE001
                    logging.debug("%s failed to close cleanup etcd client: %s", dbg, e)

    return Result.new_error(
        ResourceCleanupError(
            message=(
                f"failed to delete owned etcd state via {first_address}: "
                + "; ".join(errors)
            ),
            resource_type="mq_etcd_state",
            resource_id=dbg,
        )
    )


def _rollback_unpublished_channel_state(
    api: KvClient,
    chan_id: str,
    dbg: str,
) -> Result[OkNone, ApiError]:
    """Remove every channel-scoped key for a channel that was never published."""

    if not isinstance(chan_id, str) or not chan_id.isdigit():
        return Result.new_error(
            ResourceCleanupError(
                message=f"invalid unpublished MPSC channel id: {chan_id!r}",
                resource_type="mpsc_channel",
                resource_id=str(chan_id),
            )
        )
    return _delete_owned_etcd_state(
        api,
        keys=[
            _new_etcd_meta_key(chan_id),
            f"cluster_lease/channels/{chan_id}",
            f"cluster_lease/id_allocator/channels/{chan_id}",
        ],
        prefixes=[
            f"/channels/{chan_id}/",
            f"dist_id_allocator/channels/{chan_id}/",
        ],
        dbg=dbg,
    )


# ---------------------------------------------------------------------------
# Channel type / role enums kept for external API
# ---------------------------------------------------------------------------


class ChanType(Enum):
    MPSC = "mpsc"
    MPMC = "mpmc"


class ChanRole(Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"


def _ensure_kvclient_lease_backend(api: KvClient, cluster: str) -> Any:
    """Ensure kvclient lease allocator/keepalive callbacks are registered and return backend uid.

    The MQ layer injects kvclient allocate/keepalive capability into the unified
    LeaseBackendUid abstraction. The underlying fluxon_util::lease_manager builds
    a KvClient backend uid keyed by the cluster name.
    """
    from ..kvclient.kvclient_interface import KvLeaseApi
    from fluxon_pyo3 import LeaseBackendUid as _PyLeaseBackendUid  # type: ignore[attr-defined]

    if not isinstance(api, KvLeaseApi):
        raise InvalidConfigurationError(
            message="KvClient must implement KvLeaseApi for MPSC payload lease",
        )

    def allocate_cb(ttl_seconds: int) -> int:
        """Bridge to KvLeaseApi.allocate_lease for the given TTL.

        Rust expects this callback to either:
          - return a valid positive lease id (int), or
          - raise a Python Exception (derives from BaseException) to signal error.

        Do NOT raise ApiError dataclasses here (they are not Exceptions) to
        avoid PyErr(TypeError: exceptions must derive from BaseException).
        """
        res = api.allocate_lease(int(ttl_seconds))
        if not res.is_ok():
            # Raise a real Python Exception so PyO3 converts it to Err(...)
            raise RuntimeError(
                f"kvclient allocate_lease failed for cluster={cluster}: {res.unwrap_error()}"
            )
        lease_id = res.unwrap()
        assert isinstance(lease_id, int) and lease_id > 0
        return lease_id

    def keepalive_cb(lease_id: int) -> None:
        """Bridge to KvLeaseApi.keepalive_lease for the given lease id.

        Rust expects a successful keepalive to return None (unit) and failures
        to raise a Python Exception. Returning a custom Result object here will
        cause type conversion errors in PyO3. See logs: "exceptions must derive
        from BaseException" when raising non-Exception ApiError values.
        """
        # Keepalive must not alter TTL; do not pass custom_ttl
        res = api.keepalive_lease(int(lease_id))
        if not res.is_ok():
            err = res.unwrap_error()
            # When the client is shutting down, background keepalive calls can race with the
            # P2P/framework shutdown and surface as a transient "SystemShutdown" network error.
            # Treat it as a no-op so the lease manager can stop cleanly without poisoning the
            # process exit code after successful workload completion.
            if isinstance(err, NetworkError) and ("SystemShutdown" in str(err)):
                return None
            # Raise a real Python Exception so PyO3 converts it to Err(...)
            raise RuntimeError(
                f"kvclient keepalive_lease failed for cluster={cluster}: {err}"
            )
        # Success: consume Ok(None) to satisfy strict Result policy
        _ = res.unwrap()
        # Success path: return None explicitly to map to Rust ()
        return None

    # Inject kvclient allocate/keepalive callbacks while constructing LeaseBackendUid.
    return _PyLeaseBackendUid.kv_client_with_callbacks(
        cluster,
        allocate_cb,
        keepalive_cb,
    )




class MpscContext:
    """Python wrapper around Rust MpscContext with shared kv backend state."""

    def __init__(self, api: KvClient) -> None:
        cluster = api.get_cluster_name()
        etcd_endpoints: List[str] = api.get_etcd_config()

        self.api = api
        self.cluster = cluster
        self.etcd_endpoints = etcd_endpoints

        # Inject kvclient lease capability via LeaseBackendUid during construction.
        self.kv_backend_uid = _ensure_kvclient_lease_backend(api, cluster)

        # Underlying Rust context receives endpoints plus the kv backend uid
        # that already carries kvclient allocate/keepalive callbacks.
        raw = getattr(api, "_client", None)
        if raw is None:
            raise InvalidConfigurationError(
                message="MPSC requires a fluxon-backed KvClient exposing `_client` (fluxon_pyo3.KvClient)",
            )
        self._inner = _RustMpscContext(etcd_endpoints, self.kv_backend_uid, raw)

    def new_producer(
        self,
        chan_id: Optional[str],
        ttl_seconds: int,
        weight: Optional[int],
        capacity: Optional[int],
        override_global_lease_id: Optional[int],
        override_member_lease_id: Optional[int],
        override_payload_lease_id: Optional[int],
        parent_mpmc_id_opt: Optional[str] = None,
        parent_mpmc_member_id_opt: Optional[int] = None,
        bind_shutdown_ctl: Optional[Any] = None,
    ):
        chan_id_int_opt: Optional[int] = None if chan_id is None else int(chan_id)
        parent_mpmc_id_int_opt: Optional[int] = (
            None if parent_mpmc_id_opt is None else int(parent_mpmc_id_opt)
        )
        return self._inner.new_producer(
            chan_id_int_opt,
            ttl_seconds,
            weight,
            capacity,
            override_global_lease_id,
            override_member_lease_id,
            override_payload_lease_id,
            parent_mpmc_id_int_opt,
            parent_mpmc_member_id_opt,
            bind_shutdown_ctl,
        )

    def new_consumer(
        self,
        chan_id: Optional[str],
        ttl_seconds: int,
        capacity: Optional[int],
        override_global_lease_id: Optional[int],
        override_member_lease_id: Optional[int],
        override_payload_lease_id: Optional[int] = None,
        parent_mpmc_id_opt: Optional[str] = None,
        parent_mpmc_member_id_opt: Optional[int] = None,
    ):
        chan_id_int_opt: Optional[int] = None if chan_id is None else int(chan_id)
        parent_mpmc_id_int_opt: Optional[int] = (
            None if parent_mpmc_id_opt is None else int(parent_mpmc_id_opt)
        )
        return self._inner.new_consumer(
            chan_id_int_opt,
            ttl_seconds,
            capacity,
            override_global_lease_id,
            override_member_lease_id,
            override_payload_lease_id,
            parent_mpmc_id_int_opt,
            parent_mpmc_member_id_opt,
        )

    def close(self) -> None:
        self._inner.close()


# ---------------------------------------------------------------------------
# Rust-backed MPSC producer/consumer shims
# ---------------------------------------------------------------------------


class MPSCChanProducer(ChannelProducer):
    """Thin Python wrapper over Rust-backed MPSC producer.

    This class maintains the original constructor signature so that
    higher-level APIs (e.g. `api_ext_chan`) and tests can continue to
    construct producers, but all channel/lease management is delegated
    to the Rust implementation exposed via `fluxon_pyo3`.

    Data-path operations (`put_data`) are currently placeholders and
    should be implemented on the Rust side and wired through here.
    """

    def __init__(
        self,
        api: KvClient,
        chan_id: Optional[str],
        chan_config: Dict[str, int],
        etcd_client: Optional[Any] = None,
        override_member_lease: Optional[Any] = None,
        override_chan_lease: Optional[Any] = None,
        *,
        override_payload_lease_id: Optional[int] = None,
        parent_mpmc_id_opt: Optional[str] = None,
        parent_mpmc_member_id_opt: Optional[int] = None,
        _parent_shutdown_ctl: Optional[MqShutdownCtl] = None,
    ) -> None:
        # Lifecycle safety: initialize critical fields first so close() can be
        # invoked without hasattr/getattr checks even if construction fails.
        self._handle_shutdown_ctl = _RustMpscContext.new_shutdown_ctl()
        self._created_new_channel = chan_id is None
        self._parent_mpmc_id = parent_mpmc_id_opt
        self._ctx = _NoopCloseable()
        self._handle = None  # type: ignore[assignment]
        # PyO3 handle methods borrow `&mut self`, so calls on one handle are
        # single-flight. Lifecycle shutdown must never wait on this lock.
        self._data_path_lock = threading.Lock()
        self._chan_id = "-1"
        self._producer_id = "unknown"
        self._closed_local = False
        self._membership_cleanup_done = False
        self._unpublished_rollback_done = False
        self.shutdown_ctl = MqShutdownCtl()
        # Validate config strictly (no implicit defaults/fallbacks).
        chan_config = validate_mpsc_config(chan_config, role=ChanRole.PRODUCER)
        self.api = api
        self.chan_config = chan_config
        # Use MpscContext to manage etcd/cluster and the unified KV backend.
        ctx = MpscContext(api)
        self._ctx = ctx

        # Create/bind the channel via MpscContext (PyO3).
        # If chan_id is None, Rust allocates it via the ID allocator.
        #
        # When used as an MPMC submodule, override global/member leases. The caller
        # passes etcd3.Lease objects as override_*_lease; we only forward their ids
        # to Rust, while the lifetime is still managed by the caller.
        override_global_lease_id: Optional[int]
        override_member_lease_id: Optional[int]
        if override_chan_lease is not None:
            override_global_lease_id = int(override_chan_lease.id)  # type: ignore[attr-defined]
        else:
            override_global_lease_id = None

        if override_member_lease is not None:
            override_member_lease_id = int(override_member_lease.id)  # type: ignore[attr-defined]
        else:
            override_member_lease_id = override_global_lease_id

        unregister_parent_close: Callable[[], None] = lambda: None
        if _parent_shutdown_ctl is not None:
            if not isinstance(_parent_shutdown_ctl, MqShutdownCtl):
                raise TypeError("_parent_shutdown_ctl must be MqShutdownCtl")
            unregister_parent_close = _parent_shutdown_ctl.register_construction_shutdown(
                self._handle_shutdown_ctl.close
            )
        try:
            handle = ctx.new_producer(
                chan_id,
                chan_config["ttl_seconds"],
                chan_config.get("weight"),
                chan_config.get("capacity"),
                override_global_lease_id,
                override_member_lease_id,
                override_payload_lease_id,
                parent_mpmc_id_opt,
                parent_mpmc_member_id_opt,
                self._handle_shutdown_ctl,
            )
        finally:
            unregister_parent_close()
        self._handle = handle
        self._handle_shutdown_ctl = handle.shutdown_clone()
        # Guard to make close idempotent without relying on None checks.
        self._closed_local = False
        # Cache identifiers eagerly to avoid re-entering the PyO3 handle
        # while it is mutably borrowed by put_flat_dict_ptrs. Calling back
        # into _handle (even read-only methods) from inside the Rust-side
        # callback would trigger "Already mutably borrowed".
        self._chan_id = str(self._handle.chan_id())  # type: ignore[attr-defined]
        self._producer_id: str = self._handle.producer_idx()  # type: ignore[attr-defined]

        # Resolve kvclient payload lease id from Rust side. Rust now
        # guarantees this is always present for any channel bound
        # through the Rust MPSC layer.
        self._payload_lease_id = self._handle.payload_lease_id()  # type: ignore[attr-defined]

        # Expose chan_id for legacy call sites that accessed the attribute.
        self.chan_id = self._chan_id

        logging.info(
            "%s initialized via Rust MPSC: chan_id=%s, producer_idx=%s",
            self.dbg_tag(),
            self.get_chan_id(),
            self.get_producer_id(),
        )

        if _parent_shutdown_ctl is not None and _parent_shutdown_ctl.closed:
            cleanup_result = (
                self._rollback_unpublished_channel()
                if self._created_new_channel
                else self.close()
            )
            if not cleanup_result.is_ok():
                raise RuntimeError(
                    "MPMC parent closed during MPSC producer construction; "
                    f"cleanup failed: {cleanup_result.unwrap_error()}"
                )
            cleanup_result.unwrap()
            raise RuntimeError("MPMC parent closed during MPSC producer construction")

    def dbg_tag(self) -> str:
        return (
            f"[MPSCChanProducer chan_id={self._chan_id} "
            f"producer_idx={self._producer_id}]"
        )

    def get_producer_id(self) -> str:
        # Return cached value to avoid touching _handle within callbacks.
        return self._producer_id  # type: ignore[no-any-return]

    def get_chan_id(self) -> str:
        # Return cached value to avoid touching _handle within callbacks.
        return self._chan_id  # type: ignore[no-any-return]

    def is_closed(self) -> bool:
        return self.shutdown_ctl.closed

    def _signal_shutdown(self) -> None:
        self.shutdown_ctl.close()
        try:
            self._handle_shutdown_ctl.close()
        except Exception as e:  # noqa: BLE001
            logging.debug("%s shutdown signal skipped: %s", self.dbg_tag(), e)

    def record_nonblocking_put_success(self, unix_ms: int) -> None:
        with self._data_path_lock:
            handle = self._handle
            if self.shutdown_ctl.closed or handle is None:
                return
            handle.record_nonblocking_put_success(unix_ms)

    def record_blocking_put_observed(self, unix_ms: int) -> None:
        with self._data_path_lock:
            handle = self._handle
            if self.shutdown_ctl.closed or handle is None:
                return
            handle.record_blocking_put_observed(unix_ms)

    # Note: historically the payload lease id was injected after
    # construction via `set_payload_lease_id`. Now we always resolve it
    # from the Rust producer handle at construction time and cache it on
    # `self` for use in callbacks (e.g. put_payload).

    def put_data(
        self, value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]]
    ) -> Result[bool, ApiError]:
        """Put data into the channel via Rust backend.

        Payload write is executed in Rust and directly calls KV `kv_put_ptrs`, so Python
        does not call `kvclient.put` or run a Python callback during the hot path.
        """

        if self.shutdown_ctl.closed:
            # Align with MPMC semantics: once the producer enters a closed state
            # (e.g. payload lease lost), all subsequent put_data calls return
            # ProducerClosedError to avoid branching on different error types.
            return Result[bool, ApiError].new_error(
                ProducerClosedError(
                    message="producer is closed",
                    channel_id=self.get_chan_id(),
                    producer_idx=self.get_producer_id(),
                )
            )

        if not isinstance(value, dict):
            return Result[bool, ApiError].new_error(
                InvalidArgumentError(
                    message=(
                        "MPSC put_data requires a flat dict payload: "
                        "Dict[str, Union[int, float, bool, str, bytes, dlpack]]"
                    )
                )
            )

        keepalive: List[bytes] = []
        dlpack_capsules: List[object] = []
        try:
            ptrs = _fluxon_kv.build_flat_dict_ptrs(value, keepalive, dlpack_capsules)
        except Exception as e:  # pragma: no cover - thin shim
            return Result[bool, ApiError].new_error(e)  # type: ignore[arg-type]

        try:
            with self._data_path_lock:
                handle = self._handle
                if self.shutdown_ctl.closed or handle is None:
                    return Result[bool, ApiError].new_error(
                        ProducerClosedError(
                            message="producer is closed",
                            channel_id=self.get_chan_id(),
                            producer_idx=self.get_producer_id(),
                        )
                    )
                handle.put_flat_dict_ptrs(ptrs)
        except Exception as e:  # pragma: no cover - thin shim
            if self.shutdown_ctl.closed:
                logging.debug("%s put stopped by close: %s", self.dbg_tag(), e)
                return Result[bool, ApiError].new_error(
                    ProducerClosedError(
                        message="producer is closed",
                        channel_id=self.get_chan_id(),
                        producer_idx=self.get_producer_id(),
                    )
                )
            if _is_close_during_put_error(e):
                self._signal_shutdown()
                logging.info("%s put aborted by close: %s", self.dbg_tag(), e)
                return Result[bool, ApiError].new_error(
                    ProducerClosedError(
                        message="producer is closed",
                        channel_id=self.get_chan_id(),
                        producer_idx=self.get_producer_id(),
                    )
                )

            # The exception here is an extension-layer ApiError mapped from Rust (or an
            # equivalent exception). When LeaseNotFound is returned (payload/etcd lease
            # lost), the channel semantics are no longer valid. Do not attempt fallback
            # recovery: mark this producer as closed to prevent subsequent puts.
            #
            # Contract (must stay in sync with Rust):
            #   - KV backend LeaseMgrError::{LeaseNotFound, LeaseExpired}
            #     is mapped via fluxon_kv::rpcresp_kvresult_convert::KvError
            #     into fluxon_pyo3.error.py_error_from_kv_error;
            #   - py_error_from_kv_error narrows that KvError into PayloadLeaseNotFoundError
            #     (an extension-layer ApiError), instead of generalizing it as NetworkError;
            #   - The put_payload callback raises err for unretryable failures, so `e` here
            #     is expected to be a PayloadLeaseNotFoundError instance.
            #
            # In other words, "payload lease lost" is detected in Python via
            # `isinstance(e, PayloadLeaseNotFoundError)`, not via fragile string matching.
            # If Rust changes LeaseMgrError variants or mappings, update:
            #   1) The LeaseMgrError mapping in py_error_from_kv_error;
            #   2) The check here and its corresponding tests.
            logging.error("%s put_flat_dict_ptrs failed: %s", self.dbg_tag(), e)
            if isinstance(e, PayloadLeaseNotFoundError):
                self._signal_shutdown()

                return Result[bool, ApiError].new_error(
                    ProducerClosedError(
                        message="payload lease not found; producer is closed",
                        channel_id=self.get_chan_id(),
                        producer_idx=self.get_producer_id(),
                    )
                )

            # Other errors: return as-is (no fallback/default behavior). Result.new_error
            # will serialize traceback uniformly.
            return Result[bool, ApiError].new_error(e)  # type: ignore[arg-type]
        # Success path: explicitly construct ok variant for consistency with MPMC
        return Result[bool, ApiError].new_ok(True)

    def close(self) -> Result[OkNone, ApiError]:
        # Use safe attribute access to tolerate partially-initialized objects
        chan_id = getattr(self, "_chan_id", None)
        dbg = getattr(self, "_dbg_tag", "[MPSCChanProducer]")
        by_gc = bool(getattr(self, "_closing_by_gc", False))
        producer_id = getattr(self, "_producer_id", None)
        if not getattr(self, "_closed_local", False):
            logging.debug(
                "%s close begin chan_id=%s parent_mpmc_id=%s",
                dbg,
                chan_id,
                self._parent_mpmc_id,
            )
            self._closed_local = True
            self._signal_shutdown()
            self._handle_shutdown_ctl = None  # type: ignore[assignment]
            # In-flight calls retain their own strong reference and unwind
            # after the independent Rust shutdown signal above.
            self._handle = None  # type: ignore[assignment]
            if hasattr(self, "_chan_id") and hasattr(self, "_producer_id"):
                tag = f"mpsc:producer:{self._chan_id}:{self._producer_id}"
                _record_test_close_marker(tag, by_gc)

        context_close_result = _close_owned_mpsc_context(
            self._ctx,
            channel_id=chan_id,
        )
        if not context_close_result.is_ok():
            return context_close_result
        context_close_result.unwrap()
        self._ctx = _NoopCloseable()

        if (
            not by_gc
            and not getattr(self, "_membership_cleanup_done", False)
            and isinstance(chan_id, str)
            and isinstance(producer_id, str)
            and chan_id != "-1"
            and producer_id != "unknown"
        ):
            cleanup_result = _delete_owned_etcd_state(
                self.api,
                keys=[
                    _new_etcd_producer_key(chan_id, producer_id),
                    _new_etcd_producer_weight_key(chan_id, producer_id),
                ],
                prefixes=[],
                dbg=dbg,
            )
            if not cleanup_result.is_ok():
                return cleanup_result
            cleanup_result.unwrap()
            self._membership_cleanup_done = True

        logging.debug(
            "%s close end chan_id=%s",
            dbg,
            chan_id,
        )
        return Result.new_ok(OkNone())

    def _rollback_unpublished_channel(self) -> Result[OkNone, ApiError]:
        """Close and remove a newly created channel that was never published."""

        if not getattr(self, "_created_new_channel", False):
            return Result.new_error(
                ResourceCleanupError(
                    message="cannot roll back a bound existing MPSC channel",
                    resource_type="mpsc_channel",
                    resource_id=self.get_chan_id(),
                )
            )
        if getattr(self, "_unpublished_rollback_done", False):
            return Result.new_ok(OkNone())
        close_result = self.close()
        if not close_result.is_ok():
            return close_result
        close_result.unwrap()
        rollback_result = _rollback_unpublished_channel_state(
            self.api,
            self.get_chan_id(),
            self.dbg_tag(),
        )
        if not rollback_result.is_ok():
            return rollback_result
        rollback_result.unwrap()
        self._unpublished_rollback_done = True
        return Result.new_ok(OkNone())

    def __del__(self) -> None:  # pragma: no cover - best-effort GC hook
        """Best-effort shutdown when GC drops the producer.

        Tests occasionally rely on GC to release channel resources (simulated crash).
        We make shutdown idempotent and lightweight here:
        - mark closed to short-circuit any in-flight put paths
        - notify Rust shutdown controller
        - drop the PyO3 handle eagerly to stop keepalive tasks
        """
        
        # Mark that this close is driven by GC (__del__) for test verification
        self._closing_by_gc = True  # type: ignore[attr-defined]
        try:
            res = self.close()
            # Consume the Result explicitly to satisfy strict policy even in GC path
            if res.is_ok():
                _ = res.unwrap()
            else:
                _ = res.unwrap_error()
        except Exception as e:  # noqa: BLE001
            logging.warning("%s __del__ close raised: %s", getattr(self, "_dbg_tag", "[MPSCChanProducer]"), e)
        finally:
            if hasattr(self, "_closing_by_gc"):
                delattr(self, "_closing_by_gc")

class MPSCChanConsumer(ChannelConsumer):
    """Thin Python wrapper over Rust-backed MPSC consumer.

    Keeps the original constructor signature, but only constructs the Rust-side
    MpscConsumerHandle. The data path (prefetch/offset, etc.) should live in Rust
    and be exposed via fluxon_pyo3.
    """

    def __init__(
        self,
        api: KvClient,
        chan_id: Optional[str],
        chan_config: Dict[str, int],
        etcd_client: Optional[Any] = None,
        override_member_lease: Optional[Any] = None,
        override_chan_lease: Optional[Any] = None,
        *,
        override_payload_lease_id: Optional[int] = None,
        parent_mpmc_id_opt: Optional[str] = None,
        parent_mpmc_member_id_opt: Optional[int] = None,
    ) -> None:
        # Lifecycle safety defaults; see producer for rationale
        self._handle_shutdown_ctl = _NoopCloseable()
        self._created_new_channel = chan_id is None
        self._parent_mpmc_id = parent_mpmc_id_opt
        self._ctx = _NoopCloseable()
        self._handle = None  # type: ignore[assignment]
        # Consumer ordering is single-flight at the PyO3 `&mut self` boundary.
        # close() signals shutdown independently and never waits on this lock.
        self._data_path_lock = threading.Lock()
        self._chan_id = "-1"
        self._consumer_id = "unknown"
        # MPMC may claim the sub-channel ready key before returning this
        # consumer to the outer wrapper. Default to False for direct MPSC usage.
        self._mpmc_ready_claimed = False
        self._dbg_tag = "[MPSCChanConsumer]"
        self._closed_local = False
        self._membership_cleanup_done = False
        self._unpublished_rollback_done = False
        self.shutdown_ctl = MqShutdownCtl()
        # Validate config strictly (no implicit defaults/fallbacks).
        chan_config = validate_mpsc_config(chan_config, role=ChanRole.CONSUMER)
        self.api = api
        self.chan_config = chan_config
        self.override_member_lease = override_member_lease
        self.override_chan_lease = override_chan_lease


        # Same as producer: manage etcd/cluster and kv backend via MpscContext.
        ctx = MpscContext(api)
        self._ctx = ctx

        # If chan_id is None, Rust allocates it via the ID allocator.
        # Lease override semantics match the producer.
        override_global_lease_id: Optional[int]
        override_member_lease_id: Optional[int]
        if override_chan_lease is not None:
            override_global_lease_id = int(override_chan_lease.id)  # type: ignore[attr-defined]
        else:
            override_global_lease_id = None

        if override_member_lease is not None:
            override_member_lease_id = int(override_member_lease.id)  # type: ignore[attr-defined]
        else:
            override_member_lease_id = override_global_lease_id

        # Pass parent_mpmc_id_opt through to the Rust side when provided so
        # sub-consumers created by MPMC can tag their parent channel id. Kept
        # optional to remain source-compatible with direct MPSC usage.
        handle = ctx.new_consumer(
            chan_id,
            chan_config["ttl_seconds"],
            chan_config.get("capacity"),
            override_global_lease_id,
            override_member_lease_id,
            override_payload_lease_id,
            parent_mpmc_id_opt,
            parent_mpmc_member_id_opt,
        )

        # Cache chan_id/consumer_idx early to avoid re-entering PyO3 via _handle
        # inside callbacks, which would trigger "Already mutably borrowed".
        self._handle = handle
        self._handle_shutdown_ctl=handle.shutdown_clone()
        self._chan_id = str(self._handle.chan_id())  # type: ignore[attr-defined]
        self._consumer_id: str = self._handle.consumer_idx()  # type: ignore[attr-defined]
        self._dbg_tag: str = (
            f"[MPSCChanConsumer chan_id={self._chan_id} "
            f"consumer_idx={self._consumer_id}]"
        )
        # Expose chan_id for legacy call sites that accessed the attribute.
        self.chan_id = self._chan_id

        # Initialize payload/delete callbacks during construction so get() no longer
        # needs to pass callback objects, making it easier to plug in a prefetch actor.
        #
        # payload_backend is validated by validate_mpsc_config and defaults to Rust-KV
        # (explicitly requested by business) to avoid the legacy Python callback +
        # threadpool overhead. Use payload_backend=1 to force the old Python path
        # for benchmark comparisons.
        payload_backend = int(chan_config["payload_backend"])
        if payload_backend == 2:
            self._handle.init_payload_callback_rust_kv()  # type: ignore[attr-defined]
            self._handle.init_delete_callback_rust_kv()  # type: ignore[attr-defined]
        else:
            self._handle.init_payload_callback(self._build_get_payload())  # type: ignore[attr-defined]
            self._handle.init_delete_callback(self._build_delete_callback())  # type: ignore[attr-defined]
        # Guard to make close idempotent without relying on None checks.
        self._closed_local: bool = False

        logging.info(
            "%s initialized via Rust MPSC: chan_id=%s, consumer_idx=%s, payload_backend=%s",
            self._dbg_tag,
            self._chan_id,
            self._consumer_id,
            payload_backend,
        )

    def dbg_tag(self) -> str:
        return self._dbg_tag

    def get_chan_id(self) -> str:
        """Return bound channel id.

        Keeps the same external interface as the legacy implementation so that
        higher-level modules (e.g. MPMC) can reference chan_id in logs and
        capacity-control keys.
        """

        return self._chan_id

    def get_consumer_id(self) -> str:
        return self._consumer_id

    def is_closed(self) -> bool:
        return self.shutdown_ctl.closed

    def _signal_shutdown(self) -> None:
        self.shutdown_ctl.close()
        try:
            self._handle_shutdown_ctl.close()
        except Exception as e:
            logging.debug("%s shutdown signal skipped: %s", self.dbg_tag(), e)

    def close(self) -> Result[OkNone, ApiError]:
        chan_id = getattr(self, "_chan_id", None)
        dbg = getattr(self, "_dbg_tag", "[MPSCChanConsumer]")
        by_gc = bool(getattr(self, "_closing_by_gc", False))
        consumer_id = getattr(self, "_consumer_id", None)
        if not getattr(self, "_closed_local", False):
            logging.debug(
                "%s close begin chan_id=%s parent_mpmc_id=%s",
                dbg,
                chan_id,
                self._parent_mpmc_id,
            )
            self._closed_local = True
            self._signal_shutdown()
            self._handle_shutdown_ctl = None  # type: ignore[assignment]
            # The active get keeps the PyO3 object alive until it observes
            # shutdown and returns; detaching here does not invalidate it.
            self._handle = None  # type: ignore[assignment]
            if hasattr(self, "_chan_id") and hasattr(self, "_consumer_id"):
                tag = f"mpsc:consumer:{self._chan_id}:{self._consumer_id}"
                _record_test_close_marker(tag, by_gc)

        context_close_result = _close_owned_mpsc_context(
            self._ctx,
            channel_id=chan_id,
        )
        if not context_close_result.is_ok():
            return context_close_result
        context_close_result.unwrap()
        self._ctx = _NoopCloseable()

        if (
            not by_gc
            and not getattr(self, "_membership_cleanup_done", False)
            and isinstance(chan_id, str)
            and isinstance(consumer_id, str)
            and chan_id != "-1"
            and consumer_id != "unknown"
        ):
            cleanup_result = _delete_owned_etcd_state(
                self.api,
                keys=[_new_etcd_consumer_key(chan_id, consumer_id)],
                prefixes=[],
                dbg=dbg,
            )
            if not cleanup_result.is_ok():
                return cleanup_result
            cleanup_result.unwrap()
            self._membership_cleanup_done = True

        logging.debug(
            "%s close end chan_id=%s",
            dbg,
            chan_id,
        )
        return Result.new_ok(OkNone())

    def _rollback_unpublished_channel(self) -> Result[OkNone, ApiError]:
        """Close and remove a newly created channel that was never published."""

        if not getattr(self, "_created_new_channel", False):
            return Result.new_error(
                ResourceCleanupError(
                    message="cannot roll back a bound existing MPSC channel",
                    resource_type="mpsc_channel",
                    resource_id=self.get_chan_id(),
                )
            )
        if getattr(self, "_unpublished_rollback_done", False):
            return Result.new_ok(OkNone())
        close_result = self.close()
        if not close_result.is_ok():
            return close_result
        close_result.unwrap()
        rollback_result = _rollback_unpublished_channel_state(
            self.api,
            self.get_chan_id(),
            self.dbg_tag(),
        )
        if not rollback_result.is_ok():
            return rollback_result
        rollback_result.unwrap()
        self._unpublished_rollback_done = True
        return Result.new_ok(OkNone())

    def __del__(self) -> None:  # pragma: no cover - best-effort GC hook
        """Best-effort shutdown when GC drops the consumer.

        Mirrors producer-side semantics to stop prefetch/retry actors quickly
        and let TTL-based cleanup reclaim keys in etcd.
        """
        
        # Mark that this close is driven by GC (__del__) for test verification
        self._closing_by_gc = True  # type: ignore[attr-defined]
        try:
            res = self.close()
            if res.is_ok():
                _ = res.unwrap()
            else:
                _ = res.unwrap_error()
        except Exception as e:  # noqa: BLE001
            logging.warning("%s __del__ close raised: %s", getattr(self, "_dbg_tag", "[MPSCChanConsumer]"), e)
        finally:
            if hasattr(self, "_closing_by_gc"):
                delattr(self, "_closing_by_gc")

    def _is_acting_as_submodule(self) -> bool:
        """Whether this MPSC consumer is used as a submodule.

        When used as an MPMC submodule, the caller passes override leases; we
        reuse the legacy predicate to detect that mode.
        """

        return self.override_member_lease is not None or self.override_chan_lease is not None
    def _build_get_payload(self):
        """Build the get_payload(producer_id, key) closure passed to Rust.

        - producer_id: selected producer index string
        - key:         message key generated by Rust from offset

        The default implementation fetches and decodes the message from the unified
        KV backend using key only. When running as a submodule (e.g. invoked by MPMC),
        it returns a `ConsumedMessage` wrapper so the upper layer can perform
        post-consume actions such as capacity release.
        """

        # Capture only immutable primitives and external deps to avoid creating
        # a reference cycle (self -> _handle -> callback -> self). The boolean
        # flag is computed up front so the inner closure never dereferences self.
        api = self.api
        chan_id_for_log = self._chan_id
        dbg_tag = self._dbg_tag
        acting_as_submodule = self._is_acting_as_submodule()

        def get_payload(
            producer_id: str, key: str
        ) -> Union[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ConsumedMessage, tuple]:
            # Fetch from the underlying KV API and decode into a flat dict.
            # Errors are raised and handled uniformly by the upper layer.
            res = api.get(key)
            if not res.is_ok():
                err = res.unwrap_error()
                logging.error(
                    "%s get_data immediate error for key %s: %s",
                    dbg_tag,
                    key,
                    err,
                )
                # Treat network errors as retryable. By fluxon_pyo3's contract, the
                # callback must return (code:int, msg:str) to be recognized as an error;
                # otherwise it will be treated as a normal payload and may lead to
                # confusing failures like "object of type 'int' has no len()".
                if isinstance(err, NetworkError):
                    return (1, f"retryable network error on immediate get: key={key}, chan_id={chan_id_for_log}, err={err}")

                raise RuntimeError(f"get_data immediate error for key {key}: {err}")

            fut = res.unwrap()
            assert fut is not None
            wait_res = fut.wait()
            if not wait_res.is_ok():
                err = wait_res.unwrap_error()
                logging.error(
                    "%s get_data wait error for key %s: %s",
                    dbg_tag,
                    key,
                    err,
                )
                if isinstance(err, NetworkError):
                    return (1, f"retryable network error on get wait: key={key}, chan_id={chan_id_for_log}, err={err}")

                raise RuntimeError(f"get_data wait error for key {key}: {err}")

            holder = wait_res.unwrap()
            assert holder is not None
            access_res = holder.access()
            if not access_res.is_ok():
                err = access_res.unwrap_error()
                logging.error(
                    "%s get_data access error for key %s: %s",
                    dbg_tag,
                    key,
                    err,
                )
                if isinstance(err, NetworkError):
                    return (
                        1,
                        f"retryable network error on get access: key={key}, chan_id={chan_id_for_log}, err={err}",
                    )

                raise RuntimeError(f"get_data access error for key {key}: {err}")

            payload_dict = access_res.unwrap()

            # When running as a submodule, return ConsumedMessage with producer/channel
            # metadata so the upper layer (e.g. MPMC) can release capacity after consume.
            # When used standalone, keep the legacy behavior and return the raw payload.
            if acting_as_submodule:
                return ConsumedMessage(
                    data=payload_dict,
                    producer_id=producer_id,
                    channel_id=chan_id_for_log,
                )

            return payload_dict

        return get_payload

    def _build_delete_callback(self):
        """Build the delete_payload(key) closure passed to Rust.

        - key: message key to delete, computed by Rust after consume_offset commit succeeds.

        Return value contract:
        - 0: success (including KeyNotFound treated as idempotent success)
        - 1: retryable error (network-related)
        - other errors: raise exception; Rust maps it as unrecoverable
        """

        api = self.api
        dbg_tag = self._dbg_tag

        def delete_payload(key: str) -> int:
            res = api.remove(key)
            if res.is_ok():
                _ = res.unwrap()
                return 0
            err = res.unwrap_error()

            # Idempotent delete: treat KeyNotFound as success.
            if isinstance(err, KeyNotFoundError):
                return 0

            # Network errors are retryable: return 1 so Rust can retry.
            if isinstance(err, NetworkError):
                logging.warning("%s delete retryable for key %s: %s", dbg_tag, key, err)
                return 1

            # Other errors: raise and let Rust map it as unrecoverable.
            raise RuntimeError(f"delete error for key {key}: {err}")

        return delete_payload

    # Removed: try_get_data to avoid split API; use get_data with try_time=0 for non-blocking semantics.

    def get_data(
        self,
        batch_size: int = 1,
        try_time: Optional[int] = None,
        prefetch_num: int = 0,
    ) -> Result[
        List[Union[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ConsumedMessage]],
        ApiError,
    ]:
        """Unified prefetch-first get API.

        Semantics:
        - If it returns Ok([...]), each element is from a successful get_one call.
        - If any get_one in this batch raises an error, the entire batch fails and
          returns Err(ApiError) immediately (no "partial success" Ok list).

        The window size is mapped to `batch_size + prefetch_num`, so the underlying
        Rust actor maintains a local prefetch queue of that size.
        """
        if self.shutdown_ctl.closed:
            return Result.new_error(
                ChannelClosedError(
                    message="Consumer is closed.",
                    channel_id=self._chan_id,
                )
            )

        prefetch_target = batch_size + max(prefetch_num, 0)

        # Inline minimal fetch loop with explicit prefetch_target to keep
        # ChannelConsumer.try_get_data signature aligned while still
        # honoring the calculated window size here.
        results: List[Union[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ConsumedMessage]] = []
        # try_time is seconds in Python; Rust get_one expects milliseconds.
        timeout_ms: Optional[int]
        if try_time is None:
            timeout_ms = None
        else:
            # Compatibility: try_time must not be 0; if callers pass 0, treat it as 1 second.
            t_sec = try_time if try_time > 0 else 1
            timeout_ms = int(t_sec * 1000)
            assert timeout_ms > 0

        for _ in range(batch_size):
            try:
                # Pass timeout_ms (converted from try_time seconds) to Rust.
                with self._data_path_lock:
                    handle = self._handle
                    if self.shutdown_ctl.closed or handle is None:
                        return Result.new_error(
                            ChannelClosedError(
                                message="Consumer is closed.",
                                channel_id=self._chan_id,
                            )
                        )
                    obj = handle.get_one(prefetch_target, timeout_ms)
            except Exception as e:
                # Rust is expected to raise an extension-layer ApiError. To avoid carrying
                # arbitrary Exception types in Result, wrap non-ApiError into
                # MqGetDataUnknownError to keep the error taxonomy narrow.
                if self.shutdown_ctl.closed:
                    logging.debug("%s get_one stopped by close: %s", self.dbg_tag(), e)
                    api_err = ChannelClosedError(
                        message="Consumer is closed.",
                        channel_id=self._chan_id,
                    )
                elif isinstance(e, ApiError):
                    logging.error("%s get_one failed: %s", self.dbg_tag(), e)
                    api_err = e
                else:
                    logging.error("%s get_one failed: %s", self.dbg_tag(), e)
                    api_err = MqGetDataUnknownError.from_exception(
                        e, channel_id=self._chan_id, consumer_id=self._consumer_id
                    )
                return Result[
                    List[
                        Union[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ConsumedMessage]
                    ],
                    ApiError,
                ].new_error(api_err)

            results.append(obj)  # type: ignore[arg-type]

        if not results:
            return Result[
                List[
                    Union[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ConsumedMessage]
                ],
                ApiError,
            ].new_error(
                MessageConsumptionNoNewMessageError("No message available")
            )

        return Result(results)


__all__ = [
    "MPSCChanProducer",
    "MPSCChanConsumer",
    "ChanType",
    "ChanRole",
    "ConsumedMessage",
    "_new_etcd_meta_key",
    "_new_etcd_producer_key",
    "_new_etcd_consumer_key",
    "_new_consume_offset_of_one_producer_key",
    # test helpers
    "test_get_close_marker",
    "test_clear_close_marker",
]
