"""KV client interface definitions.

This module hosts the abstract base classes used by the Python KV
client layer:

- ``KvFuture``: async operation handle
- ``MemHolder``: value holder
- ``KvClient``: high-level KV client interface (factory-only)
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Tuple, Union, List, Dict
from concurrent.futures import Future

from ..api_error import ApiError, Result, OkNone
from ..config import FluxonKvClientConfig
from .factory_only import FactoryOnly
from dataclasses import dataclass
from .nonzerocopy_encode import DLPacked, decode_flat_kv_dict, encode_flat_kv_dict

FlatDict = Dict[str, Union[int, float, bool, str, bytes, DLPacked]]


@dataclass(frozen=True)
class GetStartResult:
    """
    Result of a group-prefix best-effort get_start().

    Semantics:
    - ``keys`` is the caller-provided ordered page-key sequence.
    - ``raw_prefix_hit_len`` is the page-level continuous hit prefix.
    - ``transferable_len`` is rounded down to complete atomic groups and is the
      maximum prefix that can be consumed by get_transfer(). A caller may
      consume a shorter prefix only at an atomic-group boundary.
    """

    keys: Tuple[str, ...]
    raw_prefix_hit_len: int
    transferable_len: int
    prefix_hit_groups: int
    atomic_group_lens: Optional[Tuple[int, ...]]
    prefix_best_effort: bool
    first_miss_index: Optional[int]
    first_miss_group_index: Optional[int]
    all_hit: bool


@dataclass
class GetStartHandle:
    """
    Opaque-ish handle returned by get_start() and consumed by get_transfer().

    Callers must pass this handle to cancel_get_transfer() when abandoning it
    without calling get_transfer(). Fluxon backends may keep strong holder
    references alive while this handle is live.
    """

    keys: Tuple[str, ...]
    result: GetStartResult
    created_at_ns: int
    backend_token: Optional[int] = None
    backend_handle: int = 0
    closed: bool = False


@dataclass
class PutOptionalArgs:
    """
    Optional arguments for put() operations.

    - lease_id: attach the written key to a lease on commit.
    - reject_if_inflight_same_key: ask Fluxon to fail-fast when the same key is already
      being written by another inflight put.
    - reject_if_exist_same_key: ask Fluxon to fail-fast when the key already has a
      committed live replica.
    - write_through: keep synchronous remote-placement semantics when the backend
      supports an async write-back path. Defaults to True to match SGLang
      HiCache's default write policy.
    - make_replica_task: enqueue an asynchronous replica after a local write-back
      commit. Set False for a local-only write-back.
    - make_replica_task_mask: optional per-key replica admission decisions for
      local_fast_put_start(). Its length must match the key batch. The scalar
      make_replica_task remains the batch-wide gate.
    - atomic_group_lens: optional positive lengths that partition the ordered
      local_fast_put_start() key batch into atomic groups. Replica admission
      must be uniform within every group.
    """
    lease_id: Optional[int] = None
    reject_if_inflight_same_key: bool = False
    reject_if_exist_same_key: bool = False
    write_through: bool = True
    make_replica_task: bool = True
    make_replica_task_mask: Optional[List[bool]] = None
    atomic_group_lens: Optional[List[int]] = None

    def support_mooncake(self) -> Tuple[bool, List[str]]:
        """
        Check Mooncake compatibility for current options.

        Returns:
            (supported: bool, unsupported_fields: list[str])

        Notes:
            - Mooncake is write-once; currently does not support lease binding.
        """
        unsupported: List[str] = []
        if self.lease_id is not None:
            unsupported.append("lease_id")
        if self.reject_if_inflight_same_key:
            unsupported.append("reject_if_inflight_same_key")
        if self.reject_if_exist_same_key:
            unsupported.append("reject_if_exist_same_key")
        if self.write_through:
            unsupported.append("write_through")
        if not self.make_replica_task:
            unsupported.append("make_replica_task")
        if self.make_replica_task_mask is not None:
            unsupported.append("make_replica_task_mask")
        if self.atomic_group_lens is not None:
            unsupported.append("atomic_group_lens")
        return (len(unsupported) == 0, unsupported)


class KvFuture(ABC):
    """Abstract base class for KV operation futures.

    Provides both polling and blocking interfaces for async operations.
    """

    @abstractmethod
    def is_waiting(self) -> bool:
        """Return True if the operation is still waiting to complete."""

    @abstractmethod
    def wait(self) -> Result[Union[Any, "MemHolder"], ApiError]:
        """Block until completion and return the result."""


class MemHolder(ABC):
    """Abstract base class for memory holders.

    Provides access to cached data with lifetime management.
    """

    @abstractmethod
    def access(self) -> Result[Dict[str, Union[int, float, bool, str, bytes, DLPacked]], ApiError]:
        """Access the held value as a flat dict."""

    # release() is intentionally not part of the interface for now.


class KvClient(FactoryOnly):
    """Abstract base class for distributed KV cache clients.

    Public KV backends expose both:

    - async submission APIs: ``put()`` / ``get()``
    - blocking APIs: ``put_blocking()`` / ``get_blocking()``

    Backends may override the blocking APIs with a more efficient native
    implementation. The default implementation is a correctness-first
    wrapper around the async path.
    """

    @classmethod
    @abstractmethod
    def new(cls, config: "FluxonKvClientConfig") -> Result["KvClient", ApiError]:
        """Initialize and setup the distributed store."""

    @abstractmethod
    def put(
        self,
        key: str,
        value: FlatDict,
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result["KvFuture", ApiError]:
        """Store a key-value pair.

        Accepted value forms:
        - exactly one flat dict:
          ``Dict[str, Union[int, float, bool, str, bytes, dlpack]]``
        """

    @abstractmethod
    def get(
        self,
        key: str,
    ) -> Result["KvFuture", ApiError]:
        """Retrieve a value by key."""

    def put_blocking(
        self,
        key: str,
        value: FlatDict,
        opts: Optional[PutOptionalArgs] = None,
    ) -> Result[OkNone, ApiError]:
        """Synchronously store a key-value pair.

        Default implementation delegates to ``put()`` followed by
        ``wait()``. Backends with a native sync fast path should override
        this method directly.
        """
        put_result = self.put(key, value, opts=opts)
        if not put_result.is_ok():
            return Result.new_error(put_result.unwrap_error())
        wait_result = put_result.unwrap().wait()
        if not wait_result.is_ok():
            return Result.new_error(wait_result.unwrap_error())
        _ = wait_result.unwrap()
        return Result.new_ok(OkNone())

    def get_blocking(self, key: str) -> Result[Union[Any, "MemHolder"], ApiError]:
        """Synchronously retrieve a value by key.

        Default implementation delegates to ``get()`` followed by
        ``wait()``. Backends with a native sync fast path should override
        this method directly.
        """
        get_result = self.get(key)
        if not get_result.is_ok():
            return Result.new_error(get_result.unwrap_error())
        return get_result.unwrap().wait()

    def batch_put_blocking(
        self,
        keys: List[str],
        values: List[FlatDict],
        opts: Optional[PutOptionalArgs] = None,
        concurrency: Optional[int] = None,
    ) -> List[Result[OkNone, ApiError]]:
        """Synchronously store a batch of key-value pairs."""
        if len(keys) != len(values):
            raise ValueError("batch_put_blocking requires keys and values to have the same length")
        _ = concurrency
        return [self.put_blocking(key, value, opts=opts) for key, value in zip(keys, values)]

    def batch_get_blocking(
        self,
        keys: List[str],
        concurrency: Optional[int] = None,
    ) -> List[Result[Union[Any, "MemHolder"], ApiError]]:
        """Synchronously retrieve a batch of keys."""
        _ = concurrency
        return [self.get_blocking(key) for key in keys]

    def local_fast_put_start(
        self,
        keys: List[str],
        value_len: int,
        opts: Optional[PutOptionalArgs] = None,
    ) -> int:
        _ = keys
        _ = value_len
        _ = opts
        raise NotImplementedError(
            "local_fast_put_start is only implemented by backends with native plan_ptr support"
        )

    def local_fast_put_commit(self, plan_ptr: int) -> "KvFuture":
        _ = plan_ptr
        raise NotImplementedError(
            "local_fast_put_commit is only implemented by backends with native plan_ptr support"
        )

    def put_abort(self, plan_ptr: int) -> None:
        _ = plan_ptr
        raise NotImplementedError(
            "put_abort is only implemented by backends with native plan_ptr support"
        )

    def get_views(
        self,
        keys: List[str],
        concurrency: Optional[int] = None,
    ) -> int:
        _ = keys
        _ = concurrency
        raise NotImplementedError(
            "get_views is only implemented by backends with native plan_ptr support"
        )

    def release_views(self, plan_ptr: int) -> None:
        _ = plan_ptr
        raise NotImplementedError(
            "release_views is only implemented by backends with native plan_ptr support"
        )

    def get_start(
        self,
        keys: List[str],
        prefix_best_effort: bool = True,
        atomic_group_lens: Optional[List[int]] = None,
    ) -> GetStartHandle:
        _ = keys
        _ = prefix_best_effort
        _ = atomic_group_lens
        raise NotImplementedError(
            "get_start is only implemented by backends with native prefix get support"
        )

    def get_transfer(
        self,
        handle: GetStartHandle,
        concurrency: Optional[int] = None,
        *,
        consume_prefix_len: Optional[int] = None,
    ) -> int:
        _ = handle
        _ = concurrency
        _ = consume_prefix_len
        raise NotImplementedError(
            "get_transfer is only implemented by backends with native prefix get support"
        )

    def cancel_get_transfer(self, handle: GetStartHandle) -> None:
        _ = handle
        raise NotImplementedError(
            "cancel_get_transfer is only implemented by backends with native prefix get support"
        )

    @abstractmethod
    def get_size(self, key: str) -> Result[int, ApiError]:
        """Get the size of a stored value (non-blocking)."""

    @abstractmethod
    def is_exist(self, key: str) -> Result[bool, ApiError]:
        """Check if a key exists in the store (non-blocking)."""

    @abstractmethod
    def remove(self, key: str) -> Result[OkNone, ApiError]:
        """Remove a key from the store (non-blocking)."""

    @abstractmethod
    def sync_kv_to_file(
        self,
        key: str,
        target_instance_key: str,
        filepath: str,
        file_offset: int,
        bytes_field_key: str,
        timeout_ms: int = 60_000,
    ) -> Result["KvFuture", ApiError]:
        """Sync a bytes field of a KV value to a file on a remote instance.

        Semantics:
        - On `target_instance_key` node, fetch `key`, extract `bytes_field_key` (must be bytes),
          and write it into `filepath` at `file_offset`.

        Notes:
        - `bytes_field_key` is required (no fallback to implicit fields).
        - The default `timeout_ms=60_000` is intentionally exposed in the signature so callers
          can discover the RPC timeout directly from the interface.
        """

    @abstractmethod
    def instance_key(self) -> Result[str, ApiError]:
        """Get the unique instance key for this store instance."""

    @abstractmethod
    def close(self) -> Result[OkNone, ApiError]:
        """Close and tear down the store."""
        """Whether the store is write-once (keys cannot be overwritten)."""

    @abstractmethod
    def config(self) -> FluxonKvClientConfig:
        """Return the configuration of the store."""


    @abstractmethod
    def get_cluster_name(self) -> str:
        """Return the cluster name used by channel APIs."""

    @abstractmethod
    def get_etcd_config(self) -> List[str]:
        """Return etcd endpoint list as raw host:port strings (no scheme)."""

    @abstractmethod
    def third_party_logs_dir(self) -> Result[str, ApiError]:
        """Return the owner-derived log root for third-party Python components."""

    @abstractmethod
    def ensure_zero_contribution_for_channel(self) -> None:
        """Validate this KvClient is safe to use for channel storage."""

    def __enter__(self) -> "KvClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: best-effort close."""
        self.close()


class KvLeaseApi(ABC):
    """Lease operations abstraction for KV clients.

    Backends that support client-side leases should implement this
    interface to expose a unified lease API.
    """

    @abstractmethod
    def allocate_lease(self, ttl_seconds: int) -> Result[int, ApiError]:
        """Allocate a client lease with specified TTL seconds.

        Constraints:
        - `ttl_seconds` must be greater than or equal to the minimum client
          lease TTL enforced by the backend.
        - `ttl_seconds < 90` is invalid and should be rejected at the outermost
          API boundary, instead of letting the request reach the backend and
          fail later with a configuration error.
        """

    @abstractmethod
    def keepalive_lease(self, lease_id: int) -> Result[OkNone, ApiError]:
        """Keepalive a client lease using its existing TTL."""


class KvRpcApi(ABC):
    """User-level RPC abstraction for KV clients.

    This is intentionally separate from :class:`KvClient` to avoid
    forcing every backend to implement user-RPC.
    """

    @abstractmethod
    def rpc_call(
        self,
        node_id: str,
        path: str,
        payload: FlatDict,
        timeout_ms: int = 10_000,
    ) -> Result["KvFuture", ApiError]:
        """Call a user-defined RPC on a remote node.

        Notes:
        - Default timeout is 10000ms.
        - If a caller overrides timeout_ms, it must be >= 10000ms.
        """

    @abstractmethod
    def rpc_register(
        self,
        path: str,
        handler: Callable[[str, FlatDict], FlatDict],
    ) -> Result[OkNone, ApiError]:
        """Register a user RPC handler on this node."""

    @abstractmethod
    def rpc_call_bytes(
        self,
        node_id: str,
        path: str,
        payload: bytes,
        timeout_ms: int = 10_000,
    ) -> Result["KvFuture", ApiError]:
        """Call a user-defined RPC with a raw bytes payload."""

    @abstractmethod
    def rpc_register_bytes(
        self,
        path: str,
        handler: Callable[[str, bytes], bytes],
    ) -> Result[OkNone, ApiError]:
        """Register a raw bytes user RPC handler on this node."""
