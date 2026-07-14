from concurrent.futures import thread
from itertools import count
from math import log
import os, time, struct, threading, json, random, typing, etcd3, copy, fcntl
from typing import Callable, Dict, Optional, Tuple, Any, List, Set, Union, cast
from abc import ABC, abstractmethod

from etcd3 import etcdrpc
from etcd3 import utils as etcd_utils

try:
    import torch
except ImportError:
    torch = None

import io
import msgpack
from ..kvclient.kvclient_interface import KvClient, KvLeaseApi
from ..kvclient.factory_only import FactoryOnly
from ..api_error import Result, ApiError, OkNone, OK_NONE, ApiTimeoutError
from ..api_error import InvalidArgumentError
from ..kvclient.kvclient_interface import DLPacked
from .mpsc import (
    ConsumedMessage,
    MPSCChanProducer,
    MPSCChanConsumer,
    ChanRole,
    _delete_owned_etcd_state,
    _ensure_kvclient_lease_backend,
)
from ..api_error import (
    ApiFileNotFoundError as ExtFileNotFoundError,
    FileAccessDeniedError,
    FileReadError,
    InvalidRangeError,
    CacheCorruptedError,
    CacheInvalidationError,
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
    InvalidConfigurationError,
    ResourceCleanupError,
    ChanKeyNotFoundError,
    ChanConfigEmptyError,
    ChanMessageConsumptionError,
    ChanCreateError,
    ChanDeleteError,
    ChanBindError,
    ChanUnBindError,
    ChanIdxDuplicateError,
    ConsumerRegistrationError,
    ConsumerUnBindError,
    EtcdError,
    TransportName,
    TransportUser,
    exception_to_ext_error,
    validate_file_range,
    validate_channel_config,
)
from ..api_error import NetworkError, PayloadLeaseNotFoundError
from enum import Enum
from .mq_lifecycle import MqShutdownCtl
from . import ChannelProducer, ChannelConsumer
from .utils import TimedPriorityQueue
from fluxon_py.logging import init_logger
from ..tool import import_fluxon_pyo3_local

_fluxon_pyo3 = import_fluxon_pyo3_local()
LeaseManagerHandle = _fluxon_pyo3.LeaseManagerHandle  # type: ignore[attr-defined]
EtcdLock = _fluxon_pyo3.EtcdLock  # type: ignore[attr-defined]
import weakref
from ..etcd import DistributeIdAllocator
from .mq_config_check import validate_mpmc_config


logging = init_logger()
LOCAL_MEMBER_ID_RANGE_SIZE = 32
MPMC_CREATE_LOCK_TTL_SECONDS = 10
MPMC_CREATE_LOCK_TIMEOUT_SECONDS = 10.0
MPMC_ETCD_RPC_TIMEOUT_SECONDS = 10
MPMC_WATCH_RPC_TIMEOUT_SECONDS = 5
MPMC_WATCH_STOP_TIMEOUT_SECONDS = 10.0


def new_etcd_client(api: KvClient) -> Result[etcd3.Etcd3Client, ApiError]:
    """Create etcd client"""
    etcd_config: List[str] = api.get_etcd_config()
    first_address: str = etcd_config[0]
    host: str
    port_str: str
    host, port_str = first_address.split(":")
    print(f"new_etcd_client: {host}:{port_str}")
    try:
        client: etcd3.Etcd3Client = etcd3.client(
            host=host,
            port=int(port_str),
            timeout=MPMC_ETCD_RPC_TIMEOUT_SECONDS,
        )
        return Result.new_ok(client)
    except Exception as e:
        return Result.new_error(
            EtcdError(
                message=(
                    f"Failed to create etcd grpc client for endpoint {first_address}: {type(e).__name__}: {e}"
                ),
                component="mpmc.new_etcd_client",
                transport=TransportName.GRPC,
                transport_user=TransportUser.ETCD,
            )
        )

def stable_delete_ready_keys_for_member(
    api: KvClient, mpmc_id: str, member_id: int
) -> Result[OkNone, ApiError]:
    if not isinstance(mpmc_id, str) or not mpmc_id.isdigit() or int(mpmc_id) <= 0:
        raise ValueError(f"invalid mpmc_id: {mpmc_id!r}")
    if not isinstance(member_id, int) or member_id <= 0:
        raise ValueError(f"invalid member_id: {member_id!r}")

    endpoints = api.get_etcd_config()
    endpoint = endpoints[0] if endpoints else None
    prefix = _new_mpmc_ready_channels_prefix(mpmc_id)
    member_id_str = str(member_id)

    errors: List[str] = []
    for attempt in range(3):
        client_res = new_etcd_client(api)
        if not client_res.is_ok():
            err = client_res.unwrap_error()
            errors.append(str(err))
            continue

        client = client_res.unwrap()
        try:
            keys_to_delete: List[bytes] = []
            for value, meta in client.get_prefix(prefix):
                if value is None:
                    continue
                if value.decode() != member_id_str:
                    continue
                keys_to_delete.append(meta.key)

            for key in keys_to_delete:
                client.delete(key)

            # Verify: keys should be gone immediately after delete on the same prefix.
            remaining: List[bytes] = []
            for value, meta in client.get_prefix(prefix):
                if value is None:
                    continue
                if value.decode() != member_id_str:
                    continue
                remaining.append(meta.key)

            if len(remaining) == 0:
                return Result.new_ok(OK_NONE)

            errors.append(
                f"attempt={attempt}: remaining ready keys after delete: {remaining!r}"
            )
            time.sleep(0.1)
        except Exception as e:  # noqa: BLE001
            errors.append(f"attempt={attempt}: {e}")
            time.sleep(0.1)
        finally:
            try:
                client.close()
            except Exception as e:  # noqa: BLE001
                logging.warning(
                    f"stable_delete_ready_keys_for_member failed to close etcd client: {e}"
                )

    return Result.new_error(
        NetworkError(
            message=(
                "stable_delete_ready_keys_for_member failed for "
                f"mpmc_id={mpmc_id}, member_id={member_id}, errors={errors}"
            ),
            endpoint=endpoint,
        )
    )


def _local_member_id_cache_path(kv_api: KvClient, mpmc_id: str, role: ChanRole) -> str:
    cfg = kv_api.config()
    share_mem_path = cfg.fluxonkv_spec_share_mem_path
    if not isinstance(share_mem_path, str) or not share_mem_path.strip():
        raise ValueError("fluxonkv_spec.share_mem_path must be a non-empty string for local member-id cache")
    cluster_name = kv_api.get_cluster_name()
    role_name = role.value
    cache_dir = os.path.join(share_mem_path, cluster_name, "mq_member_id_cache")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, f"mpmc_{mpmc_id}_{role_name}.json")


def _allocate_mpmc_member_id_with_local_cache(
    *,
    etcd_client: etcd3.Etcd3Client,
    kv_api: KvClient,
    mpmc_id: str,
    role: ChanRole,
    id_allocator_cluster_lease_id: int,
    range_size: int = LOCAL_MEMBER_ID_RANGE_SIZE,
) -> Result[int, ApiError]:
    if not isinstance(range_size, int) or range_size <= 0:
        return Result.new_error(
            InvalidConfigurationError(
                message=f"local member-id range_size must be positive int, got {range_size!r}"
            )
        )

    cache_path = _local_member_id_cache_path(kv_api, mpmc_id, role)
    lock_path = cache_path + ".lock"
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    allocator_counter_key = f"dist_id_allocator/mpmc_channels/{mpmc_id}"

    id_allocator_cluster_lease = etcd3.Lease(
        int(id_allocator_cluster_lease_id),
        30 * 60,
        etcd_client,
    )

    with open(lock_path, "a+", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            cache_obj: Dict[str, int] = {}
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, "r", encoding="utf-8") as cache_fh:
                        raw = cache_fh.read().strip()
                    if raw:
                        parsed = json.loads(raw)
                        if isinstance(parsed, dict):
                            cache_obj = parsed
                except Exception as e:
                    logging.warning(
                        "failed to read local member-id cache %s for mpmc=%s role=%s: %s; will refresh range",
                        cache_path,
                        mpmc_id,
                        role.value,
                        e,
                    )
                    cache_obj = {}

            next_id = cache_obj.get("next_id")
            end_id = cache_obj.get("end_id")
            if cache_obj:
                try:
                    counter_value_raw, _ = etcd_client.get(allocator_counter_key)
                except Exception as e:
                    return Result.new_error(
                        EtcdError(
                            message=(
                                f"failed to read allocator counter {allocator_counter_key} "
                                f"for local member-id cache validation: {e}"
                            )
                        )
                    )
                counter_value: Optional[int]
                if counter_value_raw is None:
                    counter_value = None
                else:
                    try:
                        counter_value = int(counter_value_raw.decode())
                    except Exception as e:
                        return Result.new_error(
                            InvalidConfigurationError(
                                message=(
                                    f"allocator counter {allocator_counter_key} is not a valid int "
                                    f"during local member-id cache validation: {e}"
                                )
                            )
                        )

                cache_valid = (
                    isinstance(next_id, int)
                    and isinstance(end_id, int)
                    and next_id >= 1
                    and end_id >= next_id
                    and counter_value is not None
                    and counter_value >= end_id
                )
                if not cache_valid:
                    logging.warning(
                        "discard invalid local member-id cache %s for mpmc=%s role=%s: "
                        "next_id=%r end_id=%r allocator_counter=%r",
                        cache_path,
                        mpmc_id,
                        role.value,
                        next_id,
                        end_id,
                        counter_value,
                    )
                    cache_obj = {}
                    next_id = None
                    end_id = None

            if isinstance(next_id, int) and isinstance(end_id, int) and 1 <= next_id <= end_id:
                allocated_id = next_id
                cache_obj["next_id"] = allocated_id + 1
                if cache_obj["next_id"] > end_id:
                    cache_obj = {}
            else:
                alloc_res = DistributeIdAllocator(
                    etcd_client,
                    f"mpmc_channels/{mpmc_id}",
                    id_allocator_cluster_lease,
                ).allocate_range(range_size)
                if not alloc_res.is_ok():
                    return Result.new_error(alloc_res.unwrap_error())
                start_id, new_end_id = alloc_res.unwrap()
                allocated_id = start_id
                if start_id < new_end_id:
                    cache_obj = {
                        "next_id": start_id + 1,
                        "end_id": new_end_id,
                    }
                else:
                    cache_obj = {}
                logging.debug(
                    "allocated local member-id range for mpmc=%s role=%s: start=%s end=%s",
                    mpmc_id,
                    role.value,
                    start_id,
                    new_end_id,
                )

            tmp_path = cache_path + ".tmp"
            if cache_obj:
                with open(tmp_path, "w", encoding="utf-8") as cache_fh:
                    json.dump(cache_obj, cache_fh)
                os.replace(tmp_path, cache_path)
            else:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                if os.path.exists(cache_path):
                    os.unlink(cache_path)

            return Result.new_ok(allocated_id)
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

def _new_mpmc_meta_key(mpmc_id: str) -> str:
    """
    Get the meta key of the given MPMC channel id.
    """
    return f"/mpmc_channels/{mpmc_id}/meta"

def _new_mpmc_role_key_prefix(mpmc_id: str, role: ChanRole) -> str:
    """
    Get the key prefix for storing MPMC channel role.
    """
    if role == ChanRole.PRODUCER:
        return f"/mpmc_channels/producer/{mpmc_id}"
    elif role == ChanRole.CONSUMER:
        return f"/mpmc_channels/consumer/{mpmc_id}"
    else:
        raise ValueError(f"Invalid role: {role}")

def _new_mpmc_role_key(mpmc_id: str, role: ChanRole, member_id: int) -> str:
    """
    Get the key for storing MPMC channel role.
    """
    if role == ChanRole.PRODUCER:
        return f"/mpmc_channels/producer/{mpmc_id}/{member_id}"
    elif role == ChanRole.CONSUMER:
        return f"/mpmc_channels/consumer/{mpmc_id}/{member_id}"
    else:
        raise ValueError(f"Invalid role: {role}")

def _extract_mpmc_member_id_from_role_key(key: bytes, mpmc_id: str, role: ChanRole) -> int:
    key_str = key.decode()
    prefix = _new_mpmc_role_key_prefix(mpmc_id, role) + "/"
    if not key_str.startswith(prefix):
        raise ValueError(f"Invalid MPMC role key format: {key_str}")

    member_id_raw = key_str[len(prefix):]
    if "/" in member_id_raw or not member_id_raw.isdigit():
        raise ValueError(f"Invalid MPMC role key member id: {key_str}")
    return int(member_id_raw)

def _new_mpmc_mpsc_channels_key(mpmc_id: str) -> str:
    """
    Get the key for storing MPSC channel IDs in MPMC channel.
    """
    return f"/mpmc_channels/{mpmc_id}/mpsc_channels"


def _new_mpmc_ready_channel_key(mpmc_id: str, mpsc_id: str) -> str:
    """
    Get the key for marking a specific MPSC channel as ready in MPMC channel.
    """
    return f"/mpmc_channels/ready/{mpmc_id}/{mpsc_id}"


def _new_mpmc_ready_channels_prefix(mpmc_id: str) -> str:
    """
    Get the prefix for all ready channels in MPMC channel.
    """
    return f"/mpmc_channels/ready/{mpmc_id}/" # we need the / at the end for extracting mpsc_id from key


def _extract_mpsc_id_from_ready_key(key: bytes, mpmc_id: str) -> str:
    """
    Extract MPSC channel ID from a ready channel key.
    
    Args:
        key(bytes): The key from etcd
        expected_mpmc_id(int): Expected MPMC channel ID for validation
        
    Returns:
        int: MPSC channel ID
        
    Raises:
        ValueError: If key format is invalid or mpsc_id is not numeric
        AssertionError: If mpmc_id doesn't match expected value
    """
    try:
        key_str = key.decode()
        prefix = _new_mpmc_ready_channels_prefix(mpmc_id)
        if not key_str.startswith(prefix):
            raise ValueError(f"Invalid ready channel key format (wrong structure): {key_str}")

        mpsc_id = key_str[len(prefix):]
        if len(mpsc_id) == 0:
            raise ValueError(f"Invalid ready channel key format (empty mpsc_id): {key_str}")
        return mpsc_id
        
    except (ValueError, UnicodeDecodeError) as e:
        raise ValueError(f"Error parsing ready channel key {key}: {e}")

def _new_mpmc_next_channel_id_key(mpmc_id: str) -> str:
    """
    Get the key for next MPSC channel ID allocation in MPMC channel.
    """
    return f"/mpmc_channels/{mpmc_id}/next_channel_id"


def _new_mpmc_metadata_lease_key(mpmc_id: str) -> str:
    """
    Get the key for storing metadata lease ID in MPMC channel.
    """
    return f"/mpmc_channels/{mpmc_id}/metadata_lease_id"

# removed id_reserve_key; ID allocation now uses a shared cluster lease



class MPMCChannel(FactoryOnly):
    """
    MPMC Channel that manages multiple MPSC channels.
    """
    
    def __init__(
        self,
        mpmc_id: str,
        chan_config: dict,
        etcd_client: etcd3.Etcd3Client,
        role: ChanRole,
        new_ready_channels_callback: Optional[Callable[[List[str]], None]],
        remove_ready_channels_callback: Optional[Callable[[List[str]], None]],
        mpmc_global_lease: etcd3.Lease,
        kv_api: KvClient,
        payload_lease_id: int,
        shutdown_ctl: "MqShutdownCtl",
        id_allocator_cluster_lease_id: int,
        id_allocator_cluster_lease_handle_opt: Optional[object],
        keep_shared_mpmc_leases: bool,
    ):
        """
        Initialize MPMC Channel.
        
        Args:
            mpmc_id(int): MPMC channel ID
            chan_config(dict): Channel configuration
            etcd_client(etcd3.Etcd3Client): Etcd client
            metadata_lease(Optional[etcd3.Lease]): Lease for metadata operations (for new MPMC channels)
        """
        # Validate config strictly (no implicit defaults/fallbacks).
        chan_config = validate_mpmc_config(chan_config, role=role)
        self.mpmc_id = mpmc_id
        self.chan_config = chan_config
        self.role = role
        self.etcd_client: etcd3.Etcd3Client = etcd_client
        self.kv_api = kv_api
        self.new_ready_channels_callback = new_ready_channels_callback
        self.remove_ready_channels_callback = remove_ready_channels_callback
        # Shared shutdown controller between outer MPMC objects and this channel.
        # Must be provided by the caller so outer/inner share the same lifecycle controller.
        self.shutdown_ctl: MqShutdownCtl = shutdown_ctl
        self._close_done = False
        self._close_lock = threading.Lock()

        # MQ lease manager bridge (Rust RAII). Use this to register etcd and kvclient leases.
        self._lease_mgr = LeaseManagerHandle()
        # Shared kvclient payload lease id; this channel may register a local
        # keepalive contributor below.
        self.payload_lease_id: Optional[int] = payload_lease_id
        self._lm_kv_payload: Optional[object] = None
        # Declare member/global/cluster lease handles to satisfy static analyzers
        self.mpmc_member_id: Optional[int] = None
        self._lm_mpmc_global: Optional[object] = None
        self._lm_mpmc_member: Optional[object] = None
        # Keep the keepalive entry alive if the factory already registered it.
        self._lm_cluster_long: Optional[object] = id_allocator_cluster_lease_handle_opt
        
        # NOTE: We now require the caller to provide the metadata lease via
        # factory methods instead of letting the constructor infer it. This
        # avoids hidden fallback paths and keeps lifecycle clear (see
        # new_global_mpmc_channel / new_existed_global_mpmc_channel).
        if mpmc_global_lease is None:  # type: ignore[unreachable]
            raise ValueError(
                "mpmc_global_lease must be provided by factory; do not call MPMCChannel() directly"
            )
        self.mpmc_global_lease: etcd3.Lease = mpmc_global_lease
        logging.debug(
            f"Using provided metadata lease {mpmc_global_lease.id} for MPMC channel {mpmc_id}"
        )

        if not isinstance(id_allocator_cluster_lease_id, int) or id_allocator_cluster_lease_id <= 0:
            raise ValueError(
                f"invalid id_allocator_cluster_lease_id for MPMC {mpmc_id}: {id_allocator_cluster_lease_id!r}"
            )
        self._id_allocator_cluster_lease_id = id_allocator_cluster_lease_id
        if self._lm_cluster_long is not None:
            # The factory probes the lease before any `with_lease` writes.
            assert int(self._lm_cluster_long.id) == int(self._id_allocator_cluster_lease_id)  # type: ignore[attr-defined]
        
        # Created only after shared setup and member-id allocation. At p160/c8
        # scale those earlier steps can consume most of a short TTL before the
        # role key is published, so the per-member lease must start late.
        self.mpmc_member_lease: etcd3.Lease = None  # type: ignore[assignment]
        # Save endpoints for etcd lease keepalive registration
        # Only allow obtaining endpoints from KvClient; disallow other sources.
        if kv_api is None:
            raise ValueError(
                "kv_api is required to obtain etcd endpoints; only KvClient config is allowed"
            )
        self._etcd_endpoints: List[str] = kv_api.get_etcd_config()
        # Construct kvclient backend uid carrying allocate/keepalive callbacks (unified style)
        cluster = kv_api.get_cluster_name()
        self.kv_backend_uid = _ensure_kvclient_lease_backend(kv_api, cluster)

        # Lease setup steps are split into closures for clarity.

        # 1) Global etcd lease keepalive contributor.
        def _setup_global_lease_keepalive():
            logging.debug(
                f"[mpmc-lease] begin register global etcd keepalive: "
                f"mpmc_id={mpmc_id}, lease_id={int(self.mpmc_global_lease.id)}, "
                f"ttl={int(chan_config['ttl_seconds'])}, endpoints={self._etcd_endpoints}"
            )
            try:
                self._lm_mpmc_global = self._lease_mgr.register_etcd_lease(
                    self._etcd_endpoints,
                    int(chan_config["ttl_seconds"]),
                    int(self.mpmc_global_lease.id),
                    register_by=f"mpmc_channel_global:{mpmc_id}",
                )
            except Exception as e:
                logging.warning(f"failed to register etcd keepalive for mpmc global lease: {e}")
                self._lm_mpmc_global = None
            finally:
                logging.debug(
                    f"[mpmc-lease] end register global etcd keepalive: mpmc_id={mpmc_id}, "
                    f"ok={self._lm_mpmc_global is not None}"
                )

        # 2) Shared kvclient payload lease keepalive (payload_lease_id provided by factory)
        def _setup_payload_lease_keepalive():
            # Factory must pass payload_lease_id; do not read meta here.
            assert kv_api is not None and isinstance(kv_api, KvLeaseApi)
            assert isinstance(payload_lease_id, int) and payload_lease_id > 0
            self.payload_lease_id = payload_lease_id
            # Register keepalive for payload lease
            logging.debug(
                f"[mpmc-lease] begin register kvclient payload lease keepalive: "
                f"mpmc_id={mpmc_id}, payload_lease_id={self.payload_lease_id}, "
                f"ttl={int(chan_config['ttl_seconds'])}"
            )
            try:
                role_label = "producer" if role == ChanRole.PRODUCER else "consumer"
                self._lm_kv_payload = self._lease_mgr.register_kvclient_lease_via_backend(
                    self.kv_backend_uid,
                    self.payload_lease_id,
                    int(chan_config["ttl_seconds"]),
                    register_by=f"mpmc_{role_label}_payload_lease:{mpmc_id}",
                )
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(
                    "MPMC payload lease keepalive registration failed. "
                    f"mpmc_id={mpmc_id} payload_lease_id={self.payload_lease_id}. "
                    "This usually means the payload lease id stored in MPMC meta has expired "
                    "(e.g. process down longer than TTL) or belongs to a different cluster. "
                    "Recreate the MPMC channel metadata (new mpmc_id or delete the old meta key) "
                    "so a fresh payload lease can be allocated."
                ) from e
            finally:
                logging.debug(
                    f"[mpmc-lease] end register kvclient payload lease keepalive: mpmc_id={mpmc_id}, "
                    f"ok={self._lm_kv_payload is not None}"
                )

        # 3) Id-allocator cluster lease keepalive contributor.
        def _setup_id_allocator_cluster_keepalive():
            if self._lm_cluster_long is not None:
                logging.debug(
                    f"[mpmc-lease] id-allocator cluster lease already registered by factory: "
                    f"mpmc_id={mpmc_id}, cluster_lease_id={int(self._id_allocator_cluster_lease_id)}"
                )
                return
            logging.debug(
                f"[mpmc-lease] begin register id-allocator cluster etcd keepalive: "
                f"mpmc_id={mpmc_id}, cluster_lease_id={int(self._id_allocator_cluster_lease_id)}"
            )
            # Lease must be valid; fail fast if it cannot be kept alive.
            self._lm_cluster_long = self._lease_mgr.register_etcd_lease(
                self._etcd_endpoints,
                30 * 60,
                int(self._id_allocator_cluster_lease_id),
                register_by=f"mpmc_id_allocator_cluster_long:{mpmc_id}",
            )
            logging.debug(
                f"[mpmc-lease] end register id-allocator cluster etcd keepalive: mpmc_id={mpmc_id}, "
                f"ok={self._lm_cluster_long is not None}"
            )

        # 4) Allocate member id and register member etcd lease keepalive, then publish role key
        def _setup_member_and_role_key():
            logging.debug(
                f"[mpmc-lease] begin allocate mpmc member id and register member lease: "
                f"mpmc_id={mpmc_id}"
            )
            mpmc_member_id_result = _allocate_mpmc_member_id_with_local_cache(
                etcd_client=etcd_client,
                kv_api=kv_api,
                mpmc_id=mpmc_id,
                role=role,
                id_allocator_cluster_lease_id=int(self._id_allocator_cluster_lease_id),
            )
            if not mpmc_member_id_result.is_ok():
                raise ValueError(
                    f"Failed to allocate MPMC member ID for MPMC channel {mpmc_id}: {mpmc_member_id_result.unwrap_error()}"
                )
            self.mpmc_member_id = mpmc_member_id_result.unwrap()

            self.mpmc_member_lease = etcd_client.lease(int(chan_config["ttl_seconds"]))
            member_lease_id = int(self.mpmc_member_lease.id)
            logging.debug(
                f"[mpmc-lease] allocated member lease: mpmc_id={mpmc_id}, "
                f"member_id={self.mpmc_member_id}, member_lease_id={member_lease_id}"
            )
            try:
                self._lm_mpmc_member = self._lease_mgr.register_newly_granted_etcd_lease(
                    self._etcd_endpoints,
                    int(chan_config["ttl_seconds"]),
                    member_lease_id,
                    register_by=f"mpmc_channel_member:{mpmc_id}/{self.mpmc_member_id}",
                )
            except Exception as e:
                self._lm_mpmc_member = None
                raise RuntimeError(
                    "failed to register etcd keepalive for mpmc member lease: "
                    f"mpmc_id={mpmc_id}, member_id={self.mpmc_member_id}, "
                    f"member_lease_id={member_lease_id}: {e}"
                ) from e
            else:
                if self._lm_mpmc_member is None:
                    raise RuntimeError(
                        "register_newly_granted_etcd_lease returned no handle for mpmc member lease: "
                        f"mpmc_id={mpmc_id}, member_id={self.mpmc_member_id}, "
                        f"member_lease_id={member_lease_id}"
                    )
            finally:
                logging.debug(
                    f"[mpmc-lease] end register member lease: mpmc_id={mpmc_id}, "
                    f"member_id={self.mpmc_member_id}, ok={self._lm_mpmc_member is not None}"
                )

            mpmc_role_key = _new_mpmc_role_key(mpmc_id, role, self.mpmc_member_id)
            logging.debug(
                f"[mpmc-lease] begin put role key: key={mpmc_role_key}"
            )
            try:
                etcd_client.put(mpmc_role_key, b"dummy_value", self.mpmc_member_lease)
            except Exception as e:
                raise ValueError(f"put role key {mpmc_role_key} failed: {e}")
            else:
                logging.debug(
                    f"[mpmc-lease] end put role key: key={mpmc_role_key}"
                )

        # Top-level members all contribute keepalive for shared metadata,
        # payload, and allocator leases. Local sub-MPSC handles may also
        # register keepalive contributors, but cleanup remains explicit owner
        # work and is never driven by lease-handle drop.
        if keep_shared_mpmc_leases:
            _t0 = time.time(); logging.debug(f"[mpmc-lease] STEP1 global keepalive begin: mpmc_id={mpmc_id}")
            _setup_global_lease_keepalive(); logging.debug(f"[mpmc-lease] STEP1 global keepalive end: elapsed={time.time()-_t0:.3f}s")

            _t1 = time.time(); logging.debug(f"[mpmc-lease] STEP2 payload lease keepalive begin: mpmc_id={mpmc_id}")
            _setup_payload_lease_keepalive(); logging.debug(f"[mpmc-lease] STEP2 payload lease keepalive end: elapsed={time.time()-_t1:.3f}s")

            _t2 = time.time(); logging.debug(f"[mpmc-lease] STEP3 id-allocator cluster lease keepalive begin: mpmc_id={mpmc_id}")
            _setup_id_allocator_cluster_keepalive(); logging.debug(f"[mpmc-lease] STEP3 id-allocator cluster lease keepalive end: elapsed={time.time()-_t2:.3f}s")
        else:
            logging.debug(
                f"[mpmc-lease] skip shared lease keepalive registration for this channel instance: "
                f"mpmc_id={mpmc_id}, metadata_lease_id={int(self.mpmc_global_lease.id)}, "
                f"payload_lease_id={int(payload_lease_id)}, "
                f"id_allocator_cluster_lease_id={int(self._id_allocator_cluster_lease_id)}"
            )

        _t3 = time.time(); logging.debug(f"[mpmc-lease] STEP4 member id and role-key begin: mpmc_id={mpmc_id}")
        _setup_member_and_role_key(); logging.debug(f"[mpmc-lease] STEP4 member id and role-key end: elapsed={time.time()-_t3:.3f}s")

        self.mpsc_channels = []  # List of MPSC channel IDs
        self.ready_channels = []  # List of ready MPSC channel IDs
        self.unready_channels = []  # List of unready MPSC channel IDs
        self._ready_channels_lock = threading.Lock()  # Lock for thread-safe access to ready_channels
        self._watch_lock = threading.Lock()
        self.stop_flag = threading.Event()
        self.watch_thread: Optional[threading.Thread] = None
        self._watch_client: Optional[etcd3.Etcd3Client] = None
        self._watch_stream: Optional[Any] = None
        self._watch_request_stop: Optional[threading.Event] = None
        
    def get_meta(self) -> Result[Dict[str, Any], ApiError]:
        """
        Get MPMC channel metadata.
        
        Returns:
            Result[Dict[str, Any]]: Channel metadata
        """
        meta_key = _new_mpmc_meta_key(self.mpmc_id)
        meta_data, _ = self.etcd_client.get(meta_key)
        if meta_data is None:
            return Result[Dict[str, Any], ApiError].new_error(
                ChanKeyNotFoundError(f"MPMC channel {self.mpmc_id} not found")
            )
        meta_object = json.loads(meta_data.decode())
        return Result.new_ok(meta_object)
    
    def _read_mpsc_channels_snapshot(
        self,
        client: etcd3.Etcd3Client,
    ) -> Tuple[List[str], Optional[bytes]]:
        """Read and validate the channel list together with its CAS value."""
        channels_key = _new_mpmc_mpsc_channels_key(self.mpmc_id)
        channels_data, _ = client.get(channels_key)
        if channels_data is None:
            return [], None
        raw = json.loads(channels_data.decode())
        if not isinstance(raw, list):
            raise ValueError(f"invalid mpsc_channels value for mpmc_id={self.mpmc_id}: {raw!r}")

        channels: List[str] = []
        for item in raw:
            if not isinstance(item, str):
                raise ValueError(
                    f"invalid mpsc_channels element type for mpmc_id={self.mpmc_id}: {item!r} "
                    "(expected digit-only string)"
                )
            if not item.isdigit():
                raise ValueError(f"invalid mpsc_id element for mpmc_id={self.mpmc_id}: {item!r}")
            channels.append(item)

        return channels, channels_data

    def _read_remote_ready_channels(
        self,
        client: etcd3.Etcd3Client,
    ) -> List[str]:
        ready_prefix = _new_mpmc_ready_channels_prefix(self.mpmc_id)
        kv_pairs = [(meta.key, value) for value, meta in client.get_prefix(ready_prefix)]
        logging.debug("get_ready_channels: %s", kv_pairs)
        return [
            _extract_mpsc_id_from_ready_key(key, self.mpmc_id)
            for key, _ in kv_pairs
        ]

    def get_remote_ready_channels(self) -> List[str]:
        """
        Get ready MPSC channel IDs by scanning the ready prefix.
        
        Returns:
            List[str]: List of ready MPSC channel IDs
        """
        return self._read_remote_ready_channels(self.etcd_client)
    
    def get_ready_channels(self) -> List[str]:
        """
        Thread-safe getter for ready channels.
        
        Returns:
            List[str]: Copy of ready channel IDs list
        """
        with self._ready_channels_lock:
            return self.ready_channels.copy()
    
    def set_ready_channels(self, channels: List[str]) -> None:
        """
        Thread-safe setter for ready channels.
        
        Args:
            channels (List[str]): New list of ready channel IDs
        """
        with self._ready_channels_lock:
            self.ready_channels = channels.copy()

    def try_claim_ready_channel(self, mpsc_id: str) -> Result[bool, ApiError]:
        """Atomically claim an MPSC sub-channel for this MPMC consumer member.

        The ready key is the single authority that a sub-MPSC has been assigned
        to one MPMC consumer. Claim before binding/publishing so concurrent
        consumers do not carry a stale "unready" snapshot into a second bind.
        """

        if not isinstance(mpsc_id, str) or not mpsc_id.isdigit():
            raise ValueError(f"invalid mpsc_id: {mpsc_id!r}")
        if self.mpmc_member_id is None:
            raise ValueError(f"mpmc_member_id is None for mpmc_id={self.mpmc_id}")

        ready_key = _new_mpmc_ready_channel_key(self.mpmc_id, mpsc_id)
        try:
            success, _ = self.etcd_client.transaction(
                compare=[
                    self.etcd_client.transactions.create(ready_key) == 0
                ],
                success=[
                    self.etcd_client.transactions.put(
                        ready_key,
                        str(self.mpmc_member_id).encode(),
                        self.mpmc_member_lease,
                    )
                ],
                failure=[],
            )
            return Result.new_ok(bool(success))
        except Exception as e:
            return Result.new_error(
                ChanBindError(
                    f"Failed to claim ready key for mpmc_id={self.mpmc_id}, "
                    f"mpsc_id={mpsc_id}: {e}"
                )
            )

    def _best_effort_delete_ready_channel(self, mpsc_id: str, *, reason: str) -> None:
        if not isinstance(mpsc_id, str) or not mpsc_id.isdigit():
            raise ValueError(f"invalid mpsc_id: {mpsc_id!r}")

        ready_key = _new_mpmc_ready_channel_key(self.mpmc_id, mpsc_id)
        try:
            self.etcd_client.delete(ready_key)
        except Exception as e:
            logging.warning(
                "Failed to delete ready key after %s for mpmc_id=%s mpsc_id=%s: %s",
                reason,
                self.mpmc_id,
                mpsc_id,
                e,
            )

    def _try_bind_existing_unready_consumer(
        self,
        api: KvClient,
        chan_config: Dict[str, int],
        candidate_mpsc_ids: List[str],
    ) -> Optional[Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError]]:
        """Claim and bind one existing unready sub-MPSC for an MPMC consumer.

        The caller already decided these candidates are "unready" from a
        snapshot. Re-check via the ready-key transaction per candidate so only
        one concurrent consumer can claim it. If all claims are lost, return
        None so the caller may re-evaluate whether a new sub-MPSC is required.
        """

        for mpsc_id in candidate_mpsc_ids:
            claim_res = self.try_claim_ready_channel(mpsc_id)
            if not claim_res.is_ok():
                return Result.new_error(claim_res.unwrap_error())
            claimed = claim_res.unwrap()
            if not claimed:
                logging.debug(
                    "Existing unready MPSC lost ready-claim race for mpmc_id=%s, mpsc_id=%s",
                    self.mpmc_id,
                    mpsc_id,
                )
                continue

            try:
                mpsc_consumer = MPSCChanConsumer(
                    api,
                    mpsc_id,
                    chan_config,
                    self.etcd_client,
                    self.mpmc_member_lease,
                    self.mpmc_global_lease,
                    override_payload_lease_id=self.payload_lease_id,
                    parent_mpmc_id_opt=self.mpmc_id,
                    parent_mpmc_member_id_opt=self.mpmc_member_id,
                )
            except Exception as e:
                self._best_effort_delete_ready_channel(
                    mpsc_id,
                    reason="existing_unready_bind_failure",
                )
                return Result.new_error(
                    ChanBindError(
                        f"Failed to bind claimed existing MPSC consumer for "
                        f"mpmc_id={self.mpmc_id}, mpsc_id={mpsc_id}: {e}"
                    )
                )

            mpsc_consumer._mpmc_ready_claimed = True
            logging.debug(
                "Bound claimed existing unready MPSC consumer for mpmc_id=%s, mpsc_id=%s",
                self.mpmc_id,
                mpsc_id,
            )
            return Result.new_ok(mpsc_consumer)

        return None

    def _ensure_member_lease_alive(self) -> Result[OkNone, ApiError]:
        lease_id = int(self.mpmc_member_lease.id)
        endpoint = self._etcd_endpoints[0] if self._etcd_endpoints else None
        try:
            info = self.etcd_client.get_lease_info(lease_id)
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            if "not found" in msg or "requested lease not found" in msg:
                ttl_val = 0
            else:
                return Result.new_error(
                    NetworkError(
                        message=f"get_lease_info failed for lease_id={lease_id}: {e}",
                        endpoint=endpoint,
                    )
                )
        else:
            ttl_val = getattr(info, "TTL", None)
            if not isinstance(ttl_val, int):
                return Result.new_error(
                    EtcdError(
                        message=(
                            "get_lease_info returned invalid TTL type "
                            f"for lease_id={lease_id}: {ttl_val!r}"
                        ),
                        component="mpmc._ensure_member_lease_alive",
                        transport=TransportName.GRPC,
                        transport_user=TransportUser.ETCD,
                    )
                )
        if ttl_val > 0:
            return Result.new_ok(OK_NONE)

        self.shutdown_ctl.close()
        return Result.new_error(
            ChannelClosedError(
                message=(
                    "MPMC member lease expired; stop using this MPMC owner and recreate it. "
                    f"mpmc_id={self.mpmc_id} member_id={self.mpmc_member_id} lease_id={lease_id}"
                ),
                channel_id=self.mpmc_id,
            )
        )

    
    
    def get_next_available_channel(
        self,
        api: KvClient,
        chan_config: Dict[str, int],
        producer: Optional["MPMCChanProducer"] = None,
    ) -> Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError]:
        """Get the next available MPSC object.

        This method gives priority to ready channels and falls back to
        unready or newly created channels as needed. The loop respects
        the shared shutdown controller so callers that have initiated a
        graceful close via :class:`MqShutdownCtl` will eventually
        observe a :class:`ChannelClosedError` here.
        """

        # Fast path: channel already closed.
        if self.shutdown_ctl.closed:
            return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(
                ChannelClosedError(
                    message="MPMC channel is closed.",
                    channel_id=self.mpmc_id,
                )
            )

        member_lease_res = self._ensure_member_lease_alive()
        if not member_lease_res.is_ok():
            return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(
                member_lease_res.unwrap_error()
            )
        _ = member_lease_res.unwrap()

        chan_role = ChanRole.PRODUCER if producer is not None else ChanRole.CONSUMER
        logging.debug(
            "Getting next available channel for MPMC channel %s, is producer: %s",
            self.mpmc_id,
            producer is not None,
        )
        role = "producer" if producer is not None else "consumer"
        tag = f"[get_next_available_channel by {role}]"

        # Try existing channels: ready -> unready
        def get_existing_channel(mpsc_id: str):
            if producer is not None:
                logging.debug(
                    f"{tag} Getting existing MPSC producer for MPMC channel {self.mpmc_id}, mpsc_id: {mpsc_id}"
                )
                try:
                    return Result.new_ok(producer._new_or_get_mpsc_producer(mpsc_id))
                except Exception as e:
                    return Result.new_error(
                        ChanBindError(
                            f"Failed to bind existing MPSC producer for "
                            f"mpmc_id={self.mpmc_id}, mpsc_id={mpsc_id}: {e}"
                        )
                    )
            raise ValueError("consumer path should use _try_bind_existing_unready_consumer")

        def try_existing_channels(
            ready_channels: List[str], unready_channels: List[str]
        ) -> Optional[Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError]]:
            if producer is not None:
                try:
                    mpsc_producer = producer._get_next_channel_from_heap(ready_channels, unready_channels)
                except Exception as e:
                    return Result.new_error(
                        ChanBindError(
                            f"Failed to lazily bind ready MPSC producer for "
                            f"mpmc_id={self.mpmc_id}: {e}"
                        )
                    )
                if mpsc_producer is not None:
                    logging.debug(
                        f"{tag} Successfully got next available MPSC producer from heap for MPMC channel {self.mpmc_id}"
                    )
                    return Result.new_ok(mpsc_producer)

            if not unready_channels:
                return None

            if producer is None:
                logging.debug(
                    f"{tag} Try claiming existing unready mpsc for MPMC consumer {self.mpmc_id}: {unready_channels}"
                )
                return self._try_bind_existing_unready_consumer(
                    api,
                    chan_config,
                    unready_channels,
                )

            logging.debug(
                f"{tag} Try getting existing mpsc for MPMC {chan_role} {self.mpmc_id} from unready channels"
            )
            res = get_existing_channel(unready_channels[0])
            if res.is_ok():
                logging.debug(
                    f"{tag} Successfully got existing MPSC {chan_role} for MPMC channel {self.mpmc_id}"
                )
                return res
            logging.warning(
                f"{tag} Failed to get existing channel for MPMC channel {self.mpmc_id}, error: {res.unwrap_error()}"
            )
            return None

        if producer is not None:
            # Producer hot path should prefer local ready-cache maintained by
            # the background watch. Synchronous etcd refresh stays as the
            # authority fallback when the local snapshot cannot yield a route.
            ready_channels = self.get_ready_channels()
            unready_channels = self.unready_channels
            logging.debug(f"{tag} Local ready snapshot: ready={ready_channels}, unready={unready_channels}")

            existing_result = try_existing_channels(ready_channels, unready_channels)
            if existing_result is not None:
                return existing_result

            logging.debug(
                f"{tag} Local producer snapshot miss for MPMC channel {self.mpmc_id}, refreshing authority state"
            )

        logging.debug(f"{tag} Refreshing ready/unready state for MPMC channel {self.mpmc_id}")
        self._refresh_local_ready_state(self.etcd_client)
        ready_channels = self.get_ready_channels()
        unready_channels = self.unready_channels
        logging.debug(f"{tag} Ready channels: {ready_channels}, Unready channels: {unready_channels}")

        existing_result = try_existing_channels(ready_channels, unready_channels)
        if existing_result is not None:
            return existing_result

        logging.debug(f"{tag} No usable existing channels, will try creating a new mpsc")

        # Create new channel
        create_result = self.try_create_mpsc_channel(
            api,
            chan_config,
            ChanRole.PRODUCER if producer is not None else ChanRole.CONSUMER,
        )

        if create_result.is_ok():
            mpsc_object = create_result.unwrap()
            if producer is not None:
                assert isinstance(mpsc_object, MPSCChanProducer)
                producer._record_mpsc_producer(mpsc_object)
            logging.debug(
                f"{tag} Successfully created new MPSC {chan_role} for MPMC channel {self.mpmc_id}"
            )
            return Result.new_ok(mpsc_object)

        create_error = create_result.unwrap_error()
        if (
            producer is not None
            and isinstance(create_error, ChanCreateError)
            and create_error.message == "Producer can only create the first channel"
        ):
            # Losing the create race means another member already published a sub-MPSC.
            logging.debug(
                f"{tag} Producer lost create race for MPMC channel {self.mpmc_id}; refresh and retry existing-channel bind"
            )
            self._refresh_local_ready_state(self.etcd_client)
            ready_channels = self.get_ready_channels()
            unready_channels = self.unready_channels
            logging.debug(
                f"{tag} After lost create race refresh MPMC channel {self.mpmc_id}: ready={ready_channels}, unready={unready_channels}"
            )
            existing_result = try_existing_channels(ready_channels, unready_channels)
            if existing_result is not None:
                return existing_result

        logging.warning(
            f"{tag} Failed to create new channel for MPMC channel {self.mpmc_id}, error: {create_error}"
        )
        return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(
            create_error
        )
        
        # return Result[Union[MPSCChanConsumer, MPSCChanProducer]].new_error(ChanCreateError("Failed to create new channel after all attempts, errors: " + str(fail_results)))
    
    def try_create_mpsc_channel(
            self,
            api: KvClient,
            chan_config: Dict[str, int],
            chan_role: ChanRole,
        ) -> Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError]:
        """
        Try to create a new MPSC channel and add it to this MPMC channel.
        Uses etcd lock to ensure atomic creation.
        Producer: only create if this is the first channel
        Consumer: only create if active consumer count > existing MPSC channels
        
        Args:
            api(KvClient): KV store API (required for creating MPSC objects)
            api(KvClient): KV store API (required for creating MPSC objects)
            chan_config(Dict[str, int]): Channel configuration (required for creating MPSC objects)
            chan_role(ChanRole): Channel role (PRODUCER or CONSUMER)
            
        Returns:
            Result[Union[MPSCChanConsumer, MPSCChanProducer]]: New MPSC object
        """
        lock_key = f"/mpmc_channels/{self.mpmc_id}/create_lock"
        published_channel: Optional[Union[MPSCChanConsumer, MPSCChanProducer]] = None
        unpublished_channel: Optional[Union[MPSCChanConsumer, MPSCChanProducer]] = None

        def rollback_unpublished_channel(
            channel: Union[MPSCChanConsumer, MPSCChanProducer],
            *,
            reason: str,
        ) -> Result[OkNone, ApiError]:
            try:
                rollback_result = channel._rollback_unpublished_channel()
                if not rollback_result.is_ok():
                    logging.warning(
                        "Failed to roll back unpublished MPSC after %s for mpmc_id=%s: %s",
                        reason,
                        self.mpmc_id,
                        rollback_result.unwrap_error(),
                    )
                    return Result.new_error(rollback_result.unwrap_error())
                rollback_result.unwrap()
                return Result.new_ok(OK_NONE)
            except Exception as close_error:
                error = ResourceCleanupError(
                    message=(
                        f"exception while rolling back unpublished MPSC after {reason}: "
                        f"{type(close_error).__name__}: {close_error}"
                    ),
                    resource_type="mpsc_channel",
                    resource_id=channel.get_chan_id(),
                )
                logging.warning("%s", error)
                return Result.new_error(error)

        def channels_snapshot_compare(channels_key: str, expected_value: Optional[bytes]):
            if expected_value is None:
                return self.etcd_client.transactions.create(channels_key) == 0
            return self.etcd_client.transactions.value(channels_key) == expected_value

        try:
            with EtcdLock(
                self._etcd_endpoints,
                lock_key,
                MPMC_CREATE_LOCK_TTL_SECONDS,
                MPMC_CREATE_LOCK_TIMEOUT_SECONDS,
            ):
                # Keep the raw value from the same read. The create lock is an
                # optimization only: its lease may expire during costly MPSC
                # construction, so publication must compare this exact snapshot.
                current_mpscs, expected_channels_value = self._read_mpsc_channels_snapshot(
                    self.etcd_client
                )
                
                # Role-specific constraints
                if chan_role == ChanRole.PRODUCER:
                    # Producer: only create if this is the first channel
                    if current_mpscs:
                        return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(ChanCreateError("Producer can only create the first channel"))
                elif chan_role == ChanRole.CONSUMER:
                    # Consumer: lock-free snapshots are not authoritative. Re-read
                    # under the create lock and claim any existing unready MPSC
                    # before deciding to allocate a new one.
                    ready_channel_set = set(self.get_remote_ready_channels())
                    current_unready_mpscs = [
                        mpsc_id for mpsc_id in current_mpscs if mpsc_id not in ready_channel_set
                    ]
                    if current_unready_mpscs:
                        logging.debug(
                            "Consumer create-lock recheck found existing unready MPSCs for "
                            "mpmc_id=%s: %s",
                            self.mpmc_id,
                            current_unready_mpscs,
                        )
                        existing_consumer_res = self._try_bind_existing_unready_consumer(
                            api,
                            chan_config,
                            current_unready_mpscs,
                        )
                        if existing_consumer_res is not None:
                            return existing_consumer_res

                    # Only create if the lock-protected recheck still found no
                    # claimable unready channel and active consumers outnumber
                    # the existing sub-MPSC count.
                    active_consumers = len(
                        self.get_active_member_ids(ChanRole.CONSUMER)
                    )
                    if active_consumers <= len(current_mpscs):
                        return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(ChanCreateError("Not enough active consumers to create new channel"))
                else:
                    return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(ChanCreateError(f"Invalid channel role: {chan_role}"))
                
                # Create MPSC object (let it handle its own ID allocation)
                if chan_role == ChanRole.PRODUCER:
                    logging.debug(f"Creating new MPSC producer for MPMC channel {self.mpmc_id}")
                    mpsc_producer = MPSCChanProducer(
                        api,
                        None,
                        chan_config,
                        self.etcd_client,
                        # Tie producer membership keys to per-member lease so restarts do not collide.
                        self.mpmc_member_lease,
                        # Keep channel meta stable under the shared/global lease.
                        self.mpmc_global_lease,
                        # IMPORTANT: Always reuse the MPMC-shared kv payload lease for
                        # newly created sub MPSC channels so all producers bind payload
                        # keys under the SAME lease. Not doing this would cause subtle
                        # payload lease "split" across sub-channels even though they
                        # belong to the same MPMC. This is not a fallback; it's a
                        # required invariant for MPMC semantics.
                        override_payload_lease_id=self.payload_lease_id,
                        parent_mpmc_id_opt=self.mpmc_id,
                        parent_mpmc_member_id_opt=self.mpmc_member_id,
                        _parent_shutdown_ctl=self.shutdown_ctl,
                    )
                    unpublished_channel = mpsc_producer
                    mpsc_id = mpsc_producer.chan_id
                    channels_key = _new_mpmc_mpsc_channels_key(self.mpmc_id)
                    published_mpscs = current_mpscs + [mpsc_id]
                    try:
                        success, _ = self.etcd_client.transaction(
                            compare=[
                                channels_snapshot_compare(
                                    channels_key,
                                    expected_channels_value,
                                )
                            ],
                            success=[
                                self.etcd_client.transactions.put(
                                    channels_key,
                                    json.dumps(published_mpscs).encode(),
                                    self.mpmc_global_lease,
                                )
                            ],
                            failure=[],
                        )
                    except Exception:
                        rollback_result = rollback_unpublished_channel(
                            mpsc_producer,
                            reason="producer publish exception",
                        )
                        unpublished_channel = None
                        if not rollback_result.is_ok():
                            raise RuntimeError(
                                "producer publish failed and unpublished MPSC rollback failed: "
                                f"{rollback_result.unwrap_error()}"
                            )
                        rollback_result.unwrap()
                        raise
                    if not success:
                        rollback_result = rollback_unpublished_channel(
                            mpsc_producer,
                            reason="producer channel-list CAS conflict",
                        )
                        unpublished_channel = None
                        if not rollback_result.is_ok():
                            return Result.new_error(rollback_result.unwrap_error())
                        rollback_result.unwrap()
                        return Result[
                            Union[MPSCChanConsumer, MPSCChanProducer], ApiError
                        ].new_error(
                            ChanCreateError("Producer can only create the first channel")
                        )
                    published_channel = mpsc_producer
                    unpublished_channel = None
                    return Result.new_ok(mpsc_producer)
                elif chan_role == ChanRole.CONSUMER:
                    logging.debug(f"Creating new MPSC consumer for MPMC channel {self.mpmc_id}")
                    mpsc_consumer: Optional[MPSCChanConsumer] = None
                    try:
                        mpsc_consumer = MPSCChanConsumer(
                            api,
                            None,
                            chan_config,
                            self.etcd_client,
                            # Tie consumer membership keys to per-member lease so restarts do not collide.
                            self.mpmc_member_lease,
                            # Keep channel meta stable under the shared/global lease.
                            self.mpmc_global_lease,
                            # Match producer-side semantics: new sub MPSC must reuse
                            # the shared kv payload lease of the parent MPMC.
                            override_payload_lease_id=self.payload_lease_id,
                            parent_mpmc_id_opt=self.mpmc_id,
                            parent_mpmc_member_id_opt=self.mpmc_member_id,
                        )
                    except Exception as e:
                        logging.error(f"Fatal error creating MPSC consumer for MPMC channel {self.mpmc_id}: {e}")
                        return Result[
                            Union[MPSCChanConsumer, MPSCChanProducer], ApiError
                        ].new_error(
                            ChanCreateError(
                                f"Failed to create MPSC consumer when try_create_mpsc_channel: {e}"
                            )
                        )
                    assert mpsc_consumer is not None
                    unpublished_channel = mpsc_consumer
                    mpsc_id = mpsc_consumer.chan_id
                    channels_key = _new_mpmc_mpsc_channels_key(self.mpmc_id)
                    ready_key = _new_mpmc_ready_channel_key(self.mpmc_id, mpsc_id)
                    for publish_attempt in count(1):
                        published_mpscs = current_mpscs + [mpsc_id]
                        try:
                            success, _ = self.etcd_client.transaction(
                                compare=[
                                    channels_snapshot_compare(
                                        channels_key,
                                        expected_channels_value,
                                    ),
                                    self.etcd_client.transactions.create(ready_key) == 0,
                                ],
                                success=[
                                    self.etcd_client.transactions.put(
                                        channels_key,
                                        json.dumps(published_mpscs).encode(),
                                        self.mpmc_global_lease,
                                    ),
                                    self.etcd_client.transactions.put(
                                        ready_key,
                                        str(self.mpmc_member_id).encode(),
                                        self.mpmc_member_lease,
                                    ),
                                ],
                                failure=[],
                            )
                        except Exception:
                            rollback_result = rollback_unpublished_channel(
                                mpsc_consumer,
                                reason="consumer publish exception",
                            )
                            unpublished_channel = None
                            if not rollback_result.is_ok():
                                raise RuntimeError(
                                    "consumer publish failed and unpublished MPSC rollback failed: "
                                    f"{rollback_result.unwrap_error()}"
                                )
                            rollback_result.unwrap()
                            raise

                        if success:
                            mpsc_consumer._mpmc_ready_claimed = True
                            published_channel = mpsc_consumer
                            unpublished_channel = None
                            logging.debug(
                                "Created new MPSC consumer %s for MPMC channel %s "
                                "after %s publish attempt(s)",
                                mpsc_id,
                                self.mpmc_id,
                                publish_attempt,
                            )
                            return Result.new_ok(mpsc_consumer)

                        latest_mpscs, latest_channels_value = self._read_mpsc_channels_snapshot(
                            self.etcd_client
                        )
                        if mpsc_id in latest_mpscs:
                            rollback_result = rollback_unpublished_channel(
                                mpsc_consumer,
                                reason="unexpected duplicate channel id after CAS conflict",
                            )
                            unpublished_channel = None
                            if not rollback_result.is_ok():
                                return Result.new_error(rollback_result.unwrap_error())
                            rollback_result.unwrap()
                            return Result[
                                Union[MPSCChanConsumer, MPSCChanProducer], ApiError
                            ].new_error(
                                ChanCreateError(
                                    f"MPSC channel {mpsc_id} appeared in MPMC channel "
                                    f"{self.mpmc_id} without its atomic ready publication"
                                )
                            )

                        current_mpsc_set = set(current_mpscs)
                        latest_mpsc_set = set(latest_mpscs)
                        if (
                            len(latest_mpscs) <= len(current_mpscs)
                            or not current_mpsc_set.issubset(latest_mpsc_set)
                        ):
                            rollback_result = rollback_unpublished_channel(
                                mpsc_consumer,
                                reason="non-monotonic channel-list CAS conflict",
                            )
                            unpublished_channel = None
                            if not rollback_result.is_ok():
                                return Result.new_error(rollback_result.unwrap_error())
                            rollback_result.unwrap()
                            return Result[
                                Union[MPSCChanConsumer, MPSCChanProducer], ApiError
                            ].new_error(
                                ChanCreateError(
                                    "MPMC channel list did not advance monotonically after "
                                    f"publish conflict for mpmc_id={self.mpmc_id}"
                                )
                            )

                        # A concurrent creator advanced the append-only list.
                        # Prefer its unclaimed channel before retrying this new one.
                        latest_ready_set = set(self.get_remote_ready_channels())
                        latest_unready_mpscs = [
                            candidate
                            for candidate in latest_mpscs
                            if candidate not in latest_ready_set
                        ]
                        if latest_unready_mpscs:
                            rollback_result = rollback_unpublished_channel(
                                mpsc_consumer,
                                reason="new unready channel won publish race",
                            )
                            unpublished_channel = None
                            if not rollback_result.is_ok():
                                return Result.new_error(rollback_result.unwrap_error())
                            rollback_result.unwrap()
                            existing_consumer_res = (
                                self._try_bind_existing_unready_consumer(
                                    api,
                                    chan_config,
                                    latest_unready_mpscs,
                                )
                            )
                            if existing_consumer_res is not None:
                                return existing_consumer_res
                            return Result[
                                Union[MPSCChanConsumer, MPSCChanProducer], ApiError
                            ].new_error(
                                ChanCreateError(
                                    "Lost ready-claim race after MPMC channel-list "
                                    f"publish conflict for mpmc_id={self.mpmc_id}"
                                )
                            )

                        active_consumers = len(
                            self.get_active_member_ids(ChanRole.CONSUMER)
                        )
                        if active_consumers <= len(latest_mpscs):
                            rollback_result = rollback_unpublished_channel(
                                mpsc_consumer,
                                reason="channel list already covers active consumers",
                            )
                            unpublished_channel = None
                            if not rollback_result.is_ok():
                                return Result.new_error(rollback_result.unwrap_error())
                            rollback_result.unwrap()
                            return Result[
                                Union[MPSCChanConsumer, MPSCChanProducer], ApiError
                            ].new_error(
                                ChanCreateError(
                                    "Not enough active consumers to create new channel"
                                )
                            )

                        logging.debug(
                            "Retrying MPSC consumer publication after channel-list CAS "
                            "conflict: mpmc_id=%s mpsc_id=%s attempt=%s old_count=%s "
                            "new_count=%s active_consumers=%s",
                            self.mpmc_id,
                            mpsc_id,
                            publish_attempt,
                            len(current_mpscs),
                            len(latest_mpscs),
                            active_consumers,
                        )
                        current_mpscs = latest_mpscs
                        expected_channels_value = latest_channels_value
                
        except Exception as e:
            if published_channel is not None:
                logging.warning(
                    "MPMC create lock cleanup failed after atomic publication; "
                    "keeping published channel: mpmc_id=%s mpsc_id=%s error=%s",
                    self.mpmc_id,
                    published_channel.chan_id,
                    e,
                )
                return Result.new_ok(published_channel)
            if unpublished_channel is not None:
                rollback_result = rollback_unpublished_channel(
                    unpublished_channel,
                    reason="unexpected create-path exception",
                )
                unpublished_channel = None
                if not rollback_result.is_ok():
                    return Result.new_error(rollback_result.unwrap_error())
                rollback_result.unwrap()
            return Result[Union[MPSCChanConsumer, MPSCChanProducer], ApiError].new_error(ChanCreateError(f"Failed to create MPSC channel: {e}"))
    
    def get_active_member_ids(self, role: ChanRole) -> List[int]:
        """Return active MPMC member ids for one role."""

        role_key_prefix = _new_mpmc_role_key_prefix(self.mpmc_id, role) + "/"
        member_ids: List[int] = []
        for _, meta in self.etcd_client.get_prefix(role_key_prefix):
            member_ids.append(
                _extract_mpmc_member_id_from_role_key(meta.key, self.mpmc_id, role)
            )
        return sorted(member_ids)
    
    
    @staticmethod
    def new_global_mpmc_channel(
        chan_config: Dict[str, int],
        etcd_client: etcd3.Etcd3Client,
        role: ChanRole,
        new_ready_channels_callback: Optional[Callable[[List[str]], None]],
        remove_ready_channels_callback: Optional[Callable[[List[str]], None]],
        kv_api: KvClient,
        shutdown_ctl: "MqShutdownCtl",
    ) -> "MPMCChannel":
        """
        Create a new MPMC channel with available ID.
        
        This function is used for the FIRST creation of an entire MPMC channel.
        It should only be called when the overall MPMC metadata has not been registered yet.
        
        Args:
            chan_config(Dict[str, int]): Channel configuration
            etcd_client(etcd3.Etcd3Client): Etcd client
            
        Returns:
            MPMCChannel: New MPMC channel
            
        Raises:
            ValueError: If too many MPMC channels are created
        """
        # Validate config (strict required fields).
        chan_config = validate_mpmc_config(chan_config, role=role)
        # Phase 1: allocate id with a short temporary lease context (not bound to counter)
        temp_lease = etcd_client.lease(30)
        allocator = DistributeIdAllocator(etcd_client, "mpmc_channels", temp_lease)
        id_res = allocator.allocate_id()
        if not id_res.is_ok():
            raise ValueError(f"Failed to allocate MPMC id: {id_res.unwrap_error()}")
        mpmc_id_int = id_res.unwrap()
        assert mpmc_id_int is not None
        mpmc_id = str(mpmc_id_int)

        # Phase 2: allocate a fresh long-lived etcd lease for id allocator (meta-owned).
        # We must probe/register it before any txn/put uses it.
        if kv_api is None:
            raise ValueError("kv_api is required to register cluster long lease for new MPMC channel")
        endpoints = kv_api.get_etcd_config()
        cluster_long_lease = etcd_client.lease(30 * 60)
        cluster_long_lease_handle = LeaseManagerHandle().register_etcd_lease(
            endpoints,
            30 * 60,
            int(cluster_long_lease.id),
            register_by=f"mpmc_id_allocator_cluster_long:{mpmc_id}",
        )
        allocator.update_lease(cluster_long_lease)

        metadata_lease = etcd_client.lease(int(chan_config["ttl_seconds"]))

        # Allocate payload lease in advance so we can persist it inside meta
        ttl = int(chan_config["ttl_seconds"])
        if kv_api is None:
            raise ValueError("kv_api is required to allocate payload lease for new MPMC channel")
        assert isinstance(kv_api, KvLeaseApi)
        res = kv_api.allocate_lease(ttl)
        if not res.is_ok():
            raise ValueError(f"Failed to allocate payload lease for new MPMC channel: {res.unwrap_error()}")
        payload_lease_id = res.unwrap()
        assert isinstance(payload_lease_id, int) and payload_lease_id > 0

        # Use transaction to create meta and related keys for the allocated id
        meta_key = _new_mpmc_meta_key(mpmc_id)
        next_id_key = _new_mpmc_next_channel_id_key(mpmc_id)
        success, _ = etcd_client.transaction(
            compare=[
                etcd_client.transactions.create(meta_key) == 0,
                etcd_client.transactions.create(next_id_key) == 0,
            ],
            success=[
                etcd_client.transactions.put(meta_key, json.dumps({
                    "capacity": chan_config["capacity"],
                    "ttl_seconds": chan_config["ttl_seconds"],
                    "created_at": time.time(),
                    "metadata_lease_id": int(metadata_lease.id),
                    # Save payload lease id into meta for discoverability
                    "payload_lease_id": payload_lease_id,
                    # Save id-allocator cluster lease id into meta for discoverability
                    "id_allocator_cluster_lease_id": int(cluster_long_lease.id),
                }).encode(), metadata_lease),
                # next_channel_id is a per-channel key; it must expire with the channel metadata.
                etcd_client.transactions.put(next_id_key, b"0", metadata_lease),
            ],
            failure=[]
        )
        if not success:
            raise ValueError(f"Failed to create meta for MPMC channel {mpmc_id}")

        logging.debug(
            f"Published payload lease id={payload_lease_id} for new MPMC {mpmc_id} (saved in meta)"
        )

        # Create MPMC channel with the prepared metadata lease
        # Use FactoryOnly gate to construct instance
        MPMCChannel._allow_init = True
        try:
            mpmc_channel = MPMCChannel(
                mpmc_id,
                chan_config,
                etcd_client,
                role,
                new_ready_channels_callback,
                remove_ready_channels_callback,
                metadata_lease,
                kv_api,
                payload_lease_id,
                shutdown_ctl,
                int(cluster_long_lease.id),
                cluster_long_lease_handle,
                True,
            )
        except Exception as e:
            logging.warning(
                f"failed to construct MPMCChannel(id={mpmc_id}, role={role}): {e}"
            )
            raise
        finally:
            MPMCChannel._allow_init = False

        logging.debug(f"Created new MPMC channel {mpmc_id} with cluster long lease {cluster_long_lease.id}")
        return mpmc_channel

    @staticmethod
    def new_existed_global_mpmc_channel(
        mpmc_id: str,
        chan_config: Dict[str, int],
        etcd_client: etcd3.Etcd3Client,
        role: ChanRole,
        new_ready_channels_callback: Optional[Callable[[List[str]], None]],
        remove_ready_channels_callback: Optional[Callable[[List[str]], None]],
        kv_api: KvClient,
        shutdown_ctl: "MqShutdownCtl",
    ) -> "MPMCChannel":
        """
        Attach to an existing global MPMC channel by id.

        This does not create any metadata or fallback leases. It reads stored
        metadata (including payload lease id stored in MPMC meta) and the
        metadata lease id, then
        constructs the channel using those. If required metadata is missing,
        it raises, because the channel is expected to have been created via
        new_global_mpmc_channel.

        Args:
            mpmc_id(int): Existing MPMC channel id
            chan_config(Dict[str, int]): Channel configuration
            etcd_client(etcd3.Etcd3Client): Etcd client
        """
        # Validate config (strict required fields).
        chan_config = validate_mpmc_config(chan_config, role=role)
        # Validate meta presence and parse cluster-lease id for id allocator
        meta_key = _new_mpmc_meta_key(mpmc_id)
        meta_data, _ = etcd_client.get(meta_key)
        if meta_data is None:
            raise ValueError(f"MPMC meta not found for id={mpmc_id}")
        try:
            meta_obj = json.loads(meta_data.decode())
        except Exception as e:
            raise ValueError(f"MPMC meta is not valid JSON for id={mpmc_id}: {e}")
        cluster_lease_id_val = meta_obj.get("id_allocator_cluster_lease_id")
        if not (isinstance(cluster_lease_id_val, int) and cluster_lease_id_val > 0):
            raise ValueError(
                f"MPMC {mpmc_id} meta missing valid 'id_allocator_cluster_lease_id' for existing channel attach"
            )
        cluster_lease_id_from_meta: int = cluster_lease_id_val
        # Payload lease id must exist in meta for existing channel attach
        payload_lease_val = meta_obj.get("payload_lease_id")
        if not (isinstance(payload_lease_val, int) and payload_lease_val > 0):
            raise ValueError(
                f"MPMC {mpmc_id} meta missing valid 'payload_lease_id' for existing channel attach"
            )
        payload_lease_id_from_meta: int = payload_lease_val

        metadata_lease_id_val = meta_obj.get("metadata_lease_id")
        if not (isinstance(metadata_lease_id_val, int) and metadata_lease_id_val > 0):
            raise ValueError(
                f"MPMC {mpmc_id} meta missing valid 'metadata_lease_id' for existing channel attach"
            )
        metadata_lease_id = int(metadata_lease_id_val)

        mpmc_global_lease = etcd3.Lease(
            metadata_lease_id,
            int(chan_config["ttl_seconds"]),
            etcd_client,
        )

        if kv_api is None:
            raise ValueError("kv_api is required for existing MPMC channel attach")
        if not isinstance(kv_api, KvLeaseApi):
            raise ValueError("kv_api must implement KvLeaseApi for existing MPMC channel attach")

        def _metadata_lease_is_valid(lease_id: int) -> bool:
            try:
                info = etcd_client.get_lease_info(int(lease_id))
            except Exception as e:  # noqa: BLE001
                if "not found" in str(e).lower():
                    return False
                raise ValueError(
                    f"MPMC {mpmc_id} get_lease_info failed for metadata_lease_id={int(lease_id)}: {e}"
                ) from e
            ttl_val = getattr(info, "TTL", None)
            if not isinstance(ttl_val, int):
                raise ValueError(
                    f"MPMC {mpmc_id} invalid lease TTL type for metadata_lease_id={int(lease_id)}: {ttl_val!r}"
                )
            return ttl_val > 0

        metadata_ok = _metadata_lease_is_valid(metadata_lease_id)
        # Payload lease liveness is validated by keepalive registration during
        # MPMCChannel construction. Existing attaches are top-level members and
        # contribute keepalive for the shared payload lease.
        # The id-allocator cluster lease is required for correct membership/id allocation semantics.
        # Treat it as part of the MPMC meta contract: if it is dead, the meta is stale.
        def _id_allocator_cluster_lease_is_valid(lease_id: int) -> bool:
            try:
                info = etcd_client.get_lease_info(int(lease_id))
            except Exception as e:  # noqa: BLE001
                if "not found" in str(e).lower():
                    return False
                raise ValueError(
                    f"MPMC {mpmc_id} get_lease_info failed for id_allocator_cluster_lease_id={int(lease_id)}: {e}"
                ) from e
            ttl_val = getattr(info, "TTL", None)
            if not isinstance(ttl_val, int):
                raise ValueError(
                    f"MPMC {mpmc_id} invalid lease TTL type for id_allocator_cluster_lease_id={int(lease_id)}: {ttl_val!r}"
                )
            return ttl_val > 0

        id_alloc_ok = _id_allocator_cluster_lease_is_valid(int(cluster_lease_id_from_meta))
        if not metadata_ok:
            raise InvalidConfigurationError(
                message=(
                    "MPMC meta is stale and cannot be bound safely. "
                    f"mpmc_id={mpmc_id} metadata_lease_id={metadata_lease_id} metadata_ok={metadata_ok}. "
                    "Delete the stale MPMC meta and unique mapping, then recreate a new MPMC channel."
                ),
                config_key="mpmc_meta_stale",
            )
        if not id_alloc_ok:
            raise InvalidConfigurationError(
                message=(
                    "MPMC meta is stale and cannot be bound safely. "
                    f"mpmc_id={mpmc_id} id_allocator_cluster_lease_id={int(cluster_lease_id_from_meta)} id_alloc_ok={id_alloc_ok}. "
                    "Delete the stale MPMC meta and unique mapping, then recreate a new MPMC channel."
                ),
                config_key="mpmc_meta_stale",
            )

        # Existing attaches are per-member endpoints. The creator owns shared
        # metadata/payload/id-allocator keepalives; each attach only needs its
        # own member lease to publish membership.
        # Use FactoryOnly gate to construct instance
        MPMCChannel._allow_init = True
        try:
            channel = MPMCChannel(
                mpmc_id,
                chan_config,
                etcd_client,
                role,
                new_ready_channels_callback,
                remove_ready_channels_callback,
                mpmc_global_lease,
                kv_api,
                payload_lease_id_from_meta,
                shutdown_ctl,
                cluster_lease_id_from_meta,
                None,
                False,
            )
            return channel
        except Exception as e:
            logging.warning(
                f"failed to construct existing MPMCChannel(id={mpmc_id}, role={role}): {e}"
            )
            raise
        finally:
            MPMCChannel._allow_init = False
    
    def start_watching(self) -> None:
        """Start the ready-channel watch on a dedicated gRPC client."""

        with self._watch_lock:
            if self.watch_thread is not None and self.watch_thread.is_alive():
                return
            endpoint = self._etcd_endpoints[0]
            host, port_str = endpoint.split(":")
            watch_client = etcd3.client(
                host=host,
                port=int(port_str),
                timeout=MPMC_WATCH_RPC_TIMEOUT_SECONDS,
            )
            self.stop_flag.clear()
            self._watch_client = watch_client
            self._watch_stream = None
            self._watch_request_stop = None
            self.watch_thread = threading.Thread(
                target=self._watch_channels,
                args=(watch_client,),
                name=f"mpmc-ready-watch-{self.mpmc_id}",
                daemon=True,
            )
            self.watch_thread.start()

    def _watch_channels(self, watch_client: etcd3.Etcd3Client) -> None:
        """Own and process the ready-channel gRPC stream until shutdown."""

        ready_prefix = _new_mpmc_ready_channels_prefix(self.mpmc_id)
        ready_prefix_bytes = etcd_utils.to_bytes(ready_prefix)
        ready_range_end = etcd_utils.increment_last_byte(ready_prefix_bytes)

        try:
            while not self.stop_flag.is_set():
                request_stop = threading.Event()
                watch_stream: Optional[Any] = None

                create_request = etcdrpc.WatchCreateRequest(
                    key=ready_prefix_bytes,
                    range_end=ready_range_end,
                )
                watch_request = etcdrpc.WatchRequest(create_request=create_request)

                def watch_requests(
                    request: Any = watch_request,
                    stop: threading.Event = request_stop,
                ):
                    yield request
                    while not self.stop_flag.is_set() and not stop.wait(0.1):
                        pass

                try:
                    watch_stub = etcdrpc.WatchStub(watch_client.channel)
                    watch_stream = watch_stub.Watch(
                        watch_requests(),
                        credentials=watch_client.call_credentials,
                        metadata=watch_client.metadata,
                    )
                    with self._watch_lock:
                        if self.stop_flag.is_set():
                            request_stop.set()
                            watch_stream.cancel()
                            break
                        self._watch_stream = watch_stream
                        self._watch_request_stop = request_stop

                    watch_created = False
                    for response in watch_stream:
                        if self.stop_flag.is_set():
                            break
                        if response.created:
                            if response.compact_revision != 0:
                                raise RuntimeError(
                                    "ready-channel watch creation was compacted at revision "
                                    f"{response.compact_revision}"
                                )
                            self._refresh_local_ready_state(watch_client)
                            watch_created = True
                        if response.canceled:
                            raise RuntimeError(
                                "ready-channel watch was canceled by etcd: "
                                f"{response.cancel_reason or 'no reason provided'}"
                            )
                        if response.events and not watch_created:
                            raise RuntimeError(
                                "ready-channel watch delivered events before its creation response"
                            )
                        for event in response.events:
                            if self.stop_flag.is_set():
                                break
                            self._handle_channel_event(event, watch_client)

                    if not self.stop_flag.is_set():
                        raise RuntimeError("ready-channel watch stream ended unexpectedly")
                except Exception as e:  # noqa: BLE001
                    if not self.stop_flag.is_set():
                        logging.warning(
                            "MPMC channel %s watch iteration failed: %s",
                            self.mpmc_id,
                            e,
                        )
                        self.stop_flag.wait(1.0)
                finally:
                    request_stop.set()
                    if watch_stream is not None:
                        try:
                            watch_stream.cancel()
                        except Exception as e:  # noqa: BLE001
                            logging.debug(
                                "MPMC channel %s watch stream cancel cleanup failed: %s",
                                self.mpmc_id,
                                e,
                            )
                    with self._watch_lock:
                        if self._watch_stream is watch_stream:
                            self._watch_stream = None
                        if self._watch_request_stop is request_stop:
                            self._watch_request_stop = None
        finally:
            try:
                watch_client.close()
            except Exception as e:  # noqa: BLE001
                logging.debug("MPMC channel %s watch client close failed: %s", self.mpmc_id, e)
            with self._watch_lock:
                if self._watch_client is watch_client:
                    self._watch_client = None
                if self.watch_thread is threading.current_thread():
                    self.watch_thread = None

    def _refresh_local_ready_state(self, client: etcd3.Etcd3Client) -> None:
        """Refresh the local ready/unready snapshot from one explicit client."""

        old_ready_channels = self.get_ready_channels()
        new_channels = self._read_remote_ready_channels(client)
        all_channels, _ = self._read_mpsc_channels_snapshot(client)
        self.set_ready_channels(new_channels)
        self.unready_channels = [
            mpsc_id for mpsc_id in all_channels if mpsc_id not in set(new_channels)
        ]

        if self.new_ready_channels_callback is not None:
            added_ready_channels = [
                channel for channel in new_channels if channel not in old_ready_channels
            ]
            self.new_ready_channels_callback(added_ready_channels)
        if self.remove_ready_channels_callback is not None:
            removed_ready_channels = [
                channel for channel in old_ready_channels if channel not in new_channels
            ]
            self.remove_ready_channels_callback(removed_ready_channels)

    def _handle_channel_event(
        self,
        event: Any,
        client: etcd3.Etcd3Client,
    ) -> None:
        logging.debug(
            "MPMC channel %s event %s triggered ready-state refresh",
            self.mpmc_id,
            event,
        )
        self._refresh_local_ready_state(client)

    def stop_watching(self) -> None:
        """Cancel and join the owned watch stream before releasing its client."""

        with self._watch_lock:
            self.stop_flag.set()
            watch_thread = self.watch_thread
            watch_client = self._watch_client
            watch_stream = self._watch_stream
            request_stop = self._watch_request_stop
        if request_stop is not None:
            request_stop.set()
        if watch_stream is not None:
            try:
                watch_stream.cancel()
            except Exception as e:  # noqa: BLE001
                logging.warning(f"MPMC channel {self.mpmc_id} cancel watch stream failed: {e}")
        if watch_thread is not None:
            watch_thread.join(timeout=MPMC_WATCH_STOP_TIMEOUT_SECONDS)
            if watch_thread.is_alive():
                raise RuntimeError(
                    f"MPMC channel {self.mpmc_id} watch thread did not stop within "
                    f"{MPMC_WATCH_STOP_TIMEOUT_SECONDS:.1f}s"
                )
        close_client = False
        with self._watch_lock:
            if self.watch_thread is watch_thread:
                self.watch_thread = None
            if self._watch_stream is watch_stream:
                self._watch_stream = None
            if self._watch_request_stop is request_stop:
                self._watch_request_stop = None
            if self._watch_client is watch_client:
                self._watch_client = None
                close_client = watch_client is not None
        if close_client:
            try:
                watch_client.close()
            except Exception as e:  # noqa: BLE001
                logging.warning(f"MPMC channel {self.mpmc_id} close watch client failed: {e}")
    
    def close(self) -> Result[OkNone, ApiError]:
        """Close the MPMC channel.

        The shared shutdown flag is only a stop-request signal. Actual inner
        lease-handle cleanup must be guarded by a dedicated close-done bit,
        otherwise an outer producer/consumer that sets shutdown first would
        cause this channel close to return early and leak the shared leases.
        """

        with self._close_lock:
            if self._close_done:
                return Result.new_ok(OK_NONE)
            self.shutdown_ctl.close()

            try:
                self.stop_watching()
            except Exception as e:  # noqa: BLE001
                return Result.new_error(
                    ResourceCleanupError(
                        message=f"failed to stop MPMC watch thread: {e}",
                        resource_type="mpmc_watch_thread",
                        resource_id=self.mpmc_id,
                    )
                )

            if isinstance(self.mpmc_member_id, int):
                role_key = _new_mpmc_role_key(self.mpmc_id, self.role, self.mpmc_member_id)
                role_cleanup = _delete_owned_etcd_state(
                    self.kv_api,
                    keys=[role_key],
                    prefixes=[],
                    dbg=f"MPMCChannel role cleanup mpmc_id={self.mpmc_id}",
                )
                if not role_cleanup.is_ok():
                    return Result.new_error(role_cleanup.unwrap_error())
                role_cleanup.unwrap()

            self._lm_mpmc_member = None  # type: ignore[assignment]
            self._lm_mpmc_global = None  # type: ignore[assignment]
            self._lm_cluster_long = None  # type: ignore[assignment]
            self._lm_kv_payload = None  # type: ignore[assignment]
            self._close_done = True
            return Result.new_ok(OK_NONE)


class MPMCChanProducer(ChannelProducer):
    """
    MPMC Producer that can produce messages to multiple MPSC channels.
    """
    
    def __init__(
        self,
        api: KvClient,
        mpmc_id: Optional[str],
        chan_config: Dict[str, int],
        etcd_client: Optional[etcd3.Etcd3Client] = None,
    ):
        """
        Initialize MPMC Producer.
        
        Args:
            api(KvClient): KV store API
            api(KvClient): KV store API
            mpmc_id(Optional[str]): MPMC channel ID
            chan_config(Dict[str, int]): Channel configuration
            etcd_client(Optional[etcd3.Etcd3Client]): Etcd client
        """
        assert isinstance(api, KvLeaseApi)

        # Enforce zero-contribution store for channel usage via config
        api.ensure_zero_contribution_for_channel()
        # Validate config strictly (no implicit defaults/fallbacks).
        chan_config = validate_mpmc_config(chan_config, role=ChanRole.PRODUCER)
        self.api = api
        self.mpmc_id = mpmc_id
        self.chan_config = chan_config  # Store for creating MPSC producers
        self.keep_alive_interval = chan_config["ttl_seconds"] / 2 - 0.5
        # Initialize to invalid until a MPSC is actually bound; avoids attribute-missing in close/__del__.
        self.bound_mpsc_id: Optional[str] = None
        self._new_or_get_mpsc_producer_lock = threading.Lock()
        # Shared shutdown controller: used both by this producer and
        # the internal MPMCChannel instance to coordinate close/ops.
        self.shutdown_ctl = MqShutdownCtl()
        self._close_done = False
        self._close_lock = threading.Lock()
        # Shared shutdown controller: used both by this producer and
        # the internal MPMCChannel instance to coordinate close/ops.
        
        if etcd_client is None:
            result: Result[etcd3.Etcd3Client, ApiError] = new_etcd_client(api)
            if not result.is_ok():
                raise ValueError(f"Failed to create etcd client: {result.unwrap_error()}")
            etcd_client = result.unwrap()
            assert etcd_client is not None, "etcd client is None"
        
        self.etcd_client: etcd3.Etcd3Client = etcd_client
        
        # Initialize MPMC channel
        if mpmc_id is not None:
            if not mpmc_id.isdigit():
                raise ValueError(f"invalid mpmc_id: {mpmc_id!r}")
            self.mpmc_channel = MPMCChannel.new_existed_global_mpmc_channel(
                mpmc_id,
                chan_config,
                etcd_client,
                ChanRole.PRODUCER,
                self._new_ready_channels_callback,
                self._remove_ready_channels_callback,
                api,
                self.shutdown_ctl,
            )
        else:
            # Create new MPMC channel
            self.mpmc_channel = MPMCChannel.new_global_mpmc_channel(
                chan_config,
                self.etcd_client,
                ChanRole.PRODUCER,
                self._new_ready_channels_callback,
                self._remove_ready_channels_callback,
                api,
                self.shutdown_ctl,
            )
            self.mpmc_id = self.mpmc_channel.mpmc_id
        

        # Payload lease keepalive is managed by MPMCChannel (shared/common part)

        # Cache per-owner sub-MPSC producers locally so repeated routing within
        # one MPMC producer does not rebind the same sub-channel.
        self.mpsc_producers: Dict[str, MPSCChanProducer] = {}

        # Priority queue for fair channel selection
        self._channel_queue = TimedPriorityQueue()
        self._channel_queue_lock = threading.Lock()

        # Synchronous refresh in get_next_available_channel remains the authority path.
        # Construction has finished at this point, so the producer can rely on
        # the ready-channel watch to keep its local routing snapshot warm.
        self._initialize_priority_queue()
        self.mpmc_channel.start_watching()
        
    # close() is defined later in the class to follow the concurrency pattern
    # (set closed, acquire op-lock, verify closed), see around line ~1386.

    def _load_ready_channels(self, new_ready_channels: List[str]):
        with self._channel_queue_lock:
            for mpsc_id in sorted(new_ready_channels, key=lambda item: int(item)):
                logging.debug("Loading ready channel id lazily: %s", mpsc_id)
                self._channel_queue.update(mpsc_id)

    def _new_ready_channels_callback(self, new_ready_channels: List[str]):
        logging.debug(f"mpmc {self.mpmc_id} producer {self.mpmc_channel.mpmc_member_id} watched new ready channels: {new_ready_channels}")
        logging.debug(f"mpmc {self.mpmc_id} producer {self.mpmc_channel.mpmc_member_id} existing mpsc producers: {self.mpsc_producers.keys()}")
        self._load_ready_channels(new_ready_channels)

    def _remove_ready_channels_callback(self, removed_ready_channels: List[str]):
        with self._channel_queue_lock:
            for mpsc_id in removed_ready_channels:
                self._channel_queue.remove(mpsc_id)

    def _initialize_priority_queue(self):
        """
        Initialize the priority queue with existing ready channels.
        """
        self.mpmc_channel._refresh_local_ready_state(self.mpmc_channel.etcd_client)
        with self._channel_queue_lock:
            for mpsc_id in sorted(self.mpmc_channel.get_ready_channels(), key=lambda item: int(item)):
                self._channel_queue.update(mpsc_id)
            

    def _new_or_get_mpsc_producer(self, mpsc_id: str) -> MPSCChanProducer:
        """
        Create a new MPSC producer or get an existing one.
        """
        with self._new_or_get_mpsc_producer_lock:
            # Re-check closed after acquiring the lock to avoid creating new sub-channels
            # while shutdown is in progress.
            if self.shutdown_ctl.closed:
                raise RuntimeError("MPMCChanProducer is closed; cannot create or fetch MPSC producer")
            if mpsc_id not in self.mpsc_producers:
                logging.debug(
                    "mpmc %s producer %s binding local mpsc producer %s",
                    self.mpmc_id,
                    self.mpmc_channel.mpmc_member_id,
                    mpsc_id,
                )
                mpsc_producer = MPSCChanProducer(
                    self.api,
                    mpsc_id,
                    self.chan_config,
                    self.etcd_client,
                    # Tie producer membership keys to this outer producer member lease.
                    self.mpmc_channel.mpmc_member_lease,
                    # Keep channel meta stable under the shared/global lease.
                    self.mpmc_channel.mpmc_global_lease,
                    override_payload_lease_id=self.mpmc_channel.payload_lease_id,
                    parent_mpmc_id_opt=self.mpmc_id,
                    parent_mpmc_member_id_opt=self.mpmc_channel.mpmc_member_id,
                    _parent_shutdown_ctl=self.shutdown_ctl,
                )

                if self.shutdown_ctl.closed:
                    close_result = mpsc_producer.close()
                    if close_result.is_ok():
                        _ = close_result.unwrap()
                    else:
                        close_error = close_result.unwrap_error()
                        self.mpsc_producers[mpsc_id] = mpsc_producer
                        logging.warning(
                            "mpmc %s retained MPSC producer %s after shutdown cleanup failed: %s",
                            self.mpmc_id,
                            mpsc_id,
                            close_error,
                        )
                        raise RuntimeError(
                            "MPMCChanProducer closed while binding MPSC producer; "
                            f"cleanup failed: {close_error}"
                        )
                    raise RuntimeError(
                        "MPMCChanProducer closed while binding MPSC producer"
                    )
                self.mpsc_producers[mpsc_id] = mpsc_producer

            return self.mpsc_producers[mpsc_id]

    def _update_channel_usage_2_priority_q(self, channel: MPSCChanProducer) -> None:
        """Record the latest use time for ``channel``."""

        with self._channel_queue_lock:
            self._channel_queue.update(channel.chan_id)

    def _get_next_channel_from_heap(self, ready_channels: List[str], unready_channels: List[str]) -> Optional[MPSCChanProducer]:
        """
        Get the next channel from heap, prioritizing ready channels.
        
        Args:
            ready_channels(List[str]): List of ready channel IDs
            unready_channels(List[str]): List of unready channel IDs
            
        Returns:
            Optional[MPSCChanProducer]: MPSC producer, or None if heap is empty
        """

        if not ready_channels:
            return None

        sorted_ready_channels = sorted(ready_channels, key=lambda item: int(item))
        with self._channel_queue_lock:
            self._channel_queue.ensure_tracked(sorted_ready_channels)
            mpsc_id = self._channel_queue.pop_ready(sorted_ready_channels)
            if mpsc_id is None:
                return None
            # Immediately requeue the channel to keep scheduling state local
            # and avoid relying on distant call sites to update usage.
            self._channel_queue.update(mpsc_id)

        producer = self.mpsc_producers.get(mpsc_id)
        if producer is None:
            logging.debug(
                "Binding ready channel lazily for MPMC producer: mpsc_id=%s", mpsc_id
            )
            producer = self._new_or_get_mpsc_producer(mpsc_id)
        return producer
    
    def _record_mpsc_producer(self, mpsc_producer: MPSCChanProducer):
        """
        Record a MPSC producer.
        """
        chan_id = mpsc_producer.chan_id
        if chan_id in self.mpsc_producers:
            return
        self.mpsc_producers[chan_id] = mpsc_producer

    # removed: legacy _fluxon_lease_keepalive_loop (keepalive managed by LeaseManagerHandle)

    def put_data(
        self, value: Dict[str, Union[int, float, bool, str, bytes, DLPacked]]
    ) -> Result[bool, ApiError]:
        """Put data to the MPMC channel.

        Callers may invoke put_data / close concurrently from multiple threads.
        close() publishes shutdown before touching inner resources. This method
        rechecks that signal after channel selection or bind completes, so a
        concurrent close cannot advance into capacity or payload work.
        """

        # Fast path: return error if already closed.
        if self.shutdown_ctl.closed:
            return Result[bool, ApiError].new_error(
                ProducerClosedError("MPMC producer is closed.")
            )

        if not isinstance(value, dict):
            return Result[bool, ApiError].new_error(
                InvalidArgumentError(
                    message=(
                        "MPMC put_data requires a flat dict payload: "
                        "Dict[str, Union[int, float, bool, str, bytes, dlpack]]"
                    )
                )
            )

        # Keep count_prefix and put_data outside _op_lock. Close signals the inner
        # producer directly, so it does not wait behind either network operation.
        capacity = int(self.chan_config["capacity"])  # validated upfront
        while True:
            if self.shutdown_ctl.closed:
                return Result[bool, ApiError].new_error(
                    ProducerClosedError("MPMC producer is closed.")
                )

            with self.shutdown_ctl._op_lock:
                if self.shutdown_ctl.closed:
                    return Result[bool, ApiError].new_error(
                        ProducerClosedError("MPMC producer is closed.")
                    )
                next_channel_result = self.mpmc_channel.get_next_available_channel(
                    self.api, self.chan_config, self
                )

            if not next_channel_result.is_ok():
                return Result[bool, ApiError].new_error(
                    next_channel_result.unwrap_error()
                )

            candidate = next_channel_result.unwrap()
            # Binding a newly published MPSC can block while its Rust handle is
            # initialized. Shutdown may arrive after the pre-bind check, so do
            # not continue into capacity or payload work once binding returns.
            if self.shutdown_ctl.closed:
                return Result[bool, ApiError].new_error(
                    ProducerClosedError("MPMC producer is closed.")
                )
            if not isinstance(candidate, MPSCChanProducer):
                time.sleep(0.02)
                continue

            producer_id = candidate.get_producer_id()
            assert producer_id is not None, (
                "Next_channel should have available producer_idx otherwise nowhere to put!"
            )
            mpsc_id = candidate.get_chan_id()
            assert isinstance(mpsc_id, str) and mpsc_id.isdigit(), f"invalid mpsc_id: {mpsc_id!r}"
            prefix = f"/mpmc/{self.mpmc_channel.mpmc_id}/mpsc_{mpsc_id}/"

            count_res: Optional[Result[int, ApiError]] = None
            count_error: Optional[ApiError] = None
            count_attempts = 10
            for attempt_idx in range(count_attempts):
                if self.shutdown_ctl.closed:
                    return Result[bool, ApiError].new_error(
                        ProducerClosedError("MPMC producer is closed.")
                    )

                # Capacity gating here uses the master-side derived prefix index.
                # It is suitable for aggregate backpressure, but it is not an
                # immediate strong-consistency visibility probe for a fresh put.
                count_res = self.api.count_prefix(prefix)
                if count_res.is_ok():
                    break
                err = count_res.unwrap_error()
                count_error = err
                if self.shutdown_ctl.closed:
                    return Result[bool, ApiError].new_error(
                        ProducerClosedError("MPMC producer is closed.")
                    )
                if not isinstance(err, NetworkError):
                    return Result[bool, ApiError].new_error(err)

                if attempt_idx + 1 == count_attempts:
                    break
                logging.warning(
                    "MPMCChanProducer mpmc_id=%s producer_idx=%s count_prefix failed "
                    "for prefix %s: %s; retrying attempt %d/%d",
                    self.mpmc_id,
                    producer_id,
                    prefix,
                    err,
                    attempt_idx + 2,
                    count_attempts,
                )
                time.sleep(0.1)
            assert count_res is not None
            if not count_res.is_ok():
                assert count_error is not None
                return Result[bool, ApiError].new_error(count_error)

            current = count_res.unwrap()
            assert isinstance(current, int), f"count_prefix returned non-int: {type(current)}"

            if self.shutdown_ctl.closed:
                return Result[bool, ApiError].new_error(
                    ProducerClosedError("MPMC producer is closed.")
                )

            if current >= capacity:
                blocking_observed_unix_ms = int(time.time() * 1000)
                try:
                    candidate.record_blocking_put_observed(blocking_observed_unix_ms)
                except Exception as e:  # noqa: BLE001
                    logging.warning(
                        "MPMCChanProducer mpmc_id=%s failed to record blocking put observation on mpsc_id=%s producer_idx=%s: %s",
                        self.mpmc_id,
                        candidate.get_chan_id(),
                        candidate.get_producer_id(),
                        e,
                    )
                logging.debug(
                    "MPMCChanProducer mpmc_id=%s capacity reached for prefix %s: count=%s, capacity=%s; sleep 1s",
                    self.mpmc_id,
                    prefix,
                    current,
                    capacity,
                )
                time.sleep(1.0)
                continue

            put_result = candidate.put_data(value)
            if put_result.is_ok():
                _ = put_result.unwrap()
                nonblocking_success_unix_ms = int(time.time() * 1000)
                try:
                    candidate.record_nonblocking_put_success(nonblocking_success_unix_ms)
                except Exception as e:  # noqa: BLE001
                    logging.warning(
                        "MPMCChanProducer mpmc_id=%s failed to record nonblocking put success on mpsc_id=%s producer_idx=%s: %s",
                        self.mpmc_id,
                        candidate.get_chan_id(),
                        candidate.get_producer_id(),
                        e,
                    )
                logging.debug(
                    f"MPMCChanProducer mpmc_id={self.mpmc_id} put success: "
                    f"mpsc_id={candidate.get_chan_id()} producer_idx={candidate.get_producer_id()} "
                )
                return Result[bool, ApiError].new_ok(True)

            err = put_result.unwrap_error()
            logging.error(
                "MPMCChanProducer mpmc_id=%s failed to put data on mpsc_id=%s producer_idx=%s: %s",
                self.mpmc_id,
                candidate.get_chan_id(),
                candidate.get_producer_id(),
                err,
            )

            # If the backend returns LeaseNotFound, the shared payload/etcd lease is no
            # longer valid. Do not attempt implicit rebuild (avoid hidden recovery paths).
            # Mark the whole MPMC producer as closed to prevent further puts; the caller
            # can rebuild if needed.
            # Prefer type-based check (PayloadLeaseNotFoundError) from py_error_from_kv_error;
            # fall back to string match for RPC deserialization paths that may yield NetworkError.
            if isinstance(err, PayloadLeaseNotFoundError) or (
                isinstance(err, NetworkError) and ("LeaseNotFound" in str(err))
            ):
                self.shutdown_ctl.close()
                return Result[bool, ApiError].new_error(
                    ProducerClosedError(
                        message="payload lease not found; mpmc producer is closed",
                        channel_id=self.get_chan_id(),
                        producer_idx=self.get_producer_id(),
                    )
                )

            # Non-LeaseNotFound: return the underlying error as-is.
            return Result[bool, ApiError].new_error(err)

    
    def close(self) -> Result[OkNone, ApiError]:
        """
        Close the MPMC producer.
        """
        assert hasattr(self, "shutdown_ctl"), "MPMCChanProducer.close called but 'shutdown_ctl' is missing"

        with self._close_lock:
            if self._close_done:
                return Result.new_ok(OK_NONE)
            self.shutdown_ctl.close()

            with self._new_or_get_mpsc_producer_lock:
                for mpsc_id, local_producer in list(self.mpsc_producers.items()):
                    close_result = local_producer.close()
                    if not close_result.is_ok():
                        return Result.new_error(close_result.unwrap_error())
                    close_result.unwrap()
                    self.mpsc_producers.pop(mpsc_id, None)

            if self.mpmc_channel is not None:
                channel_close_result = self.mpmc_channel.close()
                if not channel_close_result.is_ok():
                    return Result.new_error(channel_close_result.unwrap_error())
                channel_close_result.unwrap()
                self.mpmc_channel = None  # type: ignore[assignment]
            self._close_done = True
            return Result.new_ok(OK_NONE)

            # Payload lease keepalive is managed by MPMCChannel; nothing to drop here
    
    
    def __del__(self):
        """
        Destructor.
        """
        try:
            res = self.close()
            if res.is_ok():
                # Consume ok to satisfy strict Result policy
                res.unwrap()
            else:
                # Do not raise from __del__; log and consume error branch
                err = res.unwrap_error()
                logging.warning(
                    f"MPMCChanProducer.__del__ close returned error: {err}"
                )
        except Exception as e:  # noqa: BLE001
            # Avoid raising from destructor; log for diagnostics.
            logging.debug(f"MPMCChanProducer.__del__ cleanup error: {e}")

    def get_producer_id(self) -> str:
        return f"mpmc_{self.get_chan_id()}_fake_producer_id"

    def get_chan_id(self) -> str:
        """
        Get the channel id.
        """
        assert self.mpmc_id is not None, "MPMC channel ID is None"
        return self.mpmc_id


class MPMCChanConsumer(ChannelConsumer):
    """
    MPMC Consumer that binds to a specific MPSC channel.
    """
    
    def __init__(
        self,
        api: KvClient,
        mpmc_id: Optional[str],
        chan_config: Dict[str, int],
        etcd_client: Optional[etcd3.Etcd3Client] = None,
    ):
        """
        Initialize MPMC Consumer.
        
        Args:
            api(KvClient): KV store API
            api(KvClient): KV store API
            mpmc_id(Optional[str]): MPMC channel ID
            chan_config(Dict[str, int]): Channel configuration
            etcd_client(Optional[etcd3.Etcd3Client]): Etcd client
        """
        # Enforce zero-contribution store for channel usage via config
        api.ensure_zero_contribution_for_channel()

        # Validate config strictly (no implicit defaults/fallbacks).
        chan_config = validate_mpmc_config(chan_config, role=ChanRole.CONSUMER)

        self.api = api
        self.mpmc_id = mpmc_id
        self.chan_config = chan_config
        self.keep_alive_interval = chan_config["ttl_seconds"] / 2 - 0.5
        # Shared shutdown controller: used both by this consumer and
        # the internal MPMCChannel instance to coordinate close/ops.
        # put/get/close coordinate via this controller's lock and closed flag.
        self.shutdown_ctl = MqShutdownCtl()
        self._close_done = False
        self._close_lock = threading.Lock()
        
        if etcd_client is None:
            result: Result[etcd3.Etcd3Client, ApiError] = new_etcd_client(api)
            if not result.is_ok():
                raise ValueError(f"Failed to create etcd client: {result.unwrap_error()}")
            etcd_client = result.unwrap()
            assert etcd_client is not None, "etcd client is None"
        
        self.etcd_client: etcd3.Etcd3Client = etcd_client
        
        # Initialize MPMC channel
        if mpmc_id is not None:
            if not mpmc_id.isdigit():
                raise ValueError(f"invalid mpmc_id: {mpmc_id!r}")
            self.mpmc_channel = MPMCChannel.new_existed_global_mpmc_channel(
                mpmc_id,
                chan_config,
                etcd_client,
                ChanRole.CONSUMER,
                None,
                None,
                api,
                self.shutdown_ctl,
            )
        else:
            # Create new MPMC channel
            logging.debug(f"Creating new MPMC channel")
            self.mpmc_channel = MPMCChannel.new_global_mpmc_channel(
                chan_config,
                self.etcd_client,
                ChanRole.CONSUMER,
                None,
                None,
                api,
                self.shutdown_ctl,
            )
            logging.debug(f"New MPMC channel created, mpmc_id: {self.mpmc_channel.mpmc_id}")
            self.mpmc_id = self.mpmc_channel.mpmc_id
        
        # Initialize optional fields to avoid hasattr checks later
        self.mpsc_consumer: Optional[MPSCChanConsumer] = None
        self.bound_mpsc_id: Optional[str] = None

        # Get next available channel and bind to it
        fails=[]
        for i in range(10):
            next_channel_result = self.mpmc_channel.get_next_available_channel(self.api, self.chan_config)
            if not next_channel_result.is_ok():
                raise ValueError(f"Failed to get next available channel: {next_channel_result.unwrap_error()}")
            
            next_channel = next_channel_result.unwrap()
            if next_channel is None:
                raise ValueError("Failed to get valid channel")
            
            # We always get a consumer object now
            if isinstance(next_channel, MPSCChanConsumer):
                if next_channel._mpmc_ready_claimed:
                    self.mpsc_consumer = next_channel
                    self.bound_mpsc_id = next_channel.get_chan_id()
                    logging.debug(
                        "Binded mpmc consumer to already-claimed mpsc %s, mpmc_id: %s successfully",
                        self.bound_mpsc_id,
                        self.mpmc_id,
                    )
                    return

                # Direct bind path still claims ready here. Existing/new channels
                # claimed inside MPMCChannel return with _mpmc_ready_claimed=True.
                res=self.mark_channel_ready(next_channel.get_chan_id())
                if not res.is_ok():
                    logging.warning(f"Failed to mark channel ready: {res.unwrap_error()}")
                    # Close the just-created/bound MPSC consumer to avoid dangling consumers
                    try:
                        next_channel.close().unwrap()
                    except Exception as e:
                        logging.debug(f"close leaked MPSC consumer error: {e}")
                    fails.append(res.unwrap_error())
                    continue
                if res.unwrap():
                    self.mpsc_consumer = next_channel
                    self.bound_mpsc_id = next_channel.get_chan_id()
                    logging.debug(f"Binded mpmc consumer to mpsc {self.bound_mpsc_id}, mpmc_id: {self.mpmc_id} successfully")
                    
                    return
                else:
                    logging.warning(f"Failed to mark channel ready by condition, retry {i}")
                    # Close the MPSC consumer we just created/bound since we lost the race
                    try:
                        next_channel.close().unwrap()
                    except Exception as e:
                        logging.debug(f"close leaked MPSC consumer error: {e}")
                    fails.append("transaction failed")
                    continue
            else:
                raise ValueError(f"Unexpected channel type: {type(next_channel)}")
            
        raise ValueError(f"Failed to mark channel ready with {len(fails)} fails: {fails}")

    def get_chan_id(self) -> str:
        """
        Get the channel id.
        """
        assert self.mpmc_id is not None, "MPMC channel ID is None"
        return self.mpmc_id
    
    def get_consumer_id(self) -> str:
        """
        Get the consumer index.
        """
        return self.get_chan_id()
    
    def get_data(
        self, batch_size: int = 1, try_time: Optional[int] = None, prefetch_num: int = 0
    ) -> Result[List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]], ApiError]:
        """Get data from the bound MPSC channel.

        To cooperate with close(), hold MqShutdownCtl._op_lock before entering the
        underlying get_data call. If shutdown_ctl.closed is already set, return
        ChannelClosedError immediately to avoid blocking inside the internal loop
        after shutdown.
        """

        if self.shutdown_ctl.closed:
            return Result[
                List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]], ApiError
            ].new_error(
                ChannelClosedError(
                    message="Consumer is closed.",
                    channel_id=self.mpmc_id,
                )
            )

        with self.shutdown_ctl._op_lock:
            if self.shutdown_ctl.closed:
                return Result[
                    List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]], ApiError
                ].new_error(
                    ChannelClosedError(
                        message="Consumer is closed.",
                        channel_id=self.mpmc_id,
                    )
                )

            result = self.mpsc_consumer.get_data(
                batch_size, try_time, prefetch_num=prefetch_num
            )
            if not result.is_ok():
                err = result.unwrap_error()
                if self.shutdown_ctl.closed:
                    return Result[
                        List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]], ApiError
                    ].new_error(
                        ChannelClosedError(
                            message="Consumer is closed.",
                            channel_id=self.mpmc_id,
                        )
                    )
                return Result[
                    List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]], ApiError
                ].new_error(err)

            consumed_items = result.unwrap()
            assert consumed_items is not None, "consumed_items is None"

            if consumed_items == []:
                logging.debug(
                    f"MPMCChanConsumer mpmc_id={self.mpmc_id} got empty list of data from mpsc_id={self.bound_mpsc_id}"
                )

            # Process items
            data_list: List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]] = []
            for item in consumed_items:
                if isinstance(item, ConsumedMessage):
                    data_list.append(item.data)
                else:
                    # MPSC is standalone, just extract data
                    data_list.append(item)
            return Result[
                List[Dict[str, Union[int, float, bool, str, bytes, DLPacked]]], ApiError
            ].new_ok(data_list)
    
    # Removed: try_get_data to avoid split API; use get_data with try_time=0 for non-blocking semantics.
    
    def close(self) -> Result[OkNone, ApiError]:
        """Close the MPMC consumer with eager wake-up for in-flight get_data."""
        with self._close_lock:
            if self._close_done:
                return Result.new_ok(OK_NONE)
            self.shutdown_ctl.close()

            # Inner close first wakes any active get and verifies that this exact
            # consumer membership is gone. The ready key must remain owned until
            # that deletion succeeds, otherwise another consumer can bind while
            # producers still observe the old membership.
            if self.mpsc_consumer is not None:
                close_result = self.mpsc_consumer.close()
                if not close_result.is_ok():
                    return Result.new_error(close_result.unwrap_error())
                close_result.unwrap()

            # Wait until an in-flight outer get has observed the shutdown signal.
            with self.shutdown_ctl._op_lock:
                pass

            mpmc_id = self.mpmc_id
            assert mpmc_id is not None, "MPMC channel ID is None"
            member_id = None
            if self.mpmc_channel is not None:
                member_id = self.mpmc_channel.mpmc_member_id
            if isinstance(member_id, int):
                delete_res = stable_delete_ready_keys_for_member(self.api, mpmc_id, member_id)
                if not delete_res.is_ok():
                    return Result.new_error(delete_res.unwrap_error())
                delete_res.unwrap()
            elif self.bound_mpsc_id is not None:
                ready_key = _new_mpmc_ready_channel_key(mpmc_id, self.bound_mpsc_id)
                try:
                    self.etcd_client.delete(ready_key)
                    if self.etcd_client.get(ready_key)[0] is not None:
                        raise RuntimeError(f"ready key still exists after delete: {ready_key}")
                except Exception as e:  # noqa: BLE001
                    return Result.new_error(
                        ResourceCleanupError(
                            message=f"failed to delete ready key {ready_key}: {e}",
                            resource_type="mpmc_ready_key",
                            resource_id=ready_key,
                        )
                    )

            self.mpsc_consumer = None

            try:
                if hasattr(self, "rate_limiter") and self.rate_limiter is not None:
                    self.rate_limiter.close()
            except Exception as e:  # noqa: BLE001
                logging.warning(
                    f"MPMCChanConsumer {self.get_consumer_id()} failed to close rate limiter: {e}"
                )

            if self.mpmc_channel is not None:
                channel_close_result = self.mpmc_channel.close()
                if not channel_close_result.is_ok():
                    return Result.new_error(channel_close_result.unwrap_error())
                channel_close_result.unwrap()
            self.mpmc_channel = None  # type: ignore[assignment]
            self._close_done = True

            return Result.new_ok(OK_NONE)

    def __del__(self):
        """Destructor: call close() and consume Result, avoid raising from GC."""
        try:
            res = self.close()
            if res.is_ok():
                res.unwrap()
            else:
                err = res.unwrap_error()
                logging.warning(
                    f"MPMCChanConsumer.__del__ close returned error: {err}"
                )
        except Exception as e:
            logging.debug(f"MPMCChanConsumer.__del__ cleanup error: {e}")

    def mark_channel_ready(self, mpsc_id: str) -> Result[bool, ApiError]:
        """
        Mark a MPSC channel as ready by creating a KV entry.
        This method is called when the consumer binds to a channel.
        
        Args:
            mpsc_id(str): MPSC channel ID
            
        Returns:
            Result[bool]: Success status (True if newly marked, False if already marked)
        """

        if not isinstance(mpsc_id, str) or not mpsc_id.isdigit():
            errmsg = f"Invalid mpsc_id: {mpsc_id!r}"
            logging.warning(errmsg)
            raise ValueError(errmsg)

        logging.debug(f"Marking mpsc {mpsc_id} of MPMC channel {self.mpmc_id} as ready")
        # assert self.mpmc_id is not None, "MPMC channel ID is None"
        if self.mpmc_id is None:
            errmsg = f"MPMC channel ID is None, mpmc_id: {self.mpmc_id}"
            logging.warning(errmsg)
            raise ValueError(errmsg)

        res = self.mpmc_channel.try_claim_ready_channel(mpsc_id)
        if not res.is_ok():
            return res
        claimed = res.unwrap()
        if claimed:
            logging.debug(f"Marked channel {mpsc_id} of MPMC channel {self.mpmc_id} as ready")
        else:
            logging.debug(f"Channel {mpsc_id} of MPMC channel {self.mpmc_id} is already marked as ready")
        return Result.new_ok(claimed)
